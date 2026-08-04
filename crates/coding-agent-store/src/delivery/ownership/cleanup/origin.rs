use std::str::FromStr;

use coding_agent_domain::{ClientRequestId, TaskId};
use sqlx::SqliteConnection;

use crate::StoreError;
use crate::delivery::receipts::lookup_receipt;
use crate::delivery::{
    ArtifactDispositionRecord, CleanupKind, CleanupOperationRecord, CleanupOperationState,
    DeleteBranchCommandRequest, DeliveryAcceptedOperationState, DeliveryOperationId,
    DeliveryVersion, MergeOperationRecord, RemoveWorktreeCommandRequest, validate_cleanup_state,
};

use super::super::decode::{parse_branch_state, parse_version, parse_worktree_state};
use super::super::ownership_invariant;

struct HistoricalDisposition {
    version: DeliveryVersion,
    state: String,
    transition_id: i64,
    transitioned_at: String,
}

pub(in crate::delivery::ownership) async fn validate_cleanup_origin(
    connection: &mut SqliteConnection,
    cleanup: &CleanupOperationRecord,
    disposition: &ArtifactDispositionRecord,
    merged: &MergeOperationRecord,
) -> Result<(), StoreError> {
    let initial_state = load_initial_cleanup_state(&mut *connection, cleanup).await?;
    let worktree = load_disposition_before(
        &mut *connection,
        "worktree_disposition",
        cleanup.disposition_task_id,
        cleanup.initial_transition_id,
    )
    .await?;
    let branch = load_disposition_before(
        &mut *connection,
        "branch_disposition",
        cleanup.disposition_task_id,
        cleanup.initial_transition_id,
    )
    .await?;
    validate_historical_anchors(cleanup, disposition, merged, &worktree, &branch)?;

    let worktree_state = parse_worktree_state(worktree.state)?;
    let branch_state = parse_branch_state(branch.state)?;
    validate_cleanup_state(cleanup.kind, initial_state, worktree_state, branch_state)
        .map_err(|_| ownership_invariant())?;
    let origin_version = match cleanup.kind {
        CleanupKind::RemoveWorktree => worktree.version,
        CleanupKind::DeleteBranch => branch.version,
    };
    validate_origin_receipt(
        &mut *connection,
        cleanup,
        merged.operation_id,
        origin_version,
        initial_state,
    )
    .await
}

async fn load_initial_cleanup_state(
    connection: &mut SqliteConnection,
    cleanup: &CleanupOperationRecord,
) -> Result<CleanupOperationState, StoreError> {
    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT to_state, transitioned_at \
         FROM task_delivery_operation_transitions \
         WHERE transition_id = ? AND entity_kind = 'cleanup_operation' \
           AND entity_id = ? AND entity_version = 1 \
           AND from_state = 'absent' AND failure_code IS NULL",
    )
    .bind(cleanup.initial_transition_id)
    .bind(cleanup.operation_id.to_string())
    .fetch_optional(connection)
    .await?;
    let (state, transitioned_at) = row.ok_or_else(ownership_invariant)?;
    if transitioned_at != cleanup.created_at.to_string() {
        return Err(ownership_invariant());
    }
    state.parse().map_err(|_| ownership_invariant())
}

async fn load_disposition_before(
    connection: &mut SqliteConnection,
    entity_kind: &str,
    task_id: TaskId,
    cleanup_transition_id: i64,
) -> Result<HistoricalDisposition, StoreError> {
    let row: Option<(i64, String, i64, String)> = sqlx::query_as(
        "SELECT entity_version, to_state, transition_id, transitioned_at \
         FROM task_delivery_operation_transitions \
         WHERE entity_kind = ? AND entity_id = ? AND transition_id < ? \
         ORDER BY transition_id DESC LIMIT 1",
    )
    .bind(entity_kind)
    .bind(task_id.to_string())
    .bind(cleanup_transition_id)
    .fetch_optional(connection)
    .await?;
    let (version, state, transition_id, transitioned_at) = row.ok_or_else(ownership_invariant)?;
    Ok(HistoricalDisposition {
        version: parse_version(version)?,
        state,
        transition_id,
        transitioned_at,
    })
}

fn validate_historical_anchors(
    cleanup: &CleanupOperationRecord,
    disposition: &ArtifactDispositionRecord,
    merged: &MergeOperationRecord,
    worktree: &HistoricalDisposition,
    branch: &HistoricalDisposition,
) -> Result<(), StoreError> {
    let created_at = cleanup.created_at.to_string();
    let target_shape_is_exact = match cleanup.kind {
        CleanupKind::RemoveWorktree => {
            cleanup.origin_target_head.is_none()
                && cleanup.expected_target_ref.is_none()
                && cleanup.expected_target_head.is_none()
        }
        CleanupKind::DeleteBranch => {
            cleanup.expected_target_ref.as_ref() == Some(&merged.target_branch)
                && cleanup.origin_target_head.is_some()
                && cleanup.expected_target_head.is_some()
        }
    };
    let exact = cleanup.identity == disposition.identity
        && cleanup.disposition_task_id == disposition.identity.task_id()
        && disposition.merged_operation_id == merged.operation_id
        && merged.provenance.identity == cleanup.identity
        && merged.current_transition_id < cleanup.initial_transition_id
        && disposition.worktree_initial_transition_id < cleanup.initial_transition_id
        && disposition.branch_initial_transition_id < cleanup.initial_transition_id
        && worktree.transition_id < cleanup.initial_transition_id
        && branch.transition_id < cleanup.initial_transition_id
        && worktree.transitioned_at <= created_at
        && branch.transitioned_at <= created_at
        && disposition.created_at.to_string() <= created_at
        && target_shape_is_exact;
    if exact {
        Ok(())
    } else {
        Err(ownership_invariant())
    }
}

async fn validate_origin_receipt(
    connection: &mut SqliteConnection,
    cleanup: &CleanupOperationRecord,
    merged_operation_id: DeliveryOperationId,
    origin_version: DeliveryVersion,
    initial_state: CleanupOperationState,
) -> Result<(), StoreError> {
    let client_request_id = ClientRequestId::from_str(&cleanup.origin_receipt_id.to_string())
        .map_err(|_| ownership_invariant())?;
    let accepted_state = accepted_state(cleanup.kind, initial_state)?;
    let receipt = match cleanup.kind {
        CleanupKind::RemoveWorktree => {
            let command = RemoveWorktreeCommandRequest::try_new(
                client_request_id,
                cleanup.identity.task_id(),
                origin_version,
                merged_operation_id,
                cleanup.expected_source_ref.clone(),
                cleanup.expected_source_oid.clone(),
            )
            .map_err(|_| ownership_invariant())?;
            lookup_receipt(connection, &command).await
        }
        CleanupKind::DeleteBranch => {
            let command = DeleteBranchCommandRequest::try_new(
                client_request_id,
                cleanup.identity.task_id(),
                origin_version,
                merged_operation_id,
                cleanup.expected_source_ref.clone(),
                cleanup.expected_source_oid.clone(),
                cleanup
                    .expected_target_ref
                    .clone()
                    .ok_or_else(ownership_invariant)?,
                cleanup
                    .origin_target_head
                    .clone()
                    .ok_or_else(ownership_invariant)?,
            )
            .map_err(|_| ownership_invariant())?;
            lookup_receipt(connection, &command).await
        }
    };
    let receipt = match receipt {
        Ok(Some(receipt)) => receipt,
        Ok(None)
        | Err(StoreError::IdempotencyConflict)
        | Err(StoreError::InvariantViolation(_)) => {
            return Err(ownership_invariant());
        }
        Err(error @ StoreError::Database(_)) => return Err(error),
        Err(_) => return Err(ownership_invariant()),
    };
    let exact = receipt.client_request_id == cleanup.origin_receipt_id
        && receipt.operation_id == cleanup.operation_id
        && receipt.identity == cleanup.identity
        && receipt.accepted_operation_version == DeliveryVersion::initial()
        && receipt.accepted_operation_state == accepted_state
        && receipt.created_at == cleanup.created_at;
    if exact {
        Ok(())
    } else {
        Err(ownership_invariant())
    }
}

fn accepted_state(
    kind: CleanupKind,
    state: CleanupOperationState,
) -> Result<DeliveryAcceptedOperationState, StoreError> {
    match (kind, state) {
        (CleanupKind::RemoveWorktree, CleanupOperationState::UnlockPending) => {
            Ok(DeliveryAcceptedOperationState::UnlockPending)
        }
        (CleanupKind::RemoveWorktree, CleanupOperationState::RemovePending) => {
            Ok(DeliveryAcceptedOperationState::RemovePending)
        }
        (CleanupKind::DeleteBranch, CleanupOperationState::DeletePending) => {
            Ok(DeliveryAcceptedOperationState::DeletePending)
        }
        _ => Err(ownership_invariant()),
    }
}
