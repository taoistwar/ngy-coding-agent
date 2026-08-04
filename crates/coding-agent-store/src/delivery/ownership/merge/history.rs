use sqlx::SqliteConnection;

use crate::StoreError;
use crate::delivery::{DeliveryTimestamp, MergeOperationRecord, MergeOperationState};

use super::super::accepted::{AcceptReceiptAudit, audit_accept_receipt};
use super::super::origin::{PreflightReceiptAudit, audit_preflight_receipt};
use super::super::ownership_invariant;

pub(super) async fn validate_merge_historical_shape(
    connection: &mut SqliteConnection,
    operation: &MergeOperationRecord,
) -> Result<(), StoreError> {
    if audit_preflight_receipt(connection, operation).await? != PreflightReceiptAudit::Exact {
        return Err(ownership_invariant());
    }
    validate_abort_child_receipt_uniqueness(connection, operation).await?;
    validate_phase_history(connection, operation).await?;
    validate_accepted_metadata_history(connection, operation).await
}

async fn validate_abort_child_receipt_uniqueness(
    connection: &mut SqliteConnection,
    operation: &MergeOperationRecord,
) -> Result<(), StoreError> {
    let Some(child_receipt_id) = operation.abort_child_receipt_id else {
        return Ok(());
    };
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_merge_operations WHERE abort_child_receipt_id = ?",
    )
    .bind(child_receipt_id.to_string())
    .fetch_one(connection)
    .await?;
    if count == 1 {
        Ok(())
    } else {
        Err(ownership_invariant())
    }
}

async fn validate_phase_history(
    connection: &mut SqliteConnection,
    operation: &MergeOperationRecord,
) -> Result<(), StoreError> {
    let from_state: Option<String> = sqlx::query_scalar(
        "SELECT from_state FROM task_delivery_operation_transitions \
         WHERE transition_id = ? AND entity_kind = 'merge_operation' \
           AND entity_id = ? AND entity_version = ? AND to_state = ?",
    )
    .bind(operation.current_transition_id)
    .bind(operation.operation_id.to_string())
    .bind(i64::try_from(operation.version.get()).map_err(|_| ownership_invariant())?)
    .bind(operation.state.as_str())
    .fetch_optional(connection)
    .await?;
    let phase = match (operation.state, from_state.as_deref()) {
        (MergeOperationState::PreflightPending, Some("absent"))
        | (MergeOperationState::Rejected, Some("preflight_pending"))
        | (MergeOperationState::Stale, Some("preflight_pending"))
        | (MergeOperationState::ReconciliationRequired, Some("preflight_pending")) => {
            MergeFactPhase::Pending
        }
        (MergeOperationState::PreflightReady, Some("preflight_pending"))
        | (MergeOperationState::Conflict, Some("preflight_pending"))
        | (MergeOperationState::Stale, Some("preflight_ready"))
        | (MergeOperationState::Superseded, Some("preflight_ready"))
        | (MergeOperationState::ReconciliationRequired, Some("preflight_ready")) => {
            MergeFactPhase::PreflightResult
        }
        (MergeOperationState::Accepted, Some("preflight_ready"))
        | (MergeOperationState::ReconciliationRequired, Some("accepted")) => {
            MergeFactPhase::Accepted
        }
        (MergeOperationState::Failed, Some("accepted")) => MergeFactPhase::SourceBound,
        (MergeOperationState::MergePending, Some("accepted"))
        | (MergeOperationState::Failed, Some("merge_pending"))
        | (MergeOperationState::ReconciliationRequired, Some("merge_pending")) => {
            MergeFactPhase::MergePending
        }
        (MergeOperationState::AbortPending, Some("merge_pending"))
        | (MergeOperationState::Conflict, Some("abort_pending"))
        | (MergeOperationState::ReconciliationRequired, Some("abort_pending")) => {
            MergeFactPhase::AbortPending
        }
        (MergeOperationState::Merged, Some("merge_pending")) => MergeFactPhase::Merged,
        _ => return Err(ownership_invariant()),
    };
    if fact_phase_matches(operation, phase) {
        Ok(())
    } else {
        Err(ownership_invariant())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MergeFactPhase {
    Pending,
    PreflightResult,
    Accepted,
    SourceBound,
    MergePending,
    AbortPending,
    Merged,
}

fn fact_phase_matches(operation: &MergeOperationRecord, phase: MergeFactPhase) -> bool {
    let preflight_field_count = usize::from(operation.merge_base.is_some())
        + usize::from(operation.candidate_merge_tree.is_some());
    let accepted_field_count = usize::from(operation.accept_receipt_id.is_some())
        + usize::from(operation.merge_metadata.is_some());
    let source_field_count = usize::from(operation.delivery_source_task_id.is_some())
        + usize::from(operation.source_commit.is_some());
    let abort_field_count = usize::from(operation.abort_child_receipt_id.is_some())
        + usize::from(operation.abort_merge_head.is_some())
        + usize::from(operation.abort_index_stages_digest.is_some())
        + usize::from(operation.abort_worktree_digest.is_some())
        + usize::from(operation.abort_merge_autostash_proof.is_some());
    if !matches!(preflight_field_count, 0 | 2)
        || !matches!(accepted_field_count, 0 | 2)
        || !matches!(source_field_count, 0 | 2)
        || !matches!(abort_field_count, 0 | 5)
    {
        return false;
    }
    let preflight_result = preflight_field_count == 2;
    let accepted = accepted_field_count == 2;
    let source_bound = source_field_count == 2;
    let expected_merge = operation.expected_merge_commit.is_some();
    let abort = abort_field_count == 5;
    if abort
        && (operation.abort_merge_head.as_ref() != operation.source_commit.as_ref()
            || operation.abort_merge_autostash_proof.as_deref() != Some("absent"))
    {
        return false;
    }
    let disposition = operation.merged_disposition_task_id.is_some();
    let actual = (
        preflight_result,
        accepted,
        source_bound,
        expected_merge,
        abort,
        disposition,
    );
    let expected = match phase {
        MergeFactPhase::Pending => (false, false, false, false, false, false),
        MergeFactPhase::PreflightResult => (true, false, false, false, false, false),
        MergeFactPhase::Accepted => (true, true, false, false, false, false),
        MergeFactPhase::SourceBound => (true, true, true, false, false, false),
        MergeFactPhase::MergePending => (true, true, true, true, false, false),
        MergeFactPhase::AbortPending => (true, true, true, true, true, false),
        MergeFactPhase::Merged => (true, true, true, true, false, true),
    };
    actual == expected
}

async fn validate_accepted_metadata_history(
    connection: &mut SqliteConnection,
    operation: &MergeOperationRecord,
) -> Result<(), StoreError> {
    let Some(accept_receipt_id) = operation.accept_receipt_id else {
        if operation.merge_metadata.is_none() {
            return Ok(());
        }
        return Err(ownership_invariant());
    };
    if !matches!(
        audit_accept_receipt(&mut *connection, operation).await?,
        AcceptReceiptAudit::Exact(_)
    ) {
        return Err(ownership_invariant());
    }
    let row: Option<(i64, String, String, Option<String>, String, String)> = sqlx::query_as(
        "SELECT t.entity_version, t.from_state, t.to_state, t.failure_code, \
                t.transitioned_at, r.created_at \
         FROM task_delivery_command_receipts r \
         JOIN task_delivery_operation_transitions t \
           ON t.entity_kind = 'merge_operation' AND t.entity_id = r.operation_id \
          AND t.entity_version = r.accepted_operation_version \
         WHERE r.client_request_id = ? AND r.command_kind = 'accept_merge' \
           AND r.operation_id = ?",
    )
    .bind(accept_receipt_id.to_string())
    .bind(operation.operation_id.to_string())
    .fetch_optional(connection)
    .await?;
    let Some((_version, from, to, failure, transitioned_at, receipt_at)) = row else {
        return Err(ownership_invariant());
    };
    let timestamp: DeliveryTimestamp =
        transitioned_at.parse().map_err(|_| ownership_invariant())?;
    let expected_date = format!(
        "{} +0000",
        timestamp.as_utc().as_offset_date_time().unix_timestamp()
    );
    let metadata = operation
        .merge_metadata
        .as_ref()
        .ok_or_else(ownership_invariant)?;
    let exact = from == "preflight_ready"
        && to == "accepted"
        && failure.is_none()
        && transitioned_at == receipt_at
        && metadata.author_date_bytes == expected_date
        && metadata.committer_date_bytes == expected_date;
    if exact {
        Ok(())
    } else {
        Err(ownership_invariant())
    }
}
