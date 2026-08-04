use sqlx::SqliteConnection;

use crate::StoreError;
use crate::delivery::ownership::load_disposition_exact;
use crate::delivery::{
    ArtifactDispositionRecord, BranchDisposition, CleanupKind, CleanupOperationRecord,
    CleanupOperationState, DeliveryTimestamp, DeliveryVersion, WorktreeDisposition,
};

use super::super::cleanup_invariant;
use super::super::model::{CleanupOperationAnchor, CleanupTransitionReceipt};
use super::super::replay::{
    CleanupOperationLookup, load_operation_for_caller, require_transition, version_i64,
};

pub(super) struct BranchContext {
    pub(super) operation: CleanupOperationRecord,
    pub(super) disposition: ArtifactDispositionRecord,
}

pub(super) async fn load_context(
    connection: &mut SqliteConnection,
    anchor: CleanupOperationAnchor,
) -> Result<Option<BranchContext>, StoreError> {
    let CleanupOperationLookup::Exact(operation) =
        load_operation_for_caller(&mut *connection, anchor.operation_id, anchor.task_id).await?
    else {
        return Ok(None);
    };
    let Some(disposition) = load_disposition_exact(&mut *connection, anchor.task_id).await? else {
        return Err(cleanup_invariant());
    };
    let exact = operation.kind == CleanupKind::DeleteBranch
        && operation.disposition_task_id == anchor.task_id
        && operation.identity == disposition.identity
        && operation.expected_source_oid == disposition.source_commit
        && operation.expected_target_ref.is_some()
        && operation.expected_target_head.is_some()
        && operation.origin_target_head.is_some();
    if !exact {
        return Err(cleanup_invariant());
    }
    Ok(Some(BranchContext {
        operation: *operation,
        disposition,
    }))
}

pub(super) fn operation_is_current(
    context: &BranchContext,
    anchor: CleanupOperationAnchor,
) -> bool {
    context.operation.operation_id == anchor.operation_id
        && context.operation.version == anchor.expected_version
        && context.operation.state == CleanupOperationState::DeletePending
        && context.operation.failure_code.is_none()
        && context.operation.expected_disposition_version == context.disposition.branch_version
}

pub(super) fn retained_branch_facts_are_exact(context: &BranchContext) -> bool {
    context.disposition.worktree_state == WorktreeDisposition::Removed
        && context.disposition.worktree_failure_code.is_none()
        && context.disposition.branch_state == BranchDisposition::Retained
        && context.disposition.branch_failure_code.is_none()
}

pub(super) async fn advance_disposition(
    connection: &mut SqliteConnection,
    context: &BranchContext,
    operation_version: DeliveryVersion,
    operation_state: CleanupOperationState,
    branch_state: BranchDisposition,
    failure_code: Option<&str>,
    timestamp: DeliveryTimestamp,
) -> Result<DeliveryVersion, StoreError> {
    let version = context.disposition.branch_version.next()?;
    let updated = sqlx::query(
        "UPDATE task_artifact_dispositions \
         SET branch_state = ?, branch_version = ?, branch_failure_code = ?, \
             branch_cleanup_operation_id = ?, branch_cleanup_operation_version = ?, \
             branch_cleanup_operation_state = ?, branch_updated_at = ? \
         WHERE task_id = ? AND branch_state = 'retained' AND branch_version = ? \
           AND worktree_state = 'removed'",
    )
    .bind(branch_state.as_str())
    .bind(version_i64(version)?)
    .bind(failure_code)
    .bind(context.operation.operation_id.to_string())
    .bind(version_i64(operation_version)?)
    .bind(operation_state.as_str())
    .bind(timestamp.to_string())
    .bind(context.operation.identity.task_id().to_string())
    .bind(version_i64(context.disposition.branch_version)?)
    .execute(&mut *connection)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(cleanup_invariant());
    }
    Ok(version)
}

pub(super) async fn advance_operation(
    connection: &mut SqliteConnection,
    context: &BranchContext,
    state: CleanupOperationState,
    version: DeliveryVersion,
    disposition_version: DeliveryVersion,
    failure_code: Option<&str>,
    timestamp: DeliveryTimestamp,
) -> Result<(), StoreError> {
    let updated = sqlx::query(
        "UPDATE task_cleanup_operations \
         SET expected_disposition_version = ?, state = ?, failure_code = ?, \
             version = ?, updated_at = ? \
         WHERE operation_id = ? AND task_id = ? AND kind = 'delete_branch' \
           AND state = 'delete_pending' AND version = ? \
           AND expected_disposition_version = ? AND expected_target_head = ?",
    )
    .bind(version_i64(disposition_version)?)
    .bind(state.as_str())
    .bind(failure_code)
    .bind(version_i64(version)?)
    .bind(timestamp.to_string())
    .bind(context.operation.operation_id.to_string())
    .bind(context.operation.identity.task_id().to_string())
    .bind(version_i64(context.operation.version)?)
    .bind(version_i64(context.operation.expected_disposition_version)?)
    .bind(
        context
            .operation
            .expected_target_head
            .as_ref()
            .ok_or_else(cleanup_invariant)?
            .as_str(),
    )
    .execute(&mut *connection)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(cleanup_invariant());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn verify_applied(
    connection: &mut SqliteConnection,
    anchor: CleanupOperationAnchor,
    to: CleanupOperationState,
    version: DeliveryVersion,
    disposition_state: BranchDisposition,
    disposition_version: DeliveryVersion,
    failure_code: Option<&str>,
) -> Result<CleanupTransitionReceipt, StoreError> {
    let receipt = require_transition(
        &mut *connection,
        anchor.operation_id,
        version,
        CleanupOperationState::DeletePending,
        to,
        failure_code,
    )
    .await?;
    let context = load_context(&mut *connection, anchor)
        .await?
        .ok_or_else(cleanup_invariant)?;
    let exact = context.operation.version == version
        && context.operation.state == to
        && context.operation.expected_disposition_version == disposition_version
        && context
            .operation
            .failure_code
            .as_ref()
            .map(|code| code.as_str())
            == failure_code
        && context.disposition.branch_state == disposition_state
        && context.disposition.branch_version == disposition_version
        && context.operation.target_head_at(version)
            == context.operation.expected_target_head.as_ref();
    if exact {
        Ok(receipt)
    } else {
        Err(cleanup_invariant())
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn verify_paired_applied(
    connection: &mut SqliteConnection,
    anchor: CleanupOperationAnchor,
    to: CleanupOperationState,
    version: DeliveryVersion,
    disposition_state: BranchDisposition,
    disposition_version: DeliveryVersion,
    failure_code: Option<&str>,
) -> Result<CleanupTransitionReceipt, StoreError> {
    let receipt = verify_applied(
        &mut *connection,
        anchor,
        to,
        version,
        disposition_state,
        disposition_version,
        failure_code,
    )
    .await?;
    let context = load_context(&mut *connection, anchor)
        .await?
        .ok_or_else(cleanup_invariant)?;
    let pointer_is_exact = context.disposition.branch_cleanup_operation_id
        == Some(context.operation.operation_id)
        && context.disposition.branch_cleanup_operation_version == Some(version)
        && context.disposition.branch_cleanup_operation_state == Some(to);
    let failure_is_exact = context
        .disposition
        .branch_failure_code
        .as_ref()
        .map(|code| code.as_str())
        == failure_code;
    if pointer_is_exact
        && failure_is_exact
        && context.disposition.branch_updated_at == receipt.transitioned_at
    {
        Ok(receipt)
    } else {
        Err(cleanup_invariant())
    }
}
