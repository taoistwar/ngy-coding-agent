use crate::StoreError;
use crate::delivery::{
    DeliverySourceRecord, DeliveryTimestamp, DeliveryVersion, MergeOperationRecord,
    MergeOperationState,
};
use crate::tasks::current_timestamp;

use super::super::merge_invariant;
use super::super::model::{CompleteMergeRequest, MergeTransitionReceipt};
use super::super::replay::{TransitionLookup, lookup_transition, version_i64};

pub(super) async fn apply_fresh_merge(
    connection: &mut sqlx::SqliteConnection,
    operation: &MergeOperationRecord,
    source: &DeliverySourceRecord,
    request: &CompleteMergeRequest,
    target_version: DeliveryVersion,
) -> Result<MergeTransitionReceipt, StoreError> {
    let timestamp: DeliveryTimestamp = current_timestamp()?.to_string().parse()?;
    let updated = sqlx::query(
        "UPDATE task_merge_operations \
         SET merged_disposition_task_id = ?, state = 'merged', failure_code = NULL, \
             version = ?, updated_at = ? \
         WHERE operation_id = ? AND task_id = ? AND state = 'merge_pending' AND version = ? \
           AND merged_disposition_task_id IS NULL AND abort_child_receipt_id IS NULL",
    )
    .bind(request.task_id.to_string())
    .bind(version_i64(target_version)?)
    .bind(timestamp.to_string())
    .bind(request.operation_id.to_string())
    .bind(request.task_id.to_string())
    .bind(version_i64(request.expected_version)?)
    .execute(&mut *connection)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(merge_invariant());
    }
    insert_initial_disposition(&mut *connection, operation, source, timestamp).await?;
    match lookup_transition(
        &mut *connection,
        request.operation_id,
        target_version,
        MergeOperationState::MergePending,
        MergeOperationState::Merged,
        None,
    )
    .await?
    {
        TransitionLookup::Exact(receipt) => Ok(receipt),
        TransitionLookup::Missing | TransitionLookup::Conflict => Err(merge_invariant()),
    }
}

async fn insert_initial_disposition(
    connection: &mut sqlx::SqliteConnection,
    operation: &MergeOperationRecord,
    source: &DeliverySourceRecord,
    timestamp: DeliveryTimestamp,
) -> Result<(), StoreError> {
    let source_commit = source
        .expected_source_commit
        .as_ref()
        .ok_or_else(merge_invariant)?;
    let inserted = sqlx::query(
        "INSERT INTO task_artifact_dispositions ( \
             task_id, repository_id, attempt, merged_operation_id, delivery_source_task_id, \
             source_commit_oid, worktree_state, worktree_version, worktree_failure_code, \
             worktree_updated_at, branch_state, branch_version, branch_failure_code, \
             branch_updated_at, created_at \
         ) VALUES (?, ?, ?, ?, ?, ?, 'retained_locked', 1, NULL, ?, \
                   'retained', 1, NULL, ?, ?)",
    )
    .bind(operation.provenance.identity.task_id().to_string())
    .bind(operation.provenance.identity.repository_id().to_string())
    .bind(i64::from(operation.provenance.identity.attempt()))
    .bind(operation.operation_id.to_string())
    .bind(operation.provenance.identity.task_id().to_string())
    .bind(source_commit.as_str())
    .bind(timestamp.to_string())
    .bind(timestamp.to_string())
    .bind(timestamp.to_string())
    .execute(connection)
    .await?;
    if inserted.rows_affected() == 1 {
        Ok(())
    } else {
        Err(merge_invariant())
    }
}
