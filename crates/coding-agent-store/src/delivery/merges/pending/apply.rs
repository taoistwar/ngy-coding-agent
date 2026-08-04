use crate::StoreError;
use crate::delivery::{
    DeliverySourceRecord, DeliveryTimestamp, DeliveryVersion, MergeOperationRecord,
    MergeOperationState,
};
use crate::tasks::current_timestamp;

use super::super::merge_invariant;
use super::super::model::{EnterMergePendingRequest, MergeTransitionReceipt};
use super::super::replay::{TransitionLookup, lookup_transition, version_i64};

pub(super) async fn apply_fresh_pending(
    connection: &mut sqlx::SqliteConnection,
    operation: &MergeOperationRecord,
    source: &DeliverySourceRecord,
    request: &EnterMergePendingRequest,
    target_version: DeliveryVersion,
) -> Result<MergeTransitionReceipt, StoreError> {
    let source_commit = source
        .expected_source_commit
        .as_ref()
        .ok_or_else(merge_invariant)?;
    let timestamp: DeliveryTimestamp = current_timestamp()?.to_string().parse()?;
    let updated = sqlx::query(
        "UPDATE task_merge_operations \
         SET delivery_source_task_id = ?, source_commit_oid = ?, \
             expected_merge_commit_oid = ?, state = 'merge_pending', failure_code = NULL, \
             version = ?, updated_at = ? \
         WHERE operation_id = ? AND task_id = ? AND state = 'accepted' AND version = ? \
           AND delivery_source_task_id IS NULL AND source_commit_oid IS NULL \
           AND expected_merge_commit_oid IS NULL",
    )
    .bind(request.task_id.to_string())
    .bind(source_commit.as_str())
    .bind(request.proof.expected_merge_commit.as_str())
    .bind(version_i64(target_version)?)
    .bind(timestamp.to_string())
    .bind(operation.operation_id.to_string())
    .bind(request.task_id.to_string())
    .bind(version_i64(request.expected_version)?)
    .execute(&mut *connection)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(merge_invariant());
    }
    match lookup_transition(
        connection,
        request.operation_id,
        target_version,
        MergeOperationState::Accepted,
        MergeOperationState::MergePending,
        None,
    )
    .await?
    {
        TransitionLookup::Exact(receipt) => Ok(receipt),
        TransitionLookup::Missing | TransitionLookup::Conflict => Err(merge_invariant()),
    }
}
