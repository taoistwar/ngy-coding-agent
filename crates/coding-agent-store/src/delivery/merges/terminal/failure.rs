use crate::delivery::ownership::{load_merge_operation_exact, load_source_exact};
use crate::delivery::{
    DeliverySourceRecord, DeliveryTimestamp, DeliveryVersion, MergeOperationRecord,
    MergeOperationState,
};
use crate::tasks::current_timestamp;
use crate::{Store, StoreError};

use super::super::merge_invariant;
use super::super::model::{MergeTransitionOutcome, RecordMergeKnownFailureRequest};
use super::super::replay::{
    OperationLookup, TransitionLookup, load_operation_for_caller, lookup_transition, version_i64,
};
use super::validate::{audit_failed_source_origin, committed_source_matches, require_transition};

pub(super) async fn record(
    store: &Store,
    request: RecordMergeKnownFailureRequest,
) -> Result<MergeTransitionOutcome, StoreError> {
    let target_version = request.expected_version.next()?;
    let failure = request.reason.as_failure_code();
    let mut transaction = store.pool.begin_with("BEGIN IMMEDIATE").await?;
    let operation =
        match load_operation_for_caller(&mut transaction, request.operation_id, request.task_id)
            .await?
        {
            OperationLookup::Exact(operation) => operation,
            OperationLookup::WrongTask | OperationLookup::Missing => {
                transaction.commit().await?;
                return Ok(MergeTransitionOutcome::Conflict);
            }
        };
    match lookup_transition(
        &mut transaction,
        request.operation_id,
        target_version,
        request.expected_state,
        MergeOperationState::Failed,
        Some(failure),
    )
    .await?
    {
        TransitionLookup::Exact(receipt) => {
            audit_failed_source_origin(&mut transaction, &operation, request.expected_state)
                .await?;
            transaction.commit().await?;
            return Ok(MergeTransitionOutcome::Existing(receipt));
        }
        TransitionLookup::Conflict => {
            transaction.commit().await?;
            return Ok(MergeTransitionOutcome::Conflict);
        }
        TransitionLookup::Missing => {}
    }
    if operation.state != request.expected_state
        || operation.version != request.expected_version
        || operation.failure_code.is_some()
    {
        transaction.commit().await?;
        return Ok(MergeTransitionOutcome::Conflict);
    }
    let Some(source) = load_source_exact(&mut transaction, request.task_id).await? else {
        transaction.commit().await?;
        return Ok(MergeTransitionOutcome::Conflict);
    };
    if !committed_source_matches(&source, &operation) {
        transaction.commit().await?;
        return Ok(MergeTransitionOutcome::Conflict);
    }
    let timestamp: DeliveryTimestamp = current_timestamp()?.to_string().parse()?;
    apply_known_failure(
        &mut transaction,
        &operation,
        &source,
        &request,
        target_version,
        timestamp,
    )
    .await?;
    let receipt = require_transition(
        &mut transaction,
        request.operation_id,
        target_version,
        request.expected_state,
        MergeOperationState::Failed,
        failure,
    )
    .await?;
    let updated = load_merge_operation_exact(&mut transaction, request.operation_id).await?;
    audit_failed_source_origin(&mut transaction, &updated, request.expected_state).await?;
    transaction.commit().await?;
    Ok(MergeTransitionOutcome::Applied(receipt))
}

async fn apply_known_failure(
    connection: &mut sqlx::SqliteConnection,
    operation: &MergeOperationRecord,
    source: &DeliverySourceRecord,
    request: &RecordMergeKnownFailureRequest,
    target_version: DeliveryVersion,
    timestamp: DeliveryTimestamp,
) -> Result<(), StoreError> {
    let source_commit = source
        .expected_source_commit
        .as_ref()
        .ok_or_else(merge_invariant)?;
    let updated = match request.expected_state {
        MergeOperationState::Accepted => {
            sqlx::query(
                "UPDATE task_merge_operations \
                 SET delivery_source_task_id = ?, source_commit_oid = ?, state = 'failed', \
                     failure_code = ?, version = ?, updated_at = ? \
                 WHERE operation_id = ? AND task_id = ? AND state = 'accepted' AND version = ? \
                   AND delivery_source_task_id IS NULL AND source_commit_oid IS NULL \
                   AND expected_merge_commit_oid IS NULL",
            )
            .bind(request.task_id.to_string())
            .bind(source_commit.as_str())
            .bind(request.reason.as_failure_code())
            .bind(version_i64(target_version)?)
            .bind(timestamp.to_string())
            .bind(operation.operation_id.to_string())
            .bind(request.task_id.to_string())
            .bind(version_i64(request.expected_version)?)
            .execute(&mut *connection)
            .await?
        }
        MergeOperationState::MergePending => {
            sqlx::query(
                "UPDATE task_merge_operations \
                 SET state = 'failed', failure_code = ?, version = ?, updated_at = ? \
                 WHERE operation_id = ? AND task_id = ? AND state = 'merge_pending' AND version = ? \
                   AND delivery_source_task_id = ? AND source_commit_oid = ? \
                   AND expected_merge_commit_oid IS NOT NULL",
            )
            .bind(request.reason.as_failure_code())
            .bind(version_i64(target_version)?)
            .bind(timestamp.to_string())
            .bind(operation.operation_id.to_string())
            .bind(request.task_id.to_string())
            .bind(version_i64(request.expected_version)?)
            .bind(request.task_id.to_string())
            .bind(source_commit.as_str())
            .execute(&mut *connection)
            .await?
        }
        _ => return Err(merge_invariant()),
    };
    if updated.rows_affected() == 1 {
        Ok(())
    } else {
        Err(merge_invariant())
    }
}
