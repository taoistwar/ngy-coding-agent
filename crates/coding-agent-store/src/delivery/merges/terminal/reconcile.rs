use crate::delivery::ownership::load_merge_operation_exact;
use crate::delivery::{DeliveryTimestamp, MergeOperationState};
use crate::tasks::current_timestamp;
use crate::{Store, StoreError};

use super::super::merge_invariant;
use super::super::model::{MergeTransitionOutcome, ReconcileMergeRequest};
use super::super::replay::{
    OperationLookup, TransitionLookup, load_operation_for_caller, lookup_transition, version_i64,
};
use super::validate::{
    audit_reconciliation_source_origin, merge_only_reconciliation_is_blocked, require_transition,
};

pub(super) async fn record(
    store: &Store,
    request: ReconcileMergeRequest,
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
        MergeOperationState::ReconciliationRequired,
        Some(failure),
    )
    .await?
    {
        TransitionLookup::Exact(receipt) => {
            audit_reconciliation_source_origin(
                &mut transaction,
                &operation,
                request.expected_state,
            )
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
    if merge_only_reconciliation_is_blocked(
        &mut transaction,
        &operation,
        request.expected_state,
        request.reason,
    )
    .await?
    {
        transaction.commit().await?;
        return Ok(MergeTransitionOutcome::Conflict);
    }
    let timestamp: DeliveryTimestamp = current_timestamp()?.to_string().parse()?;
    let updated = sqlx::query(
        "UPDATE task_merge_operations \
         SET state = 'reconciliation_required', failure_code = ?, version = ?, updated_at = ? \
         WHERE operation_id = ? AND task_id = ? AND state = ? AND version = ?",
    )
    .bind(failure)
    .bind(version_i64(target_version)?)
    .bind(timestamp.to_string())
    .bind(request.operation_id.to_string())
    .bind(request.task_id.to_string())
    .bind(request.expected_state.as_str())
    .bind(version_i64(request.expected_version)?)
    .execute(&mut *transaction)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(merge_invariant());
    }
    let receipt = require_transition(
        &mut transaction,
        request.operation_id,
        target_version,
        request.expected_state,
        MergeOperationState::ReconciliationRequired,
        failure,
    )
    .await?;
    let updated = load_merge_operation_exact(&mut transaction, request.operation_id).await?;
    audit_reconciliation_source_origin(&mut transaction, &updated, request.expected_state).await?;
    transaction.commit().await?;
    Ok(MergeTransitionOutcome::Applied(receipt))
}
