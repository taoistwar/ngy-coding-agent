use crate::delivery::ownership::load_merge_operation_exact;
use crate::delivery::{
    DeliveryTimestamp, DeliveryVersion, MergeOperationRecord, MergeOperationState,
};
use crate::tasks::current_timestamp;
use crate::{Store, StoreError};

use super::super::merge_invariant;
use super::super::model::{
    CompleteMergeAbortRequest, MergeTransitionOutcome, MergeTransitionReceipt,
};
use super::super::replay::{
    OperationLookup, TransitionLookup, load_operation_for_caller, lookup_transition, version_i64,
};
use super::validate::abort_applied_proof_matches;

impl Store {
    pub async fn complete_merge_abort(
        &self,
        request: CompleteMergeAbortRequest,
    ) -> Result<MergeTransitionOutcome, StoreError> {
        let target_version = request.expected_version.next()?;
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let operation = match load_operation_for_caller(
            &mut transaction,
            request.operation_id,
            request.task_id,
        )
        .await?
        {
            OperationLookup::Exact(operation) => operation,
            OperationLookup::WrongTask | OperationLookup::Missing => {
                transaction.commit().await?;
                return Ok(MergeTransitionOutcome::Conflict);
            }
        };
        if let Some(outcome) =
            classify_complete_replay(&mut transaction, &operation, &request, target_version).await?
        {
            transaction.commit().await?;
            return Ok(outcome);
        }
        if operation.state != MergeOperationState::AbortPending
            || operation.version != request.expected_version
            || operation.failure_code.is_some()
            || !abort_applied_proof_matches(&operation, &request)
        {
            transaction.commit().await?;
            return Ok(MergeTransitionOutcome::Conflict);
        }
        let receipt = apply_complete_abort(&mut transaction, &request, target_version).await?;
        let updated = load_merge_operation_exact(&mut transaction, request.operation_id).await?;
        if !abort_applied_proof_matches(&updated, &request) {
            return Err(merge_invariant());
        }
        transaction.commit().await?;
        Ok(MergeTransitionOutcome::Applied(receipt))
    }
}

async fn classify_complete_replay(
    connection: &mut sqlx::SqliteConnection,
    operation: &MergeOperationRecord,
    request: &CompleteMergeAbortRequest,
    target_version: DeliveryVersion,
) -> Result<Option<MergeTransitionOutcome>, StoreError> {
    match lookup_transition(
        connection,
        request.operation_id,
        target_version,
        MergeOperationState::AbortPending,
        MergeOperationState::Conflict,
        Some("MERGE_CONFLICT"),
    )
    .await?
    {
        TransitionLookup::Exact(receipt) if abort_applied_proof_matches(operation, request) => {
            Ok(Some(MergeTransitionOutcome::Existing(receipt)))
        }
        TransitionLookup::Exact(_) | TransitionLookup::Conflict => {
            Ok(Some(MergeTransitionOutcome::Conflict))
        }
        TransitionLookup::Missing => Ok(None),
    }
}

async fn apply_complete_abort(
    connection: &mut sqlx::SqliteConnection,
    request: &CompleteMergeAbortRequest,
    target_version: DeliveryVersion,
) -> Result<MergeTransitionReceipt, StoreError> {
    let timestamp: DeliveryTimestamp = current_timestamp()?.to_string().parse()?;
    let updated = sqlx::query(
        "UPDATE task_merge_operations \
         SET state = 'conflict', failure_code = 'MERGE_CONFLICT', \
              version = ?, updated_at = ? \
          WHERE operation_id = ? AND task_id = ? AND state = 'abort_pending' AND version = ? \
            AND conflict_path_count > 0",
    )
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
    require_complete_transition(connection, request, target_version).await
}

async fn require_complete_transition(
    connection: &mut sqlx::SqliteConnection,
    request: &CompleteMergeAbortRequest,
    target_version: DeliveryVersion,
) -> Result<MergeTransitionReceipt, StoreError> {
    match lookup_transition(
        connection,
        request.operation_id,
        target_version,
        MergeOperationState::AbortPending,
        MergeOperationState::Conflict,
        Some("MERGE_CONFLICT"),
    )
    .await?
    {
        TransitionLookup::Exact(receipt) => Ok(receipt),
        TransitionLookup::Missing | TransitionLookup::Conflict => Err(merge_invariant()),
    }
}
