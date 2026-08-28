use crate::delivery::ownership::load_merge_operation_exact;
use crate::delivery::{
    DeliveryTimestamp, DeliveryVersion, MergeOperationRecord, MergeOperationState,
};
use crate::tasks::current_timestamp;
use crate::{Store, StoreError};

use super::super::conflicts::insert_conflict_paths;
use super::super::merge_invariant;
use super::super::model::{BeginMergeAbortRequest, MergeTransitionOutcome, MergeTransitionReceipt};
use super::super::replay::{
    OperationLookup, TransitionLookup, load_operation_for_caller, lookup_transition, version_i64,
};
use super::validate::{abort_facts_match, begin_input_matches};

impl Store {
    pub async fn begin_merge_abort(
        &self,
        request: BeginMergeAbortRequest,
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
            classify_begin_replay(&mut transaction, &operation, &request, target_version).await?
        {
            transaction.commit().await?;
            return Ok(outcome);
        }
        if !begin_input_matches(&operation, &request)
            || abort_child_is_bound(&mut transaction, &request).await?
        {
            transaction.commit().await?;
            return Ok(MergeTransitionOutcome::Conflict);
        }
        let receipt =
            apply_begin_abort(&mut transaction, &operation, &request, target_version).await?;
        let updated = load_merge_operation_exact(&mut transaction, request.operation_id).await?;
        if !abort_facts_match(&updated, &request) {
            return Err(merge_invariant());
        }
        transaction.commit().await?;
        Ok(MergeTransitionOutcome::Applied(receipt))
    }
}

async fn classify_begin_replay(
    connection: &mut sqlx::SqliteConnection,
    operation: &MergeOperationRecord,
    request: &BeginMergeAbortRequest,
    target_version: DeliveryVersion,
) -> Result<Option<MergeTransitionOutcome>, StoreError> {
    match lookup_transition(
        connection,
        request.operation_id,
        target_version,
        MergeOperationState::MergePending,
        MergeOperationState::AbortPending,
        None,
    )
    .await?
    {
        TransitionLookup::Exact(receipt) if abort_facts_match(operation, request) => {
            Ok(Some(MergeTransitionOutcome::Existing(receipt)))
        }
        TransitionLookup::Exact(_) | TransitionLookup::Conflict => {
            Ok(Some(MergeTransitionOutcome::Conflict))
        }
        TransitionLookup::Missing => Ok(None),
    }
}

async fn abort_child_is_bound(
    connection: &mut sqlx::SqliteConnection,
    request: &BeginMergeAbortRequest,
) -> Result<bool, StoreError> {
    let bound_to: Option<String> = sqlx::query_scalar(
        "SELECT operation_id FROM task_merge_operations \
         WHERE abort_child_receipt_id = ? LIMIT 1",
    )
    .bind(request.proof.child_receipt_id.to_string())
    .fetch_optional(connection)
    .await?;
    Ok(bound_to.is_some())
}

async fn apply_begin_abort(
    connection: &mut sqlx::SqliteConnection,
    operation: &MergeOperationRecord,
    request: &BeginMergeAbortRequest,
    target_version: DeliveryVersion,
) -> Result<MergeTransitionReceipt, StoreError> {
    let timestamp: DeliveryTimestamp = current_timestamp()?.to_string().parse()?;
    let updated = sqlx::query(
        "UPDATE task_merge_operations \
         SET abort_child_receipt_id = ?, abort_merge_head_oid = ?, \
              abort_index_stages_digest = ?, abort_worktree_digest = ?, \
              abort_merge_autostash_proof = 'absent', conflict_path_count = ?, \
              state = 'abort_pending', \
              failure_code = NULL, version = ?, updated_at = ? \
         WHERE operation_id = ? AND task_id = ? AND state = 'merge_pending' AND version = ? \
           AND abort_child_receipt_id IS NULL AND abort_merge_head_oid IS NULL \
            AND abort_index_stages_digest IS NULL AND abort_worktree_digest IS NULL \
            AND abort_merge_autostash_proof IS NULL AND conflict_path_count IS NULL \
            AND source_commit_oid = ?",
    )
    .bind(request.proof.child_receipt_id.to_string())
    .bind(request.proof.merge_head.as_str())
    .bind(request.proof.index_stages_digest.as_str())
    .bind(request.proof.worktree_digest.as_str())
    .bind(i64::try_from(request.proof.conflict_paths.len()).map_err(|_| merge_invariant())?)
    .bind(version_i64(target_version)?)
    .bind(timestamp.to_string())
    .bind(operation.operation_id.to_string())
    .bind(request.task_id.to_string())
    .bind(version_i64(request.expected_version)?)
    .bind(request.proof.merge_head.as_str())
    .execute(&mut *connection)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(merge_invariant());
    }
    insert_conflict_paths(
        &mut *connection,
        request.operation_id,
        &request.proof.conflict_paths,
    )
    .await?;
    require_begin_transition(connection, request, target_version).await
}

async fn require_begin_transition(
    connection: &mut sqlx::SqliteConnection,
    request: &BeginMergeAbortRequest,
    target_version: DeliveryVersion,
) -> Result<MergeTransitionReceipt, StoreError> {
    match lookup_transition(
        connection,
        request.operation_id,
        target_version,
        MergeOperationState::MergePending,
        MergeOperationState::AbortPending,
        None,
    )
    .await?
    {
        TransitionLookup::Exact(receipt) => Ok(receipt),
        TransitionLookup::Missing | TransitionLookup::Conflict => Err(merge_invariant()),
    }
}
