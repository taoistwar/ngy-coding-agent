use crate::delivery::{DeliveryTimestamp, MergeOperationRecord};
use crate::tasks::current_timestamp;
use crate::{Store, StoreError};

use super::conflicts::insert_conflict_paths;
use super::merge_invariant;
use super::model::{
    MergePreflightResult, MergeTransitionOutcome, RecordMergePreflightResultRequest,
};
use super::replay::{
    OperationLookup, TransitionLookup, load_operation_for_caller, lookup_transition, version_i64,
};

impl Store {
    pub async fn record_merge_preflight_result(
        &self,
        request: RecordMergePreflightResultRequest,
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
        validate_result_algorithm(&operation, &request.result)?;

        match lookup_transition(
            &mut transaction,
            request.operation_id,
            target_version,
            crate::delivery::MergeOperationState::PreflightPending,
            request.result.state(),
            request.result.failure_code(),
        )
        .await?
        {
            TransitionLookup::Exact(receipt) => {
                validate_persisted_result(&operation, &request.result)?;
                transaction.commit().await?;
                return Ok(MergeTransitionOutcome::Existing(receipt));
            }
            TransitionLookup::Conflict => {
                transaction.commit().await?;
                return Ok(MergeTransitionOutcome::Conflict);
            }
            TransitionLookup::Missing => {}
        }

        if operation.state != crate::delivery::MergeOperationState::PreflightPending
            || operation.version != request.expected_version
            || operation.failure_code.is_some()
        {
            transaction.commit().await?;
            return Ok(MergeTransitionOutcome::Conflict);
        }

        let timestamp: DeliveryTimestamp = current_timestamp()?.to_string().parse()?;
        apply_result(&mut transaction, &request, target_version, timestamp).await?;
        let receipt = match lookup_transition(
            &mut transaction,
            request.operation_id,
            target_version,
            crate::delivery::MergeOperationState::PreflightPending,
            request.result.state(),
            request.result.failure_code(),
        )
        .await?
        {
            TransitionLookup::Exact(receipt) => receipt,
            TransitionLookup::Missing | TransitionLookup::Conflict => {
                return Err(merge_invariant());
            }
        };
        let updated = match load_operation_for_caller(
            &mut transaction,
            request.operation_id,
            request.task_id,
        )
        .await?
        {
            OperationLookup::Exact(operation) => operation,
            OperationLookup::WrongTask | OperationLookup::Missing => {
                return Err(merge_invariant());
            }
        };
        validate_persisted_result(&updated, &request.result)?;
        transaction.commit().await?;
        Ok(MergeTransitionOutcome::Applied(receipt))
    }
}

fn validate_result_algorithm(
    operation: &MergeOperationRecord,
    result: &MergePreflightResult,
) -> Result<(), StoreError> {
    let expected = operation.provenance.base_commit.algorithm();
    let valid = match result {
        MergePreflightResult::Ready {
            merge_base,
            candidate_merge_tree,
        }
        | MergePreflightResult::Conflict {
            merge_base,
            candidate_merge_tree,
            ..
        } => merge_base.algorithm() == expected && candidate_merge_tree.algorithm() == expected,
        MergePreflightResult::Rejected(_)
        | MergePreflightResult::Stale(_)
        | MergePreflightResult::ReconciliationRequired(_) => true,
    };
    if valid {
        Ok(())
    } else {
        Err(StoreError::Delivery(
            crate::delivery::DeliveryError::InvalidCommandRequest,
        ))
    }
}

async fn apply_result(
    connection: &mut sqlx::SqliteConnection,
    request: &RecordMergePreflightResultRequest,
    target_version: crate::delivery::DeliveryVersion,
    timestamp: DeliveryTimestamp,
) -> Result<(), StoreError> {
    let (merge_base, merge_tree, conflict_path_count) = match &request.result {
        MergePreflightResult::Ready {
            merge_base,
            candidate_merge_tree,
        }
        | MergePreflightResult::Conflict {
            merge_base,
            candidate_merge_tree,
            ..
        } => (
            Some(merge_base.as_str()),
            Some(candidate_merge_tree.as_str()),
            match &request.result {
                MergePreflightResult::Conflict { paths, .. } => {
                    Some(i64::try_from(paths.len()).map_err(|_| merge_invariant())?)
                }
                _ => None,
            },
        ),
        _ => (None, None, None),
    };
    let updated = sqlx::query(
        "UPDATE task_merge_operations \
         SET merge_base_oid = ?, candidate_merge_tree_oid = ?, conflict_path_count = ?, \
             state = ?, failure_code = ?, \
             version = ?, updated_at = ? \
         WHERE operation_id = ? AND task_id = ? AND state = 'preflight_pending' AND version = ? \
           AND merge_base_oid IS NULL AND candidate_merge_tree_oid IS NULL",
    )
    .bind(merge_base)
    .bind(merge_tree)
    .bind(conflict_path_count)
    .bind(request.result.state().as_str())
    .bind(request.result.failure_code())
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
    if let MergePreflightResult::Conflict { paths, .. } = &request.result {
        insert_conflict_paths(connection, request.operation_id, paths).await?;
    }
    Ok(())
}

fn validate_persisted_result(
    operation: &MergeOperationRecord,
    result: &MergePreflightResult,
) -> Result<(), StoreError> {
    let exact = match result {
        MergePreflightResult::Ready {
            merge_base,
            candidate_merge_tree,
        } => {
            operation.conflict_path_count.is_none()
                && operation.merge_base.as_ref() == Some(merge_base)
                && operation.candidate_merge_tree.as_ref() == Some(candidate_merge_tree)
                && operation.conflicts.is_empty()
        }
        MergePreflightResult::Conflict {
            merge_base,
            candidate_merge_tree,
            paths,
        } => {
            operation.conflict_path_count == u8::try_from(paths.len()).ok()
                && operation.merge_base.as_ref() == Some(merge_base)
                && operation.candidate_merge_tree.as_ref() == Some(candidate_merge_tree)
                && operation.conflicts == paths.encoded
        }
        MergePreflightResult::Rejected(_)
        | MergePreflightResult::Stale(_)
        | MergePreflightResult::ReconciliationRequired(_) => {
            operation.conflict_path_count.is_none()
                && operation.merge_base.is_none()
                && operation.candidate_merge_tree.is_none()
                && operation.conflicts.is_empty()
        }
    };
    if exact {
        Ok(())
    } else {
        Err(merge_invariant())
    }
}
