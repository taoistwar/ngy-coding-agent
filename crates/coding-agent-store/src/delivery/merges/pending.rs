mod apply;
mod replay;
mod validate;

use crate::delivery::ownership::{load_merge_operation_exact, load_source_exact};
use crate::{Store, StoreError};

use super::model::{EnterMergePendingRequest, MergeTransitionOutcome};
use super::replay::{OperationLookup, load_operation_for_caller};
use apply::apply_fresh_pending;
use replay::classify_pending_replay;
use validate::{accepted_input_matches, pending_facts_match};

impl Store {
    pub async fn enter_merge_pending(
        &self,
        request: EnterMergePendingRequest,
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
        let Some(source) = load_source_exact(&mut transaction, request.task_id).await? else {
            transaction.commit().await?;
            return Ok(MergeTransitionOutcome::Conflict);
        };
        if let Some(outcome) = classify_pending_replay(
            &mut transaction,
            &operation,
            &source,
            &request,
            target_version,
        )
        .await?
        {
            transaction.commit().await?;
            return Ok(outcome);
        }
        if !accepted_input_matches(&operation, &source, &request) {
            transaction.commit().await?;
            return Ok(MergeTransitionOutcome::Conflict);
        }
        let receipt = apply_fresh_pending(
            &mut transaction,
            &operation,
            &source,
            &request,
            target_version,
        )
        .await?;
        let updated = load_merge_operation_exact(&mut transaction, request.operation_id).await?;
        if !pending_facts_match(&updated, &source, &request, target_version) {
            return Err(super::merge_invariant());
        }
        transaction.commit().await?;
        Ok(MergeTransitionOutcome::Applied(receipt))
    }
}
