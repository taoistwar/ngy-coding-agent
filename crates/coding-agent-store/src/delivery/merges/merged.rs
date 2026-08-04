mod apply;
mod replay;
mod validate;

use crate::delivery::ownership::{load_merge_operation_exact, validate_merged_disposition_origin};
use crate::{Store, StoreError};

use super::merge_invariant;
use super::model::{CompleteMergeRequest, MergeTransitionOutcome};
use super::replay::{OperationLookup, load_operation_for_caller};
use apply::apply_fresh_merge;
use replay::classify_merge_replay;
use validate::{
    applied_proof_matches, fresh_input_matches, require_committed_source,
    require_merged_disposition,
};

impl Store {
    pub async fn complete_merge(
        &self,
        request: CompleteMergeRequest,
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
            classify_merge_replay(&mut transaction, &operation, &request, target_version).await?
        {
            transaction.commit().await?;
            return Ok(outcome);
        }
        if !fresh_input_matches(&operation, &request) {
            transaction.commit().await?;
            return Ok(MergeTransitionOutcome::Conflict);
        }
        let source = require_committed_source(&mut transaction, &operation).await?;
        if !applied_proof_matches(&operation, &source, &request) {
            transaction.commit().await?;
            return Ok(MergeTransitionOutcome::Conflict);
        }

        let receipt = apply_fresh_merge(
            &mut transaction,
            &operation,
            &source,
            &request,
            target_version,
        )
        .await?;
        let merged = load_merge_operation_exact(&mut transaction, request.operation_id).await?;
        let disposition = require_merged_disposition(&mut transaction, &merged).await?;
        validate_merged_disposition_origin(&mut transaction, &merged, &source, &disposition)
            .await?;
        if !applied_proof_matches(&merged, &source, &request) {
            return Err(merge_invariant());
        }
        transaction.commit().await?;
        Ok(MergeTransitionOutcome::Applied(receipt))
    }
}
