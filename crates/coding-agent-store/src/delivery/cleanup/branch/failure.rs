use crate::delivery::{
    BranchDisposition, CleanupOperationState, CleanupTransitionOutcome, DeliveryTimestamp,
    RecordBranchCleanupFailureRequest,
};
use crate::tasks::current_timestamp;
use crate::{Store, StoreError};

use super::super::replay::{CleanupTransitionLookup, lookup_transition};
use super::common::{
    advance_operation, load_context, operation_is_current, retained_branch_facts_are_exact,
    verify_applied,
};

impl Store {
    pub async fn record_branch_cleanup_failure(
        &self,
        request: RecordBranchCleanupFailureRequest,
    ) -> Result<CleanupTransitionOutcome, StoreError> {
        let target_version = request.anchor.expected_version.next()?;
        let failure = request.reason.as_failure_code();
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let Some(context) = load_context(&mut transaction, request.anchor).await? else {
            transaction.commit().await?;
            return Ok(CleanupTransitionOutcome::Conflict);
        };
        match lookup_transition(
            &mut transaction,
            request.anchor.operation_id,
            target_version,
            CleanupOperationState::DeletePending,
            CleanupOperationState::Failed,
            Some(failure),
        )
        .await?
        {
            CleanupTransitionLookup::Exact(receipt) => {
                transaction.commit().await?;
                return Ok(CleanupTransitionOutcome::Existing(receipt));
            }
            CleanupTransitionLookup::Conflict => {
                transaction.commit().await?;
                return Ok(CleanupTransitionOutcome::Conflict);
            }
            CleanupTransitionLookup::Missing => {}
        }
        if !operation_is_current(&context, request.anchor)
            || !retained_branch_facts_are_exact(&context)
        {
            transaction.commit().await?;
            return Ok(CleanupTransitionOutcome::Conflict);
        }
        let timestamp: DeliveryTimestamp = current_timestamp()?.to_string().parse()?;
        let disposition_version = context.disposition.branch_version;
        advance_operation(
            &mut transaction,
            &context,
            CleanupOperationState::Failed,
            target_version,
            disposition_version,
            Some(failure),
            timestamp,
        )
        .await?;
        let receipt = verify_applied(
            &mut transaction,
            request.anchor,
            CleanupOperationState::Failed,
            target_version,
            BranchDisposition::Retained,
            disposition_version,
            Some(failure),
        )
        .await?;
        transaction.commit().await?;
        Ok(CleanupTransitionOutcome::Applied(receipt))
    }
}
