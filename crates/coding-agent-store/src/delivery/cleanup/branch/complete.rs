use crate::delivery::{
    BranchDisposition, CleanupOperationState, CleanupTransitionOutcome,
    CompleteBranchCleanupRequest, DeliveryTimestamp,
};
use crate::tasks::current_timestamp;
use crate::{Store, StoreError};

use super::super::replay::{CleanupTransitionLookup, lookup_transition};
use super::common::{
    advance_disposition, advance_operation, load_context, operation_is_current,
    retained_branch_facts_are_exact, verify_paired_applied,
};

impl Store {
    pub async fn complete_branch_cleanup(
        &self,
        request: CompleteBranchCleanupRequest,
    ) -> Result<CleanupTransitionOutcome, StoreError> {
        let target_version = request.anchor.expected_version.next()?;
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
            CleanupOperationState::Completed,
            None,
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
        let disposition_version = advance_disposition(
            &mut transaction,
            &context,
            target_version,
            CleanupOperationState::Completed,
            BranchDisposition::Deleted,
            None,
            timestamp,
        )
        .await?;
        advance_operation(
            &mut transaction,
            &context,
            CleanupOperationState::Completed,
            target_version,
            disposition_version,
            None,
            timestamp,
        )
        .await?;
        let receipt = verify_paired_applied(
            &mut transaction,
            request.anchor,
            CleanupOperationState::Completed,
            target_version,
            BranchDisposition::Deleted,
            disposition_version,
            None,
        )
        .await?;
        transaction.commit().await?;
        Ok(CleanupTransitionOutcome::Applied(receipt))
    }
}
