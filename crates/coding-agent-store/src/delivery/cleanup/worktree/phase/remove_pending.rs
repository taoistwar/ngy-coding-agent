use crate::delivery::{
    CleanupOperationState, CleanupTransitionOutcome, DeliveryTimestamp,
    EnterWorktreeRemovePendingRequest, WorktreeDisposition,
};
use crate::tasks::current_timestamp;
use crate::{Store, StoreError};

use super::super::super::replay::{CleanupTransitionLookup, lookup_transition};
use super::super::common::{
    advance_operation, branch_is_retained, load_context, operation_is_current, verify_applied,
    worktree_fact_is,
};

impl Store {
    pub async fn enter_worktree_remove_pending(
        &self,
        request: EnterWorktreeRemovePendingRequest,
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
            CleanupOperationState::UnlockedPendingRemove,
            CleanupOperationState::RemovePending,
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
        if !operation_is_current(
            &context,
            request.anchor,
            CleanupOperationState::UnlockedPendingRemove,
        ) || !worktree_fact_is(&context, WorktreeDisposition::RetainedUnlocked)
            || !branch_is_retained(&context)
        {
            transaction.commit().await?;
            return Ok(CleanupTransitionOutcome::Conflict);
        }
        let timestamp: DeliveryTimestamp = current_timestamp()?.to_string().parse()?;
        let disposition_version = context.disposition.worktree_version;
        advance_operation(
            &mut transaction,
            &context,
            CleanupOperationState::RemovePending,
            target_version,
            disposition_version,
            None,
            timestamp,
        )
        .await?;
        let receipt = verify_applied(
            &mut transaction,
            request.anchor,
            CleanupOperationState::UnlockedPendingRemove,
            CleanupOperationState::RemovePending,
            target_version,
            WorktreeDisposition::RetainedUnlocked,
            disposition_version,
            None,
        )
        .await?;
        transaction.commit().await?;
        Ok(CleanupTransitionOutcome::Applied(receipt))
    }
}
