use crate::delivery::{
    CleanupOperationState, CleanupTransitionOutcome, DeliveryTimestamp,
    RecordWorktreeUnlockedRequest, WorktreeDisposition,
};
use crate::tasks::current_timestamp;
use crate::{Store, StoreError};

use super::super::super::replay::{CleanupTransitionLookup, lookup_transition};
use super::super::common::{
    advance_disposition, advance_operation, branch_is_retained, load_context, operation_is_current,
    verify_paired_applied, worktree_fact_is,
};

impl Store {
    pub async fn record_worktree_unlocked(
        &self,
        request: RecordWorktreeUnlockedRequest,
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
            CleanupOperationState::UnlockPending,
            CleanupOperationState::UnlockedPendingRemove,
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
            CleanupOperationState::UnlockPending,
        ) || !worktree_fact_is(&context, WorktreeDisposition::RetainedLocked)
            || !branch_is_retained(&context)
        {
            transaction.commit().await?;
            return Ok(CleanupTransitionOutcome::Conflict);
        }
        let timestamp: DeliveryTimestamp = current_timestamp()?.to_string().parse()?;
        let disposition_version = advance_disposition(
            &mut transaction,
            &context,
            target_version,
            CleanupOperationState::UnlockedPendingRemove,
            WorktreeDisposition::RetainedUnlocked,
            None,
            timestamp,
        )
        .await?;
        advance_operation(
            &mut transaction,
            &context,
            CleanupOperationState::UnlockedPendingRemove,
            target_version,
            disposition_version,
            None,
            timestamp,
        )
        .await?;
        let receipt = verify_paired_applied(
            &mut transaction,
            request.anchor,
            CleanupOperationState::UnlockPending,
            CleanupOperationState::UnlockedPendingRemove,
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
