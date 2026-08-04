use crate::delivery::{
    CleanupOperationState, CleanupTransitionOutcome, DeliveryTimestamp,
    RecordWorktreeCleanupFailureRequest, WorktreeDisposition,
};
use crate::tasks::current_timestamp;
use crate::{Store, StoreError};

use super::super::replay::{CleanupTransitionLookup, lookup_transition};
use super::common::{
    advance_operation, branch_is_retained, load_context, operation_is_current, verify_applied,
    worktree_fact_is,
};

impl Store {
    pub async fn record_worktree_cleanup_failure(
        &self,
        request: RecordWorktreeCleanupFailureRequest,
    ) -> Result<CleanupTransitionOutcome, StoreError> {
        let target_version = request.anchor.expected_version.next()?;
        let failure = request.reason.as_failure_code();
        let expected_fact = expected_worktree_fact(request.expected_state);
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let Some(context) = load_context(&mut transaction, request.anchor).await? else {
            transaction.commit().await?;
            return Ok(CleanupTransitionOutcome::Conflict);
        };
        match lookup_transition(
            &mut transaction,
            request.anchor.operation_id,
            target_version,
            request.expected_state,
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
        if !operation_is_current(&context, request.anchor, request.expected_state)
            || !worktree_fact_is(&context, expected_fact)
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
            request.expected_state,
            CleanupOperationState::Failed,
            target_version,
            expected_fact,
            disposition_version,
            Some(failure),
        )
        .await?;
        transaction.commit().await?;
        Ok(CleanupTransitionOutcome::Applied(receipt))
    }
}

fn expected_worktree_fact(state: CleanupOperationState) -> WorktreeDisposition {
    match state {
        CleanupOperationState::UnlockPending => WorktreeDisposition::RetainedLocked,
        CleanupOperationState::RemovePending => WorktreeDisposition::RetainedUnlocked,
        _ => unreachable!("request constructor restricts known failure states"),
    }
}
