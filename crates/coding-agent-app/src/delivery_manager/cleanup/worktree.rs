use super::transitions::*;
use super::*;

pub(super) async fn drive_worktree_stage(
    dependencies: &DeliveryManagerLiveDependencies,
    session: &dyn DeliveryCleanupRuntimeSession,
    context: &DeliveryCleanupRecoveryContext,
) -> LiveStageOutcome {
    let operation = &context.operation;
    let intent = match timeout(
        LIVE_RUNTIME_STAGE_TIMEOUT,
        session.bind_worktree_cleanup(
            &context.snapshot,
            DeliveryWorktreeCleanupBinding::Persisted(operation),
        ),
    )
    .await
    {
        Ok(Ok(intent)) => intent,
        Ok(Err(error)) => return runtime_error(dependencies, context, error).await,
        Err(_) => return LiveStageOutcome::Release,
    };
    match operation.state {
        CleanupOperationState::UnlockPending => {
            let capability = match timeout(
                LIVE_ORCHESTRATION_TIMEOUT,
                intent.authorize_unlock(&dependencies.store, operation),
            )
            .await
            {
                Ok(Ok(capability)) => capability,
                Ok(Err(error)) => return runtime_error(dependencies, context, error).await,
                Err(_) => return LiveStageOutcome::Release,
            };
            let disposition = match timeout(
                LIVE_RUNTIME_STAGE_TIMEOUT,
                session.drive_unlock_pending(capability),
            )
            .await
            {
                Ok(Ok(disposition)) => disposition,
                Ok(Err(error)) => return runtime_error(dependencies, context, error).await,
                Err(_) => return LiveStageOutcome::Release,
            };
            match disposition {
                coding_agent_runtime::DeliveryUnlockPendingDisposition::RetryExactUnlock => {
                    LiveStageOutcome::Retry
                }
                coding_agent_runtime::DeliveryUnlockPendingDisposition::UnlockApplied => {
                    record_worktree_unlocked(dependencies, operation).await
                }
                coding_agent_runtime::DeliveryUnlockPendingDisposition::ReconciliationRequired => {
                    reconcile_cleanup(
                        dependencies,
                        operation,
                        CleanupReconciliationReason::WorktreeIdentityMismatch,
                    )
                    .await
                }
            }
        }
        CleanupOperationState::UnlockedPendingRemove => {
            let capability = match timeout(
                LIVE_ORCHESTRATION_TIMEOUT,
                intent.authorize_unlocked_pending_remove(&dependencies.store, operation),
            )
            .await
            {
                Ok(Ok(capability)) => capability,
                Ok(Err(error)) => return runtime_error(dependencies, context, error).await,
                Err(_) => return LiveStageOutcome::Release,
            };
            let disposition = match timeout(
                LIVE_RUNTIME_STAGE_TIMEOUT,
                session.drive_unlocked_pending_remove(capability),
            )
            .await
            {
                Ok(Ok(disposition)) => disposition,
                Ok(Err(error)) => return runtime_error(dependencies, context, error).await,
                Err(_) => return LiveStageOutcome::Release,
            };
            match disposition {
                coding_agent_runtime::DeliveryUnlockedPendingRemoveDisposition::EnterRemovePending => {
                    enter_worktree_remove_pending(dependencies, operation).await
                }
                coding_agent_runtime::DeliveryUnlockedPendingRemoveDisposition::ReconciliationRequired => {
                    reconcile_cleanup(
                        dependencies,
                        operation,
                        CleanupReconciliationReason::WorktreeIdentityMismatch,
                    )
                    .await
                }
            }
        }
        CleanupOperationState::RemovePending => {
            let capability = match timeout(
                LIVE_ORCHESTRATION_TIMEOUT,
                intent.authorize_remove(&dependencies.store, operation),
            )
            .await
            {
                Ok(Ok(capability)) => capability,
                Ok(Err(error)) => return runtime_error(dependencies, context, error).await,
                Err(_) => return LiveStageOutcome::Release,
            };
            let disposition = match timeout(
                LIVE_RUNTIME_STAGE_TIMEOUT,
                session.drive_remove_pending(capability),
            )
            .await
            {
                Ok(Ok(disposition)) => disposition,
                Ok(Err(error)) => return runtime_error(dependencies, context, error).await,
                Err(_) => return LiveStageOutcome::Release,
            };
            match disposition {
                coding_agent_runtime::DeliveryRemovePendingDisposition::RetryExactRemove => {
                    LiveStageOutcome::Retry
                }
                coding_agent_runtime::DeliveryRemovePendingDisposition::Removed => {
                    complete_worktree_cleanup(dependencies, operation).await
                }
                coding_agent_runtime::DeliveryRemovePendingDisposition::KnownNotAppliedDirty => {
                    record_worktree_failure(
                        dependencies,
                        operation,
                        WorktreeCleanupKnownNotAppliedReason::TargetWorktreeDirty,
                    )
                    .await
                }
                coding_agent_runtime::DeliveryRemovePendingDisposition::ReconciliationRequired => {
                    reconcile_cleanup(
                        dependencies,
                        operation,
                        CleanupReconciliationReason::WorktreeIdentityMismatch,
                    )
                    .await
                }
            }
        }
        CleanupOperationState::Completed
        | CleanupOperationState::Failed
        | CleanupOperationState::ReconciliationRequired => {
            recovery_stage_for_state(operation.state)
        }
        CleanupOperationState::DeletePending => LiveStageOutcome::Poison,
    }
}

async fn record_worktree_unlocked(
    dependencies: &DeliveryManagerLiveDependencies,
    operation: &CleanupOperationRecord,
) -> LiveStageOutcome {
    let Some(anchor) = cleanup_anchor(operation) else {
        return LiveStageOutcome::Poison;
    };
    let request = match RecordWorktreeUnlockedRequest::try_new(anchor) {
        Ok(request) => request,
        Err(_) => return LiveStageOutcome::Poison,
    };
    let command =
        DeliveryWriteCommand::Cleanup(DeliveryCleanupWriteCommand::RecordWorktreeUnlocked(request));
    side_effect_transition_outcome(
        execute_exact_delivery_write(&dependencies.writer, command).await,
        operation,
        CleanupOperationState::UnlockedPendingRemove,
    )
}

async fn enter_worktree_remove_pending(
    dependencies: &DeliveryManagerLiveDependencies,
    operation: &CleanupOperationRecord,
) -> LiveStageOutcome {
    let Some(anchor) = cleanup_anchor(operation) else {
        return LiveStageOutcome::Poison;
    };
    let request = match EnterWorktreeRemovePendingRequest::try_new(anchor) {
        Ok(request) => request,
        Err(_) => return LiveStageOutcome::Poison,
    };
    let command = DeliveryWriteCommand::Cleanup(
        DeliveryCleanupWriteCommand::EnterWorktreeRemovePending(request),
    );
    transition_outcome(
        execute_exact_delivery_write(&dependencies.writer, command).await,
        operation,
        CleanupOperationState::RemovePending,
    )
}

async fn complete_worktree_cleanup(
    dependencies: &DeliveryManagerLiveDependencies,
    operation: &CleanupOperationRecord,
) -> LiveStageOutcome {
    let Some(anchor) = cleanup_anchor(operation) else {
        return LiveStageOutcome::Poison;
    };
    let request = match CompleteWorktreeCleanupRequest::try_new(anchor) {
        Ok(request) => request,
        Err(_) => return LiveStageOutcome::Poison,
    };
    let command =
        DeliveryWriteCommand::Cleanup(DeliveryCleanupWriteCommand::CompleteWorktree(request));
    side_effect_terminal_transition_outcome(
        execute_exact_delivery_write(&dependencies.writer, command).await,
        operation,
        CleanupOperationState::Completed,
    )
}

async fn record_worktree_failure(
    dependencies: &DeliveryManagerLiveDependencies,
    operation: &CleanupOperationRecord,
    reason: WorktreeCleanupKnownNotAppliedReason,
) -> LiveStageOutcome {
    let Some(anchor) = cleanup_anchor(operation) else {
        return LiveStageOutcome::Poison;
    };
    let request =
        match RecordWorktreeCleanupFailureRequest::try_new(anchor, operation.state, reason) {
            Ok(request) => request,
            Err(_) => return LiveStageOutcome::Poison,
        };
    let command =
        DeliveryWriteCommand::Cleanup(DeliveryCleanupWriteCommand::RecordWorktreeFailure(request));
    terminal_transition_outcome(
        execute_exact_delivery_write(&dependencies.writer, command).await,
        operation,
        CleanupOperationState::Failed,
    )
}
