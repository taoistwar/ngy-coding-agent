use super::*;

pub(super) async fn reconcile_cleanup(
    dependencies: &DeliveryManagerLiveDependencies,
    operation: &CleanupOperationRecord,
    reason: CleanupReconciliationReason,
) -> LiveStageOutcome {
    let Some(anchor) = cleanup_anchor(operation) else {
        return LiveStageOutcome::Poison;
    };
    let command = match operation.kind {
        CleanupKind::RemoveWorktree => {
            let request =
                match ReconcileWorktreeCleanupRequest::try_new(anchor, operation.state, reason) {
                    Ok(request) => request,
                    Err(_) => return LiveStageOutcome::Poison,
                };
            DeliveryCleanupWriteCommand::ReconcileWorktree(request)
        }
        CleanupKind::DeleteBranch => {
            let request = match ReconcileBranchCleanupRequest::try_new(anchor, reason) {
                Ok(request) => request,
                Err(_) => return LiveStageOutcome::Poison,
            };
            DeliveryCleanupWriteCommand::ReconcileBranch(request)
        }
    };
    let write = DeliveryWriteCommand::Cleanup(command);
    match execute_exact_delivery_write(&dependencies.writer, write).await {
        ExactDeliveryWriteResult::Confirmed(DeliveryWriteOutcome::Cleanup(
            DeliveryCleanupWriteOutcome::ReconcileWorktree(
                CleanupTransitionOutcome::Applied(receipt)
                | CleanupTransitionOutcome::Existing(receipt),
            )
            | DeliveryCleanupWriteOutcome::ReconcileBranch(
                CleanupTransitionOutcome::Applied(receipt)
                | CleanupTransitionOutcome::Existing(receipt),
            ),
        )) if receipt.operation_id == operation.operation_id
            && receipt.state == CleanupOperationState::ReconciliationRequired
            && receipt.version == next_version(operation.version) =>
        {
            LiveStageOutcome::Poison
        }
        other => reconciliation_write_outcome(other),
    }
}

pub(super) fn transition_outcome(
    outcome: ExactDeliveryWriteResult,
    operation: &CleanupOperationRecord,
    expected: CleanupOperationState,
) -> LiveStageOutcome {
    checked_transition_outcome(outcome, operation, expected, write_outcome)
}

pub(super) fn side_effect_transition_outcome(
    outcome: ExactDeliveryWriteResult,
    operation: &CleanupOperationRecord,
    expected: CleanupOperationState,
) -> LiveStageOutcome {
    checked_transition_outcome(outcome, operation, expected, side_effect_write_outcome)
}

pub(super) fn terminal_transition_outcome(
    outcome: ExactDeliveryWriteResult,
    operation: &CleanupOperationRecord,
    expected: CleanupOperationState,
) -> LiveStageOutcome {
    checked_terminal_transition_outcome(outcome, operation, expected, write_outcome)
}

pub(super) fn side_effect_terminal_transition_outcome(
    outcome: ExactDeliveryWriteResult,
    operation: &CleanupOperationRecord,
    expected: CleanupOperationState,
) -> LiveStageOutcome {
    checked_terminal_transition_outcome(outcome, operation, expected, side_effect_write_outcome)
}

pub(super) fn write_outcome(outcome: ExactDeliveryWriteResult) -> LiveStageOutcome {
    match outcome {
        ExactDeliveryWriteResult::KnownNotApplied { .. } => LiveStageOutcome::Retry,
        ExactDeliveryWriteResult::OutcomeUnknown => LiveStageOutcome::Retain,
        ExactDeliveryWriteResult::InvariantConflict | ExactDeliveryWriteResult::Confirmed(_) => {
            LiveStageOutcome::Poison
        }
    }
}

fn side_effect_write_outcome(outcome: ExactDeliveryWriteResult) -> LiveStageOutcome {
    match outcome {
        ExactDeliveryWriteResult::KnownNotApplied { .. } => LiveStageOutcome::RetryThenRetain,
        other => write_outcome(other),
    }
}

fn reconciliation_write_outcome(outcome: ExactDeliveryWriteResult) -> LiveStageOutcome {
    match outcome {
        ExactDeliveryWriteResult::KnownNotApplied { .. } => LiveStageOutcome::Poison,
        other => write_outcome(other),
    }
}

fn checked_transition_outcome(
    outcome: ExactDeliveryWriteResult,
    operation: &CleanupOperationRecord,
    expected: CleanupOperationState,
    classify_other: fn(ExactDeliveryWriteResult) -> LiveStageOutcome,
) -> LiveStageOutcome {
    match outcome {
        ExactDeliveryWriteResult::Confirmed(DeliveryWriteOutcome::Cleanup(
            DeliveryCleanupWriteOutcome::RecordWorktreeUnlocked(
                CleanupTransitionOutcome::Applied(receipt)
                | CleanupTransitionOutcome::Existing(receipt),
            )
            | DeliveryCleanupWriteOutcome::EnterWorktreeRemovePending(
                CleanupTransitionOutcome::Applied(receipt)
                | CleanupTransitionOutcome::Existing(receipt),
            )
            | DeliveryCleanupWriteOutcome::RefreshBranchTarget(
                CleanupTransitionOutcome::Applied(receipt)
                | CleanupTransitionOutcome::Existing(receipt),
            ),
        )) if receipt.operation_id == operation.operation_id
            && receipt.state == expected
            && receipt.version == next_version(operation.version) =>
        {
            LiveStageOutcome::Continue
        }
        other => classify_other(other),
    }
}

fn checked_terminal_transition_outcome(
    outcome: ExactDeliveryWriteResult,
    operation: &CleanupOperationRecord,
    expected: CleanupOperationState,
    classify_other: fn(ExactDeliveryWriteResult) -> LiveStageOutcome,
) -> LiveStageOutcome {
    match outcome {
        ExactDeliveryWriteResult::Confirmed(DeliveryWriteOutcome::Cleanup(
            DeliveryCleanupWriteOutcome::CompleteWorktree(
                CleanupTransitionOutcome::Applied(receipt)
                | CleanupTransitionOutcome::Existing(receipt),
            )
            | DeliveryCleanupWriteOutcome::RecordWorktreeFailure(
                CleanupTransitionOutcome::Applied(receipt)
                | CleanupTransitionOutcome::Existing(receipt),
            )
            | DeliveryCleanupWriteOutcome::CompleteBranch(
                CleanupTransitionOutcome::Applied(receipt)
                | CleanupTransitionOutcome::Existing(receipt),
            )
            | DeliveryCleanupWriteOutcome::RecordBranchFailure(
                CleanupTransitionOutcome::Applied(receipt)
                | CleanupTransitionOutcome::Existing(receipt),
            ),
        )) if receipt.operation_id == operation.operation_id
            && receipt.state == expected
            && receipt.version == next_version(operation.version) =>
        {
            LiveStageOutcome::Finished
        }
        other => classify_other(other),
    }
}

pub(super) async fn runtime_error(
    dependencies: &DeliveryManagerLiveDependencies,
    context: &DeliveryCleanupRecoveryContext,
    error: DeliveryLiveCleanupRuntimeError,
) -> LiveStageOutcome {
    match error {
        DeliveryLiveCleanupRuntimeError::Unavailable => LiveStageOutcome::Release,
        DeliveryLiveCleanupRuntimeError::TargetWorktreeDirty => LiveStageOutcome::Poison,
        DeliveryLiveCleanupRuntimeError::ProcessCleanupUnproven => LiveStageOutcome::Retain,
        DeliveryLiveCleanupRuntimeError::ReconciliationRequired(reason) => {
            reconcile_cleanup(dependencies, &context.operation, reason).await
        }
    }
}
