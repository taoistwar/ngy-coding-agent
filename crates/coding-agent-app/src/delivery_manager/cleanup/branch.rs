use super::transitions::*;
use super::*;
use crate::delivery_manager::runtime_stage::{ProcessStageCompletion, run_process_stage};

pub(super) async fn drive_branch_stage(
    dependencies: &DeliveryManagerLiveDependencies,
    session: &dyn DeliveryCleanupRuntimeSession,
    context: &DeliveryCleanupRecoveryContext,
) -> LiveStageOutcome {
    let operation = &context.operation;
    if operation.state != CleanupOperationState::DeletePending {
        return recovery_stage_for_state(operation.state);
    }
    let intent = match run_process_stage(
        LIVE_RUNTIME_STAGE_TIMEOUT,
        session.bind_branch_cleanup(
            &context.snapshot,
            DeliveryBranchCleanupBinding::Persisted(operation),
        ),
    )
    .await
    {
        ProcessStageCompletion::Completed(Ok(intent)) => intent,
        ProcessStageCompletion::Completed(Err(error)) => {
            return runtime_error(dependencies, context, error).await;
        }
        ProcessStageCompletion::TimedOutWithCleanupUnproven => {
            return LiveStageOutcome::Retain;
        }
    };
    drive_authorized_branch_stage(dependencies, session, context, intent, 0).await
}

async fn drive_authorized_branch_stage(
    dependencies: &DeliveryManagerLiveDependencies,
    session: &dyn DeliveryCleanupRuntimeSession,
    context: &DeliveryCleanupRecoveryContext,
    intent: DeliveryLiveBranchCleanupIntent,
    refresh_count: usize,
) -> LiveStageOutcome {
    if refresh_count >= MAX_LIVE_ATTEMPTS {
        return LiveStageOutcome::Release;
    }
    let capability = match timeout(
        LIVE_ORCHESTRATION_TIMEOUT,
        intent.authorize_delete(&dependencies.store, &context.operation),
    )
    .await
    {
        Ok(Ok(capability)) => capability,
        Ok(Err(error)) => return runtime_error(dependencies, context, error).await,
        Err(_) => return LiveStageOutcome::Release,
    };
    let disposition = match run_process_stage(
        LIVE_RUNTIME_STAGE_TIMEOUT,
        session.drive_delete_pending(capability),
    )
    .await
    {
        ProcessStageCompletion::Completed(Ok(disposition)) => disposition,
        ProcessStageCompletion::Completed(Err(error)) => {
            return runtime_error(dependencies, context, error).await;
        }
        ProcessStageCompletion::TimedOutWithCleanupUnproven => {
            return LiveStageOutcome::Retain;
        }
    };
    match disposition {
        DeliveryLiveDeletePendingDisposition::RetryExactDelete => LiveStageOutcome::Retry,
        DeliveryLiveDeletePendingDisposition::Deleted => {
            complete_branch_cleanup(dependencies, &context.operation).await
        }
        DeliveryLiveDeletePendingDisposition::KnownNotAppliedSourceNotMerged => {
            record_branch_failure(
                dependencies,
                &context.operation,
                BranchCleanupKnownNotAppliedReason::SourceBranchNotMerged,
            )
            .await
        }
        DeliveryLiveDeletePendingDisposition::KnownNotAppliedCommandTimedOut => {
            record_branch_failure(
                dependencies,
                &context.operation,
                BranchCleanupKnownNotAppliedReason::CommandTimedOut,
            )
            .await
        }
        DeliveryLiveDeletePendingDisposition::ReconciliationRequired => {
            reconcile_cleanup(
                dependencies,
                &context.operation,
                CleanupReconciliationReason::SourceInconsistent,
            )
            .await
        }
        DeliveryLiveDeletePendingDisposition::RefreshExpectedTarget(proof) => {
            let fresh_target = proof.fresh_target_head().clone();
            let stage = refresh_branch_target(dependencies, &context.operation, fresh_target).await;
            if stage != LiveStageOutcome::Continue {
                return stage;
            }
            let refreshed_intent = match proof.into_refreshed_intent() {
                Ok(intent) => intent,
                Err(error) => return runtime_error(dependencies, context, error).await,
            };
            let refreshed =
                match load_cleanup_operation_context(dependencies, context.operation.operation_id)
                    .await
                {
                    Ok(context) => context,
                    Err(_) => return LiveStageOutcome::Poison,
                };
            Box::pin(drive_authorized_branch_stage(
                dependencies,
                session,
                &refreshed,
                refreshed_intent,
                refresh_count + 1,
            ))
            .await
        }
    }
}

async fn refresh_branch_target(
    dependencies: &DeliveryManagerLiveDependencies,
    operation: &CleanupOperationRecord,
    fresh_target: coding_agent_store::GitCommitOid,
) -> LiveStageOutcome {
    let Some(anchor) = cleanup_anchor(operation) else {
        return LiveStageOutcome::Poison;
    };
    let Some(expected_target) = operation.expected_target_head.clone() else {
        return LiveStageOutcome::Poison;
    };
    let request = match RefreshBranchCleanupTargetRequest::try_new(
        anchor,
        expected_target,
        fresh_target.clone(),
    ) {
        Ok(request) => request,
        Err(_) => return LiveStageOutcome::Poison,
    };
    let command =
        DeliveryWriteCommand::Cleanup(DeliveryCleanupWriteCommand::RefreshBranchTarget(request));
    let stage = transition_outcome(
        execute_exact_delivery_write(&dependencies.writer, command).await,
        operation,
        CleanupOperationState::DeletePending,
    );
    if stage != LiveStageOutcome::Continue {
        return stage;
    }
    match load_cleanup_operation_context(dependencies, operation.operation_id).await {
        Ok(context)
            if context.operation.version == next_version(operation.version)
                && context.operation.expected_target_head.as_ref() == Some(&fresh_target)
                && context.operation.target_head_at(context.operation.version)
                    == Some(&fresh_target) =>
        {
            LiveStageOutcome::Continue
        }
        Ok(_) | Err(_) => LiveStageOutcome::Poison,
    }
}

async fn complete_branch_cleanup(
    dependencies: &DeliveryManagerLiveDependencies,
    operation: &CleanupOperationRecord,
) -> LiveStageOutcome {
    let Some(anchor) = cleanup_anchor(operation) else {
        return LiveStageOutcome::Poison;
    };
    let request = match CompleteBranchCleanupRequest::try_new(anchor) {
        Ok(request) => request,
        Err(_) => return LiveStageOutcome::Poison,
    };
    let command =
        DeliveryWriteCommand::Cleanup(DeliveryCleanupWriteCommand::CompleteBranch(request));
    side_effect_terminal_transition_outcome(
        execute_exact_delivery_write(&dependencies.writer, command).await,
        operation,
        CleanupOperationState::Completed,
    )
}

async fn record_branch_failure(
    dependencies: &DeliveryManagerLiveDependencies,
    operation: &CleanupOperationRecord,
    reason: BranchCleanupKnownNotAppliedReason,
) -> LiveStageOutcome {
    let Some(anchor) = cleanup_anchor(operation) else {
        return LiveStageOutcome::Poison;
    };
    let request = match RecordBranchCleanupFailureRequest::try_new(anchor, reason) {
        Ok(request) => request,
        Err(_) => return LiveStageOutcome::Poison,
    };
    let command =
        DeliveryWriteCommand::Cleanup(DeliveryCleanupWriteCommand::RecordBranchFailure(request));
    terminal_transition_outcome(
        execute_exact_delivery_write(&dependencies.writer, command).await,
        operation,
        CleanupOperationState::Failed,
    )
}
