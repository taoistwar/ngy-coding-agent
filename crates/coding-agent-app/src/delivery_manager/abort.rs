use crate::{
    DeliveryMergeWriteCommand, DeliveryMergeWriteOutcome, DeliveryWriteCommand,
    DeliveryWriteOutcome,
};
use coding_agent_store::{
    CompleteMergeAbortRequest, DeliverySourceState, MergeOperationState, MergeTransitionOutcome,
};

use super::DeliveryManagerLiveDependencies;
use super::live_runtime::{
    DeliveryLiveAbortDisposition, DeliveryLiveRuntimeError, DeliveryLiveRuntimeSession,
};
use super::merge::reconcile_operation;
use super::recovery::{
    DeliveryRecoveryContext, ExactDeliveryWriteResult, LIVE_RUNTIME_STAGE_TIMEOUT,
    LiveStageOutcome, execute_exact_delivery_write,
};
use super::runtime_stage::{ProcessStageCompletion, run_process_stage};

pub(super) async fn drive_abort_stage(
    dependencies: &DeliveryManagerLiveDependencies,
    session: &dyn DeliveryLiveRuntimeSession,
    context: &DeliveryRecoveryContext,
) -> LiveStageOutcome {
    let operation = &context.operation;
    let Some(source) = context.source.as_ref() else {
        return LiveStageOutcome::Poison;
    };
    if operation.state != MergeOperationState::AbortPending
        || source.state != DeliverySourceState::Committed
    {
        return LiveStageOutcome::Poison;
    }
    let disposition = match run_process_stage(
        LIVE_RUNTIME_STAGE_TIMEOUT,
        session.drive_abort_pending(operation, source),
    )
    .await
    {
        ProcessStageCompletion::Completed(Ok(disposition)) => disposition,
        ProcessStageCompletion::Completed(Err(
            DeliveryLiveRuntimeError::ProcessCleanupUnproven,
        )) => {
            return LiveStageOutcome::Retain;
        }
        ProcessStageCompletion::Completed(Err(
            DeliveryLiveRuntimeError::ReconciliationRequired(reason),
        )) => {
            return reconcile_operation(dependencies, operation, reason).await;
        }
        ProcessStageCompletion::Completed(Err(DeliveryLiveRuntimeError::Unavailable)) => {
            return LiveStageOutcome::Release;
        }
        ProcessStageCompletion::TimedOutWithCleanupUnproven => return LiveStageOutcome::Retain,
    };
    match disposition {
        DeliveryLiveAbortDisposition::Applied(proof) => {
            let proof = match proof.into_store_proof() {
                Ok(proof) => proof,
                Err(DeliveryLiveRuntimeError::ReconciliationRequired(reason)) => {
                    return reconcile_operation(dependencies, operation, reason).await;
                }
                Err(DeliveryLiveRuntimeError::ProcessCleanupUnproven) => {
                    return LiveStageOutcome::Retain;
                }
                Err(DeliveryLiveRuntimeError::Unavailable) => {
                    return LiveStageOutcome::Release;
                }
            };
            let request = match CompleteMergeAbortRequest::try_new(
                operation.provenance.identity.task_id(),
                operation.operation_id,
                operation.version,
                proof,
            ) {
                Ok(request) => request,
                Err(_) => return LiveStageOutcome::Poison,
            };
            let command =
                DeliveryWriteCommand::Merge(DeliveryMergeWriteCommand::CompleteAbort(request));
            match execute_exact_delivery_write(&dependencies.writer, command).await {
                ExactDeliveryWriteResult::Confirmed(DeliveryWriteOutcome::Merge(
                    DeliveryMergeWriteOutcome::CompleteAbort(
                        MergeTransitionOutcome::Applied(receipt)
                        | MergeTransitionOutcome::Existing(receipt),
                    ),
                )) if receipt.operation_id == operation.operation_id
                    && receipt.state == MergeOperationState::Conflict
                    && receipt.version == next_version(operation.version) =>
                {
                    LiveStageOutcome::Finished
                }
                other => side_effect_write_outcome(other),
            }
        }
        DeliveryLiveAbortDisposition::Pending => LiveStageOutcome::Retry,
        DeliveryLiveAbortDisposition::ReconciliationRequired(reason) => {
            reconcile_operation(dependencies, operation, reason).await
        }
        DeliveryLiveAbortDisposition::ProcessCleanupUnproven => LiveStageOutcome::Retain,
    }
}

fn write_outcome(outcome: ExactDeliveryWriteResult) -> LiveStageOutcome {
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

fn next_version(
    version: coding_agent_store::DeliveryVersion,
) -> coding_agent_store::DeliveryVersion {
    version
        .next()
        .expect("persisted delivery version can advance")
}
