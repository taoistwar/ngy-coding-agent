use std::str::FromStr;

use crate::{
    DeliverySourceWriteCommand, DeliverySourceWriteOutcome, DeliveryWriteCommand,
    DeliveryWriteOutcome,
};
use coding_agent_domain::ClientRequestId;
use coding_agent_store::{
    AcceptMergeCommandRequest, AdvanceDeliverySourceObjectRequest, CommitDeliverySourceRequest,
    CreateDeliverySourceOutcome, CreateDeliverySourceRequest, DeliverySourceAnchor,
    DeliverySourceReconciliationReason, DeliverySourceState, DeliverySourceTransitionOutcome,
    ReconcileDeliverySourceOutcome, ReconcileDeliverySourceRequest,
    RecordDeliverySourceRetryRequest,
};

use super::DeliveryManagerLiveDependencies;
use super::live_runtime::{
    DeliveryLiveRuntimeError, DeliveryLiveRuntimeSession, DeliveryLiveSourceDisposition,
};
use super::recovery::{
    DeliveryRecoveryContext, ExactDeliveryWriteResult, LIVE_RUNTIME_STAGE_TIMEOUT,
    LiveStageOutcome, execute_exact_delivery_write,
};
use super::runtime_stage::{ProcessStageCompletion, run_process_stage};

pub(super) async fn drive_source_stage(
    dependencies: &DeliveryManagerLiveDependencies,
    session: &dyn DeliveryLiveRuntimeSession,
    context: &DeliveryRecoveryContext,
) -> LiveStageOutcome {
    let Some(source) = context.source.as_ref() else {
        return create_source(dependencies, context).await;
    };
    match source.state {
        DeliverySourceState::ObjectPending => {
            let proof = match run_process_stage(
                LIVE_RUNTIME_STAGE_TIMEOUT,
                session.build_source_object(source),
            )
            .await
            {
                ProcessStageCompletion::Completed(Ok(proof)) => proof,
                ProcessStageCompletion::Completed(Err(
                    DeliveryLiveRuntimeError::ReconciliationRequired(reason),
                )) => {
                    return reconcile_source(dependencies, context, reason).await;
                }
                ProcessStageCompletion::Completed(Err(error)) => return runtime_error(error),
                ProcessStageCompletion::TimedOutWithCleanupUnproven => {
                    return LiveStageOutcome::Retain;
                }
            };
            let proof = match proof.into_store_proof() {
                Ok(proof) => proof,
                Err(DeliveryLiveRuntimeError::ReconciliationRequired(reason)) => {
                    return reconcile_source(dependencies, context, reason).await;
                }
                Err(error) => return runtime_error(error),
            };
            let anchor = match source_anchor(source) {
                Some(anchor) => anchor,
                None => return LiveStageOutcome::Poison,
            };
            let request =
                match AdvanceDeliverySourceObjectRequest::try_new(anchor, source.version, proof) {
                    Ok(request) => request,
                    Err(_) => return LiveStageOutcome::Poison,
                };
            let command =
                DeliveryWriteCommand::Source(DeliverySourceWriteCommand::AdvanceObject(request));
            match execute_exact_delivery_write(&dependencies.writer, command).await {
                ExactDeliveryWriteResult::Confirmed(DeliveryWriteOutcome::Source(
                    DeliverySourceWriteOutcome::AdvanceObject(
                        DeliverySourceTransitionOutcome::Applied(receipt)
                        | DeliverySourceTransitionOutcome::Existing(receipt),
                    ),
                )) if receipt.task_id == source.provenance.identity.task_id()
                    && receipt.state == DeliverySourceState::CommitPending
                    && receipt.version == next_version(source.version) =>
                {
                    LiveStageOutcome::Continue
                }
                other => write_outcome(other),
            }
        }
        DeliverySourceState::CommitPending => {
            let result = match run_process_stage(
                LIVE_RUNTIME_STAGE_TIMEOUT,
                session.apply_source_commit(source),
            )
            .await
            {
                ProcessStageCompletion::Completed(Ok(result)) => result,
                ProcessStageCompletion::Completed(Err(
                    DeliveryLiveRuntimeError::ReconciliationRequired(reason),
                )) => {
                    return reconcile_source(dependencies, context, reason).await;
                }
                ProcessStageCompletion::Completed(Err(error)) => return runtime_error(error),
                ProcessStageCompletion::TimedOutWithCleanupUnproven => {
                    return LiveStageOutcome::Retain;
                }
            };
            match result.disposition() {
                DeliveryLiveSourceDisposition::Applied => {
                    let Some(proof) = result.into_applied_proof() else {
                        return LiveStageOutcome::Poison;
                    };
                    let proof = match proof.into_store_proof() {
                        Ok(proof) => proof,
                        Err(DeliveryLiveRuntimeError::ReconciliationRequired(reason)) => {
                            return reconcile_source(dependencies, context, reason).await;
                        }
                        Err(error) => return runtime_error(error),
                    };
                    commit_source(dependencies, source, proof).await
                }
                DeliveryLiveSourceDisposition::KnownNotApplied(reason) => {
                    record_source_retry(dependencies, source, reason).await
                }
                DeliveryLiveSourceDisposition::ReconciliationRequired(reason) => {
                    reconcile_source(dependencies, context, reason).await
                }
                DeliveryLiveSourceDisposition::ProcessCleanupUnproven => LiveStageOutcome::Retain,
            }
        }
        DeliverySourceState::Committed => LiveStageOutcome::Continue,
        DeliverySourceState::ReconciliationRequired => LiveStageOutcome::Poison,
    }
}

async fn create_source(
    dependencies: &DeliveryManagerLiveDependencies,
    context: &DeliveryRecoveryContext,
) -> LiveStageOutcome {
    let operation = &context.operation;
    let Some(accept_receipt_id) = operation.accept_receipt_id else {
        return LiveStageOutcome::Poison;
    };
    let client_request_id = match ClientRequestId::from_str(&accept_receipt_id.to_string()) {
        Ok(value) => value,
        Err(_) => return LiveStageOutcome::Poison,
    };
    let accept = match AcceptMergeCommandRequest::try_new(
        client_request_id,
        operation.provenance.identity.task_id(),
        operation.operation_id,
        previous_version(operation.version),
        operation.provenance.evidence.workspace_generation(),
        operation
            .provenance
            .evidence
            .workspace_fingerprint()
            .clone(),
        operation.target_branch.clone(),
        operation.expected_target_head.clone(),
    ) {
        Ok(request) => request,
        Err(_) => return LiveStageOutcome::Poison,
    };
    let request = match CreateDeliverySourceRequest::try_new(accept) {
        Ok(request) => request,
        Err(_) => return LiveStageOutcome::Poison,
    };
    let command = DeliveryWriteCommand::Source(DeliverySourceWriteCommand::Create(request));
    match execute_exact_delivery_write(&dependencies.writer, command).await {
        ExactDeliveryWriteResult::Confirmed(DeliveryWriteOutcome::Source(
            DeliverySourceWriteOutcome::Create(
                CreateDeliverySourceOutcome::Created(source)
                | CreateDeliverySourceOutcome::Existing(source),
            ),
        )) if source.origin_accepted_operation_id == operation.operation_id
            && source.state == DeliverySourceState::ObjectPending =>
        {
            LiveStageOutcome::Continue
        }
        other => write_outcome(other),
    }
}

async fn commit_source(
    dependencies: &DeliveryManagerLiveDependencies,
    source: &coding_agent_store::DeliverySourceRecord,
    proof: coding_agent_store::DeliverySourceAppliedProof,
) -> LiveStageOutcome {
    let Some(anchor) = source_anchor(source) else {
        return LiveStageOutcome::Poison;
    };
    let request = match CommitDeliverySourceRequest::try_new(anchor, source.version, proof) {
        Ok(request) => request,
        Err(_) => return LiveStageOutcome::Poison,
    };
    let command = DeliveryWriteCommand::Source(DeliverySourceWriteCommand::Commit(request));
    match execute_exact_delivery_write(&dependencies.writer, command).await {
        ExactDeliveryWriteResult::Confirmed(DeliveryWriteOutcome::Source(
            DeliverySourceWriteOutcome::Commit(
                DeliverySourceTransitionOutcome::Applied(receipt)
                | DeliverySourceTransitionOutcome::Existing(receipt),
            ),
        )) if receipt.task_id == source.provenance.identity.task_id()
            && receipt.state == DeliverySourceState::Committed
            && receipt.version == next_version(source.version) =>
        {
            LiveStageOutcome::Continue
        }
        other => side_effect_write_outcome(other),
    }
}

async fn record_source_retry(
    dependencies: &DeliveryManagerLiveDependencies,
    source: &coding_agent_store::DeliverySourceRecord,
    reason: coding_agent_store::DeliverySourceRetryReason,
) -> LiveStageOutcome {
    let Some(anchor) = source_anchor(source) else {
        return LiveStageOutcome::Poison;
    };
    let request = match RecordDeliverySourceRetryRequest::try_new(
        anchor,
        source.state,
        source.version,
        reason,
    ) {
        Ok(request) => request,
        Err(_) => return LiveStageOutcome::Poison,
    };
    let command = DeliveryWriteCommand::Source(DeliverySourceWriteCommand::RecordRetry(request));
    match execute_exact_delivery_write(&dependencies.writer, command).await {
        ExactDeliveryWriteResult::Confirmed(DeliveryWriteOutcome::Source(
            DeliverySourceWriteOutcome::RecordRetry(
                DeliverySourceTransitionOutcome::Applied(receipt)
                | DeliverySourceTransitionOutcome::Existing(receipt),
            ),
        )) if receipt.task_id == source.provenance.identity.task_id()
            && receipt.state == source.state
            && receipt.version == next_version(source.version) =>
        {
            LiveStageOutcome::Retry
        }
        other => write_outcome(other),
    }
}

pub(super) async fn reconcile_source(
    dependencies: &DeliveryManagerLiveDependencies,
    context: &DeliveryRecoveryContext,
    reason: coding_agent_store::MergeReconciliationReason,
) -> LiveStageOutcome {
    let Some(source) = context.source.as_ref() else {
        return LiveStageOutcome::Poison;
    };
    let Some(anchor) = source_anchor(source) else {
        return LiveStageOutcome::Poison;
    };
    let source_reason = match reason {
        coding_agent_store::MergeReconciliationReason::ProcessTreeCleanupFailed => {
            DeliverySourceReconciliationReason::ProcessTreeCleanupFailed
        }
        _ => DeliverySourceReconciliationReason::SourceInconsistent,
    };
    let request = match ReconcileDeliverySourceRequest::try_new(
        anchor,
        source.state,
        source.version,
        context.operation.version,
        source_reason,
    ) {
        Ok(request) => request,
        Err(_) => return LiveStageOutcome::Poison,
    };
    let command = DeliveryWriteCommand::Source(DeliverySourceWriteCommand::Reconcile(request));
    match execute_exact_delivery_write(&dependencies.writer, command).await {
        ExactDeliveryWriteResult::Confirmed(DeliveryWriteOutcome::Source(
            DeliverySourceWriteOutcome::Reconcile(
                ReconcileDeliverySourceOutcome::Applied(receipt)
                | ReconcileDeliverySourceOutcome::Existing(receipt),
            ),
        )) if receipt.source.task_id == source.provenance.identity.task_id()
            && receipt.source.state == DeliverySourceState::ReconciliationRequired
            && receipt.merge_operation_id == context.operation.operation_id =>
        {
            LiveStageOutcome::Poison
        }
        other => reconciliation_write_outcome(other),
    }
}

fn source_anchor(
    source: &coding_agent_store::DeliverySourceRecord,
) -> Option<DeliverySourceAnchor> {
    DeliverySourceAnchor::try_new(
        source.provenance.identity.task_id(),
        source.origin_accepted_operation_id,
        source.origin_accepted_version,
    )
    .ok()
}

fn runtime_error(error: DeliveryLiveRuntimeError) -> LiveStageOutcome {
    match error {
        DeliveryLiveRuntimeError::Unavailable => LiveStageOutcome::Release,
        DeliveryLiveRuntimeError::ProcessCleanupUnproven => LiveStageOutcome::Retain,
        DeliveryLiveRuntimeError::ReconciliationRequired(_) => LiveStageOutcome::Poison,
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

fn reconciliation_write_outcome(outcome: ExactDeliveryWriteResult) -> LiveStageOutcome {
    match outcome {
        ExactDeliveryWriteResult::KnownNotApplied { .. } => LiveStageOutcome::Poison,
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

fn previous_version(
    version: coding_agent_store::DeliveryVersion,
) -> coding_agent_store::DeliveryVersion {
    coding_agent_store::DeliveryVersion::try_new(version.get() - 1)
        .expect("accepted operation version always has a predecessor")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn known_not_applied() -> ExactDeliveryWriteResult {
        ExactDeliveryWriteResult::KnownNotApplied {
            reason: crate::KnownNotAppliedReason::BusyRolledBack,
            error: None,
        }
    }

    #[test]
    fn known_not_applied_classification_preserves_stage_ownership_boundary() {
        assert_eq!(write_outcome(known_not_applied()), LiveStageOutcome::Retry);
        assert_eq!(
            side_effect_write_outcome(known_not_applied()),
            LiveStageOutcome::RetryThenRetain
        );
        assert_eq!(
            reconciliation_write_outcome(known_not_applied()),
            LiveStageOutcome::Poison
        );
    }
}
