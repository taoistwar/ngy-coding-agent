use std::sync::Arc;

use coding_agent_store::{
    BindMergePreflightInputsRequest, DeliveryCommandReceipt, DeliveryVersion, MergeOperationRecord,
    MergeOperationState, MergeReconciliationReason, PreflightCommandRequest,
};
use tokio::time::timeout;

use crate::delivery_api_projection::DeliveryPreflightDurability;
use crate::delivery_manager::runtime_stage::{ProcessStageCompletion, run_process_stage};
use crate::delivery_manager::{
    DeliveryManagerLiveDependencies, DeliveryPreparedPreflight, DeliveryRuntimeAuthentication,
    DeliveryRuntimeFailure, DeliveryRuntimeSession,
};
use crate::{DeliveryMergeWriteCommand, DeliveryWriteCommand, RepositoryControlLease};

use super::admission::{
    PRE_RUNTIME_STAGE_TIMEOUT, PreflightAttemptResult, clean_and_release, inconsistent_outcome,
    poison_and_release, retain_and_fail_closed, retain_unknown,
};
use super::eligibility::EligiblePreflight;
use super::persist::{
    ExactWriteResult, execute_exact_write, persist_prepared_failure, persist_prepared_result,
    persist_prepared_runtime_timeout, persist_unbound_failure, persist_unbound_runtime_timeout,
    retry_pending,
};
use super::routing::{PendingShape, pending_shape};

pub(super) struct AuthenticatedPreflight {
    pub(super) eligible: EligiblePreflight,
    pub(super) session: Arc<dyn DeliveryRuntimeSession>,
    pub(super) authentication: DeliveryRuntimeAuthentication,
    pub(super) known_failure: Option<DeliveryRuntimeFailure>,
}

pub(super) async fn authenticate(
    dependencies: &DeliveryManagerLiveDependencies,
    eligible: EligiblePreflight,
) -> Result<AuthenticatedPreflight, PreflightAttemptResult> {
    let session = match timeout(
        PRE_RUNTIME_STAGE_TIMEOUT,
        dependencies
            .runtime_registry
            .open_session(&eligible.routed.snapshot),
    )
    .await
    {
        Ok(Ok(session)) => session,
        Ok(Err(DeliveryRuntimeFailure::ProcessCleanupUnproven)) => {
            return Err(retain_and_fail_closed(
                eligible.routed.lease,
                crate::DeliveryPreflightOutcome::Unavailable(
                    crate::DeliveryPreflightUnavailableReason::ProcessProofUnavailable,
                ),
            ));
        }
        Ok(Err(_)) | Err(_) => {
            return Err(poison_and_release(
                eligible.routed.lease,
                crate::DeliveryPreflightOutcome::Unavailable(
                    crate::DeliveryPreflightUnavailableReason::RuntimeUnavailable,
                ),
            ));
        }
    };
    let authentication_outcome = match run_process_stage(
        PRE_RUNTIME_STAGE_TIMEOUT,
        session.authenticate_preflight(&eligible.routed.command),
    )
    .await
    {
        ProcessStageCompletion::Completed(Ok(outcome)) => outcome,
        ProcessStageCompletion::Completed(Err(DeliveryRuntimeFailure::ProcessCleanupUnproven)) => {
            return Err(retain_and_fail_closed(
                eligible.routed.lease,
                crate::DeliveryPreflightOutcome::Unavailable(
                    crate::DeliveryPreflightUnavailableReason::ProcessProofUnavailable,
                ),
            ));
        }
        ProcessStageCompletion::Completed(Err(_)) => {
            return Err(poison_and_release(
                eligible.routed.lease,
                crate::DeliveryPreflightOutcome::Unavailable(
                    crate::DeliveryPreflightUnavailableReason::RuntimeUnavailable,
                ),
            ));
        }
        ProcessStageCompletion::TimedOutWithCleanupUnproven => {
            return Err(retain_and_fail_closed(
                eligible.routed.lease,
                crate::DeliveryPreflightOutcome::Unavailable(
                    crate::DeliveryPreflightUnavailableReason::RuntimeUnavailable,
                ),
            ));
        }
    };
    let (authentication, known_failure) = authentication_outcome.into_parts();
    if !authentication.authorizes(
        &eligible.routed.snapshot,
        &eligible.routed.command,
        eligible.routed.lease.coordination_key(),
    ) {
        return Err(poison_and_release(
            eligible.routed.lease,
            inconsistent_outcome(),
        ));
    }
    if let Some(operation) = eligible.routed.operation.as_ref()
        && (!authentication.authorizes_operation(operation)
            || eligible
                .routed
                .snapshot
                .evidence_identity
                .as_ref()
                .is_none_or(|evidence| &operation.provenance.evidence != evidence))
    {
        return Err(poison_and_release(
            eligible.routed.lease,
            inconsistent_outcome(),
        ));
    }

    Ok(AuthenticatedPreflight {
        eligible,
        session,
        authentication,
        known_failure,
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn resume_pending_preflight(
    dependencies: &DeliveryManagerLiveDependencies,
    session: &dyn DeliveryRuntimeSession,
    authentication: DeliveryRuntimeAuthentication,
    known_failure: Option<DeliveryRuntimeFailure>,
    command: PreflightCommandRequest,
    receipt: DeliveryCommandReceipt,
    operation: &MergeOperationRecord,
    lease: RepositoryControlLease,
) -> PreflightAttemptResult {
    match pending_shape(operation) {
        Some(PendingShape::UnboundV1) => {
            continue_unbound_preflight(
                dependencies,
                session,
                authentication,
                known_failure,
                command,
                receipt,
                DeliveryPreflightDurability::Existing,
                lease,
            )
            .await
        }
        Some(PendingShape::PreparedV2) => {
            let persisted_inputs = operation
                .preflight_inputs
                .as_ref()
                .expect("validated prepared pending has immutable inputs")
                .clone();
            if let Some(failure) = known_failure {
                return persist_prepared_failure(
                    dependencies,
                    command.task_id(),
                    receipt.operation_id,
                    DeliveryPreflightDurability::Existing,
                    failure,
                    lease,
                )
                .await;
            }
            let prepared =
                match run_process_stage(PRE_RUNTIME_STAGE_TIMEOUT, session.prepare_preflight())
                    .await
                {
                    ProcessStageCompletion::Completed(Ok(prepared)) => prepared,
                    ProcessStageCompletion::Completed(Err(failure)) => {
                        return persist_prepared_failure(
                            dependencies,
                            command.task_id(),
                            receipt.operation_id,
                            DeliveryPreflightDurability::Existing,
                            failure,
                            lease,
                        )
                        .await;
                    }
                    ProcessStageCompletion::TimedOutWithCleanupUnproven => {
                        return persist_prepared_runtime_timeout(
                            dependencies,
                            command.task_id(),
                            receipt.operation_id,
                            DeliveryPreflightDurability::Existing,
                            lease,
                        )
                        .await;
                    }
                };
            if prepared.candidate_tree() != &persisted_inputs.candidate_tree
                || prepared.source_commit() != &persisted_inputs.preflight_source_commit
                || !authentication.authorizes_prepared(&prepared)
            {
                return persist_prepared_failure(
                    dependencies,
                    command.task_id(),
                    receipt.operation_id,
                    DeliveryPreflightDurability::Existing,
                    DeliveryRuntimeFailure::ReconciliationRequired(
                        MergeReconciliationReason::DeliveryStateInconsistent,
                    ),
                    lease,
                )
                .await;
            }
            // The authentication value is deliberately kept alive through
            // re-preparation: it is the proof that the opaque prepared object
            // belongs to the same source/target authority checked above.
            drop(authentication);
            run_prepared_preflight(
                dependencies,
                session,
                command.task_id(),
                receipt.operation_id,
                DeliveryPreflightDurability::Existing,
                prepared,
                lease,
            )
            .await
        }
        None => poison_and_release(lease, inconsistent_outcome()),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn continue_unbound_preflight(
    dependencies: &DeliveryManagerLiveDependencies,
    session: &dyn DeliveryRuntimeSession,
    authentication: DeliveryRuntimeAuthentication,
    known_failure: Option<DeliveryRuntimeFailure>,
    command: PreflightCommandRequest,
    receipt: DeliveryCommandReceipt,
    durability: DeliveryPreflightDurability,
    lease: RepositoryControlLease,
) -> PreflightAttemptResult {
    if let Some(failure) = known_failure {
        return persist_unbound_failure(
            dependencies,
            command.task_id(),
            receipt.operation_id,
            durability,
            failure,
            lease,
        )
        .await;
    }
    let prepared =
        match run_process_stage(PRE_RUNTIME_STAGE_TIMEOUT, session.prepare_preflight()).await {
            ProcessStageCompletion::Completed(Ok(prepared)) => prepared,
            ProcessStageCompletion::Completed(Err(failure)) => {
                return persist_unbound_failure(
                    dependencies,
                    command.task_id(),
                    receipt.operation_id,
                    durability,
                    failure,
                    lease,
                )
                .await;
            }
            ProcessStageCompletion::TimedOutWithCleanupUnproven => {
                return persist_unbound_runtime_timeout(
                    dependencies,
                    command.task_id(),
                    receipt.operation_id,
                    durability,
                    lease,
                )
                .await;
            }
        };
    if !authentication.authorizes_prepared(&prepared) {
        return persist_unbound_failure(
            dependencies,
            command.task_id(),
            receipt.operation_id,
            durability,
            DeliveryRuntimeFailure::ReconciliationRequired(
                MergeReconciliationReason::DeliveryStateInconsistent,
            ),
            lease,
        )
        .await;
    }
    let request = match BindMergePreflightInputsRequest::try_new(
        command.task_id(),
        receipt.operation_id,
        DeliveryVersion::initial(),
        prepared.candidate_tree().clone(),
        prepared.source_commit().clone(),
    ) {
        Ok(request) => request,
        Err(_) => return poison_and_release(lease, inconsistent_outcome()),
    };
    let write =
        DeliveryWriteCommand::Merge(DeliveryMergeWriteCommand::BindPreflightInputs(request));
    match execute_exact_write(&dependencies.writer, write).await {
        ExactWriteResult::Confirmed(crate::DeliveryMergeWriteOutcome::BindPreflightInputs(
            coding_agent_store::MergeTransitionOutcome::Applied(transition)
            | coding_agent_store::MergeTransitionOutcome::Existing(transition),
        )) if transition.operation_id == receipt.operation_id
            && transition.state == MergeOperationState::PreflightPending
            && transition.version == DeliveryVersion::try_new(2).expect("version two is valid") =>
        {
            run_prepared_preflight(
                dependencies,
                session,
                command.task_id(),
                receipt.operation_id,
                durability,
                prepared,
                lease,
            )
            .await
        }
        ExactWriteResult::KnownNotApplied { .. } => {
            clean_and_release(lease, retry_pending(receipt.operation_id, durability))
        }
        ExactWriteResult::Unknown => retain_unknown(
            lease,
            crate::DeliveryPreflightOutcome::Unavailable(
                crate::DeliveryPreflightUnavailableReason::OutcomeUnknown,
            ),
        ),
        ExactWriteResult::InvariantConflict | ExactWriteResult::Confirmed(_) => {
            poison_and_release(lease, inconsistent_outcome())
        }
    }
}

async fn run_prepared_preflight(
    dependencies: &DeliveryManagerLiveDependencies,
    session: &dyn DeliveryRuntimeSession,
    task_id: coding_agent_domain::TaskId,
    operation_id: coding_agent_store::DeliveryOperationId,
    durability: DeliveryPreflightDurability,
    prepared: DeliveryPreparedPreflight,
    lease: RepositoryControlLease,
) -> PreflightAttemptResult {
    let (result, retained_process_cleanup) = match run_process_stage(
        PRE_RUNTIME_STAGE_TIMEOUT,
        session.run_preflight(&prepared),
    )
    .await
    {
        ProcessStageCompletion::Completed(Ok(result)) => (result, false),
        ProcessStageCompletion::Completed(Err(failure)) => (
            failure.prepared_failure(),
            failure.requires_retained_repository_ownership(),
        ),
        ProcessStageCompletion::TimedOutWithCleanupUnproven => {
            (DeliveryRuntimeFailure::Unavailable.prepared_failure(), true)
        }
    };
    persist_prepared_result(
        dependencies,
        task_id,
        operation_id,
        durability,
        result,
        retained_process_cleanup,
        lease,
    )
    .await
}
