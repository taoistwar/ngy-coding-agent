use std::sync::{Arc, Mutex};

use coding_agent_store::{
    AcceptMergeCommandRequest, AcceptMergeOutcome, BeginMergeAbortRequest, CompleteMergeRequest,
    DeliveryAcceptedOperationState, DeliveryCommand, DeliveryCommandLookup, DeliveryCommandReceipt,
    DeliveryOperationId, DeliverySourceState, EnterMergePendingRequest, MarkPreflightStaleOutcome,
    MarkPreflightStaleRequest, MergeOperationState, MergeReconciliationReason,
    MergeTransitionOutcome, PreflightRejectedReason, PreflightStaleReason, ReconcileMergeRequest,
    RecordMergeKnownFailureRequest, StoreError,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::time::{sleep, timeout};

use crate::{
    DeliveryCommandConflict, DeliveryMergeAcceptance, DeliveryMergeAcceptanceOutcome,
    DeliveryMergeReceiptDisposition, DeliveryMergeWriteCommand, DeliveryMergeWriteOutcome,
    DeliveryPreflightBusyReason, DeliveryPreflightUnavailableReason, DeliveryWriteCommand,
    DeliveryWriteOutcome, RepositoryControlCoordinator, RepositoryControlError,
    RepositoryControlLease, RepositoryControlPoisonReason, ServiceState, ServiceStateController,
    ServiceStateSnapshot, TaskActiveOwnership,
};

use super::abort::drive_abort_stage;
use super::command::{
    DeliveryManagerCommand, DeliveryWorkerCompletion, DeliveryWorkerRetainedOwnership,
    DeliveryWorkerRetention,
};
use super::live_runtime::{
    DeliveryAcceptAuthenticationError, DeliveryLiveMergeDisposition, DeliveryLiveRuntimeError,
    DeliveryLiveRuntimeSession,
};
use super::query::persistent_reasons;
use super::recovery::{
    DeliveryRecoveryContext, ExactDeliveryWriteResult, LIVE_ORCHESTRATION_TIMEOUT,
    LIVE_RETRY_DELAY, LIVE_RUNTIME_STAGE_TIMEOUT, LiveStageOutcome, MAX_LIVE_ATTEMPTS,
    RecoveryLoadError, STORE_READ_TIMEOUT, execute_exact_delivery_write, load_operation_context,
};
use super::runtime_stage::{ProcessStageCompletion, run_process_stage};
use super::source::{drive_source_stage, reconcile_source};
use super::{
    DeliveryAcceptRequest, DeliveryIntakeGate, DeliveryManagerBackend,
    DeliveryManagerLiveDependencies, DeliveryOperationRecoveryOutcome,
};

mod admission;
mod persist;
mod routing;
mod runtime;
mod validation;

const MAX_PIPELINE_STEPS: usize = 16;

type AcceptResponseSlot = Arc<Mutex<Option<oneshot::Sender<DeliveryMergeAcceptanceOutcome>>>>;

struct AcceptFlow {
    dependencies: Arc<DeliveryManagerLiveDependencies>,
    repository_control: Arc<RepositoryControlCoordinator>,
    intake_gate: Arc<DeliveryIntakeGate>,
    service_state: ServiceStateController,
    command: AcceptMergeCommandRequest,
    response: AcceptResponseSlot,
}

// This is the actor-to-worker ownership handoff; the explicit guards, gates
// and response channel are part of the fail-closed contract.
#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_accept_worker(
    worker_id: u64,
    global_git_operations: Arc<Semaphore>,
    repository_control: Arc<RepositoryControlCoordinator>,
    intake_gate: Arc<DeliveryIntakeGate>,
    service_state: ServiceStateController,
    backend: DeliveryManagerBackend,
    service: ServiceStateSnapshot,
    request: DeliveryAcceptRequest,
    completion_sender: mpsc::Sender<DeliveryManagerCommand>,
    response: oneshot::Sender<DeliveryMergeAcceptanceOutcome>,
) {
    let response = Arc::new(Mutex::new(Some(response)));
    tokio::spawn(async move {
        let execution_response = Arc::clone(&response);
        let execution = tokio::spawn(async move {
            match backend {
                DeliveryManagerBackend::Unavailable => {
                    let outcome = unavailable_accept_outcome(service, intake_gate.as_ref());
                    send_accept_response(&execution_response, outcome.clone());
                    WorkerFinish::released(DeliveryOperationRecoveryOutcome::Unavailable)
                        .with_accept_fallback(outcome)
                }
                DeliveryManagerBackend::Live(dependencies) => {
                    run_accept_worker(
                        dependencies,
                        global_git_operations,
                        repository_control,
                        intake_gate,
                        service_state,
                        request.into_command(),
                        Arc::clone(&execution_response),
                    )
                    .await
                }
            }
        })
        .await;
        let finish = match execution {
            Ok(finish) => finish,
            Err(_) => WorkerFinish::released(DeliveryOperationRecoveryOutcome::Unavailable)
                .with_accept_fallback(DeliveryMergeAcceptanceOutcome::Unavailable(
                    DeliveryPreflightUnavailableReason::OrchestrationUnavailable,
                )),
        };
        if let Some(outcome) = finish.accept_fallback.clone() {
            send_accept_response(&response, outcome);
        }
        let retention = finish.into_retention();
        let _ = completion_sender
            .send(DeliveryManagerCommand::WorkerCompleted {
                worker_id,
                completion: Box::new(DeliveryWorkerCompletion::Merge { retention }),
            })
            .await;
    });
}

pub(super) fn spawn_recovery_worker(
    worker_id: u64,
    global_git_operations: Arc<Semaphore>,
    repository_control: Arc<RepositoryControlCoordinator>,
    backend: DeliveryManagerBackend,
    operation_id: DeliveryOperationId,
    completion_sender: mpsc::Sender<DeliveryManagerCommand>,
    response: oneshot::Sender<DeliveryOperationRecoveryOutcome>,
) {
    tokio::spawn(async move {
        let execution = tokio::spawn(async move {
            match backend {
                DeliveryManagerBackend::Unavailable => {
                    WorkerFinish::released(DeliveryOperationRecoveryOutcome::Unavailable)
                }
                DeliveryManagerBackend::Live(dependencies) => {
                    run_recovery_worker(
                        dependencies,
                        global_git_operations,
                        repository_control,
                        operation_id,
                    )
                    .await
                }
            }
        })
        .await;
        let finish = execution.unwrap_or_else(|_| {
            WorkerFinish::released(DeliveryOperationRecoveryOutcome::Unavailable)
        });
        let outcome = finish.recovery_outcome;
        let retention = finish.into_retention();
        let _ = completion_sender
            .send(DeliveryManagerCommand::WorkerCompleted {
                worker_id,
                completion: Box::new(DeliveryWorkerCompletion::Recovery {
                    outcome,
                    retention,
                    response,
                }),
            })
            .await;
    });
}

struct WorkerFinish {
    recovery_outcome: DeliveryOperationRecoveryOutcome,
    ownership: Option<(OwnedSemaphorePermit, RepositoryControlLease)>,
    accept_fallback: Option<DeliveryMergeAcceptanceOutcome>,
}

impl WorkerFinish {
    const fn released(recovery_outcome: DeliveryOperationRecoveryOutcome) -> Self {
        Self {
            recovery_outcome,
            ownership: None,
            accept_fallback: None,
        }
    }

    fn retained(
        recovery_outcome: DeliveryOperationRecoveryOutcome,
        permit: OwnedSemaphorePermit,
        lease: RepositoryControlLease,
    ) -> Self {
        Self {
            recovery_outcome,
            ownership: Some((permit, lease)),
            accept_fallback: None,
        }
    }

    fn with_accept_fallback(mut self, outcome: DeliveryMergeAcceptanceOutcome) -> Self {
        self.accept_fallback = Some(outcome);
        self
    }

    fn into_retention(self) -> DeliveryWorkerRetention {
        match self.ownership {
            Some((permit, lease)) => DeliveryWorkerRetention::RetainedFailClosed(
                DeliveryWorkerRetainedOwnership::new(permit, lease),
            ),
            None => DeliveryWorkerRetention::Released,
        }
    }
}

async fn run_accept_worker(
    dependencies: Arc<DeliveryManagerLiveDependencies>,
    global_git_operations: Arc<Semaphore>,
    repository_control: Arc<RepositoryControlCoordinator>,
    intake_gate: Arc<DeliveryIntakeGate>,
    service_state: ServiceStateController,
    command: AcceptMergeCommandRequest,
    response: AcceptResponseSlot,
) -> WorkerFinish {
    let flow = AcceptFlow {
        dependencies,
        repository_control,
        intake_gate,
        service_state,
        command,
        response,
    };
    let admitted = match admission::admit(&flow, global_git_operations).await {
        Ok(admitted) => admitted,
        Err(finish) => return finish,
    };
    let routed = match routing::refresh(&flow, admitted).await {
        Ok(routed) => routed,
        Err(finish) => return finish,
    };
    let validated = match validation::validate(&flow, routed) {
        Ok(validated) => validated,
        Err(finish) => return finish,
    };
    let authenticated = match runtime::authenticate(&flow, validated).await {
        Ok(authenticated) => authenticated,
        Err(finish) => return finish,
    };
    persist::persist(&flow, authenticated).await
}

async fn run_recovery_worker(
    dependencies: Arc<DeliveryManagerLiveDependencies>,
    global_git_operations: Arc<Semaphore>,
    repository_control: Arc<RepositoryControlCoordinator>,
    operation_id: DeliveryOperationId,
) -> WorkerFinish {
    let routing_context = match load_operation_context(dependencies.as_ref(), operation_id).await {
        Ok(context) => context,
        Err(RecoveryLoadError::NotFound) => {
            return WorkerFinish::released(DeliveryOperationRecoveryOutcome::NotFound);
        }
        Err(RecoveryLoadError::Unavailable) => {
            return WorkerFinish::released(DeliveryOperationRecoveryOutcome::Unavailable);
        }
        Err(RecoveryLoadError::Inconsistent) => {
            return WorkerFinish::released(
                DeliveryOperationRecoveryOutcome::ReconciliationRequired,
            );
        }
    };
    if operation_is_terminal(routing_context.operation.state) {
        return WorkerFinish::released(recovery_outcome_for_state(routing_context.operation.state));
    }
    let permit = match timeout(
        LIVE_ORCHESTRATION_TIMEOUT,
        Arc::clone(&global_git_operations).acquire_owned(),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(_)) | Err(_) => {
            return WorkerFinish::released(DeliveryOperationRecoveryOutcome::Unavailable);
        }
    };
    let repository_id = routing_context.snapshot.task.repository_id;
    let key = match repository_control.delivery_coordination_key(repository_id) {
        Ok(key) => key,
        Err(_) => {
            return WorkerFinish::released(DeliveryOperationRecoveryOutcome::Unavailable);
        }
    };
    let lease = match repository_control.try_acquire_delivery(key) {
        Ok(lease) => lease,
        Err(_) => return WorkerFinish::released(DeliveryOperationRecoveryOutcome::Pending),
    };
    let context = match load_operation_context(dependencies.as_ref(), operation_id).await {
        Ok(context) => context,
        Err(_) => return finish_stage(permit, lease, LiveStageOutcome::Poison),
    };
    if context.snapshot.task.repository_id != repository_id {
        return finish_stage(permit, lease, LiveStageOutcome::Poison);
    }
    match timeout(
        LIVE_ORCHESTRATION_TIMEOUT,
        dependencies
            .task_ownership
            .active_ownership(context.snapshot.task.id),
    )
    .await
    {
        Ok(Ok(TaskActiveOwnership::Inactive)) => {}
        Ok(Ok(TaskActiveOwnership::Active { .. })) => {
            return finish_stage(permit, lease, LiveStageOutcome::Release);
        }
        Ok(Err(_)) | Err(_) => return finish_stage(permit, lease, LiveStageOutcome::Poison),
    }
    match timeout(
        LIVE_ORCHESTRATION_TIMEOUT,
        dependencies
            .process_proofs
            .observe(context.snapshot.task.id),
    )
    .await
    {
        Ok(Ok(super::DeliveryProcessProof::Clean)) => {}
        Ok(Ok(super::DeliveryProcessProof::Active)) => {
            return finish_stage(permit, lease, LiveStageOutcome::Release);
        }
        Ok(Ok(super::DeliveryProcessProof::CleanupUnproven)) | Ok(Err(_)) | Err(_) => {
            return finish_stage(permit, lease, LiveStageOutcome::Retain);
        }
    }
    let stage = drive_pipeline(dependencies.as_ref(), context).await;
    finish_stage(permit, lease, stage)
}

async fn drive_pipeline(
    dependencies: &DeliveryManagerLiveDependencies,
    mut context: DeliveryRecoveryContext,
) -> LiveStageOutcome {
    let mut retry_count = 0usize;
    let mut retain_on_failure = false;
    for _ in 0..MAX_PIPELINE_STEPS {
        if operation_is_terminal(context.operation.state) {
            return if context.operation.state == MergeOperationState::ReconciliationRequired {
                LiveStageOutcome::Poison
            } else {
                LiveStageOutcome::Finished
            };
        }
        let Some(registry) = dependencies.live_runtime_registry.as_ref() else {
            return release_or_retain(retain_on_failure);
        };
        let session = match timeout(
            LIVE_RUNTIME_STAGE_TIMEOUT,
            registry.open_live_session(&context.snapshot),
        )
        .await
        {
            Ok(Ok(session)) => session,
            Ok(Err(DeliveryLiveRuntimeError::ProcessCleanupUnproven)) => {
                return LiveStageOutcome::Retain;
            }
            Ok(Err(DeliveryLiveRuntimeError::ReconciliationRequired(reason))) => {
                return if context.source.as_ref().is_some_and(|source| {
                    matches!(
                        source.state,
                        DeliverySourceState::ObjectPending | DeliverySourceState::CommitPending
                    )
                }) {
                    reconcile_source(dependencies, &context, reason).await
                } else {
                    reconcile_operation(dependencies, &context.operation, reason).await
                };
            }
            Ok(Err(DeliveryLiveRuntimeError::Unavailable)) | Err(_) => {
                return release_or_retain(retain_on_failure);
            }
        };
        let stage = match context.operation.state {
            MergeOperationState::Accepted => {
                if context
                    .source
                    .as_ref()
                    .is_some_and(|source| source.state == DeliverySourceState::Committed)
                {
                    drive_merge_stage(dependencies, session.as_ref(), &context).await
                } else {
                    drive_source_stage(dependencies, session.as_ref(), &context).await
                }
            }
            MergeOperationState::MergePending => {
                drive_merge_stage(dependencies, session.as_ref(), &context).await
            }
            MergeOperationState::AbortPending => {
                drive_abort_stage(dependencies, session.as_ref(), &context).await
            }
            _ => LiveStageOutcome::Poison,
        };
        if stage == LiveStageOutcome::RetryThenRetain {
            retain_on_failure = true;
        }
        match stage {
            LiveStageOutcome::Continue => {
                retry_count = 0;
                retain_on_failure = false;
            }
            LiveStageOutcome::Retry | LiveStageOutcome::RetryThenRetain
                if retry_count + 1 < MAX_LIVE_ATTEMPTS =>
            {
                retry_count += 1;
                sleep(LIVE_RETRY_DELAY).await;
            }
            LiveStageOutcome::Retry => return release_or_retain(retain_on_failure),
            LiveStageOutcome::RetryThenRetain => return LiveStageOutcome::Retain,
            LiveStageOutcome::Release => return release_or_retain(retain_on_failure),
            terminal => return terminal,
        }
        context = match load_operation_context(dependencies, context.operation.operation_id).await {
            Ok(context) => context,
            Err(RecoveryLoadError::Unavailable) => {
                return release_or_retain(retain_on_failure);
            }
            Err(RecoveryLoadError::NotFound | RecoveryLoadError::Inconsistent) => {
                return LiveStageOutcome::Poison;
            }
        };
    }
    release_or_retain(retain_on_failure)
}

const fn release_or_retain(retain_on_failure: bool) -> LiveStageOutcome {
    if retain_on_failure {
        LiveStageOutcome::Retain
    } else {
        LiveStageOutcome::Release
    }
}

async fn drive_merge_stage(
    dependencies: &DeliveryManagerLiveDependencies,
    session: &dyn DeliveryLiveRuntimeSession,
    context: &DeliveryRecoveryContext,
) -> LiveStageOutcome {
    let operation = &context.operation;
    let Some(source) = context.source.as_ref() else {
        return LiveStageOutcome::Poison;
    };
    if source.state != DeliverySourceState::Committed {
        return LiveStageOutcome::Poison;
    }
    match operation.state {
        MergeOperationState::Accepted => {
            let proof = match run_process_stage(
                LIVE_RUNTIME_STAGE_TIMEOUT,
                session.build_expected_merge(operation, source),
            )
            .await
            {
                ProcessStageCompletion::Completed(Ok(proof)) => proof,
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
                ProcessStageCompletion::TimedOutWithCleanupUnproven => {
                    return LiveStageOutcome::Retain;
                }
            };
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
            let request = match EnterMergePendingRequest::try_new(
                operation.provenance.identity.task_id(),
                operation.operation_id,
                operation.version,
                proof,
            ) {
                Ok(request) => request,
                Err(_) => return LiveStageOutcome::Poison,
            };
            let command =
                DeliveryWriteCommand::Merge(DeliveryMergeWriteCommand::EnterPending(request));
            match execute_exact_delivery_write(&dependencies.writer, command).await {
                ExactDeliveryWriteResult::Confirmed(DeliveryWriteOutcome::Merge(
                    DeliveryMergeWriteOutcome::EnterPending(
                        MergeTransitionOutcome::Applied(receipt)
                        | MergeTransitionOutcome::Existing(receipt),
                    ),
                )) if receipt.operation_id == operation.operation_id
                    && receipt.state == MergeOperationState::MergePending
                    && receipt.version == next_version(operation.version) =>
                {
                    LiveStageOutcome::Continue
                }
                other => write_stage_outcome(other),
            }
        }
        MergeOperationState::MergePending => {
            let disposition = match run_process_stage(
                LIVE_RUNTIME_STAGE_TIMEOUT,
                session.drive_merge_pending(operation, source),
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
                ProcessStageCompletion::TimedOutWithCleanupUnproven => {
                    return LiveStageOutcome::Retain;
                }
            };
            match disposition {
                DeliveryLiveMergeDisposition::Applied(proof) => match (*proof).into_store_proof() {
                    Ok(proof) => complete_merge(dependencies, operation, proof).await,
                    Err(DeliveryLiveRuntimeError::ReconciliationRequired(reason)) => {
                        reconcile_operation(dependencies, operation, reason).await
                    }
                    Err(DeliveryLiveRuntimeError::ProcessCleanupUnproven) => {
                        LiveStageOutcome::Retain
                    }
                    Err(DeliveryLiveRuntimeError::Unavailable) => LiveStageOutcome::Release,
                },
                DeliveryLiveMergeDisposition::Conflict(proof) => {
                    match (*proof).into_store_proof() {
                        Ok(proof) => begin_abort(dependencies, operation, proof).await,
                        Err(DeliveryLiveRuntimeError::ReconciliationRequired(reason)) => {
                            reconcile_operation(dependencies, operation, reason).await
                        }
                        Err(DeliveryLiveRuntimeError::ProcessCleanupUnproven) => {
                            LiveStageOutcome::Retain
                        }
                        Err(DeliveryLiveRuntimeError::Unavailable) => LiveStageOutcome::Release,
                    }
                }
                DeliveryLiveMergeDisposition::KnownNotApplied(reason) => {
                    record_merge_failure(dependencies, operation, reason).await
                }
                DeliveryLiveMergeDisposition::ReconciliationRequired(reason) => {
                    reconcile_operation(dependencies, operation, reason).await
                }
                DeliveryLiveMergeDisposition::ProcessCleanupUnproven => LiveStageOutcome::Retain,
            }
        }
        _ => LiveStageOutcome::Poison,
    }
}

async fn complete_merge(
    dependencies: &DeliveryManagerLiveDependencies,
    operation: &coding_agent_store::MergeOperationRecord,
    proof: coding_agent_store::MergeAppliedProof,
) -> LiveStageOutcome {
    let request = match CompleteMergeRequest::try_new(
        operation.provenance.identity.task_id(),
        operation.operation_id,
        operation.version,
        proof,
    ) {
        Ok(request) => request,
        Err(_) => return LiveStageOutcome::Poison,
    };
    let command = DeliveryWriteCommand::Merge(DeliveryMergeWriteCommand::Complete(request));
    match execute_exact_delivery_write(&dependencies.writer, command).await {
        ExactDeliveryWriteResult::Confirmed(DeliveryWriteOutcome::Merge(
            DeliveryMergeWriteOutcome::Complete(
                MergeTransitionOutcome::Applied(receipt)
                | MergeTransitionOutcome::Existing(receipt),
            ),
        )) if receipt.operation_id == operation.operation_id
            && receipt.state == MergeOperationState::Merged
            && receipt.version == next_version(operation.version) =>
        {
            LiveStageOutcome::Finished
        }
        other => side_effect_write_stage_outcome(other),
    }
}

async fn begin_abort(
    dependencies: &DeliveryManagerLiveDependencies,
    operation: &coding_agent_store::MergeOperationRecord,
    proof: coding_agent_store::MergeAbortProof,
) -> LiveStageOutcome {
    let request = match BeginMergeAbortRequest::try_new(
        operation.provenance.identity.task_id(),
        operation.operation_id,
        operation.version,
        proof,
    ) {
        Ok(request) => request,
        Err(_) => return LiveStageOutcome::Poison,
    };
    let command = DeliveryWriteCommand::Merge(DeliveryMergeWriteCommand::BeginAbort(request));
    match execute_exact_delivery_write(&dependencies.writer, command).await {
        ExactDeliveryWriteResult::Confirmed(DeliveryWriteOutcome::Merge(
            DeliveryMergeWriteOutcome::BeginAbort(
                MergeTransitionOutcome::Applied(receipt)
                | MergeTransitionOutcome::Existing(receipt),
            ),
        )) if receipt.operation_id == operation.operation_id
            && receipt.state == MergeOperationState::AbortPending
            && receipt.version == next_version(operation.version) =>
        {
            // The runtime abort is deliberately deferred until this confirmed
            // durable proof is reloaded by the next pipeline iteration.
            LiveStageOutcome::Continue
        }
        other => side_effect_write_stage_outcome(other),
    }
}

async fn record_merge_failure(
    dependencies: &DeliveryManagerLiveDependencies,
    operation: &coding_agent_store::MergeOperationRecord,
    reason: coding_agent_store::MergeKnownNotAppliedReason,
) -> LiveStageOutcome {
    let request = match RecordMergeKnownFailureRequest::try_new(
        operation.provenance.identity.task_id(),
        operation.operation_id,
        operation.state,
        operation.version,
        reason,
    ) {
        Ok(request) => request,
        Err(_) => return LiveStageOutcome::Poison,
    };
    let command =
        DeliveryWriteCommand::Merge(DeliveryMergeWriteCommand::RecordKnownFailure(request));
    match execute_exact_delivery_write(&dependencies.writer, command).await {
        ExactDeliveryWriteResult::Confirmed(DeliveryWriteOutcome::Merge(
            DeliveryMergeWriteOutcome::RecordKnownFailure(
                MergeTransitionOutcome::Applied(receipt)
                | MergeTransitionOutcome::Existing(receipt),
            ),
        )) if receipt.operation_id == operation.operation_id
            && receipt.state == MergeOperationState::Failed
            && receipt.version == next_version(operation.version) =>
        {
            LiveStageOutcome::Finished
        }
        other => write_stage_outcome(other),
    }
}

pub(super) async fn reconcile_operation(
    dependencies: &DeliveryManagerLiveDependencies,
    operation: &coding_agent_store::MergeOperationRecord,
    reason: coding_agent_store::MergeReconciliationReason,
) -> LiveStageOutcome {
    let request = match ReconcileMergeRequest::try_new(
        operation.provenance.identity.task_id(),
        operation.operation_id,
        operation.state,
        operation.version,
        reason,
    ) {
        Ok(request) => request,
        Err(_) => return LiveStageOutcome::Poison,
    };
    let command = DeliveryWriteCommand::Merge(DeliveryMergeWriteCommand::Reconcile(request));
    match execute_exact_delivery_write(&dependencies.writer, command).await {
        ExactDeliveryWriteResult::Confirmed(DeliveryWriteOutcome::Merge(
            DeliveryMergeWriteOutcome::Reconcile(
                MergeTransitionOutcome::Applied(receipt)
                | MergeTransitionOutcome::Existing(receipt),
            ),
        )) if receipt.operation_id == operation.operation_id
            && receipt.state == MergeOperationState::ReconciliationRequired
            && receipt.version == next_version(operation.version) =>
        {
            LiveStageOutcome::Poison
        }
        other => reconciliation_write_stage_outcome(other),
    }
}

fn finish_stage(
    permit: OwnedSemaphorePermit,
    lease: RepositoryControlLease,
    stage: LiveStageOutcome,
) -> WorkerFinish {
    match stage {
        LiveStageOutcome::Finished => {
            let outcome = if lease.clean_release().is_ok() {
                DeliveryOperationRecoveryOutcome::Converged
            } else {
                DeliveryOperationRecoveryOutcome::ReconciliationRequired
            };
            drop(permit);
            WorkerFinish::released(outcome)
        }
        LiveStageOutcome::Release | LiveStageOutcome::Retry | LiveStageOutcome::Continue => {
            let outcome = if lease.clean_release().is_ok() {
                DeliveryOperationRecoveryOutcome::Pending
            } else {
                DeliveryOperationRecoveryOutcome::ReconciliationRequired
            };
            drop(permit);
            WorkerFinish::released(outcome)
        }
        LiveStageOutcome::Retain | LiveStageOutcome::RetryThenRetain => WorkerFinish::retained(
            DeliveryOperationRecoveryOutcome::RetainedFailClosed,
            permit,
            lease,
        ),
        LiveStageOutcome::Poison => {
            let _ = lease.poison(RepositoryControlPoisonReason::SideEffectIdentityMismatch);
            drop(permit);
            WorkerFinish::released(DeliveryOperationRecoveryOutcome::ReconciliationRequired)
        }
    }
}

fn clean_accept(
    permit: OwnedSemaphorePermit,
    lease: RepositoryControlLease,
    response: &AcceptResponseSlot,
    outcome: DeliveryMergeAcceptanceOutcome,
) -> WorkerFinish {
    send_accept_response(response, outcome.clone());
    let recovery = if lease.clean_release().is_ok() {
        DeliveryOperationRecoveryOutcome::Pending
    } else {
        DeliveryOperationRecoveryOutcome::ReconciliationRequired
    };
    drop(permit);
    WorkerFinish::released(recovery).with_accept_fallback(outcome)
}

fn poison_accept(
    permit: OwnedSemaphorePermit,
    lease: RepositoryControlLease,
    response: &AcceptResponseSlot,
    outcome: DeliveryMergeAcceptanceOutcome,
) -> WorkerFinish {
    send_accept_response(response, outcome.clone());
    let _ = lease.poison(RepositoryControlPoisonReason::SideEffectIdentityMismatch);
    drop(permit);
    WorkerFinish::released(DeliveryOperationRecoveryOutcome::ReconciliationRequired)
        .with_accept_fallback(outcome)
}

fn accept_released(
    response: &AcceptResponseSlot,
    outcome: DeliveryMergeAcceptanceOutcome,
) -> WorkerFinish {
    send_accept_response(response, outcome.clone());
    WorkerFinish::released(DeliveryOperationRecoveryOutcome::Unavailable)
        .with_accept_fallback(outcome)
}

fn durable_acceptance(
    receipt: &DeliveryCommandReceipt,
    disposition: DeliveryMergeReceiptDisposition,
) -> DeliveryMergeAcceptanceOutcome {
    DeliveryMergeAcceptanceOutcome::Durable(DeliveryMergeAcceptance::new(
        receipt.operation_id,
        receipt.accepted_operation_version,
        disposition,
    ))
}

fn merge_known_not_applied(
    reason: crate::KnownNotAppliedReason,
    error: Option<StoreError>,
) -> DeliveryMergeAcceptanceOutcome {
    match error {
        Some(StoreError::IdempotencyConflict) => {
            DeliveryMergeAcceptanceOutcome::Conflict(DeliveryCommandConflict::IdempotencyConflict)
        }
        Some(StoreError::DeliveryOperationInProgress) => {
            DeliveryMergeAcceptanceOutcome::Conflict(DeliveryCommandConflict::OperationInProgress)
        }
        Some(StoreError::TaskNotFound) => DeliveryMergeAcceptanceOutcome::Ineligible(vec![
            crate::DeliveryEligibilityReason::TaskNotFound,
        ]),
        Some(StoreError::TaskNotMergeEligible) => DeliveryMergeAcceptanceOutcome::Ineligible(vec![
            crate::DeliveryEligibilityReason::TaskNotCompleted,
        ]),
        Some(StoreError::DeliveryReconciliationRequired) => {
            DeliveryMergeAcceptanceOutcome::Ineligible(vec![
                crate::DeliveryEligibilityReason::ReconciliationRequired,
            ])
        }
        _ if reason == crate::KnownNotAppliedReason::DeadlineBeforeStart => {
            DeliveryMergeAcceptanceOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::CommandTimedOut,
            )
        }
        _ => DeliveryMergeAcceptanceOutcome::Unavailable(
            DeliveryPreflightUnavailableReason::StoreUnavailable,
        ),
    }
}

fn rejected_accept_outcome(reason: PreflightRejectedReason) -> DeliveryMergeAcceptanceOutcome {
    let reason = match reason {
        PreflightRejectedReason::TaskNotMergeEligible => {
            crate::DeliveryEligibilityReason::TaskNotCompleted
        }
        PreflightRejectedReason::TargetBranchDetached => {
            crate::DeliveryEligibilityReason::TargetBranchDetached
        }
        PreflightRejectedReason::TargetBranchMismatch => {
            crate::DeliveryEligibilityReason::TargetBranchMismatch
        }
        PreflightRejectedReason::TargetWorktreeDirty => {
            crate::DeliveryEligibilityReason::TargetWorktreeDirty
        }
        PreflightRejectedReason::TargetIgnoredPathCollision => {
            crate::DeliveryEligibilityReason::TargetIgnoredPathCollision
        }
        PreflightRejectedReason::TargetGitOperationInProgress => {
            crate::DeliveryEligibilityReason::TargetGitOperationInProgress
        }
        PreflightRejectedReason::UnsafeGitConfiguration => {
            crate::DeliveryEligibilityReason::UnsafeGitConfiguration
        }
        PreflightRejectedReason::UnsupportedGitAttributes => {
            crate::DeliveryEligibilityReason::UnsupportedGitAttributes
        }
        PreflightRejectedReason::SourceAlreadyInTarget => {
            crate::DeliveryEligibilityReason::SourceAlreadyInTarget
        }
    };
    DeliveryMergeAcceptanceOutcome::Ineligible(vec![reason])
}

fn stale_accept_outcome(reason: PreflightStaleReason) -> DeliveryMergeAcceptanceOutcome {
    let conflict = match reason {
        PreflightStaleReason::EvidenceStale => DeliveryCommandConflict::EvidenceStale,
        PreflightStaleReason::TargetBranchChanged => DeliveryCommandConflict::TargetBranchMismatch,
        PreflightStaleReason::TargetHeadChanged => DeliveryCommandConflict::TargetHeadChanged,
        PreflightStaleReason::SourceChanged => DeliveryCommandConflict::SourceChanged,
    };
    DeliveryMergeAcceptanceOutcome::Conflict(conflict)
}

fn merge_reconciliation_admission_outcome(
    reason: MergeReconciliationReason,
) -> DeliveryMergeAcceptanceOutcome {
    match reason {
        MergeReconciliationReason::DeliveryStateInconsistent => {
            DeliveryMergeAcceptanceOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::RuntimeUnavailable,
            )
        }
        MergeReconciliationReason::SourceInconsistent => {
            DeliveryMergeAcceptanceOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::SourceInconsistent,
            )
        }
        MergeReconciliationReason::ProcessTreeCleanupFailed => {
            DeliveryMergeAcceptanceOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::ProcessProofUnavailable,
            )
        }
        MergeReconciliationReason::WorktreeIdentityMismatch => {
            DeliveryMergeAcceptanceOutcome::Conflict(
                DeliveryCommandConflict::WorktreeIdentityMismatch,
            )
        }
        MergeReconciliationReason::UnsafeGitConfiguration => {
            DeliveryMergeAcceptanceOutcome::Ineligible(vec![
                crate::DeliveryEligibilityReason::UnsafeGitConfiguration,
            ])
        }
        MergeReconciliationReason::UnsupportedGitAttributes => {
            DeliveryMergeAcceptanceOutcome::Ineligible(vec![
                crate::DeliveryEligibilityReason::UnsupportedGitAttributes,
            ])
        }
    }
}

fn send_accept_response(slot: &AcceptResponseSlot, outcome: DeliveryMergeAcceptanceOutcome) {
    if let Some(response) = slot.lock().expect("lock accept response slot").take() {
        let _ = response.send(outcome);
    }
}

fn unavailable_accept_outcome(
    service: ServiceStateSnapshot,
    intake_gate: &DeliveryIntakeGate,
) -> DeliveryMergeAcceptanceOutcome {
    let reason = if intake_gate.snapshot().0 || service.state == ServiceState::Quiescing {
        DeliveryPreflightUnavailableReason::ManagerQuiescing
    } else if service.state != ServiceState::Ready {
        DeliveryPreflightUnavailableReason::ServiceNotReady
    } else {
        DeliveryPreflightUnavailableReason::OrchestrationUnavailable
    };
    DeliveryMergeAcceptanceOutcome::Unavailable(reason)
}

fn inconsistent_accept_outcome() -> DeliveryMergeAcceptanceOutcome {
    DeliveryMergeAcceptanceOutcome::Unavailable(
        DeliveryPreflightUnavailableReason::RepositoryControlUnavailable,
    )
}

const fn operation_is_terminal(state: MergeOperationState) -> bool {
    matches!(
        state,
        MergeOperationState::Merged
            | MergeOperationState::Conflict
            | MergeOperationState::Rejected
            | MergeOperationState::Stale
            | MergeOperationState::Superseded
            | MergeOperationState::Failed
            | MergeOperationState::ReconciliationRequired
    )
}

fn recovery_outcome_for_state(state: MergeOperationState) -> DeliveryOperationRecoveryOutcome {
    if state == MergeOperationState::ReconciliationRequired {
        DeliveryOperationRecoveryOutcome::ReconciliationRequired
    } else {
        DeliveryOperationRecoveryOutcome::Converged
    }
}

fn write_stage_outcome(outcome: ExactDeliveryWriteResult) -> LiveStageOutcome {
    match outcome {
        ExactDeliveryWriteResult::KnownNotApplied { .. } => LiveStageOutcome::Retry,
        ExactDeliveryWriteResult::OutcomeUnknown => LiveStageOutcome::Retain,
        ExactDeliveryWriteResult::InvariantConflict | ExactDeliveryWriteResult::Confirmed(_) => {
            LiveStageOutcome::Poison
        }
    }
}

fn side_effect_write_stage_outcome(outcome: ExactDeliveryWriteResult) -> LiveStageOutcome {
    match outcome {
        ExactDeliveryWriteResult::KnownNotApplied { .. } => LiveStageOutcome::RetryThenRetain,
        other => write_stage_outcome(other),
    }
}

fn reconciliation_write_stage_outcome(outcome: ExactDeliveryWriteResult) -> LiveStageOutcome {
    match outcome {
        ExactDeliveryWriteResult::KnownNotApplied { .. } => LiveStageOutcome::Poison,
        other => write_stage_outcome(other),
    }
}

fn next_version(
    version: coding_agent_store::DeliveryVersion,
) -> coding_agent_store::DeliveryVersion {
    version
        .next()
        .expect("persisted delivery version can advance")
}
