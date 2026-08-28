use std::sync::{Arc, Mutex};

use coding_agent_store::{
    BranchCleanupKnownNotAppliedReason, BranchDisposition, CleanupAcceptanceOutcome, CleanupKind,
    CleanupOperationAnchor, CleanupOperationRecord, CleanupOperationState,
    CleanupReconciliationReason, CleanupTransitionOutcome, CompleteBranchCleanupRequest,
    CompleteWorktreeCleanupRequest, DeleteBranchCommandRequest, DeliveryAcceptedOperationState,
    DeliveryCommand, DeliveryCommandLookup, DeliveryCommandReceipt, DeliveryEligibilitySnapshot,
    DeliveryOperationId, DeliverySourceState, EnterWorktreeRemovePendingRequest,
    ReconcileBranchCleanupRequest, ReconcileWorktreeCleanupRequest,
    RecordBranchCleanupFailureRequest, RecordWorktreeCleanupFailureRequest,
    RecordWorktreeUnlockedRequest, RefreshBranchCleanupTargetRequest, RemoveWorktreeCommandRequest,
    StoreError, WorktreeCleanupKnownNotAppliedReason, WorktreeDisposition,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::time::{sleep, timeout};

use crate::{
    DeliveryCleanupAcceptance, DeliveryCleanupAcceptanceOutcome, DeliveryCleanupOperationKind,
    DeliveryCleanupOperationState, DeliveryCleanupReceiptDisposition, DeliveryCleanupWriteCommand,
    DeliveryCleanupWriteOutcome, DeliveryCommandConflict, DeliveryEligibilityReason,
    DeliveryPreflightBusyReason, DeliveryPreflightUnavailableReason, DeliveryWriteCommand,
    DeliveryWriteOutcome, RepositoryControlCoordinator, RepositoryControlError,
    RepositoryControlLease, RepositoryControlPoisonReason, ServiceState, ServiceStateController,
    ServiceStateSnapshot, TaskActiveOwnership,
};

use super::cleanup_runtime::{
    DeliveryBranchCleanupBinding, DeliveryCleanupRuntimeSession, DeliveryLiveBranchCleanupIntent,
    DeliveryLiveCleanupRuntimeError, DeliveryLiveDeletePendingDisposition,
    DeliveryWorktreeCleanupBinding,
};
use super::command::{
    DeliveryManagerCommand, DeliveryWorkerCompletion, DeliveryWorkerRetainedOwnership,
    DeliveryWorkerRetention,
};
use super::recovery::{
    DeliveryCleanupRecoveryContext, ExactDeliveryWriteResult, LIVE_ORCHESTRATION_TIMEOUT,
    LIVE_RETRY_DELAY, LIVE_RUNTIME_STAGE_TIMEOUT, LiveStageOutcome, MAX_LIVE_ATTEMPTS,
    RecoveryLoadError, STORE_READ_TIMEOUT, execute_exact_delivery_write,
    load_cleanup_operation_context,
};
use super::{
    DeliveryDeleteBranchRequest, DeliveryIntakeGate, DeliveryManagerBackend,
    DeliveryManagerLiveDependencies, DeliveryOperationRecoveryOutcome,
    DeliveryRemoveWorktreeRequest,
};

mod admission;
mod branch;
mod transitions;
mod worktree;

use admission::{send_cleanup_response, unavailable_cleanup_outcome};

const MAX_CLEANUP_PIPELINE_STEPS: usize = 12;

type CleanupResponseSlot = Arc<Mutex<Option<oneshot::Sender<DeliveryCleanupAcceptanceOutcome>>>>;

#[derive(Clone)]
enum CleanupCommand {
    RemoveWorktree(RemoveWorktreeCommandRequest),
    DeleteBranch(DeleteBranchCommandRequest),
}

impl CleanupCommand {
    const fn task_id(&self) -> coding_agent_domain::TaskId {
        match self {
            Self::RemoveWorktree(command) => command.task_id(),
            Self::DeleteBranch(command) => command.task_id(),
        }
    }

    const fn kind(&self) -> CleanupKind {
        match self {
            Self::RemoveWorktree(_) => CleanupKind::RemoveWorktree,
            Self::DeleteBranch(_) => CleanupKind::DeleteBranch,
        }
    }

    fn as_store_command(&self) -> DeliveryCommand {
        match self {
            Self::RemoveWorktree(command) => DeliveryCommand::RemoveWorktree(command.clone()),
            Self::DeleteBranch(command) => DeliveryCommand::DeleteBranch(command.clone()),
        }
    }

    fn write_command(&self) -> DeliveryWriteCommand {
        DeliveryWriteCommand::Cleanup(match self {
            Self::RemoveWorktree(command) => {
                DeliveryCleanupWriteCommand::AcceptWorktree(command.clone())
            }
            Self::DeleteBranch(command) => {
                DeliveryCleanupWriteCommand::AcceptBranch(command.clone())
            }
        })
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_remove_worktree_worker(
    worker_id: u64,
    global_git_operations: Arc<Semaphore>,
    repository_control: Arc<RepositoryControlCoordinator>,
    intake_gate: Arc<DeliveryIntakeGate>,
    service_state: ServiceStateController,
    backend: DeliveryManagerBackend,
    service: ServiceStateSnapshot,
    request: DeliveryRemoveWorktreeRequest,
    completion_sender: mpsc::Sender<DeliveryManagerCommand>,
    response: oneshot::Sender<DeliveryCleanupAcceptanceOutcome>,
) {
    spawn_accept_worker(
        worker_id,
        global_git_operations,
        repository_control,
        intake_gate,
        service_state,
        backend,
        service,
        CleanupCommand::RemoveWorktree(request.into_command()),
        completion_sender,
        response,
    );
}

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_delete_branch_worker(
    worker_id: u64,
    global_git_operations: Arc<Semaphore>,
    repository_control: Arc<RepositoryControlCoordinator>,
    intake_gate: Arc<DeliveryIntakeGate>,
    service_state: ServiceStateController,
    backend: DeliveryManagerBackend,
    service: ServiceStateSnapshot,
    request: DeliveryDeleteBranchRequest,
    completion_sender: mpsc::Sender<DeliveryManagerCommand>,
    response: oneshot::Sender<DeliveryCleanupAcceptanceOutcome>,
) {
    spawn_accept_worker(
        worker_id,
        global_git_operations,
        repository_control,
        intake_gate,
        service_state,
        backend,
        service,
        CleanupCommand::DeleteBranch(request.into_command()),
        completion_sender,
        response,
    );
}

#[allow(clippy::too_many_arguments)]
fn spawn_accept_worker(
    worker_id: u64,
    global_git_operations: Arc<Semaphore>,
    repository_control: Arc<RepositoryControlCoordinator>,
    intake_gate: Arc<DeliveryIntakeGate>,
    service_state: ServiceStateController,
    backend: DeliveryManagerBackend,
    service: ServiceStateSnapshot,
    command: CleanupCommand,
    completion_sender: mpsc::Sender<DeliveryManagerCommand>,
    response: oneshot::Sender<DeliveryCleanupAcceptanceOutcome>,
) {
    let response = Arc::new(Mutex::new(Some(response)));
    tokio::spawn(async move {
        let execution_response = Arc::clone(&response);
        let execution = tokio::spawn(async move {
            match backend {
                DeliveryManagerBackend::Unavailable => {
                    let outcome = unavailable_cleanup_outcome(service, intake_gate.as_ref());
                    send_cleanup_response(&execution_response, outcome.clone());
                    WorkerFinish::released(DeliveryOperationRecoveryOutcome::Unavailable)
                        .with_accept_fallback(outcome)
                }
                DeliveryManagerBackend::Live(dependencies) => {
                    admission::run_accept_worker(
                        dependencies,
                        global_git_operations,
                        repository_control,
                        intake_gate,
                        service_state,
                        command,
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
                .with_accept_fallback(DeliveryCleanupAcceptanceOutcome::Unavailable(
                    DeliveryPreflightUnavailableReason::OrchestrationUnavailable,
                )),
        };
        if let Some(outcome) = finish.accept_fallback.clone() {
            send_cleanup_response(&response, outcome);
        }
        let retention = finish.into_retention();
        let _ = completion_sender
            .send(DeliveryManagerCommand::WorkerCompleted {
                worker_id,
                completion: Box::new(DeliveryWorkerCompletion::Cleanup { retention }),
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
    accept_fallback: Option<DeliveryCleanupAcceptanceOutcome>,
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

    fn with_accept_fallback(mut self, outcome: DeliveryCleanupAcceptanceOutcome) -> Self {
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

// The existing variant deliberately carries the single audited Store snapshot
// through admission; keeping it by value avoids a second load or partial copy.
#[allow(clippy::large_enum_variant)]
enum CleanupReceiptStatus {
    Missing,
    Existing {
        receipt: DeliveryCommandReceipt,
        context: DeliveryCleanupRecoveryContext,
    },
}

async fn run_recovery_worker(
    dependencies: Arc<DeliveryManagerLiveDependencies>,
    global_git_operations: Arc<Semaphore>,
    repository_control: Arc<RepositoryControlCoordinator>,
    operation_id: DeliveryOperationId,
) -> WorkerFinish {
    let routing_context =
        match load_cleanup_operation_context(dependencies.as_ref(), operation_id).await {
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
    if cleanup_operation_is_terminal(routing_context.operation.state) {
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
    let context = match load_cleanup_operation_context(dependencies.as_ref(), operation_id).await {
        Ok(context) => context,
        Err(_) => return finish_stage(permit, lease, LiveStageOutcome::Poison),
    };
    if context.snapshot.task.repository_id != repository_id {
        return finish_stage(permit, lease, LiveStageOutcome::Poison);
    }
    match observe_task_ownership(dependencies.as_ref(), &context.snapshot).await {
        OwnershipObservation::Inactive => {}
        OwnershipObservation::Active => {
            return finish_stage(permit, lease, LiveStageOutcome::Release);
        }
        OwnershipObservation::Mismatch => {
            return finish_stage(permit, lease, LiveStageOutcome::Poison);
        }
    }
    match observe_processes(dependencies.as_ref(), context.snapshot.task.id).await {
        ProcessObservation::Clean => {}
        ProcessObservation::Active => {
            return finish_stage(permit, lease, LiveStageOutcome::Release);
        }
        ProcessObservation::CleanupUnproven => {
            return finish_stage(permit, lease, LiveStageOutcome::Retain);
        }
        ProcessObservation::Mismatch => {
            return finish_stage(permit, lease, LiveStageOutcome::Poison);
        }
    }
    let stage = drive_cleanup_pipeline(dependencies.as_ref(), context).await;
    finish_stage(permit, lease, stage)
}

async fn drive_cleanup_pipeline(
    dependencies: &DeliveryManagerLiveDependencies,
    mut context: DeliveryCleanupRecoveryContext,
) -> LiveStageOutcome {
    let mut retry_attempts = 0;
    let mut retain_on_failure = false;
    for _ in 0..MAX_CLEANUP_PIPELINE_STEPS {
        if cleanup_operation_is_terminal(context.operation.state) {
            return recovery_stage_for_state(context.operation.state);
        }
        let Some(registry) = dependencies.cleanup_runtime_registry.as_ref() else {
            return release_or_retain(retain_on_failure);
        };
        let session = match timeout(
            LIVE_RUNTIME_STAGE_TIMEOUT,
            registry.open_cleanup_session(&context.snapshot),
        )
        .await
        {
            Ok(Ok(session)) => session,
            Ok(Err(error)) => {
                let stage = transitions::runtime_error(dependencies, &context, error).await;
                return if stage == LiveStageOutcome::Release {
                    release_or_retain(retain_on_failure)
                } else {
                    stage
                };
            }
            Err(_) => return release_or_retain(retain_on_failure),
        };
        let stage = match context.operation.kind {
            CleanupKind::RemoveWorktree => {
                worktree::drive_worktree_stage(dependencies, session.as_ref(), &context).await
            }
            CleanupKind::DeleteBranch => {
                branch::drive_branch_stage(dependencies, session.as_ref(), &context).await
            }
        };
        if stage == LiveStageOutcome::RetryThenRetain {
            retain_on_failure = true;
        }
        match stage {
            LiveStageOutcome::Continue => {
                retry_attempts = 0;
                retain_on_failure = false;
            }
            LiveStageOutcome::Retry | LiveStageOutcome::RetryThenRetain
                if retry_attempts + 1 < MAX_LIVE_ATTEMPTS =>
            {
                retry_attempts += 1;
                sleep(LIVE_RETRY_DELAY).await;
            }
            LiveStageOutcome::Retry => return release_or_retain(retain_on_failure),
            LiveStageOutcome::RetryThenRetain => return LiveStageOutcome::Retain,
            LiveStageOutcome::Release => return release_or_retain(retain_on_failure),
            other => return other,
        }
        context = match load_cleanup_operation_context(dependencies, context.operation.operation_id)
            .await
        {
            Ok(context) => context,
            Err(RecoveryLoadError::Unavailable) if retain_on_failure => {
                return LiveStageOutcome::Retain;
            }
            Err(_) => return LiveStageOutcome::Poison,
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

enum OwnershipObservation {
    Inactive,
    Active,
    Mismatch,
}

async fn observe_task_ownership(
    dependencies: &DeliveryManagerLiveDependencies,
    snapshot: &DeliveryEligibilitySnapshot,
) -> OwnershipObservation {
    match timeout(
        LIVE_ORCHESTRATION_TIMEOUT,
        dependencies
            .task_ownership
            .active_ownership(snapshot.task.id),
    )
    .await
    {
        Ok(Ok(TaskActiveOwnership::Inactive)) => OwnershipObservation::Inactive,
        Ok(Ok(TaskActiveOwnership::Active {
            repository_id,
            attempt,
        })) if repository_id == snapshot.task.repository_id && attempt == snapshot.task.attempt => {
            OwnershipObservation::Active
        }
        Ok(Ok(TaskActiveOwnership::Active { .. })) | Ok(Err(_)) | Err(_) => {
            OwnershipObservation::Mismatch
        }
    }
}

enum ProcessObservation {
    Clean,
    Active,
    CleanupUnproven,
    Mismatch,
}

async fn observe_processes(
    dependencies: &DeliveryManagerLiveDependencies,
    task_id: coding_agent_domain::TaskId,
) -> ProcessObservation {
    match timeout(
        LIVE_ORCHESTRATION_TIMEOUT,
        dependencies.process_proofs.observe(task_id),
    )
    .await
    {
        Ok(Ok(super::DeliveryProcessProof::Clean)) => ProcessObservation::Clean,
        Ok(Ok(super::DeliveryProcessProof::Active)) => ProcessObservation::Active,
        Ok(Ok(super::DeliveryProcessProof::CleanupUnproven)) => ProcessObservation::CleanupUnproven,
        Ok(Err(_)) | Err(_) => ProcessObservation::Mismatch,
    }
}

async fn acquire_global_permit(
    semaphore: Arc<Semaphore>,
) -> Result<OwnedSemaphorePermit, DeliveryCleanupAcceptanceOutcome> {
    match timeout(LIVE_ORCHESTRATION_TIMEOUT, semaphore.acquire_owned()).await {
        Ok(Ok(permit)) => Ok(permit),
        Ok(Err(_)) => Err(DeliveryCleanupAcceptanceOutcome::Unavailable(
            DeliveryPreflightUnavailableReason::OrchestrationUnavailable,
        )),
        Err(_) => Err(DeliveryCleanupAcceptanceOutcome::Busy(
            DeliveryPreflightBusyReason::WorkerQueueFull,
        )),
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
    response: &CleanupResponseSlot,
    outcome: DeliveryCleanupAcceptanceOutcome,
) -> WorkerFinish {
    send_cleanup_response(response, outcome.clone());
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
    response: &CleanupResponseSlot,
    outcome: DeliveryCleanupAcceptanceOutcome,
) -> WorkerFinish {
    send_cleanup_response(response, outcome.clone());
    let _ = lease.poison(RepositoryControlPoisonReason::SideEffectIdentityMismatch);
    drop(permit);
    WorkerFinish::released(DeliveryOperationRecoveryOutcome::ReconciliationRequired)
        .with_accept_fallback(outcome)
}

fn accept_released(
    response: &CleanupResponseSlot,
    outcome: DeliveryCleanupAcceptanceOutcome,
) -> WorkerFinish {
    send_cleanup_response(response, outcome.clone());
    WorkerFinish::released(DeliveryOperationRecoveryOutcome::Unavailable)
        .with_accept_fallback(outcome)
}

fn cleanup_anchor(operation: &CleanupOperationRecord) -> Option<CleanupOperationAnchor> {
    CleanupOperationAnchor::try_new(
        operation.identity.task_id(),
        operation.operation_id,
        operation.version,
    )
    .ok()
}

const fn cleanup_operation_is_terminal(state: CleanupOperationState) -> bool {
    matches!(
        state,
        CleanupOperationState::Completed
            | CleanupOperationState::Failed
            | CleanupOperationState::ReconciliationRequired
    )
}

const fn recovery_stage_for_state(state: CleanupOperationState) -> LiveStageOutcome {
    match state {
        CleanupOperationState::Completed | CleanupOperationState::Failed => {
            LiveStageOutcome::Finished
        }
        CleanupOperationState::ReconciliationRequired => LiveStageOutcome::Poison,
        CleanupOperationState::UnlockPending
        | CleanupOperationState::UnlockedPendingRemove
        | CleanupOperationState::RemovePending
        | CleanupOperationState::DeletePending => LiveStageOutcome::Continue,
    }
}

const fn recovery_outcome_for_state(
    state: CleanupOperationState,
) -> DeliveryOperationRecoveryOutcome {
    match state {
        CleanupOperationState::Completed | CleanupOperationState::Failed => {
            DeliveryOperationRecoveryOutcome::Converged
        }
        CleanupOperationState::ReconciliationRequired => {
            DeliveryOperationRecoveryOutcome::ReconciliationRequired
        }
        CleanupOperationState::UnlockPending
        | CleanupOperationState::UnlockedPendingRemove
        | CleanupOperationState::RemovePending
        | CleanupOperationState::DeletePending => DeliveryOperationRecoveryOutcome::Pending,
    }
}

fn next_version(
    version: coding_agent_store::DeliveryVersion,
) -> coding_agent_store::DeliveryVersion {
    version
        .next()
        .expect("persisted cleanup version can advance")
}
