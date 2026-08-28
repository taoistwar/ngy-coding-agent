use std::time::Duration;

use coding_agent_domain::TaskId;
use coding_agent_store::{
    DeliveryEligibilitySnapshot, PreflightRejectedReason, PreflightStaleReason,
};
use tokio::time::timeout;

use crate::{
    DeliveryEligibilityReason, DeliveryTargetObservation, DeliveryTargetUnavailableReason,
    RepositoryControlState, ServiceState, ServiceStateSnapshot, TaskActiveOwnership,
};

use super::super::DeliveryManagerLiveDependencies;
use super::super::runtime::{
    DeliveryProcessProof, DeliveryRuntimeFailure, DeliveryRuntimeObservation,
    DeliveryRuntimeObservationUnavailableReason,
};

const QUERY_OBSERVATION_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) enum SnapshotObservation {
    Found(Box<DeliveryEligibilitySnapshot>),
    NotFound,
    Unavailable,
}

pub(super) struct DynamicObservations {
    pub(super) target: DeliveryTargetObservation,
    pub(super) ineligible: Vec<DeliveryEligibilityReason>,
    pub(super) unavailable: Vec<DeliveryEligibilityReason>,
}

pub(super) async fn load_snapshot(
    dependencies: &DeliveryManagerLiveDependencies,
    task_id: TaskId,
) -> SnapshotObservation {
    match timeout(
        QUERY_OBSERVATION_TIMEOUT,
        dependencies.store.delivery_eligibility_snapshot(task_id),
    )
    .await
    {
        Ok(Ok(Some(snapshot))) => SnapshotObservation::Found(Box::new(snapshot)),
        Ok(Ok(None)) => SnapshotObservation::NotFound,
        Ok(Err(_)) | Err(_) => SnapshotObservation::Unavailable,
    }
}

pub(super) async fn collect(
    dependencies: &DeliveryManagerLiveDependencies,
    service: ServiceStateSnapshot,
    task_id: TaskId,
    snapshot: &DeliveryEligibilitySnapshot,
    mut ineligible: Vec<DeliveryEligibilityReason>,
) -> DynamicObservations {
    let task_manager = timeout(
        QUERY_OBSERVATION_TIMEOUT,
        dependencies.task_ownership.active_ownership(task_id),
    );
    let process = timeout(
        QUERY_OBSERVATION_TIMEOUT,
        dependencies.process_proofs.observe(task_id),
    );
    let runtime_session = timeout(
        QUERY_OBSERVATION_TIMEOUT,
        dependencies.runtime_registry.open_session(snapshot),
    );
    let (task_manager, process, runtime_session) =
        tokio::join!(task_manager, process, runtime_session);

    let mut unavailable = Vec::new();
    if service.state != ServiceState::Ready {
        unavailable.push(DeliveryEligibilityReason::ServiceNotReady);
    }

    classify_task_ownership(task_manager, snapshot, &mut ineligible, &mut unavailable);
    classify_process_proof(process, &mut ineligible, &mut unavailable);
    classify_repository_control(dependencies, snapshot, &mut unavailable);
    let target = observe_target(runtime_session, &mut ineligible, &mut unavailable).await;

    DynamicObservations {
        target,
        ineligible,
        unavailable,
    }
}

fn classify_task_ownership<E>(
    task_manager: Result<Result<TaskActiveOwnership, E>, tokio::time::error::Elapsed>,
    snapshot: &DeliveryEligibilitySnapshot,
    ineligible: &mut Vec<DeliveryEligibilityReason>,
    unavailable: &mut Vec<DeliveryEligibilityReason>,
) {
    match task_manager {
        Ok(Ok(TaskActiveOwnership::Inactive)) => {}
        Ok(Ok(TaskActiveOwnership::Active {
            repository_id,
            attempt,
        })) if repository_id == snapshot.task.repository_id && attempt == snapshot.task.attempt => {
            ineligible.push(DeliveryEligibilityReason::TaskActive);
        }
        Ok(Ok(TaskActiveOwnership::Active { .. })) => {
            unavailable.push(DeliveryEligibilityReason::ReconciliationRequired);
        }
        Ok(Err(_)) | Err(_) => {
            unavailable.push(DeliveryEligibilityReason::RuntimeObservationUnavailable);
        }
    }
}

fn classify_process_proof<E>(
    process: Result<Result<DeliveryProcessProof, E>, tokio::time::error::Elapsed>,
    ineligible: &mut Vec<DeliveryEligibilityReason>,
    unavailable: &mut Vec<DeliveryEligibilityReason>,
) {
    match process {
        Ok(Ok(DeliveryProcessProof::Clean)) => {}
        Ok(Ok(DeliveryProcessProof::Active)) => {
            ineligible.push(DeliveryEligibilityReason::TaskActive);
        }
        Ok(Ok(DeliveryProcessProof::CleanupUnproven)) | Ok(Err(_)) | Err(_) => {
            unavailable.push(DeliveryEligibilityReason::ProcessCleanupUnproven);
        }
    }
}

fn classify_repository_control(
    dependencies: &DeliveryManagerLiveDependencies,
    snapshot: &DeliveryEligibilitySnapshot,
    unavailable: &mut Vec<DeliveryEligibilityReason>,
) {
    match dependencies
        .repository_control
        .coordination_key(snapshot.task.repository_id)
        .and_then(|_| {
            dependencies
                .repository_control
                .control_state(snapshot.task.repository_id)
        }) {
        Ok(RepositoryControlState::Available) => {}
        Ok(RepositoryControlState::Busy) => {
            unavailable.push(DeliveryEligibilityReason::RepositoryBusy);
        }
        Ok(RepositoryControlState::Poisoned) => {
            unavailable.push(DeliveryEligibilityReason::ReconciliationRequired);
        }
        Err(_) => unavailable.push(DeliveryEligibilityReason::RepositoryUnavailable),
    }
}

async fn observe_target(
    runtime_session: Result<
        Result<
            std::sync::Arc<dyn super::super::runtime::DeliveryRuntimeSession>,
            DeliveryRuntimeFailure,
        >,
        tokio::time::error::Elapsed,
    >,
    ineligible: &mut Vec<DeliveryEligibilityReason>,
    unavailable: &mut Vec<DeliveryEligibilityReason>,
) -> DeliveryTargetObservation {
    match runtime_session {
        Ok(Ok(session)) => match timeout(QUERY_OBSERVATION_TIMEOUT, session.observe()).await {
            Ok(Ok(DeliveryRuntimeObservation::Available { branch, head })) => {
                DeliveryTargetObservation::available(branch, head)
            }
            Ok(Ok(DeliveryRuntimeObservation::Unavailable { reason })) => {
                classify_observation_reason(reason, ineligible, unavailable);
                DeliveryTargetObservation::unavailable(project_observation_reason(reason))
            }
            Ok(Err(failure)) => {
                classify_runtime_failure(failure, ineligible, unavailable);
                DeliveryTargetObservation::unavailable(project_failure_reason(failure))
            }
            Err(_) => runtime_unavailable(unavailable),
        },
        Ok(Err(failure)) => {
            classify_runtime_failure(failure, ineligible, unavailable);
            DeliveryTargetObservation::unavailable(project_failure_reason(failure))
        }
        Err(_) => runtime_unavailable(unavailable),
    }
}

fn runtime_unavailable(
    unavailable: &mut Vec<DeliveryEligibilityReason>,
) -> DeliveryTargetObservation {
    unavailable.push(DeliveryEligibilityReason::RuntimeObservationUnavailable);
    DeliveryTargetObservation::unavailable(DeliveryTargetUnavailableReason::RuntimeUnavailable)
}

fn classify_runtime_failure(
    failure: DeliveryRuntimeFailure,
    ineligible: &mut Vec<DeliveryEligibilityReason>,
    unavailable: &mut Vec<DeliveryEligibilityReason>,
) {
    match failure {
        DeliveryRuntimeFailure::Rejected(reason) => {
            ineligible.push(project_rejected_reason(reason));
        }
        DeliveryRuntimeFailure::Stale(_) => {
            unavailable.push(DeliveryEligibilityReason::RuntimeDrift);
        }
        DeliveryRuntimeFailure::ReconciliationRequired(_) => {
            unavailable.push(DeliveryEligibilityReason::ReconciliationRequired);
        }
        DeliveryRuntimeFailure::ProcessCleanupUnproven => {
            unavailable.push(DeliveryEligibilityReason::ProcessCleanupUnproven);
        }
        DeliveryRuntimeFailure::Unavailable => {
            unavailable.push(DeliveryEligibilityReason::RuntimeObservationUnavailable);
        }
    }
}

fn classify_observation_reason(
    reason: DeliveryRuntimeObservationUnavailableReason,
    ineligible: &mut Vec<DeliveryEligibilityReason>,
    unavailable: &mut Vec<DeliveryEligibilityReason>,
) {
    match reason {
        DeliveryRuntimeObservationUnavailableReason::TargetBranchDetached => {
            ineligible.push(DeliveryEligibilityReason::TargetBranchDetached)
        }
        DeliveryRuntimeObservationUnavailableReason::TargetBranchMismatch => {
            ineligible.push(DeliveryEligibilityReason::TargetBranchMismatch)
        }
        DeliveryRuntimeObservationUnavailableReason::TargetWorktreeDirty => {
            ineligible.push(DeliveryEligibilityReason::TargetWorktreeDirty)
        }
        DeliveryRuntimeObservationUnavailableReason::TargetIgnoredPathCollision => {
            ineligible.push(DeliveryEligibilityReason::TargetIgnoredPathCollision)
        }
        DeliveryRuntimeObservationUnavailableReason::TargetGitOperationInProgress => {
            ineligible.push(DeliveryEligibilityReason::TargetGitOperationInProgress)
        }
        DeliveryRuntimeObservationUnavailableReason::UnsafeGitConfiguration => {
            ineligible.push(DeliveryEligibilityReason::UnsafeGitConfiguration)
        }
        DeliveryRuntimeObservationUnavailableReason::UnsupportedGitAttributes => {
            ineligible.push(DeliveryEligibilityReason::UnsupportedGitAttributes)
        }
        DeliveryRuntimeObservationUnavailableReason::SourceAlreadyInTarget => {
            ineligible.push(DeliveryEligibilityReason::SourceAlreadyInTarget)
        }
        DeliveryRuntimeObservationUnavailableReason::TargetHeadChanged => {
            unavailable.push(DeliveryEligibilityReason::RuntimeDrift)
        }
        DeliveryRuntimeObservationUnavailableReason::RuntimeUnavailable => {
            unavailable.push(DeliveryEligibilityReason::RuntimeObservationUnavailable)
        }
        DeliveryRuntimeObservationUnavailableReason::ProcessCleanupUnproven => {
            unavailable.push(DeliveryEligibilityReason::ProcessCleanupUnproven)
        }
        DeliveryRuntimeObservationUnavailableReason::ReconciliationRequired => {
            unavailable.push(DeliveryEligibilityReason::ReconciliationRequired)
        }
    }
}

const fn project_rejected_reason(reason: PreflightRejectedReason) -> DeliveryEligibilityReason {
    match reason {
        PreflightRejectedReason::TaskNotMergeEligible => {
            DeliveryEligibilityReason::TaskNotCompleted
        }
        PreflightRejectedReason::TargetBranchDetached => {
            DeliveryEligibilityReason::TargetBranchDetached
        }
        PreflightRejectedReason::TargetBranchMismatch => {
            DeliveryEligibilityReason::TargetBranchMismatch
        }
        PreflightRejectedReason::TargetWorktreeDirty => {
            DeliveryEligibilityReason::TargetWorktreeDirty
        }
        PreflightRejectedReason::TargetIgnoredPathCollision => {
            DeliveryEligibilityReason::TargetIgnoredPathCollision
        }
        PreflightRejectedReason::TargetGitOperationInProgress => {
            DeliveryEligibilityReason::TargetGitOperationInProgress
        }
        PreflightRejectedReason::UnsafeGitConfiguration => {
            DeliveryEligibilityReason::UnsafeGitConfiguration
        }
        PreflightRejectedReason::UnsupportedGitAttributes => {
            DeliveryEligibilityReason::UnsupportedGitAttributes
        }
        PreflightRejectedReason::SourceAlreadyInTarget => {
            DeliveryEligibilityReason::SourceAlreadyInTarget
        }
    }
}

const fn project_failure_reason(
    failure: DeliveryRuntimeFailure,
) -> DeliveryTargetUnavailableReason {
    match failure {
        DeliveryRuntimeFailure::Rejected(reason) => match reason {
            PreflightRejectedReason::TaskNotMergeEligible => {
                DeliveryTargetUnavailableReason::RuntimeUnavailable
            }
            PreflightRejectedReason::TargetBranchDetached => {
                DeliveryTargetUnavailableReason::TargetBranchDetached
            }
            PreflightRejectedReason::TargetBranchMismatch => {
                DeliveryTargetUnavailableReason::TargetBranchMismatch
            }
            PreflightRejectedReason::TargetWorktreeDirty => {
                DeliveryTargetUnavailableReason::TargetWorktreeDirty
            }
            PreflightRejectedReason::TargetIgnoredPathCollision => {
                DeliveryTargetUnavailableReason::TargetIgnoredPathCollision
            }
            PreflightRejectedReason::TargetGitOperationInProgress => {
                DeliveryTargetUnavailableReason::TargetGitOperationInProgress
            }
            PreflightRejectedReason::UnsafeGitConfiguration => {
                DeliveryTargetUnavailableReason::UnsafeGitConfiguration
            }
            PreflightRejectedReason::UnsupportedGitAttributes => {
                DeliveryTargetUnavailableReason::UnsupportedGitAttributes
            }
            PreflightRejectedReason::SourceAlreadyInTarget => {
                DeliveryTargetUnavailableReason::SourceAlreadyInTarget
            }
        },
        DeliveryRuntimeFailure::Stale(reason) => match reason {
            PreflightStaleReason::TargetBranchChanged => {
                DeliveryTargetUnavailableReason::TargetBranchMismatch
            }
            PreflightStaleReason::TargetHeadChanged => {
                DeliveryTargetUnavailableReason::TargetHeadChanged
            }
            PreflightStaleReason::EvidenceStale | PreflightStaleReason::SourceChanged => {
                DeliveryTargetUnavailableReason::RuntimeUnavailable
            }
        },
        DeliveryRuntimeFailure::ReconciliationRequired(_) => {
            DeliveryTargetUnavailableReason::ReconciliationRequired
        }
        DeliveryRuntimeFailure::ProcessCleanupUnproven => {
            DeliveryTargetUnavailableReason::ProcessCleanupUnproven
        }
        DeliveryRuntimeFailure::Unavailable => DeliveryTargetUnavailableReason::RuntimeUnavailable,
    }
}

const fn project_observation_reason(
    reason: DeliveryRuntimeObservationUnavailableReason,
) -> DeliveryTargetUnavailableReason {
    match reason {
        DeliveryRuntimeObservationUnavailableReason::TargetBranchDetached => {
            DeliveryTargetUnavailableReason::TargetBranchDetached
        }
        DeliveryRuntimeObservationUnavailableReason::TargetBranchMismatch => {
            DeliveryTargetUnavailableReason::TargetBranchMismatch
        }
        DeliveryRuntimeObservationUnavailableReason::TargetWorktreeDirty => {
            DeliveryTargetUnavailableReason::TargetWorktreeDirty
        }
        DeliveryRuntimeObservationUnavailableReason::TargetIgnoredPathCollision => {
            DeliveryTargetUnavailableReason::TargetIgnoredPathCollision
        }
        DeliveryRuntimeObservationUnavailableReason::TargetGitOperationInProgress => {
            DeliveryTargetUnavailableReason::TargetGitOperationInProgress
        }
        DeliveryRuntimeObservationUnavailableReason::UnsafeGitConfiguration => {
            DeliveryTargetUnavailableReason::UnsafeGitConfiguration
        }
        DeliveryRuntimeObservationUnavailableReason::UnsupportedGitAttributes => {
            DeliveryTargetUnavailableReason::UnsupportedGitAttributes
        }
        DeliveryRuntimeObservationUnavailableReason::SourceAlreadyInTarget => {
            DeliveryTargetUnavailableReason::SourceAlreadyInTarget
        }
        DeliveryRuntimeObservationUnavailableReason::TargetHeadChanged => {
            DeliveryTargetUnavailableReason::TargetHeadChanged
        }
        DeliveryRuntimeObservationUnavailableReason::RuntimeUnavailable => {
            DeliveryTargetUnavailableReason::RuntimeUnavailable
        }
        DeliveryRuntimeObservationUnavailableReason::ProcessCleanupUnproven => {
            DeliveryTargetUnavailableReason::ProcessCleanupUnproven
        }
        DeliveryRuntimeObservationUnavailableReason::ReconciliationRequired => {
            DeliveryTargetUnavailableReason::ReconciliationRequired
        }
    }
}
