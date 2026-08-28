use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

use crate::delivery_api_projection::{
    DeliveryPreflightOutcome, DeliveryPreflightUnavailableReason,
};
use crate::{
    RepositoryControlCoordinator, RepositoryControlLease, ServiceState, ServiceStateController,
    ServiceStateSnapshot,
};

use super::command::{
    DeliveryManagerCommand, DeliveryWorkerCompletion, DeliveryWorkerRetainedOwnership,
    DeliveryWorkerRetention,
};
use super::{DeliveryIntakeGate, DeliveryManagerBackend, DeliveryPreflightRequest};

mod admission;
mod eligibility;
mod persist;
mod routing;
mod runtime;

// This is the actor-to-worker ownership handoff; keeping each guard, gate and
// response channel explicit makes omissions visible at the safety boundary.
#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_preflight_worker(
    worker_id: u64,
    global_git_operations: Arc<Semaphore>,
    repository_control: Arc<RepositoryControlCoordinator>,
    intake_gate: Arc<DeliveryIntakeGate>,
    service_state: ServiceStateController,
    backend: DeliveryManagerBackend,
    service: ServiceStateSnapshot,
    request: DeliveryPreflightRequest,
    completion_sender: mpsc::Sender<DeliveryManagerCommand>,
    response: oneshot::Sender<DeliveryPreflightOutcome>,
) {
    tokio::spawn(async move {
        // Keep the completion sender outside the inner task so a runtime fake,
        // adapter, or future production session panic cannot strand the actor's
        // worker accounting permanently.
        let execution = tokio::spawn(async move {
            match backend {
                DeliveryManagerBackend::Unavailable => LivePreflightCompletion::released(
                    unavailable_backend_outcome(service, intake_gate.as_ref()),
                ),
                DeliveryManagerBackend::Live(dependencies) => {
                    admission::run_live_preflight(
                        dependencies,
                        global_git_operations,
                        repository_control,
                        intake_gate,
                        service_state,
                        request.into_command(),
                    )
                    .await
                }
            }
        })
        .await;
        let completion = match execution {
            Ok(completion) => completion,
            Err(_) => LivePreflightCompletion::released(DeliveryPreflightOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::OrchestrationUnavailable,
            )),
        };
        let (outcome, retention) = match completion {
            LivePreflightCompletion::Released(outcome) => {
                (outcome, DeliveryWorkerRetention::Released)
            }
            LivePreflightCompletion::Retained {
                outcome,
                global_permit,
                repository_lease,
            } => (
                outcome,
                DeliveryWorkerRetention::RetainedFailClosed(DeliveryWorkerRetainedOwnership::new(
                    global_permit,
                    repository_lease,
                )),
            ),
        };
        let _ = completion_sender
            .send(DeliveryManagerCommand::WorkerCompleted {
                worker_id,
                completion: Box::new(DeliveryWorkerCompletion::Preflight {
                    outcome,
                    retention,
                    response,
                }),
            })
            .await;
    });
}

fn unavailable_backend_outcome(
    service: ServiceStateSnapshot,
    intake_gate: &DeliveryIntakeGate,
) -> DeliveryPreflightOutcome {
    if intake_gate.snapshot().0 || service.state == ServiceState::Quiescing {
        DeliveryPreflightOutcome::Unavailable(DeliveryPreflightUnavailableReason::ManagerQuiescing)
    } else if service.state != ServiceState::Ready {
        DeliveryPreflightOutcome::Unavailable(DeliveryPreflightUnavailableReason::ServiceNotReady)
    } else {
        DeliveryPreflightOutcome::Unavailable(
            DeliveryPreflightUnavailableReason::OrchestrationUnavailable,
        )
    }
}

enum LivePreflightCompletion {
    Released(DeliveryPreflightOutcome),
    Retained {
        outcome: DeliveryPreflightOutcome,
        global_permit: OwnedSemaphorePermit,
        repository_lease: RepositoryControlLease,
    },
}

impl LivePreflightCompletion {
    fn released(outcome: DeliveryPreflightOutcome) -> Self {
        Self::Released(outcome)
    }

    fn retained(
        outcome: DeliveryPreflightOutcome,
        permit: OwnedSemaphorePermit,
        repository_lease: RepositoryControlLease,
    ) -> Self {
        Self::Retained {
            outcome,
            global_permit: permit,
            repository_lease,
        }
    }
}
