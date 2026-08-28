use crate::{DeliveryQueryUnavailableReason, DeliveryTaskQueryOutcome, ServiceStateSnapshot};
use coding_agent_domain::TaskId;
use tokio::sync::{mpsc, oneshot};

use super::command::{DeliveryManagerCommand, DeliveryWorkerCompletion};
use super::{DeliveryManagerBackend, DeliveryManagerLiveDependencies};

mod decision;
mod observations;
mod projection;

pub(super) use decision::persistent_reasons;

pub(super) fn spawn_query_worker(
    worker_id: u64,
    backend: DeliveryManagerBackend,
    service: ServiceStateSnapshot,
    task_id: TaskId,
    completion_sender: mpsc::Sender<DeliveryManagerCommand>,
    response: oneshot::Sender<DeliveryTaskQueryOutcome>,
) {
    tokio::spawn(async move {
        let execution = tokio::spawn(async move {
            match backend {
                DeliveryManagerBackend::Unavailable => unavailable_outcome(task_id),
                DeliveryManagerBackend::Live(dependencies) => {
                    query_live(dependencies.as_ref(), service, task_id).await
                }
            }
        })
        .await;
        let outcome = execution.unwrap_or_else(|_| unavailable_outcome(task_id));
        let _ = completion_sender
            .send(DeliveryManagerCommand::WorkerCompleted {
                worker_id,
                completion: Box::new(DeliveryWorkerCompletion::Query {
                    outcome: Box::new(outcome),
                    response,
                }),
            })
            .await;
    });
}

fn unavailable_outcome(task_id: TaskId) -> DeliveryTaskQueryOutcome {
    DeliveryTaskQueryOutcome::unavailable(
        task_id,
        DeliveryQueryUnavailableReason::OrchestrationUnavailable,
    )
}

async fn query_live(
    dependencies: &DeliveryManagerLiveDependencies,
    service: ServiceStateSnapshot,
    task_id: TaskId,
) -> DeliveryTaskQueryOutcome {
    // This is the sole Store snapshot used by one task GET projection. Every
    // process/runtime/coordinator observation below is joined to this exact
    // task/repository/attempt tuple. Exact operation-id GET uses its separate
    // audited query port and never scans this projection.
    let snapshot = match observations::load_snapshot(dependencies, task_id).await {
        observations::SnapshotObservation::Found(snapshot) => snapshot,
        observations::SnapshotObservation::NotFound => {
            return DeliveryTaskQueryOutcome::not_found(task_id);
        }
        observations::SnapshotObservation::Unavailable => {
            return DeliveryTaskQueryOutcome::unavailable(
                task_id,
                DeliveryQueryUnavailableReason::StoreUnavailable,
            );
        }
    };
    let observations = observations::collect(
        dependencies,
        service,
        task_id,
        &snapshot,
        persistent_reasons(&snapshot),
    )
    .await;
    let decision = decision::build(&snapshot, observations.ineligible, observations.unavailable);
    projection::task_outcome(task_id, &snapshot, decision, observations.target)
}
