use std::time::Duration;

use coding_agent_store::{
    CleanupKind, CleanupOperationRecord, CleanupOperationState, DeliveryOperationId,
    DeliveryOperationSnapshot, MergeConflictPathEncoding, MergeOperationRecord,
    MergeOperationState, Store,
};
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

use crate::{
    DeliveryCleanupOperationKind, DeliveryCleanupOperationProjection,
    DeliveryCleanupOperationState, DeliveryConflictPathEncoding, DeliveryConflictPathProjection,
    DeliveryConflictSummaryProjection, DeliveryMergeOperationProjection,
    DeliveryOperationProjection, DeliveryOperationQueryOutcome, DeliveryPreflightState,
    DeliveryQueryUnavailableReason,
};

use super::DeliveryManagerBackend;
use super::command::{DeliveryManagerCommand, DeliveryWorkerCompletion};

mod sealed {
    pub trait OperationQuery {}
}

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub trait DeliveryOperationQueryTestSeam {}

#[cfg(feature = "test-support")]
impl<T: DeliveryOperationQueryTestSeam> sealed::OperationQuery for T {}

#[async_trait::async_trait]
pub trait DeliveryOperationQuery: sealed::OperationQuery + Send + Sync + 'static {
    async fn lookup(&self, operation_id: DeliveryOperationId) -> DeliveryOperationQueryOutcome;
}

pub(super) struct StoreDeliveryOperationQuery {
    store: Store,
}

impl StoreDeliveryOperationQuery {
    pub(super) fn new(store: Store) -> Self {
        Self { store }
    }
}

impl sealed::OperationQuery for StoreDeliveryOperationQuery {}

#[async_trait::async_trait]
impl DeliveryOperationQuery for StoreDeliveryOperationQuery {
    async fn lookup(&self, operation_id: DeliveryOperationId) -> DeliveryOperationQueryOutcome {
        match self.store.delivery_operation_snapshot(operation_id).await {
            Ok(Some(snapshot)) if snapshot.operation_id() == operation_id => {
                DeliveryOperationQueryOutcome::found(project_operation(snapshot))
            }
            Ok(Some(_)) => DeliveryOperationQueryOutcome::unavailable(
                operation_id,
                DeliveryQueryUnavailableReason::StoreUnavailable,
            ),
            Ok(None) => DeliveryOperationQueryOutcome::not_found(operation_id),
            Err(_) => DeliveryOperationQueryOutcome::unavailable(
                operation_id,
                DeliveryQueryUnavailableReason::StoreUnavailable,
            ),
        }
    }
}

const OPERATION_QUERY_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) fn spawn_operation_query_worker(
    worker_id: u64,
    backend: DeliveryManagerBackend,
    operation_id: DeliveryOperationId,
    completion_sender: mpsc::Sender<DeliveryManagerCommand>,
    response: oneshot::Sender<DeliveryOperationQueryOutcome>,
) {
    tokio::spawn(async move {
        let execution = tokio::spawn(async move {
            match backend {
                DeliveryManagerBackend::Unavailable => DeliveryOperationQueryOutcome::unavailable(
                    operation_id,
                    DeliveryQueryUnavailableReason::OrchestrationUnavailable,
                ),
                DeliveryManagerBackend::Live(dependencies) => {
                    match timeout(
                        OPERATION_QUERY_TIMEOUT,
                        dependencies.operation_query.lookup(operation_id),
                    )
                    .await
                    {
                        Ok(outcome) if outcome_matches_request(&outcome, operation_id) => outcome,
                        Ok(_) | Err(_) => DeliveryOperationQueryOutcome::unavailable(
                            operation_id,
                            DeliveryQueryUnavailableReason::OrchestrationUnavailable,
                        ),
                    }
                }
            }
        })
        .await;
        let outcome = execution.unwrap_or_else(|_| {
            DeliveryOperationQueryOutcome::unavailable(
                operation_id,
                DeliveryQueryUnavailableReason::OrchestrationUnavailable,
            )
        });
        let _ = completion_sender
            .send(DeliveryManagerCommand::WorkerCompleted {
                worker_id,
                completion: Box::new(DeliveryWorkerCompletion::OperationQuery {
                    outcome,
                    response,
                }),
            })
            .await;
    });
}

fn outcome_matches_request(
    outcome: &DeliveryOperationQueryOutcome,
    operation_id: DeliveryOperationId,
) -> bool {
    match outcome {
        DeliveryOperationQueryOutcome::Found { operation } => {
            operation.operation_id() == operation_id
        }
        DeliveryOperationQueryOutcome::NotFound {
            operation_id: returned,
        }
        | DeliveryOperationQueryOutcome::Unavailable {
            operation_id: returned,
            ..
        } => *returned == operation_id,
    }
}

fn project_operation(snapshot: DeliveryOperationSnapshot) -> DeliveryOperationProjection {
    match snapshot {
        DeliveryOperationSnapshot::Merge(operation) => project_merge_operation(operation.as_ref()),
        DeliveryOperationSnapshot::Cleanup(operation) => {
            project_cleanup_operation(operation.as_ref())
        }
    }
}

pub(super) fn project_merge_operation(
    operation: &MergeOperationRecord,
) -> DeliveryOperationProjection {
    DeliveryOperationProjection::merge_detailed(project_merge_operation_details(operation))
}

pub(super) fn project_merge_operation_details(
    operation: &MergeOperationRecord,
) -> DeliveryMergeOperationProjection {
    let conflicts = operation.conflict_path_count.map(|path_count| {
        let paths = operation
            .conflicts
            .iter()
            .map(|conflict| {
                DeliveryConflictPathProjection::new(
                    match conflict.path_encoding {
                        MergeConflictPathEncoding::Utf8 => DeliveryConflictPathEncoding::Utf8,
                        MergeConflictPathEncoding::Base64Url => {
                            DeliveryConflictPathEncoding::Base64url
                        }
                    },
                    conflict.path_value.clone(),
                )
            })
            .collect::<Vec<_>>();
        let payload_bytes = operation
            .conflicts
            .iter()
            .map(|conflict| conflict.path_value.len())
            .sum::<usize>();
        DeliveryConflictSummaryProjection::new(
            u32::from(path_count),
            paths,
            u32::try_from(payload_bytes).expect("audited conflict payload is API bounded"),
            usize::from(path_count) > operation.conflicts.len(),
        )
    });
    DeliveryMergeOperationProjection::new(
        operation.operation_id,
        operation.provenance.identity.task_id(),
        operation.version,
        project_merge_state(operation.state),
        operation.provenance.evidence.workspace_generation(),
        operation
            .provenance
            .evidence
            .workspace_fingerprint()
            .clone(),
        operation
            .preflight_inputs
            .as_ref()
            .map(|inputs| inputs.candidate_tree.clone()),
        operation
            .preflight_inputs
            .as_ref()
            .map(|inputs| inputs.preflight_source_commit.clone()),
        operation.source_commit.clone(),
        operation.target_branch.clone(),
        operation.expected_target_head.clone(),
        conflicts,
        operation
            .failure_code
            .as_ref()
            .map(|failure| failure.as_str().to_owned()),
    )
}

pub(super) fn project_cleanup_operation(
    operation: &CleanupOperationRecord,
) -> DeliveryOperationProjection {
    DeliveryOperationProjection::cleanup_detailed(project_cleanup_operation_details(operation))
}

pub(super) fn project_cleanup_operation_details(
    operation: &CleanupOperationRecord,
) -> DeliveryCleanupOperationProjection {
    DeliveryCleanupOperationProjection::new(
        operation.operation_id,
        operation.identity.task_id(),
        project_cleanup_kind(operation.kind),
        operation.version,
        project_cleanup_state(operation.state),
        operation.expected_disposition_version,
        operation.expected_merge_operation_id,
        operation.expected_source_ref.clone(),
        operation.expected_source_oid.clone(),
        operation.expected_target_ref.clone(),
        operation.expected_target_head.clone(),
        operation
            .failure_code
            .as_ref()
            .map(|failure| failure.as_str().to_owned()),
    )
}

const fn project_cleanup_kind(kind: CleanupKind) -> DeliveryCleanupOperationKind {
    match kind {
        CleanupKind::RemoveWorktree => DeliveryCleanupOperationKind::RemoveWorktree,
        CleanupKind::DeleteBranch => DeliveryCleanupOperationKind::DeleteBranch,
    }
}

const fn project_cleanup_state(state: CleanupOperationState) -> DeliveryCleanupOperationState {
    match state {
        CleanupOperationState::UnlockPending => DeliveryCleanupOperationState::UnlockPending,
        CleanupOperationState::UnlockedPendingRemove => {
            DeliveryCleanupOperationState::UnlockedPendingRemove
        }
        CleanupOperationState::RemovePending => DeliveryCleanupOperationState::RemovePending,
        CleanupOperationState::DeletePending => DeliveryCleanupOperationState::DeletePending,
        CleanupOperationState::Completed => DeliveryCleanupOperationState::Completed,
        CleanupOperationState::Failed => DeliveryCleanupOperationState::Failed,
        CleanupOperationState::ReconciliationRequired => {
            DeliveryCleanupOperationState::ReconciliationRequired
        }
    }
}

pub(super) const fn project_merge_state(state: MergeOperationState) -> DeliveryPreflightState {
    match state {
        MergeOperationState::PreflightPending => DeliveryPreflightState::PreflightPending,
        MergeOperationState::PreflightReady => DeliveryPreflightState::PreflightReady,
        MergeOperationState::Conflict => DeliveryPreflightState::Conflict,
        MergeOperationState::Rejected => DeliveryPreflightState::Rejected,
        MergeOperationState::Stale => DeliveryPreflightState::Stale,
        MergeOperationState::Superseded => DeliveryPreflightState::Superseded,
        MergeOperationState::Accepted => DeliveryPreflightState::Accepted,
        MergeOperationState::MergePending => DeliveryPreflightState::MergePending,
        MergeOperationState::Merged => DeliveryPreflightState::Merged,
        MergeOperationState::AbortPending => DeliveryPreflightState::AbortPending,
        MergeOperationState::Failed => DeliveryPreflightState::Failed,
        MergeOperationState::ReconciliationRequired => {
            DeliveryPreflightState::ReconciliationRequired
        }
    }
}
