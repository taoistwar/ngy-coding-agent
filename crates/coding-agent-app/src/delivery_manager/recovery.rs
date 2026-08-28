use std::sync::Arc;
use std::time::Duration;

use coding_agent_store::{
    ArtifactDispositionRecord, CleanupKind, CleanupOperationRecord, CleanupOperationState,
    DeliveryEligibilitySnapshot, DeliveryOperationId, DeliveryOperationSnapshot,
    DeliverySourceRecord, DeliverySourceState, MergeOperationRecord, MergeOperationState,
    StoreError,
};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::time::{Instant, timeout};

use crate::{DeliveryDisposition, DeliveryWriteCommand, DeliveryWriteOutcome, StoreWriterHandle};

use super::command::{DeliveryManagerCommand, DeliveryWorkerCompletion, DeliveryWorkerRetention};
use super::{
    DeliveryManagerBackend, DeliveryManagerLiveDependencies, DeliveryOperationRecoveryOutcome,
};

pub(super) const STORE_READ_TIMEOUT: Duration = Duration::from_secs(30);
pub(super) const STORE_WRITE_TIMEOUT: Duration = Duration::from_secs(30);
pub(super) const LIVE_ORCHESTRATION_TIMEOUT: Duration = Duration::from_secs(30);
// Production runtime children enforce a ten-minute deadline and then prove
// process-tree cleanup. Keep the actor's outer runtime budget above that
// bound so it cannot erase the runtime's typed outcome by cancelling early.
pub(super) const LIVE_RUNTIME_STAGE_TIMEOUT: Duration = Duration::from_secs(11 * 60);
pub(super) const LIVE_RETRY_DELAY: Duration = Duration::from_millis(100);
pub(super) const MAX_LIVE_ATTEMPTS: usize = 3;
const MAX_EXACT_RECONCILIATION_ATTEMPTS: usize = 3;

pub(super) fn spawn_operation_recovery_worker(
    worker_id: u64,
    global_git_operations: Arc<Semaphore>,
    repository_control: Arc<crate::RepositoryControlCoordinator>,
    backend: DeliveryManagerBackend,
    operation_id: DeliveryOperationId,
    completion_sender: mpsc::Sender<DeliveryManagerCommand>,
    response: oneshot::Sender<DeliveryOperationRecoveryOutcome>,
) {
    tokio::spawn(async move {
        let kind = match &backend {
            DeliveryManagerBackend::Unavailable => None,
            DeliveryManagerBackend::Live(dependencies) => match timeout(
                STORE_READ_TIMEOUT,
                dependencies.store.delivery_operation_snapshot(operation_id),
            )
            .await
            {
                Ok(Ok(Some(DeliveryOperationSnapshot::Merge(_)))) => Some(false),
                Ok(Ok(Some(DeliveryOperationSnapshot::Cleanup(_)))) => Some(true),
                Ok(Ok(None)) => {
                    send_recovery_completion(
                        worker_id,
                        DeliveryOperationRecoveryOutcome::NotFound,
                        completion_sender,
                        response,
                    )
                    .await;
                    return;
                }
                Ok(Err(_)) | Err(_) => None,
            },
        };
        match kind {
            Some(false) => super::merge::spawn_recovery_worker(
                worker_id,
                global_git_operations,
                repository_control,
                backend,
                operation_id,
                completion_sender,
                response,
            ),
            Some(true) => super::cleanup::spawn_recovery_worker(
                worker_id,
                global_git_operations,
                repository_control,
                backend,
                operation_id,
                completion_sender,
                response,
            ),
            None => {
                send_recovery_completion(
                    worker_id,
                    DeliveryOperationRecoveryOutcome::Unavailable,
                    completion_sender,
                    response,
                )
                .await;
            }
        }
    });
}

async fn send_recovery_completion(
    worker_id: u64,
    outcome: DeliveryOperationRecoveryOutcome,
    completion_sender: mpsc::Sender<DeliveryManagerCommand>,
    response: oneshot::Sender<DeliveryOperationRecoveryOutcome>,
) {
    let _ = completion_sender
        .send(DeliveryManagerCommand::WorkerCompleted {
            worker_id,
            completion: Box::new(DeliveryWorkerCompletion::Recovery {
                outcome,
                retention: DeliveryWorkerRetention::Released,
                response,
            }),
        })
        .await;
}

#[derive(Debug, Clone)]
pub(super) struct DeliveryRecoveryContext {
    pub(super) snapshot: DeliveryEligibilitySnapshot,
    pub(super) operation: MergeOperationRecord,
    pub(super) source: Option<DeliverySourceRecord>,
}

#[derive(Debug, Clone)]
pub(super) struct DeliveryCleanupRecoveryContext {
    pub(super) snapshot: DeliveryEligibilitySnapshot,
    pub(super) operation: CleanupOperationRecord,
    pub(super) disposition: ArtifactDispositionRecord,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RecoveryLoadError {
    NotFound,
    Unavailable,
    Inconsistent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LiveStageOutcome {
    Continue,
    /// Retry the exact live stage, but retain repository ownership if the
    /// bounded live retry budget is exhausted after an applied side effect.
    RetryThenRetain,
    /// Retry the exact live stage, then release when no child or repository
    /// side effect needs ownership beyond the bounded retry budget.
    Retry,
    Finished,
    Release,
    Retain,
    Poison,
}

pub(super) async fn load_operation_context(
    dependencies: &DeliveryManagerLiveDependencies,
    operation_id: DeliveryOperationId,
) -> Result<DeliveryRecoveryContext, RecoveryLoadError> {
    let operation = match timeout(
        STORE_READ_TIMEOUT,
        dependencies.store.delivery_operation_snapshot(operation_id),
    )
    .await
    {
        Ok(Ok(Some(DeliveryOperationSnapshot::Merge(operation)))) => *operation,
        Ok(Ok(Some(DeliveryOperationSnapshot::Cleanup(_)))) => {
            return Err(RecoveryLoadError::Inconsistent);
        }
        Ok(Ok(None)) => return Err(RecoveryLoadError::NotFound),
        Ok(Err(_)) | Err(_) => return Err(RecoveryLoadError::Unavailable),
    };
    let snapshot = match timeout(
        STORE_READ_TIMEOUT,
        dependencies
            .store
            .delivery_eligibility_snapshot(operation.provenance.identity.task_id()),
    )
    .await
    {
        Ok(Ok(Some(snapshot))) => snapshot,
        Ok(Ok(None)) => return Err(RecoveryLoadError::Inconsistent),
        Ok(Err(_)) | Err(_) => return Err(RecoveryLoadError::Unavailable),
    };
    let Some(snapshot_operation) = snapshot
        .ownership
        .merge_operations
        .iter()
        .find(|candidate| candidate.operation_id == operation_id)
        .cloned()
    else {
        return Err(RecoveryLoadError::Inconsistent);
    };
    if snapshot_operation != operation
        || snapshot.task.id != operation.provenance.identity.task_id()
        || snapshot.task.repository_id != operation.provenance.identity.repository_id()
        || snapshot.task.attempt != operation.provenance.identity.attempt()
    {
        return Err(RecoveryLoadError::Inconsistent);
    }
    let source = snapshot.ownership.source.clone();
    if source.as_ref().is_some_and(|source| {
        source.provenance.identity != operation.provenance.identity
            || source.origin_accepted_operation_id != operation.operation_id
    }) {
        return Err(RecoveryLoadError::Inconsistent);
    }
    Ok(DeliveryRecoveryContext {
        snapshot,
        operation,
        source,
    })
}

pub(super) async fn load_cleanup_operation_context(
    dependencies: &DeliveryManagerLiveDependencies,
    operation_id: DeliveryOperationId,
) -> Result<DeliveryCleanupRecoveryContext, RecoveryLoadError> {
    let operation = match timeout(
        STORE_READ_TIMEOUT,
        dependencies.store.delivery_operation_snapshot(operation_id),
    )
    .await
    {
        Ok(Ok(Some(DeliveryOperationSnapshot::Cleanup(operation)))) => *operation,
        Ok(Ok(Some(DeliveryOperationSnapshot::Merge(_)))) => {
            return Err(RecoveryLoadError::Inconsistent);
        }
        Ok(Ok(None)) => return Err(RecoveryLoadError::NotFound),
        Ok(Err(_)) | Err(_) => return Err(RecoveryLoadError::Unavailable),
    };
    let snapshot = match timeout(
        STORE_READ_TIMEOUT,
        dependencies
            .store
            .delivery_eligibility_snapshot(operation.identity.task_id()),
    )
    .await
    {
        Ok(Ok(Some(snapshot))) => snapshot,
        Ok(Ok(None)) => return Err(RecoveryLoadError::Inconsistent),
        Ok(Err(_)) | Err(_) => return Err(RecoveryLoadError::Unavailable),
    };
    let Some(snapshot_operation) = snapshot
        .ownership
        .cleanup_operations
        .iter()
        .find(|candidate| candidate.operation_id == operation_id)
        .cloned()
    else {
        return Err(RecoveryLoadError::Inconsistent);
    };
    let Some(source) = snapshot.ownership.source.clone() else {
        return Err(RecoveryLoadError::Inconsistent);
    };
    let Some(disposition) = snapshot.ownership.disposition.clone() else {
        return Err(RecoveryLoadError::Inconsistent);
    };
    let Some(merge) = snapshot
        .ownership
        .merge_operations
        .iter()
        .find(|candidate| candidate.operation_id == disposition.merged_operation_id)
        .cloned()
    else {
        return Err(RecoveryLoadError::Inconsistent);
    };
    if snapshot_operation != operation
        || snapshot.task.id != operation.identity.task_id()
        || snapshot.task.repository_id != operation.identity.repository_id()
        || snapshot.task.attempt != operation.identity.attempt()
        || source.provenance.identity != operation.identity
        || source.state != DeliverySourceState::Committed
        || source.expected_source_commit.as_ref() != Some(&operation.expected_source_oid)
        || source.provenance.source_branch != operation.expected_source_ref
        || source.provenance.worktree_path != operation.expected_worktree_path
        || source.provenance.worktree_admin_identity != operation.expected_admin_identity
        || source.provenance.common_git_identity != operation.expected_common_git_identity
        || operation.expected_merge_operation_id != merge.operation_id
        || source.origin_accepted_operation_id != merge.operation_id
        || disposition.identity != operation.identity
        || disposition.source_commit != operation.expected_source_oid
        || merge.provenance.identity != operation.identity
        || merge.state != MergeOperationState::Merged
        || merge.operation_id != disposition.merged_operation_id
        || (operation.kind == CleanupKind::DeleteBranch
            && operation.expected_target_ref.as_ref() != Some(&merge.target_branch))
        || !cleanup_fact_mapping_is_exact(&operation, &disposition)
    {
        return Err(RecoveryLoadError::Inconsistent);
    }
    Ok(DeliveryCleanupRecoveryContext {
        snapshot,
        operation,
        disposition,
    })
}

fn cleanup_fact_mapping_is_exact(
    operation: &CleanupOperationRecord,
    disposition: &ArtifactDispositionRecord,
) -> bool {
    if operation.disposition_task_id != operation.identity.task_id() {
        return false;
    }
    match operation.kind {
        CleanupKind::RemoveWorktree => {
            operation.expected_target_ref.is_none()
                && operation.expected_target_head.is_none()
                && operation.origin_target_head.is_none()
                && operation.expected_disposition_version == disposition.worktree_version
                && match operation.state {
                    CleanupOperationState::UnlockPending => {
                        disposition.worktree_state
                            == coding_agent_store::WorktreeDisposition::RetainedLocked
                            && operation.failure_code.is_none()
                    }
                    CleanupOperationState::UnlockedPendingRemove
                    | CleanupOperationState::RemovePending => {
                        disposition.worktree_state
                            == coding_agent_store::WorktreeDisposition::RetainedUnlocked
                            && operation.failure_code.is_none()
                    }
                    CleanupOperationState::Completed => {
                        disposition.worktree_state
                            == coding_agent_store::WorktreeDisposition::Removed
                            && operation.failure_code.is_none()
                    }
                    CleanupOperationState::Failed => {
                        matches!(
                            disposition.worktree_state,
                            coding_agent_store::WorktreeDisposition::RetainedLocked
                                | coding_agent_store::WorktreeDisposition::RetainedUnlocked
                        ) && operation.failure_code.is_some()
                    }
                    CleanupOperationState::ReconciliationRequired => {
                        disposition.worktree_state
                            == coding_agent_store::WorktreeDisposition::ReconciliationRequired
                            && operation.failure_code.is_some()
                    }
                    CleanupOperationState::DeletePending => false,
                }
        }
        CleanupKind::DeleteBranch => {
            operation.expected_target_ref.is_some()
                && operation.expected_target_head.is_some()
                && operation.origin_target_head.is_some()
                && operation.expected_disposition_version == disposition.branch_version
                && disposition.worktree_state == coding_agent_store::WorktreeDisposition::Removed
                && match operation.state {
                    CleanupOperationState::DeletePending => {
                        disposition.branch_state == coding_agent_store::BranchDisposition::Retained
                            && operation.failure_code.is_none()
                    }
                    CleanupOperationState::Completed => {
                        disposition.branch_state == coding_agent_store::BranchDisposition::Deleted
                            && operation.failure_code.is_none()
                    }
                    CleanupOperationState::Failed => {
                        disposition.branch_state == coding_agent_store::BranchDisposition::Retained
                            && operation.failure_code.is_some()
                    }
                    CleanupOperationState::ReconciliationRequired => {
                        disposition.branch_state
                            == coding_agent_store::BranchDisposition::ReconciliationRequired
                            && operation.failure_code.is_some()
                    }
                    CleanupOperationState::UnlockPending
                    | CleanupOperationState::UnlockedPendingRemove
                    | CleanupOperationState::RemovePending => false,
                }
        }
    }
}

// This short-lived envelope is consumed immediately after one StoreWriter
// completion. Boxing the confirmed typed outcome would add an allocation to
// every delivery transition without reducing retained actor state.
#[allow(clippy::large_enum_variant)]
pub(super) enum ExactDeliveryWriteResult {
    Confirmed(DeliveryWriteOutcome),
    KnownNotApplied {
        reason: crate::KnownNotAppliedReason,
        error: Option<StoreError>,
    },
    OutcomeUnknown,
    InvariantConflict,
}

/// Replays only the exact typed StoreWriter command retained by an unknown
/// receipt. A channel close is never interpreted as rollback.
pub(super) async fn execute_exact_delivery_write(
    writer: &StoreWriterHandle,
    initial_command: DeliveryWriteCommand,
) -> ExactDeliveryWriteResult {
    let mut command = initial_command;
    let mut reconciliation_lane = false;
    let mut observed_unknown = false;
    for _ in 0..=MAX_EXACT_RECONCILIATION_ATTEMPTS {
        let exact_command = command.clone();
        let submission = if reconciliation_lane {
            writer.reconcile_delivery(command, Instant::now() + STORE_WRITE_TIMEOUT)
        } else {
            writer.submit_delivery(command, Instant::now() + STORE_WRITE_TIMEOUT)
        };
        let completion = match timeout(STORE_WRITE_TIMEOUT, submission.completion()).await {
            Ok(completion) => completion,
            Err(_) => {
                observed_unknown = true;
                command = exact_command;
                reconciliation_lane = true;
                continue;
            }
        };
        match completion.disposition {
            DeliveryDisposition::Confirmed(outcome) => {
                return ExactDeliveryWriteResult::Confirmed(outcome);
            }
            DeliveryDisposition::KnownNotApplied {
                reason,
                outcome: None,
                error,
            } if !observed_unknown => {
                return ExactDeliveryWriteResult::KnownNotApplied { reason, error };
            }
            DeliveryDisposition::KnownNotApplied { outcome: None, .. } => {
                command = exact_command;
                reconciliation_lane = true;
            }
            DeliveryDisposition::OutcomeUnknown {
                command: replay, ..
            } => {
                observed_unknown = true;
                command = replay;
                reconciliation_lane = true;
            }
            DeliveryDisposition::KnownNotApplied {
                outcome: Some(_), ..
            }
            | DeliveryDisposition::InvariantConflict { .. } => {
                return ExactDeliveryWriteResult::InvariantConflict;
            }
        }
    }
    ExactDeliveryWriteResult::OutcomeUnknown
}
