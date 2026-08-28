use std::sync::Arc;

use coding_agent_runtime::{
    DeliveryBranchCleanupRecoveryBindingOutcome, DeliveryDeletePendingDisposition,
    DeliveryRemovePendingDisposition, DeliveryUnlockPendingDisposition,
    DeliveryUnlockedPendingRemoveDisposition, DeliveryWorktreeCleanupError,
    DeliveryWorktreeCleanupRecoveryBindingOutcome, DeliveryWorktreeCleanupRecoveryPhase,
    SealedProcessLivenessScope,
};
use coding_agent_store::{
    ArtifactDispositionRecord, CleanupKind, CleanupOperationState, DeliveryEligibilitySnapshot,
    DeliverySourceRecord, DeliverySourceState, MergeOperationRecord, MergeOperationState,
};
use tokio_util::sync::CancellationToken;

use super::live::{persisted_source, persisted_target_for};
use super::{ProductionDeliveryRegistry, ProductionDeliverySession};
use crate::delivery_manager::{
    DeliveryCleanupRuntimeRegistrySeal, DeliveryCleanupRuntimeSessionSeal,
};
use crate::{
    DeliveryBranchCleanupBinding, DeliveryCleanupRuntimeRegistry, DeliveryCleanupRuntimeSession,
    DeliveryLiveBranchCleanupIntent, DeliveryLiveCleanupRuntimeError,
    DeliveryLiveDeletePendingCapability, DeliveryLiveDeletePendingDisposition,
    DeliveryLiveRemovePendingCapability, DeliveryLiveRuntimeError,
    DeliveryLiveUnlockPendingCapability, DeliveryLiveUnlockedPendingRemoveCapability,
    DeliveryLiveWorktreeCleanupIntent, DeliveryWorktreeCleanupBinding,
};

macro_rules! cleanup_recovery_diagnostic {
    ($predicate:literal) => {
        #[cfg(feature = "test-support")]
        eprintln!(
            "test-support production cleanup binding rejected: predicate={}",
            $predicate
        );
    };
}

impl DeliveryCleanupRuntimeRegistrySeal for ProductionDeliveryRegistry {}

struct ProductionDeliveryCleanupSession {
    session: ProductionDeliverySession,
    processes: SealedProcessLivenessScope,
}

impl DeliveryCleanupRuntimeSessionSeal for ProductionDeliveryCleanupSession {}

#[async_trait::async_trait]
impl DeliveryCleanupRuntimeRegistry for ProductionDeliveryRegistry {
    async fn open_cleanup_session(
        &self,
        snapshot: &DeliveryEligibilitySnapshot,
    ) -> Result<Arc<dyn DeliveryCleanupRuntimeSession>, DeliveryLiveCleanupRuntimeError> {
        let session = self
            .open(snapshot)
            .await
            .map_err(|_| DeliveryLiveCleanupRuntimeError::Unavailable)?;
        let processes = session
            .worker_process_scope
            .seal_task_scope(*snapshot.task.id.as_uuid().as_bytes())
            .map_err(|_| DeliveryLiveCleanupRuntimeError::ProcessCleanupUnproven)?;
        Ok(Arc::new(ProductionDeliveryCleanupSession {
            session,
            processes,
        }))
    }
}

#[async_trait::async_trait]
impl DeliveryCleanupRuntimeSession for ProductionDeliveryCleanupSession {
    async fn bind_worktree_cleanup(
        &self,
        snapshot: &DeliveryEligibilitySnapshot,
        binding: DeliveryWorktreeCleanupBinding<'_>,
    ) -> Result<DeliveryLiveWorktreeCleanupIntent, DeliveryLiveCleanupRuntimeError> {
        let graph = cleanup_graph(&self.session, snapshot)?;
        let recovery_phase = match &binding {
            DeliveryWorktreeCleanupBinding::Acceptance(_) => None,
            DeliveryWorktreeCleanupBinding::Persisted(operation) => Some(match operation.state {
                CleanupOperationState::UnlockPending => {
                    DeliveryWorktreeCleanupRecoveryPhase::UnlockPending
                }
                CleanupOperationState::UnlockedPendingRemove => {
                    DeliveryWorktreeCleanupRecoveryPhase::UnlockedPendingRemove
                }
                CleanupOperationState::RemovePending => {
                    DeliveryWorktreeCleanupRecoveryPhase::RemovePending
                }
                _ => return Err(inconsistent()),
            }),
        };
        let is_acceptance = recovery_phase.is_none();
        validate_worktree_binding(&graph, binding)?;
        let target_head = graph.merge.expected_merge_commit.as_ref().ok_or_else(|| {
            cleanup_recovery_diagnostic!("expected_merge_commit");
            inconsistent()
        })?;
        let source = persisted_source(&self.session, graph.source).map_err(|error| {
            cleanup_recovery_diagnostic!("persisted_source");
            map_live_error(error)
        })?;
        let target = persisted_target_for(graph.merge, &graph.merge.target_branch, target_head)
            .map_err(|error| {
                cleanup_recovery_diagnostic!("persisted_target");
                map_live_error(error)
            })?;
        let binding = match recovery_phase {
            Some(phase) => {
                self.session
                    .cleanup
                    .bind_persisted_delivery_worktree_cleanup(
                        phase,
                        self.session.source.as_ref(),
                        self.session.target.as_ref(),
                        &self.session.reservation,
                        &source,
                        &target,
                        &self.processes,
                        CancellationToken::new(),
                    )
                    .await
            }
            None => {
                self.session
                    .cleanup
                    .bind_delivery_worktree_cleanup_acceptance(
                        self.session.source.as_ref(),
                        self.session.target.as_ref(),
                        &self.session.reservation,
                        &source,
                        &target,
                        &self.processes,
                        CancellationToken::new(),
                    )
                    .await
            }
        }
        .map_err(|error| map_worktree_binding_error(error, is_acceptance))?;
        match binding {
            DeliveryWorktreeCleanupRecoveryBindingOutcome::Bound(intent) => {
                Ok(DeliveryLiveWorktreeCleanupIntent::from_runtime(intent))
            }
            DeliveryWorktreeCleanupRecoveryBindingOutcome::ReconciliationRequired => {
                cleanup_recovery_diagnostic!("runtime_worktree_recovery");
                Err(inconsistent())
            }
        }
    }

    async fn bind_branch_cleanup(
        &self,
        snapshot: &DeliveryEligibilitySnapshot,
        binding: DeliveryBranchCleanupBinding<'_>,
    ) -> Result<DeliveryLiveBranchCleanupIntent, DeliveryLiveCleanupRuntimeError> {
        let graph = cleanup_graph(&self.session, snapshot)?;
        let (target_branch, target_head) = validate_branch_binding(&graph, binding)?;
        let source = persisted_source(&self.session, graph.source).map_err(map_live_error)?;
        let target = persisted_target_for(graph.merge, target_branch, target_head)
            .map_err(map_live_error)?;
        match self
            .session
            .cleanup
            .bind_persisted_delivery_branch_cleanup(
                self.session.source.as_ref(),
                self.session.target.as_ref(),
                &self.session.reservation,
                &source,
                &target,
                &self.processes,
                CancellationToken::new(),
            )
            .await
            .map_err(map_cleanup_error)?
        {
            DeliveryBranchCleanupRecoveryBindingOutcome::Bound(intent) => {
                Ok(DeliveryLiveBranchCleanupIntent::from_runtime(intent))
            }
            DeliveryBranchCleanupRecoveryBindingOutcome::ReconciliationRequired => {
                Err(inconsistent())
            }
        }
    }

    async fn drive_unlock_pending(
        &self,
        capability: DeliveryLiveUnlockPendingCapability,
    ) -> Result<DeliveryUnlockPendingDisposition, DeliveryLiveCleanupRuntimeError> {
        let capability = capability.into_runtime().ok_or_else(inconsistent)?;
        let observed = self
            .session
            .cleanup
            .classify_delivery_unlock_pending(
                self.session.source.as_ref(),
                &capability,
                &self.processes,
                CancellationToken::new(),
            )
            .await
            .map_err(map_cleanup_error)?;
        if observed != DeliveryUnlockPendingDisposition::RetryExactUnlock {
            return Ok(observed);
        }
        self.session
            .cleanup
            .retry_delivery_unlock_pending(
                self.session.source.as_ref(),
                capability,
                &self.processes,
                CancellationToken::new(),
            )
            .await
            .map_err(map_cleanup_error)
    }

    async fn drive_unlocked_pending_remove(
        &self,
        capability: DeliveryLiveUnlockedPendingRemoveCapability,
    ) -> Result<DeliveryUnlockedPendingRemoveDisposition, DeliveryLiveCleanupRuntimeError> {
        let capability = capability.into_runtime().ok_or_else(inconsistent)?;
        self.session
            .cleanup
            .classify_delivery_unlocked_pending_remove(
                self.session.source.as_ref(),
                &capability,
                &self.processes,
                CancellationToken::new(),
            )
            .await
            .map_err(map_cleanup_error)
    }

    async fn drive_remove_pending(
        &self,
        capability: DeliveryLiveRemovePendingCapability,
    ) -> Result<DeliveryRemovePendingDisposition, DeliveryLiveCleanupRuntimeError> {
        let capability = capability.into_runtime().ok_or_else(inconsistent)?;
        let observed = self
            .session
            .cleanup
            .classify_delivery_remove_pending(
                self.session.source.as_ref(),
                &capability,
                &self.processes,
                CancellationToken::new(),
            )
            .await
            .map_err(map_cleanup_error)?;
        if observed != DeliveryRemovePendingDisposition::RetryExactRemove {
            return Ok(observed);
        }
        self.session
            .cleanup
            .retry_delivery_remove_pending(
                self.session.source.as_ref(),
                capability,
                &self.processes,
                CancellationToken::new(),
            )
            .await
            .map_err(map_cleanup_error)
    }

    async fn drive_delete_pending(
        &self,
        capability: DeliveryLiveDeletePendingCapability,
    ) -> Result<DeliveryLiveDeletePendingDisposition, DeliveryLiveCleanupRuntimeError> {
        let capability = capability.into_runtime().ok_or_else(inconsistent)?;
        let observed = self
            .session
            .cleanup
            .classify_delivery_delete_pending(
                &capability,
                &self.processes,
                CancellationToken::new(),
            )
            .await
            .map_err(map_cleanup_error)?;
        let disposition = match observed {
            DeliveryDeletePendingDisposition::RetryExactDelete => self
                .session
                .cleanup
                .retry_delivery_delete_pending(
                    capability,
                    &self.processes,
                    CancellationToken::new(),
                )
                .await
                .map_err(map_cleanup_error)?,
            disposition => disposition,
        };
        DeliveryLiveDeletePendingDisposition::from_runtime(disposition)
    }
}

struct CleanupGraph<'a> {
    source: &'a DeliverySourceRecord,
    merge: &'a MergeOperationRecord,
    disposition: &'a ArtifactDispositionRecord,
}

fn cleanup_graph<'a>(
    session: &ProductionDeliverySession,
    snapshot: &'a DeliveryEligibilitySnapshot,
) -> Result<CleanupGraph<'a>, DeliveryLiveCleanupRuntimeError> {
    if snapshot != &session.snapshot {
        cleanup_recovery_diagnostic!("cleanup_graph_snapshot");
        return Err(inconsistent());
    }
    let source = snapshot.ownership.source.as_ref().ok_or_else(|| {
        cleanup_recovery_diagnostic!("cleanup_graph_source");
        inconsistent()
    })?;
    let disposition = snapshot.ownership.disposition.as_ref().ok_or_else(|| {
        cleanup_recovery_diagnostic!("cleanup_graph_disposition");
        inconsistent()
    })?;
    let merge = snapshot
        .ownership
        .merge_operations
        .iter()
        .find(|operation| operation.operation_id == disposition.merged_operation_id)
        .ok_or_else(|| {
            cleanup_recovery_diagnostic!("cleanup_graph_merge");
            inconsistent()
        })?;
    let graph_is_exact = source.state == DeliverySourceState::Committed
        && source.provenance.identity == disposition.identity
        && source.provenance.identity == merge.provenance.identity
        && source.origin_accepted_operation_id == merge.operation_id
        && source.expected_source_commit.as_ref() == Some(&disposition.source_commit)
        && merge.state == MergeOperationState::Merged
        && merge.merged_disposition_task_id == Some(snapshot.task.id)
        && merge.expected_merge_commit.is_some()
        && disposition.delivery_source_task_id == snapshot.task.id;
    if !graph_is_exact {
        cleanup_recovery_diagnostic!("cleanup_graph_cross_row_ownership");
        return Err(inconsistent());
    }
    Ok(CleanupGraph {
        source,
        merge,
        disposition,
    })
}

fn validate_worktree_binding(
    graph: &CleanupGraph<'_>,
    binding: DeliveryWorktreeCleanupBinding<'_>,
) -> Result<(), DeliveryLiveCleanupRuntimeError> {
    let exact = match binding {
        DeliveryWorktreeCleanupBinding::Acceptance(command) => {
            command.task_id() == graph.source.provenance.identity.task_id()
                && command.expected_disposition_version() == graph.disposition.worktree_version
                && command.expected_merge_operation_id() == graph.merge.operation_id
                && command.expected_source_ref() == &graph.source.provenance.source_branch
                && Some(command.expected_source_oid())
                    == graph.source.expected_source_commit.as_ref()
        }
        DeliveryWorktreeCleanupBinding::Persisted(operation) => {
            operation.kind == CleanupKind::RemoveWorktree
                && operation.identity == graph.source.provenance.identity
                && operation.expected_merge_operation_id == graph.merge.operation_id
                && operation.expected_source_ref == graph.source.provenance.source_branch
                && Some(&operation.expected_source_oid)
                    == graph.source.expected_source_commit.as_ref()
                && operation.expected_common_git_identity
                    == graph.source.provenance.common_git_identity
                && operation.expected_admin_identity
                    == graph.source.provenance.worktree_admin_identity
                && operation.expected_worktree_path == graph.source.provenance.worktree_path
                && operation.expected_target_ref.is_none()
                && operation.expected_target_head.is_none()
        }
    };
    if exact {
        Ok(())
    } else {
        cleanup_recovery_diagnostic!("worktree_acceptance_binding");
        Err(inconsistent())
    }
}

fn validate_branch_binding<'a>(
    graph: &CleanupGraph<'a>,
    binding: DeliveryBranchCleanupBinding<'a>,
) -> Result<
    (
        &'a coding_agent_store::GitBranchRef,
        &'a coding_agent_store::GitCommitOid,
    ),
    DeliveryLiveCleanupRuntimeError,
> {
    match binding {
        DeliveryBranchCleanupBinding::Acceptance(command)
            if command.task_id() == graph.source.provenance.identity.task_id()
                && command.expected_disposition_version() == graph.disposition.branch_version
                && command.expected_merge_operation_id() == graph.merge.operation_id
                && command.expected_source_ref() == &graph.source.provenance.source_branch
                && Some(command.expected_source_oid())
                    == graph.source.expected_source_commit.as_ref()
                && command.target_branch() == &graph.merge.target_branch =>
        {
            Ok((command.target_branch(), command.target_head()))
        }
        DeliveryBranchCleanupBinding::Persisted(operation)
            if operation.kind == CleanupKind::DeleteBranch
                && operation.identity == graph.source.provenance.identity
                && operation.expected_merge_operation_id == graph.merge.operation_id
                && operation.expected_source_ref == graph.source.provenance.source_branch
                && Some(&operation.expected_source_oid)
                    == graph.source.expected_source_commit.as_ref()
                && operation.expected_common_git_identity
                    == graph.source.provenance.common_git_identity
                && operation.expected_admin_identity
                    == graph.source.provenance.worktree_admin_identity
                && operation.expected_worktree_path == graph.source.provenance.worktree_path
                && operation.expected_target_ref.as_ref() == Some(&graph.merge.target_branch) =>
        {
            Ok((
                operation
                    .expected_target_ref
                    .as_ref()
                    .ok_or_else(inconsistent)?,
                operation
                    .expected_target_head
                    .as_ref()
                    .ok_or_else(inconsistent)?,
            ))
        }
        _ => Err(inconsistent()),
    }
}

fn map_live_error(error: DeliveryLiveRuntimeError) -> DeliveryLiveCleanupRuntimeError {
    match error {
        DeliveryLiveRuntimeError::ProcessCleanupUnproven => {
            DeliveryLiveCleanupRuntimeError::ProcessCleanupUnproven
        }
        DeliveryLiveRuntimeError::ReconciliationRequired(_) => inconsistent(),
        DeliveryLiveRuntimeError::Unavailable => DeliveryLiveCleanupRuntimeError::Unavailable,
    }
}

fn map_cleanup_error(error: DeliveryWorktreeCleanupError) -> DeliveryLiveCleanupRuntimeError {
    use coding_agent_store::CleanupReconciliationReason;
    match error {
        DeliveryWorktreeCleanupError::ProcessStateUnproven
        | DeliveryWorktreeCleanupError::ProcessCleanupUnproven => {
            DeliveryLiveCleanupRuntimeError::ProcessCleanupUnproven
        }
        DeliveryWorktreeCleanupError::AuthenticationChanged => {
            DeliveryLiveCleanupRuntimeError::ReconciliationRequired(
                CleanupReconciliationReason::WorktreeIdentityMismatch,
            )
        }
        DeliveryWorktreeCleanupError::SourceChanged => {
            DeliveryLiveCleanupRuntimeError::ReconciliationRequired(
                CleanupReconciliationReason::SourceInconsistent,
            )
        }
        DeliveryWorktreeCleanupError::Dirty => inconsistent(),
        DeliveryWorktreeCleanupError::ChildOutcomeUnknown => inconsistent(),
        DeliveryWorktreeCleanupError::TimedOut => {
            DeliveryLiveCleanupRuntimeError::ReconciliationRequired(
                CleanupReconciliationReason::CommandTimedOut,
            )
        }
        DeliveryWorktreeCleanupError::Cancelled
        | DeliveryWorktreeCleanupError::CommandFailed
        | DeliveryWorktreeCleanupError::InvalidConfiguration
        | DeliveryWorktreeCleanupError::Internal => DeliveryLiveCleanupRuntimeError::Unavailable,
    }
}

fn map_worktree_binding_error(
    error: DeliveryWorktreeCleanupError,
    is_acceptance: bool,
) -> DeliveryLiveCleanupRuntimeError {
    if is_acceptance && error == DeliveryWorktreeCleanupError::Dirty {
        DeliveryLiveCleanupRuntimeError::TargetWorktreeDirty
    } else {
        map_cleanup_error(error)
    }
}

const fn inconsistent() -> DeliveryLiveCleanupRuntimeError {
    DeliveryLiveCleanupRuntimeError::ReconciliationRequired(
        coding_agent_store::CleanupReconciliationReason::DeliveryStateInconsistent,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unexpected_persisted_dirty_error_fails_closed_while_fresh_dirty_is_ineligible() {
        assert_eq!(
            map_worktree_binding_error(DeliveryWorktreeCleanupError::Dirty, true),
            DeliveryLiveCleanupRuntimeError::TargetWorktreeDirty,
        );
        assert_eq!(
            map_worktree_binding_error(DeliveryWorktreeCleanupError::Dirty, false),
            inconsistent(),
            "an unexpected persisted dirty binding error must fail closed",
        );
    }
}
