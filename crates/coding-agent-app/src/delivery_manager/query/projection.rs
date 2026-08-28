use coding_agent_domain::TaskId;
use coding_agent_store::{
    ArtifactDispositionRecord, BranchDisposition, DeliveryEligibilitySnapshot,
    DeliverySourceRecord, DeliverySourceState, WorktreeDisposition,
};

use crate::delivery_api_projection::{
    DeliveryProjectionDecision, DeliveryTaskProjectionContext, project_delivery_task_with_context,
};
use crate::{
    DeliveryArtifactDispositionProjection, DeliveryBranchDispositionState,
    DeliveryEvidenceProjection, DeliveryOperationProjection, DeliverySourceProjection,
    DeliverySourceProjectionState, DeliveryTargetObservation, DeliveryTaskQueryOutcome,
    DeliveryWorktreeDispositionState,
};

use super::super::operation_query::{
    project_cleanup_operation, project_cleanup_operation_details, project_merge_operation,
    project_merge_operation_details,
};
use super::decision::{latest_cleanup_operation, latest_merge_operation};

pub(super) fn task_outcome(
    task_id: TaskId,
    snapshot: &DeliveryEligibilitySnapshot,
    decision: DeliveryProjectionDecision,
    target: DeliveryTargetObservation,
) -> DeliveryTaskQueryOutcome {
    let latest_merge = latest_merge_operation(snapshot).map(project_merge_operation_details);
    let latest_cleanup = latest_cleanup_operation(snapshot).map(project_cleanup_operation_details);
    let latest_operation = latest_operation_projection(snapshot);
    let evidence = snapshot.evidence_identity.as_ref().map(|identity| {
        DeliveryEvidenceProjection::new(
            identity.workspace_generation(),
            identity.workspace_fingerprint().clone(),
        )
    });
    let source = snapshot.ownership.source.as_ref().map(project_source);
    let disposition = snapshot
        .ownership
        .disposition
        .as_ref()
        .and_then(|disposition| {
            snapshot
                .ownership
                .source
                .as_ref()
                .map(|source| project_disposition(disposition, source))
        });
    DeliveryTaskQueryOutcome::found(project_delivery_task_with_context(
        task_id,
        decision,
        DeliveryTaskProjectionContext {
            latest_operation,
            target,
            evidence,
            source,
            latest_merge,
            latest_cleanup,
            disposition,
        },
    ))
}

fn latest_operation_projection(
    snapshot: &DeliveryEligibilitySnapshot,
) -> Option<DeliveryOperationProjection> {
    let merge = latest_merge_operation(snapshot);
    let cleanup = latest_cleanup_operation(snapshot);
    match (merge, cleanup) {
        (Some(merge), Some(cleanup))
            if cleanup.initial_transition_id > merge.initial_transition_id =>
        {
            Some(project_cleanup_operation(cleanup))
        }
        (Some(merge), _) => Some(project_merge_operation(merge)),
        (None, Some(cleanup)) => Some(project_cleanup_operation(cleanup)),
        (None, None) => None,
    }
}

fn project_source(source: &DeliverySourceRecord) -> DeliverySourceProjection {
    DeliverySourceProjection::new(
        match source.state {
            DeliverySourceState::ObjectPending => DeliverySourceProjectionState::ObjectPending,
            DeliverySourceState::CommitPending => DeliverySourceProjectionState::CommitPending,
            DeliverySourceState::Committed => DeliverySourceProjectionState::Committed,
            DeliverySourceState::ReconciliationRequired => {
                DeliverySourceProjectionState::ReconciliationRequired
            }
        },
        source.version,
        source.provenance.source_branch.clone(),
        source.expected_source_commit.clone(),
    )
}

fn project_disposition(
    disposition: &ArtifactDispositionRecord,
    source: &DeliverySourceRecord,
) -> DeliveryArtifactDispositionProjection {
    DeliveryArtifactDispositionProjection::new(
        disposition.merged_operation_id,
        source.provenance.source_branch.clone(),
        disposition.source_commit.clone(),
        match disposition.worktree_state {
            WorktreeDisposition::RetainedLocked => DeliveryWorktreeDispositionState::RetainedLocked,
            WorktreeDisposition::RetainedUnlocked => {
                DeliveryWorktreeDispositionState::RetainedUnlocked
            }
            WorktreeDisposition::Removed => DeliveryWorktreeDispositionState::Removed,
            WorktreeDisposition::ReconciliationRequired => {
                DeliveryWorktreeDispositionState::ReconciliationRequired
            }
        },
        disposition.worktree_version,
        disposition
            .worktree_failure_code
            .as_ref()
            .map(|failure| failure.as_str().to_owned()),
        match disposition.branch_state {
            BranchDisposition::Retained => DeliveryBranchDispositionState::Retained,
            BranchDisposition::Deleted => DeliveryBranchDispositionState::Deleted,
            BranchDisposition::ReconciliationRequired => {
                DeliveryBranchDispositionState::ReconciliationRequired
            }
        },
        disposition.branch_version,
        disposition
            .branch_failure_code
            .as_ref()
            .map(|failure| failure.as_str().to_owned()),
    )
}
