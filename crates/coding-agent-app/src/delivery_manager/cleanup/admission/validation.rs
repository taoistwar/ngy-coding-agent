use super::*;

pub(super) struct ValidatedCleanupAcceptance {
    pub(super) repository_id: coding_agent_domain::RepositoryId,
}

pub(super) async fn load_cleanup_acceptance_snapshot(
    dependencies: &DeliveryManagerLiveDependencies,
    task_id: coding_agent_domain::TaskId,
) -> Result<DeliveryEligibilitySnapshot, DeliveryCleanupAcceptanceOutcome> {
    match timeout(
        STORE_READ_TIMEOUT,
        dependencies.store.delivery_eligibility_snapshot(task_id),
    )
    .await
    {
        Ok(Ok(Some(snapshot))) => Ok(snapshot),
        Ok(Ok(None)) => Err(DeliveryCleanupAcceptanceOutcome::Ineligible(vec![
            DeliveryEligibilityReason::TaskNotFound,
        ])),
        Ok(Err(_)) | Err(_) => Err(DeliveryCleanupAcceptanceOutcome::Unavailable(
            DeliveryPreflightUnavailableReason::StoreUnavailable,
        )),
    }
}

pub(super) fn validate_cleanup_acceptance(
    snapshot: &DeliveryEligibilitySnapshot,
    command: &CleanupCommand,
) -> Result<ValidatedCleanupAcceptance, DeliveryCleanupAcceptanceOutcome> {
    let Some(source) = snapshot.ownership.source.as_ref() else {
        return Err(DeliveryCleanupAcceptanceOutcome::Conflict(
            DeliveryCommandConflict::ArtifactCleanupNotAllowed,
        ));
    };
    let Some(disposition) = snapshot.ownership.disposition.as_ref() else {
        return Err(DeliveryCleanupAcceptanceOutcome::Conflict(
            DeliveryCommandConflict::ArtifactCleanupNotAllowed,
        ));
    };
    let Some(merge) = snapshot
        .ownership
        .merge_operations
        .iter()
        .find(|operation| operation.operation_id == disposition.merged_operation_id)
    else {
        return Err(inconsistent_cleanup_outcome());
    };
    if snapshot.task.id != command.task_id()
        || source.provenance.identity.task_id() != snapshot.task.id
        || source.provenance.identity.repository_id() != snapshot.task.repository_id
        || source.provenance.identity.attempt() != snapshot.task.attempt
        || source.state != DeliverySourceState::Committed
        || source.expected_source_commit.as_ref() != Some(&disposition.source_commit)
        || merge.provenance.identity != source.provenance.identity
        || merge.state != coding_agent_store::MergeOperationState::Merged
        || disposition.identity != source.provenance.identity
    {
        return Err(inconsistent_cleanup_outcome());
    }
    if snapshot
        .ownership
        .cleanup_operations
        .iter()
        .any(|operation| {
            operation.state.is_side_effect_active() || operation.state.is_reconciliation()
        })
    {
        return Err(DeliveryCleanupAcceptanceOutcome::Conflict(
            DeliveryCommandConflict::OperationInProgress,
        ));
    }
    match command {
        CleanupCommand::RemoveWorktree(command) => {
            validate_worktree_cleanup_anchors(snapshot, source, disposition, command)?
        }
        CleanupCommand::DeleteBranch(command) => {
            validate_branch_cleanup_anchors(source, disposition, merge, command)?
        }
    }
    Ok(ValidatedCleanupAcceptance {
        repository_id: snapshot.task.repository_id,
    })
}

fn validate_worktree_cleanup_anchors(
    snapshot: &DeliveryEligibilitySnapshot,
    source: &coding_agent_store::DeliverySourceRecord,
    disposition: &coding_agent_store::ArtifactDispositionRecord,
    command: &RemoveWorktreeCommandRequest,
) -> Result<(), DeliveryCleanupAcceptanceOutcome> {
    if source.provenance.source_branch != *command.expected_source_ref()
        || disposition.source_commit != *command.expected_source_oid()
    {
        return Err(DeliveryCleanupAcceptanceOutcome::Conflict(
            DeliveryCommandConflict::WorktreeIdentityMismatch,
        ));
    }
    if disposition.merged_operation_id != command.expected_merge_operation_id()
        || disposition.worktree_version != command.expected_disposition_version()
        || disposition.branch_state != BranchDisposition::Retained
    {
        return Err(DeliveryCleanupAcceptanceOutcome::Conflict(
            DeliveryCommandConflict::ArtifactCleanupNotAllowed,
        ));
    }
    let state_is_eligible =
        match disposition.worktree_state {
            WorktreeDisposition::RetainedLocked => true,
            WorktreeDisposition::RetainedUnlocked => snapshot
                .ownership
                .cleanup_operations
                .iter()
                .any(|operation| {
                    operation.kind == CleanupKind::RemoveWorktree
                        && operation.state == CleanupOperationState::Failed
                        && operation.expected_disposition_version == disposition.worktree_version
                }),
            WorktreeDisposition::Removed | WorktreeDisposition::ReconciliationRequired => false,
        };
    if !state_is_eligible {
        return Err(DeliveryCleanupAcceptanceOutcome::Conflict(
            DeliveryCommandConflict::ArtifactCleanupNotAllowed,
        ));
    }
    Ok(())
}

fn validate_branch_cleanup_anchors(
    source: &coding_agent_store::DeliverySourceRecord,
    disposition: &coding_agent_store::ArtifactDispositionRecord,
    merge: &coding_agent_store::MergeOperationRecord,
    command: &DeleteBranchCommandRequest,
) -> Result<(), DeliveryCleanupAcceptanceOutcome> {
    if source.provenance.source_branch != *command.expected_source_ref()
        || disposition.source_commit != *command.expected_source_oid()
    {
        return Err(DeliveryCleanupAcceptanceOutcome::Conflict(
            DeliveryCommandConflict::WorktreeIdentityMismatch,
        ));
    }
    if merge.target_branch != *command.target_branch()
        || command.expected_source_ref() == command.target_branch()
    {
        return Err(DeliveryCleanupAcceptanceOutcome::Conflict(
            DeliveryCommandConflict::TargetBranchMismatch,
        ));
    }
    if merge.expected_merge_commit.as_ref() != Some(command.target_head()) {
        return Err(DeliveryCleanupAcceptanceOutcome::Conflict(
            DeliveryCommandConflict::TargetHeadChanged,
        ));
    }
    if disposition.merged_operation_id != command.expected_merge_operation_id()
        || disposition.branch_version != command.expected_disposition_version()
        || disposition.worktree_state != WorktreeDisposition::Removed
        || disposition.branch_state != BranchDisposition::Retained
    {
        return Err(DeliveryCleanupAcceptanceOutcome::Conflict(
            DeliveryCommandConflict::ArtifactCleanupNotAllowed,
        ));
    }
    Ok(())
}
