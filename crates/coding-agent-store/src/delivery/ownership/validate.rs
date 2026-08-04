use coding_agent_domain::{Task, TaskId};
use sqlx::SqliteConnection;

use crate::{StoreError, TaskAttemptArtifact};

use super::super::{
    ArtifactDispositionRecord, CleanupKind, CleanupOperationRecord, DeliveryArtifactProvenance,
    DeliveryIdentity, DeliverySourceRecord, DeliverySourceState, EvidenceIdentityV1, GitBranchRef,
    GitCommitOid, MergeOperationRecord, MergeOperationState, validate_cleanup_state,
    validate_merge_source_state,
};
use super::cleanup::{validate_cleanup_history, validate_cleanup_origin};
use super::decode::{parse_branch_state, parse_value, parse_worktree_state};
use super::ownership_invariant;
use super::transitions::{disposition_state_at, source_state_at};
use super::{
    reconciliation_accept_origin_is_exact, validate_source_merge_reconciliation_pair,
    validate_source_origin,
};

pub(super) fn validate_artifact_parent(
    task: &Task,
    artifact: &TaskAttemptArtifact,
) -> Result<(), StoreError> {
    let identity_matches = artifact.identity.task_id == task.id
        && artifact.identity.repository_id == task.repository_id
        && artifact.identity.attempt == task.attempt;
    if !identity_matches {
        return Err(ownership_invariant());
    }
    if artifact.state == crate::AttemptArtifactState::Ready {
        normalized_artifact_ref(&artifact.branch_name)?;
        parse_value::<GitCommitOid>(artifact.base_commit.clone())?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn validate_ownership_graph(
    connection: &mut SqliteConnection,
    task: &Task,
    expected_evidence: &EvidenceIdentityV1,
    artifact: &TaskAttemptArtifact,
    source: Option<&DeliverySourceRecord>,
    merge_operations: &[MergeOperationRecord],
    disposition: Option<&ArtifactDispositionRecord>,
    cleanup_operations: &[CleanupOperationRecord],
) -> Result<(), StoreError> {
    validate_merge_slot_exclusivity(merge_operations)?;
    let identity = DeliveryIdentity::try_new(task.id, task.repository_id, task.attempt)
        .map_err(|_| ownership_invariant())?;
    let artifact_ref = normalized_artifact_ref(&artifact.branch_name)?;
    let artifact_base: GitCommitOid = parse_value(artifact.base_commit.clone())?;
    if let Some(source) = source {
        validate_source_origin(&mut *connection, source, merge_operations).await?;
        validate_provenance(
            &source.provenance,
            identity,
            expected_evidence,
            artifact,
            &artifact_ref,
            &artifact_base,
        )?;
        if source.expected_parent != artifact_base {
            return Err(ownership_invariant());
        }
    }
    for operation in merge_operations {
        validate_provenance(
            &operation.provenance,
            identity,
            expected_evidence,
            artifact,
            &artifact_ref,
            &artifact_base,
        )?;
        if let Some(source) = source
            && (operation.provenance != source.provenance
                || operation.candidate_tree != source.candidate_tree)
        {
            return Err(ownership_invariant());
        }
        let source_state =
            source_state_at(&mut *connection, task.id, operation.current_transition_id).await?;
        validate_merge_source_state(operation.state, source_state)
            .map_err(|_| ownership_invariant())?;
        validate_merge_source_link(task.id, source, operation)?;
    }
    if let Some(source) = source
        && source.state == DeliverySourceState::ReconciliationRequired
    {
        let mut paired = merge_operations
            .iter()
            .filter(|operation| operation.state == MergeOperationState::ReconciliationRequired);
        let operation = paired.next().ok_or_else(ownership_invariant)?;
        if paired.next().is_some() {
            return Err(ownership_invariant());
        }
        validate_source_merge_reconciliation_pair(&mut *connection, source, operation).await?;
        if !reconciliation_accept_origin_is_exact(&mut *connection, operation).await? {
            return Err(ownership_invariant());
        }
    }
    validate_disposition_graph(identity, source, merge_operations, disposition)?;
    for cleanup in cleanup_operations {
        validate_cleanup_graph(
            &mut *connection,
            identity,
            source,
            merge_operations,
            disposition,
            cleanup,
        )
        .await?;
    }
    validate_disposition_operation_pointers(&mut *connection, disposition, cleanup_operations).await
}

pub(super) fn validate_merge_slot_exclusivity(
    merge_operations: &[MergeOperationRecord],
) -> Result<(), StoreError> {
    let active = merge_operations
        .iter()
        .filter(|operation| {
            matches!(
                operation.state,
                MergeOperationState::PreflightPending
                    | MergeOperationState::PreflightReady
                    | MergeOperationState::Accepted
                    | MergeOperationState::MergePending
                    | MergeOperationState::AbortPending
            )
        })
        .count();
    let merged = merge_operations
        .iter()
        .filter(|operation| operation.state == MergeOperationState::Merged)
        .count();
    let reconciliation = merge_operations
        .iter()
        .filter(|operation| operation.state == MergeOperationState::ReconciliationRequired)
        .count();
    let occupied_slots =
        usize::from(active > 0) + usize::from(merged > 0) + usize::from(reconciliation > 0);
    if active <= 1 && merged <= 1 && reconciliation <= 1 && occupied_slots <= 1 {
        Ok(())
    } else {
        Err(ownership_invariant())
    }
}

fn normalized_artifact_ref(branch_name: &str) -> Result<GitBranchRef, StoreError> {
    if branch_name.is_empty()
        || branch_name.starts_with('/')
        || branch_name.starts_with("refs/")
        || branch_name.contains("//")
    {
        return Err(ownership_invariant());
    }
    format!("refs/heads/{branch_name}")
        .parse()
        .map_err(|_| ownership_invariant())
}

fn validate_provenance(
    provenance: &DeliveryArtifactProvenance,
    identity: DeliveryIdentity,
    expected_evidence: &EvidenceIdentityV1,
    artifact: &TaskAttemptArtifact,
    artifact_ref: &GitBranchRef,
    artifact_base: &GitCommitOid,
) -> Result<(), StoreError> {
    let valid = provenance.identity == identity
        && provenance.evidence == *expected_evidence
        && provenance.base_commit == *artifact_base
        && provenance.source_branch == *artifact_ref
        && provenance.worktree_path == artifact.worktree_path
        && provenance.fixed_lock_reason == "codex-reserved";
    if valid {
        Ok(())
    } else {
        Err(ownership_invariant())
    }
}

fn validate_merge_source_link(
    task_id: TaskId,
    source: Option<&DeliverySourceRecord>,
    operation: &MergeOperationRecord,
) -> Result<(), StoreError> {
    match (
        operation.delivery_source_task_id,
        operation.source_commit.as_ref(),
    ) {
        (None, None) => Ok(()),
        (Some(link), Some(commit)) => {
            let source = source.ok_or_else(ownership_invariant)?;
            if link == task_id
                && source.provenance.identity.task_id() == link
                && source.expected_source_commit.as_ref() == Some(commit)
            {
                Ok(())
            } else {
                Err(ownership_invariant())
            }
        }
        _ => Err(ownership_invariant()),
    }
}

fn validate_disposition_graph(
    identity: DeliveryIdentity,
    source: Option<&DeliverySourceRecord>,
    merge_operations: &[MergeOperationRecord],
    disposition: Option<&ArtifactDispositionRecord>,
) -> Result<(), StoreError> {
    let Some(disposition) = disposition else {
        if merge_operations
            .iter()
            .any(|operation| operation.state == MergeOperationState::Merged)
        {
            return Err(ownership_invariant());
        }
        return Ok(());
    };
    if disposition.identity != identity || disposition.delivery_source_task_id != identity.task_id()
    {
        return Err(ownership_invariant());
    }
    let source = source.ok_or_else(ownership_invariant)?;
    if source.state != DeliverySourceState::Committed
        || source.expected_source_commit.as_ref() != Some(&disposition.source_commit)
    {
        return Err(ownership_invariant());
    }
    let merged = merge_operations
        .iter()
        .find(|operation| operation.operation_id == disposition.merged_operation_id)
        .ok_or_else(ownership_invariant)?;
    if merged.state != MergeOperationState::Merged
        || merged.merged_disposition_task_id != Some(identity.task_id())
        || merged.source_commit.as_ref() != Some(&disposition.source_commit)
    {
        return Err(ownership_invariant());
    }
    Ok(())
}

async fn validate_cleanup_graph(
    connection: &mut SqliteConnection,
    identity: DeliveryIdentity,
    source: Option<&DeliverySourceRecord>,
    merge_operations: &[MergeOperationRecord],
    disposition: Option<&ArtifactDispositionRecord>,
    cleanup: &CleanupOperationRecord,
) -> Result<(), StoreError> {
    let source = source.ok_or_else(ownership_invariant)?;
    let disposition = disposition.ok_or_else(ownership_invariant)?;
    let merged = merge_operations
        .iter()
        .find(|operation| operation.operation_id == disposition.merged_operation_id)
        .ok_or_else(ownership_invariant)?;
    validate_cleanup_history(&mut *connection, cleanup).await?;
    validate_cleanup_origin(&mut *connection, cleanup, disposition, merged).await?;
    if cleanup.identity != identity
        || cleanup.disposition_task_id != identity.task_id()
        || cleanup.expected_worktree_path != source.provenance.worktree_path
        || cleanup.expected_admin_identity != source.provenance.worktree_admin_identity
        || cleanup.expected_common_git_identity != source.provenance.common_git_identity
        || cleanup.expected_source_ref != source.provenance.source_branch
        || source.expected_source_commit.as_ref() != Some(&cleanup.expected_source_oid)
    {
        return Err(ownership_invariant());
    }
    match cleanup.kind {
        CleanupKind::RemoveWorktree => {
            if cleanup.expected_target_ref.is_some() || cleanup.expected_target_head.is_some() {
                return Err(ownership_invariant());
            }
        }
        CleanupKind::DeleteBranch => {
            // A fresh target head can legally advance after merge. Store binds the
            // canonical observation while runtime proves ancestry and freshness.
            if cleanup.expected_target_ref.as_ref() != Some(&merged.target_branch)
                || cleanup.expected_target_head.is_none()
            {
                return Err(ownership_invariant());
            }
        }
    }
    let worktree = disposition_state_at(
        &mut *connection,
        "worktree_disposition",
        identity.task_id(),
        cleanup.current_transition_id,
    )
    .await?;
    let branch = disposition_state_at(
        &mut *connection,
        "branch_disposition",
        identity.task_id(),
        cleanup.current_transition_id,
    )
    .await?;
    let worktree_state = parse_worktree_state(worktree.1)?;
    let branch_state = parse_branch_state(branch.1)?;
    let affected_version = match cleanup.kind {
        CleanupKind::RemoveWorktree => worktree.0,
        CleanupKind::DeleteBranch => branch.0,
    };
    if affected_version != cleanup.expected_disposition_version {
        return Err(ownership_invariant());
    }
    validate_cleanup_state(cleanup.kind, cleanup.state, worktree_state, branch_state)
        .map_err(|_| ownership_invariant())
}

async fn validate_disposition_operation_pointers(
    connection: &mut SqliteConnection,
    disposition: Option<&ArtifactDispositionRecord>,
    cleanups: &[CleanupOperationRecord],
) -> Result<(), StoreError> {
    let Some(disposition) = disposition else {
        return Ok(());
    };
    for (id, version, state, kind) in [
        (
            disposition.worktree_cleanup_operation_id,
            disposition.worktree_cleanup_operation_version,
            disposition.worktree_cleanup_operation_state,
            CleanupKind::RemoveWorktree,
        ),
        (
            disposition.branch_cleanup_operation_id,
            disposition.branch_cleanup_operation_version,
            disposition.branch_cleanup_operation_state,
            CleanupKind::DeleteBranch,
        ),
    ] {
        match (id, version, state) {
            (None, None, None) => {}
            (Some(id), Some(version), Some(state)) => {
                let operation = cleanups
                    .iter()
                    .find(|operation| operation.operation_id == id)
                    .ok_or_else(ownership_invariant)?;
                if operation.kind != kind || operation.version.get() < version.get() {
                    return Err(ownership_invariant());
                }
                let transition: Option<(i64, String)> = sqlx::query_as(
                    "SELECT transition_id, to_state \
                     FROM task_delivery_operation_transitions \
                     WHERE entity_kind = 'cleanup_operation' AND entity_id = ? \
                       AND entity_version = ?",
                )
                .bind(id.to_string())
                .bind(i64::try_from(version.get()).map_err(|_| ownership_invariant())?)
                .fetch_optional(&mut *connection)
                .await?;
                let (cleanup_transition_id, transition_state) =
                    transition.ok_or_else(ownership_invariant)?;
                if transition_state != state.as_str()
                    || (operation.version == version && operation.state != state)
                {
                    return Err(ownership_invariant());
                }
                let (axis_kind, disposition_transition_id, disposition_version, disposition_state) =
                    match kind {
                        CleanupKind::RemoveWorktree => (
                            "worktree_disposition",
                            disposition.worktree_current_transition_id,
                            disposition.worktree_version,
                            disposition.worktree_state.as_str(),
                        ),
                        CleanupKind::DeleteBranch => (
                            "branch_disposition",
                            disposition.branch_current_transition_id,
                            disposition.branch_version,
                            disposition.branch_state.as_str(),
                        ),
                    };
                let historical = disposition_state_at(
                    &mut *connection,
                    axis_kind,
                    disposition.identity.task_id(),
                    cleanup_transition_id,
                )
                .await?;
                if disposition_transition_id >= cleanup_transition_id
                    || historical.0 != disposition_version
                    || historical.1 != disposition_state
                {
                    return Err(ownership_invariant());
                }
            }
            _ => return Err(ownership_invariant()),
        }
    }
    Ok(())
}
