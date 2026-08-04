use std::fmt;

use uuid::Uuid;

use crate::delivery::{DeliveryError, DirectoryIdentity, GitBranchRef, GitCommitOid, Sha256Digest};

use super::{MergeAutostashObservation, OtherGitOperationObservation};

#[derive(Clone, PartialEq, Eq)]
pub struct MergeAbortProof {
    pub(in crate::delivery::merges) child_receipt_id: Uuid,
    pub(in crate::delivery::merges) target_branch: GitBranchRef,
    pub(in crate::delivery::merges) target_head: GitCommitOid,
    pub(in crate::delivery::merges) source_branch: GitBranchRef,
    pub(in crate::delivery::merges) source_oid: GitCommitOid,
    pub(in crate::delivery::merges) merge_head: GitCommitOid,
    pub(in crate::delivery::merges) common_git_identity: DirectoryIdentity,
    pub(in crate::delivery::merges) worktree_admin_identity: DirectoryIdentity,
    pub(in crate::delivery::merges) fixed_lock_reason: String,
    pub(in crate::delivery::merges) config_attributes_digest: Sha256Digest,
    pub(in crate::delivery::merges) index_stages_digest: Sha256Digest,
    pub(in crate::delivery::merges) worktree_digest: Sha256Digest,
}

impl MergeAbortProof {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        child_receipt_id: Uuid,
        target_branch: GitBranchRef,
        target_head: GitCommitOid,
        source_branch: GitBranchRef,
        source_oid: GitCommitOid,
        merge_head: GitCommitOid,
        common_git_identity: DirectoryIdentity,
        worktree_admin_identity: DirectoryIdentity,
        fixed_lock_reason: String,
        config_attributes_digest: Sha256Digest,
        index_stages_digest: Sha256Digest,
        worktree_digest: Sha256Digest,
        merge_autostash: MergeAutostashObservation,
        other_git_operation: OtherGitOperationObservation,
    ) -> Result<Self, DeliveryError> {
        if child_receipt_id.is_nil()
            || target_head.algorithm() != merge_head.algorithm()
            || source_oid != merge_head
            || fixed_lock_reason != "codex-reserved"
            || merge_autostash != MergeAutostashObservation::Absent
            || other_git_operation != OtherGitOperationObservation::Clear
        {
            return Err(DeliveryError::InvalidCommandRequest);
        }
        Ok(Self {
            child_receipt_id,
            target_branch,
            target_head,
            source_branch,
            source_oid,
            merge_head,
            common_git_identity,
            worktree_admin_identity,
            fixed_lock_reason,
            config_attributes_digest,
            index_stages_digest,
            worktree_digest,
        })
    }
}

impl fmt::Debug for MergeAbortProof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MergeAbortProof")
            .field("conflict_observation", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct MergeAbortAppliedProof {
    pub(in crate::delivery::merges) target_branch: GitBranchRef,
    pub(in crate::delivery::merges) target_head: GitCommitOid,
    pub(in crate::delivery::merges) source_branch: GitBranchRef,
    pub(in crate::delivery::merges) source_oid: GitCommitOid,
    pub(in crate::delivery::merges) common_git_identity: DirectoryIdentity,
    pub(in crate::delivery::merges) worktree_admin_identity: DirectoryIdentity,
    pub(in crate::delivery::merges) fixed_lock_reason: String,
    pub(in crate::delivery::merges) config_attributes_digest: Sha256Digest,
}

impl MergeAbortAppliedProof {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        target_branch: GitBranchRef,
        target_head: GitCommitOid,
        source_branch: GitBranchRef,
        source_oid: GitCommitOid,
        common_git_identity: DirectoryIdentity,
        worktree_admin_identity: DirectoryIdentity,
        fixed_lock_reason: String,
        config_attributes_digest: Sha256Digest,
        staged_entry_count: u32,
        unstaged_entry_count: u32,
        untracked_entry_count: u32,
        unmerged_entry_count: u32,
        merge_head: Option<GitCommitOid>,
        merge_autostash: MergeAutostashObservation,
        git_operation_state: OtherGitOperationObservation,
    ) -> Result<Self, DeliveryError> {
        if target_head.algorithm() != source_oid.algorithm()
            || staged_entry_count != 0
            || unstaged_entry_count != 0
            || untracked_entry_count != 0
            || unmerged_entry_count != 0
            || merge_head.is_some()
            || fixed_lock_reason != "codex-reserved"
            || merge_autostash != MergeAutostashObservation::Absent
            || git_operation_state != OtherGitOperationObservation::Clear
        {
            return Err(DeliveryError::InvalidCommandRequest);
        }
        Ok(Self {
            target_branch,
            target_head,
            source_branch,
            source_oid,
            common_git_identity,
            worktree_admin_identity,
            fixed_lock_reason,
            config_attributes_digest,
        })
    }
}

impl fmt::Debug for MergeAbortAppliedProof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MergeAbortAppliedProof")
            .field("postcondition", &"<redacted>")
            .finish()
    }
}
