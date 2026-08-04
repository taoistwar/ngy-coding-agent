use std::fmt;

use crate::delivery::{
    DeliveryError, DirectoryIdentity, GitBranchRef, GitCommitOid, GitTreeOid, Sha256Digest,
};

use super::{MergeAutostashObservation, MergeCommitObjectProof, OtherGitOperationObservation};

#[derive(Clone, PartialEq, Eq)]
pub struct MergeAppliedProof {
    pub(in crate::delivery::merges) object: MergeCommitObjectProof,
    pub(in crate::delivery::merges) target_branch: GitBranchRef,
    pub(in crate::delivery::merges) target_head: GitCommitOid,
    pub(in crate::delivery::merges) source_branch: GitBranchRef,
    pub(in crate::delivery::merges) source_oid: GitCommitOid,
    pub(in crate::delivery::merges) common_git_identity: DirectoryIdentity,
    pub(in crate::delivery::merges) worktree_admin_identity: DirectoryIdentity,
    pub(in crate::delivery::merges) fixed_lock_reason: String,
    pub(in crate::delivery::merges) config_attributes_digest: Sha256Digest,
    pub(in crate::delivery::merges) index_tree: GitTreeOid,
    pub(in crate::delivery::merges) worktree_tree: GitTreeOid,
}

impl MergeAppliedProof {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        object: MergeCommitObjectProof,
        target_branch: GitBranchRef,
        target_head: GitCommitOid,
        source_branch: GitBranchRef,
        source_oid: GitCommitOid,
        common_git_identity: DirectoryIdentity,
        worktree_admin_identity: DirectoryIdentity,
        fixed_lock_reason: String,
        config_attributes_digest: Sha256Digest,
        index_tree: GitTreeOid,
        worktree_tree: GitTreeOid,
        staged_entry_count: u32,
        unstaged_entry_count: u32,
        untracked_entry_count: u32,
        unmerged_entry_count: u32,
        merge_head: Option<GitCommitOid>,
        merge_autostash: MergeAutostashObservation,
        git_operation_state: OtherGitOperationObservation,
    ) -> Result<Self, DeliveryError> {
        let algorithm = object.expected_merge_commit.algorithm();
        if target_head.algorithm() != algorithm
            || source_oid.algorithm() != algorithm
            || index_tree.algorithm() != algorithm
            || worktree_tree.algorithm() != algorithm
            || index_tree != object.tree
            || worktree_tree != object.tree
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
            object,
            target_branch,
            target_head,
            source_branch,
            source_oid,
            common_git_identity,
            worktree_admin_identity,
            fixed_lock_reason,
            config_attributes_digest,
            index_tree,
            worktree_tree,
        })
    }
}

impl fmt::Debug for MergeAppliedProof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MergeAppliedProof")
            .field("merge_postcondition", &"<redacted>")
            .finish()
    }
}
