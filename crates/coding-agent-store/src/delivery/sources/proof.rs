use std::fmt;

use crate::delivery::{
    DeliveryCommitMetadata, DeliveryError, DirectoryIdentity, GitBranchRef, GitCommitOid,
    GitTreeOid, Sha256Digest,
};

#[derive(Clone, PartialEq, Eq)]
pub struct DeliverySourceObjectProof {
    pub(super) expected_source_commit: GitCommitOid,
    pub(super) tree: GitTreeOid,
    pub(super) parents: Vec<GitCommitOid>,
    pub(super) metadata: DeliveryCommitMetadata,
}

impl DeliverySourceObjectProof {
    pub fn try_new(
        expected_source_commit: GitCommitOid,
        tree: GitTreeOid,
        parents: Vec<GitCommitOid>,
        metadata: DeliveryCommitMetadata,
    ) -> Result<Self, DeliveryError> {
        if parents.len() != 1
            || tree.algorithm() != expected_source_commit.algorithm()
            || parents[0].algorithm() != expected_source_commit.algorithm()
        {
            return Err(DeliveryError::InvalidCommandRequest);
        }
        Ok(Self {
            expected_source_commit,
            tree,
            parents,
            metadata,
        })
    }
}

impl fmt::Debug for DeliverySourceObjectProof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliverySourceObjectProof")
            .field("object_shape", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SourceWorktreeProof {
    pub(super) index_tree: GitTreeOid,
    pub(super) worktree_tree: GitTreeOid,
    pub(super) staged_entry_count: u32,
    pub(super) unstaged_entry_count: u32,
    pub(super) untracked_entry_count: u32,
    pub(super) unmerged_entry_count: u32,
}

impl SourceWorktreeProof {
    pub fn try_new(
        index_tree: GitTreeOid,
        worktree_tree: GitTreeOid,
        staged_entry_count: u32,
        unstaged_entry_count: u32,
        untracked_entry_count: u32,
        unmerged_entry_count: u32,
    ) -> Result<Self, DeliveryError> {
        if index_tree.algorithm() != worktree_tree.algorithm() {
            return Err(DeliveryError::InvalidCommandRequest);
        }
        Ok(Self {
            index_tree,
            worktree_tree,
            staged_entry_count,
            unstaged_entry_count,
            untracked_entry_count,
            unmerged_entry_count,
        })
    }
}

impl fmt::Debug for SourceWorktreeProof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SourceWorktreeProof")
            .field("repository_observation", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct DeliverySourceAppliedProof {
    pub(super) object: DeliverySourceObjectProof,
    pub(super) symbolic_source_ref: GitBranchRef,
    pub(super) source_ref_oid: GitCommitOid,
    pub(super) head_oid: GitCommitOid,
    pub(super) worktree: SourceWorktreeProof,
    pub(super) common_git_identity: DirectoryIdentity,
    pub(super) worktree_admin_identity: DirectoryIdentity,
    pub(super) fixed_lock_reason: String,
    pub(super) config_attributes_digest: Sha256Digest,
}

impl DeliverySourceAppliedProof {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        object: DeliverySourceObjectProof,
        symbolic_source_ref: GitBranchRef,
        source_ref_oid: GitCommitOid,
        head_oid: GitCommitOid,
        worktree: SourceWorktreeProof,
        common_git_identity: DirectoryIdentity,
        worktree_admin_identity: DirectoryIdentity,
        fixed_lock_reason: String,
        config_attributes_digest: Sha256Digest,
    ) -> Result<Self, DeliveryError> {
        let algorithm = object.expected_source_commit.algorithm();
        if source_ref_oid.algorithm() != algorithm
            || head_oid.algorithm() != algorithm
            || worktree.index_tree.algorithm() != algorithm
        {
            return Err(DeliveryError::InvalidCommandRequest);
        }
        Ok(Self {
            object,
            symbolic_source_ref,
            source_ref_oid,
            head_oid,
            worktree,
            common_git_identity,
            worktree_admin_identity,
            fixed_lock_reason,
            config_attributes_digest,
        })
    }
}

impl fmt::Debug for DeliverySourceAppliedProof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliverySourceAppliedProof")
            .field("source_application", &"<redacted>")
            .finish()
    }
}
