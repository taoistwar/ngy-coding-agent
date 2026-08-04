mod abort;
mod accept;
mod conflicts;
mod merged;
mod model;
mod pending;
mod preflight;
mod preflight_result;
mod proof;
mod replay;
mod terminal;

pub use model::{
    AcceptMergeOutcome, BeginMergeAbortRequest, CompleteMergeAbortRequest, CompleteMergeRequest,
    EnterMergePendingRequest, MergeConflictPaths, MergeKnownNotAppliedReason, MergePreflightResult,
    MergeReconciliationReason, MergeTransitionOutcome, MergeTransitionReceipt,
    PreflightRejectedReason, ReconcileMergeRequest, RecordMergeKnownFailureRequest,
    RecordMergePreflightResultRequest,
};
pub(in crate::delivery) use model::{merge_failure_code_is_valid, raw_relative_path_is_canonical};
pub use proof::{
    MergeAbortAppliedProof, MergeAbortProof, MergeAppliedProof, MergeAutostashObservation,
    MergeCommitObjectProof, OtherGitOperationObservation,
};
pub(in crate::delivery) use replay::{OperationLookup, load_operation_for_caller};

use std::fmt;

use super::{
    DeliveryError, DirectoryIdentity, GitObjectAlgorithm, GitTreeOid, PreflightCommandRequest,
    Sha256Digest,
};
use crate::StoreError;

#[derive(Clone, PartialEq, Eq)]
pub struct CreatePreflightRequest {
    command: PreflightCommandRequest,
    candidate_tree: GitTreeOid,
    preflight_source_commit: super::GitCommitOid,
    common_git_identity: DirectoryIdentity,
    worktree_admin_identity: DirectoryIdentity,
    config_attributes_digest: Sha256Digest,
}

impl fmt::Debug for CreatePreflightRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreatePreflightRequest")
            .field("task_id", &self.command.task_id())
            .field("request_and_repository_values", &"<redacted>")
            .finish()
    }
}

impl CreatePreflightRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        command: PreflightCommandRequest,
        candidate_tree: GitTreeOid,
        preflight_source_commit: super::GitCommitOid,
        common_git_identity: DirectoryIdentity,
        worktree_admin_identity: DirectoryIdentity,
        config_attributes_digest: Sha256Digest,
    ) -> Result<Self, DeliveryError> {
        let algorithm = command.expected_target_head().algorithm();
        if candidate_tree.algorithm() != algorithm
            || preflight_source_commit.algorithm() != algorithm
        {
            return Err(DeliveryError::InvalidCommandRequest);
        }
        Ok(Self {
            command,
            candidate_tree,
            preflight_source_commit,
            common_git_identity,
            worktree_admin_identity,
            config_attributes_digest,
        })
    }

    pub const fn command(&self) -> &PreflightCommandRequest {
        &self.command
    }

    pub const fn candidate_tree(&self) -> &GitTreeOid {
        &self.candidate_tree
    }

    pub const fn preflight_source_commit(&self) -> &super::GitCommitOid {
        &self.preflight_source_commit
    }

    pub const fn common_git_identity(&self) -> &DirectoryIdentity {
        &self.common_git_identity
    }

    pub const fn worktree_admin_identity(&self) -> &DirectoryIdentity {
        &self.worktree_admin_identity
    }

    pub const fn config_attributes_digest(&self) -> &Sha256Digest {
        &self.config_attributes_digest
    }

    pub const fn object_algorithm(&self) -> GitObjectAlgorithm {
        self.candidate_tree.algorithm()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreatePreflightOutcome {
    Created(super::DeliveryCommandReceipt),
    Existing(super::DeliveryCommandReceipt),
}

fn invalid_preflight_request() -> StoreError {
    StoreError::Delivery(DeliveryError::InvalidCommandRequest)
}

const MERGE_INVARIANT: &str = "delivery merge operation is inconsistent";

fn merge_invariant() -> StoreError {
    StoreError::InvariantViolation(MERGE_INVARIANT)
}
