use std::str::FromStr;
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use coding_agent_runtime::{
    DeliveryAbortAppliedPersistenceBinding, DeliveryAbortPersistenceBinding,
    DeliveryCommitPersistenceMetadata, DeliveryConflictPathEncoding as RuntimeConflictPathEncoding,
    DeliveryExpectedMergePersistenceBinding, DeliveryMergeAppliedPersistenceBinding,
    DeliverySourceAppliedPersistenceBinding, DeliverySourceObjectPersistenceBinding,
};
use coding_agent_store::{
    AcceptMergeCommandRequest, DeliveryCommitMetadata, DeliveryEligibilitySnapshot,
    DeliverySourceAppliedProof, DeliverySourceObjectProof, DeliverySourceRecord,
    DeliverySourceRetryReason, DirectoryIdentity, GitBranchRef, GitCommitOid, GitTreeOid,
    MergeAbortAppliedProof, MergeAbortProof, MergeAppliedProof, MergeAutostashObservation,
    MergeCommitObjectProof, MergeConflictPaths, MergeKnownNotAppliedReason, MergeOperationRecord,
    MergeReconciliationReason, OtherGitOperationObservation, PreflightRejectedReason,
    PreflightStaleReason, Sha256Digest, SourceWorktreeProof,
};
use uuid::Uuid;

use super::runtime::DeliveryRuntimeAuthentication;

mod sealed {
    pub trait LiveRuntimeRegistry {}
    pub trait LiveRuntimeSession {}
}

pub(crate) use sealed::{
    LiveRuntimeRegistry as DeliveryLiveRuntimeRegistrySeal,
    LiveRuntimeSession as DeliveryLiveRuntimeSessionSeal,
};

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub trait DeliveryLiveRuntimeRegistryTestSeam {}

#[cfg(feature = "test-support")]
impl<T: DeliveryLiveRuntimeRegistryTestSeam> sealed::LiveRuntimeRegistry for T {}

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub trait DeliveryLiveRuntimeSessionTestSeam {}

#[cfg(feature = "test-support")]
impl<T: DeliveryLiveRuntimeSessionTestSeam> sealed::LiveRuntimeSession for T {}

macro_rules! runtime_proof_wrapper {
    ($name:ident, $runtime:ty, $store:ty, $convert:ident) => {
        #[derive(Clone)]
        pub struct $name {
            evidence: RuntimeProofEvidence<$runtime, $store>,
        }

        impl $name {
            #[allow(dead_code)]
            pub(crate) fn from_runtime(binding: $runtime) -> Self {
                Self {
                    evidence: RuntimeProofEvidence::Runtime(binding),
                }
            }

            #[cfg(feature = "test-support")]
            #[doc(hidden)]
            pub fn from_store_proof_for_test(proof: $store) -> Self {
                Self {
                    evidence: RuntimeProofEvidence::Test(proof),
                }
            }

            pub(crate) fn into_store_proof(self) -> Result<$store, DeliveryLiveRuntimeError> {
                match self.evidence {
                    RuntimeProofEvidence::Runtime(binding) => $convert(&binding),
                    RuntimeProofEvidence::Test(proof) => Ok(proof),
                }
            }
        }

        impl std::fmt::Debug for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(concat!(stringify!($name), "(<opaque>)"))
            }
        }
    };
}

#[derive(Clone)]
enum RuntimeProofEvidence<R, S> {
    Runtime(R),
    Test(S),
}

runtime_proof_wrapper!(
    DeliveryLiveSourceObjectProof,
    DeliverySourceObjectPersistenceBinding,
    DeliverySourceObjectProof,
    source_object_store_proof
);
runtime_proof_wrapper!(
    DeliveryLiveSourceAppliedProof,
    DeliverySourceAppliedPersistenceBinding,
    DeliverySourceAppliedProof,
    source_applied_store_proof
);
runtime_proof_wrapper!(
    DeliveryLiveExpectedMergeProof,
    DeliveryExpectedMergePersistenceBinding,
    MergeCommitObjectProof,
    expected_merge_store_proof
);
runtime_proof_wrapper!(
    DeliveryLiveMergeAppliedProof,
    DeliveryMergeAppliedPersistenceBinding,
    MergeAppliedProof,
    merge_applied_store_proof
);
runtime_proof_wrapper!(
    DeliveryLiveAbortProof,
    DeliveryAbortPersistenceBinding,
    MergeAbortProof,
    abort_store_proof
);
runtime_proof_wrapper!(
    DeliveryLiveAbortAppliedProof,
    DeliveryAbortAppliedPersistenceBinding,
    MergeAbortAppliedProof,
    abort_applied_store_proof
);

/// Closed classifications from the source side of the live runtime.
///
/// A retry outcome proves the Git command did not apply. Unknown child
/// outcomes are never represented here: implementations must return
/// `ProcessCleanupUnproven` so DeliveryManager retains both ownership guards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryLiveSourceDisposition {
    Applied,
    KnownNotApplied(DeliverySourceRetryReason),
    ReconciliationRequired(MergeReconciliationReason),
    ProcessCleanupUnproven,
}

#[derive(Debug, Clone)]
pub struct DeliveryLiveSourceResult {
    disposition: DeliveryLiveSourceDisposition,
    proof: Option<DeliveryLiveSourceAppliedProof>,
}

impl DeliveryLiveSourceResult {
    pub fn applied(proof: DeliveryLiveSourceAppliedProof) -> Self {
        Self {
            disposition: DeliveryLiveSourceDisposition::Applied,
            proof: Some(proof),
        }
    }

    pub const fn known_not_applied(reason: DeliverySourceRetryReason) -> Self {
        Self {
            disposition: DeliveryLiveSourceDisposition::KnownNotApplied(reason),
            proof: None,
        }
    }

    pub const fn reconciliation_required(reason: MergeReconciliationReason) -> Self {
        Self {
            disposition: DeliveryLiveSourceDisposition::ReconciliationRequired(reason),
            proof: None,
        }
    }

    pub const fn process_cleanup_unproven() -> Self {
        Self {
            disposition: DeliveryLiveSourceDisposition::ProcessCleanupUnproven,
            proof: None,
        }
    }

    pub const fn disposition(&self) -> DeliveryLiveSourceDisposition {
        self.disposition
    }

    pub fn into_applied_proof(self) -> Option<DeliveryLiveSourceAppliedProof> {
        self.proof
    }
}

/// Query-first classification for one already-durable `MergePending` intent.
#[derive(Debug, Clone)]
pub enum DeliveryLiveMergeDisposition {
    Applied(Box<DeliveryLiveMergeAppliedProof>),
    Conflict(Box<DeliveryLiveAbortProof>),
    KnownNotApplied(MergeKnownNotAppliedReason),
    ReconciliationRequired(MergeReconciliationReason),
    ProcessCleanupUnproven,
}

/// Query-first classification for one already-durable `AbortPending` intent.
#[derive(Debug, Clone)]
pub enum DeliveryLiveAbortDisposition {
    Applied(DeliveryLiveAbortAppliedProof),
    Pending,
    ReconciliationRequired(MergeReconciliationReason),
    ProcessCleanupUnproven,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DeliveryLiveRuntimeError {
    #[error("delivery live runtime is unavailable")]
    Unavailable,
    #[error("delivery live process cleanup is unproven")]
    ProcessCleanupUnproven,
    #[error("delivery live runtime requires reconciliation")]
    ReconciliationRequired(MergeReconciliationReason),
}

/// Fresh Ready-to-Accept authentication can reject a command before its
/// durable accept receipt exists. Keeping this separate from live runtime
/// errors prevents a post-accept source or target mismatch from being exposed
/// as a client-correctable 409.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DeliveryAcceptAuthenticationError {
    #[error("delivery accept preflight was rejected")]
    Rejected(PreflightRejectedReason),
    #[error("delivery accept preflight became stale")]
    Stale(PreflightStaleReason),
    #[error("delivery accept preflight found merge conflicts")]
    MergeConflict,
    #[error("delivery accept authentication timed out")]
    CommandTimedOut,
    #[error("delivery accept process cleanup is unproven")]
    ProcessCleanupUnproven,
    #[error("delivery accept authentication requires reconciliation")]
    ReconciliationRequired(MergeReconciliationReason),
    #[error("delivery accept authentication is unavailable")]
    Unavailable,
}

/// Sealed runtime authority for accepted source/merge/abort work.
///
/// Every method is a fresh authentication boundary. In particular,
/// `drive_merge_pending` must re-prove the committed source object, source
/// worktree/lock, target branch/HEAD/config/security, ancestry and ignored-path
/// collision immediately before a child is spawned. Store records are inert
/// scalar inputs and can never grant Git authority by themselves.
#[async_trait::async_trait]
pub trait DeliveryLiveRuntimeSession: sealed::LiveRuntimeSession + Send + Sync + 'static {
    async fn authenticate_accept(
        &self,
        command: &AcceptMergeCommandRequest,
    ) -> Result<DeliveryRuntimeAuthentication, DeliveryAcceptAuthenticationError>;

    async fn build_source_object(
        &self,
        source: &DeliverySourceRecord,
    ) -> Result<DeliveryLiveSourceObjectProof, DeliveryLiveRuntimeError>;

    async fn apply_source_commit(
        &self,
        source: &DeliverySourceRecord,
    ) -> Result<DeliveryLiveSourceResult, DeliveryLiveRuntimeError>;

    async fn build_expected_merge(
        &self,
        operation: &MergeOperationRecord,
        source: &DeliverySourceRecord,
    ) -> Result<DeliveryLiveExpectedMergeProof, DeliveryLiveRuntimeError>;

    async fn drive_merge_pending(
        &self,
        operation: &MergeOperationRecord,
        source: &DeliverySourceRecord,
    ) -> Result<DeliveryLiveMergeDisposition, DeliveryLiveRuntimeError>;

    async fn drive_abort_pending(
        &self,
        operation: &MergeOperationRecord,
        source: &DeliverySourceRecord,
    ) -> Result<DeliveryLiveAbortDisposition, DeliveryLiveRuntimeError>;
}

#[async_trait::async_trait]
pub trait DeliveryLiveRuntimeRegistry: sealed::LiveRuntimeRegistry + Send + Sync + 'static {
    /// Opens a fresh runtime binding from one audited Store snapshot. The
    /// implementation may retain opaque capabilities, but may not treat the
    /// snapshot's paths, OIDs or digests as authority.
    async fn open_live_session(
        &self,
        snapshot: &DeliveryEligibilitySnapshot,
    ) -> Result<Arc<dyn DeliveryLiveRuntimeSession>, DeliveryLiveRuntimeError>;
}

fn source_object_store_proof(
    binding: &DeliverySourceObjectPersistenceBinding,
) -> Result<DeliverySourceObjectProof, DeliveryLiveRuntimeError> {
    DeliverySourceObjectProof::try_new(
        parse_commit(binding.expected_source_commit())?,
        parse_tree(binding.tree())?,
        vec![parse_commit(binding.parent())?],
        store_metadata(binding.metadata())?,
    )
    .map_err(|_| inconsistent_runtime_projection())
}

fn source_applied_store_proof(
    binding: &DeliverySourceAppliedPersistenceBinding,
) -> Result<DeliverySourceAppliedProof, DeliveryLiveRuntimeError> {
    let worktree = SourceWorktreeProof::try_new(
        parse_tree(binding.index_tree())?,
        parse_tree(binding.worktree_tree())?,
        binding.staged_entry_count(),
        binding.unstaged_entry_count(),
        binding.untracked_entry_count(),
        binding.unmerged_entry_count(),
    )
    .map_err(|_| inconsistent_runtime_projection())?;
    DeliverySourceAppliedProof::try_new(
        source_object_store_proof(binding.object())?,
        parse_branch(binding.source_branch())?,
        parse_commit(binding.source_ref_oid())?,
        parse_commit(binding.head_oid())?,
        worktree,
        parse_directory_identity(
            binding.common_git_identity_algorithm(),
            binding.common_git_identity_digest(),
        )?,
        parse_directory_identity(
            binding.worktree_admin_identity_algorithm(),
            binding.worktree_admin_identity_digest(),
        )?,
        binding.fixed_lock_reason().to_owned(),
        parse_digest(binding.source_config_attributes_digest())?,
    )
    .map_err(|_| inconsistent_runtime_projection())
}

fn expected_merge_store_proof(
    binding: &DeliveryExpectedMergePersistenceBinding,
) -> Result<MergeCommitObjectProof, DeliveryLiveRuntimeError> {
    MergeCommitObjectProof::try_new(
        parse_commit(binding.expected_merge_commit())?,
        parse_tree(binding.tree())?,
        vec![
            parse_commit(binding.target_parent())?,
            parse_commit(binding.source_parent())?,
        ],
        store_metadata(binding.metadata())?,
    )
    .map_err(|_| inconsistent_runtime_projection())
}

fn merge_applied_store_proof(
    binding: &DeliveryMergeAppliedPersistenceBinding,
) -> Result<MergeAppliedProof, DeliveryLiveRuntimeError> {
    MergeAppliedProof::try_new(
        expected_merge_store_proof(binding.object())?,
        parse_branch(binding.target_branch())?,
        parse_commit(binding.target_head())?,
        parse_branch(binding.source_branch())?,
        parse_commit(binding.source_oid())?,
        parse_directory_identity(
            binding.common_git_identity_algorithm(),
            binding.common_git_identity_digest(),
        )?,
        parse_directory_identity(
            binding.worktree_admin_identity_algorithm(),
            binding.worktree_admin_identity_digest(),
        )?,
        binding.fixed_lock_reason().to_owned(),
        parse_digest(binding.source_config_attributes_digest())?,
        parse_tree(binding.index_tree())?,
        parse_tree(binding.worktree_tree())?,
        binding.staged_entry_count(),
        binding.unstaged_entry_count(),
        binding.untracked_entry_count(),
        binding.unmerged_entry_count(),
        None,
        if binding.merge_autostash_is_absent() {
            MergeAutostashObservation::Absent
        } else {
            MergeAutostashObservation::Unobservable
        },
        if binding.other_git_operation_is_clear() {
            OtherGitOperationObservation::Clear
        } else {
            OtherGitOperationObservation::Unobservable
        },
    )
    .map_err(|_| inconsistent_runtime_projection())
}

fn abort_store_proof(
    binding: &DeliveryAbortPersistenceBinding,
) -> Result<MergeAbortProof, DeliveryLiveRuntimeError> {
    let raw_paths = binding
        .conflict_paths()
        .iter()
        .map(|path| match path.encoding() {
            RuntimeConflictPathEncoding::Utf8 => Ok(path.value().as_bytes().to_vec()),
            RuntimeConflictPathEncoding::Base64Url => URL_SAFE_NO_PAD
                .decode(path.value())
                .map_err(|_| inconsistent_runtime_projection()),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let paths = MergeConflictPaths::try_from_raw(raw_paths)
        .map_err(|_| inconsistent_runtime_projection())?;
    MergeAbortProof::try_new(
        Uuid::from_bytes(binding.child_receipt_id()),
        parse_branch(binding.target_branch())?,
        parse_commit(binding.target_head())?,
        parse_branch(binding.source_branch())?,
        parse_commit(binding.source_oid())?,
        parse_commit(binding.merge_head())?,
        parse_directory_identity(
            binding.common_git_identity_algorithm(),
            binding.common_git_identity_digest(),
        )?,
        parse_directory_identity(
            binding.worktree_admin_identity_algorithm(),
            binding.worktree_admin_identity_digest(),
        )?,
        binding.fixed_lock_reason().to_owned(),
        parse_digest(binding.source_config_attributes_digest())?,
        parse_digest(binding.index_stages_digest())?,
        parse_digest(binding.worktree_digest())?,
        if binding.merge_autostash_is_absent() {
            MergeAutostashObservation::Absent
        } else {
            MergeAutostashObservation::Unobservable
        },
        if binding.other_git_operation_is_clear() {
            OtherGitOperationObservation::Clear
        } else {
            OtherGitOperationObservation::Unobservable
        },
        paths,
    )
    .map_err(|_| inconsistent_runtime_projection())
}

fn abort_applied_store_proof(
    binding: &DeliveryAbortAppliedPersistenceBinding,
) -> Result<MergeAbortAppliedProof, DeliveryLiveRuntimeError> {
    MergeAbortAppliedProof::try_new(
        parse_branch(binding.target_branch())?,
        parse_commit(binding.target_head())?,
        parse_branch(binding.source_branch())?,
        parse_commit(binding.source_oid())?,
        parse_directory_identity(
            binding.common_git_identity_algorithm(),
            binding.common_git_identity_digest(),
        )?,
        parse_directory_identity(
            binding.worktree_admin_identity_algorithm(),
            binding.worktree_admin_identity_digest(),
        )?,
        binding.fixed_lock_reason().to_owned(),
        parse_digest(binding.source_config_attributes_digest())?,
        binding.staged_entry_count(),
        binding.unstaged_entry_count(),
        binding.untracked_entry_count(),
        binding.unmerged_entry_count(),
        None,
        if binding.merge_autostash_is_absent() {
            MergeAutostashObservation::Absent
        } else {
            MergeAutostashObservation::Unobservable
        },
        if binding.other_git_operation_is_clear() {
            OtherGitOperationObservation::Clear
        } else {
            OtherGitOperationObservation::Unobservable
        },
    )
    .map_err(|_| inconsistent_runtime_projection())
}

fn store_metadata(
    metadata: &DeliveryCommitPersistenceMetadata,
) -> Result<DeliveryCommitMetadata, DeliveryLiveRuntimeError> {
    Ok(DeliveryCommitMetadata {
        author_name: metadata.author_name().to_owned(),
        author_email: metadata.author_email().to_owned(),
        committer_name: metadata.committer_name().to_owned(),
        committer_email: metadata.committer_email().to_owned(),
        author_date_bytes: String::from_utf8(metadata.author_date_bytes().to_vec())
            .map_err(|_| inconsistent_runtime_projection())?,
        committer_date_bytes: String::from_utf8(metadata.committer_date_bytes().to_vec())
            .map_err(|_| inconsistent_runtime_projection())?,
        message_template_version: metadata.message_template_version(),
        message_bytes: metadata.message_bytes().to_vec(),
    })
}

fn parse_commit(value: &str) -> Result<GitCommitOid, DeliveryLiveRuntimeError> {
    GitCommitOid::from_str(value).map_err(|_| inconsistent_runtime_projection())
}

fn parse_tree(value: &str) -> Result<GitTreeOid, DeliveryLiveRuntimeError> {
    GitTreeOid::from_str(value).map_err(|_| inconsistent_runtime_projection())
}

fn parse_branch(value: &str) -> Result<GitBranchRef, DeliveryLiveRuntimeError> {
    GitBranchRef::from_str(value).map_err(|_| inconsistent_runtime_projection())
}

fn parse_digest(value: &str) -> Result<Sha256Digest, DeliveryLiveRuntimeError> {
    Sha256Digest::from_str(value).map_err(|_| inconsistent_runtime_projection())
}

fn parse_directory_identity(
    algorithm: &str,
    digest: &str,
) -> Result<DirectoryIdentity, DeliveryLiveRuntimeError> {
    DirectoryIdentity::try_new(algorithm, digest).map_err(|_| inconsistent_runtime_projection())
}

const fn inconsistent_runtime_projection() -> DeliveryLiveRuntimeError {
    DeliveryLiveRuntimeError::ReconciliationRequired(
        MergeReconciliationReason::DeliveryStateInconsistent,
    )
}
