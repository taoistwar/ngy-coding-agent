use std::fmt;
use std::sync::Arc;

use crate::command_policy::{
    CommandPolicyError, DeliveryGitMutationCommandFactory, ExecutionDirectory, PinnedExecutable,
};

mod abort;
mod cleanup;
mod collision;
pub(crate) mod command;
mod config;
mod git_state;
mod merge;
mod observation;
pub(crate) mod output;
mod persistence;
mod preflight;
mod probe;
mod recovery;
mod sandbox;
mod source_commit;
mod source_tree;
mod target;
mod types;

pub use abort::{
    DeliveryAbortAppliedProof, DeliveryAbortCapability, DeliveryAbortError, DeliveryAbortOutcome,
    DeliveryAbortPendingAuthorizer, DeliveryAbortPendingDisposition, DeliveryAbortProof,
    DeliveryAbortProofCapture, DeliveryKnownMergeConflict, DeliveryMergeAppliedProof,
    DeliveryMergePendingDisposition, abort_expected_delivery_merge,
    authorize_persisted_delivery_abort, capture_delivery_abort_proof,
};
pub use cleanup::{
    DeliveryBranchCleanupIntent, DeliveryBranchCleanupRecoveryBindingOutcome,
    DeliveryBranchCleanupRefreshProof, DeliveryDeletePendingAuthorizer,
    DeliveryDeletePendingCapability, DeliveryDeletePendingDisposition,
    DeliveryRemovePendingAuthorizer, DeliveryRemovePendingCapability,
    DeliveryRemovePendingDisposition, DeliveryUnlockPendingAuthorizer,
    DeliveryUnlockPendingCapability, DeliveryUnlockPendingDisposition,
    DeliveryUnlockedPendingRemoveAuthorizer, DeliveryUnlockedPendingRemoveCapability,
    DeliveryUnlockedPendingRemoveDisposition, DeliveryWorktreeCleanupError,
    DeliveryWorktreeCleanupIntent, DeliveryWorktreeCleanupProvisioner,
    DeliveryWorktreeCleanupRecoveryBindingOutcome, DeliveryWorktreeCleanupRecoveryPhase,
    authorize_persisted_delivery_branch_delete, authorize_persisted_delivery_remove,
    authorize_persisted_delivery_unlock, authorize_persisted_delivery_unlocked_pending_remove,
};
pub use merge::{
    DeliveryExpectedMerge, DeliveryMergeError, DeliveryMergeInput, DeliveryMergeOutcome,
    apply_expected_delivery_merge, build_expected_delivery_merge,
};
pub use observation::{DeliverySourceCapability, DeliverySourceProvisioner};
pub use persistence::{
    DeliveryAbortAppliedPersistenceBinding, DeliveryAbortPersistenceBinding,
    DeliveryCommitPersistenceMetadata, DeliveryExpectedMergePersistenceBinding,
    DeliveryMergeAppliedPersistenceBinding, DeliveryPersistedMergeRecovery,
    DeliveryPersistedSourceRecovery, DeliveryPersistedSourceState, DeliveryPersistedTargetRecovery,
    DeliveryPersistenceBinding, DeliveryPersistenceInputError,
    DeliverySourceAppliedPersistenceBinding, DeliverySourceObjectPersistenceBinding,
};
pub use preflight::{
    DeliveryPreflightSource, preflight_delivery_merge, preflight_prepared_delivery_merge,
};
pub use probe::probe_delivery_git;
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use probe::probe_delivery_git_with_after_initialize_hook_for_test;
pub use recovery::{
    DeliveryMergeRecoveryBindingOutcome, DeliveryMergeRecoveryCapability,
    DeliveryPersistedAbortRecoveryObservation, DeliverySourceRecoveryBindingOutcome,
    DeliverySourceRecoveryCapability, DeliverySourceRecoveryIntent,
    DeliveryTargetRecoveryBindingOutcome, DeliveryTargetRecoveryCapability,
    DeliveryTargetRecoveryIntent, bind_persisted_delivery_merge_recovery,
    build_expected_persisted_delivery_merge, capture_delivery_abort_proof_from_recovery,
    capture_persisted_delivery_abort_proof, capture_persisted_delivery_abort_recovery,
    classify_delivery_abort_pending, classify_delivery_merge_pending,
    classify_persisted_delivery_merge_pending, project_persisted_delivery_source_applied,
    project_persisted_delivery_source_object, retry_delivery_abort_pending,
    retry_delivery_merge_pending, retry_persisted_delivery_abort_pending,
    retry_persisted_delivery_merge_pending,
};
pub use source_commit::DeliverySourceCommitInput;
pub use target::{
    DeliveryTargetCapability, DeliveryTargetProvisioner, RegisteredDeliveryTargetObservation,
};
pub(crate) use types::{
    CandidateTreeProvenance, DeliveryCommitOid, DeliveryTreeOid, MAX_MERGE_CONFLICT_PATH_BYTES,
    MAX_MERGE_CONFLICT_PATHS, MAX_MERGE_CONFLICT_PAYLOAD_BYTES,
};
pub use types::{
    DeliveryCandidateTree, DeliveryConflictPath, DeliveryConflictPathEncoding,
    DeliveryPreflightError, DeliveryPreflightResult, DeliverySourceCommit, DeliverySourceError,
    DeliverySourceLimits, DeliverySourcePendingState, DeliverySourceRecoveryDisposition,
    DeliveryTargetError, DeliveryTargetRequest, PreparedDeliveryPreflightSource,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryGitObjectFormat {
    Sha1,
    Sha256,
}

impl DeliveryGitObjectFormat {
    pub(crate) const fn hexadecimal_length(self) -> usize {
        match self {
            Self::Sha1 => 40,
            Self::Sha256 => 64,
        }
    }

    pub(crate) fn parse_exact_git_output(output: &[u8]) -> Option<Self> {
        match output {
            b"sha1\n" => Some(Self::Sha1),
            b"sha256\n" => Some(Self::Sha256),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryGitVersion {
    major: u32,
    minor: u32,
    patch: u32,
}

impl DeliveryGitVersion {
    const fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    pub const fn major(self) -> u32 {
        self.major
    }

    pub const fn minor(self) -> u32 {
        self.minor
    }

    pub const fn patch(self) -> u32 {
        self.patch
    }

    pub const fn is_at_least(self, major: u32, minor: u32) -> bool {
        self.major > major || (self.major == major && self.minor >= minor)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DeliveryGitCapabilities {
    required_merge_options: bool,
    merge_tree: bool,
    atomic_ref_transaction: bool,
}

/// Non-forgeable proof that one exact pinned Git executable passed the
/// application-private delivery capability probe.
pub struct ProbedDeliveryGit {
    git: Arc<PinnedExecutable>,
    private_runtime: Arc<ExecutionDirectory>,
    version: DeliveryGitVersion,
    object_format: DeliveryGitObjectFormat,
    repository_object_format_bound: bool,
    capabilities: DeliveryGitCapabilities,
}

impl ProbedDeliveryGit {
    fn from_successful_probe(
        git: Arc<PinnedExecutable>,
        private_runtime: Arc<ExecutionDirectory>,
        version: DeliveryGitVersion,
        object_format: DeliveryGitObjectFormat,
    ) -> Result<Self, DeliveryGitProbeError> {
        git.revalidate()
            .map_err(|_| DeliveryGitProbeError::ExecutableChanged)?;
        private_runtime
            .revalidate()
            .map_err(|_| DeliveryGitProbeError::InvalidConfiguration)?;
        Ok(Self {
            git,
            private_runtime,
            version,
            object_format,
            repository_object_format_bound: false,
            capabilities: DeliveryGitCapabilities {
                required_merge_options: true,
                merge_tree: true,
                atomic_ref_transaction: true,
            },
        })
    }

    pub const fn version(&self) -> DeliveryGitVersion {
        self.version
    }

    pub const fn object_format(&self) -> DeliveryGitObjectFormat {
        self.object_format
    }

    pub const fn supports_required_merge_options(&self) -> bool {
        self.capabilities.required_merge_options
    }

    pub const fn supports_merge_tree(&self) -> bool {
        self.capabilities.merge_tree
    }

    pub const fn supports_atomic_ref_transaction(&self) -> bool {
        self.capabilities.atomic_ref_transaction
    }

    pub fn verify_current_executable(&self) -> Result<(), DeliveryGitProbeError> {
        self.git
            .revalidate()
            .map_err(|_| DeliveryGitProbeError::ExecutableChanged)
    }

    pub(crate) fn verify_for_mutation(&self) -> Result<(), CommandPolicyError> {
        if !self.supports_required_merge_options()
            || !self.supports_merge_tree()
            || !self.supports_atomic_ref_transaction()
        {
            return Err(CommandPolicyError::InvalidGitBinding);
        }
        self.git.revalidate()
    }

    pub(crate) const fn pinned_executable(&self) -> &Arc<PinnedExecutable> {
        &self.git
    }

    pub(crate) const fn private_runtime(&self) -> &Arc<ExecutionDirectory> {
        &self.private_runtime
    }

    pub(crate) fn mutation_command_factory(
        &self,
    ) -> Result<DeliveryGitMutationCommandFactory, CommandPolicyError> {
        DeliveryGitMutationCommandFactory::try_from_probe(self)
    }

    /// Binds the already-probed executable capabilities to the object format
    /// reported by one authenticated repository. No public constructor accepts
    /// a caller-selected format.
    pub(crate) fn bind_repository_object_format(
        &self,
        object_format: DeliveryGitObjectFormat,
    ) -> Result<Self, DeliveryGitProbeError> {
        if self.repository_object_format_bound && self.object_format != object_format {
            return Err(DeliveryGitProbeError::InvalidConfiguration);
        }
        self.verify_for_mutation()
            .map_err(DeliveryGitProbeError::from)?;
        self.private_runtime
            .revalidate()
            .map_err(|_| DeliveryGitProbeError::InvalidConfiguration)?;
        Ok(Self {
            git: Arc::clone(&self.git),
            private_runtime: Arc::clone(&self.private_runtime),
            version: self.version,
            object_format,
            repository_object_format_bound: true,
            capabilities: self.capabilities,
        })
    }

    pub(crate) fn shares_probed_authority_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.git, &other.git)
            && Arc::ptr_eq(&self.private_runtime, &other.private_runtime)
            && self.version == other.version
            && self.capabilities == other.capabilities
    }

    pub(crate) fn shares_repository_format_authority_with(&self, other: &Self) -> bool {
        self.repository_object_format_bound
            && other.repository_object_format_bound
            && self.shares_probed_authority_with(other)
            && self.object_format == other.object_format
    }

    pub(crate) const fn has_repository_object_format_binding(&self) -> bool {
        self.repository_object_format_bound
    }
}

impl fmt::Debug for ProbedDeliveryGit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProbedDeliveryGit(<opaque>)")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DeliveryGitProbeError {
    #[error("delivery Git probe configuration is invalid")]
    InvalidConfiguration,
    #[error("the pinned delivery Git executable changed")]
    ExecutableChanged,
    #[error("required delivery Git capabilities are unavailable")]
    CapabilityUnavailable,
    #[error("delivery Git capability probing was cancelled")]
    Cancelled,
    #[error("delivery Git probe cleanup could not be proven")]
    CleanupUnproven,
}

impl DeliveryGitProbeError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidConfiguration => "DELIVERY_GIT_PROBE_INVALID",
            Self::ExecutableChanged => "DELIVERY_GIT_EXECUTABLE_CHANGED",
            Self::CapabilityUnavailable => "DELIVERY_GIT_CAPABILITY_UNAVAILABLE",
            Self::Cancelled => "DELIVERY_GIT_PROBE_CANCELLED",
            Self::CleanupUnproven => "DELIVERY_GIT_PROBE_CLEANUP_UNPROVEN",
        }
    }
}

impl From<CommandPolicyError> for DeliveryGitProbeError {
    fn from(error: CommandPolicyError) -> Self {
        if matches!(error, CommandPolicyError::IdentityChanged) {
            Self::ExecutableChanged
        } else {
            Self::CapabilityUnavailable
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn git_a_probe_handle_cannot_authorize_git_b() {
        let fixture = ExecutableCopies::new();
        let git_a = Arc::new(PinnedExecutable::open(&fixture.git_a).unwrap());
        let git_b = Arc::new(PinnedExecutable::open(&fixture.git_b).unwrap());
        let probe = ProbedDeliveryGit::from_successful_probe(
            Arc::clone(&git_a),
            fixture.private_runtime(),
            DeliveryGitVersion::new(2, 53, 0),
            DeliveryGitObjectFormat::Sha1,
        )
        .unwrap();

        let factory = probe.mutation_command_factory().unwrap();
        assert!(factory.is_bound_to_for_test(&git_a));
        assert!(!factory.is_bound_to_for_test(&git_b));
        assert_eq!(
            format!("{factory:?}"),
            "DeliveryGitMutationCommandFactory(<opaque>)"
        );
    }

    #[test]
    fn executable_replacement_is_blocked_or_detected_after_probe() {
        let fixture = ExecutableCopies::new();
        let git_a = Arc::new(PinnedExecutable::open(&fixture.git_a).unwrap());
        let probe = ProbedDeliveryGit::from_successful_probe(
            git_a,
            fixture.private_runtime(),
            DeliveryGitVersion::new(2, 53, 0),
            DeliveryGitObjectFormat::Sha1,
        )
        .unwrap();

        match std::fs::write(&fixture.git_a, b"replacement") {
            Ok(()) => assert_eq!(
                probe.verify_current_executable().unwrap_err(),
                DeliveryGitProbeError::ExecutableChanged
            ),
            Err(_) => probe.verify_current_executable().unwrap(),
        }
    }

    #[test]
    fn repository_format_authority_rejects_cross_probe_and_format_mismatch() {
        let fixture = ExecutableCopies::new();
        let private_runtime = fixture.private_runtime();
        let git_a = Arc::new(PinnedExecutable::open(&fixture.git_a).unwrap());
        let git_b = Arc::new(PinnedExecutable::open(&fixture.git_b).unwrap());
        let probe_a = ProbedDeliveryGit::from_successful_probe(
            git_a,
            Arc::clone(&private_runtime),
            DeliveryGitVersion::new(2, 53, 0),
            DeliveryGitObjectFormat::Sha1,
        )
        .unwrap();
        let sha1 = probe_a
            .bind_repository_object_format(DeliveryGitObjectFormat::Sha1)
            .unwrap();
        let sha256 = probe_a
            .bind_repository_object_format(DeliveryGitObjectFormat::Sha256)
            .unwrap();
        let other_probe = ProbedDeliveryGit::from_successful_probe(
            git_b,
            private_runtime,
            DeliveryGitVersion::new(2, 53, 0),
            DeliveryGitObjectFormat::Sha1,
        )
        .unwrap();

        assert!(!probe_a.has_repository_object_format_binding());
        assert!(sha1.has_repository_object_format_binding());
        assert!(sha256.has_repository_object_format_binding());
        assert!(sha1.shares_probed_authority_with(&sha256));
        assert!(
            sha1.shares_repository_format_authority_with(
                &probe_a
                    .bind_repository_object_format(DeliveryGitObjectFormat::Sha1)
                    .unwrap()
            )
        );
        assert!(!sha1.shares_repository_format_authority_with(&sha256));
        assert!(!sha1.shares_repository_format_authority_with(&other_probe));
        assert_eq!(
            sha1.bind_repository_object_format(DeliveryGitObjectFormat::Sha256)
                .unwrap_err(),
            DeliveryGitProbeError::InvalidConfiguration
        );
    }

    #[test]
    fn repository_object_format_parser_accepts_only_one_exact_git_line() {
        assert_eq!(
            DeliveryGitObjectFormat::parse_exact_git_output(b"sha1\n"),
            Some(DeliveryGitObjectFormat::Sha1)
        );
        assert_eq!(
            DeliveryGitObjectFormat::parse_exact_git_output(b"sha256\n"),
            Some(DeliveryGitObjectFormat::Sha256)
        );
        for malformed in [
            b"sha256".as_slice(),
            b"sha256\r\n",
            b"SHA256\n",
            b"sha256\nextra\n",
            b"sha256\0\n",
        ] {
            assert_eq!(
                DeliveryGitObjectFormat::parse_exact_git_output(malformed),
                None
            );
        }
    }

    struct ExecutableCopies {
        _temporary: tempfile::TempDir,
        git_a: std::path::PathBuf,
        git_b: std::path::PathBuf,
    }

    impl ExecutableCopies {
        fn new() -> Self {
            let temporary = tempfile::tempdir().unwrap();
            let extension = if cfg!(windows) { ".exe" } else { "" };
            let git_a = temporary.path().join(format!("git-a{extension}"));
            let git_b = temporary.path().join(format!("git-b{extension}"));
            let current = std::env::current_exe().unwrap();
            std::fs::copy(&current, &git_a).unwrap();
            std::fs::copy(current, &git_b).unwrap();
            Self {
                _temporary: temporary,
                git_a,
                git_b,
            }
        }

        fn private_runtime(&self) -> Arc<ExecutionDirectory> {
            Arc::new(ExecutionDirectory::open(self._temporary.path()).unwrap())
        }
    }
}
