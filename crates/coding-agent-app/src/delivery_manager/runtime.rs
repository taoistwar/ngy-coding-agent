use std::any::Any;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use coding_agent_domain::{RepositoryId, TaskId};
use coding_agent_runtime::{
    DeliveryGitObjectFormat, DeliveryPersistenceBinding, ProcessCleanupProof, ProcessLivenessScope,
};
use coding_agent_store::{
    AcceptMergeCommandRequest, AttemptArtifactState, DeliveryEligibilitySnapshot, DeliveryIdentity,
    DirectoryIdentity, GitBranchRef, GitCommitOid, GitObjectAlgorithm, GitTreeOid,
    MergeOperationRecord, MergePreflightResult, MergeReconciliationReason, PreflightCommandRequest,
    PreflightRejectedReason, PreflightStaleReason, Sha256Digest,
};

use crate::RepositoryCoordinationKey;

mod sealed {
    pub trait ProcessProofProvider {}
    pub trait RuntimeSession {}
    pub trait RuntimeRegistry {}
}

pub(crate) use sealed::{
    RuntimeRegistry as DeliveryRuntimeRegistrySeal, RuntimeSession as DeliveryRuntimeSessionSeal,
};

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub trait DeliveryProcessProofProviderTestSeam {}

#[cfg(feature = "test-support")]
impl<T: DeliveryProcessProofProviderTestSeam> sealed::ProcessProofProvider for T {}

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub trait DeliveryRuntimeSessionTestSeam {}

#[cfg(feature = "test-support")]
impl<T: DeliveryRuntimeSessionTestSeam> sealed::RuntimeSession for T {}

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub trait DeliveryRuntimeRegistryTestSeam {}

#[cfg(feature = "test-support")]
impl<T: DeliveryRuntimeRegistryTestSeam> sealed::RuntimeRegistry for T {}

/// Independent process-tree observation used alongside TaskManager ownership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryProcessProof {
    Clean,
    Active,
    CleanupUnproven,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DeliveryProcessProofError {
    #[error("delivery process ownership is unavailable")]
    Unavailable,
}

#[async_trait::async_trait]
pub trait DeliveryProcessProofProvider:
    sealed::ProcessProofProvider + Send + Sync + 'static
{
    async fn observe(
        &self,
        task_id: TaskId,
    ) -> Result<DeliveryProcessProof, DeliveryProcessProofError>;
}

/// Production process-tree observer bound to the already-exclusive primary
/// liveness namespace. Observation is read-only: the operation-specific
/// runtime seals and owns its child scope only when it is ready to spawn.
pub(crate) struct ProcessLivenessDeliveryProofProvider {
    instance_scope: ProcessLivenessScope,
}

impl ProcessLivenessDeliveryProofProvider {
    pub(crate) fn new(instance_scope: ProcessLivenessScope) -> Self {
        Self { instance_scope }
    }
}

impl sealed::ProcessProofProvider for ProcessLivenessDeliveryProofProvider {}

#[async_trait::async_trait]
impl DeliveryProcessProofProvider for ProcessLivenessDeliveryProofProvider {
    async fn observe(
        &self,
        task_id: TaskId,
    ) -> Result<DeliveryProcessProof, DeliveryProcessProofError> {
        let worker_scope = self
            .instance_scope
            .task_scope(*task_id.as_uuid().as_bytes())
            .map_err(|_| DeliveryProcessProofError::Unavailable)?;
        let delivery_scope = self
            .instance_scope
            .task_scope(delivery_process_scope_id(task_id))
            .map_err(|_| DeliveryProcessProofError::Unavailable)?;
        let worker = worker_scope
            .cleanup_proof()
            .map_err(|_| DeliveryProcessProofError::Unavailable)?;
        let delivery = delivery_scope
            .cleanup_proof()
            .map_err(|_| DeliveryProcessProofError::Unavailable)?;
        match (worker, delivery) {
            (ProcessCleanupProof::Confirmed, ProcessCleanupProof::Confirmed) => {
                Ok(DeliveryProcessProof::Clean)
            }
            (ProcessCleanupProof::Held, _) | (_, ProcessCleanupProof::Held) => {
                Ok(DeliveryProcessProof::Active)
            }
            _ => Ok(DeliveryProcessProof::CleanupUnproven),
        }
    }
}

/// Stable sibling selector for delivery Git children. It is deliberately
/// distinct from the TaskManager worker selector, while remaining recoverable
/// across process restarts.
pub(crate) fn delivery_process_scope_id(task_id: TaskId) -> [u8; 16] {
    let mut selector = *task_id.as_uuid().as_bytes();
    selector[0] ^= 0x80;
    if selector.iter().all(|byte| *byte == 0) {
        selector[15] = 1;
    }
    selector
}

/// Stable classification returned by a runtime session. Values are deliberately
/// path-free and are sufficient for the application to choose an exact Store
/// transition without parsing messages or command output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryRuntimeFailure {
    Rejected(PreflightRejectedReason),
    Stale(PreflightStaleReason),
    ReconciliationRequired(MergeReconciliationReason),
    ProcessCleanupUnproven,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryRuntimeObservationUnavailableReason {
    TargetBranchDetached,
    TargetBranchMismatch,
    TargetWorktreeDirty,
    TargetIgnoredPathCollision,
    TargetGitOperationInProgress,
    UnsafeGitConfiguration,
    UnsupportedGitAttributes,
    SourceAlreadyInTarget,
    TargetHeadChanged,
    RuntimeUnavailable,
    ProcessCleanupUnproven,
    ReconciliationRequired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryRuntimeObservation {
    Available {
        branch: GitBranchRef,
        head: GitCommitOid,
    },
    Unavailable {
        reason: DeliveryRuntimeObservationUnavailableReason,
    },
}

impl DeliveryRuntimeObservation {
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn available_for_test(branch: GitBranchRef, head: GitCommitOid) -> Self {
        Self::Available { branch, head }
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub const fn unavailable_for_test(reason: DeliveryRuntimeObservationUnavailableReason) -> Self {
        Self::Unavailable { reason }
    }
}

impl DeliveryRuntimeFailure {
    pub(crate) const fn unbound_failure(self) -> coding_agent_store::UnboundMergePreflightFailure {
        match self {
            Self::Rejected(reason) => {
                coding_agent_store::UnboundMergePreflightFailure::Rejected(reason)
            }
            Self::Stale(reason) => coding_agent_store::UnboundMergePreflightFailure::Stale(reason),
            Self::ReconciliationRequired(reason) => {
                coding_agent_store::UnboundMergePreflightFailure::ReconciliationRequired(reason)
            }
            Self::ProcessCleanupUnproven => {
                coding_agent_store::UnboundMergePreflightFailure::ReconciliationRequired(
                    MergeReconciliationReason::ProcessTreeCleanupFailed,
                )
            }
            Self::Unavailable => {
                coding_agent_store::UnboundMergePreflightFailure::ReconciliationRequired(
                    MergeReconciliationReason::DeliveryStateInconsistent,
                )
            }
        }
    }

    pub(crate) const fn prepared_failure(self) -> MergePreflightResult {
        match self {
            Self::Rejected(reason) => MergePreflightResult::rejected(reason),
            Self::Stale(reason) => MergePreflightResult::stale(reason),
            Self::ReconciliationRequired(reason) => {
                MergePreflightResult::reconciliation_required(reason)
            }
            Self::ProcessCleanupUnproven => MergePreflightResult::reconciliation_required(
                MergeReconciliationReason::ProcessTreeCleanupFailed,
            ),
            Self::Unavailable => MergePreflightResult::reconciliation_required(
                MergeReconciliationReason::DeliveryStateInconsistent,
            ),
        }
    }

    pub(crate) const fn requires_retained_repository_ownership(self) -> bool {
        matches!(self, Self::ProcessCleanupUnproven)
    }
}

/// Redacted scalar projection minted from one authenticated source/target
/// runtime pair. It grants no filesystem, command, or process authority.
#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryRuntimeAuthentication {
    coordination_key: RepositoryCoordinationKey,
    source_identity: DeliveryIdentity,
    source_base_commit: GitCommitOid,
    source_branch: GitBranchRef,
    approved_workspace_fingerprint: Sha256Digest,
    object_algorithm: GitObjectAlgorithm,
    common_git_identity: DirectoryIdentity,
    worktree_admin_identity: DirectoryIdentity,
    source_config_attributes_digest: Sha256Digest,
    target_branch: GitBranchRef,
    expected_target_head: GitCommitOid,
    target_config_attributes_digest: Sha256Digest,
    target_security_digest: Sha256Digest,
}

impl DeliveryRuntimeAuthentication {
    #[allow(dead_code)]
    pub(crate) fn from_persistence_binding(
        coordination_key: RepositoryCoordinationKey,
        binding: &DeliveryPersistenceBinding,
    ) -> Result<Self, DeliveryRuntimeFailure> {
        let source = binding.source_identity();
        let repository_id = RepositoryId::from_str(source.repository_id())
            .map_err(|_| inconsistent_authentication())?;
        let task_id =
            TaskId::from_str(source.task_id()).map_err(|_| inconsistent_authentication())?;
        let source_identity = DeliveryIdentity::try_new(task_id, repository_id, source.attempt())
            .map_err(|_| inconsistent_authentication())?;
        let source_base_commit = GitCommitOid::from_str(binding.source_base_commit())
            .map_err(|_| inconsistent_authentication())?;
        let source_branch = GitBranchRef::from_str(binding.source_branch())
            .map_err(|_| inconsistent_authentication())?;
        let approved_workspace_fingerprint =
            Sha256Digest::from_str(&encode_lower_hex(binding.approved_fingerprint().as_bytes()))
                .map_err(|_| inconsistent_authentication())?;
        let object_algorithm = match binding.object_format() {
            DeliveryGitObjectFormat::Sha1 => GitObjectAlgorithm::Sha1,
            DeliveryGitObjectFormat::Sha256 => GitObjectAlgorithm::Sha256,
        };
        let common_git_identity = DirectoryIdentity::try_new(
            binding.common_git_identity_algorithm(),
            binding.common_git_identity_digest(),
        )
        .map_err(|_| inconsistent_authentication())?;
        let worktree_admin_identity = DirectoryIdentity::try_new(
            binding.worktree_admin_identity_algorithm(),
            binding.worktree_admin_identity_digest(),
        )
        .map_err(|_| inconsistent_authentication())?;
        let source_config_attributes_digest =
            Sha256Digest::from_str(binding.source_config_attributes_digest())
                .map_err(|_| inconsistent_authentication())?;
        let target_branch = GitBranchRef::from_str(binding.target_branch())
            .map_err(|_| inconsistent_authentication())?;
        let expected_target_head = GitCommitOid::from_str(binding.expected_target_head())
            .map_err(|_| inconsistent_authentication())?;
        let target_config_attributes_digest =
            Sha256Digest::from_str(binding.target_config_attributes_digest())
                .map_err(|_| inconsistent_authentication())?;
        let target_security_digest = Sha256Digest::from_str(binding.target_security_digest())
            .map_err(|_| inconsistent_authentication())?;
        Self::try_from_parts(
            coordination_key,
            source_identity,
            source_base_commit,
            source_branch,
            approved_workspace_fingerprint,
            object_algorithm,
            common_git_identity,
            worktree_admin_identity,
            source_config_attributes_digest,
            target_branch,
            expected_target_head,
            target_config_attributes_digest,
            target_security_digest,
        )
    }

    #[cfg(feature = "test-support")]
    #[allow(clippy::too_many_arguments)]
    #[doc(hidden)]
    pub fn new_for_test(
        coordination_key: RepositoryCoordinationKey,
        source_identity: DeliveryIdentity,
        source_base_commit: GitCommitOid,
        source_branch: GitBranchRef,
        approved_workspace_fingerprint: Sha256Digest,
        object_algorithm: GitObjectAlgorithm,
        common_git_identity: DirectoryIdentity,
        worktree_admin_identity: DirectoryIdentity,
        source_config_attributes_digest: Sha256Digest,
        target_branch: GitBranchRef,
        expected_target_head: GitCommitOid,
        target_config_attributes_digest: Sha256Digest,
        target_security_digest: Sha256Digest,
    ) -> Result<Self, DeliveryRuntimeFailure> {
        Self::try_from_parts(
            coordination_key,
            source_identity,
            source_base_commit,
            source_branch,
            approved_workspace_fingerprint,
            object_algorithm,
            common_git_identity,
            worktree_admin_identity,
            source_config_attributes_digest,
            target_branch,
            expected_target_head,
            target_config_attributes_digest,
            target_security_digest,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn try_from_parts(
        coordination_key: RepositoryCoordinationKey,
        source_identity: DeliveryIdentity,
        source_base_commit: GitCommitOid,
        source_branch: GitBranchRef,
        approved_workspace_fingerprint: Sha256Digest,
        object_algorithm: GitObjectAlgorithm,
        common_git_identity: DirectoryIdentity,
        worktree_admin_identity: DirectoryIdentity,
        source_config_attributes_digest: Sha256Digest,
        target_branch: GitBranchRef,
        expected_target_head: GitCommitOid,
        target_config_attributes_digest: Sha256Digest,
        target_security_digest: Sha256Digest,
    ) -> Result<Self, DeliveryRuntimeFailure> {
        if source_base_commit.algorithm() != object_algorithm
            || expected_target_head.algorithm() != object_algorithm
        {
            return Err(inconsistent_authentication());
        }
        Ok(Self {
            coordination_key,
            source_identity,
            source_base_commit,
            source_branch,
            approved_workspace_fingerprint,
            object_algorithm,
            common_git_identity,
            worktree_admin_identity,
            source_config_attributes_digest,
            target_branch,
            expected_target_head,
            target_config_attributes_digest,
            target_security_digest,
        })
    }

    pub const fn common_git_identity(&self) -> &DirectoryIdentity {
        &self.common_git_identity
    }

    pub const fn worktree_admin_identity(&self) -> &DirectoryIdentity {
        &self.worktree_admin_identity
    }

    pub const fn source_config_attributes_digest(&self) -> &Sha256Digest {
        &self.source_config_attributes_digest
    }

    pub const fn target_branch(&self) -> &GitBranchRef {
        &self.target_branch
    }

    pub const fn expected_target_head(&self) -> &GitCommitOid {
        &self.expected_target_head
    }

    pub const fn target_config_attributes_digest(&self) -> &Sha256Digest {
        &self.target_config_attributes_digest
    }

    pub const fn target_security_digest(&self) -> &Sha256Digest {
        &self.target_security_digest
    }

    pub(crate) fn authorizes(
        &self,
        snapshot: &DeliveryEligibilitySnapshot,
        command: &PreflightCommandRequest,
        lease_key: RepositoryCoordinationKey,
    ) -> bool {
        self.authorizes_snapshot(snapshot, lease_key)
            && &self.target_branch == command.target_branch()
            && &self.expected_target_head == command.expected_target_head()
    }

    pub(crate) fn authorizes_accept(
        &self,
        snapshot: &DeliveryEligibilitySnapshot,
        command: &AcceptMergeCommandRequest,
        lease_key: RepositoryCoordinationKey,
    ) -> bool {
        let Some(evidence) = snapshot.evidence_identity.as_ref() else {
            return false;
        };
        self.authorizes_snapshot(snapshot, lease_key)
            && evidence.workspace_generation() == command.expected_review_generation()
            && evidence.workspace_fingerprint() == command.expected_workspace_fingerprint()
            && &self.target_branch == command.target_branch()
            && &self.expected_target_head == command.expected_target_head()
    }

    fn authorizes_snapshot(
        &self,
        snapshot: &DeliveryEligibilitySnapshot,
        lease_key: RepositoryCoordinationKey,
    ) -> bool {
        let Some(evidence) = snapshot.evidence_identity.as_ref() else {
            return false;
        };
        let Some(artifact) = snapshot.ownership.artifact.as_ref() else {
            return false;
        };
        let artifact_base = GitCommitOid::from_str(&artifact.base_commit).ok();
        let artifact_branch =
            GitBranchRef::from_str(&format!("refs/heads/{}", artifact.branch_name)).ok();
        let snapshot_identity = DeliveryIdentity::try_new(
            snapshot.task.id,
            snapshot.task.repository_id,
            snapshot.task.attempt,
        )
        .ok();
        self.coordination_key == lease_key
            && snapshot_identity == Some(self.source_identity)
            && evidence.identity() == self.source_identity
            && evidence.workspace_fingerprint() == &self.approved_workspace_fingerprint
            && artifact.identity.task_id == self.source_identity.task_id()
            && artifact.identity.repository_id == self.source_identity.repository_id()
            && artifact.identity.attempt == self.source_identity.attempt()
            && artifact.state == AttemptArtifactState::Ready
            && artifact_base.as_ref() == Some(&self.source_base_commit)
            && artifact_branch.as_ref() == Some(&self.source_branch)
            && self.source_base_commit.algorithm() == self.object_algorithm
            && self.expected_target_head.algorithm() == self.object_algorithm
            && snapshot.ownership.source.as_ref().is_none_or(|source| {
                source.provenance.identity == self.source_identity
                    && &source.provenance.evidence == evidence
                    && source.provenance.base_commit == self.source_base_commit
                    && source.provenance.source_branch == self.source_branch
                    && source.provenance.common_git_identity == self.common_git_identity
                    && source.provenance.worktree_admin_identity == self.worktree_admin_identity
                    && source.provenance.config_attributes_digest
                        == self.source_config_attributes_digest
            })
    }

    pub(crate) fn authorizes_operation(&self, operation: &MergeOperationRecord) -> bool {
        operation.provenance.identity == self.source_identity
            && operation.provenance.base_commit == self.source_base_commit
            && operation.provenance.source_branch == self.source_branch
            && operation.provenance.evidence.workspace_fingerprint()
                == &self.approved_workspace_fingerprint
            && operation.provenance.common_git_identity == self.common_git_identity
            && operation.provenance.worktree_admin_identity == self.worktree_admin_identity
            && operation.provenance.config_attributes_digest == self.source_config_attributes_digest
            && operation.target_branch == self.target_branch
            && operation.expected_target_head == self.expected_target_head
            && operation.target_config_attributes_digest == self.target_config_attributes_digest
            && operation.target_security_digest == self.target_security_digest
    }

    pub(crate) fn authorizes_prepared(&self, prepared: &DeliveryPreparedPreflight) -> bool {
        prepared.candidate_tree().algorithm() == self.object_algorithm
            && prepared.source_commit().algorithm() == self.object_algorithm
    }
}

fn encode_lower_hex(bytes: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

const fn inconsistent_authentication() -> DeliveryRuntimeFailure {
    DeliveryRuntimeFailure::ReconciliationRequired(
        MergeReconciliationReason::DeliveryStateInconsistent,
    )
}

impl fmt::Debug for DeliveryRuntimeAuthentication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryRuntimeAuthentication(<redacted>)")
    }
}

/// Prepared deterministic source inputs plus runtime-private proof state.
///
/// The Store-facing object identities are format validated. `runtime_state`
/// remains opaque to DeliveryManager and is interpreted only by the session
/// which minted it.
#[derive(Clone)]
pub struct DeliveryPreparedPreflight {
    candidate_tree: GitTreeOid,
    source_commit: GitCommitOid,
    runtime_state: Arc<dyn Any + Send + Sync>,
}

impl DeliveryPreparedPreflight {
    #[allow(dead_code)]
    pub(crate) fn new<T>(
        candidate_tree: GitTreeOid,
        source_commit: GitCommitOid,
        runtime_state: T,
    ) -> Self
    where
        T: Any + Send + Sync,
    {
        Self {
            candidate_tree,
            source_commit,
            runtime_state: Arc::new(runtime_state),
        }
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn new_for_test<T>(
        candidate_tree: GitTreeOid,
        source_commit: GitCommitOid,
        runtime_state: T,
    ) -> Self
    where
        T: Any + Send + Sync,
    {
        Self::new(candidate_tree, source_commit, runtime_state)
    }

    pub const fn candidate_tree(&self) -> &GitTreeOid {
        &self.candidate_tree
    }

    pub const fn source_commit(&self) -> &GitCommitOid {
        &self.source_commit
    }

    pub fn runtime_state<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.runtime_state.downcast_ref::<T>()
    }
}

impl fmt::Debug for DeliveryPreparedPreflight {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryPreparedPreflight(<opaque>)")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryRuntimeAuthenticationOutcome {
    Ready(DeliveryRuntimeAuthentication),
    KnownFailure {
        authentication: DeliveryRuntimeAuthentication,
        failure: DeliveryRuntimeFailure,
    },
}

impl DeliveryRuntimeAuthenticationOutcome {
    pub(crate) fn into_parts(
        self,
    ) -> (
        DeliveryRuntimeAuthentication,
        Option<DeliveryRuntimeFailure>,
    ) {
        match self {
            Self::Ready(authentication) => (authentication, None),
            Self::KnownFailure {
                authentication,
                failure,
            } => (authentication, Some(failure)),
        }
    }
}

#[async_trait::async_trait]
pub trait DeliveryRuntimeSession: sealed::RuntimeSession + Send + Sync + 'static {
    /// Performs a fresh read-only runtime observation for GET projection.
    async fn observe(&self) -> Result<DeliveryRuntimeObservation, DeliveryRuntimeFailure>;

    /// Authenticates the exact target command and returns persistence-only
    /// facts derived from the retained source/target capabilities.
    async fn authenticate_preflight(
        &self,
        command: &PreflightCommandRequest,
    ) -> Result<DeliveryRuntimeAuthenticationOutcome, DeliveryRuntimeFailure>;

    /// Creates the deterministic dangling candidate/source objects only after
    /// the application has durably persisted PreflightPending.
    async fn prepare_preflight(&self) -> Result<DeliveryPreparedPreflight, DeliveryRuntimeFailure>;

    /// Runs the fresh target-side merge-tree check against the exact opaque
    /// preparation returned by this session.
    async fn run_preflight(
        &self,
        prepared: &DeliveryPreparedPreflight,
    ) -> Result<MergePreflightResult, DeliveryRuntimeFailure>;
}

#[async_trait::async_trait]
pub trait DeliveryRuntimeRegistry: sealed::RuntimeRegistry + Send + Sync + 'static {
    async fn open_session(
        &self,
        snapshot: &DeliveryEligibilitySnapshot,
    ) -> Result<Arc<dyn DeliveryRuntimeSession>, DeliveryRuntimeFailure>;
}
