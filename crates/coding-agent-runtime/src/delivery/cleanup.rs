//! Phase-bound worktree cleanup and recovery.
//!
//! Cleanup deliberately has three independent authorization boundaries.  A
//! capability minted for one durable phase cannot construct either command
//! admitted by another phase, and every retry starts with a fresh observation
//! of the application-owned worktree topology.

mod branch;
mod recovery;

pub use branch::{
    DeliveryBranchCleanupIntent, DeliveryBranchCleanupRefreshProof,
    DeliveryDeletePendingAuthorizer, DeliveryDeletePendingCapability,
    DeliveryDeletePendingDisposition, authorize_persisted_delivery_branch_delete,
};
pub use recovery::{
    DeliveryBranchCleanupRecoveryBindingOutcome, DeliveryWorktreeCleanupRecoveryBindingOutcome,
};

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::command::DeliveryCleanupCommands;
use super::observation::{
    DeliveryCommandExecutor, DeliveryCommittedSourceCleanupCaptureError,
    DeliveryCommittedSourceCleanupObservation, DeliveryCommittedSourceCleanupProof,
    parse_object_id,
};
use super::output::DeliveryCommandExit;
use super::sandbox::DeliveryCommandSandbox;
use super::{
    DeliveryCandidateTree, DeliveryCommitOid, DeliverySourceCapability, DeliverySourceCommit,
    DeliverySourceCommitInput, DeliverySourceError, DeliverySourceLimits,
    DeliverySourcePendingState, DeliverySourceProvisioner, DeliverySourceRecoveryIntent,
    ProbedDeliveryGit,
};
use crate::command_policy::ExecutionDirectory;
use crate::process_liveness::{
    ProcessCleanupProof, ProcessLivenessError, ProcessLivenessScope, SealedProcessLivenessScope,
};
use crate::process_supervisor::{PlatformEnvironment, ProcessLimits};
use crate::worktree::{
    CleanupAbsentAuthentication, CleanupPresentAuthentication, CleanupTopologyIntentV1,
    CleanupTopologyObservation, CleanupWorktreeAuthenticator, LinkedWorktreeAuthenticator,
};
use crate::{WorktreeError, WorktreeProvisioner, WorktreeReservation};

/// Opaque cleanup evidence captured from a live locked source or rebound by
/// the trusted persisted adapter. Raw Store strings are never accepted as a
/// replacement for this value.
#[derive(Clone)]
pub struct DeliveryWorktreeCleanupIntent {
    inner: Arc<DeliveryWorktreeCleanupIntentInner>,
}

struct DeliveryWorktreeCleanupIntentInner {
    repository_probe: Arc<ProbedDeliveryGit>,
    reservation: WorktreeReservation,
    source: DeliverySourceRecoveryIntent,
    source_cleanup: Option<DeliveryCommittedSourceCleanupProof>,
    expected_source_commit: DeliveryCommitOid,
    source_branch: String,
    topology: CleanupTopologyIntentV1,
}

impl DeliveryWorktreeCleanupIntent {
    /// Compares only opaque runtime provenance. This lets a trusted
    /// application authorizer bind a phase transition to the exact in-memory
    /// intent it durably accepted without exposing paths, refs, object IDs,
    /// or directory identity digests. The trusted persisted adapter returns
    /// the same opaque shape only after fresh topology and object proofs.
    pub fn is_same_runtime_intent(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl fmt::Debug for DeliveryWorktreeCleanupIntent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryWorktreeCleanupIntent(<opaque>)")
    }
}

/// Durable `UnlockPending` authorization.  Implementations normally bind this
/// call to the Store operation/version that accepted the user's one cleanup
/// request.
#[async_trait]
pub trait DeliveryUnlockPendingAuthorizer: Send + Sync {
    type Error: Send;

    async fn authorize_persisted_unlock_pending(
        &self,
        intent: &DeliveryWorktreeCleanupIntent,
    ) -> Result<(), Self::Error>;
}

/// Durable `UnlockedPendingRemove` authorization.
#[async_trait]
pub trait DeliveryUnlockedPendingRemoveAuthorizer: Send + Sync {
    type Error: Send;

    async fn authorize_persisted_unlocked_pending_remove(
        &self,
        intent: &DeliveryWorktreeCleanupIntent,
    ) -> Result<(), Self::Error>;
}

/// Durable `RemovePending` authorization.
#[async_trait]
pub trait DeliveryRemovePendingAuthorizer: Send + Sync {
    type Error: Send;

    async fn authorize_persisted_remove_pending(
        &self,
        intent: &DeliveryWorktreeCleanupIntent,
    ) -> Result<(), Self::Error>;
}

/// Opaque command capability for exactly one persisted `UnlockPending` phase.
pub struct DeliveryUnlockPendingCapability {
    intent: DeliveryWorktreeCleanupIntent,
}

/// Opaque transition capability for exactly one persisted
/// `UnlockedPendingRemove` phase.
pub struct DeliveryUnlockedPendingRemoveCapability {
    intent: DeliveryWorktreeCleanupIntent,
}

/// Opaque command capability for exactly one persisted `RemovePending` phase.
pub struct DeliveryRemovePendingCapability {
    intent: DeliveryWorktreeCleanupIntent,
}

macro_rules! opaque_capability_debug {
    ($type:ty, $name:literal) => {
        impl fmt::Debug for $type {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str($name)
            }
        }
    };
}

opaque_capability_debug!(
    DeliveryUnlockPendingCapability,
    "DeliveryUnlockPendingCapability(<opaque>)"
);
opaque_capability_debug!(
    DeliveryUnlockedPendingRemoveCapability,
    "DeliveryUnlockedPendingRemoveCapability(<opaque>)"
);
opaque_capability_debug!(
    DeliveryRemovePendingCapability,
    "DeliveryRemovePendingCapability(<opaque>)"
);

pub async fn authorize_persisted_delivery_unlock<A>(
    authorizer: &A,
    intent: DeliveryWorktreeCleanupIntent,
) -> Result<DeliveryUnlockPendingCapability, A::Error>
where
    A: DeliveryUnlockPendingAuthorizer,
{
    authorizer
        .authorize_persisted_unlock_pending(&intent)
        .await?;
    Ok(DeliveryUnlockPendingCapability { intent })
}

pub async fn authorize_persisted_delivery_unlocked_pending_remove<A>(
    authorizer: &A,
    intent: DeliveryWorktreeCleanupIntent,
) -> Result<DeliveryUnlockedPendingRemoveCapability, A::Error>
where
    A: DeliveryUnlockedPendingRemoveAuthorizer,
{
    authorizer
        .authorize_persisted_unlocked_pending_remove(&intent)
        .await?;
    Ok(DeliveryUnlockedPendingRemoveCapability { intent })
}

pub async fn authorize_persisted_delivery_remove<A>(
    authorizer: &A,
    intent: DeliveryWorktreeCleanupIntent,
) -> Result<DeliveryRemovePendingCapability, A::Error>
where
    A: DeliveryRemovePendingAuthorizer,
{
    authorizer
        .authorize_persisted_remove_pending(&intent)
        .await?;
    Ok(DeliveryRemovePendingCapability { intent })
}

/// Query-first result for `UnlockPending`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryUnlockPendingDisposition {
    RetryExactUnlock,
    UnlockApplied,
    ReconciliationRequired,
}

/// Query-first result for `UnlockedPendingRemove`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryUnlockedPendingRemoveDisposition {
    EnterRemovePending,
    ReconciliationRequired,
}

/// Query-first result for `RemovePending`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryRemovePendingDisposition {
    RetryExactRemove,
    Removed,
    KnownNotAppliedDirty,
    ReconciliationRequired,
}

/// Durable worktree-cleanup phase being rebound after acceptance. The runtime
/// uses this only to admit an authenticated observation to the existing
/// query-first phase classifier; it does not construct mutation authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryWorktreeCleanupRecoveryPhase {
    UnlockPending,
    UnlockedPendingRemove,
    RemovePending,
}

/// Stable, path-free cleanup failures.  Git paths, refs, object IDs, child
/// output and identity digests never cross this boundary.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum DeliveryWorktreeCleanupError {
    InvalidConfiguration,
    AuthenticationChanged,
    SourceChanged,
    Dirty,
    ProcessStateUnproven,
    Cancelled,
    TimedOut,
    CommandFailed,
    ChildOutcomeUnknown,
    ProcessCleanupUnproven,
    Internal,
}

impl DeliveryWorktreeCleanupError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::AuthenticationChanged => "WORKTREE_IDENTITY_MISMATCH",
            Self::SourceChanged => "DELIVERY_SOURCE_CHANGED",
            Self::Dirty => "TARGET_WORKTREE_DIRTY",
            Self::ProcessStateUnproven => "PROCESS_TREE_STATE_UNPROVEN",
            Self::Cancelled => "COMMAND_CANCELLED",
            Self::TimedOut => "COMMAND_TIMED_OUT",
            Self::CommandFailed => "DELIVERY_CLEANUP_COMMAND_FAILED",
            Self::ChildOutcomeUnknown => "DELIVERY_RECONCILIATION_REQUIRED",
            Self::ProcessCleanupUnproven => "PROCESS_TREE_CLEANUP_FAILED",
            Self::InvalidConfiguration | Self::Internal => "DELIVERY_CLEANUP_INVALID",
        }
    }
}

impl fmt::Debug for DeliveryWorktreeCleanupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryWorktreeCleanupError(<redacted>)")
    }
}

impl fmt::Display for DeliveryWorktreeCleanupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("delivery worktree cleanup failed")
    }
}

impl std::error::Error for DeliveryWorktreeCleanupError {}

impl From<DeliverySourceError> for DeliveryWorktreeCleanupError {
    fn from(error: DeliverySourceError) -> Self {
        match error {
            DeliverySourceError::SourceChanged | DeliverySourceError::UnsafeIndex => {
                Self::SourceChanged
            }
            DeliverySourceError::AuthenticationChanged => Self::AuthenticationChanged,
            DeliverySourceError::UnsafeGitConfiguration => Self::SourceChanged,
            DeliverySourceError::Cancelled => Self::Cancelled,
            DeliverySourceError::TimedOut => Self::TimedOut,
            DeliverySourceError::ChildOutcomeUnknown => Self::ChildOutcomeUnknown,
            DeliverySourceError::ProcessCleanupUnproven
            | DeliverySourceError::SandboxCleanupUnproven => Self::ProcessCleanupUnproven,
            DeliverySourceError::CommandFailed => Self::CommandFailed,
            DeliverySourceError::InvalidLimits
            | DeliverySourceError::InvalidEnvironment
            | DeliverySourceError::CommandPolicy
            | DeliverySourceError::BoundsExceeded
            | DeliverySourceError::SandboxUnavailable => Self::InvalidConfiguration,
            DeliverySourceError::Internal => Self::Internal,
        }
    }
}

impl From<DeliveryCommittedSourceCleanupCaptureError> for DeliveryWorktreeCleanupError {
    fn from(error: DeliveryCommittedSourceCleanupCaptureError) -> Self {
        match error {
            DeliveryCommittedSourceCleanupCaptureError::Dirty => Self::Dirty,
            DeliveryCommittedSourceCleanupCaptureError::Source(error) => error.into(),
        }
    }
}

impl From<WorktreeError> for DeliveryWorktreeCleanupError {
    fn from(error: WorktreeError) -> Self {
        match error {
            WorktreeError::Cancelled => Self::Cancelled,
            WorktreeError::TimedOut => Self::TimedOut,
            WorktreeError::Process(error) if error.process_cleanup_is_unproven() => {
                Self::ProcessCleanupUnproven
            }
            WorktreeError::InvalidLimits | WorktreeError::InvalidEnvironment => {
                Self::InvalidConfiguration
            }
            WorktreeError::Io(_)
            | WorktreeError::CommandPolicy(_)
            | WorktreeError::InvalidIdentity
            | WorktreeError::InvalidReservation
            | WorktreeError::CommonGitIdentityUnavailable
            | WorktreeError::CommonGitIdentityMismatch
            | WorktreeError::InvalidRepository
            | WorktreeError::CargoWorkspaceOutsideRepository
            | WorktreeError::DestinationConflict
            | WorktreeError::ArtifactPathInvalid
            | WorktreeError::BranchConflict
            | WorktreeError::LinkedMetadataInvalid
            | WorktreeError::PostconditionFailed
            | WorktreeError::WorktreeContentChanged
            | WorktreeError::PartialCreation
            | WorktreeError::InconsistentArtifact
            | WorktreeError::NestedWorkspaceMissing => Self::AuthenticationChanged,
            WorktreeError::UnbornHead
            | WorktreeError::UnsafeGitConfiguration
            | WorktreeError::GitCommandFailed
            | WorktreeError::OutputInvalid
            | WorktreeError::Process(_)
            | WorktreeError::Cargo(_) => Self::CommandFailed,
        }
    }
}

impl From<ProcessLivenessError> for DeliveryWorktreeCleanupError {
    fn from(_: ProcessLivenessError) -> Self {
        Self::ProcessStateUnproven
    }
}

/// Runtime resources shared by all three cleanup phases.  The linked-source
/// authenticator is used only for the initial locked capture; later phases use
/// the independent cleanup topology authenticator.
pub struct DeliveryWorktreeCleanupProvisioner {
    probe: Arc<ProbedDeliveryGit>,
    source_authenticator: LinkedWorktreeAuthenticator,
    topology_authenticator: CleanupWorktreeAuthenticator,
    sandbox: Arc<DeliveryCommandSandbox>,
    platform: PlatformEnvironment,
    executor: DeliveryCommandExecutor,
    worker_process_scope: ProcessLivenessScope,
    limits: DeliverySourceLimits,
    #[cfg(feature = "test-support")]
    cleanup_boundary_hook: Option<Arc<dyn Fn(&'static str) + Send + Sync + 'static>>,
}

impl DeliveryWorktreeCleanupProvisioner {
    #[allow(clippy::too_many_arguments)]
    pub fn from_worktree_provisioner(
        worktrees: &WorktreeProvisioner,
        probe: Arc<ProbedDeliveryGit>,
        temporary_directory: impl AsRef<Path>,
        delivery_process_scope: ProcessLivenessScope,
        process_limits: ProcessLimits,
        limits: DeliverySourceLimits,
    ) -> Result<Self, DeliveryWorktreeCleanupError> {
        let worker_process_scope = worktrees.process_liveness_scope().clone();
        if !worker_process_scope.is_task_scope()
            || !delivery_process_scope.is_task_scope()
            || !worker_process_scope.is_same_instance(&delivery_process_scope)
            || worker_process_scope.is_same_scope(&delivery_process_scope)
        {
            return Err(DeliveryWorktreeCleanupError::InvalidConfiguration);
        }
        probe
            .verify_current_executable()
            .map_err(|_| DeliveryWorktreeCleanupError::AuthenticationChanged)?;
        let source_authenticator = worktrees
            .delivery_source_authenticator(probe.pinned_executable())
            .map_err(DeliveryWorktreeCleanupError::from)?;
        let topology_authenticator = worktrees
            .delivery_cleanup_authenticator(probe.pinned_executable())
            .map_err(DeliveryWorktreeCleanupError::from)?;
        let (temporary_path, temporary) =
            authenticated_temporary_directory(temporary_directory.as_ref())?;
        if !temporary.has_same_identity(probe.private_runtime()) {
            return Err(DeliveryWorktreeCleanupError::AuthenticationChanged);
        }
        let platform = delivery_platform_environment(temporary_path)?;
        let sandbox = Arc::new(DeliveryCommandSandbox::create(Arc::clone(
            probe.private_runtime(),
        ))?);
        sandbox.revalidate()?;
        Ok(Self {
            probe,
            source_authenticator,
            topology_authenticator,
            sandbox,
            platform,
            executor: DeliveryCommandExecutor::new(process_limits, delivery_process_scope),
            worker_process_scope,
            limits,
            #[cfg(feature = "test-support")]
            cleanup_boundary_hook: None,
        })
    }

    /// Captures the exact committed source and locked topology while consuming
    /// the live source capability.  Consuming it closes the retained
    /// source/admin/worktree handles before any later remove spawn.
    #[allow(clippy::too_many_arguments)]
    pub async fn capture_intent(
        &self,
        source_provisioner: &DeliverySourceProvisioner,
        reservation: &WorktreeReservation,
        source: DeliverySourceCapability,
        candidate: &DeliveryCandidateTree,
        source_commit: &DeliverySourceCommit,
        input: &DeliverySourceCommitInput,
        processes: &SealedProcessLivenessScope,
        cancellation: CancellationToken,
    ) -> Result<DeliveryWorktreeCleanupIntent, DeliveryWorktreeCleanupError> {
        self.require_process_cleanup(processes)?;
        source_provisioner
            .revalidate_preflight_committed_source(
                &source,
                candidate,
                source_commit,
                input,
                cancellation.clone(),
            )
            .await?;
        if !self.probe.shares_probed_authority_with(source.probe()) {
            return Err(DeliveryWorktreeCleanupError::AuthenticationChanged);
        }
        let repository_probe = Arc::clone(source.probe());
        let source_intent = DeliverySourceRecoveryIntent::from_source(
            DeliverySourcePendingState::CommitPending,
            &source,
            candidate,
            Some(source_commit),
            input.clone(),
        )?;
        let expected_source_commit =
            DeliveryCommitOid::try_new(source_commit.object_id(), repository_probe.object_format())
                .ok_or(DeliveryWorktreeCleanupError::AuthenticationChanged)?;
        let source_branch = source.branch_name().to_owned();
        let source_authentication = self.source_authenticator.authenticate(reservation)?;
        let topology = self
            .topology_authenticator
            .capture_locked_identity(reservation, source_authentication)?;
        drop(source);

        let CleanupTopologyObservation::Locked(committed_present) =
            self.topology_authenticator.observe_topology(&topology)
        else {
            return Err(DeliveryWorktreeCleanupError::AuthenticationChanged);
        };
        let source_cleanup = source_provisioner
            .capture_committed_source_cleanup_proof(
                &committed_present,
                reservation,
                &source_intent,
                cancellation.clone(),
            )
            .await?;
        drop(committed_present);

        // Reopen once after every handle from the accepted source capability
        // has closed.  This is the closing source observation for capture.
        let closing = source_provisioner
            .open_delivery_source_for_recovery(reservation, &source_intent, cancellation.clone())
            .await?;
        if source_provisioner
            .classify_source_recovery(&closing, cancellation)
            .await?
            != super::DeliverySourceRecoveryDisposition::Applied
        {
            return Err(DeliveryWorktreeCleanupError::SourceChanged);
        }
        drop(closing);
        if !matches!(
            self.topology_authenticator.observe_topology(&topology),
            crate::worktree::CleanupTopologyObservation::Locked(_)
        ) {
            return Err(DeliveryWorktreeCleanupError::AuthenticationChanged);
        }
        self.require_process_cleanup(processes)?;

        Ok(DeliveryWorktreeCleanupIntent {
            inner: Arc::new(DeliveryWorktreeCleanupIntentInner {
                repository_probe,
                reservation: reservation.clone(),
                source: source_intent,
                source_cleanup: Some(source_cleanup),
                expected_source_commit,
                source_branch,
                topology,
            }),
        })
    }

    /// Classifies one persisted `UnlockPending` operation without mutation.
    pub async fn classify_delivery_unlock_pending(
        &self,
        source_provisioner: &DeliverySourceProvisioner,
        capability: &DeliveryUnlockPendingCapability,
        processes: &SealedProcessLivenessScope,
        cancellation: CancellationToken,
    ) -> Result<DeliveryUnlockPendingDisposition, DeliveryWorktreeCleanupError> {
        let observation = self
            .observe_cleanup_state(
                source_provisioner,
                &capability.intent,
                processes,
                cancellation,
            )
            .await?;
        Ok(unlock_decision(observation.fact()))
    }

    /// Classifies the Store-only bridge phase after unlock.  This method can
    /// authorize only the durable transition into `RemovePending`; it owns no
    /// command-spawn branch.
    pub async fn classify_delivery_unlocked_pending_remove(
        &self,
        source_provisioner: &DeliverySourceProvisioner,
        capability: &DeliveryUnlockedPendingRemoveCapability,
        processes: &SealedProcessLivenessScope,
        cancellation: CancellationToken,
    ) -> Result<DeliveryUnlockedPendingRemoveDisposition, DeliveryWorktreeCleanupError> {
        let observation = self
            .observe_cleanup_state(
                source_provisioner,
                &capability.intent,
                processes,
                cancellation,
            )
            .await?;
        Ok(unlocked_pending_remove_decision(observation.fact()))
    }

    /// Classifies one persisted `RemovePending` operation without mutation.
    pub async fn classify_delivery_remove_pending(
        &self,
        source_provisioner: &DeliverySourceProvisioner,
        capability: &DeliveryRemovePendingCapability,
        processes: &SealedProcessLivenessScope,
        cancellation: CancellationToken,
    ) -> Result<DeliveryRemovePendingDisposition, DeliveryWorktreeCleanupError> {
        let observation = self
            .observe_cleanup_state(
                source_provisioner,
                &capability.intent,
                processes,
                cancellation,
            )
            .await?;
        Ok(remove_decision(observation.fact()))
    }

    /// Query-first exact unlock.  The transient source context is consumed
    /// before the command is constructed, so the child never inherits a
    /// removable worktree/admin directory lease.
    pub async fn retry_delivery_unlock_pending(
        &self,
        source_provisioner: &DeliverySourceProvisioner,
        capability: DeliveryUnlockPendingCapability,
        processes: &SealedProcessLivenessScope,
        cancellation: CancellationToken,
    ) -> Result<DeliveryUnlockPendingDisposition, DeliveryWorktreeCleanupError> {
        let observed = self
            .observe_cleanup_state(
                source_provisioner,
                &capability.intent,
                processes,
                cancellation.clone(),
            )
            .await?;
        let first_present = match observed {
            AuthenticatedCleanupObservation::LockedClean(present)
            | AuthenticatedCleanupObservation::LockedDirty(present) => present,
            other => return Ok(unlock_decision(other.fact())),
        };
        drop(first_present.into_target());
        self.run_cleanup_boundary_hook("after-query-before-unlock-spawn");
        let rechecked = self
            .observe_cleanup_state(
                source_provisioner,
                &capability.intent,
                processes,
                cancellation.clone(),
            )
            .await?;
        let present = match rechecked {
            AuthenticatedCleanupObservation::LockedClean(present)
            | AuthenticatedCleanupObservation::LockedDirty(present) => present,
            other => return Ok(unlock_decision(other.fact())),
        };
        let target = present.into_target();
        let commands = self.cleanup_commands(&target, &capability.intent)?;
        let command = commands.unlock()?;
        self.run_cleanup_boundary_hook("before-actual-unlock-spawn");
        let child = self
            .executor
            .run(
                command,
                cancellation.clone(),
                self.limits.max_status_bytes(),
            )
            .await;
        drop(commands);
        drop(target);
        // Once the child has started, request cancellation no longer decides
        // the durable fact. Use an independent recovery observation after all
        // child/target authority has been released so timeout, cancellation,
        // nonzero exit and reply loss can still be classified from Git state.
        let closing = match fresh_closing_observation_after_child(&child, |closing_cancellation| {
            self.observe_cleanup_state(
                source_provisioner,
                &capability.intent,
                processes,
                closing_cancellation,
            )
        })
        .await?
        {
            ChildClosingObservation::Observed(closing) => closing,
            ChildClosingObservation::ReconciliationRequired => {
                return Ok(DeliveryUnlockPendingDisposition::ReconciliationRequired);
            }
        };
        Ok(match closing.fact() {
            CleanupObservation::UnlockedClean | CleanupObservation::UnlockedDirty => {
                DeliveryUnlockPendingDisposition::UnlockApplied
            }
            CleanupObservation::LockedClean if child.is_err() => {
                DeliveryUnlockPendingDisposition::RetryExactUnlock
            }
            CleanupObservation::LockedDirty if child.is_err() => {
                DeliveryUnlockPendingDisposition::RetryExactUnlock
            }
            CleanupObservation::LockedClean
            | CleanupObservation::LockedDirty
            | CleanupObservation::AbsentExact
            | CleanupObservation::Inconsistent => {
                DeliveryUnlockPendingDisposition::ReconciliationRequired
            }
        })
    }

    /// Query-first exact non-force remove.  No other phase capability can
    /// reach this spawn branch.
    pub async fn retry_delivery_remove_pending(
        &self,
        source_provisioner: &DeliverySourceProvisioner,
        capability: DeliveryRemovePendingCapability,
        processes: &SealedProcessLivenessScope,
        cancellation: CancellationToken,
    ) -> Result<DeliveryRemovePendingDisposition, DeliveryWorktreeCleanupError> {
        let observed = self
            .observe_cleanup_state(
                source_provisioner,
                &capability.intent,
                processes,
                cancellation.clone(),
            )
            .await?;
        let first_present = match observed {
            AuthenticatedCleanupObservation::UnlockedClean(present) => present,
            other => return Ok(remove_decision(other.fact())),
        };
        // Keep the authenticated worktree/admin handles alive across every
        // deterministic adversarial boundary. The final full observation
        // below is the only authority allowed to construct the remove child.
        self.run_cleanup_boundary_hook("after-query-before-remove-spawn");
        self.run_cleanup_boundary_hook("before-actual-remove-spawn");
        drop(first_present);
        let rechecked = self
            .observe_cleanup_state(
                source_provisioner,
                &capability.intent,
                processes,
                cancellation.clone(),
            )
            .await?;
        let present = match rechecked {
            AuthenticatedCleanupObservation::UnlockedClean(present) => present,
            other => return Ok(remove_decision(other.fact())),
        };
        // Construct the fixed command while the fresh no-follow capability is
        // still retained. Only after command construction succeeds may the
        // removable handles be released for Windows and the child admitted.
        let commands = self.cleanup_commands(present.target(), &capability.intent)?;
        let command = commands.remove()?;
        drop(commands);
        drop(present);
        let child = self
            .executor
            .run(
                command,
                cancellation.clone(),
                self.limits.max_status_bytes(),
            )
            .await;
        let closing = match fresh_closing_observation_after_child(&child, |closing_cancellation| {
            self.observe_cleanup_state(
                source_provisioner,
                &capability.intent,
                processes,
                closing_cancellation,
            )
        })
        .await?
        {
            ChildClosingObservation::Observed(closing) => closing,
            ChildClosingObservation::ReconciliationRequired => {
                return Ok(DeliveryRemovePendingDisposition::ReconciliationRequired);
            }
        };
        Ok(remove_after_child_decision(closing.fact(), child.is_err()))
    }

    async fn observe_cleanup_state(
        &self,
        source_provisioner: &DeliverySourceProvisioner,
        intent: &DeliveryWorktreeCleanupIntent,
        processes: &SealedProcessLivenessScope,
        cancellation: CancellationToken,
    ) -> Result<AuthenticatedCleanupObservation, DeliveryWorktreeCleanupError> {
        if cancellation.is_cancelled() {
            return Err(DeliveryWorktreeCleanupError::Cancelled);
        }
        if self.require_process_cleanup(processes).is_err() {
            return Ok(AuthenticatedCleanupObservation::Inconsistent);
        }
        match self
            .topology_authenticator
            .observe_topology(&intent.inner.topology)
        {
            CleanupTopologyObservation::Locked(first) => {
                self.observe_present_sequence(
                    source_provisioner,
                    intent,
                    processes,
                    PresentKind::Locked,
                    first,
                    cancellation,
                )
                .await
            }
            CleanupTopologyObservation::Unlocked(first) => {
                self.observe_present_sequence(
                    source_provisioner,
                    intent,
                    processes,
                    PresentKind::Unlocked,
                    first,
                    cancellation,
                )
                .await
            }
            CleanupTopologyObservation::Absent(first) => {
                self.observe_absent_sequence(intent, processes, first, cancellation)
                    .await
            }
            CleanupTopologyObservation::Inconsistent | CleanupTopologyObservation::Unavailable => {
                Ok(AuthenticatedCleanupObservation::Inconsistent)
            }
        }
    }

    async fn observe_present_sequence(
        &self,
        source_provisioner: &DeliverySourceProvisioner,
        intent: &DeliveryWorktreeCleanupIntent,
        processes: &SealedProcessLivenessScope,
        kind: PresentKind,
        first: CleanupPresentAuthentication,
        cancellation: CancellationToken,
    ) -> Result<AuthenticatedCleanupObservation, DeliveryWorktreeCleanupError> {
        let first_source = self
            .observe_present_source(source_provisioner, intent, &first, cancellation.clone())
            .await?;
        drop(first);
        if self.require_process_cleanup(processes).is_err() {
            return Ok(AuthenticatedCleanupObservation::Inconsistent);
        }

        let Some(closing) = self.observe_present_kind(intent, kind) else {
            return Ok(AuthenticatedCleanupObservation::Inconsistent);
        };
        let closing_source = self
            .observe_present_source(source_provisioner, intent, &closing, cancellation)
            .await?;
        if first_source != closing_source || self.require_process_cleanup(processes).is_err() {
            return Ok(AuthenticatedCleanupObservation::Inconsistent);
        }

        Ok(match (kind, closing_source) {
            (PresentKind::Locked, DeliveryCommittedSourceCleanupObservation::ExactClean) => {
                AuthenticatedCleanupObservation::LockedClean(closing)
            }
            (PresentKind::Locked, DeliveryCommittedSourceCleanupObservation::ExactDirty) => {
                AuthenticatedCleanupObservation::LockedDirty(closing)
            }
            (PresentKind::Unlocked, DeliveryCommittedSourceCleanupObservation::ExactClean) => {
                AuthenticatedCleanupObservation::UnlockedClean(closing)
            }
            (PresentKind::Unlocked, DeliveryCommittedSourceCleanupObservation::ExactDirty) => {
                drop(closing);
                AuthenticatedCleanupObservation::UnlockedDirty
            }
            (_, DeliveryCommittedSourceCleanupObservation::Inconsistent) => {
                AuthenticatedCleanupObservation::Inconsistent
            }
        })
    }

    async fn observe_absent_sequence(
        &self,
        intent: &DeliveryWorktreeCleanupIntent,
        processes: &SealedProcessLivenessScope,
        first: CleanupAbsentAuthentication,
        cancellation: CancellationToken,
    ) -> Result<AuthenticatedCleanupObservation, DeliveryWorktreeCleanupError> {
        let first_ref_exact = self
            .source_ref_is_exact(first.target(), intent, cancellation.clone())
            .await?;
        let first_process_exact = self.require_process_cleanup(processes).is_ok();
        if !first_ref_exact || !first_process_exact {
            return Ok(AuthenticatedCleanupObservation::Inconsistent);
        }
        drop(first);
        let CleanupTopologyObservation::Absent(closing) = self
            .topology_authenticator
            .observe_topology(&intent.inner.topology)
        else {
            return Ok(AuthenticatedCleanupObservation::Inconsistent);
        };
        let closing_ref_exact = self
            .source_ref_is_exact(closing.target(), intent, cancellation)
            .await?;
        let closing_process_exact = self.require_process_cleanup(processes).is_ok();
        if !closing_ref_exact || !closing_process_exact {
            return Ok(AuthenticatedCleanupObservation::Inconsistent);
        }
        drop(closing);
        Ok(AuthenticatedCleanupObservation::AbsentExact)
    }

    async fn observe_present_source(
        &self,
        source_provisioner: &DeliverySourceProvisioner,
        intent: &DeliveryWorktreeCleanupIntent,
        present: &CleanupPresentAuthentication,
        cancellation: CancellationToken,
    ) -> Result<DeliveryCommittedSourceCleanupObservation, DeliveryWorktreeCleanupError> {
        let Some(expected_source_cleanup) = intent.inner.source_cleanup.as_ref() else {
            return Ok(DeliveryCommittedSourceCleanupObservation::Inconsistent);
        };
        let observation = source_provisioner
            .observe_committed_source_for_cleanup(
                present,
                &intent.inner.reservation,
                &intent.inner.source,
                expected_source_cleanup,
                cancellation.clone(),
            )
            .await?;
        if self
            .source_ref_is_exact(present.target(), intent, cancellation)
            .await?
        {
            Ok(observation)
        } else {
            Ok(DeliveryCommittedSourceCleanupObservation::Inconsistent)
        }
    }

    fn observe_present_kind(
        &self,
        intent: &DeliveryWorktreeCleanupIntent,
        kind: PresentKind,
    ) -> Option<CleanupPresentAuthentication> {
        match (
            kind,
            self.topology_authenticator
                .observe_topology(&intent.inner.topology),
        ) {
            (PresentKind::Locked, CleanupTopologyObservation::Locked(present))
            | (PresentKind::Unlocked, CleanupTopologyObservation::Unlocked(present)) => {
                Some(present)
            }
            _ => None,
        }
    }

    async fn source_ref_is_exact(
        &self,
        target: &crate::worktree::CleanupWorktreeTarget,
        intent: &DeliveryWorktreeCleanupIntent,
        cancellation: CancellationToken,
    ) -> Result<bool, DeliveryWorktreeCleanupError> {
        let commands = self.cleanup_commands(target, intent)?;
        let Some((symbolic, symbolic_output)) = cleanup_ref_protocol_observation(
            self.executor
                .run_machine_protocol(
                    commands.source_ref_symbolic()?,
                    cancellation.clone(),
                    self.limits.max_status_bytes(),
                )
                .await,
        )?
        else {
            return Ok(false);
        };
        match symbolic {
            DeliveryCommandExit::Matched if !symbolic_output.is_empty() => return Ok(false),
            DeliveryCommandExit::NotMatched if symbolic_output.is_empty() => {}
            DeliveryCommandExit::Matched | DeliveryCommandExit::NotMatched => {
                return Err(DeliveryWorktreeCleanupError::CommandFailed);
            }
        }
        let Some((outcome, output)) = cleanup_ref_protocol_observation(
            self.executor
                .run_machine_protocol(
                    commands.resolve_source_ref()?,
                    cancellation,
                    self.limits.max_status_bytes(),
                )
                .await,
        )?
        else {
            return Ok(false);
        };
        if outcome == DeliveryCommandExit::NotMatched {
            return if output.is_empty() {
                Ok(false)
            } else {
                Err(DeliveryWorktreeCleanupError::CommandFailed)
            };
        }
        let observed = parse_object_id(
            &output,
            intent
                .inner
                .repository_probe
                .object_format()
                .hexadecimal_length(),
        )?;
        Ok(observed == intent.inner.expected_source_commit.as_str())
    }

    fn cleanup_commands<'target>(
        &self,
        target: &'target crate::worktree::CleanupWorktreeTarget,
        intent: &DeliveryWorktreeCleanupIntent,
    ) -> Result<DeliveryCleanupCommands<'target>, DeliveryWorktreeCleanupError> {
        DeliveryCleanupCommands::try_new(
            &intent.inner.repository_probe,
            target,
            &intent.inner.source_branch,
            Arc::clone(&self.sandbox),
            &self.platform,
            self.limits.timeout(),
        )
        .map_err(Into::into)
    }

    /// Installs deterministic side-effect boundaries for integration tests.
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn set_cleanup_boundary_hook_for_tests(
        &mut self,
        hook: impl Fn(&'static str) + Send + Sync + 'static,
    ) {
        self.cleanup_boundary_hook = Some(Arc::new(hook));
    }

    fn run_cleanup_boundary_hook(&self, phase: &'static str) {
        #[cfg(feature = "test-support")]
        if let Some(hook) = &self.cleanup_boundary_hook {
            hook(phase);
        }
        #[cfg(not(feature = "test-support"))]
        let _ = phase;
    }

    fn require_process_cleanup(
        &self,
        processes: &SealedProcessLivenessScope,
    ) -> Result<(), DeliveryWorktreeCleanupError> {
        if !processes.is_bound_to(&self.worker_process_scope) {
            return Err(DeliveryWorktreeCleanupError::ProcessStateUnproven);
        }
        match processes.cleanup_proof()? {
            ProcessCleanupProof::Confirmed => Ok(()),
            ProcessCleanupProof::Held | ProcessCleanupProof::Unknown => {
                Err(DeliveryWorktreeCleanupError::ProcessStateUnproven)
            }
        }
    }
}

impl fmt::Debug for DeliveryWorktreeCleanupProvisioner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryWorktreeCleanupProvisioner(<opaque>)")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CleanupObservation {
    LockedClean,
    LockedDirty,
    UnlockedClean,
    UnlockedDirty,
    AbsentExact,
    Inconsistent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PresentKind {
    Locked,
    Unlocked,
}

enum AuthenticatedCleanupObservation {
    LockedClean(CleanupPresentAuthentication),
    LockedDirty(CleanupPresentAuthentication),
    UnlockedClean(CleanupPresentAuthentication),
    UnlockedDirty,
    AbsentExact,
    Inconsistent,
}

impl AuthenticatedCleanupObservation {
    const fn fact(&self) -> CleanupObservation {
        match self {
            Self::LockedClean(_) => CleanupObservation::LockedClean,
            Self::LockedDirty(_) => CleanupObservation::LockedDirty,
            Self::UnlockedClean(_) => CleanupObservation::UnlockedClean,
            Self::UnlockedDirty => CleanupObservation::UnlockedDirty,
            Self::AbsentExact => CleanupObservation::AbsentExact,
            Self::Inconsistent => CleanupObservation::Inconsistent,
        }
    }
}

enum ChildClosingObservation {
    Observed(Box<AuthenticatedCleanupObservation>),
    ReconciliationRequired,
}

/// Runs the closing observation under a token that is independent from the
/// caller's command token. Once a cleanup child may have started, cancellation,
/// timeout, and an unknown child outcome are observations about the request,
/// not durable Git facts. Conversely, an unproven process/sandbox cleanup must
/// not start another observation command under potentially live descendants.
async fn fresh_closing_observation_after_child<Observe, Observation>(
    child: &Result<Vec<u8>, DeliverySourceError>,
    observe: Observe,
) -> Result<ChildClosingObservation, DeliveryWorktreeCleanupError>
where
    Observe: FnOnce(CancellationToken) -> Observation,
    Observation: std::future::Future<
            Output = Result<AuthenticatedCleanupObservation, DeliveryWorktreeCleanupError>,
        >,
{
    if let Some(error) = child_cleanup_unproven_error(child) {
        return Err(error);
    }
    match observe(CancellationToken::new()).await {
        Ok(observation) => Ok(ChildClosingObservation::Observed(Box::new(observation))),
        Err(
            error @ (DeliveryWorktreeCleanupError::ProcessCleanupUnproven
            | DeliveryWorktreeCleanupError::ChildOutcomeUnknown),
        ) => Err(error),
        Err(_) => Ok(ChildClosingObservation::ReconciliationRequired),
    }
}

fn child_cleanup_unproven_error(
    result: &Result<Vec<u8>, DeliverySourceError>,
) -> Option<DeliveryWorktreeCleanupError> {
    match result {
        Err(
            DeliverySourceError::ProcessCleanupUnproven
            | DeliverySourceError::SandboxCleanupUnproven,
        ) => Some(DeliveryWorktreeCleanupError::ProcessCleanupUnproven),
        _ => None,
    }
}

const fn unlock_decision(observation: CleanupObservation) -> DeliveryUnlockPendingDisposition {
    match observation {
        CleanupObservation::LockedClean | CleanupObservation::LockedDirty => {
            DeliveryUnlockPendingDisposition::RetryExactUnlock
        }
        CleanupObservation::UnlockedClean | CleanupObservation::UnlockedDirty => {
            DeliveryUnlockPendingDisposition::UnlockApplied
        }
        CleanupObservation::AbsentExact | CleanupObservation::Inconsistent => {
            DeliveryUnlockPendingDisposition::ReconciliationRequired
        }
    }
}

const fn unlocked_pending_remove_decision(
    observation: CleanupObservation,
) -> DeliveryUnlockedPendingRemoveDisposition {
    match observation {
        CleanupObservation::UnlockedClean => {
            DeliveryUnlockedPendingRemoveDisposition::EnterRemovePending
        }
        CleanupObservation::LockedClean
        | CleanupObservation::LockedDirty
        | CleanupObservation::UnlockedDirty
        | CleanupObservation::AbsentExact
        | CleanupObservation::Inconsistent => {
            DeliveryUnlockedPendingRemoveDisposition::ReconciliationRequired
        }
    }
}

const fn remove_decision(observation: CleanupObservation) -> DeliveryRemovePendingDisposition {
    match observation {
        CleanupObservation::UnlockedClean => DeliveryRemovePendingDisposition::RetryExactRemove,
        CleanupObservation::UnlockedDirty => DeliveryRemovePendingDisposition::KnownNotAppliedDirty,
        CleanupObservation::AbsentExact => DeliveryRemovePendingDisposition::Removed,
        CleanupObservation::LockedClean
        | CleanupObservation::LockedDirty
        | CleanupObservation::Inconsistent => {
            DeliveryRemovePendingDisposition::ReconciliationRequired
        }
    }
}

const fn remove_after_child_decision(
    observation: CleanupObservation,
    child_failed: bool,
) -> DeliveryRemovePendingDisposition {
    match observation {
        CleanupObservation::AbsentExact => DeliveryRemovePendingDisposition::Removed,
        CleanupObservation::UnlockedClean if child_failed => {
            DeliveryRemovePendingDisposition::RetryExactRemove
        }
        CleanupObservation::LockedClean
        | CleanupObservation::LockedDirty
        | CleanupObservation::UnlockedClean
        | CleanupObservation::UnlockedDirty
        | CleanupObservation::Inconsistent => {
            DeliveryRemovePendingDisposition::ReconciliationRequired
        }
    }
}

fn cleanup_ref_protocol_observation(
    result: Result<(DeliveryCommandExit, Vec<u8>), DeliverySourceError>,
) -> Result<Option<(DeliveryCommandExit, Vec<u8>)>, DeliveryWorktreeCleanupError> {
    match result {
        Ok(observation) => Ok(Some(observation)),
        Err(DeliverySourceError::BoundsExceeded) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn authenticated_temporary_directory(
    path: &Path,
) -> Result<(PathBuf, Arc<ExecutionDirectory>), DeliveryWorktreeCleanupError> {
    let original = ExecutionDirectory::open(path)
        .map_err(|_| DeliveryWorktreeCleanupError::InvalidConfiguration)?;
    let canonical = std::fs::canonicalize(path)
        .map_err(|_| DeliveryWorktreeCleanupError::InvalidConfiguration)?;
    let canonical_directory = ExecutionDirectory::open(&canonical)
        .map_err(|_| DeliveryWorktreeCleanupError::InvalidConfiguration)?;
    if !original.has_same_identity(&canonical_directory) {
        return Err(DeliveryWorktreeCleanupError::AuthenticationChanged);
    }
    Ok((canonical, Arc::new(canonical_directory)))
}

fn delivery_platform_environment(
    path: PathBuf,
) -> Result<PlatformEnvironment, DeliveryWorktreeCleanupError> {
    #[cfg(windows)]
    let system_root = std::env::var_os("SYSTEMROOT")
        .or_else(|| std::env::var_os("WINDIR"))
        .map(PathBuf::from);
    #[cfg(unix)]
    let system_root = None;
    PlatformEnvironment::try_new(path, system_root)
        .map_err(|_| DeliveryWorktreeCleanupError::InvalidConfiguration)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn each_phase_accepts_only_its_exact_documented_facts() {
        assert_eq!(
            unlock_decision(CleanupObservation::LockedClean),
            DeliveryUnlockPendingDisposition::RetryExactUnlock
        );
        assert_eq!(
            unlock_decision(CleanupObservation::LockedDirty),
            DeliveryUnlockPendingDisposition::RetryExactUnlock
        );
        assert_eq!(
            unlock_decision(CleanupObservation::UnlockedClean),
            DeliveryUnlockPendingDisposition::UnlockApplied
        );
        assert_eq!(
            unlock_decision(CleanupObservation::UnlockedDirty),
            DeliveryUnlockPendingDisposition::UnlockApplied
        );
        assert_eq!(
            unlocked_pending_remove_decision(CleanupObservation::UnlockedClean),
            DeliveryUnlockedPendingRemoveDisposition::EnterRemovePending
        );
        assert_eq!(
            remove_decision(CleanupObservation::UnlockedClean),
            DeliveryRemovePendingDisposition::RetryExactRemove
        );
        assert_eq!(
            remove_decision(CleanupObservation::UnlockedDirty),
            DeliveryRemovePendingDisposition::KnownNotAppliedDirty
        );
        assert_eq!(
            remove_decision(CleanupObservation::AbsentExact),
            DeliveryRemovePendingDisposition::Removed
        );
    }

    #[test]
    fn cross_phase_and_inconsistent_facts_fail_closed() {
        for observation in [
            CleanupObservation::AbsentExact,
            CleanupObservation::Inconsistent,
        ] {
            assert_eq!(
                unlock_decision(observation),
                DeliveryUnlockPendingDisposition::ReconciliationRequired
            );
        }
        for observation in [
            CleanupObservation::LockedClean,
            CleanupObservation::LockedDirty,
            CleanupObservation::UnlockedDirty,
            CleanupObservation::AbsentExact,
            CleanupObservation::Inconsistent,
        ] {
            assert_eq!(
                unlocked_pending_remove_decision(observation),
                DeliveryUnlockedPendingRemoveDisposition::ReconciliationRequired
            );
        }
        for observation in [
            CleanupObservation::LockedClean,
            CleanupObservation::LockedDirty,
            CleanupObservation::Inconsistent,
        ] {
            assert_eq!(
                remove_decision(observation),
                DeliveryRemovePendingDisposition::ReconciliationRequired
            );
        }
    }

    #[test]
    fn post_child_dirty_state_is_not_proof_that_remove_had_no_effect() {
        assert_eq!(
            remove_after_child_decision(CleanupObservation::UnlockedDirty, true),
            DeliveryRemovePendingDisposition::ReconciliationRequired
        );
        assert_eq!(
            remove_after_child_decision(CleanupObservation::UnlockedClean, true),
            DeliveryRemovePendingDisposition::RetryExactRemove
        );
        assert_eq!(
            remove_after_child_decision(CleanupObservation::AbsentExact, false),
            DeliveryRemovePendingDisposition::Removed
        );
    }

    #[test]
    fn oversized_raw_ref_observation_is_an_unobservable_fact() {
        assert!(matches!(
            cleanup_ref_protocol_observation(Err(DeliverySourceError::BoundsExceeded)),
            Ok(None)
        ));
        assert!(matches!(
            cleanup_ref_protocol_observation(Err(DeliverySourceError::ProcessCleanupUnproven)),
            Err(DeliveryWorktreeCleanupError::ProcessCleanupUnproven)
        ));
    }

    #[tokio::test]
    async fn uncertain_started_child_outcomes_use_a_fresh_closing_observation_token() {
        for error in [
            DeliverySourceError::Cancelled,
            DeliverySourceError::TimedOut,
            DeliverySourceError::ChildOutcomeUnknown,
        ] {
            let command_cancellation = CancellationToken::new();
            command_cancellation.cancel();
            let observed = Arc::new(AtomicBool::new(false));
            let observed_by_closure = Arc::clone(&observed);

            let closing = fresh_closing_observation_after_child(
                &Err(error),
                move |closing_cancellation| async move {
                    assert!(command_cancellation.is_cancelled());
                    assert!(
                        !closing_cancellation.is_cancelled(),
                        "closing observation must not inherit command cancellation"
                    );
                    observed_by_closure.store(true, Ordering::SeqCst);
                    Ok(AuthenticatedCleanupObservation::Inconsistent)
                },
            )
            .await;

            assert!(observed.load(Ordering::SeqCst));
            assert!(matches!(
                closing,
                Ok(ChildClosingObservation::Observed(observation))
                    if matches!(observation.as_ref(), AuthenticatedCleanupObservation::Inconsistent)
            ));
        }
    }

    #[tokio::test]
    async fn cleanup_unproven_skips_closing_observation_and_preserves_the_typed_error() {
        for error in [
            DeliverySourceError::ProcessCleanupUnproven,
            DeliverySourceError::SandboxCleanupUnproven,
        ] {
            let observed = Arc::new(AtomicBool::new(false));
            let observed_by_closure = Arc::clone(&observed);

            let closing = fresh_closing_observation_after_child(
                &Err(error),
                move |_closing_cancellation| async move {
                    observed_by_closure.store(true, Ordering::SeqCst);
                    Ok(AuthenticatedCleanupObservation::Inconsistent)
                },
            )
            .await;

            assert!(!observed.load(Ordering::SeqCst));
            assert!(matches!(
                closing,
                Err(DeliveryWorktreeCleanupError::ProcessCleanupUnproven)
            ));
        }
    }

    #[tokio::test]
    async fn closing_observation_cleanup_unknown_preserves_ownership_error() {
        let closing = fresh_closing_observation_after_child(&Ok(Vec::new()), |_| async {
            Err(DeliveryWorktreeCleanupError::ChildOutcomeUnknown)
        })
        .await;

        assert!(matches!(
            closing,
            Err(DeliveryWorktreeCleanupError::ChildOutcomeUnknown)
        ));
    }
}
