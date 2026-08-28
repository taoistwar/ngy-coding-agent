//! Trusted restart adapters for the two independent cleanup actions.
//!
//! Persisted values remain inert until fresh worktree topology, source object,
//! registered target, and process-cleanup proofs all agree. These binders run
//! no mutation and expose neither raw paths nor command construction.

use std::fmt;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use super::{
    CleanupObservation, DeliveryBranchCleanupIntent, DeliveryWorktreeCleanupError,
    DeliveryWorktreeCleanupIntent, DeliveryWorktreeCleanupIntentInner,
    DeliveryWorktreeCleanupProvisioner, DeliveryWorktreeCleanupRecoveryPhase,
};
use crate::WorktreeReservation;
use crate::delivery::observation::DeliveryCommittedSourceCleanupCaptureError;
use crate::delivery::source_commit::verify_batched_source_commit;
use crate::delivery::{
    DeliveryPersistedSourceRecovery, DeliveryPersistedSourceState, DeliveryPersistedTargetRecovery,
    DeliverySourceError, DeliverySourceProvisioner, DeliverySourceRecoveryIntent,
    DeliveryTargetError, DeliveryTargetProvisioner, DeliveryTargetRecoveryBindingOutcome,
};
use crate::process_liveness::SealedProcessLivenessScope;
use crate::worktree::CleanupTopologyObservation;

#[derive(Clone, Copy)]
enum WorktreeCleanupBindingPurpose {
    Acceptance,
    Recovery(DeliveryWorktreeCleanupRecoveryPhase),
    BranchCleanup,
}

impl WorktreeCleanupBindingPurpose {
    const fn accepts_authenticated_dirty(self) -> bool {
        !matches!(self, Self::Acceptance)
    }

    const fn accepts_closing(self, observation: CleanupObservation) -> bool {
        match self {
            Self::Acceptance => matches!(
                observation,
                CleanupObservation::LockedClean
                    | CleanupObservation::UnlockedClean
                    | CleanupObservation::AbsentExact
            ),
            // Every persisted worktree phase is query-first. Once identity is
            // exact, its phase-specific classifier decides clean/dirty/absent;
            // no mutation capability is created by this binding alone.
            Self::Recovery(
                DeliveryWorktreeCleanupRecoveryPhase::UnlockPending
                | DeliveryWorktreeCleanupRecoveryPhase::UnlockedPendingRemove
                | DeliveryWorktreeCleanupRecoveryPhase::RemovePending,
            ) => !matches!(observation, CleanupObservation::Inconsistent),
            Self::BranchCleanup => matches!(observation, CleanupObservation::AbsentExact),
        }
    }
}

macro_rules! worktree_binding_reconciliation {
    ($predicate:literal) => {{
        #[cfg(feature = "test-support")]
        eprintln!(
            "test-support delivery worktree cleanup binding rejected: predicate={}",
            $predicate
        );
        DeliveryWorktreeCleanupRecoveryBindingOutcome::ReconciliationRequired
    }};
}

macro_rules! worktree_binding_diagnostic {
    ($predicate:literal) => {
        #[cfg(feature = "test-support")]
        eprintln!(
            "test-support delivery worktree cleanup binding rejected: predicate={}",
            $predicate
        );
    };
}

/// Result of authenticating one persisted worktree-cleanup record. A bound
/// value is still inert until a fresh phase-specific Store authorizer mints the
/// corresponding unlock/remove capability.
///
/// ```compile_fail
/// use coding_agent_runtime::{
///     DeliveryPersistedSourceRecovery, DeliveryWorktreeCleanupIntent,
/// };
/// fn raw_store_values_have_no_cleanup_authority(
///     persisted: DeliveryPersistedSourceRecovery,
/// ) {
///     let _: DeliveryWorktreeCleanupIntent = persisted.into();
/// }
/// ```
pub enum DeliveryWorktreeCleanupRecoveryBindingOutcome {
    Bound(DeliveryWorktreeCleanupIntent),
    ReconciliationRequired,
}

impl fmt::Debug for DeliveryWorktreeCleanupRecoveryBindingOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bound(_) => formatter
                .write_str("DeliveryWorktreeCleanupRecoveryBindingOutcome::Bound(<opaque>)"),
            Self::ReconciliationRequired => formatter
                .write_str("DeliveryWorktreeCleanupRecoveryBindingOutcome::ReconciliationRequired"),
        }
    }
}

/// Result of independently authenticating the later source-branch deletion
/// action. The generation-one intent remains subject to the existing target
/// refresh/adoption gate before any exact delete capability can be used.
pub enum DeliveryBranchCleanupRecoveryBindingOutcome {
    Bound(DeliveryBranchCleanupIntent),
    ReconciliationRequired,
}

impl fmt::Debug for DeliveryBranchCleanupRecoveryBindingOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bound(_) => {
                formatter.write_str("DeliveryBranchCleanupRecoveryBindingOutcome::Bound(<opaque>)")
            }
            Self::ReconciliationRequired => formatter
                .write_str("DeliveryBranchCleanupRecoveryBindingOutcome::ReconciliationRequired"),
        }
    }
}

impl DeliveryWorktreeCleanupProvisioner {
    /// Performs the strict pre-receipt binding for a new cleanup request. A
    /// present source must be clean; authenticated dirty state is returned as
    /// a typed rejection before any cleanup intent is exposed.
    #[allow(clippy::too_many_arguments)]
    pub async fn bind_delivery_worktree_cleanup_acceptance(
        &self,
        source_provisioner: &DeliverySourceProvisioner,
        target_provisioner: &DeliveryTargetProvisioner,
        reservation: &WorktreeReservation,
        persisted: &DeliveryPersistedSourceRecovery,
        persisted_target: &DeliveryPersistedTargetRecovery,
        processes: &SealedProcessLivenessScope,
        cancellation: CancellationToken,
    ) -> Result<DeliveryWorktreeCleanupRecoveryBindingOutcome, DeliveryWorktreeCleanupError> {
        self.bind_authenticated_delivery_worktree_cleanup(
            WorktreeCleanupBindingPurpose::Acceptance,
            source_provisioner,
            target_provisioner,
            reservation,
            persisted,
            persisted_target,
            processes,
            cancellation,
        )
        .await
    }

    /// Authenticates a committed source Store snapshot against a fresh cleanup
    /// topology. Locked, unlocked, and already-absent scenes are admitted only
    /// when the exact common/admin identities and committed source object are
    /// re-proven. A clean still-present source also reproduces the persisted
    /// config/attributes digest from its fresh committed scene. An authenticated
    /// dirty source instead remains bound to that exact dirty scene and can be
    /// consumed only by the existing query-first phase classifier; it cannot
    /// become removal authority. Once exact removal has deleted the
    /// worktree-specific evidence, the historical digest remains a trusted
    /// Store fact while topology/ref/object checks close the absent scene. No
    /// cleanup phase authorization is minted here.
    // Keeping these independently authenticated inputs explicit prevents a
    // caller from mixing source, target, topology, or process-proof bundles.
    #[allow(clippy::too_many_arguments)]
    pub async fn bind_persisted_delivery_worktree_cleanup(
        &self,
        phase: DeliveryWorktreeCleanupRecoveryPhase,
        source_provisioner: &DeliverySourceProvisioner,
        target_provisioner: &DeliveryTargetProvisioner,
        reservation: &WorktreeReservation,
        persisted: &DeliveryPersistedSourceRecovery,
        persisted_target: &DeliveryPersistedTargetRecovery,
        processes: &SealedProcessLivenessScope,
        cancellation: CancellationToken,
    ) -> Result<DeliveryWorktreeCleanupRecoveryBindingOutcome, DeliveryWorktreeCleanupError> {
        self.bind_authenticated_delivery_worktree_cleanup(
            WorktreeCleanupBindingPurpose::Recovery(phase),
            source_provisioner,
            target_provisioner,
            reservation,
            persisted,
            persisted_target,
            processes,
            cancellation,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn bind_authenticated_delivery_worktree_cleanup(
        &self,
        purpose: WorktreeCleanupBindingPurpose,
        source_provisioner: &DeliverySourceProvisioner,
        target_provisioner: &DeliveryTargetProvisioner,
        reservation: &WorktreeReservation,
        persisted: &DeliveryPersistedSourceRecovery,
        persisted_target: &DeliveryPersistedTargetRecovery,
        processes: &SealedProcessLivenessScope,
        cancellation: CancellationToken,
    ) -> Result<DeliveryWorktreeCleanupRecoveryBindingOutcome, DeliveryWorktreeCleanupError> {
        if cancellation.is_cancelled() {
            return Err(DeliveryWorktreeCleanupError::Cancelled);
        }
        self.require_process_cleanup(processes)?;
        if persisted.state() != DeliveryPersistedSourceState::Committed {
            return Ok(worktree_binding_reconciliation!(
                "persisted_source_committed"
            ));
        }

        let topology = match self.topology_authenticator.bind_persisted_identity(
            reservation,
            persisted.common_git_identity_digest(),
            persisted.worktree_admin_identity_digest(),
        ) {
            Ok(topology) => topology,
            Err(_) => {
                return Ok(worktree_binding_reconciliation!("topology_identity"));
            }
        };
        let source = match DeliverySourceRecoveryIntent::from_persisted_cleanup(
            reservation,
            persisted,
            &topology,
        ) {
            Ok(source) => source,
            Err(_) => {
                return Ok(worktree_binding_reconciliation!("source_recovery_intent"));
            }
        };

        let target = match target_provisioner
            .bind_persisted_delivery_target_recovery(persisted_target, cancellation.clone())
            .await
        {
            Ok(DeliveryTargetRecoveryBindingOutcome::Bound(target)) => target,
            Ok(DeliveryTargetRecoveryBindingOutcome::ReconciliationRequired) => {
                return Ok(worktree_binding_reconciliation!("target_recovery"));
            }
            Err(error) => return Err(map_target_binding_error(error)),
        };
        if !self
            .persisted_source_commit_is_exact(
                &source,
                &target,
                reservation,
                persisted,
                processes,
                cancellation.clone(),
            )
            .await?
        {
            return Ok(worktree_binding_reconciliation!(
                "persisted_source_commit_exact"
            ));
        }
        let repository_probe = Arc::clone(target.target().probe());
        drop(target);

        let source_cleanup = match self.topology_authenticator.observe_topology(&topology) {
            CleanupTopologyObservation::Locked(present)
            | CleanupTopologyObservation::Unlocked(present) => {
                let captured = if purpose.accepts_authenticated_dirty() {
                    source_provisioner
                        .capture_committed_source_cleanup_recovery_proof(
                            &present,
                            reservation,
                            &source,
                            cancellation.clone(),
                        )
                        .await
                } else {
                    source_provisioner
                        .capture_committed_source_cleanup_proof(
                            &present,
                            reservation,
                            &source,
                            cancellation.clone(),
                        )
                        .await
                };
                let proof = match captured {
                    Ok(proof) => proof,
                    Err(error) => return worktree_source_binding_failure(error),
                };
                if !proof.is_authenticated_dirty()
                    && !proof.matches_persisted_config_attributes_digest(
                        persisted.source_config_attributes_digest(),
                    )
                {
                    return Ok(worktree_binding_reconciliation!(
                        "source_config_attributes_digest"
                    ));
                }
                drop(present);
                Some(proof)
            }
            CleanupTopologyObservation::Absent(absent) => {
                drop(absent);
                None
            }
            CleanupTopologyObservation::Inconsistent | CleanupTopologyObservation::Unavailable => {
                return Ok(worktree_binding_reconciliation!("closing_topology"));
            }
        };
        self.require_process_cleanup(processes)?;

        let intent = DeliveryWorktreeCleanupIntent {
            inner: Arc::new(DeliveryWorktreeCleanupIntentInner {
                repository_probe,
                reservation: reservation.clone(),
                source,
                source_cleanup,
                expected_source_commit: persisted
                    .expected_source_commit()
                    .expect("committed persisted source has an expected commit")
                    .clone(),
                source_branch: reservation.branch_name().to_owned(),
                topology,
            }),
        };
        let closing = match self
            .observe_cleanup_state(source_provisioner, &intent, processes, cancellation)
            .await
        {
            Ok(closing) => closing,
            Err(
                DeliveryWorktreeCleanupError::AuthenticationChanged
                | DeliveryWorktreeCleanupError::SourceChanged,
            ) => {
                return Ok(worktree_binding_reconciliation!("closing_observation"));
            }
            Err(error) => return Err(error),
        };
        self.require_process_cleanup(processes)?;
        if purpose.accepts_closing(closing.fact()) {
            Ok(DeliveryWorktreeCleanupRecoveryBindingOutcome::Bound(intent))
        } else {
            Ok(worktree_binding_reconciliation!("closing_fact"))
        }
    }

    /// Authenticates the later branch-cleanup action independently from the
    /// worktree mutation receipt. The worktree must already classify as exact
    /// absent and the persisted target must bind through a fresh registered
    /// checkout before the existing branch intent can be captured.
    #[allow(clippy::too_many_arguments)]
    pub async fn bind_persisted_delivery_branch_cleanup(
        &self,
        source_provisioner: &DeliverySourceProvisioner,
        target_provisioner: &DeliveryTargetProvisioner,
        reservation: &WorktreeReservation,
        persisted_source: &DeliveryPersistedSourceRecovery,
        persisted_target: &DeliveryPersistedTargetRecovery,
        processes: &SealedProcessLivenessScope,
        cancellation: CancellationToken,
    ) -> Result<DeliveryBranchCleanupRecoveryBindingOutcome, DeliveryWorktreeCleanupError> {
        let worktree = match self
            .bind_authenticated_delivery_worktree_cleanup(
                WorktreeCleanupBindingPurpose::BranchCleanup,
                source_provisioner,
                target_provisioner,
                reservation,
                persisted_source,
                persisted_target,
                processes,
                cancellation.clone(),
            )
            .await?
        {
            DeliveryWorktreeCleanupRecoveryBindingOutcome::Bound(intent) => intent,
            DeliveryWorktreeCleanupRecoveryBindingOutcome::ReconciliationRequired => {
                return Ok(DeliveryBranchCleanupRecoveryBindingOutcome::ReconciliationRequired);
            }
        };
        self.require_process_cleanup(processes)?;
        let target = match target_provisioner
            .bind_persisted_delivery_target_recovery(persisted_target, cancellation.clone())
            .await
        {
            Ok(DeliveryTargetRecoveryBindingOutcome::Bound(target)) => target,
            Ok(DeliveryTargetRecoveryBindingOutcome::ReconciliationRequired) => {
                return Ok(DeliveryBranchCleanupRecoveryBindingOutcome::ReconciliationRequired);
            }
            Err(error) => return Err(map_target_binding_error(error)),
        };
        match self
            .capture_branch_cleanup_intent(
                source_provisioner,
                worktree,
                target.into_target(),
                processes,
                cancellation,
            )
            .await
        {
            Ok(intent) => Ok(DeliveryBranchCleanupRecoveryBindingOutcome::Bound(intent)),
            Err(
                DeliveryWorktreeCleanupError::AuthenticationChanged
                | DeliveryWorktreeCleanupError::SourceChanged,
            ) => Ok(DeliveryBranchCleanupRecoveryBindingOutcome::ReconciliationRequired),
            Err(error) => Err(error),
        }
    }

    async fn persisted_source_commit_is_exact(
        &self,
        source: &DeliverySourceRecoveryIntent,
        target: &crate::delivery::DeliveryTargetRecoveryCapability,
        reservation: &WorktreeReservation,
        persisted: &DeliveryPersistedSourceRecovery,
        processes: &SealedProcessLivenessScope,
        cancellation: CancellationToken,
    ) -> Result<bool, DeliveryWorktreeCleanupError> {
        let target = target.target();
        let probe_authority_is_exact = if self.probe.has_repository_object_format_binding() {
            target
                .probe()
                .shares_repository_format_authority_with(&self.probe)
        } else {
            target.probe().has_repository_object_format_binding()
                && target.probe().shares_probed_authority_with(&self.probe)
        };
        if !probe_authority_is_exact {
            worktree_binding_diagnostic!("source_exact_probe_authority");
            return Ok(false);
        }
        if target.probe().object_format() != persisted.object_format() {
            worktree_binding_diagnostic!("source_exact_object_format");
            return Ok(false);
        }
        if target.branch_name() == reservation.branch_name() {
            worktree_binding_diagnostic!("source_exact_distinct_branches");
            return Ok(false);
        }
        if !source.matches_cleanup_common_identity(target.common_directory_identity()) {
            worktree_binding_diagnostic!("source_exact_common_identity");
            return Ok(false);
        }
        self.require_process_cleanup(processes)?;
        if let Err(error) = target.revalidate_branch_cleanup_security(self.limits) {
            return if target_recovery_drift(error) {
                worktree_binding_diagnostic!("source_exact_target_security_before");
                Ok(false)
            } else {
                Err(map_target_binding_error(error))
            };
        }
        let commands = match target.commands().branch_cleanup_commands(
            target.probe(),
            target.authentication().command_context(),
            reservation.branch_name(),
            target.branch_name(),
            persisted
                .expected_source_commit()
                .expect("committed persisted source has an expected commit"),
            target.head(),
        ) {
            Ok(commands) => commands,
            Err(error) if source_recovery_drift(error) => {
                worktree_binding_diagnostic!("source_exact_commands");
                return Ok(false);
            }
            Err(error) => return Err(error.into()),
        };
        for token in [cancellation.clone(), cancellation] {
            let command = match commands.inspect_expected_source_commit() {
                Ok(command) => command,
                Err(error) if source_recovery_drift(error) => {
                    worktree_binding_diagnostic!("source_exact_command_build");
                    return Ok(false);
                }
                Err(error) => return Err(error.into()),
            };
            let output = match self
                .executor
                .run(command, token, self.limits.max_status_bytes())
                .await
            {
                Ok(output) => output,
                Err(error)
                    if error == DeliverySourceError::BoundsExceeded
                        || source_recovery_drift(error) =>
                {
                    worktree_binding_diagnostic!("source_exact_command_execution");
                    return Ok(false);
                }
                Err(error) => return Err(error.into()),
            };
            if verify_batched_source_commit(
                &output,
                persisted
                    .expected_source_commit()
                    .expect("committed persisted source has an expected commit"),
                persisted.candidate_tree(),
                persisted.base_commit(),
                persisted.source_input(),
            )
            .is_err()
            {
                worktree_binding_diagnostic!("source_exact_commit_proof");
                return Ok(false);
            }
            self.require_process_cleanup(processes)?;
        }
        if let Err(error) = target.revalidate_branch_cleanup_security(self.limits) {
            return if target_recovery_drift(error) {
                worktree_binding_diagnostic!("source_exact_target_security_after");
                Ok(false)
            } else {
                Err(map_target_binding_error(error))
            };
        }
        self.require_process_cleanup(processes)?;
        Ok(true)
    }
}

fn worktree_source_binding_failure(
    error: DeliveryCommittedSourceCleanupCaptureError,
) -> Result<DeliveryWorktreeCleanupRecoveryBindingOutcome, DeliveryWorktreeCleanupError> {
    let DeliveryCommittedSourceCleanupCaptureError::Source(error) = error else {
        return Err(DeliveryWorktreeCleanupError::Dirty);
    };
    match error {
        DeliverySourceError::AuthenticationChanged => Ok(worktree_binding_reconciliation!(
            "source_cleanup_authentication"
        )),
        DeliverySourceError::SourceChanged => {
            Ok(worktree_binding_reconciliation!("source_cleanup_source"))
        }
        DeliverySourceError::UnsafeIndex => {
            Ok(worktree_binding_reconciliation!("source_cleanup_index"))
        }
        DeliverySourceError::UnsafeGitConfiguration => Ok(worktree_binding_reconciliation!(
            "source_cleanup_git_configuration"
        )),
        error => Err(error.into()),
    }
}

fn source_recovery_drift(error: DeliverySourceError) -> bool {
    matches!(
        error,
        DeliverySourceError::AuthenticationChanged
            | DeliverySourceError::SourceChanged
            | DeliverySourceError::UnsafeIndex
            | DeliverySourceError::UnsafeGitConfiguration
    )
}

fn target_recovery_drift(error: DeliveryTargetError) -> bool {
    matches!(
        error,
        DeliveryTargetError::AuthenticationChanged
            | DeliveryTargetError::TargetDetached
            | DeliveryTargetError::TargetBranchMismatch
            | DeliveryTargetError::TargetHeadChanged
            | DeliveryTargetError::TargetWorktreeDirty
            | DeliveryTargetError::TargetIgnoredPathCollision
            | DeliveryTargetError::TargetGitOperationInProgress
            | DeliveryTargetError::UnsafeGitConfiguration
            | DeliveryTargetError::UnsupportedGitAttributes
    )
}

fn map_target_binding_error(error: DeliveryTargetError) -> DeliveryWorktreeCleanupError {
    match error {
        DeliveryTargetError::Cancelled => DeliveryWorktreeCleanupError::Cancelled,
        DeliveryTargetError::TimedOut => DeliveryWorktreeCleanupError::TimedOut,
        DeliveryTargetError::ChildOutcomeUnknown => {
            DeliveryWorktreeCleanupError::ChildOutcomeUnknown
        }
        DeliveryTargetError::ProcessCleanupUnproven => {
            DeliveryWorktreeCleanupError::ProcessCleanupUnproven
        }
        DeliveryTargetError::AuthenticationChanged
        | DeliveryTargetError::TargetDetached
        | DeliveryTargetError::TargetBranchMismatch
        | DeliveryTargetError::TargetHeadChanged
        | DeliveryTargetError::TargetWorktreeDirty
        | DeliveryTargetError::TargetIgnoredPathCollision
        | DeliveryTargetError::TargetGitOperationInProgress
        | DeliveryTargetError::UnsafeGitConfiguration
        | DeliveryTargetError::UnsupportedGitAttributes => {
            DeliveryWorktreeCleanupError::AuthenticationChanged
        }
        DeliveryTargetError::InvalidLimits
        | DeliveryTargetError::InvalidRequest
        | DeliveryTargetError::BoundsExceeded => DeliveryWorktreeCleanupError::InvalidConfiguration,
        DeliveryTargetError::CommandFailed => DeliveryWorktreeCleanupError::CommandFailed,
        DeliveryTargetError::Internal => DeliveryWorktreeCleanupError::Internal,
    }
}
