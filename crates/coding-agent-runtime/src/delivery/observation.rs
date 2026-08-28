use std::ffi::OsStr;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use coding_agent_core::WorkspaceFingerprint;
use tokio_util::sync::CancellationToken;

use crate::fingerprint::{DeliveryFingerprintObservation, parse_delivery_tracked_paths};
use crate::native_fs::child_entry_exists;
use crate::process_supervisor::{ExactChildInput, PlatformEnvironment};
use crate::root_capability::DurableDirectoryIdentityV1;
use crate::worktree::{
    CleanupPresentAuthentication, LinkedWorktreeAuthentication, LinkedWorktreeAuthenticator,
    LinkedWorktreeCommandContext,
};
use crate::{
    FingerprintLimits, ProbedDeliveryGit, ProcessLimits, ProcessLivenessScope,
    WorkspaceFingerprinter, WorktreeIdentity, WorktreeProvisioner, WorktreeReservation,
};

use super::command::{DeliverySourceReadCommands, DeliverySourceRealIndexCommands};
use super::config::GitSecuritySnapshot;
use super::git_state::has_in_progress_git_state;
use super::output::DeliveryCommandExit;
use super::recovery::{
    self as delivery_recovery, DeliverySourceRecoveryBindingOutcome,
    DeliverySourceRecoveryCapability, DeliverySourceRecoveryIntent, RecoveryObservation,
};
use super::sandbox::DeliveryCommandSandbox;
use super::source_commit::{self, DeliverySourceCommitInput};
use super::source_tree;
use super::{
    CandidateTreeProvenance, DeliveryCandidateTree, DeliveryCommitOid, DeliveryGitObjectFormat,
    DeliveryPersistedSourceRecovery, DeliveryPersistedSourceState, DeliverySourceCommit,
    DeliverySourceError, DeliverySourceLimits, DeliverySourcePendingState,
    DeliverySourceRecoveryDisposition, DeliveryTreeOid, PreparedDeliveryPreflightSource,
};

pub(crate) mod execution;
pub(crate) mod parsing;

pub(crate) use execution::DeliveryCommandExecutor;
use execution::DeliveryMutationCommandError;
pub(crate) use parsing::parse_object_id;
use parsing::{attribute_path_chunks, parse_one_line};

macro_rules! committed_cleanup_diagnostic {
    ($predicate:literal) => {
        #[cfg(feature = "test-support")]
        eprintln!(
            "test-support committed source cleanup observation rejected: predicate={}",
            $predicate
        );
    };
}

mod cleanup;
mod live;
mod recovery;

/// Independent P4-B entry point for an exact reviewed dirty worktree.
///
/// Construction binds one successful Git probe to the exact Git retained by
/// the P4-A provisioner, but opening never calls or weakens `open_ready`.
pub struct DeliverySourceProvisioner {
    probe: Arc<ProbedDeliveryGit>,
    authenticator: LinkedWorktreeAuthenticator,
    sandbox: Arc<DeliveryCommandSandbox>,
    platform: PlatformEnvironment,
    executor: DeliveryCommandExecutor,
    limits: DeliverySourceLimits,
    fingerprint_limits: FingerprintLimits,
    #[cfg(feature = "test-support")]
    authentication_boundary_hook: Option<Arc<dyn Fn(&'static str) + Send + Sync + 'static>>,
}

struct ReviewedSourceObservation<'a> {
    commands: &'a DeliverySourceReadCommands,
    authentication: &'a LinkedWorktreeAuthentication,
    expected_base: &'a str,
    expected_branch: &'a str,
    approved_fingerprint: WorkspaceFingerprint,
    security: &'a GitSecuritySnapshot,
    cancellation: CancellationToken,
}

/// Immutable inputs shared by the two recovery observations.  Keeping these
/// values together makes the repeated observation explicitly prove the same
/// source/candidate/commit binding twice.
struct PendingSourceStateObservation<'a> {
    source: &'a DeliverySourceCapability,
    candidate: &'a DeliveryCandidateTree,
    expected: Option<&'a DeliverySourceCommit>,
    input: &'a DeliverySourceCommitInput,
    base: &'a DeliveryCommitOid,
    commands: &'a DeliverySourceRealIndexCommands,
}

/// Cleanup-only classification of an already committed source while a fresh
/// transient linked-worktree context is retained. The transient context is
/// consumed before either cleanup mutation command can be constructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DeliveryCommittedSourceCleanupObservation {
    ExactClean,
    ExactDirty,
    Inconsistent,
}

/// Separates an authenticated dirty worktree from source identity drift while
/// capturing the cleanup baseline. The distinction is intentionally internal:
/// callers may reject a fresh cleanup request as ineligible, while persisted
/// recovery continues to fail closed under its existing state matrix.
pub(super) enum DeliveryCommittedSourceCleanupCaptureError {
    Dirty,
    Source(DeliverySourceError),
}

impl From<DeliverySourceError> for DeliveryCommittedSourceCleanupCaptureError {
    fn from(error: DeliverySourceError) -> Self {
        Self::Source(error)
    }
}

/// Opaque committed-phase scene captured only after the exact source commit
/// and control-plane identity are re-proven. Fresh acceptance captures only a
/// clean scene; persisted recovery may retain an authenticated dirty scene so
/// its query-first phase classifier can unlock or fail closed without minting
/// removal authority. It deliberately stays inside the runtime cleanup intent;
/// pre-stage fingerprints are a different domain and cannot authorize later
/// removal.
pub(super) struct DeliveryCommittedSourceCleanupProof {
    scene: CommittedSourceCleanupScene,
}

impl DeliveryCommittedSourceCleanupProof {
    /// Reuses the freshly authenticated committed-source scene to bind a
    /// persisted pre-removal config/attributes digest. This predicate is kept
    /// on the opaque proof so cleanup recovery never receives the observed
    /// digest or its path domain as raw authority.
    pub(super) fn matches_persisted_config_attributes_digest(&self, expected: &[u8; 32]) -> bool {
        self.scene
            .clean
            .as_ref()
            .is_some_and(|clean| clean.attributes == *expected)
    }

    /// A stable dirty scene can authorize only query-first persisted recovery.
    /// It never reproduces the clean candidate/fingerprint baseline and thus
    /// cannot become remove authority if the worktree later changes again.
    pub(super) fn is_authenticated_dirty(&self) -> bool {
        !self.scene.status.is_empty() || !self.scene.ignored_untracked.is_empty()
    }
}

#[derive(PartialEq, Eq)]
struct CommittedSourceCleanupScene {
    security: GitSecuritySnapshot,
    status: Vec<u8>,
    ignored_untracked: Vec<u8>,
    clean: Option<CommittedSourceCleanupCleanScene>,
}

#[derive(PartialEq, Eq)]
struct CommittedSourceCleanupCleanScene {
    tracked_paths: Vec<Vec<u8>>,
    attributes: [u8; 32],
    fingerprint: WorkspaceFingerprint,
    fingerprint_paths: Vec<Vec<u8>>,
}

impl DeliverySourceProvisioner {
    #[allow(clippy::too_many_arguments)]
    pub fn from_worktree_provisioner(
        worktrees: &WorktreeProvisioner,
        probe: Arc<ProbedDeliveryGit>,
        temporary_directory: impl AsRef<Path>,
        process_liveness_scope: ProcessLivenessScope,
        process_limits: ProcessLimits,
        limits: DeliverySourceLimits,
        fingerprint_limits: FingerprintLimits,
    ) -> Result<Self, DeliverySourceError> {
        require_replayable_snapshot_file_limit(fingerprint_limits)?;
        probe
            .verify_current_executable()
            .map_err(|_| DeliverySourceError::AuthenticationChanged)?;
        let authenticator = worktrees
            .delivery_source_authenticator(probe.pinned_executable())
            .map_err(DeliverySourceError::from)?;
        let (temporary_path, temporary) =
            authenticated_temporary_directory(temporary_directory.as_ref())?;
        if !temporary.has_same_identity(probe.private_runtime()) {
            return Err(DeliverySourceError::AuthenticationChanged);
        }
        let platform = delivery_platform_environment(temporary_path)?;
        let sandbox = Arc::new(DeliveryCommandSandbox::create(Arc::clone(
            probe.private_runtime(),
        ))?);
        sandbox.revalidate()?;
        Ok(Self {
            probe,
            authenticator,
            sandbox,
            platform,
            executor: DeliveryCommandExecutor::new(process_limits, process_liveness_scope),
            limits,
            fingerprint_limits,
            #[cfg(feature = "test-support")]
            authentication_boundary_hook: None,
        })
    }
}

impl DeliverySourceProvisioner {
    fn authenticate_source(
        &self,
        reservation: &WorktreeReservation,
    ) -> Result<(LinkedWorktreeAuthentication, GitSecuritySnapshot), DeliverySourceError> {
        self.sandbox.revalidate()?;
        let authentication = self
            .authenticator
            .authenticate(reservation)
            .map_err(DeliverySourceError::from)?;
        let security = GitSecuritySnapshot::capture_authenticated(
            authentication.command_context(),
            self.limits,
        )?;
        self.after_first_security_snapshot();
        let repeated = GitSecuritySnapshot::capture_authenticated(
            authentication.command_context(),
            self.limits,
        )?;
        require_same_security_snapshot(&security, &repeated)?;
        Ok((authentication, security))
    }

    async fn repository_bound_source_commands(
        &self,
        context: &LinkedWorktreeCommandContext,
        cancellation: CancellationToken,
    ) -> Result<(Arc<ProbedDeliveryGit>, DeliverySourceReadCommands), DeliverySourceError> {
        let discovery_commands = DeliverySourceReadCommands::try_new(
            &self.probe,
            context,
            Arc::clone(&self.sandbox),
            &self.platform,
            self.limits.timeout(),
        )?;
        let output = self
            .executor
            .run(
                discovery_commands.repository_object_format()?,
                cancellation.clone(),
                self.limits.max_status_bytes(),
            )
            .await?;
        let object_format = DeliveryGitObjectFormat::parse_exact_git_output(&output)
            .ok_or(DeliverySourceError::CommandFailed)?;
        let repository_probe = Arc::new(
            self.probe
                .bind_repository_object_format(object_format)
                .map_err(|_| DeliverySourceError::AuthenticationChanged)?,
        );
        let commands = DeliverySourceReadCommands::try_new(
            &repository_probe,
            context,
            Arc::clone(&self.sandbox),
            &self.platform,
            self.limits.timeout(),
        )?;
        self.require_repository_object_format(&commands, &repository_probe, cancellation)
            .await?;
        Ok((repository_probe, commands))
    }

    async fn require_repository_object_format(
        &self,
        commands: &DeliverySourceReadCommands,
        repository_probe: &ProbedDeliveryGit,
        cancellation: CancellationToken,
    ) -> Result<(), DeliverySourceError> {
        let output = self
            .executor
            .run(
                commands.repository_object_format()?,
                cancellation,
                self.limits.max_status_bytes(),
            )
            .await?;
        if DeliveryGitObjectFormat::parse_exact_git_output(&output)
            == Some(repository_probe.object_format())
        {
            Ok(())
        } else {
            Err(DeliverySourceError::AuthenticationChanged)
        }
    }

    async fn observe_reviewed_source(
        &self,
        input: ReviewedSourceObservation<'_>,
    ) -> Result<[u8; 32], DeliverySourceError> {
        let ReviewedSourceObservation {
            commands,
            authentication,
            expected_base,
            expected_branch,
            approved_fingerprint,
            security,
            cancellation,
        } = input;
        self.require_control_state(
            commands,
            expected_base,
            expected_branch,
            cancellation.clone(),
        )
        .await?;
        let observed = self
            .collect_fingerprint(commands, authentication, cancellation.clone())
            .await?;
        require_approved_fingerprint(&observed, approved_fingerprint)?;
        let attributes_digest = self
            .require_safe_attributes(commands, &observed, security, cancellation.clone())
            .await?;
        let final_observed = self
            .collect_fingerprint(commands, authentication, cancellation.clone())
            .await?;
        require_approved_fingerprint(&final_observed, approved_fingerprint)?;
        let final_attributes_digest = self
            .require_safe_attributes(commands, &final_observed, security, cancellation.clone())
            .await?;
        if attributes_digest != final_attributes_digest {
            return Err(DeliverySourceError::SourceChanged);
        }
        self.require_control_state(commands, expected_base, expected_branch, cancellation)
            .await?;
        Ok(attributes_digest)
    }

    async fn revalidate_open_source(
        &self,
        source: &DeliverySourceCapability,
        cancellation: CancellationToken,
    ) -> Result<(), DeliverySourceError> {
        self.require_capability_binding(source)?;
        source.sandbox().revalidate()?;
        source
            .authentication()
            .reauthenticate()
            .map_err(DeliverySourceError::from)?;
        let security = GitSecuritySnapshot::capture_authenticated(
            source.authentication().command_context(),
            self.limits,
        )?;
        let repeated = GitSecuritySnapshot::capture_authenticated(
            source.authentication().command_context(),
            self.limits,
        )?;
        require_same_security_snapshot(&security, &repeated)?;
        let attributes_digest = self
            .observe_reviewed_source(ReviewedSourceObservation {
                commands: source.commands(),
                authentication: source.authentication(),
                expected_base: source.base_commit(),
                expected_branch: source.branch_name(),
                approved_fingerprint: source.approved_fingerprint(),
                security: &security,
                cancellation: cancellation.clone(),
            })
            .await?;
        if attributes_digest != *source.config_attributes_digest() {
            return Err(DeliverySourceError::SourceChanged);
        }
        self.require_repository_object_format(source.commands(), source.probe(), cancellation)
            .await?;
        self.finalize_authentication(source.authentication(), &security)
    }

    fn require_capability_binding(
        &self,
        source: &DeliverySourceCapability,
    ) -> Result<(), DeliverySourceError> {
        if self.probe.shares_probed_authority_with(source.probe())
            && Arc::ptr_eq(&self.sandbox, source.sandbox())
        {
            Ok(())
        } else {
            Err(DeliverySourceError::AuthenticationChanged)
        }
    }

    fn finalize_authentication(
        &self,
        authentication: &LinkedWorktreeAuthentication,
        expected_security: &GitSecuritySnapshot,
    ) -> Result<(), DeliverySourceError> {
        let final_security = GitSecuritySnapshot::capture_authenticated(
            authentication.command_context(),
            self.limits,
        )?;
        require_same_security_snapshot(expected_security, &final_security)?;
        authentication
            .reauthenticate()
            .map_err(DeliverySourceError::from)?;
        self.sandbox.revalidate()
    }

    async fn collect_fingerprint(
        &self,
        commands: &DeliverySourceReadCommands,
        authentication: &LinkedWorktreeAuthentication,
        cancellation: CancellationToken,
    ) -> Result<DeliveryFingerprintObservation, DeliverySourceError> {
        WorkspaceFingerprinter::collect_delivery(
            self.executor.supervisor(),
            commands,
            Arc::clone(&authentication.command_context().worktree.execution),
            self.fingerprint_limits,
            self.limits.max_status_bytes(),
            commands.object_format().hexadecimal_length(),
            cancellation,
        )
        .await
    }

    async fn require_control_state(
        &self,
        commands: &DeliverySourceReadCommands,
        expected_base: &str,
        expected_branch: &str,
        cancellation: CancellationToken,
    ) -> Result<(), DeliverySourceError> {
        let head = self
            .executor
            .run(
                commands.resolve_head()?,
                cancellation.clone(),
                self.limits.max_status_bytes(),
            )
            .await?;
        let symbolic = self
            .executor
            .run(
                commands.symbolic_head()?,
                cancellation,
                self.limits.max_status_bytes(),
            )
            .await?;
        if parse_object_id(&head, commands.object_format().hexadecimal_length())? != expected_base
            || parse_one_line(&symbolic)? != format!("refs/heads/{expected_branch}")
        {
            return Err(DeliverySourceError::AuthenticationChanged);
        }
        Ok(())
    }

    async fn require_safe_attributes(
        &self,
        commands: &DeliverySourceReadCommands,
        observed: &DeliveryFingerprintObservation,
        security: &GitSecuritySnapshot,
        cancellation: CancellationToken,
    ) -> Result<[u8; 32], DeliverySourceError> {
        self.require_safe_attributes_for_paths(commands, &observed.paths, security, cancellation)
            .await
    }

    async fn require_safe_attributes_for_paths(
        &self,
        commands: &DeliverySourceReadCommands,
        paths: &[Vec<u8>],
        security: &GitSecuritySnapshot,
        cancellation: CancellationToken,
    ) -> Result<[u8; 32], DeliverySourceError> {
        if paths.len() > self.limits.max_paths() {
            return Err(DeliverySourceError::BoundsExceeded);
        }
        let mut digest = security.config_attributes_digest_builder();
        for paths in attribute_path_chunks(paths)? {
            let output = self
                .executor
                .run(
                    commands.check_attributes(paths)?,
                    cancellation.clone(),
                    self.limits.max_attributes_bytes(),
                )
                .await?;
            digest.append_checked_attributes(&output, paths)?;
        }
        Ok(digest.finish())
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn set_authentication_boundary_hook_for_tests(
        &mut self,
        hook: impl Fn(&'static str) + Send + Sync + 'static,
    ) {
        self.authentication_boundary_hook = Some(Arc::new(hook));
    }

    fn after_first_security_snapshot(&self) {
        self.run_boundary_hook("after-config-authentication");
    }

    fn after_candidate_tree_write(&self) {
        self.run_boundary_hook("after-write-tree-before-fresh-fingerprint");
    }

    fn after_candidate_revalidation(&self) {
        self.run_boundary_hook("after-candidate-revalidation-before-tree-build");
    }

    fn after_real_index_add(&self) {
        self.run_boundary_hook("after-real-index-stage-before-source-object-reverify");
    }

    fn after_source_object_reverify(&self) {
        self.run_boundary_hook("after-source-object-reverify-before-cas");
    }

    fn after_source_ref_cas(&self) {
        self.run_boundary_hook("after-source-cas-before-postverify");
    }

    fn run_boundary_hook(&self, phase: &'static str) {
        #[cfg(feature = "test-support")]
        if let Some(hook) = &self.authentication_boundary_hook {
            hook(phase);
        }
        #[cfg(not(feature = "test-support"))]
        let _ = phase;
    }
}

impl fmt::Debug for DeliverySourceProvisioner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliverySourceProvisioner(<opaque>)")
    }
}

/// Opaque proof that the exact reviewed source survived A/B authentication.
pub struct DeliverySourceCapability {
    identity: WorktreeIdentity,
    base_commit: String,
    branch_name: String,
    approved_fingerprint: WorkspaceFingerprint,
    config_attributes_digest: [u8; 32],
    common_identity: DurableDirectoryIdentityV1,
    admin_identity: DurableDirectoryIdentityV1,
    probe: Arc<ProbedDeliveryGit>,
    _authentication: LinkedWorktreeAuthentication,
    _commands: DeliverySourceReadCommands,
    _sandbox: Arc<DeliveryCommandSandbox>,
}

impl DeliverySourceCapability {
    fn new(
        reservation: &WorktreeReservation,
        approved_fingerprint: WorkspaceFingerprint,
        config_attributes_digest: [u8; 32],
        probe: Arc<ProbedDeliveryGit>,
        authentication: LinkedWorktreeAuthentication,
        commands: DeliverySourceReadCommands,
        sandbox: Arc<DeliveryCommandSandbox>,
    ) -> Self {
        let common_identity = authentication.command_context().common_identity.clone();
        let admin_identity = authentication.command_context().admin_identity.clone();
        Self {
            identity: reservation.identity().clone(),
            base_commit: reservation.base_commit().to_owned(),
            branch_name: reservation.branch_name().to_owned(),
            approved_fingerprint,
            config_attributes_digest,
            common_identity,
            admin_identity,
            probe,
            _authentication: authentication,
            _commands: commands,
            _sandbox: sandbox,
        }
    }

    pub fn identity(&self) -> &WorktreeIdentity {
        &self.identity
    }

    pub fn base_commit(&self) -> &str {
        &self.base_commit
    }

    pub fn branch_name(&self) -> &str {
        &self.branch_name
    }

    pub const fn approved_fingerprint(&self) -> WorkspaceFingerprint {
        self.approved_fingerprint
    }

    pub const fn config_attributes_digest(&self) -> &[u8; 32] {
        &self.config_attributes_digest
    }

    #[allow(dead_code)]
    pub(super) const fn common_directory_identity(&self) -> &DurableDirectoryIdentityV1 {
        &self.common_identity
    }

    #[allow(dead_code)]
    pub(super) const fn admin_directory_identity(&self) -> &DurableDirectoryIdentityV1 {
        &self.admin_identity
    }

    pub(super) fn candidate_tree_provenance(
        &self,
    ) -> Result<CandidateTreeProvenance, DeliverySourceError> {
        let base_commit =
            DeliveryCommitOid::try_new(self.base_commit(), self.probe.object_format())
                .ok_or(DeliverySourceError::AuthenticationChanged)?;
        Ok(CandidateTreeProvenance::new(
            self.identity.clone(),
            base_commit,
            self.branch_name.clone(),
            self.approved_fingerprint,
            self.config_attributes_digest,
            self.common_identity.clone(),
            self.admin_identity.clone(),
        ))
    }

    // These crate-internal authority projections are intentionally wired at
    // the Task 10/11 boundary. They never expose namespace paths publicly.
    #[allow(dead_code)]
    pub(super) const fn probe(&self) -> &Arc<ProbedDeliveryGit> {
        &self.probe
    }

    #[allow(dead_code)]
    pub(super) const fn authentication(&self) -> &LinkedWorktreeAuthentication {
        &self._authentication
    }

    #[allow(dead_code)]
    pub(super) const fn commands(&self) -> &DeliverySourceReadCommands {
        &self._commands
    }

    #[allow(dead_code)]
    pub(super) const fn sandbox(&self) -> &Arc<DeliveryCommandSandbox> {
        &self._sandbox
    }
}

impl fmt::Debug for DeliverySourceCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliverySourceCapability(<opaque>)")
    }
}

fn authenticated_temporary_directory(
    path: &Path,
) -> Result<(PathBuf, Arc<crate::ExecutionDirectory>), DeliverySourceError> {
    let original = crate::ExecutionDirectory::open(path)?;
    let canonical =
        std::fs::canonicalize(path).map_err(|_| DeliverySourceError::InvalidEnvironment)?;
    let canonical_directory = crate::ExecutionDirectory::open(&canonical)?;
    if !original.has_same_identity(&canonical_directory) {
        return Err(DeliverySourceError::AuthenticationChanged);
    }
    Ok((canonical, Arc::new(canonical_directory)))
}

fn delivery_platform_environment(
    path: PathBuf,
) -> Result<PlatformEnvironment, DeliverySourceError> {
    #[cfg(windows)]
    let system_root = std::env::var_os("SYSTEMROOT")
        .or_else(|| std::env::var_os("WINDIR"))
        .map(PathBuf::from);
    #[cfg(unix)]
    let system_root = None;
    PlatformEnvironment::try_new(path, system_root)
        .map_err(|_| DeliverySourceError::InvalidEnvironment)
}

fn require_not_cancelled(cancellation: &CancellationToken) -> Result<(), DeliverySourceError> {
    if cancellation.is_cancelled() {
        Err(DeliverySourceError::Cancelled)
    } else {
        Ok(())
    }
}

/// Candidate construction replays every approved single-file byte stream via
/// the exact redacted stdin channel. Refuse an otherwise-valid fingerprint
/// configuration that cannot be replayed without changing its file bound.
fn require_replayable_snapshot_file_limit(
    fingerprint_limits: FingerprintLimits,
) -> Result<(), DeliverySourceError> {
    let exact_input_maximum = u64::try_from(ExactChildInput::maximum_bytes())
        .map_err(|_| DeliverySourceError::InvalidLimits)?;
    if fingerprint_limits.max_file_bytes() > exact_input_maximum {
        Err(DeliverySourceError::InvalidLimits)
    } else {
        Ok(())
    }
}

fn require_same_security_snapshot(
    expected: &GitSecuritySnapshot,
    observed: &GitSecuritySnapshot,
) -> Result<(), DeliverySourceError> {
    if expected == observed {
        Ok(())
    } else {
        Err(DeliverySourceError::UnsafeGitConfiguration)
    }
}

fn require_approved_fingerprint(
    observed: &DeliveryFingerprintObservation,
    approved: WorkspaceFingerprint,
) -> Result<(), DeliverySourceError> {
    if observed.fingerprint == approved {
        Ok(())
    } else {
        Err(DeliverySourceError::SourceChanged)
    }
}

/// Git's fixed `cat-file -t` protocol emits one lowercase type and a newline.
/// Anything except the exact tree token is a persisted-object mismatch: in
/// particular, commits and tags are never accepted through peeling.
fn require_exact_tree_object_type(output: &[u8]) -> Result<(), DeliverySourceError> {
    if output == b"tree\n" {
        Ok(())
    } else {
        Err(DeliverySourceError::SourceChanged)
    }
}

const EXACT_GIT_OBJECT_TYPE_OUTPUT_LIMIT: usize = 16;

fn is_recovery_mismatch(error: DeliverySourceError) -> bool {
    matches!(
        error,
        DeliverySourceError::SourceChanged
            | DeliverySourceError::AuthenticationChanged
            | DeliverySourceError::UnsafeGitConfiguration
            | DeliverySourceError::UnsafeIndex
            | DeliverySourceError::CommandFailed
    )
}

fn is_cleanup_observation_mismatch(error: DeliverySourceError) -> bool {
    is_recovery_mismatch(error) || matches!(error, DeliverySourceError::BoundsExceeded)
}

/// The first real-index mutation is the sole point at which a supervisor
/// pre-spawn proof still establishes that the durable CommitPending intent is
/// known not to have been applied. Every child/result failure after admission
/// remains conservatively unknown.
fn first_real_index_mutation_error(error: DeliveryMutationCommandError) -> DeliverySourceError {
    match error {
        DeliveryMutationCommandError::NotStarted => DeliverySourceError::CommandFailed,
        DeliveryMutationCommandError::ChildOrResult(error) => post_real_index_mutation_error(error),
    }
}

/// Once the real index mutation sequence has been handed to Git, a non-success
/// result cannot prove that the index or source ref remained untouched. Keep
/// the existing process-cleanup proof class when it is known, and otherwise
/// require durable reconciliation rather than letting a caller retry blindly.
fn post_real_index_mutation_error(error: DeliverySourceError) -> DeliverySourceError {
    match error {
        DeliverySourceError::ChildOutcomeUnknown
        | DeliverySourceError::ProcessCleanupUnproven
        | DeliverySourceError::SandboxCleanupUnproven => error,
        _ => DeliverySourceError::ChildOutcomeUnknown,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn candidate_snapshot_limit_must_fit_exact_child_input() {
        let exact = u64::try_from(ExactChildInput::maximum_bytes()).unwrap();
        let accepted = FingerprintLimits::try_new(Duration::from_secs(1), 1, exact, exact)
            .expect("exact bound is a valid fingerprint configuration");
        let rejected = FingerprintLimits::try_new(
            Duration::from_secs(1),
            1,
            exact.checked_add(1).unwrap(),
            exact.checked_add(1).unwrap(),
        )
        .expect("larger fingerprint limit is independently well-formed");

        assert!(require_replayable_snapshot_file_limit(accepted).is_ok());
        assert_eq!(
            require_replayable_snapshot_file_limit(rejected),
            Err(DeliverySourceError::InvalidLimits)
        );
    }

    #[test]
    fn cleanup_workspace_bounds_are_an_inconsistent_fact_not_invalid_configuration() {
        assert!(is_cleanup_observation_mismatch(
            DeliverySourceError::BoundsExceeded
        ));
        assert!(!is_cleanup_observation_mismatch(
            DeliverySourceError::ProcessCleanupUnproven
        ));
    }

    #[test]
    fn candidate_object_type_protocol_accepts_only_exact_tree_line() {
        assert_eq!(require_exact_tree_object_type(b"tree\n"), Ok(()));
        for rejected in [
            b"commit\n".as_slice(),
            b"tag\n".as_slice(),
            b"blob\n".as_slice(),
            b"tree".as_slice(),
            b"tree\r\n".as_slice(),
            b"tree\nextra".as_slice(),
        ] {
            assert_eq!(
                require_exact_tree_object_type(rejected),
                Err(DeliverySourceError::SourceChanged)
            );
        }
    }
}
