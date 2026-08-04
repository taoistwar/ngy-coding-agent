use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fmt;
use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::command_policy::{
    CommandPolicyError, ExecutionDirectory, GitCommandBinding, ValidatedCommand, child_visible_path,
};
use crate::native_fs::read_directory_names;
use crate::process_supervisor::{
    ChildEnvironment, CommandResult, PlatformEnvironment, ProcessError, ProcessLimits,
    ProcessSupervisor,
};
use crate::root_capability::DirectoryPathGuard;
use crate::{
    CargoCatalog, CargoToolLimits, CargoTools, DirectoryIdentityError, DirectoryIdentityMarker,
    GitTools, ProcessLivenessScope, RelativePath, RootCapability, ToolchainPaths,
};

const ADMIN_FILE_LIMIT: u64 = 16 * 1024;
const MAX_ADMIN_ENTRIES: usize = 4_096;
const MAX_LOCAL_CONFIG_ENTRIES: usize = 4_096;
const MAX_LOCAL_CONFIG_KEY_BYTES: usize = 1_024;

/// Stable, application-owned identity for one task attempt.
///
/// Values are intentionally narrower than arbitrary Git refs or filesystem
/// names. They are control-plane identifiers, never model-provided strings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorktreeIdentity {
    repository_id: String,
    task_id: String,
    attempt: u32,
}

impl WorktreeIdentity {
    pub fn try_new(
        repository_id: impl Into<String>,
        task_id: impl Into<String>,
        attempt: u32,
    ) -> Result<Self, WorktreeError> {
        let repository_id = repository_id.into();
        let task_id = task_id.into();
        if attempt == 0
            || !is_safe_identity_component(&repository_id)
            || !is_safe_identity_component(&task_id)
        {
            return Err(WorktreeError::InvalidIdentity);
        }
        Ok(Self {
            repository_id,
            task_id,
            attempt,
        })
    }

    pub fn repository_id(&self) -> &str {
        &self.repository_id
    }

    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    pub const fn attempt(&self) -> u32 {
        self.attempt
    }

    pub fn branch_name(&self) -> String {
        format!("codex/task-{}-attempt-{}", self.task_id, self.attempt)
    }

    pub fn relative_path(&self) -> PathBuf {
        PathBuf::from("worktrees")
            .join(&self.repository_id)
            .join(&self.task_id)
            .join(self.attempt.to_string())
    }
}

/// Trusted deadlines for worktree control-plane Git operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorktreeLimits {
    command_timeout: Duration,
}

impl WorktreeLimits {
    pub fn try_new(command_timeout: Duration) -> Result<Self, WorktreeError> {
        if command_timeout.is_zero() {
            return Err(WorktreeError::InvalidLimits);
        }
        Ok(Self { command_timeout })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GitSideEffectKind {
    WorktreeAdd,
    Reset,
}

impl GitSideEffectKind {
    #[cfg(feature = "test-support")]
    const fn test_label(self) -> &'static str {
        match self {
            Self::WorktreeAdd => "worktree-add",
            Self::Reset => "reset",
        }
    }
}

#[derive(Clone, Copy)]
struct GitSideEffectConditions<'a> {
    binding: &'a GitCommandBinding,
    expected_head: &'a str,
    expected_symbolic_head: Option<&'a str>,
}

/// Deterministic control-plane values that must be persisted as `reserved`
/// before any Git worktree side effect is allowed.
#[derive(Clone, PartialEq, Eq)]
pub struct WorktreeReservation {
    identity: WorktreeIdentity,
    base_commit: String,
    branch_name: String,
    worktree_path: PathBuf,
    source_common_git_identity: DirectoryIdentityMarker,
    cargo_workspace_offset: PathBuf,
}

impl fmt::Debug for WorktreeReservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorktreeReservation")
            .field("identity", &self.identity)
            .field("base_commit", &self.base_commit)
            .field("branch_name", &self.branch_name)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorktreeObservation {
    Absent,
    BranchOnly,
    AdministrativeCreated,
    CheckoutPartial,
    Ready,
    Inconsistent,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorktreeObservationOutcome {
    observation: WorktreeObservation,
    process_cleanup_unproven: bool,
    repository_poison_required: bool,
}

impl WorktreeObservationOutcome {
    fn exact(observation: WorktreeObservation) -> Self {
        Self {
            observation,
            process_cleanup_unproven: false,
            repository_poison_required: observation == WorktreeObservation::Inconsistent,
        }
    }

    fn positive_evidence(observation: WorktreeObservation, error: &WorktreeError) -> Self {
        Self {
            observation,
            process_cleanup_unproven: false,
            repository_poison_required: error.requires_repository_poison(),
        }
    }

    pub const fn observation(self) -> WorktreeObservation {
        self.observation
    }

    pub const fn process_cleanup_is_unproven(self) -> bool {
        self.process_cleanup_unproven
    }

    pub const fn repository_poison_required(self) -> bool {
        self.repository_poison_required
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorktreeArtifactState {
    Absent,
    Partial,
    Ready,
    Inconsistent,
    Unavailable,
}

#[derive(Debug, thiserror::Error)]
#[error("worktree provisioning failed after observing {observation:?}: {cause}")]
pub struct WorktreeProvisionError {
    cause: WorktreeError,
    observation: WorktreeObservation,
    process_cleanup_unproven: bool,
    repository_poison_required: bool,
}

impl WorktreeProvisionError {
    pub fn cause(&self) -> &WorktreeError {
        &self.cause
    }

    pub const fn observation(&self) -> WorktreeObservation {
        self.observation
    }

    pub const fn process_cleanup_is_unproven(&self) -> bool {
        self.process_cleanup_unproven
    }

    pub const fn repository_poison_required(&self) -> bool {
        self.repository_poison_required
    }

    pub const fn artifact_state(&self) -> WorktreeArtifactState {
        match self.observation {
            WorktreeObservation::Absent => WorktreeArtifactState::Absent,
            WorktreeObservation::BranchOnly
            | WorktreeObservation::AdministrativeCreated
            | WorktreeObservation::CheckoutPartial => WorktreeArtifactState::Partial,
            WorktreeObservation::Ready => WorktreeArtifactState::Ready,
            WorktreeObservation::Inconsistent => WorktreeArtifactState::Inconsistent,
            WorktreeObservation::Unavailable => WorktreeArtifactState::Unavailable,
        }
    }

    pub const fn code(&self) -> &'static str {
        self.cause.code()
    }
}

impl WorktreeReservation {
    pub fn identity(&self) -> &WorktreeIdentity {
        &self.identity
    }

    pub fn base_commit(&self) -> &str {
        &self.base_commit
    }

    pub fn branch_name(&self) -> &str {
        &self.branch_name
    }

    pub fn worktree_path(&self) -> &Path {
        &self.worktree_path
    }
}

/// A fully revalidated linked worktree and the capabilities later typed tools
/// must use. In particular, `git_directory` points into the original common
/// `.git/worktrees` administration tree and is not derived from the linked
/// worktree's hidden `.git` pointer.
#[derive(Debug)]
pub struct ProvisionedWorktree {
    identity: WorktreeIdentity,
    base_commit: String,
    branch_name: String,
    worktree_path: PathBuf,
    cargo_workspace_path: PathBuf,
    git_directory: Arc<ExecutionDirectory>,
    work_tree: Arc<ExecutionDirectory>,
    cargo_workspace: Arc<ExecutionDirectory>,
    target_directory: Arc<ExecutionDirectory>,
    cargo_catalog: CargoCatalog,
}

impl ProvisionedWorktree {
    pub fn identity(&self) -> &WorktreeIdentity {
        &self.identity
    }

    pub fn base_commit(&self) -> &str {
        &self.base_commit
    }

    pub fn branch_name(&self) -> &str {
        &self.branch_name
    }

    pub fn worktree_path(&self) -> &Path {
        &self.worktree_path
    }

    pub fn cargo_workspace_path(&self) -> &Path {
        &self.cargo_workspace_path
    }

    pub fn git_directory(&self) -> Arc<ExecutionDirectory> {
        Arc::clone(&self.git_directory)
    }

    pub fn work_tree(&self) -> Arc<ExecutionDirectory> {
        Arc::clone(&self.work_tree)
    }

    pub fn cargo_workspace(&self) -> Arc<ExecutionDirectory> {
        Arc::clone(&self.cargo_workspace)
    }

    pub fn target_directory(&self) -> Arc<ExecutionDirectory> {
        Arc::clone(&self.target_directory)
    }

    /// Validated, bounded Cargo selectors. This projection contains package
    /// and integration-test names only; Cargo metadata paths are discarded.
    pub fn cargo_catalog(&self) -> &CargoCatalog {
        &self.cargo_catalog
    }

    pub fn repository_context(&self) -> String {
        self.cargo_catalog.repository_context()
    }

    pub fn target_directory_matches(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<bool, CommandPolicyError> {
        self.target_directory.matches_path(path.as_ref())
    }

    /// Convenience binding that preserves the already validated git-dir and
    /// work-tree capabilities for all later model-visible Git operations.
    pub fn bind_git_tools(
        &self,
        toolchain: &ToolchainPaths,
        temporary_directory: impl AsRef<Path>,
        process_liveness_scope: ProcessLivenessScope,
        process_limits: ProcessLimits,
        limits: crate::GitToolLimits,
    ) -> Result<GitTools, crate::GitToolError> {
        GitTools::from_trusted_capabilities(
            toolchain,
            self.git_directory(),
            self.work_tree(),
            temporary_directory,
            process_liveness_scope,
            process_limits,
            limits,
        )
    }
}

/// Provisions one linked worktree from the registered repository's committed
/// HEAD. The original work tree is used only by fixed Git metadata commands;
/// its dirty files are never copied or inspected by this type.
pub struct WorktreeProvisioner {
    supervisor: ProcessSupervisor,
    repository_id: String,
    toolchain: ToolchainPaths,
    process_liveness_scope: ProcessLivenessScope,
    process_limits: ProcessLimits,
    temporary_directory: PathBuf,
    git: Arc<crate::PinnedExecutable>,
    original_binding: GitCommandBinding,
    common_git_directory: PathBuf,
    common_git_identity: DirectoryIdentityMarker,
    common_git_capability: RootCapability,
    artifact_root: PathBuf,
    artifact_root_directory: Arc<ExecutionDirectory>,
    artifact_root_capability: RootCapability,
    cargo_workspace_offset: PathBuf,
    environment: ChildEnvironment,
    limits: WorktreeLimits,
    #[cfg(feature = "test-support")]
    side_effect_boundary_hook:
        Option<Arc<dyn Fn(&'static str, &'static str) + Send + Sync + 'static>>,
    #[cfg(feature = "test-support")]
    side_effect_outcome_for_tests: Option<WorktreeSideEffectTestOutcome>,
}

/// Deterministic command outcomes used to prove the common-Git postcondition
/// gate precedence, including the fail-closed exception for unproven cleanup.
#[cfg(feature = "test-support")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum WorktreeSideEffectTestOutcome {
    NonZero,
    TimedOut,
    ProcessError,
    CleanupUnproven,
    BuildError,
}

impl WorktreeProvisioner {
    #[allow(clippy::too_many_arguments)]
    pub fn from_trusted_paths(
        toolchain: &ToolchainPaths,
        repository_id: impl Into<String>,
        registered_git_root: impl AsRef<Path>,
        registered_cargo_workspace: impl AsRef<Path>,
        artifact_root: impl AsRef<Path>,
        temporary_directory: impl AsRef<Path>,
        process_liveness_scope: ProcessLivenessScope,
        process_limits: ProcessLimits,
        limits: WorktreeLimits,
    ) -> Result<Self, WorktreeError> {
        let repository_id = repository_id.into();
        if !is_safe_identity_component(&repository_id) {
            return Err(WorktreeError::InvalidIdentity);
        }
        let (original_git_root, original_root_directory) =
            validated_directory(registered_git_root.as_ref())?;
        let common_git_path = original_git_root.join(".git");
        let (common_git_directory, common_git_execution_directory) =
            validated_directory(&common_git_path).map_err(|_| WorktreeError::InvalidRepository)?;
        // The execution directory, retained capability, and marker must all
        // originate from one authenticated directory object. Reopening the
        // path here would introduce a replacement window between them.
        let common_git_capability = common_git_execution_directory
            .cloned_root_capability()
            .map_err(WorktreeError::CommandPolicy)?;
        let common_git_identity = common_git_capability
            .identity_marker()
            .map_err(map_common_git_identity_error)?;

        let (cargo_workspace_path, cargo_workspace_directory) =
            validated_directory(registered_cargo_workspace.as_ref())?;
        let cargo_workspace_offset = cargo_workspace_path
            .strip_prefix(&original_git_root)
            .map_err(|_| WorktreeError::CargoWorkspaceOutsideRepository)?
            .to_owned();
        validate_relative_directory_mapping(
            &original_git_root,
            &cargo_workspace_offset,
            &cargo_workspace_directory,
        )?;

        let (artifact_root, artifact_root_directory) = validated_directory(artifact_root.as_ref())?;
        let artifact_root_capability =
            RootCapability::open(&artifact_root).map_err(WorktreeError::Io)?;
        let platform = platform_environment(temporary_directory.as_ref())?;
        let temporary_directory =
            std::fs::canonicalize(temporary_directory.as_ref()).map_err(WorktreeError::Io)?;
        let original_binding = GitCommandBinding::try_new(
            Arc::new(common_git_execution_directory),
            Arc::new(original_root_directory),
        )
        .map_err(WorktreeError::CommandPolicy)?;

        Ok(Self {
            supervisor: ProcessSupervisor::new(process_limits, process_liveness_scope.clone()),
            repository_id,
            toolchain: toolchain.clone(),
            process_liveness_scope,
            process_limits,
            temporary_directory,
            git: toolchain.git(),
            original_binding,
            common_git_directory,
            common_git_identity,
            common_git_capability,
            artifact_root,
            artifact_root_directory: Arc::new(artifact_root_directory),
            artifact_root_capability,
            cargo_workspace_offset,
            environment: worktree_environment(&platform),
            limits,
            #[cfg(feature = "test-support")]
            side_effect_boundary_hook: None,
            #[cfg(feature = "test-support")]
            side_effect_outcome_for_tests: None,
        })
    }

    /// Returns the authenticated common-Git object identity without exposing
    /// its directory path or platform identity components.
    pub const fn common_git_identity_marker(&self) -> DirectoryIdentityMarker {
        self.common_git_identity
    }

    /// Clones the retained, authenticated common-Git directory capability for
    /// read-only physical-volume sampling.
    ///
    /// The clone comes from the same directory object used by Git command
    /// binding and is revalidated before it crosses the runtime boundary. No
    /// repository path is reopened by the caller.
    pub fn clone_common_git_capability_for_volume_sampling(
        &self,
    ) -> Result<RootCapability, WorktreeError> {
        self.validate_common_git_identity()?;
        self.common_git_capability
            .try_clone_capability()
            .map_err(|_| WorktreeError::CommonGitIdentityUnavailable)
    }

    /// Installs a deterministic side-effect boundary hook for integration
    /// tests. Production builds do not contain this seam.
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn set_side_effect_boundary_hook_for_tests(
        &mut self,
        hook: impl Fn(&'static str, &'static str) + Send + Sync + 'static,
    ) {
        self.side_effect_boundary_hook = Some(Arc::new(hook));
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn set_side_effect_outcome_for_tests(&mut self, outcome: WorktreeSideEffectTestOutcome) {
        self.side_effect_outcome_for_tests = Some(outcome);
    }

    /// Computes and validates the exact artifact identity without creating a
    /// branch, worktree, admin entry, or artifact directory. The application
    /// persists this value as `reserved` before calling `provision_reserved`.
    pub async fn prepare(
        &self,
        identity: WorktreeIdentity,
        cancellation: CancellationToken,
    ) -> Result<WorktreeReservation, WorktreeError> {
        if identity.repository_id() != self.repository_id.as_str() {
            return Err(WorktreeError::InvalidReservation);
        }
        self.artifact_root_directory
            .revalidate()
            .map_err(WorktreeError::CommandPolicy)?;
        self.validate_common_git_identity()?;
        self.original_binding
            .revalidate()
            .map_err(WorktreeError::CommandPolicy)?;

        let target = self.artifact_root.join(identity.relative_path());
        if path_entry_exists(&target)? {
            return Err(WorktreeError::DestinationConflict);
        }
        let base_commit = self.resolve_head(cancellation.clone()).await?;
        self.reject_unsafe_local_configuration(cancellation.clone())
            .await?;
        let branch_name = identity.branch_name();
        if self.branch_exists(&branch_name, cancellation).await? {
            return Err(WorktreeError::BranchConflict);
        }
        Ok(WorktreeReservation {
            identity,
            base_commit,
            branch_name,
            worktree_path: target,
            source_common_git_identity: self.common_git_identity,
            cargo_workspace_offset: self.cargo_workspace_offset.clone(),
        })
    }

    /// Reconstructs an in-memory reservation from persisted control-plane
    /// columns. Repository identity markers are always derived from this
    /// provisioner rather than accepted from the database.
    pub fn restore_reservation(
        &self,
        identity: WorktreeIdentity,
        base_commit: impl Into<String>,
        branch_name: impl Into<String>,
        worktree_path: impl Into<PathBuf>,
    ) -> Result<WorktreeReservation, WorktreeError> {
        let reservation = WorktreeReservation {
            identity,
            base_commit: base_commit.into(),
            branch_name: branch_name.into(),
            worktree_path: worktree_path.into(),
            source_common_git_identity: self.common_git_identity,
            cargo_workspace_offset: self.cargo_workspace_offset.clone(),
        };
        self.validate_reservation(&reservation)?;
        Ok(reservation)
    }

    /// Performs the first Git side effect for a previously persisted
    /// reservation, then authenticates and materializes the linked worktree.
    pub async fn provision_reserved(
        &self,
        reservation: WorktreeReservation,
        cancellation: CancellationToken,
    ) -> Result<ProvisionedWorktree, WorktreeProvisionError> {
        macro_rules! before_add {
            ($result:expr) => {
                match $result {
                    Ok(value) => value,
                    Err(cause) => {
                        return Err(self
                            .classify_provision_failure(&reservation, cause, false)
                            .await);
                    }
                }
            };
        }

        before_add!(self.validate_reservation(&reservation));
        before_add!(
            self.artifact_root_directory
                .revalidate()
                .map_err(WorktreeError::CommandPolicy)
        );
        before_add!(self.validate_common_git_identity());
        before_add!(
            self.original_binding
                .revalidate()
                .map_err(WorktreeError::CommandPolicy)
        );

        // Configuration and identity are rechecked after persistence so a
        // host-side change cannot silently alter the reserved checkout.
        before_add!(
            self.reject_unsafe_local_configuration(cancellation.clone())
                .await
        );
        if before_add!(
            self.branch_exists(reservation.branch_name(), cancellation.clone())
                .await
        ) {
            return Err(self
                .classify_provision_failure(&reservation, WorktreeError::BranchConflict, false)
                .await);
        }

        let identity_path = reservation.identity.relative_path();
        let parent_path = before_add!(identity_path.parent().ok_or(WorktreeError::InvalidIdentity));
        let parent_relative = before_add!(relative_path_from_path(parent_path));
        let artifact_parent_guard = before_add!(
            self.artifact_root_capability
                .ensure_directory_path(&parent_relative)
                .map_err(|_| WorktreeError::ArtifactPathInvalid)
        );
        let _artifact_parent_handle = before_add!(
            artifact_parent_guard
                .try_clone_final()
                .map_err(WorktreeError::Io)
        );
        let attempt_name = OsString::from(reservation.identity.attempt().to_string());
        if !before_add!(
            artifact_parent_guard
                .child_is_absent(&attempt_name)
                .map_err(WorktreeError::Io)
        ) {
            return Err(self
                .classify_provision_failure(&reservation, WorktreeError::DestinationConflict, false)
                .await);
        }
        before_add!(
            self.artifact_root_directory
                .revalidate()
                .map_err(WorktreeError::CommandPolicy)
        );

        let before_admin_entries =
            before_add!(list_worktree_admin_entries(&self.common_git_capability));
        let add_result = match self
            .run_git_side_effect(
                GitSideEffectKind::WorktreeAdd,
                GitSideEffectConditions {
                    binding: &self.original_binding,
                    expected_head: reservation.base_commit(),
                    expected_symbolic_head: None,
                },
                cancellation.clone(),
                || {
                    ValidatedCommand::git_worktree_add(
                        Arc::clone(&self.git),
                        &self.original_binding,
                        self.environment.clone(),
                        reservation.branch_name(),
                        reservation.worktree_path(),
                        reservation.base_commit(),
                        self.limits.command_timeout,
                    )
                    .map_err(WorktreeError::CommandPolicy)
                },
            )
            .await
        {
            Ok(result) => result,
            Err(cause) => {
                return Err(self
                    .classify_provision_failure(&reservation, cause, false)
                    .await);
            }
        };
        if !command_succeeded(&add_result) {
            let cause = command_result_error(&add_result);
            return Err(self
                .classify_provision_failure(&reservation, cause, false)
                .await);
        }

        match self
            .finish_after_add(
                &reservation,
                &before_admin_entries,
                &artifact_parent_guard,
                &attempt_name,
                cancellation,
            )
            .await
        {
            Ok(worktree) => Ok(worktree),
            Err(cause) => Err(self
                .classify_provision_failure(&reservation, cause, true)
                .await),
        }
    }

    async fn finish_after_add(
        &self,
        reservation: &WorktreeReservation,
        before_admin_entries: &BTreeSet<OsString>,
        artifact_parent_guard: &DirectoryPathGuard,
        attempt_name: &std::ffi::OsStr,
        cancellation: CancellationToken,
    ) -> Result<ProvisionedWorktree, WorktreeError> {
        let identity = reservation.identity.clone();
        let base_commit = reservation.base_commit.clone();
        let branch_name = reservation.branch_name.clone();
        let target = reservation.worktree_path.clone();

        let (worktree_path, work_tree) = validated_directory(&target)?;
        let linked_git_directory =
            self.find_linked_git_directory(before_admin_entries, &worktree_path, &branch_name)?;
        let (_, linked_git_execution_directory) = validated_directory(&linked_git_directory)?;
        let linked_binding = GitCommandBinding::try_new(
            Arc::new(linked_git_execution_directory),
            Arc::new(work_tree),
        )
        .map_err(WorktreeError::CommandPolicy)?;

        let actual_head = self
            .resolve_bound_head(&linked_binding, cancellation.clone())
            .await?;
        if actual_head != base_commit {
            return Err(WorktreeError::PostconditionFailed);
        }
        let actual_ref = self
            .resolve_symbolic_head(&linked_binding, cancellation.clone())
            .await?;
        if actual_ref != format!("refs/heads/{branch_name}") {
            return Err(WorktreeError::PostconditionFailed);
        }

        self.validate_common_worktree_record(
            &worktree_path,
            &base_commit,
            &branch_name,
            cancellation.clone(),
        )
        .await?;
        if artifact_parent_guard
            .child_is_absent(attempt_name)
            .map_err(WorktreeError::Io)?
        {
            return Err(WorktreeError::PostconditionFailed);
        }

        let expected_symbolic_head = format!("refs/heads/{branch_name}");
        let reset_result = self
            .run_git_side_effect(
                GitSideEffectKind::Reset,
                GitSideEffectConditions {
                    binding: &linked_binding,
                    expected_head: &base_commit,
                    expected_symbolic_head: Some(&expected_symbolic_head),
                },
                cancellation.clone(),
                || {
                    ValidatedCommand::git_worktree_reset(
                        Arc::clone(&self.git),
                        &linked_binding,
                        self.environment.clone(),
                        &base_commit,
                        self.limits.command_timeout,
                    )
                    .map_err(WorktreeError::CommandPolicy)
                },
            )
            .await?;
        require_success(&reset_result)?;

        // The shared side-effect wrapper already proved that reset neither
        // detached nor moved HEAD. The remaining checks cover checkout and
        // common-side administrative postconditions.
        self.validate_clean_worktree(&linked_binding, cancellation.clone())
            .await?;
        self.validate_common_worktree_record(
            &worktree_path,
            &base_commit,
            &branch_name,
            cancellation.clone(),
        )
        .await?;

        let (cargo_workspace_path, cargo_workspace, target_directory, cargo_catalog) = self
            .create_and_validate_cargo_workspace(&worktree_path, cancellation.clone())
            .await?;
        self.validate_clean_worktree(&linked_binding, cancellation.clone())
            .await?;
        self.validate_common_worktree_record(
            &worktree_path,
            &base_commit,
            &branch_name,
            cancellation.clone(),
        )
        .await?;
        if cancellation.is_cancelled() {
            return Err(WorktreeError::Cancelled);
        }

        let git_directory = Arc::new(
            ExecutionDirectory::open(&linked_git_directory)
                .map_err(WorktreeError::CommandPolicy)?,
        );
        let work_tree = Arc::new(
            ExecutionDirectory::open(&worktree_path).map_err(WorktreeError::CommandPolicy)?,
        );
        Ok(ProvisionedWorktree {
            identity,
            base_commit,
            branch_name,
            worktree_path,
            cargo_workspace_path,
            git_directory,
            work_tree,
            cargo_workspace,
            target_directory,
            cargo_catalog,
        })
    }

    fn validate_reservation(&self, reservation: &WorktreeReservation) -> Result<(), WorktreeError> {
        if reservation.branch_name != reservation.identity.branch_name()
            || reservation.identity.repository_id() != self.repository_id.as_str()
            || reservation.worktree_path
                != self
                    .artifact_root
                    .join(reservation.identity.relative_path())
            || reservation.source_common_git_identity != self.common_git_identity
            || reservation.cargo_workspace_offset != self.cargo_workspace_offset
            || parse_commit_output(reservation.base_commit.as_bytes()).is_err()
        {
            return Err(WorktreeError::InvalidReservation);
        }
        Ok(())
    }

    /// Read-only reconstruction used after crashes, cancellation, timeout, or
    /// any ambiguous Git/process outcome. It never deletes or repairs state.
    pub async fn observe(
        &self,
        reservation: &WorktreeReservation,
        cancellation: CancellationToken,
    ) -> WorktreeObservation {
        self.observe_with_safety(reservation, cancellation)
            .await
            .observation()
    }

    /// Preserves process-cleanup provenance for control-plane callers. A plain
    /// `Unavailable` observation is safe to retry only when this outcome says
    /// every observation child process was proven stopped.
    pub async fn observe_with_safety(
        &self,
        reservation: &WorktreeReservation,
        cancellation: CancellationToken,
    ) -> WorktreeObservationOutcome {
        match self.observe_inner(reservation, cancellation).await {
            Ok(outcome) => outcome,
            Err(error) => {
                let observation = match error.artifact_state_after_failed_observation() {
                    WorktreeArtifactState::Inconsistent => WorktreeObservation::Inconsistent,
                    WorktreeArtifactState::Unavailable => WorktreeObservation::Unavailable,
                    WorktreeArtifactState::Absent
                    | WorktreeArtifactState::Partial
                    | WorktreeArtifactState::Ready => {
                        unreachable!(
                            "failed observation only classifies exact or unavailable state"
                        )
                    }
                };
                WorktreeObservationOutcome {
                    observation,
                    process_cleanup_unproven: error.process_cleanup_is_unproven(),
                    repository_poison_required: error.requires_repository_poison(),
                }
            }
        }
    }

    /// Reopens an already persisted and fully ready reservation after process
    /// recovery. This path is deliberately read-only: it authenticates the
    /// existing linked-worktree administration entry and checkout, but never
    /// creates, resets, repairs, or removes repository state.
    pub async fn open_ready(
        &self,
        reservation: &WorktreeReservation,
        cancellation: CancellationToken,
    ) -> Result<ProvisionedWorktree, WorktreeError> {
        self.validate_reservation(reservation)?;
        if self
            .observe_inner(reservation, cancellation.clone())
            .await?
            .observation()
            != WorktreeObservation::Ready
        {
            return Err(WorktreeError::InconsistentArtifact);
        }

        let git_directory_path = self
            .find_reserved_git_directory(reservation)?
            .ok_or(WorktreeError::InconsistentArtifact)?;
        let (worktree_path, work_tree) = validated_directory(reservation.worktree_path())?;
        let (_, git_directory) = validated_directory(&git_directory_path)?;
        let git_directory = Arc::new(git_directory);
        let work_tree = Arc::new(work_tree);
        let binding =
            GitCommandBinding::try_new(Arc::clone(&git_directory), Arc::clone(&work_tree))
                .map_err(WorktreeError::CommandPolicy)?;
        let (cargo_workspace, target_directory, cargo_catalog) = self
            .validate_existing_cargo_workspace(reservation, cancellation.clone())
            .await?;

        // Repeat the control-plane checks after all long-lived capabilities
        // are open, closing the recovery-time substitution window.
        if self
            .resolve_bound_head(&binding, cancellation.clone())
            .await?
            != reservation.base_commit()
            || self
                .resolve_symbolic_head(&binding, cancellation.clone())
                .await?
                != format!("refs/heads/{}", reservation.branch_name())
        {
            return Err(WorktreeError::InconsistentArtifact);
        }
        self.validate_clean_worktree(&binding, cancellation.clone())
            .await?;
        self.validate_common_worktree_record(
            &worktree_path,
            reservation.base_commit(),
            reservation.branch_name(),
            cancellation.clone(),
        )
        .await?;
        if cancellation.is_cancelled() {
            return Err(WorktreeError::Cancelled);
        }

        Ok(ProvisionedWorktree {
            identity: reservation.identity.clone(),
            base_commit: reservation.base_commit.clone(),
            branch_name: reservation.branch_name.clone(),
            worktree_path: worktree_path.clone(),
            cargo_workspace_path: worktree_path.join(&reservation.cargo_workspace_offset),
            git_directory,
            work_tree,
            cargo_workspace,
            target_directory,
            cargo_catalog,
        })
    }

    async fn observe_inner(
        &self,
        reservation: &WorktreeReservation,
        cancellation: CancellationToken,
    ) -> Result<WorktreeObservationOutcome, WorktreeError> {
        self.validate_reservation(reservation)?;
        self.validate_common_git_identity()?;
        self.original_binding
            .revalidate()
            .map_err(WorktreeError::CommandPolicy)?;
        self.artifact_root_directory
            .revalidate()
            .map_err(WorktreeError::CommandPolicy)?;

        let branch = self
            .resolve_optional_branch(reservation.branch_name(), cancellation.clone())
            .await?;
        if branch
            .as_ref()
            .is_some_and(|commit| commit != reservation.base_commit())
        {
            return Ok(WorktreeObservationOutcome::exact(
                WorktreeObservation::Inconsistent,
            ));
        }

        let target_relative = relative_path_from_path(&reservation.identity.relative_path())?;
        let target_directory = match self
            .artifact_root_capability
            .open_directory(&target_relative)
        {
            Ok(directory) => Some(directory),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(WorktreeError::Io(error)),
        };
        let admin = self.find_reserved_git_directory(reservation)?;

        match (branch.is_some(), admin.as_ref(), target_directory.as_ref()) {
            (false, None, None) => {
                return Ok(WorktreeObservationOutcome::exact(
                    WorktreeObservation::Absent,
                ));
            }
            (true, None, None) => {
                return Ok(WorktreeObservationOutcome::exact(
                    WorktreeObservation::BranchOnly,
                ));
            }
            (true, Some(_), None) => {
                return Ok(WorktreeObservationOutcome::exact(
                    WorktreeObservation::AdministrativeCreated,
                ));
            }
            (true, Some(_), Some(_)) => {}
            _ => {
                return Ok(WorktreeObservationOutcome::exact(
                    WorktreeObservation::Inconsistent,
                ));
            }
        }
        if target_directory
            .as_ref()
            .is_some_and(directory_contains_only_git_pointer)
        {
            return Ok(WorktreeObservationOutcome::exact(
                WorktreeObservation::AdministrativeCreated,
            ));
        }

        let git_directory_path = admin.expect("matched observation has an admin directory");
        let (_, git_directory) = validated_directory(&git_directory_path)?;
        let (_, work_tree) = validated_directory(reservation.worktree_path())?;
        let binding = GitCommandBinding::try_new(Arc::new(git_directory), Arc::new(work_tree))
            .map_err(WorktreeError::CommandPolicy)?;
        if self
            .resolve_bound_head(&binding, cancellation.clone())
            .await?
            != reservation.base_commit()
            || self
                .resolve_symbolic_head(&binding, cancellation.clone())
                .await?
                != format!("refs/heads/{}", reservation.branch_name())
        {
            return Ok(WorktreeObservationOutcome::exact(
                WorktreeObservation::Inconsistent,
            ));
        }
        if let Err(error) = self
            .validate_common_worktree_record(
                reservation.worktree_path(),
                reservation.base_commit(),
                reservation.branch_name(),
                cancellation.clone(),
            )
            .await
        {
            if error.is_positive_artifact_evidence() {
                return Ok(WorktreeObservationOutcome::positive_evidence(
                    WorktreeObservation::Inconsistent,
                    &error,
                ));
            }
            return Err(error);
        }

        if let Err(error) = self
            .validate_clean_worktree(&binding, cancellation.clone())
            .await
        {
            if error.is_positive_artifact_evidence() {
                return Ok(WorktreeObservationOutcome::positive_evidence(
                    WorktreeObservation::CheckoutPartial,
                    &error,
                ));
            }
            return Err(error);
        }
        if let Err(error) = self
            .validate_existing_cargo_workspace(reservation, cancellation.clone())
            .await
        {
            if error.is_positive_artifact_evidence() {
                return Ok(WorktreeObservationOutcome::positive_evidence(
                    WorktreeObservation::CheckoutPartial,
                    &error,
                ));
            }
            return Err(error);
        }
        if let Err(error) = self
            .validate_clean_worktree(&binding, cancellation.clone())
            .await
        {
            if error.is_positive_artifact_evidence() {
                return Ok(WorktreeObservationOutcome::positive_evidence(
                    WorktreeObservation::CheckoutPartial,
                    &error,
                ));
            }
            return Err(error);
        }
        if let Err(error) = self
            .validate_common_worktree_record(
                reservation.worktree_path(),
                reservation.base_commit(),
                reservation.branch_name(),
                cancellation,
            )
            .await
        {
            if error.is_positive_artifact_evidence() {
                return Ok(WorktreeObservationOutcome::positive_evidence(
                    WorktreeObservation::CheckoutPartial,
                    &error,
                ));
            }
            return Err(error);
        }
        Ok(WorktreeObservationOutcome::exact(
            WorktreeObservation::Ready,
        ))
    }

    async fn resolve_optional_branch(
        &self,
        branch_name: &str,
        cancellation: CancellationToken,
    ) -> Result<Option<String>, WorktreeError> {
        if !self
            .branch_exists(branch_name, cancellation.clone())
            .await?
        {
            return Ok(None);
        }
        let command = ValidatedCommand::git_resolve_branch(
            Arc::clone(&self.git),
            &self.original_binding,
            self.environment.clone(),
            branch_name,
            self.limits.command_timeout,
        )
        .map_err(WorktreeError::CommandPolicy)?;
        let result = self.run(command, cancellation).await?;
        require_success(&result)?;
        parse_commit_output(&complete_stdout(&result)?).map(Some)
    }

    async fn validate_existing_cargo_workspace(
        &self,
        reservation: &WorktreeReservation,
        cancellation: CancellationToken,
    ) -> Result<
        (
            Arc<ExecutionDirectory>,
            Arc<ExecutionDirectory>,
            CargoCatalog,
        ),
        WorktreeError,
    > {
        let cargo_path = reservation
            .worktree_path
            .join(&reservation.cargo_workspace_offset);
        let (_, cargo_directory) = match validated_directory(&cargo_path) {
            Ok(directory) => directory,
            Err(error) if error_is_not_found(&error) => {
                return Err(WorktreeError::NestedWorkspaceMissing);
            }
            Err(error) => return Err(error),
        };
        let cargo_directory = Arc::new(cargo_directory);
        let cargo_capability = RootCapability::open(&cargo_path).map_err(WorktreeError::Io)?;
        let target_relative = RelativePath::parse("target".to_owned())
            .map_err(|_| WorktreeError::ArtifactPathInvalid)?;
        match cargo_capability.open_directory(&target_relative) {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(WorktreeError::PartialCreation);
            }
            Err(error) => return Err(WorktreeError::Io(error)),
        }
        let (_, target_directory) = match validated_directory(&cargo_path.join("target")) {
            Ok(directory) => directory,
            Err(error) if error_is_not_found(&error) => {
                return Err(WorktreeError::PartialCreation);
            }
            Err(error) => return Err(error),
        };
        let target_directory = Arc::new(target_directory);
        let catalog = self
            .validate_cargo_metadata(
                Arc::clone(&cargo_directory),
                Arc::clone(&target_directory),
                cancellation,
                true,
            )
            .await?;
        Ok((cargo_directory, target_directory, catalog))
    }

    async fn create_and_validate_cargo_workspace(
        &self,
        worktree_path: &Path,
        cancellation: CancellationToken,
    ) -> Result<
        (
            PathBuf,
            Arc<ExecutionDirectory>,
            Arc<ExecutionDirectory>,
            CargoCatalog,
        ),
        WorktreeError,
    > {
        let cargo_path = worktree_path.join(&self.cargo_workspace_offset);
        let (_, cargo_directory) = match validated_directory(&cargo_path) {
            Ok(directory) => directory,
            Err(error) if error_is_not_found(&error) => {
                return Err(WorktreeError::NestedWorkspaceMissing);
            }
            Err(error) => return Err(error),
        };
        let cargo_directory = Arc::new(cargo_directory);
        let cargo_capability = RootCapability::open(&cargo_path).map_err(WorktreeError::Io)?;
        let target_relative = RelativePath::parse("target".to_owned())
            .map_err(|_| WorktreeError::ArtifactPathInvalid)?;
        let target_guard = cargo_capability
            .ensure_directory_path(&target_relative)
            .map_err(WorktreeError::Io)?;
        let (_, target_directory) = validated_directory(&cargo_path.join("target"))?;
        let target_directory = Arc::new(target_directory);
        let catalog = self
            .validate_cargo_metadata(
                Arc::clone(&cargo_directory),
                Arc::clone(&target_directory),
                cancellation,
                false,
            )
            .await?;
        drop(target_guard);
        Ok((cargo_path, cargo_directory, target_directory, catalog))
    }

    async fn validate_cargo_metadata(
        &self,
        cargo_directory: Arc<ExecutionDirectory>,
        target_directory: Arc<ExecutionDirectory>,
        cancellation: CancellationToken,
        read_only: bool,
    ) -> Result<CargoCatalog, WorktreeError> {
        let limits = CargoToolLimits::try_new(self.limits.command_timeout, 256, 2_048, 256)
            .map_err(WorktreeError::Cargo)?;
        let tools = CargoTools::from_trusted_capabilities(
            &self.toolchain,
            cargo_directory,
            target_directory,
            &self.temporary_directory,
            self.process_liveness_scope.clone(),
            self.process_limits,
            limits,
        )
        .map_err(WorktreeError::Cargo)?;
        if read_only {
            tools
                .catalog_read_only(cancellation)
                .await
                .map_err(WorktreeError::Cargo)
        } else {
            tools
                .catalog(cancellation)
                .await
                .map_err(WorktreeError::Cargo)
        }
    }

    async fn resolve_head(&self, cancellation: CancellationToken) -> Result<String, WorktreeError> {
        let command = ValidatedCommand::git_resolve_head(
            Arc::clone(&self.git),
            &self.original_binding,
            self.environment.clone(),
            self.limits.command_timeout,
        )
        .map_err(WorktreeError::CommandPolicy)?;
        let result = self.run(command, cancellation).await?;
        if result.cancelled {
            return Err(WorktreeError::Cancelled);
        }
        if result.timed_out {
            return Err(WorktreeError::TimedOut);
        }
        if !command_succeeded(&result) {
            return Err(WorktreeError::UnbornHead);
        }
        parse_commit_output(&complete_stdout(&result)?)
    }

    async fn reject_unsafe_local_configuration(
        &self,
        cancellation: CancellationToken,
    ) -> Result<(), WorktreeError> {
        let command = ValidatedCommand::git_scan_local_configuration(
            Arc::clone(&self.git),
            &self.original_binding,
            self.environment.clone(),
            self.limits.command_timeout,
        )
        .map_err(WorktreeError::CommandPolicy)?;
        let result = self.run(command, cancellation).await?;
        if result.cancelled {
            return Err(WorktreeError::Cancelled);
        }
        if result.timed_out {
            return Err(WorktreeError::TimedOut);
        }
        if !command_succeeded(&result) {
            return Err(WorktreeError::GitCommandFailed);
        }
        let output = complete_stdout(&result)?;
        if output.is_empty() {
            return Ok(());
        }
        let Some(names) = output.strip_suffix(&[0]) else {
            return Err(WorktreeError::UnsafeGitConfiguration);
        };
        let mut count = 0usize;
        for raw_name in names.split(|byte| *byte == 0) {
            count += 1;
            if count > MAX_LOCAL_CONFIG_ENTRIES
                || raw_name.is_empty()
                || raw_name.len() > MAX_LOCAL_CONFIG_KEY_BYTES
            {
                return Err(WorktreeError::UnsafeGitConfiguration);
            }
            let name = std::str::from_utf8(raw_name)
                .map_err(|_| WorktreeError::UnsafeGitConfiguration)?
                .to_ascii_lowercase();
            if name.starts_with("filter.")
                || name == "include.path"
                || (name.starts_with("includeif.") && name.ends_with(".path"))
                || name == "extensions.worktreeconfig"
                || name == "extensions.relativeworktrees"
                || name == "worktree.userelativepaths"
            {
                return Err(WorktreeError::UnsafeGitConfiguration);
            }
        }
        Ok(())
    }

    async fn branch_exists(
        &self,
        branch_name: &str,
        cancellation: CancellationToken,
    ) -> Result<bool, WorktreeError> {
        let command = ValidatedCommand::git_branch_exists(
            Arc::clone(&self.git),
            &self.original_binding,
            self.environment.clone(),
            branch_name,
            self.limits.command_timeout,
        )
        .map_err(WorktreeError::CommandPolicy)?;
        let result = self.run(command, cancellation).await?;
        if result.cancelled {
            return Err(WorktreeError::Cancelled);
        }
        if result.timed_out {
            return Err(WorktreeError::TimedOut);
        }
        match (result.exit_code, result.signal) {
            (Some(0), None) => Ok(true),
            (Some(1), None) => Ok(false),
            _ => Err(WorktreeError::GitCommandFailed),
        }
    }

    async fn resolve_bound_head(
        &self,
        binding: &GitCommandBinding,
        cancellation: CancellationToken,
    ) -> Result<String, WorktreeError> {
        let command = ValidatedCommand::git_resolve_head(
            Arc::clone(&self.git),
            binding,
            self.environment.clone(),
            self.limits.command_timeout,
        )
        .map_err(WorktreeError::CommandPolicy)?;
        let result = self.run(command, cancellation).await?;
        require_success(&result)?;
        parse_commit_output(&complete_stdout(&result)?)
    }

    async fn resolve_symbolic_head(
        &self,
        binding: &GitCommandBinding,
        cancellation: CancellationToken,
    ) -> Result<String, WorktreeError> {
        let command = ValidatedCommand::git_symbolic_head(
            Arc::clone(&self.git),
            binding,
            self.environment.clone(),
            self.limits.command_timeout,
        )
        .map_err(WorktreeError::CommandPolicy)?;
        let result = self.run(command, cancellation).await?;
        require_success(&result)?;
        parse_one_line(&complete_stdout(&result)?)
    }

    async fn validate_clean_worktree(
        &self,
        binding: &GitCommandBinding,
        cancellation: CancellationToken,
    ) -> Result<(), WorktreeError> {
        let command = ValidatedCommand::git_status(
            Arc::clone(&self.git),
            binding,
            self.environment.clone(),
            self.limits.command_timeout,
        )
        .map_err(WorktreeError::CommandPolicy)?;
        let result = self.run(command, cancellation).await?;
        require_success(&result)?;
        if complete_stdout(&result)?.is_empty() {
            Ok(())
        } else {
            Err(WorktreeError::WorktreeContentChanged)
        }
    }

    async fn validate_common_worktree_record(
        &self,
        worktree_path: &Path,
        base_commit: &str,
        branch_name: &str,
        cancellation: CancellationToken,
    ) -> Result<(), WorktreeError> {
        let command = ValidatedCommand::git_worktree_list(
            Arc::clone(&self.git),
            &self.original_binding,
            self.environment.clone(),
            self.limits.command_timeout,
        )
        .map_err(WorktreeError::CommandPolicy)?;
        let result = self.run(command, cancellation).await?;
        require_success(&result)?;
        validate_worktree_list_record(
            &complete_stdout(&result)?,
            worktree_path,
            base_commit,
            branch_name,
        )
    }

    fn validate_common_git_identity(&self) -> Result<(), WorktreeError> {
        self.common_git_capability
            .require_identity(self.common_git_identity)
            .map_err(map_common_git_identity_error)?;
        let live = RootCapability::open(&self.common_git_directory)
            .map_err(|_| WorktreeError::CommonGitIdentityUnavailable)?;
        live.require_identity(self.common_git_identity)
            .map_err(map_common_git_identity_error)
    }

    async fn validate_git_side_effect_boundary(
        &self,
        conditions: GitSideEffectConditions<'_>,
    ) -> Result<(), WorktreeError> {
        self.validate_common_git_identity()?;
        conditions
            .binding
            .revalidate()
            .map_err(WorktreeError::CommandPolicy)?;
        if self
            .resolve_bound_head(conditions.binding, CancellationToken::new())
            .await?
            != conditions.expected_head
        {
            return Err(WorktreeError::PostconditionFailed);
        }
        if let Some(expected_symbolic_head) = conditions.expected_symbolic_head
            && self
                .resolve_symbolic_head(conditions.binding, CancellationToken::new())
                .await?
                != expected_symbolic_head
        {
            return Err(WorktreeError::PostconditionFailed);
        }
        Ok(())
    }

    /// Executes every common-Git mutation through one fail-closed boundary.
    ///
    /// Post-validation runs after every process-clean command outcome,
    /// including cancellation, timeout, and non-zero exit. If process cleanup
    /// is unproven, no further Git command may start and that safety fact keeps
    /// absolute precedence over secondary identity evidence.
    async fn run_git_side_effect(
        &self,
        _kind: GitSideEffectKind,
        conditions: GitSideEffectConditions<'_>,
        cancellation: CancellationToken,
        build: impl FnOnce() -> Result<ValidatedCommand, WorktreeError>,
    ) -> Result<CommandResult, WorktreeError> {
        self.validate_git_side_effect_boundary(conditions).await?;

        #[cfg(feature = "test-support")]
        if let Some(hook) = &self.side_effect_boundary_hook {
            hook(_kind.test_label(), "before-command");
        }

        #[cfg(feature = "test-support")]
        let command_result = match self.side_effect_outcome_for_tests {
            Some(outcome) => injected_side_effect_outcome(outcome),
            None => match build() {
                Ok(command) => self.run(command, cancellation).await,
                Err(error) => Err(error),
            },
        };
        #[cfg(not(feature = "test-support"))]
        let command_result = match build() {
            Ok(command) => self.run(command, cancellation).await,
            Err(error) => Err(error),
        };

        #[cfg(feature = "test-support")]
        if let Some(hook) = &self.side_effect_boundary_hook {
            hook(_kind.test_label(), "after-command");
        }

        if command_result
            .as_ref()
            .is_err_and(WorktreeError::process_cleanup_is_unproven)
        {
            return command_result;
        }
        self.validate_git_side_effect_boundary(conditions).await?;
        command_result
    }

    async fn run(
        &self,
        command: ValidatedCommand,
        cancellation: CancellationToken,
    ) -> Result<CommandResult, WorktreeError> {
        self.supervisor
            .run(command, cancellation)
            .await
            .map_err(WorktreeError::Process)
    }

    async fn classify_provision_failure(
        &self,
        reservation: &WorktreeReservation,
        cause: WorktreeError,
        add_reported_success: bool,
    ) -> WorktreeProvisionError {
        if cause.process_cleanup_is_unproven() {
            return WorktreeProvisionError {
                repository_poison_required: cause.requires_repository_poison(),
                cause,
                observation: WorktreeObservation::Unavailable,
                process_cleanup_unproven: true,
            };
        }
        let fresh_observation = self
            .observe_with_safety(reservation, CancellationToken::new())
            .await;
        let process_cleanup_unproven = fresh_observation.process_cleanup_is_unproven();
        let repository_poison_required =
            cause.requires_repository_poison() || fresh_observation.repository_poison_required();
        let mut observation = fresh_observation.observation();
        if (add_reported_success && observation == WorktreeObservation::Absent)
            || (observation == WorktreeObservation::Unavailable
                && cause.is_positive_artifact_evidence())
        {
            observation = WorktreeObservation::Inconsistent;
        }
        WorktreeProvisionError {
            cause,
            observation,
            process_cleanup_unproven,
            repository_poison_required,
        }
    }

    fn find_linked_git_directory(
        &self,
        previous: &BTreeSet<OsString>,
        worktree_path: &Path,
        branch_name: &str,
    ) -> Result<PathBuf, WorktreeError> {
        let current = list_worktree_admin_entries(&self.common_git_capability)?;
        let expected_pointer = match std::fs::canonicalize(worktree_path.join(".git")) {
            Ok(pointer) => pointer,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(WorktreeError::LinkedMetadataInvalid);
            }
            Err(error) => return Err(WorktreeError::Io(error)),
        };
        let mut matches = Vec::new();
        for name in current.difference(previous) {
            let Some(name) = name.to_str() else {
                continue;
            };
            if RelativePath::parse(name.to_owned()).is_err() || name.contains('/') {
                continue;
            }
            let pointer = read_admin_gitdir(&self.common_git_capability, name)?;
            let pointer = match std::fs::canonicalize(pointer) {
                Ok(pointer) => pointer,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(WorktreeError::Io(error)),
            };
            if pointer == expected_pointer {
                let expected_head = format!("ref: refs/heads/{branch_name}");
                if read_admin_line(&self.common_git_capability, name, "HEAD")? != expected_head
                    || read_admin_line(&self.common_git_capability, name, "locked")?
                        != "codex-reserved"
                    || !admin_commondir_matches(
                        &self.common_git_capability,
                        &self.common_git_directory,
                        name,
                    )?
                {
                    continue;
                }
                matches.push(self.common_git_directory.join("worktrees").join(name));
            }
        }
        if matches.len() == 1 {
            Ok(matches.remove(0))
        } else {
            Err(WorktreeError::LinkedMetadataInvalid)
        }
    }

    fn find_reserved_git_directory(
        &self,
        reservation: &WorktreeReservation,
    ) -> Result<Option<PathBuf>, WorktreeError> {
        let current = list_worktree_admin_entries(&self.common_git_capability)?;
        let mut matches = Vec::new();
        for name in current {
            let Some(name) = name.to_str() else {
                continue;
            };
            if RelativePath::parse(name.to_owned()).is_err() || name.contains('/') {
                continue;
            }
            let pointer = match read_admin_gitdir(&self.common_git_capability, name) {
                Ok(pointer) => pointer,
                Err(WorktreeError::LinkedMetadataInvalid) => continue,
                Err(error) => return Err(error),
            };
            if !admin_backlink_matches(&pointer, reservation.worktree_path())? {
                continue;
            }
            if read_admin_line(&self.common_git_capability, name, "HEAD")?
                != format!("ref: refs/heads/{}", reservation.branch_name())
                || read_admin_line(&self.common_git_capability, name, "locked")? != "codex-reserved"
                || !admin_commondir_matches(
                    &self.common_git_capability,
                    &self.common_git_directory,
                    name,
                )?
            {
                return Err(WorktreeError::LinkedMetadataInvalid);
            }
            matches.push(self.common_git_directory.join("worktrees").join(name));
        }
        match matches.len() {
            0 => Ok(None),
            1 => Ok(matches.pop()),
            _ => Err(WorktreeError::LinkedMetadataInvalid),
        }
    }
}

fn admin_backlink_matches(pointer: &Path, worktree_path: &Path) -> Result<bool, WorktreeError> {
    let expected = child_visible_path(worktree_path).join(".git");
    match (
        std::fs::canonicalize(pointer),
        std::fs::canonicalize(&expected),
    ) {
        (Ok(pointer), Ok(expected)) => Ok(pointer == expected),
        (Err(pointer_error), _) if pointer_error.kind() != io::ErrorKind::NotFound => {
            Err(WorktreeError::Io(pointer_error))
        }
        (_, Err(expected_error)) if expected_error.kind() != io::ErrorKind::NotFound => {
            Err(WorktreeError::Io(expected_error))
        }
        _ => Ok(pointer == expected),
    }
}

fn directory_contains_only_git_pointer(directory: &File) -> bool {
    let Ok(mut directory) = directory.try_clone() else {
        return false;
    };
    let Ok(names) = read_directory_names(&mut directory, 4) else {
        return false;
    };
    let names = names
        .into_iter()
        .filter(|name| name != "." && name != "..")
        .collect::<Vec<_>>();
    names.len() == 1
        && names[0]
            .to_str()
            .is_some_and(|name| name.eq_ignore_ascii_case(".git"))
}

fn validated_directory(path: &Path) -> Result<(PathBuf, ExecutionDirectory), WorktreeError> {
    let original = ExecutionDirectory::open(path).map_err(WorktreeError::CommandPolicy)?;
    let canonical = std::fs::canonicalize(path).map_err(WorktreeError::Io)?;
    let canonical_directory =
        ExecutionDirectory::open(&canonical).map_err(WorktreeError::CommandPolicy)?;
    if !original.has_same_identity(&canonical_directory) {
        return Err(WorktreeError::InvalidRepository);
    }
    Ok((canonical, canonical_directory))
}

#[cfg(feature = "test-support")]
fn injected_side_effect_outcome(
    outcome: WorktreeSideEffectTestOutcome,
) -> Result<CommandResult, WorktreeError> {
    match outcome {
        WorktreeSideEffectTestOutcome::NonZero => Ok(test_command_result(Some(17), false)),
        WorktreeSideEffectTestOutcome::TimedOut => Ok(test_command_result(None, true)),
        WorktreeSideEffectTestOutcome::ProcessError => {
            Err(WorktreeError::Process(ProcessError::InvalidCommand))
        }
        WorktreeSideEffectTestOutcome::CleanupUnproven => {
            Err(WorktreeError::Process(ProcessError::CleanupTimedOut))
        }
        WorktreeSideEffectTestOutcome::BuildError => Err(WorktreeError::CommandPolicy(
            CommandPolicyError::InvalidGitBinding,
        )),
    }
}

#[cfg(feature = "test-support")]
fn test_command_result(exit_code: Option<i32>, timed_out: bool) -> CommandResult {
    let stream = || crate::CapturedStream {
        head: Vec::new(),
        tail: Vec::new(),
        observed_bytes: 0,
        omitted_observed_bytes: 0,
        truncated: false,
        complete: true,
    };
    CommandResult {
        exit_code,
        signal: None,
        timed_out,
        cancelled: false,
        stdout: stream(),
        stderr: stream(),
        truncated: false,
        duration_ms: 0,
    }
}

fn validate_relative_directory_mapping(
    root: &Path,
    offset: &Path,
    expected: &ExecutionDirectory,
) -> Result<(), WorktreeError> {
    if offset.is_absolute()
        || offset.components().any(|component| {
            matches!(
                component,
                std::path::Component::CurDir
                    | std::path::Component::ParentDir
                    | std::path::Component::RootDir
                    | std::path::Component::Prefix(_)
            )
        })
    {
        return Err(WorktreeError::CargoWorkspaceOutsideRepository);
    }
    let mapped = ExecutionDirectory::open(root.join(offset))
        .map_err(|_| WorktreeError::CargoWorkspaceOutsideRepository)?;
    if mapped.has_same_identity(expected) {
        Ok(())
    } else {
        Err(WorktreeError::CargoWorkspaceOutsideRepository)
    }
}

fn list_worktree_admin_entries(
    capability: &RootCapability,
) -> Result<BTreeSet<OsString>, WorktreeError> {
    let relative = RelativePath::parse("worktrees".to_owned())
        .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
    let mut directory = match capability.open_directory(&relative) {
        Ok(directory) => directory,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(BTreeSet::new()),
        Err(error) => return Err(WorktreeError::Io(error)),
    };
    let names =
        read_directory_names(&mut directory, MAX_ADMIN_ENTRIES).map_err(WorktreeError::Io)?;
    Ok(names
        .into_iter()
        .filter(|name| name != "." && name != "..")
        .collect())
}

fn read_admin_gitdir(capability: &RootCapability, name: &str) -> Result<PathBuf, WorktreeError> {
    let value = read_admin_line(capability, name, "gitdir")?;
    let path = PathBuf::from(value);
    if path.is_absolute() {
        Ok(path)
    } else {
        Err(WorktreeError::LinkedMetadataInvalid)
    }
}

fn read_admin_line(
    capability: &RootCapability,
    name: &str,
    file_name: &str,
) -> Result<String, WorktreeError> {
    let relative = RelativePath::parse(format!("worktrees/{name}/{file_name}"))
        .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
    let mut file = match capability.open_file_for_read(&relative) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(WorktreeError::LinkedMetadataInvalid);
        }
        Err(error) => return Err(WorktreeError::Io(error)),
    };
    let mut bytes = Vec::new();
    file.by_ref()
        .take(ADMIN_FILE_LIMIT + 1)
        .read_to_end(&mut bytes)
        .map_err(WorktreeError::Io)?;
    if bytes.len() as u64 > ADMIN_FILE_LIMIT || bytes.contains(&0) {
        return Err(WorktreeError::LinkedMetadataInvalid);
    }
    while matches!(bytes.last(), Some(b'\n' | b'\r')) {
        bytes.pop();
    }
    let value = std::str::from_utf8(&bytes).map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
    if value.is_empty() || value.contains(['\r', '\n']) {
        Err(WorktreeError::LinkedMetadataInvalid)
    } else {
        Ok(value.to_owned())
    }
}

fn admin_commondir_matches(
    capability: &RootCapability,
    common_git_directory: &Path,
    name: &str,
) -> Result<bool, WorktreeError> {
    let commondir = read_admin_line(capability, name, "commondir")?;
    // Git's common-side linked-worktree layout is fixed. Requiring the exact
    // relative backlink avoids treating arbitrary metadata text as a path
    // authority, while canonical identity checks catch namespace rebinding.
    if commondir != "../.." {
        return Ok(false);
    }
    let resolved = match std::fs::canonicalize(
        common_git_directory
            .join("worktrees")
            .join(name)
            .join(&commondir),
    ) {
        Ok(resolved) => resolved,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(WorktreeError::Io(error)),
    };
    Ok(resolved == common_git_directory)
}

fn validate_worktree_list_record(
    output: &[u8],
    expected_path: &Path,
    base_commit: &str,
    branch_name: &str,
) -> Result<(), WorktreeError> {
    let expected_path = match std::fs::canonicalize(expected_path) {
        Ok(path) => path,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(WorktreeError::LinkedMetadataInvalid);
        }
        Err(error) => return Err(WorktreeError::Io(error)),
    };
    let expected_branch = format!("refs/heads/{branch_name}");
    let mut matches = 0usize;
    let mut path: Option<PathBuf> = None;
    let mut head: Option<&str> = None;
    let mut branch: Option<&str> = None;

    for field in output.split(|byte| *byte == 0) {
        if field.is_empty() {
            if let Some(candidate) = path.take() {
                let canonical = match std::fs::canonicalize(candidate) {
                    Ok(path) => Some(path),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => None,
                    Err(error) => return Err(WorktreeError::Io(error)),
                };
                if canonical.as_deref() == Some(expected_path.as_path())
                    && head == Some(base_commit)
                    && branch == Some(expected_branch.as_str())
                {
                    matches += 1;
                }
            }
            head = None;
            branch = None;
            continue;
        }
        let field = std::str::from_utf8(field).map_err(|_| WorktreeError::OutputInvalid)?;
        if let Some(value) = field.strip_prefix("worktree ") {
            if path.is_some() {
                return Err(WorktreeError::OutputInvalid);
            }
            path = Some(PathBuf::from(value));
        } else if let Some(value) = field.strip_prefix("HEAD ") {
            head = Some(value);
        } else if let Some(value) = field.strip_prefix("branch ") {
            branch = Some(value);
        }
    }
    if path.is_some() {
        return Err(WorktreeError::OutputInvalid);
    }
    if matches == 1 {
        Ok(())
    } else {
        Err(WorktreeError::PostconditionFailed)
    }
}

fn relative_path_from_path(path: &Path) -> Result<RelativePath, WorktreeError> {
    let mut value = String::new();
    for component in path.components() {
        let std::path::Component::Normal(component) = component else {
            return Err(WorktreeError::InvalidIdentity);
        };
        let component = component.to_str().ok_or(WorktreeError::InvalidIdentity)?;
        if !value.is_empty() {
            value.push('/');
        }
        value.push_str(component);
    }
    RelativePath::parse(value).map_err(|_| WorktreeError::InvalidIdentity)
}

fn path_entry_exists(path: &Path) -> Result<bool, WorktreeError> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(WorktreeError::Io(error)),
    }
}

fn complete_stdout(result: &CommandResult) -> Result<Vec<u8>, WorktreeError> {
    if result.stdout.truncated || !result.stdout.complete || result.truncated {
        return Err(WorktreeError::OutputInvalid);
    }
    let mut bytes = result.stdout.head.clone();
    bytes.extend_from_slice(&result.stdout.tail);
    Ok(bytes)
}

fn parse_commit_output(output: &[u8]) -> Result<String, WorktreeError> {
    let value = parse_one_line(output)?;
    if matches!(value.len(), 40 | 64)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        Ok(value)
    } else {
        Err(WorktreeError::OutputInvalid)
    }
}

fn parse_one_line(output: &[u8]) -> Result<String, WorktreeError> {
    if output.contains(&0) {
        return Err(WorktreeError::OutputInvalid);
    }
    let output = std::str::from_utf8(output).map_err(|_| WorktreeError::OutputInvalid)?;
    let line = output
        .strip_suffix("\r\n")
        .or_else(|| output.strip_suffix('\n'))
        .unwrap_or(output);
    if line.is_empty()
        || line.contains(['\r', '\n'])
        || line.trim_matches(|character: char| character.is_ascii_whitespace()) != line
    {
        return Err(WorktreeError::OutputInvalid);
    }
    Ok(line.to_owned())
}

fn command_succeeded(result: &CommandResult) -> bool {
    result.exit_code == Some(0) && result.signal.is_none() && !result.cancelled && !result.timed_out
}

fn require_success(result: &CommandResult) -> Result<(), WorktreeError> {
    if result.cancelled {
        Err(WorktreeError::Cancelled)
    } else if result.timed_out {
        Err(WorktreeError::TimedOut)
    } else if command_succeeded(result) {
        Ok(())
    } else {
        Err(WorktreeError::GitCommandFailed)
    }
}

fn command_result_error(result: &CommandResult) -> WorktreeError {
    if result.cancelled {
        WorktreeError::Cancelled
    } else if result.timed_out {
        WorktreeError::TimedOut
    } else {
        WorktreeError::GitCommandFailed
    }
}

fn is_safe_identity_component(value: &str) -> bool {
    (1..=128).contains(&value.len())
        && value
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
}

fn platform_environment(temporary_directory: &Path) -> Result<PlatformEnvironment, WorktreeError> {
    #[cfg(windows)]
    let system_root = std::env::var_os("SYSTEMROOT")
        .or_else(|| std::env::var_os("WINDIR"))
        .map(PathBuf::from);
    #[cfg(unix)]
    let system_root = None;

    PlatformEnvironment::try_new(temporary_directory.to_owned(), system_root)
        .map_err(|_| WorktreeError::InvalidEnvironment)
}

fn worktree_environment(platform: &PlatformEnvironment) -> ChildEnvironment {
    let mut entries = ChildEnvironment::for_git(platform).entries().clone();
    #[cfg(windows)]
    let null_device = OsString::from("NUL");
    #[cfg(unix)]
    let null_device = OsString::from("/dev/null");
    entries.insert(OsString::from("GIT_CONFIG_GLOBAL"), null_device.clone());
    entries.insert(OsString::from("GIT_CONFIG_SYSTEM"), null_device);
    ChildEnvironment::from_entries(entries)
}

fn map_common_git_identity_error(error: DirectoryIdentityError) -> WorktreeError {
    match error {
        DirectoryIdentityError::Unavailable => WorktreeError::CommonGitIdentityUnavailable,
        DirectoryIdentityError::Mismatch => WorktreeError::CommonGitIdentityMismatch,
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WorktreeError {
    #[error("worktree identity is invalid")]
    InvalidIdentity,
    #[error("worktree limits must be non-zero")]
    InvalidLimits,
    #[error("persisted worktree reservation does not match this provisioner")]
    InvalidReservation,
    #[error("the authenticated common Git directory identity is unavailable")]
    CommonGitIdentityUnavailable,
    #[error("the authenticated common Git directory identity changed")]
    CommonGitIdentityMismatch,
    #[error("the registered repository is invalid or is not a primary worktree")]
    InvalidRepository,
    #[error("the registered Cargo workspace is outside the repository")]
    CargoWorkspaceOutsideRepository,
    #[error("the worktree destination already exists")]
    DestinationConflict,
    #[error("the application-owned worktree parent path is unsafe")]
    ArtifactPathInvalid,
    #[error("the attempt branch already exists")]
    BranchConflict,
    #[error("the registered repository has no committed HEAD")]
    UnbornHead,
    #[error("repository configuration can change or execute checkout behavior")]
    UnsafeGitConfiguration,
    #[error("the Git child-process environment is invalid")]
    InvalidEnvironment,
    #[error("worktree provisioning was cancelled")]
    Cancelled,
    #[error("worktree provisioning timed out")]
    TimedOut,
    #[error("a supervised Git worktree command failed")]
    GitCommandFailed,
    #[error("Git returned incomplete or malformed control-plane output")]
    OutputInvalid,
    #[error("linked-worktree metadata could not be authenticated from the common Git directory")]
    LinkedMetadataInvalid,
    #[error("the created branch or worktree did not match its reservation")]
    PostconditionFailed,
    #[error("the reserved worktree content changed before it became ready")]
    WorktreeContentChanged,
    #[error("Git left an exact but incomplete artifact for this attempt")]
    PartialCreation,
    #[error("Git left an artifact whose identity cannot be proven")]
    InconsistentArtifact,
    #[error("the mapped Cargo workspace is absent from the committed worktree")]
    NestedWorkspaceMissing,
    #[error("the worktree command was rejected by typed command policy")]
    CommandPolicy(#[source] CommandPolicyError),
    #[error("the supervised Git process failed")]
    Process(#[source] ProcessError),
    #[error("trusted Cargo workspace validation failed")]
    Cargo(#[source] crate::CargoToolError),
    #[error("a worktree filesystem operation failed")]
    Io(#[source] io::Error),
}

impl WorktreeError {
    pub fn process_cleanup_is_unproven(&self) -> bool {
        match self {
            Self::Process(error) => error.process_cleanup_is_unproven(),
            Self::Cargo(error) => error.process_cleanup_is_unproven(),
            _ => false,
        }
    }

    /// Identifies positive evidence that the repository/worktree control
    /// identity changed outside this operation. Ordinary partial creation and
    /// content/tool failures still become durable `inconsistent`, but do not
    /// permanently poison every alias.
    pub fn requires_repository_poison(&self) -> bool {
        match self {
            Self::CommonGitIdentityMismatch
            | Self::DestinationConflict
            | Self::BranchConflict
            | Self::LinkedMetadataInvalid
            | Self::PostconditionFailed
            | Self::InconsistentArtifact => true,
            Self::CommandPolicy(
                CommandPolicyError::IdentityChanged | CommandPolicyError::InvalidGitBinding,
            ) => true,
            Self::Cargo(error) => matches!(
                error,
                crate::CargoToolError::WorkspaceRootMismatch
                    | crate::CargoToolError::MetadataPathOutsideWorkspace
                    | crate::CargoToolError::CommandPolicy(
                        CommandPolicyError::IdentityChanged | CommandPolicyError::InvalidGitBinding
                    )
            ),
            _ => false,
        }
    }

    /// Classifies a failed read-only artifact observation without inventing
    /// positive evidence. Callers must retain a reservation for `Unavailable`;
    /// only `Inconsistent` proves that the observed artifact cannot be trusted.
    pub fn artifact_state_after_failed_observation(&self) -> WorktreeArtifactState {
        if self.is_positive_artifact_evidence() {
            WorktreeArtifactState::Inconsistent
        } else {
            WorktreeArtifactState::Unavailable
        }
    }

    fn is_positive_artifact_evidence(&self) -> bool {
        match self {
            Self::InvalidIdentity
            | Self::InvalidReservation
            | Self::CommonGitIdentityMismatch
            | Self::InvalidRepository
            | Self::CargoWorkspaceOutsideRepository
            | Self::DestinationConflict
            | Self::ArtifactPathInvalid
            | Self::BranchConflict
            | Self::LinkedMetadataInvalid
            | Self::PostconditionFailed
            | Self::WorktreeContentChanged
            | Self::PartialCreation
            | Self::InconsistentArtifact
            | Self::NestedWorkspaceMissing => true,
            Self::CommandPolicy(
                CommandPolicyError::IdentityChanged | CommandPolicyError::InvalidGitBinding,
            ) => true,
            Self::Cargo(error) => matches!(
                error,
                crate::CargoToolError::MetadataCommandFailed
                    | crate::CargoToolError::WorkspaceRootMismatch
                    | crate::CargoToolError::MetadataPathOutsideWorkspace
                    | crate::CargoToolError::CatalogTooLarge
                    | crate::CargoToolError::CommandPolicy(
                        CommandPolicyError::IdentityChanged | CommandPolicyError::InvalidGitBinding
                    )
            ),
            _ => false,
        }
    }

    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidIdentity
            | Self::CargoWorkspaceOutsideRepository
            | Self::ArtifactPathInvalid => "WORKTREE_PATH_ESCAPE",
            Self::InvalidLimits | Self::InvalidEnvironment => "WORKTREE_CONFIGURATION_INVALID",
            Self::InvalidReservation
            | Self::OutputInvalid
            | Self::LinkedMetadataInvalid
            | Self::PostconditionFailed
            | Self::WorktreeContentChanged
            | Self::InconsistentArtifact => "WORKTREE_STATE_INCONSISTENT",
            Self::CommonGitIdentityUnavailable => "REPOSITORY_IDENTITY_UNAVAILABLE",
            Self::CommonGitIdentityMismatch => "REPOSITORY_IDENTITY_MISMATCH",
            Self::InvalidRepository => "REPOSITORY_INVALID",
            Self::DestinationConflict
            | Self::BranchConflict
            | Self::GitCommandFailed
            | Self::PartialCreation
            | Self::NestedWorkspaceMissing
            | Self::Io(_) => "WORKTREE_CREATE_FAILED",
            Self::Cargo(error) => {
                if error.process_cleanup_is_unproven() {
                    "PROCESS_TREE_CLEANUP_FAILED"
                } else {
                    "WORKTREE_CREATE_FAILED"
                }
            }
            Self::UnbornHead => "GIT_HEAD_UNBORN",
            Self::UnsafeGitConfiguration => "UNSAFE_GIT_CONFIGURATION",
            Self::Cancelled => "COMMAND_CANCELLED",
            Self::TimedOut => "COMMAND_TIMED_OUT",
            Self::CommandPolicy(error) => error.code(),
            Self::Process(error) => error.code(),
        }
    }
}

fn error_is_not_found(error: &WorktreeError) -> bool {
    match error {
        WorktreeError::Io(error)
        | WorktreeError::CommandPolicy(CommandPolicyError::OpenFailed(error)) => {
            error.kind() == io::ErrorKind::NotFound
        }
        _ => false,
    }
}
