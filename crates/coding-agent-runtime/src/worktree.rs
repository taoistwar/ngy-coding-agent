use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::command_policy::{
    CommandPolicyError, DirectoryIdentityMarker, ExecutionDirectory, GitCommandBinding,
    ValidatedCommand, child_visible_path,
};
use crate::native_fs::read_directory_names;
use crate::process_supervisor::{
    ChildEnvironment, CommandResult, PlatformEnvironment, ProcessError, ProcessLimits,
    ProcessSupervisor,
};
use crate::root_capability::DirectoryPathGuard;
use crate::{
    CargoCatalog, CargoToolLimits, CargoTools, GitTools, RelativePath, RootCapability,
    ToolchainPaths,
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

/// Deterministic control-plane values that must be persisted as `reserved`
/// before any Git worktree side effect is allowed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorktreeReservation {
    identity: WorktreeIdentity,
    base_commit: String,
    branch_name: String,
    worktree_path: PathBuf,
    source_common_git_path: PathBuf,
    source_common_git_identity: DirectoryIdentityMarker,
    cargo_workspace_offset: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorktreeObservation {
    Absent,
    BranchOnly,
    AdministrativeCreated,
    CheckoutPartial,
    Ready,
    Inconsistent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorktreeArtifactState {
    Absent,
    Partial,
    Ready,
    Inconsistent,
}

#[derive(Debug, thiserror::Error)]
#[error("worktree provisioning failed after observing {observation:?}: {cause}")]
pub struct WorktreeProvisionError {
    cause: WorktreeError,
    observation: WorktreeObservation,
}

impl WorktreeProvisionError {
    fn absent(cause: WorktreeError) -> Self {
        Self {
            cause,
            observation: WorktreeObservation::Absent,
        }
    }

    pub fn cause(&self) -> &WorktreeError {
        &self.cause
    }

    pub const fn observation(&self) -> WorktreeObservation {
        self.observation
    }

    pub const fn artifact_state(&self) -> WorktreeArtifactState {
        match self.observation {
            WorktreeObservation::Absent => WorktreeArtifactState::Absent,
            WorktreeObservation::BranchOnly
            | WorktreeObservation::AdministrativeCreated
            | WorktreeObservation::CheckoutPartial => WorktreeArtifactState::Partial,
            WorktreeObservation::Ready => WorktreeArtifactState::Ready,
            WorktreeObservation::Inconsistent => WorktreeArtifactState::Inconsistent,
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
        process_limits: ProcessLimits,
        limits: crate::GitToolLimits,
    ) -> Result<GitTools, crate::GitToolError> {
        GitTools::from_trusted_capabilities(
            toolchain,
            self.git_directory(),
            self.work_tree(),
            temporary_directory,
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
    toolchain: ToolchainPaths,
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
}

impl WorktreeProvisioner {
    #[allow(clippy::too_many_arguments)]
    pub fn from_trusted_paths(
        toolchain: &ToolchainPaths,
        registered_git_root: impl AsRef<Path>,
        registered_cargo_workspace: impl AsRef<Path>,
        artifact_root: impl AsRef<Path>,
        temporary_directory: impl AsRef<Path>,
        process_limits: ProcessLimits,
        limits: WorktreeLimits,
    ) -> Result<Self, WorktreeError> {
        let (original_git_root, original_root_directory) =
            validated_directory(registered_git_root.as_ref())?;
        let common_git_path = original_git_root.join(".git");
        let (common_git_directory, common_git_execution_directory) =
            validated_directory(&common_git_path).map_err(|_| WorktreeError::InvalidRepository)?;
        let common_git_capability =
            RootCapability::open(&common_git_directory).map_err(WorktreeError::Io)?;
        let common_git_identity = common_git_execution_directory.identity_marker();

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
            supervisor: ProcessSupervisor::new(process_limits),
            toolchain: toolchain.clone(),
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
        })
    }

    /// Computes and validates the exact artifact identity without creating a
    /// branch, worktree, admin entry, or artifact directory. The application
    /// persists this value as `reserved` before calling `provision_reserved`.
    pub async fn prepare(
        &self,
        identity: WorktreeIdentity,
        cancellation: CancellationToken,
    ) -> Result<WorktreeReservation, WorktreeError> {
        self.artifact_root_directory
            .revalidate()
            .map_err(WorktreeError::CommandPolicy)?;
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
            source_common_git_path: self.common_git_directory.clone(),
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
            source_common_git_path: self.common_git_directory.clone(),
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
        self.validate_reservation(&reservation)
            .map_err(WorktreeProvisionError::absent)?;
        self.artifact_root_directory
            .revalidate()
            .map_err(WorktreeError::CommandPolicy)
            .map_err(WorktreeProvisionError::absent)?;
        self.original_binding
            .revalidate()
            .map_err(WorktreeError::CommandPolicy)
            .map_err(WorktreeProvisionError::absent)?;

        // Configuration and identity are rechecked after persistence so a
        // host-side change cannot silently alter the reserved checkout.
        self.reject_unsafe_local_configuration(cancellation.clone())
            .await
            .map_err(WorktreeProvisionError::absent)?;
        if self
            .branch_exists(reservation.branch_name(), cancellation.clone())
            .await
            .map_err(WorktreeProvisionError::absent)?
        {
            return Err(WorktreeProvisionError::absent(
                WorktreeError::BranchConflict,
            ));
        }

        let identity_path = reservation.identity.relative_path();
        let parent_path = identity_path
            .parent()
            .ok_or(WorktreeError::InvalidIdentity)
            .map_err(WorktreeProvisionError::absent)?;
        let parent_relative =
            relative_path_from_path(parent_path).map_err(WorktreeProvisionError::absent)?;
        let artifact_parent_guard = self
            .artifact_root_capability
            .ensure_directory_path(&parent_relative)
            .map_err(|_| WorktreeError::ArtifactPathInvalid)
            .map_err(WorktreeProvisionError::absent)?;
        let _artifact_parent_handle = artifact_parent_guard
            .try_clone_final()
            .map_err(WorktreeError::Io)
            .map_err(WorktreeProvisionError::absent)?;
        let attempt_name = OsString::from(reservation.identity.attempt().to_string());
        if !artifact_parent_guard
            .child_is_absent(&attempt_name)
            .map_err(WorktreeError::Io)
            .map_err(WorktreeProvisionError::absent)?
        {
            return Err(WorktreeProvisionError::absent(
                WorktreeError::DestinationConflict,
            ));
        }
        self.artifact_root_directory
            .revalidate()
            .map_err(WorktreeError::CommandPolicy)
            .map_err(WorktreeProvisionError::absent)?;

        let before_admin_entries = list_worktree_admin_entries(&self.common_git_capability)
            .map_err(WorktreeProvisionError::absent)?;
        let add = ValidatedCommand::git_worktree_add(
            Arc::clone(&self.git),
            &self.original_binding,
            self.environment.clone(),
            reservation.branch_name(),
            reservation.worktree_path(),
            reservation.base_commit(),
            self.limits.command_timeout,
        )
        .map_err(WorktreeError::CommandPolicy)
        .map_err(WorktreeProvisionError::absent)?;
        let add_result = match self.run(add, cancellation.clone()).await {
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

        let (worktree_path, work_tree) =
            validated_directory(&target).map_err(|_| WorktreeError::PostconditionFailed)?;
        let linked_git_directory =
            self.find_linked_git_directory(before_admin_entries, &worktree_path, &branch_name)?;
        let (_, linked_git_execution_directory) = validated_directory(&linked_git_directory)
            .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
        let linked_binding = GitCommandBinding::try_new(
            Arc::new(linked_git_execution_directory),
            Arc::new(work_tree),
        )
        .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;

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

        let reset = ValidatedCommand::git_worktree_reset(
            Arc::clone(&self.git),
            &linked_binding,
            self.environment.clone(),
            &base_commit,
            self.limits.command_timeout,
        )
        .map_err(WorktreeError::CommandPolicy)?;
        let reset_result = self.run(reset, cancellation.clone()).await?;
        require_success(&reset_result)?;

        // Reset must neither detach/move HEAD nor leave checkout debris.
        if self
            .resolve_bound_head(&linked_binding, cancellation.clone())
            .await?
            != base_commit
            || self
                .resolve_symbolic_head(&linked_binding, cancellation.clone())
                .await?
                != format!("refs/heads/{branch_name}")
        {
            return Err(WorktreeError::PostconditionFailed);
        }
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
            || reservation.worktree_path
                != self
                    .artifact_root
                    .join(reservation.identity.relative_path())
            || reservation.source_common_git_path != self.common_git_directory
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
        match self.observe_inner(reservation, cancellation).await {
            Ok(observation) => observation,
            Err(_) => WorktreeObservation::Inconsistent,
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
    ) -> Result<WorktreeObservation, WorktreeError> {
        self.validate_reservation(reservation)?;
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
            return Ok(WorktreeObservation::Inconsistent);
        }

        let target_relative = relative_path_from_path(&reservation.identity.relative_path())?;
        let target_directory = match self
            .artifact_root_capability
            .open_directory(&target_relative)
        {
            Ok(directory) => Some(directory),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(_) => return Ok(WorktreeObservation::Inconsistent),
        };
        let admin = self.find_reserved_git_directory(reservation)?;

        match (branch.is_some(), admin.as_ref(), target_directory.as_ref()) {
            (false, None, None) => return Ok(WorktreeObservation::Absent),
            (true, None, None) => return Ok(WorktreeObservation::BranchOnly),
            (true, Some(_), None) => return Ok(WorktreeObservation::AdministrativeCreated),
            (true, Some(_), Some(_)) => {}
            _ => return Ok(WorktreeObservation::Inconsistent),
        }
        if target_directory
            .as_ref()
            .is_some_and(directory_contains_only_git_pointer)
        {
            return Ok(WorktreeObservation::AdministrativeCreated);
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
            || self
                .validate_common_worktree_record(
                    reservation.worktree_path(),
                    reservation.base_commit(),
                    reservation.branch_name(),
                    cancellation.clone(),
                )
                .await
                .is_err()
        {
            return Ok(WorktreeObservation::Inconsistent);
        }

        if self
            .validate_clean_worktree(&binding, cancellation.clone())
            .await
            .is_err()
        {
            return Ok(WorktreeObservation::CheckoutPartial);
        }
        if self
            .validate_existing_cargo_workspace(reservation, cancellation.clone())
            .await
            .is_err()
        {
            return Ok(WorktreeObservation::CheckoutPartial);
        }
        if self
            .validate_clean_worktree(&binding, cancellation.clone())
            .await
            .is_err()
            || self
                .validate_common_worktree_record(
                    reservation.worktree_path(),
                    reservation.base_commit(),
                    reservation.branch_name(),
                    cancellation,
                )
                .await
                .is_err()
        {
            return Ok(WorktreeObservation::CheckoutPartial);
        }
        Ok(WorktreeObservation::Ready)
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
        let (_, cargo_directory) = validated_directory(&cargo_path)?;
        let cargo_directory = Arc::new(cargo_directory);
        let cargo_capability = RootCapability::open(&cargo_path).map_err(WorktreeError::Io)?;
        let target_relative = RelativePath::parse("target".to_owned())
            .map_err(|_| WorktreeError::ArtifactPathInvalid)?;
        cargo_capability
            .open_directory(&target_relative)
            .map_err(WorktreeError::Io)?;
        let (_, target_directory) = validated_directory(&cargo_path.join("target"))?;
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
        let (_, cargo_directory) =
            validated_directory(&cargo_path).map_err(|_| WorktreeError::NestedWorkspaceMissing)?;
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
            Err(WorktreeError::PostconditionFailed)
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
        let mut observation = self.observe(reservation, CancellationToken::new()).await;
        if add_reported_success && observation == WorktreeObservation::Absent {
            observation = WorktreeObservation::Inconsistent;
        }
        WorktreeProvisionError { cause, observation }
    }

    fn find_linked_git_directory(
        &self,
        previous: &BTreeSet<OsString>,
        worktree_path: &Path,
        branch_name: &str,
    ) -> Result<PathBuf, WorktreeError> {
        let current = list_worktree_admin_entries(&self.common_git_capability)?;
        let expected_pointer = std::fs::canonicalize(worktree_path.join(".git"))
            .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
        let mut matches = Vec::new();
        for name in current.difference(previous) {
            let Some(name) = name.to_str() else {
                continue;
            };
            if RelativePath::parse(name.to_owned()).is_err() || name.contains('/') {
                continue;
            }
            let pointer = read_admin_gitdir(&self.common_git_capability, name)?;
            let Ok(pointer) = std::fs::canonicalize(pointer) else {
                continue;
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
                Err(_) => continue,
            };
            if !admin_backlink_matches(&pointer, reservation.worktree_path()) {
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

fn admin_backlink_matches(pointer: &Path, worktree_path: &Path) -> bool {
    let expected = child_visible_path(worktree_path).join(".git");
    match (
        std::fs::canonicalize(pointer),
        std::fs::canonicalize(&expected),
    ) {
        (Ok(pointer), Ok(expected)) => pointer == expected,
        _ => pointer == expected,
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
        Err(_) => return Err(WorktreeError::LinkedMetadataInvalid),
    };
    let names = read_directory_names(&mut directory, MAX_ADMIN_ENTRIES)
        .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
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
    let mut file = capability
        .open_file_for_read(&relative)
        .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
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
    let resolved = std::fs::canonicalize(
        common_git_directory
            .join("worktrees")
            .join(name)
            .join(&commondir),
    )
    .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
    Ok(resolved == common_git_directory)
}

fn validate_worktree_list_record(
    output: &[u8],
    expected_path: &Path,
    base_commit: &str,
    branch_name: &str,
) -> Result<(), WorktreeError> {
    let expected_path =
        std::fs::canonicalize(expected_path).map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
    let expected_branch = format!("refs/heads/{branch_name}");
    let mut matches = 0usize;
    let mut path: Option<PathBuf> = None;
    let mut head: Option<&str> = None;
    let mut branch: Option<&str> = None;

    for field in output.split(|byte| *byte == 0) {
        if field.is_empty() {
            if let Some(candidate) = path.take() {
                let canonical = std::fs::canonicalize(candidate).ok();
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

#[derive(Debug, thiserror::Error)]
pub enum WorktreeError {
    #[error("worktree identity is invalid")]
    InvalidIdentity,
    #[error("worktree limits must be non-zero")]
    InvalidLimits,
    #[error("persisted worktree reservation does not match this provisioner")]
    InvalidReservation,
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
            | Self::InconsistentArtifact => "WORKTREE_STATE_INCONSISTENT",
            Self::InvalidRepository => "REPOSITORY_INVALID",
            Self::DestinationConflict
            | Self::BranchConflict
            | Self::GitCommandFailed
            | Self::PartialCreation
            | Self::NestedWorkspaceMissing
            | Self::Cargo(_)
            | Self::Io(_) => "WORKTREE_CREATE_FAILED",
            Self::UnbornHead => "GIT_HEAD_UNBORN",
            Self::UnsafeGitConfiguration => "UNSAFE_GIT_CONFIGURATION",
            Self::Cancelled => "COMMAND_CANCELLED",
            Self::TimedOut => "COMMAND_TIMED_OUT",
            Self::CommandPolicy(error) => error.code(),
            Self::Process(error) => error.code(),
        }
    }
}
