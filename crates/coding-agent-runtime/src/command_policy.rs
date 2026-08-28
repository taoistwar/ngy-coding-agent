use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

#[cfg(unix)]
use crate::native_fs::open_child_file;
#[cfg(windows)]
use crate::native_fs::reopen_file_read_lease;
use crate::process_supervisor::ChildEnvironment;
use crate::process_supervisor::ExactChildInput;
pub(crate) use crate::root_capability::DirectoryIdentityMarker;
#[cfg(windows)]
use crate::root_capability::directory_identity_marker;
#[cfg(windows)]
use crate::root_capability::ensure_plain_directory;
use crate::root_capability::{DirectoryIdentityError, RootCapability, ensure_plain_file};

mod delivery_binding;
mod git_delivery;

pub(crate) use delivery_binding::DeliveryGitEmptyConfig;
#[cfg(unix)]
use delivery_binding::{
    DELIVERY_GIT_DIRECTORY_ARGUMENT_INDEX, DELIVERY_GIT_DIRECTORY_SENTINEL,
    DELIVERY_WORK_TREE_ARGUMENT_INDEX, DELIVERY_WORK_TREE_SENTINEL,
};
#[cfg(unix)]
pub(crate) use delivery_binding::{
    DELIVERY_GIT_TEMPORARY_INDEX_SENTINEL, UnixDeliveryDirectoryBinding,
    UnixDeliveryDirectoryBindings, UnixDeliveryDirectoryRole,
};

pub(crate) use git_delivery::{
    DeliveryGitCommitEnvironment, DeliveryGitMutationCommandFactory, DeliveryGitProbeCommands,
    DeliveryGitRepositoryProbeCommands, DeliveryGitSourceMutationBinding,
    DeliveryGitTargetMutationBinding, DeliveryGitTemporaryIndexEnvironment, ProbeGitObjectId,
};

#[derive(Debug, thiserror::Error)]
pub enum CommandPolicyError {
    #[error("pinned command paths must be absolute")]
    RelativePath,
    #[error("the executable path has no final file name")]
    MissingFileName,
    #[error("the pinned path could not be opened safely")]
    OpenFailed(#[source] io::Error),
    #[error("the pinned file is not executable")]
    NotExecutable,
    #[error("the pinned path no longer names the same object")]
    IdentityChanged,
    #[error("command timeout must be greater than zero")]
    InvalidTimeout,
    #[error("Cargo package or test selection is not allowed")]
    InvalidCargoSelection,
    #[error("Cargo parallelism environment overrides are not allowed")]
    InvalidCargoEnvironment,
    #[error("Git metadata and work-tree bindings are invalid")]
    InvalidGitBinding,
    #[error("the Git diff path is not a safe work-tree-relative path")]
    InvalidGitPath,
}

impl CommandPolicyError {
    pub const fn code(&self) -> &'static str {
        "COMMAND_NOT_ALLOWED"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    first: u64,
    second: u64,
    length: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

/// An absolute, no-follow executable whose filesystem identity is retained.
///
/// Construction is public so the application can discover tools at startup;
/// command construction remains crate-private and typed below.
#[derive(Debug)]
pub struct PinnedExecutable {
    path: PathBuf,
    file: File,
    identity: FileIdentity,
    digest: [u8; 32],
}

impl PinnedExecutable {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, CommandPolicyError> {
        let path = path.as_ref();
        let file = open_absolute_plain_file(path)?;
        ensure_executable(path, &file)?;
        #[cfg(windows)]
        let file = reopen_file_read_lease(&file).map_err(CommandPolicyError::OpenFailed)?;
        let identity = file_identity(&file).map_err(CommandPolicyError::OpenFailed)?;
        let digest = file_digest(&file).map_err(CommandPolicyError::OpenFailed)?;
        Ok(Self {
            path: path.to_owned(),
            file,
            identity,
            digest,
        })
    }

    pub(crate) fn revalidate(&self) -> Result<(), CommandPolicyError> {
        let current = Self::open(&self.path)?;
        if current.identity == self.identity && current.digest == self.digest {
            Ok(())
        } else {
            Err(CommandPolicyError::IdentityChanged)
        }
    }

    pub(crate) fn try_clone(&self) -> Result<Self, CommandPolicyError> {
        Ok(Self {
            path: self.path.clone(),
            file: self
                .file
                .try_clone()
                .map_err(CommandPolicyError::OpenFailed)?,
            identity: self.identity,
            digest: self.digest,
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn cloned_file(&self) -> Result<File, CommandPolicyError> {
        self.file
            .try_clone()
            .map_err(CommandPolicyError::OpenFailed)
    }
}

/// An absolute, no-follow directory retained as an open root capability.
#[derive(Debug)]
pub struct ExecutionDirectory {
    path: PathBuf,
    directory: RootCapability,
    spawn_directory: File,
    identity: DirectoryIdentityMarker,
}

impl ExecutionDirectory {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, CommandPolicyError> {
        let path = path.as_ref();
        if !path.is_absolute() {
            return Err(CommandPolicyError::RelativePath);
        }
        let directory = RootCapability::open(path).map_err(CommandPolicyError::OpenFailed)?;
        Self::from_root(path, directory)
    }

    /// Binds an already-authenticated directory descriptor to its expected
    /// absolute namespace path. The path is immediately revalidated, so a
    /// replacement between the caller's handle-bound authentication and this
    /// handoff is rejected instead of becoming a new authority.
    pub fn from_retained_directory(
        path: impl AsRef<Path>,
        directory: File,
    ) -> Result<Self, CommandPolicyError> {
        let path = path.as_ref();
        if !path.is_absolute() {
            return Err(CommandPolicyError::RelativePath);
        }
        let directory = RootCapability::from_authenticated_directory(directory)
            .map_err(CommandPolicyError::OpenFailed)?;
        let execution = Self::from_root(path, directory)?;
        execution.revalidate()?;
        Ok(execution)
    }

    fn from_root(path: &Path, directory: RootCapability) -> Result<Self, CommandPolicyError> {
        let handle = directory
            .try_clone_root()
            .map_err(CommandPolicyError::OpenFailed)?;
        let identity = directory
            .identity_marker()
            .map_err(directory_identity_policy_error)?;
        Ok(Self {
            path: path.to_owned(),
            directory,
            spawn_directory: handle,
            identity,
        })
    }

    pub(crate) fn revalidate(&self) -> Result<(), CommandPolicyError> {
        if self.matches_path(&self.path)? {
            Ok(())
        } else {
            Err(CommandPolicyError::IdentityChanged)
        }
    }

    pub(crate) fn matches_path(&self, path: &Path) -> Result<bool, CommandPolicyError> {
        let current = Self::open(path)?;
        Ok(self.has_same_identity(&current))
    }

    pub(crate) fn has_same_identity(&self, other: &Self) -> bool {
        self.identity == other.identity
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn cloned_directory(&self) -> Result<File, CommandPolicyError> {
        self.spawn_directory
            .try_clone()
            .map_err(CommandPolicyError::OpenFailed)
    }

    pub(crate) fn cloned_root_capability(&self) -> Result<RootCapability, CommandPolicyError> {
        self.directory
            .try_clone_capability()
            .map_err(CommandPolicyError::OpenFailed)
    }

    #[cfg(windows)]
    pub(crate) fn acquire_spawn_path_leases(&self) -> Result<Vec<File>, CommandPolicyError> {
        let leases = windows_directory_path_leases(&self.path)?;
        let final_lease = leases.last().ok_or_else(|| {
            CommandPolicyError::OpenFailed(io::Error::new(
                io::ErrorKind::InvalidInput,
                "execution directory has no leaseable component",
            ))
        })?;
        let leased_identity =
            directory_identity_marker(final_lease).map_err(directory_identity_policy_error)?;
        if leased_identity != self.identity {
            return Err(CommandPolicyError::IdentityChanged);
        }
        Ok(leases)
    }
}

fn directory_identity_policy_error(error: DirectoryIdentityError) -> CommandPolicyError {
    match error {
        DirectoryIdentityError::Unavailable => {
            CommandPolicyError::OpenFailed(io::Error::other(error))
        }
        DirectoryIdentityError::Mismatch => CommandPolicyError::IdentityChanged,
    }
}

#[derive(Debug, Clone)]
pub(crate) struct GitCommandBinding {
    git_directory: Arc<ExecutionDirectory>,
    work_tree: Arc<ExecutionDirectory>,
}

impl GitCommandBinding {
    pub(crate) fn try_new(
        git_directory: Arc<ExecutionDirectory>,
        work_tree: Arc<ExecutionDirectory>,
    ) -> Result<Self, CommandPolicyError> {
        git_directory.revalidate()?;
        work_tree.revalidate()?;
        if git_directory.has_same_identity(&work_tree) {
            return Err(CommandPolicyError::InvalidGitBinding);
        }
        Ok(Self {
            git_directory,
            work_tree,
        })
    }

    fn fixed_arguments(&self) -> Vec<OsString> {
        vec![
            OsString::from("--no-pager"),
            OsString::from("--literal-pathspecs"),
            OsString::from("--no-optional-locks"),
            OsString::from("--no-replace-objects"),
            OsString::from("--no-lazy-fetch"),
            prefixed_path_argument("--git-dir=", self.git_directory.path()),
            prefixed_path_argument("--work-tree=", self.work_tree.path()),
            OsString::from("-c"),
            OsString::from("core.fsmonitor=false"),
            OsString::from("-c"),
            OsString::from("core.untrackedCache=false"),
            OsString::from("-c"),
            OsString::from("submodule.recurse=false"),
            OsString::from("-c"),
            OsString::from("core.sparseCheckout=false"),
            OsString::from("-c"),
            OsString::from("core.sparseCheckoutCone=false"),
            OsString::from("-c"),
            OsString::from("worktree.useRelativePaths=false"),
            OsString::from("-c"),
            OsString::from("gc.auto=0"),
            OsString::from("-c"),
            OsString::from("maintenance.auto=false"),
            OsString::from("-c"),
            OsString::from("core.excludesFile="),
            OsString::from("-c"),
            OsString::from("core.attributesFile="),
            OsString::from("-c"),
            OsString::from(git_hooks_path_configuration()),
            OsString::from("-c"),
            OsString::from("diff.external="),
            OsString::from("-c"),
            OsString::from("diff.renames=false"),
        ]
    }

    fn delivery_fixed_arguments(&self) -> Vec<OsString> {
        #[cfg(unix)]
        {
            let mut arguments = self.fixed_arguments();
            arguments[DELIVERY_GIT_DIRECTORY_ARGUMENT_INDEX] =
                OsString::from(DELIVERY_GIT_DIRECTORY_SENTINEL);
            arguments[DELIVERY_WORK_TREE_ARGUMENT_INDEX] =
                OsString::from(DELIVERY_WORK_TREE_SENTINEL);
            arguments
        }
        #[cfg(windows)]
        {
            self.fixed_arguments()
        }
    }

    pub(crate) fn revalidate(&self) -> Result<(), CommandPolicyError> {
        self.git_directory.revalidate()?;
        self.work_tree.revalidate()
    }

    /// Retained Git-directory authority for one already authenticated binding.
    ///
    /// This is crate-private so delivery can derive a target-checkout context
    /// without reopening a repository path or accepting a new directory.
    pub(crate) const fn git_directory(&self) -> &Arc<ExecutionDirectory> {
        &self.git_directory
    }

    /// Retained worktree authority for one already authenticated binding.
    /// See [`Self::git_directory`]: these are not path constructors.
    pub(crate) const fn work_tree(&self) -> &Arc<ExecutionDirectory> {
        &self.work_tree
    }
}

/// The only command representation accepted by the process supervisor.
///
/// Its fields and constructors are crate-private, and no generic program/argv
/// constructor exists. Model-controlled values can therefore only occupy the
/// individually validated Cargo selector slots.
#[derive(Clone)]
pub(crate) struct ValidatedCommand {
    executable: Arc<PinnedExecutable>,
    working_directory: Arc<ExecutionDirectory>,
    arguments: Vec<OsString>,
    environment: ChildEnvironment,
    dependent_executables: Vec<Arc<PinnedExecutable>>,
    dependent_directories: Vec<Arc<ExecutionDirectory>>,
    delivery_git_empty_config: Option<Arc<DeliveryGitEmptyConfig>>,
    #[cfg(unix)]
    unix_delivery_directory_bindings: Option<UnixDeliveryDirectoryBindings>,
    exact_input: Option<ExactChildInput>,
    timeout: Duration,
    #[cfg(unix)]
    unix_argv0: Option<OsString>,
}

impl fmt::Debug for ValidatedCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ValidatedCommand(<opaque>)")
    }
}

impl ValidatedCommand {
    #[cfg(test)]
    pub(crate) fn for_test(
        executable: PathBuf,
        arguments: Vec<OsString>,
        working_directory: PathBuf,
        environment: ChildEnvironment,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        Self::build(
            Arc::new(PinnedExecutable::open(executable)?),
            Arc::new(ExecutionDirectory::open(working_directory)?),
            arguments,
            environment,
            timeout,
        )
    }

    /// Fixed integration fixture for the exact-child-input substrate.
    ///
    /// The executable, cwd and payload are still capability bound; callers
    /// cannot choose the child argv or turn this into a generic process API.
    pub(crate) fn process_stdin_fixture_for_test(
        executable: Arc<PinnedExecutable>,
        working_directory: Arc<ExecutionDirectory>,
        environment: ChildEnvironment,
        timeout: Duration,
        exact_input: Option<ExactChildInput>,
    ) -> Result<Self, CommandPolicyError> {
        let mut command = Self::build(
            executable,
            working_directory,
            vec![
                OsString::from("--ignored"),
                OsString::from("--exact"),
                OsString::from("process_stdin_fixture_child"),
            ],
            environment,
            timeout,
        )?;
        command.exact_input = exact_input;
        Ok(command)
    }

    pub(crate) fn cargo_metadata(
        executable: Arc<PinnedExecutable>,
        working_directory: Arc<ExecutionDirectory>,
        environment: ChildEnvironment,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        Self::cargo(
            executable,
            working_directory,
            vec![
                OsString::from("metadata"),
                OsString::from("--format-version=1"),
                OsString::from("--no-deps"),
                OsString::from("--offline"),
            ],
            environment,
            timeout,
        )
    }

    /// Cargo metadata used while observing or reopening an existing artifact.
    /// `--frozen` is both offline and locked, so this command cannot repair or
    /// create a missing lockfile while a recovery path is classifying state.
    pub(crate) fn cargo_metadata_read_only(
        executable: Arc<PinnedExecutable>,
        working_directory: Arc<ExecutionDirectory>,
        environment: ChildEnvironment,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        Self::cargo(
            executable,
            working_directory,
            vec![
                OsString::from("metadata"),
                OsString::from("--format-version=1"),
                OsString::from("--no-deps"),
                OsString::from("--frozen"),
            ],
            environment,
            timeout,
        )
    }

    /// The only command admitted during trusted Rust toolchain bootstrap.
    ///
    /// Unix normally launches through a retained executable descriptor, so the
    /// kernel-visible program path is `/proc/self/fd/*` or `/dev/fd/*`. macOS is
    /// the compatibility exception: it consumes the validated pinned path after
    /// revalidation under the global spawn lock while retaining the same open
    /// executable handle through spawn. Fixing argv0 to the Rust compiler role
    /// preserves bootstrap role dispatch without admitting a caller-selected
    /// program identity.
    pub(crate) fn rustc_sysroot(
        executable: Arc<PinnedExecutable>,
        working_directory: Arc<ExecutionDirectory>,
        environment: ChildEnvironment,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        let command = Self::build(
            executable,
            working_directory,
            vec![OsString::from("--print"), OsString::from("sysroot")],
            environment,
            timeout,
        )?;
        #[cfg(unix)]
        let command = {
            let mut command = command;
            command.unix_argv0 = Some(OsString::from("rustc"));
            command
        };
        Ok(command)
    }

    /// The only command admitted while validating the pinned Git bootstrap.
    pub(crate) fn git_version(
        executable: Arc<PinnedExecutable>,
        working_directory: Arc<ExecutionDirectory>,
        environment: ChildEnvironment,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        Self::build(
            executable,
            working_directory,
            vec![OsString::from("--version")],
            environment,
            timeout,
        )
    }

    pub(crate) fn repository_git_root(
        executable: Arc<PinnedExecutable>,
        working_directory: Arc<ExecutionDirectory>,
        environment: ChildEnvironment,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        Self::build(
            executable,
            working_directory,
            vec![
                OsString::from("rev-parse"),
                OsString::from("--show-toplevel"),
            ],
            environment,
            timeout,
        )
    }

    pub(crate) fn repository_cargo_workspace_manifest(
        executable: Arc<PinnedExecutable>,
        working_directory: Arc<ExecutionDirectory>,
        environment: ChildEnvironment,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        Self::cargo(
            executable,
            working_directory,
            vec![
                OsString::from("locate-project"),
                OsString::from("--workspace"),
                OsString::from("--manifest-path"),
                OsString::from("Cargo.toml"),
                OsString::from("--message-format"),
                OsString::from("plain"),
            ],
            environment,
            timeout,
        )
    }

    pub(crate) fn cargo_check(
        executable: Arc<PinnedExecutable>,
        working_directory: Arc<ExecutionDirectory>,
        environment: ChildEnvironment,
        cargo_jobs_per_task: NonZeroU32,
        package: Option<&str>,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        validate_cargo_parallelism_environment(&environment)?;
        let mut arguments = vec![
            OsString::from("check"),
            OsString::from("--offline"),
            OsString::from("--color=never"),
            OsString::from("--message-format=json-render-diagnostics"),
        ];
        append_trusted_cargo_jobs(&mut arguments, cargo_jobs_per_task)?;
        append_package_selection(&mut arguments, package)?;
        Self::cargo(
            executable,
            working_directory,
            arguments,
            environment,
            timeout,
        )
    }

    pub(crate) fn cargo_test(
        executable: Arc<PinnedExecutable>,
        working_directory: Arc<ExecutionDirectory>,
        environment: ChildEnvironment,
        cargo_jobs_per_task: NonZeroU32,
        package: Option<&str>,
        test: Option<&str>,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        validate_cargo_parallelism_environment(&environment)?;
        let mut arguments = vec![
            OsString::from("test"),
            OsString::from("--offline"),
            OsString::from("--color=never"),
            OsString::from("--no-fail-fast"),
            OsString::from("--message-format=json-render-diagnostics"),
        ];
        append_trusted_cargo_jobs(&mut arguments, cargo_jobs_per_task)?;
        if test.is_some() && package.is_none() {
            return Err(CommandPolicyError::InvalidCargoSelection);
        }
        append_package_selection(&mut arguments, package)?;
        if let Some(test) = test {
            if !is_safe_cargo_selector(test) {
                return Err(CommandPolicyError::InvalidCargoSelection);
            }
            arguments.push(OsString::from("--test"));
            arguments.push(OsString::from(test));
        }
        Self::cargo(
            executable,
            working_directory,
            arguments,
            environment,
            timeout,
        )
    }

    pub(crate) fn git_status(
        executable: Arc<PinnedExecutable>,
        binding: &GitCommandBinding,
        environment: ChildEnvironment,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        binding.revalidate()?;
        let mut arguments = binding.fixed_arguments();
        arguments.extend([
            OsString::from("status"),
            OsString::from("--porcelain=v2"),
            OsString::from("--untracked-files=all"),
            OsString::from("--ignore-submodules=all"),
            OsString::from("--no-renames"),
            OsString::from("-z"),
        ]);
        Self::build_git(executable, binding, arguments, environment, timeout)
    }

    pub(crate) fn git_diff(
        executable: Arc<PinnedExecutable>,
        binding: &GitCommandBinding,
        environment: ChildEnvironment,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        binding.revalidate()?;
        let mut arguments = binding.fixed_arguments();
        arguments.extend([
            OsString::from("-c"),
            OsString::from("core.quotePath=true"),
            OsString::from("diff"),
            OsString::from("--no-color"),
            OsString::from("--no-ext-diff"),
            OsString::from("--no-textconv"),
            OsString::from("--no-renames"),
            OsString::from("--ignore-submodules=all"),
            OsString::from("--binary"),
            OsString::from("--full-index"),
            OsString::from("--src-prefix=a/"),
            OsString::from("--dst-prefix=b/"),
            OsString::from("HEAD"),
            OsString::from("--"),
        ]);
        Self::build_git(executable, binding, arguments, environment, timeout)
    }

    /// Counts one status-derived path relative to committed HEAD. The final
    /// path is admitted only after `--` and literal pathspec mode is fixed by
    /// the binding, so it cannot introduce Git options or pathspec magic.
    pub(crate) fn git_diff_numstat_path(
        executable: Arc<PinnedExecutable>,
        binding: &GitCommandBinding,
        environment: ChildEnvironment,
        path: &OsStr,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        binding.revalidate()?;
        validate_git_diff_path(path)?;
        let mut arguments = binding.fixed_arguments();
        arguments.extend([
            OsString::from("diff"),
            OsString::from("--no-color"),
            OsString::from("--no-ext-diff"),
            OsString::from("--no-textconv"),
            OsString::from("--no-renames"),
            OsString::from("--ignore-submodules=all"),
            OsString::from("--numstat"),
            OsString::from("-z"),
            OsString::from("HEAD"),
            OsString::from("--"),
            path.to_os_string(),
        ]);
        Self::build_git(executable, binding, arguments, environment, timeout)
    }

    /// Produces one status-derived patch relative to committed HEAD with every
    /// repository-configured executable diff mechanism disabled.
    pub(crate) fn git_diff_patch_path(
        executable: Arc<PinnedExecutable>,
        binding: &GitCommandBinding,
        environment: ChildEnvironment,
        path: &OsStr,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        binding.revalidate()?;
        validate_git_diff_path(path)?;
        let mut arguments = binding.fixed_arguments();
        arguments.extend([
            OsString::from("-c"),
            OsString::from("core.quotePath=true"),
            OsString::from("diff"),
            OsString::from("--no-color"),
            OsString::from("--no-ext-diff"),
            OsString::from("--no-textconv"),
            OsString::from("--no-renames"),
            OsString::from("--ignore-submodules=all"),
            OsString::from("--binary"),
            OsString::from("--full-index"),
            OsString::from("--src-prefix=a/"),
            OsString::from("--dst-prefix=b/"),
            OsString::from("HEAD"),
            OsString::from("--"),
            path.to_os_string(),
        ]);
        Self::build_git(executable, binding, arguments, environment, timeout)
    }

    /// Enumerates every index entry with its raw path, mode, object identity,
    /// stage and cache-state tag for workspace fingerprinting.
    pub(crate) fn git_fingerprint_tracked_paths(
        executable: Arc<PinnedExecutable>,
        binding: &GitCommandBinding,
        environment: ChildEnvironment,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        binding.revalidate()?;
        let mut arguments = binding.fixed_arguments();
        arguments.extend([
            OsString::from("ls-files"),
            OsString::from("--cached"),
            OsString::from("--stage"),
            OsString::from("-v"),
            OsString::from("-z"),
            OsString::from("--"),
        ]);
        Self::build_git(executable, binding, arguments, environment, timeout)
    }

    /// Enumerates every non-ignored untracked raw path. The binding disables
    /// host-global excludes; repository `.gitignore` and admin excludes remain
    /// authoritative for the deliverable set.
    pub(crate) fn git_fingerprint_untracked_paths(
        executable: Arc<PinnedExecutable>,
        binding: &GitCommandBinding,
        environment: ChildEnvironment,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        binding.revalidate()?;
        let mut arguments = binding.fixed_arguments();
        arguments.extend([
            OsString::from("ls-files"),
            OsString::from("--others"),
            OsString::from("--exclude-standard"),
            OsString::from("-z"),
            OsString::from("--"),
        ]);
        Self::build_git(executable, binding, arguments, environment, timeout)
    }

    /// Resolves the repository's committed HEAD without admitting a caller-
    /// selected revision or path.
    pub(crate) fn git_resolve_head(
        executable: Arc<PinnedExecutable>,
        binding: &GitCommandBinding,
        environment: ChildEnvironment,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        binding.revalidate()?;
        let mut arguments = binding.fixed_arguments();
        arguments.extend([
            OsString::from("rev-parse"),
            OsString::from("--verify"),
            OsString::from("--end-of-options"),
            OsString::from("HEAD^{commit}"),
        ]);
        Self::build_git(executable, binding, arguments, environment, timeout)
    }

    /// Lists names from the common repository's local configuration without
    /// following include/includeIf directives. The caller rejects executable
    /// filters and every include mechanism before checkout.
    pub(crate) fn git_scan_local_configuration(
        executable: Arc<PinnedExecutable>,
        binding: &GitCommandBinding,
        environment: ChildEnvironment,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        binding.revalidate()?;
        let mut arguments = binding.fixed_arguments();
        arguments.extend([
            OsString::from("config"),
            OsString::from("--local"),
            OsString::from("--no-includes"),
            OsString::from("--name-only"),
            OsString::from("--null"),
            OsString::from("--list"),
        ]);
        Self::build_git(executable, binding, arguments, environment, timeout)
    }

    /// Tests the one application-generated branch ref used for an attempt.
    pub(crate) fn git_branch_exists(
        executable: Arc<PinnedExecutable>,
        binding: &GitCommandBinding,
        environment: ChildEnvironment,
        branch_name: &str,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        validate_worktree_branch_name(branch_name)?;
        binding.revalidate()?;
        let mut arguments = binding.fixed_arguments();
        arguments.extend([
            OsString::from("show-ref"),
            OsString::from("--verify"),
            OsString::from("--quiet"),
            OsString::from(format!("refs/heads/{branch_name}")),
        ]);
        Self::build_git(executable, binding, arguments, environment, timeout)
    }

    pub(crate) fn git_resolve_branch(
        executable: Arc<PinnedExecutable>,
        binding: &GitCommandBinding,
        environment: ChildEnvironment,
        branch_name: &str,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        validate_worktree_branch_name(branch_name)?;
        binding.revalidate()?;
        let mut arguments = binding.fixed_arguments();
        arguments.extend([
            OsString::from("rev-parse"),
            OsString::from("--verify"),
            OsString::from("--end-of-options"),
            OsString::from(format!("refs/heads/{branch_name}^{{commit}}")),
        ]);
        Self::build_git(executable, binding, arguments, environment, timeout)
    }

    /// Creates exactly one application-owned branch and linked worktree from
    /// an already resolved commit. All variable fields are independently
    /// constrained and none originate in model output.
    pub(crate) fn git_worktree_add(
        executable: Arc<PinnedExecutable>,
        binding: &GitCommandBinding,
        environment: ChildEnvironment,
        branch_name: &str,
        target: &Path,
        base_commit: &str,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        validate_worktree_branch_name(branch_name)?;
        validate_worktree_target(target)?;
        validate_commit_id(base_commit)?;
        binding.revalidate()?;
        let mut arguments = binding.fixed_arguments();
        arguments.extend([
            OsString::from("worktree"),
            OsString::from("add"),
            OsString::from("--no-checkout"),
            OsString::from("--lock"),
            OsString::from("--reason=codex-reserved"),
            OsString::from("--no-track"),
            OsString::from("--no-guess-remote"),
            OsString::from("-b"),
            OsString::from(branch_name),
            OsString::from("--"),
            child_visible_path(target).into_os_string(),
            OsString::from(base_commit),
        ]);
        Self::build_git(executable, binding, arguments, environment, timeout)
    }

    /// Materializes the exact reserved commit only after the linked git-dir
    /// has been authenticated from common-side administration metadata.
    pub(crate) fn git_worktree_reset(
        executable: Arc<PinnedExecutable>,
        binding: &GitCommandBinding,
        environment: ChildEnvironment,
        base_commit: &str,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        validate_commit_id(base_commit)?;
        binding.revalidate()?;
        let mut arguments = binding.fixed_arguments();
        arguments.extend([
            OsString::from("reset"),
            OsString::from("--hard"),
            OsString::from("--no-recurse-submodules"),
            OsString::from(base_commit),
        ]);
        Self::build_git(executable, binding, arguments, environment, timeout)
    }

    /// Lists common-side worktree registrations for post-create validation.
    pub(crate) fn git_worktree_list(
        executable: Arc<PinnedExecutable>,
        binding: &GitCommandBinding,
        environment: ChildEnvironment,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        binding.revalidate()?;
        let mut arguments = binding.fixed_arguments();
        arguments.extend([
            OsString::from("worktree"),
            OsString::from("list"),
            OsString::from("--porcelain"),
            OsString::from("-z"),
        ]);
        Self::build_git(executable, binding, arguments, environment, timeout)
    }

    /// Reads the linked worktree's symbolic HEAD through a trusted git-dir /
    /// work-tree binding. The hidden root `.git` pointer is never consulted.
    pub(crate) fn git_symbolic_head(
        executable: Arc<PinnedExecutable>,
        binding: &GitCommandBinding,
        environment: ChildEnvironment,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        binding.revalidate()?;
        let mut arguments = binding.fixed_arguments();
        arguments.extend([
            OsString::from("symbolic-ref"),
            OsString::from("--quiet"),
            OsString::from("HEAD"),
        ]);
        Self::build_git(executable, binding, arguments, environment, timeout)
    }

    fn cargo(
        executable: Arc<PinnedExecutable>,
        working_directory: Arc<ExecutionDirectory>,
        arguments: Vec<OsString>,
        environment: ChildEnvironment,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        Self::build(
            executable,
            working_directory,
            arguments,
            environment,
            timeout,
        )
    }

    fn build_git(
        executable: Arc<PinnedExecutable>,
        binding: &GitCommandBinding,
        arguments: Vec<OsString>,
        environment: ChildEnvironment,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        binding.revalidate()?;
        Self::build(
            executable,
            Arc::clone(&binding.work_tree),
            arguments,
            environment,
            timeout,
        )
        .and_then(|command| {
            command.with_dependent_directories(vec![
                Arc::clone(&binding.git_directory),
                Arc::clone(&binding.work_tree),
            ])
        })
    }

    fn build(
        executable: Arc<PinnedExecutable>,
        working_directory: Arc<ExecutionDirectory>,
        arguments: Vec<OsString>,
        environment: ChildEnvironment,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        if timeout.is_zero() {
            return Err(CommandPolicyError::InvalidTimeout);
        }
        executable.revalidate()?;
        working_directory.revalidate()?;
        Ok(Self {
            executable,
            working_directory,
            arguments,
            environment,
            dependent_executables: Vec::new(),
            dependent_directories: Vec::new(),
            delivery_git_empty_config: None,
            #[cfg(unix)]
            unix_delivery_directory_bindings: None,
            exact_input: None,
            timeout,
            #[cfg(unix)]
            unix_argv0: None,
        })
    }

    pub(crate) fn executable(&self) -> &PinnedExecutable {
        &self.executable
    }

    pub(crate) fn working_directory(&self) -> &ExecutionDirectory {
        &self.working_directory
    }

    pub(crate) fn arguments(&self) -> &[OsString] {
        &self.arguments
    }

    pub(crate) fn environment(&self) -> &ChildEnvironment {
        &self.environment
    }

    pub(crate) fn with_dependent_executables(
        mut self,
        executables: Vec<Arc<PinnedExecutable>>,
    ) -> Result<Self, CommandPolicyError> {
        for executable in &executables {
            executable.revalidate()?;
        }
        self.dependent_executables = executables;
        Ok(self)
    }

    pub(crate) fn dependent_executables(&self) -> &[Arc<PinnedExecutable>] {
        &self.dependent_executables
    }

    fn with_dependent_directories(
        mut self,
        directories: Vec<Arc<ExecutionDirectory>>,
    ) -> Result<Self, CommandPolicyError> {
        for directory in &directories {
            directory.revalidate()?;
        }
        self.dependent_directories = directories;
        Ok(self)
    }

    pub(crate) fn dependent_directories(&self) -> &[Arc<ExecutionDirectory>] {
        &self.dependent_directories
    }

    fn with_delivery_git_empty_config(
        mut self,
        config: Arc<DeliveryGitEmptyConfig>,
    ) -> Result<Self, CommandPolicyError> {
        if self.delivery_git_empty_config.is_some() {
            return Err(CommandPolicyError::InvalidGitBinding);
        }
        config.validates_delivery_git_environment(&self.environment)?;
        self.delivery_git_empty_config = Some(config);
        Ok(self)
    }

    pub(crate) fn delivery_git_empty_config(&self) -> Option<&Arc<DeliveryGitEmptyConfig>> {
        self.delivery_git_empty_config.as_ref()
    }

    #[cfg(unix)]
    fn with_delivery_unix_directory_bindings(
        mut self,
        bindings: UnixDeliveryDirectoryBindings,
    ) -> Result<Self, CommandPolicyError> {
        if self.unix_delivery_directory_bindings.is_some() {
            return Err(CommandPolicyError::InvalidGitBinding);
        }
        bindings.validate(&self)?;
        self.unix_delivery_directory_bindings = Some(bindings);
        Ok(self)
    }

    #[cfg(unix)]
    pub(crate) fn unix_delivery_directory_bindings(
        &self,
    ) -> Option<&UnixDeliveryDirectoryBindings> {
        self.unix_delivery_directory_bindings.as_ref()
    }

    pub(crate) fn timeout(&self) -> Duration {
        self.timeout
    }

    pub(crate) fn take_exact_input(&mut self) -> Option<ExactChildInput> {
        self.exact_input.take()
    }

    #[cfg(unix)]
    pub(crate) fn unix_argv0(&self) -> Option<&std::ffi::OsStr> {
        self.unix_argv0.as_deref()
    }
}

#[cfg(windows)]
fn git_hooks_path_configuration() -> &'static str {
    "core.hooksPath=NUL"
}

#[cfg(unix)]
fn git_hooks_path_configuration() -> &'static str {
    "core.hooksPath=/dev/null"
}

pub(crate) fn is_safe_cargo_selector(value: &str) -> bool {
    coding_agent_core::is_valid_cargo_selector(value)
}

fn validate_worktree_branch_name(value: &str) -> Result<(), CommandPolicyError> {
    let Some(suffix) = value.strip_prefix("codex/task-") else {
        return Err(CommandPolicyError::InvalidGitBinding);
    };
    let Some((task_id, attempt)) = suffix.rsplit_once("-attempt-") else {
        return Err(CommandPolicyError::InvalidGitBinding);
    };
    if !is_safe_identity_component(task_id)
        || attempt.is_empty()
        || !attempt.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(CommandPolicyError::InvalidGitBinding);
    }
    Ok(())
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

fn validate_worktree_target(path: &Path) -> Result<(), CommandPolicyError> {
    if !path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::CurDir | std::path::Component::ParentDir
            )
        })
    {
        return Err(CommandPolicyError::InvalidGitBinding);
    }
    Ok(())
}

fn validate_commit_id(value: &str) -> Result<(), CommandPolicyError> {
    if matches!(value.len(), 40 | 64)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        Ok(())
    } else {
        Err(CommandPolicyError::InvalidGitBinding)
    }
}

#[cfg(unix)]
fn validate_git_diff_path(path: &OsStr) -> Result<(), CommandPolicyError> {
    use std::os::unix::ffi::OsStrExt;

    let bytes = path.as_bytes();
    if bytes.is_empty()
        || bytes.contains(&0)
        || bytes.split(|byte| *byte == b'/').any(|component| {
            component.is_empty()
                || component == b"."
                || component == b".."
                || component.eq_ignore_ascii_case(b".git")
        })
    {
        Err(CommandPolicyError::InvalidGitPath)
    } else {
        Ok(())
    }
}

#[cfg(windows)]
fn validate_git_diff_path(path: &OsStr) -> Result<(), CommandPolicyError> {
    if path.is_empty() || crate::RelativePath::try_from_os_path(Path::new(path)).is_err() {
        Err(CommandPolicyError::InvalidGitPath)
    } else {
        Ok(())
    }
}

fn append_package_selection(
    arguments: &mut Vec<OsString>,
    package: Option<&str>,
) -> Result<(), CommandPolicyError> {
    match package {
        Some(package) if is_safe_cargo_selector(package) => {
            arguments.push(OsString::from("--package"));
            arguments.push(OsString::from(package));
            Ok(())
        }
        Some(_) => Err(CommandPolicyError::InvalidCargoSelection),
        None => {
            arguments.push(OsString::from("--workspace"));
            Ok(())
        }
    }
}

fn append_trusted_cargo_jobs(
    arguments: &mut Vec<OsString>,
    cargo_jobs_per_task: NonZeroU32,
) -> Result<(), CommandPolicyError> {
    if arguments.iter().any(|argument| {
        argument.to_str().is_some_and(|argument| {
            argument == "--jobs"
                || argument.starts_with("--jobs=")
                || argument == "-j"
                || (argument.starts_with("-j") && argument.len() > 2)
        })
    }) {
        return Err(CommandPolicyError::InvalidCargoSelection);
    }
    arguments.push(OsString::from(format!(
        "--jobs={}",
        cargo_jobs_per_task.get()
    )));
    Ok(())
}

fn validate_cargo_parallelism_environment(
    environment: &ChildEnvironment,
) -> Result<(), CommandPolicyError> {
    let has_override = environment.entries().keys().any(|key| {
        key.to_str().is_some_and(|key| {
            key.eq_ignore_ascii_case("CARGO_BUILD_JOBS")
                || key.eq_ignore_ascii_case("RUST_TEST_THREADS")
        })
    });
    if has_override {
        return Err(CommandPolicyError::InvalidCargoEnvironment);
    }
    Ok(())
}

fn prefixed_path_argument(prefix: &str, path: &Path) -> OsString {
    let mut argument = OsString::from(prefix);
    let path = child_visible_path(path);
    argument.push(path.as_os_str());
    argument
}

#[cfg(unix)]
pub(crate) fn child_visible_path(path: &Path) -> PathBuf {
    path.to_owned()
}

#[cfg(windows)]
pub(crate) fn child_visible_path(path: &Path) -> PathBuf {
    use std::path::{Component, Prefix};

    let mut components = path.components();
    let Some(Component::Prefix(prefix)) = components.next() else {
        return path.to_owned();
    };
    let Prefix::VerbatimDisk(drive) = prefix.kind() else {
        return path.to_owned();
    };
    if !matches!(components.next(), Some(Component::RootDir)) {
        return path.to_owned();
    }
    let mut visible = PathBuf::from(format!("{}:\\", char::from(drive)));
    for component in components {
        if let Component::Normal(name) = component {
            visible.push(name);
        }
    }
    visible
}

#[cfg(windows)]
fn windows_directory_path_leases(path: &Path) -> Result<Vec<File>, CommandPolicyError> {
    use std::os::windows::fs::OpenOptionsExt;
    use std::path::{Component, Prefix};
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ, FILE_SHARE_WRITE,
    };

    let mut components = path.components();
    let drive = match components.next() {
        Some(Component::Prefix(prefix)) => match prefix.kind() {
            Prefix::Disk(drive) | Prefix::VerbatimDisk(drive) => drive,
            _ => return Err(CommandPolicyError::RelativePath),
        },
        _ => return Err(CommandPolicyError::RelativePath),
    };
    if !matches!(components.next(), Some(Component::RootDir)) {
        return Err(CommandPolicyError::RelativePath);
    }
    let mut options = std::fs::OpenOptions::new();
    options
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT);
    let mut current = PathBuf::from(format!("{}:\\", char::from(drive)));
    let mut leases = Vec::new();
    let root = options
        .open(&current)
        .map_err(CommandPolicyError::OpenFailed)?;
    ensure_plain_directory(&root).map_err(CommandPolicyError::OpenFailed)?;
    leases.push(root);
    for component in components {
        let Component::Normal(name) = component else {
            return Err(CommandPolicyError::RelativePath);
        };
        current.push(name);
        let directory = options
            .open(&current)
            .map_err(CommandPolicyError::OpenFailed)?;
        ensure_plain_directory(&directory).map_err(CommandPolicyError::OpenFailed)?;
        leases.push(directory);
    }
    Ok(leases)
}

#[cfg(unix)]
fn open_absolute_plain_file(path: &Path) -> Result<File, CommandPolicyError> {
    if !path.is_absolute() {
        return Err(CommandPolicyError::RelativePath);
    }
    let file_name = path
        .file_name()
        .ok_or(CommandPolicyError::MissingFileName)?;
    let parent = path.parent().ok_or(CommandPolicyError::MissingFileName)?;
    let root = RootCapability::open(parent).map_err(CommandPolicyError::OpenFailed)?;
    let parent = root
        .try_clone_root()
        .map_err(CommandPolicyError::OpenFailed)?;
    let file = open_child_file(&parent, file_name).map_err(CommandPolicyError::OpenFailed)?;
    ensure_plain_file(&file).map_err(CommandPolicyError::OpenFailed)?;
    Ok(file)
}

#[cfg(windows)]
fn open_absolute_plain_file(path: &Path) -> Result<File, CommandPolicyError> {
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    };

    if !path.is_absolute() {
        return Err(CommandPolicyError::RelativePath);
    }
    if path.file_name().is_none() {
        return Err(CommandPolicyError::MissingFileName);
    }
    let mut options = std::fs::OpenOptions::new();
    options
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    let file = options.open(path).map_err(CommandPolicyError::OpenFailed)?;
    ensure_plain_file(&file).map_err(CommandPolicyError::OpenFailed)?;
    Ok(file)
}

fn file_digest(file: &File) -> io::Result<[u8; 32]> {
    use sha2::{Digest, Sha256};

    let mut reader = file.try_clone()?;
    reader.seek(SeekFrom::Start(0))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().into())
}

#[cfg(unix)]
fn ensure_executable(_: &Path, file: &File) -> Result<(), CommandPolicyError> {
    use std::os::unix::fs::PermissionsExt;

    if file
        .metadata()
        .map_err(CommandPolicyError::OpenFailed)?
        .permissions()
        .mode()
        & 0o111
        == 0
    {
        Err(CommandPolicyError::NotExecutable)
    } else {
        Ok(())
    }
}

#[cfg(windows)]
fn ensure_executable(path: &Path, file: &File) -> Result<(), CommandPolicyError> {
    use std::io::{Read, Seek, SeekFrom};

    let allowed = path
        .extension()
        .and_then(OsStr::to_str)
        .is_some_and(|extension| {
            extension.eq_ignore_ascii_case("exe") || extension.eq_ignore_ascii_case("com")
        });
    if !allowed {
        return Err(CommandPolicyError::NotExecutable);
    }

    // CreateProcess accepts PE images rather than arbitrary files whose names
    // merely end in .exe/.com. Validate the DOS and PE signatures through the
    // already no-follow handle so discovery cannot bless a text-file shim.
    let mut image = file.try_clone().map_err(CommandPolicyError::OpenFailed)?;
    let mut dos_header = [0u8; 64];
    image
        .read_exact(&mut dos_header)
        .map_err(|_| CommandPolicyError::NotExecutable)?;
    if &dos_header[..2] != b"MZ" {
        return Err(CommandPolicyError::NotExecutable);
    }
    let pe_offset = u32::from_le_bytes(dos_header[0x3c..0x40].try_into().unwrap()) as u64;
    if pe_offset < 64
        || pe_offset
            > file
                .metadata()
                .map_err(CommandPolicyError::OpenFailed)?
                .len()
    {
        return Err(CommandPolicyError::NotExecutable);
    }
    image
        .seek(SeekFrom::Start(pe_offset))
        .map_err(CommandPolicyError::OpenFailed)?;
    let mut signature = [0u8; 4];
    image
        .read_exact(&mut signature)
        .map_err(|_| CommandPolicyError::NotExecutable)?;
    if signature != *b"PE\0\0" {
        Err(CommandPolicyError::NotExecutable)
    } else {
        image
            .seek(SeekFrom::Start(0))
            .map_err(CommandPolicyError::OpenFailed)?;
        Ok(())
    }
}

#[cfg(unix)]
fn file_identity(file: &File) -> io::Result<FileIdentity> {
    use std::os::unix::fs::MetadataExt;

    let metadata = file.metadata()?;
    Ok(FileIdentity {
        first: metadata.dev(),
        second: metadata.ino(),
        length: metadata.size(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
        changed_seconds: metadata.ctime(),
        changed_nanoseconds: metadata.ctime_nsec(),
    })
}

#[cfg(windows)]
fn file_identity(file: &File) -> io::Result<FileIdentity> {
    use std::mem::MaybeUninit;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
    };

    let mut information = MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::zeroed();
    if unsafe {
        GetFileInformationByHandle(file.as_raw_handle() as HANDLE, information.as_mut_ptr())
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    let information = unsafe { information.assume_init() };
    Ok(FileIdentity {
        first: u64::from(information.dwVolumeSerialNumber),
        second: (u64::from(information.nFileIndexHigh) << 32)
            | u64::from(information.nFileIndexLow),
        length: (u64::from(information.nFileSizeHigh) << 32) | u64::from(information.nFileSizeLow),
        modified_seconds: i64::from(information.ftLastWriteTime.dwHighDateTime),
        modified_nanoseconds: i64::from(information.ftLastWriteTime.dwLowDateTime),
        changed_seconds: i64::from(information.ftCreationTime.dwHighDateTime),
        changed_nanoseconds: i64::from(information.ftCreationTime.dwLowDateTime),
    })
}

#[cfg(test)]
mod tests;
