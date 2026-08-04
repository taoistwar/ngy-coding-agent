use std::ffi::{OsStr, OsString};
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
pub(crate) use crate::root_capability::DirectoryIdentityMarker;
#[cfg(windows)]
use crate::root_capability::directory_identity_marker;
#[cfg(windows)]
use crate::root_capability::ensure_plain_directory;
use crate::root_capability::{DirectoryIdentityError, RootCapability, ensure_plain_file};

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

    pub(crate) fn revalidate(&self) -> Result<(), CommandPolicyError> {
        self.git_directory.revalidate()?;
        self.work_tree.revalidate()
    }
}

/// The only command representation accepted by the process supervisor.
///
/// Its fields and constructors are crate-private, and no generic program/argv
/// constructor exists. Model-controlled values can therefore only occupy the
/// individually validated Cargo selector slots.
#[derive(Debug, Clone)]
pub(crate) struct ValidatedCommand {
    executable: Arc<PinnedExecutable>,
    working_directory: Arc<ExecutionDirectory>,
    arguments: Vec<OsString>,
    environment: ChildEnvironment,
    dependent_executables: Vec<Arc<PinnedExecutable>>,
    dependent_directories: Vec<Arc<ExecutionDirectory>>,
    timeout: Duration,
    #[cfg(unix)]
    unix_argv0: Option<OsString>,
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

    pub(crate) fn timeout(&self) -> Duration {
        self.timeout
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
mod tests {
    use std::num::NonZeroU32;

    use super::*;
    use crate::process_supervisor::{ProcessError, ProcessLimits, ProcessSupervisor};
    use tokio_util::sync::CancellationToken;

    fn canonical_test_root(temporary: &tempfile::TempDir) -> PathBuf {
        std::fs::canonicalize(temporary.path()).unwrap()
    }

    fn command_fixture() -> (
        tempfile::TempDir,
        Arc<PinnedExecutable>,
        Arc<ExecutionDirectory>,
    ) {
        let temporary = tempfile::tempdir().unwrap();
        let root = canonical_test_root(&temporary);
        let tool = root.join(if cfg!(windows) { "tool.exe" } else { "tool" });
        std::fs::copy(std::env::current_exe().unwrap(), &tool).unwrap();
        make_executable(&tool);
        let executable = Arc::new(PinnedExecutable::open(&tool).unwrap());
        let directory = Arc::new(ExecutionDirectory::open(root).unwrap());
        (temporary, executable, directory)
    }

    fn cargo_jobs_per_task() -> NonZeroU32 {
        NonZeroU32::new(3).expect("test Cargo jobs are nonzero")
    }

    #[cfg(unix)]
    fn make_executable(path: &Path) {
        use std::os::unix::fs::PermissionsExt;

        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[cfg(windows)]
    fn make_executable(_: &Path) {}

    fn arguments(command: &ValidatedCommand) -> Vec<String> {
        command
            .arguments()
            .iter()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect()
    }

    #[test]
    fn pinned_executable_rejects_relative_directory_and_non_executable_paths() {
        let temporary = tempfile::tempdir().unwrap();
        let root = canonical_test_root(&temporary);
        assert!(matches!(
            PinnedExecutable::open(Path::new("tool")),
            Err(CommandPolicyError::RelativePath)
        ));
        assert!(PinnedExecutable::open(&root).is_err());

        let non_executable = root.join("plain.txt");
        std::fs::write(&non_executable, b"not an executable").unwrap();
        assert!(matches!(
            PinnedExecutable::open(non_executable),
            Err(CommandPolicyError::NotExecutable)
        ));

        #[cfg(windows)]
        {
            let fake_image = root.join("plain.exe");
            std::fs::write(&fake_image, b"not a PE image").unwrap();
            assert!(matches!(
                PinnedExecutable::open(fake_image),
                Err(CommandPolicyError::NotExecutable)
            ));
        }
    }

    #[test]
    fn execution_directory_rejects_relative_and_file_paths() {
        let temporary = tempfile::tempdir().unwrap();
        let root = canonical_test_root(&temporary);
        assert!(matches!(
            ExecutionDirectory::open(Path::new("relative")),
            Err(CommandPolicyError::RelativePath)
        ));
        let file = root.join("file");
        std::fs::write(&file, b"file").unwrap();
        assert!(ExecutionDirectory::open(file).is_err());
    }

    #[test]
    fn executable_and_directory_namespace_replacement_is_detected() {
        let (_temporary, executable, directory) = command_fixture();
        let executable_path = executable.path().to_owned();
        #[cfg(unix)]
        {
            let held_executable = directory.path().join("held");
            std::fs::rename(&executable_path, &held_executable).unwrap();
            std::fs::copy(std::env::current_exe().unwrap(), &executable_path).unwrap();
            make_executable(&executable_path);
            assert!(matches!(
                executable.revalidate(),
                Err(CommandPolicyError::IdentityChanged)
            ));
            assert!(matches!(
                ValidatedCommand::cargo_metadata(
                    Arc::clone(&executable),
                    Arc::clone(&directory),
                    ChildEnvironment::default(),
                    Duration::from_secs(10),
                ),
                Err(CommandPolicyError::IdentityChanged)
            ));
        }
        #[cfg(windows)]
        {
            let held_executable = directory.path().join("held.exe");
            assert!(std::fs::rename(&executable_path, &held_executable).is_err());
            assert!(
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(&executable_path)
                    .is_err()
            );
            assert!(executable.revalidate().is_ok());
        }

        let directory_fixture = tempfile::tempdir().unwrap();
        let original_directory_path = canonical_test_root(&directory_fixture);
        let directory = ExecutionDirectory::open(&original_directory_path).unwrap();
        let replacement = original_directory_path.with_extension("replacement");
        std::fs::create_dir(&replacement).unwrap();
        let held_directory = original_directory_path.with_extension("held");
        std::fs::rename(&original_directory_path, &held_directory).unwrap();
        std::fs::rename(&replacement, &original_directory_path).unwrap();
        assert!(matches!(
            directory.revalidate(),
            Err(CommandPolicyError::IdentityChanged)
        ));

        std::fs::remove_dir(&original_directory_path).unwrap();
        std::fs::rename(&held_directory, &original_directory_path).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn executable_in_place_rewrite_is_detected_by_snapshot_and_digest() {
        let (_temporary, executable, _directory) = command_fixture();
        let mut image = std::fs::read(executable.path()).unwrap();
        let last = image.last_mut().unwrap();
        *last ^= 1;
        std::fs::write(executable.path(), image).unwrap();
        make_executable(executable.path());

        assert!(matches!(
            executable.revalidate(),
            Err(CommandPolicyError::IdentityChanged)
        ));
    }

    #[test]
    fn rustc_sysroot_argv_is_fixed() {
        let (_temporary, executable, directory) = command_fixture();
        let command = ValidatedCommand::rustc_sysroot(
            executable,
            directory,
            ChildEnvironment::default(),
            Duration::from_secs(15),
        )
        .unwrap();

        assert_eq!(arguments(&command), ["--print", "sysroot"]);
        #[cfg(unix)]
        assert_eq!(command.unix_argv0(), Some(std::ffi::OsStr::new("rustc")));
    }

    #[test]
    fn git_version_argv_is_fixed() {
        let (_temporary, executable, directory) = command_fixture();
        let command = ValidatedCommand::git_version(
            executable,
            directory,
            ChildEnvironment::default(),
            Duration::from_secs(15),
        )
        .unwrap();

        assert_eq!(arguments(&command), ["--version"]);
    }

    #[test]
    fn cargo_argv_is_fixed_and_selectors_cannot_inject_options_or_paths() {
        let (_temporary, executable, directory) = command_fixture();
        let metadata = ValidatedCommand::cargo_metadata(
            Arc::clone(&executable),
            Arc::clone(&directory),
            ChildEnvironment::default(),
            Duration::from_secs(10),
        )
        .unwrap();
        assert_eq!(
            arguments(&metadata),
            ["metadata", "--format-version=1", "--no-deps", "--offline"]
        );
        let read_only_metadata = ValidatedCommand::cargo_metadata_read_only(
            Arc::clone(&executable),
            Arc::clone(&directory),
            ChildEnvironment::default(),
            Duration::from_secs(10),
        )
        .unwrap();
        assert_eq!(
            arguments(&read_only_metadata),
            ["metadata", "--format-version=1", "--no-deps", "--frozen"]
        );

        let check = ValidatedCommand::cargo_check(
            Arc::clone(&executable),
            Arc::clone(&directory),
            ChildEnvironment::default(),
            cargo_jobs_per_task(),
            Some("safe-package"),
            Duration::from_secs(10),
        )
        .unwrap();
        assert_eq!(
            arguments(&check),
            [
                "check",
                "--offline",
                "--color=never",
                "--message-format=json-render-diagnostics",
                "--jobs=3",
                "--package",
                "safe-package",
            ]
        );
        let workspace_check = ValidatedCommand::cargo_check(
            Arc::clone(&executable),
            Arc::clone(&directory),
            ChildEnvironment::default(),
            cargo_jobs_per_task(),
            None,
            Duration::from_secs(10),
        )
        .unwrap();
        assert_eq!(
            arguments(&workspace_check),
            [
                "check",
                "--offline",
                "--color=never",
                "--message-format=json-render-diagnostics",
                "--jobs=3",
                "--workspace",
            ]
        );

        let test = ValidatedCommand::cargo_test(
            Arc::clone(&executable),
            Arc::clone(&directory),
            ChildEnvironment::default(),
            cargo_jobs_per_task(),
            Some("safe-package"),
            Some("integration_test"),
            Duration::from_secs(10),
        )
        .unwrap();
        assert_eq!(
            arguments(&test),
            [
                "test",
                "--offline",
                "--color=never",
                "--no-fail-fast",
                "--message-format=json-render-diagnostics",
                "--jobs=3",
                "--package",
                "safe-package",
                "--test",
                "integration_test",
            ]
        );
        let workspace_test = ValidatedCommand::cargo_test(
            Arc::clone(&executable),
            Arc::clone(&directory),
            ChildEnvironment::default(),
            cargo_jobs_per_task(),
            None,
            None,
            Duration::from_secs(10),
        )
        .unwrap();
        assert_eq!(
            arguments(&workspace_test),
            [
                "test",
                "--offline",
                "--color=never",
                "--no-fail-fast",
                "--message-format=json-render-diagnostics",
                "--jobs=3",
                "--workspace",
            ]
        );
        for command in [&check, &workspace_check, &test, &workspace_test] {
            assert_eq!(
                arguments(command)
                    .into_iter()
                    .filter(|argument| argument.starts_with("--jobs="))
                    .collect::<Vec<_>>(),
                ["--jobs=3"]
            );
            assert!(
                !command
                    .environment()
                    .entries()
                    .contains_key(&OsString::from("CARGO_BUILD_JOBS"))
            );
            assert!(
                !command
                    .environment()
                    .entries()
                    .contains_key(&OsString::from("RUST_TEST_THREADS"))
            );
        }
        assert!(matches!(
            ValidatedCommand::cargo_test(
                Arc::clone(&executable),
                Arc::clone(&directory),
                ChildEnvironment::default(),
                cargo_jobs_per_task(),
                None,
                Some("integration_test"),
                Duration::from_secs(10),
            ),
            Err(CommandPolicyError::InvalidCargoSelection)
        ));

        for invalid in [
            "",
            "--config",
            "--jobs=999",
            "../escape",
            "name=value",
            "with space",
            "unicode-工具",
        ] {
            assert!(matches!(
                ValidatedCommand::cargo_check(
                    Arc::clone(&executable),
                    Arc::clone(&directory),
                    ChildEnvironment::default(),
                    cargo_jobs_per_task(),
                    Some(invalid),
                    Duration::from_secs(10),
                ),
                Err(CommandPolicyError::InvalidCargoSelection)
            ));
        }
        assert!(matches!(
            ValidatedCommand::cargo_test(
                Arc::clone(&executable),
                Arc::clone(&directory),
                ChildEnvironment::default(),
                cargo_jobs_per_task(),
                Some("safe-package"),
                Some("--manifest-path"),
                Duration::from_secs(10),
            ),
            Err(CommandPolicyError::InvalidCargoSelection)
        ));

        for key in [
            "CARGO_BUILD_JOBS",
            "cargo_build_jobs",
            "RUST_TEST_THREADS",
            "rust_test_threads",
        ] {
            let environment =
                ChildEnvironment::from_entries([(OsString::from(key), OsString::from("999"))]);
            assert!(matches!(
                ValidatedCommand::cargo_test(
                    Arc::clone(&executable),
                    Arc::clone(&directory),
                    environment,
                    cargo_jobs_per_task(),
                    None,
                    None,
                    Duration::from_secs(10),
                ),
                Err(CommandPolicyError::InvalidCargoEnvironment)
            ));
        }

        for existing in ["--jobs", "--jobs=9", "-j", "-j9", "-j=9"] {
            let mut arguments = vec![OsString::from("test"), OsString::from(existing)];
            assert!(matches!(
                append_trusted_cargo_jobs(&mut arguments, cargo_jobs_per_task()),
                Err(CommandPolicyError::InvalidCargoSelection)
            ));
        }
    }

    #[test]
    fn git_argv_uses_only_prebuilt_bindings_and_fixed_read_only_operations() {
        let (_temporary, executable, work_tree) = command_fixture();
        let git_directory_path = work_tree.path().join("git-metadata");
        std::fs::create_dir(&git_directory_path).unwrap();
        let git_directory = Arc::new(ExecutionDirectory::open(&git_directory_path).unwrap());
        assert!(matches!(
            GitCommandBinding::try_new(Arc::clone(&work_tree), Arc::clone(&work_tree)),
            Err(CommandPolicyError::InvalidGitBinding)
        ));
        let binding = GitCommandBinding::try_new(git_directory, Arc::clone(&work_tree)).unwrap();

        let status = ValidatedCommand::git_status(
            Arc::clone(&executable),
            &binding,
            ChildEnvironment::default(),
            Duration::from_secs(10),
        )
        .unwrap();
        let status_arguments = arguments(&status);
        assert_eq!(
            &status_arguments[..5],
            [
                "--no-pager",
                "--literal-pathspecs",
                "--no-optional-locks",
                "--no-replace-objects",
                "--no-lazy-fetch",
            ]
        );
        assert_eq!(
            status_arguments[5],
            format!(
                "--git-dir={}",
                child_visible_path(&git_directory_path).display()
            )
        );
        assert_eq!(
            status_arguments[6],
            format!(
                "--work-tree={}",
                child_visible_path(work_tree.path()).display()
            )
        );
        assert_eq!(
            &status_arguments[7..],
            [
                "-c",
                "core.fsmonitor=false",
                "-c",
                "core.untrackedCache=false",
                "-c",
                "submodule.recurse=false",
                "-c",
                "core.sparseCheckout=false",
                "-c",
                "core.sparseCheckoutCone=false",
                "-c",
                "worktree.useRelativePaths=false",
                "-c",
                "gc.auto=0",
                "-c",
                "maintenance.auto=false",
                "-c",
                "core.excludesFile=",
                "-c",
                "core.attributesFile=",
                "-c",
                git_hooks_path_configuration(),
                "-c",
                "diff.external=",
                "-c",
                "diff.renames=false",
                "status",
                "--porcelain=v2",
                "--untracked-files=all",
                "--ignore-submodules=all",
                "--no-renames",
                "-z",
            ]
        );
        assert_eq!(status.working_directory().path(), work_tree.path());

        let diff = ValidatedCommand::git_diff(
            executable,
            &binding,
            ChildEnvironment::default(),
            Duration::from_secs(10),
        )
        .unwrap();
        assert_eq!(
            &arguments(&diff)[33..],
            [
                "-c",
                "core.quotePath=true",
                "diff",
                "--no-color",
                "--no-ext-diff",
                "--no-textconv",
                "--no-renames",
                "--ignore-submodules=all",
                "--binary",
                "--full-index",
                "--src-prefix=a/",
                "--dst-prefix=b/",
                "HEAD",
                "--",
            ]
        );
    }

    #[tokio::test]
    async fn git_directory_replacement_after_construction_is_rejected_before_spawn() {
        let (_temporary, executable, work_tree) = command_fixture();
        let git_directory_path = work_tree.path().join("git-metadata-replaced");
        std::fs::create_dir(&git_directory_path).unwrap();
        let git_directory = Arc::new(ExecutionDirectory::open(&git_directory_path).unwrap());
        let binding =
            GitCommandBinding::try_new(Arc::clone(&git_directory), Arc::clone(&work_tree)).unwrap();
        let command = ValidatedCommand::git_status(
            executable,
            &binding,
            ChildEnvironment::default(),
            Duration::from_secs(10),
        )
        .unwrap();
        assert_eq!(command.dependent_directories().len(), 2);

        let held_directory_path = work_tree.path().join("git-metadata-held");
        std::fs::rename(&git_directory_path, &held_directory_path).unwrap();
        std::fs::create_dir(&git_directory_path).unwrap();

        let limits = ProcessLimits::try_new(
            4 * 1024,
            4 * 1024,
            Duration::from_secs(30),
            Duration::from_secs(5),
        )
        .unwrap();
        let error = ProcessSupervisor::new(limits, crate::process_liveness::test_process_scope())
            .run(command, CancellationToken::new())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ProcessError::CommandPolicy(CommandPolicyError::IdentityChanged)
        ));

        std::fs::remove_dir(&git_directory_path).unwrap();
        std::fs::rename(&held_directory_path, &git_directory_path).unwrap();
    }

    #[test]
    fn per_path_git_diff_commands_are_literal_read_only_and_path_validated() {
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .unwrap();
        let test_root = workspace_root.join("target/command-policy-diff-tests");
        std::fs::create_dir_all(&test_root).unwrap();
        let temporary = tempfile::Builder::new()
            .prefix("literal-path-")
            .tempdir_in(test_root)
            .unwrap();
        let root = canonical_test_root(&temporary);
        let tool = root.join(if cfg!(windows) { "tool.exe" } else { "tool" });
        std::fs::copy(std::env::current_exe().unwrap(), &tool).unwrap();
        make_executable(&tool);
        let executable = Arc::new(PinnedExecutable::open(&tool).unwrap());
        let work_tree = Arc::new(ExecutionDirectory::open(root).unwrap());
        let git_directory_path = work_tree.path().join("git-metadata-path-diff");
        std::fs::create_dir(&git_directory_path).unwrap();
        let git_directory = Arc::new(ExecutionDirectory::open(&git_directory_path).unwrap());
        let binding = GitCommandBinding::try_new(git_directory, work_tree).unwrap();
        let path = OsStr::new("sub dir/-option.txt");

        let numstat = ValidatedCommand::git_diff_numstat_path(
            Arc::clone(&executable),
            &binding,
            ChildEnvironment::default(),
            path,
            Duration::from_secs(10),
        )
        .unwrap();
        let numstat_arguments = arguments(&numstat);
        assert_eq!(
            &numstat_arguments[numstat_arguments.len() - 11..],
            [
                "diff",
                "--no-color",
                "--no-ext-diff",
                "--no-textconv",
                "--no-renames",
                "--ignore-submodules=all",
                "--numstat",
                "-z",
                "HEAD",
                "--",
                "sub dir/-option.txt",
            ]
        );

        let patch = ValidatedCommand::git_diff_patch_path(
            Arc::clone(&executable),
            &binding,
            ChildEnvironment::default(),
            path,
            Duration::from_secs(10),
        )
        .unwrap();
        let patch_arguments = arguments(&patch);
        assert_eq!(
            &patch_arguments[patch_arguments.len() - 15..],
            [
                "-c",
                "core.quotePath=true",
                "diff",
                "--no-color",
                "--no-ext-diff",
                "--no-textconv",
                "--no-renames",
                "--ignore-submodules=all",
                "--binary",
                "--full-index",
                "--src-prefix=a/",
                "--dst-prefix=b/",
                "HEAD",
                "--",
                "sub dir/-option.txt",
            ]
        );

        let mut invalid_paths = vec!["", "../escape", ".GIT/config", "dir//file"];
        if cfg!(windows) {
            invalid_paths.extend(["C:/escape", "file:stream"]);
        } else {
            invalid_paths.push("/absolute");
        }
        for invalid in invalid_paths {
            assert!(matches!(
                ValidatedCommand::git_diff_numstat_path(
                    Arc::clone(&executable),
                    &binding,
                    ChildEnvironment::default(),
                    OsStr::new(invalid),
                    Duration::from_secs(10),
                ),
                Err(CommandPolicyError::InvalidGitPath)
            ));
        }

        #[cfg(unix)]
        {
            use std::os::unix::ffi::{OsStrExt, OsStringExt};

            let non_utf8 = OsString::from_vec(b"nonutf8-\xff.txt".to_vec());
            let command = ValidatedCommand::git_diff_numstat_path(
                executable,
                &binding,
                ChildEnvironment::default(),
                &non_utf8,
                Duration::from_secs(10),
            )
            .unwrap();
            assert_eq!(
                command.arguments().last().unwrap().as_bytes(),
                non_utf8.as_bytes()
            );
        }
    }

    #[test]
    fn zero_timeout_is_never_a_validated_command() {
        let (_temporary, executable, directory) = command_fixture();
        assert!(matches!(
            ValidatedCommand::cargo_metadata(
                executable,
                directory,
                ChildEnvironment::default(),
                Duration::ZERO,
            ),
            Err(CommandPolicyError::InvalidTimeout)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_executable_and_directory_are_rejected() {
        let temporary = tempfile::tempdir().unwrap();
        let root = canonical_test_root(&temporary);
        let executable_target = root.join("target");
        std::fs::copy(std::env::current_exe().unwrap(), &executable_target).unwrap();
        make_executable(&executable_target);
        let executable_link = root.join("link");
        std::os::unix::fs::symlink(&executable_target, &executable_link).unwrap();
        assert!(PinnedExecutable::open(executable_link).is_err());

        let directory_target = root.join("directory-target");
        std::fs::create_dir(&directory_target).unwrap();
        let directory_link = root.join("directory-link");
        std::os::unix::fs::symlink(&directory_target, &directory_link).unwrap();
        assert!(ExecutionDirectory::open(directory_link).is_err());
    }

    #[cfg(windows)]
    #[test]
    fn reparse_point_executable_and_directory_are_rejected_when_links_are_available() {
        let temporary = tempfile::tempdir().unwrap();
        let root = canonical_test_root(&temporary);
        let executable_target = root.join("target.exe");
        std::fs::copy(std::env::current_exe().unwrap(), &executable_target).unwrap();
        let executable_link = root.join("link.exe");
        if std::os::windows::fs::symlink_file(&executable_target, &executable_link).is_ok() {
            assert!(PinnedExecutable::open(executable_link).is_err());
        }

        let directory_target = root.join("directory-target");
        std::fs::create_dir(&directory_target).unwrap();
        let directory_link = root.join("directory-link");
        if std::os::windows::fs::symlink_dir(&directory_target, &directory_link).is_ok() {
            assert!(ExecutionDirectory::open(directory_link).is_err());
        }
    }

    #[cfg(windows)]
    #[test]
    fn spawn_directory_lease_blocks_ancestor_rebinding_only_while_held() {
        let temporary = tempfile::tempdir().unwrap();
        let ancestor_path = canonical_test_root(&temporary).join("ancestor");
        let directory_path = ancestor_path.join("working");
        std::fs::create_dir_all(&directory_path).unwrap();
        let directory = ExecutionDirectory::open(&directory_path).unwrap();
        let held = ancestor_path.with_extension("held");

        let leases = directory.acquire_spawn_path_leases().unwrap();
        drop(directory);
        assert!(std::fs::rename(&ancestor_path, &held).is_err());
        drop(leases);
        std::fs::rename(&ancestor_path, &held).unwrap();
        std::fs::rename(&held, &ancestor_path).unwrap();
    }
}
