use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use coding_agent_domain::Repository;
use coding_agent_runtime::{
    DirectoryIdentityMarker, ExecutionDirectory, RepositoryDiscoveryCommands, RootCapability,
};
use coding_agent_store::Store;
use tokio::time::{Instant, timeout_at};

use crate::repository_control::{
    RepositoryControlCoordinator, RepositoryIdentityResolutionError, RepositoryIdentityResolver,
};
#[cfg(all(test, feature = "test-support"))]
use crate::storage_monitor::StorageRegistrationAckPause;
use crate::{RepositoryCoordinationKey, StorageMonitorHandle, StorageProbeTarget};

pub(crate) const DEFAULT_APPLICATION_WRITE_BUDGET_MILLIS: u64 = 5_000;
pub(crate) const DEFAULT_APPLICATION_WRITE_BUDGET: Duration =
    Duration::from_millis(DEFAULT_APPLICATION_WRITE_BUDGET_MILLIS);

#[derive(Clone)]
pub(crate) struct AuthenticatedRepositoryRuntime {
    common_git_identity: DirectoryIdentityMarker,
    storage_target: StorageProbeTarget,
}

impl AuthenticatedRepositoryRuntime {
    pub(crate) fn new(
        common_git_identity: DirectoryIdentityMarker,
        storage_target: StorageProbeTarget,
    ) -> Self {
        Self {
            common_git_identity,
            storage_target,
        }
    }

    pub(crate) const fn common_git_identity(&self) -> DirectoryIdentityMarker {
        self.common_git_identity
    }

    pub(crate) fn storage_target(&self) -> StorageProbeTarget {
        self.storage_target.clone()
    }
}

#[async_trait::async_trait]
pub(crate) trait RepositoryRuntimeAttachmentRegistry: Send + Sync + 'static {
    async fn attach(
        &self,
        repository: &Repository,
        deadline: Instant,
    ) -> Result<AuthenticatedRepositoryRuntime, RepositoryRuntimeAttachmentError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RepositoryRuntimeAttachmentError {
    IdentityUnavailable,
    StorageUnavailable,
    Unavailable,
    DeadlineExceeded,
    IdentityConflict {
        expected: DirectoryIdentityMarker,
        observed: DirectoryIdentityMarker,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum RepositoryRuntimeRegistrationError {
    #[error("the durable repository identity is unavailable")]
    DurableIdentityUnavailable,
    #[error("the repository runtime attachment is unavailable")]
    AttachmentUnavailable,
    #[error("the repository coordination attachment is unavailable")]
    CoordinationUnavailable,
    #[error("the repository storage attachment is unavailable")]
    StorageUnavailable,
    #[error("the repository runtime registration deadline elapsed")]
    DeadlineExceeded,
}

/// Monotonic bridge from one already durable repository row to every
/// process-local admission capability.
///
/// Each constituent registry is exact and idempotent. Partial progress is
/// intentionally retained so an explicit `Existing` API retry can converge
/// after a failure or a lost apply-before-reply acknowledgement.
#[derive(Clone)]
pub(crate) struct RepositoryRuntimeRegistrar {
    store: Store,
    repository_control: Arc<RepositoryControlCoordinator>,
    attachments: Arc<dyn RepositoryRuntimeAttachmentRegistry>,
    storage_monitor: StorageMonitorHandle,
}

impl RepositoryRuntimeRegistrar {
    pub(crate) fn new(
        store: Store,
        repository_control: Arc<RepositoryControlCoordinator>,
        attachments: Arc<dyn RepositoryRuntimeAttachmentRegistry>,
        storage_monitor: StorageMonitorHandle,
    ) -> Self {
        Self {
            store,
            repository_control,
            attachments,
            storage_monitor,
        }
    }

    pub(crate) async fn attach(
        &self,
        repository: &Repository,
        deadline: Instant,
    ) -> Result<(), RepositoryRuntimeRegistrationError> {
        let lookup = timeout_at(
            deadline,
            self.store.repository_identity_lookup(repository.id),
        )
        .await
        .map_err(|_| RepositoryRuntimeRegistrationError::DeadlineExceeded)?
        .map_err(|_| RepositoryRuntimeRegistrationError::DurableIdentityUnavailable)?
        .ok_or(RepositoryRuntimeRegistrationError::DurableIdentityUnavailable)?;
        if Instant::now() >= deadline {
            return Err(RepositoryRuntimeRegistrationError::DeadlineExceeded);
        }
        let attachment = match timeout_at(deadline, self.attachments.attach(repository, deadline))
            .await
            .map_err(|_| RepositoryRuntimeRegistrationError::DeadlineExceeded)?
        {
            Ok(attachment) => attachment,
            Err(RepositoryRuntimeAttachmentError::IdentityConflict { expected, observed }) => {
                let _ = self
                    .repository_control
                    .register_authenticated_alias(lookup.clone(), expected);
                let _ = self
                    .repository_control
                    .register_authenticated_alias(lookup, observed);
                return Err(RepositoryRuntimeRegistrationError::AttachmentUnavailable);
            }
            Err(RepositoryRuntimeAttachmentError::IdentityUnavailable) => {
                self.repository_control
                    .observe_identity_unavailable(&lookup);
                return Err(RepositoryRuntimeRegistrationError::AttachmentUnavailable);
            }
            Err(
                RepositoryRuntimeAttachmentError::StorageUnavailable
                | RepositoryRuntimeAttachmentError::Unavailable,
            ) => return Err(RepositoryRuntimeRegistrationError::AttachmentUnavailable),
            Err(RepositoryRuntimeAttachmentError::DeadlineExceeded) => {
                return Err(RepositoryRuntimeRegistrationError::DeadlineExceeded);
            }
        };
        if Instant::now() >= deadline {
            return Err(RepositoryRuntimeRegistrationError::DeadlineExceeded);
        }
        let coordination_key = self
            .repository_control
            .register_authenticated_alias(lookup, attachment.common_git_identity)
            .map_err(|_| RepositoryRuntimeRegistrationError::CoordinationUnavailable)?;
        if coordination_key
            != RepositoryCoordinationKey::from_authenticated_marker(attachment.common_git_identity)
        {
            return Err(RepositoryRuntimeRegistrationError::CoordinationUnavailable);
        }
        timeout_at(
            deadline,
            self.storage_monitor.register_repository_scope(
                repository.id,
                coordination_key,
                attachment.storage_target,
            ),
        )
        .await
        .map_err(|_| RepositoryRuntimeRegistrationError::DeadlineExceeded)?
        .map_err(|_| RepositoryRuntimeRegistrationError::StorageUnavailable)
    }

    #[cfg(test)]
    pub(crate) fn repository_control(&self) -> Arc<RepositoryControlCoordinator> {
        Arc::clone(&self.repository_control)
    }

    #[cfg(all(test, feature = "test-support"))]
    pub(crate) fn pause_next_storage_ack_for_test(&self) -> StorageRegistrationAckPause {
        self.storage_monitor
            .pause_next_repository_registration_ack_for_test()
    }
}

#[async_trait::async_trait]
pub trait CommandRunner: Send + Sync + 'static {
    async fn run(
        &self,
        program: &str,
        args: &[OsString],
        current_dir: &Path,
        deadline: Instant,
    ) -> io::Result<Vec<u8>>;
}

struct SupervisedRepositoryCommandRunner {
    commands: RepositoryDiscoveryCommands,
}

#[cfg(any(test, feature = "test-support"))]
struct UnavailableRepositoryCommandRunner;

#[cfg(any(test, feature = "test-support"))]
#[async_trait::async_trait]
impl CommandRunner for UnavailableRepositoryCommandRunner {
    async fn run(
        &self,
        _program: &str,
        _args: &[OsString],
        _current_dir: &Path,
        _deadline: Instant,
    ) -> io::Result<Vec<u8>> {
        Err(io::Error::other(
            "repository discovery commands are unavailable",
        ))
    }
}

#[async_trait::async_trait]
impl CommandRunner for SupervisedRepositoryCommandRunner {
    async fn run(
        &self,
        program: &str,
        args: &[OsString],
        current_dir: &Path,
        deadline: Instant,
    ) -> io::Result<Vec<u8>> {
        let directory = Arc::new(
            ExecutionDirectory::open(current_dir)
                .map_err(|_| io::Error::other("repository directory is unavailable"))?,
        );
        if program == "git"
            && args
                == [
                    OsString::from("rev-parse"),
                    OsString::from("--show-toplevel"),
                ]
        {
            return self
                .commands
                .git_root(directory, deadline)
                .await
                .map_err(|_| io::Error::other("repository command failed"));
        }
        if program == "cargo"
            && args
                == [
                    OsString::from("locate-project"),
                    OsString::from("--workspace"),
                    OsString::from("--manifest-path"),
                    OsString::from("Cargo.toml"),
                    OsString::from("--message-format"),
                    OsString::from("plain"),
                ]
        {
            return self
                .commands
                .cargo_workspace_manifest(directory, deadline)
                .await
                .map_err(|_| io::Error::other("repository command failed"));
        }
        Err(io::Error::other(
            "repository command is outside the supervised facade",
        ))
    }
}

/// Production resolver for the Store's canonical Git-root identity seed.
///
/// It opens the common `.git` directory through the runtime's no-follow root
/// capability and returns only the opaque authenticated object marker.
#[derive(Debug, Clone, Copy, Default)]
pub struct FilesystemRepositoryIdentityResolver;

impl RepositoryIdentityResolver for FilesystemRepositoryIdentityResolver {
    fn resolve(
        &self,
        identity: &coding_agent_store::RepositoryIdentityLookup,
    ) -> Result<DirectoryIdentityMarker, RepositoryIdentityResolutionError> {
        RootCapability::open(identity.git_root.as_path().join(".git"))
            .map_err(|_| RepositoryIdentityResolutionError::Unavailable)?
            .identity_marker()
            .map_err(|_| RepositoryIdentityResolutionError::Unavailable)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredRepository {
    pub selected_path: PathBuf,
    pub display_name: String,
    pub git_root: PathBuf,
    pub cargo_workspace_root: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RepositoryDiscoveryError {
    #[error("the selected repository path was not found")]
    PathNotFound,
    #[error("the selected repository path is not a directory")]
    PathNotDirectory,
    #[error("no Cargo workspace manifest was found inside the Git repository")]
    CargoWorkspaceNotFound,
    #[error("the Cargo workspace is outside the Git repository")]
    CargoWorkspaceOutsideGitRoot,
    #[error("repository discovery command failed")]
    CommandFailed,
}

impl RepositoryDiscoveryError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::PathNotFound => "REPOSITORY_PATH_NOT_FOUND",
            Self::PathNotDirectory => "REPOSITORY_PATH_NOT_DIRECTORY",
            Self::CargoWorkspaceNotFound => "CARGO_WORKSPACE_NOT_FOUND",
            Self::CargoWorkspaceOutsideGitRoot => "CARGO_WORKSPACE_OUTSIDE_GIT_ROOT",
            Self::CommandFailed => "REPOSITORY_COMMAND_FAILED",
        }
    }
}

#[derive(Clone)]
pub struct RepositoryDiscovery {
    runner: Arc<dyn CommandRunner>,
}

impl RepositoryDiscovery {
    pub(crate) fn from_supervised_commands(commands: RepositoryDiscoveryCommands) -> Self {
        Self {
            runner: Arc::new(SupervisedRepositoryCommandRunner { commands }),
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn with_runner(_runtime_dir: impl Into<PathBuf>, runner: Arc<dyn CommandRunner>) -> Self {
        Self { runner }
    }

    /// Explicit fail-closed fixture for API tests that never exercise
    /// successful repository discovery.
    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub fn new_without_commands_for_test(runtime_dir: impl Into<PathBuf>) -> Self {
        Self::with_runner(runtime_dir, Arc::new(UnavailableRepositoryCommandRunner))
    }

    pub async fn discover(
        &self,
        selected: impl AsRef<Path>,
        deadline: Instant,
    ) -> Result<DiscoveredRepository, RepositoryDiscoveryError> {
        ensure_discovery_deadline(deadline)?;
        let selected = validate_selected(selected.as_ref())?;
        ensure_discovery_deadline(deadline)?;
        let git_stdout = self
            .runner
            .run(
                "git",
                &[
                    OsString::from("rev-parse"),
                    OsString::from("--show-toplevel"),
                ],
                &selected,
                deadline,
            )
            .await
            .map_err(|_| RepositoryDiscoveryError::CommandFailed)?;
        ensure_discovery_deadline(deadline)?;
        let git_root = canonical_command_path(&git_stdout)?;
        if selected.strip_prefix(&git_root).is_err() {
            return Err(RepositoryDiscoveryError::CommandFailed);
        }

        let manifest = nearest_manifest(&selected, &git_root)?;
        ensure_discovery_deadline(deadline)?;
        let cargo_stdout = self
            .runner
            .run(
                "cargo",
                &[
                    OsString::from("locate-project"),
                    OsString::from("--workspace"),
                    OsString::from("--manifest-path"),
                    OsString::from("Cargo.toml"),
                    OsString::from("--message-format"),
                    OsString::from("plain"),
                ],
                manifest
                    .parent()
                    .ok_or(RepositoryDiscoveryError::CargoWorkspaceNotFound)?,
                deadline,
            )
            .await
            .map_err(|_| RepositoryDiscoveryError::CommandFailed)?;
        ensure_discovery_deadline(deadline)?;
        let workspace_manifest = canonical_command_path(&cargo_stdout)
            .map_err(|_| RepositoryDiscoveryError::CargoWorkspaceNotFound)?;
        let cargo_workspace_root = workspace_manifest
            .parent()
            .ok_or(RepositoryDiscoveryError::CargoWorkspaceNotFound)?
            .to_path_buf();
        if cargo_workspace_root.strip_prefix(&git_root).is_err() {
            return Err(RepositoryDiscoveryError::CargoWorkspaceOutsideGitRoot);
        }
        let display_name = git_root
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .filter(|name| !name.is_empty())
            .ok_or(RepositoryDiscoveryError::CommandFailed)?;

        Ok(DiscoveredRepository {
            selected_path: selected,
            display_name,
            git_root,
            cargo_workspace_root,
        })
    }
}

fn ensure_discovery_deadline(deadline: Instant) -> Result<(), RepositoryDiscoveryError> {
    if Instant::now() >= deadline {
        Err(RepositoryDiscoveryError::CommandFailed)
    } else {
        Ok(())
    }
}

fn validate_selected(path: &Path) -> Result<PathBuf, RepositoryDiscoveryError> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(RepositoryDiscoveryError::PathNotFound);
        }
        Err(_) => return Err(RepositoryDiscoveryError::CommandFailed),
    }
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(RepositoryDiscoveryError::PathNotFound);
        }
        Err(_) => return Err(RepositoryDiscoveryError::CommandFailed),
    };
    if !metadata.is_dir() {
        return Err(RepositoryDiscoveryError::PathNotDirectory);
    }
    path.canonicalize()
        .map_err(|_| RepositoryDiscoveryError::CommandFailed)
}

fn canonical_command_path(bytes: &[u8]) -> Result<PathBuf, RepositoryDiscoveryError> {
    let value = std::str::from_utf8(bytes)
        .map_err(|_| RepositoryDiscoveryError::CommandFailed)?
        .trim();
    if value.is_empty() {
        return Err(RepositoryDiscoveryError::CommandFailed);
    }
    PathBuf::from(value)
        .canonicalize()
        .map_err(|_| RepositoryDiscoveryError::CommandFailed)
}

fn nearest_manifest(selected: &Path, git_root: &Path) -> Result<PathBuf, RepositoryDiscoveryError> {
    let mut current = Some(selected);
    while let Some(directory) = current {
        let manifest = directory.join("Cargo.toml");
        if std::fs::metadata(&manifest)
            .map(|metadata| metadata.is_file())
            .unwrap_or(false)
        {
            return Ok(manifest);
        }
        if directory == git_root {
            break;
        }
        current = directory.parent();
    }
    Err(RepositoryDiscoveryError::CargoWorkspaceNotFound)
}
