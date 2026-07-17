use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[async_trait::async_trait]
pub trait CommandRunner: Send + Sync + 'static {
    async fn run(
        &self,
        program: &str,
        args: &[OsString],
        current_dir: &Path,
    ) -> io::Result<Vec<u8>>;
}

struct ProcessCommandRunner;

#[async_trait::async_trait]
impl CommandRunner for ProcessCommandRunner {
    async fn run(
        &self,
        program: &str,
        args: &[OsString],
        current_dir: &Path,
    ) -> io::Result<Vec<u8>> {
        let mut command = tokio::process::Command::new(program);
        command
            .args(args)
            .current_dir(current_dir)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let child = {
            let _spawn_guard = coding_agent_runtime::acquire_process_spawn_lock();
            command.spawn()?
        };
        let output = child.wait_with_output().await?;
        if !output.status.success() {
            return Err(io::Error::other("repository command failed"));
        }
        Ok(output.stdout)
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
    runtime_dir: PathBuf,
    runner: Arc<dyn CommandRunner>,
}

impl RepositoryDiscovery {
    pub fn new(runtime_dir: impl Into<PathBuf>) -> Self {
        Self::with_runner(runtime_dir, Arc::new(ProcessCommandRunner))
    }

    pub fn with_runner(runtime_dir: impl Into<PathBuf>, runner: Arc<dyn CommandRunner>) -> Self {
        Self {
            runtime_dir: runtime_dir.into(),
            runner,
        }
    }

    pub async fn discover(
        &self,
        selected: impl AsRef<Path>,
    ) -> Result<DiscoveredRepository, RepositoryDiscoveryError> {
        let selected = validate_selected(selected.as_ref())?;
        let git_stdout = self
            .runner
            .run(
                "git",
                &[
                    OsString::from("-C"),
                    selected.as_os_str().to_owned(),
                    OsString::from("rev-parse"),
                    OsString::from("--show-toplevel"),
                ],
                &self.runtime_dir,
            )
            .await
            .map_err(|_| RepositoryDiscoveryError::CommandFailed)?;
        let git_root = canonical_command_path(&git_stdout)?;
        if selected.strip_prefix(&git_root).is_err() {
            return Err(RepositoryDiscoveryError::CommandFailed);
        }

        let manifest = nearest_manifest(&selected, &git_root)?;
        let cargo_stdout = self
            .runner
            .run(
                "cargo",
                &[
                    OsString::from("locate-project"),
                    OsString::from("--workspace"),
                    OsString::from("--manifest-path"),
                    manifest.as_os_str().to_owned(),
                    OsString::from("--message-format"),
                    OsString::from("plain"),
                ],
                &self.runtime_dir,
            )
            .await
            .map_err(|_| RepositoryDiscoveryError::CommandFailed)?;
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
