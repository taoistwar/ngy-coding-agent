use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::command_policy::{ExecutionDirectory, ValidatedCommand};
use crate::process_liveness::ProcessLivenessScope;
use crate::process_supervisor::{
    ChildEnvironment, PlatformEnvironment, ProcessError, ProcessLimits, ProcessSupervisor,
};
use crate::tool_discovery::ToolchainPaths;

/// The two fixed, read-only process queries admitted during repository
/// discovery.
///
/// Executables are pinned at startup, working directories are retained
/// capabilities, argv is fixed by this type, and every process runs through
/// the normal liveness/tree-cleanup supervisor with bounded output.
#[derive(Clone)]
pub struct RepositoryDiscoveryCommands {
    supervisor: ProcessSupervisor,
    git: Arc<crate::PinnedExecutable>,
    cargo: Arc<crate::PinnedExecutable>,
    git_environment: ChildEnvironment,
    cargo_environment: ChildEnvironment,
    max_command_timeout: Duration,
    cleanup_reserve: Duration,
}

impl RepositoryDiscoveryCommands {
    pub fn from_trusted_toolchain(
        toolchain: &ToolchainPaths,
        temporary_directory: impl AsRef<Path>,
        process_liveness_scope: ProcessLivenessScope,
        process_limits: ProcessLimits,
    ) -> Result<Self, RepositoryDiscoveryCommandError> {
        let platform = platform_environment(temporary_directory.as_ref())?;
        Ok(Self {
            supervisor: ProcessSupervisor::new(process_limits, process_liveness_scope),
            git: toolchain.git(),
            cargo: toolchain.cargo(),
            git_environment: ChildEnvironment::for_git(&platform),
            cargo_environment: ChildEnvironment::for_platform(&platform),
            max_command_timeout: process_limits.max_command_timeout(),
            cleanup_reserve: process_limits.cleanup_timeout(),
        })
    }

    pub async fn git_root(
        &self,
        selected_directory: Arc<ExecutionDirectory>,
        deadline: Instant,
    ) -> Result<Vec<u8>, RepositoryDiscoveryCommandError> {
        let timeout = self.remaining_timeout(deadline)?;
        let command = ValidatedCommand::repository_git_root(
            Arc::clone(&self.git),
            selected_directory,
            self.git_environment.clone(),
            timeout,
        )
        .map_err(|_| RepositoryDiscoveryCommandError::InvalidCapability)?;
        self.run(command).await
    }

    pub async fn cargo_workspace_manifest(
        &self,
        manifest_directory: Arc<ExecutionDirectory>,
        deadline: Instant,
    ) -> Result<Vec<u8>, RepositoryDiscoveryCommandError> {
        let timeout = self.remaining_timeout(deadline)?;
        let command = ValidatedCommand::repository_cargo_workspace_manifest(
            Arc::clone(&self.cargo),
            manifest_directory,
            self.cargo_environment.clone(),
            timeout,
        )
        .map_err(|_| RepositoryDiscoveryCommandError::InvalidCapability)?;
        self.run(command).await
    }

    fn remaining_timeout(
        &self,
        deadline: Instant,
    ) -> Result<Duration, RepositoryDiscoveryCommandError> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        bounded_repository_command_timeout(
            remaining,
            self.max_command_timeout,
            self.cleanup_reserve,
        )
    }

    async fn run(
        &self,
        command: ValidatedCommand,
    ) -> Result<Vec<u8>, RepositoryDiscoveryCommandError> {
        let result = self
            .supervisor
            .run(command, CancellationToken::new())
            .await
            .map_err(map_process_error)?;
        if result.timed_out {
            return Err(RepositoryDiscoveryCommandError::DeadlineExceeded);
        }
        if result.cancelled
            || result.exit_code != Some(0)
            || result.signal.is_some()
            || !result.stdout.complete
            || !result.stderr.complete
        {
            return Err(RepositoryDiscoveryCommandError::CommandFailed);
        }
        if result.truncated || result.stdout.truncated || result.stderr.truncated {
            return Err(RepositoryDiscoveryCommandError::OutputTooLarge);
        }
        let mut stdout = result.stdout.head;
        stdout.extend(result.stdout.tail);
        Ok(stdout)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RepositoryDiscoveryCommandError {
    #[error("the repository discovery environment is unavailable")]
    InvalidEnvironment,
    #[error("the repository discovery capability is invalid")]
    InvalidCapability,
    #[error("the repository discovery deadline elapsed")]
    DeadlineExceeded,
    #[error("the repository discovery process failed")]
    CommandFailed,
    #[error("the repository discovery process output exceeded its limit")]
    OutputTooLarge,
}

fn map_process_error(_: ProcessError) -> RepositoryDiscoveryCommandError {
    RepositoryDiscoveryCommandError::CommandFailed
}

fn bounded_repository_command_timeout(
    remaining: Duration,
    max_command_timeout: Duration,
    cleanup_reserve: Duration,
) -> Result<Duration, RepositoryDiscoveryCommandError> {
    remaining
        .checked_sub(cleanup_reserve)
        .map(|budget| budget.min(max_command_timeout))
        .filter(|budget| !budget.is_zero())
        .ok_or(RepositoryDiscoveryCommandError::DeadlineExceeded)
}

fn platform_environment(
    temporary_directory: &Path,
) -> Result<PlatformEnvironment, RepositoryDiscoveryCommandError> {
    #[cfg(windows)]
    let system_root = std::env::var_os("SYSTEMROOT")
        .or_else(|| std::env::var_os("WINDIR"))
        .map(std::path::PathBuf::from);
    #[cfg(unix)]
    let system_root = None;
    PlatformEnvironment::try_new(temporary_directory.to_owned(), system_root)
        .map_err(|_| RepositoryDiscoveryCommandError::InvalidEnvironment)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{RepositoryDiscoveryCommandError, bounded_repository_command_timeout};

    #[test]
    fn five_second_registration_budget_leaves_a_positive_bounded_command_budget() {
        assert_eq!(
            bounded_repository_command_timeout(
                Duration::from_secs(5),
                Duration::from_secs(1),
                Duration::from_millis(500),
            ),
            Ok(Duration::from_secs(1))
        );
        assert_eq!(
            bounded_repository_command_timeout(
                Duration::from_millis(500),
                Duration::from_secs(1),
                Duration::from_millis(500),
            ),
            Err(RepositoryDiscoveryCommandError::DeadlineExceeded)
        );
    }
}
