use std::path::Path;
#[cfg(windows)]
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tokio_util::sync::CancellationToken;

use crate::command_policy::{
    CommandPolicyError, ExecutionDirectory, GitCommandBinding, ValidatedCommand,
};
use crate::process_supervisor::{
    ChildEnvironment, CommandResult, PlatformEnvironment, ProcessError, ProcessLimits,
    ProcessSupervisor,
};
use crate::tool_discovery::ToolchainPaths;

/// Fixed deadlines for the two read-only Git operations exposed to the model.
///
/// These limits are selected by trusted application configuration and are not
/// accepted on individual tool calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GitToolLimits {
    status_timeout: Duration,
    diff_timeout: Duration,
}

impl GitToolLimits {
    pub fn try_new(status_timeout: Duration, diff_timeout: Duration) -> Result<Self, GitToolError> {
        if status_timeout.is_zero() || diff_timeout.is_zero() {
            return Err(GitToolError::InvalidLimits);
        }
        Ok(Self {
            status_timeout,
            diff_timeout,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GitRunStatus {
    Cancelled,
    TimedOut,
    Succeeded,
    Failed,
}

/// Exact bounded process output from a typed Git operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitRunResult {
    pub status: GitRunStatus,
    pub command: CommandResult,
}

/// A repository-bound, read-only Git façade.
///
/// Construction is a trusted composition-root operation. Once bound, callers
/// cannot supply a program, argv, cwd, repository path, pathspec, or timeout;
/// the only public operations are the fixed `status` and `diff` commands.
#[derive(Debug)]
pub struct GitTools {
    supervisor: ProcessSupervisor,
    git: Arc<crate::PinnedExecutable>,
    binding: GitCommandBinding,
    environment: ChildEnvironment,
    limits: GitToolLimits,
}

impl GitTools {
    /// Binds the startup-pinned Git executable to retained git-dir/work-tree
    /// capabilities and a minimal, cleared child-process environment.
    pub fn from_trusted_capabilities(
        toolchain: &ToolchainPaths,
        git_directory: Arc<ExecutionDirectory>,
        work_tree: Arc<ExecutionDirectory>,
        temporary_directory: impl AsRef<Path>,
        process_limits: ProcessLimits,
        limits: GitToolLimits,
    ) -> Result<Self, GitToolError> {
        let binding = GitCommandBinding::try_new(git_directory, work_tree)
            .map_err(GitToolError::CommandPolicy)?;
        let platform = platform_environment(temporary_directory.as_ref())?;
        Ok(Self::from_parts(
            ProcessSupervisor::new(process_limits),
            toolchain.git(),
            binding,
            ChildEnvironment::for_git(&platform),
            limits,
        ))
    }

    fn from_parts(
        supervisor: ProcessSupervisor,
        git: Arc<crate::PinnedExecutable>,
        binding: GitCommandBinding,
        environment: ChildEnvironment,
        limits: GitToolLimits,
    ) -> Self {
        Self {
            supervisor,
            git,
            binding,
            environment,
            limits,
        }
    }

    pub async fn status(
        &self,
        cancellation: CancellationToken,
    ) -> Result<GitRunResult, GitToolError> {
        let command = ValidatedCommand::git_status(
            Arc::clone(&self.git),
            &self.binding,
            self.environment.clone(),
            self.limits.status_timeout,
        )
        .map_err(GitToolError::CommandPolicy)?;
        self.run(command, cancellation).await
    }

    pub async fn diff(
        &self,
        cancellation: CancellationToken,
    ) -> Result<GitRunResult, GitToolError> {
        let command = ValidatedCommand::git_diff(
            Arc::clone(&self.git),
            &self.binding,
            self.environment.clone(),
            self.limits.diff_timeout,
        )
        .map_err(GitToolError::CommandPolicy)?;
        self.run(command, cancellation).await
    }

    async fn run(
        &self,
        command: ValidatedCommand,
        cancellation: CancellationToken,
    ) -> Result<GitRunResult, GitToolError> {
        let command = self
            .supervisor
            .run(command, cancellation)
            .await
            .map_err(GitToolError::Process)?;
        Ok(GitRunResult {
            status: classify_run(&command),
            command,
        })
    }
}

fn platform_environment(temporary_directory: &Path) -> Result<PlatformEnvironment, GitToolError> {
    #[cfg(windows)]
    let system_root = std::env::var_os("SYSTEMROOT")
        .or_else(|| std::env::var_os("WINDIR"))
        .map(PathBuf::from);
    #[cfg(unix)]
    let system_root = None;

    PlatformEnvironment::try_new(temporary_directory.to_owned(), system_root)
        .map_err(|_| GitToolError::InvalidEnvironment)
}

fn classify_run(result: &CommandResult) -> GitRunStatus {
    if result.cancelled {
        GitRunStatus::Cancelled
    } else if result.timed_out {
        GitRunStatus::TimedOut
    } else if result.exit_code == Some(0) && result.signal.is_none() {
        GitRunStatus::Succeeded
    } else {
        GitRunStatus::Failed
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GitToolError {
    #[error("Git tool limits must be non-zero")]
    InvalidLimits,
    #[error("the Git child-process environment is invalid")]
    InvalidEnvironment,
    #[error("the Git command was rejected by typed command policy")]
    CommandPolicy(#[source] CommandPolicyError),
    #[error("the supervised Git process failed")]
    Process(#[source] ProcessError),
}

impl GitToolError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidLimits | Self::InvalidEnvironment | Self::CommandPolicy(_) => {
                "COMMAND_NOT_ALLOWED"
            }
            Self::Process(error) => error.code(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command_result(exit_code: Option<i32>) -> CommandResult {
        CommandResult {
            exit_code,
            signal: None,
            timed_out: false,
            cancelled: false,
            stdout: crate::CapturedStream {
                head: Vec::new(),
                tail: Vec::new(),
                observed_bytes: 0,
                omitted_observed_bytes: 0,
                truncated: false,
                complete: true,
            },
            stderr: crate::CapturedStream {
                head: Vec::new(),
                tail: Vec::new(),
                observed_bytes: 0,
                omitted_observed_bytes: 0,
                truncated: false,
                complete: true,
            },
            truncated: false,
            duration_ms: 1,
        }
    }

    #[test]
    fn limits_reject_zero_deadlines() {
        assert!(matches!(
            GitToolLimits::try_new(Duration::ZERO, Duration::from_secs(1)),
            Err(GitToolError::InvalidLimits)
        ));
        assert!(matches!(
            GitToolLimits::try_new(Duration::from_secs(1), Duration::ZERO),
            Err(GitToolError::InvalidLimits)
        ));
    }

    #[test]
    fn status_precedence_is_cancelled_then_timed_out_then_exit() {
        let mut result = command_result(Some(0));
        assert_eq!(classify_run(&result), GitRunStatus::Succeeded);

        result.exit_code = Some(1);
        assert_eq!(classify_run(&result), GitRunStatus::Failed);

        result.timed_out = true;
        assert_eq!(classify_run(&result), GitRunStatus::TimedOut);

        result.cancelled = true;
        assert_eq!(classify_run(&result), GitRunStatus::Cancelled);
    }

    #[test]
    fn child_environment_does_not_forward_host_or_git_credentials() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let environment = ChildEnvironment::for_git(&platform_environment(&root).unwrap());
        let keys = environment
            .entries()
            .keys()
            .map(|key| key.to_string_lossy().into_owned())
            .collect::<Vec<_>>();

        for forbidden in [
            "HOME",
            "PATH",
            "USERPROFILE",
            "GIT_ASKPASS",
            "SSH_AUTH_SOCK",
            "HTTP_PROXY",
            "HTTPS_PROXY",
        ] {
            assert!(!keys.iter().any(|key| key == forbidden));
        }
        assert_eq!(
            environment.entries()[&std::ffi::OsString::from("GIT_CONFIG_NOSYSTEM")],
            "1"
        );
        assert_eq!(
            environment.entries()[&std::ffi::OsString::from("GIT_OPTIONAL_LOCKS")],
            "0"
        );
    }
}
