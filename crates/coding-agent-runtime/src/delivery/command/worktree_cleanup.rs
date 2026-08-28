use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::command_policy::{DeliveryGitEmptyConfig, ValidatedCommand};
use crate::process_supervisor::{ChildEnvironment, PlatformEnvironment};
use crate::worktree::CleanupWorktreeTarget;

use super::super::sandbox::DeliveryCommandSandbox;
use super::super::{DeliverySourceError, ProbedDeliveryGit};
use super::delivery_git_environment;

/// Fixed-shape Task 16 commands bound to one freshly authenticated cleanup
/// target and its registered primary checkout. The source ref is narrowed
/// once during construction; later operations expose no target path, ref,
/// argv, stdin, environment, or force option.
pub(in super::super) struct DeliveryCleanupCommands<'target> {
    factory: crate::command_policy::DeliveryGitMutationCommandFactory,
    target: &'target CleanupWorktreeTarget,
    source_branch: String,
    sandbox: Arc<DeliveryCommandSandbox>,
    config: Arc<DeliveryGitEmptyConfig>,
    environment: ChildEnvironment,
    timeout: Duration,
}

impl<'target> DeliveryCleanupCommands<'target> {
    pub(in super::super) fn try_new(
        probe: &ProbedDeliveryGit,
        target: &'target CleanupWorktreeTarget,
        source_branch: &str,
        sandbox: Arc<DeliveryCommandSandbox>,
        platform: &PlatformEnvironment,
        timeout: Duration,
    ) -> Result<Self, DeliverySourceError> {
        if timeout.is_zero() {
            return Err(DeliverySourceError::InvalidLimits);
        }
        probe
            .verify_current_executable()
            .map_err(|_| DeliverySourceError::AuthenticationChanged)?;
        if !probe.has_repository_object_format_binding() {
            return Err(DeliverySourceError::AuthenticationChanged);
        }
        let factory = probe.mutation_command_factory()?;
        factory.revalidate_for(target.git())?;
        ValidatedCommand::validate_delivery_cleanup_source_branch(source_branch)?;
        sandbox.revalidate()?;
        let sandbox_directory = sandbox.workspace_directory();
        let config = sandbox.empty_config_authority()?;
        let environment = delivery_git_environment(platform, &config, &sandbox_directory)?;
        ValidatedCommand::validate_delivery_cleanup_authority(
            &factory,
            target,
            &sandbox_directory,
            &config,
            &environment,
            timeout,
        )?;
        Ok(Self {
            factory,
            target,
            source_branch: source_branch.to_owned(),
            sandbox,
            config,
            environment,
            timeout,
        })
    }

    /// Resolves only the source branch retained by this cleanup capability.
    /// This remains usable after the removable worktree and its admin entry
    /// are absent because execution is rooted solely in the primary checkout.
    pub(in super::super) fn resolve_source_ref(
        &self,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_cleanup_resolve_source_ref(
            &self.factory,
            self.target,
            &self.source_branch,
            self.sandbox.workspace_directory(),
            Arc::clone(&self.config),
            self.environment.clone(),
            self.timeout,
        )
        .map_err(Into::into)
    }

    pub(in super::super) fn source_ref_symbolic(
        &self,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_cleanup_source_ref_symbolic(
            &self.factory,
            self.target,
            &self.source_branch,
            self.sandbox.workspace_directory(),
            Arc::clone(&self.config),
            self.environment.clone(),
            self.timeout,
        )
        .map_err(Into::into)
    }

    pub(in super::super) fn unlock(&self) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_cleanup_unlock(
            &self.factory,
            self.target,
            self.sandbox.workspace_directory(),
            Arc::clone(&self.config),
            self.environment.clone(),
            self.timeout,
        )
        .map_err(Into::into)
    }

    pub(in super::super) fn remove(&self) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_cleanup_remove(
            &self.factory,
            self.target,
            self.sandbox.workspace_directory(),
            Arc::clone(&self.config),
            self.environment.clone(),
            self.timeout,
        )
        .map_err(Into::into)
    }
}

impl fmt::Debug for DeliveryCleanupCommands<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryCleanupCommands(<opaque>)")
    }
}
