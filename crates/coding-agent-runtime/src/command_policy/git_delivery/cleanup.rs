use super::*;

/// Fixed Task 16 cleanup commands rooted in the authenticated registered
/// primary checkout. The opaque target contributes only its previously
/// authenticated namespace path; it never becomes a working directory,
/// dependent directory, or Unix descriptor binding.
impl ValidatedCommand {
    pub(crate) fn validate_delivery_cleanup_source_branch(
        source_branch: &str,
    ) -> Result<(), CommandPolicyError> {
        DeliveryGitSourceRef::try_new(source_branch).map(|_| ())
    }

    pub(crate) fn validate_delivery_cleanup_authority(
        factory: &DeliveryGitMutationCommandFactory,
        target: &CleanupWorktreeTarget,
        sandbox: &Arc<ExecutionDirectory>,
        config: &Arc<DeliveryGitEmptyConfig>,
        environment: &ChildEnvironment,
        timeout: Duration,
    ) -> Result<(), CommandPolicyError> {
        validate_delivery_cleanup_authority(factory, target, sandbox, config, environment, timeout)
    }

    pub(crate) fn delivery_cleanup_resolve_source_ref(
        factory: &DeliveryGitMutationCommandFactory,
        target: &CleanupWorktreeTarget,
        source_branch: &str,
        sandbox: Arc<ExecutionDirectory>,
        config: Arc<DeliveryGitEmptyConfig>,
        environment: ChildEnvironment,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        let source_ref = DeliveryGitSourceRef::try_new(source_branch)?;
        build_delivery_cleanup_command(
            factory,
            target,
            sandbox,
            config,
            environment,
            timeout,
            DeliveryCleanupCommand::ResolveSourceRef(source_ref.as_str().to_owned()),
        )
    }

    pub(crate) fn delivery_cleanup_source_ref_symbolic(
        factory: &DeliveryGitMutationCommandFactory,
        target: &CleanupWorktreeTarget,
        source_branch: &str,
        sandbox: Arc<ExecutionDirectory>,
        config: Arc<DeliveryGitEmptyConfig>,
        environment: ChildEnvironment,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        let source_ref = DeliveryGitSourceRef::try_new(source_branch)?;
        build_delivery_cleanup_command(
            factory,
            target,
            sandbox,
            config,
            environment,
            timeout,
            DeliveryCleanupCommand::SourceRefSymbolic(source_ref.as_str().to_owned()),
        )
    }

    pub(crate) fn delivery_cleanup_unlock(
        factory: &DeliveryGitMutationCommandFactory,
        target: &CleanupWorktreeTarget,
        sandbox: Arc<ExecutionDirectory>,
        config: Arc<DeliveryGitEmptyConfig>,
        environment: ChildEnvironment,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        build_delivery_cleanup_command(
            factory,
            target,
            sandbox,
            config,
            environment,
            timeout,
            DeliveryCleanupCommand::Unlock,
        )
    }

    pub(crate) fn delivery_cleanup_remove(
        factory: &DeliveryGitMutationCommandFactory,
        target: &CleanupWorktreeTarget,
        sandbox: Arc<ExecutionDirectory>,
        config: Arc<DeliveryGitEmptyConfig>,
        environment: ChildEnvironment,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        build_delivery_cleanup_command(
            factory,
            target,
            sandbox,
            config,
            environment,
            timeout,
            DeliveryCleanupCommand::Remove,
        )
    }
}

pub(super) enum DeliveryCleanupCommand {
    ResolveSourceRef(String),
    SourceRefSymbolic(String),
    Unlock,
    Remove,
}

#[derive(Clone, Copy)]
pub(super) struct DeliveryCleanupTargetAuthority<'authority> {
    pub(super) git: &'authority Arc<PinnedExecutable>,
    pub(super) binding: &'authority GitCommandBinding,
    pub(super) path: &'authority Path,
}

impl<'authority> From<&'authority CleanupWorktreeTarget>
    for DeliveryCleanupTargetAuthority<'authority>
{
    fn from(target: &'authority CleanupWorktreeTarget) -> Self {
        Self {
            git: target.git(),
            binding: target.command_binding(),
            path: target.path(),
        }
    }
}

fn build_delivery_cleanup_command(
    factory: &DeliveryGitMutationCommandFactory,
    target: &CleanupWorktreeTarget,
    sandbox: Arc<ExecutionDirectory>,
    config: Arc<DeliveryGitEmptyConfig>,
    environment: ChildEnvironment,
    timeout: Duration,
    operation: DeliveryCleanupCommand,
) -> Result<ValidatedCommand, CommandPolicyError> {
    target.revalidate()?;
    build_delivery_cleanup_command_with_authority(
        factory,
        DeliveryCleanupTargetAuthority::from(target),
        sandbox,
        config,
        environment,
        timeout,
        operation,
    )
}

pub(super) fn build_delivery_cleanup_command_with_authority(
    factory: &DeliveryGitMutationCommandFactory,
    target: DeliveryCleanupTargetAuthority<'_>,
    sandbox: Arc<ExecutionDirectory>,
    config: Arc<DeliveryGitEmptyConfig>,
    environment: ChildEnvironment,
    timeout: Duration,
    operation: DeliveryCleanupCommand,
) -> Result<ValidatedCommand, CommandPolicyError> {
    validate_delivery_cleanup_target_authority(
        factory,
        target,
        &sandbox,
        &config,
        &environment,
        timeout,
    )?;

    let binding = target.binding;
    let mut arguments = binding.delivery_fixed_arguments();
    append_delivery_read_only_configuration(&mut arguments);
    match operation {
        DeliveryCleanupCommand::ResolveSourceRef(source_commit) => arguments.extend([
            OsString::from("rev-parse"),
            OsString::from("--verify"),
            OsString::from("--quiet"),
            OsString::from("--end-of-options"),
            OsString::from(source_commit),
        ]),
        DeliveryCleanupCommand::SourceRefSymbolic(source_ref) => arguments.extend([
            OsString::from("symbolic-ref"),
            OsString::from("--quiet"),
            OsString::from("--no-recurse"),
            OsString::from("--"),
            OsString::from(source_ref),
        ]),
        DeliveryCleanupCommand::Unlock => arguments.extend([
            OsString::from("worktree"),
            OsString::from("unlock"),
            OsString::from("--"),
            child_visible_path(target.path).into_os_string(),
        ]),
        DeliveryCleanupCommand::Remove => arguments.extend([
            OsString::from("worktree"),
            OsString::from("remove"),
            OsString::from("--"),
            child_visible_path(target.path).into_os_string(),
        ]),
    }

    let command = ValidatedCommand::build_git(
        Arc::clone(target.git),
        binding,
        arguments,
        environment,
        timeout,
    )?
    .with_dependent_directories(vec![
        Arc::clone(binding.git_directory()),
        Arc::clone(binding.work_tree()),
        Arc::clone(&sandbox),
    ])?;
    #[cfg(unix)]
    let command = command.with_delivery_unix_directory_bindings(
        super::super::UnixDeliveryDirectoryBindings::repository(
            Arc::clone(binding.git_directory()),
            Arc::clone(binding.work_tree()),
            Arc::clone(binding.git_directory()),
        ),
    )?;
    command.with_delivery_git_empty_config(config)
}

fn validate_delivery_cleanup_authority(
    factory: &DeliveryGitMutationCommandFactory,
    target: &CleanupWorktreeTarget,
    sandbox: &ExecutionDirectory,
    config: &DeliveryGitEmptyConfig,
    environment: &ChildEnvironment,
    timeout: Duration,
) -> Result<(), CommandPolicyError> {
    target.revalidate()?;
    validate_delivery_cleanup_target_authority(
        factory,
        DeliveryCleanupTargetAuthority::from(target),
        sandbox,
        config,
        environment,
        timeout,
    )
}

fn validate_delivery_cleanup_target_authority(
    factory: &DeliveryGitMutationCommandFactory,
    target: DeliveryCleanupTargetAuthority<'_>,
    sandbox: &ExecutionDirectory,
    config: &DeliveryGitEmptyConfig,
    environment: &ChildEnvironment,
    timeout: Duration,
) -> Result<(), CommandPolicyError> {
    if timeout.is_zero() {
        return Err(CommandPolicyError::InvalidTimeout);
    }
    factory.revalidate_for(target.git)?;
    target.git.revalidate()?;
    target.binding.revalidate()?;
    sandbox.revalidate()?;
    config.validates_delivery_git_environment(environment)?;
    super::super::validate_worktree_target(target.path)?;

    let binding = target.binding;
    binding.revalidate()?;
    let primary_git = binding.git_directory();
    let primary_worktree = binding.work_tree();
    if sandbox.has_same_identity(primary_git)
        || sandbox.has_same_identity(primary_worktree)
        || cleanup_target_aliases_retained_directory(target.path, primary_git)
        || cleanup_target_aliases_retained_directory(target.path, primary_worktree)
        || cleanup_target_aliases_retained_directory(target.path, sandbox)
    {
        return Err(CommandPolicyError::InvalidGitBinding);
    }
    Ok(())
}

fn cleanup_target_aliases_retained_directory(
    target: &Path,
    directory: &ExecutionDirectory,
) -> bool {
    target == directory.path()
}
