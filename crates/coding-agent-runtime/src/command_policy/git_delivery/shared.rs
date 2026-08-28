use super::*;

pub(super) fn clone_delivery_read_only_binding(
    binding: &DeliveryGitReadOnlyBinding,
) -> DeliveryGitReadOnlyBinding {
    DeliveryGitReadOnlyBinding {
        git: Arc::clone(&binding.git),
        repository: binding.repository.clone(),
        common_git: Arc::clone(&binding.common_git),
        sandbox: Arc::clone(&binding.sandbox),
        config: Arc::clone(&binding.config),
        environment: binding.environment.clone(),
        timeout: binding.timeout,
    }
}

pub(super) fn revalidate_delivery_read_only_binding(
    binding: &DeliveryGitReadOnlyBinding,
) -> Result<(), CommandPolicyError> {
    if binding.timeout.is_zero() {
        return Err(CommandPolicyError::InvalidTimeout);
    }
    binding.git.revalidate()?;
    binding.repository.revalidate()?;
    binding.common_git.revalidate()?;
    binding.sandbox.revalidate()?;
    binding.config.revalidate()?;
    binding
        .config
        .validates_delivery_git_environment(&binding.environment)
}

pub(super) fn cat_file_batch_input(object: &str) -> Vec<u8> {
    let mut input = Vec::with_capacity(object.len() + 1);
    input.extend_from_slice(object.as_bytes());
    input.push(b'\n');
    input
}

pub(super) fn require_delivery_object_id(
    object: &str,
    object_id_length: usize,
) -> Result<(), CommandPolicyError> {
    if object.len() == object_id_length
        && object
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        && object.as_bytes().iter().any(|byte| *byte != b'0')
    {
        Ok(())
    } else {
        Err(CommandPolicyError::InvalidGitBinding)
    }
}

pub(super) fn is_canonical_delivery_merge_message(message: &str) -> bool {
    const PREFIX: &str = "coding-agent: merge task ";
    const ATTEMPT_SEPARATOR: &str = " attempt ";

    let Some(message) = message.strip_suffix('\n') else {
        return false;
    };
    if message.contains('\r') || message.contains('\n') {
        return false;
    }

    let Some(task_and_attempt) = message.strip_prefix(PREFIX) else {
        return false;
    };
    let Some((task_id, attempt)) = task_and_attempt.split_once(ATTEMPT_SEPARATOR) else {
        return false;
    };
    if task_and_attempt.matches(ATTEMPT_SEPARATOR).count() != 1 || !is_canonical_task_id(task_id) {
        return false;
    }
    parse_canonical_nonzero_attempt(attempt).is_some()
}

pub(super) fn is_canonical_task_id(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')
            }
        })
}

pub(super) fn parse_canonical_nonzero_attempt(value: &str) -> Option<u32> {
    if value.is_empty()
        || value.starts_with('0')
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    value.parse().ok().filter(|attempt: &u32| *attempt != 0)
}

pub(super) fn delivery_read_only_command<const N: usize>(
    binding: &DeliveryGitReadOnlyBinding,
    command_arguments: [&str; N],
) -> Result<ValidatedCommand, CommandPolicyError> {
    delivery_read_only_owned_command(
        binding,
        command_arguments.into_iter().map(OsString::from).collect(),
    )
}

pub(super) fn delivery_read_only_owned_command(
    binding: &DeliveryGitReadOnlyBinding,
    command_arguments: Vec<OsString>,
) -> Result<ValidatedCommand, CommandPolicyError> {
    binding.git.revalidate()?;
    binding.repository.revalidate()?;
    let mut arguments = binding.repository.delivery_fixed_arguments();
    append_delivery_read_only_configuration(&mut arguments);
    arguments.extend(command_arguments);
    let command = ValidatedCommand::build_git(
        Arc::clone(&binding.git),
        &binding.repository,
        arguments,
        binding.environment.clone(),
        binding.timeout,
    )?
    .with_dependent_directories(vec![
        Arc::clone(&binding.repository.git_directory),
        Arc::clone(&binding.repository.work_tree),
        Arc::clone(&binding.common_git),
        Arc::clone(&binding.sandbox),
    ])?;
    #[cfg(unix)]
    let command = command.with_delivery_unix_directory_bindings(
        super::super::UnixDeliveryDirectoryBindings::repository(
            Arc::clone(&binding.repository.git_directory),
            Arc::clone(&binding.repository.work_tree),
            Arc::clone(&binding.common_git),
        ),
    )?;
    let command = command.with_delivery_git_empty_config(Arc::clone(&binding.config))?;
    Ok(command)
}

pub(super) fn append_delivery_read_only_configuration(arguments: &mut Vec<OsString>) {
    for configuration in [
        "core.fsmonitor=false",
        "core.untrackedCache=false",
        "core.sparseCheckout=false",
        "core.sparseCheckoutCone=false",
        "submodule.recurse=false",
        "fetch.recurseSubmodules=false",
        "extensions.worktreeConfig=false",
        "commit.gpgSign=false",
        "tag.gpgSign=false",
        "merge.gpgSign=false",
        "merge.verifySignatures=false",
        "merge.autoStash=false",
        "rerere.enabled=false",
        "credential.helper=",
        "core.askPass=",
        "core.attributesFile=",
        "core.excludesFile=",
        "i18n.commitEncoding=UTF-8",
        "diff.external=",
    ] {
        arguments.extend([OsString::from("-c"), OsString::from(configuration)]);
    }
    arguments.extend([
        OsString::from("-c"),
        OsString::from(super::super::git_hooks_path_configuration()),
    ]);
}
