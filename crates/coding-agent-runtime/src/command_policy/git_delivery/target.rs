use super::*;

/// Capability-bound, fixed-shape mutations for a registered target checkout.
///
/// Production construction requires the retained
/// [`RegisteredCheckoutCommandContext`].  The binding therefore cannot be
/// redirected to an independently reopened repository or to a linked source
/// worktree merely by supplying equal namespace paths.  It exposes only the
/// deterministic expected-merge object commands, the one fixed actual merge
/// vocabulary admitted by Task 14, and the fixed recovery abort admitted by
/// Task 15.
pub(crate) struct DeliveryGitTargetMutationBinding {
    factory: DeliveryGitMutationCommandFactory,
    pub(super) binding: DeliveryGitReadOnlyBinding,
    object_id_length: usize,
}

impl DeliveryGitTargetMutationBinding {
    /// Narrows a registered primary checkout's already-authenticated
    /// read-only binding to the Task 14 mutation vocabulary.  The `Arc`
    /// identity checks deliberately prove provenance, rather than merely
    /// matching path or file-system identity values after a namespace reopen.
    pub(crate) fn try_from_registered_checkout(
        factory: DeliveryGitMutationCommandFactory,
        read_only: &DeliveryGitReadOnlyBinding,
        registered: &RegisteredCheckoutCommandContext,
        object_id_length: usize,
    ) -> Result<Self, CommandPolicyError> {
        if !matches!(object_id_length, 40 | 64)
            || !Arc::ptr_eq(&read_only.git, &registered.git)
            || !Arc::ptr_eq(
                read_only.repository.git_directory(),
                &registered.checkout_git.execution,
            )
            || !Arc::ptr_eq(
                read_only.repository.work_tree(),
                &registered.checkout.execution,
            )
            || !Arc::ptr_eq(&read_only.common_git, &registered.common_git.execution)
        {
            return Err(CommandPolicyError::InvalidGitBinding);
        }
        registered.git.revalidate()?;
        registered.checkout_binding.revalidate()?;
        registered.common_git.execution.revalidate()?;
        registered.checkout_git.execution.revalidate()?;
        registered.checkout.execution.revalidate()?;
        factory.revalidate_for(&read_only.git)?;
        revalidate_delivery_read_only_binding(read_only)?;
        Ok(Self {
            factory,
            binding: clone_delivery_read_only_binding(read_only),
            object_id_length,
        })
    }

    /// Test-only construction avoids fabricating a registered-checkout
    /// authentication tree in this command-policy unit module.  Production
    /// code has no equivalent generic constructor.
    #[cfg(test)]
    pub(super) fn from_read_only_for_test(
        factory: DeliveryGitMutationCommandFactory,
        read_only: &DeliveryGitReadOnlyBinding,
        object_id_length: usize,
    ) -> Result<Self, CommandPolicyError> {
        if !matches!(object_id_length, 40 | 64) {
            return Err(CommandPolicyError::InvalidGitBinding);
        }
        factory.revalidate_for(&read_only.git)?;
        revalidate_delivery_read_only_binding(read_only)?;
        Ok(Self {
            factory,
            binding: clone_delivery_read_only_binding(read_only),
            object_id_length,
        })
    }

    /// Revalidates the capability against the exact probe executable before a
    /// facade can hand out one target-side mutation command.
    pub(crate) fn revalidate_for_executable(
        &self,
        executable: &Arc<PinnedExecutable>,
    ) -> Result<(), CommandPolicyError> {
        if !Arc::ptr_eq(&self.binding.git, executable) {
            return Err(CommandPolicyError::InvalidGitBinding);
        }
        self.factory.revalidate_for(executable)?;
        revalidate_delivery_read_only_binding(&self.binding)
    }

    /// Creates the exact expected merge object with target first parent and
    /// source second parent.  Both parents must remain distinct full object
    /// IDs; no revision expression, ref, path, or caller-selected command
    /// token is accepted.
    pub(crate) fn commit_merge_tree(
        &self,
        tree: &str,
        target_parent: &str,
        source_parent: &str,
        input: ExactChildInput,
        metadata: &DeliveryGitCommitEnvironment,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        self.require_object_id(tree)?;
        self.require_object_id(target_parent)?;
        self.require_object_id(source_parent)?;
        if target_parent == source_parent {
            return Err(CommandPolicyError::InvalidGitBinding);
        }
        self.command_owned(
            vec![
                OsString::from("commit-tree"),
                OsString::from("--no-gpg-sign"),
                OsString::from(tree),
                OsString::from("-p"),
                OsString::from(target_parent),
                OsString::from("-p"),
                OsString::from(source_parent),
            ],
            Some(input),
            Some(metadata),
        )
    }

    /// Inspects exactly one validated commit object using a fixed batch input.
    pub(crate) fn cat_file_commit(
        &self,
        object: &str,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        self.require_object_id(object)?;
        let input = ExactChildInput::try_new(cat_file_batch_input(object))
            .map_err(|_| CommandPolicyError::InvalidGitBinding)?;
        self.command(["cat-file", "--batch"], Some(input), None)
    }

    /// Executes the one fixed actual-merge vocabulary.  The caller can supply
    /// only a validated complete source object ID and the canonical Task 14
    /// message value; no general argv, cwd, environment, ref, or path surface
    /// is exposed.
    pub(crate) fn merge(
        &self,
        source: &str,
        message: &str,
        metadata: &DeliveryGitCommitEnvironment,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        self.require_object_id(source)?;
        if !is_canonical_delivery_merge_message(message) {
            return Err(CommandPolicyError::InvalidGitBinding);
        }
        self.command_owned(
            [
                "merge",
                "--no-ff",
                "--strategy=ort",
                "--no-edit",
                "--no-verify",
                "--no-verify-signatures",
                "--no-gpg-sign",
                "--no-autostash",
                "--no-rerere-autoupdate",
                "--no-overwrite-ignore",
                "--no-log",
                "--no-stat",
                "--cleanup=verbatim",
                "-m",
                message,
                "--",
                source,
            ]
            .into_iter()
            .map(OsString::from)
            .collect(),
            None,
            Some(metadata),
        )
    }

    /// Aborts the target's in-progress merge through the one Task 15 recovery
    /// vocabulary. The caller cannot supply arguments, input, or commit
    /// metadata, so this retains the binding's ordinary isolated environment.
    pub(crate) fn merge_abort(&self) -> Result<ValidatedCommand, CommandPolicyError> {
        self.command(["merge", "--abort"], None, None)
    }

    pub(super) fn command<const N: usize>(
        &self,
        command_arguments: [&str; N],
        exact_input: Option<ExactChildInput>,
        commit_metadata: Option<&DeliveryGitCommitEnvironment>,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        self.command_owned(
            command_arguments.into_iter().map(OsString::from).collect(),
            exact_input,
            commit_metadata,
        )
    }

    fn command_owned(
        &self,
        command_arguments: Vec<OsString>,
        exact_input: Option<ExactChildInput>,
        commit_metadata: Option<&DeliveryGitCommitEnvironment>,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        self.revalidate_for_executable(&self.binding.git)?;
        let environment = match commit_metadata {
            Some(metadata) => metadata.child_environment(&self.binding.environment)?,
            None => self.binding.environment.clone(),
        };
        let mut arguments = self.binding.repository.delivery_fixed_arguments();
        append_delivery_read_only_configuration(&mut arguments);
        arguments.extend(command_arguments);
        let mut command = ValidatedCommand::build_git(
            Arc::clone(&self.binding.git),
            &self.binding.repository,
            arguments,
            environment,
            self.binding.timeout,
        )?
        .with_dependent_directories(vec![
            Arc::clone(&self.binding.repository.git_directory),
            Arc::clone(&self.binding.repository.work_tree),
            Arc::clone(&self.binding.common_git),
            Arc::clone(&self.binding.sandbox),
        ])?;
        #[cfg(unix)]
        {
            command = command.with_delivery_unix_directory_bindings(
                super::super::UnixDeliveryDirectoryBindings::repository(
                    Arc::clone(&self.binding.repository.git_directory),
                    Arc::clone(&self.binding.repository.work_tree),
                    Arc::clone(&self.binding.common_git),
                ),
            )?;
        }
        command = command.with_delivery_git_empty_config(Arc::clone(&self.binding.config))?;
        command.exact_input = exact_input;
        Ok(command)
    }

    pub(super) fn require_object_id(&self, object: &str) -> Result<(), CommandPolicyError> {
        require_delivery_object_id(object, self.object_id_length)
    }
}

impl fmt::Debug for DeliveryGitTargetMutationBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryGitTargetMutationBinding(<opaque>)")
    }
}
