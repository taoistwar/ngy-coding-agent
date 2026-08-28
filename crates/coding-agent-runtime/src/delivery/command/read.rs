use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::command_policy::{
    DeliveryGitSourceMutationBinding, DeliveryGitTargetMutationBinding, GitCommandBinding,
    ValidatedCommand,
};
use crate::process_supervisor::PlatformEnvironment;
use crate::worktree::LinkedWorktreeCommandContext;
use crate::{RegisteredCheckoutCommandContext, RelativePath};

use super::super::sandbox::DeliveryCommandSandbox;
use super::super::{
    DeliveryCommitOid, DeliveryGitObjectFormat, DeliverySourceError, DeliveryTreeOid,
    ProbedDeliveryGit,
};
use super::branch_cleanup::{DeliveryBranchCleanupCommands, DeliveryGitBranchCleanupBinding};
use super::merge::DeliveryTargetMutationCommands;
use super::source_mutation::{DeliverySourceMutationCommands, DeliverySourceRealIndexCommands};
use super::{
    DeliveryGitReadOnlyBinding, checked_path_input, delivery_git_environment,
    require_distinct_directories, require_target_directories,
};

/// Typed facade for the read-only commands admitted during source opening.
pub(crate) struct DeliverySourceReadCommands {
    binding: DeliveryGitReadOnlyBinding,
    sandbox: Arc<DeliveryCommandSandbox>,
    object_format: DeliveryGitObjectFormat,
}

impl DeliverySourceReadCommands {
    pub(in super::super) fn try_new(
        probe: &ProbedDeliveryGit,
        authenticated: &LinkedWorktreeCommandContext,
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
        probe
            .mutation_command_factory()?
            .revalidate_for(&authenticated.git)?;
        sandbox.revalidate()?;
        let sandbox_directory = sandbox.workspace_directory();
        let config = sandbox.empty_config_authority()?;
        require_distinct_directories(&[
            &authenticated.common_git.execution,
            &authenticated.worktree_admin.execution,
            &authenticated.worktree.execution,
            &sandbox_directory,
        ])?;
        let repository = GitCommandBinding::try_new(
            Arc::clone(&authenticated.worktree_admin.execution),
            Arc::clone(&authenticated.worktree.execution),
        )?;
        let environment = delivery_git_environment(platform, &config, &sandbox_directory)?;
        Ok(Self {
            binding: DeliveryGitReadOnlyBinding {
                git: Arc::clone(&authenticated.git),
                repository,
                common_git: Arc::clone(&authenticated.common_git.execution),
                sandbox: sandbox_directory,
                config,
                environment,
                timeout,
            },
            sandbox,
            object_format: probe.object_format(),
        })
    }

    pub(crate) const fn object_format(&self) -> DeliveryGitObjectFormat {
        self.object_format
    }

    pub(crate) fn resolve_head(&self) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_resolve_head(&self.binding).map_err(Into::into)
    }

    pub(crate) fn repository_object_format(&self) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_repository_object_format(&self.binding).map_err(Into::into)
    }

    pub(crate) fn symbolic_head(&self) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_symbolic_head(&self.binding).map_err(Into::into)
    }

    pub(crate) fn index_entries(&self) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_index_entries(&self.binding).map_err(Into::into)
    }

    pub(crate) fn untracked_paths(&self) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_untracked_paths(&self.binding).map_err(Into::into)
    }

    /// Captures the complete source index/worktree cleanliness fact. The
    /// fixed porcelain command includes staged, unstaged, unmerged and
    /// untracked entries and admits no caller-selected path or option.
    pub(crate) fn status_porcelain_v2(&self) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_target_status(&self.binding).map_err(Into::into)
    }

    /// Lists ignored, untracked source nodes as a separate cleanup safety
    /// fact. Ordinary porcelain status intentionally hides these nodes, while
    /// non-force `worktree remove` may otherwise delete them recursively.
    pub(crate) fn ignored_untracked_paths(&self) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_target_ignored_untracked_paths(&self.binding).map_err(Into::into)
    }

    pub(crate) fn check_attributes(
        &self,
        raw_paths: &[Vec<u8>],
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        let input = checked_path_input(raw_paths)?;
        ValidatedCommand::delivery_check_attributes(&self.binding, input).map_err(Into::into)
    }

    /// Narrows the already authenticated read binding to the fixed Task 11
    /// source-object command vocabulary.  This does not accept a path,
    /// environment variable, or arbitrary Git argument from its caller.
    pub(in super::super) fn mutation_commands(
        &self,
        probe: &ProbedDeliveryGit,
    ) -> Result<DeliverySourceMutationCommands, DeliverySourceError> {
        self.sandbox.revalidate()?;
        if !probe.has_repository_object_format_binding()
            || probe.object_format() != self.object_format
        {
            return Err(DeliverySourceError::AuthenticationChanged);
        }
        let binding = DeliveryGitSourceMutationBinding::try_new(
            probe.mutation_command_factory()?,
            &self.binding,
            self.object_format.hexadecimal_length(),
        )?;
        Ok(DeliverySourceMutationCommands {
            binding,
            sandbox: Arc::clone(&self.sandbox),
        })
    }

    /// Narrows the authenticated source binding to the one real source index
    /// and source branch admitted during CommitPending. The branch is supplied
    /// only while this capability is created; later operations expose no
    /// caller-selected ref, argv, path, or environment slot.
    pub(in super::super) fn real_index_commands(
        &self,
        probe: &ProbedDeliveryGit,
        source_branch: &str,
    ) -> Result<DeliverySourceRealIndexCommands, DeliverySourceError> {
        self.sandbox.revalidate()?;
        if !probe.has_repository_object_format_binding()
            || probe.object_format() != self.object_format
        {
            return Err(DeliverySourceError::AuthenticationChanged);
        }
        let source_objects = DeliveryGitSourceMutationBinding::try_new(
            probe.mutation_command_factory()?,
            &self.binding,
            self.object_format.hexadecimal_length(),
        )?;
        let binding = source_objects.real_index_binding(source_branch)?;
        Ok(DeliverySourceRealIndexCommands {
            binding,
            sandbox: Arc::clone(&self.sandbox),
        })
    }
}

impl fmt::Debug for DeliverySourceReadCommands {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliverySourceReadCommands(<opaque>)")
    }
}

/// Typed facade for the fixed read-only command vocabulary admitted while
/// observing a registered target checkout.
///
/// Unlike a task linked worktree, the registered primary checkout deliberately
/// aliases its common Git and checkout-Git directories.  This capability keeps
/// that one permitted alias explicit while still requiring the checkout root
/// and private command sandbox to remain distinct identities.
pub(crate) struct DeliveryTargetReadCommands {
    binding: DeliveryGitReadOnlyBinding,
    sandbox: Arc<DeliveryCommandSandbox>,
    object_id_length: usize,
}

impl DeliveryTargetReadCommands {
    pub(in super::super) fn try_new(
        probe: &ProbedDeliveryGit,
        authenticated: &RegisteredCheckoutCommandContext,
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
        probe
            .mutation_command_factory()?
            .revalidate_for(&authenticated.git)?;
        sandbox.revalidate()?;
        let sandbox_directory = sandbox.workspace_directory();
        let config = sandbox.empty_config_authority()?;
        require_target_directories(
            &authenticated.common_git.execution,
            &authenticated.checkout_git.execution,
            &authenticated.checkout.execution,
            &sandbox_directory,
        )?;
        let repository = GitCommandBinding::try_new(
            Arc::clone(&authenticated.checkout_git.execution),
            Arc::clone(&authenticated.checkout.execution),
        )?;
        let environment = delivery_git_environment(platform, &config, &sandbox_directory)?;
        Ok(Self {
            binding: DeliveryGitReadOnlyBinding {
                git: Arc::clone(&authenticated.git),
                repository,
                common_git: Arc::clone(&authenticated.common_git.execution),
                sandbox: sandbox_directory,
                config,
                environment,
                timeout,
            },
            sandbox,
            object_id_length: probe.object_format().hexadecimal_length(),
        })
    }

    pub(in super::super) fn resolve_head(&self) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_resolve_head(&self.binding).map_err(Into::into)
    }

    pub(in super::super) fn repository_object_format(
        &self,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_repository_object_format(&self.binding).map_err(Into::into)
    }

    pub(in super::super) const fn object_format(&self) -> DeliveryGitObjectFormat {
        match self.object_id_length {
            40 => DeliveryGitObjectFormat::Sha1,
            64 => DeliveryGitObjectFormat::Sha256,
            _ => unreachable!(),
        }
    }

    pub(in super::super) fn symbolic_head(&self) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_symbolic_head(&self.binding).map_err(Into::into)
    }

    /// Returns the one fixed porcelain-v2 cleanliness observation.  Callers
    /// receive no way to suppress untracked paths or ignore submodule state.
    pub(in super::super) fn status_porcelain_v2(
        &self,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_target_status(&self.binding).map_err(Into::into)
    }

    pub(in super::super) fn unmerged_entries(
        &self,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_target_unmerged_entries(&self.binding).map_err(Into::into)
    }

    /// Lists all target tracked paths before the target attribute safety scan.
    pub(in super::super) fn tracked_paths(&self) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_target_tracked_paths(&self.binding).map_err(Into::into)
    }

    /// Lists only ignored, untracked target nodes for the write-set collision
    /// check.  Git receives no caller-selected pathspec.
    pub(in super::super) fn ignored_untracked_paths(
        &self,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_target_ignored_untracked_paths(&self.binding).map_err(Into::into)
    }

    pub(in super::super) fn check_attributes(
        &self,
        raw_paths: &[Vec<u8>],
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        let input = checked_path_input(raw_paths)?;
        ValidatedCommand::delivery_check_attributes(&self.binding, input).map_err(Into::into)
    }

    /// Returns the fixed `merge-base --is-ancestor` predicate.  The executor
    /// maps only its documented 0/1 status contract; callers cannot pass a
    /// revision expression or ref name.
    pub(in super::super) fn source_is_ancestor_of_target(
        &self,
        source: &DeliveryCommitOid,
        target: &DeliveryCommitOid,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_source_is_ancestor_of_target(
            &self.binding,
            source.as_str(),
            target.as_str(),
            self.object_id_length,
        )
        .map_err(Into::into)
    }

    /// Returns every best common ancestor for two already authenticated commit
    /// IDs. `--all` avoids accepting Git's unspecified choice for a
    /// criss-cross history; the preflight layer decides whether the resulting
    /// base set is admissible.
    pub(in super::super) fn merge_base(
        &self,
        target: &DeliveryCommitOid,
        source: &DeliveryCommitOid,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_merge_base(
            &self.binding,
            target.as_str(),
            source.as_str(),
            self.object_id_length,
        )
        .map_err(Into::into)
    }

    /// Returns Git's modern object-only merge preflight command.  It is not a
    /// quiet predicate because exit 0 and 1 carry different bounded machine
    /// output grammars that the preflight parser must validate separately.
    pub(in super::super) fn merge_tree(
        &self,
        target: &DeliveryCommitOid,
        source: &DeliveryCommitOid,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_merge_tree(
            &self.binding,
            target.as_str(),
            source.as_str(),
            self.object_id_length,
        )
        .map_err(Into::into)
    }

    /// Returns the object-only diff-tree path listing used to turn a clean
    /// preflight merge tree into the candidate write set.  The merged value is
    /// a typed tree OID, not a caller-provided revision expression.
    pub(in super::super) fn merge_write_set(
        &self,
        target: &DeliveryCommitOid,
        merged_tree: &DeliveryTreeOid,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_merge_write_set(
            &self.binding,
            target.as_str(),
            merged_tree.as_str(),
            self.object_id_length,
        )
        .map_err(Into::into)
    }

    /// Returns the fixed object-only raw diff used to bind an actual
    /// mixed-conflict scene to the expected merge tree. The command carries
    /// no caller-selected pathspec; its only variable values are validated,
    /// complete object IDs.
    pub(in super::super) fn expected_merge_raw_diff(
        &self,
        target: &DeliveryCommitOid,
        merged_tree: &DeliveryTreeOid,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_expected_merge_raw_diff(
            &self.binding,
            target.as_str(),
            merged_tree.as_str(),
            self.object_id_length,
        )
        .map_err(Into::into)
    }

    /// Reads only the exact conflict-path entries from one authenticated
    /// commit tree. The typed paths have already passed the runtime's
    /// cross-platform relative-path rules; the policy boundary repeats its
    /// independent argv and aggregate-bound validation before execution.
    pub(in super::super) fn expected_conflict_tree_entries(
        &self,
        commit: &DeliveryCommitOid,
        paths: &[RelativePath],
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        let paths = paths
            .iter()
            .map(RelativePath::as_slash_str)
            .collect::<Vec<_>>();
        ValidatedCommand::delivery_expected_conflict_tree_entries(
            &self.binding,
            commit.as_str(),
            &paths,
            self.object_id_length,
        )
        .map_err(Into::into)
    }

    /// Narrows this exact registered checkout binding to the Task 14 target
    /// mutation vocabulary.  The registered context must be the same retained
    /// authority from which this target read facade was created; a reopened
    /// path or a source-worktree binding cannot satisfy its `Arc` provenance
    /// checks.
    pub(in super::super) fn mutation_commands(
        &self,
        probe: &ProbedDeliveryGit,
        registered: &RegisteredCheckoutCommandContext,
    ) -> Result<DeliveryTargetMutationCommands, DeliverySourceError> {
        self.sandbox.revalidate()?;
        if !probe.has_repository_object_format_binding()
            || probe.object_format().hexadecimal_length() != self.object_id_length
        {
            return Err(DeliverySourceError::AuthenticationChanged);
        }
        let binding = DeliveryGitTargetMutationBinding::try_from_registered_checkout(
            probe.mutation_command_factory()?,
            &self.binding,
            registered,
            self.object_id_length,
        )?;
        Ok(DeliveryTargetMutationCommands {
            binding,
            sandbox: Arc::clone(&self.sandbox),
        })
    }

    /// Narrows the authenticated registered checkout to the Task 17 branch
    /// cleanup vocabulary. Source/target refs and both persisted object IDs
    /// are bound once here; the returned facade has no ref-taking mutation or
    /// generic argv/stdin entry point.
    pub(in super::super) fn branch_cleanup_commands(
        &self,
        probe: &ProbedDeliveryGit,
        registered: &RegisteredCheckoutCommandContext,
        source_branch: &str,
        target_branch: &str,
        expected_source: &DeliveryCommitOid,
        expected_target: &DeliveryCommitOid,
    ) -> Result<DeliveryBranchCleanupCommands, DeliverySourceError> {
        self.sandbox.revalidate()?;
        if !probe.has_repository_object_format_binding()
            || probe.object_format().hexadecimal_length() != self.object_id_length
        {
            return Err(DeliverySourceError::AuthenticationChanged);
        }
        let authority = DeliveryGitTargetMutationBinding::try_from_registered_checkout(
            probe.mutation_command_factory()?,
            &self.binding,
            registered,
            self.object_id_length,
        )?;
        let binding = DeliveryGitBranchCleanupBinding::try_new(
            authority,
            source_branch,
            target_branch,
            expected_source,
            expected_target,
        )?;
        Ok(DeliveryBranchCleanupCommands {
            binding,
            sandbox: Arc::clone(&self.sandbox),
        })
    }
}

impl fmt::Debug for DeliveryTargetReadCommands {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryTargetReadCommands(<opaque>)")
    }
}
