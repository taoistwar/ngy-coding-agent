use std::fmt;
use std::sync::Arc;

use crate::command_policy::{
    DeliveryGitCommitEnvironment, DeliveryGitSourceMutationBinding,
    DeliveryGitTemporaryIndexEnvironment, ExecutionDirectory, ValidatedCommand,
};
use crate::process_supervisor::ExactChildInput;

use super::super::sandbox::DeliveryCommandSandbox;
use super::super::source_tree::DeliveryTemporaryIndex;
use super::super::{DeliveryCommitOid, DeliverySourceError, DeliveryTreeOid};
use super::{DeliveryIndexInfoInput, DeliverySnapshotHashInput};

/// Fixed-shape mutations used exclusively to construct unreferenced source
/// objects.  The surrounding source-tree/source-commit modules own lifecycle
/// validation and result parsing; this type owns no namespace path itself.
pub(in super::super) struct DeliverySourceMutationCommands {
    pub(super) binding: DeliveryGitSourceMutationBinding,
    pub(super) sandbox: Arc<DeliveryCommandSandbox>,
}

impl DeliverySourceMutationCommands {
    pub(in super::super) fn read_tree(
        &self,
        temporary_index: &DeliveryTemporaryIndex,
        base: &DeliveryCommitOid,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        let temporary_index = self.temporary_index_environment(temporary_index)?;
        self.binding
            .read_tree(&temporary_index, base.as_str())
            .map_err(Into::into)
    }

    /// Returns only the retained worktree authority required for the Task 11
    /// no-follow snapshot. The caller receives no command/path selection
    /// capability; all later Git input remains fixed typed stdin.
    pub(in super::super) fn snapshot_work_tree(
        &self,
    ) -> Result<Arc<ExecutionDirectory>, DeliverySourceError> {
        self.sandbox.revalidate()?;
        self.binding.snapshot_work_tree().map_err(Into::into)
    }

    pub(in super::super) fn hash_snapshot_file(
        &self,
        input: DeliverySnapshotHashInput,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        self.binding
            .hash_snapshot_file(input.into_exact_input())
            .map_err(Into::into)
    }

    pub(in super::super) fn update_index_info(
        &self,
        temporary_index: &DeliveryTemporaryIndex,
        input: DeliveryIndexInfoInput,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        let temporary_index = self.temporary_index_environment(temporary_index)?;
        self.binding
            .update_index_info(&temporary_index, input.into_exact_input())
            .map_err(Into::into)
    }

    pub(in super::super) fn write_tree(
        &self,
        temporary_index: &DeliveryTemporaryIndex,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        let temporary_index = self.temporary_index_environment(temporary_index)?;
        self.binding
            .write_tree(&temporary_index)
            .map_err(Into::into)
    }

    pub(in super::super) fn commit_tree(
        &self,
        tree: &DeliveryTreeOid,
        parent: &DeliveryCommitOid,
        input: ExactChildInput,
        metadata: &DeliveryGitCommitEnvironment,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        self.binding
            .commit_tree(tree.as_str(), parent.as_str(), input, metadata)
            .map_err(Into::into)
    }

    pub(in super::super) fn inspect_commit(
        &self,
        commit: &DeliveryCommitOid,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        self.binding
            .cat_file_commit(commit.as_str())
            .map_err(Into::into)
    }

    pub(in super::super) fn snapshot_index_entries(
        &self,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        self.binding.index_entries().map_err(Into::into)
    }

    pub(in super::super) fn snapshot_untracked_paths(
        &self,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        self.binding.untracked_paths().map_err(Into::into)
    }

    pub(in super::super) fn snapshot_deleted_base_paths(
        &self,
        base: &DeliveryCommitOid,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        self.binding
            .deleted_base_paths(base.as_str())
            .map_err(Into::into)
    }

    fn temporary_index_environment(
        &self,
        temporary_index: &DeliveryTemporaryIndex,
    ) -> Result<DeliveryGitTemporaryIndexEnvironment, DeliverySourceError> {
        self.sandbox.revalidate()?;
        temporary_index.revalidate()?;
        DeliveryGitTemporaryIndexEnvironment::try_new(temporary_index.directory_authority())
            .map_err(Into::into)
    }
}

impl fmt::Debug for DeliverySourceMutationCommands {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliverySourceMutationCommands(<opaque>)")
    }
}

/// Fixed-shape real-index and source-ref operations used only after an
/// unreferenced source commit has been durably recorded as CommitPending.
///
/// The command-policy binding has already retained the authenticated source
/// branch. This facade therefore accepts only runtime-owned object IDs and
/// never exposes a generic Git invocation or ref parameter.
pub(in super::super) struct DeliverySourceRealIndexCommands {
    pub(super) binding: DeliveryGitSourceMutationBinding,
    pub(super) sandbox: Arc<DeliveryCommandSandbox>,
}

impl DeliverySourceRealIndexCommands {
    pub(in super::super) fn stage_candidate_in_real_index(
        &self,
        candidate: &DeliveryTreeOid,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        self.binding
            .stage_candidate_in_real_index(candidate.as_str())
            .map_err(Into::into)
    }

    /// Refreshes the stat cache for the already staged real index. This has no
    /// caller-selected path or argument surface, and is used only after the
    /// fixed candidate-tree stage operation.
    pub(in super::super) fn refresh_real_index_stat(
        &self,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        self.binding.refresh_real_index_stat().map_err(Into::into)
    }

    /// Fixed candidate-tree proof required immediately before the first real
    /// index mutation.  The only accepted value is the already typed candidate
    /// tree returned by Task 11.
    pub(in super::super) fn inspect_candidate_object_type(
        &self,
        candidate: &DeliveryTreeOid,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        self.binding
            .cat_file_candidate_type(candidate.as_str())
            .map_err(Into::into)
    }

    pub(in super::super) fn update_source_ref_cas(
        &self,
        expected: &DeliveryCommitOid,
        base: &DeliveryCommitOid,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        self.binding
            .update_source_ref_cas(expected.as_str(), base.as_str())
            .map_err(Into::into)
    }

    /// Returns a `git diff-index --quiet` predicate command. The executor
    /// alone maps its fixed 0/1 status contract to a typed match result.
    pub(in super::super) fn index_matches_tree(
        &self,
        candidate: &DeliveryTreeOid,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        self.binding
            .index_matches_tree(candidate.as_str())
            .map_err(Into::into)
    }

    /// Returns a `git diff-files --quiet` predicate command. The executor
    /// alone maps its fixed 0/1 status contract to a typed match result.
    pub(in super::super) fn worktree_matches_index(
        &self,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        self.binding.worktree_matches_index().map_err(Into::into)
    }
}

impl fmt::Debug for DeliverySourceRealIndexCommands {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliverySourceRealIndexCommands(<opaque>)")
    }
}
