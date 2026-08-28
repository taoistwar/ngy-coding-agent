use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::RegisteredCheckoutCommandContext;
use crate::delivery::command::{DeliveryGitBranchCleanupBinding, DeliveryGitReadOnlyBinding};
use crate::delivery::{
    DeliveryCommitOid, MAX_MERGE_CONFLICT_PATH_BYTES, MAX_MERGE_CONFLICT_PATHS,
    MAX_MERGE_CONFLICT_PAYLOAD_BYTES, ProbedDeliveryGit,
};
use crate::process_supervisor::{ChildEnvironment, ExactChildInput};
use crate::worktree::CleanupWorktreeTarget;

#[cfg(unix)]
use super::DELIVERY_GIT_TEMPORARY_INDEX_SENTINEL;
use super::{
    CommandPolicyError, DeliveryGitEmptyConfig, ExecutionDirectory, GitCommandBinding,
    PinnedExecutable, ValidatedCommand, child_visible_path, validate_git_diff_path,
};

mod branch_cleanup;
mod cleanup;
mod probe;
mod read_only;
mod shared;
mod source;
mod target;

#[cfg(test)]
use branch_cleanup::delivery_branch_cleanup_transaction_input;
#[cfg(test)]
use cleanup::{
    DeliveryCleanupCommand, DeliveryCleanupTargetAuthority,
    build_delivery_cleanup_command_with_authority,
};
pub(crate) use probe::{
    DeliveryGitProbeCommands, DeliveryGitRepositoryProbeCommands, ProbeGitObjectId,
};
#[cfg(test)]
use probe::{delete_source_transaction_input, merge_arguments, unbound_probe_arguments};
use shared::{
    append_delivery_read_only_configuration, cat_file_batch_input,
    clone_delivery_read_only_binding, delivery_read_only_command, delivery_read_only_owned_command,
    is_canonical_delivery_merge_message, require_delivery_object_id,
    revalidate_delivery_read_only_binding,
};
use source::DeliveryGitSourceRef;
pub(crate) use source::{
    DeliveryGitCommitEnvironment, DeliveryGitSourceMutationBinding,
    DeliveryGitTemporaryIndexEnvironment,
};
pub(crate) use target::DeliveryGitTargetMutationBinding;

/// The mutation factory has no executable-accepting constructor. Future
/// delivery command adapters can only obtain it from a successful probe.
#[derive(Clone)]
pub(crate) struct DeliveryGitMutationCommandFactory {
    git: Arc<PinnedExecutable>,
}

impl DeliveryGitMutationCommandFactory {
    pub(crate) fn try_from_probe(probe: &ProbedDeliveryGit) -> Result<Self, CommandPolicyError> {
        probe.verify_for_mutation()?;
        Ok(Self {
            git: Arc::clone(probe.pinned_executable()),
        })
    }

    pub(crate) fn revalidate(&self) -> Result<(), CommandPolicyError> {
        self.git.revalidate()
    }

    /// Requires a downstream adapter to retain the exact `Arc` authorized by
    /// the successful probe. Equal paths or independently reopened handles do
    /// not satisfy this provenance check.
    pub(crate) fn require_same_executable(
        &self,
        executable: &Arc<PinnedExecutable>,
    ) -> Result<(), CommandPolicyError> {
        if Arc::ptr_eq(&self.git, executable) {
            Ok(())
        } else {
            Err(CommandPolicyError::InvalidGitBinding)
        }
    }

    pub(crate) fn revalidate_for(
        &self,
        executable: &Arc<PinnedExecutable>,
    ) -> Result<(), CommandPolicyError> {
        self.require_same_executable(executable)?;
        self.revalidate()
    }

    #[cfg(test)]
    pub(crate) fn is_bound_to_for_test(&self, executable: &Arc<PinnedExecutable>) -> bool {
        Arc::ptr_eq(&self.git, executable)
    }

    #[cfg(all(test, unix))]
    pub(crate) fn from_authorized_for_test(git: Arc<PinnedExecutable>) -> Self {
        Self { git }
    }
}

impl fmt::Debug for DeliveryGitMutationCommandFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryGitMutationCommandFactory(<opaque>)")
    }
}

#[cfg(feature = "test-support")]
impl ProbedDeliveryGit {
    /// Test-only observation of the exact `Arc` provenance enforced by the
    /// production mutation/read-only command factories.
    #[doc(hidden)]
    pub fn is_bound_to_for_test(&self, executable: &Arc<PinnedExecutable>) -> bool {
        Arc::ptr_eq(self.pinned_executable(), executable)
    }
}

#[cfg(test)]
mod tests;
