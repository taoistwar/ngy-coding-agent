use std::fmt;
use std::sync::Arc;

use crate::command_policy::{
    DeliveryGitCommitEnvironment, DeliveryGitTargetMutationBinding, ValidatedCommand,
};
use crate::process_supervisor::ExactChildInput;
use crate::worktree::WorktreeIdentity;

use super::super::sandbox::DeliveryCommandSandbox;
use super::super::{DeliveryCommitOid, DeliverySourceError, DeliveryTreeOid};
use super::is_canonical_task_id;

/// Canonical, fixed-shape message input for the Task 14 expected-object and
/// actual-merge commands. The raw task ID never becomes a generic command
/// token: it is accepted once here, checked for its canonical UUID spelling,
/// then retained only as the exact internal object stdin bytes and `-m`
/// argument needed by the target mutation binding.
pub(in super::super) struct DeliveryMergeMessage {
    task_id: String,
    attempt: u32,
    argument: String,
    object_input: ExactChildInput,
}

impl DeliveryMergeMessage {
    pub(in super::super) fn try_new(
        task_id: &str,
        attempt: u64,
    ) -> Result<Self, DeliverySourceError> {
        let attempt = u32::try_from(attempt).map_err(|_| DeliverySourceError::Internal)?;
        if !is_canonical_task_id(task_id) || attempt == 0 {
            return Err(DeliverySourceError::Internal);
        }
        // `git merge -m` preserves an explicit terminal LF under
        // `--cleanup=verbatim`; without it Git produces a one-byte-shorter
        // commit than the exact `commit-tree` object.  The same opaque bytes
        // must therefore feed both paths.
        let argument = format!("coding-agent: merge task {task_id} attempt {attempt}\n");
        let object_input = ExactChildInput::try_new(argument.as_bytes().to_vec())
            .map_err(|_| DeliverySourceError::BoundsExceeded)?;
        Ok(Self {
            task_id: task_id.to_owned(),
            attempt,
            argument,
            object_input,
        })
    }

    /// Binds this canonical merge template to the source capability without
    /// exposing either internal scalar as a caller-controlled command value.
    pub(in super::super) fn matches_identity(&self, identity: &WorktreeIdentity) -> bool {
        self.task_id == identity.task_id() && self.attempt == identity.attempt()
    }

    fn argument(&self) -> &str {
        &self.argument
    }

    fn cloned_object_input(&self) -> ExactChildInput {
        self.object_input.clone()
    }

    /// The sole fixed LF-terminated payload used when verifying the raw
    /// expected merge object.  It is derived from the same private canonical
    /// message that feeds `commit-tree`; callers never supply these bytes.
    pub(in super::super) fn object_message_bytes(&self) -> Vec<u8> {
        self.argument.as_bytes().to_vec()
    }
}

impl fmt::Debug for DeliveryMergeMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryMergeMessage(<validated>)")
    }
}

/// Fixed-shape mutations admitted only after target observation has retained
/// the registered primary checkout's authentication context. The facade does
/// not expose a caller-selected target path, branch, command, environment, or
/// arbitrary argument list.
pub(in super::super) struct DeliveryTargetMutationCommands {
    pub(super) binding: DeliveryGitTargetMutationBinding,
    pub(super) sandbox: Arc<DeliveryCommandSandbox>,
}

impl DeliveryTargetMutationCommands {
    pub(in super::super) fn commit_expected_merge(
        &self,
        tree: &DeliveryTreeOid,
        target_parent: &DeliveryCommitOid,
        source_parent: &DeliveryCommitOid,
        message: &DeliveryMergeMessage,
        metadata: &DeliveryGitCommitEnvironment,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        self.binding
            .commit_merge_tree(
                tree.as_str(),
                target_parent.as_str(),
                source_parent.as_str(),
                message.cloned_object_input(),
                metadata,
            )
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

    pub(in super::super) fn merge(
        &self,
        source: &DeliveryCommitOid,
        message: &DeliveryMergeMessage,
        metadata: &DeliveryGitCommitEnvironment,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        self.binding
            .merge(source.as_str(), message.argument(), metadata)
            .map_err(Into::into)
    }

    /// Fixed Task 15 recovery command for the retained target checkout. No
    /// caller-selected arguments, input, or environment can reach Git.
    pub(in super::super) fn merge_abort(&self) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        self.binding.merge_abort().map_err(Into::into)
    }
}

impl fmt::Debug for DeliveryTargetMutationCommands {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryTargetMutationCommands(<opaque>)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_message_is_canonical_reusable_and_bound_to_its_worktree_identity() {
        let task_id = "123e4567-e89b-12d3-a456-426614174000";
        let message = DeliveryMergeMessage::try_new(task_id, 7).unwrap();
        let matching = WorktreeIdentity::try_new("repository", task_id, 7).unwrap();
        let other_attempt = WorktreeIdentity::try_new("repository", task_id, 8).unwrap();

        assert_eq!(
            message.argument(),
            "coding-agent: merge task 123e4567-e89b-12d3-a456-426614174000 attempt 7\n"
        );
        assert!(message.matches_identity(&matching));
        assert!(!message.matches_identity(&other_attempt));
        assert_eq!(format!("{message:?}"), "DeliveryMergeMessage(<validated>)");
        for (task_id, attempt) in [
            ("123E4567-e89b-12d3-a456-426614174000", 7),
            ("123e4567-e89b-12d3-a456-426614174000", 0),
            (
                "123e4567-e89b-12d3-a456-426614174000",
                u64::from(u32::MAX) + 1,
            ),
        ] {
            assert!(matches!(
                DeliveryMergeMessage::try_new(task_id, attempt),
                Err(DeliverySourceError::Internal)
            ));
        }
    }
}
