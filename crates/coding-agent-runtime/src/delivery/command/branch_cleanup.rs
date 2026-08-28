use std::fmt;
use std::sync::Arc;

use crate::command_policy::{DeliveryGitTargetMutationBinding, ValidatedCommand};

use super::super::sandbox::DeliveryCommandSandbox;
use super::super::{DeliveryCommitOid, DeliverySourceError};

/// Opaque Task 17 branch-cleanup intent bound to one authenticated registered
/// checkout. The command-policy layer is the only consumer of the accessors;
/// delivery orchestration never receives a raw ref, argv, cwd, or stdin slot.
pub(crate) struct DeliveryGitBranchCleanupBinding {
    authority: DeliveryGitTargetMutationBinding,
    source_ref: String,
    target_ref: String,
    expected_source: DeliveryCommitOid,
    expected_target: DeliveryCommitOid,
}

impl DeliveryGitBranchCleanupBinding {
    pub(crate) fn try_new(
        authority: DeliveryGitTargetMutationBinding,
        source_branch: &str,
        target_branch: &str,
        expected_source: &DeliveryCommitOid,
        expected_target: &DeliveryCommitOid,
    ) -> Result<Self, DeliverySourceError> {
        let source_ref = format!("refs/heads/{source_branch}");
        let target_ref = format!("refs/heads/{target_branch}");
        ValidatedCommand::validate_delivery_branch_cleanup_binding(
            &authority,
            &source_ref,
            &target_ref,
            expected_source,
            expected_target,
        )?;
        Ok(Self {
            authority,
            source_ref,
            target_ref,
            expected_source: expected_source.clone(),
            expected_target: expected_target.clone(),
        })
    }

    pub(crate) const fn authority_for_policy(&self) -> &DeliveryGitTargetMutationBinding {
        &self.authority
    }

    pub(crate) fn source_ref_for_policy(&self) -> &str {
        &self.source_ref
    }

    pub(crate) fn target_ref_for_policy(&self) -> &str {
        &self.target_ref
    }

    pub(crate) const fn expected_source_for_policy(&self) -> &DeliveryCommitOid {
        &self.expected_source
    }

    pub(crate) const fn expected_target_for_policy(&self) -> &DeliveryCommitOid {
        &self.expected_target
    }
}

impl fmt::Debug for DeliveryGitBranchCleanupBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryGitBranchCleanupBinding(<opaque>)")
    }
}

/// Fixed-shape Task 17 query and mutation facade. Fresh target commits may be
/// supplied only as already parsed, object-format-bound OIDs and are admitted
/// solely to read-only object/ancestry queries. The delete transaction always
/// uses the persisted target/source values captured by the opaque binding.
pub(in super::super) struct DeliveryBranchCleanupCommands {
    pub(super) binding: DeliveryGitBranchCleanupBinding,
    pub(super) sandbox: Arc<DeliveryCommandSandbox>,
}

impl DeliveryBranchCleanupCommands {
    pub(in super::super) fn source_ref_symbolic(
        &self,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_branch_cleanup_source_ref_symbolic(&self.binding)
            .map_err(Into::into)
    }

    pub(in super::super) fn target_ref_symbolic(
        &self,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_branch_cleanup_target_ref_symbolic(&self.binding)
            .map_err(Into::into)
    }

    pub(in super::super) fn resolve_source_ref_raw(
        &self,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_branch_cleanup_resolve_source_ref(&self.binding)
            .map_err(Into::into)
    }

    pub(in super::super) fn resolve_target_ref_raw(
        &self,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_branch_cleanup_resolve_target_ref(&self.binding)
            .map_err(Into::into)
    }

    pub(in super::super) fn inspect_expected_source_commit(
        &self,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_branch_cleanup_expected_source_commit(&self.binding)
            .map_err(Into::into)
    }

    pub(in super::super) fn inspect_expected_target_commit(
        &self,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_branch_cleanup_expected_target_commit(&self.binding)
            .map_err(Into::into)
    }

    pub(in super::super) fn inspect_fresh_target_commit(
        &self,
        fresh_target: &DeliveryCommitOid,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_branch_cleanup_fresh_target_commit(&self.binding, fresh_target)
            .map_err(Into::into)
    }

    pub(in super::super) fn source_is_ancestor_of_fresh_target(
        &self,
        fresh_target: &DeliveryCommitOid,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_branch_cleanup_source_is_ancestor(&self.binding, fresh_target)
            .map_err(Into::into)
    }

    pub(in super::super) fn expected_target_is_ancestor_of_fresh_target(
        &self,
        fresh_target: &DeliveryCommitOid,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_branch_cleanup_target_is_ancestor(&self.binding, fresh_target)
            .map_err(Into::into)
    }

    /// Lists every linked worktree through Git's NUL-delimited porcelain
    /// protocol. The executor remains responsible for applying its fixed
    /// Task 17 stdout bound before the parser accepts the complete listing.
    pub(in super::super) fn worktree_list_porcelain(
        &self,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_branch_cleanup_worktree_list(&self.binding).map_err(Into::into)
    }

    /// Returns the sole Task 17 ref mutation. Both CAS values and both refs
    /// come from the persisted opaque binding, never from a fresh query
    /// parameter supplied at the mutation boundary.
    pub(in super::super) fn delete_source_transaction(
        &self,
    ) -> Result<ValidatedCommand, DeliverySourceError> {
        self.sandbox.revalidate()?;
        ValidatedCommand::delivery_branch_cleanup_delete_source(&self.binding).map_err(Into::into)
    }
}

impl fmt::Debug for DeliveryBranchCleanupCommands {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryBranchCleanupCommands(<opaque>)")
    }
}
