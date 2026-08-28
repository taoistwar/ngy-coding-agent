//! Private bridge from the registered worktree provisioner to the
//! crate-private primary-checkout authority.
//!
//! This is the only code that can read `WorktreeProvisioner`'s original
//! retained binding. The returned value lives at crate scope so delivery does
//! not need access to this private implementation module.

use std::sync::Arc;

use crate::RegisteredCheckoutAuthenticator;
use crate::command_policy::PinnedExecutable;

use super::super::{WorktreeError, WorktreeProvisioner};

impl WorktreeProvisioner {
    /// Derives target-checkout authority solely from the retained original
    /// checkout and common-Git objects. No caller path, ref, or directory is
    /// accepted, and no namespace is reopened to reconstruct the binding.
    pub(crate) fn registered_checkout_authenticator(
        &self,
        expected_git: &Arc<PinnedExecutable>,
    ) -> Result<RegisteredCheckoutAuthenticator, WorktreeError> {
        self.validate_common_git_identity()?;
        let original_binding = self.original_binding.clone();
        original_binding
            .revalidate()
            .map_err(WorktreeError::CommandPolicy)?;
        let common_capability = self
            .common_git_capability
            .try_clone_capability()
            .map_err(|_| WorktreeError::CommonGitIdentityUnavailable)?;
        let checkout_execution = Arc::clone(original_binding.work_tree());
        let checkout_capability = checkout_execution
            .cloned_root_capability()
            .map_err(WorktreeError::CommandPolicy)?;
        let checkout_marker = checkout_capability
            .identity_marker()
            .map_err(|_| WorktreeError::InvalidRepository)?;

        RegisteredCheckoutAuthenticator::from_retained_parts(
            Arc::clone(&self.git),
            expected_git,
            original_binding.clone(),
            Arc::clone(original_binding.git_directory()),
            common_capability,
            self.common_git_identity,
            checkout_execution,
            checkout_capability,
            checkout_marker,
        )
    }
}
