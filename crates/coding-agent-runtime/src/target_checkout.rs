//! Crate-private retained authority for a registered primary checkout.
//!
//! The object is constructed only from `worktree::authentication::target`,
//! where the original `WorktreeProvisioner` binding is visible. Delivery code
//! can then hold and revalidate this authority without reopening any checkout
//! namespace path or depending on the private worktree implementation tree.

use std::fmt;
use std::sync::Arc;

use crate::command_policy::{ExecutionDirectory, GitCommandBinding, PinnedExecutable};
use crate::root_capability::{DirectoryIdentityDomain, DurableDirectoryIdentityV1};
use crate::{DirectoryIdentityMarker, RootCapability, WorktreeError};

pub(crate) struct RegisteredCheckoutAuthenticator {
    git: Arc<PinnedExecutable>,
    original_binding: GitCommandBinding,
    common_git: RegisteredCheckoutDirectory,
    checkout: RegisteredCheckoutDirectory,
}

impl RegisteredCheckoutAuthenticator {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_retained_parts(
        provisioner_git: Arc<PinnedExecutable>,
        expected_git: &Arc<PinnedExecutable>,
        original_binding: GitCommandBinding,
        common_execution: Arc<ExecutionDirectory>,
        common_capability: RootCapability,
        common_marker: DirectoryIdentityMarker,
        checkout_execution: Arc<ExecutionDirectory>,
        checkout_capability: RootCapability,
        checkout_marker: DirectoryIdentityMarker,
    ) -> Result<Self, WorktreeError> {
        if !Arc::ptr_eq(&provisioner_git, expected_git) {
            return Err(WorktreeError::CommandPolicy(
                crate::CommandPolicyError::InvalidGitBinding,
            ));
        }
        expected_git
            .revalidate()
            .map_err(WorktreeError::CommandPolicy)?;
        original_binding
            .revalidate()
            .map_err(WorktreeError::CommandPolicy)?;
        let common_git = RegisteredCheckoutDirectory::new(
            common_execution,
            common_capability,
            common_marker,
            common_identity_mismatch,
        )?;
        let checkout = RegisteredCheckoutDirectory::new(
            checkout_execution,
            checkout_capability,
            checkout_marker,
            checkout_identity_mismatch,
        )?;
        GitCommandBinding::try_new(
            Arc::clone(&common_git.execution),
            Arc::clone(&checkout.execution),
        )
        .map_err(WorktreeError::CommandPolicy)?
        .revalidate()
        .map_err(WorktreeError::CommandPolicy)?;
        Ok(Self {
            git: Arc::clone(expected_git),
            original_binding,
            common_git,
            checkout,
        })
    }

    pub(crate) fn authenticate(&self) -> Result<RegisteredCheckoutAuthentication, WorktreeError> {
        self.revalidate_origins()?;
        let common_git = self.common_git.duplicate(common_identity_mismatch)?;
        // The primary checkout intentionally uses the common Git directory as
        // its checkout Git directory. Keep a distinct retained role so the
        // typed command binding cannot accidentally generalize this alias.
        let checkout_git = self.common_git.duplicate(common_identity_mismatch)?;
        let checkout = self.checkout.duplicate(checkout_identity_mismatch)?;
        let context = RegisteredCheckoutCommandContext::new(
            Arc::clone(&self.git),
            common_git,
            checkout_git,
            checkout,
        )?;
        let authentication = RegisteredCheckoutAuthentication {
            original_binding: self.original_binding.clone(),
            context,
        };
        authentication.reauthenticate()?;
        Ok(authentication)
    }

    fn revalidate_origins(&self) -> Result<(), WorktreeError> {
        self.git
            .revalidate()
            .map_err(WorktreeError::CommandPolicy)?;
        self.original_binding
            .revalidate()
            .map_err(WorktreeError::CommandPolicy)?;
        self.common_git.revalidate(common_identity_mismatch)?;
        self.checkout.revalidate(checkout_identity_mismatch)?;
        GitCommandBinding::try_new(
            Arc::clone(&self.common_git.execution),
            Arc::clone(&self.checkout.execution),
        )
        .map_err(WorktreeError::CommandPolicy)?
        .revalidate()
        .map_err(WorktreeError::CommandPolicy)
    }
}

impl fmt::Debug for RegisteredCheckoutAuthenticator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RegisteredCheckoutAuthenticator(<opaque>)")
    }
}

/// An A-authenticated registered checkout retained through the matching B
/// gate. Fields are crate-private authority, never public paths or refs.
pub(crate) struct RegisteredCheckoutAuthentication {
    original_binding: GitCommandBinding,
    context: RegisteredCheckoutCommandContext,
}

impl RegisteredCheckoutAuthentication {
    pub(crate) const fn command_context(&self) -> &RegisteredCheckoutCommandContext {
        &self.context
    }

    pub(crate) fn reauthenticate(&self) -> Result<(), WorktreeError> {
        self.context
            .git
            .revalidate()
            .map_err(WorktreeError::CommandPolicy)?;
        self.original_binding
            .revalidate()
            .map_err(WorktreeError::CommandPolicy)?;
        self.context
            .common_git
            .revalidate(common_identity_mismatch)?;
        self.context
            .checkout_git
            .revalidate(common_identity_mismatch)?;
        self.context
            .checkout
            .revalidate(checkout_identity_mismatch)?;
        self.context
            .checkout_binding
            .revalidate()
            .map_err(WorktreeError::CommandPolicy)?;
        require_primary_git_alias(&self.context)?;
        require_common_durable_identity(
            &self.context.common_git.capability,
            &self.context.common_identity,
        )?;
        require_common_durable_identity(
            &self.context.checkout_git.capability,
            &self.context.common_identity,
        )
    }
}

impl fmt::Debug for RegisteredCheckoutAuthentication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RegisteredCheckoutAuthentication(<opaque>)")
    }
}

pub(crate) struct RegisteredCheckoutCommandContext {
    pub(crate) git: Arc<PinnedExecutable>,
    pub(crate) common_git: RegisteredCheckoutDirectory,
    pub(crate) checkout_git: RegisteredCheckoutDirectory,
    pub(crate) checkout: RegisteredCheckoutDirectory,
    pub(crate) checkout_binding: GitCommandBinding,
    common_identity: DurableDirectoryIdentityV1,
}

impl RegisteredCheckoutCommandContext {
    fn new(
        git: Arc<PinnedExecutable>,
        common_git: RegisteredCheckoutDirectory,
        checkout_git: RegisteredCheckoutDirectory,
        checkout: RegisteredCheckoutDirectory,
    ) -> Result<Self, WorktreeError> {
        if !common_git
            .execution
            .has_same_identity(&checkout_git.execution)
            || common_git.marker != checkout_git.marker
        {
            return Err(WorktreeError::CommonGitIdentityMismatch);
        }
        let common_identity = DurableDirectoryIdentityV1::derive(
            &common_git.capability,
            DirectoryIdentityDomain::CommonGit,
        )
        .map_err(map_common_identity_error)?;
        let checkout_git_identity = DurableDirectoryIdentityV1::derive(
            &checkout_git.capability,
            DirectoryIdentityDomain::CommonGit,
        )
        .map_err(map_common_identity_error)?;
        if checkout_git_identity != common_identity {
            return Err(WorktreeError::CommonGitIdentityMismatch);
        }
        let checkout_binding = GitCommandBinding::try_new(
            Arc::clone(&checkout_git.execution),
            Arc::clone(&checkout.execution),
        )
        .map_err(WorktreeError::CommandPolicy)?;
        Ok(Self {
            git,
            common_git,
            checkout_git,
            checkout,
            checkout_binding,
            common_identity,
        })
    }

    /// The opaque durable identity of the registered repository's common Git
    /// directory. Delivery uses it only to prove that independently-created
    /// source and target capabilities belong to the same repository.
    pub(crate) const fn common_directory_identity(&self) -> &DurableDirectoryIdentityV1 {
        &self.common_identity
    }
}

impl fmt::Debug for RegisteredCheckoutCommandContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RegisteredCheckoutCommandContext(<opaque>)")
    }
}

pub(crate) struct RegisteredCheckoutDirectory {
    pub(crate) execution: Arc<ExecutionDirectory>,
    pub(crate) capability: RootCapability,
    marker: DirectoryIdentityMarker,
}

impl RegisteredCheckoutDirectory {
    fn new(
        execution: Arc<ExecutionDirectory>,
        capability: RootCapability,
        marker: DirectoryIdentityMarker,
        mismatch: fn() -> WorktreeError,
    ) -> Result<Self, WorktreeError> {
        capability
            .require_identity(marker)
            .map_err(|_| mismatch())?;
        execution
            .revalidate()
            .map_err(WorktreeError::CommandPolicy)?;
        let execution_capability = execution
            .cloned_root_capability()
            .map_err(WorktreeError::CommandPolicy)?;
        execution_capability
            .require_identity(marker)
            .map_err(|_| mismatch())?;
        Ok(Self {
            execution,
            capability,
            marker,
        })
    }

    fn duplicate(&self, mismatch: fn() -> WorktreeError) -> Result<Self, WorktreeError> {
        Self::new(
            Arc::clone(&self.execution),
            self.capability
                .try_clone_capability()
                .map_err(|_| mismatch())?,
            self.marker,
            mismatch,
        )
    }

    fn revalidate(&self, mismatch: fn() -> WorktreeError) -> Result<(), WorktreeError> {
        self.capability
            .require_identity(self.marker)
            .map_err(|_| mismatch())?;
        self.execution
            .revalidate()
            .map_err(WorktreeError::CommandPolicy)?;
        let execution_capability = self
            .execution
            .cloned_root_capability()
            .map_err(WorktreeError::CommandPolicy)?;
        execution_capability
            .require_identity(self.marker)
            .map_err(|_| mismatch())
    }
}

fn common_identity_mismatch() -> WorktreeError {
    WorktreeError::CommonGitIdentityMismatch
}

fn checkout_identity_mismatch() -> WorktreeError {
    WorktreeError::InvalidRepository
}

fn require_primary_git_alias(
    context: &RegisteredCheckoutCommandContext,
) -> Result<(), WorktreeError> {
    if context
        .common_git
        .execution
        .has_same_identity(&context.checkout_git.execution)
        && context.common_git.marker == context.checkout_git.marker
    {
        Ok(())
    } else {
        Err(WorktreeError::CommonGitIdentityMismatch)
    }
}

fn require_common_durable_identity(
    capability: &RootCapability,
    expected: &DurableDirectoryIdentityV1,
) -> Result<(), WorktreeError> {
    let current =
        DurableDirectoryIdentityV1::derive(capability, DirectoryIdentityDomain::CommonGit)
            .map_err(map_common_identity_error)?;
    if current == *expected {
        Ok(())
    } else {
        Err(WorktreeError::CommonGitIdentityMismatch)
    }
}

fn map_common_identity_error(error: crate::DirectoryIdentityError) -> WorktreeError {
    match error {
        crate::DirectoryIdentityError::Unavailable => WorktreeError::CommonGitIdentityUnavailable,
        crate::DirectoryIdentityError::Mismatch => WorktreeError::CommonGitIdentityMismatch,
    }
}
