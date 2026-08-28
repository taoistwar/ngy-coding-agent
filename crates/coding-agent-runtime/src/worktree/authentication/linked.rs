use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use crate::command_policy::{ExecutionDirectory, GitCommandBinding, PinnedExecutable};
use crate::root_capability::{DirectoryIdentityDomain, DurableDirectoryIdentityV1};
use crate::{DirectoryIdentityMarker, RelativePath, RootCapability};

use super::super::{
    WorktreeError, WorktreeProvisioner, WorktreeReservation, parse_commit_output,
    relative_path_from_path,
};
use super::identity::{
    RetainedDirectory, map_common_identity_error, reopen_execution_directory,
    require_child_identity, require_durable_identity, require_exact_git,
    require_execution_identity,
};
use super::metadata::{admin_relative_path, find_reserved_admin_record};

/// Owned authority for authenticating an existing linked worktree without
/// using the P4-A clean-worktree recovery entry point.
pub(crate) struct LinkedWorktreeAuthenticator {
    git: Arc<PinnedExecutable>,
    repository_id: String,
    original_binding: GitCommandBinding,
    common_git_directory: PathBuf,
    common_git_identity: DirectoryIdentityMarker,
    common_git_capability: RootCapability,
    common_git_execution: Arc<ExecutionDirectory>,
    artifact_root: PathBuf,
    artifact_root_directory: Arc<ExecutionDirectory>,
    artifact_root_capability: RootCapability,
    artifact_root_identity: DirectoryIdentityMarker,
    cargo_workspace_offset: PathBuf,
}

impl fmt::Debug for LinkedWorktreeAuthenticator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LinkedWorktreeAuthenticator(<opaque>)")
    }
}

impl LinkedWorktreeAuthenticator {
    pub(crate) fn from_provisioner(
        provisioner: &WorktreeProvisioner,
        expected_git: &Arc<PinnedExecutable>,
    ) -> Result<Self, WorktreeError> {
        verify_provisioner_origins(provisioner, expected_git)?;
        let common_git_capability = clone_common_capability(provisioner)?;
        let common_git_execution = reopen_execution_directory(
            &provisioner.common_git_directory,
            provisioner.common_git_identity,
            WorktreeError::CommonGitIdentityMismatch,
        )?;
        let (artifact_root_capability, artifact_root_identity) =
            clone_artifact_capability(provisioner)?;

        Ok(Self {
            git: Arc::clone(expected_git),
            repository_id: provisioner.repository_id.clone(),
            original_binding: provisioner.original_binding.clone(),
            common_git_directory: provisioner.common_git_directory.clone(),
            common_git_identity: provisioner.common_git_identity,
            common_git_capability,
            common_git_execution,
            artifact_root: provisioner.artifact_root.clone(),
            artifact_root_directory: Arc::clone(&provisioner.artifact_root_directory),
            artifact_root_capability,
            artifact_root_identity,
            cargo_workspace_offset: provisioner.cargo_workspace_offset.clone(),
        })
    }

    /// Performs the first (A) authentication and returns retained authorities
    /// that can later perform the matching B authentication.
    pub(crate) fn authenticate(
        &self,
        reservation: &WorktreeReservation,
    ) -> Result<LinkedWorktreeAuthentication, WorktreeError> {
        self.validate_reservation(reservation)?;
        self.revalidate_origins()?;
        let record = find_reserved_admin_record(
            &self.common_git_capability,
            &self.common_git_directory,
            reservation,
        )?
        .ok_or(WorktreeError::LinkedMetadataInvalid)?;
        let admin_relative = admin_relative_path(&record.name)?;
        let worktree_relative = relative_path_from_path(&reservation.identity.relative_path())?;
        let context = self.open_command_context(
            &record.path,
            &admin_relative,
            reservation.worktree_path(),
            &worktree_relative,
        )?;

        let authentication = LinkedWorktreeAuthentication {
            reservation: reservation.clone(),
            original_binding: self.original_binding.clone(),
            common_git_directory: self.common_git_directory.clone(),
            artifact_root_directory: Arc::clone(&self.artifact_root_directory),
            artifact_root_capability: self
                .artifact_root_capability
                .try_clone_capability()
                .map_err(WorktreeError::Io)?,
            artifact_root_identity: self.artifact_root_identity,
            admin_name: record.name,
            admin_relative,
            worktree_relative,
            context,
        };
        authentication.reauthenticate()?;
        Ok(authentication)
    }

    fn open_command_context(
        &self,
        admin_path: &std::path::Path,
        admin_relative: &RelativePath,
        worktree_path: &std::path::Path,
        worktree_relative: &RelativePath,
    ) -> Result<LinkedWorktreeCommandContext, WorktreeError> {
        let common_git = RetainedDirectory::from_existing(
            Arc::clone(&self.common_git_execution),
            self.common_git_capability
                .try_clone_capability()
                .map_err(|_| WorktreeError::CommonGitIdentityUnavailable)?,
            self.common_git_identity,
            WorktreeError::CommonGitIdentityMismatch,
        )?;
        let worktree_admin =
            RetainedDirectory::open_child(&self.common_git_capability, admin_relative, admin_path)?;
        let worktree = RetainedDirectory::open_child(
            &self.artifact_root_capability,
            worktree_relative,
            worktree_path,
        )?;
        LinkedWorktreeCommandContext::new(
            Arc::clone(&self.git),
            common_git,
            worktree_admin,
            worktree,
        )
    }

    fn validate_reservation(&self, reservation: &WorktreeReservation) -> Result<(), WorktreeError> {
        if reservation.branch_name != reservation.identity.branch_name()
            || reservation.identity.repository_id() != self.repository_id
            || reservation.worktree_path
                != self
                    .artifact_root
                    .join(reservation.identity.relative_path())
            || reservation.source_common_git_identity != self.common_git_identity
            || reservation.cargo_workspace_offset != self.cargo_workspace_offset
            || parse_commit_output(reservation.base_commit.as_bytes()).is_err()
        {
            return Err(WorktreeError::InvalidReservation);
        }
        Ok(())
    }

    fn revalidate_origins(&self) -> Result<(), WorktreeError> {
        self.git
            .revalidate()
            .map_err(WorktreeError::CommandPolicy)?;
        self.common_git_capability
            .require_identity(self.common_git_identity)
            .map_err(map_common_identity_error)?;
        require_execution_identity(
            &self.common_git_execution,
            self.common_git_identity,
            WorktreeError::CommonGitIdentityMismatch,
        )?;
        self.original_binding
            .revalidate()
            .map_err(WorktreeError::CommandPolicy)?;
        self.artifact_root_capability
            .require_identity(self.artifact_root_identity)
            .map_err(|_| WorktreeError::ArtifactPathInvalid)?;
        require_execution_identity(
            &self.artifact_root_directory,
            self.artifact_root_identity,
            WorktreeError::ArtifactPathInvalid,
        )
    }
}

/// An opaque A-authenticated linked worktree retained through the B gate.
pub(crate) struct LinkedWorktreeAuthentication {
    reservation: WorktreeReservation,
    original_binding: GitCommandBinding,
    common_git_directory: PathBuf,
    artifact_root_directory: Arc<ExecutionDirectory>,
    artifact_root_capability: RootCapability,
    artifact_root_identity: DirectoryIdentityMarker,
    admin_name: String,
    admin_relative: RelativePath,
    worktree_relative: RelativePath,
    context: LinkedWorktreeCommandContext,
}

impl fmt::Debug for LinkedWorktreeAuthentication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LinkedWorktreeAuthentication(<opaque>)")
    }
}

impl LinkedWorktreeAuthentication {
    pub(crate) fn command_context(&self) -> &LinkedWorktreeCommandContext {
        &self.context
    }

    pub(crate) fn reauthenticate(&self) -> Result<(), WorktreeError> {
        self.reauthenticate_origins()?;
        self.reauthenticate_children()?;
        self.reauthenticate_metadata()?;
        require_durable_identity(
            &self.context.common_git.capability,
            DirectoryIdentityDomain::CommonGit,
            &self.context.common_identity,
        )?;
        require_durable_identity(
            &self.context.worktree_admin.capability,
            DirectoryIdentityDomain::WorktreeAdmin,
            &self.context.admin_identity,
        )
    }

    pub(super) fn cleanup_origin_matches(
        &self,
        reservation: &WorktreeReservation,
        expected_git: &Arc<PinnedExecutable>,
        expected_binding: &GitCommandBinding,
        expected_artifact_root_identity: DirectoryIdentityMarker,
        expected_worktree_relative: &RelativePath,
    ) -> bool {
        &self.reservation == reservation
            && Arc::ptr_eq(&self.context.git, expected_git)
            && Arc::ptr_eq(
                self.original_binding.git_directory(),
                expected_binding.git_directory(),
            )
            && Arc::ptr_eq(
                self.original_binding.work_tree(),
                expected_binding.work_tree(),
            )
            && self.artifact_root_identity == expected_artifact_root_identity
            && self.worktree_relative == *expected_worktree_relative
    }

    pub(super) fn cleanup_admin_name(&self) -> &str {
        &self.admin_name
    }

    pub(super) fn cleanup_admin_relative(&self) -> &RelativePath {
        &self.admin_relative
    }

    pub(super) const fn cleanup_common_identity(&self) -> &DurableDirectoryIdentityV1 {
        &self.context.common_identity
    }

    pub(super) const fn cleanup_admin_identity(&self) -> &DurableDirectoryIdentityV1 {
        &self.context.admin_identity
    }

    pub(super) const fn cleanup_admin_marker(&self) -> DirectoryIdentityMarker {
        self.context.worktree_admin.marker
    }

    pub(super) const fn cleanup_worktree_marker(&self) -> DirectoryIdentityMarker {
        self.context.worktree.marker
    }

    fn reauthenticate_origins(&self) -> Result<(), WorktreeError> {
        self.context
            .git
            .revalidate()
            .map_err(WorktreeError::CommandPolicy)?;
        self.original_binding
            .revalidate()
            .map_err(WorktreeError::CommandPolicy)?;
        self.artifact_root_directory
            .revalidate()
            .map_err(WorktreeError::CommandPolicy)?;
        self.artifact_root_capability
            .require_identity(self.artifact_root_identity)
            .map_err(|_| WorktreeError::ArtifactPathInvalid)
    }

    fn reauthenticate_children(&self) -> Result<(), WorktreeError> {
        self.context.common_git.revalidate()?;
        self.context.worktree_admin.revalidate()?;
        self.context.worktree.revalidate()?;
        require_child_identity(
            &self.context.common_git.capability,
            &self.admin_relative,
            self.context.worktree_admin.marker,
        )?;
        require_child_identity(
            &self.artifact_root_capability,
            &self.worktree_relative,
            self.context.worktree.marker,
        )
    }

    fn reauthenticate_metadata(&self) -> Result<(), WorktreeError> {
        let record = find_reserved_admin_record(
            &self.context.common_git.capability,
            &self.common_git_directory,
            &self.reservation,
        )?
        .ok_or(WorktreeError::LinkedMetadataInvalid)?;
        if record.name == self.admin_name {
            Ok(())
        } else {
            Err(WorktreeError::LinkedMetadataInvalid)
        }
    }
}

/// Single aggregate projection used by delivery config and command layers.
/// Fields remain crate-private so no path authority enters the public API.
pub(crate) struct LinkedWorktreeCommandContext {
    pub(crate) git: Arc<PinnedExecutable>,
    pub(crate) common_git: RetainedDirectory,
    pub(crate) worktree_admin: RetainedDirectory,
    pub(crate) worktree: RetainedDirectory,
    pub(crate) common_identity: DurableDirectoryIdentityV1,
    pub(crate) admin_identity: DurableDirectoryIdentityV1,
}

impl LinkedWorktreeCommandContext {
    pub(super) fn new(
        git: Arc<PinnedExecutable>,
        common_git: RetainedDirectory,
        worktree_admin: RetainedDirectory,
        worktree: RetainedDirectory,
    ) -> Result<Self, WorktreeError> {
        let common_identity = DurableDirectoryIdentityV1::derive(
            &common_git.capability,
            DirectoryIdentityDomain::CommonGit,
        )
        .map_err(map_common_identity_error)?;
        let admin_identity = DurableDirectoryIdentityV1::derive(
            &worktree_admin.capability,
            DirectoryIdentityDomain::WorktreeAdmin,
        )
        .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
        Ok(Self {
            git,
            common_git,
            worktree_admin,
            worktree,
            common_identity,
            admin_identity,
        })
    }
}

impl fmt::Debug for LinkedWorktreeCommandContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LinkedWorktreeCommandContext(<opaque>)")
    }
}

fn verify_provisioner_origins(
    provisioner: &WorktreeProvisioner,
    expected_git: &Arc<PinnedExecutable>,
) -> Result<(), WorktreeError> {
    require_exact_git(&provisioner.git, expected_git)?;
    provisioner.validate_common_git_identity()?;
    provisioner
        .original_binding
        .revalidate()
        .map_err(WorktreeError::CommandPolicy)?;
    provisioner
        .artifact_root_directory
        .revalidate()
        .map_err(WorktreeError::CommandPolicy)
}

fn clone_common_capability(
    provisioner: &WorktreeProvisioner,
) -> Result<RootCapability, WorktreeError> {
    provisioner
        .common_git_capability
        .try_clone_capability()
        .map_err(|_| WorktreeError::CommonGitIdentityUnavailable)
}

fn clone_artifact_capability(
    provisioner: &WorktreeProvisioner,
) -> Result<(RootCapability, DirectoryIdentityMarker), WorktreeError> {
    let capability = provisioner
        .artifact_root_capability
        .try_clone_capability()
        .map_err(WorktreeError::Io)?;
    let identity = capability
        .identity_marker()
        .map_err(|_| WorktreeError::ArtifactPathInvalid)?;
    require_execution_identity(
        &provisioner.artifact_root_directory,
        identity,
        WorktreeError::ArtifactPathInvalid,
    )?;
    Ok((capability, identity))
}
