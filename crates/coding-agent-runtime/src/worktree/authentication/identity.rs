use std::path::Path;
use std::sync::Arc;

use crate::command_policy::{CommandPolicyError, ExecutionDirectory, PinnedExecutable};
use crate::root_capability::{DirectoryIdentityDomain, DurableDirectoryIdentityV1};
use crate::{DirectoryIdentityError, DirectoryIdentityMarker, RelativePath, RootCapability};

use super::super::{WorktreeError, validated_directory};

pub(crate) struct RetainedDirectory {
    pub(crate) execution: Arc<ExecutionDirectory>,
    pub(crate) capability: RootCapability,
    pub(super) marker: DirectoryIdentityMarker,
}

impl RetainedDirectory {
    pub(super) fn from_existing(
        execution: Arc<ExecutionDirectory>,
        capability: RootCapability,
        marker: DirectoryIdentityMarker,
        mismatch: WorktreeError,
    ) -> Result<Self, WorktreeError> {
        capability.require_identity(marker).map_err(|_| mismatch)?;
        require_execution_identity(&execution, marker, WorktreeError::LinkedMetadataInvalid)?;
        Ok(Self {
            execution,
            capability,
            marker,
        })
    }

    pub(super) fn open_child(
        parent: &RootCapability,
        relative: &RelativePath,
        namespace_path: &Path,
    ) -> Result<Self, WorktreeError> {
        let directory = parent.open_directory(relative).map_err(WorktreeError::Io)?;
        let capability =
            RootCapability::from_authenticated_directory(directory).map_err(WorktreeError::Io)?;
        let marker = capability
            .identity_marker()
            .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
        let (_, execution) = validated_directory(namespace_path)?;
        Self::from_existing(
            Arc::new(execution),
            capability,
            marker,
            WorktreeError::LinkedMetadataInvalid,
        )
    }

    pub(super) fn revalidate(&self) -> Result<(), WorktreeError> {
        self.capability
            .require_identity(self.marker)
            .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
        require_execution_identity(
            &self.execution,
            self.marker,
            WorktreeError::LinkedMetadataInvalid,
        )
    }
}

pub(super) fn require_child_identity(
    parent: &RootCapability,
    relative: &RelativePath,
    expected: DirectoryIdentityMarker,
) -> Result<(), WorktreeError> {
    let child = parent.open_directory(relative).map_err(WorktreeError::Io)?;
    RootCapability::from_authenticated_directory(child)
        .map_err(WorktreeError::Io)?
        .require_identity(expected)
        .map_err(|_| WorktreeError::LinkedMetadataInvalid)
}

pub(super) fn require_execution_identity(
    execution: &ExecutionDirectory,
    expected: DirectoryIdentityMarker,
    mismatch: WorktreeError,
) -> Result<(), WorktreeError> {
    execution
        .revalidate()
        .map_err(WorktreeError::CommandPolicy)?;
    let capability = execution
        .cloned_root_capability()
        .map_err(WorktreeError::CommandPolicy)?;
    capability.require_identity(expected).map_err(|_| mismatch)
}

pub(super) fn reopen_execution_directory(
    path: &Path,
    expected: DirectoryIdentityMarker,
    mismatch: WorktreeError,
) -> Result<Arc<ExecutionDirectory>, WorktreeError> {
    let (_, execution) = validated_directory(path)?;
    require_execution_identity(&execution, expected, mismatch)?;
    Ok(Arc::new(execution))
}

pub(super) fn require_exact_git(
    provisioner_git: &Arc<PinnedExecutable>,
    expected_git: &Arc<PinnedExecutable>,
) -> Result<(), WorktreeError> {
    if !Arc::ptr_eq(provisioner_git, expected_git) {
        return Err(WorktreeError::CommandPolicy(
            CommandPolicyError::InvalidGitBinding,
        ));
    }
    expected_git
        .revalidate()
        .map_err(WorktreeError::CommandPolicy)
}

pub(super) fn require_durable_identity(
    capability: &RootCapability,
    domain: DirectoryIdentityDomain,
    expected: &DurableDirectoryIdentityV1,
) -> Result<(), WorktreeError> {
    let current = DurableDirectoryIdentityV1::derive(capability, domain)
        .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
    if current == *expected {
        Ok(())
    } else {
        Err(WorktreeError::LinkedMetadataInvalid)
    }
}

pub(super) fn map_common_identity_error(error: DirectoryIdentityError) -> WorktreeError {
    match error {
        DirectoryIdentityError::Unavailable => WorktreeError::CommonGitIdentityUnavailable,
        DirectoryIdentityError::Mismatch => WorktreeError::CommonGitIdentityMismatch,
    }
}
