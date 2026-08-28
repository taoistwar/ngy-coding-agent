use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fs::File;
use std::io;
use std::path::Path;

use crate::RelativePath;
use crate::command_policy::ExecutionDirectory;
use crate::native_fs::read_directory_names;
#[cfg(unix)]
use crate::root_capability::ensure_plain_file;
use crate::root_capability::{DirectoryPathGuard, directory_identity_marker};

use super::{DeliveryCommandSandbox, DeliverySourceError};

impl DeliveryCommandSandbox {
    pub(super) fn validate_retained_state(&self) -> Result<(), DeliverySourceError> {
        self.parent
            .revalidate()
            .map_err(|_| DeliverySourceError::SandboxUnavailable)?;
        self.validate_workspace_identity()?;
        #[cfg(unix)]
        self.validate_config()?;
        self.validate_workspace_entries()
    }

    pub(super) fn validate_workspace_identity(&self) -> Result<(), DeliverySourceError> {
        if !is_direct_child(&self.path, self.parent.path(), &self.name) {
            return Err(DeliverySourceError::SandboxUnavailable);
        }
        let directory = self
            .workspace_directory
            .as_ref()
            .ok_or(DeliverySourceError::SandboxUnavailable)?;
        let guard = self
            .workspace_guard
            .as_ref()
            .ok_or(DeliverySourceError::SandboxUnavailable)?;
        directory
            .revalidate()
            .map_err(|_| DeliverySourceError::SandboxUnavailable)?;
        require_guard_identity(guard, directory)?;

        let root = self
            .parent
            .cloned_root_capability()
            .map_err(|_| DeliverySourceError::SandboxUnavailable)?;
        let relative = RelativePath::parse(self.name.clone())
            .map_err(|_| DeliverySourceError::SandboxUnavailable)?;
        let named = root
            .open_directory(&relative)
            .map_err(|_| DeliverySourceError::SandboxUnavailable)?;
        if directory_identity_marker(&named).map_err(|_| DeliverySourceError::SandboxUnavailable)?
            != directory_identity(directory)?
        {
            return Err(DeliverySourceError::SandboxUnavailable);
        }
        Ok(())
    }

    #[cfg(unix)]
    pub(super) fn validate_config(&self) -> Result<(), DeliverySourceError> {
        let config = self
            .config_file
            .as_ref()
            .ok_or(DeliverySourceError::SandboxUnavailable)?;
        require_empty_plain_file(config)?;
        use std::os::unix::fs::MetadataExt as _;

        if config
            .metadata()
            .map_err(|_| DeliverySourceError::SandboxUnavailable)?
            .nlink()
            == 0
        {
            Ok(())
        } else {
            Err(DeliverySourceError::SandboxUnavailable)
        }
    }

    pub(super) fn validate_workspace_entries(&self) -> Result<(), DeliverySourceError> {
        let guard = self
            .workspace_guard
            .as_ref()
            .ok_or(DeliverySourceError::SandboxUnavailable)?;
        let mut handle = guard
            .try_clone_final()
            .map_err(|_| DeliverySourceError::SandboxUnavailable)?;
        let actual = read_directory_names(&mut handle, 3)
            .map_err(|_| DeliverySourceError::SandboxUnavailable)?
            .into_iter()
            .collect::<BTreeSet<_>>();
        if actual.is_empty() {
            Ok(())
        } else {
            Err(DeliverySourceError::SandboxUnavailable)
        }
    }
}

#[cfg(unix)]
pub(super) fn require_empty_plain_file(file: &File) -> Result<(), DeliverySourceError> {
    ensure_plain_file(file).map_err(|_| DeliverySourceError::SandboxUnavailable)?;
    if file
        .metadata()
        .map_err(|_| DeliverySourceError::SandboxUnavailable)?
        .len()
        == 0
    {
        Ok(())
    } else {
        Err(DeliverySourceError::SandboxUnavailable)
    }
}

pub(super) fn require_same_directory(
    created: &File,
    guard: &DirectoryPathGuard,
    directory: &ExecutionDirectory,
) -> Result<(), DeliverySourceError> {
    let created_identity = directory_identity_marker(created)
        .map_err(|_| DeliverySourceError::SandboxCleanupUnproven)?;
    let guarded_identity = guard
        .try_clone_final()
        .and_then(|file| directory_identity_marker(&file).map_err(io::Error::other))
        .map_err(|_| DeliverySourceError::SandboxCleanupUnproven)?;
    if created_identity == guarded_identity && guarded_identity == directory_identity(directory)? {
        Ok(())
    } else {
        Err(DeliverySourceError::SandboxCleanupUnproven)
    }
}

fn require_guard_identity(
    guard: &DirectoryPathGuard,
    directory: &ExecutionDirectory,
) -> Result<(), DeliverySourceError> {
    let guarded = guard
        .try_clone_final()
        .and_then(|file| directory_identity_marker(&file).map_err(io::Error::other))
        .map_err(|_| DeliverySourceError::SandboxUnavailable)?;
    if guarded == directory_identity(directory)? {
        Ok(())
    } else {
        Err(DeliverySourceError::SandboxUnavailable)
    }
}

fn directory_identity(
    directory: &ExecutionDirectory,
) -> Result<crate::DirectoryIdentityMarker, DeliverySourceError> {
    directory
        .cloned_root_capability()
        .and_then(|root| {
            root.identity_marker()
                .map_err(|error| crate::CommandPolicyError::OpenFailed(io::Error::other(error)))
        })
        .map_err(|_| DeliverySourceError::SandboxUnavailable)
}

pub(super) fn is_direct_child(path: &Path, parent: &Path, name: &str) -> bool {
    path.is_absolute()
        && parent.is_absolute()
        && path.parent() == Some(parent)
        && path.file_name() == Some(OsStr::new(name))
        && !name.contains(['/', '\\'])
}
