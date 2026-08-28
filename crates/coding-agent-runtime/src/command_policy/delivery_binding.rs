use std::ffi::{OsStr, OsString};
use std::fmt;
#[cfg(unix)]
use std::fs::File;
#[cfg(unix)]
use std::sync::Arc;

use crate::process_supervisor::ChildEnvironment;
#[cfg(unix)]
use crate::root_capability::ensure_plain_file;

use super::CommandPolicyError;
#[cfg(unix)]
use super::{ExecutionDirectory, ValidatedCommand};

/// Typed authority for the only external Git configuration endpoint admitted
/// to a delivery child.
///
/// Unix retains an empty file descriptor after unlinking its namespace entry,
/// so the inherited descriptor is the only remaining route to that config.
/// Windows uses the immutable `NUL` device endpoint instead of a workspace
/// file: Git can open it normally and no mutable private namespace is bound.
pub(crate) struct DeliveryGitEmptyConfig {
    #[cfg(unix)]
    sandbox: Arc<ExecutionDirectory>,
    #[cfg(unix)]
    file: File,
}

impl DeliveryGitEmptyConfig {
    #[cfg(unix)]
    const UNIX_ENVIRONMENT_SENTINEL: &'static str = "<coding-agent-delivery-empty-config>";

    #[cfg(windows)]
    const WINDOWS_NUL_ENDPOINT: &'static str = "NUL";

    #[cfg(unix)]
    pub(crate) fn from_retained_sandbox_file(
        sandbox: Arc<ExecutionDirectory>,
        file: File,
    ) -> Result<Self, CommandPolicyError> {
        sandbox.revalidate()?;
        // The source/probe sandbox creates and unlinks this private file
        // before passing a duplicate of its original descriptor. Reopening a
        // path here would reintroduce a namespace race, so retain the exact
        // descriptor-only authority instead.
        let authority = Self { sandbox, file };
        authority.revalidate()?;
        Ok(authority)
    }

    #[cfg(windows)]
    pub(crate) const fn windows_nul() -> Self {
        Self {}
    }

    pub(crate) fn revalidate(&self) -> Result<(), CommandPolicyError> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;

            self.sandbox.revalidate()?;
            ensure_plain_file(&self.file).map_err(CommandPolicyError::OpenFailed)?;
            let metadata = self
                .file
                .metadata()
                .map_err(CommandPolicyError::OpenFailed)?;
            if metadata.len() == 0 && metadata.nlink() == 0 {
                Ok(())
            } else {
                Err(CommandPolicyError::InvalidGitBinding)
            }
        }
        #[cfg(windows)]
        {
            Ok(())
        }
    }

    pub(crate) fn apply_delivery_git_environment(
        &self,
        entries: &mut std::collections::BTreeMap<OsString, OsString>,
    ) -> Result<(), CommandPolicyError> {
        self.revalidate()?;
        #[cfg(unix)]
        let value = OsString::from(Self::UNIX_ENVIRONMENT_SENTINEL);
        #[cfg(windows)]
        let value = OsString::from(Self::WINDOWS_NUL_ENDPOINT);
        for key in ["GIT_CONFIG_GLOBAL", "GIT_CONFIG_SYSTEM"] {
            entries.insert(OsString::from(key), value.clone());
        }
        Ok(())
    }

    pub(super) fn validates_delivery_git_environment(
        &self,
        environment: &ChildEnvironment,
    ) -> Result<(), CommandPolicyError> {
        self.revalidate()?;
        #[cfg(unix)]
        let expected = OsStr::new(Self::UNIX_ENVIRONMENT_SENTINEL);
        #[cfg(windows)]
        let expected = OsStr::new(Self::WINDOWS_NUL_ENDPOINT);
        for key in ["GIT_CONFIG_GLOBAL", "GIT_CONFIG_SYSTEM"] {
            let actual = environment
                .entries()
                .get(OsStr::new(key))
                .map(OsString::as_os_str);
            if actual != Some(expected) {
                return Err(CommandPolicyError::InvalidGitBinding);
            }
        }
        Ok(())
    }

    #[cfg(unix)]
    pub(crate) fn cloned_file(&self) -> Result<File, CommandPolicyError> {
        self.file
            .try_clone()
            .map_err(CommandPolicyError::OpenFailed)
    }
}

impl fmt::Debug for DeliveryGitEmptyConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryGitEmptyConfig(<opaque>)")
    }
}

#[cfg(unix)]
pub(super) const DELIVERY_GIT_DIRECTORY_ARGUMENT_INDEX: usize = 5;
#[cfg(unix)]
pub(super) const DELIVERY_WORK_TREE_ARGUMENT_INDEX: usize = 6;
#[cfg(unix)]
pub(super) const DELIVERY_GIT_DIRECTORY_SENTINEL: &str =
    "--git-dir=<coding-agent-delivery-directory>";
#[cfg(unix)]
pub(super) const DELIVERY_WORK_TREE_SENTINEL: &str =
    "--work-tree=<coding-agent-delivery-working-directory>";

/// Fixed sentinel for the one private child allowed to host a temporary
/// delivery index. The process supervisor replaces it with a retained
/// directory-descriptor path immediately before spawning the child.
#[cfg(unix)]
pub(crate) const DELIVERY_GIT_TEMPORARY_INDEX_SENTINEL: &str =
    "<coding-agent-delivery-temporary-index>";

#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnixDeliveryDirectoryRole {
    GitDirectory { argument_index: usize },
    WorkTree { argument_index: usize },
    CommonGitEnvironment,
    TemporaryIndexEnvironment,
}

#[cfg(unix)]
#[derive(Debug, Clone)]
pub(crate) struct UnixDeliveryDirectoryBinding {
    role: UnixDeliveryDirectoryRole,
    directory: Arc<ExecutionDirectory>,
}

#[cfg(unix)]
impl UnixDeliveryDirectoryBinding {
    pub(crate) const fn role(&self) -> UnixDeliveryDirectoryRole {
        self.role
    }

    pub(crate) const fn directory(&self) -> &Arc<ExecutionDirectory> {
        &self.directory
    }
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnixDeliveryBindingProfile {
    Repository,
    RepositoryWithTemporaryIndex,
}

#[cfg(unix)]
#[derive(Debug, Clone)]
pub(crate) struct UnixDeliveryDirectoryBindings {
    profile: UnixDeliveryBindingProfile,
    bindings: Vec<UnixDeliveryDirectoryBinding>,
}

#[cfg(unix)]
impl UnixDeliveryDirectoryBindings {
    pub(super) fn repository(
        git_directory: Arc<ExecutionDirectory>,
        work_tree: Arc<ExecutionDirectory>,
        common_git: Arc<ExecutionDirectory>,
    ) -> Self {
        Self {
            profile: UnixDeliveryBindingProfile::Repository,
            bindings: vec![
                UnixDeliveryDirectoryBinding {
                    role: UnixDeliveryDirectoryRole::GitDirectory {
                        argument_index: DELIVERY_GIT_DIRECTORY_ARGUMENT_INDEX,
                    },
                    directory: git_directory,
                },
                UnixDeliveryDirectoryBinding {
                    role: UnixDeliveryDirectoryRole::WorkTree {
                        argument_index: DELIVERY_WORK_TREE_ARGUMENT_INDEX,
                    },
                    directory: work_tree,
                },
                UnixDeliveryDirectoryBinding {
                    role: UnixDeliveryDirectoryRole::CommonGitEnvironment,
                    directory: common_git,
                },
            ],
        }
    }

    pub(super) fn repository_with_temporary_index(
        git_directory: Arc<ExecutionDirectory>,
        work_tree: Arc<ExecutionDirectory>,
        common_git: Arc<ExecutionDirectory>,
        temporary_index: Arc<ExecutionDirectory>,
    ) -> Self {
        let mut bindings = Self::repository(git_directory, work_tree, common_git).bindings;
        bindings.push(UnixDeliveryDirectoryBinding {
            role: UnixDeliveryDirectoryRole::TemporaryIndexEnvironment,
            directory: temporary_index,
        });
        Self {
            profile: UnixDeliveryBindingProfile::RepositoryWithTemporaryIndex,
            bindings,
        }
    }

    pub(crate) fn bindings(&self) -> &[UnixDeliveryDirectoryBinding] {
        &self.bindings
    }

    pub(super) fn validate(&self, command: &ValidatedCommand) -> Result<(), CommandPolicyError> {
        let expected_roles = match self.profile {
            UnixDeliveryBindingProfile::Repository => 3,
            UnixDeliveryBindingProfile::RepositoryWithTemporaryIndex => 4,
        };
        if self.bindings.len() != expected_roles {
            return Err(CommandPolicyError::InvalidGitBinding);
        }
        for binding in &self.bindings {
            binding.directory.revalidate()?;
            if !command
                .dependent_directories
                .iter()
                .any(|directory| Arc::ptr_eq(directory, &binding.directory))
            {
                return Err(CommandPolicyError::InvalidGitBinding);
            }
            match binding.role {
                UnixDeliveryDirectoryRole::GitDirectory { argument_index } => {
                    require_descriptor_sentinel(
                        &command.arguments,
                        argument_index,
                        DELIVERY_GIT_DIRECTORY_SENTINEL,
                    )?;
                }
                UnixDeliveryDirectoryRole::WorkTree { argument_index } => {
                    if !Arc::ptr_eq(&binding.directory, &command.working_directory) {
                        return Err(CommandPolicyError::InvalidGitBinding);
                    }
                    require_descriptor_sentinel(
                        &command.arguments,
                        argument_index,
                        DELIVERY_WORK_TREE_SENTINEL,
                    )?;
                }
                UnixDeliveryDirectoryRole::CommonGitEnvironment => {
                    if command
                        .environment
                        .entries()
                        .contains_key(OsStr::new("GIT_COMMON_DIR"))
                    {
                        return Err(CommandPolicyError::InvalidGitBinding);
                    }
                }
                UnixDeliveryDirectoryRole::TemporaryIndexEnvironment => {
                    let index_file = command
                        .environment
                        .entries()
                        .get(OsStr::new("GIT_INDEX_FILE"))
                        .map(OsString::as_os_str);
                    if index_file != Some(OsStr::new(DELIVERY_GIT_TEMPORARY_INDEX_SENTINEL)) {
                        return Err(CommandPolicyError::InvalidGitBinding);
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(unix)]
fn require_descriptor_sentinel(
    arguments: &[OsString],
    index: usize,
    expected: &str,
) -> Result<(), CommandPolicyError> {
    if arguments.get(index).is_some_and(|value| value == expected) {
        Ok(())
    } else {
        Err(CommandPolicyError::InvalidGitBinding)
    }
}
