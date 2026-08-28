use std::fmt;
#[cfg(unix)]
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use crate::command_policy::{DeliveryGitEmptyConfig, ExecutionDirectory};
use crate::root_capability::DirectoryPathGuard;

use super::DeliverySourceError;

mod cleanup;
mod creation;
mod validation;

const MAX_ALLOCATION_ATTEMPTS: usize = 32;
#[cfg(unix)]
const EMPTY_CONFIG_NAME: &str = ".coding-agent-empty-gitconfig";

/// Retained, application-private environment for every delivery Git child.
///
/// Path-bearing accessors are private to the delivery module. They remain
/// usable only while this value retains all authenticated filesystem guards.
pub(super) struct DeliveryCommandSandbox {
    parent: Arc<ExecutionDirectory>,
    name: String,
    path: PathBuf,
    workspace_directory: Option<Arc<ExecutionDirectory>>,
    workspace_guard: Option<DirectoryPathGuard>,
    #[cfg(unix)]
    config_file: Option<File>,
    stage: SandboxStage,
    cleaned: bool,
}

impl DeliveryCommandSandbox {
    pub(super) fn workspace_directory(&self) -> Arc<ExecutionDirectory> {
        Arc::clone(
            self.workspace_directory
                .as_ref()
                .expect("a live delivery sandbox retains its workspace directory"),
        )
    }

    /// Creates the platform-specific typed empty-config authority admitted
    /// for delivery Git children.
    pub(super) fn empty_config_authority(
        &self,
    ) -> Result<Arc<DeliveryGitEmptyConfig>, DeliverySourceError> {
        self.revalidate()?;
        #[cfg(unix)]
        {
            let file = self
                .config_file
                .as_ref()
                .ok_or(DeliverySourceError::SandboxUnavailable)?
                .try_clone()
                .map_err(|_| DeliverySourceError::SandboxUnavailable)?;
            DeliveryGitEmptyConfig::from_retained_sandbox_file(self.workspace_directory(), file)
                .map(Arc::new)
                .map_err(Into::into)
        }
        #[cfg(windows)]
        {
            Ok(Arc::new(DeliveryGitEmptyConfig::windows_nul()))
        }
    }

    pub(super) fn revalidate(&self) -> Result<(), DeliverySourceError> {
        if self.stage != SandboxStage::Ready || self.cleaned {
            return Err(DeliverySourceError::SandboxUnavailable);
        }
        self.validate_retained_state()
    }

    #[cfg(test)]
    pub(super) fn cleanup(mut self) -> Result<(), DeliverySourceError> {
        self.cleanup_inner()
    }

    #[cfg(test)]
    fn cleanup_with_hook(
        mut self,
        mut hook: impl FnMut(cleanup::CleanupPhase),
    ) -> Result<(), DeliverySourceError> {
        self.cleanup_inner_with_hook(&mut hook)
    }

    #[cfg(unix)]
    fn workspace_root(&self) -> Result<crate::RootCapability, DeliverySourceError> {
        self.workspace_directory
            .as_ref()
            .ok_or(DeliverySourceError::SandboxUnavailable)?
            .cloned_root_capability()
            .map_err(|_| DeliverySourceError::SandboxUnavailable)
    }
}

impl fmt::Debug for DeliveryCommandSandbox {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryCommandSandbox(<opaque>)")
    }
}

impl Drop for DeliveryCommandSandbox {
    fn drop(&mut self) {
        let _ = self.cleanup_inner();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SandboxStage {
    Workspace,
    #[cfg(unix)]
    Config,
    Ready,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sandbox_uses_an_opaque_empty_config_authority_without_a_config_entry() {
        let fixture = SandboxFixture::new();
        let sandbox = DeliveryCommandSandbox::create(Arc::clone(&fixture.parent)).unwrap();

        assert_eq!(format!("{sandbox:?}"), "DeliveryCommandSandbox(<opaque>)");
        assert!(!format!("{sandbox:?}").contains(&fixture.root.to_string_lossy().to_string()));
        assert!(!sandbox.path.join(".coding-agent-empty-gitconfig").exists());
        sandbox.revalidate().unwrap();
        sandbox.cleanup().unwrap();
        fixture.assert_empty();
    }

    #[test]
    #[cfg(windows)]
    fn windows_empty_config_authority_is_opaque_and_uses_nul() {
        let fixture = SandboxFixture::new();
        let sandbox = DeliveryCommandSandbox::create(Arc::clone(&fixture.parent)).unwrap();
        let authority = sandbox.empty_config_authority().unwrap();

        assert_eq!(format!("{authority:?}"), "DeliveryGitEmptyConfig(<opaque>)");
        assert!(!format!("{authority:?}").contains(&fixture.root.to_string_lossy().to_string()));

        let mut entries = std::collections::BTreeMap::new();
        authority
            .apply_delivery_git_environment(&mut entries)
            .unwrap();
        assert_eq!(
            entries
                .get(std::ffi::OsStr::new("GIT_CONFIG_GLOBAL"))
                .map(std::ffi::OsString::as_os_str),
            Some(std::ffi::OsStr::new("NUL"))
        );
        assert!(!sandbox.path.join(".coding-agent-empty-gitconfig").exists());
        authority.revalidate().unwrap();
        drop(authority);
        sandbox.cleanup().unwrap();
        fixture.assert_empty();
    }

    #[cfg(unix)]
    #[test]
    fn unix_empty_config_is_nameless_and_remains_typed() {
        let fixture = SandboxFixture::new();
        let sandbox = DeliveryCommandSandbox::create(Arc::clone(&fixture.parent)).unwrap();
        let authority = sandbox.empty_config_authority().unwrap();

        assert!(!sandbox.path.join(EMPTY_CONFIG_NAME).exists());
        authority.revalidate().unwrap();
        sandbox.revalidate().unwrap();

        drop(authority);
        sandbox.cleanup().unwrap();
        fixture.assert_empty();
    }

    #[test]
    fn cleanup_never_deletes_a_replacement_at_the_sandbox_name() {
        let fixture = SandboxFixture::new();
        let sandbox = DeliveryCommandSandbox::create(Arc::clone(&fixture.parent)).unwrap();
        let original = sandbox.path.clone();
        let moved = fixture.root.join("moved-original");

        if std::fs::rename(&original, &moved).is_err() {
            sandbox.cleanup().unwrap();
            fixture.assert_empty();
            return;
        }
        std::fs::create_dir(&original).unwrap();
        std::fs::write(original.join("foreign"), b"keep").unwrap();

        assert!(matches!(
            sandbox.cleanup(),
            Err(DeliverySourceError::SandboxCleanupUnproven)
        ));
        assert_eq!(std::fs::read(original.join("foreign")).unwrap(), b"keep");
        std::fs::remove_dir_all(original).unwrap();
        std::fs::remove_dir_all(moved).unwrap();
        fixture.assert_empty();
    }

    #[test]
    fn cleanup_boundary_swap_never_deletes_a_foreign_replacement() {
        use std::cell::Cell;

        let fixture = SandboxFixture::new();
        let sandbox = DeliveryCommandSandbox::create(Arc::clone(&fixture.parent)).unwrap();
        let original = sandbox.path.clone();
        let moved = fixture.root.join("moved-original-at-cleanup-boundary");
        let foreign_marker = original.join("foreign-marker");
        let swapped = Cell::new(false);

        let result = sandbox.cleanup_with_hook(|phase| {
            if phase != cleanup::CleanupPhase::BeforeNamespaceRemoval {
                return;
            }
            if std::fs::rename(&original, &moved).is_ok() {
                std::fs::create_dir(&original).unwrap();
                std::fs::write(&foreign_marker, b"keep").unwrap();
                swapped.set(true);
            }
        });

        if swapped.get() {
            assert!(matches!(
                result,
                Err(DeliverySourceError::SandboxCleanupUnproven)
            ));
            assert_eq!(std::fs::read(&foreign_marker).unwrap(), b"keep");
            std::fs::remove_dir_all(original).unwrap();
            std::fs::remove_dir_all(moved).unwrap();
        } else {
            result.unwrap();
        }
        fixture.assert_empty();
    }

    #[test]
    fn drop_best_effort_cleans_an_unchanged_sandbox() {
        let fixture = SandboxFixture::new();
        {
            let sandbox = DeliveryCommandSandbox::create(Arc::clone(&fixture.parent)).unwrap();
            sandbox.revalidate().unwrap();
        }
        fixture.assert_empty();
    }

    struct SandboxFixture {
        _temporary: tempfile::TempDir,
        root: PathBuf,
        parent: Arc<ExecutionDirectory>,
    }

    impl SandboxFixture {
        fn new() -> Self {
            let temporary = tempfile::tempdir().unwrap();
            let root = temporary.path().canonicalize().unwrap();
            let parent = Arc::new(ExecutionDirectory::open(&root).unwrap());
            Self {
                _temporary: temporary,
                root,
                parent,
            }
        }

        fn assert_empty(&self) {
            assert_eq!(std::fs::read_dir(&self.root).unwrap().count(), 0);
        }
    }
}
