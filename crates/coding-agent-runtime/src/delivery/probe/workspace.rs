use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{self, Read as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::RelativePath;
use crate::command_policy::{DeliveryGitEmptyConfig, ExecutionDirectory};
use crate::native_fs::{
    child_entry_exists, open_child_file, read_directory_names, reopen_directory,
};
#[cfg(unix)]
use crate::native_fs::{create_private_child_file_exclusive, remove_child_file};
use crate::root_capability::{DirectoryPathGuard, directory_identity_marker, ensure_plain_file};

use super::DeliveryGitProbeError;

#[cfg(windows)]
mod windows;

#[cfg(windows)]
use windows::CleanupPhase;

const MAX_ALLOCATION_ATTEMPTS: usize = 32;
const MAX_CLEANUP_DEPTH: usize = 64;
const MAX_CLEANUP_ENTRIES: usize = 16_384;
#[cfg(unix)]
const EMPTY_CONFIG_NAME: &str = ".coding-agent-empty-gitconfig";
const GIT_DIRECTORY_NAME: &str = ".git";
const PROBE_WORKTREE_FILE_NAME: &str = "probe.txt";
const PROBE_WORKTREE_FILE_CONTENTS: &[u8] = b"P4-B delivery probe\n";

/// One transient, application-private Git repository used only for the
/// pre-database capability probe.
///
/// The parent is prepared by the application's owner-only runtime-directory
/// lifecycle. This type still authenticates its exact child before cleanup so
/// normal external drift is fail-closed instead of being mistaken for an
/// application-owned probe workspace.
pub(super) struct ProbeWorkspace {
    parent: Arc<ExecutionDirectory>,
    name: String,
    path: PathBuf,
    directory: Option<Arc<ExecutionDirectory>>,
    guard: Option<DirectoryPathGuard>,
    #[cfg(unix)]
    config_file: Option<File>,
}

impl ProbeWorkspace {
    pub(super) fn create(parent: Arc<ExecutionDirectory>) -> Result<Self, DeliveryGitProbeError> {
        parent
            .revalidate()
            .map_err(|_| DeliveryGitProbeError::InvalidConfiguration)?;
        let root = parent
            .cloned_root_capability()
            .map_err(|_| DeliveryGitProbeError::InvalidConfiguration)?;

        for _ in 0..MAX_ALLOCATION_ATTEMPTS {
            let name = random_workspace_name()?;
            let relative = RelativePath::parse(name.clone())
                .map_err(|_| DeliveryGitProbeError::InvalidConfiguration)?;
            match root.open_directory(&relative) {
                Ok(_) => continue,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(_) => return Err(DeliveryGitProbeError::InvalidConfiguration),
            }

            let guard = root
                .ensure_directory_path(&relative)
                .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
            let path = parent.path().join(&name);
            let directory = Arc::new(
                ExecutionDirectory::open(&path)
                    .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?,
            );
            parent
                .revalidate()
                .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
            require_same_directory(&guard, &directory)?;

            let mut workspace = Self {
                parent: Arc::clone(&parent),
                name,
                path,
                directory: Some(directory),
                guard: Some(guard),
                #[cfg(unix)]
                config_file: None,
            };
            if let Err(error) = workspace.initialize_git_sandbox() {
                workspace.cleanup()?;
                return Err(error);
            }
            return Ok(workspace);
        }

        Err(DeliveryGitProbeError::InvalidConfiguration)
    }

    pub(super) fn directory(&self) -> Arc<ExecutionDirectory> {
        Arc::clone(
            self.directory
                .as_ref()
                .expect("probe workspace directory is retained until cleanup"),
        )
    }

    /// Returns the typed empty configuration authority rather than a
    /// namespace path. The process supervisor materializes the retained file
    /// for Unix children; Windows binds Git to the fixed `NUL` endpoint.
    pub(super) fn git_sandbox(&self) -> Result<Arc<DeliveryGitEmptyConfig>, DeliveryGitProbeError> {
        self.empty_config_authority()
    }

    pub(super) fn cleanup(mut self) -> Result<(), DeliveryGitProbeError> {
        #[cfg(unix)]
        {
            self.validate_cleanup_target()?;
            self.cleanup_unix()
        }
        #[cfg(windows)]
        self.cleanup_inner(&mut |_| {})
    }

    #[cfg(all(test, windows))]
    fn cleanup_with_hook(
        mut self,
        mut hook: impl FnMut(CleanupPhase),
    ) -> Result<(), DeliveryGitProbeError> {
        self.cleanup_inner(&mut hook)
    }

    #[cfg(windows)]
    fn cleanup_inner(
        &mut self,
        hook: &mut dyn FnMut(CleanupPhase),
    ) -> Result<(), DeliveryGitProbeError> {
        self.validate_cleanup_target()?;
        self.cleanup_windows(hook)
    }

    #[cfg(unix)]
    fn cleanup_unix(&mut self) -> Result<(), DeliveryGitProbeError> {
        prepare_tree_for_removal(&self.path)?;
        drop(self.guard.take());
        drop(self.config_file.take());
        drop(self.directory.take());

        std::fs::remove_dir_all(&self.path).map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
        self.parent
            .revalidate()
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
        let parent_root = self
            .parent
            .cloned_root_capability()
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
        let parent = parent_root
            .try_clone_root()
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
        require_child_absent(&parent, OsStr::new(&self.name))
    }

    fn initialize_git_sandbox(&mut self) -> Result<(), DeliveryGitProbeError> {
        #[cfg(unix)]
        {
            return self.create_empty_config();
        }
        #[cfg(windows)]
        Ok(())
    }

    #[cfg(unix)]
    fn create_empty_config(&mut self) -> Result<(), DeliveryGitProbeError> {
        let directory = self
            .directory
            .as_ref()
            .ok_or(DeliveryGitProbeError::CleanupUnproven)?;
        let root = directory
            .cloned_root_capability()
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
        let parent = root
            .try_clone_root()
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
        let config = create_private_child_file_exclusive(&parent, OsStr::new(EMPTY_CONFIG_NAME))
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
        config
            .sync_all()
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
        let retained = config;
        require_empty_plain_file(&retained)?;
        remove_child_file(&parent, OsStr::new(EMPTY_CONFIG_NAME), &retained)
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
        if child_entry_exists(&parent, OsStr::new(EMPTY_CONFIG_NAME))
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?
        {
            return Err(DeliveryGitProbeError::CleanupUnproven);
        }
        self.config_file = Some(retained);
        self.empty_config_authority()?;
        Ok(())
    }

    #[cfg(unix)]
    fn empty_config_authority(&self) -> Result<Arc<DeliveryGitEmptyConfig>, DeliveryGitProbeError> {
        let directory = self.directory();
        let file = self
            .config_file
            .as_ref()
            .ok_or(DeliveryGitProbeError::CleanupUnproven)?
            .try_clone()
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
        DeliveryGitEmptyConfig::from_retained_sandbox_file(directory, file)
            .map(Arc::new)
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)
    }

    #[cfg(windows)]
    fn empty_config_authority(&self) -> Result<Arc<DeliveryGitEmptyConfig>, DeliveryGitProbeError> {
        Ok(Arc::new(DeliveryGitEmptyConfig::windows_nul()))
    }

    fn validate_cleanup_target(&self) -> Result<(), DeliveryGitProbeError> {
        if !is_direct_child(&self.path, self.parent.path(), &self.name) {
            return Err(DeliveryGitProbeError::CleanupUnproven);
        }
        if self.parent.revalidate().is_err() {
            return Err(DeliveryGitProbeError::CleanupUnproven);
        }
        let expected = self
            .directory
            .as_ref()
            .ok_or(DeliveryGitProbeError::CleanupUnproven)?;
        if expected.revalidate().is_err() {
            return Err(DeliveryGitProbeError::CleanupUnproven);
        }
        let parent_root = self
            .parent
            .cloned_root_capability()
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
        let relative = RelativePath::parse(self.name.clone())
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
        let named = parent_root
            .open_directory(&relative)
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
        let named_identity = directory_identity_marker(&named)
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
        let expected_identity = directory_identity(expected)?;
        if named_identity != expected_identity {
            return Err(DeliveryGitProbeError::CleanupUnproven);
        }

        #[cfg(unix)]
        {
            if self.config_file.is_some() {
                self.empty_config_authority()?;
            }
        }
        self.validate_workspace_entries()?;
        self.validate_probe_worktree_file()?;
        Ok(())
    }

    fn validate_workspace_entries(&self) -> Result<(), DeliveryGitProbeError> {
        let directory = self
            .directory
            .as_ref()
            .ok_or(DeliveryGitProbeError::CleanupUnproven)?;
        let capability = directory
            .cloned_root_capability()
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
        let root = capability
            .try_clone_root()
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
        let mut root =
            reopen_directory(&root).map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
        let names = read_directory_names(&mut root, 4)
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
        let allowed = self.allowed_workspace_entries();
        if names
            .iter()
            .all(|name| allowed.iter().any(|allowed| *allowed == name.as_os_str()))
        {
            Ok(())
        } else {
            Err(DeliveryGitProbeError::CleanupUnproven)
        }
    }

    fn allowed_workspace_entries(&self) -> BTreeSet<&OsStr> {
        BTreeSet::from([
            OsStr::new(GIT_DIRECTORY_NAME),
            OsStr::new(PROBE_WORKTREE_FILE_NAME),
        ])
    }

    fn validate_probe_worktree_file(&self) -> Result<(), DeliveryGitProbeError> {
        let directory = self
            .directory
            .as_ref()
            .ok_or(DeliveryGitProbeError::CleanupUnproven)?;
        let capability = directory
            .cloned_root_capability()
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
        let root = capability
            .try_clone_root()
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
        let mut file = match open_child_file(&root, OsStr::new(PROBE_WORKTREE_FILE_NAME)) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(_) => return Err(DeliveryGitProbeError::CleanupUnproven),
        };
        require_exact_file_contents(&mut file, PROBE_WORKTREE_FILE_CONTENTS)
    }
}

#[cfg(unix)]
fn require_empty_plain_file(file: &File) -> Result<(), DeliveryGitProbeError> {
    ensure_plain_file(file).map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
    if file
        .metadata()
        .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?
        .len()
        == 0
    {
        Ok(())
    } else {
        Err(DeliveryGitProbeError::CleanupUnproven)
    }
}

fn require_exact_file_contents(
    file: &mut File,
    expected: &[u8],
) -> Result<(), DeliveryGitProbeError> {
    ensure_plain_file(file).map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
    let mut contents = vec![0; expected.len()];
    file.read_exact(&mut contents)
        .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
    let mut trailing = [0u8; 1];
    if contents == expected && file.read(&mut trailing).is_ok_and(|count| count == 0) {
        Ok(())
    } else {
        Err(DeliveryGitProbeError::CleanupUnproven)
    }
}

fn require_child_absent(parent: &File, name: &OsStr) -> Result<(), DeliveryGitProbeError> {
    if child_entry_exists(parent, name).map_err(|_| DeliveryGitProbeError::CleanupUnproven)? {
        Err(DeliveryGitProbeError::CleanupUnproven)
    } else {
        Ok(())
    }
}

fn require_same_directory(
    guard: &DirectoryPathGuard,
    directory: &ExecutionDirectory,
) -> Result<(), DeliveryGitProbeError> {
    let guarded = guard
        .try_clone_final()
        .and_then(|file| directory_identity_marker(&file).map_err(io::Error::other))
        .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
    if guarded == directory_identity(directory)? {
        Ok(())
    } else {
        Err(DeliveryGitProbeError::CleanupUnproven)
    }
}

fn directory_identity(
    directory: &ExecutionDirectory,
) -> Result<crate::DirectoryIdentityMarker, DeliveryGitProbeError> {
    directory
        .cloned_root_capability()
        .and_then(|root| {
            root.identity_marker()
                .map_err(|error| crate::CommandPolicyError::OpenFailed(io::Error::other(error)))
        })
        .map_err(|_| DeliveryGitProbeError::CleanupUnproven)
}

fn random_workspace_name() -> Result<String, DeliveryGitProbeError> {
    let mut random = [0u8; 16];
    getrandom::fill(&mut random).map_err(|_| DeliveryGitProbeError::InvalidConfiguration)?;
    let mut name = String::from(".coding-agent-delivery-probe-");
    for byte in random {
        use std::fmt::Write as _;

        write!(&mut name, "{byte:02x}").expect("writing hexadecimal bytes to String cannot fail");
    }
    Ok(name)
}

fn is_direct_child(path: &Path, parent: &Path, name: &str) -> bool {
    path.is_absolute()
        && parent.is_absolute()
        && path.parent() == Some(parent)
        && path.file_name() == Some(OsStr::new(name))
        && !name.contains(['/', '\\'])
}

#[cfg(unix)]
fn prepare_tree_for_removal(path: &Path) -> Result<(), DeliveryGitProbeError> {
    let mut entries = 0usize;
    prepare_tree_component(path, 0, &mut entries)
}

#[cfg(unix)]
fn prepare_tree_component(
    path: &Path,
    depth: usize,
    entries: &mut usize,
) -> Result<(), DeliveryGitProbeError> {
    if depth > MAX_CLEANUP_DEPTH || *entries >= MAX_CLEANUP_ENTRIES {
        return Err(DeliveryGitProbeError::CleanupUnproven);
    }
    let metadata =
        std::fs::symlink_metadata(path).map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
    if metadata.file_type().is_symlink() {
        return Err(DeliveryGitProbeError::CleanupUnproven);
    }
    if metadata.is_dir() {
        for entry in std::fs::read_dir(path).map_err(|_| DeliveryGitProbeError::CleanupUnproven)? {
            let entry = entry.map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
            *entries += 1;
            prepare_tree_component(&entry.path(), depth + 1, entries)?;
        }
    } else if !metadata.is_file() {
        return Err(DeliveryGitProbeError::CleanupUnproven);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn git_sandbox_uses_a_retained_empty_config() {
        let fixture = WorkspaceFixture::new();
        let workspace = ProbeWorkspace::create(Arc::clone(&fixture.parent)).unwrap();
        let config = workspace.git_sandbox().unwrap();

        config.revalidate().unwrap();
        assert_eq!(format!("{config:?}"), "DeliveryGitEmptyConfig(<opaque>)");
        #[cfg(unix)]
        assert!(!workspace.path.join(EMPTY_CONFIG_NAME).exists());
        #[cfg(windows)]
        assert!(std::fs::read_dir(&workspace.path).unwrap().next().is_none());

        drop(config);
        workspace.cleanup().unwrap();
        fixture.assert_empty();
    }

    #[test]
    fn cleanup_rejects_a_foreign_reparse_entry_without_touching_its_target() {
        let fixture = WorkspaceFixture::new();
        let workspace = ProbeWorkspace::create(Arc::clone(&fixture.parent)).unwrap();
        let target = fixture.root.join("outside-target");
        std::fs::write(&target, b"foreign").unwrap();
        let link = workspace.path.join("foreign-link");
        if create_file_symlink(&target, &link).is_err() {
            workspace.cleanup().unwrap();
            std::fs::remove_file(target).unwrap();
            return;
        }

        assert_eq!(
            workspace.cleanup().unwrap_err(),
            DeliveryGitProbeError::CleanupUnproven
        );
        assert_eq!(std::fs::read(&target).unwrap(), b"foreign");
        std::fs::remove_file(link).unwrap();
        std::fs::remove_file(target).unwrap();
    }

    #[test]
    fn cleanup_never_deletes_a_replacement_at_the_probe_name() {
        let fixture = WorkspaceFixture::new();
        let workspace = ProbeWorkspace::create(Arc::clone(&fixture.parent)).unwrap();
        let original = workspace.path.clone();
        let moved = fixture.root.join("moved-original");

        if std::fs::rename(&original, &moved).is_err() {
            workspace.cleanup().unwrap();
            fixture.assert_empty();
            return;
        }
        std::fs::create_dir(&original).unwrap();
        std::fs::write(original.join("foreign"), b"keep").unwrap();

        assert_eq!(
            workspace.cleanup().unwrap_err(),
            DeliveryGitProbeError::CleanupUnproven
        );
        assert_eq!(std::fs::read(original.join("foreign")).unwrap(), b"keep");
        std::fs::remove_dir_all(original).unwrap();
        std::fs::remove_dir_all(moved).unwrap();
        fixture.assert_empty();
    }

    struct WorkspaceFixture {
        _temporary: tempfile::TempDir,
        root: PathBuf,
        parent: Arc<ExecutionDirectory>,
    }

    impl WorkspaceFixture {
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

    #[cfg(unix)]
    fn create_file_symlink(target: &Path, link: &Path) -> io::Result<()> {
        std::os::unix::fs::symlink(target, link)
    }

    #[cfg(windows)]
    fn create_file_symlink(target: &Path, link: &Path) -> io::Result<()> {
        std::os::windows::fs::symlink_file(target, link)
    }
}
