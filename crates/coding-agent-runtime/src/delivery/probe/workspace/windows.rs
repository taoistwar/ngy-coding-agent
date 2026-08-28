use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io;

use crate::native_fs::{
    child_directory_matches, child_file_matches, open_child_directory, open_child_file,
    quarantine_child_entry_no_replace, read_directory_names, remove_child_directory,
    remove_child_file, reopen_directory, reopen_directory_for_child_directory,
    reopen_directory_for_delete, reopen_file_for_delete,
};
use crate::root_capability::{
    directory_identity_marker, ensure_plain_directory, ensure_plain_file,
};

use super::{
    DeliveryGitProbeError, GIT_DIRECTORY_NAME, MAX_CLEANUP_DEPTH, MAX_CLEANUP_ENTRIES,
    PROBE_WORKTREE_FILE_NAME, ProbeWorkspace, require_child_absent,
};

#[cfg(windows)]
const MAX_QUARANTINE_ATTEMPTS: usize = 32;

#[cfg(windows)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CleanupPhase {
    BeforeNamespaceRemoval,
    AfterQuarantineSnapshot,
}

impl ProbeWorkspace {
    #[cfg(windows)]
    pub(super) fn cleanup_windows(
        &mut self,
        hook: &mut dyn FnMut(CleanupPhase),
    ) -> Result<(), DeliveryGitProbeError> {
        let retained_parent = self.cleanup_parent_handle()?;
        let retained_workspace = self.cleanup_workspace_handle()?;
        let observed_tree = observe_windows_workspace_tree(&retained_workspace)?;

        // The path guard blocks namespace replacement while Git uses the
        // workspace. After it is released, first move the retained workspace
        // to an unpredictable no-replace name. The public name is never used
        // for recursive cleanup.
        drop(self.guard.take());
        hook(CleanupPhase::BeforeNamespaceRemoval);
        let parent = reopen_directory_for_child_directory(&retained_parent)
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
        let workspace = reopen_directory_for_delete(&retained_workspace)
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
        let quarantined_name =
            quarantine_windows_workspace(&parent, OsStr::new(&self.name), &workspace)?;
        require_child_absent(&parent, OsStr::new(&self.name))?;
        let retained_tree = snapshot_windows_workspace_tree(&workspace)?;
        if !observed_tree.matches_retained(&retained_tree) {
            return Err(DeliveryGitProbeError::CleanupUnproven);
        }
        hook(CleanupPhase::AfterQuarantineSnapshot);
        retained_tree.revalidate(&workspace)?;
        retained_tree.remove()?;
        require_windows_directory_empty(&workspace)?;

        if !child_directory_matches(&parent, &quarantined_name, &workspace)
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?
        {
            return Err(DeliveryGitProbeError::CleanupUnproven);
        }
        remove_child_directory(&parent, &quarantined_name, &workspace)
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
        drop(workspace);
        drop(retained_workspace);
        drop(self.directory.take());
        require_child_absent(&parent, &quarantined_name)?;
        require_child_absent(&parent, OsStr::new(&self.name))?;
        self.parent
            .revalidate()
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)
    }

    #[cfg(windows)]
    fn cleanup_parent_handle(&self) -> Result<File, DeliveryGitProbeError> {
        let root = self
            .parent
            .cloned_root_capability()
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
        let retained = root
            .try_clone_root()
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
        Ok(retained)
    }

    #[cfg(windows)]
    fn cleanup_workspace_handle(&self) -> Result<File, DeliveryGitProbeError> {
        let directory = self
            .directory
            .as_ref()
            .ok_or(DeliveryGitProbeError::CleanupUnproven)?;
        let root = directory
            .cloned_root_capability()
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
        root.try_clone_root()
            .map_err(|_| DeliveryGitProbeError::CleanupUnproven)
    }
}

/// An exact pre-quarantine observation. It stores no live child handles, so
/// the workspace can still be renamed after its path guard is released.
#[cfg(windows)]
struct ObservedWindowsWorkspaceTree {
    entries: Vec<ObservedWindowsWorkspaceEntry>,
}

#[cfg(windows)]
enum ObservedWindowsWorkspaceEntry {
    Directory {
        name: OsString,
        identity: crate::DirectoryIdentityMarker,
        children: Vec<ObservedWindowsWorkspaceEntry>,
    },
    File {
        name: OsString,
        identity: crate::DirectoryIdentityMarker,
    },
}

#[cfg(windows)]
impl ObservedWindowsWorkspaceTree {
    fn matches_retained(&self, retained: &RetainedWindowsWorkspaceTree) -> bool {
        entries_match_retained(&self.entries, &retained.entries)
    }
}

#[cfg(windows)]
impl ObservedWindowsWorkspaceEntry {
    fn matches_retained(&self, retained: &RetainedWindowsWorkspaceEntry) -> bool {
        match (self, retained) {
            (
                Self::Directory {
                    name,
                    identity,
                    children,
                },
                RetainedWindowsWorkspaceEntry::Directory {
                    name: retained_name,
                    identity: retained_identity,
                    children: retained_children,
                    ..
                },
            ) => {
                name == retained_name
                    && identity == retained_identity
                    && entries_match_retained(children, retained_children)
            }
            (
                Self::File { name, identity },
                RetainedWindowsWorkspaceEntry::File {
                    name: retained_name,
                    identity: retained_identity,
                    ..
                },
            ) => name == retained_name && identity == retained_identity,
            _ => false,
        }
    }
}

/// Post-quarantine snapshot used for both identity revalidation and deletion.
/// Every child that can be deleted is held by a descriptor collected before
/// cleanup starts; recursive deletion never opens a current child name.
#[cfg(windows)]
struct RetainedWindowsWorkspaceTree {
    entries: Vec<RetainedWindowsWorkspaceEntry>,
}

#[cfg(windows)]
enum RetainedWindowsWorkspaceEntry {
    Directory {
        name: OsString,
        identity: crate::DirectoryIdentityMarker,
        directory: File,
        children: Vec<RetainedWindowsWorkspaceEntry>,
    },
    File {
        name: OsString,
        identity: crate::DirectoryIdentityMarker,
        file: File,
    },
}

#[cfg(windows)]
impl RetainedWindowsWorkspaceTree {
    fn revalidate(&self, root: &File) -> Result<(), DeliveryGitProbeError> {
        revalidate_retained_windows_workspace_entries(root, &self.entries)
    }

    fn remove(self) -> Result<(), DeliveryGitProbeError> {
        for entry in self.entries {
            entry.remove()?;
        }
        Ok(())
    }
}

#[cfg(windows)]
impl RetainedWindowsWorkspaceEntry {
    fn name(&self) -> &OsStr {
        match self {
            Self::Directory { name, .. } | Self::File { name, .. } => name,
        }
    }

    fn revalidate(&self, parent: &File) -> Result<(), DeliveryGitProbeError> {
        match self {
            Self::Directory {
                name,
                directory,
                children,
                ..
            } => {
                ensure_plain_directory(directory)
                    .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
                if !child_directory_matches(parent, name, directory)
                    .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?
                {
                    return Err(DeliveryGitProbeError::CleanupUnproven);
                }
                revalidate_retained_windows_workspace_entries(directory, children)
            }
            Self::File { name, file, .. } => {
                ensure_plain_file(file).map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
                if child_file_matches(parent, name, file)
                    .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?
                {
                    Ok(())
                } else {
                    Err(DeliveryGitProbeError::CleanupUnproven)
                }
            }
        }
    }

    fn remove(self) -> Result<(), DeliveryGitProbeError> {
        match self {
            Self::Directory {
                name,
                directory,
                children,
                ..
            } => {
                for child in children {
                    child.remove()?;
                }
                require_windows_directory_empty(&directory)?;
                let deletion = reopen_directory_for_delete(&directory)
                    .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
                // Windows removes this exact descriptor; `name` is never
                // reopened as a deletion target after the snapshot.
                remove_child_directory(&directory, &name, &deletion)
                    .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
                drop(deletion);
                drop(directory);
                Ok(())
            }
            Self::File { name, file, .. } => {
                let deletion = reopen_file_for_delete(&file)
                    .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
                // As above, deletion follows the retained descriptor rather
                // than a lookup of the mutable namespace name.
                remove_child_file(&file, &name, &deletion)
                    .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
                drop(deletion);
                drop(file);
                Ok(())
            }
        }
    }
}

#[cfg(windows)]
fn observe_windows_workspace_tree(
    root: &File,
) -> Result<ObservedWindowsWorkspaceTree, DeliveryGitProbeError> {
    let mut observed_entries = 0usize;
    let entries = observe_windows_workspace_entries(root, 0, &mut observed_entries, true)?;
    Ok(ObservedWindowsWorkspaceTree { entries })
}

#[cfg(windows)]
fn observe_windows_workspace_entries(
    parent: &File,
    depth: usize,
    observed_entries: &mut usize,
    top_level: bool,
) -> Result<Vec<ObservedWindowsWorkspaceEntry>, DeliveryGitProbeError> {
    let names = read_windows_workspace_names(parent, depth, observed_entries)?;
    let mut entries = Vec::with_capacity(names.len());
    for name in names {
        ensure_known_workspace_entry(top_level, name.as_os_str())?;
        entries.push(observe_windows_workspace_entry(
            parent,
            name,
            depth,
            observed_entries,
        )?);
    }
    Ok(entries)
}

#[cfg(windows)]
fn observe_windows_workspace_entry(
    parent: &File,
    name: OsString,
    depth: usize,
    observed_entries: &mut usize,
) -> Result<ObservedWindowsWorkspaceEntry, DeliveryGitProbeError> {
    match open_child_directory(parent, name.as_os_str()) {
        Ok(directory) => {
            ensure_plain_directory(&directory)
                .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
            let identity = directory_identity_marker(&directory)
                .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
            if !child_directory_matches(parent, name.as_os_str(), &directory)
                .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?
            {
                return Err(DeliveryGitProbeError::CleanupUnproven);
            }
            let children =
                observe_windows_workspace_entries(&directory, depth + 1, observed_entries, false)?;
            Ok(ObservedWindowsWorkspaceEntry::Directory {
                name,
                identity,
                children,
            })
        }
        Err(error) if windows_not_a_directory(&error) => {
            let file = open_child_file(parent, name.as_os_str())
                .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
            ensure_plain_file(&file).map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
            let identity = directory_identity_marker(&file)
                .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
            if !child_file_matches(parent, name.as_os_str(), &file)
                .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?
            {
                return Err(DeliveryGitProbeError::CleanupUnproven);
            }
            Ok(ObservedWindowsWorkspaceEntry::File { name, identity })
        }
        Err(_) => Err(DeliveryGitProbeError::CleanupUnproven),
    }
}

#[cfg(windows)]
fn snapshot_windows_workspace_tree(
    root: &File,
) -> Result<RetainedWindowsWorkspaceTree, DeliveryGitProbeError> {
    let mut observed_entries = 0usize;
    let entries = snapshot_windows_workspace_entries(root, 0, &mut observed_entries, true)?;
    Ok(RetainedWindowsWorkspaceTree { entries })
}

#[cfg(windows)]
fn snapshot_windows_workspace_entries(
    parent: &File,
    depth: usize,
    observed_entries: &mut usize,
    top_level: bool,
) -> Result<Vec<RetainedWindowsWorkspaceEntry>, DeliveryGitProbeError> {
    let names = read_windows_workspace_names(parent, depth, observed_entries)?;
    let mut entries = Vec::with_capacity(names.len());
    for name in names {
        ensure_known_workspace_entry(top_level, name.as_os_str())?;
        entries.push(snapshot_windows_workspace_entry(
            parent,
            name,
            depth,
            observed_entries,
        )?);
    }
    Ok(entries)
}

#[cfg(windows)]
fn snapshot_windows_workspace_entry(
    parent: &File,
    name: OsString,
    depth: usize,
    observed_entries: &mut usize,
) -> Result<RetainedWindowsWorkspaceEntry, DeliveryGitProbeError> {
    match open_child_directory(parent, name.as_os_str()) {
        Ok(directory) => {
            ensure_plain_directory(&directory)
                .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
            let identity = directory_identity_marker(&directory)
                .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
            if !child_directory_matches(parent, name.as_os_str(), &directory)
                .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?
            {
                return Err(DeliveryGitProbeError::CleanupUnproven);
            }
            let children =
                snapshot_windows_workspace_entries(&directory, depth + 1, observed_entries, false)?;
            Ok(RetainedWindowsWorkspaceEntry::Directory {
                name,
                identity,
                directory,
                children,
            })
        }
        Err(error) if windows_not_a_directory(&error) => {
            let file = open_child_file(parent, name.as_os_str())
                .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
            ensure_plain_file(&file).map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
            let identity = directory_identity_marker(&file)
                .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
            if !child_file_matches(parent, name.as_os_str(), &file)
                .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?
            {
                return Err(DeliveryGitProbeError::CleanupUnproven);
            }
            Ok(RetainedWindowsWorkspaceEntry::File {
                name,
                identity,
                file,
            })
        }
        Err(_) => Err(DeliveryGitProbeError::CleanupUnproven),
    }
}

#[cfg(windows)]
fn read_windows_workspace_names(
    directory: &File,
    depth: usize,
    observed_entries: &mut usize,
) -> Result<Vec<OsString>, DeliveryGitProbeError> {
    if depth > MAX_CLEANUP_DEPTH || *observed_entries >= MAX_CLEANUP_ENTRIES {
        return Err(DeliveryGitProbeError::CleanupUnproven);
    }
    let remaining = MAX_CLEANUP_ENTRIES - *observed_entries;
    let mut enumeration =
        reopen_directory(directory).map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
    let mut names = read_directory_names(&mut enumeration, remaining)
        .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
    *observed_entries += names.len();
    names.sort();
    Ok(names)
}

#[cfg(windows)]
fn ensure_known_workspace_entry(
    top_level: bool,
    name: &OsStr,
) -> Result<(), DeliveryGitProbeError> {
    if !top_level || is_known_workspace_entry(name) {
        Ok(())
    } else {
        Err(DeliveryGitProbeError::CleanupUnproven)
    }
}

#[cfg(windows)]
fn is_known_workspace_entry(name: &OsStr) -> bool {
    name == OsStr::new(GIT_DIRECTORY_NAME) || name == OsStr::new(PROBE_WORKTREE_FILE_NAME)
}

#[cfg(windows)]
fn windows_not_a_directory(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::NotADirectory || error.raw_os_error() == Some(267)
}

#[cfg(windows)]
fn entries_match_retained(
    observed: &[ObservedWindowsWorkspaceEntry],
    retained: &[RetainedWindowsWorkspaceEntry],
) -> bool {
    observed.len() == retained.len()
        && observed
            .iter()
            .zip(retained)
            .all(|(observed, retained)| observed.matches_retained(retained))
}

#[cfg(windows)]
fn revalidate_retained_windows_workspace_entries(
    parent: &File,
    entries: &[RetainedWindowsWorkspaceEntry],
) -> Result<(), DeliveryGitProbeError> {
    let mut enumeration =
        reopen_directory(parent).map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
    let actual = read_directory_names(&mut enumeration, entries.len())
        .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?
        .into_iter()
        .collect::<BTreeSet<_>>();
    let expected = entries
        .iter()
        .map(|entry| entry.name().to_os_string())
        .collect::<BTreeSet<_>>();
    if actual != expected {
        return Err(DeliveryGitProbeError::CleanupUnproven);
    }
    for entry in entries {
        entry.revalidate(parent)?;
    }
    Ok(())
}

#[cfg(windows)]
fn quarantine_windows_workspace(
    parent: &File,
    source: &OsStr,
    workspace: &File,
) -> Result<OsString, DeliveryGitProbeError> {
    if !child_directory_matches(parent, source, workspace)
        .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?
    {
        return Err(DeliveryGitProbeError::CleanupUnproven);
    }
    for _ in 0..MAX_QUARANTINE_ATTEMPTS {
        let name = random_quarantine_name()?;
        match quarantine_child_entry_no_replace(parent, source, &name, workspace) {
            Ok(()) => {
                if child_directory_matches(parent, &name, workspace)
                    .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?
                {
                    return Ok(name);
                }
                return Err(DeliveryGitProbeError::CleanupUnproven);
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(_) => return Err(DeliveryGitProbeError::CleanupUnproven),
        }
    }
    Err(DeliveryGitProbeError::CleanupUnproven)
}

#[cfg(windows)]
fn random_quarantine_name() -> Result<OsString, DeliveryGitProbeError> {
    let mut random = [0u8; 16];
    getrandom::fill(&mut random).map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
    let mut name = String::from(".coding-agent-probe-cleanup-v1-");
    for byte in random {
        use std::fmt::Write as _;

        write!(&mut name, "{byte:02x}").expect("writing hexadecimal bytes to String cannot fail");
    }
    Ok(OsString::from(name))
}

#[cfg(windows)]
fn require_windows_directory_empty(directory: &File) -> Result<(), DeliveryGitProbeError> {
    let mut reopened =
        reopen_directory(directory).map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
    if read_directory_names(&mut reopened, 1)
        .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?
        .is_empty()
    {
        Ok(())
    } else {
        Err(DeliveryGitProbeError::CleanupUnproven)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use crate::command_policy::{DeliveryGitEmptyConfig, ExecutionDirectory};

    use super::super::PROBE_WORKTREE_FILE_CONTENTS;
    use super::*;

    #[cfg(windows)]
    #[test]
    fn cleanup_boundary_swap_never_deletes_a_foreign_replacement() {
        use std::cell::Cell;

        let fixture = WorkspaceFixture::new();
        let workspace = ProbeWorkspace::create(Arc::clone(&fixture.parent)).unwrap();
        let original = workspace.path.clone();
        let moved = fixture.root.join("moved-original-at-cleanup-boundary");
        let foreign_marker = original.join("foreign-marker");
        let swapped = Cell::new(false);

        let result = workspace.cleanup_with_hook(|phase| {
            if phase != CleanupPhase::BeforeNamespaceRemoval {
                return;
            }
            if std::fs::rename(&original, &moved).is_ok() {
                std::fs::create_dir(&original).unwrap();
                std::fs::write(&foreign_marker, b"keep").unwrap();
                swapped.set(true);
            }
        });

        if swapped.get() {
            assert_eq!(result.unwrap_err(), DeliveryGitProbeError::CleanupUnproven);
            assert_eq!(std::fs::read(&foreign_marker).unwrap(), b"keep");
            std::fs::remove_dir_all(original).unwrap();
            std::fs::remove_dir_all(moved).unwrap();
        } else {
            // A retained directory handle can prevent the test rename. In
            // that normal case cleanup must still succeed and leave no
            // artifacts.
            result.unwrap();
        }
        fixture.assert_empty();
    }

    #[cfg(windows)]
    #[test]
    fn cleanup_boundary_known_child_swap_never_deletes_replacement() {
        use std::cell::Cell;

        let fixture = WorkspaceFixture::new();
        let workspace = ProbeWorkspace::create(Arc::clone(&fixture.parent)).unwrap();
        let original = workspace.path.join(PROBE_WORKTREE_FILE_NAME);
        let moved = workspace.path.join("moved-probe-worktree-file");
        std::fs::write(&original, PROBE_WORKTREE_FILE_CONTENTS).unwrap();
        let swapped = Cell::new(false);

        let result = workspace.cleanup_with_hook(|phase| {
            if phase != CleanupPhase::BeforeNamespaceRemoval {
                return;
            }
            if std::fs::rename(&original, &moved).is_ok() {
                std::fs::write(&original, b"foreign replacement").unwrap();
                swapped.set(true);
            }
        });

        if swapped.get() {
            assert_eq!(result.unwrap_err(), DeliveryGitProbeError::CleanupUnproven);
            let quarantined = fixture.quarantined_workspace();
            assert_eq!(
                std::fs::read(quarantined.join(PROBE_WORKTREE_FILE_NAME)).unwrap(),
                b"foreign replacement"
            );
            std::fs::remove_dir_all(quarantined).unwrap();
        } else {
            result.unwrap();
        }
        fixture.assert_empty();
    }

    #[cfg(windows)]
    #[test]
    fn cleanup_post_snapshot_nested_child_swap_preserves_the_foreign_injection() {
        use std::cell::Cell;

        let fixture = WorkspaceFixture::new();
        let workspace = ProbeWorkspace::create(Arc::clone(&fixture.parent)).unwrap();
        let nested = workspace
            .path
            .join(GIT_DIRECTORY_NAME)
            .join("objects")
            .join("probe");
        std::fs::create_dir_all(nested.parent().unwrap()).unwrap();
        std::fs::write(&nested, b"retained probe object").unwrap();
        let swapped = Cell::new(false);

        let result = workspace.cleanup_with_hook(|phase| {
            if phase != CleanupPhase::AfterQuarantineSnapshot {
                return;
            }
            let quarantined = fixture.quarantined_workspace();
            let original = quarantined
                .join(GIT_DIRECTORY_NAME)
                .join("objects")
                .join("probe");
            let moved = quarantined
                .join(GIT_DIRECTORY_NAME)
                .join("objects")
                .join("moved-probe");
            if std::fs::rename(&original, &moved).is_ok() {
                std::fs::write(&original, b"foreign nested injection").unwrap();
                swapped.set(true);
            }
        });

        if swapped.get() {
            assert_eq!(result.unwrap_err(), DeliveryGitProbeError::CleanupUnproven);
            let quarantined = fixture.quarantined_workspace();
            assert_eq!(
                std::fs::read(
                    quarantined
                        .join(GIT_DIRECTORY_NAME)
                        .join("objects")
                        .join("probe"),
                )
                .unwrap(),
                b"foreign nested injection"
            );
            std::fs::remove_dir_all(quarantined).unwrap();
        } else {
            result.unwrap();
        }
        fixture.assert_empty();
    }

    #[cfg(windows)]
    #[test]
    fn nul_empty_config_allows_git_to_initialize() {
        use std::collections::BTreeMap;

        let fixture = WorkspaceFixture::new();
        let workspace = ProbeWorkspace::create(Arc::clone(&fixture.parent)).unwrap();
        let config = DeliveryGitEmptyConfig::windows_nul();
        let mut environment = BTreeMap::new();
        config
            .apply_delivery_git_environment(&mut environment)
            .unwrap();
        let output = std::process::Command::new("git.exe")
            .current_dir(&workspace.path)
            .envs(environment)
            .arg("--no-pager")
            .arg("--no-optional-locks")
            .arg("-c")
            .arg("core.hooksPath=NUL")
            .arg("-c")
            .arg("init.templateDir=")
            .arg("init")
            .arg("--quiet")
            .arg("--initial-branch=main")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git init failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        workspace.cleanup().unwrap();
        fixture.assert_empty();
    }

    #[cfg(windows)]
    #[test]
    #[allow(clippy::permissions_set_readonly_false)]
    fn cleanup_removes_windows_readonly_git_style_files() {
        let fixture = WorkspaceFixture::new();
        let workspace = ProbeWorkspace::create(Arc::clone(&fixture.parent)).unwrap();
        let object = workspace
            .path
            .join(GIT_DIRECTORY_NAME)
            .join("readonly-object");
        std::fs::create_dir(object.parent().unwrap()).unwrap();
        std::fs::write(&object, b"object").unwrap();
        let mut permissions = std::fs::metadata(&object).unwrap().permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(&object, permissions).unwrap();

        workspace.cleanup().unwrap();
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

        #[cfg(windows)]
        fn quarantined_workspace(&self) -> PathBuf {
            let mut paths = std::fs::read_dir(&self.root)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .filter(|path| {
                    path.file_name().is_some_and(|name| {
                        name.to_string_lossy()
                            .starts_with(".coding-agent-probe-cleanup-v1-")
                    })
                })
                .collect::<Vec<_>>();
            assert_eq!(paths.len(), 1, "expected one quarantined workspace");
            paths.pop().unwrap()
        }
    }
}
