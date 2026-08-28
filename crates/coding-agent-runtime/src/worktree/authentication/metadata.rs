use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use crate::command_policy::child_visible_path;
use crate::native_fs::{open_child_directory, open_child_file, read_directory_names};
use crate::root_capability::ensure_plain_file;
use crate::{DirectoryIdentityError, DirectoryIdentityMarker, RelativePath, RootCapability};

use super::super::{WorktreeError, WorktreeReservation};
use super::identity::map_common_identity_error;

const ADMIN_FILE_LIMIT: u64 = 16 * 1024;
const MAX_ADMIN_ENTRIES: usize = 4_096;

pub(super) struct AdminRecord {
    pub(super) name: String,
    pub(super) path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CleanupAdminLockState {
    Locked,
    Unlocked,
}

pub(crate) fn find_reserved_git_directory(
    common_git_capability: &RootCapability,
    common_git_directory: &Path,
    reservation: &WorktreeReservation,
) -> Result<Option<PathBuf>, WorktreeError> {
    find_reserved_admin_record(common_git_capability, common_git_directory, reservation)
        .map(|record| record.map(|record| record.path))
}

pub(super) fn find_reserved_admin_record(
    common_git_capability: &RootCapability,
    common_git_directory: &Path,
    reservation: &WorktreeReservation,
) -> Result<Option<AdminRecord>, WorktreeError> {
    let current = list_worktree_admin_entries(common_git_capability)?;
    let mut matches = Vec::new();
    for name in current {
        let Some(name) = valid_admin_name(&name) else {
            continue;
        };
        let pointer = match read_admin_gitdir(common_git_capability, name) {
            Ok(pointer) => pointer,
            Err(WorktreeError::LinkedMetadataInvalid) => continue,
            Err(error) => return Err(error),
        };
        if !admin_backlink_matches(&pointer, reservation.worktree_path())? {
            continue;
        }
        validate_admin_metadata(
            common_git_capability,
            common_git_directory,
            name,
            reservation.branch_name(),
        )?;
        matches.push(AdminRecord {
            name: name.to_owned(),
            path: common_git_directory.join("worktrees").join(name),
        });
    }
    match matches.len() {
        0 => Ok(None),
        1 => Ok(matches.pop()),
        _ => Err(WorktreeError::LinkedMetadataInvalid),
    }
}

fn valid_admin_name(name: &OsString) -> Option<&str> {
    let name = name.to_str()?;
    if RelativePath::parse(name.to_owned()).is_ok() && !name.contains('/') {
        Some(name)
    } else {
        None
    }
}

fn validate_admin_metadata(
    capability: &RootCapability,
    common_git_directory: &Path,
    name: &str,
    branch_name: &str,
) -> Result<(), WorktreeError> {
    if read_admin_line(capability, name, "HEAD")? != format!("ref: refs/heads/{branch_name}")
        || read_admin_line(capability, name, "locked")? != "codex-reserved"
        || !admin_commondir_matches(capability, common_git_directory, name)?
    {
        return Err(WorktreeError::LinkedMetadataInvalid);
    }
    Ok(())
}

/// Validates cleanup metadata exclusively through the already-authenticated
/// admin and worktree directory handles. Every control file is a no-follow
/// plain file and must have the exact bytes Git writes for this topology.
///
/// This deliberately does not weaken `find_reserved_admin_record`, whose
/// normal linked-worktree authentication continues to require the lock.
pub(super) fn validate_cleanup_present_metadata(
    admin: &RootCapability,
    worktree: &RootCapability,
    common_git_directory: &Path,
    reservation: &WorktreeReservation,
    admin_name: &str,
) -> Result<CleanupAdminLockState, WorktreeError> {
    if RelativePath::parse(admin_name.to_owned()).is_err() || admin_name.contains('/') {
        return Err(WorktreeError::LinkedMetadataInvalid);
    }
    let expected_head =
        exact_control_line(&format!("ref: refs/heads/{}", reservation.branch_name()))?;
    if read_cleanup_admin_file(admin, "HEAD")? != expected_head
        || read_cleanup_admin_file(admin, "commondir")? != b"../..\n"
        || read_cleanup_admin_file(admin, "gitdir")?
            != expected_admin_gitdir_line(reservation.worktree_path())?
        || read_worktree_git_pointer(worktree)?
            != expected_worktree_git_pointer_line(common_git_directory, admin_name)?
    {
        return Err(WorktreeError::LinkedMetadataInvalid);
    }

    match read_optional_cleanup_admin_file(admin, "locked")? {
        Some(reason) if reason == b"codex-reserved\n" => Ok(CleanupAdminLockState::Locked),
        None => Ok(CleanupAdminLockState::Unlocked),
        Some(_) => Err(WorktreeError::LinkedMetadataInvalid),
    }
}

/// Proves that the captured administration directory was not merely renamed
/// and that no remaining plain admin record uses the exact target backlink.
/// Names and metadata need not be UTF-8 or otherwise valid before the captured
/// directory-object identity check is performed.
pub(super) fn cleanup_admin_identity_and_backlink_are_absent(
    namespace: &RootCapability,
    reservation: &WorktreeReservation,
    captured_admin: DirectoryIdentityMarker,
) -> Result<bool, WorktreeError> {
    scan_cleanup_admin_namespace(namespace, reservation, captured_admin)
        .map(|scan| scan.captured_count == 0 && !scan.other_backlink)
}

pub(super) fn cleanup_admin_identity_and_backlink_are_unique(
    namespace: &RootCapability,
    reservation: &WorktreeReservation,
    captured_admin: DirectoryIdentityMarker,
) -> Result<bool, WorktreeError> {
    scan_cleanup_admin_namespace(namespace, reservation, captured_admin)
        .map(|scan| scan.captured_count == 1 && !scan.other_backlink)
}

struct CleanupAdminNamespaceScan {
    captured_count: usize,
    other_backlink: bool,
}

fn scan_cleanup_admin_namespace(
    namespace: &RootCapability,
    reservation: &WorktreeReservation,
    captured_admin: DirectoryIdentityMarker,
) -> Result<CleanupAdminNamespaceScan, WorktreeError> {
    let mut root = namespace.try_clone_root().map_err(WorktreeError::Io)?;
    let entries = read_directory_names(&mut root, MAX_ADMIN_ENTRIES).map_err(WorktreeError::Io)?;
    let expected_gitdir = expected_admin_gitdir_line(reservation.worktree_path())?;
    let mut captured_count = 0usize;
    let mut other_backlink = false;
    for name in entries {
        if name == "." || name == ".." {
            continue;
        }
        let directory = match open_child_directory(&root, &name) {
            Ok(directory) => directory,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) if entry_is_not_a_plain_directory(&error) => {
                return Err(WorktreeError::LinkedMetadataInvalid);
            }
            Err(error) => return Err(WorktreeError::Io(error)),
        };
        let admin = match RootCapability::from_authenticated_directory(directory) {
            Ok(admin) => admin,
            Err(error) if entry_is_not_a_plain_directory(&error) => {
                return Err(WorktreeError::LinkedMetadataInvalid);
            }
            Err(error) => return Err(WorktreeError::Io(error)),
        };
        let marker = admin
            .identity_marker()
            .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
        if marker == captured_admin {
            captured_count = captured_count
                .checked_add(1)
                .ok_or(WorktreeError::LinkedMetadataInvalid)?;
            continue;
        }
        match read_optional_cleanup_admin_file(&admin, "gitdir") {
            Ok(Some(bytes)) if bytes == expected_gitdir => other_backlink = true,
            Ok(_) | Err(WorktreeError::LinkedMetadataInvalid) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(CleanupAdminNamespaceScan {
        captured_count,
        other_backlink,
    })
}

fn admin_backlink_matches(pointer: &Path, worktree_path: &Path) -> Result<bool, WorktreeError> {
    let expected = child_visible_path(worktree_path).join(".git");
    match (
        std::fs::canonicalize(pointer),
        std::fs::canonicalize(&expected),
    ) {
        (Ok(pointer), Ok(expected)) => Ok(pointer == expected),
        (Err(pointer_error), _) if pointer_error.kind() != io::ErrorKind::NotFound => {
            Err(WorktreeError::Io(pointer_error))
        }
        (_, Err(expected_error)) if expected_error.kind() != io::ErrorKind::NotFound => {
            Err(WorktreeError::Io(expected_error))
        }
        _ => Ok(pointer == expected),
    }
}

pub(crate) fn list_worktree_admin_entries(
    capability: &RootCapability,
) -> Result<BTreeSet<OsString>, WorktreeError> {
    let relative = RelativePath::parse("worktrees".to_owned())
        .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
    let mut directory = match capability.open_directory(&relative) {
        Ok(directory) => directory,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(BTreeSet::new()),
        Err(error) => return Err(WorktreeError::Io(error)),
    };
    let names =
        read_directory_names(&mut directory, MAX_ADMIN_ENTRIES).map_err(WorktreeError::Io)?;
    Ok(names
        .into_iter()
        .filter(|name| name != "." && name != "..")
        .collect())
}

pub(crate) fn read_admin_gitdir(
    capability: &RootCapability,
    name: &str,
) -> Result<PathBuf, WorktreeError> {
    let value = read_admin_line(capability, name, "gitdir")?;
    let path = PathBuf::from(value);
    if path.is_absolute() {
        Ok(path)
    } else {
        Err(WorktreeError::LinkedMetadataInvalid)
    }
}

pub(crate) fn read_admin_line(
    capability: &RootCapability,
    name: &str,
    file_name: &str,
) -> Result<String, WorktreeError> {
    let relative = RelativePath::parse(format!("worktrees/{name}/{file_name}"))
        .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
    let mut file = open_admin_file(capability, &relative)?;
    let mut bytes = Vec::new();
    file.by_ref()
        .take(ADMIN_FILE_LIMIT + 1)
        .read_to_end(&mut bytes)
        .map_err(WorktreeError::Io)?;
    parse_admin_line(bytes)
}

fn read_cleanup_admin_file(
    admin: &RootCapability,
    file_name: &str,
) -> Result<Vec<u8>, WorktreeError> {
    read_optional_cleanup_admin_file(admin, file_name)?.ok_or(WorktreeError::LinkedMetadataInvalid)
}

fn read_optional_cleanup_admin_file(
    admin: &RootCapability,
    file_name: &str,
) -> Result<Option<Vec<u8>>, WorktreeError> {
    let relative = RelativePath::parse(file_name.to_owned())
        .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
    let file = match admin.open_file_for_read(&relative) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(WorktreeError::Io(error)),
    };
    read_bounded_control_file(file).map(Some)
}

fn read_worktree_git_pointer(worktree: &RootCapability) -> Result<Vec<u8>, WorktreeError> {
    let root = worktree.try_clone_root().map_err(WorktreeError::Io)?;
    let file = open_child_file(&root, OsStr::new(".git")).map_err(WorktreeError::Io)?;
    ensure_plain_file(&file).map_err(WorktreeError::Io)?;
    read_bounded_control_file(file)
}

fn read_bounded_control_file(mut file: std::fs::File) -> Result<Vec<u8>, WorktreeError> {
    let mut bytes = Vec::new();
    file.by_ref()
        .take(ADMIN_FILE_LIMIT + 1)
        .read_to_end(&mut bytes)
        .map_err(WorktreeError::Io)?;
    if bytes.len() as u64 > ADMIN_FILE_LIMIT {
        Err(WorktreeError::LinkedMetadataInvalid)
    } else {
        Ok(bytes)
    }
}

fn expected_admin_gitdir_line(worktree_path: &Path) -> Result<Vec<u8>, WorktreeError> {
    exact_control_line(&git_metadata_path(
        &child_visible_path(worktree_path).join(".git"),
    )?)
}

fn expected_worktree_git_pointer_line(
    common_git_directory: &Path,
    admin_name: &str,
) -> Result<Vec<u8>, WorktreeError> {
    let admin = child_visible_path(common_git_directory)
        .join("worktrees")
        .join(admin_name);
    exact_control_line(&format!("gitdir: {}", git_metadata_path(&admin)?))
}

fn exact_control_line(value: &str) -> Result<Vec<u8>, WorktreeError> {
    if value.is_empty() || value.contains(['\0', '\r', '\n']) {
        return Err(WorktreeError::LinkedMetadataInvalid);
    }
    let mut bytes = value.as_bytes().to_vec();
    bytes.push(b'\n');
    if bytes.len() as u64 > ADMIN_FILE_LIMIT {
        Err(WorktreeError::LinkedMetadataInvalid)
    } else {
        Ok(bytes)
    }
}

fn git_metadata_path(path: &Path) -> Result<String, WorktreeError> {
    let value = path.to_str().ok_or(WorktreeError::LinkedMetadataInvalid)?;
    #[cfg(windows)]
    let value = value.replace('\\', "/");
    #[cfg(unix)]
    let value = value.to_owned();
    Ok(value)
}

fn entry_is_not_a_plain_directory(error: &io::Error) -> bool {
    if matches!(
        error.kind(),
        io::ErrorKind::NotADirectory | io::ErrorKind::InvalidData
    ) {
        return true;
    }
    #[cfg(unix)]
    {
        error.raw_os_error() == Some(libc::ELOOP)
    }
    #[cfg(windows)]
    {
        error.raw_os_error() == Some(267)
    }
}

fn open_admin_file(
    capability: &RootCapability,
    relative: &RelativePath,
) -> Result<std::fs::File, WorktreeError> {
    match capability.open_file_for_read(relative) {
        Ok(file) => Ok(file),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            Err(WorktreeError::LinkedMetadataInvalid)
        }
        Err(error) => Err(WorktreeError::Io(error)),
    }
}

fn parse_admin_line(mut bytes: Vec<u8>) -> Result<String, WorktreeError> {
    if bytes.len() as u64 > ADMIN_FILE_LIMIT || bytes.contains(&0) {
        return Err(WorktreeError::LinkedMetadataInvalid);
    }
    while matches!(bytes.last(), Some(b'\n' | b'\r')) {
        bytes.pop();
    }
    let value = std::str::from_utf8(&bytes).map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
    if value.is_empty() || value.contains(['\r', '\n']) {
        Err(WorktreeError::LinkedMetadataInvalid)
    } else {
        Ok(value.to_owned())
    }
}

pub(crate) fn admin_commondir_matches(
    capability: &RootCapability,
    common_git_directory: &Path,
    name: &str,
) -> Result<bool, WorktreeError> {
    let commondir = read_admin_line(capability, name, "commondir")?;
    if commondir != "../.." {
        return Ok(false);
    }
    let unresolved = common_git_directory
        .join("worktrees")
        .join(name)
        .join(&commondir);
    let resolved_path = match std::fs::canonicalize(unresolved) {
        Ok(resolved) => resolved,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(WorktreeError::Io(error)),
    };
    let resolved = RootCapability::open(resolved_path).map_err(WorktreeError::Io)?;
    let expected = capability
        .identity_marker()
        .map_err(map_common_identity_error)?;
    match resolved.require_identity(expected) {
        Ok(()) => Ok(true),
        Err(DirectoryIdentityError::Mismatch) => Ok(false),
        Err(error) => Err(map_common_identity_error(error)),
    }
}

pub(super) fn admin_relative_path(name: &str) -> Result<RelativePath, WorktreeError> {
    RelativePath::parse(format!("worktrees/{name}"))
        .map_err(|_| WorktreeError::LinkedMetadataInvalid)
}
