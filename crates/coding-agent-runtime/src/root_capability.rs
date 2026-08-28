use std::collections::hash_map::RandomState;
use std::ffi::OsString;
use std::fmt;
use std::fs::File;
use std::hash::{BuildHasher, Hash, Hasher};
use std::io;
use std::path::Path;
use std::sync::OnceLock;

use sha2::{Digest, Sha256};

use crate::RelativePath;
use crate::native_fs::{
    child_entry_exists, child_matches_protected_metadata, create_child_directory,
    open_child_directory, open_child_file, reopen_directory, reopen_directory_for_child_directory,
    reopen_directory_for_write, reopen_directory_path_lease,
};

/// Opaque filesystem identity for an authenticated directory capability.
///
/// The marker is useful only for equality and hashing. Its representation is
/// deliberately private, has no accessors, and is not serializable, so callers
/// cannot forge one or recover a path from it.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct DirectoryIdentityMarker {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(windows)]
    volume_serial_number: u64,
    #[cfg(windows)]
    file_id: [u8; 16],
    opaque_hash: u64,
}

impl fmt::Debug for DirectoryIdentityMarker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DirectoryIdentityMarker(<opaque>)")
    }
}

impl Hash for DirectoryIdentityMarker {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.opaque_hash.hash(state);
    }
}

/// Failure to observe or match an authenticated directory identity.
///
/// Both variants intentionally use fixed, path-free diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DirectoryIdentityError {
    #[error("authenticated directory identity is unavailable")]
    Unavailable,
    #[error("authenticated directory identity does not match")]
    Mismatch,
}

/// Closed namespaces for durable directory identities.
///
/// A directory used as a common Git directory must not compare equal to the
/// same filesystem object used as a linked-worktree administration directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DirectoryIdentityDomain {
    CommonGit,
    WorktreeAdmin,
}

impl DirectoryIdentityDomain {
    const fn tag(self) -> &'static [u8] {
        match self {
            Self::CommonGit => b"common-git",
            Self::WorktreeAdmin => b"worktree-admin",
        }
    }
}

/// Versioned, durable digest of one authenticated directory object.
///
/// Unlike [`DirectoryIdentityMarker`], this value is stable across process
/// restarts and may be persisted as typed internal provenance. Its digest
/// representation is deliberately not part of the public runtime API.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct DurableDirectoryIdentityV1 {
    digest: [u8; 32],
    hexadecimal: [u8; 64],
}

impl DurableDirectoryIdentityV1 {
    /// Derives provenance from the retained handle without resolving a path.
    pub(crate) fn derive(
        capability: &RootCapability,
        domain: DirectoryIdentityDomain,
    ) -> Result<Self, DirectoryIdentityError> {
        let platform = platform_directory_identity(&capability.root)?;
        let digest = durable_directory_identity_digest(domain, platform);
        Ok(Self {
            hexadecimal: encode_lower_hex(digest),
            digest,
        })
    }

    #[allow(dead_code)]
    pub(crate) const fn algorithm(&self) -> &'static str {
        "directory_identity_v1"
    }

    #[allow(dead_code)]
    pub(crate) fn as_hex(&self) -> &str {
        std::str::from_utf8(&self.hexadecimal)
            .expect("lower hexadecimal encoding is always valid UTF-8")
    }

    pub(crate) const fn digest(&self) -> &[u8; 32] {
        &self.digest
    }
}

impl fmt::Debug for DurableDirectoryIdentityV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DurableDirectoryIdentityV1(<redacted>)")
    }
}

#[derive(Clone, Copy)]
enum PlatformDirectoryIdentity {
    #[cfg(unix)]
    Unix { device: u64, inode: u64 },
    #[cfg(windows)]
    Windows {
        volume_serial_number: u64,
        file_id: [u8; 16],
    },
}

/// Retained handles for an application-owned directory path created relative
/// to a root capability. Keeping the guard alive preserves every validated
/// component across a later string-path consumer such as Git.
#[derive(Debug)]
pub(crate) struct DirectoryPathGuard {
    final_directory: File,
    _component_leases: Vec<File>,
}

impl DirectoryPathGuard {
    pub(crate) fn child_is_absent(&self, name: &std::ffi::OsStr) -> io::Result<bool> {
        child_entry_exists(&self.final_directory, name).map(|exists| !exists)
    }

    pub(crate) fn try_clone_final(&self) -> io::Result<File> {
        self.final_directory.try_clone()
    }
}

#[derive(Debug)]
pub struct RootCapability {
    root: File,
}

impl RootCapability {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let root = open_absolute_root_without_following(path.as_ref())?;
        ensure_plain_directory(&root)?;
        Ok(Self { root })
    }

    pub fn open_file_for_read(&self, path: &RelativePath) -> io::Result<File> {
        self.open_file_for_read_with_hook(path, |_| Ok(()))
    }

    /// Returns the identity of the retained, already-authenticated root handle.
    ///
    /// This never resolves the namespace path again and never falls back to a
    /// path-string identity.
    pub fn identity_marker(&self) -> Result<DirectoryIdentityMarker, DirectoryIdentityError> {
        directory_identity_marker(&self.root)
    }

    /// Requires this retained capability to identify the expected directory.
    pub fn require_identity(
        &self,
        expected: DirectoryIdentityMarker,
    ) -> Result<(), DirectoryIdentityError> {
        if self.identity_marker()? == expected {
            Ok(())
        } else {
            Err(DirectoryIdentityError::Mismatch)
        }
    }

    /// Duplicates the already-validated root directory handle without
    /// resolving its namespace path again.
    pub(crate) fn try_clone_root(&self) -> io::Result<File> {
        self.root.try_clone()
    }

    pub(crate) fn try_clone_capability(&self) -> io::Result<Self> {
        Ok(Self {
            root: reopen_directory(&self.root)?,
        })
    }

    pub(crate) fn from_authenticated_directory(root: File) -> io::Result<Self> {
        ensure_plain_directory(&root)?;
        Ok(Self { root })
    }

    pub(crate) fn open_directory(&self, path: &RelativePath) -> io::Result<File> {
        let mut parent = reopen_directory(&self.root)?;
        for component in path.components() {
            let child = open_child_directory(&parent, component.as_ref())?;
            ensure_child_is_not_protected_metadata(&parent, &child)?;
            ensure_plain_directory(&child)?;
            parent = child;
        }
        Ok(parent)
    }

    /// Ensures each component exists as a plain directory using only
    /// handle-relative, no-follow operations. No path-based create is issued
    /// before an ancestor has been authenticated.
    pub(crate) fn ensure_directory_path(
        &self,
        path: &RelativePath,
    ) -> io::Result<DirectoryPathGuard> {
        let mut current = reopen_directory_for_child_directory(&self.root)?;
        let mut leases = vec![reopen_directory_path_lease(&self.root)?];
        for component in path.components() {
            let child = create_child_directory(&current, component.as_ref())?;
            ensure_plain_directory(&child)?;
            leases.push(reopen_directory_path_lease(&child)?);
            current = child;
        }
        Ok(DirectoryPathGuard {
            final_directory: current,
            _component_leases: leases,
        })
    }

    pub(crate) fn open_parent_directory(
        &self,
        path: &RelativePath,
    ) -> io::Result<(File, OsString)> {
        let mut components = path.components().peekable();
        if components.peek().is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the root has no parent-relative file name",
            ));
        }

        let mut parent = reopen_directory(&self.root)?;
        while let Some(component) = components.next() {
            if components.peek().is_some() {
                let child = open_child_directory(&parent, component.as_ref())?;
                ensure_child_is_not_protected_metadata(&parent, &child)?;
                ensure_plain_directory(&child)?;
                parent = child;
            } else {
                parent = reopen_directory_for_write(&parent)?;
                ensure_plain_directory(&parent)?;
                return Ok((parent, OsString::from(component)));
            }
        }
        unreachable!("the empty path returned before traversal")
    }

    fn open_file_for_read_with_hook(
        &self,
        path: &RelativePath,
        mut after_directory_open: impl FnMut(usize) -> io::Result<()>,
    ) -> io::Result<File> {
        let mut components = path.components().peekable();
        if components.peek().is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the root is not a file path",
            ));
        }

        let mut parent = reopen_directory(&self.root)?;
        let mut depth = 0usize;
        while let Some(component) = components.next() {
            if components.peek().is_some() {
                let child = open_child_directory(&parent, component.as_ref())?;
                ensure_child_is_not_protected_metadata(&parent, &child)?;
                ensure_plain_directory(&child)?;
                parent = child;
                depth += 1;
                after_directory_open(depth)?;
            } else {
                let file = open_child_file(&parent, component.as_ref())?;
                ensure_child_is_not_protected_metadata(&parent, &file)?;
                ensure_plain_file(&file)?;
                return Ok(file);
            }
        }
        unreachable!("the empty path returned before traversal")
    }
}

#[cfg(unix)]
pub(crate) fn directory_identity_marker(
    directory: &File,
) -> Result<DirectoryIdentityMarker, DirectoryIdentityError> {
    let PlatformDirectoryIdentity::Unix { device, inode } = platform_directory_identity(directory)?;
    Ok(DirectoryIdentityMarker {
        device,
        inode,
        opaque_hash: opaque_directory_identity_hash(|hasher| {
            hasher.write_u8(1);
            hasher.write_u64(device);
            hasher.write_u64(inode);
        }),
    })
}

#[cfg(windows)]
pub(crate) fn directory_identity_marker(
    directory: &File,
) -> Result<DirectoryIdentityMarker, DirectoryIdentityError> {
    let PlatformDirectoryIdentity::Windows {
        volume_serial_number,
        file_id,
    } = platform_directory_identity(directory)?;
    Ok(DirectoryIdentityMarker {
        volume_serial_number,
        file_id,
        opaque_hash: opaque_directory_identity_hash(|hasher| {
            hasher.write_u8(2);
            hasher.write_u64(volume_serial_number);
            hasher.write(&file_id);
        }),
    })
}

#[cfg(unix)]
fn platform_directory_identity(
    directory: &File,
) -> Result<PlatformDirectoryIdentity, DirectoryIdentityError> {
    use std::os::unix::fs::MetadataExt;

    let metadata = directory
        .metadata()
        .map_err(|_| DirectoryIdentityError::Unavailable)?;
    Ok(PlatformDirectoryIdentity::Unix {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

#[cfg(windows)]
fn platform_directory_identity(
    directory: &File,
) -> Result<PlatformDirectoryIdentity, DirectoryIdentityError> {
    use std::ffi::c_void;
    use std::mem::{MaybeUninit, size_of};
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ID_INFO, FileIdInfo, GetFileInformationByHandleEx,
    };

    let mut information = MaybeUninit::<FILE_ID_INFO>::zeroed();
    let succeeded = unsafe {
        GetFileInformationByHandleEx(
            directory.as_raw_handle() as HANDLE,
            FileIdInfo,
            information.as_mut_ptr().cast::<c_void>(),
            size_of::<FILE_ID_INFO>() as u32,
        )
    };
    if succeeded == 0 {
        return Err(DirectoryIdentityError::Unavailable);
    }
    let information = unsafe { information.assume_init() };
    Ok(PlatformDirectoryIdentity::Windows {
        volume_serial_number: information.VolumeSerialNumber,
        file_id: information.FileId.Identifier,
    })
}

fn durable_directory_identity_digest(
    domain: DirectoryIdentityDomain,
    platform: PlatformDirectoryIdentity,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    write_digest_field(
        &mut hasher,
        b"schema",
        b"coding-agent/directory-identity/v1",
    );
    write_digest_field(&mut hasher, b"domain", domain.tag());
    match platform {
        #[cfg(unix)]
        PlatformDirectoryIdentity::Unix { device, inode } => {
            write_digest_field(&mut hasher, b"platform", b"unix");
            write_digest_field(&mut hasher, b"device", &device.to_be_bytes());
            write_digest_field(&mut hasher, b"inode", &inode.to_be_bytes());
        }
        #[cfg(windows)]
        PlatformDirectoryIdentity::Windows {
            volume_serial_number,
            file_id,
        } => {
            write_digest_field(&mut hasher, b"platform", b"windows");
            write_digest_field(
                &mut hasher,
                b"volume-serial-number",
                &volume_serial_number.to_be_bytes(),
            );
            write_digest_field(&mut hasher, b"file-id", &file_id);
        }
    }
    hasher.finalize().into()
}

fn write_digest_field(hasher: &mut Sha256, tag: &[u8], value: &[u8]) {
    hasher.update((tag.len() as u32).to_be_bytes());
    hasher.update(tag);
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn encode_lower_hex(digest: [u8; 32]) -> [u8; 64] {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = [0u8; 64];
    for (index, byte) in digest.into_iter().enumerate() {
        encoded[index * 2] = HEX[usize::from(byte >> 4)];
        encoded[index * 2 + 1] = HEX[usize::from(byte & 0x0f)];
    }
    encoded
}

fn opaque_directory_identity_hash(write_identity: impl FnOnce(&mut dyn Hasher)) -> u64 {
    static RANDOM_STATE: OnceLock<RandomState> = OnceLock::new();
    let mut hasher = RANDOM_STATE.get_or_init(RandomState::new).build_hasher();
    write_identity(&mut hasher);
    hasher.finish()
}

#[cfg(unix)]
fn open_absolute_root_without_following(path: &Path) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    use std::path::Component;

    if !path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "capability root must be absolute",
        ));
    }
    let mut options = std::fs::OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let mut directory = options.open(Path::new("/"))?;
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(name) => {
                directory = open_child_directory(&directory, name)?;
                ensure_plain_directory(&directory)?;
            }
            Component::CurDir | Component::ParentDir | Component::Prefix(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "capability root contains an unsupported component",
                ));
            }
        }
    }
    Ok(directory)
}

#[cfg(windows)]
fn open_absolute_root_without_following(path: &Path) -> io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt;
    use std::path::{Component, Prefix};
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_DELETE,
        FILE_SHARE_READ, FILE_SHARE_WRITE,
    };

    let mut components = path.components();
    let drive = match components.next() {
        Some(Component::Prefix(prefix)) => match prefix.kind() {
            Prefix::Disk(drive) | Prefix::VerbatimDisk(drive) => drive,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "capability root must be on a local disk",
                ));
            }
        },
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "capability root must be absolute",
            ));
        }
    };
    if !matches!(components.next(), Some(Component::RootDir)) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "capability root must include a disk root",
        ));
    }

    let disk_root = format!("{}:\\", char::from(drive));
    let mut options = std::fs::OpenOptions::new();
    options
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT);
    let mut directory = options.open(disk_root)?;
    ensure_plain_directory(&directory)?;
    for component in components {
        match component {
            Component::Normal(name) => {
                directory = open_child_directory(&directory, name)?;
                ensure_plain_directory(&directory)?;
            }
            Component::CurDir
            | Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "capability root contains an unsupported component",
                ));
            }
        }
    }
    Ok(directory)
}

pub(crate) fn ensure_plain_directory(file: &File) -> io::Result<()> {
    let metadata = file.metadata()?;
    if !metadata.is_dir() || metadata_is_reparse_point(&metadata) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "path component is not a plain directory",
        ));
    }
    Ok(())
}

pub(crate) fn ensure_plain_file(file: &File) -> io::Result<()> {
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata_is_reparse_point(&metadata) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "path component is not a plain regular file",
        ));
    }
    Ok(())
}

pub(crate) fn ensure_child_is_not_protected_metadata(
    parent: &File,
    child: &File,
) -> io::Result<()> {
    if child_matches_protected_metadata(parent, child)? {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "path resolves to protected Git metadata",
        ))
    } else {
        Ok(())
    }
}

#[cfg(windows)]
fn metadata_is_reparse_point(metadata: &std::fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;

    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(not(windows))]
fn metadata_is_reparse_point(_: &std::fs::Metadata) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::process::Command;

    use super::*;

    #[test]
    fn ancestor_namespace_swap_does_not_change_the_open_parent_capability() {
        let root_directory = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let safe = root_directory.path().join("safe");
        let held = root_directory.path().join("held");
        std::fs::create_dir(&safe).unwrap();
        std::fs::write(safe.join("value.txt"), b"inside").unwrap();
        std::fs::write(outside.path().join("value.txt"), b"outside").unwrap();

        let root = RootCapability::open(root_directory.path().canonicalize().unwrap()).unwrap();
        let path = RelativePath::parse("safe/value.txt").unwrap();
        let mut file = root
            .open_file_for_read_with_hook(&path, |depth| {
                if depth == 1 {
                    std::fs::rename(&safe, &held)?;
                    create_dir_link(outside.path(), &safe)?;
                }
                Ok(())
            })
            .unwrap();
        let mut content = String::new();
        file.read_to_string(&mut content).unwrap();

        assert_eq!(content, "inside");
    }

    #[test]
    fn directory_creation_never_follows_an_existing_link() {
        let root_directory = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let link = root_directory.path().join("linked");
        if create_dir_link(outside.path(), &link).is_err() {
            return;
        }

        let root = RootCapability::open(root_directory.path().canonicalize().unwrap()).unwrap();
        let path = RelativePath::parse("linked/escaped".to_owned()).unwrap();
        assert!(root.ensure_directory_path(&path).is_err());
        assert!(!outside.path().join("escaped").exists());
    }

    #[test]
    fn directory_creation_returns_a_guard_for_the_exact_parent() {
        let root_directory = tempfile::tempdir().unwrap();
        let root = RootCapability::open(root_directory.path().canonicalize().unwrap()).unwrap();
        let path = RelativePath::parse("one/two".to_owned()).unwrap();

        let guard = root.ensure_directory_path(&path).unwrap();

        assert!(
            guard
                .child_is_absent(std::ffi::OsStr::new("attempt"))
                .unwrap()
        );
        std::fs::write(root_directory.path().join("one/two/attempt"), b"occupied").unwrap();
        assert!(
            !guard
                .child_is_absent(std::ffi::OsStr::new("attempt"))
                .unwrap()
        );
        assert!(
            guard
                .try_clone_final()
                .unwrap()
                .metadata()
                .unwrap()
                .is_dir()
        );
    }

    #[test]
    fn durable_identity_is_stable_domain_separated_and_redacted() {
        let root_directory = tempfile::tempdir().unwrap();
        let path = root_directory.path().canonicalize().unwrap();
        let first = RootCapability::open(&path).unwrap();
        let reopened = RootCapability::open(&path).unwrap();

        let common =
            DurableDirectoryIdentityV1::derive(&first, DirectoryIdentityDomain::CommonGit).unwrap();
        let common_reopened =
            DurableDirectoryIdentityV1::derive(&reopened, DirectoryIdentityDomain::CommonGit)
                .unwrap();
        let admin =
            DurableDirectoryIdentityV1::derive(&first, DirectoryIdentityDomain::WorktreeAdmin)
                .unwrap();

        assert_eq!(common, common_reopened);
        assert_ne!(common, admin);
        assert_eq!(common.algorithm(), "directory_identity_v1");
        assert_eq!(common.as_hex().len(), 64);
        assert!(
            common
                .as_hex()
                .bytes()
                .all(|byte| { byte.is_ascii_digit() || matches!(byte, b'a'..=b'f') })
        );
        assert_eq!(
            format!("{common:?}"),
            "DurableDirectoryIdentityV1(<redacted>)"
        );
    }

    #[test]
    fn durable_identity_is_stable_across_processes() {
        let root_directory = tempfile::tempdir().unwrap();
        let directory = root_directory.path().join("authenticated-directory");
        let output_path = root_directory.path().join("child-identity");
        std::fs::create_dir(&directory).unwrap();
        let directory = directory.canonicalize().unwrap();
        let parent = DurableDirectoryIdentityV1::derive(
            &RootCapability::open(&directory).unwrap(),
            DirectoryIdentityDomain::CommonGit,
        )
        .unwrap();

        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "root_capability::tests::durable_identity_child",
                "--nocapture",
            ])
            .env("CODING_AGENT_DURABLE_IDENTITY_CHILD", "1")
            .env("CODING_AGENT_DURABLE_IDENTITY_DIRECTORY", &directory)
            .env("CODING_AGENT_DURABLE_IDENTITY_OUTPUT", &output_path)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "durable identity child failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        let child = std::fs::read_to_string(output_path).unwrap();
        assert_eq!(child, parent.as_hex());
    }

    #[test]
    fn durable_identity_child() {
        if std::env::var_os("CODING_AGENT_DURABLE_IDENTITY_CHILD").is_none() {
            return;
        }
        let directory = std::env::var_os("CODING_AGENT_DURABLE_IDENTITY_DIRECTORY").unwrap();
        let output_path = std::env::var_os("CODING_AGENT_DURABLE_IDENTITY_OUTPUT").unwrap();
        let identity = DurableDirectoryIdentityV1::derive(
            &RootCapability::open(directory).unwrap(),
            DirectoryIdentityDomain::CommonGit,
        )
        .unwrap();
        std::fs::write(output_path, identity.as_hex()).unwrap();
    }

    #[test]
    fn replacing_a_directory_changes_its_durable_identity() {
        let parent = tempfile::tempdir().unwrap();
        let live = parent.path().join("live");
        let displaced = parent.path().join("displaced");
        std::fs::create_dir(&live).unwrap();
        let original = DurableDirectoryIdentityV1::derive(
            &RootCapability::open(live.canonicalize().unwrap()).unwrap(),
            DirectoryIdentityDomain::CommonGit,
        )
        .unwrap();

        std::fs::rename(&live, &displaced).unwrap();
        std::fs::create_dir(&live).unwrap();
        let replacement = DurableDirectoryIdentityV1::derive(
            &RootCapability::open(live.canonicalize().unwrap()).unwrap(),
            DirectoryIdentityDomain::CommonGit,
        )
        .unwrap();

        assert_ne!(original, replacement);
    }

    #[cfg(unix)]
    fn create_dir_link(target: &Path, link: &Path) -> io::Result<()> {
        std::os::unix::fs::symlink(target, link)
    }

    #[cfg(windows)]
    fn create_dir_link(target: &Path, link: &Path) -> io::Result<()> {
        std::os::windows::fs::symlink_dir(target, link)
    }
}
