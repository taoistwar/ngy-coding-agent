use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{self, Read, Write};

use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::native_fs::{
    child_file_matches, create_child_file_exclusive, open_child_directory, open_child_file,
    preserve_replace_metadata, publish_child_file, remove_child_file,
};
use crate::root_capability::{ensure_child_is_not_protected_metadata, ensure_plain_file};
use crate::{RelativePath, RootCapability};

const TEMPORARY_CREATE_ATTEMPTS: usize = 16;

#[derive(Debug)]
pub struct AtomicFileReplacer {
    root: RootCapability,
    limits: AtomicReplaceLimits,
}

impl AtomicFileReplacer {
    pub const fn new(root: RootCapability, limits: AtomicReplaceLimits) -> Self {
        Self { root, limits }
    }

    pub fn replace_file(
        &self,
        path: &RelativePath,
        expected_sha256: Option<&str>,
        content: &[u8],
        cancellation: &CancellationToken,
    ) -> Result<ReplaceFileResult, AtomicReplaceError> {
        self.replace_file_with_hook(path, expected_sha256, content, cancellation, &mut |_| {})
    }

    fn replace_file_with_hook(
        &self,
        path: &RelativePath,
        expected_sha256: Option<&str>,
        content: &[u8],
        cancellation: &CancellationToken,
        hook: &mut dyn FnMut(ReplacePhase),
    ) -> Result<ReplaceFileResult, AtomicReplaceError> {
        if path.is_root() {
            return Err(AtomicReplaceError::TargetNotRegular);
        }
        let expected = ExpectedTarget::parse(expected_sha256)?;
        if content.len() > self.limits.max_content_bytes {
            return Err(AtomicReplaceError::ContentTooLarge);
        }
        check_cancelled(cancellation)?;

        let (parent, target_name) = self
            .root
            .open_parent_directory(path)
            .map_err(map_parent_error)?;
        hook(ReplacePhase::ParentOpened);
        check_cancelled(cancellation)?;

        self.validate_target(&parent, &target_name, expected)?;
        let mut temporary = TemporaryFile::create(&parent)?;
        temporary
            .file
            .write_all(content)
            .and_then(|()| temporary.file.flush())
            .and_then(|()| temporary.file.sync_all())
            .map_err(AtomicReplaceError::AtomicReplaceFailed)?;
        hook(ReplacePhase::TemporarySynced);
        check_cancelled(cancellation)?;

        // Re-open and re-hash immediately before publication. Keeping this
        // handle alive also pins the exact validated object while the platform
        // performs the namespace operation. Cross-process linear CAS is not
        // promised, but ancestor replacement cannot redirect this operation.
        let current_target = self.validate_target(&parent, &target_name, expected)?;
        if let Some(target) = current_target.as_ref() {
            preserve_replace_metadata(target, &temporary.file)
                .and_then(|()| temporary.file.sync_all())
                .map_err(AtomicReplaceError::AtomicReplaceFailed)?;
        }
        // Windows non-POSIX rename can reject an otherwise share-delete target
        // while any target handle is open. The digest and metadata checks are
        // complete, so release our validation handle before the commit syscall.
        drop(current_target);
        hook(ReplacePhase::TargetRevalidated);
        check_cancelled(cancellation)?;
        // POSIX has no portable rename-by-fd operation. This closes swaps that
        // completed before the final check; as specified, it is not a linear
        // CAS against a concurrently mutating same-user namespace actor.
        if !child_file_matches(&parent, &temporary.name, &temporary.file)
            .map_err(AtomicReplaceError::AtomicReplaceFailed)?
        {
            return Err(AtomicReplaceError::TemporaryIdentityChanged);
        }

        let replace = matches!(expected, ExpectedTarget::Digest(_));
        if let Err(error) = publish_child_file(
            &temporary.file,
            &parent,
            &temporary.name,
            &target_name,
            replace,
        ) {
            return Err(map_publication_error(error, expected));
        }

        // Publication is the commit point. Set this before every subsequent
        // observation so cancellation or best-effort durability work can never
        // turn a committed replacement into a reported cancellation/failure.
        temporary.committed = true;
        hook(ReplacePhase::Published);

        Ok(ReplaceFileResult {
            disposition: if replace {
                ReplaceDisposition::Replaced
            } else {
                ReplaceDisposition::Created
            },
            sha256: hex_digest(content),
            bytes_written: content.len() as u64,
        })
    }

    fn validate_target(
        &self,
        parent: &File,
        target_name: &OsStr,
        expected: ExpectedTarget<'_>,
    ) -> Result<Option<File>, AtomicReplaceError> {
        match open_child_file(parent, target_name) {
            Ok(mut file) => {
                ensure_child_is_not_protected_metadata(parent, &file)
                    .map_err(AtomicReplaceError::AtomicReplaceFailed)?;
                if ensure_plain_file(&file).is_err() {
                    return Err(match expected {
                        ExpectedTarget::Absent => AtomicReplaceError::TargetAlreadyExists,
                        ExpectedTarget::Digest(_) => AtomicReplaceError::TargetNotRegular,
                    });
                }
                match expected {
                    ExpectedTarget::Absent => Err(AtomicReplaceError::TargetAlreadyExists),
                    ExpectedTarget::Digest(expected_digest) => {
                        let actual = digest_open_file(&mut file, self.limits.max_content_bytes)?;
                        if actual == expected_digest {
                            Ok(Some(file))
                        } else {
                            Err(AtomicReplaceError::FileChangedSinceRead)
                        }
                    }
                }
            }
            Err(file_error) => match open_child_directory(parent, target_name) {
                Ok(directory) => {
                    ensure_child_is_not_protected_metadata(parent, &directory)
                        .map_err(AtomicReplaceError::AtomicReplaceFailed)?;
                    match expected {
                        ExpectedTarget::Absent => Err(AtomicReplaceError::TargetAlreadyExists),
                        ExpectedTarget::Digest(_) => Err(AtomicReplaceError::TargetNotRegular),
                    }
                }
                Err(directory_error)
                    if file_error.kind() == io::ErrorKind::NotFound
                        && directory_error.kind() == io::ErrorKind::NotFound =>
                {
                    match expected {
                        ExpectedTarget::Absent => Ok(None),
                        ExpectedTarget::Digest(_) => Err(AtomicReplaceError::TargetNotFound),
                    }
                }
                Err(_) => Err(map_target_open_error(file_error, expected)),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpectedTarget<'a> {
    Absent,
    Digest(&'a str),
}

impl<'a> ExpectedTarget<'a> {
    fn parse(value: Option<&'a str>) -> Result<Self, AtomicReplaceError> {
        match value {
            None => Ok(Self::Absent),
            Some(value)
                if value.len() == 64
                    && value
                        .as_bytes()
                        .iter()
                        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')) =>
            {
                Ok(Self::Digest(value))
            }
            Some(_) => Err(AtomicReplaceError::InvalidExpectedDigest),
        }
    }
}

struct TemporaryFile<'a> {
    file: File,
    parent: &'a File,
    name: OsString,
    committed: bool,
}

impl<'a> TemporaryFile<'a> {
    fn create(parent: &'a File) -> Result<Self, AtomicReplaceError> {
        for _ in 0..TEMPORARY_CREATE_ATTEMPTS {
            let name = random_temporary_name()?;
            match create_child_file_exclusive(parent, &name) {
                Ok(file) => {
                    return Ok(Self {
                        file,
                        parent,
                        name,
                        committed: false,
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(AtomicReplaceError::AtomicReplaceFailed(error)),
            }
        }
        Err(AtomicReplaceError::AtomicReplaceFailed(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not reserve an exclusive temporary file",
        )))
    }
}

impl Drop for TemporaryFile<'_> {
    fn drop(&mut self) {
        // Avoid unlinking a foreign entry after an observed name swap. This is
        // best-effort cleanup under the same non-linear-CAS boundary as publish.
        if !self.committed
            && child_file_matches(self.parent, &self.name, &self.file).unwrap_or(false)
        {
            let _ = remove_child_file(self.parent, &self.name, &self.file);
        }
    }
}

fn random_temporary_name() -> Result<OsString, AtomicReplaceError> {
    let mut random = [0u8; 16];
    getrandom::fill(&mut random).map_err(|error| {
        AtomicReplaceError::AtomicReplaceFailed(io::Error::other(error.to_string()))
    })?;
    let mut name = String::with_capacity(32 + random.len() * 2);
    name.push_str(".coding-agent-replace-");
    for byte in random {
        use std::fmt::Write as _;
        write!(&mut name, "{byte:02x}").expect("writing to String cannot fail");
    }
    name.push_str(".tmp");
    Ok(OsString::from(name))
}

fn digest_open_file(file: &mut File, max_bytes: usize) -> Result<String, AtomicReplaceError> {
    if file
        .metadata()
        .map_err(AtomicReplaceError::AtomicReplaceFailed)?
        .len()
        > max_bytes as u64
    {
        return Err(AtomicReplaceError::TargetTooLarge);
    }
    let take_limit = u64::try_from(max_bytes)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let mut bytes = Vec::with_capacity(max_bytes.min(64 * 1_024));
    file.take(take_limit)
        .read_to_end(&mut bytes)
        .map_err(AtomicReplaceError::AtomicReplaceFailed)?;
    if bytes.len() > max_bytes {
        return Err(AtomicReplaceError::TargetTooLarge);
    }
    Ok(hex_digest(&bytes))
}

fn hex_digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn check_cancelled(cancellation: &CancellationToken) -> Result<(), AtomicReplaceError> {
    if cancellation.is_cancelled() {
        Err(AtomicReplaceError::Cancelled)
    } else {
        Ok(())
    }
}

fn map_parent_error(error: io::Error) -> AtomicReplaceError {
    match error.kind() {
        io::ErrorKind::InvalidInput | io::ErrorKind::InvalidData | io::ErrorKind::NotFound => {
            AtomicReplaceError::ParentNotFound
        }
        _ => AtomicReplaceError::AtomicReplaceFailed(error),
    }
}

fn map_target_open_error(error: io::Error, expected: ExpectedTarget<'_>) -> AtomicReplaceError {
    match error.kind() {
        io::ErrorKind::InvalidInput
        | io::ErrorKind::InvalidData
        | io::ErrorKind::IsADirectory
        | io::ErrorKind::NotADirectory => match expected {
            ExpectedTarget::Absent => AtomicReplaceError::TargetAlreadyExists,
            ExpectedTarget::Digest(_) => AtomicReplaceError::TargetNotRegular,
        },
        _ if is_link_error(&error) => match expected {
            ExpectedTarget::Absent => AtomicReplaceError::TargetAlreadyExists,
            ExpectedTarget::Digest(_) => AtomicReplaceError::TargetNotRegular,
        },
        _ => AtomicReplaceError::AtomicReplaceFailed(error),
    }
}

#[cfg(unix)]
fn is_link_error(error: &io::Error) -> bool {
    error.raw_os_error() == Some(libc::ELOOP)
}

#[cfg(windows)]
fn is_link_error(_: &io::Error) -> bool {
    false
}

fn map_publication_error(error: io::Error, expected: ExpectedTarget<'_>) -> AtomicReplaceError {
    match (expected, error.kind()) {
        (ExpectedTarget::Absent, io::ErrorKind::AlreadyExists) => {
            AtomicReplaceError::TargetAlreadyExists
        }
        (ExpectedTarget::Digest(_), io::ErrorKind::NotFound) => {
            AtomicReplaceError::FileChangedSinceRead
        }
        _ => AtomicReplaceError::AtomicReplaceFailed(error),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplacePhase {
    ParentOpened,
    TemporarySynced,
    TargetRevalidated,
    Published,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AtomicReplaceLimits {
    max_content_bytes: usize,
}

impl AtomicReplaceLimits {
    pub fn try_new(max_content_bytes: usize) -> Result<Self, AtomicReplaceLimitsError> {
        if max_content_bytes == 0 {
            Err(AtomicReplaceLimitsError)
        } else {
            Ok(Self { max_content_bytes })
        }
    }

    pub const fn max_content_bytes(self) -> usize {
        self.max_content_bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("atomic replacement limits must be non-zero")]
pub struct AtomicReplaceLimitsError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplaceDisposition {
    Created,
    Replaced,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplaceFileResult {
    pub disposition: ReplaceDisposition,
    pub sha256: String,
    pub bytes_written: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum AtomicReplaceError {
    #[error("replacement was cancelled before publication")]
    Cancelled,
    #[error("expected SHA-256 must be 64 lowercase hexadecimal characters")]
    InvalidExpectedDigest,
    #[error("replacement content exceeds its configured byte limit")]
    ContentTooLarge,
    #[error("target file exceeds its configured byte limit")]
    TargetTooLarge,
    #[error("target parent directory was not found")]
    ParentNotFound,
    #[error("target was not found")]
    TargetNotFound,
    #[error("target already exists")]
    TargetAlreadyExists,
    #[error("target is not a plain regular file")]
    TargetNotRegular,
    #[error("file changed since it was read")]
    FileChangedSinceRead,
    #[error("temporary file identity changed before publication")]
    TemporaryIdentityChanged,
    #[error("atomic replacement failed")]
    AtomicReplaceFailed(#[source] io::Error),
}

impl AtomicReplaceError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Cancelled => "CANCELLED",
            Self::InvalidExpectedDigest => "INVALID_EXPECTED_SHA256",
            Self::ContentTooLarge | Self::TargetTooLarge => "FILE_TOO_LARGE",
            Self::ParentNotFound | Self::TargetNotFound => "FILE_NOT_FOUND",
            Self::TargetAlreadyExists | Self::FileChangedSinceRead => "FILE_CHANGED_SINCE_READ",
            Self::TargetNotRegular => "FILE_NOT_REGULAR",
            Self::TemporaryIdentityChanged | Self::AtomicReplaceFailed(_) => {
                "ATOMIC_REPLACE_FAILED"
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_after_sync_removes_the_exclusive_same_directory_temporary_file() {
        let temp = tempfile::tempdir().unwrap();
        let root = RootCapability::open(temp.path()).unwrap();
        let replacer = AtomicFileReplacer::new(root, AtomicReplaceLimits::try_new(1_024).unwrap());
        let cancellation = CancellationToken::new();
        let mut observed_temporary = false;

        let result = replacer.replace_file_with_hook(
            &RelativePath::parse("new.txt").unwrap(),
            None,
            b"durable content",
            &cancellation,
            &mut |phase| {
                if phase == ReplacePhase::TemporarySynced {
                    let temporary = temporary_path(temp.path());
                    let create_error = std::fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&temporary)
                        .unwrap_err();
                    assert_exclusive_create_failed(&create_error);
                    observed_temporary = true;
                    cancellation.cancel();
                }
            },
        );

        assert!(
            matches!(result, Err(AtomicReplaceError::Cancelled)),
            "unexpected replacement result: {result:?}; temporary observed={observed_temporary}"
        );
        assert!(observed_temporary);
        assert!(!temp.path().join("new.txt").exists());
        assert!(temporary_names(temp.path()).is_empty());
    }

    #[test]
    fn an_ancestor_namespace_swap_cannot_redirect_publication() {
        let temp = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let safe = temp.path().join("safe");
        let held = temp.path().join("held");
        std::fs::create_dir(&safe).unwrap();
        let root = RootCapability::open(temp.path()).unwrap();
        let replacer = AtomicFileReplacer::new(root, AtomicReplaceLimits::try_new(1_024).unwrap());

        replacer
            .replace_file_with_hook(
                &RelativePath::parse("safe/new.txt").unwrap(),
                None,
                b"inside",
                &CancellationToken::new(),
                &mut |phase| {
                    if phase == ReplacePhase::ParentOpened {
                        std::fs::rename(&safe, &held).unwrap();
                        create_dir_link(outside.path(), &safe).unwrap();
                    }
                },
            )
            .unwrap();

        assert_eq!(std::fs::read(held.join("new.txt")).unwrap(), b"inside");
        assert!(!outside.path().join("new.txt").exists());
    }

    #[test]
    fn cancellation_observed_after_publication_does_not_change_the_committed_result() {
        let temp = tempfile::tempdir().unwrap();
        let root = RootCapability::open(temp.path()).unwrap();
        let replacer = AtomicFileReplacer::new(root, AtomicReplaceLimits::try_new(1_024).unwrap());
        let cancellation = CancellationToken::new();

        let result = replacer.replace_file_with_hook(
            &RelativePath::parse("new.txt").unwrap(),
            None,
            b"committed",
            &cancellation,
            &mut |phase| {
                if phase == ReplacePhase::Published {
                    cancellation.cancel();
                }
            },
        );

        assert!(result.is_ok(), "unexpected replacement result: {result:?}");
        assert!(cancellation.is_cancelled());
        assert_eq!(
            std::fs::read(temp.path().join("new.txt")).unwrap(),
            b"committed"
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_swapped_temporary_leaf_is_never_published_or_deleted_as_if_it_were_ours() {
        let temp = tempfile::tempdir().unwrap();
        let root = RootCapability::open(temp.path()).unwrap();
        let replacer = AtomicFileReplacer::new(root, AtomicReplaceLimits::try_new(1_024).unwrap());
        let held = temp.path().join("held-original.tmp");
        let mut decoy = None;

        let result = replacer.replace_file_with_hook(
            &RelativePath::parse("new.txt").unwrap(),
            None,
            b"intended",
            &CancellationToken::new(),
            &mut |phase| {
                if phase == ReplacePhase::TemporarySynced {
                    let temporary = temporary_path(temp.path());
                    std::fs::rename(&temporary, &held).unwrap();
                    std::fs::write(&temporary, b"decoy").unwrap();
                    decoy = Some(temporary);
                }
            },
        );

        assert!(matches!(
            result,
            Err(AtomicReplaceError::TemporaryIdentityChanged)
        ));
        assert!(!temp.path().join("new.txt").exists());
        assert_eq!(std::fs::read(held).unwrap(), b"intended");
        assert_eq!(std::fs::read(decoy.unwrap()).unwrap(), b"decoy");
    }

    fn temporary_path(directory: &std::path::Path) -> std::path::PathBuf {
        let names = temporary_names(directory);
        assert_eq!(names.len(), 1);
        directory.join(&names[0])
    }

    fn temporary_names(directory: &std::path::Path) -> Vec<OsString> {
        std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .filter(|name| name.to_string_lossy().starts_with(".coding-agent-replace-"))
            .collect()
    }

    #[cfg(unix)]
    fn assert_exclusive_create_failed(error: &io::Error) {
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    }

    #[cfg(windows)]
    fn assert_exclusive_create_failed(error: &io::Error) {
        use windows_sys::Win32::Foundation::ERROR_SHARING_VIOLATION;

        assert!(
            error.kind() == io::ErrorKind::AlreadyExists
                || error.raw_os_error() == Some(ERROR_SHARING_VIOLATION as i32),
            "unexpected exclusive-create error: {error:?}"
        );
    }

    #[cfg(unix)]
    fn create_dir_link(target: &std::path::Path, link: &std::path::Path) -> io::Result<()> {
        std::os::unix::fs::symlink(target, link)
    }

    #[cfg(windows)]
    fn create_dir_link(target: &std::path::Path, link: &std::path::Path) -> io::Result<()> {
        std::os::windows::fs::symlink_dir(target, link)
    }
}
