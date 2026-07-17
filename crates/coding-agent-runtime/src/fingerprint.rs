use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fs::{File, Metadata};
use std::io::{self, Read};
use std::path::Path;
#[cfg(windows)]
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use coding_agent_core::WorkspaceFingerprint;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::command_policy::{
    CommandPolicyError, ExecutionDirectory, GitCommandBinding, ValidatedCommand,
};
#[cfg(unix)]
use crate::native_fs::child_file_matches;
#[cfg(windows)]
use crate::native_fs::reopen_file_read_lease;
use crate::native_fs::{open_child_directory, open_child_file};
use crate::process_supervisor::{
    CapturedStream, ChildEnvironment, CommandResult, PlatformEnvironment, ProcessError,
    ProcessLimits, ProcessSupervisor,
};
use crate::root_capability::{ensure_plain_directory, ensure_plain_file};
use crate::tool_discovery::ToolchainPaths;

const FINGERPRINT_DOMAIN: &[u8] = b"coding-agent-workspace-fingerprint-v1\0";
const MAX_GIT_PATH_BYTES: usize = 4_096;
const MAX_GIT_COMPONENT_BYTES: usize = 255;
const STREAM_BUFFER_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FingerprintLimits {
    git_timeout: Duration,
    max_files: usize,
    max_file_bytes: u64,
    max_total_bytes: u64,
}

impl FingerprintLimits {
    pub fn try_new(
        git_timeout: Duration,
        max_files: usize,
        max_file_bytes: u64,
        max_total_bytes: u64,
    ) -> Result<Self, FingerprintError> {
        if git_timeout.is_zero() || max_files == 0 || max_file_bytes == 0 || max_total_bytes == 0 {
            return Err(FingerprintError::InvalidLimits);
        }
        Ok(Self {
            git_timeout,
            max_files,
            max_file_bytes,
            max_total_bytes,
        })
    }
}

/// Produces a stable, capability-scoped digest of every tracked and every
/// non-ignored untracked worktree entry.
#[derive(Debug)]
pub struct WorkspaceFingerprinter {
    supervisor: ProcessSupervisor,
    git: Arc<crate::PinnedExecutable>,
    binding: GitCommandBinding,
    environment: ChildEnvironment,
    work_tree: Arc<ExecutionDirectory>,
    limits: FingerprintLimits,
}

impl WorkspaceFingerprinter {
    pub fn from_trusted_capabilities(
        toolchain: &ToolchainPaths,
        git_directory: Arc<ExecutionDirectory>,
        work_tree: Arc<ExecutionDirectory>,
        temporary_directory: impl AsRef<Path>,
        process_limits: ProcessLimits,
        limits: FingerprintLimits,
    ) -> Result<Self, FingerprintError> {
        let binding = GitCommandBinding::try_new(git_directory, Arc::clone(&work_tree))
            .map_err(FingerprintError::CommandPolicy)?;
        let platform = platform_environment(temporary_directory.as_ref())?;
        Ok(Self {
            supervisor: ProcessSupervisor::new(process_limits),
            git: toolchain.git(),
            binding,
            environment: ChildEnvironment::for_git(&platform),
            work_tree,
            limits,
        })
    }

    pub async fn collect(
        &self,
        cancellation: CancellationToken,
    ) -> Result<WorkspaceFingerprint, FingerprintError> {
        check_cancelled(&cancellation)?;
        let status_before = self.read_status(cancellation.clone()).await?;
        let tracked_before = self.read_tracked(cancellation.clone()).await?;
        let untracked_before = self.read_untracked(cancellation.clone()).await?;
        let first_entries =
            parse_entries(&tracked_before, &untracked_before, self.limits.max_files)?;
        let first = self.hash_entries(first_entries, &cancellation)?;

        let tracked_after = self.read_tracked(cancellation.clone()).await?;
        let untracked_after = self.read_untracked(cancellation.clone()).await?;
        let second_entries =
            parse_entries(&tracked_after, &untracked_after, self.limits.max_files)?;
        let second = self.hash_entries(second_entries, &cancellation)?;
        let status_after = self.read_status(cancellation).await?;

        if status_before != status_after
            || tracked_before != tracked_after
            || untracked_before != untracked_after
            || first.fingerprint != second.fingerprint
        {
            return Err(FingerprintError::WorkspaceChanged);
        }
        drop(first.leases);
        drop(second.leases);
        Ok(second.fingerprint)
    }

    async fn read_status(
        &self,
        cancellation: CancellationToken,
    ) -> Result<Vec<u8>, FingerprintError> {
        let command = ValidatedCommand::git_status(
            Arc::clone(&self.git),
            &self.binding,
            self.environment.clone(),
            self.limits.git_timeout,
        )
        .map_err(FingerprintError::CommandPolicy)?;
        self.run_machine_command(command, cancellation).await
    }

    async fn read_tracked(
        &self,
        cancellation: CancellationToken,
    ) -> Result<Vec<u8>, FingerprintError> {
        let command = ValidatedCommand::git_fingerprint_tracked_paths(
            Arc::clone(&self.git),
            &self.binding,
            self.environment.clone(),
            self.limits.git_timeout,
        )
        .map_err(FingerprintError::CommandPolicy)?;
        self.run_machine_command(command, cancellation).await
    }

    async fn read_untracked(
        &self,
        cancellation: CancellationToken,
    ) -> Result<Vec<u8>, FingerprintError> {
        let command = ValidatedCommand::git_fingerprint_untracked_paths(
            Arc::clone(&self.git),
            &self.binding,
            self.environment.clone(),
            self.limits.git_timeout,
        )
        .map_err(FingerprintError::CommandPolicy)?;
        self.run_machine_command(command, cancellation).await
    }

    async fn run_machine_command(
        &self,
        command: ValidatedCommand,
        cancellation: CancellationToken,
    ) -> Result<Vec<u8>, FingerprintError> {
        let result = self
            .supervisor
            .run(command, cancellation)
            .await
            .map_err(FingerprintError::Process)?;
        machine_stdout(&result)
    }

    fn hash_entries(
        &self,
        entries: BTreeMap<Vec<u8>, FingerprintEntry>,
        cancellation: &CancellationToken,
    ) -> Result<HashedWorkspace, FingerprintError> {
        let mut hasher = Sha256::new();
        hasher.update(FINGERPRINT_DOMAIN);
        let mut total_bytes = 0u64;
        #[cfg(windows)]
        let mut leases = Vec::new();
        #[cfg(not(windows))]
        let leases = Vec::new();

        for (raw_path, entry) in entries {
            check_cancelled(cancellation)?;
            hash_frame(&mut hasher, 1, &raw_path)?;
            match &entry.origin {
                EntryOrigin::Tracked { metadata } => hash_frame(&mut hasher, 2, metadata)?,
                EntryOrigin::Untracked => hash_frame(&mut hasher, 3, &[])?,
            }
            match open_worktree_file(&self.work_tree, &entry.path) {
                Ok(mut opened) => {
                    #[cfg(windows)]
                    let lease = reopen_file_read_lease(&opened.file)
                        .map_err(FingerprintError::UnsafeEntry)?;
                    let before = opened
                        .file
                        .metadata()
                        .map_err(FingerprintError::UnsafeEntry)?;
                    ensure_plain_file(&opened.file).map_err(FingerprintError::UnsafeEntry)?;
                    let length = before.len();
                    if length > self.limits.max_file_bytes {
                        return Err(FingerprintError::FileTooLarge);
                    }
                    total_bytes = total_bytes
                        .checked_add(length)
                        .ok_or(FingerprintError::TotalTooLarge)?;
                    if total_bytes > self.limits.max_total_bytes {
                        return Err(FingerprintError::TotalTooLarge);
                    }
                    hash_file_type(&mut hasher, &before)?;
                    hasher.update(length.to_be_bytes());
                    stream_file(
                        &mut opened.file,
                        length,
                        self.limits.max_file_bytes,
                        cancellation,
                        &mut hasher,
                    )?;
                    let after = opened
                        .file
                        .metadata()
                        .map_err(FingerprintError::UnsafeEntry)?;
                    if !same_observed_file(&before, &after)
                        || !worktree_child_matches(&opened.parent, &opened.name, &opened.file)
                            .map_err(FingerprintError::UnsafeEntry)?
                    {
                        return Err(FingerprintError::WorkspaceChanged);
                    }
                    #[cfg(windows)]
                    leases.push(lease);
                }
                Err(OpenWorktreeError::Missing)
                    if matches!(entry.origin, EntryOrigin::Tracked { .. }) =>
                {
                    hash_frame(&mut hasher, 4, &[])?;
                }
                Err(OpenWorktreeError::Missing) => {
                    return Err(FingerprintError::WorkspaceChanged);
                }
                Err(OpenWorktreeError::Unsafe(error)) => {
                    return Err(FingerprintError::UnsafeEntry(error));
                }
            }
        }
        hasher.update(total_bytes.to_be_bytes());
        Ok(HashedWorkspace {
            fingerprint: WorkspaceFingerprint::from_bytes(hasher.finalize().into()),
            leases,
        })
    }
}

struct HashedWorkspace {
    fingerprint: WorkspaceFingerprint,
    leases: Vec<File>,
}

#[derive(Debug)]
struct FingerprintEntry {
    path: RawGitPath,
    origin: EntryOrigin,
}

#[derive(Debug)]
enum EntryOrigin {
    Tracked { metadata: Vec<u8> },
    Untracked,
}

fn parse_entries(
    tracked: &[u8],
    untracked: &[u8],
    max_files: usize,
) -> Result<BTreeMap<Vec<u8>, FingerprintEntry>, FingerprintError> {
    let mut entries = BTreeMap::new();
    for record in nul_records(tracked)? {
        let tab = record
            .iter()
            .position(|byte| *byte == b'\t')
            .ok_or(FingerprintError::ListingInvalid)?;
        let metadata = &record[..tab];
        let path = RawGitPath::parse(&record[tab + 1..])?;
        validate_tracked_metadata(metadata)?;
        insert_entry(
            &mut entries,
            max_files,
            path,
            EntryOrigin::Tracked {
                metadata: metadata.to_vec(),
            },
        )?;
    }
    for record in nul_records(untracked)? {
        let path = RawGitPath::parse(record)?;
        insert_entry(&mut entries, max_files, path, EntryOrigin::Untracked)?;
    }
    Ok(entries)
}

fn insert_entry(
    entries: &mut BTreeMap<Vec<u8>, FingerprintEntry>,
    max_files: usize,
    path: RawGitPath,
    origin: EntryOrigin,
) -> Result<(), FingerprintError> {
    if entries.len() >= max_files {
        return Err(FingerprintError::TooManyFiles);
    }
    if entries
        .insert(path.raw.clone(), FingerprintEntry { path, origin })
        .is_some()
    {
        return Err(FingerprintError::ListingInvalid);
    }
    Ok(())
}

fn nul_records(output: &[u8]) -> Result<Vec<&[u8]>, FingerprintError> {
    if output.is_empty() {
        return Ok(Vec::new());
    }
    let body = output
        .strip_suffix(&[0])
        .ok_or(FingerprintError::ListingInvalid)?;
    let records = body.split(|byte| *byte == 0).collect::<Vec<_>>();
    if records.iter().any(|record| record.is_empty()) {
        return Err(FingerprintError::ListingInvalid);
    }
    Ok(records)
}

fn validate_tracked_metadata(metadata: &[u8]) -> Result<(), FingerprintError> {
    let fields = metadata.split(|byte| *byte == b' ').collect::<Vec<_>>();
    if fields.len() != 4
        || fields[0].len() != 1
        || !matches!(fields[0][0], b'H' | b'C' | b'R')
        || fields[2].len() != 40 && fields[2].len() != 64
        || !fields[2].iter().all(u8::is_ascii_hexdigit)
        || fields[3] != b"0"
    {
        return Err(FingerprintError::UnsupportedEntry);
    }
    match fields[1] {
        b"100644" | b"100755" => Ok(()),
        // Symlinks, gitlinks/submodules, sparse-directory entries and all
        // other non-regular index types are outside Project 2's safe set.
        _ => Err(FingerprintError::UnsupportedEntry),
    }
}

#[derive(Debug)]
struct RawGitPath {
    raw: Vec<u8>,
    components: Vec<Vec<u8>>,
}

impl RawGitPath {
    fn parse(raw: &[u8]) -> Result<Self, FingerprintError> {
        if raw.is_empty() || raw.len() > MAX_GIT_PATH_BYTES || raw.contains(&0) {
            return Err(FingerprintError::PathInvalid);
        }
        #[cfg(windows)]
        {
            let path = std::str::from_utf8(raw).map_err(|_| FingerprintError::PathInvalid)?;
            crate::RelativePath::parse(path.to_owned())
                .map_err(|_| FingerprintError::PathInvalid)?;
        }
        let components = raw.split(|byte| *byte == b'/').collect::<Vec<_>>();
        if components.iter().any(|component| {
            component.is_empty()
                || component.len() > MAX_GIT_COMPONENT_BYTES
                || *component == b"."
                || *component == b".."
                || component.eq_ignore_ascii_case(b".git")
        }) {
            return Err(FingerprintError::PathInvalid);
        }
        Ok(Self {
            raw: raw.to_vec(),
            components: components.into_iter().map(<[u8]>::to_vec).collect(),
        })
    }

    fn component_os_string(component: &[u8]) -> Result<OsString, FingerprintError> {
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;
            Ok(OsString::from_vec(component.to_vec()))
        }
        #[cfg(windows)]
        {
            std::str::from_utf8(component)
                .map(OsString::from)
                .map_err(|_| FingerprintError::PathInvalid)
        }
    }
}

struct OpenedWorktreeFile {
    parent: File,
    name: OsString,
    file: File,
}

enum OpenWorktreeError {
    Missing,
    Unsafe(io::Error),
}

fn open_worktree_file(
    work_tree: &ExecutionDirectory,
    path: &RawGitPath,
) -> Result<OpenedWorktreeFile, OpenWorktreeError> {
    let mut parent = work_tree
        .cloned_directory()
        .map_err(|error| OpenWorktreeError::Unsafe(io::Error::other(error)))?;
    ensure_plain_directory(&parent).map_err(OpenWorktreeError::Unsafe)?;
    for (index, component) in path.components.iter().enumerate() {
        let component = RawGitPath::component_os_string(component)
            .map_err(|error| OpenWorktreeError::Unsafe(io::Error::other(error)))?;
        if index + 1 == path.components.len() {
            let file = match open_child_file(&parent, &component) {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    return Err(OpenWorktreeError::Missing);
                }
                Err(error) => return Err(OpenWorktreeError::Unsafe(error)),
            };
            ensure_plain_file(&file).map_err(OpenWorktreeError::Unsafe)?;
            return Ok(OpenedWorktreeFile {
                parent,
                name: component,
                file,
            });
        }
        parent = match open_child_directory(&parent, &component) {
            Ok(directory) => directory,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(OpenWorktreeError::Missing);
            }
            Err(error) => return Err(OpenWorktreeError::Unsafe(error)),
        };
        ensure_plain_directory(&parent).map_err(OpenWorktreeError::Unsafe)?;
    }
    Err(OpenWorktreeError::Unsafe(io::Error::new(
        io::ErrorKind::InvalidInput,
        "empty Git path",
    )))
}

fn stream_file(
    file: &mut File,
    expected_length: u64,
    maximum: u64,
    cancellation: &CancellationToken,
    hasher: &mut Sha256,
) -> Result<(), FingerprintError> {
    let mut buffer = [0u8; STREAM_BUFFER_BYTES];
    let mut observed = 0u64;
    loop {
        check_cancelled(cancellation)?;
        let read = file
            .read(&mut buffer)
            .map_err(FingerprintError::UnsafeEntry)?;
        if read == 0 {
            break;
        }
        observed = observed
            .checked_add(read as u64)
            .ok_or(FingerprintError::FileTooLarge)?;
        if observed > maximum || observed > expected_length {
            return Err(FingerprintError::WorkspaceChanged);
        }
        hasher.update(&buffer[..read]);
    }
    if observed != expected_length {
        return Err(FingerprintError::WorkspaceChanged);
    }
    Ok(())
}

fn hash_frame(hasher: &mut Sha256, tag: u8, bytes: &[u8]) -> Result<(), FingerprintError> {
    let length = u64::try_from(bytes.len()).map_err(|_| FingerprintError::TotalTooLarge)?;
    hasher.update([tag]);
    hasher.update(length.to_be_bytes());
    hasher.update(bytes);
    Ok(())
}

#[cfg(unix)]
fn hash_file_type(hasher: &mut Sha256, metadata: &Metadata) -> Result<(), FingerprintError> {
    use std::os::unix::fs::MetadataExt;
    if !metadata.file_type().is_file() {
        return Err(FingerprintError::UnsupportedEntry);
    }
    hasher.update([5]);
    hasher.update((metadata.mode() & 0o7777).to_be_bytes());
    Ok(())
}

#[cfg(windows)]
fn hash_file_type(hasher: &mut Sha256, metadata: &Metadata) -> Result<(), FingerprintError> {
    use std::os::windows::fs::MetadataExt;
    if !metadata.file_type().is_file() {
        return Err(FingerprintError::UnsupportedEntry);
    }
    hasher.update([5]);
    hasher.update(metadata.file_attributes().to_be_bytes());
    Ok(())
}

#[cfg(unix)]
fn same_observed_file(before: &Metadata, after: &Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    before.dev() == after.dev()
        && before.ino() == after.ino()
        && before.len() == after.len()
        && before.mode() == after.mode()
        && before.mtime() == after.mtime()
        && before.mtime_nsec() == after.mtime_nsec()
        && before.ctime() == after.ctime()
        && before.ctime_nsec() == after.ctime_nsec()
}

#[cfg(windows)]
fn same_observed_file(before: &Metadata, after: &Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    before.file_size() == after.file_size()
        && before.last_write_time() == after.last_write_time()
        && before.creation_time() == after.creation_time()
        && before.file_attributes() == after.file_attributes()
}

#[cfg(unix)]
fn worktree_child_matches(parent: &File, name: &OsStr, file: &File) -> io::Result<bool> {
    child_file_matches(parent, name, file)
}

#[cfg(windows)]
fn worktree_child_matches(parent: &File, name: &OsStr, file: &File) -> io::Result<bool> {
    let reopened = open_child_file(parent, name)?;
    ensure_plain_file(&reopened)?;
    Ok(windows_file_identity(file)? == windows_file_identity(&reopened)?)
}

#[cfg(windows)]
fn windows_file_identity(file: &File) -> io::Result<(u64, u64)> {
    use std::mem::MaybeUninit;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
    };

    let mut information = MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::zeroed();
    if unsafe {
        GetFileInformationByHandle(file.as_raw_handle() as HANDLE, information.as_mut_ptr())
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    let information = unsafe { information.assume_init() };
    Ok((
        u64::from(information.dwVolumeSerialNumber),
        (u64::from(information.nFileIndexHigh) << 32) | u64::from(information.nFileIndexLow),
    ))
}

fn machine_stdout(result: &CommandResult) -> Result<Vec<u8>, FingerprintError> {
    if result.cancelled {
        return Err(FingerprintError::Cancelled);
    }
    if result.timed_out {
        return Err(FingerprintError::TimedOut);
    }
    if result.exit_code != Some(0) || result.signal.is_some() {
        return Err(FingerprintError::GitCommandFailed);
    }
    complete_stdout(&result.stdout)
}

fn complete_stdout(stream: &CapturedStream) -> Result<Vec<u8>, FingerprintError> {
    let retained = stream.head.len().saturating_add(stream.tail.len());
    if !stream.complete
        || stream.truncated
        || stream.omitted_observed_bytes != 0
        || stream.observed_bytes != retained as u64
    {
        return Err(FingerprintError::OutputIncomplete);
    }
    let mut output = Vec::with_capacity(retained);
    output.extend_from_slice(&stream.head);
    output.extend_from_slice(&stream.tail);
    Ok(output)
}

fn check_cancelled(cancellation: &CancellationToken) -> Result<(), FingerprintError> {
    if cancellation.is_cancelled() {
        Err(FingerprintError::Cancelled)
    } else {
        Ok(())
    }
}

fn platform_environment(path: &Path) -> Result<PlatformEnvironment, FingerprintError> {
    #[cfg(windows)]
    let system_root = std::env::var_os("SYSTEMROOT")
        .or_else(|| std::env::var_os("WINDIR"))
        .map(PathBuf::from);
    #[cfg(unix)]
    let system_root = None;

    PlatformEnvironment::try_new(path.to_owned(), system_root)
        .map_err(|_| FingerprintError::InvalidEnvironment)
}

#[derive(Debug, thiserror::Error)]
pub enum FingerprintError {
    #[error("fingerprint limits must all be non-zero")]
    InvalidLimits,
    #[error("the fingerprint child-process environment is invalid")]
    InvalidEnvironment,
    #[error("the fingerprint Git command was rejected by typed policy")]
    CommandPolicy(#[source] CommandPolicyError),
    #[error("the supervised fingerprint process failed")]
    Process(#[source] ProcessError),
    #[error("fingerprint collection was cancelled")]
    Cancelled,
    #[error("fingerprint Git collection timed out")]
    TimedOut,
    #[error("a fingerprint Git command failed")]
    GitCommandFailed,
    #[error("fingerprint Git output was incomplete")]
    OutputIncomplete,
    #[error("fingerprint Git output was malformed")]
    ListingInvalid,
    #[error("Git reported an unsupported symlink, gitlink, sparse or conflicted entry")]
    UnsupportedEntry,
    #[error("Git reported an invalid or protected worktree path")]
    PathInvalid,
    #[error("a worktree entry could not be opened without following it")]
    UnsafeEntry(#[source] io::Error),
    #[error("the workspace contains more files than the fingerprint bound")]
    TooManyFiles,
    #[error("a workspace file exceeds the fingerprint byte bound")]
    FileTooLarge,
    #[error("workspace files exceed the cumulative fingerprint byte bound")]
    TotalTooLarge,
    #[error("the workspace changed while it was fingerprinted")]
    WorkspaceChanged,
}

impl FingerprintError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidLimits | Self::InvalidEnvironment | Self::CommandPolicy(_) => {
                "COMMAND_NOT_ALLOWED"
            }
            Self::Process(error) => error.code(),
            Self::Cancelled => "COMMAND_CANCELLED",
            Self::TimedOut => "COMMAND_TIMED_OUT",
            Self::GitCommandFailed | Self::OutputIncomplete | Self::ListingInvalid => {
                "FINGERPRINT_GIT_FAILED"
            }
            Self::UnsupportedEntry | Self::PathInvalid | Self::UnsafeEntry(_) => {
                "WORKTREE_PATH_ESCAPE"
            }
            Self::TooManyFiles | Self::FileTooLarge | Self::TotalTooLarge => "WORKSPACE_TOO_LARGE",
            Self::WorkspaceChanged => "WORKSPACE_CHANGED",
        }
    }
}
