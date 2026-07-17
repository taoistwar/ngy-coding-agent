use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fs::{File, Metadata};
use std::io::{self, Read};
use std::path::Path;
#[cfg(windows)]
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use coding_agent_core::{DiffEvent, DiffFile, DiffFileStatus};
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

/// Trusted bounds for one deterministic diff snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiffLimits {
    status_timeout: Duration,
    per_file_timeout: Duration,
    max_files: usize,
    max_patch_bytes: usize,
    max_untracked_file_bytes: u64,
    max_total_untracked_bytes: u64,
}

impl DiffLimits {
    pub fn try_new(
        status_timeout: Duration,
        per_file_timeout: Duration,
        max_files: usize,
        max_patch_bytes: usize,
        max_untracked_file_bytes: u64,
        max_total_untracked_bytes: u64,
    ) -> Result<Self, DiffError> {
        if status_timeout.is_zero()
            || per_file_timeout.is_zero()
            || max_files == 0
            || max_patch_bytes == 0
            || max_untracked_file_bytes == 0
            || max_total_untracked_bytes == 0
        {
            return Err(DiffError::InvalidLimits);
        }
        Ok(Self {
            status_timeout,
            per_file_timeout,
            max_files,
            max_patch_bytes,
            max_untracked_file_bytes,
            max_total_untracked_bytes,
        })
    }
}

/// Collects a bounded HEAD-to-worktree snapshot without reading the linked
/// worktree `.git` pointer for authority.
///
/// The caller supplies the already-validated administrative Git directory and
/// worktree capabilities. Tracked patches are requested one path at a time so
/// a large early patch cannot hide later file counts. Untracked contents are
/// opened from the retained worktree directory handle with no-follow traversal.
#[derive(Debug)]
pub struct DiffCollector {
    supervisor: ProcessSupervisor,
    git: Arc<crate::PinnedExecutable>,
    binding: GitCommandBinding,
    environment: ChildEnvironment,
    work_tree: Arc<ExecutionDirectory>,
    limits: DiffLimits,
}

impl DiffCollector {
    pub fn from_trusted_capabilities(
        toolchain: &ToolchainPaths,
        git_directory: Arc<ExecutionDirectory>,
        work_tree: Arc<ExecutionDirectory>,
        temporary_directory: impl AsRef<Path>,
        process_limits: ProcessLimits,
        limits: DiffLimits,
    ) -> Result<Self, DiffError> {
        let binding = GitCommandBinding::try_new(git_directory, Arc::clone(&work_tree))
            .map_err(DiffError::CommandPolicy)?;
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
        revision: u64,
        cancellation: CancellationToken,
    ) -> Result<DiffEvent, DiffError> {
        let initial_status = self.read_status(cancellation.clone()).await?;
        let changes = parse_porcelain_v2(&initial_status, self.limits.max_files)?;
        let mut total_untracked_bytes = 0u64;
        let mut files = Vec::with_capacity(changes.len());
        // Windows leases prevent an untracked object from being rewritten or
        // rebound after it was read but before the final status observation.
        // Unix retains an empty vector and relies on metadata/namespace
        // revalidation because portable mandatory read leases do not exist.
        let mut retained_untracked_leases = Vec::new();

        for change in changes.values() {
            if cancellation.is_cancelled() {
                return Err(DiffError::Cancelled);
            }
            let file = match change.status {
                ChangeStatus::Untracked => {
                    let collected = self.collect_untracked(change, &mut total_untracked_bytes)?;
                    if let Some(lease) = collected.read_lease {
                        retained_untracked_leases.push(lease);
                    }
                    collected.file
                }
                ChangeStatus::Added => {
                    self.collect_tracked(change, DiffFileStatus::Added, cancellation.clone())
                        .await?
                }
                ChangeStatus::Modified => {
                    self.collect_tracked(change, DiffFileStatus::Modified, cancellation.clone())
                        .await?
                }
                ChangeStatus::Deleted => {
                    self.collect_tracked(change, DiffFileStatus::Deleted, cancellation.clone())
                        .await?
                }
            };
            files.push(file);
        }

        // Fail closed if Git observed a namespace/status change while the
        // multi-command bounded snapshot was being assembled.
        let final_status = self.read_status(cancellation).await?;
        if final_status != initial_status {
            return Err(DiffError::WorkspaceChanged);
        }

        Ok(DiffEvent { revision, files })
    }

    async fn read_status(&self, cancellation: CancellationToken) -> Result<Vec<u8>, DiffError> {
        let command = ValidatedCommand::git_status(
            Arc::clone(&self.git),
            &self.binding,
            self.environment.clone(),
            self.limits.status_timeout,
        )
        .map_err(DiffError::CommandPolicy)?;
        let result = self
            .supervisor
            .run(command, cancellation)
            .await
            .map_err(DiffError::Process)?;
        ensure_success(&result, DiffCommandKind::Status)?;
        complete_stdout(&result.stdout, DiffCommandKind::Status)
    }

    async fn collect_tracked(
        &self,
        change: &ChangedPath,
        status: DiffFileStatus,
        cancellation: CancellationToken,
    ) -> Result<DiffFile, DiffError> {
        let os_path = change.path.to_os_string()?;
        let count_command = ValidatedCommand::git_diff_numstat_path(
            Arc::clone(&self.git),
            &self.binding,
            self.environment.clone(),
            &os_path,
            self.limits.per_file_timeout,
        )
        .map_err(DiffError::CommandPolicy)?;
        let count_result = self
            .supervisor
            .run(count_command, cancellation.clone())
            .await
            .map_err(DiffError::Process)?;
        ensure_success(&count_result, DiffCommandKind::Count)?;
        let count_output = complete_stdout(&count_result.stdout, DiffCommandKind::Count)?;
        let counts = parse_numstat(&count_output, &change.path)?;
        let display_path = change.path.display();

        if counts.binary {
            let metadata = format!(
                "diff --git a/{0} b/{0}\nBinary change omitted\n",
                display_path
            );
            let (patch, _) =
                bounded_patch(metadata.into_bytes(), false, self.limits.max_patch_bytes);
            return Ok(DiffFile {
                path: display_path,
                status,
                patch,
                additions: 0,
                deletions: 0,
                truncated: true,
            });
        }

        let patch_command = ValidatedCommand::git_diff_patch_path(
            Arc::clone(&self.git),
            &self.binding,
            self.environment.clone(),
            &os_path,
            self.limits.per_file_timeout,
        )
        .map_err(DiffError::CommandPolicy)?;
        let patch_result = self
            .supervisor
            .run(patch_command, cancellation)
            .await
            .map_err(DiffError::Process)?;
        ensure_success(&patch_result, DiffCommandKind::Patch)?;
        if !patch_result.stdout.complete {
            return Err(DiffError::PatchOutputIncomplete);
        }
        let source_truncated = stream_is_truncated(&patch_result.stdout);
        // Head/tail capture is useful for diagnostics, but a diff exposed to
        // callers must remain a true prefix. Never splice the retained tail
        // onto a patch whose middle was omitted.
        let retained = if source_truncated {
            patch_result.stdout.head.clone()
        } else {
            retained_output(&patch_result.stdout)
        };
        let (patch, truncated) =
            bounded_patch(retained, source_truncated, self.limits.max_patch_bytes);

        Ok(DiffFile {
            path: display_path,
            status,
            patch,
            additions: counts.additions,
            deletions: counts.deletions,
            truncated,
        })
    }

    fn collect_untracked(
        &self,
        change: &ChangedPath,
        total_untracked_bytes: &mut u64,
    ) -> Result<CollectedUntracked, DiffError> {
        let OpenedWorktreeFile {
            parent,
            name,
            mut file,
        } = open_worktree_file(&self.work_tree, &change.path)?;
        #[cfg(windows)]
        let read_lease = Some(reopen_file_read_lease(&file).map_err(DiffError::UntrackedRead)?);
        #[cfg(unix)]
        let read_lease = None;
        let before = file.metadata().map_err(DiffError::UntrackedRead)?;
        let length = before.len();
        if length > self.limits.max_untracked_file_bytes {
            return Err(DiffError::UntrackedFileTooLarge);
        }
        let next_total = total_untracked_bytes
            .checked_add(length)
            .ok_or(DiffError::UntrackedTotalTooLarge)?;
        if next_total > self.limits.max_total_untracked_bytes {
            return Err(DiffError::UntrackedTotalTooLarge);
        }

        let read_limit = self
            .limits
            .max_untracked_file_bytes
            .checked_add(1)
            .ok_or(DiffError::InvalidLimits)?;
        let capacity = usize::try_from(length).map_err(|_| DiffError::UntrackedFileTooLarge)?;
        let mut content = Vec::new();
        content
            .try_reserve_exact(capacity)
            .map_err(|_| DiffError::UntrackedFileTooLarge)?;
        file.by_ref()
            .take(read_limit)
            .read_to_end(&mut content)
            .map_err(DiffError::UntrackedRead)?;
        if u64::try_from(content.len()).unwrap_or(u64::MAX) > self.limits.max_untracked_file_bytes {
            return Err(DiffError::UntrackedFileTooLarge);
        }
        let after = file.metadata().map_err(DiffError::UntrackedRead)?;
        if !same_observed_file(&before, &after)
            || !untracked_child_matches(&parent, &name, &file).map_err(DiffError::UntrackedRead)?
        {
            return Err(DiffError::WorkspaceChanged);
        }
        *total_untracked_bytes = next_total;

        let display_path = change.path.display();
        let mode = file_mode(&after);
        let binary = content.contains(&0) || std::str::from_utf8(&content).is_err();
        if binary {
            let metadata = format!(
                "diff --git a/{0} b/{0}\nnew file mode {1}\nBinary file b/{0} omitted\n",
                display_path, mode
            );
            let (patch, _) =
                bounded_patch(metadata.into_bytes(), false, self.limits.max_patch_bytes);
            return Ok(CollectedUntracked {
                file: DiffFile {
                    path: display_path,
                    status: DiffFileStatus::Added,
                    patch,
                    additions: 0,
                    deletions: 0,
                    truncated: true,
                },
                read_lease,
            });
        }

        let text = std::str::from_utf8(&content).expect("binary classification validated UTF-8");
        let additions = logical_line_count(text);
        let full_patch = synthesize_added_patch(&display_path, mode, text, additions);
        let (patch, truncated) =
            bounded_patch(full_patch.into_bytes(), false, self.limits.max_patch_bytes);
        Ok(CollectedUntracked {
            file: DiffFile {
                path: display_path,
                status: DiffFileStatus::Added,
                patch,
                additions,
                deletions: 0,
                truncated,
            },
            read_lease,
        })
    }
}

struct CollectedUntracked {
    file: DiffFile,
    read_lease: Option<File>,
}

fn platform_environment(path: &Path) -> Result<PlatformEnvironment, DiffError> {
    #[cfg(windows)]
    let system_root = std::env::var_os("SYSTEMROOT")
        .or_else(|| std::env::var_os("WINDIR"))
        .map(PathBuf::from);
    #[cfg(unix)]
    let system_root = None;

    PlatformEnvironment::try_new(path.to_owned(), system_root)
        .map_err(|_| DiffError::InvalidEnvironment)
}

#[derive(Debug, Clone, Copy)]
enum DiffCommandKind {
    Status,
    Count,
    Patch,
}

fn ensure_success(result: &CommandResult, kind: DiffCommandKind) -> Result<(), DiffError> {
    if result.cancelled {
        return Err(DiffError::Cancelled);
    }
    if result.timed_out {
        return Err(DiffError::TimedOut);
    }
    if result.exit_code == Some(0) && result.signal.is_none() {
        return Ok(());
    }
    Err(match kind {
        DiffCommandKind::Status => DiffError::StatusCommandFailed,
        DiffCommandKind::Count => DiffError::CountCommandFailed,
        DiffCommandKind::Patch => DiffError::PatchCommandFailed,
    })
}

fn complete_stdout(stream: &CapturedStream, kind: DiffCommandKind) -> Result<Vec<u8>, DiffError> {
    let retained = stream.head.len().saturating_add(stream.tail.len());
    if !stream.complete
        || stream.truncated
        || stream.omitted_observed_bytes != 0
        || stream.observed_bytes != retained as u64
    {
        return Err(match kind {
            DiffCommandKind::Status => DiffError::StatusOutputIncomplete,
            DiffCommandKind::Count => DiffError::CountOutputIncomplete,
            DiffCommandKind::Patch => DiffError::PatchOutputIncomplete,
        });
    }
    Ok(retained_output(stream))
}

fn retained_output(stream: &CapturedStream) -> Vec<u8> {
    let mut output = Vec::with_capacity(stream.head.len().saturating_add(stream.tail.len()));
    output.extend_from_slice(&stream.head);
    output.extend_from_slice(&stream.tail);
    output
}

fn stream_is_truncated(stream: &CapturedStream) -> bool {
    stream.truncated || stream.omitted_observed_bytes != 0
}

fn bounded_patch(bytes: Vec<u8>, already_truncated: bool, limit: usize) -> (String, bool) {
    let (mut patch, lossy) = match String::from_utf8(bytes) {
        Ok(patch) => (patch, false),
        Err(error) => (String::from_utf8_lossy(error.as_bytes()).into_owned(), true),
    };
    let mut truncated = already_truncated || lossy;
    if patch.len() > limit {
        let mut boundary = limit;
        while boundary > 0 && !patch.is_char_boundary(boundary) {
            boundary -= 1;
        }
        patch.truncate(boundary);
        truncated = true;
    }
    (patch, truncated)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChangeStatus {
    Added,
    Modified,
    Deleted,
    Untracked,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ChangedPath {
    path: GitPath,
    status: ChangeStatus,
}

fn parse_porcelain_v2(
    output: &[u8],
    max_files: usize,
) -> Result<BTreeMap<Vec<u8>, ChangedPath>, DiffError> {
    let records = output.split(|byte| *byte == 0).collect::<Vec<_>>();
    let mut index = 0usize;
    let mut changes = BTreeMap::new();
    while index < records.len() {
        let record = records[index];
        index += 1;
        if record.is_empty() {
            if index == records.len() {
                break;
            }
            return Err(DiffError::StatusOutputInvalid);
        }
        match record.first().copied() {
            Some(b'#') | Some(b'!') => {}
            Some(b'?') if record.get(1) == Some(&b' ') => {
                insert_change(
                    &mut changes,
                    &record[2..],
                    ChangeStatus::Untracked,
                    max_files,
                )?;
            }
            Some(b'1') if record.get(1) == Some(&b' ') => {
                let fields = split_fields(record, 9)?;
                if fields[2] != b"N..." {
                    return Err(DiffError::UnsupportedStatus);
                }
                let status = tracked_status(fields[1])?;
                insert_change(&mut changes, fields[8], status, max_files)?;
            }
            Some(b'2') if record.get(1) == Some(&b' ') => {
                let fields = split_fields(record, 10)?;
                if fields[2] != b"N..." || index >= records.len() {
                    return Err(DiffError::UnsupportedStatus);
                }
                let original = records[index];
                index += 1;
                let change_kind = parse_rename_or_copy(fields[1], fields[8])?;
                if change_kind == RenameOrCopy::Rename {
                    insert_change(&mut changes, original, ChangeStatus::Deleted, max_files)?;
                } else {
                    // A copy leaves its source intact. Validate the otherwise
                    // unused source path so protected/ambiguous names still
                    // fail closed rather than being silently ignored.
                    GitPath::parse(original)?;
                }
                insert_change(&mut changes, fields[9], ChangeStatus::Added, max_files)?;
            }
            Some(b'u') => return Err(DiffError::UnsupportedStatus),
            _ => return Err(DiffError::StatusOutputInvalid),
        }
    }
    Ok(changes)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RenameOrCopy {
    Rename,
    Copy,
}

fn parse_rename_or_copy(xy: &[u8], score: &[u8]) -> Result<RenameOrCopy, DiffError> {
    if xy.len() != 2 || score.len() < 2 || !score[1..].iter().all(u8::is_ascii_digit) {
        return Err(DiffError::StatusOutputInvalid);
    }
    match score[0] {
        b'R' if xy.contains(&b'R') => Ok(RenameOrCopy::Rename),
        b'C' if xy.contains(&b'C') => Ok(RenameOrCopy::Copy),
        _ => Err(DiffError::StatusOutputInvalid),
    }
}

fn split_fields(record: &[u8], count: usize) -> Result<Vec<&[u8]>, DiffError> {
    let fields = record
        .splitn(count, |byte| *byte == b' ')
        .collect::<Vec<_>>();
    if fields.len() == count && fields.iter().all(|field| !field.is_empty()) {
        Ok(fields)
    } else {
        Err(DiffError::StatusOutputInvalid)
    }
}

fn tracked_status(xy: &[u8]) -> Result<ChangeStatus, DiffError> {
    if xy.len() != 2 {
        return Err(DiffError::StatusOutputInvalid);
    }
    if xy.contains(&b'U') {
        return Err(DiffError::UnsupportedStatus);
    }
    if xy.contains(&b'D') {
        Ok(ChangeStatus::Deleted)
    } else if xy.contains(&b'A') {
        Ok(ChangeStatus::Added)
    } else if xy
        .iter()
        .all(|byte| matches!(byte, b'.' | b'M' | b'T' | b'R' | b'C'))
    {
        Ok(ChangeStatus::Modified)
    } else {
        Err(DiffError::UnsupportedStatus)
    }
}

fn insert_change(
    changes: &mut BTreeMap<Vec<u8>, ChangedPath>,
    raw_path: &[u8],
    status: ChangeStatus,
    max_files: usize,
) -> Result<(), DiffError> {
    let path = GitPath::parse(raw_path)?;
    if changes.contains_key(path.raw()) {
        return Err(DiffError::StatusOutputInvalid);
    }
    if changes.len() >= max_files {
        return Err(DiffError::TooManyFiles);
    }
    changes.insert(path.raw().to_vec(), ChangedPath { path, status });
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GitPath {
    raw: Vec<u8>,
    components: Vec<Vec<u8>>,
}

impl GitPath {
    fn parse(raw: &[u8]) -> Result<Self, DiffError> {
        if raw.is_empty() || raw.contains(&0) {
            return Err(DiffError::PathInvalid);
        }
        #[cfg(windows)]
        {
            let path = std::str::from_utf8(raw).map_err(|_| DiffError::PathInvalid)?;
            crate::RelativePath::parse(path.to_owned()).map_err(|_| DiffError::PathInvalid)?;
        }
        let components = raw.split(|byte| *byte == b'/').collect::<Vec<_>>();
        if components.iter().any(|component| {
            component.is_empty()
                || *component == b"."
                || *component == b".."
                || component_is_git_metadata(component)
        }) {
            return Err(DiffError::PathInvalid);
        }
        Ok(Self {
            raw: raw.to_vec(),
            components: components.into_iter().map(<[u8]>::to_vec).collect(),
        })
    }

    fn raw(&self) -> &[u8] {
        &self.raw
    }

    fn to_os_string(&self) -> Result<OsString, DiffError> {
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;
            Ok(OsString::from_vec(self.raw.clone()))
        }
        #[cfg(windows)]
        {
            std::str::from_utf8(&self.raw)
                .map(OsString::from)
                .map_err(|_| DiffError::PathInvalid)
        }
    }

    fn component_os_string(component: &[u8]) -> Result<OsString, DiffError> {
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;
            Ok(OsString::from_vec(component.to_vec()))
        }
        #[cfg(windows)]
        {
            std::str::from_utf8(component)
                .map(OsString::from)
                .map_err(|_| DiffError::PathInvalid)
        }
    }

    fn display(&self) -> String {
        match std::str::from_utf8(&self.raw) {
            Ok(path) => {
                let mut display = String::new();
                for character in path.chars() {
                    if character.is_ascii() && !safe_display_byte(character as u8) {
                        push_percent_encoded(&mut display, character as u8);
                    } else {
                        display.push(character);
                    }
                }
                display
            }
            Err(_) => {
                let mut display = String::new();
                for byte in &self.raw {
                    if safe_display_byte(*byte) {
                        display.push(char::from(*byte));
                    } else {
                        push_percent_encoded(&mut display, *byte);
                    }
                }
                display
            }
        }
    }
}

fn component_is_git_metadata(component: &[u8]) -> bool {
    // Reject case variants on every platform. That is conservative on a
    // case-sensitive filesystem and necessary on common case-folding Unix
    // volumes (for example default macOS worktrees).
    component.eq_ignore_ascii_case(b".git")
}

fn safe_display_byte(byte: u8) -> bool {
    matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'_' | b'-' | b'/')
}

fn push_percent_encoded(output: &mut String, byte: u8) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    output.push('%');
    output.push(char::from(HEX[usize::from(byte >> 4)]));
    output.push(char::from(HEX[usize::from(byte & 0x0f)]));
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Numstat {
    additions: u64,
    deletions: u64,
    binary: bool,
}

fn parse_numstat(output: &[u8], expected_path: &GitPath) -> Result<Numstat, DiffError> {
    let Some(record) = output.strip_suffix(&[0]) else {
        return Err(DiffError::CountOutputInvalid);
    };
    if record.contains(&0) {
        return Err(DiffError::CountOutputInvalid);
    }
    let fields = record.splitn(3, |byte| *byte == b'\t').collect::<Vec<_>>();
    if fields.len() != 3 || fields[2] != expected_path.raw() {
        return Err(DiffError::CountOutputInvalid);
    }
    if fields[0] == b"-" && fields[1] == b"-" {
        return Ok(Numstat {
            additions: 0,
            deletions: 0,
            binary: true,
        });
    }
    let additions = parse_decimal(fields[0])?;
    let deletions = parse_decimal(fields[1])?;
    Ok(Numstat {
        additions,
        deletions,
        binary: false,
    })
}

fn parse_decimal(value: &[u8]) -> Result<u64, DiffError> {
    if value.is_empty() || !value.iter().all(u8::is_ascii_digit) {
        return Err(DiffError::CountOutputInvalid);
    }
    let text = std::str::from_utf8(value).map_err(|_| DiffError::CountOutputInvalid)?;
    text.parse().map_err(|_| DiffError::CountOutputInvalid)
}

struct OpenedWorktreeFile {
    parent: File,
    name: OsString,
    file: File,
}

fn open_worktree_file(
    work_tree: &ExecutionDirectory,
    path: &GitPath,
) -> Result<OpenedWorktreeFile, DiffError> {
    let mut parent = work_tree
        .cloned_directory()
        .map_err(DiffError::CommandPolicy)?;
    ensure_plain_directory(&parent).map_err(DiffError::UntrackedRead)?;
    for (index, component) in path.components.iter().enumerate() {
        let component = GitPath::component_os_string(component)?;
        if index + 1 == path.components.len() {
            let file = open_child_file(&parent, &component)
                .and_then(|file| {
                    ensure_plain_file(&file)?;
                    Ok(file)
                })
                .map_err(DiffError::UntrackedRead)?;
            return Ok(OpenedWorktreeFile {
                parent,
                name: component,
                file,
            });
        }
        parent = open_child_directory(&parent, &component)
            .and_then(|directory| {
                ensure_plain_directory(&directory)?;
                Ok(directory)
            })
            .map_err(DiffError::UntrackedRead)?;
    }
    Err(DiffError::PathInvalid)
}

#[cfg(unix)]
fn untracked_child_matches(parent: &File, name: &OsStr, file: &File) -> io::Result<bool> {
    child_file_matches(parent, name, file)
}

#[cfg(windows)]
fn untracked_child_matches(parent: &File, name: &OsStr, file: &File) -> io::Result<bool> {
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

fn logical_line_count(content: &str) -> u64 {
    if content.is_empty() {
        0
    } else {
        content.bytes().filter(|byte| *byte == b'\n').count() as u64
            + u64::from(!content.ends_with('\n'))
    }
}

fn synthesize_added_patch(path: &str, mode: &str, content: &str, additions: u64) -> String {
    let mut patch = format!(
        "diff --git a/{0} b/{0}\nnew file mode {1}\n--- /dev/null\n+++ b/{0}\n",
        path, mode
    );
    if additions == 0 {
        return patch;
    }
    patch.push_str(&format!("@@ -0,0 +1,{additions} @@\n"));
    for line in content.split_inclusive('\n') {
        patch.push('+');
        patch.push_str(line);
    }
    if !content.ends_with('\n') {
        patch.push('\n');
        patch.push_str("\\ No newline at end of file\n");
    }
    patch
}

#[cfg(unix)]
fn file_mode(metadata: &Metadata) -> &'static str {
    use std::os::unix::fs::PermissionsExt;
    if metadata.permissions().mode() & 0o111 == 0 {
        "100644"
    } else {
        "100755"
    }
}

#[cfg(windows)]
fn file_mode(_: &Metadata) -> &'static str {
    "100644"
}

#[cfg(unix)]
fn same_observed_file(before: &Metadata, after: &Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    before.dev() == after.dev()
        && before.ino() == after.ino()
        && before.len() == after.len()
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

#[derive(Debug, thiserror::Error)]
pub enum DiffError {
    #[error("diff limits must all be non-zero")]
    InvalidLimits,
    #[error("the diff child-process environment is invalid")]
    InvalidEnvironment,
    #[error("the diff command was rejected by typed command policy")]
    CommandPolicy(#[source] CommandPolicyError),
    #[error("the supervised diff process failed")]
    Process(#[source] ProcessError),
    #[error("diff collection was cancelled")]
    Cancelled,
    #[error("diff collection timed out")]
    TimedOut,
    #[error("Git status failed")]
    StatusCommandFailed,
    #[error("Git numstat failed")]
    CountCommandFailed,
    #[error("Git patch failed")]
    PatchCommandFailed,
    #[error("Git status output was incomplete")]
    StatusOutputIncomplete,
    #[error("Git numstat output was incomplete")]
    CountOutputIncomplete,
    #[error("Git patch output was incomplete")]
    PatchOutputIncomplete,
    #[error("Git status output was malformed")]
    StatusOutputInvalid,
    #[error("Git numstat output was malformed")]
    CountOutputInvalid,
    #[error("Git reported an unsupported conflicted or submodule status")]
    UnsupportedStatus,
    #[error("Git reported an invalid or protected worktree path")]
    PathInvalid,
    #[error("the diff contains more files than the configured bound")]
    TooManyFiles,
    #[error("an untracked file exceeds the configured inspection bound")]
    UntrackedFileTooLarge,
    #[error("untracked files exceed the configured cumulative inspection bound")]
    UntrackedTotalTooLarge,
    #[error("an untracked worktree file could not be read safely")]
    UntrackedRead(#[source] io::Error),
    #[error("the worktree changed while its diff was being collected")]
    WorkspaceChanged,
}

impl DiffError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidLimits | Self::InvalidEnvironment | Self::CommandPolicy(_) => {
                "COMMAND_NOT_ALLOWED"
            }
            Self::Process(error) => error.code(),
            Self::Cancelled => "COMMAND_CANCELLED",
            Self::TimedOut => "COMMAND_TIMED_OUT",
            Self::StatusCommandFailed
            | Self::CountCommandFailed
            | Self::PatchCommandFailed
            | Self::StatusOutputIncomplete
            | Self::CountOutputIncomplete
            | Self::PatchOutputIncomplete
            | Self::StatusOutputInvalid
            | Self::CountOutputInvalid
            | Self::UnsupportedStatus => "DIFF_GIT_FAILED",
            Self::PathInvalid | Self::UntrackedRead(_) => "WORKTREE_PATH_ESCAPE",
            Self::TooManyFiles | Self::UntrackedFileTooLarge | Self::UntrackedTotalTooLarge => {
                "DIFF_TOO_LARGE"
            }
            Self::WorkspaceChanged => "WORKTREE_CHANGED_DURING_DIFF",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn porcelain_parser_is_bounded_sorted_and_handles_renames() {
        let output = b"? z.txt\0\
1 M. N... 100644 100644 100644 aaaaaaa bbbbbbb a.txt\0\
2 R. N... 100644 100644 100644 aaaaaaa bbbbbbb R100 new.txt\0old.txt\0\
2 C. N... 100644 100644 100644 aaaaaaa bbbbbbb C100 copy.txt\0a.txt\0";
        let parsed = parse_porcelain_v2(output, 5).unwrap();
        let entries = parsed.values().collect::<Vec<_>>();
        assert_eq!(entries[0].path.raw(), b"a.txt");
        assert_eq!(entries[0].status, ChangeStatus::Modified);
        assert_eq!(entries[1].path.raw(), b"copy.txt");
        assert_eq!(entries[1].status, ChangeStatus::Added);
        assert_eq!(entries[2].path.raw(), b"new.txt");
        assert_eq!(entries[2].status, ChangeStatus::Added);
        assert_eq!(entries[3].path.raw(), b"old.txt");
        assert_eq!(entries[3].status, ChangeStatus::Deleted);
        assert_eq!(entries[4].path.raw(), b"z.txt");
        assert_eq!(entries[4].status, ChangeStatus::Untracked);
        assert!(matches!(
            parse_porcelain_v2(output, 4),
            Err(DiffError::TooManyFiles)
        ));
    }

    #[test]
    fn porcelain_parser_rejects_mismatched_rename_and_copy_records() {
        let output = b"2 R. N... 100644 100644 100644 aaaaaaa bbbbbbb C100 copy.txt\0source.txt\0";
        assert!(matches!(
            parse_porcelain_v2(output, 2),
            Err(DiffError::StatusOutputInvalid)
        ));
    }

    #[test]
    fn path_display_is_reversible_for_percent_and_non_utf8_bytes() {
        assert_eq!(
            GitPath::parse(b"space name/%FF").unwrap().display(),
            "space%20name/%25FF"
        );
        #[cfg(unix)]
        assert_eq!(
            GitPath::parse(b"bad-\xff.rs").unwrap().display(),
            "bad-%FF.rs"
        );
        assert!(matches!(
            GitPath::parse(b".git/config"),
            Err(DiffError::PathInvalid)
        ));
        assert!(matches!(
            GitPath::parse(b".GIT/config"),
            Err(DiffError::PathInvalid)
        ));
        assert!(matches!(
            GitPath::parse(b"../escape"),
            Err(DiffError::PathInvalid)
        ));
        #[cfg(windows)]
        for invalid in [b".git./config".as_slice(), b"file:stream"] {
            assert!(matches!(
                GitPath::parse(invalid),
                Err(DiffError::PathInvalid)
            ));
        }
    }

    #[test]
    fn numstat_parser_reports_text_and_binary_counts() {
        let path = GitPath::parse(b"src/lib.rs").unwrap();
        assert_eq!(
            parse_numstat(b"12\t3\tsrc/lib.rs\0", &path).unwrap(),
            Numstat {
                additions: 12,
                deletions: 3,
                binary: false
            }
        );
        assert_eq!(
            parse_numstat(b"-\t-\tsrc/lib.rs\0", &path).unwrap(),
            Numstat {
                additions: 0,
                deletions: 0,
                binary: true
            }
        );
    }

    #[test]
    fn synthesized_added_patch_counts_lines_and_obeys_byte_cap() {
        let patch = synthesize_added_patch("new.txt", "100644", "one\ntwo", 2);
        assert!(patch.contains("@@ -0,0 +1,2 @@"));
        assert!(patch.contains("+one\n+two\n\\ No newline at end of file\n"));
        let (bounded, truncated) = bounded_patch(patch.into_bytes(), false, 32);
        assert!(truncated);
        assert!(bounded.len() <= 32);
    }

    #[test]
    fn patch_caps_count_utf8_bytes_and_never_split_a_character() {
        let (bounded, truncated) = bounded_patch("aéz".as_bytes().to_vec(), false, 2);
        assert_eq!(bounded, "a");
        assert!(truncated);
        assert!(bounded.len() <= 2);

        let (lossy, truncated) = bounded_patch(vec![b'a', 0xff, b'z'], false, 4);
        assert_eq!(lossy, "a�");
        assert!(truncated);
        assert!(lossy.len() <= 4);
    }
}
