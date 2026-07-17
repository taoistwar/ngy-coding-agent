use std::ffi::OsStr;
use std::fs::File;
use std::io::{self, Read};

use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::native_fs::{open_child_directory, open_child_file, read_directory_names};
use crate::root_capability::{
    ensure_child_is_not_protected_metadata, ensure_plain_directory, ensure_plain_file,
};
use crate::{RelativePath, RelativePathError, RootCapability};

const MAX_QUERY_BYTES: usize = 4_096;
const MAX_GLOB_BYTES: usize = 1_024;
const MAX_GLOB_COMPONENTS: usize = 64;
const MAX_GLOB_WILDCARDS: usize = 128;
const MAX_TRAVERSAL_DEPTH: u32 = 128;
const NUMBERED_LINE_BUDGET_OVERHEAD: usize = 32;

#[derive(Debug)]
pub struct FileTools {
    root: RootCapability,
    limits: FileToolLimits,
}

impl FileTools {
    pub const fn new(root: RootCapability, limits: FileToolLimits) -> Self {
        Self { root, limits }
    }

    pub fn read_file(
        &self,
        path: &RelativePath,
        start_line: u64,
        end_line: u64,
    ) -> Result<ReadFileResult, FileToolError> {
        self.read_file_cancellable(path, start_line, end_line, &CancellationToken::new())
    }

    pub fn read_file_cancellable(
        &self,
        path: &RelativePath,
        start_line: u64,
        end_line: u64,
        cancellation: &CancellationToken,
    ) -> Result<ReadFileResult, FileToolError> {
        check_cancelled(cancellation)?;
        if start_line == 0 || end_line < start_line {
            return Err(FileToolError::InvalidLineRange);
        }
        if path.is_root() {
            return Err(FileToolError::FileNotRegular);
        }
        let mut file = self
            .root
            .open_file_for_read(path)
            .map_err(map_direct_file_error)?;
        let bytes = read_bounded(&mut file, self.limits.max_file_bytes, cancellation, None)?;
        let file_bytes = bytes.len() as u64;
        let sha256 = hex_digest(&bytes);
        let text = text_from_bytes(&bytes)?;
        let all_lines = text.lines().collect::<Vec<_>>();
        let total_lines = all_lines.len() as u64;
        let mut lines = Vec::new();
        let mut returned_bytes = 0usize;
        let mut result_budget_bytes = 0usize;
        let mut next_line = None;

        let first_index = usize::try_from(start_line - 1).unwrap_or(usize::MAX);
        let last_index = usize::try_from(end_line).unwrap_or(usize::MAX);
        for (index, line) in all_lines
            .iter()
            .enumerate()
            .skip(first_index)
            .take(last_index.saturating_sub(first_index))
        {
            check_cancelled(cancellation)?;
            let candidate_budget = line.len().saturating_add(NUMBERED_LINE_BUDGET_OVERHEAD);
            if result_budget_bytes.saturating_add(candidate_budget)
                > self.limits.max_read_result_bytes
            {
                if lines.is_empty() {
                    return Err(FileToolError::FileTooLarge);
                }
                next_line = Some(index as u64 + 1);
                break;
            }
            result_budget_bytes += candidate_budget;
            returned_bytes += line.len();
            lines.push(NumberedLine {
                number: index as u64 + 1,
                text: (*line).to_owned(),
            });
        }

        if next_line.is_none() && end_line < total_lines {
            next_line = Some(end_line + 1);
        }
        Ok(ReadFileResult {
            lines,
            sha256,
            file_bytes,
            total_lines,
            returned_bytes,
            truncated: next_line.is_some(),
            next_line,
        })
    }

    pub fn list_files(
        &self,
        path: &RelativePath,
        depth: u32,
        limit: usize,
    ) -> Result<ListFilesResult, FileToolError> {
        self.list_files_cancellable(path, depth, limit, &CancellationToken::new())
    }

    pub fn list_files_cancellable(
        &self,
        path: &RelativePath,
        depth: u32,
        limit: usize,
        cancellation: &CancellationToken,
    ) -> Result<ListFilesResult, FileToolError> {
        self.list_files_with_hook(path, depth, limit, cancellation, &mut |_| Ok(()))
    }

    fn list_files_with_hook(
        &self,
        path: &RelativePath,
        depth: u32,
        limit: usize,
        cancellation: &CancellationToken,
        after_directory_open: &mut dyn FnMut(&RelativePath) -> io::Result<()>,
    ) -> Result<ListFilesResult, FileToolError> {
        check_cancelled(cancellation)?;
        if depth == 0
            || depth > self.limits.max_depth
            || limit == 0
            || limit > self.limits.max_result_items
        {
            return Err(FileToolError::InvalidLimit);
        }
        let directory = self
            .root
            .open_directory(path)
            .map_err(map_direct_directory_error)?;
        let mut result = ListFilesResult::default();
        let mut output_bytes = 0usize;
        self.list_directory(
            directory,
            path,
            depth,
            limit,
            &mut output_bytes,
            &mut result,
            cancellation,
            after_directory_open,
        )?;
        Ok(result)
    }

    pub fn search_text(
        &self,
        query: &str,
        path: &RelativePath,
        glob: Option<&str>,
        limit: usize,
    ) -> Result<SearchTextResult, FileToolError> {
        self.search_text_cancellable(query, path, glob, limit, &CancellationToken::new())
    }

    pub fn search_text_cancellable(
        &self,
        query: &str,
        path: &RelativePath,
        glob: Option<&str>,
        limit: usize,
        cancellation: &CancellationToken,
    ) -> Result<SearchTextResult, FileToolError> {
        self.search_text_with_hook(query, path, glob, limit, cancellation, &mut |_| Ok(()))
    }

    fn search_text_with_hook(
        &self,
        query: &str,
        path: &RelativePath,
        glob: Option<&str>,
        limit: usize,
        cancellation: &CancellationToken,
        after_directory_open: &mut dyn FnMut(&RelativePath) -> io::Result<()>,
    ) -> Result<SearchTextResult, FileToolError> {
        check_cancelled(cancellation)?;
        if limit == 0 || limit > self.limits.max_result_items {
            return Err(FileToolError::InvalidLimit);
        }
        if query.is_empty() || query.len() > MAX_QUERY_BYTES || query.contains(['\0', '\r', '\n']) {
            return Err(FileToolError::InvalidQuery);
        }
        let glob = glob.map(SearchGlob::parse).transpose()?;
        let mut result = SearchTextResult::default();
        let mut visited_entries = 0usize;
        let mut output_bytes = 0usize;
        let mut scanned_bytes = ScannedBytes::new(self.limits.max_search_bytes);

        match self.root.open_directory(path) {
            Ok(directory) => {
                self.search_directory(
                    directory,
                    path,
                    path,
                    query,
                    glob.as_ref(),
                    limit,
                    0,
                    &mut visited_entries,
                    &mut output_bytes,
                    &mut scanned_bytes,
                    &mut result,
                    cancellation,
                    after_directory_open,
                )?;
            }
            Err(directory_error) => {
                if path.is_root() {
                    return Err(map_direct_directory_error(directory_error));
                }
                let file = self
                    .root
                    .open_file_for_read(path)
                    .map_err(map_direct_file_error)?;
                let relative_for_glob = path
                    .components()
                    .last()
                    .expect("a non-root relative path has a component");
                if glob
                    .as_ref()
                    .is_none_or(|glob| glob.matches(relative_for_glob))
                {
                    self.search_file(
                        file,
                        path,
                        query,
                        limit,
                        false,
                        &mut output_bytes,
                        &mut scanned_bytes,
                        &mut result,
                        cancellation,
                    )?;
                }
            }
        }
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    fn list_directory(
        &self,
        mut directory: File,
        logical_path: &RelativePath,
        depth: u32,
        requested_limit: usize,
        output_bytes: &mut usize,
        result: &mut ListFilesResult,
        cancellation: &CancellationToken,
        after_directory_open: &mut dyn FnMut(&RelativePath) -> io::Result<()>,
    ) -> Result<bool, FileToolError> {
        check_cancelled(cancellation)?;
        let names = self.directory_names(&mut directory)?;
        for name in names {
            check_cancelled(cancellation)?;
            result.visited_entries += 1;
            if result.visited_entries > self.limits.max_visited_entries {
                return Err(FileToolError::TraversalLimitExceeded);
            }
            let Some(name) = name.to_str() else {
                result.omitted_entries += 1;
                continue;
            };
            if protected_metadata_name(name) {
                result.omitted_entries += 1;
                continue;
            }
            let child = match logical_path.join_component(name) {
                Ok(child) => child,
                Err(_) => {
                    result.omitted_entries += 1;
                    continue;
                }
            };
            match open_entry(&directory, name)? {
                OpenedEntry::Directory(child_directory) => {
                    if ignored_directory_name(name) {
                        result.omitted_entries += 1;
                        continue;
                    }
                    after_directory_open(&child)?;
                    if !push_list_entry(
                        &child,
                        FileEntryKind::Directory,
                        requested_limit,
                        self.limits.max_result_bytes,
                        output_bytes,
                        result,
                    ) {
                        return Ok(true);
                    }
                    if depth > 1
                        && self.list_directory(
                            child_directory,
                            &child,
                            depth - 1,
                            requested_limit,
                            output_bytes,
                            result,
                            cancellation,
                            after_directory_open,
                        )?
                    {
                        return Ok(true);
                    }
                }
                OpenedEntry::File(_) => {
                    if !push_list_entry(
                        &child,
                        FileEntryKind::File,
                        requested_limit,
                        self.limits.max_result_bytes,
                        output_bytes,
                        result,
                    ) {
                        return Ok(true);
                    }
                }
                OpenedEntry::Omitted => result.omitted_entries += 1,
            }
        }
        Ok(false)
    }

    #[allow(clippy::too_many_arguments)]
    fn search_directory(
        &self,
        mut directory: File,
        logical_path: &RelativePath,
        search_root: &RelativePath,
        query: &str,
        glob: Option<&SearchGlob>,
        requested_limit: usize,
        depth: u32,
        visited_entries: &mut usize,
        output_bytes: &mut usize,
        scanned_bytes: &mut ScannedBytes,
        result: &mut SearchTextResult,
        cancellation: &CancellationToken,
        after_directory_open: &mut dyn FnMut(&RelativePath) -> io::Result<()>,
    ) -> Result<bool, FileToolError> {
        check_cancelled(cancellation)?;
        let names = self.directory_names(&mut directory)?;
        for name in names {
            check_cancelled(cancellation)?;
            *visited_entries += 1;
            if *visited_entries > self.limits.max_visited_entries {
                return Err(FileToolError::TraversalLimitExceeded);
            }
            let Some(name) = name.to_str() else {
                result.skipped_files += 1;
                continue;
            };
            if protected_metadata_name(name) {
                result.skipped_files += 1;
                continue;
            }
            let child = match logical_path.join_component(name) {
                Ok(child) => child,
                Err(_) => {
                    result.skipped_files += 1;
                    continue;
                }
            };
            match open_entry(&directory, name)? {
                OpenedEntry::Directory(child_directory) => {
                    if ignored_directory_name(name) {
                        result.skipped_files += 1;
                        continue;
                    }
                    if depth == self.limits.max_depth {
                        return Err(FileToolError::TraversalLimitExceeded);
                    }
                    after_directory_open(&child)?;
                    if self.search_directory(
                        child_directory,
                        &child,
                        search_root,
                        query,
                        glob,
                        requested_limit,
                        depth + 1,
                        visited_entries,
                        output_bytes,
                        scanned_bytes,
                        result,
                        cancellation,
                        after_directory_open,
                    )? {
                        return Ok(true);
                    }
                }
                OpenedEntry::File(file) => {
                    let relative_for_glob = relative_to_root(search_root, &child);
                    if glob.is_some_and(|glob| !glob.matches(relative_for_glob)) {
                        continue;
                    }
                    if self.search_file(
                        file,
                        &child,
                        query,
                        requested_limit,
                        true,
                        output_bytes,
                        scanned_bytes,
                        result,
                        cancellation,
                    )? {
                        return Ok(true);
                    }
                }
                OpenedEntry::Omitted => result.skipped_files += 1,
            }
        }
        Ok(false)
    }

    #[allow(clippy::too_many_arguments)]
    fn search_file(
        &self,
        mut file: File,
        logical_path: &RelativePath,
        query: &str,
        requested_limit: usize,
        omit_unsupported: bool,
        output_bytes: &mut usize,
        scanned_bytes: &mut ScannedBytes,
        result: &mut SearchTextResult,
        cancellation: &CancellationToken,
    ) -> Result<bool, FileToolError> {
        check_cancelled(cancellation)?;
        result.visited_files += 1;
        let bytes = match read_bounded(
            &mut file,
            self.limits.max_file_bytes,
            cancellation,
            Some(scanned_bytes),
        ) {
            Ok(bytes) => bytes,
            Err(FileToolError::FileTooLarge) if omit_unsupported => {
                result.skipped_files += 1;
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        let text = match text_from_bytes(&bytes) {
            Ok(text) => text,
            Err(FileToolError::Binary | FileToolError::NotUtf8) if omit_unsupported => {
                result.skipped_files += 1;
                return Ok(false);
            }
            Err(error) => return Err(error),
        };

        for (line_index, line) in text.lines().enumerate() {
            check_cancelled(cancellation)?;
            for (byte_index, _) in line.match_indices(query) {
                check_cancelled(cancellation)?;
                let candidate_bytes = logical_path
                    .as_slash_str()
                    .len()
                    .saturating_add(line.len())
                    .saturating_add(32);
                if result.matches.len() == requested_limit
                    || result.matches.len() == self.limits.max_result_items
                    || output_bytes.saturating_add(candidate_bytes) > self.limits.max_result_bytes
                {
                    result.truncated = true;
                    return Ok(true);
                }
                *output_bytes += candidate_bytes;
                result.matches.push(SearchMatch {
                    path: logical_path.clone(),
                    line_number: line_index as u64 + 1,
                    column: line[..byte_index].chars().count() as u64 + 1,
                    preview: line.to_owned(),
                });
            }
        }
        Ok(false)
    }

    fn directory_names(
        &self,
        directory: &mut File,
    ) -> Result<Vec<std::ffi::OsString>, FileToolError> {
        let allowance = self.limits.max_directory_entries.saturating_add(2);
        let names = read_directory_names(directory, allowance).map_err(|error| {
            if error.kind() == io::ErrorKind::FileTooLarge {
                FileToolError::DirectoryTooLarge
            } else {
                FileToolError::Io(error)
            }
        })?;
        let mut names = names
            .into_iter()
            .filter(|name| name != "." && name != "..")
            .collect::<Vec<_>>();
        if names.len() > self.limits.max_directory_entries {
            return Err(FileToolError::DirectoryTooLarge);
        }
        names.sort();
        Ok(names)
    }
}

fn push_list_entry(
    path: &RelativePath,
    kind: FileEntryKind,
    requested_limit: usize,
    max_result_bytes: usize,
    output_bytes: &mut usize,
    result: &mut ListFilesResult,
) -> bool {
    let candidate_bytes = path.as_slash_str().len().saturating_add(1);
    if result.entries.len() == requested_limit
        || output_bytes.saturating_add(candidate_bytes) > max_result_bytes
    {
        result.truncated = true;
        return false;
    }
    *output_bytes += candidate_bytes;
    result.entries.push(FileEntry {
        path: path.clone(),
        kind,
    });
    true
}

enum OpenedEntry {
    Directory(File),
    File(File),
    Omitted,
}

fn open_entry(parent: &File, name: &str) -> Result<OpenedEntry, FileToolError> {
    let name = OsStr::new(name);
    match open_child_directory(parent, name) {
        Ok(directory) => {
            if let Err(error) = ensure_child_is_not_protected_metadata(parent, &directory) {
                return if error.kind() == io::ErrorKind::PermissionDenied {
                    Ok(OpenedEntry::Omitted)
                } else {
                    Err(FileToolError::Io(error))
                };
            }
            return match ensure_plain_directory(&directory) {
                Ok(()) => Ok(OpenedEntry::Directory(directory)),
                Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                    Ok(OpenedEntry::Omitted)
                }
                Err(error) => Err(FileToolError::Io(error)),
            };
        }
        Err(error) if is_link_loop(&error) => return Ok(OpenedEntry::Omitted),
        Err(error) if can_retry_as_other_entry_kind(&error) => {}
        Err(error) => return Err(FileToolError::Io(error)),
    }
    match open_child_file(parent, name) {
        Ok(file) => {
            if let Err(error) = ensure_child_is_not_protected_metadata(parent, &file) {
                return if error.kind() == io::ErrorKind::PermissionDenied {
                    Ok(OpenedEntry::Omitted)
                } else {
                    Err(FileToolError::Io(error))
                };
            }
            match ensure_plain_file(&file) {
                Ok(()) => Ok(OpenedEntry::File(file)),
                Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                    Ok(OpenedEntry::Omitted)
                }
                Err(error) => Err(FileToolError::Io(error)),
            }
        }
        Err(error) if is_link_loop(&error) || can_omit_entry_race_or_type(&error) => {
            Ok(OpenedEntry::Omitted)
        }
        Err(error) => Err(FileToolError::Io(error)),
    }
}

fn can_retry_as_other_entry_kind(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::NotADirectory | io::ErrorKind::IsADirectory
    )
}

fn can_omit_entry_race_or_type(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::NotADirectory | io::ErrorKind::IsADirectory
    )
}

#[cfg(unix)]
fn is_link_loop(error: &io::Error) -> bool {
    error.raw_os_error() == Some(libc::ELOOP)
}

#[cfg(windows)]
fn is_link_loop(_: &io::Error) -> bool {
    false
}

struct ScannedBytes {
    observed: usize,
    maximum: usize,
}

impl ScannedBytes {
    const fn new(maximum: usize) -> Self {
        Self {
            observed: 0,
            maximum,
        }
    }

    fn consume(&mut self, bytes: usize) -> Result<(), FileToolError> {
        let next = self
            .observed
            .checked_add(bytes)
            .ok_or(FileToolError::SearchLimitExceeded)?;
        if next > self.maximum {
            return Err(FileToolError::SearchLimitExceeded);
        }
        self.observed = next;
        Ok(())
    }

    fn next_read_limit(&self, chunk_bytes: usize) -> usize {
        self.maximum
            .saturating_sub(self.observed)
            .saturating_add(1)
            .min(chunk_bytes)
    }
}

fn read_bounded(
    file: &mut File,
    max_bytes: usize,
    cancellation: &CancellationToken,
    mut scanned_bytes: Option<&mut ScannedBytes>,
) -> Result<Vec<u8>, FileToolError> {
    check_cancelled(cancellation)?;
    if file.metadata()?.len() > max_bytes as u64 {
        return Err(FileToolError::FileTooLarge);
    }
    let mut bytes = Vec::with_capacity(max_bytes.min(64 * 1_024));
    let mut chunk = [0_u8; 64 * 1_024];
    loop {
        check_cancelled(cancellation)?;
        let read_limit = scanned_bytes
            .as_deref()
            .map_or(chunk.len(), |scanned| scanned.next_read_limit(chunk.len()));
        let read = file.read(&mut chunk[..read_limit])?;
        if read == 0 {
            break;
        }
        if bytes.len().saturating_add(read) > max_bytes {
            return Err(FileToolError::FileTooLarge);
        }
        if let Some(scanned_bytes) = scanned_bytes.as_deref_mut() {
            scanned_bytes.consume(read)?;
        }
        bytes.extend_from_slice(&chunk[..read]);
    }
    Ok(bytes)
}

fn check_cancelled(cancellation: &CancellationToken) -> Result<(), FileToolError> {
    if cancellation.is_cancelled() {
        Err(FileToolError::Cancelled)
    } else {
        Ok(())
    }
}

fn text_from_bytes(bytes: &[u8]) -> Result<&str, FileToolError> {
    if bytes.contains(&0) {
        return Err(FileToolError::Binary);
    }
    std::str::from_utf8(bytes).map_err(|_| FileToolError::NotUtf8)
}

fn hex_digest(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

fn protected_metadata_name(name: &str) -> bool {
    name.trim_end_matches(['.', ' '])
        .eq_ignore_ascii_case(".git")
}

fn ignored_directory_name(name: &str) -> bool {
    name == "target"
}

fn relative_to_root<'a>(root: &RelativePath, path: &'a RelativePath) -> &'a str {
    if root.is_root() {
        return path.as_slash_str();
    }
    let prefix_length = root.as_slash_str().len() + 1;
    path.as_slash_str()
        .get(prefix_length..)
        .unwrap_or(path.as_slash_str())
}

fn map_direct_file_error(error: io::Error) -> FileToolError {
    if is_link_loop(&error) {
        return FileToolError::FileNotRegular;
    }
    match error.kind() {
        io::ErrorKind::NotFound => FileToolError::FileNotFound,
        io::ErrorKind::InvalidInput | io::ErrorKind::InvalidData | io::ErrorKind::IsADirectory => {
            FileToolError::FileNotRegular
        }
        _ => FileToolError::Io(error),
    }
}

fn map_direct_directory_error(error: io::Error) -> FileToolError {
    if is_link_loop(&error) {
        return FileToolError::DirectoryNotFound;
    }
    match error.kind() {
        io::ErrorKind::NotFound => FileToolError::DirectoryNotFound,
        io::ErrorKind::InvalidInput | io::ErrorKind::InvalidData | io::ErrorKind::NotADirectory => {
            FileToolError::DirectoryNotFound
        }
        _ => FileToolError::Io(error),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SearchGlob {
    components: Vec<String>,
}

impl SearchGlob {
    fn parse(pattern: &str) -> Result<Self, FileToolError> {
        if pattern.is_empty()
            || pattern.len() > MAX_GLOB_BYTES
            || pattern.starts_with('/')
            || pattern.contains(['\\', '\0', ':'])
            || pattern
                .bytes()
                .filter(|byte| matches!(byte, b'*' | b'?'))
                .count()
                > MAX_GLOB_WILDCARDS
        {
            return Err(FileToolError::InvalidGlob);
        }
        let components = pattern.split('/').map(str::to_owned).collect::<Vec<_>>();
        if components.len() > MAX_GLOB_COMPONENTS
            || components.iter().any(|component| {
                component.is_empty()
                    || component == "."
                    || component == ".."
                    || component.len() > 255
                    || component.contains(['[', ']', '{', '}'])
                    || protected_metadata_name(component)
            })
        {
            return Err(FileToolError::InvalidGlob);
        }
        Ok(Self { components })
    }

    fn matches(&self, path: &str) -> bool {
        let path = path.split('/').collect::<Vec<_>>();
        match_components(&self.components, &path)
    }
}

fn match_components(pattern: &[String], path: &[&str]) -> bool {
    let mut previous = vec![false; path.len() + 1];
    previous[0] = true;
    for component in pattern {
        let mut current = vec![false; path.len() + 1];
        if component == "**" {
            current[0] = previous[0];
            for path_index in 1..=path.len() {
                current[path_index] = previous[path_index] || current[path_index - 1];
            }
        } else {
            for path_index in 1..=path.len() {
                current[path_index] =
                    previous[path_index - 1] && match_component(component, path[path_index - 1]);
            }
        }
        previous = current;
    }
    previous[path.len()]
}

fn match_component(pattern: &str, value: &str) -> bool {
    let pattern = pattern.chars().collect::<Vec<_>>();
    let value = value.chars().collect::<Vec<_>>();
    let mut pattern_index = 0usize;
    let mut value_index = 0usize;
    let mut last_star = None;
    let mut star_value_index = 0usize;

    while value_index < value.len() {
        match pattern.get(pattern_index) {
            Some('?') => {
                pattern_index += 1;
                value_index += 1;
            }
            Some('*') => {
                last_star = Some(pattern_index);
                pattern_index += 1;
                star_value_index = value_index;
            }
            Some(expected) if value.get(value_index) == Some(expected) => {
                pattern_index += 1;
                value_index += 1;
            }
            _ if last_star.is_some() => {
                pattern_index = last_star.expect("checked above") + 1;
                star_value_index += 1;
                value_index = star_value_index;
            }
            _ => return false,
        }
    }
    while pattern.get(pattern_index) == Some(&'*') {
        pattern_index += 1;
    }
    pattern_index == pattern.len()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileToolLimits {
    max_file_bytes: usize,
    max_search_bytes: usize,
    max_read_result_bytes: usize,
    max_result_bytes: usize,
    max_depth: u32,
    max_visited_entries: usize,
    max_directory_entries: usize,
    max_result_items: usize,
}

impl FileToolLimits {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        max_file_bytes: usize,
        max_search_bytes: usize,
        max_read_result_bytes: usize,
        max_result_bytes: usize,
        max_depth: u32,
        max_visited_entries: usize,
        max_directory_entries: usize,
        max_result_items: usize,
    ) -> Result<Self, FileToolLimitsError> {
        if max_depth > MAX_TRAVERSAL_DEPTH {
            return Err(FileToolLimitsError::DepthTooLarge);
        }
        if max_file_bytes == 0
            || max_search_bytes == 0
            || max_read_result_bytes == 0
            || max_result_bytes == 0
            || max_depth == 0
            || max_visited_entries == 0
            || max_directory_entries == 0
            || max_result_items == 0
        {
            return Err(FileToolLimitsError::ZeroLimit);
        }
        Ok(Self {
            max_file_bytes,
            max_search_bytes,
            max_read_result_bytes,
            max_result_bytes,
            max_depth,
            max_visited_entries,
            max_directory_entries,
            max_result_items,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FileToolLimitsError {
    #[error("file tool limits must all be non-zero")]
    ZeroLimit,
    #[error("file tool traversal depth exceeds its hard maximum")]
    DepthTooLarge,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadFileResult {
    pub lines: Vec<NumberedLine>,
    pub sha256: String,
    pub file_bytes: u64,
    pub total_lines: u64,
    pub returned_bytes: usize,
    pub truncated: bool,
    pub next_line: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NumberedLine {
    pub number: u64,
    pub text: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListFilesResult {
    pub entries: Vec<FileEntry>,
    pub visited_entries: usize,
    pub omitted_entries: usize,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileEntry {
    pub path: RelativePath,
    pub kind: FileEntryKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileEntryKind {
    File,
    Directory,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchTextResult {
    pub matches: Vec<SearchMatch>,
    pub visited_files: usize,
    pub skipped_files: usize,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchMatch {
    pub path: RelativePath,
    pub line_number: u64,
    pub column: u64,
    pub preview: String,
}

#[derive(Debug, thiserror::Error)]
pub enum FileToolError {
    #[error("filesystem operation failed")]
    Io(#[source] io::Error),
    #[error("file was not found")]
    FileNotFound,
    #[error("path is not a regular file")]
    FileNotRegular,
    #[error("directory was not found")]
    DirectoryNotFound,
    #[error("file is not valid UTF-8")]
    NotUtf8,
    #[error("file is binary")]
    Binary,
    #[error("file or requested complete line exceeds its configured byte limit")]
    FileTooLarge,
    #[error("directory exceeds its configured entry limit")]
    DirectoryTooLarge,
    #[error("line range must be one-indexed and ordered")]
    InvalidLineRange,
    #[error("requested limit is zero or exceeds the configured maximum")]
    InvalidLimit,
    #[error("search query is empty or outside the supported subset")]
    InvalidQuery,
    #[error("search glob is outside the supported safe subset")]
    InvalidGlob,
    #[error("directory traversal exceeded its configured maximum")]
    TraversalLimitExceeded,
    #[error("search input exceeded its cumulative byte limit")]
    SearchLimitExceeded,
    #[error("file operation was cancelled")]
    Cancelled,
    #[error(transparent)]
    InvalidPath(#[from] RelativePathError),
}

impl From<io::Error> for FileToolError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recursive_list_keeps_the_open_child_when_its_namespace_name_is_swapped() {
        let root_path = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let safe = root_path.path().join("safe");
        let held = root_path.path().join("held");
        std::fs::create_dir(&safe).unwrap();
        std::fs::write(safe.join("inside.txt"), b"inside").unwrap();
        std::fs::write(outside.path().join("outside.txt"), b"outside").unwrap();
        let root = RootCapability::open(root_path.path()).unwrap();
        let limits =
            FileToolLimits::try_new(1_024, 100 * 1_024, 1_024, 1_024, 3, 100, 100, 100).unwrap();
        let tools = FileTools::new(root, limits);
        let mut swapped = false;

        let result = tools
            .list_files_with_hook(
                &RelativePath::parse("").unwrap(),
                2,
                100,
                &CancellationToken::new(),
                &mut |path| {
                    if !swapped && path.as_slash_str() == "safe" {
                        std::fs::rename(&safe, &held)?;
                        create_dir_link(outside.path(), &safe)?;
                        swapped = true;
                    }
                    Ok(())
                },
            )
            .unwrap();
        let paths = result
            .entries
            .iter()
            .map(|entry| entry.path.as_slash_str())
            .collect::<Vec<_>>();

        assert_eq!(paths, vec!["safe", "safe/inside.txt"]);
    }

    #[test]
    fn recursive_search_keeps_the_open_child_when_its_namespace_name_is_swapped() {
        let root_path = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let safe = root_path.path().join("safe");
        let held = root_path.path().join("held");
        std::fs::create_dir(&safe).unwrap();
        std::fs::write(safe.join("inside.txt"), b"needle inside").unwrap();
        std::fs::write(outside.path().join("outside.txt"), b"needle outside").unwrap();
        let root = RootCapability::open(root_path.path()).unwrap();
        let limits =
            FileToolLimits::try_new(1_024, 100 * 1_024, 1_024, 1_024, 3, 100, 100, 100).unwrap();
        let tools = FileTools::new(root, limits);
        let mut swapped = false;

        let result = tools
            .search_text_with_hook(
                "needle",
                &RelativePath::parse("").unwrap(),
                None,
                100,
                &CancellationToken::new(),
                &mut |path| {
                    if !swapped && path.as_slash_str() == "safe" {
                        std::fs::rename(&safe, &held)?;
                        create_dir_link(outside.path(), &safe)?;
                        swapped = true;
                    }
                    Ok(())
                },
            )
            .unwrap();

        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].path.as_slash_str(), "safe/inside.txt");
        assert_eq!(result.matches[0].preview, "needle inside");
    }

    #[test]
    fn recursive_search_observes_cancellation_after_the_walk_started() {
        let root_path = tempfile::tempdir().unwrap();
        let nested = root_path.path().join("nested");
        std::fs::create_dir(&nested).unwrap();
        std::fs::write(nested.join("large.txt"), vec![b'x'; 512 * 1_024]).unwrap();
        let root = RootCapability::open(root_path.path()).unwrap();
        let limits =
            FileToolLimits::try_new(1024 * 1024, 1024 * 1024, 1024, 1024, 3, 100, 100, 100)
                .unwrap();
        let tools = FileTools::new(root, limits);
        let cancellation = CancellationToken::new();

        let result = tools.search_text_with_hook(
            "missing",
            &RelativePath::parse("").unwrap(),
            None,
            100,
            &cancellation,
            &mut |path| {
                if path.as_slash_str() == "nested" {
                    cancellation.cancel();
                }
                Ok(())
            },
        );

        assert!(matches!(result, Err(FileToolError::Cancelled)));
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
