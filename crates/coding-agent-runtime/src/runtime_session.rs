use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use coding_agent_core::{
    AgentRuntime, RuntimeError, TerminalSnapshot, ToolRequest, ToolResult, ToolRuntime, ToolStatus,
    WorkspaceFingerprint,
};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::{
    AtomicFileReplacer, AtomicReplaceError, AtomicReplaceLimits, CargoCatalog, CargoRunResult,
    CargoRunStatus, CargoToolError, CargoToolLimits, CargoTools, CommandPolicyError, DiffCollector,
    DiffError, DiffLimits, FileEntryKind, FileToolError, FileToolLimits, FileTools,
    FingerprintError, FingerprintLimits, GitRunResult, GitRunStatus, GitToolError, GitToolLimits,
    GitTools, ProcessLimits, ProvisionedWorktree, RelativePath, ToolchainPaths,
    WorkspaceFingerprinter,
};

#[derive(Debug, Clone, Copy)]
pub struct RuntimeSessionLimits {
    process: ProcessLimits,
    files: FileToolLimits,
    replace: AtomicReplaceLimits,
    cargo: CargoToolLimits,
    git: GitToolLimits,
    diff: DiffLimits,
    fingerprint: FingerprintLimits,
}

impl RuntimeSessionLimits {
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        process: ProcessLimits,
        files: FileToolLimits,
        replace: AtomicReplaceLimits,
        cargo: CargoToolLimits,
        git: GitToolLimits,
        diff: DiffLimits,
        fingerprint: FingerprintLimits,
    ) -> Self {
        Self {
            process,
            files,
            replace,
            cargo,
            git,
            diff,
            fingerprint,
        }
    }

    /// Conservative Project 2 defaults. Applications may supply lower trusted
    /// limits, but model input never selects these bounds.
    pub fn project_2_defaults() -> Self {
        Self {
            process: ProcessLimits::try_new(
                512 * 1024,
                256 * 1024,
                Duration::from_secs(10 * 60),
                Duration::from_secs(5),
            )
            .expect("constant process limits are valid"),
            files: FileToolLimits::try_new(
                2 * 1024 * 1024,
                16 * 1024 * 1024,
                256 * 1024,
                256 * 1024,
                16,
                10_000,
                5_000,
                2_000,
            )
            .expect("constant file limits are valid"),
            replace: AtomicReplaceLimits::try_new(2 * 1024 * 1024)
                .expect("constant replacement limits are valid"),
            cargo: CargoToolLimits::try_new(Duration::from_secs(30), 512, 4_096, 512)
                .expect("constant Cargo limits are valid"),
            git: GitToolLimits::try_new(Duration::from_secs(30), Duration::from_secs(60))
                .expect("constant Git limits are valid"),
            diff: DiffLimits::try_new(
                Duration::from_secs(30),
                Duration::from_secs(30),
                1_000,
                256 * 1024,
                2 * 1024 * 1024,
                16 * 1024 * 1024,
            )
            .expect("constant diff limits are valid"),
            fingerprint: FingerprintLimits::try_new(
                Duration::from_secs(30),
                10_000,
                8 * 1024 * 1024,
                128 * 1024 * 1024,
            )
            .expect("constant fingerprint limits are valid"),
        }
    }
}

/// Concrete per-attempt implementation of every model-visible tool plus the
/// fingerprint and terminal snapshot operations used by `AgentLoop`.
#[derive(Debug)]
pub struct RuntimeSession {
    files: Arc<FileTools>,
    replacer: Arc<AtomicFileReplacer>,
    cargo: CargoTools,
    git: GitTools,
    diff: DiffCollector,
    fingerprint: WorkspaceFingerprinter,
    cargo_catalog: CargoCatalog,
    output_redactor: KnownPathRedactor,
}

impl RuntimeSession {
    pub fn from_provisioned_worktree(
        worktree: &ProvisionedWorktree,
        toolchain: &ToolchainPaths,
        temporary_directory: impl AsRef<Path>,
        limits: RuntimeSessionLimits,
    ) -> Result<Self, RuntimeSessionError> {
        let work_tree = worktree.work_tree();
        let file_root = work_tree
            .cloned_root_capability()
            .map_err(RuntimeSessionError::CommandPolicy)?;
        let replace_root = work_tree
            .cloned_root_capability()
            .map_err(RuntimeSessionError::CommandPolicy)?;
        let temporary_directory = temporary_directory.as_ref();
        let cargo = CargoTools::from_trusted_capabilities(
            toolchain,
            worktree.cargo_workspace(),
            worktree.target_directory(),
            temporary_directory,
            limits.process,
            limits.cargo,
        )
        .map_err(RuntimeSessionError::Cargo)?;
        let output_redactor = KnownPathRedactor::for_session(
            worktree,
            toolchain,
            temporary_directory,
            cargo.redaction_paths(),
        );
        let git = GitTools::from_trusted_capabilities(
            toolchain,
            worktree.git_directory(),
            Arc::clone(&work_tree),
            temporary_directory,
            limits.process,
            limits.git,
        )
        .map_err(RuntimeSessionError::Git)?;
        let diff = DiffCollector::from_trusted_capabilities(
            toolchain,
            worktree.git_directory(),
            Arc::clone(&work_tree),
            temporary_directory,
            limits.process,
            limits.diff,
        )
        .map_err(RuntimeSessionError::Diff)?;
        let fingerprint = WorkspaceFingerprinter::from_trusted_capabilities(
            toolchain,
            worktree.git_directory(),
            work_tree,
            temporary_directory,
            limits.process,
            limits.fingerprint,
        )
        .map_err(RuntimeSessionError::Fingerprint)?;
        Ok(Self {
            files: Arc::new(FileTools::new(file_root, limits.files)),
            replacer: Arc::new(AtomicFileReplacer::new(replace_root, limits.replace)),
            cargo,
            git,
            diff,
            fingerprint,
            cargo_catalog: worktree.cargo_catalog().clone(),
            output_redactor,
        })
    }

    pub fn cargo_catalog(&self) -> &CargoCatalog {
        &self.cargo_catalog
    }

    /// Bounded repository context safe to place in the initial model request.
    pub fn repository_context(&self) -> String {
        self.cargo_catalog.repository_context()
    }

    async fn stable_terminal_snapshot(
        &self,
        revision: u64,
        cancellation: CancellationToken,
    ) -> Result<TerminalSnapshot, RuntimeError> {
        let before = self
            .fingerprint
            .collect(cancellation.clone())
            .await
            .map_err(runtime_fingerprint_error)?;
        let mut diff = self
            .diff
            .collect(revision, cancellation.clone())
            .await
            .map_err(runtime_diff_error)?;
        let after = self
            .fingerprint
            .collect(cancellation)
            .await
            .map_err(runtime_fingerprint_error)?;
        if before != after {
            return Err(RuntimeError::new(
                "WORKSPACE_CHANGED",
                "workspace changed during terminal snapshot",
                true,
            ));
        }
        diff.revision = revision;
        Ok(TerminalSnapshot {
            fingerprint: after,
            diff,
        })
    }
}

#[async_trait::async_trait]
impl ToolRuntime for RuntimeSession {
    async fn invoke(
        &self,
        request: ToolRequest,
        cancellation: CancellationToken,
    ) -> Result<ToolResult, RuntimeError> {
        if cancellation.is_cancelled() {
            return Err(cancelled_error());
        }
        match request {
            ToolRequest::ListFiles { path, depth, limit } => {
                let path = match RelativePath::parse(path) {
                    Ok(path) => path,
                    Err(_) => return Ok(failed_result("COMMAND_NOT_ALLOWED")),
                };
                let files = Arc::clone(&self.files);
                let operation_cancellation = cancellation.clone();
                let result = await_blocking(tokio::task::spawn_blocking(move || {
                    files.list_files_cancellable(&path, depth, limit, &operation_cancellation)
                }))
                .await?;
                match result {
                    Ok(result) => {
                        let entries = result
                            .entries
                            .iter()
                            .map(|entry| {
                                json!({
                                    "path": entry.path.as_slash_str(),
                                    "kind": match entry.kind {
                                        FileEntryKind::File => "file",
                                        FileEntryKind::Directory => "directory",
                                    },
                                })
                            })
                            .collect::<Vec<_>>();
                        Ok(success_result(
                            json!({
                                "entries": entries,
                                "visited_entries": result.visited_entries,
                                "omitted_entries": result.omitted_entries,
                            }),
                            result.truncated,
                        ))
                    }
                    Err(error) => file_error_result(error),
                }
            }
            ToolRequest::ReadFile {
                path,
                start_line,
                end_line,
            } => {
                let path = match RelativePath::parse(path) {
                    Ok(path) => path,
                    Err(_) => return Ok(failed_result("COMMAND_NOT_ALLOWED")),
                };
                let files = Arc::clone(&self.files);
                let operation_cancellation = cancellation.clone();
                let result = await_blocking(tokio::task::spawn_blocking(move || {
                    files.read_file_cancellable(
                        &path,
                        start_line,
                        end_line,
                        &operation_cancellation,
                    )
                }))
                .await?;
                match result {
                    Ok(result) => Ok(success_result(
                        json!({
                            "lines": result.lines.iter().map(|line| json!({
                                "number": line.number,
                                "text": line.text,
                            })).collect::<Vec<_>>(),
                            "sha256": result.sha256,
                            "file_bytes": result.file_bytes,
                            "total_lines": result.total_lines,
                            "returned_bytes": result.returned_bytes,
                            "next_line": result.next_line,
                        }),
                        result.truncated,
                    )),
                    Err(error) => file_error_result(error),
                }
            }
            ToolRequest::SearchText {
                query,
                path,
                glob,
                limit,
            } => {
                let path = match RelativePath::parse(path) {
                    Ok(path) => path,
                    Err(_) => return Ok(failed_result("COMMAND_NOT_ALLOWED")),
                };
                let files = Arc::clone(&self.files);
                let operation_cancellation = cancellation.clone();
                let result = await_blocking(tokio::task::spawn_blocking(move || {
                    files.search_text_cancellable(
                        &query,
                        &path,
                        glob.as_deref(),
                        limit,
                        &operation_cancellation,
                    )
                }))
                .await?;
                match result {
                    Ok(result) => Ok(success_result(
                        json!({
                            "matches": result.matches.iter().map(|entry| json!({
                                "path": entry.path.as_slash_str(),
                                "line_number": entry.line_number,
                                "column": entry.column,
                                "preview": entry.preview,
                            })).collect::<Vec<_>>(),
                            "visited_files": result.visited_files,
                            "skipped_files": result.skipped_files,
                        }),
                        result.truncated,
                    )),
                    Err(error) => file_error_result(error),
                }
            }
            ToolRequest::ReplaceFile {
                path,
                expected_sha256,
                content,
            } => {
                let path = match RelativePath::parse(path) {
                    Ok(path) => path,
                    Err(_) => return Ok(failed_result("COMMAND_NOT_ALLOWED")),
                };
                let replacer = Arc::clone(&self.replacer);
                let operation_cancellation = cancellation.clone();
                let result = await_blocking(tokio::task::spawn_blocking(move || {
                    replacer.replace_file(
                        &path,
                        expected_sha256.as_deref(),
                        content.as_bytes(),
                        &operation_cancellation,
                    )
                }))
                .await?;
                match result {
                    Ok(result) => Ok(success_result(
                        json!({
                            "disposition": format!("{:?}", result.disposition).to_ascii_lowercase(),
                            "sha256": result.sha256,
                            "bytes_written": result.bytes_written,
                        }),
                        false,
                    )),
                    Err(AtomicReplaceError::Cancelled) => Err(cancelled_error()),
                    Err(error) => Ok(failed_result(error.code())),
                }
            }
            ToolRequest::CargoCheck {
                package,
                timeout_ms,
            } => match self
                .cargo
                .check(
                    package.as_deref(),
                    Duration::from_millis(timeout_ms),
                    cancellation,
                )
                .await
            {
                Ok(result) => cargo_result(result, &self.output_redactor),
                Err(error) => cargo_error(error),
            },
            ToolRequest::CargoTest {
                package,
                test,
                timeout_ms,
            } => match self
                .cargo
                .test(
                    package.as_deref(),
                    test.as_deref(),
                    Duration::from_millis(timeout_ms),
                    cancellation,
                )
                .await
            {
                Ok(result) => cargo_result(result, &self.output_redactor),
                Err(error) => cargo_error(error),
            },
            ToolRequest::GitStatus => match self.git.status(cancellation).await {
                Ok(result) => git_result(result, &self.output_redactor),
                Err(error) => Err(runtime_git_error(error)),
            },
            ToolRequest::GitDiff => match self.git.diff(cancellation).await {
                Ok(result) => git_result(result, &self.output_redactor),
                Err(error) => Err(runtime_git_error(error)),
            },
        }
    }
}

#[async_trait::async_trait]
impl AgentRuntime for RuntimeSession {
    async fn workspace_fingerprint(
        &self,
        cancellation: CancellationToken,
    ) -> Result<WorkspaceFingerprint, RuntimeError> {
        self.fingerprint
            .collect(cancellation)
            .await
            .map_err(runtime_fingerprint_error)
    }

    async fn terminal_snapshot(
        &self,
        revision: u64,
        cancellation: CancellationToken,
    ) -> Result<TerminalSnapshot, RuntimeError> {
        self.stable_terminal_snapshot(revision, cancellation).await
    }
}

fn cargo_result(
    result: CargoRunResult,
    redactor: &KnownPathRedactor,
) -> Result<ToolResult, RuntimeError> {
    match result.status {
        CargoRunStatus::Cancelled => Err(cancelled_error()),
        CargoRunStatus::TimedOut => Err(timeout_error()),
        CargoRunStatus::Passed => Ok(command_result(
            &result.command,
            ToolStatus::Succeeded,
            redactor,
        )),
        CargoRunStatus::Failed => Ok(command_result(
            &result.command,
            ToolStatus::Failed,
            redactor,
        )),
    }
}

fn git_result(
    result: GitRunResult,
    redactor: &KnownPathRedactor,
) -> Result<ToolResult, RuntimeError> {
    match result.status {
        GitRunStatus::Cancelled => Err(cancelled_error()),
        GitRunStatus::TimedOut => Err(timeout_error()),
        GitRunStatus::Succeeded => Ok(command_result(
            &result.command,
            ToolStatus::Succeeded,
            redactor,
        )),
        GitRunStatus::Failed => Ok(command_result(
            &result.command,
            ToolStatus::Failed,
            redactor,
        )),
    }
}

fn command_result(
    command: &crate::CommandResult,
    status: ToolStatus,
    redactor: &KnownPathRedactor,
) -> ToolResult {
    let value = json!({
        "exit_code": command.exit_code,
        "signal": command.signal,
        "duration_ms": command.duration_ms,
        "stdout": captured_stream(&command.stdout, redactor),
        "stderr": captured_stream(&command.stderr, redactor),
    });
    match status {
        ToolStatus::Succeeded if command.truncated => ToolResult::truncated_text(value.to_string()),
        ToolStatus::Succeeded => ToolResult::text(value.to_string()),
        ToolStatus::Failed if command.truncated => {
            ToolResult::truncated_failed_text(value.to_string())
        }
        ToolStatus::Failed => ToolResult::failed_text(value.to_string()),
    }
}

fn captured_stream(stream: &crate::CapturedStream, redactor: &KnownPathRedactor) -> Value {
    let mut retained = Vec::with_capacity(stream.head.len().saturating_add(stream.tail.len()));
    retained.extend_from_slice(&stream.head);
    retained.extend_from_slice(&stream.tail);
    json!({
        "text": redactor.redact(&String::from_utf8_lossy(&retained)),
        "observed_bytes": stream.observed_bytes,
        "omitted_observed_bytes": stream.omitted_observed_bytes,
        "truncated": stream.truncated,
        "complete": stream.complete,
    })
}

const REDACTED_PATH: &str = "<redacted-path>";

#[derive(Clone)]
struct KnownPathRedactor {
    patterns: Vec<String>,
}

impl KnownPathRedactor {
    fn for_session(
        worktree: &ProvisionedWorktree,
        toolchain: &ToolchainPaths,
        temporary_directory: &Path,
        additional_paths: &[PathBuf],
    ) -> Self {
        let mut paths = vec![
            worktree.worktree_path().to_owned(),
            worktree.cargo_workspace_path().to_owned(),
            worktree.git_directory().path().to_owned(),
            worktree.work_tree().path().to_owned(),
            worktree.cargo_workspace().path().to_owned(),
            worktree.target_directory().path().to_owned(),
            temporary_directory.to_owned(),
            toolchain.cargo_home().to_owned(),
            toolchain.cargo().path().to_owned(),
            toolchain.rustc().path().to_owned(),
            toolchain.rustdoc().path().to_owned(),
            toolchain.git().path().to_owned(),
        ];
        paths.extend(toolchain.search_directories().iter().cloned());
        paths.extend(additional_paths.iter().cloned());

        // The linked Git directory is nested below `<repository>/.git/worktrees`.
        // Include its trusted ancestors so diagnostics that print the common
        // Git directory rather than the exact linked entry are also redacted.
        paths.extend(
            worktree
                .git_directory()
                .path()
                .ancestors()
                .skip(1)
                .take(3)
                .map(Path::to_owned),
        );
        // Attempt worktrees are rooted at
        // `<artifact>/worktrees/<repository>/<task>/<attempt>`.
        if let Some(artifact_root) = worktree.worktree_path().ancestors().nth(4) {
            paths.push(artifact_root.to_owned());
        }
        for path in paths.clone() {
            if let Ok(canonical) = std::fs::canonicalize(path) {
                paths.push(canonical);
            }
        }
        Self::from_paths(paths)
    }

    fn from_paths(paths: impl IntoIterator<Item = PathBuf>) -> Self {
        let mut patterns = Vec::new();
        for path in paths {
            if !path.is_absolute() {
                continue;
            }
            let native = path.to_string_lossy().into_owned();
            #[cfg(windows)]
            let unprefixed = native.strip_prefix(r"\\?\").map(str::to_owned);
            Self::push_native_path_patterns(&mut patterns, native);
            #[cfg(windows)]
            if let Some(unprefixed) = unprefixed {
                Self::push_native_path_patterns(&mut patterns, unprefixed);
            }
        }
        patterns.sort_by_key(|pattern| std::cmp::Reverse(pattern.len()));
        patterns.dedup();
        Self { patterns }
    }

    fn push_native_path_patterns(patterns: &mut Vec<String>, native: String) {
        Self::push_pattern(patterns, native.clone());
        Self::push_pattern(patterns, native.replace('\\', "/"));
        #[cfg(windows)]
        Self::push_pattern(patterns, native.replace('\\', r"\\"));
    }

    fn push_pattern(patterns: &mut Vec<String>, pattern: String) {
        // Never admit a filesystem root as a replacement pattern: doing so
        // would destroy the bounded diagnostic rather than redact a known path.
        if pattern.len() >= 4 && !patterns.contains(&pattern) {
            patterns.push(pattern);
        }
    }

    fn redact(&self, value: &str) -> String {
        self.patterns
            .iter()
            .fold(value.to_owned(), |redacted, pattern| {
                replace_known_path(&redacted, pattern, REDACTED_PATH)
            })
    }
}

impl fmt::Debug for KnownPathRedactor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KnownPathRedactor")
            .field("pattern_count", &self.patterns.len())
            .finish()
    }
}

fn replace_known_path(value: &str, pattern: &str, replacement: &str) -> String {
    if pattern.is_empty() || value.len() < pattern.len() {
        return value.to_owned();
    }
    let value_bytes = value.as_bytes();
    let pattern_bytes = pattern.as_bytes();
    let mut output = String::with_capacity(value.len());
    let mut copied = 0usize;
    let mut index = 0usize;
    while index + pattern_bytes.len() <= value_bytes.len() {
        let end = index + pattern_bytes.len();
        let matches = value.is_char_boundary(index) && value.is_char_boundary(end) && {
            #[cfg(windows)]
            {
                value_bytes[index..end].eq_ignore_ascii_case(pattern_bytes)
            }
            #[cfg(not(windows))]
            {
                &value_bytes[index..end] == pattern_bytes
            }
        };
        if matches && has_path_boundaries(value, index, end) {
            output.push_str(&value[copied..index]);
            output.push_str(replacement);
            copied = end;
            index = end;
        } else {
            index += 1;
        }
    }
    output.push_str(&value[copied..]);
    output
}

fn has_path_boundaries(value: &str, start: usize, end: usize) -> bool {
    let before = value[..start].chars().next_back();
    let after = value[end..].chars().next();
    before.is_none_or(|character| !is_path_token_character(character))
        && after.is_none_or(|character| {
            matches!(character, '/' | '\\') || !is_path_token_character(character)
        })
}

fn is_path_token_character(character: char) -> bool {
    character.is_alphanumeric() || matches!(character, '_' | '-' | '.')
}

fn success_result(value: Value, truncated: bool) -> ToolResult {
    if truncated {
        ToolResult::truncated_text(value.to_string())
    } else {
        ToolResult::text(value.to_string())
    }
}

fn failed_result(code: &str) -> ToolResult {
    ToolResult::failed_text(json!({ "code": code }).to_string())
}

async fn await_blocking<T>(handle: tokio::task::JoinHandle<T>) -> Result<T, RuntimeError>
where
    T: Send + 'static,
{
    handle
        .await
        .map_err(|_| RuntimeError::new("FILE_WORKER_FAILED", "bounded file worker failed", true))
}

fn file_error_result(error: FileToolError) -> Result<ToolResult, RuntimeError> {
    if matches!(error, FileToolError::Cancelled) {
        return Err(cancelled_error());
    }
    let code = match error {
        FileToolError::FileNotFound | FileToolError::DirectoryNotFound => "FILE_NOT_FOUND",
        FileToolError::FileNotRegular => "FILE_NOT_REGULAR",
        FileToolError::NotUtf8 | FileToolError::Binary => "FILE_NOT_TEXT",
        FileToolError::FileTooLarge
        | FileToolError::DirectoryTooLarge
        | FileToolError::TraversalLimitExceeded
        | FileToolError::SearchLimitExceeded => "FILE_TOO_LARGE",
        FileToolError::InvalidLineRange
        | FileToolError::InvalidLimit
        | FileToolError::InvalidQuery
        | FileToolError::InvalidGlob
        | FileToolError::InvalidPath(_) => "COMMAND_NOT_ALLOWED",
        FileToolError::Io(_) => "FILE_IO_FAILED",
        FileToolError::Cancelled => unreachable!("handled above"),
    };
    Ok(failed_result(code))
}

fn cargo_error(error: CargoToolError) -> Result<ToolResult, RuntimeError> {
    match error {
        CargoToolError::Cancelled => Err(cancelled_error()),
        CargoToolError::TimedOut => Err(timeout_error()),
        CargoToolError::Process(error) => Err(RuntimeError::new(
            error.code(),
            "Cargo process supervision failed",
            retryable_code(error.code()),
        )),
        error => Ok(failed_result(error.code())),
    }
}

fn runtime_git_error(error: GitToolError) -> RuntimeError {
    RuntimeError::new(
        error.code(),
        "Git tool execution failed",
        retryable_code(error.code()),
    )
}

fn runtime_fingerprint_error(error: FingerprintError) -> RuntimeError {
    RuntimeError::new(
        error.code(),
        "workspace fingerprint failed",
        retryable_code(error.code()),
    )
}

fn runtime_diff_error(error: DiffError) -> RuntimeError {
    RuntimeError::new(
        error.code(),
        "terminal diff collection failed",
        retryable_code(error.code()),
    )
}

fn retryable_code(code: &str) -> bool {
    matches!(
        code,
        "COMMAND_TIMED_OUT" | "WORKSPACE_CHANGED" | "WORKTREE_CHANGED_DURING_DIFF"
    )
}

fn cancelled_error() -> RuntimeError {
    RuntimeError::new("COMMAND_CANCELLED", "runtime operation cancelled", false)
}

fn timeout_error() -> RuntimeError {
    RuntimeError::new("COMMAND_TIMED_OUT", "runtime operation timed out", true)
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeSessionError {
    #[error("the worktree root capability could not be cloned")]
    CommandPolicy(#[source] CommandPolicyError),
    #[error("Cargo tools could not be bound")]
    Cargo(#[source] CargoToolError),
    #[error("Git tools could not be bound")]
    Git(#[source] GitToolError),
    #[error("diff collection could not be bound")]
    Diff(#[source] DiffError),
    #[error("fingerprint collection could not be bound")]
    Fingerprint(#[source] FingerprintError),
}

impl RuntimeSessionError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::CommandPolicy(error) => error.code(),
            Self::Cargo(error) => error.code(),
            Self::Git(error) => error.code(),
            Self::Diff(error) => error.code(),
            Self::Fingerprint(error) => error.code(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_fully_validated_and_model_independent() {
        let _ = RuntimeSessionLimits::project_2_defaults();
    }

    #[test]
    fn failed_command_status_remains_an_ordinary_tool_result() {
        let redactor = KnownPathRedactor::from_paths(std::iter::empty());
        let result = crate::CommandResult {
            exit_code: Some(1),
            signal: None,
            timed_out: false,
            cancelled: false,
            stdout: crate::CapturedStream {
                head: b"bounded".to_vec(),
                tail: Vec::new(),
                observed_bytes: 7,
                omitted_observed_bytes: 0,
                truncated: false,
                complete: true,
            },
            stderr: crate::CapturedStream {
                head: Vec::new(),
                tail: Vec::new(),
                observed_bytes: 0,
                omitted_observed_bytes: 0,
                truncated: false,
                complete: true,
            },
            truncated: false,
            duration_ms: 2,
        };
        let result = command_result(&result, ToolStatus::Failed, &redactor);
        assert_eq!(result.status(), ToolStatus::Failed);
        assert!(result.content().contains("bounded"));
    }

    #[test]
    fn captured_command_streams_redact_native_and_slash_known_paths() {
        let root = std::env::temp_dir().join("runtime-session-secret-root");
        let native = root.to_string_lossy().into_owned();
        let slash = native.replace('\\', "/");
        let redactor = KnownPathRedactor::from_paths([root]);
        let encoded = format!("native={native}\nslash={slash}\n");
        let observed_bytes = encoded.len() as u64;
        let command = crate::CommandResult {
            exit_code: Some(0),
            signal: None,
            timed_out: false,
            cancelled: false,
            stdout: crate::CapturedStream {
                head: encoded.into_bytes(),
                tail: Vec::new(),
                observed_bytes,
                omitted_observed_bytes: 0,
                truncated: false,
                complete: true,
            },
            stderr: crate::CapturedStream {
                head: Vec::new(),
                tail: Vec::new(),
                observed_bytes: 0,
                omitted_observed_bytes: 0,
                truncated: false,
                complete: true,
            },
            truncated: false,
            duration_ms: 1,
        };

        let result = command_result(&command, ToolStatus::Succeeded, &redactor);

        assert!(!result.content().contains(&native));
        assert!(!result.content().contains(&slash));
        assert_eq!(result.content().matches(REDACTED_PATH).count(), 2);

        let descendant = format!("{native}/src/lib.rs");
        assert_eq!(
            redactor.redact(&descendant),
            format!("{REDACTED_PATH}/src/lib.rs")
        );
        let similar_source_text = format!("{native}-example");
        assert_eq!(redactor.redact(&similar_source_text), similar_source_text);
    }

    #[cfg(windows)]
    #[test]
    fn cargo_json_escaped_windows_paths_are_redacted_with_token_boundaries() {
        let unprefixed = r"C:\Users\runneradmin\AppData\Local\Temp\coding-agent";
        let prefixed = format!(r"\\?\{unprefixed}");
        let redactor = KnownPathRedactor::from_paths([PathBuf::from(&prefixed)]);
        let escaped_prefixed = prefixed.replace('\\', r"\\");
        let escaped_unprefixed = unprefixed.replace('\\', r"\\");
        let encoded = format!(
            r#"{{"descendant":"{escaped_prefixed}\\workspace","root":"{escaped_unprefixed}"}}"#
        );
        let observed_bytes = encoded.len() as u64;
        let stream = crate::CapturedStream {
            head: encoded.into_bytes(),
            tail: Vec::new(),
            observed_bytes,
            omitted_observed_bytes: 0,
            truncated: false,
            complete: true,
        };

        let captured = captured_stream(&stream, &redactor);
        let text = captured["text"].as_str().unwrap();
        assert!(!text.contains(&escaped_prefixed));
        assert!(!text.contains(&escaped_unprefixed));
        let inner: Value = serde_json::from_str(text).unwrap();
        assert_eq!(
            inner["descendant"],
            Value::String(format!(r"{REDACTED_PATH}\workspace"))
        );
        assert_eq!(inner["root"], Value::String(REDACTED_PATH.to_owned()));

        let similar = format!(r#""{escaped_unprefixed}-example""#);
        assert_eq!(redactor.redact(&similar), similar);
    }
}
