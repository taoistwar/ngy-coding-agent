use std::fmt::Write as _;

use coding_agent_domain::{MAX_WORKSPACE_GENERATION, ReviewCoverageEvidence, WorkspaceDigest};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    ContextRedactor, DiffFileStatus, MAX_REVIEW_DIFF_CHUNKS, WorkspaceCheckpoint,
    WorkspaceFingerprint,
};

/// Versioned domain separator for the Project 3 canonical review manifest.
///
/// Changing the canonical field order or encoding requires a new separator.
pub const REVIEW_DIFF_MANIFEST_DOMAIN: &[u8] = b"coding-agent-review-diff-manifest-v1\0";
/// Typed content bound for one authoritative diff chunk.
///
/// This is deliberately not the protocol-wrapper bound. Task 9 owns the
/// wrapper-inclusive 20 KiB proof.
pub const MAX_REVIEW_DIFF_TYPED_CHUNK_BYTES: usize = 16 * 1024;
const MAX_REVIEW_DIFF_TYPED_STREAM_BYTES: usize =
    MAX_REVIEW_DIFF_TYPED_CHUNK_BYTES * MAX_REVIEW_DIFF_CHUNKS as usize;
/// Typed canonical-manifest bound.
///
/// Task 9 separately proves the complete retained ToolResult fits 24 KiB.
pub const MAX_REVIEW_DIFF_TYPED_MANIFEST_BYTES: usize = 20 * 1024;
pub const MAX_REVIEW_DIFF_BATCH_CHUNKS: u8 = 2;
pub const MAX_REVIEW_DIFF_BATCHES: u8 = 4;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ReviewDiffError {
    #[error("the review diff checkpoint is invalid")]
    InvalidCheckpoint,
    #[error("a review diff path is not a safe canonical UTF-8 relative path")]
    InvalidPath,
    #[error("the review diff contains duplicate paths")]
    DuplicatePath,
    #[error("the review diff patch is not safely representable")]
    InvalidPatch,
    #[error("review diff numeric metadata exceeds the JSON safe-integer range")]
    InvalidFileMetadata,
    #[error("review diff redaction is unsafe, unstable, or changes structured path identity")]
    UnsafeRedaction,
    #[error("the canonical review diff manifest exceeds its typed bound")]
    ManifestTooLarge,
    #[error("the review diff requires more than eight typed chunks")]
    TooManyChunks,
    #[error("the review diff chunk request is invalid")]
    InvalidChunkRequest,
    #[error("the review diff chunk request does not match the cached manifest")]
    ManifestMismatch,
    #[error("review coverage does not match the authoritative manifest")]
    CoverageMismatch,
    #[error("approved review coverage is incomplete")]
    IncompleteCoverage,
}

/// Immutable authority passed from core to the runtime.
///
/// Generation and digest are derived from the same trusted checkpoint. A
/// provider cannot independently choose any of these fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewDiffCheckpoint {
    generation: u64,
    fingerprint: WorkspaceFingerprint,
    workspace_digest: WorkspaceDigest,
}

impl ReviewDiffCheckpoint {
    pub fn from_workspace_checkpoint(checkpoint: &WorkspaceCheckpoint) -> Self {
        Self {
            generation: checkpoint.generation(),
            fingerprint: checkpoint.fingerprint(),
            workspace_digest: checkpoint.workspace_digest(),
        }
    }

    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub const fn fingerprint(&self) -> WorkspaceFingerprint {
        self.fingerprint
    }

    pub const fn workspace_digest(&self) -> &WorkspaceDigest {
        &self.workspace_digest
    }
}

/// Complete UTF-8 input for one file in the authoritative review diff.
///
/// Runtime collection must reject binary, lossy, truncated, or otherwise
/// incomplete inputs before constructing this value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewDiffInputFile {
    path: String,
    status: DiffFileStatus,
    additions: u64,
    deletions: u64,
    patch: String,
}

impl ReviewDiffInputFile {
    pub fn try_new(
        path: impl Into<String>,
        status: DiffFileStatus,
        additions: u64,
        deletions: u64,
        patch: impl Into<String>,
    ) -> Result<Self, ReviewDiffError> {
        let path = path.into();
        let patch = patch.into();
        if !is_valid_review_path(&path) {
            return Err(ReviewDiffError::InvalidPath);
        }
        if additions > MAX_WORKSPACE_GENERATION || deletions > MAX_WORKSPACE_GENERATION {
            return Err(ReviewDiffError::InvalidFileMetadata);
        }
        // JSON/protocol encoding can safely escape other UTF-8 controls, but a
        // NUL would make the textual stream ambiguous with versioned framing.
        if patch.contains('\0') {
            return Err(ReviewDiffError::InvalidPatch);
        }
        Ok(Self {
            path,
            status,
            additions,
            deletions,
            patch,
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub const fn status(&self) -> DiffFileStatus {
        self.status
    }

    pub const fn additions(&self) -> u64 {
        self.additions
    }

    pub const fn deletions(&self) -> u64 {
        self.deletions
    }

    pub fn patch(&self) -> &str {
        &self.patch
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewDiffManifestFile {
    path: String,
    status: DiffFileStatus,
    additions: u64,
    deletions: u64,
    patch_bytes: u64,
    patch_sha256: String,
    first_chunk: u8,
    chunk_count: u8,
}

impl ReviewDiffManifestFile {
    pub fn path(&self) -> &str {
        &self.path
    }

    pub const fn status(&self) -> DiffFileStatus {
        self.status
    }

    pub const fn additions(&self) -> u64 {
        self.additions
    }

    pub const fn deletions(&self) -> u64 {
        self.deletions
    }

    pub const fn patch_bytes(&self) -> u64 {
        self.patch_bytes
    }

    pub fn patch_sha256(&self) -> &str {
        &self.patch_sha256
    }

    pub const fn first_chunk(&self) -> u8 {
        self.first_chunk
    }

    pub const fn chunk_count(&self) -> u8 {
        self.chunk_count
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewDiffManifest {
    generation: u64,
    workspace_digest: WorkspaceDigest,
    files: Vec<ReviewDiffManifestFile>,
    chunk_count: u8,
    manifest_sha256: String,
    canonical_bytes: Vec<u8>,
}

impl ReviewDiffManifest {
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub const fn workspace_digest(&self) -> &WorkspaceDigest {
        &self.workspace_digest
    }

    pub fn files(&self) -> &[ReviewDiffManifestFile] {
        &self.files
    }

    pub const fn chunk_count(&self) -> u8 {
        self.chunk_count
    }

    pub fn manifest_sha256(&self) -> &str {
        &self.manifest_sha256
    }

    /// Canonical v1 JSON payload bytes excluding the domain separator.
    ///
    /// This is the sole encoding of the visible manifest fields. Task 9 must
    /// embed these bytes (plus the separately returned SHA-256) rather than
    /// independently re-encode a second manifest representation.
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    pub const fn required_batch_count(&self) -> u8 {
        self.chunk_count.div_ceil(MAX_REVIEW_DIFF_BATCH_CHUNKS)
    }

    /// Constructs domain evidence from an externally authoritative visible set.
    ///
    /// This method validates shape and identity only. It intentionally does not
    /// claim the chunks reached a provider transcript; Task 11 supplies that
    /// model-visibility authority.
    pub fn coverage_evidence(
        &self,
        covered_chunks: Vec<u8>,
    ) -> Result<ReviewCoverageEvidence, ReviewDiffError> {
        ReviewCoverageEvidence::try_new(
            self.generation,
            self.workspace_digest.clone(),
            self.manifest_sha256.clone(),
            covered_chunks,
            self.chunk_count,
        )
        .map_err(|_| ReviewDiffError::CoverageMismatch)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewDiffChunk {
    index: u8,
    stream_start: u64,
    stream_end: u64,
    content: String,
}

impl ReviewDiffChunk {
    pub const fn index(&self) -> u8 {
        self.index
    }

    pub const fn stream_start(&self) -> u64 {
        self.stream_start
    }

    pub const fn stream_end(&self) -> u64 {
        self.stream_end
    }

    pub fn content(&self) -> &str {
        &self.content
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewDiffChunkRequest {
    generation: u64,
    workspace_digest: WorkspaceDigest,
    manifest_sha256: String,
    start_chunk: u8,
    count: u8,
}

impl ReviewDiffChunkRequest {
    pub fn try_exact(
        generation: u64,
        workspace_digest: WorkspaceDigest,
        manifest_sha256: impl Into<String>,
        start_chunk: u8,
        count: u8,
    ) -> Result<Self, ReviewDiffError> {
        let manifest_sha256 = manifest_sha256.into();
        let end = start_chunk
            .checked_add(count)
            .ok_or(ReviewDiffError::InvalidChunkRequest)?;
        if generation > MAX_WORKSPACE_GENERATION
            || manifest_sha256.len() != 64
            || !manifest_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || count == 0
            || count > MAX_REVIEW_DIFF_BATCH_CHUNKS
            || end > MAX_REVIEW_DIFF_CHUNKS
        {
            return Err(ReviewDiffError::InvalidChunkRequest);
        }
        Ok(Self {
            generation,
            workspace_digest,
            manifest_sha256,
            start_chunk,
            count,
        })
    }

    pub fn for_manifest(
        manifest: &ReviewDiffManifest,
        start_chunk: u8,
        count: u8,
    ) -> Result<Self, ReviewDiffError> {
        let end = start_chunk
            .checked_add(count)
            .ok_or(ReviewDiffError::InvalidChunkRequest)?;
        if count == 0 || count > MAX_REVIEW_DIFF_BATCH_CHUNKS || end > manifest.chunk_count {
            return Err(ReviewDiffError::InvalidChunkRequest);
        }
        Ok(Self {
            generation: manifest.generation,
            workspace_digest: manifest.workspace_digest.clone(),
            manifest_sha256: manifest.manifest_sha256.clone(),
            start_chunk,
            count,
        })
    }

    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub const fn workspace_digest(&self) -> &WorkspaceDigest {
        &self.workspace_digest
    }

    pub fn manifest_sha256(&self) -> &str {
        &self.manifest_sha256
    }

    pub const fn start_chunk(&self) -> u8 {
        self.start_chunk
    }

    pub const fn count(&self) -> u8 {
        self.count
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewDiffChunkBatch {
    generation: u64,
    workspace_digest: WorkspaceDigest,
    manifest_sha256: String,
    start_chunk: u8,
    chunks: Vec<ReviewDiffChunk>,
}

impl ReviewDiffChunkBatch {
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub const fn workspace_digest(&self) -> &WorkspaceDigest {
        &self.workspace_digest
    }

    pub fn manifest_sha256(&self) -> &str {
        &self.manifest_sha256
    }

    pub const fn start_chunk(&self) -> u8 {
        self.start_chunk
    }

    pub fn chunks(&self) -> &[ReviewDiffChunk] {
        &self.chunks
    }
}

/// One immutable terminal-diff representation used by both manifest and
/// chunks. Runtime caches this bundle only for its exact checkpoint.
#[derive(Debug, PartialEq, Eq)]
pub struct ReviewDiffBundle {
    manifest: ReviewDiffManifest,
    chunks: Vec<ReviewDiffChunk>,
}

impl ReviewDiffBundle {
    pub fn try_new(
        checkpoint: &ReviewDiffCheckpoint,
        mut files: Vec<ReviewDiffInputFile>,
        redactor: &dyn ContextRedactor,
    ) -> Result<Self, ReviewDiffError> {
        if checkpoint.generation > MAX_WORKSPACE_GENERATION
            || checkpoint.workspace_digest.value() != fingerprint_hex(checkpoint.fingerprint)
        {
            return Err(ReviewDiffError::InvalidCheckpoint);
        }

        // Redact each complete raw UTF-8 patch exactly once at this authority
        // boundary, before any byte count, hash, chunk, cache, or manifest is
        // derived. Reapplying the same redactor must be a no-op so later
        // protocol layers never need to redact typed diff data again.
        for file in &mut files {
            if !redaction_is_stable(redactor, &file.path) {
                return Err(ReviewDiffError::UnsafeRedaction);
            }
            let redacted_patch = redactor.redact(&file.patch);
            if redacted_patch.contains('\0')
                || redactor.redact(&file.patch) != redacted_patch
                || !redaction_is_stable(redactor, &redacted_patch)
            {
                return Err(ReviewDiffError::UnsafeRedaction);
            }
            file.patch = redacted_patch;
        }

        files.sort_by(|left, right| left.path.as_bytes().cmp(right.path.as_bytes()));
        if files.windows(2).any(|pair| pair[0].path == pair[1].path) {
            return Err(ReviewDiffError::DuplicatePath);
        }

        let mut stream = String::new();
        let mut file_ranges = Vec::with_capacity(files.len());
        for file in &files {
            let start = stream.len();
            write!(
                &mut stream,
                "=== coding-agent-review-diff-file-v1 ===\npath: {}\nstatus: {}\n\
                 additions: {}\ndeletions: {}\npatch-bytes: {}\npatch:\n",
                file.path,
                status_name(file.status),
                file.additions,
                file.deletions,
                file.patch.len(),
            )
            .expect("writing a review diff frame to String cannot fail");
            let suffix_bytes = usize::from(!file.patch.ends_with('\n'))
                + "=== end-coding-agent-review-diff-file-v1 ===\n".len();
            if stream
                .len()
                .checked_add(file.patch.len())
                .and_then(|length| length.checked_add(suffix_bytes))
                .is_none_or(|length| length > MAX_REVIEW_DIFF_TYPED_STREAM_BYTES)
            {
                return Err(ReviewDiffError::TooManyChunks);
            }
            stream.push_str(&file.patch);
            if !file.patch.ends_with('\n') {
                stream.push('\n');
            }
            stream.push_str("=== end-coding-agent-review-diff-file-v1 ===\n");
            file_ranges.push((start, stream.len()));
        }
        // Individually stable fields can form a new regex match at framing
        // boundaries. The complete representation must therefore already be
        // stable before it is split into model-visible chunks.
        if !redaction_is_stable(redactor, &stream) {
            return Err(ReviewDiffError::UnsafeRedaction);
        }

        let chunks = split_chunks(&stream)?;
        let chunk_count = u8::try_from(chunks.len()).map_err(|_| ReviewDiffError::TooManyChunks)?;
        if chunk_count > MAX_REVIEW_DIFF_CHUNKS {
            return Err(ReviewDiffError::TooManyChunks);
        }

        let mut manifest_files = Vec::with_capacity(files.len());
        for (file, (start, end)) in files.into_iter().zip(file_ranges) {
            let first_chunk = chunks
                .iter()
                .position(|chunk| usize::try_from(chunk.stream_end).unwrap_or(usize::MAX) > start)
                .ok_or(ReviewDiffError::TooManyChunks)?;
            let last_chunk = chunks
                .iter()
                .rposition(|chunk| usize::try_from(chunk.stream_start).unwrap_or(usize::MAX) < end)
                .ok_or(ReviewDiffError::TooManyChunks)?;
            manifest_files.push(ReviewDiffManifestFile {
                path: file.path,
                status: file.status,
                additions: file.additions,
                deletions: file.deletions,
                patch_bytes: u64::try_from(file.patch.len())
                    .map_err(|_| ReviewDiffError::TooManyChunks)?,
                patch_sha256: sha256_hex(file.patch.as_bytes()),
                first_chunk: u8::try_from(first_chunk)
                    .map_err(|_| ReviewDiffError::TooManyChunks)?,
                chunk_count: u8::try_from(last_chunk - first_chunk + 1)
                    .map_err(|_| ReviewDiffError::TooManyChunks)?,
            });
        }

        let canonical_bytes = encode_manifest(
            checkpoint.generation,
            &checkpoint.workspace_digest,
            &manifest_files,
            chunk_count,
        )?;
        let canonical_manifest =
            std::str::from_utf8(&canonical_bytes).map_err(|_| ReviewDiffError::UnsafeRedaction)?;
        // JSON escaping and adjacent fields create another composition
        // boundary. Hash only a complete canonical payload that the same task
        // redactor proves is already stable.
        if !redaction_is_stable(redactor, canonical_manifest) {
            return Err(ReviewDiffError::UnsafeRedaction);
        }
        if canonical_bytes.len() > MAX_REVIEW_DIFF_TYPED_MANIFEST_BYTES {
            return Err(ReviewDiffError::ManifestTooLarge);
        }
        let mut hasher = Sha256::new();
        hasher.update(REVIEW_DIFF_MANIFEST_DOMAIN);
        hasher.update(&canonical_bytes);
        let digest = hasher.finalize();
        let manifest_sha256 = hex_digest(&digest);

        Ok(Self {
            manifest: ReviewDiffManifest {
                generation: checkpoint.generation,
                workspace_digest: checkpoint.workspace_digest.clone(),
                files: manifest_files,
                chunk_count,
                manifest_sha256,
                canonical_bytes,
            },
            chunks,
        })
    }

    pub const fn manifest(&self) -> &ReviewDiffManifest {
        &self.manifest
    }

    pub fn chunk_batch(
        &self,
        request: &ReviewDiffChunkRequest,
    ) -> Result<ReviewDiffChunkBatch, ReviewDiffError> {
        if request.generation != self.manifest.generation
            || request.workspace_digest != self.manifest.workspace_digest
            || request.manifest_sha256 != self.manifest.manifest_sha256
        {
            return Err(ReviewDiffError::ManifestMismatch);
        }
        let start = usize::from(request.start_chunk);
        let end = start
            .checked_add(usize::from(request.count))
            .filter(|end| *end <= self.chunks.len())
            .ok_or(ReviewDiffError::InvalidChunkRequest)?;
        if request.count == 0 || request.count > MAX_REVIEW_DIFF_BATCH_CHUNKS {
            return Err(ReviewDiffError::InvalidChunkRequest);
        }
        Ok(ReviewDiffChunkBatch {
            generation: self.manifest.generation,
            workspace_digest: self.manifest.workspace_digest.clone(),
            manifest_sha256: self.manifest.manifest_sha256.clone(),
            start_chunk: request.start_chunk,
            chunks: self.chunks[start..end].to_vec(),
        })
    }
}

/// Validates typed coverage against the exact manifest observed by the role.
///
/// This is a shape/identity gate. Task 11 additionally proves each listed
/// chunk result entered a subsequent provider request.
pub fn validate_review_coverage(
    manifest: &ReviewDiffManifest,
    coverage: Option<&ReviewCoverageEvidence>,
    require_complete: bool,
) -> Result<(), ReviewDiffError> {
    let Some(coverage) = coverage else {
        return if require_complete {
            Err(ReviewDiffError::IncompleteCoverage)
        } else {
            Ok(())
        };
    };
    if coverage.generation() != manifest.generation
        || coverage.workspace_digest() != &manifest.workspace_digest
        || coverage.manifest_sha256() != manifest.manifest_sha256
        || coverage.total_chunks() != manifest.chunk_count
    {
        return Err(ReviewDiffError::CoverageMismatch);
    }
    if require_complete && !coverage.is_complete() {
        return Err(ReviewDiffError::IncompleteCoverage);
    }
    Ok(())
}

/// Rebinds approved evidence to a freshly collected terminal manifest.
pub fn validate_terminal_review_coverage(
    coverage: &ReviewCoverageEvidence,
    terminal_manifest: &ReviewDiffManifest,
) -> Result<(), ReviewDiffError> {
    validate_review_coverage(terminal_manifest, Some(coverage), true)
}

fn split_chunks(stream: &str) -> Result<Vec<ReviewDiffChunk>, ReviewDiffError> {
    let mut chunks = Vec::new();
    let mut start = 0usize;
    while start < stream.len() {
        if chunks.len() >= usize::from(MAX_REVIEW_DIFF_CHUNKS) {
            return Err(ReviewDiffError::TooManyChunks);
        }
        let mut end = start
            .checked_add(MAX_REVIEW_DIFF_TYPED_CHUNK_BYTES)
            .unwrap_or(stream.len())
            .min(stream.len());
        while end > start && !stream.is_char_boundary(end) {
            end -= 1;
        }
        if end == start {
            return Err(ReviewDiffError::InvalidPatch);
        }
        let content = stream[start..end].to_owned();
        let index = u8::try_from(chunks.len()).map_err(|_| ReviewDiffError::TooManyChunks)?;
        chunks.push(ReviewDiffChunk {
            index,
            stream_start: u64::try_from(start).map_err(|_| ReviewDiffError::TooManyChunks)?,
            stream_end: u64::try_from(end).map_err(|_| ReviewDiffError::TooManyChunks)?,
            content,
        });
        start = end;
    }
    Ok(chunks)
}

#[derive(Serialize)]
struct CanonicalManifest<'a> {
    format_version: u8,
    generation: u64,
    digest: &'a WorkspaceDigest,
    files: Vec<CanonicalManifestFile<'a>>,
    chunk_count: u8,
}

#[derive(Serialize)]
struct CanonicalManifestFile<'a> {
    path: &'a str,
    status: &'static str,
    additions: u64,
    deletions: u64,
    patch_bytes: u64,
    patch_sha256: &'a str,
    first_chunk: u8,
    chunk_count: u8,
}

fn encode_manifest(
    generation: u64,
    digest: &WorkspaceDigest,
    files: &[ReviewDiffManifestFile],
    chunk_count: u8,
) -> Result<Vec<u8>, ReviewDiffError> {
    let files = files
        .iter()
        .map(|file| CanonicalManifestFile {
            path: &file.path,
            status: status_name(file.status),
            additions: file.additions,
            deletions: file.deletions,
            patch_bytes: file.patch_bytes,
            patch_sha256: &file.patch_sha256,
            first_chunk: file.first_chunk,
            chunk_count: file.chunk_count,
        })
        .collect();
    serde_json::to_vec(&CanonicalManifest {
        format_version: 1,
        generation,
        digest,
        files,
        chunk_count,
    })
    .map_err(|_| ReviewDiffError::ManifestTooLarge)
}

fn status_name(status: DiffFileStatus) -> &'static str {
    match status {
        DiffFileStatus::Added => "added",
        DiffFileStatus::Modified => "modified",
        DiffFileStatus::Deleted => "deleted",
    }
}

fn redaction_is_stable(redactor: &dyn ContextRedactor, value: &str) -> bool {
    let first = redactor.redact(value);
    first == value && redactor.redact(value) == first && redactor.redact(&first) == first
}

fn fingerprint_hex(fingerprint: WorkspaceFingerprint) -> String {
    hex_digest(fingerprint.as_bytes())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    hex_digest(&digest)
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut value = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        write!(&mut value, "{byte:02x}").expect("writing hexadecimal to String cannot fail");
    }
    value
}

fn is_valid_review_path(value: &str) -> bool {
    const MAX_PATH_BYTES: usize = 4_096;
    const MAX_COMPONENT_BYTES: usize = 255;
    if value.is_empty()
        || value.len() > MAX_PATH_BYTES
        || value.starts_with('/')
        || has_drive_prefix(value)
        || value.contains(['\\', '\0'])
        || value.chars().any(char::is_control)
    {
        return false;
    }
    value.split('/').all(|component| {
        if component.is_empty()
            || component == "."
            || component == ".."
            || component.len() > MAX_COMPONENT_BYTES
            || component.contains(':')
        {
            return false;
        }
        let windows_equivalent = component.trim_end_matches(['.', ' ']);
        windows_equivalent.len() == component.len()
            && !windows_equivalent.eq_ignore_ascii_case(".git")
            && !is_reserved_device_name(windows_equivalent)
    })
}

fn has_drive_prefix(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

fn is_reserved_device_name(component: &str) -> bool {
    let stem = component.split('.').next().unwrap_or(component);
    if ["CON", "PRN", "AUX", "NUL", "CLOCK$"]
        .iter()
        .any(|reserved| stem.eq_ignore_ascii_case(reserved))
    {
        return true;
    }
    let bytes = stem.as_bytes();
    bytes.len() == 4
        && (bytes[..3].eq_ignore_ascii_case(b"COM") || bytes[..3].eq_ignore_ascii_case(b"LPT"))
        && matches!(bytes[3], b'1'..=b'9')
}
