use std::fmt;

use serde::Serialize;
use thiserror::Error;

use crate::{
    ContextRedactor, ModelMessage, ReviewDiffChunk, ReviewDiffChunkBatch, ReviewDiffManifest,
    ToolResult, ToolStatus,
};

/// Provider protocol bound for one opaque tool-call identifier.
pub const MAX_RETAINED_TOOL_CALL_ID_BYTES: usize = 256;
/// Wrapper-complete retained manifest bound from the Project 3 protocol.
pub const MAX_RETAINED_REVIEW_MANIFEST_BYTES: usize = 24 * 1024;
/// Wrapper-complete retained result bound for one authoritative diff chunk.
pub const MAX_RETAINED_REVIEW_CHUNK_BYTES: usize = 20 * 1024;
/// Wrapper-complete retained result bound for a batch of at most two chunks.
pub const MAX_RETAINED_REVIEW_CHUNK_BATCH_BYTES: usize = 40 * 1024;
/// Complete manifest plus four two-chunk batches.
pub const MAX_RETAINED_REVIEW_COVERAGE_BYTES: usize = 184 * 1024;

const TOOL_STATUS_PREFIX: &str = "[tool_status=";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum RetainedResultError {
    #[error("the retained tool-call identifier is invalid")]
    InvalidToolCallId,
    #[error("the retained tool result could not be encoded")]
    EncodingFailed,
    #[error("the retained tool-result wrapper is not redaction-stable")]
    RedactionUnstable,
    #[error("the retained tool-result wrapper cap is too small for a safe result")]
    WrapperLimitTooSmall,
    #[error("the retained review manifest exceeds its wrapper-complete bound")]
    ReviewManifestTooLarge,
    #[error("a retained review diff chunk exceeds its wrapper-complete bound")]
    ReviewChunkTooLarge,
    #[error("the retained review diff batch exceeds its wrapper-complete bound")]
    ReviewChunkBatchTooLarge,
    #[error("the retained review diff batch is empty or contains more than two chunks")]
    InvalidReviewChunkBatch,
}

/// The sole canonical representation of one retained model-visible tool result.
///
/// The counted bytes are the complete provider `role=tool` message wrapper,
/// including JSON escaping of both the opaque call ID and content. Callers can
/// construct this value only from semantic fields; the task budget never
/// accepts a caller-supplied byte slice as a substitute for the real wrapper.
#[derive(Clone, PartialEq, Eq)]
pub struct RetainedToolResult {
    tool_call_id: String,
    content: String,
    wrapper_bytes: Box<[u8]>,
    truncated: bool,
    status: ToolStatus,
}

impl RetainedToolResult {
    pub fn try_from_tool_result(
        tool_call_id: impl Into<String>,
        result: &ToolResult,
        redactor: &dyn ContextRedactor,
    ) -> Result<Self, RetainedResultError> {
        let redacted = redactor.redact(result.content());
        let redaction_changed = redacted != result.content();
        let content = contextual_content(
            result.status(),
            result.truncated() || redaction_changed,
            &redacted,
        );
        Self::try_from_context(
            tool_call_id.into(),
            content,
            result.status(),
            result.truncated() || redaction_changed,
            redactor,
        )
    }

    /// Encodes an ordinary/validation result under the exact dynamic cap
    /// reserved by whole-batch preflight. Truncation happens before the value
    /// is charged or appended to the transcript, and the returned wrapper is
    /// the one and only retained representation.
    pub fn try_from_tool_result_with_limit(
        tool_call_id: impl Into<String>,
        result: &ToolResult,
        redactor: &dyn ContextRedactor,
        wrapper_limit: usize,
    ) -> Result<Self, RetainedResultError> {
        let tool_call_id = tool_call_id.into();
        validate_tool_call_id(&tool_call_id)?;
        let redacted = redactor.redact(result.content());
        let redaction_changed = redacted != result.content();
        let full_truncated = result.truncated() || redaction_changed;
        let contextual = contextual_content(result.status(), full_truncated, &redacted);
        if encode_wrapper(&tool_call_id, &contextual)?.len() <= wrapper_limit {
            return Self::try_from_context(
                tool_call_id,
                contextual,
                result.status(),
                full_truncated,
                redactor,
            );
        }

        const OMITTED: &str = "\n...[tool result truncated]...";
        let boundaries = redacted
            .char_indices()
            .map(|(index, _)| index)
            .chain(std::iter::once(redacted.len()))
            .collect::<Vec<_>>();
        let mut low = 0usize;
        let mut high = boundaries.len();
        let mut best = None;
        while low < high {
            let middle = low + (high - low) / 2;
            let end = boundaries[middle];
            let candidate_body = format!("{}{OMITTED}", &redacted[..end]);
            let candidate_context = contextual_content(result.status(), true, &candidate_body);
            if encode_wrapper(&tool_call_id, &candidate_context)?.len() <= wrapper_limit {
                best = Some(candidate_context);
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        let contextual = best.ok_or(RetainedResultError::WrapperLimitTooSmall)?;
        Self::try_from_context(tool_call_id, contextual, result.status(), true, redactor)
    }

    /// Constructs an already typed result while still applying and verifying
    /// the task redactor across the complete composed wrapper.
    pub fn try_from_parts(
        tool_call_id: impl Into<String>,
        content: impl Into<String>,
        status: ToolStatus,
        truncated: bool,
        redactor: &dyn ContextRedactor,
    ) -> Result<Self, RetainedResultError> {
        let content = content.into();
        let redacted = redactor.redact(&content);
        let redaction_changed = redacted != content;
        let contextual = contextual_content(status, truncated || redaction_changed, &redacted);
        Self::try_from_context(
            tool_call_id.into(),
            contextual,
            status,
            truncated || redaction_changed,
            redactor,
        )
    }

    pub fn try_review_manifest(
        tool_call_id: impl Into<String>,
        manifest: &ReviewDiffManifest,
        redactor: &dyn ContextRedactor,
    ) -> Result<Self, RetainedResultError> {
        let canonical_manifest = std::str::from_utf8(manifest.canonical_bytes())
            .map_err(|_| RetainedResultError::EncodingFailed)?;
        let content = format!(
            "{{\"manifest\":{canonical_manifest},\"manifest_sha256\":\"{}\"}}",
            manifest.manifest_sha256()
        );
        let result = Self::try_from_complete_parts(
            tool_call_id.into(),
            content,
            ToolStatus::Succeeded,
            redactor,
        )?;
        if result.wrapper_len() > MAX_RETAINED_REVIEW_MANIFEST_BYTES {
            return Err(RetainedResultError::ReviewManifestTooLarge);
        }
        Ok(result)
    }

    pub fn try_review_chunk_batch(
        tool_call_id: impl Into<String>,
        batch: &ReviewDiffChunkBatch,
        redactor: &dyn ContextRedactor,
    ) -> Result<Self, RetainedResultError> {
        let tool_call_id = tool_call_id.into();
        if batch.chunks().is_empty() || batch.chunks().len() > 2 {
            return Err(RetainedResultError::InvalidReviewChunkBatch);
        }

        // Prove the per-chunk bound in the same complete wrapper and with the
        // same opaque ID that will be sent for the actual batch.
        for chunk in batch.chunks() {
            let single_content = encode_chunk_content(
                batch.generation(),
                batch.workspace_digest(),
                batch.manifest_sha256(),
                chunk.index(),
                std::slice::from_ref(chunk),
            )?;
            let single = Self::try_from_complete_parts(
                tool_call_id.clone(),
                single_content,
                ToolStatus::Succeeded,
                redactor,
            )?;
            if single.wrapper_len() > MAX_RETAINED_REVIEW_CHUNK_BYTES {
                return Err(RetainedResultError::ReviewChunkTooLarge);
            }
        }

        let content = encode_chunk_content(
            batch.generation(),
            batch.workspace_digest(),
            batch.manifest_sha256(),
            batch.start_chunk(),
            batch.chunks(),
        )?;
        let result =
            Self::try_from_complete_parts(tool_call_id, content, ToolStatus::Succeeded, redactor)?;
        if result.wrapper_len() > MAX_RETAINED_REVIEW_CHUNK_BATCH_BYTES {
            return Err(RetainedResultError::ReviewChunkBatchTooLarge);
        }
        Ok(result)
    }

    pub fn tool_call_id(&self) -> &str {
        &self.tool_call_id
    }

    pub fn content(&self) -> &str {
        &self.content
    }

    pub const fn status(&self) -> ToolStatus {
        self.status
    }

    pub const fn truncated(&self) -> bool {
        self.truncated
    }

    pub fn wrapper_bytes(&self) -> &[u8] {
        &self.wrapper_bytes
    }

    pub fn wrapper_len(&self) -> usize {
        self.wrapper_bytes.len()
    }

    pub fn into_model_message(self) -> ModelMessage {
        ModelMessage::tool_result(self.tool_call_id, self.content)
    }

    fn try_from_context(
        tool_call_id: String,
        content: String,
        status: ToolStatus,
        truncated: bool,
        redactor: &dyn ContextRedactor,
    ) -> Result<Self, RetainedResultError> {
        validate_tool_call_id(&tool_call_id)?;
        // Redacting fields independently is insufficient: field adjacency and
        // JSON punctuation can create a new configured/regex secret.
        if redactor.redact(&tool_call_id) != tool_call_id || redactor.redact(&content) != content {
            return Err(RetainedResultError::RedactionUnstable);
        }
        let wrapper_bytes = encode_wrapper(&tool_call_id, &content)?;
        let wrapper =
            std::str::from_utf8(&wrapper_bytes).map_err(|_| RetainedResultError::EncodingFailed)?;
        if redactor.redact(wrapper) != wrapper {
            return Err(RetainedResultError::RedactionUnstable);
        }
        Ok(Self {
            tool_call_id,
            content,
            wrapper_bytes: wrapper_bytes.into_boxed_slice(),
            truncated,
            status,
        })
    }

    fn try_from_complete_parts(
        tool_call_id: String,
        content: String,
        status: ToolStatus,
        redactor: &dyn ContextRedactor,
    ) -> Result<Self, RetainedResultError> {
        if redactor.redact(&content) != content {
            return Err(RetainedResultError::RedactionUnstable);
        }
        let contextual = contextual_content(status, false, &content);
        Self::try_from_context(tool_call_id, contextual, status, false, redactor)
    }
}

impl fmt::Debug for RetainedToolResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedToolResult")
            .field("tool_call_id", &self.tool_call_id)
            .field("wrapper_len", &self.wrapper_len())
            .field("truncated", &self.truncated)
            .field("status", &self.status)
            .field("content", &"<redacted>")
            .finish()
    }
}

fn encode_wrapper(tool_call_id: &str, content: &str) -> Result<Vec<u8>, RetainedResultError> {
    serde_json::to_vec(&canonical_tool_result_wire_value(tool_call_id, content)?)
        .map_err(|_| RetainedResultError::EncodingFailed)
}

/// Shared provider/core encoder for the exact `role=tool` wire object.
///
/// Returning the same `serde_json::Value` used by the provider prevents field
/// ordering or escaping drift between ledger measurement and request encoding.
pub fn canonical_tool_result_wire_value(
    tool_call_id: &str,
    content: &str,
) -> Result<serde_json::Value, RetainedResultError> {
    validate_tool_call_id(tool_call_id)?;
    Ok(serde_json::json!({
        "role": "tool",
        "tool_call_id": tool_call_id,
        "content": content,
    }))
}

fn validate_tool_call_id(tool_call_id: &str) -> Result<(), RetainedResultError> {
    if tool_call_id.is_empty()
        || tool_call_id.len() > MAX_RETAINED_TOOL_CALL_ID_BYTES
        || tool_call_id.chars().any(char::is_control)
    {
        return Err(RetainedResultError::InvalidToolCallId);
    }
    Ok(())
}

fn contextual_content(status: ToolStatus, truncated: bool, content: &str) -> String {
    format!(
        "{TOOL_STATUS_PREFIX}{}; truncated={truncated}]\n{content}",
        match status {
            ToolStatus::Succeeded => "succeeded",
            ToolStatus::Failed => "failed",
        }
    )
}

#[derive(Serialize)]
struct CanonicalChunkBatch<'a> {
    generation: u64,
    workspace_digest: &'a coding_agent_domain::WorkspaceDigest,
    manifest_sha256: &'a str,
    start_chunk: u8,
    chunks: Vec<CanonicalChunk<'a>>,
}

#[derive(Serialize)]
struct CanonicalChunk<'a> {
    index: u8,
    stream_start: u64,
    stream_end: u64,
    content: &'a str,
}

fn encode_chunk_content(
    generation: u64,
    workspace_digest: &coding_agent_domain::WorkspaceDigest,
    manifest_sha256: &str,
    start_chunk: u8,
    chunks: &[ReviewDiffChunk],
) -> Result<String, RetainedResultError> {
    let chunks = chunks
        .iter()
        .map(|chunk| CanonicalChunk {
            index: chunk.index(),
            stream_start: chunk.stream_start(),
            stream_end: chunk.stream_end(),
            content: chunk.content(),
        })
        .collect();
    let bytes = serde_json::to_vec(&CanonicalChunkBatch {
        generation,
        workspace_digest,
        manifest_sha256,
        start_chunk,
        chunks,
    })
    .map_err(|_| RetainedResultError::EncodingFailed)?;
    String::from_utf8(bytes).map_err(|_| RetainedResultError::EncodingFailed)
}
