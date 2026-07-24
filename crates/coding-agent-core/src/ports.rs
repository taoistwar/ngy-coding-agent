use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use coding_agent_domain::{CheckEvidenceStatus, RequiredCheck, RequiredCheckSelector};

use crate::{
    AgentEvent, ModelRequest, ModelResponse, ProviderError, ReviewDiffCheckpoint,
    ReviewDiffChunkBatch, ReviewDiffChunkRequest, ReviewDiffManifest, RuntimeError,
    TerminalSnapshot, ToolRequest, ToolResult, ToolStatus, WorkspaceFingerprint,
};

/// Maximum UTF-8 bytes retained in the model-visible portion of one validation
/// observation. Authoritative status, duration, and selector fields are stored
/// separately and are never reconstructed from this text.
pub const MAX_VALIDATION_MODEL_RESULT_BYTES: usize = 8 * 1024;

/// A trusted Cargo selector catalog. Implementations discover selectors from
/// typed repository metadata rather than model-visible repository context.
#[async_trait::async_trait]
pub trait RepositoryCheckCatalog: Send + Sync + 'static {
    async fn required_check_selectors(
        &self,
        cancellation: CancellationToken,
    ) -> Result<Vec<RequiredCheckSelector>, RuntimeError>;
}

/// One authoritative result returned by [`ValidationRuntime`].
///
/// `model_result` is display/transcript data only. Quality gates must consume
/// `check`, `status`, and `duration_ms` directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationObservation {
    model_result: ToolResult,
    check: RequiredCheck,
    status: CheckEvidenceStatus,
    duration_ms: u64,
    truncated: bool,
}

impl ValidationObservation {
    pub fn try_new(
        model_result: ToolResult,
        check: RequiredCheck,
        status: CheckEvidenceStatus,
        duration_ms: u64,
        truncated: bool,
    ) -> Result<Self, RuntimeError> {
        let expected_tool_status = match status {
            CheckEvidenceStatus::Passed => ToolStatus::Succeeded,
            CheckEvidenceStatus::Failed | CheckEvidenceStatus::Cancelled => ToolStatus::Failed,
        };
        if duration_ms > 9_007_199_254_740_991
            || model_result.content().len() > MAX_VALIDATION_MODEL_RESULT_BYTES
            || model_result.status() != expected_tool_status
            || model_result.truncated() != truncated
        {
            return Err(RuntimeError::new(
                "INVALID_VALIDATION_OBSERVATION",
                "validation result fields are inconsistent or exceed their bound",
                false,
            ));
        }
        Ok(Self {
            model_result,
            check,
            status,
            duration_ms,
            truncated,
        })
    }

    pub const fn model_result(&self) -> &ToolResult {
        &self.model_result
    }

    pub const fn check(&self) -> &RequiredCheck {
        &self.check
    }

    pub const fn status(&self) -> CheckEvidenceStatus {
        self.status
    }

    pub const fn duration_ms(&self) -> u64 {
        self.duration_ms
    }

    pub const fn truncated(&self) -> bool {
        self.truncated
    }
}

/// Executes one canonical required check under runtime-owned command policy.
///
/// There is deliberately no caller-supplied timeout, command line, working
/// directory, manifest, target, or Cargo configuration in this interface.
/// The observation intentionally omits a fingerprint: orchestration must bind
/// it to its current checkpoint after the runtime's own pre/post stability
/// check, rather than trusting a second digest source.
#[async_trait::async_trait]
pub trait ValidationRuntime: Send + Sync + 'static {
    async fn run_validation(
        &self,
        check: RequiredCheck,
        cancellation: CancellationToken,
    ) -> Result<ValidationObservation, RuntimeError>;
}

/// Reviewer-only authoritative diff operations.
///
/// These operations are intentionally separate from ordinary `git_diff` and
/// `read_file` ToolResults. Only values returned through this typed port can
/// participate in review coverage evidence.
#[async_trait::async_trait]
pub trait ReviewDiffRuntime: Send + Sync + 'static {
    async fn review_diff_manifest(
        &self,
        checkpoint: ReviewDiffCheckpoint,
        redactor: Arc<dyn ContextRedactor>,
        cancellation: CancellationToken,
    ) -> Result<ReviewDiffManifest, RuntimeError>;

    async fn review_diff_chunks(
        &self,
        request: ReviewDiffChunkRequest,
        cancellation: CancellationToken,
    ) -> Result<ReviewDiffChunkBatch, RuntimeError>;

    /// Recollects the terminal diff without accepting a cached manifest.
    async fn terminal_review_diff_manifest(
        &self,
        checkpoint: ReviewDiffCheckpoint,
        redactor: Arc<dyn ContextRedactor>,
        cancellation: CancellationToken,
    ) -> Result<ReviewDiffManifest, RuntimeError>;
}

#[async_trait::async_trait]
pub trait ModelProvider: Send + Sync + 'static {
    async fn complete(
        &self,
        request: ModelRequest,
        cancellation: CancellationToken,
    ) -> Result<ModelResponse, ProviderError>;
}

/// A single-use encoded provider request whose protocol bytes remain inside
/// the provider implementation.
///
/// Core orchestration may inspect only the exact encoded length and the
/// provider-enforced response ceiling. This is enough to perform its budget
/// preflight before [`Self::send`] without exposing model context, opaque
/// reasoning, or provider credentials.
///
/// The object owns the exact task-scoped provider session that prepared it.
/// Consuming `Box<Self>` prevents replay and avoids any downcast or
/// cross-session identity decision in orchestration.
#[async_trait::async_trait]
pub trait PreparedProviderRequest: Send + 'static {
    fn encoded_len(&self) -> usize;

    fn maximum_response_bytes(&self) -> usize;

    async fn send(
        self: Box<Self>,
        cancellation: CancellationToken,
    ) -> Result<Box<dyn RawProviderResponse>, ProviderError>;
}

/// An encoded provider response whose raw body remains inside the provider
/// implementation.
///
/// The exact received length is available before decode so core can charge a
/// malformed, tool-choice-violating, or secret-containing response before the
/// provider rejects it. `decode` consumes the object so the body cannot be
/// decoded twice or retained outside the provider boundary.
pub trait RawProviderResponse: Send + 'static {
    fn encoded_len(&self) -> usize;

    fn decode(self: Box<Self>) -> Result<ModelResponse, ProviderError>;
}

/// A provider boundary that separates exact protocol encoding, transport, and
/// response interpretation.
///
/// The intended order is:
///
/// 1. `prepare`
/// 2. core budget preflight using [`PreparedProviderRequest`]
/// 3. [`PreparedProviderRequest::send`]
/// 4. core response charge using [`RawProviderResponse`]
/// 5. [`RawProviderResponse::decode`]
///
/// The returned objects are owned and single-use. This trait remains
/// object-safe so app factories and scripted providers can supply different
/// private transport types behind one `dyn PreparedModelProvider`.
pub trait PreparedModelProvider: Send + Sync + 'static {
    fn prepare(
        &self,
        request: ModelRequest,
    ) -> Result<Box<dyn PreparedProviderRequest>, ProviderError>;
}

#[async_trait::async_trait]
pub trait ToolRuntime: Send + Sync + 'static {
    async fn invoke(
        &self,
        request: ToolRequest,
        cancellation: CancellationToken,
    ) -> Result<ToolResult, RuntimeError>;
}

/// Runtime operations needed by the deterministic orchestration loop in
/// addition to individual model-visible tools.
#[async_trait::async_trait]
pub trait AgentRuntime: ToolRuntime {
    async fn workspace_fingerprint(
        &self,
        cancellation: CancellationToken,
    ) -> Result<WorkspaceFingerprint, RuntimeError>;

    async fn terminal_snapshot(
        &self,
        revision: u64,
        cancellation: CancellationToken,
    ) -> Result<TerminalSnapshot, RuntimeError>;
}

/// Receives only provider/runtime-neutral events suitable for app projection.
#[async_trait::async_trait]
pub trait AgentEventSink: Send + Sync + 'static {
    async fn emit(&self, event: AgentEvent) -> Result<(), RuntimeError>;
}

/// Removes configured and structural secrets before runtime output is sent
/// back to the provider as model context.
pub trait ContextRedactor: Send + Sync + 'static {
    fn redact(&self, content: &str) -> String;
}
