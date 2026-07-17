use tokio_util::sync::CancellationToken;

use crate::{
    AgentEvent, ModelRequest, ModelResponse, ProviderError, RuntimeError, TerminalSnapshot,
    ToolRequest, ToolResult, WorkspaceFingerprint,
};

#[async_trait::async_trait]
pub trait ModelProvider: Send + Sync + 'static {
    async fn complete(
        &self,
        request: ModelRequest,
        cancellation: CancellationToken,
    ) -> Result<ModelResponse, ProviderError>;
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
