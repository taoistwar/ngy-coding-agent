//! Provider- and runtime-neutral coding-agent orchestration contracts.

mod agent_loop;
mod error;
mod event;
mod limits;
mod model;
mod ports;

pub use agent_loop::{
    AgentCancellation, AgentCompletion, AgentFailure, AgentInput, AgentLoop, AgentOutcome,
};
pub use error::{ProviderError, RuntimeError};
pub use event::{
    ActivityEvent, ActivityLevel, AgentEvent, DiffEvent, DiffFile, DiffFileStatus, PlanEvent,
    PlanItem, PlanItemStatus, TestCase, TestEvent, TestStatus,
};
pub use limits::{AgentLimits, AgentLimitsError};
pub use model::{
    ModelMessage, ModelRequest, ModelResponse, TerminalSnapshot, ToolCall, ToolRequest, ToolResult,
    ToolStatus, WorkspaceFingerprint,
};
pub use ports::{AgentEventSink, AgentRuntime, ContextRedactor, ModelProvider, ToolRuntime};
