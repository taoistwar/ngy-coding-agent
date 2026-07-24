use crate::{ActionRequest, AllowedActions, DiffEvent, RequiredAction, RuntimeActionRequest};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelMessage {
    System(String),
    User(String),
    Assistant(String),
    AssistantToolCalls(ToolCallBatch),
    ToolResult {
        tool_call_id: String,
        content: String,
    },
}

impl ModelMessage {
    pub fn system(content: impl Into<String>) -> Self {
        Self::System(content.into())
    }

    pub fn user(content: impl Into<String>) -> Self {
        Self::User(content.into())
    }

    pub fn assistant(content: impl Into<String>) -> Self {
        Self::Assistant(content.into())
    }

    pub fn tool_result(tool_call_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self::ToolResult {
            tool_call_id: tool_call_id.into(),
            content: content.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelRequest {
    pub messages: Vec<ModelMessage>,
    pub allowed_actions: AllowedActions,
    pub tool_choice: ModelToolChoice,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelToolChoice {
    Auto,
    None,
    Required(RequiredAction),
    /// Transitional Project 2 convergence choice.
    RequiredCargoTest,
}

impl ModelToolChoice {
    pub fn permits(&self, response: &ModelResponse) -> bool {
        match self {
            Self::Auto => true,
            Self::None => matches!(response, ModelResponse::Final { .. }),
            Self::Required(required) => matches!(
                response,
                ModelResponse::ToolCalls(ToolCallBatch { calls, .. })
                    if matches!(calls.as_slice(), [call] if required.matches(&call.request))
            ),
            Self::RequiredCargoTest => matches!(
                response,
                ModelResponse::ToolCalls(ToolCallBatch { calls, .. })
                    if matches!(
                        calls.as_slice(),
                        [ToolCall {
                            request: ActionRequest::Runtime(
                                RuntimeActionRequest::Tool(ToolRequest::CargoTest { .. })
                            ),
                            ..
                        }]
                    )
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelResponse {
    ToolCalls(ToolCallBatch),
    Final { content: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCallBatch {
    /// Optional assistant text that accompanied the calls and must remain in provider history.
    pub assistant_content: Option<String>,
    /// Opaque provider reasoning state required to continue a thinking-mode tool-call turn.
    /// It is never surfaced as user-visible assistant content.
    pub reasoning_content: Option<String>,
    /// Tool calls in the exact order returned by the provider.
    pub calls: Vec<ToolCall>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCall {
    pub id: String,
    pub request: ActionRequest,
}

impl ToolCall {
    pub fn runtime(id: impl Into<String>, request: ToolRequest) -> Self {
        Self {
            id: id.into(),
            request: ActionRequest::runtime(request),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolRequest {
    ListFiles {
        path: String,
        depth: u32,
        limit: usize,
    },
    ReadFile {
        path: String,
        start_line: u64,
        end_line: u64,
    },
    SearchText {
        query: String,
        path: String,
        glob: Option<String>,
        limit: usize,
    },
    ReplaceFile {
        path: String,
        expected_sha256: Option<String>,
        content: String,
    },
    CargoCheck {
        package: Option<String>,
        timeout_ms: u64,
    },
    CargoTest {
        package: Option<String>,
        test: Option<String>,
        timeout_ms: u64,
    },
    GitStatus,
    GitDiff,
}

/// Shared semantic validation for every publicly constructible Project 2 tool
/// request. Provider schemas are only a wire aid; both the legacy loop and the
/// Project 3 role boundary must re-check these invariants locally before any
/// side effect can run.
pub(crate) fn is_valid_tool_request(request: &ToolRequest) -> bool {
    match request {
        ToolRequest::ListFiles { depth, limit, .. } => *depth != 0 && *limit != 0,
        ToolRequest::ReadFile {
            path,
            start_line,
            end_line,
        } => !path.is_empty() && *start_line != 0 && end_line >= start_line,
        ToolRequest::SearchText { query, limit, .. } => !query.is_empty() && *limit != 0,
        ToolRequest::ReplaceFile {
            path,
            expected_sha256,
            ..
        } => {
            !path.is_empty()
                && expected_sha256.as_ref().is_none_or(|digest| {
                    digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
                })
        }
        ToolRequest::CargoCheck {
            package,
            timeout_ms,
        } => *timeout_ms != 0 && package.as_ref().is_none_or(|value| !value.is_empty()),
        ToolRequest::CargoTest {
            package,
            test,
            timeout_ms,
        } => {
            *timeout_ms != 0
                && package.as_ref().is_none_or(|value| !value.is_empty())
                && test.as_ref().is_none_or(|value| !value.is_empty())
        }
        ToolRequest::GitStatus | ToolRequest::GitDiff => true,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolResult {
    content: String,
    truncated: bool,
    status: ToolStatus,
}

impl ToolResult {
    pub fn text(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            truncated: false,
            status: ToolStatus::Succeeded,
        }
    }

    pub fn truncated_text(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            truncated: true,
            status: ToolStatus::Succeeded,
        }
    }

    pub fn failed_text(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            truncated: false,
            status: ToolStatus::Failed,
        }
    }

    pub fn truncated_failed_text(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            truncated: true,
            status: ToolStatus::Failed,
        }
    }

    pub fn content(&self) -> &str {
        &self.content
    }

    pub const fn truncated(&self) -> bool {
        self.truncated
    }

    pub const fn status(&self) -> ToolStatus {
        self.status
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolStatus {
    Succeeded,
    Failed,
}

/// A deterministic digest over the complete deliverable workspace set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WorkspaceFingerprint([u8; 32]);

impl WorkspaceFingerprint {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// A terminal diff and the fingerprint observed for the same stable snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalSnapshot {
    pub fingerprint: WorkspaceFingerprint,
    pub diff: DiffEvent,
}
