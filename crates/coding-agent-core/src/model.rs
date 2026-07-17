use crate::DiffEvent;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelMessage {
    System(String),
    User(String),
    Assistant(String),
    AssistantToolCall(ToolCall),
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelResponse {
    ToolCall(ToolCall),
    Final { content: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCall {
    pub id: String,
    pub request: ToolRequest,
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
