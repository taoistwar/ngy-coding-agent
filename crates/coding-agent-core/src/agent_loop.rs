use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::{
    ActivityEvent, ActivityLevel, AgentEvent, AgentEventSink, AgentLimits, AgentRuntime,
    ContextRedactor, ModelMessage, ModelProvider, ModelRequest, ModelResponse, PlanEvent, PlanItem,
    PlanItemStatus, RuntimeError, TerminalSnapshot, TestCase, TestEvent, TestStatus, ToolCall,
    ToolRequest, ToolResult, ToolStatus, WorkspaceFingerprint,
};

const SYSTEM_POLICY: &str = "Use only the supplied typed tools. Return one tool call or a concise final answer. Never claim a test passed unless its tool result says so.";
const INVALID_TOOL_CALL: &str = "INVALID_TOOL_CALL";
const PROVIDER_SECRET_DETECTED: &str = "PROVIDER_SECRET_DETECTED";
const AGENT_STEP_LIMIT_REACHED: &str = "AGENT_STEP_LIMIT_REACHED";
const AGENT_CONTEXT_LIMIT_REACHED: &str = "AGENT_CONTEXT_LIMIT_REACHED";
const CURRENT_TEST_REQUIRED: &str = "CURRENT_TEST_REQUIRED";
const TERMINAL_DIFF_TRUNCATED: &str = "TERMINAL_DIFF_TRUNCATED";
const TERMINAL_FINALIZATION_TIMEOUT: &str = "TERMINAL_FINALIZATION_TIMEOUT";
const WORKSPACE_REVISION_EXHAUSTED: &str = "WORKSPACE_REVISION_EXHAUSTED";
const MAX_TOOL_CALL_ID_BYTES: usize = 256;
// Terminal work is best-effort after cancellation or failure, but it must not
// keep a task alive forever when a runtime or event sink stops responding. One
// deadline covers the terminal test event, snapshot, queued event, and diff.
const TERMINAL_FINALIZATION_BUDGET: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentInput {
    pub task_prompt: String,
    pub repository_context: String,
}

impl AgentInput {
    pub fn new(task_prompt: impl Into<String>, repository_context: impl Into<String>) -> Self {
        Self {
            task_prompt: task_prompt.into(),
            repository_context: repository_context.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentOutcome {
    Completed(AgentCompletion),
    Failed(AgentFailure),
    Cancelled(AgentCancellation),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentCompletion {
    pub final_text: String,
    pub workspace_revision: u64,
    pub terminal_snapshot: TerminalSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentFailure {
    pub code: String,
    pub retryable: bool,
    pub workspace_revision: u64,
    pub terminal_snapshot: Option<TerminalSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentCancellation {
    pub workspace_revision: u64,
    pub terminal_snapshot: Option<TerminalSnapshot>,
}

/// Deterministic single-role provider/tool orchestration.
pub struct AgentLoop {
    provider: Arc<dyn ModelProvider>,
    runtime: Arc<dyn AgentRuntime>,
    events: Arc<dyn AgentEventSink>,
    redactor: Arc<dyn ContextRedactor>,
    limits: AgentLimits,
}

impl AgentLoop {
    pub fn new(
        provider: Arc<dyn ModelProvider>,
        runtime: Arc<dyn AgentRuntime>,
        events: Arc<dyn AgentEventSink>,
        redactor: Arc<dyn ContextRedactor>,
        limits: AgentLimits,
    ) -> Self {
        Self {
            provider,
            runtime,
            events,
            redactor,
            limits,
        }
    }

    pub async fn run(&self, input: AgentInput, cancellation: CancellationToken) -> AgentOutcome {
        let mut state = WorkspaceState::default();
        if let Err(stop) = self
            .emit_live(initial_plan(), &cancellation)
            .await
            .and_then(|_| cancellation_stop(&cancellation).map_or(Ok(()), Err))
        {
            return self.finish(stop, &mut state, &cancellation).await;
        }

        let initial_fingerprint = match self.fingerprint_live(&cancellation).await {
            Ok(fingerprint) => fingerprint,
            Err(stop) => return self.finish(stop, &mut state, &cancellation).await,
        };
        state.fingerprint = Some(initial_fingerprint);

        let task_prompt = self.redactor.redact(&input.task_prompt);
        let repository_context = self.redactor.redact(&input.repository_context);
        let mut messages = vec![
            ModelMessage::system(SYSTEM_POLICY),
            ModelMessage::user(format!(
                "Task:\n{}\n\nRepository context:\n{}",
                task_prompt, repository_context
            )),
        ];
        let mut budget = Budget::default();
        let mut seen_call_ids = BTreeSet::new();

        for _ in 0..self.limits.max_model_steps() {
            if let Some(stop) = cancellation_stop(&cancellation) {
                return self.finish(stop, &mut state, &cancellation).await;
            }
            let Some(request_bytes) = model_request_bytes(&messages) else {
                return self
                    .finish(Stop::context_limit(), &mut state, &cancellation)
                    .await;
            };
            if !budget.consume_provider(request_bytes, self.limits.max_provider_bytes()) {
                return self
                    .finish(Stop::context_limit(), &mut state, &cancellation)
                    .await;
            }
            let response = match self
                .complete_live(
                    ModelRequest {
                        messages: messages.clone(),
                    },
                    &cancellation,
                )
                .await
            {
                Ok(response) => response,
                Err(stop) => return self.finish(stop, &mut state, &cancellation).await,
            };
            let Some(response_bytes) = model_response_bytes(&response) else {
                return self
                    .finish(Stop::context_limit(), &mut state, &cancellation)
                    .await;
            };
            if !budget.consume_provider(response_bytes, self.limits.max_provider_bytes()) {
                return self
                    .finish(Stop::context_limit(), &mut state, &cancellation)
                    .await;
            }

            let call = match response {
                ModelResponse::Final { content } => {
                    return self
                        .finish(
                            Stop::Final(self.redactor.redact(&content)),
                            &mut state,
                            &cancellation,
                        )
                        .await;
                }
                ModelResponse::ToolCall(call) => call,
            };
            if !tool_call_is_redaction_stable(&call, self.redactor.as_ref()) {
                return self
                    .finish(
                        Stop::failed(PROVIDER_SECRET_DETECTED, false),
                        &mut state,
                        &cancellation,
                    )
                    .await;
            }
            if !valid_tool_call(&call, &seen_call_ids) {
                return self
                    .finish(
                        Stop::failed(INVALID_TOOL_CALL, false),
                        &mut state,
                        &cancellation,
                    )
                    .await;
            }
            seen_call_ids.insert(call.id.clone());
            if budget.tool_calls >= self.limits.max_tool_calls() {
                return self
                    .finish(Stop::step_limit(), &mut state, &cancellation)
                    .await;
            }
            budget.tool_calls += 1;

            if let Err(stop) = self
                .emit_live(
                    AgentEvent::Activity(ActivityEvent {
                        level: ActivityLevel::Info,
                        message: format!("tool {} started", tool_name(&call.request)),
                    }),
                    &cancellation,
                )
                .await
            {
                return self.finish(stop, &mut state, &cancellation).await;
            }

            let validation_start = if is_cargo_validation(&call.request) {
                let fingerprint = match self.fingerprint_live(&cancellation).await {
                    Ok(fingerprint) => fingerprint,
                    Err(stop) => return self.finish(stop, &mut state, &cancellation).await,
                };
                match state.observe(fingerprint) {
                    Ok(true) => {
                        if let Err(stop) = self.emit_queued(&state, &cancellation).await {
                            return self.finish(stop, &mut state, &cancellation).await;
                        }
                    }
                    Ok(false) => {}
                    Err(stop) => return self.finish(stop, &mut state, &cancellation).await,
                }
                if let Err(stop) = self
                    .emit_live(
                        validation_event(&call.request, state.revision, TestStatus::Running),
                        &cancellation,
                    )
                    .await
                {
                    return self.finish(stop, &mut state, &cancellation).await;
                }
                Some(fingerprint)
            } else {
                None
            };

            let result = match self.invoke_live(call.request.clone(), &cancellation).await {
                Ok(result) => result,
                Err(stop) => {
                    let terminal_test_event = validation_start.is_some().then(|| {
                        let status = if matches!(&stop, Stop::Cancelled) {
                            TestStatus::Cancelled
                        } else {
                            TestStatus::Failed
                        };
                        validation_event(&call.request, state.revision, status)
                    });
                    return self
                        .finish_with_event(stop, &mut state, &cancellation, terminal_test_event)
                        .await;
                }
            };

            if matches!(call.request, ToolRequest::ReplaceFile { .. })
                && result.status() == ToolStatus::Succeeded
            {
                if let Err(stop) = state.replaced() {
                    return self.finish(stop, &mut state, &cancellation).await;
                }
                let mut snapshot = match self.snapshot_live(state.revision, &cancellation).await {
                    Ok(snapshot) => snapshot,
                    Err(stop) => return self.finish(stop, &mut state, &cancellation).await,
                };
                state.record_replacement_snapshot(snapshot.fingerprint);
                snapshot.diff.revision = state.revision;
                if let Err(stop) = self
                    .emit_live(AgentEvent::Diff(snapshot.diff), &cancellation)
                    .await
                {
                    return self.finish(stop, &mut state, &cancellation).await;
                }
                if let Err(stop) = self.emit_queued(&state, &cancellation).await {
                    return self.finish(stop, &mut state, &cancellation).await;
                }
                if let Err(stop) = self.emit_live(modification_plan(2), &cancellation).await {
                    return self.finish(stop, &mut state, &cancellation).await;
                }
            }

            if let Some(start) = validation_start {
                let end = match self.fingerprint_live(&cancellation).await {
                    Ok(fingerprint) => fingerprint,
                    Err(stop) => return self.finish(stop, &mut state, &cancellation).await,
                };
                if start != end {
                    match state.observe(end) {
                        Ok(_) => {}
                        Err(stop) => return self.finish(stop, &mut state, &cancellation).await,
                    }
                    if let Err(stop) = self.emit_queued(&state, &cancellation).await {
                        return self.finish(stop, &mut state, &cancellation).await;
                    }
                } else {
                    let test_status = match result.status() {
                        ToolStatus::Succeeded => TestStatus::Passed,
                        ToolStatus::Failed => TestStatus::Failed,
                    };
                    if let Err(stop) = self
                        .emit_live(
                            validation_event(&call.request, state.revision, test_status),
                            &cancellation,
                        )
                        .await
                    {
                        return self.finish(stop, &mut state, &cancellation).await;
                    }
                    if matches!(call.request, ToolRequest::CargoTest { .. })
                        && result.status() == ToolStatus::Succeeded
                    {
                        state.passed = Some(TestEvidence {
                            revision: state.revision,
                            fingerprint: end,
                        });
                        if let Err(stop) = self.emit_live(validation_plan(3), &cancellation).await {
                            return self.finish(stop, &mut state, &cancellation).await;
                        }
                    }
                }
            }

            let redacted_content = self.redactor.redact(result.content());
            let contextual_result = tool_result_context(
                &result,
                &redacted_content,
                redacted_content != result.content(),
            );
            if !budget
                .consume_tool_result(contextual_result.len(), self.limits.max_tool_result_bytes())
            {
                return self
                    .finish(Stop::context_limit(), &mut state, &cancellation)
                    .await;
            }
            if let Err(stop) = self
                .emit_live(
                    AgentEvent::Activity(ActivityEvent {
                        level: match result.status() {
                            ToolStatus::Succeeded => ActivityLevel::Info,
                            ToolStatus::Failed => ActivityLevel::Warning,
                        },
                        message: format!(
                            "tool {} {}",
                            tool_name(&call.request),
                            match result.status() {
                                ToolStatus::Succeeded => "succeeded",
                                ToolStatus::Failed => "failed",
                            }
                        ),
                    }),
                    &cancellation,
                )
                .await
            {
                return self.finish(stop, &mut state, &cancellation).await;
            }
            messages.push(ModelMessage::AssistantToolCall(call.clone()));
            messages.push(ModelMessage::tool_result(call.id, contextual_result));
        }

        self.finish(Stop::step_limit(), &mut state, &cancellation)
            .await
    }

    async fn complete_live(
        &self,
        request: ModelRequest,
        cancellation: &CancellationToken,
    ) -> Result<ModelResponse, Stop> {
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(Stop::Cancelled),
            result = self.provider.complete(request, cancellation.clone()) => result,
        };
        if cancellation.is_cancelled() {
            return Err(Stop::Cancelled);
        }
        result.map_err(|error| Stop::failed(error.code, error.retryable))
    }

    async fn invoke_live(
        &self,
        request: ToolRequest,
        cancellation: &CancellationToken,
    ) -> Result<ToolResult, Stop> {
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(Stop::Cancelled),
            result = self.runtime.invoke(request, cancellation.clone()) => result,
        };
        if cancellation.is_cancelled() {
            return Err(Stop::Cancelled);
        }
        result.map_err(stop_from_runtime)
    }

    async fn fingerprint_live(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<WorkspaceFingerprint, Stop> {
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(Stop::Cancelled),
            result = self.runtime.workspace_fingerprint(cancellation.clone()) => result,
        };
        if cancellation.is_cancelled() {
            return Err(Stop::Cancelled);
        }
        result.map_err(stop_from_runtime)
    }

    async fn snapshot_live(
        &self,
        revision: u64,
        cancellation: &CancellationToken,
    ) -> Result<TerminalSnapshot, Stop> {
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(Stop::Cancelled),
            result = self.runtime.terminal_snapshot(revision, cancellation.clone()) => result,
        };
        if cancellation.is_cancelled() {
            return Err(Stop::Cancelled);
        }
        result.map_err(stop_from_runtime)
    }

    async fn emit_live(
        &self,
        event: AgentEvent,
        cancellation: &CancellationToken,
    ) -> Result<(), Stop> {
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(Stop::Cancelled),
            result = self.events.emit(event) => result,
        };
        if cancellation.is_cancelled() {
            return Err(Stop::Cancelled);
        }
        result.map_err(stop_from_runtime)
    }

    async fn emit_queued(
        &self,
        state: &WorkspaceState,
        cancellation: &CancellationToken,
    ) -> Result<(), Stop> {
        self.emit_live(queued_event(state.revision), cancellation)
            .await
    }

    async fn finish(
        &self,
        stop: Stop,
        state: &mut WorkspaceState,
        cancellation: &CancellationToken,
    ) -> AgentOutcome {
        self.finish_with_event(stop, state, cancellation, None)
            .await
    }

    async fn finish_with_event(
        &self,
        mut stop: Stop,
        state: &mut WorkspaceState,
        cancellation: &CancellationToken,
        terminal_event: Option<AgentEvent>,
    ) -> AgentOutcome {
        if cancellation.is_cancelled() {
            stop = Stop::Cancelled;
        }
        let terminal_cancellation = CancellationToken::new();
        let terminal_result = tokio::time::timeout(TERMINAL_FINALIZATION_BUDGET, async {
            if let Some(event) = terminal_event
                && let Err(error) = self.events.emit(event).await
                && !matches!(stop, Stop::Cancelled)
            {
                stop = stop_from_runtime(error);
            }

            let terminal_result = self
                .runtime
                .terminal_snapshot(state.revision, terminal_cancellation.clone())
                .await;
            if cancellation.is_cancelled() {
                stop = Stop::Cancelled;
            }

            match terminal_result {
                Ok(mut snapshot) => {
                    match state.observe(snapshot.fingerprint) {
                        Ok(true) => {
                            snapshot.diff.revision = state.revision;
                            if let Err(error) = self.events.emit(queued_event(state.revision)).await
                                && matches!(stop, Stop::Final(_))
                            {
                                stop = stop_from_runtime(error);
                            }
                        }
                        Ok(false) => snapshot.diff.revision = state.revision,
                        Err(revision_error) => {
                            if matches!(stop, Stop::Final(_)) {
                                stop = revision_error;
                            }
                        }
                    }
                    if let Err(error) = self
                        .events
                        .emit(AgentEvent::Diff(snapshot.diff.clone()))
                        .await
                        && matches!(stop, Stop::Final(_))
                    {
                        stop = stop_from_runtime(error);
                    }
                    Some(snapshot)
                }
                Err(error) => {
                    if matches!(stop, Stop::Final(_)) {
                        stop = stop_from_runtime(error);
                    }
                    None
                }
            }
        })
        .await;
        terminal_cancellation.cancel();

        let mut snapshot = match terminal_result {
            Ok(snapshot) => snapshot,
            Err(_) => {
                if matches!(stop, Stop::Final(_)) {
                    stop = Stop::failed(TERMINAL_FINALIZATION_TIMEOUT, true);
                }
                None
            }
        };
        if cancellation.is_cancelled() {
            stop = Stop::Cancelled;
        }

        match stop {
            Stop::Final(final_text) => {
                let Some(terminal_snapshot) = snapshot.take() else {
                    return AgentOutcome::Failed(AgentFailure {
                        code: "TERMINAL_SNAPSHOT_FAILED".to_owned(),
                        retryable: true,
                        workspace_revision: state.revision,
                        terminal_snapshot: None,
                    });
                };
                if state.passed.as_ref().is_some_and(|passed| {
                    passed.revision == state.revision
                        && passed.fingerprint == terminal_snapshot.fingerprint
                }) {
                    if terminal_snapshot
                        .diff
                        .files
                        .iter()
                        .any(|file| file.truncated)
                    {
                        return AgentOutcome::Failed(AgentFailure {
                            code: TERMINAL_DIFF_TRUNCATED.to_owned(),
                            retryable: false,
                            workspace_revision: state.revision,
                            terminal_snapshot: Some(terminal_snapshot),
                        });
                    }
                    AgentOutcome::Completed(AgentCompletion {
                        final_text,
                        workspace_revision: state.revision,
                        terminal_snapshot,
                    })
                } else {
                    AgentOutcome::Failed(AgentFailure {
                        code: CURRENT_TEST_REQUIRED.to_owned(),
                        retryable: false,
                        workspace_revision: state.revision,
                        terminal_snapshot: Some(terminal_snapshot),
                    })
                }
            }
            Stop::Failed { code, retryable } => AgentOutcome::Failed(AgentFailure {
                code,
                retryable,
                workspace_revision: state.revision,
                terminal_snapshot: snapshot,
            }),
            Stop::Cancelled => AgentOutcome::Cancelled(AgentCancellation {
                workspace_revision: state.revision,
                terminal_snapshot: snapshot,
            }),
        }
    }
}

#[derive(Default)]
struct WorkspaceState {
    revision: u64,
    fingerprint: Option<WorkspaceFingerprint>,
    passed: Option<TestEvidence>,
}

impl WorkspaceState {
    fn observe(&mut self, fingerprint: WorkspaceFingerprint) -> Result<bool, Stop> {
        if self
            .fingerprint
            .is_some_and(|current| current != fingerprint)
        {
            self.increment()?;
            self.fingerprint = Some(fingerprint);
            self.passed = None;
            Ok(true)
        } else {
            self.fingerprint = Some(fingerprint);
            Ok(false)
        }
    }

    fn replaced(&mut self) -> Result<(), Stop> {
        self.increment()?;
        self.fingerprint = None;
        self.passed = None;
        Ok(())
    }

    fn record_replacement_snapshot(&mut self, fingerprint: WorkspaceFingerprint) {
        self.fingerprint = Some(fingerprint);
    }

    fn increment(&mut self) -> Result<(), Stop> {
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or_else(|| Stop::failed(WORKSPACE_REVISION_EXHAUSTED, false))?;
        Ok(())
    }
}

struct TestEvidence {
    revision: u64,
    fingerprint: WorkspaceFingerprint,
}

#[derive(Default)]
struct Budget {
    provider_bytes: usize,
    tool_result_bytes: usize,
    tool_calls: u32,
}

impl Budget {
    fn consume_provider(&mut self, bytes: usize, maximum: usize) -> bool {
        consume(&mut self.provider_bytes, bytes, maximum)
    }

    fn consume_tool_result(&mut self, bytes: usize, maximum: usize) -> bool {
        consume(&mut self.tool_result_bytes, bytes, maximum)
    }
}

fn consume(observed: &mut usize, bytes: usize, maximum: usize) -> bool {
    let Some(next) = observed.checked_add(bytes) else {
        return false;
    };
    if next > maximum {
        return false;
    }
    *observed = next;
    true
}

enum Stop {
    Final(String),
    Failed { code: String, retryable: bool },
    Cancelled,
}

impl Stop {
    fn failed(code: impl Into<String>, retryable: bool) -> Self {
        Self::Failed {
            code: code.into(),
            retryable,
        }
    }

    fn step_limit() -> Self {
        Self::failed(AGENT_STEP_LIMIT_REACHED, true)
    }

    fn context_limit() -> Self {
        Self::failed(AGENT_CONTEXT_LIMIT_REACHED, true)
    }
}

fn stop_from_runtime(error: RuntimeError) -> Stop {
    if error.code == "COMMAND_CANCELLED" {
        Stop::Cancelled
    } else {
        Stop::failed(error.code, error.retryable)
    }
}

fn cancellation_stop(cancellation: &CancellationToken) -> Option<Stop> {
    cancellation.is_cancelled().then_some(Stop::Cancelled)
}

fn valid_tool_call(call: &ToolCall, seen: &BTreeSet<String>) -> bool {
    !call.id.is_empty()
        && call.id.len() <= MAX_TOOL_CALL_ID_BYTES
        && !call.id.chars().any(char::is_control)
        && !seen.contains(&call.id)
        && valid_tool_request(&call.request)
}

fn valid_tool_request(request: &ToolRequest) -> bool {
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

fn tool_call_is_redaction_stable(call: &ToolCall, redactor: &dyn ContextRedactor) -> bool {
    redactor.redact(&call.id) == call.id
        && match &call.request {
            ToolRequest::ListFiles { path, .. } | ToolRequest::ReadFile { path, .. } => {
                redactor.redact(path) == *path
            }
            ToolRequest::SearchText {
                query, path, glob, ..
            } => {
                redactor.redact(query) == *query
                    && redactor.redact(path) == *path
                    && glob
                        .as_ref()
                        .is_none_or(|value| redactor.redact(value) == *value)
            }
            ToolRequest::ReplaceFile {
                path,
                expected_sha256,
                content,
            } => {
                redactor.redact(path) == *path
                    && redactor.redact(content) == *content
                    && expected_sha256
                        .as_ref()
                        .is_none_or(|value| redactor.redact(value) == *value)
            }
            ToolRequest::CargoCheck { package, .. } => package
                .as_ref()
                .is_none_or(|value| redactor.redact(value) == *value),
            ToolRequest::CargoTest { package, test, .. } => {
                package
                    .as_ref()
                    .is_none_or(|value| redactor.redact(value) == *value)
                    && test
                        .as_ref()
                        .is_none_or(|value| redactor.redact(value) == *value)
            }
            ToolRequest::GitStatus | ToolRequest::GitDiff => true,
        }
}

fn is_cargo_validation(request: &ToolRequest) -> bool {
    matches!(
        request,
        ToolRequest::CargoCheck { .. } | ToolRequest::CargoTest { .. }
    )
}

fn tool_name(request: &ToolRequest) -> &'static str {
    match request {
        ToolRequest::ListFiles { .. } => "list_files",
        ToolRequest::ReadFile { .. } => "read_file",
        ToolRequest::SearchText { .. } => "search_text",
        ToolRequest::ReplaceFile { .. } => "replace_file",
        ToolRequest::CargoCheck { .. } => "cargo_check",
        ToolRequest::CargoTest { .. } => "cargo_test",
        ToolRequest::GitStatus => "git_status",
        ToolRequest::GitDiff => "git_diff",
    }
}

fn model_request_bytes(messages: &[ModelMessage]) -> Option<usize> {
    messages.iter().try_fold(0usize, |total, message| {
        total.checked_add(model_message_bytes(message)?)
    })
}

fn model_message_bytes(message: &ModelMessage) -> Option<usize> {
    match message {
        ModelMessage::System(content)
        | ModelMessage::User(content)
        | ModelMessage::Assistant(content) => 1usize.checked_add(content.len()),
        ModelMessage::AssistantToolCall(call) => 1usize
            .checked_add(call.id.len())?
            .checked_add(tool_request_bytes(&call.request)?),
        ModelMessage::ToolResult {
            tool_call_id,
            content,
        } => 1usize
            .checked_add(tool_call_id.len())?
            .checked_add(content.len()),
    }
}

fn model_response_bytes(response: &ModelResponse) -> Option<usize> {
    match response {
        ModelResponse::Final { content } => 1usize.checked_add(content.len()),
        ModelResponse::ToolCall(call) => 1usize
            .checked_add(call.id.len())?
            .checked_add(tool_request_bytes(&call.request)?),
    }
}

fn tool_request_bytes(request: &ToolRequest) -> Option<usize> {
    let strings = match request {
        ToolRequest::ListFiles { path, .. } | ToolRequest::ReadFile { path, .. } => path.len(),
        ToolRequest::SearchText {
            query, path, glob, ..
        } => query
            .len()
            .checked_add(path.len())?
            .checked_add(glob.as_ref().map_or(0, String::len))?,
        ToolRequest::ReplaceFile {
            path,
            expected_sha256,
            content,
        } => path
            .len()
            .checked_add(expected_sha256.as_ref().map_or(0, String::len))?
            .checked_add(content.len())?,
        ToolRequest::CargoCheck { package, .. } => package.as_ref().map_or(0, String::len),
        ToolRequest::CargoTest { package, test, .. } => package
            .as_ref()
            .map_or(0, String::len)
            .checked_add(test.as_ref().map_or(0, String::len))?,
        ToolRequest::GitStatus | ToolRequest::GitDiff => 0,
    };
    16usize.checked_add(strings)
}

fn tool_result_context(result: &ToolResult, content: &str, redacted: bool) -> String {
    format!(
        "[tool_status={}; truncated={}]\n{}",
        match result.status() {
            ToolStatus::Succeeded => "succeeded",
            ToolStatus::Failed => "failed",
        },
        result.truncated() || redacted,
        content
    )
}

fn initial_plan() -> AgentEvent {
    plan_event(
        1,
        PlanItemStatus::Running,
        PlanItemStatus::Pending,
        PlanItemStatus::Pending,
    )
}

fn modification_plan(revision: u64) -> AgentEvent {
    plan_event(
        revision,
        PlanItemStatus::Completed,
        PlanItemStatus::Running,
        PlanItemStatus::Pending,
    )
}

fn validation_plan(revision: u64) -> AgentEvent {
    plan_event(
        revision,
        PlanItemStatus::Completed,
        PlanItemStatus::Completed,
        PlanItemStatus::Completed,
    )
}

fn plan_event(
    revision: u64,
    inspect: PlanItemStatus,
    modify: PlanItemStatus,
    validate: PlanItemStatus,
) -> AgentEvent {
    AgentEvent::Plan(PlanEvent {
        revision,
        items: vec![
            PlanItem {
                id: "inspect".to_owned(),
                title: "Inspect repository".to_owned(),
                status: inspect,
            },
            PlanItem {
                id: "modify".to_owned(),
                title: "Modify code".to_owned(),
                status: modify,
            },
            PlanItem {
                id: "validate".to_owned(),
                title: "Run validation".to_owned(),
                status: validate,
            },
        ],
    })
}

fn queued_event(revision: u64) -> AgentEvent {
    test_event("cargo-test", "cargo test", revision, TestStatus::Queued)
}

fn validation_event(request: &ToolRequest, revision: u64, status: TestStatus) -> AgentEvent {
    match request {
        ToolRequest::CargoCheck { .. } => {
            test_event("cargo-check", "cargo check", revision, status)
        }
        ToolRequest::CargoTest { .. } => test_event("cargo-test", "cargo test", revision, status),
        _ => unreachable!("only Cargo validation requests emit test events"),
    }
}

fn test_event(id: &str, name: &str, revision: u64, status: TestStatus) -> AgentEvent {
    AgentEvent::Tests(TestEvent {
        revision,
        status,
        cases: vec![TestCase {
            id: id.to_owned(),
            name: name.to_owned(),
            status,
            duration_ms: None,
            summary: match status {
                TestStatus::Queued => "workspace changed; validation required",
                TestStatus::Running => "validation running",
                TestStatus::Passed => "validation passed",
                TestStatus::Failed => "validation failed",
                TestStatus::Cancelled => "validation cancelled",
            }
            .to_owned(),
        }],
    })
}
