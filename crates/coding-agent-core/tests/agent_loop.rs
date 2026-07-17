use std::collections::VecDeque;
use std::future::pending;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use coding_agent_core::{
    AgentEvent, AgentEventSink, AgentInput, AgentLimits, AgentLoop, AgentOutcome, AgentRuntime,
    ContextRedactor, DiffEvent, DiffFile, DiffFileStatus, ModelMessage, ModelProvider,
    ModelRequest, ModelResponse, ProviderError, RuntimeError, TerminalSnapshot, TestStatus,
    ToolCall, ToolRequest, ToolResult, ToolRuntime, WorkspaceFingerprint,
};
use tokio_util::sync::CancellationToken;

enum ProviderStep {
    Response(ModelResponse),
    Error(ProviderError),
    CancelThenError(ProviderError),
}

struct ScriptedProvider {
    steps: Mutex<VecDeque<ProviderStep>>,
    requests: Mutex<Vec<ModelRequest>>,
}

impl ScriptedProvider {
    fn new(steps: impl IntoIterator<Item = ProviderStep>) -> Self {
        Self {
            steps: Mutex::new(steps.into_iter().collect()),
            requests: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl ModelProvider for ScriptedProvider {
    async fn complete(
        &self,
        request: ModelRequest,
        cancellation: CancellationToken,
    ) -> Result<ModelResponse, ProviderError> {
        self.requests.lock().unwrap().push(request);
        match self.steps.lock().unwrap().pop_front().unwrap() {
            ProviderStep::Response(response) => Ok(response),
            ProviderStep::Error(error) => Err(error),
            ProviderStep::CancelThenError(error) => {
                cancellation.cancel();
                Err(error)
            }
        }
    }
}

struct ScriptedRuntime {
    invoke_steps: Mutex<VecDeque<Result<ToolResult, RuntimeError>>>,
    fingerprints: Mutex<VecDeque<Result<WorkspaceFingerprint, RuntimeError>>>,
    terminals: Mutex<VecDeque<Result<TerminalSnapshot, RuntimeError>>>,
    requests: Mutex<Vec<ToolRequest>>,
    terminal_revisions: Mutex<Vec<u64>>,
}

impl ScriptedRuntime {
    fn new(
        invoke_steps: impl IntoIterator<Item = Result<ToolResult, RuntimeError>>,
        fingerprints: impl IntoIterator<Item = Result<WorkspaceFingerprint, RuntimeError>>,
        terminals: impl IntoIterator<Item = Result<TerminalSnapshot, RuntimeError>>,
    ) -> Self {
        Self {
            invoke_steps: Mutex::new(invoke_steps.into_iter().collect()),
            fingerprints: Mutex::new(fingerprints.into_iter().collect()),
            terminals: Mutex::new(terminals.into_iter().collect()),
            requests: Mutex::new(Vec::new()),
            terminal_revisions: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl ToolRuntime for ScriptedRuntime {
    async fn invoke(
        &self,
        request: ToolRequest,
        _: CancellationToken,
    ) -> Result<ToolResult, RuntimeError> {
        self.requests.lock().unwrap().push(request);
        self.invoke_steps.lock().unwrap().pop_front().unwrap()
    }
}

#[async_trait::async_trait]
impl AgentRuntime for ScriptedRuntime {
    async fn workspace_fingerprint(
        &self,
        _: CancellationToken,
    ) -> Result<WorkspaceFingerprint, RuntimeError> {
        self.fingerprints.lock().unwrap().pop_front().unwrap()
    }

    async fn terminal_snapshot(
        &self,
        revision: u64,
        _: CancellationToken,
    ) -> Result<TerminalSnapshot, RuntimeError> {
        self.terminal_revisions.lock().unwrap().push(revision);
        self.terminals.lock().unwrap().pop_front().unwrap()
    }
}

#[derive(Default)]
struct RecordingSink {
    events: Mutex<Vec<AgentEvent>>,
}

struct IdentityRedactor;

impl ContextRedactor for IdentityRedactor {
    fn redact(&self, content: &str) -> String {
        content.to_owned()
    }
}

struct KnownSecretRedactor;

impl ContextRedactor for KnownSecretRedactor {
    fn redact(&self, content: &str) -> String {
        content.replace("provider-secret", "<redacted>")
    }
}

#[async_trait::async_trait]
impl AgentEventSink for RecordingSink {
    async fn emit(&self, event: AgentEvent) -> Result<(), RuntimeError> {
        self.events.lock().unwrap().push(event);
        Ok(())
    }
}

struct PendingTerminalRuntime {
    fingerprint: WorkspaceFingerprint,
    terminal_calls: AtomicUsize,
}

#[async_trait::async_trait]
impl ToolRuntime for PendingTerminalRuntime {
    async fn invoke(
        &self,
        _: ToolRequest,
        _: CancellationToken,
    ) -> Result<ToolResult, RuntimeError> {
        panic!("pending terminal runtime is not invoked")
    }
}

#[async_trait::async_trait]
impl AgentRuntime for PendingTerminalRuntime {
    async fn workspace_fingerprint(
        &self,
        _: CancellationToken,
    ) -> Result<WorkspaceFingerprint, RuntimeError> {
        Ok(self.fingerprint)
    }

    async fn terminal_snapshot(
        &self,
        _: u64,
        _: CancellationToken,
    ) -> Result<TerminalSnapshot, RuntimeError> {
        self.terminal_calls.fetch_add(1, Ordering::SeqCst);
        pending().await
    }
}

#[derive(Default)]
struct PendingFailedTestSink {
    pending_started: AtomicBool,
}

#[async_trait::async_trait]
impl AgentEventSink for PendingFailedTestSink {
    async fn emit(&self, event: AgentEvent) -> Result<(), RuntimeError> {
        if matches!(
            event,
            AgentEvent::Tests(ref test) if test.status == TestStatus::Failed
        ) {
            self.pending_started.store(true, Ordering::SeqCst);
            return pending().await;
        }
        Ok(())
    }
}

#[tokio::test]
async fn tool_call_result_continuation_and_final_snapshot_complete() {
    let provider = Arc::new(ScriptedProvider::new([
        ProviderStep::Response(tool_call("call-1", cargo_test())),
        ProviderStep::Response(ModelResponse::Final {
            content: "finished".to_owned(),
        }),
    ]));
    let fingerprint = fp(1);
    let runtime = Arc::new(ScriptedRuntime::new(
        [Ok(ToolResult::text("tests passed"))],
        [Ok(fingerprint), Ok(fingerprint), Ok(fingerprint)],
        [Ok(snapshot(fingerprint))],
    ));
    let sink = Arc::new(RecordingSink::default());

    let outcome = loop_for(
        provider.clone(),
        runtime.clone(),
        sink.clone(),
        generous_limits(),
    )
    .run(
        AgentInput::new("fix it", "workspace"),
        CancellationToken::new(),
    )
    .await;

    let AgentOutcome::Completed(completed) = outcome else {
        panic!("current passed Cargo test must permit completion");
    };
    assert_eq!(completed.final_text, "finished");
    assert_eq!(completed.workspace_revision, 0);
    assert_eq!(completed.terminal_snapshot.fingerprint, fingerprint);
    let requests = provider.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(matches!(
        &requests[1].messages[2],
        ModelMessage::AssistantToolCall(ToolCall { id, .. }) if id == "call-1"
    ));
    assert!(matches!(
        &requests[1].messages[3],
        ModelMessage::ToolResult { tool_call_id, content }
            if tool_call_id == "call-1"
                && content == "[tool_status=succeeded; truncated=false]\ntests passed"
    ));
    assert!(sink.events.lock().unwrap().iter().any(|event| matches!(
        event,
        AgentEvent::Tests(test) if test.status == TestStatus::Passed
    )));
    assert!(
        sink.events
            .lock()
            .unwrap()
            .iter()
            .any(|event| matches!(event, AgentEvent::Diff(_)))
    );
    assert_eq!(*runtime.terminal_revisions.lock().unwrap(), [0]);
}

#[tokio::test]
async fn ordinary_failed_tool_result_is_bounded_context_and_continues() {
    let provider = Arc::new(ScriptedProvider::new([
        ProviderStep::Response(tool_call("status", ToolRequest::GitStatus)),
        ProviderStep::Response(tool_call("test", cargo_test())),
        ProviderStep::Response(ModelResponse::Final {
            content: "done".to_owned(),
        }),
    ]));
    let fingerprint = fp(2);
    let runtime = Arc::new(ScriptedRuntime::new(
        [
            Ok(ToolResult::truncated_failed_text("git failed safely")),
            Ok(ToolResult::text("passed")),
        ],
        [Ok(fingerprint), Ok(fingerprint), Ok(fingerprint)],
        [Ok(snapshot(fingerprint))],
    ));

    let outcome = loop_for(
        provider.clone(),
        runtime,
        Arc::new(RecordingSink::default()),
        generous_limits(),
    )
    .run(AgentInput::new("task", "repo"), CancellationToken::new())
    .await;
    assert!(matches!(outcome, AgentOutcome::Completed(_)));
    assert!(matches!(
        &provider.requests.lock().unwrap()[1].messages[3],
        ModelMessage::ToolResult { content, .. }
            if content == "[tool_status=failed; truncated=true]\ngit failed safely"
    ));
}

#[tokio::test]
async fn invalid_or_repeated_tool_calls_never_reach_runtime() {
    for steps in [
        vec![ProviderStep::Response(tool_call(
            "",
            ToolRequest::GitStatus,
        ))],
        vec![
            ProviderStep::Response(tool_call("same", ToolRequest::GitStatus)),
            ProviderStep::Response(tool_call("same", ToolRequest::GitDiff)),
        ],
    ] {
        let fingerprint = fp(3);
        let expected_invocations = usize::from(steps.len() == 2);
        let runtime = Arc::new(ScriptedRuntime::new(
            [Ok(ToolResult::text("first"))],
            [Ok(fingerprint)],
            [Ok(snapshot(fingerprint))],
        ));
        let outcome = loop_for(
            Arc::new(ScriptedProvider::new(steps)),
            runtime.clone(),
            Arc::new(RecordingSink::default()),
            generous_limits(),
        )
        .run(AgentInput::new("task", "repo"), CancellationToken::new())
        .await;
        let AgentOutcome::Failed(failure) = outcome else {
            panic!("invalid calls must fail");
        };
        assert_eq!(failure.code, "INVALID_TOOL_CALL");
        assert_eq!(runtime.requests.lock().unwrap().len(), expected_invocations);
    }
}

#[tokio::test]
async fn provider_and_runtime_retryability_is_preserved_without_raw_messages() {
    for retryable in [false, true] {
        let secret = "raw-provider-body-secret";
        let provider = Arc::new(ScriptedProvider::new([ProviderStep::Error(
            ProviderError::new("PROVIDER_DOWN", secret, retryable),
        )]));
        let fingerprint = fp(4);
        let sink = Arc::new(RecordingSink::default());
        let outcome = loop_for(
            provider,
            Arc::new(ScriptedRuntime::new(
                [],
                [Ok(fingerprint)],
                [Ok(snapshot(fingerprint))],
            )),
            sink.clone(),
            generous_limits(),
        )
        .run(AgentInput::new("task", "repo"), CancellationToken::new())
        .await;
        let AgentOutcome::Failed(failure) = outcome else {
            panic!("provider error must fail");
        };
        assert_eq!(failure.code, "PROVIDER_DOWN");
        assert_eq!(failure.retryable, retryable);
        assert!(!format!("{failure:?}").contains(secret));
        assert!(!format!("{:?}", sink.events.lock().unwrap()).contains(secret));

        let runtime = Arc::new(ScriptedRuntime::new(
            [Err(RuntimeError::new("RUNTIME_DOWN", "private", retryable))],
            [Ok(fingerprint)],
            [Ok(snapshot(fingerprint))],
        ));
        let outcome = loop_for(
            Arc::new(ScriptedProvider::new([ProviderStep::Response(tool_call(
                "status",
                ToolRequest::GitStatus,
            ))])),
            runtime,
            Arc::new(RecordingSink::default()),
            generous_limits(),
        )
        .run(AgentInput::new("task", "repo"), CancellationToken::new())
        .await;
        let AgentOutcome::Failed(failure) = outcome else {
            panic!("runtime error must fail");
        };
        assert_eq!(failure.code, "RUNTIME_DOWN");
        assert_eq!(failure.retryable, retryable);
    }
}

#[tokio::test]
async fn tool_results_are_redacted_before_the_next_provider_request() {
    let fingerprint = fp(13);
    let provider = Arc::new(ScriptedProvider::new([
        ProviderStep::Response(tool_call("read", ToolRequest::GitStatus)),
        ProviderStep::Error(ProviderError::new("STOP", "safe", false)),
    ]));
    let loop_ = AgentLoop::new(
        provider.clone(),
        Arc::new(ScriptedRuntime::new(
            [Ok(ToolResult::text("Authorization: provider-secret"))],
            [Ok(fingerprint)],
            [Ok(snapshot(fingerprint))],
        )),
        Arc::new(RecordingSink::default()),
        Arc::new(KnownSecretRedactor),
        generous_limits(),
    );
    let _ = loop_
        .run(AgentInput::new("task", "repo"), CancellationToken::new())
        .await;
    let rendered = format!("{:?}", provider.requests.lock().unwrap()[1]);
    assert!(!rendered.contains("provider-secret"));
    assert!(rendered.contains("<redacted>"));
    assert!(rendered.contains("truncated=true"));
}

#[tokio::test]
async fn input_and_final_text_are_redacted_at_provider_and_outcome_boundaries() {
    let fingerprint = fp(14);
    let provider = Arc::new(ScriptedProvider::new([
        ProviderStep::Response(tool_call("test", cargo_test())),
        ProviderStep::Response(ModelResponse::Final {
            content: "finished provider-secret".to_owned(),
        }),
    ]));
    let loop_ = AgentLoop::new(
        provider.clone(),
        Arc::new(ScriptedRuntime::new(
            [Ok(ToolResult::text("passed"))],
            [Ok(fingerprint), Ok(fingerprint), Ok(fingerprint)],
            [Ok(snapshot(fingerprint))],
        )),
        Arc::new(RecordingSink::default()),
        Arc::new(KnownSecretRedactor),
        generous_limits(),
    );
    let outcome = loop_
        .run(
            AgentInput::new("task provider-secret", "repo provider-secret"),
            CancellationToken::new(),
        )
        .await;
    let requests = format!("{:?}", provider.requests.lock().unwrap());
    assert!(!requests.contains("provider-secret"));
    assert!(requests.contains("<redacted>"));
    let rendered_outcome = format!("{outcome:?}");
    assert!(!rendered_outcome.contains("provider-secret"));
    assert!(rendered_outcome.contains("<redacted>"));
}

#[tokio::test]
async fn secret_bearing_tool_arguments_are_rejected_before_execution() {
    let fingerprint = fp(15);
    let runtime = Arc::new(ScriptedRuntime::new(
        [],
        [Ok(fingerprint)],
        [Ok(snapshot(fingerprint))],
    ));
    let loop_ = AgentLoop::new(
        Arc::new(ScriptedProvider::new([ProviderStep::Response(tool_call(
            "replace",
            ToolRequest::ReplaceFile {
                path: "src/lib.rs".to_owned(),
                expected_sha256: None,
                content: "Authorization: provider-secret".to_owned(),
            },
        ))])),
        runtime.clone(),
        Arc::new(RecordingSink::default()),
        Arc::new(KnownSecretRedactor),
        generous_limits(),
    );
    let outcome = loop_
        .run(AgentInput::new("task", "repo"), CancellationToken::new())
        .await;
    let AgentOutcome::Failed(failure) = outcome else {
        panic!("secret-bearing tool calls must fail closed");
    };
    assert_eq!(failure.code, "PROVIDER_SECRET_DETECTED");
    assert!(runtime.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn every_model_tool_and_provider_byte_budget_fails_retryably() {
    let fingerprint = fp(5);

    let tiny_provider = AgentLimits::try_new(4, 4, 1, 1024).unwrap();
    let provider = Arc::new(ScriptedProvider::new([]));
    let outcome = loop_for(
        provider.clone(),
        Arc::new(ScriptedRuntime::new(
            [],
            [Ok(fingerprint)],
            [Ok(snapshot(fingerprint))],
        )),
        Arc::new(RecordingSink::default()),
        tiny_provider,
    )
    .run(AgentInput::new("task", "repo"), CancellationToken::new())
    .await;
    assert_limit(outcome, "AGENT_CONTEXT_LIMIT_REACHED");
    assert!(provider.requests.lock().unwrap().is_empty());

    let tiny_tool = AgentLimits::try_new(4, 4, 64 * 1024, 3).unwrap();
    let outcome = loop_for(
        Arc::new(ScriptedProvider::new([ProviderStep::Response(tool_call(
            "status",
            ToolRequest::GitStatus,
        ))])),
        Arc::new(ScriptedRuntime::new(
            [Ok(ToolResult::text("long"))],
            [Ok(fingerprint)],
            [Ok(snapshot(fingerprint))],
        )),
        Arc::new(RecordingSink::default()),
        tiny_tool,
    )
    .run(AgentInput::new("task", "repo"), CancellationToken::new())
    .await;
    assert_limit(outcome, "AGENT_CONTEXT_LIMIT_REACHED");

    let one_step = AgentLimits::try_new(1, 4, 64 * 1024, 1024).unwrap();
    let outcome = loop_for(
        Arc::new(ScriptedProvider::new([ProviderStep::Response(tool_call(
            "status",
            ToolRequest::GitStatus,
        ))])),
        Arc::new(ScriptedRuntime::new(
            [Ok(ToolResult::text("ok"))],
            [Ok(fingerprint)],
            [Ok(snapshot(fingerprint))],
        )),
        Arc::new(RecordingSink::default()),
        one_step,
    )
    .run(AgentInput::new("task", "repo"), CancellationToken::new())
    .await;
    assert_limit(outcome, "AGENT_STEP_LIMIT_REACHED");

    let one_tool = AgentLimits::try_new(4, 1, 64 * 1024, 1024).unwrap();
    let outcome = loop_for(
        Arc::new(ScriptedProvider::new([
            ProviderStep::Response(tool_call("one", ToolRequest::GitStatus)),
            ProviderStep::Response(tool_call("two", ToolRequest::GitDiff)),
        ])),
        Arc::new(ScriptedRuntime::new(
            [Ok(ToolResult::text("ok"))],
            [Ok(fingerprint)],
            [Ok(snapshot(fingerprint))],
        )),
        Arc::new(RecordingSink::default()),
        one_tool,
    )
    .run(AgentInput::new("task", "repo"), CancellationToken::new())
    .await;
    assert_limit(outcome, "AGENT_STEP_LIMIT_REACHED");
}

#[tokio::test]
async fn cancellation_wins_over_simultaneous_fatal_error_and_still_snapshots() {
    let fingerprint = fp(6);
    let runtime = Arc::new(ScriptedRuntime::new(
        [],
        [Ok(fingerprint)],
        [Ok(snapshot(fingerprint))],
    ));
    let outcome = loop_for(
        Arc::new(ScriptedProvider::new([ProviderStep::CancelThenError(
            ProviderError::new("FATAL", "must lose", false),
        )])),
        runtime.clone(),
        Arc::new(RecordingSink::default()),
        generous_limits(),
    )
    .run(AgentInput::new("task", "repo"), CancellationToken::new())
    .await;
    assert!(matches!(outcome, AgentOutcome::Cancelled(_)));
    assert_eq!(runtime.terminal_revisions.lock().unwrap().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn cancellation_with_pending_terminal_snapshot_returns_within_total_budget() {
    let runtime = Arc::new(PendingTerminalRuntime {
        fingerprint: fp(17),
        terminal_calls: AtomicUsize::new(0),
    });
    let outcome = tokio::time::timeout(
        Duration::from_secs(11),
        AgentLoop::new(
            Arc::new(ScriptedProvider::new([ProviderStep::CancelThenError(
                ProviderError::new("FATAL", "must lose", false),
            )])),
            runtime.clone(),
            Arc::new(RecordingSink::default()),
            Arc::new(IdentityRedactor),
            generous_limits(),
        )
        .run(AgentInput::new("task", "repo"), CancellationToken::new()),
    )
    .await
    .expect("terminal finalization must beat the verified total deadline");

    let AgentOutcome::Cancelled(cancelled) = outcome else {
        panic!("cancellation must retain precedence");
    };
    assert!(cancelled.terminal_snapshot.is_none());
    assert_eq!(runtime.terminal_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn failed_cargo_terminal_event_pending_returns_within_total_budget() {
    let fingerprint = fp(18);
    let sink = Arc::new(PendingFailedTestSink::default());
    let outcome = tokio::time::timeout(
        Duration::from_secs(11),
        AgentLoop::new(
            Arc::new(ScriptedProvider::new([ProviderStep::Response(tool_call(
                "test",
                cargo_test(),
            ))])),
            Arc::new(ScriptedRuntime::new(
                [Err(RuntimeError::new("RUNTIME_DOWN", "private", true))],
                [Ok(fingerprint), Ok(fingerprint)],
                [Ok(snapshot(fingerprint))],
            )),
            sink.clone(),
            Arc::new(IdentityRedactor),
            generous_limits(),
        )
        .run(AgentInput::new("task", "repo"), CancellationToken::new()),
    )
    .await
    .expect("terminal event and snapshot must share one bounded deadline");

    let AgentOutcome::Failed(failure) = outcome else {
        panic!("runtime failure must remain a failure");
    };
    assert_eq!(failure.code, "RUNTIME_DOWN");
    assert!(failure.retryable);
    assert!(failure.terminal_snapshot.is_none());
    assert!(sink.pending_started.load(Ordering::SeqCst));
}

#[tokio::test]
async fn cancelled_cargo_run_emits_terminal_cancelled_test_status() {
    let fingerprint = fp(16);
    let sink = Arc::new(RecordingSink::default());
    let outcome = loop_for(
        Arc::new(ScriptedProvider::new([ProviderStep::Response(tool_call(
            "test",
            cargo_test(),
        ))])),
        Arc::new(ScriptedRuntime::new(
            [Err(RuntimeError::new(
                "COMMAND_CANCELLED",
                "cancelled",
                false,
            ))],
            [Ok(fingerprint), Ok(fingerprint)],
            [Ok(snapshot(fingerprint))],
        )),
        sink.clone(),
        generous_limits(),
    )
    .run(AgentInput::new("task", "repo"), CancellationToken::new())
    .await;

    assert!(matches!(outcome, AgentOutcome::Cancelled(_)));
    assert!(sink.events.lock().unwrap().iter().any(|event| matches!(
        event,
        AgentEvent::Tests(test) if test.status == TestStatus::Cancelled
    )));
}

#[tokio::test]
async fn replacement_increments_revision_and_queues_before_current_test_passes() {
    let before = fp(7);
    let after = fp(8);
    let sink = Arc::new(RecordingSink::default());
    let outcome = loop_for(
        Arc::new(ScriptedProvider::new([
            ProviderStep::Response(tool_call(
                "replace",
                ToolRequest::ReplaceFile {
                    path: "src/lib.rs".to_owned(),
                    expected_sha256: None,
                    content: "new".to_owned(),
                },
            )),
            ProviderStep::Response(tool_call("test", cargo_test())),
            ProviderStep::Response(ModelResponse::Final {
                content: "done".to_owned(),
            }),
        ])),
        Arc::new(ScriptedRuntime::new(
            [
                Ok(ToolResult::text("replaced")),
                Ok(ToolResult::text("passed")),
            ],
            [Ok(before), Ok(after), Ok(after)],
            [Ok(snapshot(after)), Ok(snapshot(after))],
        )),
        sink.clone(),
        generous_limits(),
    )
    .run(AgentInput::new("task", "repo"), CancellationToken::new())
    .await;
    let AgentOutcome::Completed(completed) = outcome else {
        panic!("post-replacement test is current");
    };
    assert_eq!(completed.workspace_revision, 1);
    let tests = sink
        .events
        .lock()
        .unwrap()
        .iter()
        .filter_map(|event| match event {
            AgentEvent::Tests(test) => Some((test.revision, test.status)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(tests.contains(&(1, TestStatus::Queued)));
    assert!(tests.contains(&(1, TestStatus::Passed)));
    let diffs = sink
        .events
        .lock()
        .unwrap()
        .iter()
        .filter_map(|event| match event {
            AgentEvent::Diff(diff) => Some(diff.revision),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        diffs,
        [1, 1],
        "replace and terminal snapshots both emit diffs"
    );
}

#[tokio::test]
async fn test_or_external_fingerprint_change_invalidates_pass() {
    let a = fp(9);
    let b = fp(10);
    for (fingerprints, terminal) in [
        (vec![Ok(a), Ok(a), Ok(b)], b),
        (vec![Ok(a), Ok(a), Ok(a)], b),
    ] {
        let outcome = loop_for(
            Arc::new(ScriptedProvider::new([
                ProviderStep::Response(tool_call("test", cargo_test())),
                ProviderStep::Response(ModelResponse::Final {
                    content: "done".to_owned(),
                }),
            ])),
            Arc::new(ScriptedRuntime::new(
                [Ok(ToolResult::text("passed"))],
                fingerprints,
                [Ok(snapshot(terminal))],
            )),
            Arc::new(RecordingSink::default()),
            generous_limits(),
        )
        .run(AgentInput::new("task", "repo"), CancellationToken::new())
        .await;
        let AgentOutcome::Failed(failure) = outcome else {
            panic!("stale pass must not complete");
        };
        assert_eq!(failure.code, "CURRENT_TEST_REQUIRED");
        assert_eq!(failure.workspace_revision, 1);
    }
}

#[tokio::test]
async fn terminal_snapshot_failure_cannot_prove_success() {
    let fingerprint = fp(11);
    let outcome = loop_for(
        Arc::new(ScriptedProvider::new([
            ProviderStep::Response(tool_call("test", cargo_test())),
            ProviderStep::Response(ModelResponse::Final {
                content: "done".to_owned(),
            }),
        ])),
        Arc::new(ScriptedRuntime::new(
            [Ok(ToolResult::text("passed"))],
            [Ok(fingerprint), Ok(fingerprint), Ok(fingerprint)],
            [Err(RuntimeError::new(
                "WORKSPACE_TOO_LARGE",
                "capped",
                false,
            ))],
        )),
        Arc::new(RecordingSink::default()),
        generous_limits(),
    )
    .run(AgentInput::new("task", "repo"), CancellationToken::new())
    .await;
    let AgentOutcome::Failed(failure) = outcome else {
        panic!("missing terminal snapshot must fail");
    };
    assert_eq!(failure.code, "WORKSPACE_TOO_LARGE");
    assert!(failure.terminal_snapshot.is_none());
}

#[tokio::test]
async fn truncated_terminal_diff_is_persisted_but_cannot_prove_success() {
    let fingerprint = fp(12);
    let mut terminal = snapshot(fingerprint);
    terminal.diff.files.push(DiffFile {
        path: "large.rs".to_owned(),
        status: DiffFileStatus::Modified,
        patch: "bounded prefix".to_owned(),
        additions: 10,
        deletions: 10,
        truncated: true,
    });
    let outcome = loop_for(
        Arc::new(ScriptedProvider::new([
            ProviderStep::Response(tool_call("test", cargo_test())),
            ProviderStep::Response(ModelResponse::Final {
                content: "done".to_owned(),
            }),
        ])),
        Arc::new(ScriptedRuntime::new(
            [Ok(ToolResult::text("passed"))],
            [Ok(fingerprint), Ok(fingerprint), Ok(fingerprint)],
            [Ok(terminal)],
        )),
        Arc::new(RecordingSink::default()),
        generous_limits(),
    )
    .run(AgentInput::new("task", "repo"), CancellationToken::new())
    .await;
    let AgentOutcome::Failed(failure) = outcome else {
        panic!("truncated terminal diff must not complete");
    };
    assert_eq!(failure.code, "TERMINAL_DIFF_TRUNCATED");
    assert!(!failure.retryable);
    assert!(failure.terminal_snapshot.unwrap().diff.files[0].truncated);
}

fn loop_for(
    provider: Arc<ScriptedProvider>,
    runtime: Arc<ScriptedRuntime>,
    sink: Arc<RecordingSink>,
    limits: AgentLimits,
) -> AgentLoop {
    AgentLoop::new(provider, runtime, sink, Arc::new(IdentityRedactor), limits)
}

fn generous_limits() -> AgentLimits {
    AgentLimits::try_new(16, 16, 1024 * 1024, 64 * 1024).unwrap()
}

fn tool_call(id: &str, request: ToolRequest) -> ModelResponse {
    ModelResponse::ToolCall(ToolCall {
        id: id.to_owned(),
        request,
    })
}

fn cargo_test() -> ToolRequest {
    ToolRequest::CargoTest {
        package: Some("package".to_owned()),
        test: None,
        timeout_ms: 1_000,
    }
}

fn fp(byte: u8) -> WorkspaceFingerprint {
    WorkspaceFingerprint::from_bytes([byte; 32])
}

fn snapshot(fingerprint: WorkspaceFingerprint) -> TerminalSnapshot {
    TerminalSnapshot {
        fingerprint,
        diff: DiffEvent {
            revision: u64::MAX,
            files: Vec::new(),
        },
    }
}

fn assert_limit(outcome: AgentOutcome, code: &str) {
    let AgentOutcome::Failed(failure) = outcome else {
        panic!("budget exhaustion must fail");
    };
    assert_eq!(failure.code, code);
    assert!(failure.retryable);
}
