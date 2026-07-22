mod support;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use coding_agent_app::{
    AttemptArtifactObservation, AttemptReservation, CodingAgentAttempt, CodingAgentAttemptFactory,
    CodingAgentRunner, CodingAgentRunnerConfig, CodingAttemptError, CodingAttemptProvisionError,
    EventDispatcherHandle, QuiesceResult, ServiceState, ServiceStateController, StoreWriterHandle,
    SystemWallClock, TaskAgentRuntime, TaskManagerHandle, TaskModelProviderFactory,
    TaskModelSession,
};
use coding_agent_core::{
    AgentLimits, AgentRuntime, ContextRedactor, DiffEvent, DiffFile, DiffFileStatus, ModelMessage,
    ModelProvider, ModelRequest, ModelResponse, ProviderError, RuntimeError, TerminalSnapshot,
    ToolCall, ToolCallBatch, ToolRequest, ToolResult, ToolRuntime, WorkspaceFingerprint,
};
use coding_agent_domain::{CanonicalPath, EventCursor, Task, TaskEventKind, TaskId, TaskStatus};
use coding_agent_runtime::WorktreeIdentity;
use coding_agent_store::{AttemptArtifactState, Store};
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const SECRET: &str = "provider-key-123456";

#[derive(Clone, Copy)]
enum ProviderMode {
    SuccessfulChange,
    Blocking,
    UnsafeFailure,
}

struct ScriptedProviderFactory {
    mode: ProviderMode,
    starts: AtomicUsize,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    entered: Arc<Notify>,
}

impl ScriptedProviderFactory {
    fn new(mode: ProviderMode) -> Self {
        Self {
            mode,
            starts: AtomicUsize::new(0),
            requests: Arc::new(Mutex::new(Vec::new())),
            entered: Arc::new(Notify::new()),
        }
    }
}

impl TaskModelProviderFactory for ScriptedProviderFactory {
    fn start_task(&self) -> TaskModelSession {
        self.starts.fetch_add(1, Ordering::SeqCst);
        let responses = match self.mode {
            ProviderMode::SuccessfulChange => VecDeque::from([
                Ok(ModelResponse::ToolCalls(ToolCallBatch {
                    assistant_content: None,
                    reasoning_content: None,
                    calls: vec![
                        ToolCall {
                            id: "replace-1".to_owned(),
                            request: ToolRequest::ReplaceFile {
                                path: "src/lib.rs".to_owned(),
                                expected_sha256: None,
                                content: "pub fn changed() {}\n".to_owned(),
                            },
                        },
                        ToolCall {
                            id: "test-1".to_owned(),
                            request: ToolRequest::CargoTest {
                                package: Some("demo".to_owned()),
                                test: None,
                                timeout_ms: 1_000,
                            },
                        },
                    ],
                })),
                Ok(ModelResponse::Final {
                    content: "done".to_owned(),
                }),
            ]),
            ProviderMode::Blocking | ProviderMode::UnsafeFailure => VecDeque::new(),
        };
        TaskModelSession::new(
            Arc::new(ScriptedProvider {
                mode: self.mode,
                responses: Mutex::new(responses),
                requests: Arc::clone(&self.requests),
                entered: Arc::clone(&self.entered),
            }),
            Arc::new(TestRedactor),
        )
    }
}

struct ScriptedProvider {
    mode: ProviderMode,
    responses: Mutex<VecDeque<Result<ModelResponse, ProviderError>>>,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    entered: Arc<Notify>,
}

#[async_trait::async_trait]
impl ModelProvider for ScriptedProvider {
    async fn complete(
        &self,
        request: ModelRequest,
        cancellation: CancellationToken,
    ) -> Result<ModelResponse, ProviderError> {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(request);
        self.entered.notify_one();
        match self.mode {
            ProviderMode::SuccessfulChange => self
                .responses
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_front()
                .expect("one scripted response per provider request"),
            ProviderMode::Blocking => {
                cancellation.cancelled().await;
                Err(ProviderError::new(
                    "PROVIDER_CANCELLED",
                    "provider request cancelled",
                    false,
                ))
            }
            ProviderMode::UnsafeFailure => Err(ProviderError::new(
                "unsafe/code",
                format!("must not persist {SECRET}"),
                false,
            )),
        }
    }
}

struct TestRedactor;

impl ContextRedactor for TestRedactor {
    fn redact(&self, content: &str) -> String {
        content.replace(SECRET, "<redacted>")
    }
}

#[derive(Clone, Copy)]
enum ProvisionMode {
    Ready,
    PartialFailure,
}

struct ScriptedAttemptFactory {
    mode: ProvisionMode,
    runtime: Arc<ScriptedRuntime>,
    prepared: AtomicUsize,
    provisioned: Arc<AtomicUsize>,
}

impl ScriptedAttemptFactory {
    fn new(mode: ProvisionMode, runtime: Arc<ScriptedRuntime>) -> Self {
        Self {
            mode,
            runtime,
            prepared: AtomicUsize::new(0),
            provisioned: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait::async_trait]
impl CodingAgentAttemptFactory for ScriptedAttemptFactory {
    async fn prepare(
        &self,
        identity: WorktreeIdentity,
        repository: coding_agent_domain::Repository,
        cancellation: CancellationToken,
    ) -> Result<Box<dyn CodingAgentAttempt>, CodingAttemptError> {
        if cancellation.is_cancelled() {
            return Err(CodingAttemptError::new("COMMAND_CANCELLED", false));
        }
        self.prepared.fetch_add(1, Ordering::SeqCst);
        let path = repository
            .git_root
            .as_path()
            .join("scripted-worktrees")
            .join(identity.task_id())
            .join(identity.attempt().to_string());
        Ok(Box::new(ScriptedAttempt {
            mode: self.mode,
            reservation: AttemptReservation::new(
                "0123456789abcdef0123456789abcdef01234567",
                identity.branch_name(),
                CanonicalPath::try_from_canonical(path).unwrap(),
            ),
            runtime: Arc::clone(&self.runtime),
            provisioned: Arc::clone(&self.provisioned),
            ready: AtomicBool::new(false),
        }))
    }
}

struct ScriptedAttempt {
    mode: ProvisionMode,
    reservation: AttemptReservation,
    runtime: Arc<ScriptedRuntime>,
    provisioned: Arc<AtomicUsize>,
    ready: AtomicBool,
}

#[async_trait::async_trait]
impl CodingAgentAttempt for ScriptedAttempt {
    fn reservation(&self) -> &AttemptReservation {
        &self.reservation
    }

    async fn provision(
        &mut self,
        _cancellation: CancellationToken,
    ) -> Result<(), CodingAttemptProvisionError> {
        self.provisioned.fetch_add(1, Ordering::SeqCst);
        match self.mode {
            ProvisionMode::Ready => {
                self.ready.store(true, Ordering::Release);
                Ok(())
            }
            ProvisionMode::PartialFailure => Err(CodingAttemptProvisionError::new(
                CodingAttemptError::new("WORKTREE_CREATE_FAILED", true),
                AttemptArtifactObservation::Partial,
            )),
        }
    }

    async fn runtime(
        &self,
        _cancellation: CancellationToken,
    ) -> Result<TaskAgentRuntime, CodingAttemptError> {
        if !self.ready.load(Ordering::Acquire) {
            return Err(CodingAttemptError::new(
                "WORKTREE_STATE_INCONSISTENT",
                false,
            ));
        }
        TaskAgentRuntime::try_new(
            self.runtime.clone(),
            "Cargo package demo; targets: library; validation selector: package demo",
        )
    }
}

struct ScriptedRuntime {
    workspace_version: AtomicU8,
    terminal_calls: AtomicUsize,
}

impl ScriptedRuntime {
    fn new() -> Self {
        Self {
            workspace_version: AtomicU8::new(0),
            terminal_calls: AtomicUsize::new(0),
        }
    }

    fn fingerprint(&self) -> WorkspaceFingerprint {
        WorkspaceFingerprint::from_bytes([self.workspace_version.load(Ordering::Acquire); 32])
    }
}

#[async_trait::async_trait]
impl ToolRuntime for ScriptedRuntime {
    async fn invoke(
        &self,
        request: ToolRequest,
        _cancellation: CancellationToken,
    ) -> Result<ToolResult, RuntimeError> {
        if matches!(request, ToolRequest::ReplaceFile { .. }) {
            self.workspace_version.store(1, Ordering::Release);
        }
        Ok(ToolResult::text(format!("ok {SECRET}")))
    }
}

#[async_trait::async_trait]
impl AgentRuntime for ScriptedRuntime {
    async fn workspace_fingerprint(
        &self,
        _cancellation: CancellationToken,
    ) -> Result<WorkspaceFingerprint, RuntimeError> {
        Ok(self.fingerprint())
    }

    async fn terminal_snapshot(
        &self,
        revision: u64,
        _cancellation: CancellationToken,
    ) -> Result<TerminalSnapshot, RuntimeError> {
        self.terminal_calls.fetch_add(1, Ordering::SeqCst);
        Ok(TerminalSnapshot {
            fingerprint: self.fingerprint(),
            diff: DiffEvent {
                revision,
                files: vec![DiffFile {
                    path: "src/lib.rs".to_owned(),
                    status: DiffFileStatus::Modified,
                    patch: "+pub fn changed() {}\n".to_owned(),
                    additions: 1,
                    deletions: 0,
                    truncated: false,
                }],
            },
        })
    }
}

struct Fixture {
    base: support::StoreFixture,
    writer: StoreWriterHandle,
    manager: TaskManagerHandle,
    providers: Arc<ScriptedProviderFactory>,
    attempts: Arc<ScriptedAttemptFactory>,
    runtime: Arc<ScriptedRuntime>,
}

impl Fixture {
    async fn new(provider_mode: ProviderMode, provision_mode: ProvisionMode) -> Self {
        let base = support::store_fixture().await;
        let dispatcher = EventDispatcherHandle::spawn(base.store.clone(), 1_024)
            .await
            .unwrap();
        let writer = StoreWriterHandle::spawn(base.store.clone(), Arc::new(dispatcher.clone()), 64);
        let providers = Arc::new(ScriptedProviderFactory::new(provider_mode));
        let runtime = Arc::new(ScriptedRuntime::new());
        let attempts = Arc::new(ScriptedAttemptFactory::new(
            provision_mode,
            Arc::clone(&runtime),
        ));
        let runner = Arc::new(CodingAgentRunner::new(
            writer.clone(),
            providers.clone(),
            attempts.clone(),
            Arc::new(SystemWallClock),
            AgentLimits::try_new(8, 8, 256 * 1024, 256 * 1024).unwrap(),
            CodingAgentRunnerConfig::try_new(Duration::from_secs(5), Duration::from_secs(10))
                .unwrap(),
        ));
        let manager = TaskManagerHandle::spawn(
            base.store.clone(),
            writer.clone(),
            dispatcher,
            ServiceStateController::new(ServiceState::Ready),
            runner,
            1,
            64,
        );
        Self {
            base,
            writer,
            manager,
            providers,
            attempts,
            runtime,
        }
    }

    async fn enqueue(&self) -> Task {
        let task = self
            .writer
            .create_task(
                support::new_task(self.base.repository.id, "change the demo"),
                support::deadline(),
            )
            .await
            .unwrap()
            .value
            .task()
            .clone();
        self.manager.notify_queued(task.id).await.unwrap();
        task
    }

    async fn wait_for(&self, task_id: TaskId, status: TaskStatus) -> Task {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let task = self.detail(task_id).await.task;
                if task.status == status {
                    break task;
                }
                if matches!(
                    task.status,
                    TaskStatus::Completed
                        | TaskStatus::Failed
                        | TaskStatus::Cancelled
                        | TaskStatus::Interrupted
                ) {
                    panic!(
                        "task reached unexpected terminal state {:?}: {:?}",
                        task.status, task.failure
                    );
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap()
    }

    async fn detail(&self, task_id: TaskId) -> coding_agent_store::TaskDetail {
        self.base.store.task_detail(task_id).await.unwrap().unwrap()
    }

    async fn event_kinds(&self, task_id: TaskId) -> Vec<TaskEventKind> {
        self.base
            .store
            .task_events_after(task_id, EventCursor::ZERO, usize::MAX)
            .await
            .unwrap()
            .events
            .into_iter()
            .map(|event| event.payload.kind())
            .collect()
    }

    fn store(&self) -> &Store {
        &self.base.store
    }
}

#[tokio::test]
async fn runner_persists_ready_artifact_and_maps_live_and_terminal_panels() {
    let fixture = Fixture::new(ProviderMode::SuccessfulChange, ProvisionMode::Ready).await;
    let task = fixture.enqueue().await;
    fixture.wait_for(task.id, TaskStatus::Completed).await;

    let detail = fixture.detail(task.id).await;
    assert_eq!(detail.plan.unwrap().revision, 3);
    assert!(detail.activity.len() >= 5);
    let diff = detail.diff.unwrap();
    assert_eq!(diff.revision, 1);
    assert!(!diff.files[0].truncated);
    assert_eq!(
        detail.tests.unwrap().status,
        coding_agent_domain::TestStatus::Passed
    );
    let artifact = fixture
        .store()
        .load_attempt_artifact(task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(artifact.state, AttemptArtifactState::Ready);
    assert_eq!(fixture.providers.starts.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.attempts.prepared.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.attempts.provisioned.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.runtime.terminal_calls.load(Ordering::SeqCst),
        2,
        "replace collects a live diff and completion collects the terminal snapshot"
    );

    let kinds = fixture.event_kinds(task.id).await;
    assert!(kinds.contains(&TaskEventKind::PlanUpdated));
    assert!(kinds.contains(&TaskEventKind::ActivityAppended));
    assert!(kinds.contains(&TaskEventKind::DiffUpdated));
    assert!(kinds.contains(&TaskEventKind::TestUpdated));
    assert_eq!(kinds.last(), Some(&TaskEventKind::TaskCompleted));
    assert_eq!(
        kinds
            .iter()
            .filter(|kind| **kind == TaskEventKind::DiffUpdated)
            .count(),
        1,
        "live and terminal copies of the same snapshot are coalesced"
    );

    let requests = fixture
        .providers
        .requests
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(requests.len(), 2);
    let first_user = requests[0]
        .messages
        .iter()
        .find_map(|message| match message {
            ModelMessage::User(content) => Some(content),
            _ => None,
        })
        .unwrap();
    assert!(first_user.contains("Cargo package demo"));
    assert!(!first_user.contains(&fixture.base.repository.git_root.to_string()));
    let ModelMessage::AssistantToolCalls(batch) = &requests[1].messages[2] else {
        panic!("the second provider request must preserve one ordered assistant batch");
    };
    assert_eq!(
        batch
            .calls
            .iter()
            .map(|call| call.id.as_str())
            .collect::<Vec<_>>(),
        ["replace-1", "test-1"]
    );
    assert!(matches!(
        &requests[1].messages[3],
        ModelMessage::ToolResult { tool_call_id, .. } if tool_call_id == "replace-1"
    ));
    assert!(matches!(
        &requests[1].messages[4],
        ModelMessage::ToolResult { tool_call_id, .. } if tool_call_id == "test-1"
    ));
    assert!(
        requests
            .iter()
            .flat_map(|request| &request.messages)
            .all(|message| match message {
                ModelMessage::ToolResult { content, .. } => !content.contains(SECRET),
                _ => true,
            })
    );
    assert!(
        requests
            .iter()
            .flat_map(|request| &request.messages)
            .any(|message| match message {
                ModelMessage::ToolResult { content, .. } => content.contains("<redacted>"),
                _ => false,
            })
    );
}

#[tokio::test]
async fn normal_cancellation_forces_terminal_diff_and_retains_ready_artifact() {
    let fixture = Fixture::new(ProviderMode::Blocking, ProvisionMode::Ready).await;
    let task = fixture.enqueue().await;
    tokio::time::timeout(Duration::from_secs(5), fixture.providers.entered.notified())
        .await
        .unwrap();
    fixture.manager.cancel(task.id).await.unwrap();
    fixture.wait_for(task.id, TaskStatus::Cancelled).await;

    let detail = fixture.detail(task.id).await;
    assert!(detail.diff.is_some());
    assert!(fixture.runtime.terminal_calls.load(Ordering::SeqCst) >= 1);
    assert_eq!(
        fixture
            .store()
            .load_attempt_artifact(task.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        AttemptArtifactState::Ready
    );
}

#[tokio::test]
async fn forced_quiesce_rejects_the_runners_late_terminal_diff() {
    let fixture = Fixture::new(ProviderMode::Blocking, ProvisionMode::Ready).await;
    let task = fixture.enqueue().await;
    tokio::time::timeout(Duration::from_secs(5), fixture.providers.entered.notified())
        .await
        .unwrap();

    let active = match fixture
        .manager
        .quiesce_and_interrupt(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap()
    {
        QuiesceResult::Durable { active, .. } => active,
        QuiesceResult::Frozen { .. } => panic!("quiesce should commit"),
    };
    assert_eq!(
        fixture.detail(task.id).await.task.status,
        TaskStatus::Interrupted
    );
    assert_eq!(active.len(), 1);
    active.first().unwrap().cancellation.cancel();
    active.into_iter().next().unwrap().done.await.unwrap();

    let detail = fixture.detail(task.id).await;
    assert_eq!(detail.task.status, TaskStatus::Interrupted);
    assert!(
        detail.diff.is_none(),
        "the post-barrier diff stays rejected"
    );
    assert!(fixture.runtime.terminal_calls.load(Ordering::SeqCst) >= 1);
    assert_eq!(
        fixture.event_kinds(task.id).await.last(),
        Some(&TaskEventKind::TaskInterrupted)
    );
}

#[tokio::test]
async fn unsafe_provider_failure_is_safely_mapped_and_ready_artifact_is_retained() {
    let fixture = Fixture::new(ProviderMode::UnsafeFailure, ProvisionMode::Ready).await;
    let task = fixture.enqueue().await;
    let failed = fixture.wait_for(task.id, TaskStatus::Failed).await;

    let failure = failed.failure.unwrap();
    assert_eq!(failure.code, "CODING_AGENT_FAILED");
    assert_eq!(
        failure.message,
        "the coding agent could not complete the task"
    );
    assert!(!failure.message.contains(SECRET));
    assert_eq!(
        fixture
            .store()
            .load_attempt_artifact(task.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        AttemptArtifactState::Ready
    );
    assert!(fixture.detail(task.id).await.diff.is_some());
}

#[tokio::test]
async fn partial_provision_failure_is_marked_inconsistent_and_never_starts_provider() {
    let fixture = Fixture::new(
        ProviderMode::SuccessfulChange,
        ProvisionMode::PartialFailure,
    )
    .await;
    let task = fixture.enqueue().await;
    let failed = fixture.wait_for(task.id, TaskStatus::Failed).await;

    assert_eq!(failed.failure.unwrap().code, "WORKTREE_CREATE_FAILED");
    let artifact = fixture
        .store()
        .load_attempt_artifact(task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(artifact.state, AttemptArtifactState::Inconsistent);
    assert_eq!(
        artifact.failure_code.as_deref(),
        Some("WORKTREE_STATE_INCONSISTENT")
    );
    assert_eq!(fixture.providers.starts.load(Ordering::SeqCst), 0);
}
