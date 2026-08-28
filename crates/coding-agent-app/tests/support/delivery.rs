use std::any::Any;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::State;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{Request, Response, StatusCode};
use axum::routing::post;
use coding_agent_app::{
    PlatformPaths, PreActorStartupRunnerContext, PrimaryRuntime, ProductionStartupRunnerFactory,
    StartupOutcome, StartupRunnerContext, StartupRunnerFactory, StartupRunnerFactoryError,
    StartupRunnerSelection, launch,
};
use coding_agent_domain::{
    CanonicalPath, DeliveryReadiness, NewRepository, Repository, Task, TaskId, TaskStatus,
};
use coding_agent_provider::{ChatCompletionsClient, ClientLimits, ProviderConfig};
use coding_agent_runtime::ProcessLivenessScope;
use coding_agent_store::{RegisterRepositoryOutcome, Store};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use super::{StartupBehavior, StartupFixture, deadline, new_task};

pub mod process;

const API_KEY: &str = "offline-controlled-delivery-key";
const MODEL: &str = "offline-controlled-delivery-model";
const PACKAGE: &str = "delivery_fixture";
const INTEGRATION_TEST: &str = "answer";
const INITIAL_SOURCE: &str = "pub fn answer() -> u32 { 41 }\n";
const CHANGED_SOURCE: &str = "pub fn answer() -> u32 { 42 }\n// approved controlled delivery\n";

/// Full production startup with exactly one test-owned provider substitution.
/// Git probing, repository attachment, runner construction, startup recovery,
/// and the delivery runtime are delegated to the production factory unchanged.
struct OfflineProductionFactory {
    provider: ChatCompletionsClient,
}

#[async_trait::async_trait]
impl StartupRunnerFactory for OfflineProductionFactory {
    async fn validate_pre_database(
        &self,
        _paths: &PlatformPaths,
    ) -> Result<Arc<dyn Any + Send + Sync>, StartupRunnerFactoryError> {
        Ok(Arc::new(self.provider.clone()))
    }

    async fn probe_delivery_git_pre_database(
        &self,
        paths: &PlatformPaths,
        process_liveness_scope: ProcessLivenessScope,
    ) -> Result<Arc<dyn Any + Send + Sync>, StartupRunnerFactoryError> {
        ProductionStartupRunnerFactory
            .probe_delivery_git_pre_database(paths, process_liveness_scope)
            .await
    }

    async fn prepare_before_actors(
        &self,
        context: &PreActorStartupRunnerContext,
    ) -> Result<Arc<dyn Any + Send + Sync>, StartupRunnerFactoryError> {
        ProductionStartupRunnerFactory
            .prepare_before_actors(context)
            .await
    }

    async fn create(
        &self,
        context: StartupRunnerContext,
    ) -> Result<StartupRunnerSelection, StartupRunnerFactoryError> {
        ProductionStartupRunnerFactory.create(context).await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WireRole {
    Planner,
    Executor,
    Reviewer,
}

struct ProviderState {
    calls: AtomicUsize,
}

impl ProviderState {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
        }
    }
}

/// Deterministic loopback-only provider which exercises the real Planner,
/// Executor, Reviewer, file mutation, Cargo test, and review evidence path.
struct ApprovalProvider {
    address: std::net::SocketAddr,
    state: Arc<ProviderState>,
    task: JoinHandle<()>,
}

impl ApprovalProvider {
    async fn spawn() -> Self {
        let state = Arc::new(ProviderState::new());
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind loopback-only delivery provider");
        let address = listener.local_addr().expect("delivery provider address");
        assert!(address.ip().is_loopback());
        let router = Router::new()
            .route("/v1/chat/completions", post(provider_request))
            .with_state(Arc::clone(&state));
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        Self {
            address,
            state,
            task,
        }
    }

    fn client(&self) -> ChatCompletionsClient {
        let encoded = serde_json::to_vec(&serde_json::json!({
            "base_url": format!("http://{}", self.address),
            "model": MODEL,
            "api_key": API_KEY,
            "tool_choice_compatibility": "strict",
        }))
        .expect("encode delivery provider config");
        let config = ProviderConfig::from_json_allow_loopback_http_for_test(&encoded)
            .expect("allow the explicit loopback delivery provider");
        ChatCompletionsClient::new(config, ClientLimits::default())
            .expect("construct delivery provider client")
    }

    pub fn calls(&self) -> usize {
        self.state.calls.load(Ordering::Acquire)
    }
}

impl Drop for ApprovalProvider {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub struct ControlledDeliveryFixture {
    pub startup: StartupFixture,
    pub repository_path: PathBuf,
    pub repository: Repository,
    pub base_head: String,
    pub primary: Box<PrimaryRuntime>,
    provider: ApprovalProvider,
}

impl ControlledDeliveryFixture {
    pub async fn new() -> Self {
        let startup = StartupFixture::new();
        startup.prepare();
        let root = startup
            .paths
            .data_dir
            .parent()
            .expect("startup data directory has a fixture parent")
            .to_path_buf();
        let repository_path = root.join("controlled-delivery-repository");
        seed_repository(&repository_path);
        let repository_path = repository_path
            .canonicalize()
            .expect("canonical delivery repository");
        let base_head = git_line(&repository_path, &["rev-parse", "HEAD"]);

        let store = Store::open(&startup.paths.database_path)
            .await
            .expect("open delivery startup store");
        store
            .migrate()
            .await
            .expect("migrate delivery startup store");
        let repository = match store
            .register_repository(NewRepository {
                selected_path: canonical(&repository_path),
                display_name: "controlled-delivery-e2e".to_owned(),
                git_root: canonical(&repository_path),
                cargo_workspace_root: canonical(&repository_path),
            })
            .await
            .expect("register delivery repository")
        {
            RegisterRepositoryOutcome::Created(repository)
            | RegisterRepositoryOutcome::Existing(repository) => repository,
        };
        store.close().await;

        let provider = ApprovalProvider::spawn().await;
        let mut dependencies = startup.dependencies(StartupBehavior::default());
        dependencies.runner_factory = Arc::new(OfflineProductionFactory {
            provider: provider.client(),
        });
        let primary = match launch(dependencies)
            .await
            .expect("launch isolated production delivery primary")
        {
            StartupOutcome::Primary(primary) => primary,
            StartupOutcome::Secondary(_) => panic!("isolated delivery runtime must be primary"),
        };

        Self {
            startup,
            repository_path,
            repository,
            base_head,
            primary,
            provider,
        }
    }

    pub fn handles(&self) -> coding_agent_app::PrimaryRuntimeTestHandles {
        self.primary.test_handles()
    }

    pub async fn approve_task(&self) -> Task {
        let handles = self.handles();
        let task = handles
            .writer
            .create_task(
                new_task(
                    self.repository.id,
                    "change answer to forty two, run the focused test, and approve the exact diff",
                ),
                deadline(),
            )
            .await
            .expect("create delivery E2E task")
            .value
            .task()
            .clone();
        handles
            .task_manager
            .notify_queued(task.id)
            .await
            .expect("notify delivery E2E task");
        self.wait_for_task(task.id, TaskStatus::Completed).await
    }

    pub async fn wait_for_task(&self, task_id: TaskId, expected: TaskStatus) -> Task {
        tokio::time::timeout(Duration::from_secs(180), async {
            loop {
                let detail = self
                    .handles()
                    .store
                    .task_detail(task_id)
                    .await
                    .expect("load delivery E2E task")
                    .expect("delivery E2E task exists");
                if detail.task.status == expected {
                    return detail.task;
                }
                if matches!(
                    detail.task.status,
                    TaskStatus::Completed
                        | TaskStatus::Failed
                        | TaskStatus::Cancelled
                        | TaskStatus::Interrupted
                ) {
                    panic!(
                        "delivery task reached {:?}, expected {expected:?}: {:?}; provider_calls={}; activity={:?}; diff={:?}; tests={:?}; reviews={:?}",
                        detail.task.status,
                        detail.task.failure,
                        self.provider_calls(),
                        detail.activity,
                        detail.diff,
                        detail.tests,
                        detail.reviews,
                    );
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("delivery task {task_id} did not reach {expected:?}"))
    }

    pub async fn approved_artifact(
        &self,
        task_id: TaskId,
    ) -> coding_agent_store::TaskAttemptArtifact {
        let task = self
            .handles()
            .store
            .task_detail(task_id)
            .await
            .expect("load approved delivery task")
            .expect("approved delivery task exists")
            .task;
        assert_eq!(task.delivery_readiness, DeliveryReadiness::ReviewApproved);
        self.handles()
            .store
            .load_attempt_artifact(task_id)
            .await
            .expect("load approved delivery artifact")
            .expect("approved delivery artifact exists")
    }

    pub fn provider_calls(&self) -> usize {
        self.provider.calls()
    }

    pub fn git_line(&self, arguments: &[&str]) -> String {
        git_line(&self.repository_path, arguments)
    }

    pub fn git_output(&self, arguments: &[&str]) -> Output {
        git_output(&self.repository_path, arguments)
    }

    pub async fn shutdown(self) {
        let outcome = self.primary.shutdown().await;
        assert!(
            !format!("{outcome:?}").contains("Degraded"),
            "delivery fixture shutdown degraded: {outcome:?}"
        );
        assert!(
            !self.startup.paths.instance_descriptor.exists(),
            "delivery fixture leaked its runtime descriptor"
        );
    }
}

async fn provider_request(
    State(state): State<Arc<ProviderState>>,
    request: Request<Body>,
) -> Response<Body> {
    assert_eq!(
        request
            .headers()
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
        Some("Bearer offline-controlled-delivery-key")
    );
    let encoded = to_bytes(request.into_body(), 512 * 1024)
        .await
        .expect("read bounded delivery provider request");
    let body: serde_json::Value =
        serde_json::from_slice(&encoded).expect("delivery provider request is JSON");
    assert_eq!(body["model"], MODEL);
    assert_eq!(body["parallel_tool_calls"], false);
    assert_eq!(body["stream"], false);
    state.calls.fetch_add(1, Ordering::AcqRel);

    let role = request_role(&body);
    let role_run = request_role_run(&body, role);
    match role {
        WireRole::Planner => planner_response(),
        WireRole::Executor => executor_response(&body, role_run),
        WireRole::Reviewer => reviewer_response(&body, role_run),
    }
}

fn planner_response() -> Response<Body> {
    tool_response(
        "planner-submit",
        "submit_plan",
        serde_json::json!({
            "summary": "Change the answer in one isolated worktree and validate it",
            "steps": [{
                "title": "Implement and validate",
                "description": "Update the answer implementation and run its focused test.",
                "acceptance_criteria": ["The answer integration test passes."]
            }],
            "initial_required_checks": [{
                "kind": "cargo_test",
                "package": PACKAGE,
                "integration_test": INTEGRATION_TEST
            }]
        }),
    )
}

fn executor_response(body: &serde_json::Value, role_run: u8) -> Response<Body> {
    let history = tool_history(body);
    if !history.iter().any(|(_, name)| name == "read_file") {
        return tool_response(
            &format!("executor-{role_run}-read"),
            "read_file",
            serde_json::json!({"path": "src/lib.rs", "start_line": 1, "end_line": 12}),
        );
    }
    if !history.iter().any(|(_, name)| name == "replace_file") {
        let payload = latest_success_payload(body);
        return tool_response(
            &format!("executor-{role_run}-replace"),
            "replace_file",
            serde_json::json!({
                "path": "src/lib.rs",
                "expected_sha256": payload["sha256"],
                "content": CHANGED_SOURCE,
            }),
        );
    }
    if !history.iter().any(|(_, name)| name == "cargo_test") {
        return tool_response(
            &format!("executor-{role_run}-test"),
            "cargo_test",
            validation_arguments(body, "cargo_test"),
        );
    }
    if !history
        .iter()
        .any(|(_, name)| name == "update_plan_progress")
    {
        return tool_response(
            &format!("executor-{role_run}-progress"),
            "update_plan_progress",
            serde_json::json!({"updates": [{"step_id": "step-01", "status": "completed"}]}),
        );
    }
    tool_response(
        &format!("executor-{role_run}-submit"),
        "submit_execution",
        serde_json::json!({"summary": "implemented and validated the exact requested change"}),
    )
}

fn reviewer_response(body: &serde_json::Value, role_run: u8) -> Response<Body> {
    let history = tool_history(body);
    if !history
        .iter()
        .any(|(_, name)| name == "review_diff_manifest")
    {
        let manifest_properties = function_properties(body, "review_diff_manifest");
        if manifest_properties
            .get("generation")
            .and_then(|property| property.get("const"))
            .is_none()
        {
            let read_count = history
                .iter()
                .filter(|(_, name)| name == "read_file")
                .count();
            if read_count < 3 {
                return tool_response(
                    &format!("reviewer-{role_run}-read-{read_count}"),
                    "read_file",
                    serde_json::json!({"path": "src/lib.rs", "start_line": 1, "end_line": 12}),
                );
            }
            return tool_batch_response(
                (0..8)
                    .map(|index| {
                        (
                            format!("reviewer-{role_run}-reserved-read-{index}"),
                            "read_file".to_owned(),
                            serde_json::json!({
                                "path": "src/lib.rs",
                                "start_line": 1,
                                "end_line": 12
                            }),
                        )
                    })
                    .collect(),
            );
        }
        let checkpoint = request_handoff(body)["checkpoint"].clone();
        return tool_response(
            &format!("reviewer-{role_run}-manifest"),
            "review_diff_manifest",
            serde_json::json!({
                "generation": checkpoint["generation"],
                "workspace_digest": checkpoint["workspace_digest"],
            }),
        );
    }
    if let Some(arguments) = required_chunk_arguments(body) {
        return tool_response(
            &format!("reviewer-{role_run}-chunks-{}", arguments["start_chunk"]),
            "review_diff_chunks",
            arguments,
        );
    }
    tool_response(
        &format!("reviewer-{role_run}-approved"),
        "submit_review",
        serde_json::json!({
            "verdict": "approved",
            "summary": "reviewed every byte of the current diff and approved it",
            "findings": [],
            "add_required_checks": [],
        }),
    )
}

fn request_role(body: &serde_json::Value) -> WireRole {
    let policy = body["messages"][0]["content"]
        .as_str()
        .expect("role system policy");
    if policy.contains("Planner #1") {
        WireRole::Planner
    } else if policy.contains("Executor") {
        WireRole::Executor
    } else if policy.contains("Reviewer") {
        WireRole::Reviewer
    } else {
        panic!("unknown delivery provider role policy")
    }
}

fn request_role_run(body: &serde_json::Value, role: WireRole) -> u8 {
    if role == WireRole::Planner {
        return 1;
    }
    request_handoff(body)["role_run"]
        .as_u64()
        .and_then(|value| u8::try_from(value).ok())
        .expect("bounded role run")
}

fn request_handoff(body: &serde_json::Value) -> serde_json::Value {
    serde_json::from_str(
        body["messages"][1]["content"]
            .as_str()
            .expect("canonical role handoff"),
    )
    .expect("role handoff is JSON")
}

fn tool_history(body: &serde_json::Value) -> Vec<(String, String)> {
    body["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .filter(|message| message["role"] == "assistant")
        .flat_map(|message| {
            message["tool_calls"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|call| {
                    (
                        call["id"].as_str().expect("tool id").to_owned(),
                        call["function"]["name"]
                            .as_str()
                            .expect("tool name")
                            .to_owned(),
                    )
                })
        })
        .collect()
}

fn latest_success_payload(body: &serde_json::Value) -> serde_json::Value {
    let content = body["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .rev()
        .find(|message| message["role"] == "tool")
        .and_then(|message| message["content"].as_str())
        .expect("latest tool result");
    assert!(content.starts_with("[tool_status=succeeded;"));
    let (_, payload) = content.split_once('\n').expect("tool result envelope");
    serde_json::from_str(payload).expect("tool result payload")
}

fn validation_arguments(body: &serde_json::Value, name: &str) -> serde_json::Value {
    let properties = function_properties(body, name);
    let mut arguments = serde_json::Map::new();
    if let Some(check_id) = properties
        .get("check_id")
        .and_then(|property| property.get("const"))
    {
        arguments.insert("check_id".to_owned(), check_id.clone());
    }
    arguments.insert(
        "package".to_owned(),
        properties
            .get("package")
            .and_then(|property| property.get("const"))
            .cloned()
            .unwrap_or_else(|| serde_json::json!(PACKAGE)),
    );
    arguments.insert(
        "integration_test".to_owned(),
        properties
            .get("integration_test")
            .and_then(|property| property.get("const"))
            .cloned()
            .unwrap_or_else(|| serde_json::json!(INTEGRATION_TEST)),
    );
    serde_json::Value::Object(arguments)
}

fn required_chunk_arguments(body: &serde_json::Value) -> Option<serde_json::Value> {
    let properties = function_properties_optional(body, "review_diff_chunks")?;
    Some(serde_json::json!({
        "generation": properties.get("generation")?.get("const")?.clone(),
        "workspace_digest": {
            "algorithm": properties.get("workspace_digest")?.get("properties")?.get("algorithm")?.get("const")?.clone(),
            "value": properties.get("workspace_digest")?.get("properties")?.get("value")?.get("const")?.clone(),
        },
        "manifest_sha256": properties.get("manifest_sha256")?.get("const")?.clone(),
        "start_chunk": properties.get("start_chunk")?.get("const")?.clone(),
        "count": properties.get("count")?.get("const")?.clone(),
    }))
}

fn function_properties<'a>(
    body: &'a serde_json::Value,
    name: &str,
) -> &'a serde_json::Map<String, serde_json::Value> {
    function_properties_optional(body, name)
        .unwrap_or_else(|| panic!("request does not expose {name}"))
}

fn function_properties_optional<'a>(
    body: &'a serde_json::Value,
    name: &str,
) -> Option<&'a serde_json::Map<String, serde_json::Value>> {
    body["tools"]
        .as_array()?
        .iter()
        .find(|tool| tool["function"]["name"] == name)?
        .get("function")?
        .get("parameters")?
        .get("properties")?
        .as_object()
}

fn tool_response(id: &str, name: &str, arguments: serde_json::Value) -> Response<Body> {
    json_response(serde_json::json!({
        "id": format!("chatcmpl-{id}"),
        "object": "chat.completion",
        "created": 1,
        "model": MODEL,
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "id": id,
                    "type": "function",
                    "function": {
                        "name": name,
                        "arguments": serde_json::to_string(&arguments).expect("encode tool arguments"),
                    }
                }]
            },
            "finish_reason": "tool_calls"
        }]
    }))
}

fn tool_batch_response(calls: Vec<(String, String, serde_json::Value)>) -> Response<Body> {
    let calls = calls
        .into_iter()
        .enumerate()
        .map(|(index, (id, name, arguments))| {
            serde_json::json!({
                "index": index,
                "id": id,
                "type": "function",
                "function": {
                    "name": name,
                    "arguments": serde_json::to_string(&arguments).expect("encode batch tool arguments"),
                }
            })
        })
        .collect::<Vec<_>>();
    json_response(serde_json::json!({
        "id": "chatcmpl-reviewer-batch",
        "object": "chat.completion",
        "created": 1,
        "model": MODEL,
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": null, "tool_calls": calls},
            "finish_reason": "tool_calls"
        }]
    }))
}

fn json_response(value: serde_json::Value) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&value).expect("encode provider response"),
        ))
        .expect("build provider response")
}

fn seed_repository(repository: &Path) {
    std::fs::create_dir_all(repository.join("src")).expect("create delivery source directory");
    std::fs::create_dir_all(repository.join("tests")).expect("create delivery tests directory");
    std::fs::write(
        repository.join("Cargo.toml"),
        b"[workspace]\n\n[package]\nname = \"delivery_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .expect("write delivery Cargo manifest");
    std::fs::write(
        repository.join("Cargo.lock"),
        b"# This file is automatically @generated by Cargo.\n# It is not intended for manual editing.\nversion = 4\n\n[[package]]\nname = \"delivery_fixture\"\nversion = \"0.1.0\"\n",
    )
    .expect("write delivery Cargo lockfile");
    std::fs::write(repository.join(".gitignore"), b"/target/\n")
        .expect("write delivery Git ignore rules");
    std::fs::write(repository.join("src/lib.rs"), INITIAL_SOURCE).expect("write delivery source");
    std::fs::write(
        repository.join("tests/answer.rs"),
        b"#[test]\nfn answer_is_forty_two() { assert_eq!(delivery_fixture::answer(), 42); }\n",
    )
    .expect("write delivery integration test");
    git_ok(repository, &["init", "--initial-branch=main"]);
    git_ok(repository, &["config", "user.name", "Delivery Fixture"]);
    git_ok(
        repository,
        &["config", "user.email", "delivery-fixture@example.invalid"],
    );
    git_ok(repository, &["add", "--", "."]);
    git_ok(repository, &["commit", "-m", "seed delivery fixture"]);
}

fn canonical(path: &Path) -> CanonicalPath {
    CanonicalPath::try_from_canonical(path.to_path_buf()).expect("canonical delivery fixture path")
}

fn git_ok(repository: &Path, arguments: &[&str]) {
    let output = git_output(repository, arguments);
    assert!(
        output.status.success(),
        "git {arguments:?} failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn git_line(repository: &Path, arguments: &[&str]) -> String {
    let output = git_output(repository, arguments);
    assert!(
        output.status.success(),
        "git {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("Git output is UTF-8")
        .trim()
        .to_owned()
}

fn git_output(repository: &Path, arguments: &[&str]) -> Output {
    Command::new(if cfg!(windows) { "git.exe" } else { "git" })
        .current_dir(repository)
        .args(arguments)
        .output()
        .expect("run delivery fixture Git")
}
