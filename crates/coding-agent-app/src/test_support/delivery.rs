//! Test-feature-only full-production runner composition for offline delivery E2E.
//!
//! Only the provider transport is substituted. The production factory still
//! owns toolchain/Git probing, startup reconciliation, runner construction,
//! repository attachment, and delivery runtime construction.

use std::any::Any;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::State;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{Request, Response, StatusCode};
use axum::routing::post;
use coding_agent_provider::{ChatCompletionsClient, ClientLimits, ProviderConfig};
use coding_agent_runtime::ProcessLivenessScope;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::runner_factory::TestDeliveryTargetBoundary;
use crate::{
    PlatformPaths, PreActorStartupRunnerContext, ProductionStartupRunnerFactory,
    StartupRunnerContext, StartupRunnerFactory, StartupRunnerFactoryError, StartupRunnerSelection,
};

const API_KEY: &str = "offline-process-delivery-key";
const MODEL: &str = "offline-process-delivery-model";
const PACKAGE: &str = "delivery_fixture";
const INTEGRATION_TEST: &str = "answer";
const APPROVED_SOURCE: &str =
    "pub fn fixture_value() -> u32 { 43 }\n// approved offline delivery\n";
const CONFLICT_SOURCE: &str =
    "pub fn fixture_value() -> u32 { 43 }\n// conflict-source offline delivery\n";
const IGNORED_COLLISION_SOURCE: &str = "approved offline ignored collision\n";
const RUNTIME_CONFLICT_SOURCE: &str =
    "pub const SOURCE_SIDE: &str = \"source\";\n\npub const TARGET_SIDE: &str = \"base\";\n";
const RUNTIME_CONFLICT_ATTRIBUTE: &[u8] = b"src/runtime_conflict.rs merge=binary\n";
const BEFORE_ACTUAL_MERGE: &str = "after-last-collision-recheck-before-actual-merge-spawn";
const AFTER_ACTUAL_MERGE: &str = "after-actual-merge-child-before-outcome-proof";
const BEFORE_ACTUAL_ABORT: &str = "before-actual-abort-spawn";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProcessRunnerMode {
    ScriptedFake {},
    ProductionOfflineDelivery {
        repository_path: PathBuf,
        provider_scenario: ProcessDeliveryProviderScenario,
        process_fault: ProcessDeliveryProcessFault,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessDeliveryProviderScenario {
    Approve,
    Conflict,
    IgnoredCollision,
    RuntimeConflict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessDeliveryProcessFault {
    None,
    AuthenticatePreflightFirstChildCleanupFailure,
}

impl ProcessRunnerMode {
    pub(super) fn production_repository(&self) -> Option<&Path> {
        match self {
            Self::ScriptedFake {} => None,
            Self::ProductionOfflineDelivery {
                repository_path, ..
            } => Some(repository_path),
        }
    }

    pub(super) fn offline_delivery(
        &self,
    ) -> Option<(
        &Path,
        ProcessDeliveryProviderScenario,
        ProcessDeliveryProcessFault,
    )> {
        match self {
            Self::ScriptedFake {} => None,
            Self::ProductionOfflineDelivery {
                repository_path,
                provider_scenario,
                process_fault,
            } => Some((repository_path, *provider_scenario, *process_fault)),
        }
    }
}

pub(super) struct ProcessOfflineDeliveryRuntime {
    factory: Arc<dyn StartupRunnerFactory>,
    _server: OfflineProviderServer,
}

impl ProcessOfflineDeliveryRuntime {
    pub(super) fn start(
        repository_path: &Path,
        runtime_path: &Path,
        scenario: ProcessDeliveryProviderScenario,
        process_fault: ProcessDeliveryProcessFault,
    ) -> Result<Self, io::Error> {
        let repository_path = canonicalize_process_repository_path(repository_path)?;
        let server = OfflineProviderServer::start(scenario)?;
        let encoded = serde_json::to_vec(&serde_json::json!({
            "base_url": server.base_url(),
            "model": MODEL,
            "api_key": API_KEY,
            "tool_choice_compatibility": "strict",
        }))
        .map_err(io::Error::other)?;
        let config = ProviderConfig::from_json_allow_loopback_http_for_test(&encoded)
            .map_err(|_| io::Error::other("offline provider configuration invalid"))?;
        let provider = ChatCompletionsClient::new(config, ClientLimits::default())
            .map_err(|_| io::Error::other("offline provider client unavailable"))?;
        Ok(Self {
            factory: Arc::new(OfflineProductionFactory {
                provider,
                runtime_conflict: (scenario == ProcessDeliveryProviderScenario::RuntimeConflict)
                    .then(|| {
                        RuntimeConflictBoundary::new(
                            repository_path.clone(),
                            runtime_path.to_path_buf(),
                        )
                    }),
                process_fault: match process_fault {
                    ProcessDeliveryProcessFault::None => None,
                    ProcessDeliveryProcessFault::AuthenticatePreflightFirstChildCleanupFailure => {
                        Some(
                            crate::runner_factory::TestDeliveryProcessFaultBoundary::authenticate_preflight_first_child_cleanup_failure(
                                repository_path.clone(),
                            ),
                        )
                    }
                },
            }),
            _server: server,
        })
    }

    pub(super) fn factory(&self) -> Arc<dyn StartupRunnerFactory> {
        Arc::clone(&self.factory)
    }
}

fn canonicalize_process_repository_path(repository_path: &Path) -> Result<PathBuf, io::Error> {
    std::fs::canonicalize(repository_path)
}

impl std::fmt::Debug for ProcessOfflineDeliveryRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ProcessOfflineDeliveryRuntime(<offline>)")
    }
}

struct OfflineProductionFactory {
    provider: ChatCompletionsClient,
    runtime_conflict: Option<RuntimeConflictBoundary>,
    process_fault: Option<crate::runner_factory::TestDeliveryProcessFaultBoundary>,
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
        mut context: StartupRunnerContext,
    ) -> Result<StartupRunnerSelection, StartupRunnerFactoryError> {
        if let Some(boundary) = &self.runtime_conflict {
            context = context.with_test_delivery_target_boundary(TestDeliveryTargetBoundary::new(
                boundary.repository_path.clone(),
                boundary.hook(),
            ));
        }
        if let Some(process_fault) = &self.process_fault {
            context = context.with_test_delivery_process_fault(process_fault.clone());
        }
        ProductionStartupRunnerFactory.create(context).await
    }
}

#[derive(Clone)]
struct RuntimeConflictBoundary {
    repository_path: PathBuf,
    attributes_path: PathBuf,
    abort_spawns_path: PathBuf,
    original: Arc<Mutex<Option<OriginalAttributes>>>,
}

enum OriginalAttributes {
    Missing,
    Present(Vec<u8>),
}

impl RuntimeConflictBoundary {
    fn new(repository_path: PathBuf, runtime_path: PathBuf) -> Self {
        Self {
            attributes_path: repository_path.join(".git/info/attributes"),
            abort_spawns_path: runtime_path.join("delivery-runtime-conflict-abort-spawns"),
            repository_path,
            original: Arc::new(Mutex::new(None)),
        }
    }

    fn hook(&self) -> Arc<dyn Fn(&'static str) + Send + Sync + 'static> {
        let boundary = self.clone();
        Arc::new(move |phase| boundary.on_boundary(phase))
    }

    fn on_boundary(&self, phase: &'static str) {
        match phase {
            BEFORE_ACTUAL_MERGE => self.inject_fixed_attributes(),
            AFTER_ACTUAL_MERGE => self.restore_attributes(),
            BEFORE_ACTUAL_ABORT => self.record_abort_spawn(),
            _ => {}
        }
    }

    fn inject_fixed_attributes(&self) {
        let original = match std::fs::read(&self.attributes_path) {
            Ok(bytes) => OriginalAttributes::Present(bytes),
            Err(error) if error.kind() == io::ErrorKind::NotFound => OriginalAttributes::Missing,
            Err(error) => panic!("read fixed runtime-conflict attributes: {error}"),
        };
        let mut guard = self.original.lock().expect("runtime-conflict state lock");
        assert!(
            guard.is_none(),
            "runtime-conflict attributes already injected"
        );
        std::fs::write(&self.attributes_path, RUNTIME_CONFLICT_ATTRIBUTE)
            .expect("inject fixed runtime-conflict attributes");
        *guard = Some(original);
    }

    fn restore_attributes(&self) {
        let original = self
            .original
            .lock()
            .expect("runtime-conflict state lock")
            .take()
            .expect("runtime-conflict attributes were injected");
        match original {
            OriginalAttributes::Missing => match std::fs::remove_file(&self.attributes_path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => panic!("remove fixed runtime-conflict attributes: {error}"),
            },
            OriginalAttributes::Present(bytes) => {
                std::fs::write(&self.attributes_path, bytes)
                    .expect("restore fixed runtime-conflict attributes");
            }
        }
    }

    fn record_abort_spawn(&self) {
        let mut marker = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.abort_spawns_path)
            .expect("open fixed runtime-conflict abort marker");
        marker
            .write_all(b"x")
            .expect("record fixed runtime-conflict abort spawn");
        marker
            .sync_all()
            .expect("sync fixed runtime-conflict abort marker");
    }
}

struct OfflineProviderServer {
    address: std::net::SocketAddr,
    cancellation: CancellationToken,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl OfflineProviderServer {
    fn start(scenario: ProcessDeliveryProviderScenario) -> Result<Self, io::Error> {
        let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        if !address.ip().is_loopback() {
            return Err(io::Error::other("offline provider is not loopback-only"));
        }
        let cancellation = CancellationToken::new();
        let server_cancellation = cancellation.clone();
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let thread = std::thread::Builder::new()
            .name("coding-agent-offline-provider".to_owned())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(_) => {
                        let _ = started_tx.send(false);
                        return;
                    }
                };
                runtime.block_on(async move {
                    let listener = match tokio::net::TcpListener::from_std(listener) {
                        Ok(listener) => listener,
                        Err(_) => {
                            let _ = started_tx.send(false);
                            return;
                        }
                    };
                    let router = Router::new()
                        .route("/v1/chat/completions", post(provider_request))
                        .with_state(ProviderState { scenario });
                    let _ = started_tx.send(true);
                    let _ = axum::serve(listener, router)
                        .with_graceful_shutdown(server_cancellation.cancelled_owned())
                        .await;
                });
            })?;
        if started_rx.recv().unwrap_or(false) {
            Ok(Self {
                address,
                cancellation,
                thread: Mutex::new(Some(thread)),
            })
        } else {
            cancellation.cancel();
            let _ = thread.join();
            Err(io::Error::other("offline provider thread failed to start"))
        }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.address)
    }
}

impl Drop for OfflineProviderServer {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(thread) = self
            .thread
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = thread.join();
        }
    }
}

#[derive(Clone, Copy)]
struct ProviderState {
    scenario: ProcessDeliveryProviderScenario,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WireRole {
    Planner,
    Executor,
    Reviewer,
}

async fn provider_request(
    State(state): State<ProviderState>,
    request: Request<Body>,
) -> Response<Body> {
    assert_eq!(
        request
            .headers()
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
        Some("Bearer offline-process-delivery-key")
    );
    let encoded = to_bytes(request.into_body(), 512 * 1024)
        .await
        .expect("read bounded offline provider request");
    let body: serde_json::Value =
        serde_json::from_slice(&encoded).expect("offline provider request is JSON");
    assert_eq!(body["model"], MODEL);
    assert_eq!(body["parallel_tool_calls"], false);
    assert_eq!(body["stream"], false);
    let role = request_role(&body);
    let role_run = request_role_run(&body, role);
    match role {
        WireRole::Planner => planner_response(),
        WireRole::Executor => executor_response(&body, role_run, state.scenario),
        WireRole::Reviewer => reviewer_response(&body, role_run),
    }
}

fn planner_response() -> Response<Body> {
    tool_response(
        "planner-submit",
        "submit_plan",
        serde_json::json!({
            "summary": "Change the fixture value and validate the current package",
            "steps": [{
                "title": "Implement and validate",
                "description": "Update src/lib.rs and run the package tests.",
                "acceptance_criteria": ["The package tests pass for the exact current diff."]
            }],
            "initial_required_checks": [{
                "kind": "cargo_test",
                "package": PACKAGE,
                "integration_test": INTEGRATION_TEST
            }]
        }),
    )
}

fn executor_response(
    body: &serde_json::Value,
    role_run: u8,
    scenario: ProcessDeliveryProviderScenario,
) -> Response<Body> {
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
        let content = match scenario {
            ProcessDeliveryProviderScenario::Approve
            | ProcessDeliveryProviderScenario::IgnoredCollision
            | ProcessDeliveryProviderScenario::RuntimeConflict => APPROVED_SOURCE,
            ProcessDeliveryProviderScenario::Conflict => CONFLICT_SOURCE,
        };
        return tool_response(
            &format!("executor-{role_run}-replace"),
            "replace_file",
            serde_json::json!({
                "path": "src/lib.rs",
                "expected_sha256": payload["sha256"],
                "content": content,
            }),
        );
    }
    if scenario == ProcessDeliveryProviderScenario::RuntimeConflict
        && !history.iter().any(|(id, _)| id.ends_with("-runtime-read"))
    {
        return tool_response(
            &format!("executor-{role_run}-runtime-read"),
            "read_file",
            serde_json::json!({
                "path": "src/runtime_conflict.rs",
                "start_line": 1,
                "end_line": 12
            }),
        );
    }
    if scenario == ProcessDeliveryProviderScenario::RuntimeConflict
        && !history
            .iter()
            .any(|(id, _)| id.ends_with("-runtime-replace"))
    {
        let payload = latest_success_payload(body);
        return tool_response(
            &format!("executor-{role_run}-runtime-replace"),
            "replace_file",
            serde_json::json!({
                "path": "src/runtime_conflict.rs",
                "expected_sha256": payload["sha256"],
                "content": RUNTIME_CONFLICT_SOURCE,
            }),
        );
    }
    if scenario == ProcessDeliveryProviderScenario::IgnoredCollision
        && !history.iter().any(|(id, _)| id.ends_with("-collision"))
    {
        return tool_response(
            &format!("executor-{role_run}-collision"),
            "replace_file",
            serde_json::json!({
                "path": "collision.txt",
                "expected_sha256": null,
                "content": IGNORED_COLLISION_SOURCE,
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
        serde_json::json!({"summary": "implemented and validated the exact fixture change"}),
    )
}

fn reviewer_response(body: &serde_json::Value, role_run: u8) -> Response<Body> {
    let history = tool_history(body);
    if !history
        .iter()
        .any(|(_, name)| name == "review_diff_manifest")
    {
        let properties = function_properties(body, "review_diff_manifest");
        if properties
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
                            format!("reviewer-{role_run}-reserved-{index}"),
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
            "summary": "reviewed the complete current diff and exact validation evidence",
            "findings": [],
            "add_required_checks": [],
        }),
    )
}

fn request_role(body: &serde_json::Value) -> WireRole {
    let policy = body["messages"][0]["content"]
        .as_str()
        .expect("offline provider role policy");
    if policy.contains("Planner #1") {
        WireRole::Planner
    } else if policy.contains("Executor") {
        WireRole::Executor
    } else if policy.contains("Reviewer") {
        WireRole::Reviewer
    } else {
        panic!("unknown offline provider role")
    }
}

fn request_role_run(body: &serde_json::Value, role: WireRole) -> u8 {
    if role == WireRole::Planner {
        return 1;
    }
    request_handoff(body)["role_run"]
        .as_u64()
        .and_then(|value| u8::try_from(value).ok())
        .expect("bounded offline role run")
}

fn request_handoff(body: &serde_json::Value) -> serde_json::Value {
    serde_json::from_str(
        body["messages"][1]["content"]
            .as_str()
            .expect("offline canonical role handoff"),
    )
    .expect("offline role handoff is JSON")
}

fn tool_history(body: &serde_json::Value) -> Vec<(String, String)> {
    body["messages"]
        .as_array()
        .expect("offline messages array")
        .iter()
        .filter(|message| message["role"] == "assistant")
        .flat_map(|message| {
            message["tool_calls"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|call| {
                    (
                        call["id"].as_str().expect("offline tool id").to_owned(),
                        call["function"]["name"]
                            .as_str()
                            .expect("offline tool name")
                            .to_owned(),
                    )
                })
        })
        .collect()
}

fn latest_success_payload(body: &serde_json::Value) -> serde_json::Value {
    let content = body["messages"]
        .as_array()
        .expect("offline messages array")
        .iter()
        .rev()
        .find(|message| message["role"] == "tool")
        .and_then(|message| message["content"].as_str())
        .expect("latest offline tool result");
    assert!(content.starts_with("[tool_status=succeeded;"));
    let (_, payload) = content.split_once('\n').expect("offline tool envelope");
    serde_json::from_str(payload).expect("offline tool payload")
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
        .unwrap_or_else(|| panic!("offline request does not expose {name}"))
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
                        "arguments": serde_json::to_string(&arguments).expect("encode offline tool arguments"),
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
                    "arguments": serde_json::to_string(&arguments).expect("encode offline batch arguments"),
                }
            })
        })
        .collect::<Vec<_>>();
    json_response(serde_json::json!({
        "id": "chatcmpl-offline-reviewer-batch",
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
            serde_json::to_vec(&value).expect("encode offline provider response"),
        ))
        .expect("build offline provider response")
}

#[cfg(test)]
mod tests {
    use super::{RuntimeConflictBoundary, canonicalize_process_repository_path};

    #[test]
    fn raw_process_repository_path_is_canonicalized_before_test_boundaries() {
        let temporary = tempfile::tempdir().expect("create process-boundary fixture");
        let repository = temporary.path().join("repository");
        let runtime = temporary.path().join("runtime");
        std::fs::create_dir(&repository).expect("create process-boundary repository");
        std::fs::create_dir(&runtime).expect("create process-boundary runtime");

        let canonical =
            std::fs::canonicalize(&repository).expect("canonicalize fixture repository");
        let boundary_path = canonicalize_process_repository_path(&repository.join("."))
            .expect("canonicalize raw process scenario repository");
        assert_eq!(boundary_path, canonical);

        let boundary = RuntimeConflictBoundary::new(boundary_path.clone(), runtime);
        assert_eq!(boundary.repository_path, boundary_path);
        assert_eq!(
            boundary.attributes_path,
            canonical.join(".git/info/attributes")
        );
    }
}
