//! Real child-process fixture for crash/restart delivery assertions.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use coding_agent_app::{
    LegacyV2Seed, PlatformPaths, PrivateFile, ProcessDeliveryProcessFault,
    ProcessDeliveryProviderScenario, ProcessRunnerMode, ProcessStorageSample, ProcessTestConfig,
    RuntimeDescriptor, StoreWriterFaultPoint, StoreWriterFaultSpec, StoreWriterOperationKind,
    TEST_APP_DATA_ENV, TEST_RUNTIME_ENV, TEST_SCENARIO_ENV, VirtualReleaseSignal,
    VirtualReleaseTarget,
};
use coding_agent_store::Store;
use http::header::{ACCEPT, CONTENT_TYPE, HOST, SET_COOKIE};
use http::{HeaderMap, Method, Request, StatusCode};
use http_body_util::{BodyExt as _, Full};
use hyper::body::Bytes;
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use tempfile::TempDir;

// One production live call may legitimately consume eleven minutes. A single
// durable transition can require three such calls before the Store exposes
// another version: preflight does open + authenticate + prepare before binding
// its inputs, while cleanup does open + bind + drive before recording a phase.
// Every later merge/source interval is only open + drive. The no-progress
// window therefore covers the actual three-call maximum plus Store slack.
const LIVE_STAGE_TIMEOUT: Duration = Duration::from_secs(11 * 60);
const DURABLE_NO_PROGRESS_TIMEOUT: Duration =
    Duration::from_secs(3 * LIVE_STAGE_TIMEOUT.as_secs() + 2 * 60);
// The longest scenario supported by this fixture is RuntimeConflict: four
// preflight live calls, then five source/merge/abort phases, each of which opens
// a session and drives one stage. Scripted task execution/startup gets a fixed
// fifteen-minute allowance. This scenario-derived cap prevents bogus durable
// version churn from renewing the shorter deadline indefinitely.
const MAX_SCENARIO_LIVE_CALLS_PER_GENERATION: u64 = 4 + (5 * 2);
const SCRIPTED_TASK_AND_STARTUP_ALLOWANCE: Duration = Duration::from_secs(15 * 60);
const GENERATION_HARD_TIMEOUT: Duration = Duration::from_secs(
    MAX_SCENARIO_LIVE_CALLS_PER_GENERATION * LIVE_STAGE_TIMEOUT.as_secs()
        + SCRIPTED_TASK_AND_STARTUP_ALLOWANCE.as_secs(),
);
const STARTUP_OR_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(12 * 60);
const SHUTDOWN_FENCE_OBSERVATION_TIMEOUT: Duration = Duration::from_secs(12);
const CHILD_REAP_TIMEOUT: Duration = Duration::from_secs(30);
const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const HTTP_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);
const HTTP_SEND_TIMEOUT: Duration = DURABLE_NO_PROGRESS_TIMEOUT;
const HTTP_BODY_TIMEOUT: Duration = Duration::from_secs(15);
const STORE_OBSERVATION_TIMEOUT: Duration = Duration::from_secs(15);
const POLL_INTERVAL: Duration = Duration::from_millis(25);
const PROGRESS_POLL_INTERVAL: Duration = Duration::from_millis(250);
const RUNTIME_CONFLICT_TARGET: &[u8] =
    b"pub const SOURCE_SIDE: &str = \"base\";\n\npub const TARGET_SIDE: &str = \"target\";\n";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ShutdownFenceObservation {
    pub(crate) descriptor_closed: bool,
    pub(crate) listener_closed: bool,
}

pub(crate) async fn observe_shutdown_fence(
    descriptor_path: &Path,
    listener_probe: impl std::future::Future<Output = bool>,
) -> ShutdownFenceObservation {
    let listener_closed = listener_probe.await;
    let descriptor_closed = !descriptor_path.exists();
    ShutdownFenceObservation {
        descriptor_closed,
        listener_closed,
    }
}

pub struct ProcessDeliveryFixture {
    temporary: Option<TempDir>,
    pub root: PathBuf,
    pub data_dir: PathBuf,
    pub runtime_dir: PathBuf,
    pub repository_path: PathBuf,
    pub database_path: PathBuf,
    pub descriptor_path: PathBuf,
    pub base_head: String,
    provider_scenario: ProcessDeliveryProviderScenario,
    process_fault: ProcessDeliveryProcessFault,
    binary: PathBuf,
    child_log_path: PathBuf,
    generation: u32,
    child: Option<ProcessChildGuard>,
    generation_started_at: Option<Instant>,
    current_pause_reached: Option<PathBuf>,
}

struct ProcessChildGuard {
    child: Child,
    cleanup_attempted: bool,
}

struct DurableProgressDeadline {
    hard_deadline: Instant,
    no_progress_deadline: Instant,
    last_progress: Option<String>,
}

#[derive(Clone)]
pub struct ProcessSession {
    client: ProcessHttpClient,
    mutation_origin: String,
    cookie: String,
    csrf: String,
    hard_deadline: tokio::time::Instant,
}

#[derive(Clone)]
struct ProcessHttpClient {
    port: u16,
    origin: String,
}

struct ProcessHttpResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Vec<u8>,
}

struct HttpConnectionGuard(tokio::task::JoinHandle<()>);

impl Drop for HttpConnectionGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl ProcessChildGuard {
    fn new(child: Child) -> Self {
        Self {
            child,
            cleanup_attempted: false,
        }
    }

    fn id(&self) -> u32 {
        self.child.id()
    }

    fn try_wait(&mut self) -> std::io::Result<Option<ExitStatus>> {
        self.child.try_wait()
    }

    fn kill(&mut self) -> std::io::Result<()> {
        self.child.kill()
    }

    fn kill_and_reap_blocking(&mut self) -> bool {
        self.cleanup_attempted = true;
        match self.child.try_wait() {
            Ok(Some(_)) => return true,
            Ok(None) => {}
            Err(_) => return false,
        }
        if self.child.kill().is_err() {
            return matches!(self.child.try_wait(), Ok(Some(_)));
        }
        let deadline = Instant::now() + CHILD_REAP_TIMEOUT;
        while Instant::now() < deadline {
            match self.child.try_wait() {
                Ok(Some(_)) => return true,
                Ok(None) => std::thread::sleep(POLL_INTERVAL),
                Err(_) => return false,
            }
        }
        false
    }
}

impl Drop for ProcessChildGuard {
    fn drop(&mut self) {
        if !self.cleanup_attempted {
            let _ = self.kill_and_reap_blocking();
        }
    }
}

impl DurableProgressDeadline {
    fn new(hard_deadline: Instant) -> Self {
        Self {
            hard_deadline,
            no_progress_deadline: Instant::now() + DURABLE_NO_PROGRESS_TIMEOUT,
            last_progress: None,
        }
    }

    fn observe(&mut self, progress: String) {
        if self.last_progress.as_ref() != Some(&progress) {
            self.last_progress = Some(progress);
            self.no_progress_deadline = Instant::now() + DURABLE_NO_PROGRESS_TIMEOUT;
        }
    }

    fn assert_open(&self, label: &str, diagnostic: &str) {
        let now = Instant::now();
        assert!(
            now < self.no_progress_deadline,
            "process delivery made no durable progress before {label}; last_progress={:?}; diagnostic={diagnostic}",
            self.last_progress
        );
        assert!(
            now < self.hard_deadline,
            "process delivery generation exceeded its hard cap before {label}; last_progress={:?}; diagnostic={diagnostic}",
            self.last_progress
        );
    }
}

fn task_progress_snapshot(detail: &Value) -> String {
    serde_json::to_string(&json!({
        "task": detail["task"],
        "activity": detail["activity"],
        "diff": detail["diff"],
        "tests": detail["tests"],
        "reviews": detail["reviews"],
    }))
    .expect("serialize process task durable progress")
}

fn delivery_progress_snapshot(delivery: &Value) -> String {
    serde_json::to_string(&json!({
        "latest_merge": {
            "operation_id": delivery["latest_merge"]["operation_id"],
            "state": delivery["latest_merge"]["state"],
            "version": delivery["latest_merge"]["version"],
        },
        "source": {
            "state": delivery["source"]["state"],
            "version": delivery["source"]["version"],
        },
        "worktree": {
            "state": delivery["disposition"]["worktree"]["state"],
            "version": delivery["disposition"]["worktree"]["version"],
        },
        "branch": {
            "state": delivery["disposition"]["branch"]["state"],
            "version": delivery["disposition"]["branch"]["version"],
        },
    }))
    .expect("serialize process delivery durable progress")
}

impl ProcessDeliveryFixture {
    pub fn new() -> Self {
        Self::new_with_scenario(ProcessDeliveryProviderScenario::Approve)
    }

    pub fn new_with_scenario(provider_scenario: ProcessDeliveryProviderScenario) -> Self {
        Self::new_with_scenario_and_fault(provider_scenario, ProcessDeliveryProcessFault::None)
    }

    pub fn new_with_scenario_and_fault(
        provider_scenario: ProcessDeliveryProviderScenario,
        process_fault: ProcessDeliveryProcessFault,
    ) -> Self {
        let temporary = tempfile::tempdir().expect("create process delivery root");
        let root = temporary.path().canonicalize().unwrap();
        let data_dir = root.join("data");
        let runtime_dir = root.join("runtime");
        let repository_path = root.join("repository");
        PlatformPaths::new(&data_dir, &runtime_dir)
            .prepare()
            .expect("prepare private process delivery roots");
        seed_process_repository(&repository_path);
        let repository_path = repository_path
            .canonicalize()
            .expect("canonical process delivery repository");
        let base_head = git_line(&repository_path, &["rev-parse", "HEAD"]);
        let database_path = data_dir.join("coding-agent.sqlite3");
        let descriptor_path = runtime_dir.join("instance.json");
        let child_log_path = root.join("child.log");
        Self {
            temporary: Some(temporary),
            root,
            data_dir,
            runtime_dir,
            repository_path,
            database_path,
            descriptor_path,
            base_head,
            provider_scenario,
            process_fault,
            binary: PathBuf::from(env!("CARGO_BIN_EXE_coding-agent-app")),
            child_log_path,
            generation: 0,
            child: None,
            generation_started_at: None,
            current_pause_reached: None,
        }
    }

    pub async fn start_ready(&mut self, pause: Option<StoreWriterOperationKind>) -> ProcessSession {
        self.spawn(pause);
        let descriptor = self.wait_for_descriptor().await;
        let session = ProcessSession::exchange(
            &descriptor,
            tokio::time::Instant::from_std(self.generation_hard_deadline()),
        )
        .await;
        self.wait_for_service_ready(&session).await;
        session
    }

    pub async fn start_until_store_pause(&mut self, operation: StoreWriterOperationKind) {
        self.spawn(Some(operation));
        self.wait_for_store_pause().await;
    }

    pub async fn wait_for_store_pause(&mut self) {
        let reached = self
            .current_pause_reached
            .clone()
            .expect("the current generation has a StoreWriter pause");
        self.wait_for_file(&reached, "StoreWriter durable pause")
            .await;
    }

    pub async fn hard_kill(&mut self) {
        let child = self
            .child
            .as_mut()
            .expect("a process generation is running");
        child.kill().expect("hard-kill process delivery primary");
        let status = tokio::time::timeout(CHILD_REAP_TIMEOUT, async {
            loop {
                if let Some(status) = child
                    .try_wait()
                    .expect("poll hard-killed process delivery primary")
                {
                    return status;
                }
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        })
        .await
        .expect("hard-killed process delivery primary exits");
        assert!(
            !status.success(),
            "hard-killed process delivery primary exited successfully"
        );
        drop(self.child.take());
        self.generation_started_at = None;
        self.current_pause_reached = None;
    }

    pub async fn shutdown(&mut self, session: &ProcessSession) {
        let response = session
            .post_json("/api/app/quit", &json!({}))
            .await
            .expect("send protected process quit");
        assert_eq!(response.status, StatusCode::ACCEPTED);
        let child = self
            .child
            .as_mut()
            .expect("final process generation is running");
        let status = tokio::time::timeout(STARTUP_OR_SHUTDOWN_TIMEOUT, async {
            loop {
                if let Some(status) = child
                    .try_wait()
                    .expect("poll final process delivery primary")
                {
                    return status;
                }
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        })
        .await
        .expect("final process delivery primary exits");
        assert!(
            status.success(),
            "final process shutdown degraded: {status}"
        );
        drop(self.child.take());
        self.generation_started_at = None;
        tokio::time::timeout(STARTUP_OR_SHUTDOWN_TIMEOUT, async {
            while self.descriptor_path.exists() {
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        })
        .await
        .expect("clean shutdown removes the runtime descriptor");
    }

    pub fn clear_process_fault(&mut self) {
        assert!(
            self.child.is_none(),
            "the process fault changes only between primary generations"
        );
        self.process_fault = ProcessDeliveryProcessFault::None;
    }

    pub async fn post_preflight(
        &self,
        session: &ProcessSession,
        task_id: &str,
    ) -> (StatusCode, Value) {
        let delivery = session
            .get_json(&format!("/api/tasks/{task_id}/delivery"))
            .await
            .expect("load process preflight target");
        let target = &delivery["target"];
        let response = session
            .post_json(
                &format!("/api/tasks/{task_id}/merge/preflight"),
                &json!({
                    "client_request_id": uuid::Uuid::new_v4(),
                    "target_branch": target["branch"],
                    "expected_target_head": target["head"],
                }),
            )
            .await
            .expect("submit raw real process preflight");
        let body = serde_json::from_slice(&response.body).unwrap_or_else(|error| {
            panic!(
                "decode real process preflight response: {error}; body={}",
                String::from_utf8_lossy(&response.body)
            )
        });
        (response.status, body)
    }

    pub async fn delivery_projection(&self, session: &ProcessSession, task_id: &str) -> Value {
        session
            .get_json(&format!("/api/tasks/{task_id}/delivery"))
            .await
            .expect("load process delivery projection")
    }

    pub async fn request_quit(&self, session: &ProcessSession) {
        let response = session
            .post_json("/api/app/quit", &json!({}))
            .await
            .expect("send protected process quit");
        assert_eq!(response.status, StatusCode::ACCEPTED);
    }

    pub async fn wait_for_shutdown_fence(&mut self, session: &ProcessSession) {
        let deadline = Instant::now() + SHUTDOWN_FENCE_OBSERVATION_TIMEOUT;
        loop {
            let ShutdownFenceObservation {
                descriptor_closed,
                listener_closed,
            } = observe_shutdown_fence(&self.descriptor_path, async {
                tokio::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, session.client.port))
                    .await
                    .is_err()
            })
            .await;
            if descriptor_closed && listener_closed {
                break;
            }
            self.assert_child_running("retain its primary shutdown safety fences");
            assert!(
                Instant::now() < deadline,
                "shutdown did not close descriptor/listener within the product budget plus observation slack; descriptor_closed={descriptor_closed}; listener_closed={listener_closed}; child_log={}",
                self.child_log()
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }
        self.assert_child_running("remain alive with fail-closed delivery ownership");
    }

    pub fn child_pid(&self) -> u32 {
        self.child
            .as_ref()
            .expect("a process generation is running")
            .id()
    }

    pub fn instance_lock_path(&self) -> PathBuf {
        self.runtime_dir.join("instance.lock")
    }

    pub async fn create_approved_task(&self, session: &ProcessSession) -> (String, String) {
        let bootstrap = session
            .get_json("/api/bootstrap")
            .await
            .expect("load process bootstrap");
        let repository_id = bootstrap["repositories"][0]["id"]
            .as_str()
            .expect("bootstrap contains the seeded repository")
            .to_owned();
        let prompt = "approve the exact process crash delivery fixture";
        let created = session
            .post_json(
                "/api/tasks",
                &json!({
                    "client_request_id": uuid::Uuid::new_v4(),
                    "repository_id": repository_id,
                    "prompt": prompt,
                }),
            )
            .await
            .expect("create process delivery task");
        assert!(
            matches!(created.status, StatusCode::OK | StatusCode::CREATED),
            "create process delivery task returned {}: {}",
            created.status,
            String::from_utf8_lossy(&created.body)
        );
        let task_id = created.json()["id"]
            .as_str()
            .expect("created task has an ID")
            .to_owned();
        let mut progress = DurableProgressDeadline::new(self.generation_hard_deadline());
        loop {
            let detail = session
                .get_json(&format!("/api/tasks/{task_id}"))
                .await
                .expect("poll process delivery task");
            progress.observe(task_progress_snapshot(&detail));
            let task = &detail["task"];
            let status = task["status"].as_str().unwrap_or("invalid");
            if status == "completed" && task["delivery_readiness"] == "review_approved" {
                break;
            }
            assert!(
                !matches!(status, "failed" | "cancelled" | "interrupted"),
                "process delivery task became terminal: {detail}; child_log={}",
                self.child_log()
            );
            progress.assert_open("approved task completion", &detail.to_string());
            tokio::time::sleep(POLL_INTERVAL).await;
        }
        (task_id, repository_id)
    }

    pub async fn preflight_ready(&self, session: &ProcessSession, task_id: &str) -> Value {
        let delivery = session
            .get_json(&format!("/api/tasks/{task_id}/delivery"))
            .await
            .expect("load preflight target");
        let target = &delivery["target"];
        let response = session
            .post_json(
                &format!("/api/tasks/{task_id}/merge/preflight"),
                &json!({
                    "client_request_id": uuid::Uuid::new_v4(),
                    "target_branch": target["branch"],
                    "expected_target_head": target["head"],
                }),
            )
            .await
            .expect("submit real process preflight");
        assert!(matches!(
            response.status,
            StatusCode::OK | StatusCode::CREATED
        ));
        self.wait_delivery(session, task_id, |delivery| {
            (delivery["latest_merge"]["state"] == "preflight_ready").then_some(())
        })
        .await;
        session
            .get_json(&format!("/api/tasks/{task_id}/delivery"))
            .await
            .expect("reload ready process preflight")
    }

    pub async fn accept_merge(&self, session: &ProcessSession, task_id: &str, ready: &Value) {
        let operation = &ready["latest_merge"];
        let evidence = &ready["evidence"];
        let response = session
            .post_json(
                &format!("/api/tasks/{task_id}/merge"),
                &json!({
                    "client_request_id": uuid::Uuid::new_v4(),
                    "preflight_operation_id": operation["operation_id"],
                    "expected_operation_version": operation["version"],
                    "expected_review_generation": evidence["review_generation"],
                    "expected_workspace_fingerprint": evidence["workspace_fingerprint"],
                    "target_branch": operation["target_branch"],
                    "expected_target_head": operation["target_head"],
                }),
            )
            .await
            .expect("accept real process merge");
        assert!(matches!(
            response.status,
            StatusCode::OK | StatusCode::ACCEPTED
        ));
    }

    pub async fn wait_merge_state(
        &self,
        session: &ProcessSession,
        task_id: &str,
        state: &str,
    ) -> Value {
        self.wait_delivery(session, task_id, |delivery| {
            (delivery["latest_merge"]["state"] == state).then_some(delivery.clone())
        })
        .await
    }

    pub async fn wait_worktree_state(
        &self,
        session: &ProcessSession,
        task_id: &str,
        state: &str,
    ) -> Value {
        self.wait_delivery(session, task_id, |delivery| {
            (delivery["disposition"]["worktree"]["state"] == state).then_some(delivery.clone())
        })
        .await
    }

    pub async fn wait_branch_state(
        &self,
        session: &ProcessSession,
        task_id: &str,
        state: &str,
    ) -> Value {
        self.wait_delivery(session, task_id, |delivery| {
            (delivery["disposition"]["branch"]["state"] == state).then_some(delivery.clone())
        })
        .await
    }

    pub async fn worktree_cleanup_body(&self, session: &ProcessSession, task_id: &str) -> Value {
        let delivery = session
            .get_json(&format!("/api/tasks/{task_id}/delivery"))
            .await
            .expect("load worktree cleanup projection");
        json!({
            "client_request_id": uuid::Uuid::new_v4(),
            "expected_disposition_version": delivery["disposition"]["worktree"]["version"],
            "expected_merge_operation_id": delivery["disposition"]["merged_operation_id"],
            "expected_source_ref": delivery["disposition"]["source_ref"],
            "expected_source_oid": delivery["disposition"]["source_oid"],
        })
    }

    pub async fn post_worktree_cleanup(
        &self,
        session: &ProcessSession,
        task_id: &str,
        body: &Value,
    ) -> (StatusCode, Value) {
        let response = session
            .post_json(&format!("/api/tasks/{task_id}/cleanup/worktree"), body)
            .await
            .expect("submit real process worktree cleanup");
        let json = serde_json::from_slice(&response.body).unwrap_or_else(|error| {
            panic!(
                "decode real process worktree cleanup response: {error}; body={}",
                String::from_utf8_lossy(&response.body)
            )
        });
        (response.status, json)
    }

    pub async fn branch_cleanup_body(&self, session: &ProcessSession, task_id: &str) -> Value {
        let delivery = session
            .get_json(&format!("/api/tasks/{task_id}/delivery"))
            .await
            .expect("load branch cleanup projection");
        json!({
            "client_request_id": uuid::Uuid::new_v4(),
            "expected_disposition_version": delivery["disposition"]["branch"]["version"],
            "expected_merge_operation_id": delivery["disposition"]["merged_operation_id"],
            "expected_source_ref": delivery["disposition"]["source_ref"],
            "expected_source_oid": delivery["disposition"]["source_oid"],
            "target_branch": delivery["target"]["branch"],
            "target_head": delivery["target"]["head"],
        })
    }

    pub fn spawn_mutation(
        &self,
        session: ProcessSession,
        path: String,
        body: Value,
    ) -> tokio::task::JoinHandle<Result<StatusCode, String>> {
        tokio::spawn(async move {
            let response = session.post_json(&path, &body).await?;
            if matches!(response.status, StatusCode::OK | StatusCode::ACCEPTED) {
                Ok(response.status)
            } else {
                Err(format!(
                    "POST {path} returned {}: {}",
                    response.status,
                    String::from_utf8_lossy(&response.body)
                ))
            }
        })
    }

    pub async fn wait_for_store_pause_or_mutation(
        &mut self,
        mut request: tokio::task::JoinHandle<Result<StatusCode, String>>,
    ) -> Result<(), String> {
        tokio::select! {
            result = &mut request => {
                let status = result
                    .map_err(|error| format!("process mutation client task was not joinable: {error}"))??;
                assert!(
                    matches!(status, StatusCode::OK | StatusCode::ACCEPTED),
                    "process mutation returned unexpected status {status}"
                );
                self.wait_for_store_pause().await;
            }
            () = self.wait_for_store_pause() => {
                request.abort();
                let _ = request.await;
            }
        }
        Ok(())
    }

    pub async fn diagnostic_snapshot(&self, session: &ProcessSession, task_id: &str) -> Value {
        let delivery = session
            .get_json(&format!("/api/tasks/{task_id}/delivery"))
            .await
            .unwrap_or_else(|error| json!({"error": error}));
        let task = session
            .get_json(&format!("/api/tasks/{task_id}"))
            .await
            .unwrap_or_else(|error| json!({"error": error}));
        let store = Store::open(&self.database_path)
            .await
            .expect("open killed-process diagnostic store");
        let source = diagnostic_json_row(
            &store,
            "SELECT json_object(\
                'state', state, 'version', version, \
                'source_ref', artifact_source_branch, \
                'source_oid', expected_source_commit_oid, \
                'accepted_operation_id', origin_accepted_operation_id, \
                'failure_code', failure_code, \
                'reconciliation_key', required_merge_reconciliation_key\
             ) FROM task_delivery_sources WHERE task_id = ?",
            task_id,
        )
        .await;
        let merge = diagnostic_json_row(
            &store,
            "SELECT json_object(\
                'operation_id', operation_id, 'state', state, 'version', version, \
                'source_oid', source_commit_oid, \
                'target_branch', target_branch, 'target_head', expected_target_head, \
                'expected_merge_oid', expected_merge_commit_oid, \
                'merged_disposition_task_id', merged_disposition_task_id, \
                'abort_child_receipt_id', abort_child_receipt_id\
             ) FROM task_merge_operations WHERE task_id = ? \
             ORDER BY created_at DESC LIMIT 1",
            task_id,
        )
        .await;
        let disposition = diagnostic_json_row(
            &store,
            "SELECT json_object(\
                'merged_operation_id', merged_operation_id, \
                'source_oid', source_commit_oid, \
                'worktree_state', worktree_state, 'worktree_version', worktree_version, \
                'worktree_failure_code', worktree_failure_code, \
                'branch_state', branch_state, 'branch_version', branch_version, \
                'branch_failure_code', branch_failure_code\
             ) FROM task_artifact_dispositions WHERE task_id = ?",
            task_id,
        )
        .await;
        let artifact = diagnostic_json_row(
            &store,
            "SELECT json_object(\
                'state', state, 'attempt', attempt, 'branch_name', branch_name, \
                'base_commit', base_commit, 'worktree_path', worktree_path\
             ) FROM task_attempt_artifacts WHERE task_id = ?",
            task_id,
        )
        .await;
        let cleanup = diagnostic_json_rows(
            &store,
            "SELECT json_object(\
                'operation_id', operation_id, 'kind', kind, 'state', state, \
                'version', version, 'failure_code', failure_code\
             ) FROM task_cleanup_operations WHERE task_id = ? ORDER BY created_at",
            task_id,
        )
        .await;
        let transitions = diagnostic_json_rows(
            &store,
            "SELECT json_object(\
                'entity_kind', entity_kind, 'from_state', from_state, \
                'to_state', to_state, 'entity_version', entity_version\
             ) FROM task_delivery_operation_transitions \
             WHERE entity_id = ? OR entity_id IN (\
                SELECT operation_id FROM task_merge_operations WHERE task_id = ?\
             ) OR entity_id IN (\
                SELECT operation_id FROM task_cleanup_operations WHERE task_id = ?\
             ) ORDER BY transition_id",
            task_id,
        )
        .await;
        store.close().await;

        let source_worktree = self.source_worktree_path(task_id).await;
        let source_ref = self.source_ref(task_id).await;
        let source_ref_oid = self.git_ref(&source_ref);
        let source_status = git_output(&source_worktree, &["status", "--porcelain=v2", "-z"]);
        let source_ignored_untracked = git_output(
            &source_worktree,
            &[
                "ls-files",
                "--others",
                "--ignored",
                "--exclude-standard",
                "--directory",
                "-z",
                "--",
            ],
        );
        let source_head = git_output(&source_worktree, &["rev-parse", "HEAD"]);
        let source_tree = git_output(&source_worktree, &["rev-parse", "HEAD^{tree}"]);
        let worktree_list = git_output(
            &self.repository_path,
            &["worktree", "list", "--porcelain", "-z"],
        );
        let source_locked = git_output(&source_worktree, &["rev-parse", "--git-path", "locked"]);
        let source_locked_path = String::from_utf8_lossy(&source_locked.stdout)
            .trim()
            .to_owned();
        let source_locked_path = PathBuf::from(source_locked_path);
        let source_locked_path = if source_locked_path.is_absolute() {
            source_locked_path
        } else {
            source_worktree.join(source_locked_path)
        };
        let source_locked = source_locked_path.is_file();
        let source_locked_path = source_locked_path.display().to_string();
        let target_status = self.git_status();
        json!({
            "task": task,
            "delivery": delivery,
            "rows": {
                "source": source,
                "merge": merge,
                "disposition": disposition,
                "artifact": artifact,
                "cleanup": cleanup,
                "transitions": transitions,
            },
            "git": {
                "target_head": self.git_line(&["rev-parse", "HEAD"]),
                "target_status": String::from_utf8_lossy(&target_status),
                "source_head": String::from_utf8_lossy(&source_head.stdout).trim(),
                "source_tree": String::from_utf8_lossy(&source_tree.stdout).trim(),
                "source_ref": source_ref,
                "source_ref_oid": source_ref_oid,
                "source_status": String::from_utf8_lossy(&source_status.stdout),
                "source_ignored_untracked": String::from_utf8_lossy(&source_ignored_untracked.stdout),
                "source_locked_path": source_locked_path,
                "source_locked": source_locked,
                "worktree_list": String::from_utf8_lossy(&worktree_list.stdout),
            },
            "child_log": self.child_log(),
        })
    }

    pub async fn persisted_source_state(&self, task_id: &str) -> String {
        self.scalar_text(
            "SELECT state FROM task_delivery_sources WHERE task_id = ?",
            task_id,
        )
        .await
    }

    pub async fn persisted_source_state_optional(&self, task_id: &str) -> Option<String> {
        let store = Store::open(&self.database_path)
            .await
            .expect("open killed-process optional source store");
        let value = sqlx::query_scalar::<_, String>(
            "SELECT state FROM task_delivery_sources WHERE task_id = ?",
        )
        .bind(task_id)
        .fetch_optional(store.pool())
        .await
        .expect("load optional persisted process delivery source");
        store.close().await;
        value
    }

    pub async fn source_ignored_untracked(&self, task_id: &str) -> Vec<u8> {
        let source_worktree = self.source_worktree_path(task_id).await;
        let output = git_output(
            &source_worktree,
            &[
                "ls-files",
                "--others",
                "--ignored",
                "--exclude-standard",
                "--directory",
                "-z",
                "--",
            ],
        );
        assert!(
            output.status.success(),
            "inspect source ignored-untracked paths failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        output.stdout
    }

    /// Removes only the fixed Cargo output directory created by this fixture's
    /// offline Executor validation. Production cleanup never calls this: the
    /// test makes the source genuinely clean before exercising an accepted
    /// worktree cleanup.
    pub async fn clean_fixture_cargo_outputs(&self, task_id: &str) {
        let source_worktree = self.source_worktree_path(task_id).await;
        let private_root = self
            .root
            .canonicalize()
            .expect("canonical fixture private root before cleaning Cargo output");
        let source_identity = source_worktree
            .canonicalize()
            .expect("canonical fixture source worktree before cleaning Cargo output");
        assert!(
            source_identity.starts_with(&private_root),
            "fixture source worktree must stay inside its private root"
        );
        let cargo_target = source_worktree.join("target");
        if cargo_target.exists() {
            let target_metadata = std::fs::symlink_metadata(&cargo_target)
                .expect("inspect fixed fixture Cargo output directory");
            assert!(target_metadata.is_dir());
            assert!(!target_metadata.file_type().is_symlink());
            let target_identity = cargo_target
                .canonicalize()
                .expect("canonical fixed fixture Cargo output directory");
            assert_eq!(
                target_identity.parent(),
                Some(source_identity.as_path()),
                "fixture cleanup may remove only its direct target child"
            );
            std::fs::remove_dir_all(&cargo_target)
                .expect("remove fixed fixture Cargo output directory");
        }
        assert!(!cargo_target.exists());
        assert!(
            self.source_ignored_untracked(task_id).await.is_empty(),
            "fixture source has no ignored output after explicit test cleanup"
        );
        let status = git_output(
            &source_worktree,
            &["status", "--porcelain=v2", "-z", "--untracked-files=all"],
        );
        assert!(status.status.success());
        assert!(
            status.stdout.is_empty(),
            "fixture source is truly clean before accepted cleanup"
        );
    }

    pub fn assert_ignored_cargo_output_was_the_dirty_predicate(&self) {
        assert!(
            self.child_log()
                .contains("predicate=source_ignored_untracked_nonempty"),
            "production cleanup must reject the fixture at the exact ignored-untracked predicate"
        );
    }

    pub async fn persisted_merge_state(&self, task_id: &str) -> String {
        self.scalar_text(
            "SELECT state FROM task_merge_operations WHERE task_id = ? ORDER BY created_at DESC LIMIT 1",
            task_id,
        )
        .await
    }

    pub async fn persisted_abort_child_receipt_id(&self, task_id: &str) -> Option<String> {
        let store = Store::open(&self.database_path)
            .await
            .expect("open killed-process abort receipt store");
        let value = sqlx::query_scalar::<_, String>(
            "SELECT abort_child_receipt_id FROM task_merge_operations WHERE task_id = ? ORDER BY created_at DESC LIMIT 1",
        )
        .bind(task_id)
        .fetch_optional(store.pool())
        .await
        .expect("load persisted process abort child receipt");
        store.close().await;
        value
    }

    pub async fn persisted_cleanup_state(&self, task_id: &str, kind: &str) -> String {
        let store = Store::open(&self.database_path)
            .await
            .expect("open killed-process cleanup store");
        let value = sqlx::query_scalar::<_, String>(
            "SELECT state FROM task_cleanup_operations WHERE task_id = ? AND kind = ? ORDER BY created_at DESC LIMIT 1",
        )
        .bind(task_id)
        .bind(kind)
        .fetch_one(store.pool())
        .await
        .expect("load persisted cleanup state");
        store.close().await;
        value
    }

    pub async fn persisted_cleanup_state_optional(
        &self,
        task_id: &str,
        kind: &str,
    ) -> Option<String> {
        let store = Store::open(&self.database_path)
            .await
            .expect("open killed-process optional cleanup store");
        let value = sqlx::query_scalar::<_, String>(
            "SELECT state FROM task_cleanup_operations WHERE task_id = ? AND kind = ? ORDER BY created_at DESC LIMIT 1",
        )
        .bind(task_id)
        .bind(kind)
        .fetch_optional(store.pool())
        .await
        .expect("load optional persisted cleanup state");
        store.close().await;
        value
    }

    pub async fn receipt_count(&self, task_id: &str, kind: &str) -> i64 {
        let store = Store::open(&self.database_path)
            .await
            .expect("open killed-process receipt store");
        let value = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task_delivery_command_receipts WHERE task_id = ? AND command_kind = ?",
        )
        .bind(task_id)
        .bind(kind)
        .fetch_one(store.pool())
        .await
        .expect("count exact process delivery receipts");
        store.close().await;
        value
    }

    pub async fn transition_count(&self, task_id: &str, entity: &str, state: &str) -> i64 {
        let store = Store::open(&self.database_path)
            .await
            .expect("open killed-process transition store");
        let value = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task_delivery_operation_transitions t \
             WHERE t.entity_kind = ? AND t.to_state = ? AND ( \
                 (t.entity_kind = 'delivery_source' AND t.entity_id = ?) OR \
                 (t.entity_kind = 'merge_operation' AND t.entity_id IN (SELECT operation_id FROM task_merge_operations WHERE task_id = ?)) OR \
                 (t.entity_kind = 'cleanup_operation' AND t.entity_id IN (SELECT operation_id FROM task_cleanup_operations WHERE task_id = ?)) \
             )",
        )
        .bind(entity)
        .bind(state)
        .bind(task_id)
        .bind(task_id)
        .bind(task_id)
        .fetch_one(store.pool())
        .await
        .expect("count exact process delivery transitions");
        store.close().await;
        value
    }

    pub async fn source_worktree_path(&self, task_id: &str) -> PathBuf {
        let store = Store::open(&self.database_path)
            .await
            .expect("open killed-process artifact store");
        let value = sqlx::query_scalar::<_, String>(
            "SELECT worktree_path FROM task_attempt_artifacts WHERE task_id = ?",
        )
        .bind(task_id)
        .fetch_one(store.pool())
        .await
        .expect("load real source worktree path");
        store.close().await;
        PathBuf::from(value)
    }

    pub async fn source_ref(&self, task_id: &str) -> String {
        let branch_name = self
            .scalar_text(
                "SELECT branch_name FROM task_attempt_artifacts WHERE task_id = ?",
                task_id,
            )
            .await;
        format!("refs/heads/{branch_name}")
    }

    pub fn git_line(&self, arguments: &[&str]) -> String {
        git_line(&self.repository_path, arguments)
    }

    pub fn git_status(&self) -> Vec<u8> {
        git_output(&self.repository_path, &["status", "--porcelain=v1", "-z"]).stdout
    }

    pub fn commit_runtime_conflict_target(&self) -> String {
        std::fs::write(
            self.repository_path.join("src/runtime_conflict.rs"),
            RUNTIME_CONFLICT_TARGET,
        )
        .expect("write runtime-conflict target side");
        git_ok(
            &self.repository_path,
            &["add", "--", "src/runtime_conflict.rs"],
        );
        git_ok(
            &self.repository_path,
            &["commit", "-m", "advance runtime-conflict target side"],
        );
        self.git_line(&["rev-parse", "HEAD"])
    }

    pub fn assert_runtime_conflict_target_restored(&self) {
        assert_eq!(
            std::fs::read(self.repository_path.join("src/runtime_conflict.rs"))
                .expect("read restored runtime-conflict target"),
            RUNTIME_CONFLICT_TARGET
        );
        assert!(
            !self.repository_path.join(".git/MERGE_HEAD").exists(),
            "the exact abort removes MERGE_HEAD"
        );
        assert!(
            !self.repository_path.join(".git/info/attributes").exists(),
            "the fixed test attribute is restored before durable AbortPending"
        );
    }

    pub fn assert_runtime_conflict_attribute_restored(&self) {
        assert!(
            !self.repository_path.join(".git/info/attributes").exists(),
            "the fixed test attribute must be absent after the actual merge child"
        );
    }

    pub fn abort_spawn_count(&self) -> usize {
        match std::fs::read(
            self.runtime_dir
                .join("delivery-runtime-conflict-abort-spawns"),
        ) {
            Ok(bytes) => bytes.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => panic!("read runtime-conflict abort marker: {error}"),
        }
    }

    pub fn git_ref(&self, reference: &str) -> Option<String> {
        let existence = git_output(
            &self.repository_path,
            &["show-ref", "--quiet", "--verify", reference],
        );
        if !existence.status.success() {
            if existence.status.code() == Some(1) {
                return None;
            }
            panic!(
                "Git ref existence query failed with status {:?}: {}",
                existence.status.code(),
                String::from_utf8_lossy(&existence.stderr)
            );
        }

        let output = git_output(
            &self.repository_path,
            &["show-ref", "--verify", "--hash", reference],
        );
        assert!(
            output.status.success(),
            "Git ref hash query failed with status {:?}: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        );
        let oid = String::from_utf8(output.stdout)
            .expect("Git ref output is UTF-8")
            .trim()
            .to_owned();
        assert!(!oid.is_empty(), "Git ref hash query returned an empty OID");
        Some(oid)
    }

    pub fn finish(mut self) -> PathBuf {
        assert!(
            self.child.is_none(),
            "finish requires no live child process"
        );
        assert!(
            self.generation_started_at.is_none(),
            "finish requires no active process generation"
        );
        let sentinel_directory = self.runtime_dir.join("process-liveness");
        if sentinel_directory.is_dir() {
            let sentinels = std::fs::read_dir(&sentinel_directory)
                .expect("inspect final process-liveness sentinel directory")
                .map(|entry| {
                    entry
                        .expect("read final process-liveness sentinel entry")
                        .file_name()
                })
                .collect::<Vec<_>>();
            assert!(
                sentinels.is_empty(),
                "clean process delivery shutdown left process-liveness sentinels: {sentinels:?}"
            );
        }
        let path = self.root.clone();
        self.temporary
            .take()
            .expect("process temporary root exists")
            .close()
            .expect("remove the complete process delivery temporary root");
        assert!(
            !path.exists(),
            "process delivery temporary root leaked after close"
        );
        path
    }

    fn spawn(&mut self, pause: Option<StoreWriterOperationKind>) -> Option<PathBuf> {
        assert!(self.child.is_none(), "only one child generation may run");
        self.generation += 1;
        let signal_dir = self.runtime_dir.join("signals");
        let release = pause.map(|operation| {
            signal_dir.join(format!(
                "generation-{}-{}.release",
                self.generation,
                serde_json::to_value(operation)
                    .expect("serialize StoreWriter operation")
                    .as_str()
                    .expect("StoreWriter operation serializes as string")
            ))
        });
        let reached = release.as_ref().map(|path| {
            let mut name = path
                .file_name()
                .expect("release signal has a file name")
                .to_os_string();
            name.push(".reached");
            path.with_file_name(name)
        });
        let scenario = ProcessTestConfig {
            runner_mode: ProcessRunnerMode::ProductionOfflineDelivery {
                repository_path: self.repository_path.clone(),
                provider_scenario: self.provider_scenario,
                process_fault: self.process_fault,
            },
            runtime_config: None,
            fake_scenarios: Vec::new(),
            storage_samples: vec![ProcessStorageSample::Native],
            store_writer_faults: pause
                .map(|operation| StoreWriterFaultSpec {
                    point: StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
                    operation: Some(operation),
                    count: 1,
                })
                .into_iter()
                .collect(),
            actor_pauses: Vec::new(),
            virtual_release_signals: release
                .iter()
                .map(|path| VirtualReleaseSignal {
                    name: format!("generation-{}", self.generation),
                    path: path.clone(),
                    target: VirtualReleaseTarget::StoreWriterAfterCommitBeforeWake,
                })
                .collect(),
            legacy_v2_seed: LegacyV2Seed::None,
            marker_write_failure: false,
        };
        let scenario_path = self
            .data_dir
            .join(format!("scenario-{}.json", self.generation));
        let encoded =
            serde_json::to_vec(&scenario).expect("encode strict process delivery scenario");
        let mut scenario_file = PrivateFile::create_new(&scenario_path)
            .expect("create private process delivery scenario");
        std::io::Write::write_all(&mut scenario_file, &encoded)
            .expect("write strict process delivery scenario");
        drop(scenario_file);
        let child_log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.child_log_path)
            .expect("open process delivery child log");
        let child_stderr = child_log
            .try_clone()
            .expect("clone process delivery child log");
        let child = Command::new(&self.binary)
            .current_dir(&self.root)
            .env(TEST_APP_DATA_ENV, &self.data_dir)
            .env(TEST_RUNTIME_ENV, &self.runtime_dir)
            .env(TEST_SCENARIO_ENV, &scenario_path)
            .env("RUST_LOG", "warn")
            .stdin(Stdio::null())
            .stdout(Stdio::from(child_log))
            .stderr(Stdio::from(child_stderr))
            .spawn()
            .expect("spawn real process delivery binary");
        self.child = Some(ProcessChildGuard::new(child));
        self.generation_started_at = Some(Instant::now());
        self.current_pause_reached = reached.clone();
        reached
    }

    fn child_log(&self) -> String {
        std::fs::read_to_string(&self.child_log_path)
            .unwrap_or_else(|error| format!("<unavailable: {error}>"))
    }

    async fn wait_for_descriptor(&mut self) -> RuntimeDescriptor {
        let expected_pid = self.child.as_ref().expect("child is running").id();
        let deadline = Instant::now() + STARTUP_OR_SHUTDOWN_TIMEOUT;
        loop {
            if let Ok(descriptor) = RuntimeDescriptor::read(&self.descriptor_path)
                && descriptor.pid().get() == expected_pid
            {
                return descriptor;
            }
            self.assert_child_running("publish its runtime descriptor");
            assert!(
                Instant::now() < deadline,
                "process delivery child did not publish its descriptor; child_log={}",
                self.child_log()
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    async fn wait_for_service_ready(&mut self, session: &ProcessSession) {
        let deadline = Instant::now() + STARTUP_OR_SHUTDOWN_TIMEOUT;
        loop {
            self.assert_child_running("open service admission after publishing its descriptor");
            let bootstrap = session
                .get_json("/api/bootstrap")
                .await
                .unwrap_or_else(|error| {
                    panic!(
                        "load process bootstrap while waiting for Ready: {error}; child_log={}",
                        self.child_log()
                    )
                });
            match bootstrap["service_state"].as_str() {
                Some("ready") => return,
                Some("store_degraded") => {}
                Some("quiescing") => panic!(
                    "process entered Quiescing before opening service admission; bootstrap={bootstrap}; child_log={}",
                    self.child_log()
                ),
                state => panic!(
                    "process bootstrap returned an unknown service state {state:?}; bootstrap={bootstrap}; child_log={}",
                    self.child_log()
                ),
            }
            self.assert_child_running("open service admission after publishing its descriptor");
            assert!(
                Instant::now() < deadline,
                "process did not open service admission after publishing its descriptor; bootstrap={bootstrap}; child_log={}",
                self.child_log()
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    async fn wait_for_file(&mut self, path: &Path, label: &str) {
        let mut progress = DurableProgressDeadline::new(self.generation_hard_deadline());
        let mut next_progress_poll = Instant::now();
        while !path.is_file() {
            self.assert_child_running(label);
            if Instant::now() >= next_progress_poll {
                if let Some(snapshot) = self.durable_progress_snapshot().await {
                    progress.observe(snapshot);
                }
                next_progress_poll = Instant::now() + PROGRESS_POLL_INTERVAL;
            }
            progress.assert_open(label, &self.child_log());
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    fn assert_child_running(&mut self, label: &str) {
        let status = self
            .child
            .as_mut()
            .expect("child generation exists")
            .try_wait()
            .expect("poll process delivery child");
        assert!(
            status.is_none(),
            "process child exited before {label}: {status:?}"
        );
    }

    async fn wait_delivery<T>(
        &self,
        session: &ProcessSession,
        task_id: &str,
        mut predicate: impl FnMut(&Value) -> Option<T>,
    ) -> T {
        let mut progress = DurableProgressDeadline::new(self.generation_hard_deadline());
        loop {
            let delivery = session
                .get_json(&format!("/api/tasks/{task_id}/delivery"))
                .await
                .expect("poll process delivery projection");
            progress.observe(delivery_progress_snapshot(&delivery));
            if let Some(value) = predicate(&delivery) {
                return value;
            }
            let state = delivery["latest_merge"]["state"].as_str();
            assert!(
                !matches!(state, Some("failed" | "reconciliation_required")),
                "process delivery reached terminal failure: {delivery}"
            );
            progress.assert_open("delivery projection convergence", &delivery.to_string());
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    fn generation_hard_deadline(&self) -> Instant {
        self.generation_started_at
            .expect("a process generation is active")
            + GENERATION_HARD_TIMEOUT
    }

    async fn durable_progress_snapshot(&self) -> Option<String> {
        tokio::time::timeout(STORE_OBSERVATION_TIMEOUT, async {
            if !self.database_path.is_file() {
                return None;
            }
            let store = Store::open(&self.database_path).await.ok()?;
            let rows = sqlx::query_scalar::<_, String>(
                "SELECT token FROM ( \
                     SELECT 'task|' || id || '|' || status || '|' || CAST(last_event_id AS TEXT) AS token FROM tasks \
                     UNION ALL SELECT 'source|' || task_id || '|' || state || '|' || CAST(version AS TEXT) FROM task_delivery_sources \
                     UNION ALL SELECT 'merge|' || operation_id || '|' || state || '|' || CAST(version AS TEXT) FROM task_merge_operations \
                     UNION ALL SELECT 'cleanup|' || operation_id || '|' || state || '|' || CAST(version AS TEXT) FROM task_cleanup_operations \
                     UNION ALL SELECT 'receipt|' || task_id || '|' || command_kind || '|' || client_request_id FROM task_delivery_command_receipts \
                     UNION ALL SELECT 'transition|' || CAST(COALESCE(MAX(transition_id), 0) AS TEXT) FROM task_delivery_operation_transitions \
                 ) ORDER BY token",
            )
            .fetch_all(store.pool())
            .await;
            store.close().await;
            rows.ok().map(|rows| rows.join("\n"))
        })
        .await
        .ok()
        .flatten()
    }

    async fn scalar_text(&self, query: &'static str, task_id: &str) -> String {
        let store = Store::open(&self.database_path)
            .await
            .expect("open killed-process delivery store");
        let value = sqlx::query_scalar::<_, String>(query)
            .bind(task_id)
            .fetch_one(store.pool())
            .await
            .expect("load persisted process delivery state");
        store.close().await;
        value
    }
}

impl ProcessSession {
    async fn exchange(descriptor: &RuntimeDescriptor, hard_deadline: tokio::time::Instant) -> Self {
        let client = ProcessHttpClient::new(descriptor.port().get());
        let reopen = client
            .request(
                Method::POST,
                "/_local/reopen",
                &[("x-launcher-secret", descriptor.launcher_secret())],
                &[],
                hard_deadline,
            )
            .await
            .expect("request process launch grant");
        assert_eq!(reopen.status, StatusCode::OK);
        let reopen = reopen.json();
        let url = reopen["url"].as_str().expect("reopen response has URL");
        let (exchange_origin, token) = url
            .split_once("/#token=")
            .expect("reopen URL carries a fragment token");
        let exchange_body =
            serde_json::to_vec(&json!({"token": token})).expect("encode process token exchange");
        let exchange = client
            .request(
                Method::POST,
                "/api/session/exchange",
                &[
                    ("origin", exchange_origin),
                    ("content-type", "application/json"),
                ],
                &exchange_body,
                hard_deadline,
            )
            .await
            .expect("exchange process launch token");
        assert_eq!(
            exchange.status,
            StatusCode::NO_CONTENT,
            "launch-token exchange failed for {url} from {}: {}",
            client.origin,
            String::from_utf8_lossy(&exchange.body)
        );
        let cookie = exchange
            .headers
            .get(SET_COOKIE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .expect("token exchange sets a session cookie")
            .to_owned();
        let bootstrap = client
            .request(
                Method::GET,
                "/api/bootstrap",
                &[("cookie", &cookie)],
                &[],
                hard_deadline,
            )
            .await
            .expect("load authenticated process bootstrap");
        assert_eq!(bootstrap.status, StatusCode::OK);
        let csrf = bootstrap.json()["csrf_token"]
            .as_str()
            .expect("bootstrap carries CSRF token")
            .to_owned();
        Self {
            client,
            mutation_origin: exchange_origin.to_owned(),
            cookie,
            csrf,
            hard_deadline,
        }
    }

    async fn get_json(&self, path: &str) -> Result<Value, String> {
        let response = self
            .client
            .request(
                Method::GET,
                path,
                &[("cookie", &self.cookie)],
                &[],
                self.hard_deadline,
            )
            .await?;
        if response.status != StatusCode::OK {
            return Err(format!("GET {path} returned {}", response.status));
        }
        Ok(response.json())
    }

    async fn post_json(&self, path: &str, body: &Value) -> Result<ProcessHttpResponse, String> {
        let encoded = serde_json::to_vec(body).map_err(|error| error.to_string())?;
        self.client
            .request(
                Method::POST,
                path,
                &[
                    ("cookie", &self.cookie),
                    ("origin", &self.mutation_origin),
                    ("x-csrf-token", &self.csrf),
                    ("content-type", "application/json"),
                    ("accept", "application/json"),
                ],
                &encoded,
                self.hard_deadline,
            )
            .await
    }
}

impl ProcessHttpClient {
    fn new(port: u16) -> Self {
        Self {
            port,
            origin: format!("http://127.0.0.1:{port}"),
        }
    }

    async fn request(
        &self,
        method: Method,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
        hard_deadline: tokio::time::Instant,
    ) -> Result<ProcessHttpResponse, String> {
        let stream = tokio::time::timeout_at(
            bounded_stage_deadline(hard_deadline, HTTP_CONNECT_TIMEOUT),
            tokio::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, self.port)),
        )
        .await
        .map_err(|_| {
            format!(
                "process HTTP connect exceeded its stage or generation deadline: {}{path}",
                self.origin
            )
        })?
        .map_err(|error| error.to_string())?;
        let (mut sender, connection) = tokio::time::timeout_at(
            bounded_stage_deadline(hard_deadline, HTTP_HANDSHAKE_TIMEOUT),
            hyper::client::conn::http1::handshake(TokioIo::new(stream)),
        )
        .await
        .map_err(|_| {
            format!(
                "process HTTP handshake exceeded its stage or generation deadline: {}{path}",
                self.origin
            )
        })?
        .map_err(|error| error.to_string())?;
        let _connection = HttpConnectionGuard(tokio::spawn(async move {
            let _ = connection.await;
        }));
        let mut builder = Request::builder()
            .method(method)
            .uri(path)
            .header(HOST, format!("127.0.0.1:{}", self.port))
            .header(ACCEPT, "application/json");
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        if !body.is_empty() && !headers.iter().any(|(name, _)| *name == "content-type") {
            builder = builder.header(CONTENT_TYPE, "application/json");
        }
        let request = builder
            .body(Full::new(Bytes::copy_from_slice(body)))
            .map_err(|error| error.to_string())?;
        let response = tokio::time::timeout_at(
            bounded_stage_deadline(hard_deadline, HTTP_SEND_TIMEOUT),
            sender.send_request(request),
        )
        .await
        .map_err(|_| {
            format!(
                "process HTTP response headers exceeded its stage or generation deadline: {}{path}",
                self.origin
            )
        })?
        .map_err(|error| error.to_string())?;
        let status = response.status();
        let headers = response.headers().clone();
        let body = tokio::time::timeout_at(
            bounded_stage_deadline(hard_deadline, HTTP_BODY_TIMEOUT),
            response.into_body().collect(),
        )
        .await
        .map_err(|_| {
            format!(
                "process HTTP response body exceeded its stage or generation deadline: {}{path}",
                self.origin
            )
        })?
        .map_err(|error| error.to_string())?
        .to_bytes()
        .to_vec();
        Ok(ProcessHttpResponse {
            status,
            headers,
            body,
        })
    }
}

fn bounded_stage_deadline(
    hard_deadline: tokio::time::Instant,
    stage_budget: Duration,
) -> tokio::time::Instant {
    (tokio::time::Instant::now() + stage_budget).min(hard_deadline)
}

impl ProcessHttpResponse {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|error| {
            panic!(
                "process response body is not JSON ({error}): {}",
                String::from_utf8_lossy(&self.body)
            )
        })
    }
}

impl Drop for ProcessDeliveryFixture {
    fn drop(&mut self) {
        // Reap before TempDir's field destructor can touch a child's private
        // runtime. If the platform cannot prove the child reaped, retain the
        // complete tree instead of racing deletion against a live process.
        let child_reaped = self
            .child
            .as_mut()
            .is_none_or(ProcessChildGuard::kill_and_reap_blocking);
        drop(self.child.take());
        if !child_reaped && let Some(temporary) = self.temporary.take() {
            let _retained_fixture = temporary.keep();
            eprintln!(
                "process delivery child could not be reaped; retained its private fixture root"
            );
        }
    }
}

async fn diagnostic_json_row(store: &Store, query: &'static str, task_id: &str) -> Value {
    diagnostic_json_rows(store, query, task_id)
        .await
        .as_array()
        .and_then(|rows| rows.first())
        .cloned()
        .unwrap_or(Value::Null)
}

async fn diagnostic_json_rows(store: &Store, query: &'static str, task_id: &str) -> Value {
    let mut statement = sqlx::query_scalar::<_, String>(query);
    for _ in 0..query.bytes().filter(|byte| *byte == b'?').count() {
        statement = statement.bind(task_id);
    }
    Value::Array(
        statement
            .fetch_all(store.pool())
            .await
            .expect("load process delivery diagnostic rows")
            .into_iter()
            .map(|encoded| serde_json::from_str(&encoded).expect("SQLite diagnostic row is JSON"))
            .collect(),
    )
}

fn seed_process_repository(repository: &Path) {
    std::fs::create_dir_all(repository.join("src"))
        .expect("create process fixture source directory");
    std::fs::create_dir_all(repository.join("tests"))
        .expect("create process fixture test directory");
    std::fs::write(
        repository.join("Cargo.toml"),
        "[package]\nname = \"delivery_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .expect("write process fixture manifest");
    std::fs::write(
        repository.join("Cargo.lock"),
        "# This file is automatically @generated by Cargo.\n# It is not intended for manual editing.\nversion = 4\n\n[[package]]\nname = \"delivery_fixture\"\nversion = \"0.1.0\"\n",
    )
    .expect("write process fixture lockfile");
    std::fs::write(repository.join(".gitignore"), "/target\n")
        .expect("write process fixture ignore rules");
    std::fs::write(
        repository.join("src/lib.rs"),
        "pub fn fixture_value() -> u32 { 42 }\n",
    )
    .expect("write process fixture source");
    std::fs::write(
        repository.join("src/runtime_conflict.rs"),
        "pub const SOURCE_SIDE: &str = \"base\";\n\npub const TARGET_SIDE: &str = \"base\";\n",
    )
    .expect("write process runtime-conflict source");
    std::fs::write(
        repository.join("tests/answer.rs"),
        "use delivery_fixture::fixture_value;\n\n#[test]\nfn fixture_value_is_updated() {\n    assert_eq!(fixture_value(), 43);\n}\n",
    )
    .expect("write process fixture integration test");
    git_ok(repository, &["init", "--initial-branch=main"]);
    git_ok(repository, &["config", "user.name", "Process Delivery E2E"]);
    git_ok(
        repository,
        &["config", "user.email", "process-delivery@example.invalid"],
    );
    git_ok(repository, &["add", "."]);
    git_ok(repository, &["commit", "-m", "initial process fixture"]);
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
        .expect("git output is UTF-8")
        .trim()
        .to_owned()
}

fn git_output(repository: &Path, arguments: &[&str]) -> std::process::Output {
    Command::new(if cfg!(windows) { "git.exe" } else { "git" })
        .args(arguments)
        .current_dir(repository)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .expect("run process fixture Git")
}
