#![allow(dead_code)]

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

use axum::body::Body;
use coding_agent_api::{
    AddRepositoryRequest, ApiBackend, ApiError, ApiResult, AuthContext, BootstrapResponse,
    CancelResult, CreateResult, CreateTaskRequest, LiveEventStream, QuitAcceptance, RepositoryDto,
    RequestSecurity, SchedulerAdmissionStateDto, SchedulerLimitsDto, SchedulerQueueReasonDto,
    SchedulerQueuedTaskDto, SchedulerRepositoryStorageDto, SchedulerStateDto, SchedulerStorageDto,
    SchedulerStorageScopeDto, SchedulerStorageStateDto, ServiceStateControl, ServiceStateDto,
    ServiceStateStream, SessionExchange, SseBackend, TaskDetailDto, TaskDto, TaskEventDto,
};
use coding_agent_domain::{
    CanonicalPath, ClientRequestId, DeliveryReadiness, EventId, Repository, RepositoryId, Task,
    TaskId, TaskStatus, UtcTimestamp,
};
use futures_util::Stream;
use futures_util::stream;
use http::header::{COOKIE, HOST, ORIGIN};
use http::request::Parts;
use http::{HeaderValue, Request, Response, StatusCode};
use http_body_util::BodyExt as _;
use tower::ServiceExt as _;

pub const HOST_VALUE: &str = "127.0.0.1:43121";
pub const ORIGIN_VALUE: &str = "http://127.0.0.1:43121";
pub const COOKIE_VALUE: &str = "coding_agent_session=test-session";
pub const CSRF_VALUE: &str = "test-csrf";
pub const REQUEST_ID: &str = "123e4567-e89b-12d3-a456-426614174000";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PickMode {
    Created = 0,
    Existing = 1,
    Cancelled = 2,
    Busy = 3,
    Invalid = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelMode {
    Finished = 0,
    Accepted = 1,
    Conflict = 2,
    StopAlreadyRequested = 3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryMode {
    Created = 0,
    Existing = 1,
    Conflict = 2,
    QueueFull = 3,
}

pub struct FakeBackend {
    repository: RepositoryDto,
    task: TaskDto,
    pick_mode: AtomicU8,
    cancel_mode: AtomicU8,
    retry_mode: AtomicU8,
    quit_triggered: Arc<AtomicBool>,
    calls: Mutex<Vec<&'static str>>,
}

impl FakeBackend {
    pub fn new() -> Self {
        let repository = repository();
        let task = task(repository.id);
        Self {
            repository,
            task,
            pick_mode: AtomicU8::new(PickMode::Created as u8),
            cancel_mode: AtomicU8::new(CancelMode::Finished as u8),
            retry_mode: AtomicU8::new(RetryMode::Created as u8),
            quit_triggered: Arc::new(AtomicBool::new(false)),
            calls: Mutex::new(Vec::new()),
        }
    }

    pub fn repository(&self) -> RepositoryDto {
        self.repository.clone()
    }

    pub fn task(&self) -> TaskDto {
        self.task.clone()
    }

    pub fn set_pick_mode(&self, mode: PickMode) {
        self.pick_mode.store(mode as u8, Ordering::SeqCst);
    }

    pub fn set_cancel_mode(&self, mode: CancelMode) {
        self.cancel_mode.store(mode as u8, Ordering::SeqCst);
    }

    pub fn set_retry_mode(&self, mode: RetryMode) {
        self.retry_mode.store(mode as u8, Ordering::SeqCst);
    }

    pub fn quit_triggered(&self) -> bool {
        self.quit_triggered.load(Ordering::SeqCst)
    }

    pub fn calls(&self) -> Vec<&'static str> {
        self.calls.lock().expect("lock fake calls").clone()
    }

    fn record(&self, name: &'static str) {
        self.calls.lock().expect("lock fake calls").push(name);
    }
}

#[async_trait::async_trait]
impl ApiBackend for FakeBackend {
    async fn bootstrap(&self, _: &AuthContext) -> ApiResult<BootstrapResponse> {
        self.record("bootstrap");
        Ok(BootstrapResponse {
            csrf_token: CSRF_VALUE.to_owned(),
            repositories: vec![self.repository.clone()],
            tasks: vec![self.task.clone()],
            latest_event_id: 1,
            server_started_at: timestamp().into(),
            service_state: ServiceStateDto::Ready,
            service_state_generation: 0,
            max_concurrent_tasks: 4,
            scheduler: scheduler(self.repository.id, self.task.id),
        })
    }

    async fn list_repositories(&self, _: &AuthContext) -> ApiResult<Vec<RepositoryDto>> {
        self.record("list_repositories");
        Ok(vec![self.repository.clone()])
    }

    async fn add_repository(
        &self,
        _: &AuthContext,
        request: AddRepositoryRequest,
    ) -> ApiResult<CreateResult<RepositoryDto>> {
        self.record("add_repository");
        let marker = request.path.to_string_lossy();
        if marker.contains("busy") {
            return Err(error(StatusCode::SERVICE_UNAVAILABLE, "STORE_BUSY", true));
        }
        if marker.contains("degraded") {
            return Err(error(
                StatusCode::SERVICE_UNAVAILABLE,
                "STORE_DEGRADED",
                true,
            ));
        }
        if marker.contains("invalid") {
            return Err(error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "REPOSITORY_PATH_NOT_FOUND",
                false,
            ));
        }
        if marker.contains("existing") {
            Ok(CreateResult::Existing(self.repository.clone()))
        } else {
            Ok(CreateResult::Created(self.repository.clone()))
        }
    }

    async fn pick_repository(
        &self,
        _: &AuthContext,
    ) -> ApiResult<Option<CreateResult<RepositoryDto>>> {
        self.record("pick_repository");
        match self.pick_mode.load(Ordering::SeqCst) {
            value if value == PickMode::Created as u8 => {
                Ok(Some(CreateResult::Created(self.repository.clone())))
            }
            value if value == PickMode::Existing as u8 => {
                Ok(Some(CreateResult::Existing(self.repository.clone())))
            }
            value if value == PickMode::Cancelled as u8 => Ok(None),
            value if value == PickMode::Busy as u8 => {
                Err(error(StatusCode::CONFLICT, "PICKER_ALREADY_OPEN", false))
            }
            _ => Err(error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "REPOSITORY_PATH_NOT_FOUND",
                false,
            )),
        }
    }

    async fn list_tasks(
        &self,
        _: &AuthContext,
        _: Option<RepositoryId>,
    ) -> ApiResult<Vec<TaskDto>> {
        self.record("list_tasks");
        Ok(vec![self.task.clone()])
    }

    async fn create_task(
        &self,
        _: &AuthContext,
        request: CreateTaskRequest,
    ) -> ApiResult<CreateResult<TaskDto>> {
        self.record("create_task");
        match request.prompt.as_str() {
            "existing" => Ok(CreateResult::Existing(self.task.clone())),
            "conflict" => Err(error(StatusCode::CONFLICT, "IDEMPOTENCY_CONFLICT", false)),
            "busy" => Err(error(StatusCode::SERVICE_UNAVAILABLE, "STORE_BUSY", true)),
            "degraded" => Err(error(
                StatusCode::SERVICE_UNAVAILABLE,
                "STORE_DEGRADED",
                true,
            )),
            "queue-full" => Err(ApiError::task_queue_full(32, 32)),
            "panic-known-prompt" => panic!("injected backend panic"),
            prompt if prompt.trim().is_empty() || prompt.chars().count() > 50_000 => Err(error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "INVALID_PROMPT",
                false,
            )),
            _ => Ok(CreateResult::Created(self.task.clone())),
        }
    }

    async fn task_detail(&self, _: &AuthContext, _: TaskId) -> ApiResult<TaskDetailDto> {
        self.record("task_detail");
        Ok(TaskDetailDto {
            task: self.task.clone(),
            plan: None,
            activity: Vec::new(),
            diff: None,
            tests: None,
            reviews: Vec::new(),
            timeline: Vec::new(),
            event_cursor: 1,
        })
    }

    async fn cancel_task(&self, _: &AuthContext, _: TaskId) -> ApiResult<CancelResult> {
        self.record("cancel_task");
        match self.cancel_mode.load(Ordering::SeqCst) {
            value if value == CancelMode::Finished as u8 => {
                Ok(CancelResult::Finished(self.task.clone()))
            }
            value if value == CancelMode::Accepted as u8 => Ok(CancelResult::Accepted {
                task: self.task.clone(),
            }),
            value if value == CancelMode::Conflict as u8 => {
                Err(error(StatusCode::CONFLICT, "TASK_NOT_CANCELLABLE", false))
            }
            _ => Err(ApiError::task_stop_already_requested()),
        }
    }

    async fn retry_task(&self, _: &AuthContext, _: TaskId) -> ApiResult<CreateResult<TaskDto>> {
        self.record("retry_task");
        match self.retry_mode.load(Ordering::SeqCst) {
            value if value == RetryMode::Created as u8 => {
                Ok(CreateResult::Created(self.task.clone()))
            }
            value if value == RetryMode::Existing as u8 => {
                Ok(CreateResult::Existing(self.task.clone()))
            }
            value if value == RetryMode::Conflict as u8 => {
                Err(error(StatusCode::CONFLICT, "TASK_NOT_RETRYABLE", false))
            }
            _ => Err(ApiError::task_queue_full(32, 32)),
        }
    }

    async fn task_events(
        &self,
        _: &AuthContext,
        _: TaskId,
        _: i64,
    ) -> ApiResult<Vec<TaskEventDto>> {
        self.record("task_events");
        Ok(Vec::new())
    }

    async fn request_quit(&self, _: &AuthContext) -> ApiResult<QuitAcceptance> {
        self.record("request_quit");
        let triggered = self.quit_triggered.clone();
        Ok(QuitAcceptance::new(move || {
            triggered.store(true, Ordering::SeqCst);
        }))
    }
}

#[derive(Default)]
pub struct FakeSecurity;

#[async_trait::async_trait]
impl RequestSecurity for FakeSecurity {
    fn validate_host(&self, parts: &Parts) -> ApiResult<()> {
        require_host(parts)
    }

    async fn exchange(&self, parts: &Parts, token: &str) -> ApiResult<SessionExchange> {
        require_host(parts)?;
        require_header(
            parts,
            ORIGIN,
            ORIGIN_VALUE,
            StatusCode::FORBIDDEN,
            "SECURITY_INVALID_ORIGIN",
        )?;
        if token != "launch-token" {
            return Err(error(
                StatusCode::UNAUTHORIZED,
                "SECURITY_INVALID_LAUNCH_TOKEN",
                false,
            ));
        }
        let mut set_cookie = HeaderValue::from_static(
            "coding_agent_session=test-session; HttpOnly; SameSite=Strict; Path=/",
        );
        set_cookie.set_sensitive(true);
        Ok(SessionExchange { set_cookie })
    }

    fn authorize_read(&self, parts: &Parts) -> ApiResult<AuthContext> {
        require_host(parts)?;
        require_header(
            parts,
            COOKIE,
            COOKIE_VALUE,
            StatusCode::UNAUTHORIZED,
            "SECURITY_INVALID_SESSION",
        )?;
        Ok(AuthContext {
            session_id: "test-session".to_owned(),
        })
    }

    fn authorize_mutation(&self, parts: &Parts) -> ApiResult<AuthContext> {
        let auth = self.authorize_read(parts)?;
        require_header(
            parts,
            ORIGIN,
            ORIGIN_VALUE,
            StatusCode::FORBIDDEN,
            "SECURITY_INVALID_ORIGIN",
        )?;
        require_header(
            parts,
            http::HeaderName::from_static("x-csrf-token"),
            CSRF_VALUE,
            StatusCode::FORBIDDEN,
            "SECURITY_INVALID_CSRF",
        )?;
        Ok(auth)
    }

    fn expected_public_origin(&self) -> &str {
        ORIGIN_VALUE
    }
}

#[derive(Default)]
pub struct FakeSse;

#[async_trait::async_trait]
impl SseBackend for FakeSse {
    fn subscribe_live(&self) -> LiveEventStream {
        Box::pin(stream::empty())
    }

    fn subscribe_service_state(&self) -> ServiceStateStream {
        Box::pin(stream::empty())
    }

    fn subscribe_scheduler_state(
        &self,
    ) -> Pin<Box<dyn Stream<Item = ApiResult<Arc<SchedulerStateDto>>> + Send + 'static>> {
        Box::pin(stream::empty())
    }

    async fn current_service_state(&self) -> ApiResult<ServiceStateControl> {
        Ok(ServiceStateControl::new(ServiceStateDto::Ready, 0))
    }

    async fn current_scheduler_state(&self) -> ApiResult<Arc<SchedulerStateDto>> {
        Ok(Arc::new(scheduler(uuid::Uuid::nil(), uuid::Uuid::nil())))
    }

    async fn membership_watermark_through(&self, _: i64) -> ApiResult<i64> {
        Ok(0)
    }

    async fn latest_event_id(&self) -> ApiResult<i64> {
        Ok(1)
    }

    async fn events_between(&self, _: i64, _: i64, _: usize) -> ApiResult<Vec<TaskEventDto>> {
        Ok(Vec::new())
    }
}

pub struct Ports {
    pub backend: Arc<FakeBackend>,
    pub security: Arc<FakeSecurity>,
    pub sse: Arc<FakeSse>,
}

impl Ports {
    pub fn new() -> Self {
        Self {
            backend: Arc::new(FakeBackend::new()),
            security: Arc::new(FakeSecurity),
            sse: Arc::new(FakeSse),
        }
    }
}

pub fn request(method: http::Method, uri: &str) -> http::request::Builder {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(HOST, HOST_VALUE)
        .header("x-request-id", REQUEST_ID)
}

pub fn read_request(method: http::Method, uri: &str) -> Request<Body> {
    request(method, uri)
        .header(COOKIE, COOKIE_VALUE)
        .body(Body::empty())
        .expect("build read request")
}

pub fn mutation_request(uri: &str, body: serde_json::Value) -> Request<Body> {
    request(http::Method::POST, uri)
        .header(COOKIE, COOKIE_VALUE)
        .header(ORIGIN, ORIGIN_VALUE)
        .header("x-csrf-token", CSRF_VALUE)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .expect("build mutation request")
}

pub fn empty_mutation_request(uri: &str) -> Request<Body> {
    request(http::Method::POST, uri)
        .header(COOKIE, COOKIE_VALUE)
        .header(ORIGIN, ORIGIN_VALUE)
        .header("x-csrf-token", CSRF_VALUE)
        .body(Body::empty())
        .expect("build empty mutation request")
}

pub async fn send(router: axum::Router, request: Request<Body>) -> Response<axum::body::Body> {
    router
        .oneshot(request)
        .await
        .unwrap_or_else(|error: Infallible| match error {})
}

pub async fn json(response: Response<Body>) -> (StatusCode, http::HeaderMap, serde_json::Value) {
    let status = response.status();
    let headers = response.headers().clone();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("collect response body")
        .to_bytes();
    let value = serde_json::from_slice(&body).expect("decode JSON response");
    (status, headers, value)
}

pub async fn drain(response: Response<Body>) -> (StatusCode, http::HeaderMap, Vec<u8>) {
    let status = response.status();
    let headers = response.headers().clone();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("collect response body")
        .to_bytes()
        .to_vec();
    (status, headers, body)
}

pub fn create_task_body(prompt: &str) -> serde_json::Value {
    serde_json::json!({
        "client_request_id": ClientRequestId::new(),
        "repository_id": repository().id,
        "prompt": prompt,
    })
}

pub fn error(status: StatusCode, code: &str, retryable: bool) -> ApiError {
    ApiError {
        status,
        code: code.to_owned(),
        message: format!("safe message for {code}"),
        retryable,
        details: BTreeMap::new(),
    }
}

fn require_host(parts: &Parts) -> ApiResult<()> {
    require_header(
        parts,
        HOST,
        HOST_VALUE,
        StatusCode::FORBIDDEN,
        "SECURITY_INVALID_HOST",
    )
}

fn require_header(
    parts: &Parts,
    name: http::HeaderName,
    expected: &str,
    invalid_status: StatusCode,
    code: &str,
) -> ApiResult<()> {
    let mut values = parts.headers.get_all(&name).iter();
    let first = values.next();
    if values.next().is_some() {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "SECURITY_DUPLICATE_HEADER",
            false,
        ));
    }
    if first.and_then(|value| value.to_str().ok()) == Some(expected) {
        Ok(())
    } else {
        Err(error(invalid_status, code, false))
    }
}

fn repository() -> RepositoryDto {
    let root = if cfg!(windows) {
        PathBuf::from(r"C:\fixture\repository")
    } else {
        PathBuf::from("/fixture/repository")
    };
    RepositoryDto::from(Repository {
        id: RepositoryId::new(),
        selected_path: canonical(root.join("selected")),
        display_name: "fixture".to_owned(),
        git_root: canonical(root.clone()),
        cargo_workspace_root: canonical(root.join("workspace")),
        created_at: timestamp(),
        last_opened_at: timestamp(),
    })
}

fn task(repository_id: uuid::Uuid) -> TaskDto {
    TaskDto::from(Task {
        id: TaskId::new(),
        client_request_id: ClientRequestId::new(),
        repository_id: repository_id.to_string().parse().expect("repository ID"),
        prompt: "safe fixture prompt".to_owned(),
        status: TaskStatus::Queued,
        delivery_readiness: DeliveryReadiness::Unreviewed,
        attempt: 1,
        retry_of: None,
        created_at: timestamp(),
        started_at: None,
        finished_at: None,
        last_event_id: EventId::new(1).expect("event ID"),
        failure: None,
    })
}

fn scheduler(repository_id: uuid::Uuid, queued_task_id: uuid::Uuid) -> SchedulerStateDto {
    SchedulerStateDto {
        schema_version: 1,
        server_instance_id: uuid::Uuid::parse_str("123e4567-e89b-42d3-a456-426614174000")
            .expect("scheduler instance UUID"),
        server_started_at: timestamp().into(),
        generation: 1,
        as_of_event_id: 1,
        service_state_generation: 0,
        admission_state: SchedulerAdmissionStateDto::Running,
        limits: SchedulerLimitsDto {
            global: 4,
            per_repository: 2,
            queued: 32,
            cargo_jobs_per_task: 1,
        },
        active_task_count: 0,
        queued_task_count: 1,
        queued_tasks: vec![SchedulerQueuedTaskDto {
            task_id: queued_task_id,
            reason: SchedulerQueueReasonDto::RepositoryControlBusy,
        }],
        stopping_tasks: Vec::new(),
        storage: SchedulerStorageDto {
            state: SchedulerStorageStateDto::Normal,
            data: SchedulerStorageScopeDto {
                state: SchedulerStorageStateDto::Normal,
            },
            runtime: SchedulerStorageScopeDto {
                state: SchedulerStorageStateDto::Normal,
            },
            repositories: vec![SchedulerRepositoryStorageDto {
                repository_id,
                state: SchedulerStorageStateDto::Normal,
            }],
        },
    }
}

fn timestamp() -> UtcTimestamp {
    UtcTimestamp::parse_rfc3339("2026-07-15T00:00:00Z").expect("fixture timestamp")
}

fn canonical(path: PathBuf) -> CanonicalPath {
    CanonicalPath::try_from_canonical(path).expect("fixture canonical path")
}
