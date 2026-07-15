use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Request, State};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use coding_agent_api::{
    AddRepositoryRequest, ApiBackend, ApiError, ApiErrorResponse, ApiResult, AuthContext,
    BootstrapResponse, CancelResult, CreateResult, CreateTaskRequest, LiveEventStream,
    QuitAcceptance, RepositoryDto, ServiceStateControl, ServiceStateDto, ServiceStateStream,
    SseBackend, TaskDetailDto, TaskDto, TaskEventDto,
};
use coding_agent_domain::{
    CanonicalPath, DomainError, EventCursor, NewRepository, NewTask, RepositoryId, TaskId,
    UtcTimestamp,
};
use coding_agent_store::{
    CreateTaskOutcome, RegisterRepositoryOutcome, RetryTaskOutcome, Store, StoreError,
};
use http::{HeaderValue, StatusCode};
use serde::Serialize;
use tokio::sync::Notify;
use tokio::time::Instant;
use uuid::Uuid;

use crate::security::LAUNCH_TOKEN_LIFETIME;
use crate::store_writer::sqlite_code_is_retryable;
use crate::{
    CancelOutcome, EventDispatcherHandle, NativeDialogService, PickerError, RepositoryDiscovery,
    RepositoryDiscoveryError, SecurityManager, ServiceState, ServiceStateController, StartupPhase,
    StartupPhaseController, StoreWriterError, StoreWriterHandle, TaskManagerError,
    TaskManagerHandle, WallClock,
};

const REQUEST_ID_HEADER: &str = "x-request-id";
const LOCAL_READY_PATH: &str = "/_local/ready";
const LOCAL_REOPEN_PATH: &str = "/_local/reopen";
const CONTENT_SECURITY_POLICY: &str = "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'";

#[derive(Clone)]
struct RuntimeRouterState {
    instance_id: Uuid,
    phase: StartupPhaseController,
    security: SecurityManager,
    wall_clock: Arc<dyn WallClock>,
}

#[derive(Clone)]
struct RuntimeRequestId(String);

#[derive(Serialize)]
struct ReadyResponse {
    instance_id: Uuid,
    state: StartupPhase,
}

#[derive(Serialize)]
struct ReopenResponse {
    url: String,
    expires_at: String,
}

/// Wraps the business API with the process-local startup and launcher endpoints.
///
/// The guard is deliberately outside the supplied API router: every request, including
/// requests rejected while the process is starting, must pass exact Host validation.
pub fn build_runtime_router(
    api_router: Router,
    instance_id: Uuid,
    phase: StartupPhaseController,
    security: SecurityManager,
    wall_clock: Arc<dyn WallClock>,
) -> Router {
    let state = RuntimeRouterState {
        instance_id,
        phase,
        security,
        wall_clock,
    };
    let local_router = Router::new()
        .route(LOCAL_READY_PATH, get(local_ready))
        .route(LOCAL_REOPEN_PATH, post(local_reopen))
        .with_state(state.clone());

    local_router
        .merge(api_router)
        .fallback_service(crate::StaticAssetService::new())
        .layer(middleware::from_fn_with_state(state, runtime_guard))
}

async fn runtime_guard(
    State(state): State<RuntimeRouterState>,
    mut request: Request,
    next: Next,
) -> Response {
    let request_id = choose_runtime_request_id(request.headers());
    request
        .extensions_mut()
        .insert(RuntimeRequestId(request_id.clone()));
    let (parts, body) = request.into_parts();
    let no_store_request =
        path_is_within(parts.uri.path(), "/api") || path_is_within(parts.uri.path(), "/_local");
    let host_result = state.security.validate_host(&parts);
    let local_startup_path = matches!(parts.uri.path(), LOCAL_READY_PATH | LOCAL_REOPEN_PATH);
    let request = Request::from_parts(parts, body);

    let response = if let Err(error) = host_result {
        runtime_error_response(error, &request_id)
    } else if state.phase.current() == StartupPhase::Starting && !local_startup_path {
        runtime_error_response(app_starting(), &request_id)
    } else {
        next.run(request).await
    };

    apply_response_policy(with_request_id(response, &request_id), no_store_request)
}

fn apply_response_policy(mut response: Response, no_store_request: bool) -> Response {
    let html_response = response
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("text/html"));
    let headers = response.headers_mut();

    headers.insert(
        "content-security-policy",
        HeaderValue::from_static(CONTENT_SECURITY_POLICY),
    );
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    headers.insert("referrer-policy", HeaderValue::from_static("no-referrer"));
    if no_store_request || html_response {
        headers.insert(
            http::header::CACHE_CONTROL,
            HeaderValue::from_static("no-store"),
        );
    }

    for header in [
        "access-control-allow-credentials",
        "access-control-allow-headers",
        "access-control-allow-methods",
        "access-control-allow-origin",
        "access-control-allow-private-network",
        "access-control-expose-headers",
        "access-control-max-age",
    ] {
        headers.remove(header);
    }

    response
}

fn path_is_within(path: &str, prefix: &str) -> bool {
    path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

async fn local_ready(State(state): State<RuntimeRouterState>, request: Request) -> Response {
    let request_id = runtime_request_id(&request);
    let (parts, _) = request.into_parts();
    if let Err(error) = state.security.authorize_launcher(&parts) {
        return runtime_error_response(error, &request_id);
    }
    Json(ReadyResponse {
        instance_id: state.instance_id,
        state: state.phase.current(),
    })
    .into_response()
}

async fn local_reopen(State(state): State<RuntimeRouterState>, request: Request) -> Response {
    let request_id = runtime_request_id(&request);
    let (parts, _) = request.into_parts();
    if let Err(error) = state.security.authorize_launcher(&parts) {
        return runtime_error_response(error, &request_id);
    }
    if state.phase.current() != StartupPhase::Ready {
        return runtime_error_response(app_starting(), &request_id);
    }

    // Build the externally reported deadline before issuing the token. This makes the
    // advertised lifetime no longer than the monotonic two-minute security lifetime.
    let expires_at = match UtcTimestamp::new(
        state
            .wall_clock
            .now_utc()
            .saturating_add(time::Duration::seconds(
                i64::try_from(LAUNCH_TOKEN_LIFETIME.as_secs())
                    .expect("launch-token lifetime fits an i64"),
            )),
    ) {
        Ok(value) => value.to_string(),
        Err(_) => return runtime_error_response(internal_error(), &request_id),
    };
    let token = match state.security.issue_launch_token() {
        Ok(token) => token,
        Err(_) => {
            return runtime_error_response(
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "SECURITY_RANDOM_UNAVAILABLE",
                    "secure random generation is temporarily unavailable",
                    false,
                ),
                &request_id,
            );
        }
    };
    Json(ReopenResponse {
        url: format!(
            "{}/#token={}",
            state.security.public_origin(),
            token.as_str()
        ),
        expires_at,
    })
    .into_response()
}

fn runtime_request_id(request: &Request) -> String {
    request
        .extensions()
        .get::<RuntimeRequestId>()
        .map(|value| value.0.clone())
        .unwrap_or_else(|| Uuid::new_v4().to_string())
}

fn choose_runtime_request_id(headers: &http::HeaderMap) -> String {
    let mut values = headers.get_all(REQUEST_ID_HEADER).iter();
    let first = values.next();
    let only_one = values.next().is_none();
    first
        .filter(|_| only_one)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| Uuid::parse_str(value).ok().map(|id| (value, id)))
        .filter(|(value, id)| *value == id.to_string())
        .map(|(_, id)| id.to_string())
        .unwrap_or_else(|| Uuid::new_v4().to_string())
}

fn runtime_error_response(error: ApiError, request_id: &str) -> Response {
    (
        error.status,
        Json(ApiErrorResponse {
            code: error.code,
            message: error.message,
            retryable: error.retryable,
            request_id: request_id.to_owned(),
            details: error.details,
        }),
    )
        .into_response()
}

fn with_request_id(mut response: Response, request_id: &str) -> Response {
    if !response.headers().contains_key(REQUEST_ID_HEADER) {
        response.headers_mut().insert(
            REQUEST_ID_HEADER,
            HeaderValue::from_str(request_id).expect("UUID request ID is a valid header"),
        );
    }
    response
}

#[derive(Clone)]
pub struct MutationGate {
    inner: Arc<MutationGateInner>,
}

struct MutationGateInner {
    service_state: ServiceStateController,
    lifecycle: Mutex<MutationLifecycle>,
    idle: Notify,
}

#[derive(Debug, Default)]
struct MutationLifecycle {
    closed: bool,
    active: usize,
}

pub struct MutationGuard {
    inner: Option<Arc<MutationGateInner>>,
}

impl MutationGate {
    pub fn new(service_state: ServiceStateController) -> Self {
        Self {
            inner: Arc::new(MutationGateInner {
                service_state,
                lifecycle: Mutex::new(MutationLifecycle::default()),
                idle: Notify::new(),
            }),
        }
    }

    pub fn enter_data_mutation(&self) -> ApiResult<MutationGuard> {
        let mut lifecycle = self
            .inner
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if lifecycle.closed {
            return Err(app_shutting_down());
        }
        match self.inner.service_state.current().state {
            ServiceState::Ready => {}
            ServiceState::StoreDegraded => return Err(store_degraded()),
            ServiceState::Quiescing => return Err(app_shutting_down()),
        }
        lifecycle.active = lifecycle
            .active
            .checked_add(1)
            .expect("mutation gate active-count overflow");
        Ok(MutationGuard {
            inner: Some(self.inner.clone()),
        })
    }

    pub fn prepare_quit(&self) -> ApiResult<()> {
        let lifecycle = self
            .inner
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if lifecycle.closed || self.inner.service_state.current().state == ServiceState::Quiescing {
            Err(app_shutting_down())
        } else {
            Ok(())
        }
    }

    pub async fn wait_for_idle(&self) {
        loop {
            let notified = self.inner.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self
                .inner
                .lifecycle
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .active
                == 0
            {
                return;
            }
            notified.await;
        }
    }

    pub(crate) fn begin_quiescing(&self) -> bool {
        let mut lifecycle = self
            .inner
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if lifecycle.closed {
            return false;
        }
        lifecycle.closed = true;
        let _ = self.inner.service_state.set(ServiceState::Quiescing);
        if lifecycle.active == 0 {
            self.inner.idle.notify_waiters();
        }
        true
    }
}

impl Drop for MutationGuard {
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        let mut lifecycle = inner
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert!(lifecycle.active > 0, "mutation gate guard underflow");
        lifecycle.active -= 1;
        if lifecycle.active == 0 {
            inner.idle.notify_waiters();
        }
    }
}

pub struct ApplicationBackend {
    store: Store,
    writer: StoreWriterHandle,
    dispatcher: EventDispatcherHandle,
    task_manager: TaskManagerHandle,
    discovery: RepositoryDiscovery,
    dialog: Option<NativeDialogService>,
    security: SecurityManager,
    service_state: ServiceStateController,
    mutation_gate: MutationGate,
    server_started_at: UtcTimestamp,
    max_concurrent_tasks: u32,
    write_budget: Duration,
    quit_signal: Arc<dyn Fn() + Send + Sync + 'static>,
}

impl ApplicationBackend {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: Store,
        writer: StoreWriterHandle,
        dispatcher: EventDispatcherHandle,
        task_manager: TaskManagerHandle,
        discovery: RepositoryDiscovery,
        dialog: Option<NativeDialogService>,
        security: SecurityManager,
        service_state: ServiceStateController,
        mutation_gate: MutationGate,
        server_started_at: UtcTimestamp,
        max_concurrent_tasks: u32,
        write_budget: Duration,
        quit_signal: Arc<dyn Fn() + Send + Sync + 'static>,
    ) -> Self {
        assert!(
            max_concurrent_tasks > 0,
            "task concurrency must be positive"
        );
        assert!(!write_budget.is_zero(), "write budget must be positive");
        Self {
            store,
            writer,
            dispatcher,
            task_manager,
            discovery,
            dialog,
            security,
            service_state,
            mutation_gate,
            server_started_at,
            max_concurrent_tasks,
            write_budget,
            quit_signal,
        }
    }

    pub fn mutation_gate(&self) -> &MutationGate {
        &self.mutation_gate
    }

    fn deadline(&self) -> Instant {
        Instant::now() + self.write_budget
    }

    async fn register_path(&self, path: &Path) -> ApiResult<CreateResult<RepositoryDto>> {
        let discovered = self
            .discovery
            .discover(path)
            .await
            .map_err(map_discovery_error)?;
        let input = NewRepository {
            selected_path: canonical(discovered.selected_path)?,
            display_name: discovered.display_name,
            git_root: canonical(discovered.git_root)?,
            cargo_workspace_root: canonical(discovered.cargo_workspace_root)?,
        };
        let receipt = self
            .writer
            .register_repository(input, self.deadline())
            .await
            .map_err(map_writer_error)?;
        let result = match receipt.value {
            RegisterRepositoryOutcome::Created(repository) => {
                tracing::info!(repository_id = %repository.id, "repository registered");
                CreateResult::Created(repository.into())
            }
            RegisterRepositoryOutcome::Existing(repository) => {
                tracing::info!(repository_id = %repository.id, "repository reopened");
                CreateResult::Existing(repository.into())
            }
        };
        Ok(result)
    }
}

#[async_trait::async_trait]
impl ApiBackend for ApplicationBackend {
    async fn bootstrap(&self, auth: &AuthContext) -> ApiResult<BootstrapResponse> {
        let snapshot = self
            .store
            .bootstrap_snapshot()
            .await
            .map_err(map_store_error)?;
        let state = self.service_state.current();
        Ok(BootstrapResponse {
            csrf_token: self.security.csrf_for_auth(auth)?,
            repositories: snapshot.repositories.into_iter().map(Into::into).collect(),
            tasks: snapshot.tasks.into_iter().map(Into::into).collect(),
            latest_event_id: snapshot.latest_event_id.get(),
            server_started_at: self.server_started_at.into(),
            service_state: service_state(state.state),
            service_state_generation: state.generation,
            max_concurrent_tasks: self.max_concurrent_tasks,
        })
    }

    async fn list_repositories(&self, _: &AuthContext) -> ApiResult<Vec<RepositoryDto>> {
        Ok(self
            .store
            .list_repositories()
            .await
            .map_err(map_store_error)?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    async fn add_repository(
        &self,
        _: &AuthContext,
        request: AddRepositoryRequest,
    ) -> ApiResult<CreateResult<RepositoryDto>> {
        let _guard = self.mutation_gate.enter_data_mutation()?;
        let result = self.register_path(&request.path).await;
        if let Err(error) = &result {
            log_failure("repository.add", error);
        }
        result
    }

    async fn pick_repository(
        &self,
        _: &AuthContext,
    ) -> ApiResult<Option<CreateResult<RepositoryDto>>> {
        let _guard = self.mutation_gate.enter_data_mutation()?;
        let dialog = self.dialog.as_ref().ok_or_else(picker_unavailable)?;
        let selected = dialog.pick_repository().await.map_err(map_picker_error)?;
        match selected {
            Some(path) => self.register_path(&path).await.map(Some),
            None => Ok(None),
        }
    }

    async fn list_tasks(
        &self,
        _: &AuthContext,
        repository_id: Option<RepositoryId>,
    ) -> ApiResult<Vec<TaskDto>> {
        let snapshot = self
            .store
            .bootstrap_snapshot()
            .await
            .map_err(map_store_error)?;
        Ok(snapshot
            .tasks
            .into_iter()
            .filter(|task| repository_id.is_none_or(|id| task.repository_id == id))
            .map(Into::into)
            .collect())
    }

    async fn create_task(
        &self,
        _: &AuthContext,
        request: CreateTaskRequest,
    ) -> ApiResult<CreateResult<TaskDto>> {
        let _guard = self.mutation_gate.enter_data_mutation()?;
        let input = NewTask::try_new(
            request.client_request_id,
            request.repository_id,
            request.prompt,
        )
        .map_err(map_domain_error)?;
        let receipt = self
            .writer
            .create_task(input, self.deadline())
            .await
            .map_err(map_writer_error);
        let receipt = match receipt {
            Ok(receipt) => receipt,
            Err(error) => {
                log_failure("task.create", &error);
                return Err(error);
            }
        };
        match receipt.value {
            CreateTaskOutcome::Created { task, .. } => {
                notify_after_commit(&self.task_manager, task.id).await;
                tracing::info!(
                    task_id = %task.id,
                    repository_id = %task.repository_id,
                    disposition = "created",
                    "task mutation committed"
                );
                Ok(CreateResult::Created(task.into()))
            }
            CreateTaskOutcome::Existing { task } => {
                tracing::info!(
                    task_id = %task.id,
                    repository_id = %task.repository_id,
                    disposition = "existing",
                    "task mutation replayed"
                );
                Ok(CreateResult::Existing(task.into()))
            }
        }
    }

    async fn task_detail(&self, _: &AuthContext, id: TaskId) -> ApiResult<TaskDetailDto> {
        let detail = self
            .store
            .task_detail(id)
            .await
            .map_err(map_store_error)?
            .ok_or_else(task_not_found)?;
        Ok(TaskDetailDto {
            task: detail.task.into(),
            plan: detail.plan.map(Into::into),
            activity: detail.activity.into_iter().map(Into::into).collect(),
            diff: detail.diff.map(Into::into),
            tests: detail.tests.map(Into::into),
            timeline: detail.timeline.into_iter().map(Into::into).collect(),
            event_cursor: detail.event_cursor.get(),
        })
    }

    async fn cancel_task(&self, _: &AuthContext, id: TaskId) -> ApiResult<CancelResult> {
        let _guard = self.mutation_gate.enter_data_mutation()?;
        let outcome = self
            .task_manager
            .cancel(id)
            .await
            .map_err(map_manager_error)?;
        Ok(match outcome {
            CancelOutcome::Cancelled { task } => CancelResult::Finished(task.into()),
            CancelOutcome::Accepted { task } => CancelResult::Accepted { task: task.into() },
        })
    }

    async fn retry_task(&self, _: &AuthContext, id: TaskId) -> ApiResult<CreateResult<TaskDto>> {
        let _guard = self.mutation_gate.enter_data_mutation()?;
        let receipt = self
            .writer
            .retry_task(id, self.deadline())
            .await
            .map_err(map_writer_error);
        let receipt = match receipt {
            Ok(receipt) => receipt,
            Err(error) => {
                log_failure("task.retry", &error);
                return Err(error);
            }
        };
        match receipt.value {
            RetryTaskOutcome::Created { task, .. } => {
                notify_after_commit(&self.task_manager, task.id).await;
                tracing::info!(
                    task_id = %task.id,
                    repository_id = %task.repository_id,
                    disposition = "created",
                    "task retry committed"
                );
                Ok(CreateResult::Created(task.into()))
            }
            RetryTaskOutcome::Existing { task } => {
                tracing::info!(
                    task_id = %task.id,
                    repository_id = %task.repository_id,
                    disposition = "existing",
                    "task retry replayed"
                );
                Ok(CreateResult::Existing(task.into()))
            }
        }
    }

    async fn task_events(
        &self,
        _: &AuthContext,
        id: TaskId,
        after: i64,
    ) -> ApiResult<Vec<TaskEventDto>> {
        if self
            .store
            .task_detail(id)
            .await
            .map_err(map_store_error)?
            .is_none()
        {
            return Err(task_not_found());
        }
        let after = EventCursor::new(after).map_err(map_domain_error)?;
        Ok(self
            .store
            .task_events_after(id, after, usize::MAX)
            .await
            .map_err(map_store_error)?
            .events
            .into_iter()
            .map(Into::into)
            .collect())
    }

    async fn request_quit(&self, _: &AuthContext) -> ApiResult<QuitAcceptance> {
        self.mutation_gate.prepare_quit()?;
        let gate = self.mutation_gate.clone();
        let signal = self.quit_signal.clone();
        Ok(QuitAcceptance::new(move || {
            if gate.begin_quiescing() {
                signal();
            }
        }))
    }
}

#[async_trait::async_trait]
impl SseBackend for ApplicationBackend {
    fn subscribe_live(&self) -> LiveEventStream {
        let mut receiver = self.dispatcher.subscribe();
        let mut service = self.service_state.subscribe();
        Box::pin(async_stream::stream! {
            loop {
                if service.borrow().state == ServiceState::Quiescing {
                    return;
                }
                tokio::select! {
                    event = receiver.recv() => match event {
                        Ok(event) => yield coding_agent_api::LiveEventItem::Event(event.into()),
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            yield coding_agent_api::LiveEventItem::Lagged;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                    },
                    changed = service.changed() => {
                        if changed.is_err()
                            || service.borrow_and_update().state == ServiceState::Quiescing
                        {
                            return;
                        }
                    }
                }
            }
        })
    }

    fn subscribe_service_state(&self) -> ServiceStateStream {
        let mut receiver = self.service_state.subscribe();
        Box::pin(async_stream::stream! {
            let current = *receiver.borrow();
            if current.state == ServiceState::Quiescing {
                yield ServiceStateControl::new(
                    service_state(current.state),
                    current.generation,
                );
                return;
            }
            loop {
                if receiver.changed().await.is_err() {
                    return;
                }
                let snapshot = *receiver.borrow_and_update();
                yield ServiceStateControl::new(
                    service_state(snapshot.state),
                    snapshot.generation,
                );
                if snapshot.state == ServiceState::Quiescing {
                    return;
                }
            }
        })
    }

    async fn current_service_state(&self) -> ApiResult<ServiceStateControl> {
        let state = self.service_state.current();
        Ok(ServiceStateControl::new(
            service_state(state.state),
            state.generation,
        ))
    }

    async fn latest_event_id(&self) -> ApiResult<i64> {
        Ok(self
            .store
            .latest_event_id()
            .await
            .map_err(map_store_error)?
            .get())
    }

    async fn events_between(
        &self,
        after: i64,
        through: i64,
        limit: usize,
    ) -> ApiResult<Vec<TaskEventDto>> {
        let after = EventCursor::new(after).map_err(map_domain_error)?;
        Ok(self
            .store
            .events_after(after, limit)
            .await
            .map_err(map_store_error)?
            .events
            .into_iter()
            .filter(|event| event.id.get() <= through)
            .map(Into::into)
            .collect())
    }
}

async fn notify_after_commit(manager: &TaskManagerHandle, task_id: TaskId) {
    if manager.notify_queued(task_id).await.is_err() {
        tracing::warn!(
            task_id = %task_id,
            "queued-task notification was lost after durable commit"
        );
    }
}

fn service_state(state: ServiceState) -> ServiceStateDto {
    match state {
        ServiceState::Ready => ServiceStateDto::Ready,
        ServiceState::StoreDegraded => ServiceStateDto::StoreDegraded,
        ServiceState::Quiescing => ServiceStateDto::Quiescing,
    }
}

fn canonical(path: impl Into<std::path::PathBuf>) -> ApiResult<CanonicalPath> {
    CanonicalPath::try_from_canonical(path).map_err(|_| {
        api_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "INVALID_REPOSITORY_PATH",
            "the repository path is invalid",
            false,
        )
    })
}

fn map_domain_error(error: DomainError) -> ApiError {
    match error {
        DomainError::InvalidPrompt => api_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "INVALID_PROMPT",
            "prompt must contain between 1 and 50,000 Unicode scalar values",
            false,
        ),
        DomainError::InvalidEventCursor => api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_QUERY",
            "the event cursor must be nonnegative",
            false,
        ),
        _ => internal_error(),
    }
}

fn map_store_error(error: StoreError) -> ApiError {
    if let StoreError::Database(database) = &error {
        let code = database.as_database_error().and_then(|error| error.code());
        if let Some(error) = map_database_code(code.as_deref()) {
            return error;
        }
    }
    match error {
        StoreError::Domain(error) => map_domain_error(error),
        StoreError::IdempotencyConflict => api_error(
            StatusCode::CONFLICT,
            "IDEMPOTENCY_CONFLICT",
            "the client request ID belongs to different task input",
            false,
        ),
        StoreError::TaskNotFound => task_not_found(),
        StoreError::TaskNotRetryable => api_error(
            StatusCode::CONFLICT,
            "TASK_NOT_RETRYABLE",
            "the task is not terminal and cannot be retried",
            false,
        ),
        _ => internal_error(),
    }
}

fn map_writer_error(error: StoreWriterError) -> ApiError {
    match error {
        StoreWriterError::Busy => store_busy(),
        StoreWriterError::Store(error) => map_store_error(error),
        StoreWriterError::Closed => app_shutting_down(),
    }
}

fn map_database_code(code: Option<&str>) -> Option<ApiError> {
    code.filter(|code| sqlite_code_is_retryable(code))
        .map(|_| store_busy())
}

fn map_manager_error(error: TaskManagerError) -> ApiError {
    match error {
        TaskManagerError::Closed => app_shutting_down(),
        TaskManagerError::Store(error) => map_store_error(error),
        TaskManagerError::StoreWriter(error) => map_writer_error(error),
        TaskManagerError::TaskNotFound => task_not_found(),
        TaskManagerError::TaskNotCancellable { .. } => api_error(
            StatusCode::CONFLICT,
            "TASK_NOT_CANCELLABLE",
            "the task cannot be cancelled in its current state",
            false,
        ),
        TaskManagerError::Frozen | TaskManagerError::StoreDegraded => store_degraded(),
        TaskManagerError::Invariant(_) => internal_error(),
    }
}

fn map_discovery_error(error: RepositoryDiscoveryError) -> ApiError {
    api_error(
        StatusCode::UNPROCESSABLE_ENTITY,
        error.code(),
        "the selected repository could not be validated",
        false,
    )
}

fn map_picker_error(error: PickerError) -> ApiError {
    match error {
        PickerError::AlreadyOpen => api_error(
            StatusCode::CONFLICT,
            error.code(),
            "a repository picker is already open",
            false,
        ),
        PickerError::Unavailable => picker_unavailable(),
        PickerError::MainThreadRequired => internal_error(),
    }
}

fn picker_unavailable() -> ApiError {
    api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "PICKER_UNAVAILABLE",
        "the repository picker is unavailable",
        true,
    )
}

fn task_not_found() -> ApiError {
    api_error(
        StatusCode::NOT_FOUND,
        "TASK_NOT_FOUND",
        "the task was not found",
        false,
    )
}

fn store_degraded() -> ApiError {
    api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "STORE_DEGRADED",
        "the local store is degraded; data mutations are temporarily disabled",
        true,
    )
}

fn store_busy() -> ApiError {
    api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "STORE_BUSY",
        "the local store is busy; retry the request",
        true,
    )
}

fn app_shutting_down() -> ApiError {
    api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "APP_SHUTTING_DOWN",
        "the application is shutting down",
        true,
    )
}

fn app_starting() -> ApiError {
    api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "APP_STARTING",
        "the application is starting",
        true,
    )
}

fn internal_error() -> ApiError {
    api_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "INTERNAL_ERROR",
        "the request could not be completed",
        false,
    )
}

fn api_error(status: StatusCode, code: &str, message: &str, retryable: bool) -> ApiError {
    ApiError {
        status,
        code: code.to_owned(),
        message: message.to_owned(),
        retryable,
        details: BTreeMap::new(),
    }
}

fn log_failure(operation: &'static str, error: &ApiError) {
    tracing::info!(operation, error_code = %error.code, "application mutation rejected");
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::Body;
    use axum::response::Html;
    use http::Method;
    use http_body_util::BodyExt as _;
    use serde_json::{Value, json};
    use tower::ServiceExt as _;

    use crate::{SecuritySeed, SystemSecurityClock, SystemWallClock};

    use super::*;

    struct RuntimeFixture {
        router: Router,
        phase: StartupPhaseController,
        security: SecurityManager,
        instance_id: Uuid,
        host: String,
        launcher_secret: String,
    }

    impl RuntimeFixture {
        fn new() -> Self {
            let instance_id = Uuid::new_v4();
            let host = "127.0.0.1:43121".to_owned();
            let seed = SecuritySeed::generate().expect("generate runtime secrets");
            let launcher_secret = seed.launcher_secret().as_str().to_owned();
            let security = SecurityManager::from_seed(
                seed,
                format!("http://{host}"),
                Arc::new(SystemSecurityClock),
            )
            .expect("construct runtime security");
            let phase = StartupPhaseController::new();
            let api_router = Router::new()
                .route("/api/ping", get(|| async { StatusCode::NO_CONTENT }))
                .route(
                    "/page",
                    get(|| async {
                        let mut response = Html("<!doctype html>").into_response();
                        response.headers_mut().insert(
                            http::header::ACCESS_CONTROL_ALLOW_ORIGIN,
                            HeaderValue::from_static("*"),
                        );
                        response
                    }),
                );
            let router = build_runtime_router(
                api_router,
                instance_id,
                phase.clone(),
                security.clone(),
                Arc::new(SystemWallClock),
            );
            Self {
                router,
                phase,
                security,
                instance_id,
                host,
                launcher_secret,
            }
        }

        fn request(&self, method: Method, path: &str) -> http::Request<Body> {
            http::Request::builder()
                .method(method)
                .uri(path)
                .header(http::header::HOST, &self.host)
                .header("x-launcher-secret", &self.launcher_secret)
                .body(Body::empty())
                .expect("build runtime request")
        }
    }

    async fn json_body(response: Response) -> Value {
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("collect response body")
            .to_bytes();
        serde_json::from_slice(&bytes).expect("decode response JSON")
    }

    async fn error_code(response: Response) -> String {
        json_body(response).await["code"]
            .as_str()
            .expect("error code is a string")
            .to_owned()
    }

    #[test]
    fn sqlite_busy_codes_map_to_the_retryable_store_busy_contract() {
        for code in ["5", "6", "261", "262"] {
            let error = map_database_code(Some(code)).expect("busy code must map");
            assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE, "code={code}");
            assert_eq!(error.code, "STORE_BUSY", "code={code}");
            assert!(error.retryable, "code={code}");
        }

        for code in [None, Some("4"), Some("7"), Some("260"), Some("not-a-code")] {
            assert!(map_database_code(code).is_none(), "code={code:?}");
        }

        let fallback = map_store_error(StoreError::InvariantViolation("test fallback"));
        assert_eq!(fallback.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(fallback.code, "INTERNAL_ERROR");
        assert!(!fallback.retryable);

        let pool_timeout = map_store_error(StoreError::Database(sqlx::Error::PoolTimedOut));
        assert_eq!(pool_timeout.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(pool_timeout.code, "INTERNAL_ERROR");
        assert!(!pool_timeout.retryable);
    }

    #[tokio::test]
    async fn starting_ready_probe_returns_only_instance_and_phase() {
        let fixture = RuntimeFixture::new();
        let response = fixture
            .router
            .clone()
            .oneshot(fixture.request(Method::GET, LOCAL_READY_PATH))
            .await
            .expect("call ready probe");

        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().contains_key(REQUEST_ID_HEADER));
        assert_eq!(
            response.headers().get(http::header::CACHE_CONTROL),
            Some(&HeaderValue::from_static("no-store"))
        );
        assert!(
            !response
                .headers()
                .contains_key(http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
        );
        let body = json_body(response).await;
        assert_eq!(body.as_object().expect("ready object").len(), 2);
        assert_eq!(body["instance_id"], json!(fixture.instance_id));
        assert_eq!(body["state"], "starting");
    }

    #[tokio::test]
    async fn outer_response_policy_covers_success_errors_html_and_api_without_cors() {
        const EXPECTED_CSP: &str = "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'";

        fn assert_common_policy(response: &Response) {
            assert_eq!(
                response.headers().get("x-content-type-options"),
                Some(&HeaderValue::from_static("nosniff"))
            );
            assert_eq!(
                response.headers().get("referrer-policy"),
                Some(&HeaderValue::from_static("no-referrer"))
            );
            assert_eq!(
                response.headers().get("content-security-policy"),
                Some(&HeaderValue::from_static(EXPECTED_CSP))
            );
            assert!(
                !response
                    .headers()
                    .contains_key(http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
            );
        }

        let fixture = RuntimeFixture::new();
        assert!(fixture.phase.mark_ready());

        let api = fixture
            .router
            .clone()
            .oneshot(fixture.request(Method::GET, "/api/ping"))
            .await
            .expect("call API");
        assert_common_policy(&api);
        assert_eq!(
            api.headers().get(http::header::CACHE_CONTROL),
            Some(&HeaderValue::from_static("no-store"))
        );

        let missing_api = fixture
            .router
            .clone()
            .oneshot(fixture.request(Method::GET, "/api/not-a-route"))
            .await
            .expect("call missing API route");
        assert_eq!(missing_api.status(), StatusCode::NOT_FOUND);
        assert_common_policy(&missing_api);
        assert_eq!(
            missing_api.headers().get(http::header::CACHE_CONTROL),
            Some(&HeaderValue::from_static("no-store"))
        );

        let html = fixture
            .router
            .clone()
            .oneshot(fixture.request(Method::GET, "/page"))
            .await
            .expect("call HTML route");
        assert_common_policy(&html);
        assert_eq!(
            html.headers().get(http::header::CACHE_CONTROL),
            Some(&HeaderValue::from_static("no-store"))
        );

        let wrong_host = http::Request::builder()
            .uri("/api/ping")
            .header(http::header::HOST, "localhost:43121")
            .body(Body::empty())
            .expect("build wrong-host request");
        let error = fixture
            .router
            .clone()
            .oneshot(wrong_host)
            .await
            .expect("call API with wrong Host");
        assert_eq!(error.status(), StatusCode::FORBIDDEN);
        assert_common_policy(&error);
        assert_eq!(
            error.headers().get(http::header::CACHE_CONTROL),
            Some(&HeaderValue::from_static("no-store"))
        );
    }

    #[tokio::test]
    async fn starting_blocks_reopen_and_api_after_exact_host_validation() {
        let fixture = RuntimeFixture::new();
        let reopen = fixture
            .router
            .clone()
            .oneshot(fixture.request(Method::POST, LOCAL_REOPEN_PATH))
            .await
            .expect("call reopen while starting");
        assert_eq!(reopen.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error_code(reopen).await, "APP_STARTING");

        let api = fixture
            .router
            .clone()
            .oneshot(fixture.request(Method::GET, "/api/ping"))
            .await
            .expect("call API while starting");
        assert_eq!(api.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error_code(api).await, "APP_STARTING");

        let wrong_host = http::Request::builder()
            .uri("/api/ping")
            .header(http::header::HOST, "localhost:43121")
            .body(Body::empty())
            .expect("build wrong-host request");
        let response = fixture
            .router
            .clone()
            .oneshot(wrong_host)
            .await
            .expect("call API with wrong Host");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(error_code(response).await, "SECURITY_INVALID_HOST");
    }

    #[tokio::test]
    async fn launcher_endpoints_reject_wrong_or_duplicated_security_headers() {
        let fixture = RuntimeFixture::new();
        let wrong_secret = http::Request::builder()
            .uri(LOCAL_READY_PATH)
            .header(http::header::HOST, &fixture.host)
            .header("x-launcher-secret", "not-a-valid-secret")
            .body(Body::empty())
            .expect("build wrong-secret request");
        let response = fixture
            .router
            .clone()
            .oneshot(wrong_secret)
            .await
            .expect("call with wrong launcher secret");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            error_code(response).await,
            "SECURITY_INVALID_LAUNCHER_SECRET"
        );

        let mut duplicated = fixture.request(Method::GET, LOCAL_READY_PATH);
        duplicated.headers_mut().append(
            "x-launcher-secret",
            HeaderValue::from_str(&fixture.launcher_secret).expect("launcher header value"),
        );
        let response = fixture
            .router
            .clone()
            .oneshot(duplicated)
            .await
            .expect("call with duplicate launcher secret");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error_code(response).await, "SECURITY_DUPLICATE_HEADER");
    }

    #[tokio::test]
    async fn ready_reopen_issues_a_fresh_two_minute_fragment_url_and_opens_api_gate() {
        let fixture = RuntimeFixture::new();
        assert!(fixture.phase.mark_ready());
        let requested_at = time::OffsetDateTime::now_utc();

        let response = fixture
            .router
            .clone()
            .oneshot(fixture.request(Method::POST, LOCAL_REOPEN_PATH))
            .await
            .expect("call ready reopen");
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(body.as_object().expect("reopen object").len(), 2);
        let url = body["url"].as_str().expect("fragment URL");
        let expected_prefix = format!("{}/#token=", fixture.security.public_origin());
        let token = url
            .strip_prefix(&expected_prefix)
            .expect("URL uses exact public origin and token fragment");
        assert!(!token.is_empty());
        let expires_at = UtcTimestamp::parse_rfc3339(
            body["expires_at"]
                .as_str()
                .expect("RFC3339 expiration string"),
        )
        .expect("parse RFC3339 expiration");
        let observed_at = time::OffsetDateTime::now_utc();
        assert!(
            expires_at.as_offset_date_time()
                >= requested_at.saturating_add(time::Duration::seconds(119))
        );
        assert!(
            expires_at.as_offset_date_time()
                <= observed_at.saturating_add(time::Duration::seconds(121))
        );

        let api = fixture
            .router
            .clone()
            .oneshot(fixture.request(Method::GET, "/api/ping"))
            .await
            .expect("call API after ready");
        assert_eq!(api.status(), StatusCode::NO_CONTENT);
    }
}
