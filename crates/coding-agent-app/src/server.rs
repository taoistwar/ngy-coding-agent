use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use coding_agent_api::{
    AddRepositoryRequest, ApiBackend, ApiError, ApiResult, AuthContext, BootstrapResponse,
    CancelResult, CreateResult, CreateTaskRequest, LiveEventStream, QuitAcceptance, RepositoryDto,
    ServiceStateControl, ServiceStateDto, ServiceStateStream, SseBackend, TaskDetailDto, TaskDto,
    TaskEventDto,
};
use coding_agent_domain::{
    CanonicalPath, DomainError, EventCursor, NewRepository, NewTask, RepositoryId, TaskId,
    UtcTimestamp,
};
use coding_agent_store::{
    CreateTaskOutcome, RegisterRepositoryOutcome, RetryTaskOutcome, Store, StoreError,
};
use futures_util::stream;
use http::StatusCode;
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::{
    CancelOutcome, NativeDialogService, PickerError, RepositoryDiscovery, RepositoryDiscoveryError,
    SecurityManager, ServiceState, ServiceStateController, StoreWriterError, StoreWriterHandle,
    TaskManagerError, TaskManagerHandle,
};

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

    fn begin_quiescing(&self) -> bool {
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
        Box::pin(stream::empty())
    }

    fn subscribe_service_state(&self) -> ServiceStateStream {
        Box::pin(stream::empty())
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
        StoreWriterError::Busy => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "STORE_BUSY",
            "the local store is busy; retry the request",
            true,
        ),
        StoreWriterError::Store(error) => map_store_error(error),
        StoreWriterError::Closed => app_shutting_down(),
    }
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

fn app_shutting_down() -> ApiError {
    api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "APP_SHUTTING_DOWN",
        "the application is shutting down",
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
