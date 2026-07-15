use std::fmt;
use std::pin::Pin;

use coding_agent_domain::{RepositoryId, TaskId};
use futures_util::Stream;

use crate::{
    AddRepositoryRequest, ApiResult, BootstrapResponse, CreateTaskRequest, RepositoryDto,
    ServiceStateControl, TaskDetailDto, TaskDto, TaskEventDto,
};

#[derive(Clone, PartialEq, Eq)]
pub struct AuthContext {
    pub session_id: String,
}

impl fmt::Debug for AuthContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthContext")
            .field("session_id", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SessionExchange {
    pub set_cookie: http::HeaderValue,
}

impl fmt::Debug for SessionExchange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionExchange")
            .field("set_cookie", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateResult<T> {
    Created(T),
    Existing(T),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelResult {
    Finished(TaskDto),
    Accepted { task: TaskDto },
}

pub struct QuitAcceptance {
    trigger_after_response: Option<Box<dyn FnOnce() + Send + 'static>>,
}

impl QuitAcceptance {
    pub fn new(trigger_after_response: impl FnOnce() + Send + 'static) -> Self {
        Self {
            trigger_after_response: Some(Box::new(trigger_after_response)),
        }
    }

    pub fn take_trigger(&mut self) -> Option<Box<dyn FnOnce() + Send + 'static>> {
        self.trigger_after_response.take()
    }
}

#[async_trait::async_trait]
pub trait ApiBackend: Send + Sync + 'static {
    async fn bootstrap(&self, auth: &AuthContext) -> ApiResult<BootstrapResponse>;
    async fn list_repositories(&self, auth: &AuthContext) -> ApiResult<Vec<RepositoryDto>>;
    async fn add_repository(
        &self,
        auth: &AuthContext,
        request: AddRepositoryRequest,
    ) -> ApiResult<CreateResult<RepositoryDto>>;
    async fn pick_repository(
        &self,
        auth: &AuthContext,
    ) -> ApiResult<Option<CreateResult<RepositoryDto>>>;
    async fn list_tasks(
        &self,
        auth: &AuthContext,
        repository_id: Option<RepositoryId>,
    ) -> ApiResult<Vec<TaskDto>>;
    async fn create_task(
        &self,
        auth: &AuthContext,
        request: CreateTaskRequest,
    ) -> ApiResult<CreateResult<TaskDto>>;
    async fn task_detail(&self, auth: &AuthContext, id: TaskId) -> ApiResult<TaskDetailDto>;
    async fn cancel_task(&self, auth: &AuthContext, id: TaskId) -> ApiResult<CancelResult>;
    async fn retry_task(&self, auth: &AuthContext, id: TaskId) -> ApiResult<CreateResult<TaskDto>>;
    async fn task_events(
        &self,
        auth: &AuthContext,
        id: TaskId,
        after: i64,
    ) -> ApiResult<Vec<TaskEventDto>>;
    async fn request_quit(&self, auth: &AuthContext) -> ApiResult<QuitAcceptance>;
}

// The port shape intentionally carries the typed event directly; boxing it would make every
// producer and consumer depend on an allocation that is not part of the approved contract.
#[allow(clippy::large_enum_variant)]
pub enum LiveEventItem {
    Event(TaskEventDto),
    Lagged,
}

pub type LiveEventStream = Pin<Box<dyn Stream<Item = LiveEventItem> + Send + 'static>>;
pub type ServiceStateStream = Pin<Box<dyn Stream<Item = ServiceStateControl> + Send + 'static>>;

#[async_trait::async_trait]
pub trait SseBackend: Send + Sync + 'static {
    fn subscribe_live(&self) -> LiveEventStream;
    fn subscribe_service_state(&self) -> ServiceStateStream;
    async fn current_service_state(&self) -> ApiResult<ServiceStateControl>;
    async fn latest_event_id(&self) -> ApiResult<i64>;
    async fn events_between(
        &self,
        after: i64,
        through: i64,
        limit: usize,
    ) -> ApiResult<Vec<TaskEventDto>>;
}

#[async_trait::async_trait]
pub trait RequestSecurity: Send + Sync + 'static {
    async fn exchange(
        &self,
        parts: &http::request::Parts,
        token: &str,
    ) -> ApiResult<SessionExchange>;
    fn authorize_read(&self, parts: &http::request::Parts) -> ApiResult<AuthContext>;
    fn authorize_mutation(&self, parts: &http::request::Parts) -> ApiResult<AuthContext>;
    fn expected_public_origin(&self) -> &str;
}
