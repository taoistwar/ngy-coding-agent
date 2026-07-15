use std::fmt;
use std::path::PathBuf;

use coding_agent_domain::{
    ActivityEntry, ActivityLevel, CanonicalPath, ClientRequestId, DiffFile, DiffFileStatus,
    DiffSnapshot, PlanItem, PlanItemStatus, PlanSnapshot, Repository, RepositoryId, Task,
    TaskEvent, TaskEventKind, TaskEventPayload, TaskFailure, TaskId, TaskStatus, TestCase,
    TestSnapshot, TestStatus, TimelineEntry, UtcTimestamp,
};
use serde::Serialize;
use utoipa::openapi::Ref;
use utoipa::openapi::schema::{Discriminator, OneOfBuilder, Schema};
use utoipa::{OpenApi, PartialSchema, ToSchema};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(transparent)]
#[schema(value_type = String, format = DateTime)]
pub struct UtcTimestampDto(String);

impl From<UtcTimestamp> for UtcTimestampDto {
    fn from(value: UtcTimestamp) -> Self {
        Self(value.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(transparent)]
#[schema(value_type = String)]
pub struct CanonicalPathDto(String);

impl From<CanonicalPath> for CanonicalPathDto {
    fn from(value: CanonicalPath) -> Self {
        Self(value.as_path().to_string_lossy().into_owned())
    }
}

#[derive(Clone, PartialEq, Eq, serde::Deserialize, ToSchema)]
pub struct SessionExchangeRequest {
    pub token: String,
}

impl fmt::Debug for SessionExchangeRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionExchangeRequest")
            .field("token", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, ToSchema)]
pub struct AddRepositoryRequest {
    #[schema(value_type = String)]
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, ToSchema)]
pub struct CreateTaskRequest {
    #[schema(value_type = uuid::Uuid)]
    pub client_request_id: ClientRequestId,
    #[schema(value_type = uuid::Uuid)]
    pub repository_id: RepositoryId,
    pub prompt: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct RepositoryDto {
    pub id: uuid::Uuid,
    pub selected_path: CanonicalPathDto,
    pub display_name: String,
    pub git_root: CanonicalPathDto,
    pub cargo_workspace_root: CanonicalPathDto,
    pub created_at: UtcTimestampDto,
    pub last_opened_at: UtcTimestampDto,
}

impl From<Repository> for RepositoryDto {
    fn from(value: Repository) -> Self {
        Self {
            id: value.id.as_uuid(),
            selected_path: value.selected_path.into(),
            display_name: value.display_name,
            git_root: value.git_root.into(),
            cargo_workspace_root: value.cargo_workspace_root.into(),
            created_at: value.created_at.into(),
            last_opened_at: value.last_opened_at.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatusDto {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
    Interrupted,
}

impl From<TaskStatus> for TaskStatusDto {
    fn from(value: TaskStatus) -> Self {
        match value {
            TaskStatus::Queued => Self::Queued,
            TaskStatus::Running => Self::Running,
            TaskStatus::Completed => Self::Completed,
            TaskStatus::Failed => Self::Failed,
            TaskStatus::Cancelled => Self::Cancelled,
            TaskStatus::Interrupted => Self::Interrupted,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct TaskFailureDto {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

impl From<TaskFailure> for TaskFailureDto {
    fn from(value: TaskFailure) -> Self {
        Self {
            code: value.code,
            message: value.message,
            retryable: value.retryable,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct TaskDto {
    pub id: uuid::Uuid,
    pub client_request_id: uuid::Uuid,
    pub repository_id: uuid::Uuid,
    pub prompt: String,
    pub status: TaskStatusDto,
    pub attempt: u32,
    pub retry_of: Option<uuid::Uuid>,
    pub created_at: UtcTimestampDto,
    pub started_at: Option<UtcTimestampDto>,
    pub finished_at: Option<UtcTimestampDto>,
    pub last_event_id: i64,
    pub failure: Option<TaskFailureDto>,
}

impl From<Task> for TaskDto {
    fn from(value: Task) -> Self {
        Self {
            id: value.id.as_uuid(),
            client_request_id: value.client_request_id.as_uuid(),
            repository_id: value.repository_id.as_uuid(),
            prompt: value.prompt,
            status: value.status.into(),
            attempt: value.attempt,
            retry_of: value.retry_of.map(TaskId::as_uuid),
            created_at: value.created_at.into(),
            started_at: value.started_at.map(Into::into),
            finished_at: value.finished_at.map(Into::into),
            last_event_id: value.last_event_id.get(),
            failure: value.failure.map(Into::into),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct PlanSnapshotDto {
    pub revision: u64,
    pub items: Vec<PlanItemDto>,
}

impl From<PlanSnapshot> for PlanSnapshotDto {
    fn from(value: PlanSnapshot) -> Self {
        Self {
            revision: value.revision,
            items: value.items.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct PlanItemDto {
    pub id: String,
    pub title: String,
    pub status: PlanItemStatusDto,
}

impl From<PlanItem> for PlanItemDto {
    fn from(value: PlanItem) -> Self {
        Self {
            id: value.id,
            title: value.title,
            status: value.status.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PlanItemStatusDto {
    Pending,
    Running,
    Completed,
}

impl From<PlanItemStatus> for PlanItemStatusDto {
    fn from(value: PlanItemStatus) -> Self {
        match value {
            PlanItemStatus::Pending => Self::Pending,
            PlanItemStatus::Running => Self::Running,
            PlanItemStatus::Completed => Self::Completed,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct ActivityEntryDto {
    pub id: String,
    pub level: ActivityLevelDto,
    pub message: String,
    pub created_at: UtcTimestampDto,
}

impl From<ActivityEntry> for ActivityEntryDto {
    fn from(value: ActivityEntry) -> Self {
        Self {
            id: value.id,
            level: value.level.into(),
            message: value.message,
            created_at: value.created_at.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ActivityLevelDto {
    Info,
    Warning,
    Error,
}

impl From<ActivityLevel> for ActivityLevelDto {
    fn from(value: ActivityLevel) -> Self {
        match value {
            ActivityLevel::Info => Self::Info,
            ActivityLevel::Warning => Self::Warning,
            ActivityLevel::Error => Self::Error,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct DiffSnapshotDto {
    pub revision: u64,
    pub files: Vec<DiffFileDto>,
}

impl From<DiffSnapshot> for DiffSnapshotDto {
    fn from(value: DiffSnapshot) -> Self {
        Self {
            revision: value.revision,
            files: value.files.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct DiffFileDto {
    pub path: String,
    pub status: DiffFileStatusDto,
    pub patch: String,
    pub additions: u64,
    pub deletions: u64,
}

impl From<DiffFile> for DiffFileDto {
    fn from(value: DiffFile) -> Self {
        Self {
            path: value.path,
            status: value.status.into(),
            patch: value.patch,
            additions: value.additions,
            deletions: value.deletions,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DiffFileStatusDto {
    Added,
    Modified,
    Deleted,
}

impl From<DiffFileStatus> for DiffFileStatusDto {
    fn from(value: DiffFileStatus) -> Self {
        match value {
            DiffFileStatus::Added => Self::Added,
            DiffFileStatus::Modified => Self::Modified,
            DiffFileStatus::Deleted => Self::Deleted,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct TestSnapshotDto {
    pub revision: u64,
    pub status: TestStatusDto,
    pub cases: Vec<TestCaseDto>,
}

impl From<TestSnapshot> for TestSnapshotDto {
    fn from(value: TestSnapshot) -> Self {
        Self {
            revision: value.revision,
            status: value.status.into(),
            cases: value.cases.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct TestCaseDto {
    pub id: String,
    pub name: String,
    pub status: TestStatusDto,
    pub duration_ms: u64,
    pub summary: String,
}

impl From<TestCase> for TestCaseDto {
    fn from(value: TestCase) -> Self {
        Self {
            id: value.id,
            name: value.name,
            status: value.status.into(),
            duration_ms: value.duration_ms,
            summary: value.summary,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TestStatusDto {
    Queued,
    Running,
    Passed,
    Failed,
    Cancelled,
}

impl From<TestStatus> for TestStatusDto {
    fn from(value: TestStatus) -> Self {
        match value {
            TestStatus::Queued => Self::Queued,
            TestStatus::Running => Self::Running,
            TestStatus::Passed => Self::Passed,
            TestStatus::Failed => Self::Failed,
            TestStatus::Cancelled => Self::Cancelled,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
pub enum TaskEventKindDto {
    #[serde(rename = "task.queued")]
    TaskQueued,
    #[serde(rename = "task.started")]
    TaskStarted,
    #[serde(rename = "plan.updated")]
    PlanUpdated,
    #[serde(rename = "activity.appended")]
    ActivityAppended,
    #[serde(rename = "diff.updated")]
    DiffUpdated,
    #[serde(rename = "test.updated")]
    TestUpdated,
    #[serde(rename = "task.completed")]
    TaskCompleted,
    #[serde(rename = "task.failed")]
    TaskFailed,
    #[serde(rename = "task.cancelled")]
    TaskCancelled,
    #[serde(rename = "task.interrupted")]
    TaskInterrupted,
}

impl From<TaskEventKind> for TaskEventKindDto {
    fn from(value: TaskEventKind) -> Self {
        match value {
            TaskEventKind::TaskQueued => Self::TaskQueued,
            TaskEventKind::TaskStarted => Self::TaskStarted,
            TaskEventKind::PlanUpdated => Self::PlanUpdated,
            TaskEventKind::ActivityAppended => Self::ActivityAppended,
            TaskEventKind::DiffUpdated => Self::DiffUpdated,
            TaskEventKind::TestUpdated => Self::TestUpdated,
            TaskEventKind::TaskCompleted => Self::TaskCompleted,
            TaskEventKind::TaskFailed => Self::TaskFailed,
            TaskEventKind::TaskCancelled => Self::TaskCancelled,
            TaskEventKind::TaskInterrupted => Self::TaskInterrupted,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct TimelineEntryDto {
    pub event_id: i64,
    pub kind: TaskEventKindDto,
    pub label: String,
    pub created_at: UtcTimestampDto,
    pub failure: Option<TaskFailureDto>,
}

impl From<TimelineEntry> for TimelineEntryDto {
    fn from(value: TimelineEntry) -> Self {
        Self {
            event_id: value.event_id.get(),
            kind: value.kind.into(),
            label: value.label,
            created_at: value.created_at.into(),
            failure: value.failure.map(Into::into),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct TaskDetailDto {
    pub task: TaskDto,
    #[schema(nullable = true)]
    pub plan: Option<PlanSnapshotDto>,
    pub activity: Vec<ActivityEntryDto>,
    #[schema(nullable = true)]
    pub diff: Option<DiffSnapshotDto>,
    #[schema(nullable = true)]
    pub tests: Option<TestSnapshotDto>,
    pub timeline: Vec<TimelineEntryDto>,
    pub event_cursor: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct TaskLifecyclePayloadDto {
    pub task: TaskDto,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct PlanUpdatedPayloadDto {
    pub plan: PlanSnapshotDto,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct ActivityAppendedPayloadDto {
    pub entry: ActivityEntryDto,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct DiffUpdatedPayloadDto {
    pub diff: DiffSnapshotDto,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct TestUpdatedPayloadDto {
    pub tests: TestSnapshotDto,
}

macro_rules! event_kind {
    ($name:ident, $variant:ident, $wire:literal) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
        pub enum $name {
            #[serde(rename = $wire)]
            #[schema(rename = $wire)]
            $variant,
        }
    };
}

event_kind!(TaskQueuedKind, TaskQueued, "task.queued");
event_kind!(TaskStartedKind, TaskStarted, "task.started");
event_kind!(PlanUpdatedKind, PlanUpdated, "plan.updated");
event_kind!(ActivityAppendedKind, ActivityAppended, "activity.appended");
event_kind!(DiffUpdatedKind, DiffUpdated, "diff.updated");
event_kind!(TestUpdatedKind, TestUpdated, "test.updated");
event_kind!(TaskCompletedKind, TaskCompleted, "task.completed");
event_kind!(TaskFailedKind, TaskFailed, "task.failed");
event_kind!(TaskCancelledKind, TaskCancelled, "task.cancelled");
event_kind!(TaskInterruptedKind, TaskInterrupted, "task.interrupted");

macro_rules! event_envelope {
    ($name:ident, $kind:ty, $payload:ty) => {
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
        pub struct $name {
            pub id: i64,
            pub schema_version: u16,
            pub task_id: uuid::Uuid,
            pub kind: $kind,
            pub payload: $payload,
            pub created_at: UtcTimestampDto,
        }
    };
}

event_envelope!(TaskQueuedEventDto, TaskQueuedKind, TaskLifecyclePayloadDto);
event_envelope!(
    TaskStartedEventDto,
    TaskStartedKind,
    TaskLifecyclePayloadDto
);
event_envelope!(PlanUpdatedEventDto, PlanUpdatedKind, PlanUpdatedPayloadDto);
event_envelope!(
    ActivityAppendedEventDto,
    ActivityAppendedKind,
    ActivityAppendedPayloadDto
);
event_envelope!(DiffUpdatedEventDto, DiffUpdatedKind, DiffUpdatedPayloadDto);
event_envelope!(TestUpdatedEventDto, TestUpdatedKind, TestUpdatedPayloadDto);
event_envelope!(
    TaskCompletedEventDto,
    TaskCompletedKind,
    TaskLifecyclePayloadDto
);
event_envelope!(TaskFailedEventDto, TaskFailedKind, TaskLifecyclePayloadDto);
event_envelope!(
    TaskCancelledEventDto,
    TaskCancelledKind,
    TaskLifecyclePayloadDto
);
event_envelope!(
    TaskInterruptedEventDto,
    TaskInterruptedKind,
    TaskLifecyclePayloadDto
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum TaskEventDto {
    TaskQueued(TaskQueuedEventDto),
    TaskStarted(TaskStartedEventDto),
    PlanUpdated(PlanUpdatedEventDto),
    ActivityAppended(ActivityAppendedEventDto),
    DiffUpdated(DiffUpdatedEventDto),
    TestUpdated(TestUpdatedEventDto),
    TaskCompleted(TaskCompletedEventDto),
    TaskFailed(TaskFailedEventDto),
    TaskCancelled(TaskCancelledEventDto),
    TaskInterrupted(TaskInterruptedEventDto),
}

impl TaskEventDto {
    pub(crate) const fn id(&self) -> i64 {
        match self {
            Self::TaskQueued(event) => event.id,
            Self::TaskStarted(event) => event.id,
            Self::PlanUpdated(event) => event.id,
            Self::ActivityAppended(event) => event.id,
            Self::DiffUpdated(event) => event.id,
            Self::TestUpdated(event) => event.id,
            Self::TaskCompleted(event) => event.id,
            Self::TaskFailed(event) => event.id,
            Self::TaskCancelled(event) => event.id,
            Self::TaskInterrupted(event) => event.id,
        }
    }

    pub(crate) const fn event_name(&self) -> &'static str {
        match self {
            Self::TaskQueued(_) => "task.queued",
            Self::TaskStarted(_) => "task.started",
            Self::PlanUpdated(_) => "plan.updated",
            Self::ActivityAppended(_) => "activity.appended",
            Self::DiffUpdated(_) => "diff.updated",
            Self::TestUpdated(_) => "test.updated",
            Self::TaskCompleted(_) => "task.completed",
            Self::TaskFailed(_) => "task.failed",
            Self::TaskCancelled(_) => "task.cancelled",
            Self::TaskInterrupted(_) => "task.interrupted",
        }
    }
}

impl PartialSchema for TaskEventDto {
    fn schema() -> utoipa::openapi::RefOr<Schema> {
        OneOfBuilder::new()
            .item(Ref::from_schema_name("TaskQueuedEventDto"))
            .item(Ref::from_schema_name("TaskStartedEventDto"))
            .item(Ref::from_schema_name("PlanUpdatedEventDto"))
            .item(Ref::from_schema_name("ActivityAppendedEventDto"))
            .item(Ref::from_schema_name("DiffUpdatedEventDto"))
            .item(Ref::from_schema_name("TestUpdatedEventDto"))
            .item(Ref::from_schema_name("TaskCompletedEventDto"))
            .item(Ref::from_schema_name("TaskFailedEventDto"))
            .item(Ref::from_schema_name("TaskCancelledEventDto"))
            .item(Ref::from_schema_name("TaskInterruptedEventDto"))
            .discriminator(Some(Discriminator::with_mapping(
                "kind",
                [
                    ("task.queued", "#/components/schemas/TaskQueuedEventDto"),
                    ("task.started", "#/components/schemas/TaskStartedEventDto"),
                    ("plan.updated", "#/components/schemas/PlanUpdatedEventDto"),
                    (
                        "activity.appended",
                        "#/components/schemas/ActivityAppendedEventDto",
                    ),
                    ("diff.updated", "#/components/schemas/DiffUpdatedEventDto"),
                    ("test.updated", "#/components/schemas/TestUpdatedEventDto"),
                    (
                        "task.completed",
                        "#/components/schemas/TaskCompletedEventDto",
                    ),
                    ("task.failed", "#/components/schemas/TaskFailedEventDto"),
                    (
                        "task.cancelled",
                        "#/components/schemas/TaskCancelledEventDto",
                    ),
                    (
                        "task.interrupted",
                        "#/components/schemas/TaskInterruptedEventDto",
                    ),
                ],
            )))
            .into()
    }
}

impl ToSchema for TaskEventDto {}

impl From<TaskEvent> for TaskEventDto {
    fn from(value: TaskEvent) -> Self {
        let id = value.id.get();
        let schema_version = value.schema_version;
        let task_id = value.task_id.as_uuid();
        let created_at = value.created_at.into();

        match value.payload {
            TaskEventPayload::TaskQueued { task } => Self::TaskQueued(TaskQueuedEventDto {
                id,
                schema_version,
                task_id,
                kind: TaskQueuedKind::TaskQueued,
                payload: TaskLifecyclePayloadDto { task: task.into() },
                created_at,
            }),
            TaskEventPayload::TaskStarted { task } => Self::TaskStarted(TaskStartedEventDto {
                id,
                schema_version,
                task_id,
                kind: TaskStartedKind::TaskStarted,
                payload: TaskLifecyclePayloadDto { task: task.into() },
                created_at,
            }),
            TaskEventPayload::PlanUpdated { plan } => Self::PlanUpdated(PlanUpdatedEventDto {
                id,
                schema_version,
                task_id,
                kind: PlanUpdatedKind::PlanUpdated,
                payload: PlanUpdatedPayloadDto { plan: plan.into() },
                created_at,
            }),
            TaskEventPayload::ActivityAppended { entry } => {
                Self::ActivityAppended(ActivityAppendedEventDto {
                    id,
                    schema_version,
                    task_id,
                    kind: ActivityAppendedKind::ActivityAppended,
                    payload: ActivityAppendedPayloadDto {
                        entry: entry.into(),
                    },
                    created_at,
                })
            }
            TaskEventPayload::DiffUpdated { diff } => Self::DiffUpdated(DiffUpdatedEventDto {
                id,
                schema_version,
                task_id,
                kind: DiffUpdatedKind::DiffUpdated,
                payload: DiffUpdatedPayloadDto { diff: diff.into() },
                created_at,
            }),
            TaskEventPayload::TestUpdated { tests } => Self::TestUpdated(TestUpdatedEventDto {
                id,
                schema_version,
                task_id,
                kind: TestUpdatedKind::TestUpdated,
                payload: TestUpdatedPayloadDto {
                    tests: tests.into(),
                },
                created_at,
            }),
            TaskEventPayload::TaskCompleted { task } => {
                Self::TaskCompleted(TaskCompletedEventDto {
                    id,
                    schema_version,
                    task_id,
                    kind: TaskCompletedKind::TaskCompleted,
                    payload: TaskLifecyclePayloadDto { task: task.into() },
                    created_at,
                })
            }
            TaskEventPayload::TaskFailed { task } => Self::TaskFailed(TaskFailedEventDto {
                id,
                schema_version,
                task_id,
                kind: TaskFailedKind::TaskFailed,
                payload: TaskLifecyclePayloadDto { task: task.into() },
                created_at,
            }),
            TaskEventPayload::TaskCancelled { task } => {
                Self::TaskCancelled(TaskCancelledEventDto {
                    id,
                    schema_version,
                    task_id,
                    kind: TaskCancelledKind::TaskCancelled,
                    payload: TaskLifecyclePayloadDto { task: task.into() },
                    created_at,
                })
            }
            TaskEventPayload::TaskInterrupted { task } => {
                Self::TaskInterrupted(TaskInterruptedEventDto {
                    id,
                    schema_version,
                    task_id,
                    kind: TaskInterruptedKind::TaskInterrupted,
                    payload: TaskLifecyclePayloadDto { task: task.into() },
                    created_at,
                })
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, ToSchema)]
pub enum StreamResetKind {
    #[serde(rename = "stream.reset")]
    StreamReset,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct StreamResetControl {
    pub schema_version: u16,
    pub kind: StreamResetKind,
    pub latest_event_id: i64,
}

impl StreamResetControl {
    pub const fn new(latest_event_id: i64) -> Self {
        Self {
            schema_version: 1,
            kind: StreamResetKind::StreamReset,
            latest_event_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, ToSchema)]
pub enum ServiceStateKind {
    #[serde(rename = "service.state")]
    ServiceState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ServiceStateDto {
    Ready,
    StoreDegraded,
    Quiescing,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct ServiceStateControl {
    pub schema_version: u16,
    pub kind: ServiceStateKind,
    pub state: ServiceStateDto,
    pub generation: u64,
}

impl ServiceStateControl {
    pub const fn new(state: ServiceStateDto, generation: u64) -> Self {
        Self {
            schema_version: 1,
            kind: ServiceStateKind::ServiceState,
            state,
            generation,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(untagged)]
// The documented SSE wire union owns its typed event. Keeping it direct also lets Utoipa expose
// the approved oneOf without an implementation-only allocation wrapper.
#[allow(clippy::large_enum_variant)]
pub enum SseMessage {
    TaskEvent(TaskEventDto),
    StreamReset(StreamResetControl),
    ServiceState(ServiceStateControl),
}

#[derive(Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct BootstrapResponse {
    pub csrf_token: String,
    pub repositories: Vec<RepositoryDto>,
    pub tasks: Vec<TaskDto>,
    pub latest_event_id: i64,
    pub server_started_at: UtcTimestampDto,
    pub service_state: ServiceStateDto,
    pub service_state_generation: u64,
    pub max_concurrent_tasks: u32,
}

impl fmt::Debug for BootstrapResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BootstrapResponse")
            .field("contents", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct CancellationAcceptedResponse {
    pub task: TaskDto,
    pub cancellation_requested: bool,
}

impl CancellationAcceptedResponse {
    pub const fn new(task: TaskDto) -> Self {
        Self {
            task,
            cancellation_requested: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
pub enum QuitStatus {
    #[serde(rename = "shutting_down")]
    ShuttingDown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct QuitResponse {
    pub status: QuitStatus,
}

impl QuitResponse {
    pub const fn shutting_down() -> Self {
        Self {
            status: QuitStatus::ShuttingDown,
        }
    }
}

#[derive(OpenApi)]
#[openapi(components(schemas(
    UtcTimestampDto,
    CanonicalPathDto,
    SessionExchangeRequest,
    AddRepositoryRequest,
    CreateTaskRequest,
    RepositoryDto,
    TaskStatusDto,
    TaskFailureDto,
    TaskDto,
    PlanSnapshotDto,
    PlanItemDto,
    PlanItemStatusDto,
    ActivityEntryDto,
    ActivityLevelDto,
    DiffSnapshotDto,
    DiffFileDto,
    DiffFileStatusDto,
    TestSnapshotDto,
    TestCaseDto,
    TestStatusDto,
    TaskEventKindDto,
    TimelineEntryDto,
    TaskDetailDto,
    TaskLifecyclePayloadDto,
    PlanUpdatedPayloadDto,
    ActivityAppendedPayloadDto,
    DiffUpdatedPayloadDto,
    TestUpdatedPayloadDto,
    TaskQueuedKind,
    TaskStartedKind,
    PlanUpdatedKind,
    ActivityAppendedKind,
    DiffUpdatedKind,
    TestUpdatedKind,
    TaskCompletedKind,
    TaskFailedKind,
    TaskCancelledKind,
    TaskInterruptedKind,
    TaskQueuedEventDto,
    TaskStartedEventDto,
    PlanUpdatedEventDto,
    ActivityAppendedEventDto,
    DiffUpdatedEventDto,
    TestUpdatedEventDto,
    TaskCompletedEventDto,
    TaskFailedEventDto,
    TaskCancelledEventDto,
    TaskInterruptedEventDto,
    TaskEventDto,
    StreamResetKind,
    StreamResetControl,
    ServiceStateKind,
    ServiceStateDto,
    ServiceStateControl,
    SseMessage,
    BootstrapResponse,
    crate::ApiErrorResponse,
    CancellationAcceptedResponse,
    QuitStatus,
    QuitResponse,
)))]
pub struct ApiDoc;
