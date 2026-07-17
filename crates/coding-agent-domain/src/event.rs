use crate::{EventId, Task, TaskFailure, TaskId, UtcTimestamp};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PlanSnapshot {
    pub revision: u64,
    pub items: Vec<PlanItem>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PlanItem {
    pub id: String,
    pub title: String,
    pub status: PlanItemStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanItemStatus {
    Pending,
    Running,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ActivityEntry {
    pub id: String,
    pub level: ActivityLevel,
    pub message: String,
    pub created_at: UtcTimestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityLevel {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DiffSnapshot {
    pub revision: u64,
    pub files: Vec<DiffFile>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DiffFile {
    pub path: String,
    pub status: DiffFileStatus,
    pub patch: String,
    pub additions: u64,
    pub deletions: u64,
    #[serde(default)]
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffFileStatus {
    Added,
    Modified,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TestSnapshot {
    pub revision: u64,
    pub status: TestStatus,
    pub cases: Vec<TestCase>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TestCase {
    pub id: String,
    pub name: String,
    pub status: TestStatus,
    pub duration_ms: u64,
    pub summary: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TestStatus {
    Queued,
    Running,
    Passed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TaskEventKind {
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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", content = "payload")]
pub enum TaskEventPayload {
    #[serde(rename = "task.queued")]
    TaskQueued { task: Task },
    #[serde(rename = "task.started")]
    TaskStarted { task: Task },
    #[serde(rename = "plan.updated")]
    PlanUpdated { plan: PlanSnapshot },
    #[serde(rename = "activity.appended")]
    ActivityAppended { entry: ActivityEntry },
    #[serde(rename = "diff.updated")]
    DiffUpdated { diff: DiffSnapshot },
    #[serde(rename = "test.updated")]
    TestUpdated { tests: TestSnapshot },
    #[serde(rename = "task.completed")]
    TaskCompleted { task: Task },
    #[serde(rename = "task.failed")]
    TaskFailed { task: Task },
    #[serde(rename = "task.cancelled")]
    TaskCancelled { task: Task },
    #[serde(rename = "task.interrupted")]
    TaskInterrupted { task: Task },
}

impl TaskEventPayload {
    pub const fn kind(&self) -> TaskEventKind {
        match self {
            Self::TaskQueued { .. } => TaskEventKind::TaskQueued,
            Self::TaskStarted { .. } => TaskEventKind::TaskStarted,
            Self::PlanUpdated { .. } => TaskEventKind::PlanUpdated,
            Self::ActivityAppended { .. } => TaskEventKind::ActivityAppended,
            Self::DiffUpdated { .. } => TaskEventKind::DiffUpdated,
            Self::TestUpdated { .. } => TaskEventKind::TestUpdated,
            Self::TaskCompleted { .. } => TaskEventKind::TaskCompleted,
            Self::TaskFailed { .. } => TaskEventKind::TaskFailed,
            Self::TaskCancelled { .. } => TaskEventKind::TaskCancelled,
            Self::TaskInterrupted { .. } => TaskEventKind::TaskInterrupted,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TaskEvent {
    pub id: EventId,
    pub schema_version: u16,
    pub task_id: TaskId,
    #[serde(flatten)]
    pub payload: TaskEventPayload,
    pub created_at: UtcTimestamp,
}

impl TaskEvent {
    pub fn new(
        id: EventId,
        task_id: TaskId,
        payload: TaskEventPayload,
        created_at: UtcTimestamp,
    ) -> Self {
        Self {
            id,
            schema_version: 1,
            task_id,
            payload,
            created_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TimelineEntry {
    pub event_id: EventId,
    pub kind: TaskEventKind,
    pub label: String,
    pub created_at: UtcTimestamp,
    pub failure: Option<TaskFailure>,
}
