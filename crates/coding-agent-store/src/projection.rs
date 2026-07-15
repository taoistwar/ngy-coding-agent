use std::path::PathBuf;

use coding_agent_domain::{
    ActivityEntry, CanonicalPath, DiffSnapshot, EventCursor, EventId, PlanSnapshot, Repository,
    Task, TaskEvent, TaskEventKind, TaskEventPayload, TaskFailure, TaskId, TaskStatus,
    TestSnapshot, TimelineEntry, UtcTimestamp,
};
use serde_json::Value;
use sqlx::SqliteConnection;

use crate::tasks::{
    TaskRecord, latest_event_cursor, load_task, parse_event_kind, task_from_record,
};
use crate::{Store, StoreError};

type RepositoryRecord = (String, String, String, String, String, String, String);
type EventRecord = (i64, i64, String, String, String, String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapSnapshot {
    pub repositories: Vec<Repository>,
    pub tasks: Vec<Task>,
    pub latest_event_id: EventCursor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskDetail {
    pub task: Task,
    pub plan: Option<PlanSnapshot>,
    pub activity: Vec<ActivityEntry>,
    pub diff: Option<DiffSnapshot>,
    pub tests: Option<TestSnapshot>,
    pub timeline: Vec<TimelineEntry>,
    pub event_cursor: EventCursor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventPage {
    pub events: Vec<TaskEvent>,
    pub high_watermark: EventCursor,
}

impl Store {
    pub async fn bootstrap_snapshot(&self) -> Result<BootstrapSnapshot, StoreError> {
        let mut transaction = self.pool.begin().await?;
        let repository_records: Vec<RepositoryRecord> = sqlx::query_as(
            "SELECT id, selected_path, display_name, git_root, cargo_workspace_root, \
                    created_at, last_opened_at \
             FROM repositories \
             ORDER BY last_opened_at DESC, id",
        )
        .fetch_all(&mut *transaction)
        .await?;
        let repositories = repository_records
            .into_iter()
            .map(repository_from_record)
            .collect::<Result<Vec<_>, _>>()?;

        let task_records: Vec<TaskRecord> = sqlx::query_as(
            "SELECT id, client_request_id, repository_id, prompt, status, attempt, retry_of, \
                    created_at, started_at, finished_at, last_event_id, failure_json \
             FROM tasks \
             ORDER BY created_at DESC, id",
        )
        .fetch_all(&mut *transaction)
        .await?;
        let tasks = task_records
            .into_iter()
            .map(task_from_record)
            .collect::<Result<Vec<_>, _>>()?;
        let latest_event_id = latest_event_cursor(&mut transaction).await?;
        transaction.commit().await?;

        Ok(BootstrapSnapshot {
            repositories,
            tasks,
            latest_event_id,
        })
    }

    pub async fn task_detail(&self, task_id: TaskId) -> Result<Option<TaskDetail>, StoreError> {
        let mut transaction = self.pool.begin().await?;
        let Some(task) = load_task(&mut transaction, task_id).await? else {
            transaction.commit().await?;
            return Ok(None);
        };
        let events = load_events(&mut transaction, Some(task_id), EventCursor::ZERO, None).await?;
        let event_cursor = latest_event_cursor(&mut transaction).await?;
        transaction.commit().await?;

        Ok(Some(project_task_detail(task, events, event_cursor)))
    }

    pub async fn events_after(
        &self,
        after: EventCursor,
        limit: usize,
    ) -> Result<EventPage, StoreError> {
        self.event_page(None, after, limit).await
    }

    pub async fn task_events_after(
        &self,
        task_id: TaskId,
        after: EventCursor,
        limit: usize,
    ) -> Result<EventPage, StoreError> {
        self.event_page(Some(task_id), after, limit).await
    }

    pub async fn latest_event_id(&self) -> Result<EventCursor, StoreError> {
        let mut connection = self.pool.acquire().await?;
        latest_event_cursor(&mut connection).await
    }

    async fn event_page(
        &self,
        task_id: Option<TaskId>,
        after: EventCursor,
        limit: usize,
    ) -> Result<EventPage, StoreError> {
        let mut transaction = self.pool.begin().await?;
        let high_watermark = latest_event_cursor(&mut transaction).await?;
        let events = load_events(
            &mut transaction,
            task_id,
            after,
            Some((high_watermark, limit)),
        )
        .await?;
        transaction.commit().await?;
        Ok(EventPage {
            events,
            high_watermark,
        })
    }
}

async fn load_events(
    connection: &mut SqliteConnection,
    task_id: Option<TaskId>,
    after: EventCursor,
    page: Option<(EventCursor, usize)>,
) -> Result<Vec<TaskEvent>, StoreError> {
    let records: Vec<EventRecord> = match (task_id, page) {
        (None, None) => {
            sqlx::query_as(
                "SELECT id, schema_version, task_id, kind, payload_json, created_at \
                 FROM task_events WHERE id > ? ORDER BY id",
            )
            .bind(after.get())
            .fetch_all(connection)
            .await?
        }
        (Some(task_id), None) => {
            sqlx::query_as(
                "SELECT id, schema_version, task_id, kind, payload_json, created_at \
                 FROM task_events WHERE task_id = ? AND id > ? ORDER BY id",
            )
            .bind(task_id.to_string())
            .bind(after.get())
            .fetch_all(connection)
            .await?
        }
        (None, Some((high_watermark, limit))) => {
            sqlx::query_as(
                "SELECT id, schema_version, task_id, kind, payload_json, created_at \
                 FROM task_events WHERE id > ? AND id <= ? ORDER BY id LIMIT ?",
            )
            .bind(after.get())
            .bind(high_watermark.get())
            .bind(limit_as_i64(limit))
            .fetch_all(connection)
            .await?
        }
        (Some(task_id), Some((high_watermark, limit))) => {
            sqlx::query_as(
                "SELECT id, schema_version, task_id, kind, payload_json, created_at \
                 FROM task_events \
                 WHERE task_id = ? AND id > ? AND id <= ? \
                 ORDER BY id LIMIT ?",
            )
            .bind(task_id.to_string())
            .bind(after.get())
            .bind(high_watermark.get())
            .bind(limit_as_i64(limit))
            .fetch_all(connection)
            .await?
        }
    };
    records.into_iter().map(event_from_record).collect()
}

fn event_from_record(record: EventRecord) -> Result<TaskEvent, StoreError> {
    let schema_version =
        u16::try_from(record.1).map_err(|_| StoreError::InvalidEventSchemaVersion(record.1))?;
    if schema_version != 1 {
        return Err(StoreError::InvalidEventSchemaVersion(record.1));
    }
    let event_id = EventId::new(record.0)?;
    let task_id = record.2.parse().map_err(StoreError::InvalidTaskId)?;
    let kind = parse_event_kind(&record.3)?;
    let payload_value: Value = serde_json::from_str(&record.4)?;
    let payload = payload_from_value(kind, &payload_value, task_id, event_id)?;
    Ok(TaskEvent {
        id: event_id,
        schema_version,
        task_id,
        payload,
        created_at: UtcTimestamp::parse_rfc3339(&record.5)?,
    })
}

fn payload_from_value(
    kind: TaskEventKind,
    payload: &Value,
    task_id: TaskId,
    event_id: EventId,
) -> Result<TaskEventPayload, StoreError> {
    match kind {
        TaskEventKind::TaskQueued => Ok(TaskEventPayload::TaskQueued {
            task: lifecycle_task(payload, task_id, event_id, TaskStatus::Queued)?,
        }),
        TaskEventKind::TaskStarted => Ok(TaskEventPayload::TaskStarted {
            task: lifecycle_task(payload, task_id, event_id, TaskStatus::Running)?,
        }),
        TaskEventKind::PlanUpdated => Ok(TaskEventPayload::PlanUpdated {
            plan: serde_json::from_value(payload_field(payload, "plan"))?,
        }),
        TaskEventKind::ActivityAppended => Ok(TaskEventPayload::ActivityAppended {
            entry: serde_json::from_value(payload_field(payload, "entry"))?,
        }),
        TaskEventKind::DiffUpdated => Ok(TaskEventPayload::DiffUpdated {
            diff: serde_json::from_value(payload_field(payload, "diff"))?,
        }),
        TaskEventKind::TestUpdated => Ok(TaskEventPayload::TestUpdated {
            tests: serde_json::from_value(payload_field(payload, "tests"))?,
        }),
        TaskEventKind::TaskCompleted => Ok(TaskEventPayload::TaskCompleted {
            task: lifecycle_task(payload, task_id, event_id, TaskStatus::Completed)?,
        }),
        TaskEventKind::TaskFailed => Ok(TaskEventPayload::TaskFailed {
            task: lifecycle_task(payload, task_id, event_id, TaskStatus::Failed)?,
        }),
        TaskEventKind::TaskCancelled => Ok(TaskEventPayload::TaskCancelled {
            task: lifecycle_task(payload, task_id, event_id, TaskStatus::Cancelled)?,
        }),
        TaskEventKind::TaskInterrupted => Ok(TaskEventPayload::TaskInterrupted {
            task: lifecycle_task(payload, task_id, event_id, TaskStatus::Interrupted)?,
        }),
    }
}

fn lifecycle_task(
    payload: &Value,
    task_id: TaskId,
    event_id: EventId,
    expected_status: TaskStatus,
) -> Result<Task, StoreError> {
    let task: Task = serde_json::from_value(payload_field(payload, "task"))?;
    let task = Task::try_from_stored(task)?;
    if task.id != task_id {
        return Err(StoreError::InvariantViolation(
            "lifecycle payload task ID does not match event task ID",
        ));
    }
    if task.last_event_id != event_id {
        return Err(StoreError::InvariantViolation(
            "lifecycle payload last event ID does not match event ID",
        ));
    }
    if task.status != expected_status {
        return Err(StoreError::InvariantViolation(
            "lifecycle payload task status does not match event kind",
        ));
    }
    Ok(task)
}

fn payload_field(payload: &Value, field: &str) -> Value {
    payload.get(field).cloned().unwrap_or(Value::Null)
}

fn project_task_detail(
    task: Task,
    events: Vec<TaskEvent>,
    event_cursor: EventCursor,
) -> TaskDetail {
    let mut detail = TaskDetail {
        task,
        plan: None,
        activity: Vec::new(),
        diff: None,
        tests: None,
        timeline: Vec::new(),
        event_cursor,
    };

    for event in events {
        match &event.payload {
            TaskEventPayload::PlanUpdated { plan } => detail.plan = Some(plan.clone()),
            TaskEventPayload::ActivityAppended { entry } => {
                if detail
                    .activity
                    .iter()
                    .all(|existing| existing.id != entry.id)
                {
                    detail.activity.push(entry.clone());
                }
            }
            TaskEventPayload::DiffUpdated { diff } => detail.diff = Some(diff.clone()),
            TaskEventPayload::TestUpdated { tests } => detail.tests = Some(tests.clone()),
            TaskEventPayload::TaskQueued { task }
            | TaskEventPayload::TaskStarted { task }
            | TaskEventPayload::TaskCompleted { task }
            | TaskEventPayload::TaskFailed { task }
            | TaskEventPayload::TaskCancelled { task }
            | TaskEventPayload::TaskInterrupted { task } => {
                detail
                    .timeline
                    .push(timeline_entry(&event, task.failure.clone()));
            }
        }
    }
    detail
}

fn timeline_entry(event: &TaskEvent, task_failure: Option<TaskFailure>) -> TimelineEntry {
    let kind = event.payload.kind();
    let failure = match kind {
        TaskEventKind::TaskFailed | TaskEventKind::TaskInterrupted => task_failure,
        TaskEventKind::TaskQueued
        | TaskEventKind::TaskStarted
        | TaskEventKind::PlanUpdated
        | TaskEventKind::ActivityAppended
        | TaskEventKind::DiffUpdated
        | TaskEventKind::TestUpdated
        | TaskEventKind::TaskCompleted
        | TaskEventKind::TaskCancelled => None,
    };
    TimelineEntry {
        event_id: event.id,
        kind,
        label: timeline_label(kind).to_owned(),
        created_at: event.created_at,
        failure,
    }
}

const fn timeline_label(kind: TaskEventKind) -> &'static str {
    match kind {
        TaskEventKind::TaskQueued => "Task queued",
        TaskEventKind::TaskStarted => "Task started",
        TaskEventKind::TaskCompleted => "Task completed",
        TaskEventKind::TaskFailed => "Task failed",
        TaskEventKind::TaskCancelled => "Task cancelled",
        TaskEventKind::TaskInterrupted => "Task interrupted",
        TaskEventKind::PlanUpdated
        | TaskEventKind::ActivityAppended
        | TaskEventKind::DiffUpdated
        | TaskEventKind::TestUpdated => "",
    }
}

fn repository_from_record(record: RepositoryRecord) -> Result<Repository, StoreError> {
    Ok(Repository {
        id: record.0.parse()?,
        selected_path: CanonicalPath::try_from_canonical(PathBuf::from(record.1))?,
        display_name: record.2,
        git_root: CanonicalPath::try_from_canonical(PathBuf::from(record.3))?,
        cargo_workspace_root: CanonicalPath::try_from_canonical(PathBuf::from(record.4))?,
        created_at: UtcTimestamp::parse_rfc3339(&record.5)?,
        last_opened_at: UtcTimestamp::parse_rfc3339(&record.6)?,
    })
}

fn limit_as_i64(limit: usize) -> i64 {
    i64::try_from(limit).unwrap_or(i64::MAX)
}
