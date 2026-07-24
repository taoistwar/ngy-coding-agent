use coding_agent_domain::{
    ClientRequestId, DeliveryReadiness, DomainError, EventCursor, EventId, NewTask, Task,
    TaskEventKind, TaskEventPayload, TaskFailure, TaskId, TaskStatus, UtcTimestamp,
};
use sqlx::{SqliteConnection, Transaction};
use time::OffsetDateTime;

use crate::{Store, StoreError};

pub(crate) type TaskRecord = (
    String,
    String,
    String,
    String,
    String,
    i64,
    Option<String>,
    String,
    Option<String>,
    Option<String>,
    i64,
    Option<String>,
    Option<String>,
);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskTransition {
    Running,
    Completed,
    Failed(TaskFailure),
    Cancelled,
    Interrupted(TaskFailure),
}

impl TaskTransition {
    pub const fn next(&self) -> TaskStatus {
        match self {
            Self::Running => TaskStatus::Running,
            Self::Completed => TaskStatus::Completed,
            Self::Failed(_) => TaskStatus::Failed,
            Self::Cancelled => TaskStatus::Cancelled,
            Self::Interrupted(_) => TaskStatus::Interrupted,
        }
    }

    pub const fn failure(&self) -> Option<&TaskFailure> {
        match self {
            Self::Failed(failure) | Self::Interrupted(failure) => Some(failure),
            Self::Running | Self::Completed | Self::Cancelled => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateTaskOutcome {
    Created { task: Task, event_id: EventId },
    Existing { task: Task },
}

impl CreateTaskOutcome {
    pub const fn task(&self) -> &Task {
        match self {
            Self::Created { task, .. } | Self::Existing { task } => task,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryTaskOutcome {
    Created { task: Task, event_id: EventId },
    Existing { task: Task },
}

impl RetryTaskOutcome {
    pub const fn task(&self) -> &Task {
        match self {
            Self::Created { task, .. } | Self::Existing { task } => task,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransitionOutcome {
    Applied { task: Task, event_id: EventId },
    Conflict { current: Task },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppendEventOutcome {
    Applied { event_id: EventId },
    NotRunning { current: Task },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryOutcome {
    pub interrupted_count: usize,
    pub first_event_id: Option<EventId>,
    pub last_event_id: Option<EventId>,
    pub high_watermark: EventCursor,
}

impl Store {
    pub async fn create_task(&self, input: NewTask) -> Result<CreateTaskOutcome, StoreError> {
        let input = NewTask::try_new(input.client_request_id, input.repository_id, input.prompt)?;
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;

        if let Some(existing) =
            load_task_by_client_request(&mut transaction, input.client_request_id).await?
        {
            if existing.repository_id != input.repository_id || existing.prompt != input.prompt {
                return Err(StoreError::IdempotencyConflict);
            }
            transaction.commit().await?;
            return Ok(CreateTaskOutcome::Existing { task: existing });
        }

        let now = current_timestamp()?;
        let task_id = TaskId::new();
        sqlx::query(
            "INSERT INTO tasks (\
                 id, client_request_id, repository_id, prompt, status, attempt, retry_of,\
                 created_at, started_at, finished_at, last_event_id, failure_json\
             ) VALUES (?, ?, ?, ?, 'queued', 1, NULL, ?, NULL, NULL, 0, NULL)",
        )
        .bind(task_id.to_string())
        .bind(input.client_request_id.to_string())
        .bind(input.repository_id.to_string())
        .bind(input.prompt)
        .bind(now.to_string())
        .execute(&mut *transaction)
        .await?;

        let (task, event_id) =
            append_lifecycle_event(&mut transaction, task_id, TaskEventKind::TaskQueued, now)
                .await?;
        transaction.commit().await?;
        Ok(CreateTaskOutcome::Created { task, event_id })
    }

    pub async fn retry_task(&self, source_id: TaskId) -> Result<RetryTaskOutcome, StoreError> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let source = load_task(&mut transaction, source_id)
            .await?
            .ok_or(StoreError::TaskNotFound)?;
        if !source.status.is_retryable() {
            return Err(StoreError::TaskNotRetryable);
        }

        if let Some(existing) = load_retry_child(&mut transaction, source_id).await? {
            transaction.commit().await?;
            return Ok(RetryTaskOutcome::Existing { task: existing });
        }

        let attempt = source
            .attempt
            .checked_add(1)
            .ok_or(StoreError::TaskAttemptOverflow)?;
        let now = current_timestamp()?;
        let task_id = TaskId::new();
        let client_request_id = ClientRequestId::new();
        sqlx::query(
            "INSERT INTO tasks (\
                 id, client_request_id, repository_id, prompt, status, attempt, retry_of,\
                 created_at, started_at, finished_at, last_event_id, failure_json\
             ) VALUES (?, ?, ?, ?, 'queued', ?, ?, ?, NULL, NULL, 0, NULL)",
        )
        .bind(task_id.to_string())
        .bind(client_request_id.to_string())
        .bind(source.repository_id.to_string())
        .bind(source.prompt)
        .bind(i64::from(attempt))
        .bind(source.id.to_string())
        .bind(now.to_string())
        .execute(&mut *transaction)
        .await?;

        let (task, event_id) =
            append_lifecycle_event(&mut transaction, task_id, TaskEventKind::TaskQueued, now)
                .await?;
        transaction.commit().await?;
        Ok(RetryTaskOutcome::Created { task, event_id })
    }

    pub async fn transition_with_event(
        &self,
        task_id: TaskId,
        expected: TaskStatus,
        transition: TaskTransition,
    ) -> Result<TransitionOutcome, StoreError> {
        let next = transition.next();
        if !expected.can_transition_to(next) {
            return Err(StoreError::IllegalTransition {
                from: expected,
                to: next,
            });
        }
        if matches!(transition, TaskTransition::Completed) {
            return Err(StoreError::InvariantViolation(
                "completed tasks require reviewed finalization",
            ));
        }

        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let now = current_timestamp()?;
        let failure_json = transition
            .failure()
            .map(serde_json::to_string)
            .transpose()?;
        let updated = match transition {
            TaskTransition::Running => {
                sqlx::query(
                    "UPDATE tasks \
                     SET status = 'running', started_at = ?, finished_at = NULL, failure_json = NULL \
                     WHERE id = ? AND status = ?",
                )
                .bind(now.to_string())
                .bind(task_id.to_string())
                .bind(status_text(expected))
                .execute(&mut *transaction)
                .await?
            }
            TaskTransition::Completed => {
                return Err(StoreError::InvariantViolation(
                    "completed tasks require reviewed finalization",
                ));
            }
            TaskTransition::Failed(_)
            | TaskTransition::Cancelled
            | TaskTransition::Interrupted(_) => {
                sqlx::query(
                    "UPDATE tasks \
                     SET status = ?, finished_at = ?, failure_json = ? \
                     WHERE id = ? AND status = ?",
                )
                .bind(status_text(next))
                .bind(now.to_string())
                .bind(failure_json)
                .bind(task_id.to_string())
                .bind(status_text(expected))
                .execute(&mut *transaction)
                .await?
            }
        };

        if updated.rows_affected() == 0 {
            let current = load_task(&mut transaction, task_id).await?;
            transaction.commit().await?;
            return current
                .map(|current| TransitionOutcome::Conflict { current })
                .ok_or(StoreError::TaskNotFound);
        }
        ensure_exactly_one(
            updated.rows_affected(),
            "task transition updated multiple rows",
        )?;

        let (task, event_id) =
            append_lifecycle_event(&mut transaction, task_id, lifecycle_kind(next), now).await?;
        transaction.commit().await?;
        Ok(TransitionOutcome::Applied { task, event_id })
    }

    pub async fn append_running_event(
        &self,
        task_id: TaskId,
        payload: TaskEventPayload,
    ) -> Result<AppendEventOutcome, StoreError> {
        if !matches!(
            payload,
            TaskEventPayload::PlanUpdated { .. }
                | TaskEventPayload::ActivityAppended { .. }
                | TaskEventPayload::DiffUpdated { .. }
                | TaskEventPayload::TestUpdated { .. }
        ) {
            return Err(StoreError::InvalidRunningEvent);
        }

        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let current = load_task(&mut transaction, task_id)
            .await?
            .ok_or(StoreError::TaskNotFound)?;
        if current.status != TaskStatus::Running {
            transaction.commit().await?;
            return Ok(AppendEventOutcome::NotRunning { current });
        }

        let now = current_timestamp()?;
        let event_id = insert_event(
            &mut transaction,
            task_id,
            payload.kind(),
            &payload_json(&payload)?,
            now,
        )
        .await?;
        let updated =
            sqlx::query("UPDATE tasks SET last_event_id = ? WHERE id = ? AND status = 'running'")
                .bind(event_id.get())
                .bind(task_id.to_string())
                .execute(&mut *transaction)
                .await?;
        ensure_exactly_one(
            updated.rows_affected(),
            "running event did not update exactly one task",
        )?;
        transaction.commit().await?;
        Ok(AppendEventOutcome::Applied { event_id })
    }

    pub async fn recover_incomplete(
        &self,
        now: UtcTimestamp,
        failure: TaskFailure,
    ) -> Result<RecoveryOutcome, StoreError> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let task_ids: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM tasks \
             WHERE status IN ('queued', 'running') \
             ORDER BY created_at, id",
        )
        .fetch_all(&mut *transaction)
        .await?;
        let failure_json = serde_json::to_string(&failure)?;
        let mut event_ids = Vec::with_capacity(task_ids.len());

        for task_id in task_ids {
            let task_id: TaskId = task_id.parse().map_err(StoreError::InvalidTaskId)?;
            let updated = sqlx::query(
                "UPDATE tasks \
                 SET status = 'interrupted', finished_at = ?, failure_json = ? \
                 WHERE id = ? AND status IN ('queued', 'running')",
            )
            .bind(now.to_string())
            .bind(&failure_json)
            .bind(task_id.to_string())
            .execute(&mut *transaction)
            .await?;
            ensure_exactly_one(
                updated.rows_affected(),
                "recovery did not update exactly one task",
            )?;
            let (_, event_id) = append_lifecycle_event(
                &mut transaction,
                task_id,
                TaskEventKind::TaskInterrupted,
                now,
            )
            .await?;
            event_ids.push(event_id);
        }

        let high_watermark = latest_event_cursor(&mut transaction).await?;
        transaction.commit().await?;
        Ok(RecoveryOutcome {
            interrupted_count: event_ids.len(),
            first_event_id: event_ids.first().copied(),
            last_event_id: event_ids.last().copied(),
            high_watermark,
        })
    }
}

pub(crate) async fn load_task(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<Option<Task>, StoreError> {
    let record: Option<TaskRecord> = sqlx::query_as(
        "SELECT t.id, t.client_request_id, t.repository_id, t.prompt, t.status, t.attempt, \
                t.retry_of, t.created_at, t.started_at, t.finished_at, t.last_event_id, \
                t.failure_json, d.readiness \
         FROM tasks t \
         LEFT JOIN task_delivery_state d ON d.task_id = t.id \
         WHERE t.id = ?",
    )
    .bind(task_id.to_string())
    .fetch_optional(connection)
    .await?;
    record.map(task_from_record).transpose()
}

pub(crate) fn task_from_record(record: TaskRecord) -> Result<Task, StoreError> {
    let attempt = u32::try_from(record.5).map_err(|_| DomainError::InvalidTaskAttempt)?;
    let failure = record
        .11
        .map(|json| serde_json::from_str(&json))
        .transpose()?;
    let task = Task {
        id: record.0.parse().map_err(StoreError::InvalidTaskId)?,
        client_request_id: record
            .1
            .parse()
            .map_err(StoreError::InvalidClientRequestId)?,
        repository_id: record.2.parse()?,
        prompt: record.3,
        status: parse_status(&record.4)?,
        delivery_readiness: parse_delivery_readiness(record.12.as_deref())?,
        attempt,
        retry_of: record
            .6
            .map(|id| id.parse().map_err(StoreError::InvalidTaskId))
            .transpose()?,
        created_at: UtcTimestamp::parse_rfc3339(&record.7)?,
        started_at: record
            .8
            .map(|timestamp| UtcTimestamp::parse_rfc3339(&timestamp))
            .transpose()?,
        finished_at: record
            .9
            .map(|timestamp| UtcTimestamp::parse_rfc3339(&timestamp))
            .transpose()?,
        last_event_id: EventId::new(record.10)?,
        failure,
    };
    Ok(Task::try_from_stored(task)?)
}

pub(crate) const fn status_text(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Queued => "queued",
        TaskStatus::Running => "running",
        TaskStatus::Completed => "completed",
        TaskStatus::Failed => "failed",
        TaskStatus::Cancelled => "cancelled",
        TaskStatus::Interrupted => "interrupted",
    }
}

pub(crate) fn parse_status(value: &str) -> Result<TaskStatus, StoreError> {
    match value {
        "queued" => Ok(TaskStatus::Queued),
        "running" => Ok(TaskStatus::Running),
        "completed" => Ok(TaskStatus::Completed),
        "failed" => Ok(TaskStatus::Failed),
        "cancelled" => Ok(TaskStatus::Cancelled),
        "interrupted" => Ok(TaskStatus::Interrupted),
        _ => Err(StoreError::InvalidTaskStatus(value.to_owned())),
    }
}

pub(crate) fn parse_delivery_readiness(
    value: Option<&str>,
) -> Result<DeliveryReadiness, StoreError> {
    match value {
        None => Ok(DeliveryReadiness::Unreviewed),
        Some("review_approved") => Ok(DeliveryReadiness::ReviewApproved),
        Some("review_rejected") => Ok(DeliveryReadiness::ReviewRejected),
        Some(value) => Err(StoreError::InvalidDeliveryReadiness(value.to_owned())),
    }
}

pub(crate) const fn event_kind_text(kind: TaskEventKind) -> &'static str {
    match kind {
        TaskEventKind::TaskQueued => "task.queued",
        TaskEventKind::TaskStarted => "task.started",
        TaskEventKind::PlanUpdated => "plan.updated",
        TaskEventKind::ActivityAppended => "activity.appended",
        TaskEventKind::DiffUpdated => "diff.updated",
        TaskEventKind::TestUpdated => "test.updated",
        TaskEventKind::ReviewUpdated => "review.updated",
        TaskEventKind::TaskCompleted => "task.completed",
        TaskEventKind::TaskFailed => "task.failed",
        TaskEventKind::TaskCancelled => "task.cancelled",
        TaskEventKind::TaskInterrupted => "task.interrupted",
    }
}

pub(crate) fn parse_event_kind(value: &str) -> Result<TaskEventKind, StoreError> {
    match value {
        "task.queued" => Ok(TaskEventKind::TaskQueued),
        "task.started" => Ok(TaskEventKind::TaskStarted),
        "plan.updated" => Ok(TaskEventKind::PlanUpdated),
        "activity.appended" => Ok(TaskEventKind::ActivityAppended),
        "diff.updated" => Ok(TaskEventKind::DiffUpdated),
        "test.updated" => Ok(TaskEventKind::TestUpdated),
        "review.updated" => Ok(TaskEventKind::ReviewUpdated),
        "task.completed" => Ok(TaskEventKind::TaskCompleted),
        "task.failed" => Ok(TaskEventKind::TaskFailed),
        "task.cancelled" => Ok(TaskEventKind::TaskCancelled),
        "task.interrupted" => Ok(TaskEventKind::TaskInterrupted),
        _ => Err(StoreError::InvalidEventKind(value.to_owned())),
    }
}

pub(crate) fn payload_json(payload: &TaskEventPayload) -> Result<String, StoreError> {
    let value = match payload {
        TaskEventPayload::TaskQueued { task }
        | TaskEventPayload::TaskStarted { task }
        | TaskEventPayload::TaskCompleted { task }
        | TaskEventPayload::TaskFailed { task }
        | TaskEventPayload::TaskCancelled { task }
        | TaskEventPayload::TaskInterrupted { task } => serde_json::json!({ "task": task }),
        TaskEventPayload::PlanUpdated { plan } => serde_json::json!({ "plan": plan }),
        TaskEventPayload::ActivityAppended { entry } => serde_json::json!({ "entry": entry }),
        TaskEventPayload::DiffUpdated { diff } => serde_json::json!({ "diff": diff }),
        TaskEventPayload::TestUpdated { tests } => serde_json::json!({ "tests": tests }),
        TaskEventPayload::ReviewUpdated { .. } => {
            return Err(StoreError::InvariantViolation(
                "review events require the typed evidence writer",
            ));
        }
    };
    Ok(serde_json::to_string(&value)?)
}

pub(crate) async fn latest_event_cursor(
    connection: &mut SqliteConnection,
) -> Result<EventCursor, StoreError> {
    let maximum: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(id), 0) FROM task_events")
        .fetch_one(connection)
        .await?;
    Ok(EventCursor::new(maximum)?)
}

async fn load_task_by_client_request(
    transaction: &mut Transaction<'_, sqlx::Sqlite>,
    client_request_id: ClientRequestId,
) -> Result<Option<Task>, StoreError> {
    let record: Option<TaskRecord> = sqlx::query_as(
        "SELECT t.id, t.client_request_id, t.repository_id, t.prompt, t.status, t.attempt, \
                t.retry_of, t.created_at, t.started_at, t.finished_at, t.last_event_id, \
                t.failure_json, d.readiness \
         FROM tasks t \
         LEFT JOIN task_delivery_state d ON d.task_id = t.id \
         WHERE t.client_request_id = ?",
    )
    .bind(client_request_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    record.map(task_from_record).transpose()
}

async fn load_retry_child(
    transaction: &mut Transaction<'_, sqlx::Sqlite>,
    source_id: TaskId,
) -> Result<Option<Task>, StoreError> {
    let record: Option<TaskRecord> = sqlx::query_as(
        "SELECT t.id, t.client_request_id, t.repository_id, t.prompt, t.status, t.attempt, \
                t.retry_of, t.created_at, t.started_at, t.finished_at, t.last_event_id, \
                t.failure_json, d.readiness \
         FROM tasks t \
         LEFT JOIN task_delivery_state d ON d.task_id = t.id \
         WHERE t.retry_of = ?",
    )
    .bind(source_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    record.map(task_from_record).transpose()
}

pub(crate) async fn append_lifecycle_event(
    transaction: &mut Transaction<'_, sqlx::Sqlite>,
    task_id: TaskId,
    kind: TaskEventKind,
    created_at: UtcTimestamp,
) -> Result<(Task, EventId), StoreError> {
    let event_id = insert_event(transaction, task_id, kind, "{}", created_at).await?;
    let updated = sqlx::query("UPDATE tasks SET last_event_id = ? WHERE id = ?")
        .bind(event_id.get())
        .bind(task_id.to_string())
        .execute(&mut **transaction)
        .await?;
    ensure_exactly_one(
        updated.rows_affected(),
        "lifecycle event did not update exactly one task",
    )?;

    let task = load_task(transaction, task_id)
        .await?
        .ok_or(StoreError::TaskNotFound)?;
    let payload = lifecycle_payload(kind, task.clone())?;
    let updated =
        sqlx::query("UPDATE task_events SET payload_json = ? WHERE id = ? AND payload_json = '{}'")
            .bind(payload_json(&payload)?)
            .bind(event_id.get())
            .execute(&mut **transaction)
            .await?;
    ensure_exactly_one(
        updated.rows_affected(),
        "lifecycle payload did not update exactly one placeholder",
    )?;
    Ok((task, event_id))
}

pub(crate) async fn insert_event(
    transaction: &mut Transaction<'_, sqlx::Sqlite>,
    task_id: TaskId,
    kind: TaskEventKind,
    payload_json: &str,
    created_at: UtcTimestamp,
) -> Result<EventId, StoreError> {
    let inserted = sqlx::query(
        "INSERT INTO task_events (schema_version, task_id, kind, payload_json, created_at) \
         VALUES (1, ?, ?, ?, ?)",
    )
    .bind(task_id.to_string())
    .bind(event_kind_text(kind))
    .bind(payload_json)
    .bind(created_at.to_string())
    .execute(&mut **transaction)
    .await?;
    Ok(EventId::new(inserted.last_insert_rowid())?)
}

fn lifecycle_payload(kind: TaskEventKind, task: Task) -> Result<TaskEventPayload, StoreError> {
    match kind {
        TaskEventKind::TaskQueued => Ok(TaskEventPayload::TaskQueued { task }),
        TaskEventKind::TaskStarted => Ok(TaskEventPayload::TaskStarted { task }),
        TaskEventKind::TaskCompleted => Ok(TaskEventPayload::TaskCompleted { task }),
        TaskEventKind::TaskFailed => Ok(TaskEventPayload::TaskFailed { task }),
        TaskEventKind::TaskCancelled => Ok(TaskEventPayload::TaskCancelled { task }),
        TaskEventKind::TaskInterrupted => Ok(TaskEventPayload::TaskInterrupted { task }),
        TaskEventKind::PlanUpdated
        | TaskEventKind::ActivityAppended
        | TaskEventKind::DiffUpdated
        | TaskEventKind::TestUpdated
        | TaskEventKind::ReviewUpdated => Err(StoreError::InvariantViolation(
            "panel event passed to lifecycle payload builder",
        )),
    }
}

const fn lifecycle_kind(status: TaskStatus) -> TaskEventKind {
    match status {
        TaskStatus::Queued => TaskEventKind::TaskQueued,
        TaskStatus::Running => TaskEventKind::TaskStarted,
        TaskStatus::Completed => TaskEventKind::TaskCompleted,
        TaskStatus::Failed => TaskEventKind::TaskFailed,
        TaskStatus::Cancelled => TaskEventKind::TaskCancelled,
        TaskStatus::Interrupted => TaskEventKind::TaskInterrupted,
    }
}

pub(crate) fn ensure_exactly_one(rows: u64, message: &'static str) -> Result<(), StoreError> {
    if rows == 1 {
        Ok(())
    } else {
        Err(StoreError::InvariantViolation(message))
    }
}

pub(crate) fn current_timestamp() -> Result<UtcTimestamp, StoreError> {
    Ok(UtcTimestamp::new(OffsetDateTime::now_utc())?)
}
