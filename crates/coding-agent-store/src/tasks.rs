use std::num::NonZeroU32;

use coding_agent_domain::{
    ClientRequestId, DeliveryReadiness, DomainError, EventCursor, EventId, NewTask, Task,
    TaskEventKind, TaskEventPayload, TaskFailure, TaskId, TaskStatus, UtcTimestamp,
};
use sqlx::{SqliteConnection, Transaction};
use time::OffsetDateTime;

use crate::claims::task_lifecycle_is_exact;
use crate::stop_intents::{ensure_no_stop_intent, validate_optional_stop_intent};
use crate::{Store, StoreError};

const GENERIC_RUNNING_TRANSITION_BYPASS: &str =
    "running tasks must be committed through the typed claim transaction";

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
pub enum QueueLimitedCreateTaskOutcome {
    Created {
        task: Task,
        event_id: EventId,
    },
    Existing {
        task: Task,
    },
    QueueFull {
        queued_tasks: u64,
        max_queued_tasks: NonZeroU32,
    },
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
pub enum QueueLimitedRetryTaskOutcome {
    Created {
        task: Task,
        event_id: EventId,
    },
    Existing {
        task: Task,
    },
    QueueFull {
        queued_tasks: u64,
        max_queued_tasks: NonZeroU32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransitionOutcome {
    Applied { task: Task, event_id: EventId },
    Conflict { current: Task },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizeUnreviewedTaskRequest {
    pub task_id: TaskId,
    pub expected_repository_id: coding_agent_domain::RepositoryId,
    pub expected_attempt: u32,
    pub transition: TaskTransition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FinalizeUnreviewedTaskOutcome {
    Applied { task: Task, event_id: EventId },
    Existing { task: Task, event_id: EventId },
    InvariantConflict,
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
        match self
            .create_task_with_optional_queue_limit(input, None)
            .await?
        {
            QueueLimitedCreateTaskOutcome::Created { task, event_id } => {
                Ok(CreateTaskOutcome::Created { task, event_id })
            }
            QueueLimitedCreateTaskOutcome::Existing { task } => {
                Ok(CreateTaskOutcome::Existing { task })
            }
            QueueLimitedCreateTaskOutcome::QueueFull { .. } => Err(StoreError::InvariantViolation(
                "unlimited task creation returned queue full",
            )),
        }
    }

    pub async fn create_task_with_queue_limit(
        &self,
        input: NewTask,
        max_queued_tasks: NonZeroU32,
    ) -> Result<QueueLimitedCreateTaskOutcome, StoreError> {
        self.create_task_with_optional_queue_limit(input, Some(max_queued_tasks))
            .await
    }

    async fn create_task_with_optional_queue_limit(
        &self,
        input: NewTask,
        max_queued_tasks: Option<NonZeroU32>,
    ) -> Result<QueueLimitedCreateTaskOutcome, StoreError> {
        let input = NewTask::try_new(input.client_request_id, input.repository_id, input.prompt)?;
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;

        if let Some(existing) =
            load_task_by_client_request(&mut transaction, input.client_request_id).await?
        {
            if existing.repository_id != input.repository_id || existing.prompt != input.prompt {
                return Err(StoreError::IdempotencyConflict);
            }
            transaction.commit().await?;
            return Ok(QueueLimitedCreateTaskOutcome::Existing { task: existing });
        }

        if let Some(max_queued_tasks) = max_queued_tasks {
            let queued_tasks = count_queued_tasks(&mut transaction).await?;
            if queued_tasks >= u64::from(max_queued_tasks.get()) {
                transaction.commit().await?;
                return Ok(QueueLimitedCreateTaskOutcome::QueueFull {
                    queued_tasks,
                    max_queued_tasks,
                });
            }
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
        Ok(QueueLimitedCreateTaskOutcome::Created { task, event_id })
    }

    pub async fn retry_task(&self, source_id: TaskId) -> Result<RetryTaskOutcome, StoreError> {
        match self
            .retry_task_with_optional_queue_limit(source_id, None)
            .await?
        {
            QueueLimitedRetryTaskOutcome::Created { task, event_id } => {
                Ok(RetryTaskOutcome::Created { task, event_id })
            }
            QueueLimitedRetryTaskOutcome::Existing { task } => {
                Ok(RetryTaskOutcome::Existing { task })
            }
            QueueLimitedRetryTaskOutcome::QueueFull { .. } => Err(StoreError::InvariantViolation(
                "unlimited task retry returned queue full",
            )),
        }
    }

    pub async fn retry_task_with_queue_limit(
        &self,
        source_id: TaskId,
        max_queued_tasks: NonZeroU32,
    ) -> Result<QueueLimitedRetryTaskOutcome, StoreError> {
        self.retry_task_with_optional_queue_limit(source_id, Some(max_queued_tasks))
            .await
    }

    async fn retry_task_with_optional_queue_limit(
        &self,
        source_id: TaskId,
        max_queued_tasks: Option<NonZeroU32>,
    ) -> Result<QueueLimitedRetryTaskOutcome, StoreError> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let source = load_task(&mut transaction, source_id)
            .await?
            .ok_or(StoreError::TaskNotFound)?;
        if !source.status.is_retryable() {
            return Err(StoreError::TaskNotRetryable);
        }

        if let Some(existing) = load_retry_child(&mut transaction, source_id).await? {
            transaction.commit().await?;
            return Ok(QueueLimitedRetryTaskOutcome::Existing { task: existing });
        }

        let attempt = source
            .attempt
            .checked_add(1)
            .ok_or(StoreError::TaskAttemptOverflow)?;
        if let Some(max_queued_tasks) = max_queued_tasks {
            let queued_tasks = count_queued_tasks(&mut transaction).await?;
            if queued_tasks >= u64::from(max_queued_tasks.get()) {
                transaction.commit().await?;
                return Ok(QueueLimitedRetryTaskOutcome::QueueFull {
                    queued_tasks,
                    max_queued_tasks,
                });
            }
        }
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
        Ok(QueueLimitedRetryTaskOutcome::Created { task, event_id })
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
        if matches!(transition, TaskTransition::Running) {
            #[cfg(feature = "test-support")]
            {
                return crate::claims::transition_running_for_test(self, task_id, expected).await;
            }
            #[cfg(not(feature = "test-support"))]
            {
                return Err(StoreError::InvariantViolation(
                    GENERIC_RUNNING_TRANSITION_BYPASS,
                ));
            }
        }
        if matches!(transition, TaskTransition::Completed) {
            return Err(StoreError::InvariantViolation(
                "completed tasks require reviewed finalization",
            ));
        }

        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let current = load_task(&mut transaction, task_id)
            .await?
            .ok_or(StoreError::TaskNotFound)?;
        if current.status != expected {
            validate_optional_stop_intent(&mut transaction, &current).await?;
            transaction.commit().await?;
            return Ok(TransitionOutcome::Conflict { current });
        }
        ensure_no_stop_intent(&mut transaction, task_id).await?;
        let now = current_timestamp()?;
        let failure_json = transition
            .failure()
            .map(serde_json::to_string)
            .transpose()?;
        let updated = match transition {
            TaskTransition::Running => {
                return Err(StoreError::InvariantViolation(
                    GENERIC_RUNNING_TRANSITION_BYPASS,
                ));
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
            return Err(StoreError::InvariantViolation(
                "task transition lost its writer-fenced compare-and-swap",
            ));
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

    pub async fn finalize_unreviewed_task(
        &self,
        request: FinalizeUnreviewedTaskRequest,
    ) -> Result<FinalizeUnreviewedTaskOutcome, StoreError> {
        if request.expected_attempt == 0
            || !matches!(
                request.transition,
                TaskTransition::Failed(_) | TaskTransition::Cancelled
            )
        {
            return Ok(FinalizeUnreviewedTaskOutcome::InvariantConflict);
        }
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let current = load_task(&mut transaction, request.task_id)
            .await?
            .ok_or(StoreError::TaskNotFound)?;
        if current.id != request.task_id
            || current.repository_id != request.expected_repository_id
            || current.attempt != request.expected_attempt
        {
            transaction.commit().await?;
            return Ok(FinalizeUnreviewedTaskOutcome::InvariantConflict);
        }
        let no_stop_intent = match validate_optional_stop_intent(&mut transaction, &current).await {
            Ok(None) => true,
            Ok(Some(_)) | Err(StoreError::InvariantViolation(_)) => false,
            Err(error) => return Err(error),
        };
        let lifecycle_exact = match task_lifecycle_is_exact(&mut transaction, &current).await {
            Ok(exact) => exact,
            Err(StoreError::InvariantViolation(_)) => false,
            Err(error) => return Err(error),
        };
        if !no_stop_intent || !lifecycle_exact {
            transaction.commit().await?;
            return Ok(FinalizeUnreviewedTaskOutcome::InvariantConflict);
        }
        if exact_unreviewed_terminal(&mut transaction, &current, &request).await? {
            let event_id = current.last_event_id;
            transaction.commit().await?;
            return Ok(FinalizeUnreviewedTaskOutcome::Existing {
                task: current,
                event_id,
            });
        }
        if current.status != TaskStatus::Running
            || current.delivery_readiness != DeliveryReadiness::Unreviewed
            || current.started_at.is_none()
            || current.finished_at.is_some()
            || current.failure.is_some()
        {
            transaction.commit().await?;
            return Ok(FinalizeUnreviewedTaskOutcome::InvariantConflict);
        }

        let now = current_timestamp()?;
        let next = request.transition.next();
        let failure_json = request
            .transition
            .failure()
            .map(serde_json::to_string)
            .transpose()?;
        let updated = sqlx::query(
            "UPDATE tasks \
             SET status = ?, finished_at = ?, failure_json = ? \
             WHERE id = ? AND repository_id = ? AND attempt = ? \
               AND status = 'running' AND started_at = ? \
               AND finished_at IS NULL AND failure_json IS NULL \
               AND last_event_id = ? \
               AND NOT EXISTS (\
                   SELECT 1 FROM task_delivery_state d WHERE d.task_id = tasks.id\
               ) \
               AND NOT EXISTS (\
                   SELECT 1 FROM task_stop_intents i WHERE i.task_id = tasks.id\
               )",
        )
        .bind(status_text(next))
        .bind(now.to_string())
        .bind(failure_json)
        .bind(current.id.to_string())
        .bind(current.repository_id.to_string())
        .bind(i64::from(current.attempt))
        .bind(
            current
                .started_at
                .expect("validated running task")
                .to_string(),
        )
        .bind(current.last_event_id.get())
        .execute(&mut *transaction)
        .await?;
        ensure_exactly_one(
            updated.rows_affected(),
            "unreviewed finalization did not update exactly one running task",
        )?;
        let (task, event_id) =
            append_lifecycle_event(&mut transaction, current.id, lifecycle_kind(next), now).await?;
        if !exact_unreviewed_terminal(&mut transaction, &task, &request).await?
            || task.last_event_id != event_id
        {
            return Err(StoreError::InvariantViolation(
                "unreviewed finalization post-state is inconsistent",
            ));
        }
        transaction.commit().await?;
        Ok(FinalizeUnreviewedTaskOutcome::Applied { task, event_id })
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
        let receipt = crate::recovery::recover_incomplete_compat(self, now, failure).await?;
        Ok(RecoveryOutcome {
            interrupted_count: receipt.interrupted_count,
            first_event_id: receipt.first_event_id,
            last_event_id: receipt.last_event_id,
            high_watermark: receipt.high_watermark,
        })
    }
}

async fn exact_unreviewed_terminal(
    connection: &mut SqliteConnection,
    task: &Task,
    request: &FinalizeUnreviewedTaskRequest,
) -> Result<bool, StoreError> {
    if task.id != request.task_id
        || task.repository_id != request.expected_repository_id
        || task.attempt != request.expected_attempt
        || task.status != request.transition.next()
        || task.delivery_readiness != DeliveryReadiness::Unreviewed
        || task.started_at.is_none()
        || task.finished_at.is_none()
        || task.failure.as_ref() != request.transition.failure()
    {
        return Ok(false);
    }
    let delivery_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM task_delivery_state WHERE task_id = ?")
            .bind(task.id.to_string())
            .fetch_one(&mut *connection)
            .await?;
    let stop_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM task_stop_intents WHERE task_id = ?")
            .bind(task.id.to_string())
            .fetch_one(&mut *connection)
            .await?;
    let raw_failure: Option<String> =
        sqlx::query_scalar("SELECT failure_json FROM tasks WHERE id = ?")
            .bind(task.id.to_string())
            .fetch_one(&mut *connection)
            .await?;
    let expected_failure = request
        .transition
        .failure()
        .map(serde_json::to_string)
        .transpose()?;
    Ok(delivery_count == 0
        && stop_count == 0
        && raw_failure == expected_failure
        && task_lifecycle_is_exact(connection, task).await?)
}

pub(crate) async fn load_task(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<Option<Task>, StoreError> {
    load_task_record(connection, task_id)
        .await?
        .map(task_from_record)
        .transpose()
}

pub(crate) async fn load_task_record(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<Option<TaskRecord>, StoreError> {
    sqlx::query_as(
        "SELECT t.id, t.client_request_id, t.repository_id, t.prompt, t.status, t.attempt, \
                t.retry_of, t.created_at, t.started_at, t.finished_at, t.last_event_id, \
                t.failure_json, d.readiness \
         FROM tasks t \
         LEFT JOIN task_delivery_state d ON d.task_id = t.id \
         WHERE t.id = ?",
    )
    .bind(task_id.to_string())
    .fetch_optional(connection)
    .await
    .map_err(Into::into)
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

pub(crate) async fn count_queued_tasks(
    connection: &mut SqliteConnection,
) -> Result<u64, StoreError> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tasks WHERE status = 'queued'")
        .fetch_one(connection)
        .await?;
    u64::try_from(count)
        .map_err(|_| StoreError::InvariantViolation("queued task count is negative"))
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
