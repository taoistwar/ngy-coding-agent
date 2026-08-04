use std::collections::HashMap;

use coding_agent_domain::{
    EventCursor, EventId, Task, TaskEventKind, TaskFailure, TaskId, TaskStatus, UtcTimestamp,
};
use sqlx::{SqliteConnection, Transaction};

use crate::claims::{LoadedTask, load_claim_task, task_lifecycle_is_projection_compatible};
use crate::reviews::{load_reviews_for_task, validate_task_review_aggregate};
use crate::stop_intents::{
    FinalizeStoppedTaskOutcome, FinalizeStoppedTaskRequest, ensure_no_stop_intent,
    finalize_stopped_task_in_transaction, load_stop_intents_in_recovery_order,
    validate_stop_intent_graph,
};
use crate::tasks::{
    append_lifecycle_event, current_timestamp, ensure_exactly_one, latest_event_cursor,
};
use crate::{Store, StoreError};

const RECOVERY_INVARIANT: &str = "recovery transaction is inconsistent";
const APP_RESTARTED_MESSAGE: &str = "task was interrupted because the application restarted";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InterruptSelection {
    RunningOnly,
    QueuedAndRunning,
}

impl InterruptSelection {
    const fn includes(self, status: TaskStatus) -> bool {
        match self {
            Self::RunningOnly => matches!(status, TaskStatus::Running),
            Self::QueuedAndRunning => matches!(status, TaskStatus::Queued | TaskStatus::Running),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryReceipt {
    pub finalized_stop_count: usize,
    pub interrupted_count: usize,
    pub first_event_id: Option<EventId>,
    pub last_event_id: Option<EventId>,
    pub high_watermark: EventCursor,
    pub membership_high_watermark: EventCursor,
}

impl Store {
    /// Performs the single cold-start recovery transaction.
    ///
    /// The caller must hold the single-instance lock and must have proved that
    /// every previous process tree has exited before calling this method. The
    /// Store intentionally does not manufacture or accept a process-proof
    /// value; it only commits the durable recovery facts.
    pub async fn recover_after_restart(&self) -> Result<RecoveryReceipt, StoreError> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let before_tasks = validate_recovery_graph(&mut transaction).await?;
        let before_high_watermark = latest_event_cursor(&mut transaction).await?;

        let intents = load_stop_intents_in_recovery_order(&mut transaction).await?;
        let mut finalized_stop_count = 0;
        let mut event_ids = Vec::new();
        let mut expected_tasks = HashMap::new();
        for intent in intents {
            let request = FinalizeStoppedTaskRequest {
                task_id: intent.task_id,
                expected_repository_id: intent.repository_id,
                expected_attempt: intent.attempt,
                expected_intent: intent.kind,
            };
            match finalize_stopped_task_in_transaction(&mut transaction, &request).await? {
                FinalizeStoppedTaskOutcome::Applied(receipt) if receipt.intent == intent => {
                    finalized_stop_count += 1;
                    event_ids.push(receipt.terminal_event_id);
                    if expected_tasks
                        .insert(receipt.task.id, receipt.task)
                        .is_some()
                    {
                        return Err(recovery_invariant());
                    }
                }
                FinalizeStoppedTaskOutcome::Applied(_)
                | FinalizeStoppedTaskOutcome::Existing(_)
                | FinalizeStoppedTaskOutcome::InvariantConflict => {
                    return Err(recovery_invariant());
                }
            }
        }

        if has_running_stop_intents(&mut transaction).await? {
            return Err(recovery_invariant());
        }
        let now = current_timestamp()?;
        let interrupted_count = interrupt_selected_tasks(
            &mut transaction,
            now,
            &app_restarted_failure(),
            &mut event_ids,
            &mut expected_tasks,
            InterruptSelection::RunningOnly,
        )
        .await?;
        let after_tasks = validate_recovery_graph(&mut transaction).await?;
        validate_recovery_post_state(
            &mut transaction,
            &before_tasks,
            &after_tasks,
            &expected_tasks,
            before_high_watermark,
            &event_ids,
            InterruptSelection::RunningOnly,
        )
        .await?;
        let receipt = build_receipt(
            &mut transaction,
            finalized_stop_count,
            interrupted_count,
            &event_ids,
        )
        .await?;
        transaction.commit().await?;
        Ok(receipt)
    }

    /// Interrupts every remaining Queued or Running task after stop intents.
    ///
    /// The process owner must prove that all relevant process trees have exited
    /// before invoking this primitive. The database guard rejects the entire
    /// transaction while any Running task still has a durable stop intent; this
    /// method never completes an intent itself.
    pub async fn interrupt_remaining_after_stops(
        &self,
        failure: TaskFailure,
    ) -> Result<RecoveryReceipt, StoreError> {
        interrupt_remaining_at(self, None, failure, InterruptSelection::QueuedAndRunning).await
    }
}

/// Retains the legacy timestamp-injection facade without retaining its old
/// behavior of interrupting tasks that never left the queue.
pub(crate) async fn recover_incomplete_compat(
    store: &Store,
    now: UtcTimestamp,
    failure: TaskFailure,
) -> Result<RecoveryReceipt, StoreError> {
    interrupt_remaining_at(store, Some(now), failure, InterruptSelection::RunningOnly).await
}

async fn interrupt_remaining_at(
    store: &Store,
    requested_now: Option<UtcTimestamp>,
    failure: TaskFailure,
    selection: InterruptSelection,
) -> Result<RecoveryReceipt, StoreError> {
    let mut transaction = store.pool.begin_with("BEGIN IMMEDIATE").await?;
    let before_tasks = validate_recovery_graph(&mut transaction).await?;
    let before_high_watermark = latest_event_cursor(&mut transaction).await?;
    if has_running_stop_intents(&mut transaction).await? {
        transaction.rollback().await?;
        return Err(StoreError::InvariantViolation(
            "running stop intents must be finalized before generic interruption",
        ));
    }

    let now = match requested_now {
        Some(now) => now,
        None => current_timestamp()?,
    };
    let mut event_ids = Vec::new();
    let mut expected_tasks = HashMap::new();
    let interrupted_count = interrupt_selected_tasks(
        &mut transaction,
        now,
        &failure,
        &mut event_ids,
        &mut expected_tasks,
        selection,
    )
    .await?;
    let after_tasks = validate_recovery_graph(&mut transaction).await?;
    validate_recovery_post_state(
        &mut transaction,
        &before_tasks,
        &after_tasks,
        &expected_tasks,
        before_high_watermark,
        &event_ids,
        selection,
    )
    .await?;
    let receipt = build_receipt(&mut transaction, 0, interrupted_count, &event_ids).await?;
    transaction.commit().await?;
    Ok(receipt)
}

async fn validate_recovery_graph(
    connection: &mut SqliteConnection,
) -> Result<HashMap<TaskId, Task>, StoreError> {
    validate_stop_intent_graph(connection).await?;

    let raw_task_ids: Vec<(String, String)> = sqlx::query_as(
        "SELECT typeof(id), CAST(id AS TEXT) \
         FROM tasks ORDER BY id",
    )
    .fetch_all(&mut *connection)
    .await
    .map_err(|error| match error {
        sqlx::Error::ColumnDecode { .. } => recovery_invariant(),
        other => StoreError::Database(other),
    })?;
    let mut tasks = HashMap::with_capacity(raw_task_ids.len());
    for (storage_type, raw_task_id) in raw_task_ids {
        if storage_type != "text" {
            return Err(recovery_invariant());
        }
        let task_id: TaskId = raw_task_id.parse().map_err(|_| recovery_invariant())?;
        if raw_task_id != task_id.to_string() {
            return Err(recovery_invariant());
        }
        let task = validate_task(connection, task_id).await?;
        if tasks.insert(task_id, task).is_some() {
            return Err(recovery_invariant());
        }
    }
    Ok(tasks)
}

async fn has_running_stop_intents(connection: &mut SqliteConnection) -> Result<bool, StoreError> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) \
         FROM task_stop_intents i \
         INNER JOIN tasks t ON t.id = i.task_id \
         WHERE t.status = 'running'",
    )
    .fetch_one(connection)
    .await?;
    Ok(count != 0)
}

async fn interrupt_selected_tasks(
    transaction: &mut Transaction<'_, sqlx::Sqlite>,
    now: UtcTimestamp,
    failure: &TaskFailure,
    event_ids: &mut Vec<EventId>,
    expected_tasks: &mut HashMap<TaskId, Task>,
    selection: InterruptSelection,
) -> Result<usize, StoreError> {
    let query = match selection {
        InterruptSelection::RunningOnly => {
            "SELECT id FROM tasks \
             WHERE status = 'running' \
             ORDER BY created_at, id"
        }
        InterruptSelection::QueuedAndRunning => {
            "SELECT id FROM tasks \
             WHERE status IN ('queued', 'running') \
             ORDER BY created_at, id"
        }
    };
    let raw_task_ids: Vec<String> = sqlx::query_scalar(query)
        .fetch_all(&mut **transaction)
        .await?;
    let failure_json = serde_json::to_string(failure)?;
    let mut interrupted_count = 0;

    for raw_task_id in raw_task_ids {
        let task_id: TaskId = raw_task_id.parse().map_err(|_| recovery_invariant())?;
        if raw_task_id != task_id.to_string() {
            return Err(recovery_invariant());
        }
        let task = validate_task(transaction, task_id).await?;
        if !selection.includes(task.status) {
            return Err(recovery_invariant());
        }
        ensure_no_stop_intent(transaction, task.id).await?;

        let updated = match task.status {
            TaskStatus::Running => {
                sqlx::query(
                    "UPDATE tasks \
                     SET status = 'interrupted', finished_at = ?, failure_json = ? \
                     WHERE id = ? AND repository_id = ? AND attempt = ? \
                       AND status = 'running' AND created_at = ? AND started_at = ? \
                       AND finished_at IS NULL AND failure_json IS NULL \
                       AND last_event_id = ? \
                       AND NOT EXISTS (\
                           SELECT 1 FROM task_delivery_state d WHERE d.task_id = tasks.id\
                       ) \
                       AND NOT EXISTS (\
                           SELECT 1 FROM task_stop_intents i WHERE i.task_id = tasks.id\
                       )",
                )
                .bind(now.to_string())
                .bind(&failure_json)
                .bind(task.id.to_string())
                .bind(task.repository_id.to_string())
                .bind(i64::from(task.attempt))
                .bind(task.created_at.to_string())
                .bind(
                    task.started_at
                        .ok_or(StoreError::InvariantViolation(RECOVERY_INVARIANT))?
                        .to_string(),
                )
                .bind(task.last_event_id.get())
                .execute(&mut **transaction)
                .await?
            }
            TaskStatus::Queued => {
                sqlx::query(
                    "UPDATE tasks \
                 SET status = 'interrupted', finished_at = ?, failure_json = ? \
                 WHERE id = ? AND repository_id = ? AND attempt = ? \
                   AND status = 'queued' AND created_at = ? \
                   AND started_at IS NULL AND finished_at IS NULL AND failure_json IS NULL \
                   AND last_event_id = ? \
                   AND NOT EXISTS (\
                       SELECT 1 FROM task_delivery_state d WHERE d.task_id = tasks.id\
                   ) \
                   AND NOT EXISTS (\
                       SELECT 1 FROM task_stop_intents i WHERE i.task_id = tasks.id\
                   )",
                )
                .bind(now.to_string())
                .bind(&failure_json)
                .bind(task.id.to_string())
                .bind(task.repository_id.to_string())
                .bind(i64::from(task.attempt))
                .bind(task.created_at.to_string())
                .bind(task.last_event_id.get())
                .execute(&mut **transaction)
                .await?
            }
            TaskStatus::Completed
            | TaskStatus::Failed
            | TaskStatus::Cancelled
            | TaskStatus::Interrupted => return Err(recovery_invariant()),
        };
        ensure_exactly_one(
            updated.rows_affected(),
            "recovery did not update exactly one active task",
        )?;

        let (terminal, event_id) =
            append_lifecycle_event(transaction, task.id, TaskEventKind::TaskInterrupted, now)
                .await?;
        let mut expected = task;
        expected.status = TaskStatus::Interrupted;
        expected.finished_at = Some(now);
        expected.last_event_id = event_id;
        expected.failure = Some(failure.clone());
        if terminal != expected {
            return Err(recovery_invariant());
        }
        validate_task(transaction, terminal.id).await?;
        ensure_no_stop_intent(transaction, terminal.id).await?;
        if expected_tasks.insert(terminal.id, terminal).is_some() {
            return Err(recovery_invariant());
        }

        interrupted_count += 1;
        event_ids.push(event_id);
    }
    Ok(interrupted_count)
}

async fn validate_recovery_post_state(
    connection: &mut SqliteConnection,
    before_tasks: &HashMap<TaskId, Task>,
    after_tasks: &HashMap<TaskId, Task>,
    expected_tasks: &HashMap<TaskId, Task>,
    before_high_watermark: EventCursor,
    event_ids: &[EventId],
    selection: InterruptSelection,
) -> Result<(), StoreError> {
    if before_tasks.len() != after_tasks.len() {
        return Err(recovery_invariant());
    }

    let mut selected_count = 0;
    for (task_id, before_task) in before_tasks {
        let after_task = after_tasks.get(task_id).ok_or_else(recovery_invariant)?;
        if selection.includes(before_task.status) {
            selected_count += 1;
            let expected_task = expected_tasks.get(task_id).ok_or_else(recovery_invariant)?;
            let mut expected_from_before = before_task.clone();
            expected_from_before.status = expected_task.status;
            expected_from_before.finished_at = expected_task.finished_at;
            expected_from_before.last_event_id = expected_task.last_event_id;
            expected_from_before
                .failure
                .clone_from(&expected_task.failure);
            if expected_task.id != *task_id
                || expected_task != &expected_from_before
                || after_task != expected_task
            {
                return Err(recovery_invariant());
            }
        } else if expected_tasks.contains_key(task_id) || after_task != before_task {
            return Err(recovery_invariant());
        }
    }
    if expected_tasks.len() != selected_count
        || after_tasks
            .keys()
            .any(|task_id| !before_tasks.contains_key(task_id))
    {
        return Err(recovery_invariant());
    }

    let raw_event_ids: Vec<(String, String)> = sqlx::query_as(
        "SELECT typeof(id), CAST(id AS TEXT) \
         FROM task_events \
         WHERE id > ? \
         ORDER BY id",
    )
    .bind(before_high_watermark.get())
    .fetch_all(connection)
    .await
    .map_err(|error| match error {
        sqlx::Error::ColumnDecode { .. } => recovery_invariant(),
        other => StoreError::Database(other),
    })?;
    let mut added_event_ids = Vec::with_capacity(raw_event_ids.len());
    for (storage_type, raw_event_id) in raw_event_ids {
        if storage_type != "integer" {
            return Err(recovery_invariant());
        }
        let raw_event_id: i64 = raw_event_id.parse().map_err(|_| recovery_invariant())?;
        added_event_ids.push(EventId::new(raw_event_id).map_err(|_| recovery_invariant())?);
    }
    if added_event_ids != event_ids {
        return Err(recovery_invariant());
    }

    Ok(())
}

async fn validate_task(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<Task, StoreError> {
    let task = match load_claim_task(connection, task_id).await? {
        LoadedTask::Valid(task) => *task,
        LoadedTask::Missing | LoadedTask::Invalid => return Err(recovery_invariant()),
    };
    let reviews = load_reviews_for_task(connection, task.id).await?;
    validate_task_review_aggregate(connection, &task, &reviews).await?;
    if !task_lifecycle_is_projection_compatible(connection, &task).await? {
        return Err(recovery_invariant());
    }
    Ok(task)
}

async fn build_receipt(
    connection: &mut SqliteConnection,
    finalized_stop_count: usize,
    interrupted_count: usize,
    event_ids: &[EventId],
) -> Result<RecoveryReceipt, StoreError> {
    Ok(RecoveryReceipt {
        finalized_stop_count,
        interrupted_count,
        first_event_id: event_ids.first().copied(),
        last_event_id: event_ids.last().copied(),
        high_watermark: latest_event_cursor(connection).await?,
        membership_high_watermark: membership_high_watermark(connection).await?,
    })
}

async fn membership_high_watermark(
    connection: &mut SqliteConnection,
) -> Result<EventCursor, StoreError> {
    let maximum: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(id), 0) FROM task_events \
         WHERE kind IN (\
             'task.queued', 'task.started', 'task.completed', \
             'task.failed', 'task.cancelled', 'task.interrupted'\
         )",
    )
    .fetch_one(connection)
    .await?;
    Ok(EventCursor::new(maximum)?)
}

fn app_restarted_failure() -> TaskFailure {
    TaskFailure {
        code: "APP_RESTARTED".to_owned(),
        message: APP_RESTARTED_MESSAGE.to_owned(),
        retryable: true,
    }
}

const fn recovery_invariant() -> StoreError {
    StoreError::InvariantViolation(RECOVERY_INVARIANT)
}
