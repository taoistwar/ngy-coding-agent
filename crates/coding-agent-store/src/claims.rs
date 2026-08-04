use coding_agent_domain::{
    DeliveryReadiness, EventId, RepositoryId, Task, TaskEventKind, TaskEventPayload, TaskId,
    TaskStatus, UtcTimestamp,
};
use sqlx::SqliteConnection;

use crate::tasks::{
    TaskRecord, current_timestamp, ensure_exactly_one, insert_event, load_task, parse_event_kind,
    payload_json, task_from_record,
};
use crate::{Store, StoreError};

type TaskTypeRecord = (
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
);
type EventTypeRecord = (String, String, String, String, String, String);
type ClaimEventRecord = (i64, i64, String, String, String, String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimTaskRequest {
    pub task_id: TaskId,
    pub expected_repository_id: RepositoryId,
    pub expected_attempt: u32,
    pub expected_queued_event_id: EventId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimTaskReceipt {
    pub task: Task,
    pub started_event_id: EventId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimTaskOutcome {
    Applied(ClaimTaskReceipt),
    ExistingApplied(ClaimTaskReceipt),
    KnownNotApplied { current: Task },
    InvariantConflict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimTaskReconciliationOutcome {
    ExistingApplied(ClaimTaskReceipt),
    KnownNotApplied { current: Task },
    InvariantConflict,
}

#[derive(Debug)]
enum ExactClaimState {
    Queued(Task),
    Running(ClaimTaskReceipt),
    Terminal(Task),
    InvariantConflict,
}

#[derive(Debug)]
pub(crate) enum LoadedTask {
    Missing,
    Valid(Box<Task>),
    Invalid,
}

#[derive(Debug)]
enum LoadedEvent {
    Missing,
    Valid(ClaimEventRecord),
    Invalid,
}

impl Store {
    pub async fn claim_task(
        &self,
        request: ClaimTaskRequest,
    ) -> Result<ClaimTaskOutcome, StoreError> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        match classify_claim(&mut transaction, &request).await? {
            ExactClaimState::Queued(_) => {
                let now = current_timestamp()?;
                let updated = sqlx::query(
                    "UPDATE tasks \
                     SET status = 'running', started_at = ?, finished_at = NULL, \
                         failure_json = NULL \
                     WHERE id = ? AND repository_id = ? AND attempt = ? \
                       AND status = 'queued' AND last_event_id = ? \
                       AND started_at IS NULL AND finished_at IS NULL \
                       AND failure_json IS NULL",
                )
                .bind(now.to_string())
                .bind(request.task_id.to_string())
                .bind(request.expected_repository_id.to_string())
                .bind(i64::from(request.expected_attempt))
                .bind(request.expected_queued_event_id.get())
                .execute(&mut *transaction)
                .await?;
                ensure_exactly_one(
                    updated.rows_affected(),
                    "claim did not update exactly one queued task",
                )?;

                let event_id = insert_event(
                    &mut transaction,
                    request.task_id,
                    TaskEventKind::TaskStarted,
                    "{}",
                    now,
                )
                .await?;
                let updated = sqlx::query(
                    "UPDATE tasks SET last_event_id = ? \
                     WHERE id = ? AND repository_id = ? AND attempt = ? \
                       AND status = 'running' AND started_at = ? \
                       AND finished_at IS NULL AND failure_json IS NULL \
                       AND last_event_id = ?",
                )
                .bind(event_id.get())
                .bind(request.task_id.to_string())
                .bind(request.expected_repository_id.to_string())
                .bind(i64::from(request.expected_attempt))
                .bind(now.to_string())
                .bind(request.expected_queued_event_id.get())
                .execute(&mut *transaction)
                .await?;
                ensure_exactly_one(
                    updated.rows_affected(),
                    "claim did not advance exactly one task event cursor",
                )?;

                let claimed = load_task(&mut transaction, request.task_id)
                    .await?
                    .ok_or(StoreError::TaskNotFound)?;
                let payload = TaskEventPayload::TaskStarted {
                    task: claimed.clone(),
                };
                let updated = sqlx::query(
                    "UPDATE task_events SET payload_json = ? \
                     WHERE id = ? AND task_id = ? AND schema_version = 1 \
                       AND kind = 'task.started' AND payload_json = '{}'",
                )
                .bind(payload_json(&payload)?)
                .bind(event_id.get())
                .bind(request.task_id.to_string())
                .execute(&mut *transaction)
                .await?;
                ensure_exactly_one(
                    updated.rows_affected(),
                    "claim did not finalize exactly one started event",
                )?;

                let receipt = match classify_claim(&mut transaction, &request).await? {
                    ExactClaimState::Running(receipt)
                        if receipt.started_event_id == event_id && receipt.task == claimed =>
                    {
                        receipt
                    }
                    _ => {
                        return Err(StoreError::InvariantViolation(
                            "claim post-write receipt is inconsistent",
                        ));
                    }
                };
                transaction.commit().await?;
                Ok(ClaimTaskOutcome::Applied(receipt))
            }
            ExactClaimState::Running(receipt) => {
                transaction.commit().await?;
                Ok(ClaimTaskOutcome::ExistingApplied(receipt))
            }
            ExactClaimState::Terminal(current) => {
                transaction.commit().await?;
                Ok(ClaimTaskOutcome::KnownNotApplied { current })
            }
            ExactClaimState::InvariantConflict => {
                transaction.commit().await?;
                Ok(ClaimTaskOutcome::InvariantConflict)
            }
        }
    }

    /// Performs no DML, but intentionally uses `BEGIN IMMEDIATE` as a writer
    /// fence so reconciliation cannot classify a stale queued snapshot while a
    /// claim transaction is still in flight.
    pub async fn reconcile_task_claim(
        &self,
        request: &ClaimTaskRequest,
    ) -> Result<ClaimTaskReconciliationOutcome, StoreError> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let outcome = match classify_claim(&mut transaction, request).await? {
            ExactClaimState::Queued(current) | ExactClaimState::Terminal(current) => {
                ClaimTaskReconciliationOutcome::KnownNotApplied { current }
            }
            ExactClaimState::Running(receipt) => {
                ClaimTaskReconciliationOutcome::ExistingApplied(receipt)
            }
            ExactClaimState::InvariantConflict => ClaimTaskReconciliationOutcome::InvariantConflict,
        };
        transaction.commit().await?;
        Ok(outcome)
    }
}

#[cfg(feature = "test-support")]
pub(crate) async fn transition_running_for_test(
    store: &Store,
    task_id: TaskId,
    expected: TaskStatus,
) -> Result<crate::TransitionOutcome, StoreError> {
    let mut connection = store.pool.acquire().await?;
    let current = load_task(&mut connection, task_id)
        .await?
        .ok_or(StoreError::TaskNotFound)?;
    drop(connection);
    if current.status != expected {
        return Ok(crate::TransitionOutcome::Conflict { current });
    }

    let request = ClaimTaskRequest {
        task_id,
        expected_repository_id: current.repository_id,
        expected_attempt: current.attempt,
        expected_queued_event_id: current.last_event_id,
    };
    match store.claim_task(request).await? {
        ClaimTaskOutcome::Applied(receipt) => Ok(crate::TransitionOutcome::Applied {
            task: receipt.task,
            event_id: receipt.started_event_id,
        }),
        ClaimTaskOutcome::ExistingApplied(receipt) => Ok(crate::TransitionOutcome::Conflict {
            current: receipt.task,
        }),
        ClaimTaskOutcome::KnownNotApplied { current } => {
            Ok(crate::TransitionOutcome::Conflict { current })
        }
        ClaimTaskOutcome::InvariantConflict => Err(StoreError::InvariantViolation(
            "test start transition conflicts with the exact claim tuple",
        )),
    }
}

async fn classify_claim(
    connection: &mut SqliteConnection,
    request: &ClaimTaskRequest,
) -> Result<ExactClaimState, StoreError> {
    let task = match load_claim_task(connection, request.task_id).await? {
        LoadedTask::Missing => return Err(StoreError::TaskNotFound),
        LoadedTask::Invalid => return Ok(ExactClaimState::InvariantConflict),
        LoadedTask::Valid(task) => *task,
    };
    if request.expected_attempt == 0
        || task.id != request.task_id
        || task.repository_id != request.expected_repository_id
        || task.attempt != request.expected_attempt
    {
        return Ok(ExactClaimState::InvariantConflict);
    }

    let Some(queued_projection) = queued_projection(&task, request.expected_queued_event_id) else {
        return Ok(ExactClaimState::InvariantConflict);
    };
    if !queued_receipt_is_exact(connection, request, &queued_projection).await? {
        return Ok(ExactClaimState::InvariantConflict);
    }

    match task.status {
        TaskStatus::Queued => {
            if task == queued_projection
                && task_cursor_is_exact(connection, &task).await?
                && task.last_event_id == request.expected_queued_event_id
                && lifecycle_sequence_is_exact(
                    connection,
                    task.id,
                    &[(request.expected_queued_event_id, "task.queued")],
                )
                .await?
            {
                Ok(ExactClaimState::Queued(task))
            } else {
                Ok(ExactClaimState::InvariantConflict)
            }
        }
        TaskStatus::Running => {
            if let Some(receipt) = running_receipt(connection, request, &task).await? {
                Ok(ExactClaimState::Running(receipt))
            } else {
                Ok(ExactClaimState::InvariantConflict)
            }
        }
        TaskStatus::Completed
        | TaskStatus::Failed
        | TaskStatus::Cancelled
        | TaskStatus::Interrupted => {
            if terminal_receipt_is_exact(connection, request, &task).await? {
                Ok(ExactClaimState::Terminal(task))
            } else {
                Ok(ExactClaimState::InvariantConflict)
            }
        }
    }
}

pub(crate) async fn task_lifecycle_is_exact(
    connection: &mut SqliteConnection,
    expected: &Task,
) -> Result<bool, StoreError> {
    let current = match load_claim_task(connection, expected.id).await? {
        LoadedTask::Valid(current) => *current,
        LoadedTask::Missing | LoadedTask::Invalid => return Ok(false),
    };
    if current != *expected {
        return Ok(false);
    }
    let minimum: Option<i64> =
        sqlx::query_scalar("SELECT MIN(id) FROM task_events WHERE task_id = ?")
            .bind(expected.id.to_string())
            .fetch_one(&mut *connection)
            .await?;
    let Some(minimum) = minimum else {
        return Ok(false);
    };
    let Ok(expected_queued_event_id) = EventId::new(minimum) else {
        return Ok(false);
    };
    let request = ClaimTaskRequest {
        task_id: expected.id,
        expected_repository_id: expected.repository_id,
        expected_attempt: expected.attempt,
        expected_queued_event_id,
    };
    Ok(match classify_claim(connection, &request).await? {
        ExactClaimState::Queued(_) | ExactClaimState::Running(_) | ExactClaimState::Terminal(_) => {
            true
        }
        ExactClaimState::InvariantConflict => false,
    })
}

/// Accepts the exact Project 4 lifecycle or a canonical historical terminal
/// tuple whose final event and cursor are still authoritative. Older database
/// versions did not always persist the queued/started lifecycle prefix, so
/// read-only projection and recovery preflight must preserve those completed
/// rows without weakening active-task validation.
pub(crate) async fn task_lifecycle_is_projection_compatible(
    connection: &mut SqliteConnection,
    expected: &Task,
) -> Result<bool, StoreError> {
    if task_lifecycle_is_exact(connection, expected).await? {
        return Ok(true);
    }
    if !matches!(
        expected.status,
        TaskStatus::Completed
            | TaskStatus::Failed
            | TaskStatus::Cancelled
            | TaskStatus::Interrupted
    ) {
        return Ok(false);
    }
    let Some(finished_at) = expected.finished_at else {
        return Ok(false);
    };
    Ok(task_cursor_is_exact(connection, expected).await?
        && lifecycle_event_is_exact(
            connection,
            expected.last_event_id,
            expected.id,
            terminal_event_kind(expected.status),
            expected,
            finished_at,
        )
        .await?)
}

pub(crate) async fn load_claim_task(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<LoadedTask, StoreError> {
    let types: Option<TaskTypeRecord> = sqlx::query_as(
        "SELECT typeof(t.id), typeof(t.client_request_id), typeof(t.repository_id), \
                typeof(t.prompt), typeof(t.status), typeof(t.attempt), typeof(t.retry_of), \
                typeof(t.created_at), typeof(t.started_at), typeof(t.finished_at), \
                typeof(t.last_event_id), typeof(t.failure_json), typeof(d.readiness) \
         FROM tasks t \
         LEFT JOIN task_delivery_state d ON d.task_id = t.id \
         WHERE t.id = ?",
    )
    .bind(task_id.to_string())
    .fetch_optional(&mut *connection)
    .await?;
    let Some(types) = types else {
        return Ok(LoadedTask::Missing);
    };
    if !task_storage_types_are_valid(&types) {
        return Ok(LoadedTask::Invalid);
    }

    let record: TaskRecord = sqlx::query_as(
        "SELECT t.id, t.client_request_id, t.repository_id, t.prompt, t.status, t.attempt, \
                t.retry_of, t.created_at, t.started_at, t.finished_at, t.last_event_id, \
                t.failure_json, d.readiness \
         FROM tasks t \
         LEFT JOIN task_delivery_state d ON d.task_id = t.id \
         WHERE t.id = ?",
    )
    .bind(task_id.to_string())
    .fetch_one(&mut *connection)
    .await?;
    let raw = record.clone();
    Ok(match task_from_record(record) {
        Ok(task) if task_record_is_canonical(&raw, &task) => LoadedTask::Valid(Box::new(task)),
        Err(_) => LoadedTask::Invalid,
        Ok(_) => LoadedTask::Invalid,
    })
}

fn task_record_is_canonical(record: &TaskRecord, task: &Task) -> bool {
    let expected_failure = match task.failure.as_ref().map(serde_json::to_string).transpose() {
        Ok(expected_failure) => expected_failure,
        Err(_) => return false,
    };
    let expected_readiness = match task.delivery_readiness {
        DeliveryReadiness::Unreviewed => None,
        DeliveryReadiness::ReviewApproved => Some("review_approved"),
        DeliveryReadiness::ReviewRejected => Some("review_rejected"),
    };
    record.0 == task.id.to_string()
        && record.1 == task.client_request_id.to_string()
        && record.2 == task.repository_id.to_string()
        && record.4 == crate::tasks::status_text(task.status)
        && record.5 == i64::from(task.attempt)
        && record.6.as_deref() == task.retry_of.map(|value| value.to_string()).as_deref()
        && record.7 == task.created_at.to_string()
        && record.8.as_deref() == task.started_at.map(|value| value.to_string()).as_deref()
        && record.9.as_deref() == task.finished_at.map(|value| value.to_string()).as_deref()
        && record.10 == task.last_event_id.get()
        && record.11 == expected_failure
        && record.12.as_deref() == expected_readiness
}

fn task_storage_types_are_valid(types: &TaskTypeRecord) -> bool {
    types.0 == "text"
        && types.1 == "text"
        && types.2 == "text"
        && types.3 == "text"
        && types.4 == "text"
        && types.5 == "integer"
        && matches!(types.6.as_str(), "null" | "text")
        && types.7 == "text"
        && matches!(types.8.as_str(), "null" | "text")
        && matches!(types.9.as_str(), "null" | "text")
        && types.10 == "integer"
        && matches!(types.11.as_str(), "null" | "text")
        && matches!(types.12.as_str(), "null" | "text")
}

async fn load_claim_event(
    connection: &mut SqliteConnection,
    event_id: EventId,
) -> Result<LoadedEvent, StoreError> {
    let types: Option<EventTypeRecord> = sqlx::query_as(
        "SELECT typeof(id), typeof(schema_version), typeof(task_id), typeof(kind), \
                typeof(payload_json), typeof(created_at) \
         FROM task_events WHERE id = ?",
    )
    .bind(event_id.get())
    .fetch_optional(&mut *connection)
    .await?;
    let Some(types) = types else {
        return Ok(LoadedEvent::Missing);
    };
    if types.0 != "integer"
        || types.1 != "integer"
        || types.2 != "text"
        || types.3 != "text"
        || types.4 != "text"
        || types.5 != "text"
    {
        return Ok(LoadedEvent::Invalid);
    }
    let record = sqlx::query_as(
        "SELECT id, schema_version, task_id, kind, payload_json, created_at \
         FROM task_events WHERE id = ?",
    )
    .bind(event_id.get())
    .fetch_one(&mut *connection)
    .await?;
    Ok(LoadedEvent::Valid(record))
}

async fn queued_receipt_is_exact(
    connection: &mut SqliteConnection,
    request: &ClaimTaskRequest,
    queued: &Task,
) -> Result<bool, StoreError> {
    let minimum: Option<i64> =
        sqlx::query_scalar("SELECT MIN(id) FROM task_events WHERE task_id = ?")
            .bind(request.task_id.to_string())
            .fetch_one(&mut *connection)
            .await?;
    if minimum != Some(request.expected_queued_event_id.get()) {
        return Ok(false);
    }
    lifecycle_event_is_exact(
        connection,
        request.expected_queued_event_id,
        request.task_id,
        "task.queued",
        queued,
        queued.created_at,
    )
    .await
}

async fn running_receipt(
    connection: &mut SqliteConnection,
    request: &ClaimTaskRequest,
    task: &Task,
) -> Result<Option<ClaimTaskReceipt>, StoreError> {
    let Some(started_at) = task.started_at else {
        return Ok(None);
    };
    let started_ids = started_event_ids(connection, task.id).await?;
    let [started_event_id] = started_ids.as_slice() else {
        return Ok(None);
    };
    let Ok(started_event_id) = EventId::new(*started_event_id) else {
        return Ok(None);
    };
    let Some(running) = running_projection(task, started_event_id, started_at) else {
        return Ok(None);
    };
    if !task_cursor_is_exact(connection, task).await?
        || previous_task_event_id(connection, task.id, started_event_id).await?
            != Some(request.expected_queued_event_id.get())
        || !lifecycle_sequence_is_exact(
            connection,
            task.id,
            &[
                (request.expected_queued_event_id, "task.queued"),
                (started_event_id, "task.started"),
            ],
        )
        .await?
    {
        return Ok(None);
    }
    if !lifecycle_event_is_exact(
        connection,
        started_event_id,
        task.id,
        "task.started",
        &running,
        started_at,
    )
    .await?
    {
        return Ok(None);
    }
    Ok(Some(ClaimTaskReceipt {
        task: running,
        started_event_id,
    }))
}

async fn terminal_receipt_is_exact(
    connection: &mut SqliteConnection,
    request: &ClaimTaskRequest,
    task: &Task,
) -> Result<bool, StoreError> {
    let Some(finished_at) = task.finished_at else {
        return Ok(false);
    };
    if !task_cursor_is_exact(connection, task).await?
        || !lifecycle_event_is_exact(
            connection,
            task.last_event_id,
            task.id,
            terminal_event_kind(task.status),
            task,
            finished_at,
        )
        .await?
    {
        return Ok(false);
    }

    let started_ids = started_event_ids(connection, task.id).await?;
    match (task.started_at, started_ids.as_slice()) {
        (None, []) => Ok(
            previous_task_event_id(connection, task.id, task.last_event_id).await?
                == Some(request.expected_queued_event_id.get())
                && lifecycle_sequence_is_exact(
                    connection,
                    task.id,
                    &[
                        (request.expected_queued_event_id, "task.queued"),
                        (task.last_event_id, terminal_event_kind(task.status)),
                    ],
                )
                .await?,
        ),
        (Some(started_at), [started_event_id]) => {
            let Ok(started_event_id) = EventId::new(*started_event_id) else {
                return Ok(false);
            };
            let Some(running) = running_projection(task, started_event_id, started_at) else {
                return Ok(false);
            };
            if previous_task_event_id(connection, task.id, started_event_id).await?
                != Some(request.expected_queued_event_id.get())
                || !lifecycle_sequence_is_exact(
                    connection,
                    task.id,
                    &[
                        (request.expected_queued_event_id, "task.queued"),
                        (started_event_id, "task.started"),
                        (task.last_event_id, terminal_event_kind(task.status)),
                    ],
                )
                .await?
            {
                return Ok(false);
            }
            lifecycle_event_is_exact(
                connection,
                started_event_id,
                task.id,
                "task.started",
                &running,
                started_at,
            )
            .await
        }
        _ => Ok(false),
    }
}

async fn lifecycle_sequence_is_exact(
    connection: &mut SqliteConnection,
    task_id: TaskId,
    expected: &[(EventId, &str)],
) -> Result<bool, StoreError> {
    let kinds: Vec<(String, String)> = sqlx::query_as(
        "SELECT typeof(kind), \
                CASE WHEN typeof(kind) = 'text' THEN kind ELSE '' END \
         FROM task_events WHERE task_id = ? ORDER BY id",
    )
    .bind(task_id.to_string())
    .fetch_all(&mut *connection)
    .await?;
    if kinds
        .iter()
        .any(|(storage_type, kind)| storage_type != "text" || parse_event_kind(kind).is_err())
    {
        return Ok(false);
    }

    let lifecycle: Vec<(i64, String)> = sqlx::query_as(
        "SELECT id, kind FROM task_events \
         WHERE task_id = ? \
           AND kind IN (\
               'task.queued', 'task.started', 'task.completed', \
               'task.failed', 'task.cancelled', 'task.interrupted'\
           ) \
         ORDER BY id",
    )
    .bind(task_id.to_string())
    .fetch_all(&mut *connection)
    .await?;
    Ok(lifecycle.len() == expected.len()
        && lifecycle.iter().zip(expected).all(
            |((actual_id, actual_kind), (expected_id, expected_kind))| {
                *actual_id == expected_id.get() && actual_kind == expected_kind
            },
        ))
}

async fn task_cursor_is_exact(
    connection: &mut SqliteConnection,
    task: &Task,
) -> Result<bool, StoreError> {
    let maximum: Option<i64> =
        sqlx::query_scalar("SELECT MAX(id) FROM task_events WHERE task_id = ?")
            .bind(task.id.to_string())
            .fetch_one(&mut *connection)
            .await?;
    Ok(maximum == Some(task.last_event_id.get()))
}

async fn previous_task_event_id(
    connection: &mut SqliteConnection,
    task_id: TaskId,
    before: EventId,
) -> Result<Option<i64>, StoreError> {
    Ok(
        sqlx::query_scalar("SELECT MAX(id) FROM task_events WHERE task_id = ? AND id < ?")
            .bind(task_id.to_string())
            .bind(before.get())
            .fetch_one(&mut *connection)
            .await?,
    )
}

async fn started_event_ids(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<Vec<i64>, StoreError> {
    Ok(sqlx::query_scalar(
        "SELECT id FROM task_events \
         WHERE task_id = ? AND kind = 'task.started' ORDER BY id",
    )
    .bind(task_id.to_string())
    .fetch_all(&mut *connection)
    .await?)
}

async fn lifecycle_event_is_exact(
    connection: &mut SqliteConnection,
    event_id: EventId,
    task_id: TaskId,
    expected_kind: &str,
    expected_task: &Task,
    expected_timestamp: UtcTimestamp,
) -> Result<bool, StoreError> {
    let LoadedEvent::Valid(record) = load_claim_event(connection, event_id).await? else {
        return Ok(false);
    };
    if record.0 != event_id.get()
        || record.1 != 1
        || record.2 != task_id.to_string()
        || record.3 != expected_kind
        || !timestamp_is_exact(&record.5, expected_timestamp)
    {
        return Ok(false);
    }
    Ok(lifecycle_payload_matches(&record.4, expected_task))
}

fn lifecycle_payload_matches(payload_json: &str, expected_task: &Task) -> bool {
    let canonical = serde_json::json!({ "task": expected_task });
    let Ok(canonical_json) = serde_json::to_string(&canonical) else {
        return false;
    };
    if payload_json == canonical_json {
        return true;
    }
    if expected_task.delivery_readiness != DeliveryReadiness::Unreviewed {
        return false;
    }
    let mut legacy = canonical;
    let Some(task) = legacy
        .get_mut("task")
        .and_then(|value| value.as_object_mut())
    else {
        return false;
    };
    task.remove("delivery_readiness");
    serde_json::to_string(&legacy).is_ok_and(|legacy_json| payload_json == legacy_json)
}

fn timestamp_is_exact(raw: &str, expected: UtcTimestamp) -> bool {
    raw == expected.to_string()
        && UtcTimestamp::parse_rfc3339(raw).is_ok_and(|parsed| parsed == expected)
}

fn queued_projection(task: &Task, queued_event_id: EventId) -> Option<Task> {
    let mut queued = task.clone();
    queued.status = TaskStatus::Queued;
    queued.delivery_readiness = DeliveryReadiness::Unreviewed;
    queued.started_at = None;
    queued.finished_at = None;
    queued.last_event_id = queued_event_id;
    queued.failure = None;
    Task::try_from_stored(queued).ok()
}

fn running_projection(
    task: &Task,
    started_event_id: EventId,
    started_at: UtcTimestamp,
) -> Option<Task> {
    let mut running = task.clone();
    running.status = TaskStatus::Running;
    running.delivery_readiness = DeliveryReadiness::Unreviewed;
    running.started_at = Some(started_at);
    running.finished_at = None;
    running.last_event_id = started_event_id;
    running.failure = None;
    Task::try_from_stored(running).ok()
}

const fn terminal_event_kind(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Completed => "task.completed",
        TaskStatus::Failed => "task.failed",
        TaskStatus::Cancelled => "task.cancelled",
        TaskStatus::Interrupted => "task.interrupted",
        TaskStatus::Queued | TaskStatus::Running => "invalid",
    }
}
