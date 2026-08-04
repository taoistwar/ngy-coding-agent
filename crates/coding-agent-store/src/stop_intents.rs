use std::collections::HashSet;

use coding_agent_domain::{
    DeliveryReadiness, EventId, RepositoryId, Task, TaskEventKind, TaskFailure, TaskId, TaskStatus,
    UtcTimestamp,
};
use sqlx::{SqliteConnection, Transaction};

use crate::claims::{LoadedTask, load_claim_task, task_lifecycle_is_exact};
use crate::reviews::{load_reviews_for_task, validate_task_review_aggregate};
use crate::tasks::{append_lifecycle_event, current_timestamp, ensure_exactly_one, status_text};
use crate::{Store, StoreError};

pub const MAX_STOP_INTENT_BATCH: usize = 4;
const STOP_INTENT_INVARIANT: &str = "stop intent transaction is inconsistent";
const CRITICAL_FAILURE_MESSAGE: &str = "critical disk pressure stopped the task";

type StopIntentTypeRecord = (String, String, String, String, String);
type StopIntentRecord = (String, String, i64, String, String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StopIntentKind {
    UserCancelled,
    DiskPressureCritical,
}

impl StopIntentKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UserCancelled => "user_cancelled",
            Self::DiskPressureCritical => "disk_pressure_critical",
        }
    }

    fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "user_cancelled" => Ok(Self::UserCancelled),
            "disk_pressure_critical" => Ok(Self::DiskPressureCritical),
            _ => Err(stop_intent_invariant()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StopIntentRequest {
    pub task_id: TaskId,
    pub expected_repository_id: RepositoryId,
    pub expected_attempt: u32,
    pub kind: StopIntentKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StopIntentReceipt {
    pub task_id: TaskId,
    pub repository_id: RepositoryId,
    pub attempt: u32,
    pub kind: StopIntentKind,
    pub requested_at: UtcTimestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersistStopIntentOutcome {
    Applied(StopIntentReceipt),
    Existing(StopIntentReceipt),
    TerminalWon { current: Task },
    IntentConflict { existing: StopIntentReceipt },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StopIntentBatchItem {
    pub request: StopIntentRequest,
    pub outcome: PersistStopIntentOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StopIntentBatchReceipt {
    pub items: Vec<StopIntentBatchItem>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinalizeStoppedTaskRequest {
    pub task_id: TaskId,
    pub expected_repository_id: RepositoryId,
    pub expected_attempt: u32,
    pub expected_intent: StopIntentKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizeStoppedTaskReceipt {
    pub task: Task,
    pub intent: StopIntentReceipt,
    pub terminal_event_id: EventId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FinalizeStoppedTaskOutcome {
    Applied(FinalizeStoppedTaskReceipt),
    Existing(FinalizeStoppedTaskReceipt),
    InvariantConflict,
}

enum PersistClassification {
    Insert,
    Outcome(Box<PersistStopIntentOutcome>),
}

enum FinalizeClassification {
    Running {
        task: Task,
        intent: StopIntentReceipt,
    },
    Terminal(FinalizeStoppedTaskReceipt),
    InvariantConflict,
}

impl Store {
    pub async fn persist_stop_intent(
        &self,
        request: StopIntentRequest,
    ) -> Result<PersistStopIntentOutcome, StoreError> {
        let mut receipt = self.persist_stop_intent_batch(vec![request]).await?;
        let item = receipt
            .items
            .pop()
            .ok_or(StoreError::InvariantViolation(STOP_INTENT_INVARIANT))?;
        Ok(item.outcome)
    }

    pub async fn persist_stop_intent_batch(
        &self,
        mut requests: Vec<StopIntentRequest>,
    ) -> Result<StopIntentBatchReceipt, StoreError> {
        validate_batch_input(&requests)?;
        requests.sort_by_key(|request| request.task_id.as_uuid().as_u128());

        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let requested_at = current_timestamp()?;
        let mut items = Vec::with_capacity(requests.len());
        for request in requests {
            let outcome =
                persist_stop_intent_in_transaction(&mut transaction, request, requested_at).await?;
            items.push(StopIntentBatchItem { request, outcome });
        }
        validate_batch_post_state(&mut transaction, &items).await?;
        transaction.commit().await?;
        Ok(StopIntentBatchReceipt { items })
    }

    pub async fn finalize_stopped_task(
        &self,
        request: FinalizeStoppedTaskRequest,
    ) -> Result<FinalizeStoppedTaskOutcome, StoreError> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let outcome = finalize_stopped_task_in_transaction(&mut transaction, &request).await?;
        transaction.commit().await?;
        Ok(outcome)
    }
}

fn validate_batch_input(requests: &[StopIntentRequest]) -> Result<(), StoreError> {
    if requests.is_empty() || requests.len() > MAX_STOP_INTENT_BATCH {
        return Err(StoreError::InvariantViolation(
            "stop intent batch must contain one to four tasks",
        ));
    }
    let mut task_ids = HashSet::with_capacity(requests.len());
    if requests
        .iter()
        .any(|request| !task_ids.insert(request.task_id))
    {
        return Err(StoreError::InvariantViolation(
            "stop intent batch contains a duplicate task",
        ));
    }
    Ok(())
}

async fn persist_stop_intent_in_transaction(
    transaction: &mut Transaction<'_, sqlx::Sqlite>,
    request: StopIntentRequest,
    requested_at: UtcTimestamp,
) -> Result<PersistStopIntentOutcome, StoreError> {
    match classify_persist_stop_intent(transaction, &request).await? {
        PersistClassification::Outcome(outcome) => Ok(*outcome),
        PersistClassification::Insert => {
            let inserted = sqlx::query(
                "INSERT INTO task_stop_intents \
                     (task_id, repository_id, attempt, kind, requested_at) \
                 VALUES (?, ?, ?, ?, ?)",
            )
            .bind(request.task_id.to_string())
            .bind(request.expected_repository_id.to_string())
            .bind(i64::from(request.expected_attempt))
            .bind(request.kind.as_str())
            .bind(requested_at.to_string())
            .execute(&mut **transaction)
            .await?;
            ensure_exactly_one(
                inserted.rows_affected(),
                "stop intent insert did not affect exactly one row",
            )?;
            let receipt = StopIntentReceipt {
                task_id: request.task_id,
                repository_id: request.expected_repository_id,
                attempt: request.expected_attempt,
                kind: request.kind,
                requested_at,
            };
            match classify_persist_stop_intent(transaction, &request).await? {
                PersistClassification::Outcome(outcome) => match *outcome {
                    PersistStopIntentOutcome::Existing(existing) if existing == receipt => {
                        Ok(PersistStopIntentOutcome::Applied(receipt))
                    }
                    _ => Err(stop_intent_invariant()),
                },
                PersistClassification::Insert => Err(stop_intent_invariant()),
            }
        }
    }
}

async fn validate_batch_post_state(
    transaction: &mut Transaction<'_, sqlx::Sqlite>,
    items: &[StopIntentBatchItem],
) -> Result<(), StoreError> {
    for item in items {
        let expected = match &item.outcome {
            PersistStopIntentOutcome::Applied(receipt)
            | PersistStopIntentOutcome::Existing(receipt) => {
                PersistStopIntentOutcome::Existing(*receipt)
            }
            PersistStopIntentOutcome::TerminalWon { current } => {
                PersistStopIntentOutcome::TerminalWon {
                    current: current.clone(),
                }
            }
            PersistStopIntentOutcome::IntentConflict { existing } => {
                PersistStopIntentOutcome::IntentConflict {
                    existing: *existing,
                }
            }
        };
        match classify_persist_stop_intent(transaction, &item.request).await? {
            PersistClassification::Outcome(actual) if *actual == expected => {}
            _ => return Err(stop_intent_invariant()),
        }
    }
    Ok(())
}

async fn classify_persist_stop_intent(
    connection: &mut SqliteConnection,
    request: &StopIntentRequest,
) -> Result<PersistClassification, StoreError> {
    let task = match load_claim_task(connection, request.task_id).await? {
        LoadedTask::Missing => return Err(StoreError::TaskNotFound),
        LoadedTask::Valid(task) => *task,
        LoadedTask::Invalid => return Err(stop_intent_invariant()),
    };
    if request.expected_attempt == 0
        || task.id != request.task_id
        || task.repository_id != request.expected_repository_id
        || task.attempt != request.expected_attempt
    {
        return Err(stop_intent_invariant());
    }
    validate_task_for_stop_intent(connection, &task).await?;

    if let Some(existing) = load_stop_intent(connection, task.id).await? {
        validate_stop_intent_aggregate(connection, &task, existing).await?;
        return Ok(PersistClassification::Outcome(Box::new(
            if existing.kind == request.kind {
                PersistStopIntentOutcome::Existing(existing)
            } else {
                PersistStopIntentOutcome::IntentConflict { existing }
            },
        )));
    }

    match task.status {
        TaskStatus::Running if running_stop_target_is_exact(connection, &task).await? => {
            Ok(PersistClassification::Insert)
        }
        TaskStatus::Completed
        | TaskStatus::Failed
        | TaskStatus::Cancelled
        | TaskStatus::Interrupted => Ok(PersistClassification::Outcome(Box::new(
            PersistStopIntentOutcome::TerminalWon { current: task },
        ))),
        TaskStatus::Queued | TaskStatus::Running => Err(stop_intent_invariant()),
    }
}

pub(crate) async fn finalize_stopped_task_in_transaction(
    transaction: &mut Transaction<'_, sqlx::Sqlite>,
    request: &FinalizeStoppedTaskRequest,
) -> Result<FinalizeStoppedTaskOutcome, StoreError> {
    match classify_finalize_stop(transaction, request).await? {
        FinalizeClassification::Terminal(receipt) => {
            Ok(FinalizeStoppedTaskOutcome::Existing(receipt))
        }
        FinalizeClassification::InvariantConflict => {
            Ok(FinalizeStoppedTaskOutcome::InvariantConflict)
        }
        FinalizeClassification::Running { task, intent } => {
            let now = current_timestamp()?;
            let (status, failure, kind) = final_stop_state(request.expected_intent);
            let failure_json = failure.as_ref().map(serde_json::to_string).transpose()?;
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
                   AND EXISTS (\
                       SELECT 1 FROM task_stop_intents i \
                       WHERE i.task_id = tasks.id \
                         AND i.repository_id = tasks.repository_id \
                         AND i.attempt = tasks.attempt AND i.kind = ?\
                   )",
            )
            .bind(status_text(status))
            .bind(now.to_string())
            .bind(failure_json)
            .bind(task.id.to_string())
            .bind(task.repository_id.to_string())
            .bind(i64::from(task.attempt))
            .bind(task.started_at.expect("validated running task").to_string())
            .bind(task.last_event_id.get())
            .bind(request.expected_intent.as_str())
            .execute(&mut **transaction)
            .await?;
            ensure_exactly_one(
                updated.rows_affected(),
                "final stop did not update exactly one running task",
            )?;

            let (terminal_task, terminal_event_id) =
                append_lifecycle_event(transaction, task.id, kind, now).await?;
            let expected = FinalizeStoppedTaskReceipt {
                task: terminal_task,
                intent,
                terminal_event_id,
            };
            match classify_finalize_stop(transaction, request).await? {
                FinalizeClassification::Terminal(actual) if actual == expected => {
                    Ok(FinalizeStoppedTaskOutcome::Applied(expected))
                }
                _ => Err(stop_intent_invariant()),
            }
        }
    }
}

async fn classify_finalize_stop(
    connection: &mut SqliteConnection,
    request: &FinalizeStoppedTaskRequest,
) -> Result<FinalizeClassification, StoreError> {
    let task = match load_claim_task(connection, request.task_id).await? {
        LoadedTask::Missing => return Err(StoreError::TaskNotFound),
        LoadedTask::Valid(task) => *task,
        LoadedTask::Invalid => return Ok(FinalizeClassification::InvariantConflict),
    };
    if request.expected_attempt == 0
        || task.id != request.task_id
        || task.repository_id != request.expected_repository_id
        || task.attempt != request.expected_attempt
    {
        return Ok(FinalizeClassification::InvariantConflict);
    }
    let reviews = match load_reviews_for_task(connection, task.id).await {
        Ok(reviews) => reviews,
        Err(StoreError::InvariantViolation(_)) => {
            return Ok(FinalizeClassification::InvariantConflict);
        }
        Err(error) => return Err(error),
    };
    if let Err(error) = validate_task_review_aggregate(connection, &task, &reviews).await {
        return match error {
            StoreError::InvariantViolation(_) => Ok(FinalizeClassification::InvariantConflict),
            other => Err(other),
        };
    }
    if !task_lifecycle_is_exact(connection, &task).await? {
        return Ok(FinalizeClassification::InvariantConflict);
    }
    let Some(intent) = load_stop_intent(connection, task.id).await? else {
        return Ok(FinalizeClassification::InvariantConflict);
    };
    if intent.task_id != task.id
        || intent.repository_id != task.repository_id
        || intent.attempt != task.attempt
        || intent.kind != request.expected_intent
    {
        return Ok(FinalizeClassification::InvariantConflict);
    }

    match task.status {
        TaskStatus::Running if running_stop_target_is_exact(connection, &task).await? => {
            Ok(FinalizeClassification::Running { task, intent })
        }
        TaskStatus::Completed
        | TaskStatus::Failed
        | TaskStatus::Cancelled
        | TaskStatus::Interrupted
            if final_stop_task_is_exact(connection, &task, intent.kind).await? =>
        {
            Ok(FinalizeClassification::Terminal(
                FinalizeStoppedTaskReceipt {
                    terminal_event_id: task.last_event_id,
                    task,
                    intent,
                },
            ))
        }
        TaskStatus::Queued
        | TaskStatus::Running
        | TaskStatus::Completed
        | TaskStatus::Failed
        | TaskStatus::Cancelled
        | TaskStatus::Interrupted => Ok(FinalizeClassification::InvariantConflict),
    }
}

pub(crate) async fn ensure_no_stop_intent(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<(), StoreError> {
    if load_stop_intent(connection, task_id).await?.is_some() {
        Err(StoreError::InvariantViolation(
            "generic task mutation conflicts with a durable stop intent",
        ))
    } else {
        Ok(())
    }
}

pub(crate) async fn validate_optional_stop_intent(
    connection: &mut SqliteConnection,
    task: &Task,
) -> Result<Option<StopIntentReceipt>, StoreError> {
    let Some(intent) = load_stop_intent(connection, task.id).await? else {
        return Ok(None);
    };
    validate_stop_intent_aggregate(connection, task, intent).await?;
    Ok(Some(intent))
}

pub(crate) async fn validate_stop_intent_graph(
    connection: &mut SqliteConnection,
) -> Result<(), StoreError> {
    let type_rows: Vec<StopIntentTypeRecord> = sqlx::query_as(
        "SELECT typeof(task_id), typeof(repository_id), typeof(attempt), \
                typeof(kind), typeof(requested_at) \
         FROM task_stop_intents ORDER BY task_id",
    )
    .fetch_all(&mut *connection)
    .await?;
    if type_rows.iter().any(|types| {
        types.0 != "text"
            || types.1 != "text"
            || types.2 != "integer"
            || types.3 != "text"
            || types.4 != "text"
    }) {
        return Err(stop_intent_invariant());
    }
    let records: Vec<StopIntentRecord> = sqlx::query_as(
        "SELECT task_id, repository_id, attempt, kind, requested_at \
         FROM task_stop_intents ORDER BY task_id",
    )
    .fetch_all(&mut *connection)
    .await?;
    if records.len() != type_rows.len() {
        return Err(stop_intent_invariant());
    }
    let mut task_ids = HashSet::with_capacity(records.len());
    for record in records {
        let intent = stop_intent_from_record(record)?;
        if !task_ids.insert(intent.task_id) {
            return Err(stop_intent_invariant());
        }
        let task = match load_claim_task(connection, intent.task_id).await? {
            LoadedTask::Missing | LoadedTask::Invalid => return Err(stop_intent_invariant()),
            LoadedTask::Valid(task) => *task,
        };
        validate_task_for_stop_intent(connection, &task).await?;
        validate_stop_intent_aggregate(connection, &task, intent).await?;
    }
    Ok(())
}

pub(crate) async fn load_running_stop_intents(
    connection: &mut SqliteConnection,
) -> Result<Vec<StopIntentReceipt>, StoreError> {
    validate_stop_intent_graph(connection).await?;
    let records: Vec<StopIntentRecord> = sqlx::query_as(
        "SELECT i.task_id, i.repository_id, i.attempt, i.kind, i.requested_at \
         FROM task_stop_intents i \
         INNER JOIN tasks t ON t.id = i.task_id \
         WHERE t.status = 'running' \
         ORDER BY i.requested_at, i.task_id",
    )
    .fetch_all(&mut *connection)
    .await?;
    records.into_iter().map(stop_intent_from_record).collect()
}

pub(crate) async fn load_stop_intents_in_recovery_order(
    connection: &mut SqliteConnection,
) -> Result<Vec<StopIntentReceipt>, StoreError> {
    let records: Vec<StopIntentRecord> = sqlx::query_as(
        "SELECT i.task_id, i.repository_id, i.attempt, i.kind, i.requested_at \
         FROM task_stop_intents i \
         INNER JOIN tasks t ON t.id = i.task_id \
         WHERE t.status = 'running' \
         ORDER BY i.requested_at, i.task_id",
    )
    .fetch_all(&mut *connection)
    .await?;
    records.into_iter().map(stop_intent_from_record).collect()
}

async fn load_stop_intent(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<Option<StopIntentReceipt>, StoreError> {
    if !validate_stop_intent_task_ids(connection, task_id).await? {
        return Ok(None);
    }
    let types: Option<StopIntentTypeRecord> = sqlx::query_as(
        "SELECT typeof(task_id), typeof(repository_id), typeof(attempt), \
                typeof(kind), typeof(requested_at) \
         FROM task_stop_intents WHERE task_id = ?",
    )
    .bind(task_id.to_string())
    .fetch_optional(&mut *connection)
    .await?;
    let Some(types) = types else {
        return Err(stop_intent_invariant());
    };
    if types.0 != "text"
        || types.1 != "text"
        || types.2 != "integer"
        || types.3 != "text"
        || types.4 != "text"
    {
        return Err(stop_intent_invariant());
    }
    let record: StopIntentRecord = sqlx::query_as(
        "SELECT task_id, repository_id, attempt, kind, requested_at \
         FROM task_stop_intents WHERE task_id = ?",
    )
    .bind(task_id.to_string())
    .fetch_one(&mut *connection)
    .await?;
    stop_intent_from_record(record).map(Some)
}

async fn validate_stop_intent_task_ids(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<bool, StoreError> {
    // `uuid::Uuid::parse_str` accepts canonical, simple, braced, and URN forms. The
    // primary key uses SQLite's binary text equality, so every point lookup must
    // validate all retained raw IDs before choosing one logical winner.
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT typeof(task_id), CAST(task_id AS TEXT) \
         FROM task_stop_intents ORDER BY task_id",
    )
    .fetch_all(&mut *connection)
    .await
    .map_err(|error| match error {
        sqlx::Error::ColumnDecode { .. } => stop_intent_invariant(),
        other => StoreError::Database(other),
    })?;
    let mut task_ids = HashSet::with_capacity(rows.len());
    for (storage_type, raw) in rows {
        if storage_type != "text" {
            return Err(stop_intent_invariant());
        }
        let parsed: TaskId = raw.parse().map_err(|_| stop_intent_invariant())?;
        if raw != parsed.to_string() || !task_ids.insert(parsed) {
            return Err(stop_intent_invariant());
        }
    }
    Ok(task_ids.contains(&task_id))
}

fn stop_intent_from_record(record: StopIntentRecord) -> Result<StopIntentReceipt, StoreError> {
    let task_id: TaskId = record.0.parse().map_err(|_| stop_intent_invariant())?;
    let repository_id: RepositoryId = record.1.parse().map_err(|_| stop_intent_invariant())?;
    let attempt = u32::try_from(record.2).map_err(|_| stop_intent_invariant())?;
    let kind = StopIntentKind::parse(&record.3)?;
    let requested_at =
        UtcTimestamp::parse_rfc3339(&record.4).map_err(|_| stop_intent_invariant())?;
    if record.0 != task_id.to_string()
        || record.1 != repository_id.to_string()
        || attempt == 0
        || record.4 != requested_at.to_string()
    {
        return Err(stop_intent_invariant());
    }
    Ok(StopIntentReceipt {
        task_id,
        repository_id,
        attempt,
        kind,
        requested_at,
    })
}

async fn validate_task_for_stop_intent(
    connection: &mut SqliteConnection,
    task: &Task,
) -> Result<(), StoreError> {
    if !task_lifecycle_is_exact(connection, task).await? {
        return Err(stop_intent_invariant());
    }
    let reviews = load_reviews_for_task(connection, task.id).await?;
    validate_task_review_aggregate(connection, task, &reviews).await
}

async fn validate_stop_intent_aggregate(
    connection: &mut SqliteConnection,
    task: &Task,
    intent: StopIntentReceipt,
) -> Result<(), StoreError> {
    if intent.task_id != task.id
        || intent.repository_id != task.repository_id
        || intent.attempt != task.attempt
    {
        return Err(stop_intent_invariant());
    }
    if !task_lifecycle_is_exact(connection, task).await? {
        return Err(stop_intent_invariant());
    }
    match task.status {
        TaskStatus::Running if running_stop_target_is_exact(connection, task).await? => Ok(()),
        TaskStatus::Completed
        | TaskStatus::Failed
        | TaskStatus::Cancelled
        | TaskStatus::Interrupted
            if final_stop_task_is_exact(connection, task, intent.kind).await? =>
        {
            Ok(())
        }
        TaskStatus::Queued
        | TaskStatus::Running
        | TaskStatus::Completed
        | TaskStatus::Failed
        | TaskStatus::Cancelled
        | TaskStatus::Interrupted => Err(stop_intent_invariant()),
    }
}

async fn running_stop_target_is_exact(
    connection: &mut SqliteConnection,
    task: &Task,
) -> Result<bool, StoreError> {
    Ok(task.status == TaskStatus::Running
        && task.delivery_readiness == DeliveryReadiness::Unreviewed
        && task.started_at.is_some()
        && task.finished_at.is_none()
        && task.failure.is_none()
        && delivery_count(connection, task.id).await? == 0)
}

async fn final_stop_task_is_exact(
    connection: &mut SqliteConnection,
    task: &Task,
    kind: StopIntentKind,
) -> Result<bool, StoreError> {
    if task.delivery_readiness != DeliveryReadiness::Unreviewed
        || task.started_at.is_none()
        || task.finished_at.is_none()
        || delivery_count(connection, task.id).await? != 0
        || !final_stop_terminal_event_is_canonical(connection, task, kind).await?
    {
        return Ok(false);
    }
    let raw_failure: Option<String> =
        sqlx::query_scalar("SELECT failure_json FROM tasks WHERE id = ?")
            .bind(task.id.to_string())
            .fetch_one(&mut *connection)
            .await?;
    Ok(match kind {
        StopIntentKind::UserCancelled => {
            task.status == TaskStatus::Cancelled && task.failure.is_none() && raw_failure.is_none()
        }
        StopIntentKind::DiskPressureCritical => {
            let expected = critical_failure();
            task.status == TaskStatus::Failed
                && task.failure.as_ref() == Some(&expected)
                && raw_failure.as_deref() == Some(serde_json::to_string(&expected)?.as_str())
        }
    })
}

async fn final_stop_terminal_event_is_canonical(
    connection: &mut SqliteConnection,
    task: &Task,
    kind: StopIntentKind,
) -> Result<bool, StoreError> {
    let types: Option<(String, String, String, String, String, String)> = sqlx::query_as(
        "SELECT typeof(id), typeof(schema_version), typeof(task_id), typeof(kind), \
                typeof(payload_json), typeof(created_at) \
         FROM task_events WHERE id = ?",
    )
    .bind(task.last_event_id.get())
    .fetch_optional(&mut *connection)
    .await?;
    let Some(types) = types else {
        return Ok(false);
    };
    if types
        != (
            "integer".to_owned(),
            "integer".to_owned(),
            "text".to_owned(),
            "text".to_owned(),
            "text".to_owned(),
            "text".to_owned(),
        )
    {
        return Ok(false);
    }
    let record: (i64, i64, String, String, String, String) = sqlx::query_as(
        "SELECT id, schema_version, task_id, kind, payload_json, created_at \
         FROM task_events WHERE id = ?",
    )
    .bind(task.last_event_id.get())
    .fetch_one(&mut *connection)
    .await?;
    let expected_kind = match kind {
        StopIntentKind::UserCancelled => "task.cancelled",
        StopIntentKind::DiskPressureCritical => "task.failed",
    };
    let expected_payload = serde_json::to_string(&serde_json::json!({ "task": task }))?;
    Ok(record.0 == task.last_event_id.get()
        && record.1 == 1
        && record.2 == task.id.to_string()
        && record.3 == expected_kind
        && record.4 == expected_payload
        && task
            .finished_at
            .is_some_and(|finished_at| record.5 == finished_at.to_string()))
}

async fn delivery_count(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<i64, StoreError> {
    Ok(
        sqlx::query_scalar("SELECT COUNT(*) FROM task_delivery_state WHERE task_id = ?")
            .bind(task_id.to_string())
            .fetch_one(&mut *connection)
            .await?,
    )
}

fn final_stop_state(kind: StopIntentKind) -> (TaskStatus, Option<TaskFailure>, TaskEventKind) {
    match kind {
        StopIntentKind::UserCancelled => {
            (TaskStatus::Cancelled, None, TaskEventKind::TaskCancelled)
        }
        StopIntentKind::DiskPressureCritical => (
            TaskStatus::Failed,
            Some(critical_failure()),
            TaskEventKind::TaskFailed,
        ),
    }
}

fn critical_failure() -> TaskFailure {
    TaskFailure {
        code: "DISK_PRESSURE_CRITICAL".to_owned(),
        message: CRITICAL_FAILURE_MESSAGE.to_owned(),
        retryable: true,
    }
}

const fn stop_intent_invariant() -> StoreError {
    StoreError::InvariantViolation(STOP_INTENT_INVARIANT)
}
