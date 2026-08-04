use std::num::NonZeroU32;
use std::path::PathBuf;

use coding_agent_domain::{
    ActivityEntry, CanonicalPath, DeliveryReadiness, DiffSnapshot, DomainError, EventCursor,
    EventId, PlanSnapshot, Repository, ReviewEvidence, Task, TaskEvent, TaskEventKind,
    TaskEventPayload, TaskFailure, TaskId, TaskStatus, TestSnapshot, TimelineEntry, UtcTimestamp,
};
use serde_json::Value;
use sqlx::SqliteConnection;

use crate::claims::task_lifecycle_is_projection_compatible;
use crate::reviews::{
    load_review_by_event, load_reviews_for_task, validate_task_event_cursor,
    validate_task_review_aggregate,
};
use crate::stop_intents::{
    StopIntentReceipt, load_running_stop_intents, validate_optional_stop_intent,
};
use crate::tasks::{
    TaskRecord, count_queued_tasks, latest_event_cursor, load_task_record, parse_event_kind,
    task_from_record,
};
use crate::{Store, StoreError};

type RepositoryRecord = (
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
type EventRecord = (i64, i64, String, String, String, String);

const BOOTSTRAP_PROJECTION_INVARIANT: &str = "scheduler bootstrap projection graph is inconsistent";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapSnapshot {
    pub repositories: Vec<Repository>,
    pub tasks: Vec<Task>,
    pub latest_event_id: EventCursor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerBootstrapSnapshot {
    pub repositories: Vec<Repository>,
    pub tasks: Vec<Task>,
    pub running_stop_intents: Vec<StopIntentReceipt>,
    pub latest_event_id: EventCursor,
    pub membership_event_id: EventCursor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskDetail {
    pub task: Task,
    pub plan: Option<PlanSnapshot>,
    pub activity: Vec<ActivityEntry>,
    pub diff: Option<DiffSnapshot>,
    pub tests: Option<TestSnapshot>,
    pub reviews: Vec<ReviewEvidence>,
    pub timeline: Vec<TimelineEntry>,
    pub event_cursor: EventCursor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventPage {
    pub events: Vec<TaskEvent>,
    pub high_watermark: EventCursor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueCapacity {
    pub queued_tasks: u64,
    pub max_queued_tasks: NonZeroU32,
}

impl QueueCapacity {
    pub fn available_tasks(&self) -> u32 {
        match u32::try_from(self.queued_tasks) {
            Ok(queued_tasks) => self.max_queued_tasks.get().saturating_sub(queued_tasks),
            Err(_) => 0,
        }
    }
}

impl Store {
    pub async fn bootstrap_snapshot(&self) -> Result<BootstrapSnapshot, StoreError> {
        let mut transaction = self.pool.begin().await?;
        let state = load_bootstrap_state(&mut transaction).await?;
        let latest_event_id = latest_event_cursor(&mut transaction).await?;
        transaction.commit().await?;

        Ok(BootstrapSnapshot {
            repositories: state.repositories,
            tasks: state.tasks,
            latest_event_id,
        })
    }

    pub async fn scheduler_bootstrap_snapshot(
        &self,
    ) -> Result<SchedulerBootstrapSnapshot, StoreError> {
        let mut transaction = self.pool.begin().await?;
        let state = load_bootstrap_state(&mut transaction).await?;
        let membership_event_id = membership_event_cursor(&mut transaction, None).await?;
        let latest_event_id = latest_event_cursor(&mut transaction).await?;
        if membership_event_id > latest_event_id {
            return Err(bootstrap_projection_invariant());
        }
        transaction.commit().await?;
        Ok(SchedulerBootstrapSnapshot {
            repositories: state.repositories,
            tasks: state.tasks,
            running_stop_intents: state.running_stop_intents,
            latest_event_id,
            membership_event_id,
        })
    }

    pub async fn task_detail(&self, task_id: TaskId) -> Result<Option<TaskDetail>, StoreError> {
        let mut transaction = self.pool.begin().await?;
        ensure_task_storage_types(&mut transaction, Some(task_id)).await?;
        let Some(record) = load_task_record(&mut transaction, task_id).await? else {
            transaction.commit().await?;
            return Ok(None);
        };
        let task = task_from_canonical_projection_record(record)?;
        let events = load_events(&mut transaction, Some(task_id), EventCursor::ZERO, None).await?;
        let reviews = load_reviews_for_task(&mut transaction, task_id).await?;
        validate_task_event_cursor(&mut transaction, &task).await?;
        validate_task_review_aggregate(&mut transaction, &task, &reviews).await?;
        if !task_lifecycle_is_projection_compatible(&mut transaction, &task).await? {
            return Err(StoreError::InvariantViolation(
                "task lifecycle aggregate is inconsistent",
            ));
        }
        validate_optional_stop_intent(&mut transaction, &task).await?;
        let event_cursor = latest_event_cursor(&mut transaction).await?;
        transaction.commit().await?;

        Ok(Some(project_task_detail(
            task,
            events,
            reviews,
            event_cursor,
        )?))
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

    pub async fn membership_watermark_through(
        &self,
        through: EventCursor,
    ) -> Result<EventCursor, StoreError> {
        let mut connection = self.pool.acquire().await?;
        membership_event_cursor(&mut connection, Some(through)).await
    }

    pub async fn queue_capacity(
        &self,
        max_queued_tasks: NonZeroU32,
    ) -> Result<QueueCapacity, StoreError> {
        let mut connection = self.pool.acquire().await?;
        let queued_tasks = count_queued_tasks(&mut connection).await?;
        Ok(QueueCapacity {
            queued_tasks,
            max_queued_tasks,
        })
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

struct ProjectionBootstrapState {
    repositories: Vec<Repository>,
    tasks: Vec<Task>,
    running_stop_intents: Vec<StopIntentReceipt>,
}

async fn load_bootstrap_state(
    connection: &mut SqliteConnection,
) -> Result<ProjectionBootstrapState, StoreError> {
    ensure_bootstrap_projection_integrity(connection).await?;
    let repository_records: Vec<RepositoryRecord> = sqlx::query_as(
        "SELECT id, selected_path, display_name, git_root, cargo_workspace_root, \
                git_identity_key, cargo_identity_key, created_at, last_opened_at \
         FROM repositories \
         ORDER BY last_opened_at DESC, id",
    )
    .fetch_all(&mut *connection)
    .await?;
    let repositories = repository_records
        .into_iter()
        .map(repository_from_record)
        .collect::<Result<Vec<_>, _>>()?;

    let task_records: Vec<TaskRecord> = sqlx::query_as(
        "SELECT t.id, t.client_request_id, t.repository_id, t.prompt, t.status, t.attempt, \
                t.retry_of, t.created_at, t.started_at, t.finished_at, t.last_event_id, \
                t.failure_json, d.readiness \
         FROM tasks t \
         LEFT JOIN task_delivery_state d ON d.task_id = t.id \
         ORDER BY t.created_at DESC, t.id",
    )
    .fetch_all(&mut *connection)
    .await?;
    let tasks = task_records
        .into_iter()
        .map(task_from_canonical_projection_record)
        .collect::<Result<Vec<_>, _>>()?;
    for task in &tasks {
        let reviews = load_reviews_for_task(connection, task.id).await?;
        validate_task_review_aggregate(connection, task, &reviews).await?;
        validate_task_event_cursor(connection, task).await?;
        if !task_lifecycle_is_projection_compatible(connection, task).await? {
            return Err(StoreError::InvariantViolation(
                "task lifecycle aggregate is inconsistent",
            ));
        }
    }
    let running_stop_intents = load_running_stop_intents(connection).await?;
    Ok(ProjectionBootstrapState {
        repositories,
        tasks,
        running_stop_intents,
    })
}

async fn membership_event_cursor(
    connection: &mut SqliteConnection,
    through: Option<EventCursor>,
) -> Result<EventCursor, StoreError> {
    let maximum: i64 = match through {
        Some(through) => {
            sqlx::query_scalar(
                "SELECT COALESCE(MAX(id), 0) FROM task_events \
             WHERE id <= ? AND kind IN (\
                 'task.queued', 'task.started', 'task.completed', \
                 'task.failed', 'task.cancelled', 'task.interrupted'\
             )",
            )
            .bind(through.get())
            .fetch_one(&mut *connection)
            .await?
        }
        None => {
            sqlx::query_scalar(
                "SELECT COALESCE(MAX(id), 0) FROM task_events \
             WHERE kind IN (\
                 'task.queued', 'task.started', 'task.completed', \
                 'task.failed', 'task.cancelled', 'task.interrupted'\
             )",
            )
            .fetch_one(&mut *connection)
            .await?
        }
    };
    EventCursor::new(maximum).map_err(|_| bootstrap_projection_invariant())
}

async fn ensure_bootstrap_projection_integrity(
    connection: &mut SqliteConnection,
) -> Result<(), StoreError> {
    ensure_repository_storage_types(connection).await?;
    ensure_task_storage_types(connection, None).await?;
    ensure_bootstrap_event_integrity(connection).await?;
    ensure_review_storage_types(connection).await?;
    ensure_delivery_storage_types(connection).await?;
    ensure_bootstrap_graph_links(connection).await
}

async fn ensure_repository_storage_types(
    connection: &mut SqliteConnection,
) -> Result<(), StoreError> {
    let invalid: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM repositories \
         WHERE typeof(id) != 'text' \
            OR typeof(selected_path) != 'text' \
            OR typeof(display_name) != 'text' \
            OR typeof(git_root) != 'text' \
            OR typeof(cargo_workspace_root) != 'text' \
            OR typeof(git_identity_key) != 'text' \
            OR typeof(cargo_identity_key) != 'text' \
            OR typeof(created_at) != 'text' \
            OR typeof(last_opened_at) != 'text'",
    )
    .fetch_one(&mut *connection)
    .await?;
    ensure_no_bootstrap_integrity_failures(invalid)
}

async fn ensure_bootstrap_event_integrity(
    connection: &mut SqliteConnection,
) -> Result<(), StoreError> {
    let invalid_storage: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_events \
         WHERE typeof(id) != 'integer' \
            OR typeof(schema_version) != 'integer' \
            OR typeof(task_id) != 'text' \
            OR typeof(kind) != 'text' \
            OR typeof(payload_json) != 'text' \
            OR typeof(created_at) != 'text'",
    )
    .fetch_one(&mut *connection)
    .await?;
    ensure_no_bootstrap_integrity_failures(invalid_storage)?;

    let invalid_graph: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) \
         FROM task_events e \
         LEFT JOIN tasks t ON t.id = e.task_id \
         WHERE e.schema_version != 1 \
            OR e.kind NOT IN (\
                'task.queued', 'task.started', 'plan.updated', \
                'activity.appended', 'diff.updated', 'test.updated', \
                'review.updated', 'task.completed', 'task.failed', \
                'task.cancelled', 'task.interrupted'\
            ) \
            OR json_valid(e.payload_json) != 1 \
            OR t.id IS NULL",
    )
    .fetch_one(&mut *connection)
    .await?;
    ensure_no_bootstrap_integrity_failures(invalid_graph)?;

    let records: Vec<EventRecord> = sqlx::query_as(
        "SELECT id, schema_version, task_id, kind, payload_json, created_at \
         FROM task_events ORDER BY id",
    )
    .fetch_all(&mut *connection)
    .await?;
    for record in &records {
        validate_bootstrap_event_metadata(record)?;
    }
    Ok(())
}

async fn ensure_review_storage_types(connection: &mut SqliteConnection) -> Result<(), StoreError> {
    let invalid: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_review_evidence \
         WHERE typeof(task_id) != 'text' \
            OR typeof(repository_id) != 'text' \
            OR typeof(attempt) != 'integer' \
            OR typeof(review_round) != 'integer' \
            OR typeof(workspace_generation) != 'integer' \
            OR typeof(digest_algorithm) != 'text' \
            OR typeof(workspace_digest) != 'text' \
            OR typeof(decision_source) != 'text' \
            OR typeof(verdict) != 'text' \
            OR typeof(summary) != 'text' \
            OR typeof(findings_json) != 'text' \
            OR typeof(added_checks_json) != 'text' \
            OR typeof(required_checks_json) != 'text' \
            OR typeof(check_evidence_json) != 'text' \
            OR typeof(coverage_json) != 'text' \
            OR typeof(created_at) != 'text' \
            OR typeof(event_id) != 'integer' \
            OR typeof(event_kind) != 'text'",
    )
    .fetch_one(&mut *connection)
    .await?;
    ensure_no_bootstrap_integrity_failures(invalid)
}

async fn ensure_delivery_storage_types(
    connection: &mut SqliteConnection,
) -> Result<(), StoreError> {
    let invalid: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_delivery_state \
         WHERE typeof(task_id) != 'text' \
            OR typeof(readiness) != 'text' \
            OR typeof(final_review_round) != 'integer' \
            OR typeof(final_verdict) != 'text' \
            OR typeof(decided_at) != 'text'",
    )
    .fetch_one(&mut *connection)
    .await?;
    ensure_no_bootstrap_integrity_failures(invalid)
}

async fn ensure_bootstrap_graph_links(connection: &mut SqliteConnection) -> Result<(), StoreError> {
    let invalid_tasks: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) \
         FROM tasks t \
         LEFT JOIN repositories r ON r.id = t.repository_id \
         LEFT JOIN tasks retry ON retry.id = t.retry_of \
         WHERE r.id IS NULL \
            OR (t.retry_of IS NOT NULL AND retry.id IS NULL)",
    )
    .fetch_one(&mut *connection)
    .await?;
    ensure_no_bootstrap_integrity_failures(invalid_tasks)?;

    let invalid_reviews: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) \
         FROM task_review_evidence r \
         LEFT JOIN tasks t \
           ON t.id = r.task_id \
          AND t.repository_id = r.repository_id \
          AND t.attempt = r.attempt \
         LEFT JOIN task_events e \
           ON e.id = r.event_id \
          AND e.task_id = r.task_id \
          AND e.kind = r.event_kind \
         WHERE t.id IS NULL OR e.id IS NULL",
    )
    .fetch_one(&mut *connection)
    .await?;
    ensure_no_bootstrap_integrity_failures(invalid_reviews)?;

    let invalid_deliveries: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) \
         FROM task_delivery_state d \
         LEFT JOIN tasks t ON t.id = d.task_id \
         LEFT JOIN task_review_evidence r \
           ON r.task_id = d.task_id \
          AND r.review_round = d.final_review_round \
          AND r.verdict = d.final_verdict \
         WHERE t.id IS NULL OR r.task_id IS NULL",
    )
    .fetch_one(&mut *connection)
    .await?;
    ensure_no_bootstrap_integrity_failures(invalid_deliveries)
}

fn validate_bootstrap_event_metadata(record: &EventRecord) -> Result<(), StoreError> {
    EventId::new(record.0).map_err(|_| bootstrap_projection_invariant())?;
    if record.1 != 1 {
        return Err(bootstrap_projection_invariant());
    }
    let task_id: TaskId = record
        .2
        .parse()
        .map_err(|_| bootstrap_projection_invariant())?;
    if record.2 != task_id.to_string() {
        return Err(bootstrap_projection_invariant());
    }
    parse_event_kind(&record.3).map_err(|_| bootstrap_projection_invariant())?;
    let created_at =
        UtcTimestamp::parse_rfc3339(&record.5).map_err(|_| bootstrap_projection_invariant())?;
    if record.5 != created_at.to_string() {
        return Err(bootstrap_projection_invariant());
    }
    Ok(())
}

fn ensure_no_bootstrap_integrity_failures(invalid: i64) -> Result<(), StoreError> {
    if invalid == 0 {
        Ok(())
    } else {
        Err(bootstrap_projection_invariant())
    }
}

async fn load_events(
    connection: &mut SqliteConnection,
    task_id: Option<TaskId>,
    after: EventCursor,
    page: Option<(EventCursor, usize)>,
) -> Result<Vec<TaskEvent>, StoreError> {
    ensure_event_storage_types(connection, task_id, after, page).await?;
    let records: Vec<EventRecord> = match (task_id, page) {
        (None, None) => {
            sqlx::query_as(
                "SELECT id, schema_version, task_id, kind, payload_json, created_at \
                 FROM task_events WHERE id > ? ORDER BY id",
            )
            .bind(after.get())
            .fetch_all(&mut *connection)
            .await?
        }
        (Some(task_id), None) => {
            sqlx::query_as(
                "SELECT id, schema_version, task_id, kind, payload_json, created_at \
                 FROM task_events WHERE task_id = ? AND id > ? ORDER BY id",
            )
            .bind(task_id.to_string())
            .bind(after.get())
            .fetch_all(&mut *connection)
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
            .fetch_all(&mut *connection)
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
            .fetch_all(&mut *connection)
            .await?
        }
    };
    let mut events = Vec::with_capacity(records.len());
    for record in records {
        events.push(event_from_record(connection, record).await?);
    }
    Ok(events)
}

async fn ensure_task_storage_types(
    connection: &mut SqliteConnection,
    task_id: Option<TaskId>,
) -> Result<(), StoreError> {
    let invalid: i64 = match task_id {
        None => {
            sqlx::query_scalar(
                "SELECT COUNT(*) \
                 FROM tasks t \
                 LEFT JOIN task_delivery_state d ON d.task_id = t.id \
                 WHERE typeof(t.id) != 'text' \
                    OR typeof(t.client_request_id) != 'text' \
                    OR typeof(t.repository_id) != 'text' \
                    OR typeof(t.prompt) != 'text' \
                    OR typeof(t.status) != 'text' \
                    OR typeof(t.attempt) != 'integer' \
                    OR typeof(t.retry_of) NOT IN ('null', 'text') \
                    OR typeof(t.created_at) != 'text' \
                    OR typeof(t.started_at) NOT IN ('null', 'text') \
                    OR typeof(t.finished_at) NOT IN ('null', 'text') \
                    OR typeof(t.last_event_id) != 'integer' \
                    OR typeof(t.failure_json) NOT IN ('null', 'text') \
                    OR typeof(d.readiness) NOT IN ('null', 'text')",
            )
            .fetch_one(&mut *connection)
            .await?
        }
        Some(task_id) => {
            sqlx::query_scalar(
                "SELECT COUNT(*) \
                 FROM tasks t \
                 LEFT JOIN task_delivery_state d ON d.task_id = t.id \
                 WHERE t.id = ? \
                   AND (typeof(t.id) != 'text' \
                     OR typeof(t.client_request_id) != 'text' \
                     OR typeof(t.repository_id) != 'text' \
                     OR typeof(t.prompt) != 'text' \
                     OR typeof(t.status) != 'text' \
                     OR typeof(t.attempt) != 'integer' \
                     OR typeof(t.retry_of) NOT IN ('null', 'text') \
                     OR typeof(t.created_at) != 'text' \
                     OR typeof(t.started_at) NOT IN ('null', 'text') \
                     OR typeof(t.finished_at) NOT IN ('null', 'text') \
                     OR typeof(t.last_event_id) != 'integer' \
                     OR typeof(t.failure_json) NOT IN ('null', 'text') \
                     OR typeof(d.readiness) NOT IN ('null', 'text'))",
            )
            .bind(task_id.to_string())
            .fetch_one(&mut *connection)
            .await?
        }
    };
    if invalid == 0 {
        Ok(())
    } else {
        Err(StoreError::InvariantViolation(
            "task projection storage class is inconsistent",
        ))
    }
}

async fn ensure_event_storage_types(
    connection: &mut SqliteConnection,
    task_id: Option<TaskId>,
    after: EventCursor,
    page: Option<(EventCursor, usize)>,
) -> Result<(), StoreError> {
    let invalid: i64 = match (task_id, page) {
        (None, None) => {
            sqlx::query_scalar(
                "SELECT COUNT(*) FROM task_events \
                 WHERE id > ? AND (typeof(id) != 'integer' \
                    OR typeof(schema_version) != 'integer' \
                    OR typeof(task_id) != 'text' \
                    OR typeof(kind) != 'text' \
                    OR typeof(payload_json) != 'text' \
                    OR typeof(created_at) != 'text')",
            )
            .bind(after.get())
            .fetch_one(&mut *connection)
            .await?
        }
        (Some(task_id), None) => {
            sqlx::query_scalar(
                "SELECT COUNT(*) FROM task_events \
                 WHERE task_id = ? AND id > ? \
                   AND (typeof(id) != 'integer' \
                     OR typeof(schema_version) != 'integer' \
                     OR typeof(task_id) != 'text' \
                     OR typeof(kind) != 'text' \
                     OR typeof(payload_json) != 'text' \
                     OR typeof(created_at) != 'text')",
            )
            .bind(task_id.to_string())
            .bind(after.get())
            .fetch_one(&mut *connection)
            .await?
        }
        (None, Some((high_watermark, _))) => {
            sqlx::query_scalar(
                "SELECT COUNT(*) FROM task_events \
                 WHERE id > ? AND id <= ? \
                   AND (typeof(id) != 'integer' \
                     OR typeof(schema_version) != 'integer' \
                     OR typeof(task_id) != 'text' \
                     OR typeof(kind) != 'text' \
                     OR typeof(payload_json) != 'text' \
                     OR typeof(created_at) != 'text')",
            )
            .bind(after.get())
            .bind(high_watermark.get())
            .fetch_one(&mut *connection)
            .await?
        }
        (Some(task_id), Some((high_watermark, _))) => {
            sqlx::query_scalar(
                "SELECT COUNT(*) FROM task_events \
                 WHERE task_id = ? AND id > ? AND id <= ? \
                   AND (typeof(id) != 'integer' \
                     OR typeof(schema_version) != 'integer' \
                     OR typeof(task_id) != 'text' \
                     OR typeof(kind) != 'text' \
                     OR typeof(payload_json) != 'text' \
                     OR typeof(created_at) != 'text')",
            )
            .bind(task_id.to_string())
            .bind(after.get())
            .bind(high_watermark.get())
            .fetch_one(&mut *connection)
            .await?
        }
    };
    if invalid == 0 {
        Ok(())
    } else {
        Err(StoreError::InvariantViolation(
            "task event projection storage class is inconsistent",
        ))
    }
}

async fn event_from_record(
    connection: &mut SqliteConnection,
    record: EventRecord,
) -> Result<TaskEvent, StoreError> {
    let schema_version =
        u16::try_from(record.1).map_err(|_| StoreError::InvalidEventSchemaVersion(record.1))?;
    if schema_version != 1 {
        return Err(StoreError::InvalidEventSchemaVersion(record.1));
    }
    let event_id = EventId::new(record.0)?;
    let task_id = record.2.parse().map_err(StoreError::InvalidTaskId)?;
    let kind = parse_event_kind(&record.3)?;
    let created_at = UtcTimestamp::parse_rfc3339(&record.5)?;
    let payload = if kind == TaskEventKind::ReviewUpdated {
        if record.4 != r#"{"evidence_ref":true}"# {
            return Err(StoreError::InvariantViolation(
                "review event does not contain the exact evidence marker",
            ));
        }
        let review = load_review_by_event(connection, task_id, event_id).await?;
        if review.created_at() != created_at {
            return Err(StoreError::InvariantViolation(
                "review event and evidence timestamps do not match",
            ));
        }
        TaskEventPayload::ReviewUpdated { review }
    } else {
        let payload_value: Value = serde_json::from_str(&record.4)?;
        payload_from_value(kind, &payload_value, task_id, event_id)?
    };
    let event = TaskEvent {
        id: event_id,
        schema_version,
        task_id,
        payload,
        created_at,
    };
    if kind == TaskEventKind::ReviewUpdated && serde_json::to_vec(&event)?.len() > 192 * 1024 {
        return Err(StoreError::InvariantViolation(
            "review event exceeds the wire encoding limit",
        ));
    }
    Ok(event)
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
        TaskEventKind::ReviewUpdated => Err(StoreError::InvariantViolation(
            "review event must be loaded from typed evidence",
        )),
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
    reviews: Vec<ReviewEvidence>,
    event_cursor: EventCursor,
) -> Result<TaskDetail, StoreError> {
    let event_reviews = events
        .iter()
        .filter_map(|event| match &event.payload {
            TaskEventPayload::ReviewUpdated { review } => Some(review.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    if event_reviews != reviews {
        return Err(StoreError::InvariantViolation(
            "review event projection does not match typed evidence",
        ));
    }
    let mut detail = TaskDetail {
        task,
        plan: None,
        activity: Vec::new(),
        diff: None,
        tests: None,
        reviews,
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
                    .all(|existing| existing.id() != entry.id())
                {
                    detail.activity.push(entry.clone());
                }
            }
            TaskEventPayload::DiffUpdated { diff } => detail.diff = Some(diff.clone()),
            TaskEventPayload::TestUpdated { tests } => detail.tests = Some(tests.clone()),
            TaskEventPayload::ReviewUpdated { .. } => {}
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
    Ok(detail)
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
        | TaskEventKind::ReviewUpdated
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
        | TaskEventKind::TestUpdated
        | TaskEventKind::ReviewUpdated => "",
    }
}

fn repository_from_record(record: RepositoryRecord) -> Result<Repository, StoreError> {
    let raw = record.clone();
    let repository = Repository {
        id: record
            .0
            .parse()
            .map_err(|_| bootstrap_projection_invariant())?,
        selected_path: CanonicalPath::try_from_canonical(PathBuf::from(record.1))
            .map_err(|_| bootstrap_projection_invariant())?,
        display_name: record.2,
        git_root: CanonicalPath::try_from_canonical(PathBuf::from(record.3))
            .map_err(|_| bootstrap_projection_invariant())?,
        cargo_workspace_root: CanonicalPath::try_from_canonical(PathBuf::from(record.4))
            .map_err(|_| bootstrap_projection_invariant())?,
        created_at: UtcTimestamp::parse_rfc3339(&record.7)
            .map_err(|_| bootstrap_projection_invariant())?,
        last_opened_at: UtcTimestamp::parse_rfc3339(&record.8)
            .map_err(|_| bootstrap_projection_invariant())?,
    };
    if raw.0 != repository.id.to_string()
        || raw.1 != repository.selected_path.to_string()
        || raw.3 != repository.git_root.to_string()
        || raw.4 != repository.cargo_workspace_root.to_string()
        || raw.5 != repository_identity_key(&repository.git_root)
        || raw.6 != repository_identity_key(&repository.cargo_workspace_root)
        || raw.7 != repository.created_at.to_string()
        || raw.8 != repository.last_opened_at.to_string()
    {
        return Err(bootstrap_projection_invariant());
    }
    Ok(repository)
}

fn task_from_canonical_projection_record(record: TaskRecord) -> Result<Task, StoreError> {
    let raw = record.clone();
    let task = match task_from_record(record) {
        Ok(task) => task,
        Err(error @ StoreError::Domain(DomainError::InvalidTaskState)) => return Err(error),
        Err(_) => return Err(bootstrap_projection_invariant()),
    };
    let expected_failure = task
        .failure
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|_| bootstrap_projection_invariant())?;
    let expected_readiness = match task.delivery_readiness {
        DeliveryReadiness::Unreviewed => None,
        DeliveryReadiness::ReviewApproved => Some("review_approved"),
        DeliveryReadiness::ReviewRejected => Some("review_rejected"),
    };
    if raw.0 != task.id.to_string()
        || raw.1 != task.client_request_id.to_string()
        || raw.2 != task.repository_id.to_string()
        || raw.4 != crate::tasks::status_text(task.status)
        || raw.5 != i64::from(task.attempt)
        || raw.6.as_deref() != task.retry_of.map(|value| value.to_string()).as_deref()
        || raw.7 != task.created_at.to_string()
        || raw.8.as_deref() != task.started_at.map(|value| value.to_string()).as_deref()
        || raw.9.as_deref() != task.finished_at.map(|value| value.to_string()).as_deref()
        || raw.10 != task.last_event_id.get()
        || raw.11 != expected_failure
        || raw.12.as_deref() != expected_readiness
    {
        return Err(bootstrap_projection_invariant());
    }
    Ok(task)
}

fn repository_identity_key(path: &CanonicalPath) -> String {
    #[cfg(windows)]
    {
        path.to_string().replace('/', "\\").to_lowercase()
    }

    #[cfg(not(windows))]
    {
        path.to_string()
    }
}

fn bootstrap_projection_invariant() -> StoreError {
    StoreError::InvariantViolation(BOOTSTRAP_PROJECTION_INVARIANT)
}

fn limit_as_i64(limit: usize) -> i64 {
    i64::try_from(limit).unwrap_or(i64::MAX)
}
