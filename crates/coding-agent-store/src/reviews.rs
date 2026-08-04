use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::sqlite::SqliteRow;
use sqlx::{FromRow, Row, SqliteConnection, Transaction};

use coding_agent_domain::{
    CheckEvidence, DeliveryReadiness, EventId, NewReviewEvidence, PlanSnapshot, RepositoryId,
    RequiredCheck, ReviewCoverageEvidence, ReviewDecisionSource, ReviewEvidence, ReviewFinding,
    ReviewVerdict, Task, TaskEventKind, TaskFailure, TaskId, TaskStatus, UtcTimestamp,
    WorkspaceDigest,
};

use crate::stop_intents::{ensure_no_stop_intent, validate_optional_stop_intent};
use crate::tasks::{
    append_lifecycle_event, current_timestamp, ensure_exactly_one, insert_event, load_task,
};
use crate::{Store, StoreError};

const REVIEW_MARKER: &str = r#"{"evidence_ref":true}"#;
const REVIEW_EVENT_KIND: &str = "review.updated";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordReviewOutcome {
    Applied {
        review: ReviewEvidence,
        event_id: EventId,
    },
    Existing {
        review: ReviewEvidence,
        event_id: EventId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FinalizeReviewedTaskOutcome {
    Applied {
        task: Task,
        review: ReviewEvidence,
        review_event_id: EventId,
        terminal_event_id: EventId,
    },
    Existing {
        task: Task,
        review: ReviewEvidence,
        review_event_id: EventId,
        terminal_event_id: EventId,
    },
}

#[derive(Debug)]
struct ReviewRecord {
    task_id: String,
    repository_id: String,
    attempt: i64,
    review_round: i64,
    workspace_generation: i64,
    digest_algorithm: String,
    workspace_digest: String,
    decision_source: String,
    verdict: String,
    summary: String,
    findings_json: String,
    added_checks_json: String,
    required_checks_json: String,
    check_evidence_json: String,
    coverage_json: String,
    created_at: String,
    event_id: i64,
    event_kind: String,
}

impl<'r> FromRow<'r, SqliteRow> for ReviewRecord {
    fn from_row(row: &'r SqliteRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            task_id: row.try_get("task_id")?,
            repository_id: row.try_get("repository_id")?,
            attempt: row.try_get("attempt")?,
            review_round: row.try_get("review_round")?,
            workspace_generation: row.try_get("workspace_generation")?,
            digest_algorithm: row.try_get("digest_algorithm")?,
            workspace_digest: row.try_get("workspace_digest")?,
            decision_source: row.try_get("decision_source")?,
            verdict: row.try_get("verdict")?,
            summary: row.try_get("summary")?,
            findings_json: row.try_get("findings_json")?,
            added_checks_json: row.try_get("added_checks_json")?,
            required_checks_json: row.try_get("required_checks_json")?,
            check_evidence_json: row.try_get("check_evidence_json")?,
            coverage_json: row.try_get("coverage_json")?,
            created_at: row.try_get("created_at")?,
            event_id: row.try_get("event_id")?,
            event_kind: row.try_get("event_kind")?,
        })
    }
}

#[derive(Debug)]
pub(crate) struct StoredReview {
    pub(crate) task_id: TaskId,
    pub(crate) repository_id: RepositoryId,
    pub(crate) attempt: u32,
    pub(crate) event_id: EventId,
    pub(crate) review: ReviewEvidence,
}

impl Store {
    pub async fn record_review(
        &self,
        task_id: TaskId,
        expected_repository_id: RepositoryId,
        expected_attempt: u32,
        evidence: NewReviewEvidence,
    ) -> Result<RecordReviewOutcome, StoreError> {
        validate_nonterminal_request(&evidence)?;
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        ensure_review_graph(&mut transaction, task_id).await?;

        if let Some(existing) =
            load_review_by_round(&mut transaction, task_id, evidence.round()).await?
        {
            validate_existing_history(&mut transaction, &existing).await?;
            validate_existing_identity(
                &existing,
                task_id,
                expected_repository_id,
                expected_attempt,
            )?;
            if !same_request(&existing.review, &evidence)? {
                return Err(review_invariant());
            }
            let task = load_task(&mut transaction, task_id)
                .await?
                .ok_or(StoreError::TaskNotFound)?;
            if task.attempt != expected_attempt
                || task.repository_id != expected_repository_id
                || task.repository_id != existing.repository_id
            {
                return Err(review_invariant());
            }
            validate_task_event_cursor(&mut transaction, &task).await?;
            validate_optional_stop_intent(&mut transaction, &task).await?;
            transaction.commit().await?;
            return Ok(RecordReviewOutcome::Existing {
                review: existing.review,
                event_id: existing.event_id,
            });
        }

        let task = load_task(&mut transaction, task_id)
            .await?
            .ok_or(StoreError::TaskNotFound)?;
        validate_running_task(&task, expected_repository_id, expected_attempt)?;
        ensure_no_stop_intent(&mut transaction, task_id).await?;
        validate_next_round(&mut transaction, task_id, &evidence).await?;
        validate_task_event_cursor(&mut transaction, &task).await?;
        ensure_no_delivery(&mut transaction, task_id).await?;

        let created_at = current_timestamp()?;
        let review =
            ReviewEvidence::try_from_new(evidence, created_at).map_err(|_| review_invariant())?;
        let event_id = insert_event(
            &mut transaction,
            task_id,
            TaskEventKind::ReviewUpdated,
            REVIEW_MARKER,
            created_at,
        )
        .await?;
        insert_review_row(&mut transaction, &task, &review, event_id).await?;
        let updated = sqlx::query(
            "UPDATE tasks SET last_event_id = ? \
             WHERE id = ? AND status = 'running' AND attempt = ?",
        )
        .bind(event_id.get())
        .bind(task_id.to_string())
        .bind(i64::from(expected_attempt))
        .execute(&mut *transaction)
        .await?;
        ensure_exactly_one(
            updated.rows_affected(),
            "review event did not update exactly one running task",
        )?;
        transaction.commit().await?;
        Ok(RecordReviewOutcome::Applied { review, event_id })
    }

    pub async fn finalize_reviewed_task(
        &self,
        task_id: TaskId,
        expected_repository_id: RepositoryId,
        expected_attempt: u32,
        evidence: NewReviewEvidence,
    ) -> Result<FinalizeReviewedTaskOutcome, StoreError> {
        validate_final_request(&evidence)?;
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        ensure_review_graph(&mut transaction, task_id).await?;

        if let Some(existing) =
            load_review_by_round(&mut transaction, task_id, evidence.round()).await?
        {
            validate_existing_history(&mut transaction, &existing).await?;
            validate_existing_identity(
                &existing,
                task_id,
                expected_repository_id,
                expected_attempt,
            )?;
            if !same_request(&existing.review, &evidence)? {
                return Err(review_invariant());
            }
            let task = load_task(&mut transaction, task_id)
                .await?
                .ok_or(StoreError::TaskNotFound)?;
            ensure_no_stop_intent(&mut transaction, task_id).await?;
            let terminal_event_id =
                validate_existing_final(&mut transaction, &task, &existing).await?;
            transaction.commit().await?;
            return Ok(FinalizeReviewedTaskOutcome::Existing {
                task,
                review: existing.review,
                review_event_id: existing.event_id,
                terminal_event_id,
            });
        }

        let task = load_task(&mut transaction, task_id)
            .await?
            .ok_or(StoreError::TaskNotFound)?;
        validate_running_task(&task, expected_repository_id, expected_attempt)?;
        ensure_no_stop_intent(&mut transaction, task_id).await?;
        validate_next_round(&mut transaction, task_id, &evidence).await?;
        validate_task_event_cursor(&mut transaction, &task).await?;
        ensure_no_delivery(&mut transaction, task_id).await?;

        let created_at = current_timestamp()?;
        let review =
            ReviewEvidence::try_from_new(evidence, created_at).map_err(|_| review_invariant())?;
        let review_event_id = insert_event(
            &mut transaction,
            task_id,
            TaskEventKind::ReviewUpdated,
            REVIEW_MARKER,
            created_at,
        )
        .await?;
        insert_review_row(&mut transaction, &task, &review, review_event_id).await?;

        let (status, readiness, failure) = final_state(review.verdict());
        let failure_json = failure.as_ref().map(serde_json::to_string).transpose()?;
        sqlx::query(
            "INSERT INTO task_delivery_state (
                 task_id, readiness, final_review_round, final_verdict, decided_at
             ) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(task_id.to_string())
        .bind(readiness_text(readiness))
        .bind(i64::from(review.round()))
        .bind(verdict_text(review.verdict()))
        .bind(created_at.to_string())
        .execute(&mut *transaction)
        .await?;

        let updated = sqlx::query(
            "UPDATE tasks SET status = ?, finished_at = ?, failure_json = ? \
             WHERE id = ? AND status = 'running' AND attempt = ?",
        )
        .bind(status_text(status))
        .bind(created_at.to_string())
        .bind(failure_json)
        .bind(task_id.to_string())
        .bind(i64::from(expected_attempt))
        .execute(&mut *transaction)
        .await?;
        ensure_exactly_one(
            updated.rows_affected(),
            "review finalization did not update exactly one running task",
        )?;

        let (final_task, terminal_event_id) =
            append_lifecycle_event(&mut transaction, task_id, terminal_kind(status), created_at)
                .await?;
        transaction.commit().await?;
        Ok(FinalizeReviewedTaskOutcome::Applied {
            task: final_task,
            review,
            review_event_id,
            terminal_event_id,
        })
    }
}

pub(crate) async fn load_review_by_event(
    connection: &mut SqliteConnection,
    task_id: TaskId,
    event_id: EventId,
) -> Result<ReviewEvidence, StoreError> {
    let record = sqlx::query_as::<_, ReviewRecord>(
        "SELECT r.* FROM task_events e \
         JOIN task_review_evidence r \
           ON r.event_id = e.id AND r.task_id = e.task_id AND r.event_kind = e.kind \
         JOIN tasks t \
           ON t.id = r.task_id AND t.repository_id = r.repository_id AND t.attempt = r.attempt \
         WHERE e.id = ? AND e.task_id = ? AND e.kind = 'review.updated' \
           AND e.schema_version = 1 AND e.payload_json = ?",
    )
    .bind(event_id.get())
    .bind(task_id.to_string())
    .bind(REVIEW_MARKER)
    .fetch_optional(connection)
    .await?
    .ok_or(StoreError::InvariantViolation(
        "review event is not linked to exactly one typed evidence row",
    ))?;
    let stored = review_from_record(record)?;
    if stored.task_id != task_id || stored.event_id != event_id {
        return Err(review_invariant());
    }
    Ok(stored.review)
}

pub(crate) async fn load_stored_reviews_for_task(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<Vec<StoredReview>, StoreError> {
    ensure_review_graph(connection, task_id).await?;
    let records = sqlx::query_as::<_, ReviewRecord>(
        "SELECT r.* FROM task_review_evidence r \
         JOIN task_events e \
           ON e.id = r.event_id AND e.task_id = r.task_id AND e.kind = r.event_kind \
         JOIN tasks t \
           ON t.id = r.task_id AND t.repository_id = r.repository_id AND t.attempt = r.attempt \
         WHERE r.task_id = ? AND e.schema_version = 1 AND e.payload_json = ? \
         ORDER BY r.review_round",
    )
    .bind(task_id.to_string())
    .bind(REVIEW_MARKER)
    .fetch_all(&mut *connection)
    .await?;
    let reviews = records
        .into_iter()
        .map(review_from_record)
        .collect::<Result<Vec<_>, _>>()?;
    if reviews.iter().enumerate().any(|(index, stored)| {
        stored.task_id != task_id
            || usize::from(stored.review.round()) != index + 1
            || (index > 0 && reviews[index - 1].event_id >= stored.event_id)
    }) {
        return Err(review_invariant());
    }
    if reviews
        .iter()
        .take(reviews.len().saturating_sub(1))
        .any(|stored| {
            stored.review.verdict() != ReviewVerdict::ChangesRequested || stored.review.round() > 2
        })
    {
        return Err(review_invariant());
    }
    if !reviews.is_empty() {
        let initial_checks = load_initial_required_checks(connection, task_id).await?;
        validate_review_check_chain(&initial_checks, &reviews)?;
    }
    Ok(reviews)
}

pub(crate) async fn load_reviews_for_task(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<Vec<ReviewEvidence>, StoreError> {
    Ok(load_stored_reviews_for_task(connection, task_id)
        .await?
        .into_iter()
        .map(|stored| stored.review)
        .collect())
}

pub(crate) async fn validate_task_review_aggregate(
    connection: &mut SqliteConnection,
    task: &Task,
    reviews: &[ReviewEvidence],
) -> Result<(), StoreError> {
    if reviews.is_empty() {
        return if task.delivery_readiness == DeliveryReadiness::Unreviewed {
            Ok(())
        } else {
            Err(review_invariant())
        };
    }

    validate_task_event_cursor(connection, task).await?;
    let final_review = reviews.last().ok_or_else(review_invariant)?;
    let requires_terminal_tuple = final_review.verdict() == ReviewVerdict::Approved
        || (final_review.verdict() == ReviewVerdict::ChangesRequested && final_review.round() == 3);
    if !requires_terminal_tuple {
        let valid_nonterminal_status = matches!(
            task.status,
            TaskStatus::Running
                | TaskStatus::Failed
                | TaskStatus::Cancelled
                | TaskStatus::Interrupted
        );
        let reserved_failure_absent = task
            .failure
            .as_ref()
            .is_none_or(|failure| failure.code != "REVIEW_REJECTED");
        return if task.delivery_readiness == DeliveryReadiness::Unreviewed
            && valid_nonterminal_status
            && reserved_failure_absent
        {
            Ok(())
        } else {
            Err(review_invariant())
        };
    }

    let stored = load_review_by_round(connection, task.id, final_review.round())
        .await?
        .ok_or_else(review_invariant)?;
    if stored.review != *final_review {
        return Err(review_invariant());
    }
    validate_existing_final(connection, task, &stored)
        .await
        .map(|_| ())
}

async fn load_review_by_round(
    connection: &mut SqliteConnection,
    task_id: TaskId,
    round: u8,
) -> Result<Option<StoredReview>, StoreError> {
    sqlx::query_as::<_, ReviewRecord>(
        "SELECT r.* FROM task_review_evidence r \
         JOIN tasks t \
           ON t.id = r.task_id AND t.repository_id = r.repository_id AND t.attempt = r.attempt \
         WHERE r.task_id = ? AND r.review_round = ?",
    )
    .bind(task_id.to_string())
    .bind(i64::from(round))
    .fetch_optional(connection)
    .await?
    .map(review_from_record)
    .transpose()
}

async fn validate_existing_history(
    connection: &mut SqliteConnection,
    existing: &StoredReview,
) -> Result<(), StoreError> {
    let reviews = load_reviews_for_task(connection, existing.task_id).await?;
    if reviews.get(usize::from(existing.review.round()) - 1) == Some(&existing.review) {
        Ok(())
    } else {
        Err(review_invariant())
    }
}

async fn insert_review_row(
    transaction: &mut Transaction<'_, sqlx::Sqlite>,
    task: &Task,
    review: &ReviewEvidence,
    event_id: EventId,
) -> Result<(), StoreError> {
    let findings_json = canonical_json(review.findings())?;
    let added_checks_json = canonical_json(review.added_required_checks())?;
    let required_checks_json = canonical_json(review.required_checks())?;
    let check_evidence_json = canonical_json(review.check_evidence())?;
    let coverage_json = canonical_json(&review.coverage())?;
    let inserted = sqlx::query(
        "INSERT INTO task_review_evidence (
             task_id, repository_id, attempt, review_round,
             workspace_generation, digest_algorithm, workspace_digest,
             decision_source, verdict, summary, findings_json,
             added_checks_json, required_checks_json, check_evidence_json,
             coverage_json, created_at, event_id, event_kind
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(task.id.to_string())
    .bind(task.repository_id.to_string())
    .bind(i64::from(task.attempt))
    .bind(i64::from(review.round()))
    .bind(i64::try_from(review.workspace_generation()).map_err(|_| review_invariant())?)
    .bind(review.workspace_digest().algorithm())
    .bind(review.workspace_digest().value())
    .bind(decision_source_text(review.decision_source()))
    .bind(verdict_text(review.verdict()))
    .bind(review.summary())
    .bind(findings_json)
    .bind(added_checks_json)
    .bind(required_checks_json)
    .bind(check_evidence_json)
    .bind(coverage_json)
    .bind(review.created_at().to_string())
    .bind(event_id.get())
    .bind(REVIEW_EVENT_KIND)
    .execute(&mut **transaction)
    .await?;
    ensure_exactly_one(
        inserted.rows_affected(),
        "review evidence insert did not affect exactly one row",
    )
}

fn review_from_record(record: ReviewRecord) -> Result<StoredReview, StoreError> {
    if record.event_kind != REVIEW_EVENT_KIND
        || record.digest_algorithm != "workspace_fingerprint_v1"
    {
        return Err(StoreError::InvariantViolation(
            "stored review event kind or digest algorithm is invalid",
        ));
    }
    let task_id: TaskId = record.task_id.parse().map_err(|_| review_invariant())?;
    let repository_id: RepositoryId = record
        .repository_id
        .parse()
        .map_err(|_| review_invariant())?;
    let attempt = u32::try_from(record.attempt).map_err(|_| review_invariant())?;
    if task_id.to_string() != record.task_id
        || repository_id.to_string() != record.repository_id
        || attempt == 0
    {
        return Err(StoreError::InvariantViolation(
            "stored review identity is not canonical",
        ));
    }
    let round = u8::try_from(record.review_round).map_err(|_| review_invariant())?;
    let generation = u64::try_from(record.workspace_generation).map_err(|_| review_invariant())?;
    let digest =
        WorkspaceDigest::try_new(record.workspace_digest).map_err(|_| review_invariant())?;
    let decision_source = parse_decision_source(&record.decision_source)?;
    let verdict = parse_verdict(&record.verdict)?;
    let findings: Vec<ReviewFinding> = parse_canonical_json(&record.findings_json)
        .map_err(|_| StoreError::InvariantViolation("stored review findings are not canonical"))?;
    let added_checks: Vec<RequiredCheck> = parse_canonical_json(&record.added_checks_json)
        .map_err(|_| StoreError::InvariantViolation("stored added checks are not canonical"))?;
    let required_checks: Vec<RequiredCheck> = parse_canonical_json(&record.required_checks_json)
        .map_err(|_| StoreError::InvariantViolation("stored required checks are not canonical"))?;
    let check_evidence: Vec<CheckEvidence> = parse_canonical_json(&record.check_evidence_json)
        .map_err(|_| StoreError::InvariantViolation("stored check evidence is not canonical"))?;
    let coverage: Option<ReviewCoverageEvidence> = parse_canonical_json(&record.coverage_json)
        .map_err(|_| StoreError::InvariantViolation("stored review coverage is not canonical"))?;
    let created_at =
        UtcTimestamp::parse_rfc3339(&record.created_at).map_err(|_| review_invariant())?;
    if created_at.to_string() != record.created_at {
        return Err(StoreError::InvariantViolation(
            "stored review timestamp is not canonical",
        ));
    }
    let new = NewReviewEvidence::try_new(
        round,
        decision_source,
        generation,
        digest,
        verdict,
        record.summary,
        findings,
        added_checks,
        required_checks,
        check_evidence,
        coverage,
    )
    .map_err(|_| StoreError::InvariantViolation("stored review request is invalid"))?;
    let review = ReviewEvidence::try_from_new(new, created_at)
        .map_err(|_| StoreError::InvariantViolation("stored review evidence is invalid"))?;
    Ok(StoredReview {
        task_id,
        repository_id,
        attempt,
        event_id: EventId::new(record.event_id).map_err(|_| review_invariant())?,
        review,
    })
}

async fn ensure_review_graph(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<(), StoreError> {
    let orphan_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_events e \
         LEFT JOIN task_review_evidence r \
           ON r.event_id = e.id AND r.task_id = e.task_id AND r.event_kind = e.kind \
         WHERE e.task_id = ? AND e.kind = 'review.updated' \
           AND (
               e.schema_version != 1 OR e.payload_json != ? OR r.event_id IS NULL
               OR e.created_at != r.created_at
           )",
    )
    .bind(task_id.to_string())
    .bind(REVIEW_MARKER)
    .fetch_one(&mut *connection)
    .await?;
    let orphan_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_review_evidence r \
         LEFT JOIN task_events e \
           ON e.id = r.event_id AND e.task_id = r.task_id AND e.kind = r.event_kind \
         WHERE r.task_id = ? \
           AND (
               e.id IS NULL OR e.schema_version != 1 OR e.payload_json != ?
               OR e.created_at != r.created_at
           )",
    )
    .bind(task_id.to_string())
    .bind(REVIEW_MARKER)
    .fetch_one(connection)
    .await?;
    if orphan_events == 0 && orphan_rows == 0 {
        Ok(())
    } else {
        Err(review_invariant())
    }
}

async fn validate_next_round(
    connection: &mut SqliteConnection,
    task_id: TaskId,
    evidence: &NewReviewEvidence,
) -> Result<(), StoreError> {
    let reviews = load_reviews_for_task(connection, task_id).await?;
    if reviews.len() + 1 != usize::from(evidence.round()) {
        return Err(review_invariant());
    }
    let initial_checks;
    let previous = if let Some(previous) = reviews.last() {
        previous.required_checks()
    } else {
        initial_checks = load_initial_required_checks(connection, task_id).await?;
        &initial_checks
    };
    validate_new_check_delta(previous, evidence)
}

async fn ensure_no_delivery(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<(), StoreError> {
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM task_delivery_state WHERE task_id = ?")
            .bind(task_id.to_string())
            .fetch_one(connection)
            .await?;
    if count == 0 {
        Ok(())
    } else {
        Err(review_invariant())
    }
}

pub(crate) async fn validate_task_event_cursor(
    connection: &mut SqliteConnection,
    task: &Task,
) -> Result<(), StoreError> {
    let latest_task_event: Option<i64> =
        sqlx::query_scalar("SELECT MAX(id) FROM task_events WHERE task_id = ?")
            .bind(task.id.to_string())
            .fetch_one(connection)
            .await?;
    if latest_task_event == Some(task.last_event_id.get()) {
        Ok(())
    } else {
        Err(review_invariant())
    }
}

async fn load_initial_required_checks(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<Vec<RequiredCheck>, StoreError> {
    let event: Option<(i64, String)> = sqlx::query_as(
        "SELECT schema_version, payload_json FROM task_events \
         WHERE task_id = ? AND kind = 'plan.updated' \
         ORDER BY id DESC LIMIT 1",
    )
    .bind(task_id.to_string())
    .fetch_optional(connection)
    .await?;
    let (schema_version, payload) = event.ok_or_else(review_invariant)?;
    if schema_version != 1 {
        return Err(review_invariant());
    }
    let value: serde_json::Value =
        serde_json::from_str(&payload).map_err(|_| review_invariant())?;
    let plan: PlanSnapshot =
        serde_json::from_value(value.get("plan").cloned().ok_or_else(review_invariant)?)
            .map_err(|_| review_invariant())?;
    if plan.format_version() != 1 {
        return Err(review_invariant());
    }
    Ok(plan.initial_required_checks().to_vec())
}

fn validate_review_check_chain(
    initial_checks: &[RequiredCheck],
    reviews: &[StoredReview],
) -> Result<(), StoreError> {
    let mut previous = initial_checks;
    for stored in reviews {
        let required = stored.review.required_checks();
        let added = stored.review.added_required_checks();
        if required.len() != previous.len() + added.len()
            || !required.starts_with(previous)
            || required.get(previous.len()..) != Some(added)
        {
            return Err(review_invariant());
        }
        previous = required;
    }
    Ok(())
}

fn validate_new_check_delta(
    previous: &[RequiredCheck],
    evidence: &NewReviewEvidence,
) -> Result<(), StoreError> {
    let required = evidence.required_checks();
    let added = evidence.added_required_checks();
    if required.len() == previous.len() + added.len()
        && required.starts_with(previous)
        && required.get(previous.len()..) == Some(added)
    {
        Ok(())
    } else {
        Err(review_invariant())
    }
}

async fn validate_existing_final(
    connection: &mut SqliteConnection,
    task: &Task,
    stored: &StoredReview,
) -> Result<EventId, StoreError> {
    validate_task_event_cursor(connection, task).await?;
    if task.id != stored.task_id
        || task.repository_id != stored.repository_id
        || task.attempt != stored.attempt
        || task.last_event_id.get() != stored.event_id.get() + 1
    {
        return Err(StoreError::InvariantViolation(
            "reviewed terminal task identity or event adjacency is inconsistent",
        ));
    }
    let expected = final_state(stored.review.verdict());
    if task.status != expected.0
        || task.delivery_readiness != expected.1
        || task.failure != expected.2
    {
        return Err(StoreError::InvariantViolation(
            "reviewed terminal task state is inconsistent with the final verdict",
        ));
    }
    let delivery: Option<(String, i64, String, String)> = sqlx::query_as(
        "SELECT readiness, final_review_round, final_verdict, decided_at \
         FROM task_delivery_state WHERE task_id = ?",
    )
    .bind(task.id.to_string())
    .fetch_optional(&mut *connection)
    .await?;
    let Some((readiness, round, verdict, decided_at)) = delivery else {
        return Err(StoreError::InvariantViolation(
            "reviewed terminal task is missing delivery state",
        ));
    };
    if readiness != readiness_text(expected.1)
        || round != i64::from(stored.review.round())
        || verdict != verdict_text(stored.review.verdict())
        || decided_at != stored.review.created_at().to_string()
    {
        return Err(StoreError::InvariantViolation(
            "delivery state is inconsistent with the final review",
        ));
    }
    let event: Option<(i64, String, String, String)> = sqlx::query_as(
        "SELECT schema_version, kind, payload_json, created_at \
         FROM task_events WHERE id = ? AND task_id = ?",
    )
    .bind(task.last_event_id.get())
    .bind(task.id.to_string())
    .fetch_optional(connection)
    .await?;
    let Some((schema_version, kind, payload, created_at)) = event else {
        return Err(StoreError::InvariantViolation(
            "reviewed terminal task is missing its lifecycle event",
        ));
    };
    if schema_version != 1
        || kind != event_kind_text(terminal_kind(task.status))
        || created_at != stored.review.created_at().to_string()
    {
        return Err(StoreError::InvariantViolation(
            "terminal lifecycle metadata is inconsistent with the final review",
        ));
    }
    let value: serde_json::Value =
        serde_json::from_str(&payload).map_err(|_| review_invariant())?;
    let payload_task: Task =
        serde_json::from_value(value.get("task").cloned().ok_or_else(review_invariant)?)
            .map_err(|_| review_invariant())?;
    if Task::try_from_stored(payload_task).map_err(|_| review_invariant())? != *task {
        return Err(StoreError::InvariantViolation(
            "terminal lifecycle payload is inconsistent with the task",
        ));
    }
    Ok(task.last_event_id)
}

fn validate_existing_identity(
    stored: &StoredReview,
    task_id: TaskId,
    expected_repository_id: RepositoryId,
    expected_attempt: u32,
) -> Result<(), StoreError> {
    if stored.task_id == task_id
        && stored.repository_id == expected_repository_id
        && stored.attempt == expected_attempt
    {
        Ok(())
    } else {
        Err(review_invariant())
    }
}

fn validate_running_task(
    task: &Task,
    expected_repository_id: RepositoryId,
    expected_attempt: u32,
) -> Result<(), StoreError> {
    if task.status == TaskStatus::Running
        && task.delivery_readiness == DeliveryReadiness::Unreviewed
        && task.repository_id == expected_repository_id
        && task.attempt == expected_attempt
    {
        Ok(())
    } else {
        Err(review_invariant())
    }
}

fn validate_nonterminal_request(evidence: &NewReviewEvidence) -> Result<(), StoreError> {
    if evidence.verdict() == ReviewVerdict::ChangesRequested && evidence.round() <= 2 {
        Ok(())
    } else {
        Err(review_invariant())
    }
}

fn validate_final_request(evidence: &NewReviewEvidence) -> Result<(), StoreError> {
    if evidence.verdict() == ReviewVerdict::Approved
        || (evidence.verdict() == ReviewVerdict::ChangesRequested && evidence.round() == 3)
    {
        Ok(())
    } else {
        Err(review_invariant())
    }
}

fn same_request(review: &ReviewEvidence, request: &NewReviewEvidence) -> Result<bool, StoreError> {
    let mut stored = serde_json::to_value(review)?;
    let object = stored.as_object_mut().ok_or_else(review_invariant)?;
    object.remove("created_at");
    Ok(stored == serde_json::to_value(request)?)
}

fn final_state(verdict: ReviewVerdict) -> (TaskStatus, DeliveryReadiness, Option<TaskFailure>) {
    match verdict {
        ReviewVerdict::Approved => (
            TaskStatus::Completed,
            DeliveryReadiness::ReviewApproved,
            None,
        ),
        ReviewVerdict::ChangesRequested => (
            TaskStatus::Failed,
            DeliveryReadiness::ReviewRejected,
            Some(TaskFailure {
                code: "REVIEW_REJECTED".to_owned(),
                message: "review rejected after three rounds".to_owned(),
                retryable: true,
            }),
        ),
    }
}

fn canonical_json<T: Serialize + ?Sized>(value: &T) -> Result<String, StoreError> {
    serde_json::to_string(value).map_err(StoreError::from)
}

fn parse_canonical_json<T>(value: &str) -> Result<T, StoreError>
where
    T: DeserializeOwned + Serialize,
{
    let decoded: T = serde_json::from_str(value).map_err(|_| review_invariant())?;
    if serde_json::to_string(&decoded).map_err(|_| review_invariant())? == value {
        Ok(decoded)
    } else {
        Err(review_invariant())
    }
}

const fn decision_source_text(source: ReviewDecisionSource) -> &'static str {
    match source {
        ReviewDecisionSource::Reviewer => "reviewer",
        ReviewDecisionSource::System => "system",
    }
}

fn parse_decision_source(value: &str) -> Result<ReviewDecisionSource, StoreError> {
    match value {
        "reviewer" => Ok(ReviewDecisionSource::Reviewer),
        "system" => Ok(ReviewDecisionSource::System),
        _ => Err(review_invariant()),
    }
}

const fn verdict_text(verdict: ReviewVerdict) -> &'static str {
    match verdict {
        ReviewVerdict::Approved => "approved",
        ReviewVerdict::ChangesRequested => "changes_requested",
    }
}

fn parse_verdict(value: &str) -> Result<ReviewVerdict, StoreError> {
    match value {
        "approved" => Ok(ReviewVerdict::Approved),
        "changes_requested" => Ok(ReviewVerdict::ChangesRequested),
        _ => Err(review_invariant()),
    }
}

const fn readiness_text(readiness: DeliveryReadiness) -> &'static str {
    match readiness {
        DeliveryReadiness::ReviewApproved => "review_approved",
        DeliveryReadiness::ReviewRejected => "review_rejected",
        DeliveryReadiness::Unreviewed => "unreviewed",
    }
}

const fn status_text(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Queued => "queued",
        TaskStatus::Running => "running",
        TaskStatus::Completed => "completed",
        TaskStatus::Failed => "failed",
        TaskStatus::Cancelled => "cancelled",
        TaskStatus::Interrupted => "interrupted",
    }
}

const fn terminal_kind(status: TaskStatus) -> TaskEventKind {
    match status {
        TaskStatus::Completed => TaskEventKind::TaskCompleted,
        TaskStatus::Failed => TaskEventKind::TaskFailed,
        TaskStatus::Queued
        | TaskStatus::Running
        | TaskStatus::Cancelled
        | TaskStatus::Interrupted => TaskEventKind::TaskFailed,
    }
}

const fn event_kind_text(kind: TaskEventKind) -> &'static str {
    match kind {
        TaskEventKind::TaskCompleted => "task.completed",
        TaskEventKind::TaskFailed => "task.failed",
        TaskEventKind::TaskQueued
        | TaskEventKind::TaskStarted
        | TaskEventKind::PlanUpdated
        | TaskEventKind::ActivityAppended
        | TaskEventKind::DiffUpdated
        | TaskEventKind::TestUpdated
        | TaskEventKind::ReviewUpdated
        | TaskEventKind::TaskCancelled
        | TaskEventKind::TaskInterrupted => "",
    }
}

fn review_invariant() -> StoreError {
    StoreError::InvariantViolation("review evidence transaction is inconsistent")
}
