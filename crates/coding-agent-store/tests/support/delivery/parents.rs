use sqlx::{Acquire as _, SqlitePool};

use super::*;

#[derive(Debug, Clone, Copy)]
pub struct DeliveryParents {
    pub final_review_event_id: i64,
}

pub async fn seed_eligible_delivery_parents(pool: &SqlitePool) -> DeliveryParents {
    let mut transaction = pool.begin().await.unwrap();
    let connection = transaction.acquire().await.unwrap();

    sqlx::query(
        "INSERT INTO repositories (
             id, selected_path, display_name, git_root, cargo_workspace_root,
             git_identity_key, cargo_identity_key, created_at, last_opened_at
         ) VALUES (?, 'C:/fixtures/selected', 'delivery fixture',
                   'C:/fixtures/repository', 'C:/fixtures/repository',
                   'delivery-git', 'delivery-cargo', ?, ?)",
    )
    .bind(REPOSITORY_ID)
    .bind(TIMESTAMP)
    .bind(TIMESTAMP)
    .execute(&mut *connection)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO tasks (
             id, client_request_id, repository_id, prompt, status, attempt,
             retry_of, created_at, started_at, finished_at, last_event_id,
             failure_json
         ) VALUES (?, ?, ?, 'delivery fixture', 'running', 1,
                   NULL, ?, ?, NULL, 0, NULL)",
    )
    .bind(TASK_ID)
    .bind(TASK_CLIENT_REQUEST_ID)
    .bind(REPOSITORY_ID)
    .bind(TIMESTAMP)
    .bind(TIMESTAMP)
    .execute(&mut *connection)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO task_attempt_artifacts (
             task_id, repository_id, attempt, base_commit, branch_name,
             worktree_path, state, failure_code, created_at, updated_at
         ) VALUES (?, ?, 1, ?, 'codex/task', ?, 'ready', NULL, ?, ?)",
    )
    .bind(TASK_ID)
    .bind(REPOSITORY_ID)
    .bind(BASE_OID)
    .bind(WORKTREE_PATH)
    .bind(TIMESTAMP)
    .bind(TIMESTAMP)
    .execute(&mut *connection)
    .await
    .unwrap();

    let review_event = sqlx::query(
        "INSERT INTO task_events (
             schema_version, task_id, kind, payload_json, created_at
         ) VALUES (1, ?, 'review.updated', '{\"evidence_ref\":true}', ?)",
    )
    .bind(TASK_ID)
    .bind(TIMESTAMP)
    .execute(&mut *connection)
    .await
    .unwrap();
    let final_review_event_id = review_event.last_insert_rowid();

    sqlx::query(
        "INSERT INTO task_review_evidence (
             task_id, repository_id, attempt, review_round,
             workspace_generation, digest_algorithm, workspace_digest,
             decision_source, verdict, summary, findings_json,
             added_checks_json, required_checks_json, check_evidence_json,
             coverage_json, created_at, event_id, event_kind
         ) VALUES (
             ?, ?, 1, 1, 7, 'workspace_fingerprint_v1', ?,
             'reviewer', 'approved', 'approved fixture', '[]',
             '[]', '[\"cargo test\"]', '[]', 'null', ?, ?, 'review.updated'
         )",
    )
    .bind(TASK_ID)
    .bind(REPOSITORY_ID)
    .bind(WORKSPACE_FINGERPRINT)
    .bind(TIMESTAMP)
    .bind(final_review_event_id)
    .execute(&mut *connection)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO task_delivery_state (
             task_id, readiness, final_review_round, final_verdict, decided_at
         ) VALUES (?, 'review_approved', 1, 'approved', ?)",
    )
    .bind(TASK_ID)
    .bind(TIMESTAMP)
    .execute(&mut *connection)
    .await
    .unwrap();

    let completed_event = sqlx::query(
        "INSERT INTO task_events (
             schema_version, task_id, kind, payload_json, created_at
         ) VALUES (1, ?, 'task.completed', '{}', ?)",
    )
    .bind(TASK_ID)
    .bind(TIMESTAMP)
    .execute(&mut *connection)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE tasks
         SET status = 'completed', finished_at = ?, last_event_id = ?
         WHERE id = ? AND status = 'running'",
    )
    .bind(TIMESTAMP)
    .bind(completed_event.last_insert_rowid())
    .bind(TASK_ID)
    .execute(&mut *connection)
    .await
    .unwrap();

    transaction.commit().await.unwrap();
    DeliveryParents {
        final_review_event_id,
    }
}
