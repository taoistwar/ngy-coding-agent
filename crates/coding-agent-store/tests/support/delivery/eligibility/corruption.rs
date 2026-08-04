use coding_agent_domain::Task;
use coding_agent_store::{DeliveryOperationId, Store};

use super::DELIVERY_TIMESTAMP;

#[derive(Debug, Clone, Copy)]
pub enum MergeCopyCorruption<'a> {
    FinalReviewRound,
    FinalReviewEventId(i64),
    PriorReviewIdentity(i64),
    ArtifactBaseCommit,
    ArtifactSourceBranch,
    ArtifactWorktreePath(&'a str),
    CommonGitIdentity,
    WorktreeAdminIdentity,
    CandidateTree,
}

pub async fn corrupt_artifact_attempt(store: &Store, task: &Task) {
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("UPDATE task_attempt_artifacts SET attempt = attempt + 1 WHERE task_id = ?")
        .bind(task.id.to_string())
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
}

pub async fn delete_artifact_parent(store: &Store, task: &Task) {
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("DELETE FROM task_attempt_artifacts WHERE task_id = ?")
        .bind(task.id.to_string())
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
}

pub async fn corrupt_artifact_state(store: &Store, task: &Task, state: &str) {
    let failure = (state == "inconsistent").then_some("ARTIFACT_UNKNOWN");
    sqlx::query(
        "UPDATE task_attempt_artifacts SET state = ?, failure_code = ?, updated_at = ? \
         WHERE task_id = ?",
    )
    .bind(state)
    .bind(failure)
    .bind(DELIVERY_TIMESTAMP)
    .bind(task.id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
}

pub async fn corrupt_merge_evidence(
    store: &Store,
    operation_id: DeliveryOperationId,
    column: &str,
) {
    drop_merge_update_guards(store).await;
    let operation_id = operation_id.to_string();
    match column {
        "workspace_generation" => {
            sqlx::query(
                "UPDATE task_merge_operations SET workspace_generation = 99 \
                 WHERE operation_id = ?",
            )
            .bind(operation_id)
            .execute(store.pool())
            .await
            .unwrap();
        }
        "workspace_fingerprint" => {
            sqlx::query(
                "UPDATE task_merge_operations SET workspace_fingerprint = ? \
                 WHERE operation_id = ?",
            )
            .bind("9".repeat(64))
            .bind(operation_id)
            .execute(store.pool())
            .await
            .unwrap();
        }
        "checks_digest" => {
            sqlx::query(
                "UPDATE task_merge_operations SET checks_digest = ? WHERE operation_id = ?",
            )
            .bind("9".repeat(64))
            .bind(operation_id)
            .execute(store.pool())
            .await
            .unwrap();
        }
        "coverage_digest" => {
            sqlx::query(
                "UPDATE task_merge_operations SET coverage_digest = ? WHERE operation_id = ?",
            )
            .bind("9".repeat(64))
            .bind(operation_id)
            .execute(store.pool())
            .await
            .unwrap();
        }
        _ => panic!("unsupported corruption column"),
    }
}

pub async fn corrupt_merge_copy(
    store: &Store,
    operation_id: DeliveryOperationId,
    corruption: MergeCopyCorruption<'_>,
) {
    drop_merge_update_guards(store).await;
    let operation_id = operation_id.to_string();
    match corruption {
        MergeCopyCorruption::FinalReviewRound => {
            sqlx::query(
                "UPDATE task_merge_operations SET final_review_round = 1 WHERE operation_id = ?",
            )
            .bind(operation_id)
            .execute(store.pool())
            .await
            .unwrap();
        }
        MergeCopyCorruption::FinalReviewEventId(event_id) => {
            sqlx::query(
                "UPDATE task_merge_operations SET final_review_event_id = ? WHERE operation_id = ?",
            )
            .bind(event_id)
            .bind(operation_id)
            .execute(store.pool())
            .await
            .unwrap();
        }
        MergeCopyCorruption::PriorReviewIdentity(event_id) => {
            sqlx::query(
                "UPDATE task_merge_operations SET final_review_round = 1, \
                     final_review_event_id = ? WHERE operation_id = ?",
            )
            .bind(event_id)
            .bind(operation_id)
            .execute(store.pool())
            .await
            .unwrap();
        }
        MergeCopyCorruption::ArtifactBaseCommit => {
            sqlx::query(
                "UPDATE task_merge_operations SET artifact_base_commit = ? WHERE operation_id = ?",
            )
            .bind("9".repeat(40))
            .bind(operation_id)
            .execute(store.pool())
            .await
            .unwrap();
        }
        MergeCopyCorruption::ArtifactSourceBranch => {
            sqlx::query(
                "UPDATE task_merge_operations SET artifact_source_branch = \
                     'refs/heads/corrupt-copy' WHERE operation_id = ?",
            )
            .bind(operation_id)
            .execute(store.pool())
            .await
            .unwrap();
        }
        MergeCopyCorruption::ArtifactWorktreePath(path) => {
            sqlx::query(
                "UPDATE task_merge_operations SET artifact_worktree_path = ? WHERE operation_id = ?",
            )
            .bind(path)
            .bind(operation_id)
            .execute(store.pool())
            .await
            .unwrap();
        }
        MergeCopyCorruption::CommonGitIdentity => {
            sqlx::query(
                "UPDATE task_merge_operations SET common_git_identity_digest = ? \
                 WHERE operation_id = ?",
            )
            .bind("9".repeat(64))
            .bind(operation_id)
            .execute(store.pool())
            .await
            .unwrap();
        }
        MergeCopyCorruption::WorktreeAdminIdentity => {
            sqlx::query(
                "UPDATE task_merge_operations SET worktree_admin_identity_digest = ? \
                 WHERE operation_id = ?",
            )
            .bind("8".repeat(64))
            .bind(operation_id)
            .execute(store.pool())
            .await
            .unwrap();
        }
        MergeCopyCorruption::CandidateTree => {
            sqlx::query(
                "UPDATE task_merge_operations SET candidate_tree_oid = ? WHERE operation_id = ?",
            )
            .bind("7".repeat(40))
            .bind(operation_id)
            .execute(store.pool())
            .await
            .unwrap();
        }
    }
}

async fn drop_merge_update_guards(store: &Store) {
    sqlx::raw_sql(
        "DROP TRIGGER task_merge_operations_immutable_on_update; \
         DROP TRIGGER task_merge_operations_transition_on_update; \
         DROP TRIGGER task_merge_operations_source_consistency_on_update; \
         DROP TRIGGER task_merge_operations_journal_on_update;",
    )
    .execute(store.pool())
    .await
    .unwrap();
}

pub async fn corrupt_approved_review_without_coverage(store: &Store, task: &Task) {
    sqlx::query("DROP TRIGGER task_review_evidence_no_update")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task_review_evidence SET coverage_json = 'null' WHERE task_id = ?")
        .bind(task.id.to_string())
        .execute(store.pool())
        .await
        .unwrap();
}

pub async fn corrupt_transition_ids(
    store: &Store,
    operation_id: DeliveryOperationId,
    make_current_negative: bool,
) {
    sqlx::query("DROP TRIGGER task_delivery_operation_transitions_no_update")
        .execute(store.pool())
        .await
        .unwrap();
    let operation_id = operation_id.to_string();
    let rows: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT entity_version, transition_id FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'merge_operation' AND entity_id = ? ORDER BY entity_version",
    )
    .bind(&operation_id)
    .fetch_all(store.pool())
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    if make_current_negative {
        sqlx::query(
            "UPDATE task_delivery_operation_transitions SET transition_id = -1 \
             WHERE entity_kind = 'merge_operation' AND entity_id = ? AND entity_version = 2",
        )
        .bind(operation_id)
        .execute(store.pool())
        .await
        .unwrap();
        return;
    }
    let initial_id = rows[0].1;
    let current_id = rows[1].1;
    let mut transaction = store.pool().begin().await.unwrap();
    sqlx::query(
        "UPDATE task_delivery_operation_transitions SET transition_id = -1 \
         WHERE entity_kind = 'merge_operation' AND entity_id = ? AND entity_version = 1",
    )
    .bind(&operation_id)
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_delivery_operation_transitions SET transition_id = ? \
         WHERE entity_kind = 'merge_operation' AND entity_id = ? AND entity_version = 2",
    )
    .bind(initial_id)
    .bind(&operation_id)
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_delivery_operation_transitions SET transition_id = ? \
         WHERE entity_kind = 'merge_operation' AND entity_id = ? AND entity_version = 1",
    )
    .bind(current_id)
    .bind(operation_id)
    .execute(&mut *transaction)
    .await
    .unwrap();
    transaction.commit().await.unwrap();
}

pub async fn corrupt_transition_state_pair(store: &Store, operation_id: DeliveryOperationId) {
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("DROP TRIGGER task_delivery_operation_transitions_no_update")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("PRAGMA ignore_check_constraints = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE task_delivery_operation_transitions SET to_state = 'accepted' \
         WHERE entity_kind = 'merge_operation' AND entity_id = ? AND entity_version = 1",
    )
    .bind(operation_id.to_string())
    .execute(&mut *connection)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_delivery_operation_transitions SET from_state = 'accepted' \
         WHERE entity_kind = 'merge_operation' AND entity_id = ? AND entity_version = 2",
    )
    .bind(operation_id.to_string())
    .execute(&mut *connection)
    .await
    .unwrap();
    sqlx::query("PRAGMA ignore_check_constraints = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
}
