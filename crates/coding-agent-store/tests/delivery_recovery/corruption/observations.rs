use crate::corruption_cases::assert_recovery_invariant;
use crate::recovery_fixtures::merged_task;
use crate::support::delivery::eligibility::{
    MERGE_COMMIT, complete_worktree_cleanup, create_branch_cleanup, create_worktree_cleanup,
};

#[tokio::test]
async fn target_head_observation_missing_head_or_timestamp_mismatch_fails_closed() {
    for corruption in ["missing", "head", "timestamp"] {
        let store = crate::support::seeded_store().await;
        let task = merged_task(&store, "codex/recovery-observation-corrupt").await;
        let worktree = create_worktree_cleanup(&store, &task).await;
        complete_worktree_cleanup(&store, &task, worktree).await;
        let operation_id = create_branch_cleanup(&store, &task, MERGE_COMMIT).await;
        let trigger = match corruption {
            "missing" => "DROP TRIGGER task_cleanup_target_head_observations_no_delete",
            _ => "DROP TRIGGER task_cleanup_target_head_observations_no_update",
        };
        sqlx::query(trigger).execute(store.pool()).await.unwrap();
        match corruption {
            "missing" => {
                sqlx::query(
                    "DELETE FROM task_cleanup_target_head_observations \
                     WHERE cleanup_operation_id = ? AND operation_version = 1",
                )
                .bind(operation_id.to_string())
                .execute(store.pool())
                .await
                .unwrap();
            }
            "head" => {
                sqlx::query(
                    "UPDATE task_cleanup_target_head_observations SET target_head = ? \
                     WHERE cleanup_operation_id = ? AND operation_version = 1",
                )
                .bind("8".repeat(40))
                .bind(operation_id.to_string())
                .execute(store.pool())
                .await
                .unwrap();
            }
            "timestamp" => {
                sqlx::query(
                    "UPDATE task_cleanup_target_head_observations \
                     SET observed_at = '2026-08-04T00:00:01.000000000Z' \
                     WHERE cleanup_operation_id = ? AND operation_version = 1",
                )
                .bind(operation_id.to_string())
                .execute(store.pool())
                .await
                .unwrap();
            }
            _ => unreachable!(),
        }
        assert_recovery_invariant(&store).await;
    }
}

#[tokio::test]
async fn orphan_target_head_observation_fails_before_identity_filtering() {
    let store = crate::support::seeded_store().await;
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("DROP TRIGGER task_cleanup_target_head_observations_match_current")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO task_cleanup_target_head_observations ( \
             cleanup_operation_id, operation_version, target_head, observed_at \
         ) VALUES ('dddddddd-1111-4111-8111-111111111111', 1, ?, \
             '2026-08-04T00:00:00.000000000Z')",
    )
    .bind("8".repeat(40))
    .execute(&mut *connection)
    .await
    .unwrap();
    drop(connection);
    assert_recovery_invariant(&store).await;
}
