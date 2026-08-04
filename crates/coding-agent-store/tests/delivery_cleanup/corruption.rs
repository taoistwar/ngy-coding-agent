use coding_agent_store::{
    CleanupOperationAnchor, CompleteBranchCleanupRequest, DeliveryOperationId, DeliveryVersion,
    StoreError,
};

use super::fixtures::merged_fixture;

#[tokio::test]
async fn orphan_target_head_observation_fails_closed_for_a_missing_operation() {
    let (store, task, _) = merged_fixture("codex/task7-orphan-target-observation").await;
    let missing_operation_id = DeliveryOperationId::new();
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::raw_sql(
        "DROP TRIGGER task_cleanup_target_head_observations_match_current; \
         PRAGMA foreign_keys = OFF;",
    )
    .execute(&mut *connection)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO task_cleanup_target_head_observations ( \
             cleanup_operation_id, operation_version, target_head, observed_at \
         ) VALUES (?, 1, '3333333333333333333333333333333333333333', \
                   '2026-08-05T00:00:00.000000000Z')",
    )
    .bind(missing_operation_id.to_string())
    .execute(&mut *connection)
    .await
    .unwrap();
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
    drop(connection);

    let request = CompleteBranchCleanupRequest::try_new(
        CleanupOperationAnchor::try_new(task.id, missing_operation_id, DeliveryVersion::initial())
            .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        store.complete_branch_cleanup(request).await,
        Err(StoreError::InvariantViolation(_))
    ));
}
