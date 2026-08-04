use coding_agent_domain::Task;
use coding_agent_store::{DeliveryOperationId, Store};

use crate::support::delivery::eligibility::{
    DELIVERY_TIMESTAMP, approved_task_on_store, create_merged_delivery, create_worktree_cleanup,
};

pub async fn merged_task(store: &Store, branch: &str) -> Task {
    let (_, task) = approved_task_on_store(store.clone(), branch, 0).await;
    let evidence = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .evidence_identity
        .unwrap();
    create_merged_delivery(store, &task, &evidence).await;
    task
}

pub async fn worktree_cleanup(store: &Store, branch: &str) -> (Task, DeliveryOperationId) {
    let task = merged_task(store, branch).await;
    let operation_id = create_worktree_cleanup(store, &task).await;
    (task, operation_id)
}

pub async fn mark_unlocked_pending_remove(
    store: &Store,
    task: &Task,
    operation_id: DeliveryOperationId,
) {
    let mut transaction = store.pool().begin().await.unwrap();
    sqlx::query(
        "UPDATE task_artifact_dispositions SET worktree_state = 'retained_unlocked', \
             worktree_version = 2, worktree_cleanup_operation_id = ?, \
             worktree_cleanup_operation_version = 2, \
             worktree_cleanup_operation_state = 'unlocked_pending_remove', \
             worktree_updated_at = ? WHERE task_id = ?",
    )
    .bind(operation_id.to_string())
    .bind(DELIVERY_TIMESTAMP)
    .bind(task.id.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_cleanup_operations SET state = 'unlocked_pending_remove', \
             expected_disposition_version = 2, version = 2, updated_at = ? \
         WHERE operation_id = ?",
    )
    .bind(DELIVERY_TIMESTAMP)
    .bind(operation_id.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    transaction.commit().await.unwrap();
}

pub async fn mark_remove_pending(store: &Store, operation_id: DeliveryOperationId) {
    sqlx::query(
        "UPDATE task_cleanup_operations SET state = 'remove_pending', version = 3, \
             updated_at = ? WHERE operation_id = ?",
    )
    .bind(DELIVERY_TIMESTAMP)
    .bind(operation_id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
}
