use coding_agent_domain::TaskId;
use coding_agent_store::Store;

#[derive(Debug, PartialEq, Eq)]
pub(super) struct DeliverySnapshot {
    operations: Vec<CleanupOperationSnapshotRow>,
    receipts: Vec<(String, String, String, i64, String)>,
    cleanup_transitions: Vec<(String, i64, String, String, Option<String>, String)>,
    worktree_disposition: Option<DispositionSnapshotRow>,
    branch_disposition: Option<DispositionSnapshotRow>,
    disposition_transitions: Vec<(String, i64, String, String, Option<String>, String)>,
    observations: Vec<(String, i64, String, String)>,
}

type CleanupOperationSnapshotRow = (
    String,
    String,
    String,
    i64,
    i64,
    Option<String>,
    Option<String>,
);

type DispositionSnapshotRow = (
    String,
    i64,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<String>,
    String,
);

pub(super) async fn delivery_snapshot(store: &Store, task_id: TaskId) -> DeliverySnapshot {
    DeliverySnapshot {
        operations: sqlx::query_as(
            "SELECT operation_id, kind, state, version, expected_disposition_version, \
                    failure_code, expected_target_head \
             FROM task_cleanup_operations WHERE task_id = ? ORDER BY operation_id",
        )
        .bind(task_id.to_string())
        .fetch_all(store.pool())
        .await
        .unwrap(),
        receipts: sqlx::query_as(
            "SELECT client_request_id, command_kind, operation_id, \
                    accepted_operation_version, accepted_operation_state \
             FROM task_delivery_command_receipts \
             WHERE task_id = ? AND command_kind IN ('remove_worktree', 'delete_branch') \
             ORDER BY client_request_id",
        )
        .bind(task_id.to_string())
        .fetch_all(store.pool())
        .await
        .unwrap(),
        cleanup_transitions: sqlx::query_as(
            "SELECT entity_id, entity_version, from_state, to_state, \
                    failure_code, transitioned_at \
             FROM task_delivery_operation_transitions \
             WHERE entity_kind = 'cleanup_operation' ORDER BY transition_id",
        )
        .fetch_all(store.pool())
        .await
        .unwrap(),
        worktree_disposition: sqlx::query_as(
            "SELECT worktree_state, worktree_version, worktree_failure_code, \
                    worktree_cleanup_operation_id, worktree_cleanup_operation_version, \
                    worktree_cleanup_operation_state, worktree_updated_at \
             FROM task_artifact_dispositions WHERE task_id = ?",
        )
        .bind(task_id.to_string())
        .fetch_optional(store.pool())
        .await
        .unwrap(),
        branch_disposition: sqlx::query_as(
            "SELECT branch_state, branch_version, branch_failure_code, \
                    branch_cleanup_operation_id, branch_cleanup_operation_version, \
                    branch_cleanup_operation_state, branch_updated_at \
             FROM task_artifact_dispositions WHERE task_id = ?",
        )
        .bind(task_id.to_string())
        .fetch_optional(store.pool())
        .await
        .unwrap(),
        disposition_transitions: sqlx::query_as(
            "SELECT entity_kind, entity_version, from_state, to_state, failure_code, transitioned_at \
             FROM task_delivery_operation_transitions \
             WHERE entity_kind IN ('worktree_disposition', 'branch_disposition') \
               AND entity_id = ? ORDER BY transition_id",
        )
        .bind(task_id.to_string())
        .fetch_all(store.pool())
        .await
        .unwrap(),
        observations: sqlx::query_as(
            "SELECT cleanup_operation_id, operation_version, target_head, observed_at \
             FROM task_cleanup_target_head_observations \
             ORDER BY cleanup_operation_id, operation_version",
        )
        .fetch_all(store.pool())
        .await
        .unwrap(),
    }
}
