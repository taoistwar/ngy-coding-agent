use sqlx::SqliteConnection;

use crate::StoreError;

pub(super) async fn transition_pair_is_invalid(
    connection: &mut SqliteConnection,
    entity_id: &str,
) -> Result<bool, StoreError> {
    let invalid: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'merge_operation' AND entity_id = ? AND NOT ( \
             (from_state = 'absent' AND to_state = 'preflight_pending' \
                 AND entity_version = 1 \
                 AND failure_code IS NULL) \
             OR (from_state = 'preflight_pending' AND ( \
                 (to_state = 'preflight_pending' AND entity_version = 2 \
                    AND failure_code IS NULL) \
                 OR (to_state = 'preflight_ready' AND entity_version = 3 \
                     AND failure_code IS NULL) \
                 OR (to_state = 'conflict' AND entity_version = 3 \
                    AND failure_code IS 'MERGE_CONFLICT') \
                 OR (to_state = 'rejected' AND entity_version IN (2, 3) AND failure_code IN ( \
                     'TASK_NOT_MERGE_ELIGIBLE', 'TARGET_BRANCH_DETACHED', \
                     'TARGET_BRANCH_MISMATCH', 'TARGET_WORKTREE_DIRTY', \
                     'TARGET_IGNORED_PATH_COLLISION', \
                     'TARGET_GIT_OPERATION_IN_PROGRESS', \
                     'UNSAFE_GIT_CONFIGURATION', 'UNSUPPORTED_GIT_ATTRIBUTES', \
                     'SOURCE_ALREADY_IN_TARGET')) \
                 OR (to_state = 'stale' AND entity_version IN (2, 3) AND failure_code IN ( \
                     'DELIVERY_EVIDENCE_STALE', 'TARGET_BRANCH_MISMATCH', \
                     'TARGET_HEAD_CHANGED', 'DELIVERY_SOURCE_CHANGED')) \
                 OR (to_state = 'reconciliation_required' AND entity_version IN (2, 3) \
                    AND failure_code IN ( \
                     'DELIVERY_RECONCILIATION_REQUIRED', 'DELIVERY_SOURCE_INCONSISTENT', \
                     'PROCESS_TREE_CLEANUP_FAILED', 'WORKTREE_IDENTITY_MISMATCH', \
                     'UNSAFE_GIT_CONFIGURATION', 'UNSUPPORTED_GIT_ATTRIBUTES')))) \
             OR (from_state = 'preflight_ready' AND ( \
                 (to_state IN ('accepted', 'superseded') AND failure_code IS NULL) \
                 OR (to_state = 'stale' AND failure_code IN ( \
                     'DELIVERY_EVIDENCE_STALE', 'TARGET_BRANCH_MISMATCH', \
                     'TARGET_HEAD_CHANGED', 'DELIVERY_SOURCE_CHANGED')) \
                 OR (to_state = 'reconciliation_required' AND failure_code IN ( \
                     'DELIVERY_RECONCILIATION_REQUIRED', 'DELIVERY_SOURCE_INCONSISTENT', \
                     'PROCESS_TREE_CLEANUP_FAILED', 'WORKTREE_IDENTITY_MISMATCH', \
                     'UNSAFE_GIT_CONFIGURATION', 'UNSUPPORTED_GIT_ATTRIBUTES')))) \
             OR (from_state = 'accepted' AND ( \
                 (to_state = 'merge_pending' AND failure_code IS NULL) \
                 OR (to_state = 'failed' AND failure_code IN ( \
                     'TASK_NOT_MERGE_ELIGIBLE', 'TARGET_BRANCH_DETACHED', \
                     'TARGET_BRANCH_MISMATCH', 'TARGET_WORKTREE_DIRTY', \
                     'TARGET_IGNORED_PATH_COLLISION', \
                     'TARGET_GIT_OPERATION_IN_PROGRESS', \
                     'UNSAFE_GIT_CONFIGURATION', 'UNSUPPORTED_GIT_ATTRIBUTES', \
                     'SOURCE_ALREADY_IN_TARGET', 'TARGET_HEAD_CHANGED', \
                     'COMMAND_TIMED_OUT')) \
                 OR (to_state = 'reconciliation_required' AND failure_code IN ( \
                     'DELIVERY_RECONCILIATION_REQUIRED', 'DELIVERY_SOURCE_INCONSISTENT', \
                     'PROCESS_TREE_CLEANUP_FAILED', 'WORKTREE_IDENTITY_MISMATCH', \
                     'UNSAFE_GIT_CONFIGURATION', 'UNSUPPORTED_GIT_ATTRIBUTES')))) \
             OR (from_state = 'merge_pending' AND ( \
                 (to_state IN ('merged', 'abort_pending') AND failure_code IS NULL) \
                 OR (to_state = 'failed' AND failure_code IN ( \
                     'TASK_NOT_MERGE_ELIGIBLE', 'TARGET_BRANCH_DETACHED', \
                     'TARGET_BRANCH_MISMATCH', 'TARGET_WORKTREE_DIRTY', \
                     'TARGET_IGNORED_PATH_COLLISION', \
                     'TARGET_GIT_OPERATION_IN_PROGRESS', \
                     'UNSAFE_GIT_CONFIGURATION', 'UNSUPPORTED_GIT_ATTRIBUTES', \
                     'SOURCE_ALREADY_IN_TARGET', 'TARGET_HEAD_CHANGED', \
                     'COMMAND_TIMED_OUT')) \
                 OR (to_state = 'reconciliation_required' AND failure_code IN ( \
                     'DELIVERY_RECONCILIATION_REQUIRED', 'DELIVERY_SOURCE_INCONSISTENT', \
                     'PROCESS_TREE_CLEANUP_FAILED', 'WORKTREE_IDENTITY_MISMATCH', \
                     'UNSAFE_GIT_CONFIGURATION', 'UNSUPPORTED_GIT_ATTRIBUTES')))) \
             OR (from_state = 'abort_pending' AND ( \
                 (to_state = 'conflict' AND failure_code IS 'MERGE_CONFLICT') \
                 OR (to_state = 'reconciliation_required' AND failure_code IN ( \
                     'DELIVERY_RECONCILIATION_REQUIRED', 'DELIVERY_SOURCE_INCONSISTENT', \
                     'PROCESS_TREE_CLEANUP_FAILED', 'WORKTREE_IDENTITY_MISMATCH', \
                     'UNSAFE_GIT_CONFIGURATION', 'UNSUPPORTED_GIT_ATTRIBUTES')))) \
         ) LIMIT 1)",
    )
    .bind(entity_id)
    .fetch_one(connection)
    .await?;
    Ok(invalid == 1)
}
