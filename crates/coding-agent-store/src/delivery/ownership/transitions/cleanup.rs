use sqlx::SqliteConnection;

use crate::StoreError;

pub(super) async fn transition_pair_is_invalid(
    connection: &mut SqliteConnection,
    entity_id: &str,
) -> Result<bool, StoreError> {
    let invalid: i64 = sqlx::query_scalar(
        "SELECT EXISTS( \
             SELECT 1 FROM task_delivery_operation_transitions transition_row \
             JOIN task_cleanup_operations cleanup \
               ON cleanup.operation_id = transition_row.entity_id \
             WHERE transition_row.entity_kind = 'cleanup_operation' \
               AND transition_row.entity_id = ? AND NOT ( \
                 (transition_row.from_state = 'absent' \
                    AND ((cleanup.kind = 'remove_worktree' \
                          AND transition_row.to_state IN ('unlock_pending', 'remove_pending')) \
                         OR (cleanup.kind = 'delete_branch' \
                             AND transition_row.to_state = 'delete_pending')) \
                    AND transition_row.failure_code IS NULL) \
                 OR (cleanup.kind = 'remove_worktree' \
                    AND transition_row.from_state = 'unlock_pending' \
                    AND transition_row.to_state = 'unlocked_pending_remove' \
                    AND transition_row.failure_code IS NULL) \
                 OR (cleanup.kind = 'remove_worktree' \
                    AND transition_row.from_state = 'unlocked_pending_remove' \
                    AND transition_row.to_state = 'remove_pending' \
                    AND transition_row.failure_code IS NULL) \
                 OR (cleanup.kind = 'delete_branch' \
                    AND transition_row.from_state = 'delete_pending' \
                    AND transition_row.to_state = 'delete_pending' \
                    AND transition_row.failure_code IS NULL) \
                 OR (transition_row.to_state = 'completed' \
                    AND ((cleanup.kind = 'remove_worktree' \
                          AND transition_row.from_state = 'remove_pending') \
                         OR (cleanup.kind = 'delete_branch' \
                             AND transition_row.from_state = 'delete_pending')) \
                    AND transition_row.failure_code IS NULL) \
                 OR (transition_row.to_state = 'failed' \
                    AND ((cleanup.kind = 'remove_worktree' \
                          AND ((transition_row.from_state = 'remove_pending' \
                                AND transition_row.failure_code = 'TARGET_WORKTREE_DIRTY') \
                               OR (transition_row.from_state IN ( \
                                       'unlock_pending', 'remove_pending') \
                                   AND transition_row.failure_code = 'COMMAND_TIMED_OUT'))) \
                         OR (cleanup.kind = 'delete_branch' \
                             AND transition_row.from_state = 'delete_pending' \
                             AND transition_row.failure_code IN ( \
                                 'SOURCE_BRANCH_NOT_MERGED', \
                                 'COMMAND_TIMED_OUT')))) \
                 OR (transition_row.to_state = 'reconciliation_required' \
                    AND ((cleanup.kind = 'remove_worktree' \
                          AND transition_row.from_state IN ( \
                              'unlock_pending', 'unlocked_pending_remove', 'remove_pending')) \
                         OR (cleanup.kind = 'delete_branch' \
                             AND transition_row.from_state = 'delete_pending')) \
                    AND transition_row.failure_code IN ( \
                        'DELIVERY_RECONCILIATION_REQUIRED', \
                        'DELIVERY_SOURCE_INCONSISTENT', \
                        'PROCESS_TREE_CLEANUP_FAILED', \
                        'WORKTREE_IDENTITY_MISMATCH', \
                        'UNSAFE_GIT_CONFIGURATION', \
                        'UNSUPPORTED_GIT_ATTRIBUTES', \
                        'COMMAND_TIMED_OUT')) \
             ) LIMIT 1)",
    )
    .bind(entity_id)
    .fetch_one(connection)
    .await?;
    Ok(invalid == 1)
}
