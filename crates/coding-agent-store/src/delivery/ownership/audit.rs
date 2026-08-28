use coding_agent_domain::TaskId;
use sqlx::SqliteConnection;

use crate::StoreError;

use super::ownership_invariant;

const OPERATION_JOURNAL_AUDIT_SQL: &str = r#"
WITH operations AS (
    SELECT
        'merge_operation' AS entity_kind,
        operation_id AS entity_id,
        version AS current_version,
        state AS current_state,
        failure_code AS current_failure,
        target_config_attributes_digest AS current_target_config_attributes_digest,
        target_security_digest AS current_target_security_digest,
        updated_at AS current_timestamp
    FROM task_merge_operations
    WHERE task_id = ?
    UNION ALL
    SELECT
        'cleanup_operation' AS entity_kind,
        operation_id AS entity_id,
        version AS current_version,
        state AS current_state,
        failure_code AS current_failure,
        NULL AS current_target_config_attributes_digest,
        NULL AS current_target_security_digest,
        updated_at AS current_timestamp
    FROM task_cleanup_operations
    WHERE task_id = ?
), summaries AS (
    SELECT
        operation.entity_kind,
        operation.entity_id,
        operation.current_version,
        operation.current_state,
        operation.current_failure,
        operation.current_target_config_attributes_digest,
        operation.current_target_security_digest,
        operation.current_timestamp,
        COUNT(transition.transition_id) AS row_count,
        MIN(transition.entity_version) AS minimum_version,
        MAX(transition.entity_version) AS maximum_version,
        MAX(transition.transition_id) AS maximum_transition_id,
        MAX(
            CASE
                WHEN transition.entity_version = operation.current_version
                THEN transition.transition_id
            END
        ) AS current_transition_id,
        MAX(
            CASE
                WHEN transition.entity_version = operation.current_version
                THEN transition.to_state
            END
        ) AS journal_current_state,
        MAX(
            CASE
                WHEN transition.entity_version = operation.current_version
                THEN transition.failure_code
            END
        ) AS journal_current_failure,
        MAX(
            CASE
                WHEN transition.entity_version = operation.current_version
                THEN transition.transitioned_at
            END
        ) AS journal_current_timestamp,
        MAX(
            CASE
                WHEN transition.target_config_attributes_digest
                        IS NOT operation.current_target_config_attributes_digest
                  OR transition.target_security_digest
                        IS NOT operation.current_target_security_digest
                THEN 1 ELSE 0
            END
        ) AS target_provenance_mismatch
    FROM operations AS operation
    LEFT JOIN task_delivery_operation_transitions AS transition
      ON transition.entity_kind = operation.entity_kind
     AND transition.entity_id = operation.entity_id
    GROUP BY
        operation.entity_kind,
        operation.entity_id,
        operation.current_version,
        operation.current_state,
        operation.current_failure,
        operation.current_target_config_attributes_digest,
        operation.current_target_security_digest,
        operation.current_timestamp
)
SELECT EXISTS (
    SELECT 1
    FROM summaries AS summary
    WHERE summary.row_count != summary.current_version
       OR summary.minimum_version IS NOT 1
       OR summary.maximum_version IS NOT summary.current_version
       OR summary.maximum_transition_id IS NULL
       OR summary.maximum_transition_id <= 0
       OR summary.current_transition_id IS NOT summary.maximum_transition_id
       OR summary.journal_current_state IS NOT summary.current_state
       OR summary.journal_current_failure IS NOT summary.current_failure
       OR summary.journal_current_timestamp IS NOT summary.current_timestamp
       OR summary.target_provenance_mismatch != 0
       OR EXISTS (
            SELECT 1
            FROM task_delivery_operation_transitions AS current_transition
            LEFT JOIN task_delivery_operation_transitions AS previous_transition
              ON previous_transition.entity_kind = current_transition.entity_kind
             AND previous_transition.entity_id = current_transition.entity_id
             AND previous_transition.entity_version = current_transition.entity_version - 1
            WHERE current_transition.entity_kind = summary.entity_kind
              AND current_transition.entity_id = summary.entity_id
              AND (
                    current_transition.transition_id <= 0
                 OR (
                        current_transition.entity_version = 1
                    AND current_transition.from_state != 'absent'
                 )
                 OR (
                        current_transition.entity_version > 1
                    AND (
                           previous_transition.transition_id IS NULL
                        OR current_transition.from_state != previous_transition.to_state
                        OR current_transition.transition_id <= previous_transition.transition_id
                    )
                 )
                 OR (
                        current_transition.entity_kind = 'merge_operation'
                    AND NOT (
                           (current_transition.to_state IN (
                                'preflight_pending', 'preflight_ready', 'accepted',
                                'merge_pending', 'merged', 'abort_pending', 'superseded'
                            ) AND current_transition.failure_code IS NULL)
                        OR (current_transition.to_state = 'conflict'
                            AND current_transition.failure_code IS 'MERGE_CONFLICT')
                        OR (current_transition.to_state = 'rejected'
                            AND current_transition.failure_code IS NOT NULL
                            AND current_transition.failure_code IN (
                                'TASK_NOT_MERGE_ELIGIBLE', 'TARGET_BRANCH_DETACHED',
                                'TARGET_BRANCH_MISMATCH', 'TARGET_WORKTREE_DIRTY',
                                'TARGET_IGNORED_PATH_COLLISION',
                                'TARGET_GIT_OPERATION_IN_PROGRESS',
                                'UNSAFE_GIT_CONFIGURATION', 'UNSUPPORTED_GIT_ATTRIBUTES',
                                'SOURCE_ALREADY_IN_TARGET'
                            ))
                        OR (current_transition.to_state = 'stale'
                            AND current_transition.failure_code IS NOT NULL
                            AND current_transition.failure_code IN (
                                'DELIVERY_EVIDENCE_STALE', 'TARGET_BRANCH_MISMATCH',
                                'TARGET_HEAD_CHANGED', 'DELIVERY_SOURCE_CHANGED'
                            ))
                        OR (current_transition.to_state = 'failed'
                            AND current_transition.failure_code IS NOT NULL
                            AND current_transition.failure_code IN (
                                'TASK_NOT_MERGE_ELIGIBLE', 'TARGET_BRANCH_DETACHED',
                                'TARGET_BRANCH_MISMATCH', 'TARGET_HEAD_CHANGED',
                                'TARGET_WORKTREE_DIRTY', 'TARGET_IGNORED_PATH_COLLISION',
                                'TARGET_GIT_OPERATION_IN_PROGRESS',
                                'UNSAFE_GIT_CONFIGURATION', 'UNSUPPORTED_GIT_ATTRIBUTES',
                                'SOURCE_ALREADY_IN_TARGET', 'COMMAND_TIMED_OUT'
                            ))
                        OR (current_transition.to_state = 'reconciliation_required'
                            AND current_transition.failure_code IS NOT NULL
                            AND current_transition.failure_code IN (
                                'DELIVERY_RECONCILIATION_REQUIRED',
                                'DELIVERY_SOURCE_INCONSISTENT',
                                'PROCESS_TREE_CLEANUP_FAILED',
                                'WORKTREE_IDENTITY_MISMATCH',
                                'UNSAFE_GIT_CONFIGURATION',
                                'UNSUPPORTED_GIT_ATTRIBUTES'
                            ))
                    )
                 )
                 OR NOT (
                        (
                            summary.entity_kind = 'merge_operation'
                            AND (
                                (
                                    current_transition.from_state = 'absent'
                                    AND current_transition.to_state = 'preflight_pending'
                                    AND current_transition.entity_version = 1
                                )
                                OR (
                                    current_transition.from_state = 'preflight_pending'
                                    AND (
                                        (current_transition.to_state = 'preflight_pending'
                                            AND current_transition.entity_version = 2)
                                        OR (current_transition.to_state IN (
                                            'preflight_ready', 'conflict'
                                        ) AND current_transition.entity_version = 3)
                                        OR (current_transition.to_state IN (
                                            'rejected', 'stale', 'reconciliation_required'
                                        ) AND current_transition.entity_version IN (2, 3))
                                    )
                                )
                                OR (
                                    current_transition.from_state = 'preflight_ready'
                                    AND current_transition.to_state IN (
                                        'accepted', 'stale', 'superseded',
                                        'reconciliation_required'
                                    )
                                )
                                OR (
                                    current_transition.from_state = 'accepted'
                                    AND current_transition.to_state IN (
                                        'merge_pending', 'failed', 'reconciliation_required'
                                    )
                                )
                                OR (
                                    current_transition.from_state = 'merge_pending'
                                    AND current_transition.to_state IN (
                                        'merged', 'abort_pending', 'failed',
                                        'reconciliation_required'
                                    )
                                )
                                OR (
                                    current_transition.from_state = 'abort_pending'
                                    AND current_transition.to_state IN (
                                        'conflict', 'reconciliation_required'
                                    )
                                )
                            )
                        )
                        OR (
                            summary.entity_kind = 'cleanup_operation'
                            AND (
                                (
                                    current_transition.from_state = 'absent'
                                    AND current_transition.to_state IN (
                                        'unlock_pending', 'remove_pending', 'delete_pending'
                                    )
                                )
                                OR (
                                    current_transition.from_state = 'unlock_pending'
                                    AND current_transition.to_state IN (
                                        'unlocked_pending_remove', 'failed',
                                        'reconciliation_required'
                                    )
                                )
                                OR (
                                    current_transition.from_state = 'unlocked_pending_remove'
                                    AND current_transition.to_state IN (
                                        'remove_pending', 'reconciliation_required'
                                    )
                                )
                                OR (
                                    current_transition.from_state = 'remove_pending'
                                    AND current_transition.to_state IN (
                                        'completed', 'failed', 'reconciliation_required'
                                    )
                                )
                                OR (
                                    current_transition.from_state = 'delete_pending'
                                    AND current_transition.to_state IN (
                                        'delete_pending', 'completed', 'failed',
                                        'reconciliation_required'
                                    )
                                )
                            )
                        )
                 )
              )
            LIMIT 1
       )
    LIMIT 1
)
"#;

const CLEANUP_JOURNAL_CONTRACT_AUDIT_SQL: &str = r#"
SELECT EXISTS (
    SELECT 1
    FROM task_cleanup_operations AS cleanup
    JOIN task_delivery_operation_transitions AS transition_row
      ON transition_row.entity_kind = 'cleanup_operation'
     AND transition_row.entity_id = cleanup.operation_id
    WHERE cleanup.task_id = ?
      AND NOT (
          (
              transition_row.from_state = 'absent'
              AND (
                  (cleanup.kind = 'remove_worktree'
                      AND transition_row.to_state IN ('unlock_pending', 'remove_pending'))
                  OR (cleanup.kind = 'delete_branch'
                      AND transition_row.to_state = 'delete_pending')
              )
              AND transition_row.failure_code IS NULL
          )
          OR (
              cleanup.kind = 'remove_worktree'
              AND transition_row.from_state = 'unlock_pending'
              AND transition_row.to_state = 'unlocked_pending_remove'
              AND transition_row.failure_code IS NULL
          )
          OR (
              cleanup.kind = 'remove_worktree'
              AND transition_row.from_state = 'unlocked_pending_remove'
              AND transition_row.to_state = 'remove_pending'
              AND transition_row.failure_code IS NULL
          )
          OR (
              cleanup.kind = 'delete_branch'
              AND transition_row.from_state = 'delete_pending'
              AND transition_row.to_state = 'delete_pending'
              AND transition_row.failure_code IS NULL
          )
          OR (
              transition_row.to_state = 'completed'
              AND (
                  (cleanup.kind = 'remove_worktree'
                      AND transition_row.from_state = 'remove_pending')
                  OR (cleanup.kind = 'delete_branch'
                      AND transition_row.from_state = 'delete_pending')
              )
              AND transition_row.failure_code IS NULL
          )
          OR (
              transition_row.to_state = 'failed'
              AND (
                  (cleanup.kind = 'remove_worktree'
                      AND (
                          (transition_row.from_state = 'remove_pending'
                              AND transition_row.failure_code = 'TARGET_WORKTREE_DIRTY')
                          OR (transition_row.from_state IN ('unlock_pending', 'remove_pending')
                              AND transition_row.failure_code = 'COMMAND_TIMED_OUT')
                      ))
                  OR (cleanup.kind = 'delete_branch'
                      AND transition_row.from_state = 'delete_pending'
                      AND transition_row.failure_code IN (
                          'SOURCE_BRANCH_NOT_MERGED', 'COMMAND_TIMED_OUT'
                      ))
              )
          )
          OR (
              transition_row.to_state = 'reconciliation_required'
              AND (
                  (cleanup.kind = 'remove_worktree'
                      AND transition_row.from_state IN (
                          'unlock_pending', 'unlocked_pending_remove', 'remove_pending'
                      ))
                  OR (cleanup.kind = 'delete_branch'
                      AND transition_row.from_state = 'delete_pending')
              )
              AND transition_row.failure_code IN (
                  'DELIVERY_RECONCILIATION_REQUIRED',
                  'DELIVERY_SOURCE_INCONSISTENT',
                  'PROCESS_TREE_CLEANUP_FAILED',
                  'WORKTREE_IDENTITY_MISMATCH',
                  'UNSAFE_GIT_CONFIGURATION',
                  'UNSUPPORTED_GIT_ATTRIBUTES',
                  'COMMAND_TIMED_OUT'
              )
          )
      )
    LIMIT 1
)
"#;

pub(super) async fn audit_operation_journals(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<(), StoreError> {
    let invalid: i64 = sqlx::query_scalar(OPERATION_JOURNAL_AUDIT_SQL)
        .bind(task_id.to_string())
        .bind(task_id.to_string())
        .fetch_one(&mut *connection)
        .await?;
    let cleanup_contract_invalid: i64 = sqlx::query_scalar(CLEANUP_JOURNAL_CONTRACT_AUDIT_SQL)
        .bind(task_id.to_string())
        .fetch_one(&mut *connection)
        .await?;
    if invalid == 0 && cleanup_contract_invalid == 0 {
        Ok(())
    } else {
        Err(ownership_invariant())
    }
}
