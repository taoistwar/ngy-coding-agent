use coding_agent_domain::TaskId;
use sqlx::SqliteConnection;

use crate::StoreError;
use crate::delivery::DeliveryVersion;

use super::super::decode::parse_version;
use super::super::ownership_invariant;

pub(super) async fn transition_pair_is_invalid(
    connection: &mut SqliteConnection,
    entity_kind: &str,
    entity_id: &str,
) -> Result<bool, StoreError> {
    let invalid: i64 = match entity_kind {
        "worktree_disposition" => {
            sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM task_delivery_operation_transitions \
             WHERE entity_kind = 'worktree_disposition' AND entity_id = ? AND NOT ( \
                 (from_state = 'absent' AND to_state = 'retained_locked' \
                    AND failure_code IS NULL) \
                 OR (from_state = 'retained_locked' AND to_state = 'retained_unlocked' \
                    AND failure_code IS NULL) \
                 OR (from_state = 'retained_unlocked' AND to_state = 'removed' \
                    AND failure_code IS NULL) \
                 OR (from_state IN ('retained_locked', 'retained_unlocked', 'removed') \
                    AND to_state = 'reconciliation_required' \
                    AND failure_code IS NOT NULL) \
             ) LIMIT 1)",
            )
            .bind(entity_id)
            .fetch_one(&mut *connection)
            .await?
        }
        "branch_disposition" => {
            sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM task_delivery_operation_transitions \
             WHERE entity_kind = 'branch_disposition' AND entity_id = ? AND NOT ( \
                 (from_state = 'absent' AND to_state = 'retained' \
                    AND failure_code IS NULL) \
                 OR (from_state = 'retained' AND to_state = 'deleted' \
                    AND failure_code IS NULL) \
                 OR (from_state IN ('retained', 'deleted') \
                    AND to_state = 'reconciliation_required' \
                    AND failure_code IS NOT NULL) \
             ) LIMIT 1)",
            )
            .bind(entity_id)
            .fetch_one(&mut *connection)
            .await?
        }
        _ => return Err(ownership_invariant()),
    };
    Ok(invalid == 1)
}

pub(in crate::delivery::ownership) async fn disposition_state_at(
    connection: &mut SqliteConnection,
    entity_kind: &str,
    task_id: TaskId,
    transition_id: i64,
) -> Result<(DeliveryVersion, String), StoreError> {
    let row: Option<(i64, String)> = sqlx::query_as(
        "SELECT entity_version, to_state FROM task_delivery_operation_transitions \
         WHERE entity_kind = ? AND entity_id = ? AND transition_id <= ? \
         ORDER BY transition_id DESC LIMIT 1",
    )
    .bind(entity_kind)
    .bind(task_id.to_string())
    .bind(transition_id)
    .fetch_optional(connection)
    .await?;
    let (version_value, state) = row.ok_or_else(ownership_invariant)?;
    Ok((parse_version(version_value)?, state))
}
