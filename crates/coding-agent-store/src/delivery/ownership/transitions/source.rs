use coding_agent_domain::TaskId;
use sqlx::SqliteConnection;

use crate::StoreError;
use crate::delivery::DeliverySourceState;

use super::super::decode::parse_source_state;

pub(super) async fn transition_pair_is_invalid(
    connection: &mut SqliteConnection,
    entity_id: &str,
) -> Result<bool, StoreError> {
    let invalid: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'delivery_source' AND entity_id = ? AND NOT ( \
             (from_state = 'absent' AND to_state = 'object_pending' \
                 AND failure_code IS NULL) \
             OR (from_state = 'object_pending' AND to_state = 'object_pending' \
                 AND failure_code IS 'COMMAND_TIMED_OUT') \
             OR (from_state = 'commit_pending' AND to_state = 'commit_pending' \
                 AND failure_code IS 'COMMAND_TIMED_OUT') \
             OR (from_state = 'object_pending' AND to_state = 'commit_pending' \
                 AND failure_code IS NULL) \
             OR (from_state = 'commit_pending' AND to_state = 'committed' \
                 AND failure_code IS NULL) \
             OR (from_state IN ('object_pending', 'commit_pending', 'committed') \
                 AND to_state = 'reconciliation_required' \
                 AND failure_code IN ( \
                     'DELIVERY_SOURCE_INCONSISTENT', 'PROCESS_TREE_CLEANUP_FAILED')) \
         ) LIMIT 1)",
    )
    .bind(entity_id)
    .fetch_one(connection)
    .await?;
    Ok(invalid == 1)
}

pub(in crate::delivery::ownership) async fn source_state_at(
    connection: &mut SqliteConnection,
    task_id: TaskId,
    transition_id: i64,
) -> Result<Option<DeliverySourceState>, StoreError> {
    let state: Option<String> = sqlx::query_scalar(
        "SELECT to_state FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'delivery_source' AND entity_id = ? AND transition_id <= ? \
         ORDER BY transition_id DESC LIMIT 1",
    )
    .bind(task_id.to_string())
    .bind(transition_id)
    .fetch_optional(connection)
    .await?;
    state.map(parse_source_state).transpose()
}
