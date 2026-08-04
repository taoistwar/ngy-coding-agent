use sqlx::sqlite::SqliteRow;
use sqlx::{Row, SqliteConnection};

use crate::StoreError;
use crate::delivery::DeliveryVersion;

use super::super::ownership_invariant;
use super::transition_pair_is_invalid;

#[derive(Debug, Clone, Copy)]
pub(in crate::delivery::ownership) struct TransitionBounds {
    pub(in crate::delivery::ownership) initial_transition_id: i64,
    pub(in crate::delivery::ownership) current_transition_id: i64,
}

struct TransitionSummaryRow {
    row_count: i64,
    minimum_version: Option<i64>,
    maximum_version: Option<i64>,
    maximum_transition_id: Option<i64>,
    current_transition_id: Option<i64>,
    current_state: Option<String>,
    current_failure: Option<String>,
    current_timestamp: Option<String>,
}

impl<'row> sqlx::FromRow<'row, SqliteRow> for TransitionSummaryRow {
    fn from_row(row: &'row SqliteRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            row_count: row.try_get("row_count")?,
            minimum_version: row.try_get("minimum_version")?,
            maximum_version: row.try_get("maximum_version")?,
            maximum_transition_id: row.try_get("maximum_transition_id")?,
            current_transition_id: row.try_get("current_transition_id")?,
            current_state: row.try_get("current_state")?,
            current_failure: row.try_get("current_failure")?,
            current_timestamp: row.try_get("current_timestamp")?,
        })
    }
}

pub(in crate::delivery::ownership) async fn transition_bounds(
    connection: &mut SqliteConnection,
    entity_kind: &str,
    entity_id: &str,
    current_version: DeliveryVersion,
    current_state: &str,
    current_failure: Option<&str>,
    current_updated_at: &str,
) -> Result<TransitionBounds, StoreError> {
    let current_version_value =
        i64::try_from(current_version.get()).map_err(|_| ownership_invariant())?;
    let summary: TransitionSummaryRow = sqlx::query_as(
        "SELECT COUNT(*) AS row_count, MIN(entity_version) AS minimum_version, \
                MAX(entity_version) AS maximum_version, \
                MAX(transition_id) AS maximum_transition_id, \
                MAX(CASE WHEN entity_version = ? THEN transition_id END) \
                    AS current_transition_id, \
                MAX(CASE WHEN entity_version = ? THEN to_state END) AS current_state, \
                MAX(CASE WHEN entity_version = ? THEN failure_code END) AS current_failure, \
                MAX(CASE WHEN entity_version = ? THEN transitioned_at END) AS current_timestamp \
         FROM task_delivery_operation_transitions \
         WHERE entity_kind = ? AND entity_id = ?",
    )
    .bind(current_version_value)
    .bind(current_version_value)
    .bind(current_version_value)
    .bind(current_version_value)
    .bind(entity_kind)
    .bind(entity_id)
    .fetch_one(&mut *connection)
    .await?;
    let initial_transition_id: Option<i64> = sqlx::query_scalar(
        "SELECT transition_id FROM task_delivery_operation_transitions \
         WHERE entity_kind = ? AND entity_id = ? AND entity_version = 1",
    )
    .bind(entity_kind)
    .bind(entity_id)
    .fetch_optional(&mut *connection)
    .await?
    .flatten();
    let chain_is_invalid: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM task_delivery_operation_transitions current \
         LEFT JOIN task_delivery_operation_transitions previous \
           ON previous.entity_kind = current.entity_kind \
          AND previous.entity_id = current.entity_id \
          AND previous.entity_version = current.entity_version - 1 \
         WHERE current.entity_kind = ? AND current.entity_id = ? \
           AND (current.transition_id <= 0 \
             OR (current.entity_version = 1 AND current.from_state != 'absent') \
             OR (current.entity_version > 1 \
                 AND (previous.transition_id IS NULL \
                   OR current.from_state != previous.to_state \
                   OR current.transition_id <= previous.transition_id))) \
         LIMIT 1)",
    )
    .bind(entity_kind)
    .bind(entity_id)
    .fetch_one(&mut *connection)
    .await?;
    let pair_is_invalid =
        transition_pair_is_invalid(&mut *connection, entity_kind, entity_id).await?;
    let valid = summary.row_count == current_version_value
        && summary.minimum_version == Some(1)
        && summary.maximum_version == Some(current_version_value)
        && summary
            .maximum_transition_id
            .is_some_and(|maximum| maximum > 0 && summary.current_transition_id == Some(maximum))
        && summary.current_state.as_deref() == Some(current_state)
        && summary.current_failure.as_deref() == current_failure
        && summary.current_timestamp.as_deref() == Some(current_updated_at)
        && chain_is_invalid == 0
        && !pair_is_invalid;
    if !valid {
        return Err(ownership_invariant());
    }
    Ok(TransitionBounds {
        initial_transition_id: initial_transition_id.ok_or_else(ownership_invariant)?,
        current_transition_id: summary
            .current_transition_id
            .ok_or_else(ownership_invariant)?,
    })
}
