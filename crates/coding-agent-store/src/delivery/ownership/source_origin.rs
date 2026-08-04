use sqlx::SqliteConnection;

use crate::StoreError;
use crate::delivery::{DeliverySourceRecord, DeliveryTimestamp, MergeOperationRecord};

use super::{AcceptReceiptAudit, audit_accept_receipt, ownership_invariant};

pub(super) async fn validate_source_origin(
    connection: &mut SqliteConnection,
    source: &DeliverySourceRecord,
    operations: &[MergeOperationRecord],
) -> Result<(), StoreError> {
    let operation = operations
        .iter()
        .find(|operation| operation.operation_id == source.origin_accepted_operation_id)
        .ok_or_else(ownership_invariant)?;
    let origin_values_match = operation.provenance == source.provenance
        && operation.candidate_tree == source.candidate_tree
        && operation.accept_receipt_id == Some(source.origin_accept_receipt_id);
    if !origin_values_match {
        return Err(ownership_invariant());
    }
    if !matches!(
        audit_accept_receipt(&mut *connection, operation).await?,
        AcceptReceiptAudit::Exact(version) if version == source.origin_accepted_version
    ) {
        return Err(ownership_invariant());
    }
    validate_origin_transition(connection, source).await
}

async fn validate_origin_transition(
    connection: &mut SqliteConnection,
    source: &DeliverySourceRecord,
) -> Result<(), StoreError> {
    let row: Option<(i64, String)> = sqlx::query_as(
        "SELECT transition_id, transitioned_at \
         FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'merge_operation' AND entity_id = ? AND entity_version = ? \
           AND from_state = 'preflight_ready' AND to_state = 'accepted' \
           AND failure_code IS NULL",
    )
    .bind(source.origin_accepted_operation_id.to_string())
    .bind(i64::try_from(source.origin_accepted_version.get()).map_err(|_| ownership_invariant())?)
    .fetch_optional(connection)
    .await?;
    let Some((transition_id, transitioned_at)) = row else {
        return Err(ownership_invariant());
    };
    let transitioned_at: DeliveryTimestamp =
        transitioned_at.parse().map_err(|_| ownership_invariant())?;
    if transition_id < source.initial_transition_id && transitioned_at <= source.created_at {
        Ok(())
    } else {
        Err(ownership_invariant())
    }
}
