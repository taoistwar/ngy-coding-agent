use crate::StoreError;
use crate::delivery::{
    DeliverySourceRecord, DeliveryTimestamp, DeliveryVersion, MergeOperationRecord,
};

use super::super::model::ReconcileDeliverySourceRequest;
use super::super::source_invariant;

pub(super) struct ReconciliationPair<'a> {
    request: &'a ReconcileDeliverySourceRequest,
    source: &'a DeliverySourceRecord,
    operation: &'a MergeOperationRecord,
    source_version: DeliveryVersion,
    merge_version: DeliveryVersion,
    failure_code: &'a str,
    timestamp: DeliveryTimestamp,
}

impl<'a> ReconciliationPair<'a> {
    pub(super) fn new(
        request: &'a ReconcileDeliverySourceRequest,
        source: &'a DeliverySourceRecord,
        operation: &'a MergeOperationRecord,
        source_version: DeliveryVersion,
        merge_version: DeliveryVersion,
        failure_code: &'a str,
        timestamp: DeliveryTimestamp,
    ) -> Self {
        Self {
            request,
            source,
            operation,
            source_version,
            merge_version,
            failure_code,
            timestamp,
        }
    }
}

pub(super) async fn apply_reconciliation_pair(
    connection: &mut sqlx::SqliteConnection,
    pair: ReconciliationPair<'_>,
) -> Result<(), StoreError> {
    if pair.source.state == crate::delivery::DeliverySourceState::Committed {
        update_merge(
            &mut *connection,
            pair.request,
            pair.operation,
            pair.merge_version,
            pair.failure_code,
            pair.timestamp,
        )
        .await?;
        update_source(
            connection,
            pair.request,
            pair.source,
            pair.source_version,
            pair.failure_code,
            pair.timestamp,
        )
        .await?;
    } else {
        update_source(
            &mut *connection,
            pair.request,
            pair.source,
            pair.source_version,
            pair.failure_code,
            pair.timestamp,
        )
        .await?;
        update_merge(
            connection,
            pair.request,
            pair.operation,
            pair.merge_version,
            pair.failure_code,
            pair.timestamp,
        )
        .await?;
    }
    Ok(())
}

async fn update_source(
    connection: &mut sqlx::SqliteConnection,
    request: &ReconcileDeliverySourceRequest,
    source: &DeliverySourceRecord,
    source_version: DeliveryVersion,
    failure_code: &str,
    timestamp: DeliveryTimestamp,
) -> Result<(), StoreError> {
    let source_update = sqlx::query(
        "UPDATE task_delivery_sources \
         SET state = 'reconciliation_required', failure_code = ?, version = ?, updated_at = ? \
         WHERE task_id = ? AND repository_id = ? AND attempt = ? \
           AND state = ? AND version = ?",
    )
    .bind(failure_code)
    .bind(version_i64(source_version)?)
    .bind(timestamp.to_string())
    .bind(request.anchor.task_id.to_string())
    .bind(source.provenance.identity.repository_id().to_string())
    .bind(i64::from(source.provenance.identity.attempt()))
    .bind(request.expected_state.as_str())
    .bind(version_i64(request.expected_source_version)?)
    .execute(&mut *connection)
    .await?;
    if source_update.rows_affected() != 1 {
        return Err(source_invariant());
    }
    Ok(())
}

async fn update_merge(
    connection: &mut sqlx::SqliteConnection,
    request: &ReconcileDeliverySourceRequest,
    operation: &MergeOperationRecord,
    merge_version: DeliveryVersion,
    failure_code: &str,
    timestamp: DeliveryTimestamp,
) -> Result<(), StoreError> {
    let merge_update = sqlx::query(
        "UPDATE task_merge_operations \
         SET state = 'reconciliation_required', failure_code = ?, version = ?, updated_at = ? \
         WHERE operation_id = ? AND task_id = ? AND repository_id = ? AND attempt = ? \
           AND state = 'accepted' AND version = ?",
    )
    .bind(failure_code)
    .bind(version_i64(merge_version)?)
    .bind(timestamp.to_string())
    .bind(request.anchor.accepted_operation_id.to_string())
    .bind(request.anchor.task_id.to_string())
    .bind(operation.provenance.identity.repository_id().to_string())
    .bind(i64::from(operation.provenance.identity.attempt()))
    .bind(version_i64(request.expected_current_merge_version)?)
    .execute(connection)
    .await?;
    if merge_update.rows_affected() != 1 {
        return Err(source_invariant());
    }
    Ok(())
}

fn version_i64(version: DeliveryVersion) -> Result<i64, StoreError> {
    i64::try_from(version.get()).map_err(|_| source_invariant())
}
