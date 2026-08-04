use std::str::FromStr;

use coding_agent_domain::TaskId;
use sqlx::Row;

use crate::StoreError;
use crate::delivery::{
    DeliveryOperationId, DeliveryVersion, FailureCode, MergeOperationRecord, MergeOperationState,
};

use super::merge_invariant;
use super::model::MergeTransitionReceipt;

pub(super) enum TransitionLookup {
    Exact(MergeTransitionReceipt),
    Missing,
    Conflict,
}

pub(in crate::delivery) enum OperationLookup {
    Exact(Box<MergeOperationRecord>),
    WrongTask,
    Missing,
}

pub(in crate::delivery) async fn load_operation_for_caller(
    connection: &mut sqlx::SqliteConnection,
    operation_id: DeliveryOperationId,
    task_id: TaskId,
) -> Result<OperationLookup, StoreError> {
    let stored_task: Option<String> =
        sqlx::query_scalar("SELECT task_id FROM task_merge_operations WHERE operation_id = ?")
            .bind(operation_id.to_string())
            .fetch_optional(&mut *connection)
            .await?;
    let Some(_stored_task) = stored_task else {
        audit_no_orphan_evidence(connection, operation_id).await?;
        return Ok(OperationLookup::Missing);
    };
    let operation =
        crate::delivery::ownership::load_merge_operation_exact(connection, operation_id).await?;
    if operation.provenance.identity.task_id() != task_id {
        return Ok(OperationLookup::WrongTask);
    }
    Ok(OperationLookup::Exact(Box::new(operation)))
}

async fn audit_no_orphan_evidence(
    connection: &mut sqlx::SqliteConnection,
    operation_id: DeliveryOperationId,
) -> Result<(), StoreError> {
    let present: i64 = sqlx::query_scalar(
        "SELECT EXISTS( \
             SELECT 1 FROM task_delivery_operation_transitions \
              WHERE entity_kind = 'merge_operation' AND entity_id = ? \
             UNION ALL SELECT 1 FROM task_merge_conflicts WHERE operation_id = ? \
             UNION ALL SELECT 1 FROM task_delivery_command_receipts \
              WHERE operation_id = ? OR merge_operation_id = ? \
             UNION ALL SELECT 1 FROM task_artifact_dispositions WHERE merged_operation_id = ? \
         )",
    )
    .bind(operation_id.to_string())
    .bind(operation_id.to_string())
    .bind(operation_id.to_string())
    .bind(operation_id.to_string())
    .bind(operation_id.to_string())
    .fetch_one(connection)
    .await?;
    if present == 0 {
        Ok(())
    } else {
        Err(merge_invariant())
    }
}

pub(super) async fn lookup_transition(
    connection: &mut sqlx::SqliteConnection,
    operation_id: DeliveryOperationId,
    version: DeliveryVersion,
    from: MergeOperationState,
    to: MergeOperationState,
    failure_code: Option<&str>,
) -> Result<TransitionLookup, StoreError> {
    let row = sqlx::query(
        "SELECT entity_version, from_state, to_state, failure_code, transitioned_at \
         FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'merge_operation' AND entity_id = ? AND entity_version = ?",
    )
    .bind(operation_id.to_string())
    .bind(version_i64(version)?)
    .fetch_optional(connection)
    .await?;
    let Some(row) = row else {
        return Ok(TransitionLookup::Missing);
    };
    let actual_version = DeliveryVersion::try_new(
        u64::try_from(
            row.try_get::<i64, _>("entity_version")
                .map_err(|_| merge_invariant())?,
        )
        .map_err(|_| merge_invariant())?,
    )
    .map_err(|_| merge_invariant())?;
    let actual_from: String = row.try_get("from_state").map_err(|_| merge_invariant())?;
    let actual_to: String = row.try_get("to_state").map_err(|_| merge_invariant())?;
    let actual_failure: Option<String> =
        row.try_get("failure_code").map_err(|_| merge_invariant())?;
    if actual_version != version
        || actual_from != from.as_str()
        || actual_to != to.as_str()
        || actual_failure.as_deref() != failure_code
    {
        return Ok(TransitionLookup::Conflict);
    }
    Ok(TransitionLookup::Exact(MergeTransitionReceipt {
        operation_id,
        version,
        state: to,
        failure_code: actual_failure
            .map(|value| FailureCode::from_str(&value).map_err(|_| merge_invariant()))
            .transpose()?,
        transitioned_at: row
            .try_get::<String, _>("transitioned_at")
            .map_err(|_| merge_invariant())?
            .parse()
            .map_err(|_| merge_invariant())?,
    }))
}

pub(super) fn version_i64(version: DeliveryVersion) -> Result<i64, StoreError> {
    i64::try_from(version.get()).map_err(|_| merge_invariant())
}
