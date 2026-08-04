use coding_agent_domain::TaskId;
use sqlx::{Row, SqliteConnection};

use crate::StoreError;
use crate::delivery::ownership::{
    AcceptReceiptAudit, audit_accept_receipt, load_merge_operation_exact, load_source_exact,
};
use crate::delivery::{
    DeliveryOperationId, DeliverySourceRecord, DeliverySourceState, DeliveryTimestamp,
    DeliveryVersion, FailureCode, MergeOperationRecord, MergeOperationState,
};

use super::model::{DeliverySourceAnchor, DeliverySourceTransitionReceipt};
use super::source_invariant;

pub(super) enum TransitionLookup<T> {
    Missing,
    Exact(T),
    Conflict,
}

pub(super) enum AnchorLookup {
    Exact(Box<MergeOperationRecord>),
    Conflict,
}

pub(super) async fn load_source_context(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<Option<DeliverySourceRecord>, StoreError> {
    load_source_exact(connection, task_id)
        .await
        .map_err(map_anchor_invariant)
}

pub(super) async fn lookup_source_transition(
    connection: &mut SqliteConnection,
    task_id: TaskId,
    version: DeliveryVersion,
    from: DeliverySourceState,
    to: DeliverySourceState,
    failure_code: Option<&str>,
) -> Result<TransitionLookup<DeliverySourceTransitionReceipt>, StoreError> {
    let row = transition_row(connection, "delivery_source", &task_id.to_string(), version).await?;
    let Some(row) = row else {
        return Ok(TransitionLookup::Missing);
    };
    let stored_from: String = row.try_get("from_state").map_err(|_| source_invariant())?;
    let stored_to: String = row.try_get("to_state").map_err(|_| source_invariant())?;
    let stored_failure: Option<String> = row
        .try_get("failure_code")
        .map_err(|_| source_invariant())?;
    if stored_from != from.as_str()
        || stored_to != to.as_str()
        || stored_failure.as_deref() != failure_code
    {
        return Ok(TransitionLookup::Conflict);
    }
    let transitioned_at: String = row
        .try_get("transitioned_at")
        .map_err(|_| source_invariant())?;
    Ok(TransitionLookup::Exact(DeliverySourceTransitionReceipt {
        task_id,
        version,
        state: to,
        failure_code: stored_failure
            .map(|value| value.parse::<FailureCode>().map_err(|_| source_invariant()))
            .transpose()?,
        transitioned_at: transitioned_at
            .parse::<DeliveryTimestamp>()
            .map_err(|_| source_invariant())?,
    }))
}

pub(super) struct MergeTransitionReceipt {
    pub(super) operation_id: DeliveryOperationId,
    pub(super) version: DeliveryVersion,
    pub(super) failure_code: FailureCode,
    pub(super) transitioned_at: DeliveryTimestamp,
}

pub(super) async fn lookup_merge_reconciliation_transition(
    connection: &mut SqliteConnection,
    operation_id: DeliveryOperationId,
    version: DeliveryVersion,
    failure_code: &str,
) -> Result<TransitionLookup<MergeTransitionReceipt>, StoreError> {
    let row = transition_row(
        connection,
        "merge_operation",
        &operation_id.to_string(),
        version,
    )
    .await?;
    let Some(row) = row else {
        return Ok(TransitionLookup::Missing);
    };
    let stored_from: String = row.try_get("from_state").map_err(|_| source_invariant())?;
    let stored_to: String = row.try_get("to_state").map_err(|_| source_invariant())?;
    let stored_failure: Option<String> = row
        .try_get("failure_code")
        .map_err(|_| source_invariant())?;
    if stored_from != MergeOperationState::Accepted.as_str()
        || stored_to != MergeOperationState::ReconciliationRequired.as_str()
        || stored_failure.as_deref() != Some(failure_code)
    {
        return Ok(TransitionLookup::Conflict);
    }
    let transitioned_at: String = row
        .try_get("transitioned_at")
        .map_err(|_| source_invariant())?;
    Ok(TransitionLookup::Exact(MergeTransitionReceipt {
        operation_id,
        version,
        failure_code: failure_code.parse().map_err(|_| source_invariant())?,
        transitioned_at: transitioned_at.parse().map_err(|_| source_invariant())?,
    }))
}

async fn transition_row(
    connection: &mut SqliteConnection,
    entity_kind: &str,
    entity_id: &str,
    version: DeliveryVersion,
) -> Result<Option<sqlx::sqlite::SqliteRow>, StoreError> {
    sqlx::query(
        "SELECT from_state, to_state, failure_code, transitioned_at \
         FROM task_delivery_operation_transitions \
         WHERE entity_kind = ? AND entity_id = ? AND entity_version = ?",
    )
    .bind(entity_kind)
    .bind(entity_id)
    .bind(i64::try_from(version.get()).map_err(|_| source_invariant())?)
    .fetch_optional(connection)
    .await
    .map_err(StoreError::from)
}

pub(super) async fn load_accepted_anchor(
    connection: &mut SqliteConnection,
    anchor: DeliverySourceAnchor,
) -> Result<AnchorLookup, StoreError> {
    let operation_exists: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM task_merge_operations WHERE operation_id = ?)",
    )
    .bind(anchor.accepted_operation_id.to_string())
    .fetch_one(&mut *connection)
    .await?;
    if operation_exists != 1 {
        return Ok(AnchorLookup::Conflict);
    }
    let operation = load_merge_operation_exact(connection, anchor.accepted_operation_id)
        .await
        .map_err(map_anchor_invariant)?;
    if operation.operation_id != anchor.accepted_operation_id
        || operation.provenance.identity.task_id() != anchor.task_id
    {
        return Ok(AnchorLookup::Conflict);
    }
    let actual_version = match audit_accept_receipt(connection, &operation).await? {
        AcceptReceiptAudit::MissingPointer => return Ok(AnchorLookup::Conflict),
        AcceptReceiptAudit::Invalid => return Err(source_invariant()),
        AcceptReceiptAudit::Exact(version) => version,
    };
    if actual_version != anchor.accepted_receipt_version {
        return Ok(AnchorLookup::Conflict);
    }
    Ok(AnchorLookup::Exact(Box::new(operation)))
}

fn map_anchor_invariant(error: StoreError) -> StoreError {
    match error {
        StoreError::InvariantViolation(_) => source_invariant(),
        other => other,
    }
}
