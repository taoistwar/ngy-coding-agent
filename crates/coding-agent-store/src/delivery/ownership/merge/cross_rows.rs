use std::str::FromStr;

use sqlx::SqliteConnection;

use crate::StoreError;
use crate::delivery::{
    DeliveryOperationId, DeliverySourceRecord, DeliverySourceState, MergeOperationRecord,
    MergeOperationState, validate_merge_source_state,
};

use super::super::disposition::{load_disposition, validate_merged_disposition_origin};
use super::super::ownership_invariant;
use super::super::source::load_source;
use super::super::transitions::source_state_at;
use super::super::{
    reconciliation_accept_origin_is_exact, validate_source_merge_reconciliation_pair,
};
use super::load::load_merge_operation_local;

pub(super) async fn validate_merge_cross_rows(
    connection: &mut SqliteConnection,
    operation: &MergeOperationRecord,
) -> Result<(), StoreError> {
    let task_id = operation.provenance.identity.task_id();
    let source = load_source(&mut *connection, task_id).await?;
    if let Some(source) = source.as_ref()
        && (source.provenance != operation.provenance
            || source.candidate_tree != operation.candidate_tree)
    {
        return Err(ownership_invariant());
    }
    match (
        operation.delivery_source_task_id,
        operation.source_commit.as_ref(),
    ) {
        (None, None) => {}
        (Some(link), Some(commit)) => {
            let source = source.as_ref().ok_or_else(ownership_invariant)?;
            if link != task_id || source.expected_source_commit.as_ref() != Some(commit) {
                return Err(ownership_invariant());
            }
        }
        _ => return Err(ownership_invariant()),
    }
    let historical_source_state =
        source_state_at(&mut *connection, task_id, operation.current_transition_id).await?;
    validate_merge_source_state(operation.state, historical_source_state)
        .map_err(|_| ownership_invariant())?;
    let source_bound = operation.delivery_source_task_id.is_some();
    if source_bound && historical_source_state != Some(DeliverySourceState::Committed) {
        return Err(ownership_invariant());
    }
    validate_accepted_source_reconciliation(connection, operation, source.as_ref()).await?;
    let current_source_must_be_committed = matches!(
        operation.state,
        MergeOperationState::MergePending
            | MergeOperationState::AbortPending
            | MergeOperationState::Merged
    ) || (source_bound
        && operation.state == MergeOperationState::ReconciliationRequired);
    if current_source_must_be_committed
        && source.as_ref().is_none_or(|source| {
            source.state != DeliverySourceState::Committed || source.failure_code.is_some()
        })
    {
        return Err(ownership_invariant());
    }
    if source_bound
        && source
            .as_ref()
            .is_some_and(|source| source.state == DeliverySourceState::ReconciliationRequired)
    {
        validate_current_source_reconciliation(connection, source.as_ref().unwrap()).await?;
    }
    if operation.state == MergeOperationState::Merged {
        let source = source.as_ref().ok_or_else(ownership_invariant)?;
        let disposition = load_disposition(&mut *connection, task_id)
            .await?
            .ok_or_else(ownership_invariant)?;
        validate_merged_disposition_origin(connection, operation, source, &disposition).await?;
    }
    Ok(())
}

async fn validate_accepted_source_reconciliation(
    connection: &mut SqliteConnection,
    operation: &MergeOperationRecord,
    source: Option<&crate::delivery::DeliverySourceRecord>,
) -> Result<(), StoreError> {
    if operation.state != MergeOperationState::ReconciliationRequired {
        return Ok(());
    }
    let from_state: Option<String> = sqlx::query_scalar(
        "SELECT from_state FROM task_delivery_operation_transitions \
         WHERE transition_id = ? AND entity_kind = 'merge_operation' \
           AND entity_id = ? AND entity_version = ?",
    )
    .bind(operation.current_transition_id)
    .bind(operation.operation_id.to_string())
    .bind(i64::try_from(operation.version.get()).map_err(|_| ownership_invariant())?)
    .fetch_optional(&mut *connection)
    .await?;
    if from_state.as_deref() != Some("accepted") {
        return Ok(());
    }
    let Some(source) = source else {
        return Ok(());
    };
    match source.state {
        DeliverySourceState::ReconciliationRequired => {
            if !reconciliation_accept_origin_is_exact(&mut *connection, operation).await? {
                return Err(ownership_invariant());
            }
            validate_source_merge_reconciliation_pair(connection, source, operation).await
        }
        DeliverySourceState::ObjectPending | DeliverySourceState::CommitPending => {
            Err(ownership_invariant())
        }
        DeliverySourceState::Committed
            if operation.failure_code.as_ref().map(|code| code.as_str())
                == Some("DELIVERY_SOURCE_INCONSISTENT") =>
        {
            Err(ownership_invariant())
        }
        DeliverySourceState::Committed => Ok(()),
    }
}

async fn validate_current_source_reconciliation(
    connection: &mut SqliteConnection,
    source: &DeliverySourceRecord,
) -> Result<(), StoreError> {
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT m.operation_id FROM task_merge_operations m \
         JOIN task_delivery_operation_transitions t \
           ON t.transition_id = ( \
               SELECT MAX(current.transition_id) \
               FROM task_delivery_operation_transitions current \
               WHERE current.entity_kind = 'merge_operation' \
                 AND current.entity_id = m.operation_id \
           ) \
          AND t.entity_kind = 'merge_operation' AND t.entity_id = m.operation_id \
          AND t.entity_version = m.version \
         WHERE m.task_id = ? AND m.state = 'reconciliation_required' \
           AND m.failure_code = ? AND t.from_state = 'accepted' \
           AND t.to_state = 'reconciliation_required'",
    )
    .bind(source.provenance.identity.task_id().to_string())
    .bind(
        source
            .failure_code
            .as_ref()
            .ok_or_else(ownership_invariant)?
            .as_str(),
    )
    .fetch_all(&mut *connection)
    .await?;
    let [operation_id] = rows.as_slice() else {
        return Err(ownership_invariant());
    };
    let operation_id =
        DeliveryOperationId::from_str(operation_id).map_err(|_| ownership_invariant())?;
    let operation = load_merge_operation_local(&mut *connection, operation_id).await?;
    let exact_owner = source.provenance == operation.provenance
        && source.candidate_tree == operation.candidate_tree
        && operation.delivery_source_task_id.is_none()
        && operation.source_commit.is_none();
    if exact_owner && reconciliation_accept_origin_is_exact(&mut *connection, &operation).await? {
        validate_source_merge_reconciliation_pair(connection, source, &operation).await
    } else {
        Err(ownership_invariant())
    }
}
