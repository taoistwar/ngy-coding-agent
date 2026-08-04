use std::str::FromStr;

use coding_agent_domain::ClientRequestId;
use sqlx::SqliteConnection;

use crate::StoreError;
use crate::delivery::receipts::lookup_receipt;
use crate::delivery::{
    DeliveryAcceptedOperationState, DeliveryVersion, MergeOperationRecord, PreflightCommandRequest,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::delivery) enum PreflightReceiptAudit {
    Invalid,
    Exact,
}

pub(in crate::delivery) async fn audit_preflight_receipt(
    connection: &mut SqliteConnection,
    operation: &MergeOperationRecord,
) -> Result<PreflightReceiptAudit, StoreError> {
    let Ok(client_request_id) =
        ClientRequestId::from_str(&operation.preflight_receipt_id.to_string())
    else {
        return Ok(PreflightReceiptAudit::Invalid);
    };
    let Ok(command) = PreflightCommandRequest::try_new(
        client_request_id,
        operation.provenance.identity.task_id(),
        operation.target_branch.clone(),
        operation.expected_target_head.clone(),
    ) else {
        return Ok(PreflightReceiptAudit::Invalid);
    };
    let receipt = match lookup_receipt(connection, &command).await {
        Ok(Some(receipt)) => receipt,
        Ok(None)
        | Err(StoreError::IdempotencyConflict)
        | Err(StoreError::InvariantViolation(_)) => return Ok(PreflightReceiptAudit::Invalid),
        Err(error @ StoreError::Database(_)) => return Err(error),
        Err(_) => return Ok(PreflightReceiptAudit::Invalid),
    };
    let transition: Option<(i64, String)> = sqlx::query_as(
        "SELECT transition_id, transitioned_at \
         FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'merge_operation' AND entity_id = ? AND entity_version = 1 \
           AND from_state = 'absent' AND to_state = 'preflight_pending' \
           AND failure_code IS NULL",
    )
    .bind(operation.operation_id.to_string())
    .fetch_optional(&mut *connection)
    .await?;
    let exact_transition = transition.is_some_and(|(transition_id, transitioned_at)| {
        transition_id == operation.initial_transition_id
            && transitioned_at == operation.created_at.to_string()
            && transitioned_at == receipt.created_at.to_string()
    });
    let exact = receipt.client_request_id == operation.preflight_receipt_id
        && receipt.operation_id == operation.operation_id
        && receipt.identity == operation.provenance.identity
        && receipt.accepted_operation_version == DeliveryVersion::initial()
        && receipt.accepted_operation_state == DeliveryAcceptedOperationState::PreflightPending
        && exact_transition;
    Ok(if exact {
        PreflightReceiptAudit::Exact
    } else {
        PreflightReceiptAudit::Invalid
    })
}
