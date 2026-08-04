use std::str::FromStr;

use coding_agent_domain::ClientRequestId;
use sqlx::SqliteConnection;

use crate::StoreError;
use crate::delivery::receipts::lookup_receipt;
use crate::delivery::{
    AcceptMergeCommandRequest, DeliveryAcceptedOperationState, DeliveryVersion,
    MergeOperationRecord, MergeOperationState,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::delivery) enum AcceptReceiptAudit {
    MissingPointer,
    Invalid,
    Exact(DeliveryVersion),
}

pub(in crate::delivery) async fn audit_accept_receipt(
    connection: &mut SqliteConnection,
    operation: &MergeOperationRecord,
) -> Result<AcceptReceiptAudit, StoreError> {
    let Some(accept_receipt_id) = operation.accept_receipt_id else {
        return Ok(AcceptReceiptAudit::MissingPointer);
    };
    let stored_version: Option<i64> = sqlx::query_scalar(
        "SELECT accepted_operation_version FROM task_delivery_command_receipts \
         WHERE client_request_id = ?",
    )
    .bind(accept_receipt_id.to_string())
    .fetch_optional(&mut *connection)
    .await?;
    let Some(stored_version) = stored_version else {
        return Ok(AcceptReceiptAudit::Invalid);
    };
    let Ok(stored_version) = u64::try_from(stored_version) else {
        return Ok(AcceptReceiptAudit::Invalid);
    };
    let Ok(accepted_version) = DeliveryVersion::try_new(stored_version) else {
        return Ok(AcceptReceiptAudit::Invalid);
    };
    let Some(previous_version) = accepted_version.get().checked_sub(1) else {
        return Ok(AcceptReceiptAudit::Invalid);
    };
    let Ok(previous_version) = DeliveryVersion::try_new(previous_version) else {
        return Ok(AcceptReceiptAudit::Invalid);
    };
    let Ok(client_request_id) = ClientRequestId::from_str(&accept_receipt_id.to_string()) else {
        return Ok(AcceptReceiptAudit::Invalid);
    };
    let Ok(command) = AcceptMergeCommandRequest::try_new(
        client_request_id,
        operation.provenance.identity.task_id(),
        operation.operation_id,
        previous_version,
        operation.provenance.evidence.workspace_generation(),
        operation
            .provenance
            .evidence
            .workspace_fingerprint()
            .clone(),
        operation.target_branch.clone(),
        operation.expected_target_head.clone(),
    ) else {
        return Ok(AcceptReceiptAudit::Invalid);
    };
    let receipt = match lookup_receipt(connection, &command).await {
        Ok(Some(receipt)) => receipt,
        Ok(None)
        | Err(StoreError::IdempotencyConflict)
        | Err(StoreError::InvariantViolation(_)) => {
            return Ok(AcceptReceiptAudit::Invalid);
        }
        Err(error @ StoreError::Database(_)) => return Err(error),
        Err(_) => return Ok(AcceptReceiptAudit::Invalid),
    };
    let exact = receipt.operation_id == operation.operation_id
        && receipt.identity == operation.provenance.identity
        && receipt.accepted_operation_version == accepted_version
        && receipt.accepted_operation_state == DeliveryAcceptedOperationState::Accepted
        && receipt.client_request_id == accept_receipt_id;
    if exact {
        Ok(AcceptReceiptAudit::Exact(accepted_version))
    } else {
        Ok(AcceptReceiptAudit::Invalid)
    }
}

pub(in crate::delivery) async fn reconciliation_accept_origin_is_exact(
    connection: &mut SqliteConnection,
    operation: &MergeOperationRecord,
) -> Result<bool, StoreError> {
    if operation.state != MergeOperationState::ReconciliationRequired {
        return Ok(false);
    }
    let Ok(current_version) = i64::try_from(operation.version.get()) else {
        return Ok(false);
    };
    let transition: Option<(i64, String, String)> = sqlx::query_as(
        "SELECT entity_version, from_state, to_state \
         FROM task_delivery_operation_transitions \
         WHERE transition_id = ? AND entity_kind = 'merge_operation' AND entity_id = ?",
    )
    .bind(operation.current_transition_id)
    .bind(operation.operation_id.to_string())
    .fetch_optional(&mut *connection)
    .await?;
    let origin_is_accepted = transition.is_some_and(|(version, from, to)| {
        version == current_version
            && from == MergeOperationState::Accepted.as_str()
            && to == MergeOperationState::ReconciliationRequired.as_str()
    });
    if !origin_is_accepted {
        return Ok(false);
    }
    let expected_accepted_version = operation
        .version
        .get()
        .checked_sub(1)
        .and_then(|version| DeliveryVersion::try_new(version).ok());
    Ok(matches!(
        (audit_accept_receipt(connection, operation).await?, expected_accepted_version),
        (AcceptReceiptAudit::Exact(actual), Some(expected)) if actual == expected
    ))
}
