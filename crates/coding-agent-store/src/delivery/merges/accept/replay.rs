use crate::StoreError;
use crate::delivery::ownership::{
    AcceptReceiptAudit, PreflightReceiptAudit, audit_accept_receipt, audit_preflight_receipt,
    load_merge_operation_exact,
};
use crate::delivery::receipts::lookup_receipt;
use crate::delivery::{AcceptMergeCommandRequest, MergeOperationRecord, MergeOperationState};

use super::super::merge_invariant;
use super::super::model::AcceptMergeOutcome;
use super::super::replay::{OperationLookup, load_operation_for_caller};

pub(super) async fn try_existing_accept(
    connection: &mut sqlx::SqliteConnection,
    request: &AcceptMergeCommandRequest,
) -> Result<Option<AcceptMergeOutcome>, StoreError> {
    let Some(receipt) = lookup_receipt(&mut *connection, request).await? else {
        return Ok(None);
    };
    let operation = load_merge_operation_exact(&mut *connection, receipt.operation_id).await?;
    validate_preflight_origin(&mut *connection, &operation).await?;
    validate_accept_binding(request, &operation)?;
    Ok(Some(AcceptMergeOutcome::Existing(receipt)))
}

pub(super) async fn load_ready_operation(
    connection: &mut sqlx::SqliteConnection,
    request: &AcceptMergeCommandRequest,
) -> Result<Option<MergeOperationRecord>, StoreError> {
    let operation = match load_operation_for_caller(
        &mut *connection,
        request.preflight_operation_id(),
        request.task_id(),
    )
    .await?
    {
        OperationLookup::Exact(operation) => operation,
        OperationLookup::WrongTask | OperationLookup::Missing => return Ok(None),
    };
    validate_preflight_origin(&mut *connection, &operation).await?;
    if request_matches_ready(request, &operation) {
        return Ok(Some(*operation));
    }
    audit_non_ready_accept_state(connection, &operation).await?;
    Ok(None)
}

pub(super) fn validate_accept_binding(
    request: &AcceptMergeCommandRequest,
    operation: &MergeOperationRecord,
) -> Result<(), StoreError> {
    let accepted_version = request.expected_operation_version().next()?;
    let valid = operation.operation_id == request.preflight_operation_id()
        && operation.provenance.identity.task_id() == request.task_id()
        && operation.provenance.evidence.workspace_generation()
            == request.expected_review_generation()
        && operation.provenance.evidence.workspace_fingerprint()
            == request.expected_workspace_fingerprint()
        && operation.target_branch == *request.target_branch()
        && operation.expected_target_head == *request.expected_target_head()
        && operation.accept_receipt_id == Some(request.client_request_id())
        && operation.version.get() >= accepted_version.get()
        && operation.merge_metadata.is_some();
    if valid {
        Ok(())
    } else {
        Err(merge_invariant())
    }
}

fn request_matches_ready(
    request: &AcceptMergeCommandRequest,
    operation: &MergeOperationRecord,
) -> bool {
    operation.state == MergeOperationState::PreflightReady
        && operation.version == request.expected_operation_version()
        && operation.failure_code.is_none()
        && operation.accept_receipt_id.is_none()
        && operation.provenance.identity.task_id() == request.task_id()
        && operation.provenance.evidence.workspace_generation()
            == request.expected_review_generation()
        && operation.provenance.evidence.workspace_fingerprint()
            == request.expected_workspace_fingerprint()
        && operation.target_branch == *request.target_branch()
        && operation.expected_target_head == *request.expected_target_head()
}

async fn validate_preflight_origin(
    connection: &mut sqlx::SqliteConnection,
    operation: &MergeOperationRecord,
) -> Result<(), StoreError> {
    if audit_preflight_receipt(connection, operation).await? == PreflightReceiptAudit::Exact {
        Ok(())
    } else {
        Err(merge_invariant())
    }
}

async fn audit_non_ready_accept_state(
    connection: &mut sqlx::SqliteConnection,
    operation: &MergeOperationRecord,
) -> Result<(), StoreError> {
    if operation.accept_receipt_id.is_some()
        && !matches!(
            audit_accept_receipt(connection, operation).await?,
            AcceptReceiptAudit::Exact(_)
        )
    {
        return Err(merge_invariant());
    }
    Ok(())
}
