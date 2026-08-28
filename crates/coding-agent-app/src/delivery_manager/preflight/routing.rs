use std::time::Duration;

use coding_agent_domain::{RepositoryId, TaskId};
use coding_agent_store::{
    DeliveryCommand, DeliveryCommandLookup, DeliveryCommandReceipt, DeliveryEligibilitySnapshot,
    DeliveryVersion, MergeOperationRecord, MergeOperationState, PreflightCommandRequest,
    StoreError,
};
use tokio::time::timeout;

use crate::delivery_api_projection::{
    DeliveryCommandConflict, DeliveryPreflightDurability, DeliveryPreflightOutcome,
    DeliveryPreflightUnavailableReason,
};
use crate::delivery_manager::DeliveryManagerLiveDependencies;
use crate::{RepositoryControlCoordinator, RepositoryControlLease, RepositoryControlPoisonReason};

use super::admission::{
    PreflightAttemptResult, finish_terminal_receipt, inconsistent_outcome, poison_and_release,
};
use super::persist::durable_operation;

const STORE_READ_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub(super) enum ReceiptStatus {
    Missing,
    Resume(DeliveryCommandReceipt),
    Terminal(DeliveryPreflightOutcome),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PendingShape {
    UnboundV1,
    PreparedV2,
}

pub(super) struct RoutedPreflight {
    pub(super) command: PreflightCommandRequest,
    pub(super) snapshot: DeliveryEligibilitySnapshot,
    pub(super) receipt: Option<DeliveryCommandReceipt>,
    pub(super) operation: Option<MergeOperationRecord>,
    pub(super) lease: RepositoryControlLease,
}

pub(super) async fn load_snapshot(
    dependencies: &DeliveryManagerLiveDependencies,
    task_id: TaskId,
) -> Result<DeliveryEligibilitySnapshot, DeliveryPreflightOutcome> {
    match timeout(
        STORE_READ_TIMEOUT,
        dependencies.store.delivery_eligibility_snapshot(task_id),
    )
    .await
    {
        Ok(Ok(Some(snapshot))) => Ok(snapshot),
        Ok(Ok(None)) => Err(DeliveryPreflightOutcome::Ineligible(vec![
            crate::DeliveryEligibilityReason::TaskNotFound,
        ])),
        Ok(Err(_)) | Err(_) => Err(DeliveryPreflightOutcome::Unavailable(
            DeliveryPreflightUnavailableReason::StoreUnavailable,
        )),
    }
}

pub(super) async fn inspect_receipt_status(
    dependencies: &DeliveryManagerLiveDependencies,
    repository_control: &RepositoryControlCoordinator,
    command: &PreflightCommandRequest,
) -> Result<ReceiptStatus, DeliveryPreflightOutcome> {
    let lookup = timeout(
        STORE_READ_TIMEOUT,
        dependencies
            .store
            .lookup_delivery_command(&DeliveryCommand::Preflight(command.clone())),
    )
    .await;
    let receipt = match lookup {
        Ok(Ok(DeliveryCommandLookup::Missing)) => return Ok(ReceiptStatus::Missing),
        Ok(Ok(DeliveryCommandLookup::Existing(receipt))) => receipt,
        Ok(Err(StoreError::IdempotencyConflict)) => {
            return Err(DeliveryPreflightOutcome::Conflict(
                DeliveryCommandConflict::IdempotencyConflict,
            ));
        }
        Ok(Err(_)) | Err(_) => {
            return Err(DeliveryPreflightOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::StoreUnavailable,
            ));
        }
    };
    if receipt.identity.task_id() != command.task_id()
        || receipt.client_request_id != command.client_request_id()
    {
        poison_receipt_repository(repository_control, &receipt);
        return Err(inconsistent_outcome());
    }
    let snapshot = match load_snapshot(dependencies, command.task_id()).await {
        Ok(snapshot) => snapshot,
        Err(DeliveryPreflightOutcome::Ineligible(_)) => {
            poison_receipt_repository(repository_control, &receipt);
            return Err(inconsistent_outcome());
        }
        Err(outcome) => return Err(outcome),
    };
    if receipt.identity.repository_id() != snapshot.task.repository_id
        || receipt.identity.attempt() != snapshot.task.attempt
    {
        poison_receipt_repository(repository_control, &receipt);
        if let Ok(key) = repository_control.delivery_coordination_key(snapshot.task.repository_id) {
            let _ = repository_control.require_reconciliation(
                key,
                RepositoryControlPoisonReason::SideEffectIdentityMismatch,
            );
        }
        return Err(inconsistent_outcome());
    }
    let Some(operation) = exact_receipt_operation(&snapshot, &receipt) else {
        poison_receipt_repository(repository_control, &receipt);
        return Err(inconsistent_outcome());
    };
    if operation.state == MergeOperationState::PreflightPending {
        if pending_shape(operation).is_none() {
            poison_receipt_repository(repository_control, &receipt);
            return Err(inconsistent_outcome());
        }
        Ok(ReceiptStatus::Resume(receipt))
    } else {
        if operation.state == MergeOperationState::ReconciliationRequired {
            poison_receipt_repository(repository_control, &receipt);
        }
        Ok(ReceiptStatus::Terminal(durable_operation(
            receipt.operation_id,
            DeliveryPreflightDurability::Existing,
            operation.state,
        )))
    }
}

fn poison_receipt_repository(
    repository_control: &RepositoryControlCoordinator,
    receipt: &DeliveryCommandReceipt,
) {
    if let Ok(key) = repository_control.delivery_coordination_key(receipt.identity.repository_id())
    {
        let _ = repository_control.require_reconciliation(
            key,
            RepositoryControlPoisonReason::SideEffectIdentityMismatch,
        );
    }
}

pub(super) fn exact_receipt_operation<'a>(
    snapshot: &'a DeliveryEligibilitySnapshot,
    receipt: &DeliveryCommandReceipt,
) -> Option<&'a MergeOperationRecord> {
    snapshot
        .ownership
        .merge_operations
        .iter()
        .find(|operation| {
            operation.operation_id == receipt.operation_id
                && operation.preflight_receipt_id == receipt.client_request_id
                && operation.provenance.identity == receipt.identity
        })
}

pub(super) fn pending_shape(operation: &MergeOperationRecord) -> Option<PendingShape> {
    if operation.state != MergeOperationState::PreflightPending {
        return None;
    }
    if operation.version == DeliveryVersion::initial() && operation.preflight_inputs.is_none() {
        return Some(PendingShape::UnboundV1);
    }
    let prepared_version = DeliveryVersion::try_new(2).ok()?;
    if operation.version == prepared_version && operation.preflight_inputs.is_some() {
        return Some(PendingShape::PreparedV2);
    }
    None
}

pub(super) fn snapshot_allows_new_preflight(snapshot: &DeliveryEligibilitySnapshot) -> bool {
    snapshot
        .ownership
        .merge_operations
        .iter()
        .max_by_key(|operation| operation.initial_transition_id)
        .is_some_and(|operation| {
            matches!(
                operation.state,
                MergeOperationState::Conflict
                    | MergeOperationState::Rejected
                    | MergeOperationState::Stale
                    | MergeOperationState::Superseded
                    | MergeOperationState::Failed
            )
        })
}

pub(super) async fn refresh_under_lease(
    dependencies: &DeliveryManagerLiveDependencies,
    repository_control: &RepositoryControlCoordinator,
    command: PreflightCommandRequest,
    routing_repository_id: RepositoryId,
    lease: RepositoryControlLease,
) -> Result<RoutedPreflight, PreflightAttemptResult> {
    let status = match inspect_receipt_status(dependencies, repository_control, &command).await {
        Ok(status) => status,
        Err(outcome) => return Err(poison_and_release(lease, outcome)),
    };
    if let ReceiptStatus::Terminal(outcome) = &status {
        return Err(finish_terminal_receipt(lease, outcome.clone()));
    }

    let snapshot = match load_snapshot(dependencies, command.task_id()).await {
        Ok(snapshot) => snapshot,
        Err(outcome) => return Err(poison_and_release(lease, outcome)),
    };
    let fresh_key = repository_control.delivery_coordination_key(snapshot.task.repository_id);
    if snapshot.task.repository_id != routing_repository_id
        || fresh_key != Ok(lease.coordination_key())
    {
        return Err(poison_and_release(lease, inconsistent_outcome()));
    }

    let receipt = match status {
        ReceiptStatus::Missing => None,
        ReceiptStatus::Resume(receipt) => Some(receipt),
        ReceiptStatus::Terminal(_) => unreachable!("terminal receipt returned above"),
    };
    let operation = receipt
        .as_ref()
        .and_then(|receipt| exact_receipt_operation(&snapshot, receipt))
        .cloned();
    if receipt.is_some() && operation.is_none() {
        return Err(poison_and_release(lease, inconsistent_outcome()));
    }

    Ok(RoutedPreflight {
        command,
        snapshot,
        receipt,
        operation,
        lease,
    })
}
