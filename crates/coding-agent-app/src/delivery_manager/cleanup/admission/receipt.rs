use super::*;

pub(super) async fn inspect_cleanup_receipt(
    dependencies: &DeliveryManagerLiveDependencies,
    command: &CleanupCommand,
) -> Result<CleanupReceiptStatus, DeliveryCleanupAcceptanceOutcome> {
    let lookup = timeout(
        STORE_READ_TIMEOUT,
        dependencies
            .store
            .lookup_delivery_command(&command.as_store_command()),
    )
    .await;
    let receipt = match lookup {
        Ok(Ok(DeliveryCommandLookup::Missing)) => return Ok(CleanupReceiptStatus::Missing),
        Ok(Ok(DeliveryCommandLookup::Existing(receipt))) => receipt,
        Ok(Err(StoreError::IdempotencyConflict)) => {
            return Err(DeliveryCleanupAcceptanceOutcome::Conflict(
                DeliveryCommandConflict::IdempotencyConflict,
            ));
        }
        Ok(Err(_)) | Err(_) => {
            return Err(DeliveryCleanupAcceptanceOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::StoreUnavailable,
            ));
        }
    };
    let context = load_cleanup_operation_context(dependencies, receipt.operation_id)
        .await
        .map_err(|_| inconsistent_cleanup_outcome())?;
    let receipt_state_is_exact = match command {
        CleanupCommand::RemoveWorktree(_) => matches!(
            receipt.accepted_operation_state,
            DeliveryAcceptedOperationState::UnlockPending
                | DeliveryAcceptedOperationState::RemovePending
        ),
        CleanupCommand::DeleteBranch(_) => {
            receipt.accepted_operation_state == DeliveryAcceptedOperationState::DeletePending
        }
    };
    let command_is_exact = match command {
        CleanupCommand::RemoveWorktree(command) => {
            context.operation.kind == CleanupKind::RemoveWorktree
                && context.operation.origin_receipt_id == command.client_request_id()
                && context.operation.expected_source_ref == *command.expected_source_ref()
                && context.operation.expected_source_oid == *command.expected_source_oid()
                && context.disposition.merged_operation_id == command.expected_merge_operation_id()
        }
        CleanupCommand::DeleteBranch(command) => {
            context.operation.kind == CleanupKind::DeleteBranch
                && context.operation.origin_receipt_id == command.client_request_id()
                && context.operation.expected_source_ref == *command.expected_source_ref()
                && context.operation.expected_source_oid == *command.expected_source_oid()
                && context.operation.expected_target_ref.as_ref() == Some(command.target_branch())
                && context.operation.origin_target_head.as_ref() == Some(command.target_head())
                && context.disposition.merged_operation_id == command.expected_merge_operation_id()
        }
    };
    if receipt.identity.task_id() != command.task_id()
        || receipt.client_request_id
            != match command {
                CleanupCommand::RemoveWorktree(command) => command.client_request_id(),
                CleanupCommand::DeleteBranch(command) => command.client_request_id(),
            }
        || context.operation.operation_id != receipt.operation_id
        || context.operation.identity != receipt.identity
        || context.operation.version < receipt.accepted_operation_version
        || !receipt_state_is_exact
        || !command_is_exact
    {
        return Err(inconsistent_cleanup_outcome());
    }
    Ok(CleanupReceiptStatus::Existing { receipt, context })
}

pub(super) fn cleanup_known_not_applied(
    reason: crate::KnownNotAppliedReason,
    error: Option<StoreError>,
) -> DeliveryCleanupAcceptanceOutcome {
    match error {
        Some(StoreError::IdempotencyConflict) => {
            DeliveryCleanupAcceptanceOutcome::Conflict(DeliveryCommandConflict::IdempotencyConflict)
        }
        Some(StoreError::DeliveryOperationInProgress) => {
            DeliveryCleanupAcceptanceOutcome::Conflict(DeliveryCommandConflict::OperationInProgress)
        }
        Some(StoreError::TaskNotFound) => DeliveryCleanupAcceptanceOutcome::Ineligible(vec![
            DeliveryEligibilityReason::TaskNotFound,
        ]),
        Some(StoreError::TaskNotMergeEligible) => DeliveryCleanupAcceptanceOutcome::Conflict(
            DeliveryCommandConflict::ArtifactCleanupNotAllowed,
        ),
        Some(StoreError::DeliveryReconciliationRequired) => {
            DeliveryCleanupAcceptanceOutcome::Ineligible(vec![
                DeliveryEligibilityReason::ReconciliationRequired,
            ])
        }
        _ if reason == crate::KnownNotAppliedReason::DeadlineBeforeStart => {
            DeliveryCleanupAcceptanceOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::CommandTimedOut,
            )
        }
        _ => DeliveryCleanupAcceptanceOutcome::Unavailable(
            DeliveryPreflightUnavailableReason::StoreUnavailable,
        ),
    }
}

pub(super) fn durable_acceptance(
    receipt: &DeliveryCommandReceipt,
    kind: CleanupKind,
    disposition: DeliveryCleanupReceiptDisposition,
) -> DeliveryCleanupAcceptanceOutcome {
    let (cleanup_kind, accepted_state) = match (kind, receipt.accepted_operation_state) {
        (CleanupKind::RemoveWorktree, DeliveryAcceptedOperationState::UnlockPending) => (
            DeliveryCleanupOperationKind::RemoveWorktree,
            DeliveryCleanupOperationState::UnlockPending,
        ),
        (CleanupKind::RemoveWorktree, DeliveryAcceptedOperationState::RemovePending) => (
            DeliveryCleanupOperationKind::RemoveWorktree,
            DeliveryCleanupOperationState::RemovePending,
        ),
        (CleanupKind::DeleteBranch, DeliveryAcceptedOperationState::DeletePending) => (
            DeliveryCleanupOperationKind::DeleteBranch,
            DeliveryCleanupOperationState::DeletePending,
        ),
        _ => return inconsistent_cleanup_outcome(),
    };
    DeliveryCleanupAcceptanceOutcome::Durable(DeliveryCleanupAcceptance::new(
        receipt.operation_id,
        receipt.accepted_operation_version,
        cleanup_kind,
        accepted_state,
        disposition,
    ))
}
