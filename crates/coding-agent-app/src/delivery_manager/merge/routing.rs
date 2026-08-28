use super::admission::AcceptAdmission;
use super::*;

pub(super) enum AcceptReceiptStatus {
    Missing,
    Existing {
        receipt: DeliveryCommandReceipt,
        context: Box<DeliveryRecoveryContext>,
    },
}

pub(super) struct RoutedAccept {
    pub(super) admission: AcceptAdmission,
    pub(super) context: DeliveryRecoveryContext,
}

pub(super) async fn refresh(
    flow: &AcceptFlow,
    admission: AcceptAdmission,
) -> Result<RoutedAccept, WorkerFinish> {
    let context = match load_accept_context(flow.dependencies.as_ref(), &flow.command).await {
        Ok(context) => context,
        Err(outcome @ DeliveryMergeAcceptanceOutcome::Conflict(_)) => {
            return Err(admission.clean(&flow.response, outcome));
        }
        Err(outcome) => return Err(admission.poison(&flow.response, outcome)),
    };
    if context.snapshot.task.repository_id != admission.routing_repository
        || flow
            .repository_control
            .delivery_coordination_key(admission.routing_repository)
            != Ok(admission.lease.coordination_key())
    {
        return Err(admission.poison(&flow.response, inconsistent_accept_outcome()));
    }

    match timeout(
        LIVE_ORCHESTRATION_TIMEOUT,
        flow.dependencies
            .task_ownership
            .active_ownership(flow.command.task_id()),
    )
    .await
    {
        Ok(Ok(TaskActiveOwnership::Inactive)) => {}
        Ok(Ok(TaskActiveOwnership::Active {
            repository_id,
            attempt,
        })) if repository_id == context.snapshot.task.repository_id
            && attempt == context.snapshot.task.attempt =>
        {
            return Err(admission.clean(
                &flow.response,
                DeliveryMergeAcceptanceOutcome::Ineligible(vec![
                    crate::DeliveryEligibilityReason::TaskActive,
                ]),
            ));
        }
        Ok(Ok(TaskActiveOwnership::Active { .. })) | Ok(Err(_)) | Err(_) => {
            return Err(admission.poison(&flow.response, inconsistent_accept_outcome()));
        }
    }
    match timeout(
        LIVE_ORCHESTRATION_TIMEOUT,
        flow.dependencies
            .process_proofs
            .observe(flow.command.task_id()),
    )
    .await
    {
        Ok(Ok(super::super::DeliveryProcessProof::Clean)) => {}
        Ok(Ok(super::super::DeliveryProcessProof::Active)) => {
            return Err(admission.clean(
                &flow.response,
                DeliveryMergeAcceptanceOutcome::Ineligible(vec![
                    crate::DeliveryEligibilityReason::TaskActive,
                ]),
            ));
        }
        Ok(Ok(super::super::DeliveryProcessProof::CleanupUnproven)) | Ok(Err(_)) | Err(_) => {
            return Err(admission.retain(
                &flow.response,
                DeliveryMergeAcceptanceOutcome::Unavailable(
                    DeliveryPreflightUnavailableReason::ProcessProofUnavailable,
                ),
            ));
        }
    }

    let receipt_status =
        match inspect_accept_receipt(flow.dependencies.as_ref(), &flow.command).await {
            Ok(status) => status,
            Err(outcome @ DeliveryMergeAcceptanceOutcome::Conflict(_)) => {
                return Err(admission.clean(&flow.response, outcome));
            }
            Err(outcome) => return Err(admission.poison(&flow.response, outcome)),
        };
    if let AcceptReceiptStatus::Existing { receipt, context } = receipt_status {
        let outcome = durable_acceptance(&receipt, DeliveryMergeReceiptDisposition::Existing);
        send_accept_response(&flow.response, outcome.clone());
        let stage = drive_pipeline(flow.dependencies.as_ref(), *context).await;
        return Err(admission.finish(stage).with_accept_fallback(outcome));
    }

    Ok(RoutedAccept { admission, context })
}

pub(super) async fn inspect_accept_receipt(
    dependencies: &DeliveryManagerLiveDependencies,
    command: &AcceptMergeCommandRequest,
) -> Result<AcceptReceiptStatus, DeliveryMergeAcceptanceOutcome> {
    let lookup = timeout(
        STORE_READ_TIMEOUT,
        dependencies
            .store
            .lookup_delivery_command(&DeliveryCommand::AcceptMerge(command.clone())),
    )
    .await;
    let receipt = match lookup {
        Ok(Ok(DeliveryCommandLookup::Missing)) => return Ok(AcceptReceiptStatus::Missing),
        Ok(Ok(DeliveryCommandLookup::Existing(receipt))) => receipt,
        Ok(Err(StoreError::IdempotencyConflict)) => {
            return Err(DeliveryMergeAcceptanceOutcome::Conflict(
                DeliveryCommandConflict::IdempotencyConflict,
            ));
        }
        Ok(Err(_)) | Err(_) => {
            return Err(DeliveryMergeAcceptanceOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::StoreUnavailable,
            ));
        }
    };
    let context = load_operation_context(dependencies, receipt.operation_id)
        .await
        .map_err(|_| inconsistent_accept_outcome())?;
    if receipt.identity.task_id() != command.task_id()
        || receipt.client_request_id != command.client_request_id()
        || receipt.accepted_operation_state != DeliveryAcceptedOperationState::Accepted
        || context.operation.operation_id != command.preflight_operation_id()
        || context.operation.accept_receipt_id != Some(receipt.client_request_id)
        || context.operation.provenance.identity != receipt.identity
        || context.operation.version < receipt.accepted_operation_version
    {
        return Err(inconsistent_accept_outcome());
    }
    Ok(AcceptReceiptStatus::Existing {
        receipt,
        context: Box::new(context),
    })
}

pub(super) async fn load_accept_context(
    dependencies: &DeliveryManagerLiveDependencies,
    command: &AcceptMergeCommandRequest,
) -> Result<DeliveryRecoveryContext, DeliveryMergeAcceptanceOutcome> {
    let snapshot = match timeout(
        STORE_READ_TIMEOUT,
        dependencies
            .store
            .delivery_eligibility_snapshot(command.task_id()),
    )
    .await
    {
        Ok(Ok(Some(snapshot))) => snapshot,
        Ok(Ok(None)) => {
            return Err(DeliveryMergeAcceptanceOutcome::Ineligible(vec![
                crate::DeliveryEligibilityReason::TaskNotFound,
            ]));
        }
        Ok(Err(_)) | Err(_) => {
            return Err(DeliveryMergeAcceptanceOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::StoreUnavailable,
            ));
        }
    };
    let operation = snapshot
        .ownership
        .merge_operations
        .iter()
        .find(|operation| operation.operation_id == command.preflight_operation_id())
        .cloned()
        .ok_or(DeliveryMergeAcceptanceOutcome::Conflict(
            DeliveryCommandConflict::PreflightStale,
        ))?;
    let source = snapshot.ownership.source.clone();
    Ok(DeliveryRecoveryContext {
        snapshot,
        operation,
        source,
    })
}
