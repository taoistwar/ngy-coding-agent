use super::routing::{AcceptReceiptStatus, RoutedAccept, inspect_accept_receipt};
use super::runtime::AuthenticatedAccept;
use super::validation::ValidatedAccept;
use super::*;

pub(super) async fn persist_stale_authentication(
    flow: &AcceptFlow,
    validated: ValidatedAccept,
    reason: PreflightStaleReason,
) -> WorkerFinish {
    let ValidatedAccept { routed } = validated;
    let RoutedAccept { admission, context } = routed;
    let expected_operation_id = context.operation.operation_id;
    let expected_version = match context.operation.version.next() {
        Ok(version) => version,
        Err(_) => return admission.poison(&flow.response, inconsistent_accept_outcome()),
    };
    let request = match MarkPreflightStaleRequest::try_new(
        context.snapshot.task.id,
        expected_operation_id,
        context.operation.version,
        reason,
    ) {
        Ok(request) => request,
        Err(_) => return admission.poison(&flow.response, inconsistent_accept_outcome()),
    };
    let write = DeliveryWriteCommand::Merge(DeliveryMergeWriteCommand::MarkPreflightStale(request));
    match execute_exact_delivery_write(&flow.dependencies.writer, write).await {
        ExactDeliveryWriteResult::Confirmed(DeliveryWriteOutcome::Merge(
            DeliveryMergeWriteOutcome::MarkPreflightStale(
                MarkPreflightStaleOutcome::Applied {
                    operation_id,
                    version,
                    state,
                    reason: persisted_reason,
                }
                | MarkPreflightStaleOutcome::Existing {
                    operation_id,
                    version,
                    state,
                    reason: persisted_reason,
                },
            ),
        )) if operation_id == expected_operation_id
            && version == expected_version
            && state == MergeOperationState::Stale
            && persisted_reason == reason =>
        {
            admission.clean(&flow.response, stale_accept_outcome(reason))
        }
        ExactDeliveryWriteResult::KnownNotApplied { reason, error } => {
            admission.retain(&flow.response, merge_known_not_applied(reason, error))
        }
        ExactDeliveryWriteResult::OutcomeUnknown => admission.retain(
            &flow.response,
            DeliveryMergeAcceptanceOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::OutcomeUnknown,
            ),
        ),
        ExactDeliveryWriteResult::InvariantConflict | ExactDeliveryWriteResult::Confirmed(_) => {
            admission.poison(&flow.response, inconsistent_accept_outcome())
        }
    }
}

pub(super) async fn persist(flow: &AcceptFlow, authenticated: AuthenticatedAccept) -> WorkerFinish {
    let AuthenticatedAccept {
        validated,
        session,
        authentication,
    } = authenticated;
    let ValidatedAccept { routed } = validated;
    let RoutedAccept { admission, context } = routed;
    // The original monolithic worker retained the authenticated runtime
    // authority and validated snapshot through every Store outcome. Keep that
    // lifetime explicit while this stage owns the repository guards.
    let runtime_authority = (session, authentication);
    let validated_context = context;

    match inspect_accept_receipt(flow.dependencies.as_ref(), &flow.command).await {
        Ok(AcceptReceiptStatus::Existing { receipt, context }) => {
            let outcome = durable_acceptance(&receipt, DeliveryMergeReceiptDisposition::Existing);
            send_accept_response(&flow.response, outcome.clone());
            let stage = drive_pipeline(flow.dependencies.as_ref(), *context).await;
            return admission.finish(stage).with_accept_fallback(outcome);
        }
        Ok(AcceptReceiptStatus::Missing) => {}
        Err(outcome @ DeliveryMergeAcceptanceOutcome::Conflict(_)) => {
            return admission.clean(&flow.response, outcome);
        }
        Err(outcome) => return admission.poison(&flow.response, outcome),
    }
    let Some(generation) = admission.intake_generation else {
        return admission.poison(&flow.response, inconsistent_accept_outcome());
    };
    let service = flow.service_state.current();
    if !flow.intake_gate.still_accepts(generation) || service.state == ServiceState::Quiescing {
        return admission.clean(
            &flow.response,
            DeliveryMergeAcceptanceOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::ManagerQuiescing,
            ),
        );
    }
    if service.state != ServiceState::Ready {
        return admission.clean(
            &flow.response,
            DeliveryMergeAcceptanceOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::ServiceNotReady,
            ),
        );
    }
    let write =
        DeliveryWriteCommand::Merge(DeliveryMergeWriteCommand::Accept(flow.command.clone()));
    let (receipt, disposition) =
        match execute_exact_delivery_write(&flow.dependencies.writer, write).await {
            ExactDeliveryWriteResult::Confirmed(DeliveryWriteOutcome::Merge(
                DeliveryMergeWriteOutcome::Accept(AcceptMergeOutcome::Accepted(receipt)),
            )) => (receipt, DeliveryMergeReceiptDisposition::Created),
            ExactDeliveryWriteResult::Confirmed(DeliveryWriteOutcome::Merge(
                DeliveryMergeWriteOutcome::Accept(AcceptMergeOutcome::Existing(receipt)),
            )) => (receipt, DeliveryMergeReceiptDisposition::Existing),
            ExactDeliveryWriteResult::KnownNotApplied { reason, error } => {
                return admission.clean(&flow.response, merge_known_not_applied(reason, error));
            }
            ExactDeliveryWriteResult::OutcomeUnknown => {
                return admission.retain(
                    &flow.response,
                    DeliveryMergeAcceptanceOutcome::Unavailable(
                        DeliveryPreflightUnavailableReason::OutcomeUnknown,
                    ),
                );
            }
            ExactDeliveryWriteResult::Confirmed(DeliveryWriteOutcome::Merge(
                DeliveryMergeWriteOutcome::Accept(AcceptMergeOutcome::Conflict),
            )) => {
                return admission.clean(
                    &flow.response,
                    DeliveryMergeAcceptanceOutcome::Conflict(
                        DeliveryCommandConflict::PreflightStale,
                    ),
                );
            }
            ExactDeliveryWriteResult::InvariantConflict
            | ExactDeliveryWriteResult::Confirmed(_) => {
                return admission.poison(&flow.response, inconsistent_accept_outcome());
            }
        };
    let outcome = durable_acceptance(&receipt, disposition);
    send_accept_response(&flow.response, outcome.clone());
    let context =
        match load_operation_context(flow.dependencies.as_ref(), receipt.operation_id).await {
            Ok(context) => context,
            Err(_) => return admission.poison(&flow.response, outcome),
        };
    let stage = drive_pipeline(flow.dependencies.as_ref(), context).await;
    let finish = admission.finish(stage).with_accept_fallback(outcome);
    drop((runtime_authority, validated_context));
    finish
}
