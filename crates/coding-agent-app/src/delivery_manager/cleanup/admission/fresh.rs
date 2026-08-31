use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_fresh_acceptance(
    dependencies: &DeliveryManagerLiveDependencies,
    intake_gate: &DeliveryIntakeGate,
    service_state: &ServiceStateController,
    command: &CleanupCommand,
    response: &CleanupResponseSlot,
    fresh_snapshot: &DeliveryEligibilitySnapshot,
    repository_id: coding_agent_domain::RepositoryId,
    intake_generation: Option<u64>,
    permit: OwnedSemaphorePermit,
    lease: RepositoryControlLease,
) -> WorkerFinish {
    let acceptance = match validate_cleanup_acceptance(fresh_snapshot, command) {
        Ok(acceptance) => acceptance,
        Err(outcome) => return clean_accept(permit, lease, response, outcome),
    };
    if let Err(error) = bind_fresh_cleanup_runtime(dependencies, fresh_snapshot, command).await {
        return finish_runtime_binding_error(permit, lease, response, error);
    }
    if acceptance.repository_id != repository_id {
        return poison_accept(permit, lease, response, inconsistent_cleanup_outcome());
    }

    match inspect_cleanup_receipt(dependencies, command).await {
        Ok(CleanupReceiptStatus::Existing { receipt, context }) => {
            let outcome = durable_acceptance(
                &receipt,
                context.operation.kind,
                DeliveryCleanupReceiptDisposition::Existing,
            );
            send_cleanup_response(response, outcome.clone());
            let stage = drive_cleanup_pipeline(dependencies, context).await;
            return finish_stage(permit, lease, stage).with_accept_fallback(outcome);
        }
        Ok(CleanupReceiptStatus::Missing) => {}
        Err(outcome) => return poison_accept(permit, lease, response, outcome),
    }
    let Some(generation) = intake_generation else {
        return poison_accept(permit, lease, response, inconsistent_cleanup_outcome());
    };
    let service = service_state.current();
    if !intake_gate.still_accepts(generation) || service.state == ServiceState::Quiescing {
        return clean_accept(
            permit,
            lease,
            response,
            DeliveryCleanupAcceptanceOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::ManagerQuiescing,
            ),
        );
    }
    if service.state != ServiceState::Ready {
        return clean_accept(
            permit,
            lease,
            response,
            DeliveryCleanupAcceptanceOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::ServiceNotReady,
            ),
        );
    }

    let write = command.write_command();
    let (receipt, disposition) = match execute_exact_delivery_write(&dependencies.writer, write)
        .await
    {
        ExactDeliveryWriteResult::Confirmed(DeliveryWriteOutcome::Cleanup(
            DeliveryCleanupWriteOutcome::AcceptWorktree(CleanupAcceptanceOutcome::Accepted(
                receipt,
            ))
            | DeliveryCleanupWriteOutcome::AcceptBranch(CleanupAcceptanceOutcome::Accepted(receipt)),
        )) => (receipt, DeliveryCleanupReceiptDisposition::Created),
        ExactDeliveryWriteResult::Confirmed(DeliveryWriteOutcome::Cleanup(
            DeliveryCleanupWriteOutcome::AcceptWorktree(CleanupAcceptanceOutcome::Existing(
                receipt,
            ))
            | DeliveryCleanupWriteOutcome::AcceptBranch(CleanupAcceptanceOutcome::Existing(receipt)),
        )) => (receipt, DeliveryCleanupReceiptDisposition::Existing),
        ExactDeliveryWriteResult::KnownNotApplied { reason, error } => {
            return clean_accept(
                permit,
                lease,
                response,
                cleanup_known_not_applied(reason, error),
            );
        }
        ExactDeliveryWriteResult::OutcomeUnknown => {
            let outcome = DeliveryCleanupAcceptanceOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::OutcomeUnknown,
            );
            send_cleanup_response(response, outcome.clone());
            return WorkerFinish::retained(
                DeliveryOperationRecoveryOutcome::RetainedFailClosed,
                permit,
                lease,
            )
            .with_accept_fallback(outcome);
        }
        ExactDeliveryWriteResult::Confirmed(DeliveryWriteOutcome::Cleanup(
            DeliveryCleanupWriteOutcome::AcceptWorktree(CleanupAcceptanceOutcome::Conflict)
            | DeliveryCleanupWriteOutcome::AcceptBranch(CleanupAcceptanceOutcome::Conflict),
        )) => {
            return clean_accept(
                permit,
                lease,
                response,
                DeliveryCleanupAcceptanceOutcome::Conflict(
                    DeliveryCommandConflict::ArtifactCleanupNotAllowed,
                ),
            );
        }
        ExactDeliveryWriteResult::InvariantConflict | ExactDeliveryWriteResult::Confirmed(_) => {
            return poison_accept(permit, lease, response, inconsistent_cleanup_outcome());
        }
    };
    let outcome = durable_acceptance(&receipt, command.kind(), disposition);
    send_cleanup_response(response, outcome.clone());
    let context = match load_cleanup_operation_context(dependencies, receipt.operation_id).await {
        Ok(context) => context,
        Err(_) => return poison_accept(permit, lease, response, outcome),
    };
    let stage = drive_cleanup_pipeline(dependencies, context).await;
    finish_stage(permit, lease, stage).with_accept_fallback(outcome)
}

fn finish_runtime_binding_error(
    permit: OwnedSemaphorePermit,
    lease: RepositoryControlLease,
    response: &CleanupResponseSlot,
    error: FreshCleanupRuntimeBindingError,
) -> WorkerFinish {
    match error {
        FreshCleanupRuntimeBindingError::TimedOutWithCleanupUnproven => {
            let outcome = DeliveryCleanupAcceptanceOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::RuntimeUnavailable,
            );
            send_cleanup_response(response, outcome.clone());
            WorkerFinish::retained(
                DeliveryOperationRecoveryOutcome::RetainedFailClosed,
                permit,
                lease,
            )
            .with_accept_fallback(outcome)
        }
        FreshCleanupRuntimeBindingError::Runtime(
            DeliveryLiveCleanupRuntimeError::TargetWorktreeDirty,
        ) => clean_accept(
            permit,
            lease,
            response,
            DeliveryCleanupAcceptanceOutcome::Ineligible(vec![
                DeliveryEligibilityReason::TargetWorktreeDirty,
            ]),
        ),
        FreshCleanupRuntimeBindingError::Runtime(
            DeliveryLiveCleanupRuntimeError::ProcessCleanupUnproven,
        ) => {
            let outcome = DeliveryCleanupAcceptanceOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::ProcessProofUnavailable,
            );
            send_cleanup_response(response, outcome.clone());
            WorkerFinish::retained(
                DeliveryOperationRecoveryOutcome::RetainedFailClosed,
                permit,
                lease,
            )
            .with_accept_fallback(outcome)
        }
        FreshCleanupRuntimeBindingError::Runtime(
            DeliveryLiveCleanupRuntimeError::ReconciliationRequired(reason),
        ) => poison_accept(
            permit,
            lease,
            response,
            cleanup_reconciliation_admission_outcome(reason),
        ),
        FreshCleanupRuntimeBindingError::Runtime(DeliveryLiveCleanupRuntimeError::Unavailable) => {
            clean_accept(
                permit,
                lease,
                response,
                DeliveryCleanupAcceptanceOutcome::Unavailable(
                    DeliveryPreflightUnavailableReason::RuntimeUnavailable,
                ),
            )
        }
    }
}
