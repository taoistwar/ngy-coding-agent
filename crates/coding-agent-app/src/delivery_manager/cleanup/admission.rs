use super::*;

mod fresh;
mod outcome;
mod receipt;
mod runtime;
mod validation;

use fresh::run_fresh_acceptance;
use outcome::{cleanup_reconciliation_admission_outcome, inconsistent_cleanup_outcome};
pub(super) use outcome::{send_cleanup_response, unavailable_cleanup_outcome};
use receipt::{cleanup_known_not_applied, durable_acceptance, inspect_cleanup_receipt};
use runtime::{FreshCleanupRuntimeBindingError, bind_fresh_cleanup_runtime};
use validation::{load_cleanup_acceptance_snapshot, validate_cleanup_acceptance};

pub(super) async fn run_accept_worker(
    dependencies: Arc<DeliveryManagerLiveDependencies>,
    global_git_operations: Arc<Semaphore>,
    repository_control: Arc<RepositoryControlCoordinator>,
    intake_gate: Arc<DeliveryIntakeGate>,
    service_state: ServiceStateController,
    command: CleanupCommand,
    response: CleanupResponseSlot,
) -> WorkerFinish {
    // Receipt lookup always precedes capacity, routing, and lease acquisition.
    let receipt_status = match inspect_cleanup_receipt(dependencies.as_ref(), &command).await {
        Ok(status) => status,
        Err(outcome) => return accept_released(&response, outcome),
    };
    let existing_outcome = existing_receipt_outcome(&receipt_status, &response);
    if let Some(finish) = terminal_receipt_finish(&receipt_status, existing_outcome.as_ref()) {
        return finish;
    }
    let intake_generation =
        match fresh_intake_generation(&receipt_status, intake_gate.as_ref(), &service_state) {
            Ok(generation) => generation,
            Err(outcome) => return accept_released(&response, outcome),
        };

    // Capacity precedes routing; routing precedes the repository lease.
    let authority = match acquire_routed_authority(
        dependencies.as_ref(),
        global_git_operations,
        repository_control.as_ref(),
        &command,
        &receipt_status,
        &response,
    )
    .await
    {
        Ok(authority) => authority,
        Err(finish) => return finish,
    };
    let fresh_snapshot =
        match load_cleanup_acceptance_snapshot(dependencies.as_ref(), command.task_id()).await {
            Ok(snapshot) => snapshot,
            Err(outcome) => {
                return poison_accept(authority.permit, authority.lease, &response, outcome);
            }
        };
    if fresh_snapshot.task.repository_id != authority.repository_id
        || repository_control.delivery_coordination_key(authority.repository_id)
            != Ok(authority.lease.coordination_key())
    {
        return poison_accept(
            authority.permit,
            authority.lease,
            &response,
            inconsistent_cleanup_outcome(),
        );
    }

    let existing_context = match receipt_status {
        CleanupReceiptStatus::Existing { context, .. } => Some(context),
        CleanupReceiptStatus::Missing => None,
    };
    let authority = match prove_cleanup_is_quiescent(
        dependencies.as_ref(),
        &fresh_snapshot,
        command.task_id(),
        authority,
        existing_context.is_some(),
        existing_outcome.as_ref(),
        &response,
    )
    .await
    {
        Ok(authority) => authority,
        Err(finish) => return finish,
    };

    if let Some(context) = existing_context {
        let stage = drive_cleanup_pipeline(dependencies.as_ref(), context).await;
        let outcome = existing_outcome.unwrap_or_else(inconsistent_cleanup_outcome);
        return finish_stage(authority.permit, authority.lease, stage)
            .with_accept_fallback(outcome);
    }

    run_fresh_acceptance(
        dependencies.as_ref(),
        intake_gate.as_ref(),
        &service_state,
        &command,
        &response,
        &fresh_snapshot,
        authority.repository_id,
        intake_generation,
        authority.permit,
        authority.lease,
    )
    .await
}

struct AdmissionAuthority {
    permit: OwnedSemaphorePermit,
    lease: RepositoryControlLease,
    repository_id: coding_agent_domain::RepositoryId,
}

fn existing_receipt_outcome(
    receipt_status: &CleanupReceiptStatus,
    response: &CleanupResponseSlot,
) -> Option<DeliveryCleanupAcceptanceOutcome> {
    let CleanupReceiptStatus::Existing { receipt, context } = receipt_status else {
        return None;
    };
    let outcome = durable_acceptance(
        receipt,
        context.operation.kind,
        DeliveryCleanupReceiptDisposition::Existing,
    );
    send_cleanup_response(response, outcome.clone());
    Some(outcome)
}

fn terminal_receipt_finish(
    receipt_status: &CleanupReceiptStatus,
    outcome: Option<&DeliveryCleanupAcceptanceOutcome>,
) -> Option<WorkerFinish> {
    let CleanupReceiptStatus::Existing { context, .. } = receipt_status else {
        return None;
    };
    cleanup_operation_is_terminal(context.operation.state).then(|| {
        WorkerFinish::released(recovery_outcome_for_state(context.operation.state))
            .with_accept_fallback(
                outcome
                    .cloned()
                    .unwrap_or_else(inconsistent_cleanup_outcome),
            )
    })
}

fn fresh_intake_generation(
    receipt_status: &CleanupReceiptStatus,
    intake_gate: &DeliveryIntakeGate,
    service_state: &ServiceStateController,
) -> Result<Option<u64>, DeliveryCleanupAcceptanceOutcome> {
    if matches!(receipt_status, CleanupReceiptStatus::Existing { .. }) {
        return Ok(None);
    }
    let (quiesced, generation) = intake_gate.snapshot();
    let service = service_state.current();
    if quiesced || service.state == ServiceState::Quiescing {
        return Err(DeliveryCleanupAcceptanceOutcome::Unavailable(
            DeliveryPreflightUnavailableReason::ManagerQuiescing,
        ));
    }
    if service.state != ServiceState::Ready {
        return Err(DeliveryCleanupAcceptanceOutcome::Unavailable(
            DeliveryPreflightUnavailableReason::ServiceNotReady,
        ));
    }
    Ok(Some(generation))
}

async fn acquire_routed_authority(
    dependencies: &DeliveryManagerLiveDependencies,
    global_git_operations: Arc<Semaphore>,
    repository_control: &RepositoryControlCoordinator,
    command: &CleanupCommand,
    receipt_status: &CleanupReceiptStatus,
    response: &CleanupResponseSlot,
) -> Result<AdmissionAuthority, WorkerFinish> {
    let permit = acquire_global_permit(global_git_operations)
        .await
        .map_err(|outcome| accept_released(response, outcome))?;
    let routing_snapshot = match receipt_status {
        CleanupReceiptStatus::Existing { context, .. } => context.snapshot.clone(),
        CleanupReceiptStatus::Missing => {
            match load_cleanup_acceptance_snapshot(dependencies, command.task_id()).await {
                Ok(snapshot) => snapshot,
                Err(outcome) => return Err(accept_released(response, outcome)),
            }
        }
    };
    let repository_id = routing_snapshot.task.repository_id;
    let key = match repository_control.delivery_coordination_key(repository_id) {
        Ok(key) => key,
        Err(_) => {
            return Err(accept_released(
                response,
                DeliveryCleanupAcceptanceOutcome::Unavailable(
                    DeliveryPreflightUnavailableReason::RepositoryControlUnavailable,
                ),
            ));
        }
    };
    let lease = match repository_control.try_acquire_delivery(key) {
        Ok(lease) => lease,
        Err(RepositoryControlError::Busy) => {
            return Err(accept_released(
                response,
                DeliveryCleanupAcceptanceOutcome::Busy(DeliveryPreflightBusyReason::RepositoryBusy),
            ));
        }
        Err(_) => {
            return Err(accept_released(
                response,
                DeliveryCleanupAcceptanceOutcome::Unavailable(
                    DeliveryPreflightUnavailableReason::RepositoryControlUnavailable,
                ),
            ));
        }
    };
    Ok(AdmissionAuthority {
        permit,
        lease,
        repository_id,
    })
}

#[allow(clippy::too_many_arguments)]
async fn prove_cleanup_is_quiescent(
    dependencies: &DeliveryManagerLiveDependencies,
    snapshot: &DeliveryEligibilitySnapshot,
    task_id: coding_agent_domain::TaskId,
    authority: AdmissionAuthority,
    has_existing_context: bool,
    existing_outcome: Option<&DeliveryCleanupAcceptanceOutcome>,
    response: &CleanupResponseSlot,
) -> Result<AdmissionAuthority, WorkerFinish> {
    match observe_task_ownership(dependencies, snapshot).await {
        OwnershipObservation::Inactive => {}
        OwnershipObservation::Active if has_existing_context => {
            return Err(clean_accept(
                authority.permit,
                authority.lease,
                response,
                existing_outcome
                    .cloned()
                    .unwrap_or_else(inconsistent_cleanup_outcome),
            ));
        }
        OwnershipObservation::Active => {
            return Err(clean_accept(
                authority.permit,
                authority.lease,
                response,
                DeliveryCleanupAcceptanceOutcome::Conflict(
                    DeliveryCommandConflict::ArtifactProcessStillActive,
                ),
            ));
        }
        OwnershipObservation::Mismatch => {
            return Err(poison_accept(
                authority.permit,
                authority.lease,
                response,
                inconsistent_cleanup_outcome(),
            ));
        }
    }
    match observe_processes(dependencies, task_id).await {
        ProcessObservation::Clean => Ok(authority),
        ProcessObservation::Active if has_existing_context => Err(clean_accept(
            authority.permit,
            authority.lease,
            response,
            existing_outcome
                .cloned()
                .unwrap_or_else(inconsistent_cleanup_outcome),
        )),
        ProcessObservation::Active => Err(clean_accept(
            authority.permit,
            authority.lease,
            response,
            DeliveryCleanupAcceptanceOutcome::Conflict(
                DeliveryCommandConflict::ArtifactProcessStillActive,
            ),
        )),
        ProcessObservation::CleanupUnproven => {
            let outcome =
                existing_outcome
                    .cloned()
                    .unwrap_or(DeliveryCleanupAcceptanceOutcome::Unavailable(
                        DeliveryPreflightUnavailableReason::ProcessProofUnavailable,
                    ));
            send_cleanup_response(response, outcome.clone());
            Err(WorkerFinish::retained(
                DeliveryOperationRecoveryOutcome::RetainedFailClosed,
                authority.permit,
                authority.lease,
            )
            .with_accept_fallback(outcome))
        }
        ProcessObservation::Mismatch => Err(poison_accept(
            authority.permit,
            authority.lease,
            response,
            inconsistent_cleanup_outcome(),
        )),
    }
}
