use coding_agent_domain::RepositoryId;

use super::routing::{AcceptReceiptStatus, inspect_accept_receipt, load_accept_context};
use super::*;

pub(super) struct AcceptAdmission {
    pub(super) permit: OwnedSemaphorePermit,
    pub(super) lease: RepositoryControlLease,
    pub(super) intake_generation: Option<u64>,
    pub(super) routing_repository: RepositoryId,
}

impl AcceptAdmission {
    pub(super) fn clean(
        self,
        response: &AcceptResponseSlot,
        outcome: DeliveryMergeAcceptanceOutcome,
    ) -> WorkerFinish {
        clean_accept(self.permit, self.lease, response, outcome)
    }

    pub(super) fn poison(
        self,
        response: &AcceptResponseSlot,
        outcome: DeliveryMergeAcceptanceOutcome,
    ) -> WorkerFinish {
        poison_accept(self.permit, self.lease, response, outcome)
    }

    pub(super) fn retain(
        self,
        response: &AcceptResponseSlot,
        outcome: DeliveryMergeAcceptanceOutcome,
    ) -> WorkerFinish {
        send_accept_response(response, outcome.clone());
        WorkerFinish::retained(
            DeliveryOperationRecoveryOutcome::RetainedFailClosed,
            self.permit,
            self.lease,
        )
        .with_accept_fallback(outcome)
    }

    pub(super) fn finish(self, stage: LiveStageOutcome) -> WorkerFinish {
        finish_stage(self.permit, self.lease, stage)
    }
}

pub(super) async fn admit(
    flow: &AcceptFlow,
    global_git_operations: Arc<Semaphore>,
) -> Result<AcceptAdmission, WorkerFinish> {
    let receipt_status =
        match inspect_accept_receipt(flow.dependencies.as_ref(), &flow.command).await {
            Ok(status) => status,
            Err(outcome) => return Err(accept_released(&flow.response, outcome)),
        };
    if let AcceptReceiptStatus::Existing { receipt, context } = &receipt_status {
        let outcome = durable_acceptance(receipt, DeliveryMergeReceiptDisposition::Existing);
        send_accept_response(&flow.response, outcome.clone());
        if operation_is_terminal(context.operation.state) {
            return Err(WorkerFinish::released(recovery_outcome_for_state(
                context.operation.state,
            ))
            .with_accept_fallback(outcome));
        }
    }

    let intake_generation = if matches!(receipt_status, AcceptReceiptStatus::Missing) {
        let (quiesced, generation) = flow.intake_gate.snapshot();
        let service = flow.service_state.current();
        if quiesced || service.state == ServiceState::Quiescing {
            return Err(accept_released(
                &flow.response,
                DeliveryMergeAcceptanceOutcome::Unavailable(
                    DeliveryPreflightUnavailableReason::ManagerQuiescing,
                ),
            ));
        }
        if service.state != ServiceState::Ready {
            return Err(accept_released(
                &flow.response,
                DeliveryMergeAcceptanceOutcome::Unavailable(
                    DeliveryPreflightUnavailableReason::ServiceNotReady,
                ),
            ));
        }
        Some(generation)
    } else {
        None
    };

    let permit = match acquire_global_permit(global_git_operations).await {
        Ok(permit) => permit,
        Err(outcome) => return Err(accept_released(&flow.response, outcome)),
    };
    let routing_context = match receipt_status {
        AcceptReceiptStatus::Existing { context, .. } => *context,
        AcceptReceiptStatus::Missing => {
            match load_accept_context(flow.dependencies.as_ref(), &flow.command).await {
                Ok(context) => context,
                Err(outcome) => return Err(accept_released(&flow.response, outcome)),
            }
        }
    };
    let routing_repository = routing_context.snapshot.task.repository_id;
    let key = match flow
        .repository_control
        .delivery_coordination_key(routing_repository)
    {
        Ok(key) => key,
        Err(_) => {
            return Err(accept_released(
                &flow.response,
                DeliveryMergeAcceptanceOutcome::Unavailable(
                    DeliveryPreflightUnavailableReason::RepositoryControlUnavailable,
                ),
            ));
        }
    };
    let lease = match flow.repository_control.try_acquire_delivery(key) {
        Ok(lease) => lease,
        Err(RepositoryControlError::Busy) => {
            return Err(accept_released(
                &flow.response,
                DeliveryMergeAcceptanceOutcome::Busy(DeliveryPreflightBusyReason::RepositoryBusy),
            ));
        }
        Err(_) => {
            return Err(accept_released(
                &flow.response,
                DeliveryMergeAcceptanceOutcome::Unavailable(
                    DeliveryPreflightUnavailableReason::RepositoryControlUnavailable,
                ),
            ));
        }
    };

    Ok(AcceptAdmission {
        permit,
        lease,
        intake_generation,
        routing_repository,
    })
}

async fn acquire_global_permit(
    semaphore: Arc<Semaphore>,
) -> Result<OwnedSemaphorePermit, DeliveryMergeAcceptanceOutcome> {
    match timeout(LIVE_ORCHESTRATION_TIMEOUT, semaphore.acquire_owned()).await {
        Ok(Ok(permit)) => Ok(permit),
        Ok(Err(_)) => Err(DeliveryMergeAcceptanceOutcome::Unavailable(
            DeliveryPreflightUnavailableReason::OrchestrationUnavailable,
        )),
        Err(_) => Err(DeliveryMergeAcceptanceOutcome::Busy(
            DeliveryPreflightBusyReason::WorkerQueueFull,
        )),
    }
}
