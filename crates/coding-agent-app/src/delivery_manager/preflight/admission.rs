use std::sync::Arc;
use std::time::Duration;

use coding_agent_domain::RepositoryId;
use coding_agent_store::PreflightCommandRequest;
use tokio::sync::Semaphore;
use tokio::time::{sleep, timeout};

use crate::delivery_api_projection::{
    DeliveryPreflightBusyReason, DeliveryPreflightOutcome, DeliveryPreflightState,
    DeliveryPreflightUnavailableReason,
};
use crate::delivery_manager::{DeliveryIntakeGate, DeliveryManagerLiveDependencies};
use crate::{
    RepositoryControlCoordinator, RepositoryControlError, RepositoryControlLease,
    RepositoryControlPoisonReason, ServiceState, ServiceStateController,
};

use super::routing::{ReceiptStatus, inspect_receipt_status, load_snapshot};
use super::{LivePreflightCompletion, eligibility, persist, routing, runtime};

pub(super) const PRE_ORCHESTRATION_TIMEOUT: Duration = Duration::from_secs(30);
pub(super) const PRE_RUNTIME_STAGE_TIMEOUT: Duration = Duration::from_secs(11 * 60);
pub(super) const PREFLIGHT_RETRY_AFTER: Duration = Duration::from_millis(100);
const MAX_PREFLIGHT_ATTEMPTS: usize = 3;

pub(super) enum PreflightAttemptResult {
    Released(DeliveryPreflightOutcome),
    Retained {
        outcome: DeliveryPreflightOutcome,
        repository_lease: RepositoryControlLease,
    },
}

impl PreflightAttemptResult {
    fn released(outcome: DeliveryPreflightOutcome) -> Self {
        Self::Released(outcome)
    }

    fn retained_with_lease(
        outcome: DeliveryPreflightOutcome,
        repository_lease: RepositoryControlLease,
    ) -> Self {
        Self::Retained {
            outcome,
            repository_lease,
        }
    }
}

pub(super) async fn run_live_preflight(
    dependencies: Arc<DeliveryManagerLiveDependencies>,
    global_git_operations: Arc<Semaphore>,
    repository_control: Arc<RepositoryControlCoordinator>,
    intake_gate: Arc<DeliveryIntakeGate>,
    service_state: ServiceStateController,
    command: PreflightCommandRequest,
) -> LivePreflightCompletion {
    let mut last_retry = None;
    for attempt in 0..MAX_PREFLIGHT_ATTEMPTS {
        let receipt_status = match inspect_receipt_status(
            dependencies.as_ref(),
            repository_control.as_ref(),
            &command,
        )
        .await
        {
            Ok(status) => status,
            Err(outcome) => return LivePreflightCompletion::released(outcome),
        };
        if let ReceiptStatus::Terminal(outcome) = receipt_status {
            return LivePreflightCompletion::released(outcome);
        }

        let intake_generation = if matches!(receipt_status, ReceiptStatus::Missing) {
            let (quiesced, generation) = intake_gate.snapshot();
            let current_service = service_state.current();
            if quiesced || current_service.state == ServiceState::Quiescing {
                return LivePreflightCompletion::released(DeliveryPreflightOutcome::Unavailable(
                    DeliveryPreflightUnavailableReason::ManagerQuiescing,
                ));
            }
            if current_service.state != ServiceState::Ready {
                return LivePreflightCompletion::released(DeliveryPreflightOutcome::Unavailable(
                    DeliveryPreflightUnavailableReason::ServiceNotReady,
                ));
            }
            Some(generation)
        } else {
            // A durable pending intent is recovery work, not new user intake.
            None
        };

        let global_permit = match timeout(
            PRE_ORCHESTRATION_TIMEOUT,
            Arc::clone(&global_git_operations).acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => {
                return LivePreflightCompletion::released(DeliveryPreflightOutcome::Unavailable(
                    DeliveryPreflightUnavailableReason::OrchestrationUnavailable,
                ));
            }
            Err(_) => {
                return LivePreflightCompletion::released(DeliveryPreflightOutcome::Busy(
                    DeliveryPreflightBusyReason::WorkerQueueFull,
                ));
            }
        };

        let routing_snapshot = match load_snapshot(dependencies.as_ref(), command.task_id()).await {
            Ok(snapshot) => snapshot,
            Err(outcome) => return LivePreflightCompletion::released(outcome),
        };
        let key = match repository_control
            .delivery_coordination_key(routing_snapshot.task.repository_id)
        {
            Ok(key) => key,
            Err(_) => {
                return LivePreflightCompletion::released(DeliveryPreflightOutcome::Unavailable(
                    DeliveryPreflightUnavailableReason::RepositoryControlUnavailable,
                ));
            }
        };
        let lease = match repository_control.try_acquire_delivery(key) {
            Ok(lease) => lease,
            Err(RepositoryControlError::Busy) => {
                return LivePreflightCompletion::released(DeliveryPreflightOutcome::Busy(
                    DeliveryPreflightBusyReason::RepositoryBusy,
                ));
            }
            Err(_) => {
                return LivePreflightCompletion::released(DeliveryPreflightOutcome::Unavailable(
                    DeliveryPreflightUnavailableReason::RepositoryControlUnavailable,
                ));
            }
        };

        let result = run_with_repository_lease(
            dependencies.as_ref(),
            repository_control.as_ref(),
            intake_gate.as_ref(),
            &service_state,
            intake_generation,
            command.clone(),
            routing_snapshot.task.repository_id,
            lease,
        )
        .await;
        let outcome = match result {
            PreflightAttemptResult::Retained {
                outcome,
                repository_lease,
            } => {
                return LivePreflightCompletion::retained(outcome, global_permit, repository_lease);
            }
            PreflightAttemptResult::Released(outcome) => outcome,
        };
        drop(global_permit);

        if matches!(
            &outcome,
            DeliveryPreflightOutcome::KnownNotAppliedPersisted(_)
        ) && attempt + 1 < MAX_PREFLIGHT_ATTEMPTS
        {
            last_retry = Some(outcome);
            sleep(PREFLIGHT_RETRY_AFTER).await;
            continue;
        }
        return LivePreflightCompletion::released(outcome);
    }
    LivePreflightCompletion::released(last_retry.unwrap_or(DeliveryPreflightOutcome::Unavailable(
        DeliveryPreflightUnavailableReason::StoreUnavailable,
    )))
}

#[allow(clippy::too_many_arguments)]
async fn run_with_repository_lease(
    dependencies: &DeliveryManagerLiveDependencies,
    repository_control: &RepositoryControlCoordinator,
    intake_gate: &DeliveryIntakeGate,
    service_state: &ServiceStateController,
    intake_generation: Option<u64>,
    command: PreflightCommandRequest,
    routing_repository_id: RepositoryId,
    lease: RepositoryControlLease,
) -> PreflightAttemptResult {
    let routed = match routing::refresh_under_lease(
        dependencies,
        repository_control,
        command,
        routing_repository_id,
        lease,
    )
    .await
    {
        Ok(routed) => routed,
        Err(outcome) => return outcome,
    };

    let eligible = match eligibility::validate(dependencies, routed).await {
        Ok(eligible) => eligible,
        Err(outcome) => return outcome,
    };

    let authenticated = match runtime::authenticate(dependencies, eligible).await {
        Ok(authenticated) => authenticated,
        Err(outcome) => return outcome,
    };

    persist::continue_authenticated(
        dependencies,
        repository_control,
        intake_gate,
        service_state,
        intake_generation,
        authenticated,
    )
    .await
}

pub(super) fn inconsistent_outcome() -> DeliveryPreflightOutcome {
    DeliveryPreflightOutcome::Unavailable(
        DeliveryPreflightUnavailableReason::RepositoryControlUnavailable,
    )
}

pub(super) fn clean_and_release(
    lease: RepositoryControlLease,
    outcome: DeliveryPreflightOutcome,
) -> PreflightAttemptResult {
    match lease.clean_release() {
        Ok(()) => PreflightAttemptResult::released(outcome),
        Err(_) => PreflightAttemptResult::released(inconsistent_outcome()),
    }
}

pub(super) fn finish_terminal_receipt(
    lease: RepositoryControlLease,
    outcome: DeliveryPreflightOutcome,
) -> PreflightAttemptResult {
    let reconciliation_required = matches!(
        &outcome,
        DeliveryPreflightOutcome::Durable(operation)
            if operation.state() == DeliveryPreflightState::ReconciliationRequired
    );
    if reconciliation_required {
        poison_and_release(lease, outcome)
    } else {
        clean_and_release(lease, outcome)
    }
}

pub(super) fn poison_and_release(
    lease: RepositoryControlLease,
    outcome: DeliveryPreflightOutcome,
) -> PreflightAttemptResult {
    match lease.poison(RepositoryControlPoisonReason::SideEffectIdentityMismatch) {
        Ok(()) => PreflightAttemptResult::released(outcome),
        Err(_) => PreflightAttemptResult::released(inconsistent_outcome()),
    }
}

pub(super) fn retain_and_fail_closed(
    lease: RepositoryControlLease,
    outcome: DeliveryPreflightOutcome,
) -> PreflightAttemptResult {
    // Cleanup is unproven, but the durable failure classification itself is
    // known. Keep this exact unpoisoned lease plus the global slot owned by the
    // actor: public repository state remains Busy and no replacement Git work
    // can start. Dropping the retained guard abnormally still poisons via its
    // fail-closed Drop implementation.
    PreflightAttemptResult::retained_with_lease(outcome, lease)
}

pub(super) fn retain_unknown(
    lease: RepositoryControlLease,
    outcome: DeliveryPreflightOutcome,
) -> PreflightAttemptResult {
    PreflightAttemptResult::retained_with_lease(outcome, lease)
}
