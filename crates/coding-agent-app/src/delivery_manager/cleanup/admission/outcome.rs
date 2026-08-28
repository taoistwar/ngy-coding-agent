use super::*;

pub(super) fn cleanup_reconciliation_admission_outcome(
    reason: CleanupReconciliationReason,
) -> DeliveryCleanupAcceptanceOutcome {
    match reason {
        CleanupReconciliationReason::DeliveryStateInconsistent => {
            DeliveryCleanupAcceptanceOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::RuntimeUnavailable,
            )
        }
        CleanupReconciliationReason::SourceInconsistent => {
            DeliveryCleanupAcceptanceOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::SourceInconsistent,
            )
        }
        CleanupReconciliationReason::ProcessTreeCleanupFailed => {
            DeliveryCleanupAcceptanceOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::ProcessProofUnavailable,
            )
        }
        CleanupReconciliationReason::WorktreeIdentityMismatch => {
            DeliveryCleanupAcceptanceOutcome::Conflict(
                DeliveryCommandConflict::WorktreeIdentityMismatch,
            )
        }
        CleanupReconciliationReason::UnsafeGitConfiguration => {
            DeliveryCleanupAcceptanceOutcome::Ineligible(vec![
                DeliveryEligibilityReason::UnsafeGitConfiguration,
            ])
        }
        CleanupReconciliationReason::UnsupportedGitAttributes => {
            DeliveryCleanupAcceptanceOutcome::Ineligible(vec![
                DeliveryEligibilityReason::UnsupportedGitAttributes,
            ])
        }
        CleanupReconciliationReason::CommandTimedOut => {
            DeliveryCleanupAcceptanceOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::CommandTimedOut,
            )
        }
    }
}

pub(in crate::delivery_manager::cleanup) fn send_cleanup_response(
    slot: &CleanupResponseSlot,
    outcome: DeliveryCleanupAcceptanceOutcome,
) {
    if let Some(response) = slot.lock().expect("lock cleanup response slot").take() {
        let _ = response.send(outcome);
    }
}

pub(in crate::delivery_manager::cleanup) fn unavailable_cleanup_outcome(
    service: ServiceStateSnapshot,
    intake_gate: &DeliveryIntakeGate,
) -> DeliveryCleanupAcceptanceOutcome {
    let (quiesced, _) = intake_gate.snapshot();
    DeliveryCleanupAcceptanceOutcome::Unavailable(if quiesced {
        DeliveryPreflightUnavailableReason::ManagerQuiescing
    } else if service.state != ServiceState::Ready {
        DeliveryPreflightUnavailableReason::ServiceNotReady
    } else {
        DeliveryPreflightUnavailableReason::OrchestrationUnavailable
    })
}

pub(super) fn inconsistent_cleanup_outcome() -> DeliveryCleanupAcceptanceOutcome {
    DeliveryCleanupAcceptanceOutcome::Unavailable(
        DeliveryPreflightUnavailableReason::OrchestrationUnavailable,
    )
}
