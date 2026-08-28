use tokio::time::timeout;

use crate::delivery_manager::query::persistent_reasons;
use crate::delivery_manager::{DeliveryManagerLiveDependencies, DeliveryProcessProof};
use crate::{DeliveryPreflightOutcome, TaskActiveOwnership};

use super::admission::{
    PRE_ORCHESTRATION_TIMEOUT, PreflightAttemptResult, clean_and_release, inconsistent_outcome,
    poison_and_release, retain_and_fail_closed,
};
use super::routing::{RoutedPreflight, snapshot_allows_new_preflight};

pub(super) struct EligiblePreflight {
    pub(super) routed: RoutedPreflight,
}

pub(super) async fn validate(
    dependencies: &DeliveryManagerLiveDependencies,
    routed: RoutedPreflight,
) -> Result<EligiblePreflight, PreflightAttemptResult> {
    let mut blockers = persistent_reasons(&routed.snapshot);
    if routed.receipt.is_some() || snapshot_allows_new_preflight(&routed.snapshot) {
        blockers.retain(|reason| *reason != crate::DeliveryEligibilityReason::DeliveryOwned);
    }
    if !blockers.is_empty() {
        return Err(clean_and_release(
            routed.lease,
            DeliveryPreflightOutcome::Ineligible(blockers),
        ));
    }

    let ownership = timeout(
        PRE_ORCHESTRATION_TIMEOUT,
        dependencies
            .task_ownership
            .active_ownership(routed.command.task_id()),
    )
    .await;
    match ownership {
        Ok(Ok(TaskActiveOwnership::Inactive)) => {}
        Ok(Ok(TaskActiveOwnership::Active {
            repository_id,
            attempt,
        })) if repository_id == routed.snapshot.task.repository_id
            && attempt == routed.snapshot.task.attempt =>
        {
            return Err(clean_and_release(
                routed.lease,
                DeliveryPreflightOutcome::Ineligible(vec![
                    crate::DeliveryEligibilityReason::TaskActive,
                ]),
            ));
        }
        Ok(Ok(TaskActiveOwnership::Active { .. })) => {
            return Err(poison_and_release(routed.lease, inconsistent_outcome()));
        }
        Ok(Err(_)) | Err(_) => {
            return Err(poison_and_release(
                routed.lease,
                DeliveryPreflightOutcome::Unavailable(
                    crate::DeliveryPreflightUnavailableReason::OrchestrationUnavailable,
                ),
            ));
        }
    }

    let process = timeout(
        PRE_ORCHESTRATION_TIMEOUT,
        dependencies
            .process_proofs
            .observe(routed.command.task_id()),
    )
    .await;
    match process {
        Ok(Ok(DeliveryProcessProof::Clean)) => {}
        Ok(Ok(DeliveryProcessProof::Active)) => {
            return Err(clean_and_release(
                routed.lease,
                DeliveryPreflightOutcome::Ineligible(vec![
                    crate::DeliveryEligibilityReason::TaskActive,
                ]),
            ));
        }
        Ok(Ok(DeliveryProcessProof::CleanupUnproven)) | Ok(Err(_)) | Err(_) => {
            return Err(retain_and_fail_closed(
                routed.lease,
                DeliveryPreflightOutcome::Unavailable(
                    crate::DeliveryPreflightUnavailableReason::ProcessProofUnavailable,
                ),
            ));
        }
    }

    Ok(EligiblePreflight { routed })
}
