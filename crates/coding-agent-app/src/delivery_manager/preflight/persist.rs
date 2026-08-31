use std::time::Duration;

use coding_agent_store::{
    CreatePreflightOutcome, CreatePreflightRequest, DeliveryVersion,
    FailUnboundMergePreflightRequest, MergeOperationState, MergePreflightResult,
    MergeTransitionOutcome, PreflightCommandRequest, RecordMergePreflightResultRequest, StoreError,
};
use tokio::time::{Instant, timeout};

use crate::delivery_api_projection::{
    DeliveryCommandConflict, DeliveryPreflightDurability, DeliveryPreflightOperation,
    DeliveryPreflightOutcome, DeliveryPreflightRetry, DeliveryPreflightState,
    DeliveryPreflightUnavailableReason,
};
use crate::delivery_manager::{DeliveryIntakeGate, DeliveryManagerLiveDependencies};
use crate::{
    DeliveryDisposition, DeliveryMergeWriteCommand, DeliveryMergeWriteOutcome,
    DeliveryRuntimeAuthentication, DeliveryRuntimeFailure, DeliveryRuntimeSession,
    DeliveryWriteCommand, DeliveryWriteOutcome, RepositoryControlCoordinator,
    RepositoryControlLease, ServiceState, ServiceStateController,
};

use super::admission::{
    PREFLIGHT_RETRY_AFTER, PreflightAttemptResult, clean_and_release, finish_terminal_receipt,
    inconsistent_outcome, poison_and_release, retain_and_fail_closed, retain_unknown,
};
use super::routing::{
    ReceiptStatus, exact_receipt_operation, inspect_receipt_status, load_snapshot,
};
use super::runtime::{
    AuthenticatedPreflight, continue_unbound_preflight, resume_pending_preflight,
};

const STORE_WRITE_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_EXACT_RECONCILIATION_ATTEMPTS: usize = 3;

pub(super) async fn continue_authenticated(
    dependencies: &DeliveryManagerLiveDependencies,
    repository_control: &RepositoryControlCoordinator,
    intake_gate: &DeliveryIntakeGate,
    service_state: &ServiceStateController,
    intake_generation: Option<u64>,
    authenticated: AuthenticatedPreflight,
) -> PreflightAttemptResult {
    let routed = authenticated.eligible.routed;
    let command = routed.command;
    let lease = routed.lease;

    if let (Some(receipt), Some(operation)) = (routed.receipt, routed.operation.as_ref()) {
        return resume_pending_preflight(
            dependencies,
            authenticated.session.as_ref(),
            authenticated.authentication,
            authenticated.known_failure,
            command,
            receipt,
            operation,
            lease,
        )
        .await;
    }

    // Re-query after all fallible observation work. This closes the window in
    // which an exact receipt could have appeared outside this coordinator.
    match inspect_receipt_status(dependencies, repository_control, &command).await {
        Ok(ReceiptStatus::Terminal(outcome)) => return finish_terminal_receipt(lease, outcome),
        Ok(ReceiptStatus::Resume(receipt)) => {
            let resumed_snapshot = match load_snapshot(dependencies, command.task_id()).await {
                Ok(snapshot) => snapshot,
                Err(outcome) => return poison_and_release(lease, outcome),
            };
            let Some(operation) = exact_receipt_operation(&resumed_snapshot, &receipt) else {
                return poison_and_release(lease, inconsistent_outcome());
            };
            if !authenticated.authentication.authorizes(
                &resumed_snapshot,
                &command,
                lease.coordination_key(),
            ) || !authenticated.authentication.authorizes_operation(operation)
                || resumed_snapshot
                    .evidence_identity
                    .as_ref()
                    .is_none_or(|evidence| &operation.provenance.evidence != evidence)
            {
                return poison_and_release(lease, inconsistent_outcome());
            }
            return resume_pending_preflight(
                dependencies,
                authenticated.session.as_ref(),
                authenticated.authentication,
                authenticated.known_failure,
                command,
                receipt,
                operation,
                lease,
            )
            .await;
        }
        Ok(ReceiptStatus::Missing) => {}
        Err(outcome) => return poison_and_release(lease, outcome),
    }

    let Some(generation) = intake_generation else {
        return poison_and_release(lease, inconsistent_outcome());
    };
    let current_service = service_state.current();
    if !intake_gate.still_accepts(generation) || current_service.state == ServiceState::Quiescing {
        return clean_and_release(
            lease,
            DeliveryPreflightOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::ManagerQuiescing,
            ),
        );
    }
    if current_service.state != ServiceState::Ready {
        return clean_and_release(
            lease,
            DeliveryPreflightOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::ServiceNotReady,
            ),
        );
    }
    create_and_continue(
        dependencies,
        authenticated.session.as_ref(),
        authenticated.authentication,
        authenticated.known_failure,
        command,
        lease,
    )
    .await
}

async fn create_and_continue(
    dependencies: &DeliveryManagerLiveDependencies,
    session: &dyn DeliveryRuntimeSession,
    authentication: DeliveryRuntimeAuthentication,
    known_failure: Option<DeliveryRuntimeFailure>,
    command: PreflightCommandRequest,
    lease: RepositoryControlLease,
) -> PreflightAttemptResult {
    let create_request = match CreatePreflightRequest::try_new(
        command.clone(),
        authentication.common_git_identity().clone(),
        authentication.worktree_admin_identity().clone(),
        authentication.source_config_attributes_digest().clone(),
        authentication.target_config_attributes_digest().clone(),
        authentication.target_security_digest().clone(),
    ) {
        Ok(request) => request,
        Err(_) => return poison_and_release(lease, inconsistent_outcome()),
    };
    let write =
        DeliveryWriteCommand::Merge(DeliveryMergeWriteCommand::CreatePreflight(create_request));
    let (receipt, durability) = match execute_exact_write(&dependencies.writer, write).await {
        ExactWriteResult::Confirmed(DeliveryMergeWriteOutcome::CreatePreflight(
            CreatePreflightOutcome::Created(receipt),
        )) => (receipt, DeliveryPreflightDurability::Created),
        ExactWriteResult::Confirmed(DeliveryMergeWriteOutcome::CreatePreflight(
            CreatePreflightOutcome::Existing(receipt),
        )) => (receipt, DeliveryPreflightDurability::Existing),
        ExactWriteResult::KnownNotApplied { reason, error } => {
            return clean_and_release(lease, preflight_known_not_applied(reason, error));
        }
        ExactWriteResult::Unknown => {
            return retain_unknown(
                lease,
                DeliveryPreflightOutcome::Unavailable(
                    DeliveryPreflightUnavailableReason::OutcomeUnknown,
                ),
            );
        }
        ExactWriteResult::InvariantConflict | ExactWriteResult::Confirmed(_) => {
            return poison_and_release(lease, inconsistent_outcome());
        }
    };

    let persisted = match load_snapshot(dependencies, command.task_id()).await {
        Ok(snapshot) => snapshot,
        Err(outcome) => return poison_and_release(lease, outcome),
    };
    let Some(operation) = exact_receipt_operation(&persisted, &receipt) else {
        return poison_and_release(lease, inconsistent_outcome());
    };
    if super::routing::pending_shape(operation) != Some(super::routing::PendingShape::UnboundV1)
        || !authentication.authorizes(&persisted, &command, lease.coordination_key())
        || !authentication.authorizes_operation(operation)
    {
        return poison_and_release(lease, inconsistent_outcome());
    }
    continue_unbound_preflight(
        dependencies,
        session,
        authentication,
        known_failure,
        command,
        receipt,
        durability,
        lease,
    )
    .await
}

pub(super) async fn persist_prepared_failure(
    dependencies: &DeliveryManagerLiveDependencies,
    task_id: coding_agent_domain::TaskId,
    operation_id: coding_agent_store::DeliveryOperationId,
    durability: DeliveryPreflightDurability,
    failure: DeliveryRuntimeFailure,
    lease: RepositoryControlLease,
) -> PreflightAttemptResult {
    persist_prepared_result(
        dependencies,
        task_id,
        operation_id,
        durability,
        failure.prepared_failure(),
        failure.requires_retained_repository_ownership(),
        lease,
    )
    .await
}

pub(super) async fn persist_prepared_runtime_timeout(
    dependencies: &DeliveryManagerLiveDependencies,
    task_id: coding_agent_domain::TaskId,
    operation_id: coding_agent_store::DeliveryOperationId,
    durability: DeliveryPreflightDurability,
    lease: RepositoryControlLease,
) -> PreflightAttemptResult {
    persist_prepared_result(
        dependencies,
        task_id,
        operation_id,
        durability,
        DeliveryRuntimeFailure::Unavailable.prepared_failure(),
        true,
        lease,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn persist_prepared_result(
    dependencies: &DeliveryManagerLiveDependencies,
    task_id: coding_agent_domain::TaskId,
    operation_id: coding_agent_store::DeliveryOperationId,
    durability: DeliveryPreflightDurability,
    result: MergePreflightResult,
    retained_process_cleanup: bool,
    lease: RepositoryControlLease,
) -> PreflightAttemptResult {
    let expected_state = preflight_result_state(&result);
    let reconciliation_required =
        matches!(&result, MergePreflightResult::ReconciliationRequired(_));
    let request = match RecordMergePreflightResultRequest::try_new(
        task_id,
        operation_id,
        DeliveryVersion::try_new(2).expect("version two is valid"),
        result,
    ) {
        Ok(request) => request,
        Err(_) => {
            return retain_or_poison_inconsistent(lease, retained_process_cleanup);
        }
    };
    let write =
        DeliveryWriteCommand::Merge(DeliveryMergeWriteCommand::RecordPreflightResult(request));
    match execute_exact_write(&dependencies.writer, write).await {
        ExactWriteResult::Confirmed(DeliveryMergeWriteOutcome::RecordPreflightResult(
            MergeTransitionOutcome::Applied(transition)
            | MergeTransitionOutcome::Existing(transition),
        )) if transition.operation_id == operation_id
            && transition.state == expected_state
            && transition.version
                == DeliveryVersion::try_new(3).expect("version three is valid") =>
        {
            let outcome = durable_operation(operation_id, durability, transition.state);
            if retained_process_cleanup {
                retain_and_fail_closed(lease, outcome)
            } else if reconciliation_required
                || transition.state == MergeOperationState::ReconciliationRequired
            {
                poison_and_release(lease, outcome)
            } else {
                clean_and_release(lease, outcome)
            }
        }
        ExactWriteResult::KnownNotApplied { .. } if retained_process_cleanup => {
            retain_and_fail_closed(lease, retry_pending(operation_id, durability))
        }
        ExactWriteResult::KnownNotApplied { .. } if reconciliation_required => {
            poison_and_release(lease, retry_pending(operation_id, durability))
        }
        ExactWriteResult::KnownNotApplied { .. } => {
            clean_and_release(lease, retry_pending(operation_id, durability))
        }
        ExactWriteResult::Unknown => retain_unknown(
            lease,
            DeliveryPreflightOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::OutcomeUnknown,
            ),
        ),
        ExactWriteResult::InvariantConflict | ExactWriteResult::Confirmed(_) => {
            retain_or_poison_inconsistent(lease, retained_process_cleanup)
        }
    }
}

pub(super) async fn persist_unbound_failure(
    dependencies: &DeliveryManagerLiveDependencies,
    task_id: coding_agent_domain::TaskId,
    operation_id: coding_agent_store::DeliveryOperationId,
    durability: DeliveryPreflightDurability,
    failure: DeliveryRuntimeFailure,
    lease: RepositoryControlLease,
) -> PreflightAttemptResult {
    let retained_process_cleanup = failure.requires_retained_repository_ownership();
    persist_unbound_failure_with_retention(
        dependencies,
        task_id,
        operation_id,
        durability,
        failure,
        retained_process_cleanup,
        lease,
    )
    .await
}

pub(super) async fn persist_unbound_runtime_timeout(
    dependencies: &DeliveryManagerLiveDependencies,
    task_id: coding_agent_domain::TaskId,
    operation_id: coding_agent_store::DeliveryOperationId,
    durability: DeliveryPreflightDurability,
    lease: RepositoryControlLease,
) -> PreflightAttemptResult {
    persist_unbound_failure_with_retention(
        dependencies,
        task_id,
        operation_id,
        durability,
        DeliveryRuntimeFailure::Unavailable,
        true,
        lease,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn persist_unbound_failure_with_retention(
    dependencies: &DeliveryManagerLiveDependencies,
    task_id: coding_agent_domain::TaskId,
    operation_id: coding_agent_store::DeliveryOperationId,
    durability: DeliveryPreflightDurability,
    failure: DeliveryRuntimeFailure,
    retained_process_cleanup: bool,
    lease: RepositoryControlLease,
) -> PreflightAttemptResult {
    let expected_state = runtime_failure_state(failure);
    let reconciliation_required = matches!(
        failure,
        DeliveryRuntimeFailure::ReconciliationRequired(_)
            | DeliveryRuntimeFailure::ProcessCleanupUnproven
            | DeliveryRuntimeFailure::Unavailable
    );
    let request = match FailUnboundMergePreflightRequest::try_new(
        task_id,
        operation_id,
        DeliveryVersion::initial(),
        failure.unbound_failure(),
    ) {
        Ok(request) => request,
        Err(_) => {
            return retain_or_poison_inconsistent(lease, retained_process_cleanup);
        }
    };
    let write =
        DeliveryWriteCommand::Merge(DeliveryMergeWriteCommand::FailUnboundPreflight(request));
    match execute_exact_write(&dependencies.writer, write).await {
        ExactWriteResult::Confirmed(DeliveryMergeWriteOutcome::FailUnboundPreflight(
            MergeTransitionOutcome::Applied(transition)
            | MergeTransitionOutcome::Existing(transition),
        )) if transition.operation_id == operation_id
            && transition.state == expected_state
            && transition.version == DeliveryVersion::try_new(2).expect("version two is valid") =>
        {
            let outcome = durable_operation(operation_id, durability, transition.state);
            if retained_process_cleanup {
                retain_and_fail_closed(lease, outcome)
            } else if reconciliation_required
                || transition.state == MergeOperationState::ReconciliationRequired
            {
                poison_and_release(lease, outcome)
            } else {
                clean_and_release(lease, outcome)
            }
        }
        ExactWriteResult::KnownNotApplied { .. } if retained_process_cleanup => {
            retain_and_fail_closed(lease, retry_pending(operation_id, durability))
        }
        ExactWriteResult::KnownNotApplied { .. } if reconciliation_required => {
            poison_and_release(lease, retry_pending(operation_id, durability))
        }
        ExactWriteResult::KnownNotApplied { .. } => {
            clean_and_release(lease, retry_pending(operation_id, durability))
        }
        ExactWriteResult::Unknown => retain_unknown(
            lease,
            DeliveryPreflightOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::OutcomeUnknown,
            ),
        ),
        ExactWriteResult::InvariantConflict | ExactWriteResult::Confirmed(_) => {
            retain_or_poison_inconsistent(lease, retained_process_cleanup)
        }
    }
}

fn retain_or_poison_inconsistent(
    lease: RepositoryControlLease,
    retained_process_cleanup: bool,
) -> PreflightAttemptResult {
    if retained_process_cleanup {
        retain_and_fail_closed(lease, inconsistent_outcome())
    } else {
        poison_and_release(lease, inconsistent_outcome())
    }
}

pub(super) enum ExactWriteResult {
    Confirmed(DeliveryMergeWriteOutcome),
    KnownNotApplied {
        reason: crate::KnownNotAppliedReason,
        error: Option<StoreError>,
    },
    Unknown,
    InvariantConflict,
}

const fn preflight_result_state(result: &MergePreflightResult) -> MergeOperationState {
    match result {
        MergePreflightResult::Ready { .. } => MergeOperationState::PreflightReady,
        MergePreflightResult::Conflict { .. } => MergeOperationState::Conflict,
        MergePreflightResult::Rejected(_) => MergeOperationState::Rejected,
        MergePreflightResult::Stale(_) => MergeOperationState::Stale,
        MergePreflightResult::ReconciliationRequired(_) => {
            MergeOperationState::ReconciliationRequired
        }
    }
}

const fn runtime_failure_state(failure: DeliveryRuntimeFailure) -> MergeOperationState {
    match failure {
        DeliveryRuntimeFailure::Rejected(_) => MergeOperationState::Rejected,
        DeliveryRuntimeFailure::Stale(_) => MergeOperationState::Stale,
        DeliveryRuntimeFailure::ReconciliationRequired(_)
        | DeliveryRuntimeFailure::ProcessCleanupUnproven
        | DeliveryRuntimeFailure::Unavailable => MergeOperationState::ReconciliationRequired,
    }
}

pub(super) async fn execute_exact_write(
    writer: &crate::StoreWriterHandle,
    initial_command: DeliveryWriteCommand,
) -> ExactWriteResult {
    let mut command = initial_command;
    let mut reconciliation = false;
    let mut observed_unknown = false;
    for _ in 0..=MAX_EXACT_RECONCILIATION_ATTEMPTS {
        let exact_command = command.clone();
        let submission = if reconciliation {
            writer.reconcile_delivery(command, Instant::now() + STORE_WRITE_TIMEOUT)
        } else {
            writer.submit_delivery(command, Instant::now() + STORE_WRITE_TIMEOUT)
        };
        let disposition = match timeout(STORE_WRITE_TIMEOUT, submission.completion()).await {
            Ok(completion) => completion.disposition,
            Err(_) => {
                observed_unknown = true;
                command = exact_command;
                reconciliation = true;
                continue;
            }
        };
        match disposition {
            DeliveryDisposition::Confirmed(DeliveryWriteOutcome::Merge(outcome)) => {
                return ExactWriteResult::Confirmed(outcome);
            }
            DeliveryDisposition::Confirmed(_) => return ExactWriteResult::InvariantConflict,
            DeliveryDisposition::KnownNotApplied {
                reason,
                outcome: None,
                error,
            } if !observed_unknown => {
                return ExactWriteResult::KnownNotApplied { reason, error };
            }
            DeliveryDisposition::KnownNotApplied { outcome: None, .. } => {
                command = exact_command;
                reconciliation = true;
            }
            DeliveryDisposition::KnownNotApplied {
                outcome: Some(_), ..
            }
            | DeliveryDisposition::InvariantConflict { .. } => {
                return ExactWriteResult::InvariantConflict;
            }
            DeliveryDisposition::OutcomeUnknown {
                command: replay, ..
            } => {
                observed_unknown = true;
                command = replay;
                reconciliation = true;
            }
        }
    }
    ExactWriteResult::Unknown
}

fn preflight_known_not_applied(
    reason: crate::KnownNotAppliedReason,
    error: Option<StoreError>,
) -> DeliveryPreflightOutcome {
    match error {
        Some(StoreError::IdempotencyConflict) => {
            DeliveryPreflightOutcome::Conflict(DeliveryCommandConflict::IdempotencyConflict)
        }
        Some(StoreError::DeliveryOperationInProgress) => {
            DeliveryPreflightOutcome::Conflict(DeliveryCommandConflict::OperationInProgress)
        }
        Some(StoreError::TaskNotFound) => DeliveryPreflightOutcome::Ineligible(vec![
            crate::DeliveryEligibilityReason::TaskNotFound,
        ]),
        Some(StoreError::TaskNotMergeEligible) => DeliveryPreflightOutcome::Ineligible(vec![
            crate::DeliveryEligibilityReason::TaskNotCompleted,
        ]),
        Some(StoreError::DeliveryReconciliationRequired) => {
            DeliveryPreflightOutcome::Ineligible(vec![
                crate::DeliveryEligibilityReason::ReconciliationRequired,
            ])
        }
        _ if reason == crate::KnownNotAppliedReason::DeadlineBeforeStart => {
            DeliveryPreflightOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::CommandTimedOut,
            )
        }
        _ => DeliveryPreflightOutcome::Unavailable(
            DeliveryPreflightUnavailableReason::StoreUnavailable,
        ),
    }
}

pub(super) fn retry_pending(
    operation_id: coding_agent_store::DeliveryOperationId,
    durability: DeliveryPreflightDurability,
) -> DeliveryPreflightOutcome {
    DeliveryPreflightOutcome::KnownNotAppliedPersisted(DeliveryPreflightRetry::new(
        DeliveryPreflightOperation::new(
            operation_id,
            durability,
            DeliveryPreflightState::PreflightPending,
        ),
        PREFLIGHT_RETRY_AFTER.as_millis() as u64,
    ))
}

pub(super) fn durable_operation(
    operation_id: coding_agent_store::DeliveryOperationId,
    durability: DeliveryPreflightDurability,
    state: MergeOperationState,
) -> DeliveryPreflightOutcome {
    DeliveryPreflightOutcome::Durable(DeliveryPreflightOperation::new(
        operation_id,
        durability,
        projected_state(state),
    ))
}

const fn projected_state(state: MergeOperationState) -> DeliveryPreflightState {
    match state {
        MergeOperationState::PreflightPending => DeliveryPreflightState::PreflightPending,
        MergeOperationState::PreflightReady => DeliveryPreflightState::PreflightReady,
        MergeOperationState::Conflict => DeliveryPreflightState::Conflict,
        MergeOperationState::Rejected => DeliveryPreflightState::Rejected,
        MergeOperationState::Stale => DeliveryPreflightState::Stale,
        MergeOperationState::Superseded => DeliveryPreflightState::Superseded,
        MergeOperationState::Accepted => DeliveryPreflightState::Accepted,
        MergeOperationState::MergePending => DeliveryPreflightState::MergePending,
        MergeOperationState::Merged => DeliveryPreflightState::Merged,
        MergeOperationState::AbortPending => DeliveryPreflightState::AbortPending,
        MergeOperationState::Failed => DeliveryPreflightState::Failed,
        MergeOperationState::ReconciliationRequired => {
            DeliveryPreflightState::ReconciliationRequired
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_retry_is_bounded_and_never_claims_terminal_state() {
        let operation_id = coding_agent_store::DeliveryOperationId::new();
        let outcome = retry_pending(operation_id, DeliveryPreflightDurability::Existing);
        let DeliveryPreflightOutcome::KnownNotAppliedPersisted(retry) = outcome else {
            panic!("pending retry must remain typed");
        };
        assert_eq!(retry.operation().operation_id(), operation_id);
        assert_eq!(
            retry.operation().state(),
            DeliveryPreflightState::PreflightPending
        );
        assert_eq!(retry.retry_after_millis(), 100);
    }
}
