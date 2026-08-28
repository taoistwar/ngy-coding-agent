use super::routing::RoutedAccept;
use super::*;

pub(super) struct ValidatedAccept {
    pub(super) routed: RoutedAccept,
}

// Keep both guard-owning outcomes inline. Boxing either branch would add a
// new allocation and a distinct cancellation/drop boundary to admission.
#[allow(clippy::result_large_err)]
pub(super) fn validate(
    flow: &AcceptFlow,
    routed: RoutedAccept,
) -> Result<ValidatedAccept, WorkerFinish> {
    if let Err(outcome) = validate_accept_operation(&routed.context, &flow.command) {
        return Err(routed.admission.clean(&flow.response, outcome));
    }
    let mut blockers = persistent_reasons(&routed.context.snapshot);
    blockers.retain(|reason| *reason != crate::DeliveryEligibilityReason::DeliveryOwned);
    if !blockers.is_empty() {
        return Err(routed.admission.clean(
            &flow.response,
            DeliveryMergeAcceptanceOutcome::Ineligible(blockers),
        ));
    }
    Ok(ValidatedAccept { routed })
}

fn validate_accept_operation<'a>(
    context: &'a DeliveryRecoveryContext,
    command: &AcceptMergeCommandRequest,
) -> Result<&'a coding_agent_store::MergeOperationRecord, DeliveryMergeAcceptanceOutcome> {
    let operation = &context.operation;
    if operation.operation_id != command.preflight_operation_id()
        || operation.provenance.identity.task_id() != command.task_id()
    {
        return Err(inconsistent_accept_outcome());
    }
    if &operation.target_branch != command.target_branch() {
        return Err(DeliveryMergeAcceptanceOutcome::Conflict(
            DeliveryCommandConflict::TargetBranchMismatch,
        ));
    }
    if &operation.expected_target_head != command.expected_target_head() {
        return Err(DeliveryMergeAcceptanceOutcome::Conflict(
            DeliveryCommandConflict::TargetHeadChanged,
        ));
    }
    let Some(evidence) = context.snapshot.evidence_identity.as_ref() else {
        return Err(DeliveryMergeAcceptanceOutcome::Conflict(
            DeliveryCommandConflict::EvidenceStale,
        ));
    };
    if evidence.workspace_generation() != command.expected_review_generation()
        || evidence.workspace_fingerprint() != command.expected_workspace_fingerprint()
        || operation.provenance.evidence != *evidence
    {
        return Err(DeliveryMergeAcceptanceOutcome::Conflict(
            DeliveryCommandConflict::EvidenceStale,
        ));
    }
    if operation.version != command.expected_operation_version() {
        return Err(DeliveryMergeAcceptanceOutcome::Conflict(
            DeliveryCommandConflict::PreflightStale,
        ));
    }
    match operation.state {
        MergeOperationState::PreflightReady => {}
        MergeOperationState::Conflict => {
            return Err(DeliveryMergeAcceptanceOutcome::Conflict(
                DeliveryCommandConflict::MergeConflict,
            ));
        }
        MergeOperationState::PreflightPending
        | MergeOperationState::Accepted
        | MergeOperationState::MergePending
        | MergeOperationState::AbortPending => {
            return Err(DeliveryMergeAcceptanceOutcome::Conflict(
                DeliveryCommandConflict::OperationInProgress,
            ));
        }
        MergeOperationState::Merged => {
            return Err(DeliveryMergeAcceptanceOutcome::Ineligible(vec![
                crate::DeliveryEligibilityReason::AlreadyMerged,
            ]));
        }
        MergeOperationState::ReconciliationRequired => {
            return Err(DeliveryMergeAcceptanceOutcome::Ineligible(vec![
                crate::DeliveryEligibilityReason::ReconciliationRequired,
            ]));
        }
        MergeOperationState::Rejected
        | MergeOperationState::Stale
        | MergeOperationState::Superseded
        | MergeOperationState::Failed => {
            return Err(DeliveryMergeAcceptanceOutcome::Conflict(
                DeliveryCommandConflict::PreflightStale,
            ));
        }
    }
    if operation.delivery_source_task_id.is_some() || context.source.is_some() {
        return Err(inconsistent_accept_outcome());
    }
    Ok(operation)
}
