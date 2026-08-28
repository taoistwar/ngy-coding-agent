use coding_agent_store::{
    BranchDisposition, CleanupOperationRecord, DeliveryEligibilitySnapshot, MergeOperationRecord,
    PersistentEligibilityBlocker, WorktreeDisposition,
};

use crate::delivery_api_projection::DeliveryProjectionDecision;
use crate::{DeliveryAllowedAction, DeliveryEligibilityReason};

use super::super::operation_query::project_merge_operation;

pub(super) fn build(
    snapshot: &DeliveryEligibilitySnapshot,
    mut ineligible: Vec<DeliveryEligibilityReason>,
    mut unavailable: Vec<DeliveryEligibilityReason>,
) -> DeliveryProjectionDecision {
    let operation_actions = operation_allowed_actions(snapshot);
    normalize_owned_reasons(
        latest_merge_operation(snapshot),
        &operation_actions,
        &mut ineligible,
    );
    if !unavailable.is_empty() {
        unavailable.extend(ineligible);
        DeliveryProjectionDecision::Unavailable(unavailable)
    } else if !ineligible.is_empty() {
        DeliveryProjectionDecision::Ineligible(ineligible)
    } else if operation_actions.is_empty() {
        DeliveryProjectionDecision::Ineligible(vec![DeliveryEligibilityReason::DeliveryOwned])
    } else {
        DeliveryProjectionDecision::Eligible(operation_actions)
    }
}

pub(super) fn latest_merge_operation(
    snapshot: &DeliveryEligibilitySnapshot,
) -> Option<&MergeOperationRecord> {
    snapshot
        .ownership
        .merge_operations
        .iter()
        .max_by_key(|operation| operation.initial_transition_id)
}

pub(super) fn latest_cleanup_operation(
    snapshot: &DeliveryEligibilitySnapshot,
) -> Option<&CleanupOperationRecord> {
    snapshot
        .ownership
        .cleanup_operations
        .iter()
        .max_by_key(|operation| operation.initial_transition_id)
}

fn operation_allowed_actions(snapshot: &DeliveryEligibilitySnapshot) -> Vec<DeliveryAllowedAction> {
    let Some(merge) = latest_merge_operation(snapshot) else {
        return vec![DeliveryAllowedAction::RunPreflight];
    };
    if merge.state != coding_agent_store::MergeOperationState::Merged {
        return project_merge_operation(merge).allowed_actions().to_vec();
    }
    let Some(disposition) = snapshot.ownership.disposition.as_ref() else {
        return Vec::new();
    };
    if latest_cleanup_operation(snapshot)
        .is_some_and(|cleanup| cleanup.state.is_side_effect_active())
    {
        return Vec::new();
    }
    match (disposition.worktree_state, disposition.branch_state) {
        (
            WorktreeDisposition::RetainedLocked | WorktreeDisposition::RetainedUnlocked,
            BranchDisposition::Retained,
        ) => vec![DeliveryAllowedAction::RemoveWorktree],
        (WorktreeDisposition::Removed, BranchDisposition::Retained) => {
            vec![DeliveryAllowedAction::DeleteBranch]
        }
        _ => Vec::new(),
    }
}

fn normalize_owned_reasons(
    operation: Option<&MergeOperationRecord>,
    actions: &[DeliveryAllowedAction],
    reasons: &mut Vec<DeliveryEligibilityReason>,
) {
    if operation.is_some() && !actions.is_empty() {
        reasons.retain(|reason| {
            !matches!(
                reason,
                DeliveryEligibilityReason::DeliveryOwned | DeliveryEligibilityReason::AlreadyMerged
            )
        });
    }
}

pub(in crate::delivery_manager) fn persistent_reasons(
    snapshot: &DeliveryEligibilitySnapshot,
) -> Vec<DeliveryEligibilityReason> {
    snapshot
        .persistent_blockers
        .iter()
        .map(|reason| match reason {
            PersistentEligibilityBlocker::TaskNotCompleted => {
                DeliveryEligibilityReason::TaskNotCompleted
            }
            PersistentEligibilityBlocker::ReviewNotApproved => {
                DeliveryEligibilityReason::ReviewNotApproved
            }
            PersistentEligibilityBlocker::ApprovedEvidenceMissing => {
                DeliveryEligibilityReason::ApprovedEvidenceMissing
            }
            PersistentEligibilityBlocker::AttemptArtifactMissing => {
                DeliveryEligibilityReason::AttemptArtifactMissing
            }
            PersistentEligibilityBlocker::AttemptArtifactNotReady => {
                DeliveryEligibilityReason::AttemptArtifactNotReady
            }
            PersistentEligibilityBlocker::DeliveryOwned => DeliveryEligibilityReason::DeliveryOwned,
            PersistentEligibilityBlocker::AlreadyMerged => DeliveryEligibilityReason::AlreadyMerged,
            PersistentEligibilityBlocker::ReconciliationRequired => {
                DeliveryEligibilityReason::ReconciliationRequired
            }
        })
        .collect()
}
