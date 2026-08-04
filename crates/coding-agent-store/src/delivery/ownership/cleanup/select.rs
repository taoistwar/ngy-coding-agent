use crate::StoreError;

use super::super::super::{
    ArtifactDispositionRecord, CleanupOperationRecord, CleanupOperationState,
};
use super::super::ownership_invariant;

pub(in crate::delivery::ownership) fn validate_cleanup_slot_exclusivity(
    operations: &[CleanupOperationRecord],
) -> Result<(), StoreError> {
    let active = operations
        .iter()
        .filter(|operation| operation.state.is_side_effect_active())
        .count();
    let reconciliation = operations
        .iter()
        .filter(|operation| operation.state == CleanupOperationState::ReconciliationRequired)
        .count();
    if active <= 1 && reconciliation <= 1 && (active == 0 || reconciliation == 0) {
        Ok(())
    } else {
        Err(ownership_invariant())
    }
}

pub(in crate::delivery::ownership) fn project_cleanup_operations(
    operations: &[CleanupOperationRecord],
    disposition: Option<&ArtifactDispositionRecord>,
) -> Vec<CleanupOperationRecord> {
    let mut projected = operations
        .iter()
        .filter(|operation| {
            operation.state.is_side_effect_active()
                || operation.state == CleanupOperationState::ReconciliationRequired
        })
        .cloned()
        .collect::<Vec<_>>();
    for kind in [
        crate::delivery::CleanupKind::RemoveWorktree,
        crate::delivery::CleanupKind::DeleteBranch,
    ] {
        if let Some(operation) = operations
            .iter()
            .rev()
            .find(|operation| operation.kind == kind && operation.state.is_terminal())
            && !projected
                .iter()
                .any(|candidate| candidate.operation_id == operation.operation_id)
        {
            projected.push(operation.clone());
        }
    }
    if let Some(disposition) = disposition {
        for pointer in [
            disposition.worktree_cleanup_operation_id,
            disposition.branch_cleanup_operation_id,
        ]
        .into_iter()
        .flatten()
        {
            if !projected
                .iter()
                .any(|operation| operation.operation_id == pointer)
                && let Some(operation) = operations
                    .iter()
                    .find(|operation| operation.operation_id == pointer)
            {
                projected.push(operation.clone());
            }
        }
    }
    projected.sort_by_key(|operation| operation.initial_transition_id);
    projected
}
