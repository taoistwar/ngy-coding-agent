use std::str::FromStr;

use crate::StoreError;
use crate::delivery::ownership::{
    load_merge_operation_exact, reconciliation_accept_origin_is_exact,
    source_merge_reconciliation_values_match, validate_source_merge_reconciliation_pair,
};
use crate::delivery::{
    DeliveryOperationId, DeliverySourceRecord, DeliverySourceState, MergeOperationRecord,
    MergeOperationState, validate_merge_source_state,
};

use super::model::DeliverySourceAnchor;
use super::proof::{DeliverySourceAppliedProof, DeliverySourceObjectProof};
use super::source_invariant;

pub(super) fn validate_anchor_compatibility(
    source: &DeliverySourceRecord,
    operation: &MergeOperationRecord,
    anchor: DeliverySourceAnchor,
) -> Result<(), StoreError> {
    let immutable_match = source.provenance == operation.provenance
        && operation
            .preflight_inputs
            .as_ref()
            .is_some_and(|inputs| source.candidate_tree == inputs.candidate_tree)
        && source.expected_parent == operation.provenance.base_commit
        && operation.operation_id == anchor.accepted_operation_id
        && operation.provenance.identity.task_id() == anchor.task_id;
    let owner_is_unlinked =
        operation.delivery_source_task_id.is_none() && operation.source_commit.is_none();
    let owner_link_is_exact = operation.delivery_source_task_id
        == Some(source.provenance.identity.task_id())
        && operation.source_commit.as_ref() == source.expected_source_commit.as_ref();
    let owner_history_is_compatible = match operation.state {
        MergeOperationState::Accepted => {
            operation.version == anchor.accepted_receipt_version
                && operation.failure_code.is_none()
                && owner_is_unlinked
        }
        MergeOperationState::MergePending
        | MergeOperationState::Merged
        | MergeOperationState::AbortPending
        | MergeOperationState::Conflict
        | MergeOperationState::Failed => owner_link_is_exact,
        MergeOperationState::ReconciliationRequired => owner_is_unlinked || owner_link_is_exact,
        _ => false,
    };
    if immutable_match && owner_history_is_compatible {
        Ok(())
    } else {
        Err(source_invariant())
    }
}

pub(super) fn validate_replay_anchor(
    source: &DeliverySourceRecord,
    operation: &MergeOperationRecord,
    anchor: DeliverySourceAnchor,
) -> Result<(), StoreError> {
    validate_anchor_compatibility(source, operation, anchor)?;
    let pending_source = matches!(
        source.state,
        DeliverySourceState::ObjectPending | DeliverySourceState::CommitPending
    );
    if pending_source && operation.state != MergeOperationState::Accepted {
        Err(source_invariant())
    } else {
        Ok(())
    }
}

pub(super) fn validate_mutation_owner(
    source: &DeliverySourceRecord,
    operation: &MergeOperationRecord,
    anchor: DeliverySourceAnchor,
) -> Result<(), StoreError> {
    validate_anchor_compatibility(source, operation, anchor)?;
    validate_merge_source_state(operation.state, Some(source.state))
        .map_err(|_| source_invariant())?;
    let valid = match source.state {
        DeliverySourceState::ObjectPending
        | DeliverySourceState::CommitPending
        | DeliverySourceState::Committed => {
            operation.state == MergeOperationState::Accepted
                && operation.version == anchor.accepted_receipt_version
                && operation.failure_code.is_none()
        }
        DeliverySourceState::ReconciliationRequired => {
            source_merge_reconciliation_values_match(source, operation)
        }
    };
    if valid {
        Ok(())
    } else {
        Err(source_invariant())
    }
}

pub(super) async fn validate_current_source_reconciliation(
    connection: &mut sqlx::SqliteConnection,
    source: &DeliverySourceRecord,
) -> Result<(), StoreError> {
    if source.state != DeliverySourceState::ReconciliationRequired {
        return Ok(());
    }
    let ids: Vec<String> = sqlx::query_scalar(
        "SELECT operation_id FROM task_merge_operations \
         WHERE task_id = ? AND state = 'reconciliation_required' \
         ORDER BY operation_id LIMIT 2",
    )
    .bind(source.provenance.identity.task_id().to_string())
    .fetch_all(&mut *connection)
    .await?;
    if ids.len() != 1 {
        return Err(source_invariant());
    }
    let operation_id = DeliveryOperationId::from_str(&ids[0]).map_err(|_| source_invariant())?;
    let operation = load_merge_operation_exact(connection, operation_id).await?;
    let immutable_match = source.provenance == operation.provenance
        && operation
            .preflight_inputs
            .as_ref()
            .is_some_and(|inputs| source.candidate_tree == inputs.candidate_tree)
        && source.expected_parent == operation.provenance.base_commit;
    let owner_is_unlinked =
        operation.delivery_source_task_id.is_none() && operation.source_commit.is_none();
    let owner_link_is_exact = operation.delivery_source_task_id
        == Some(source.provenance.identity.task_id())
        && operation.source_commit.as_ref() == source.expected_source_commit.as_ref();
    let pair_is_exact =
        validate_source_merge_reconciliation_pair(&mut *connection, source, &operation)
            .await
            .is_ok()
            && reconciliation_accept_origin_is_exact(connection, &operation).await?;
    if immutable_match && (owner_is_unlinked || owner_link_is_exact) && pair_is_exact {
        Ok(())
    } else {
        Err(source_invariant())
    }
}

pub(super) fn validate_pending_source(
    source: &DeliverySourceRecord,
    operation: &MergeOperationRecord,
    anchor: DeliverySourceAnchor,
    expected_state: DeliverySourceState,
    expected_version: crate::delivery::DeliveryVersion,
) -> Result<(), StoreError> {
    validate_mutation_owner(source, operation, anchor)?;
    if source.state == expected_state
        && source.version == expected_version
        && expected_state.is_side_effect_active()
    {
        Ok(())
    } else {
        Err(source_invariant())
    }
}

pub(super) fn validate_object_proof(
    source: &DeliverySourceRecord,
    proof: &DeliverySourceObjectProof,
) -> Result<(), StoreError> {
    let expected_commit_matches = source
        .expected_source_commit
        .as_ref()
        .is_none_or(|expected| expected == &proof.expected_source_commit);
    let valid = expected_commit_matches
        && proof.expected_source_commit.algorithm() == source.provenance.base_commit.algorithm()
        && proof.tree == source.candidate_tree
        && proof.parents.as_slice() == [source.expected_parent.clone()]
        && proof.metadata == source.commit_metadata;
    if valid {
        Ok(())
    } else {
        Err(source_invariant())
    }
}

pub(super) fn validate_applied_proof(
    source: &DeliverySourceRecord,
    proof: &DeliverySourceAppliedProof,
) -> Result<(), StoreError> {
    validate_object_proof(source, &proof.object)?;
    let expected_commit = source
        .expected_source_commit
        .as_ref()
        .ok_or_else(source_invariant)?;
    let worktree = &proof.worktree;
    let valid = proof.symbolic_source_ref == source.provenance.source_branch
        && proof.source_ref_oid == *expected_commit
        && proof.head_oid == *expected_commit
        && worktree.index_tree == source.candidate_tree
        && worktree.worktree_tree == source.candidate_tree
        && worktree.staged_entry_count == 0
        && worktree.unstaged_entry_count == 0
        && worktree.untracked_entry_count == 0
        && worktree.unmerged_entry_count == 0
        && proof.common_git_identity == source.provenance.common_git_identity
        && proof.worktree_admin_identity == source.provenance.worktree_admin_identity
        && proof.fixed_lock_reason == source.provenance.fixed_lock_reason
        && proof.config_attributes_digest == source.provenance.config_attributes_digest;
    if valid {
        Ok(())
    } else {
        Err(source_invariant())
    }
}
