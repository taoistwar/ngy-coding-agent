use crate::StoreError;
use crate::delivery::ownership::{load_disposition_exact, load_source_exact};
use crate::delivery::{
    ArtifactDispositionRecord, DeliverySourceRecord, DeliverySourceState, MergeOperationRecord,
    MergeOperationState,
};

use super::super::merge_invariant;
use super::super::model::CompleteMergeRequest;

pub(super) fn fresh_input_matches(
    operation: &MergeOperationRecord,
    request: &CompleteMergeRequest,
) -> bool {
    operation.state == MergeOperationState::MergePending
        && operation.version == request.expected_version
        && operation.failure_code.is_none()
        && operation.abort_child_receipt_id.is_none()
}

pub(super) async fn require_committed_source(
    connection: &mut sqlx::SqliteConnection,
    operation: &MergeOperationRecord,
) -> Result<DeliverySourceRecord, StoreError> {
    let source = load_source_exact(connection, operation.provenance.identity.task_id())
        .await?
        .ok_or_else(merge_invariant)?;
    let exact = source.state == DeliverySourceState::Committed
        && source.failure_code.is_none()
        && source.provenance == operation.provenance
        && operation
            .preflight_inputs
            .as_ref()
            .is_some_and(|inputs| source.candidate_tree == inputs.candidate_tree)
        && source.expected_source_commit.as_ref() == operation.source_commit.as_ref()
        && operation.delivery_source_task_id == Some(operation.provenance.identity.task_id());
    if exact {
        Ok(source)
    } else {
        Err(merge_invariant())
    }
}

pub(super) async fn require_merged_disposition(
    connection: &mut sqlx::SqliteConnection,
    operation: &MergeOperationRecord,
) -> Result<ArtifactDispositionRecord, StoreError> {
    load_disposition_exact(connection, operation.provenance.identity.task_id())
        .await?
        .ok_or_else(merge_invariant)
}

pub(super) fn applied_proof_matches(
    operation: &MergeOperationRecord,
    source: &DeliverySourceRecord,
    request: &CompleteMergeRequest,
) -> bool {
    let proof = &request.proof;
    operation.expected_merge_commit.as_ref() == Some(&proof.object.expected_merge_commit)
        && operation.candidate_merge_tree.as_ref() == Some(&proof.object.tree)
        && proof.object.parents[0] == operation.expected_target_head
        && proof.object.parents[1] == proof.source_oid
        && operation.merge_metadata.as_ref() == Some(&proof.object.metadata)
        && proof.target_branch == operation.target_branch
        && proof.target_head == proof.object.expected_merge_commit
        && proof.source_branch == operation.provenance.source_branch
        && operation.source_commit.as_ref() == Some(&proof.source_oid)
        && source.expected_source_commit.as_ref() == Some(&proof.source_oid)
        && proof.common_git_identity == operation.provenance.common_git_identity
        && proof.worktree_admin_identity == operation.provenance.worktree_admin_identity
        && proof.fixed_lock_reason == operation.provenance.fixed_lock_reason
        && proof.config_attributes_digest == operation.provenance.config_attributes_digest
        && proof.index_tree == proof.object.tree
        && proof.worktree_tree == proof.object.tree
}
