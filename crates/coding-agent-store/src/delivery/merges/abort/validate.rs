use crate::delivery::{MergeOperationRecord, MergeOperationState};

use super::super::model::{BeginMergeAbortRequest, CompleteMergeAbortRequest};

pub(super) fn begin_input_matches(
    operation: &MergeOperationRecord,
    request: &BeginMergeAbortRequest,
) -> bool {
    operation.state == MergeOperationState::MergePending
        && operation.version == request.expected_version
        && operation.failure_code.is_none()
        && operation.abort_child_receipt_id.is_none()
        && operation.target_branch == request.proof.target_branch
        && operation.expected_target_head == request.proof.target_head
        && operation.provenance.source_branch == request.proof.source_branch
        && operation.source_commit.as_ref() == Some(&request.proof.source_oid)
        && operation.provenance.common_git_identity == request.proof.common_git_identity
        && operation.provenance.worktree_admin_identity == request.proof.worktree_admin_identity
        && operation.provenance.fixed_lock_reason == request.proof.fixed_lock_reason
        && operation.provenance.config_attributes_digest == request.proof.config_attributes_digest
        && operation.source_commit.as_ref() == Some(&request.proof.merge_head)
}

pub(super) fn abort_facts_match(
    operation: &MergeOperationRecord,
    request: &BeginMergeAbortRequest,
) -> bool {
    operation.abort_child_receipt_id == Some(request.proof.child_receipt_id)
        && operation.abort_merge_head.as_ref() == Some(&request.proof.merge_head)
        && operation.abort_index_stages_digest.as_ref() == Some(&request.proof.index_stages_digest)
        && operation.abort_worktree_digest.as_ref() == Some(&request.proof.worktree_digest)
        && operation.abort_merge_autostash_proof.as_deref() == Some("absent")
        && operation.source_commit.as_ref() == Some(&request.proof.merge_head)
        && operation.target_branch == request.proof.target_branch
        && operation.expected_target_head == request.proof.target_head
        && operation.provenance.source_branch == request.proof.source_branch
        && operation.source_commit.as_ref() == Some(&request.proof.source_oid)
        && operation.provenance.common_git_identity == request.proof.common_git_identity
        && operation.provenance.worktree_admin_identity == request.proof.worktree_admin_identity
        && operation.provenance.fixed_lock_reason == request.proof.fixed_lock_reason
        && operation.provenance.config_attributes_digest == request.proof.config_attributes_digest
}

pub(super) fn abort_applied_proof_matches(
    operation: &MergeOperationRecord,
    request: &CompleteMergeAbortRequest,
) -> bool {
    operation.abort_child_receipt_id.is_some()
        && operation.abort_merge_head.as_ref() == operation.source_commit.as_ref()
        && operation.abort_index_stages_digest.is_some()
        && operation.abort_worktree_digest.is_some()
        && operation.abort_merge_autostash_proof.as_deref() == Some("absent")
        && operation.target_branch == request.proof.target_branch
        && operation.expected_target_head == request.proof.target_head
        && operation.provenance.source_branch == request.proof.source_branch
        && operation.source_commit.as_ref() == Some(&request.proof.source_oid)
        && operation.provenance.common_git_identity == request.proof.common_git_identity
        && operation.provenance.worktree_admin_identity == request.proof.worktree_admin_identity
        && operation.provenance.fixed_lock_reason == request.proof.fixed_lock_reason
        && operation.provenance.config_attributes_digest == request.proof.config_attributes_digest
}
