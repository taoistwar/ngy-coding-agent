use crate::delivery::{
    DeliverySourceRecord, DeliverySourceState, DeliveryVersion, MergeOperationRecord,
    MergeOperationState,
};

use super::super::model::EnterMergePendingRequest;

pub(super) fn accepted_input_matches(
    operation: &MergeOperationRecord,
    source: &DeliverySourceRecord,
    request: &EnterMergePendingRequest,
) -> bool {
    operation.state == MergeOperationState::Accepted
        && operation.version == request.expected_version
        && operation.delivery_source_task_id.is_none()
        && operation.source_commit.is_none()
        && operation.expected_merge_commit.is_none()
        && operation.failure_code.is_none()
        && operation.abort_child_receipt_id.is_none()
        && source.state == DeliverySourceState::Committed
        && source.failure_code.is_none()
        && source.provenance == operation.provenance
        && source.candidate_tree == operation.candidate_tree
        && object_proof_matches(operation, source, request)
}

pub(super) fn pending_facts_match(
    operation: &MergeOperationRecord,
    source: &DeliverySourceRecord,
    request: &EnterMergePendingRequest,
    target_version: DeliveryVersion,
) -> bool {
    operation.version.get() >= target_version.get()
        && operation.delivery_source_task_id == Some(request.task_id)
        && operation.source_commit.as_ref() == source.expected_source_commit.as_ref()
        && operation.expected_merge_commit.as_ref() == Some(&request.proof.expected_merge_commit)
        && source.provenance == operation.provenance
        && source.candidate_tree == operation.candidate_tree
        && object_proof_matches(operation, source, request)
}

fn object_proof_matches(
    operation: &MergeOperationRecord,
    source: &DeliverySourceRecord,
    request: &EnterMergePendingRequest,
) -> bool {
    let Some(source_commit) = source.expected_source_commit.as_ref() else {
        return false;
    };
    operation.candidate_merge_tree.as_ref() == Some(&request.proof.tree)
        && request.proof.parents[0] == operation.expected_target_head
        && request.proof.parents[1] == *source_commit
        && operation.merge_metadata.as_ref() == Some(&request.proof.metadata)
        && request.proof.expected_merge_commit.algorithm()
            == operation.provenance.base_commit.algorithm()
}
