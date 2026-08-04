use super::codec::CanonicalRequestHasher;
use super::{
    AcceptMergeCommandRequest, DeleteBranchCommandRequest, DeliveryCommandKind,
    PreflightCommandRequest, RemoveWorktreeCommandRequest,
};
use crate::delivery::Sha256Digest;

pub(super) fn preflight(request: &PreflightCommandRequest) -> Sha256Digest {
    let mut hasher = action_hasher(DeliveryCommandKind::Preflight);
    uuid_field(&mut hasher, "task_id", request.task_id().as_uuid());
    hasher.field("target_branch", request.target_branch().as_str().as_bytes());
    hasher.field(
        "expected_target_head",
        request.expected_target_head().as_str().as_bytes(),
    );
    hasher.finish()
}

pub(super) fn accept_merge(request: &AcceptMergeCommandRequest) -> Sha256Digest {
    let mut hasher = action_hasher(DeliveryCommandKind::AcceptMerge);
    uuid_field(&mut hasher, "task_id", request.task_id().as_uuid());
    uuid_field(
        &mut hasher,
        "preflight_operation_id",
        request.preflight_operation_id().as_uuid(),
    );
    u64_field(
        &mut hasher,
        "expected_operation_version",
        request.expected_operation_version().get(),
    );
    u64_field(
        &mut hasher,
        "expected_review_generation",
        request.expected_review_generation(),
    );
    hasher.field(
        "expected_workspace_fingerprint",
        request.expected_workspace_fingerprint().as_str().as_bytes(),
    );
    hasher.field("target_branch", request.target_branch().as_str().as_bytes());
    hasher.field(
        "expected_target_head",
        request.expected_target_head().as_str().as_bytes(),
    );
    hasher.finish()
}

pub(super) fn remove_worktree(request: &RemoveWorktreeCommandRequest) -> Sha256Digest {
    let mut hasher = action_hasher(DeliveryCommandKind::RemoveWorktree);
    cleanup_fields(
        &mut hasher,
        request.task_id(),
        request.expected_disposition_version(),
        request.expected_merge_operation_id(),
        request.expected_source_ref(),
        request.expected_source_oid(),
    );
    hasher.finish()
}

pub(super) fn delete_branch(request: &DeleteBranchCommandRequest) -> Sha256Digest {
    let mut hasher = action_hasher(DeliveryCommandKind::DeleteBranch);
    cleanup_fields(
        &mut hasher,
        request.task_id(),
        request.expected_disposition_version(),
        request.expected_merge_operation_id(),
        request.expected_source_ref(),
        request.expected_source_oid(),
    );
    hasher.field("target_branch", request.target_branch().as_str().as_bytes());
    hasher.field("target_head", request.target_head().as_str().as_bytes());
    hasher.finish()
}

fn action_hasher(kind: DeliveryCommandKind) -> CanonicalRequestHasher {
    let mut hasher = CanonicalRequestHasher::new();
    hasher.field("action", kind.as_str().as_bytes());
    hasher
}

fn cleanup_fields(
    hasher: &mut CanonicalRequestHasher,
    task_id: coding_agent_domain::TaskId,
    expected_disposition_version: crate::delivery::DeliveryVersion,
    expected_merge_operation_id: crate::delivery::DeliveryOperationId,
    expected_source_ref: &crate::delivery::GitBranchRef,
    expected_source_oid: &crate::delivery::GitCommitOid,
) {
    uuid_field(hasher, "task_id", task_id.as_uuid());
    u64_field(
        hasher,
        "expected_disposition_version",
        expected_disposition_version.get(),
    );
    uuid_field(
        hasher,
        "expected_merge_operation_id",
        expected_merge_operation_id.as_uuid(),
    );
    hasher.field(
        "expected_source_ref",
        expected_source_ref.as_str().as_bytes(),
    );
    hasher.field(
        "expected_source_oid",
        expected_source_oid.as_str().as_bytes(),
    );
}

fn uuid_field(hasher: &mut CanonicalRequestHasher, tag: &str, value: uuid::Uuid) {
    hasher.field(tag, value.as_bytes());
}

fn u64_field(hasher: &mut CanonicalRequestHasher, tag: &str, value: u64) {
    hasher.field(tag, &value.to_be_bytes());
}
