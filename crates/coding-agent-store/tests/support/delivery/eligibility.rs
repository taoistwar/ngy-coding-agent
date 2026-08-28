// Each integration test compiles the shared support tree independently and
// intentionally consumes only the fixture subset relevant to that test.
#![allow(unused_imports)]

mod cleanup;
mod corruption;
mod merge;
mod parents;

pub use cleanup::{
    complete_branch_cleanup, complete_worktree_cleanup, create_branch_cleanup,
    create_merged_delivery, create_worktree_cleanup, create_worktree_cleanup_with_operation_id,
    fail_branch_cleanup, fail_worktree_cleanup, finish_merged_delivery, reconcile_branch_cleanup,
    reconcile_worktree_cleanup,
};
pub use corruption::{
    MergeCopyCorruption, corrupt_approved_review_without_coverage, corrupt_artifact_attempt,
    corrupt_artifact_state, corrupt_merge_copy, corrupt_merge_evidence, corrupt_transition_ids,
    corrupt_transition_state_pair, delete_artifact_parent,
};
pub use merge::{
    accept_merge, create_committed_source, fail_accepted_merge, finish_preflight_conflict,
    finish_preflight_terminal, insert_preflight, mark_preflight_ready, try_accept_merge_ready,
};
pub use parents::{
    ApprovedEvidenceVariant, FixtureArtifactState, approved_task_on_store,
    approved_task_with_artifact_state, approved_task_with_evidence_variant,
    approved_task_with_prior_rejection, approved_task_with_ready_artifact, rejected_task,
};

pub const BASE_COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";
pub const CANDIDATE_TREE: &str = "123456789abcdef0123456789abcdef012345678";
pub const PREFLIGHT_SOURCE: &str = "3456789abcdef0123456789abcdef0123456789a";
pub const TARGET_HEAD: &str = "23456789abcdef0123456789abcdef0123456789";
pub const MERGE_BASE: &str = "456789abcdef0123456789abcdef0123456789ab";
pub const MERGE_TREE: &str = "56789abcdef0123456789abcdef0123456789abc";
pub const SOURCE_COMMIT: &str = "6789abcdef0123456789abcdef0123456789abcd";
pub const MERGE_COMMIT: &str = "789abcdef0123456789abcdef0123456789abcde";
pub const COMMON_IDENTITY: &str =
    "c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1";
pub const ADMIN_IDENTITY: &str = "d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2";
pub const CONFIG_DIGEST: &str = "e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3";
pub const TARGET_CONFIG_DIGEST: &str =
    "f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4";
pub const TARGET_SECURITY_DIGEST: &str =
    "a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5";
pub const DELIVERY_TIMESTAMP: &str = "2026-08-04T00:00:00.000000000Z";
