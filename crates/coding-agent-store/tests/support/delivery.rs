pub mod cleanup;
pub mod eligibility;
pub mod merge;
pub mod parents;

pub const REPOSITORY_ID: &str = "11111111-1111-4111-8111-111111111111";
pub const TASK_ID: &str = "22222222-2222-4222-8222-222222222222";
pub const TASK_CLIENT_REQUEST_ID: &str = "33333333-3333-4333-8333-333333333333";
pub const MERGE_OPERATION_ID: &str = "44444444-4444-4444-8444-444444444444";
pub const SECOND_MERGE_OPERATION_ID: &str = "44444444-4444-4444-8444-444444444445";
pub const PREFLIGHT_RECEIPT_ID: &str = "55555555-5555-4555-8555-555555555555";
pub const ACCEPT_RECEIPT_ID: &str = "66666666-6666-4666-8666-666666666666";
pub const CLEANUP_OPERATION_ID: &str = "77777777-7777-4777-8777-777777777777";
pub const SECOND_CLEANUP_OPERATION_ID: &str = "77777777-7777-4777-8777-777777777778";
pub const CLEANUP_RECEIPT_ID: &str = "88888888-8888-4888-8888-888888888888";
pub const SECOND_CLEANUP_RECEIPT_ID: &str = "88888888-8888-4888-8888-888888888889";
pub const DELETE_CLEANUP_OPERATION_ID: &str = "99999999-9999-4999-8999-999999999999";
pub const DELETE_CLEANUP_RECEIPT_ID: &str = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
pub const BASE_OID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
pub const CANDIDATE_TREE_OID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
pub const PREFLIGHT_SOURCE_OID: &str = "cccccccccccccccccccccccccccccccccccccccc";
pub const TARGET_HEAD_OID: &str = "dddddddddddddddddddddddddddddddddddddddd";
pub const MERGE_BASE_OID: &str = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
pub const MERGE_TREE_OID: &str = "ffffffffffffffffffffffffffffffffffffffff";
pub const SOURCE_COMMIT_OID: &str = "1111111111111111111111111111111111111111";
pub const MERGE_COMMIT_OID: &str = "2222222222222222222222222222222222222222";
pub const WORKSPACE_FINGERPRINT: &str =
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
pub const CHECKS_DIGEST: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
pub const COVERAGE_DIGEST: &str =
    "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
pub const COMMON_IDENTITY_DIGEST: &str =
    "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
pub const ADMIN_IDENTITY_DIGEST: &str =
    "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
pub const CONFIG_DIGEST: &str = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
pub const TARGET_CONFIG_DIGEST: &str =
    "abababababababababababababababababababababababababababababababab";
pub const TARGET_SECURITY_DIGEST: &str =
    "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
pub const REQUEST_HASH: &str = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
pub const TIMESTAMP: &str = "2026-08-04T00:00:00.000000000Z";
pub const SOURCE_BRANCH: &str = "refs/heads/codex/task";
pub const TARGET_BRANCH: &str = "refs/heads/main";
pub const WORKTREE_PATH: &str = "C:/fixtures/codex-task";

#[derive(Debug, Clone, Copy)]
pub enum SqlTextFixture<'a> {
    Utf8(&'a str),
    RawBytes(&'a [u8]),
}

impl<'a> From<&'a str> for SqlTextFixture<'a> {
    fn from(value: &'a str) -> Self {
        Self::Utf8(value)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PreflightFixture<'a> {
    pub operation_id: &'a str,
    pub receipt_id: &'a str,
    pub command_kind: &'a str,
    pub accepted_version: i64,
    pub accepted_state: &'a str,
    pub response_discriminator: &'a str,
    pub workspace_fingerprint: &'a str,
    pub artifact_worktree_path: &'a str,
    pub candidate_tree_oid: &'a str,
    pub target_branch: SqlTextFixture<'a>,
    pub config_attributes_digest: &'a str,
    pub target_config_attributes_digest: &'a str,
    pub target_security_digest: &'a str,
    pub request_hash: &'a str,
    pub receipt_created_at: &'a str,
}

impl<'a> PreflightFixture<'a> {
    pub fn valid(operation_id: &'a str, receipt_id: &'a str) -> Self {
        Self {
            operation_id,
            receipt_id,
            command_kind: "preflight",
            accepted_version: 1,
            accepted_state: "preflight_pending",
            response_discriminator: "preflight_created",
            workspace_fingerprint: WORKSPACE_FINGERPRINT,
            artifact_worktree_path: WORKTREE_PATH,
            candidate_tree_oid: CANDIDATE_TREE_OID,
            target_branch: TARGET_BRANCH.into(),
            config_attributes_digest: CONFIG_DIGEST,
            target_config_attributes_digest: TARGET_CONFIG_DIGEST,
            target_security_digest: TARGET_SECURITY_DIGEST,
            request_hash: REQUEST_HASH,
            receipt_created_at: TIMESTAMP,
        }
    }
}
