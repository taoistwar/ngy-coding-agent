mod fixture;
mod live;
mod preflight;
pub(crate) mod teardown;

pub use fixture::DeliveryMergeFixture;
pub use live::{LiveCall, LiveFault, LiveStage};

pub const BASE_COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";
pub const TARGET_HEAD: &str = "123456789abcdef0123456789abcdef012345678";
pub const CANDIDATE_TREE: &str = "23456789abcdef0123456789abcdef0123456789";
pub const PREFLIGHT_SOURCE: &str = "3456789abcdef0123456789abcdef0123456789a";
pub const MERGE_BASE: &str = "456789abcdef0123456789abcdef0123456789ab";
pub const MERGE_TREE: &str = "56789abcdef0123456789abcdef0123456789abc";
pub const SOURCE_COMMIT: &str = "6789abcdef0123456789abcdef0123456789abcd";
pub const EXPECTED_MERGE_COMMIT: &str = "789abcdef0123456789abcdef0123456789abcde";
pub const COMMON_IDENTITY: &str =
    "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1";
pub const ADMIN_IDENTITY: &str = "b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2";
pub const SOURCE_CONFIG_DIGEST: &str =
    "c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3";
pub const TARGET_CONFIG_DIGEST: &str =
    "d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4";
pub const TARGET_SECURITY_DIGEST: &str =
    "e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5";
pub const ABORT_INDEX_DIGEST: &str =
    "f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6";
pub const ABORT_WORKTREE_DIGEST: &str =
    "0707070707070707070707070707070707070707070707070707070707070707";
