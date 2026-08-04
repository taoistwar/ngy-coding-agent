use crate::delivery::MergeOperationState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreflightRejectedReason {
    TaskNotMergeEligible,
    TargetBranchDetached,
    TargetBranchMismatch,
    TargetWorktreeDirty,
    TargetIgnoredPathCollision,
    TargetGitOperationInProgress,
    UnsafeGitConfiguration,
    UnsupportedGitAttributes,
    SourceAlreadyInTarget,
}

impl PreflightRejectedReason {
    pub const fn as_failure_code(self) -> &'static str {
        match self {
            Self::TaskNotMergeEligible => "TASK_NOT_MERGE_ELIGIBLE",
            Self::TargetBranchDetached => "TARGET_BRANCH_DETACHED",
            Self::TargetBranchMismatch => "TARGET_BRANCH_MISMATCH",
            Self::TargetWorktreeDirty => "TARGET_WORKTREE_DIRTY",
            Self::TargetIgnoredPathCollision => "TARGET_IGNORED_PATH_COLLISION",
            Self::TargetGitOperationInProgress => "TARGET_GIT_OPERATION_IN_PROGRESS",
            Self::UnsafeGitConfiguration => "UNSAFE_GIT_CONFIGURATION",
            Self::UnsupportedGitAttributes => "UNSUPPORTED_GIT_ATTRIBUTES",
            Self::SourceAlreadyInTarget => "SOURCE_ALREADY_IN_TARGET",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeReconciliationReason {
    DeliveryStateInconsistent,
    SourceInconsistent,
    ProcessTreeCleanupFailed,
    WorktreeIdentityMismatch,
    UnsafeGitConfiguration,
    UnsupportedGitAttributes,
}

impl MergeReconciliationReason {
    pub const fn as_failure_code(self) -> &'static str {
        match self {
            Self::DeliveryStateInconsistent => "DELIVERY_RECONCILIATION_REQUIRED",
            Self::SourceInconsistent => "DELIVERY_SOURCE_INCONSISTENT",
            Self::ProcessTreeCleanupFailed => "PROCESS_TREE_CLEANUP_FAILED",
            Self::WorktreeIdentityMismatch => "WORKTREE_IDENTITY_MISMATCH",
            Self::UnsafeGitConfiguration => "UNSAFE_GIT_CONFIGURATION",
            Self::UnsupportedGitAttributes => "UNSUPPORTED_GIT_ATTRIBUTES",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeKnownNotAppliedReason {
    TaskNotMergeEligible,
    TargetBranchDetached,
    TargetBranchMismatch,
    TargetWorktreeDirty,
    TargetIgnoredPathCollision,
    TargetGitOperationInProgress,
    UnsafeGitConfiguration,
    UnsupportedGitAttributes,
    SourceAlreadyInTarget,
    TargetHeadChanged,
    CommandTimedOut,
}

impl MergeKnownNotAppliedReason {
    pub const fn as_failure_code(self) -> &'static str {
        match self {
            Self::TaskNotMergeEligible => "TASK_NOT_MERGE_ELIGIBLE",
            Self::TargetBranchDetached => "TARGET_BRANCH_DETACHED",
            Self::TargetBranchMismatch => "TARGET_BRANCH_MISMATCH",
            Self::TargetWorktreeDirty => "TARGET_WORKTREE_DIRTY",
            Self::TargetIgnoredPathCollision => "TARGET_IGNORED_PATH_COLLISION",
            Self::TargetGitOperationInProgress => "TARGET_GIT_OPERATION_IN_PROGRESS",
            Self::UnsafeGitConfiguration => "UNSAFE_GIT_CONFIGURATION",
            Self::UnsupportedGitAttributes => "UNSUPPORTED_GIT_ATTRIBUTES",
            Self::SourceAlreadyInTarget => "SOURCE_ALREADY_IN_TARGET",
            Self::TargetHeadChanged => "TARGET_HEAD_CHANGED",
            Self::CommandTimedOut => "COMMAND_TIMED_OUT",
        }
    }
}

pub(in crate::delivery) fn merge_failure_code_is_valid(
    state: MergeOperationState,
    failure: Option<&str>,
) -> bool {
    use MergeOperationState::{
        AbortPending, Accepted, Conflict, Failed, MergePending, Merged, PreflightPending,
        PreflightReady, ReconciliationRequired, Rejected, Stale, Superseded,
    };
    match state {
        Conflict => failure == Some("MERGE_CONFLICT"),
        Rejected => failure.is_some_and(preflight_rejected_code_is_allowlisted),
        Stale => failure.is_some_and(stale_code_is_allowlisted),
        Failed => failure.is_some_and(known_zero_effect_failure_is_allowlisted),
        ReconciliationRequired => failure.is_some_and(reconciliation_code_is_allowlisted),
        PreflightPending | PreflightReady | Accepted | MergePending | Merged | AbortPending
        | Superseded => failure.is_none(),
    }
}

fn preflight_rejected_code_is_allowlisted(value: &str) -> bool {
    matches!(
        value,
        "TASK_NOT_MERGE_ELIGIBLE"
            | "TARGET_BRANCH_DETACHED"
            | "TARGET_BRANCH_MISMATCH"
            | "TARGET_WORKTREE_DIRTY"
            | "TARGET_IGNORED_PATH_COLLISION"
            | "TARGET_GIT_OPERATION_IN_PROGRESS"
            | "UNSAFE_GIT_CONFIGURATION"
            | "UNSUPPORTED_GIT_ATTRIBUTES"
            | "SOURCE_ALREADY_IN_TARGET"
    )
}

fn stale_code_is_allowlisted(value: &str) -> bool {
    matches!(
        value,
        "DELIVERY_EVIDENCE_STALE"
            | "TARGET_BRANCH_MISMATCH"
            | "TARGET_HEAD_CHANGED"
            | "DELIVERY_SOURCE_CHANGED"
    )
}

fn known_zero_effect_failure_is_allowlisted(value: &str) -> bool {
    matches!(value, "TARGET_HEAD_CHANGED" | "COMMAND_TIMED_OUT")
        || preflight_rejected_code_is_allowlisted(value)
}

fn reconciliation_code_is_allowlisted(value: &str) -> bool {
    matches!(
        value,
        "DELIVERY_RECONCILIATION_REQUIRED"
            | "DELIVERY_SOURCE_INCONSISTENT"
            | "PROCESS_TREE_CLEANUP_FAILED"
            | "WORKTREE_IDENTITY_MISMATCH"
            | "UNSAFE_GIT_CONFIGURATION"
            | "UNSUPPORTED_GIT_ATTRIBUTES"
    )
}
