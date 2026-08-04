/// A worktree cleanup command result whose zero-effect outcome was independently
/// proven by a fresh post-observation. A timeout without that proof must use
/// [`CleanupReconciliationReason::CommandTimedOut`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorktreeCleanupKnownNotAppliedReason {
    TargetWorktreeDirty,
    CommandTimedOut,
}

impl WorktreeCleanupKnownNotAppliedReason {
    pub const fn as_failure_code(self) -> &'static str {
        match self {
            Self::TargetWorktreeDirty => "TARGET_WORKTREE_DIRTY",
            Self::CommandTimedOut => "COMMAND_TIMED_OUT",
        }
    }
}

/// A branch cleanup result proven by a fresh post-observation to have deleted
/// no ref. In particular, [`Self::CommandTimedOut`] is valid only after that
/// observation proves the atomic ref transaction deleted nothing; callers must
/// route a bare or otherwise outcome-unknown timeout through
/// [`CleanupReconciliationReason::CommandTimedOut`] instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchCleanupKnownNotAppliedReason {
    SourceBranchNotMerged,
    CommandTimedOut,
}

impl BranchCleanupKnownNotAppliedReason {
    pub const fn as_failure_code(self) -> &'static str {
        match self {
            Self::SourceBranchNotMerged => "SOURCE_BRANCH_NOT_MERGED",
            Self::CommandTimedOut => "COMMAND_TIMED_OUT",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupReconciliationReason {
    DeliveryStateInconsistent,
    SourceInconsistent,
    ProcessTreeCleanupFailed,
    WorktreeIdentityMismatch,
    UnsafeGitConfiguration,
    UnsupportedGitAttributes,
    CommandTimedOut,
}

impl CleanupReconciliationReason {
    pub const fn as_failure_code(self) -> &'static str {
        match self {
            Self::DeliveryStateInconsistent => "DELIVERY_RECONCILIATION_REQUIRED",
            Self::SourceInconsistent => "DELIVERY_SOURCE_INCONSISTENT",
            Self::ProcessTreeCleanupFailed => "PROCESS_TREE_CLEANUP_FAILED",
            Self::WorktreeIdentityMismatch => "WORKTREE_IDENTITY_MISMATCH",
            Self::UnsafeGitConfiguration => "UNSAFE_GIT_CONFIGURATION",
            Self::UnsupportedGitAttributes => "UNSUPPORTED_GIT_ATTRIBUTES",
            Self::CommandTimedOut => "COMMAND_TIMED_OUT",
        }
    }
}
