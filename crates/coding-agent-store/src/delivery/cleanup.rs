mod accept;
mod branch;
mod model;
mod replay;
mod worktree;

pub use model::{
    BranchCleanupKnownNotAppliedReason, CleanupAcceptanceOutcome, CleanupOperationAnchor,
    CleanupReconciliationReason, CleanupTransitionOutcome, CleanupTransitionReceipt,
    CompleteBranchCleanupRequest, CompleteWorktreeCleanupRequest,
    EnterWorktreeRemovePendingRequest, ReconcileBranchCleanupRequest,
    ReconcileWorktreeCleanupRequest, RecordBranchCleanupFailureRequest,
    RecordWorktreeCleanupFailureRequest, RecordWorktreeUnlockedRequest,
    RefreshBranchCleanupTargetRequest, WorktreeCleanupKnownNotAppliedReason,
};

use crate::StoreError;

const CLEANUP_INVARIANT: &str = "delivery cleanup operation is inconsistent";

fn cleanup_invariant() -> StoreError {
    StoreError::InvariantViolation(CLEANUP_INVARIANT)
}
