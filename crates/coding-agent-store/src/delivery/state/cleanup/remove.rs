use super::{BranchDisposition, CleanupOperationState, CleanupState, WorktreeDisposition};

pub(super) fn state_is_valid(
    state: CleanupOperationState,
    worktree: WorktreeDisposition,
    branch: BranchDisposition,
) -> bool {
    use CleanupOperationState::{
        Completed, Failed, ReconciliationRequired, RemovePending, UnlockPending,
        UnlockedPendingRemove,
    };
    use WorktreeDisposition::{
        ReconciliationRequired as WorktreeReconciliation, Removed, RetainedLocked, RetainedUnlocked,
    };

    branch == BranchDisposition::Retained
        && matches!(
            (state, worktree),
            (UnlockPending, RetainedLocked)
                | (UnlockedPendingRemove | RemovePending, RetainedUnlocked)
                | (Completed, Removed)
                | (Failed, RetainedLocked | RetainedUnlocked)
                | (ReconciliationRequired, WorktreeReconciliation)
        )
}

pub(super) fn transition_is_valid(from: CleanupState, to: CleanupState) -> bool {
    use BranchDisposition::Retained;
    use CleanupOperationState::{
        Completed, Failed, ReconciliationRequired, RemovePending, UnlockPending,
        UnlockedPendingRemove,
    };
    use WorktreeDisposition::{
        ReconciliationRequired as WorktreeReconciliation, Removed, RetainedLocked, RetainedUnlocked,
    };

    matches!(
        (
            from.operation(),
            from.worktree(),
            from.branch(),
            to.operation(),
            to.worktree(),
            to.branch(),
        ),
        (
            UnlockPending,
            RetainedLocked,
            Retained,
            UnlockedPendingRemove,
            RetainedUnlocked,
            Retained
        ) | (
            UnlockPending,
            RetainedLocked,
            Retained,
            Failed,
            RetainedLocked,
            Retained
        ) | (
            UnlockPending | UnlockedPendingRemove | RemovePending,
            RetainedLocked | RetainedUnlocked,
            Retained,
            ReconciliationRequired,
            WorktreeReconciliation,
            Retained
        ) | (
            UnlockedPendingRemove,
            RetainedUnlocked,
            Retained,
            RemovePending,
            RetainedUnlocked,
            Retained
        ) | (
            RemovePending,
            RetainedUnlocked,
            Retained,
            Completed,
            Removed,
            Retained
        ) | (
            RemovePending,
            RetainedUnlocked,
            Retained,
            Failed,
            RetainedUnlocked,
            Retained
        )
    )
}
