use super::{BranchDisposition, CleanupOperationState, CleanupState, WorktreeDisposition};

pub(super) fn state_is_valid(
    state: CleanupOperationState,
    worktree: WorktreeDisposition,
    branch: BranchDisposition,
) -> bool {
    use BranchDisposition::{Deleted, ReconciliationRequired as BranchReconciliation, Retained};
    use CleanupOperationState::{Completed, DeletePending, Failed, ReconciliationRequired};

    worktree == WorktreeDisposition::Removed
        && matches!(
            (state, branch),
            (DeletePending | Failed, Retained)
                | (Completed, Deleted)
                | (ReconciliationRequired, BranchReconciliation)
        )
}

pub(super) fn transition_is_valid(from: CleanupState, to: CleanupState) -> bool {
    use BranchDisposition::{Deleted, ReconciliationRequired as BranchReconciliation, Retained};
    use CleanupOperationState::{Completed, DeletePending, Failed, ReconciliationRequired};
    use WorktreeDisposition::Removed;

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
            DeletePending,
            Removed,
            Retained,
            DeletePending,
            Removed,
            Retained
        ) | (
            DeletePending,
            Removed,
            Retained,
            Completed,
            Removed,
            Deleted
        ) | (DeletePending, Removed, Retained, Failed, Removed, Retained)
            | (
                DeletePending,
                Removed,
                Retained,
                ReconciliationRequired,
                Removed,
                BranchReconciliation
            )
    )
}
