mod delete;
mod remove;

use super::super::DeliveryError;

wire_enum!(WorktreeDisposition {
    RetainedLocked => "retained_locked",
    RetainedUnlocked => "retained_unlocked",
    Removed => "removed",
    ReconciliationRequired => "reconciliation_required",
});

impl WorktreeDisposition {
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (
                Self::RetainedLocked,
                Self::RetainedUnlocked | Self::ReconciliationRequired
            ) | (
                Self::RetainedUnlocked,
                Self::Removed | Self::ReconciliationRequired
            ) | (Self::Removed, Self::ReconciliationRequired)
        )
    }

    pub const fn is_reconciliation(self) -> bool {
        matches!(self, Self::ReconciliationRequired)
    }
}

wire_enum!(BranchDisposition {
    Retained => "retained",
    Deleted => "deleted",
    ReconciliationRequired => "reconciliation_required",
});

impl BranchDisposition {
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Retained, Self::Deleted | Self::ReconciliationRequired)
                | (Self::Deleted, Self::ReconciliationRequired)
        )
    }

    pub const fn is_reconciliation(self) -> bool {
        matches!(self, Self::ReconciliationRequired)
    }
}

wire_enum!(CleanupKind {
    RemoveWorktree => "remove_worktree",
    DeleteBranch => "delete_branch",
});

wire_enum!(CleanupOperationState {
    UnlockPending => "unlock_pending",
    UnlockedPendingRemove => "unlocked_pending_remove",
    RemovePending => "remove_pending",
    DeletePending => "delete_pending",
    Completed => "completed",
    Failed => "failed",
    ReconciliationRequired => "reconciliation_required",
});

impl CleanupOperationState {
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (
                Self::UnlockPending,
                Self::UnlockedPendingRemove | Self::Failed | Self::ReconciliationRequired
            ) | (
                Self::UnlockedPendingRemove,
                Self::RemovePending | Self::ReconciliationRequired
            ) | (
                Self::RemovePending,
                Self::Completed | Self::Failed | Self::ReconciliationRequired
            ) | (
                Self::DeletePending,
                Self::Completed | Self::Failed | Self::ReconciliationRequired
            )
        )
    }

    pub const fn is_side_effect_active(self) -> bool {
        matches!(
            self,
            Self::UnlockPending
                | Self::UnlockedPendingRemove
                | Self::RemovePending
                | Self::DeletePending
        )
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed)
    }

    pub const fn is_reconciliation(self) -> bool {
        matches!(self, Self::ReconciliationRequired)
    }
}

/// One validated cleanup operation phase coupled to the Git facts already proven.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CleanupState {
    kind: CleanupKind,
    operation: CleanupOperationState,
    worktree: WorktreeDisposition,
    branch: BranchDisposition,
}

impl CleanupState {
    pub fn try_new(
        kind: CleanupKind,
        operation: CleanupOperationState,
        worktree: WorktreeDisposition,
        branch: BranchDisposition,
    ) -> Result<Self, DeliveryError> {
        validate_cleanup_state(kind, operation, worktree, branch)?;
        Ok(Self {
            kind,
            operation,
            worktree,
            branch,
        })
    }

    pub const fn kind(self) -> CleanupKind {
        self.kind
    }

    pub const fn operation(self) -> CleanupOperationState {
        self.operation
    }

    pub const fn worktree(self) -> WorktreeDisposition {
        self.worktree
    }

    pub const fn branch(self) -> BranchDisposition {
        self.branch
    }

    pub fn can_transition_to(self, next: Self) -> bool {
        validate_cleanup_transition(self, next).is_ok()
    }
}

pub fn validate_cleanup_state(
    kind: CleanupKind,
    state: CleanupOperationState,
    worktree: WorktreeDisposition,
    branch: BranchDisposition,
) -> Result<(), DeliveryError> {
    let valid = match kind {
        CleanupKind::RemoveWorktree => remove::state_is_valid(state, worktree, branch),
        CleanupKind::DeleteBranch => delete::state_is_valid(state, worktree, branch),
    };
    valid
        .then_some(())
        .ok_or(DeliveryError::InvalidStateCombination)
}

pub fn validate_cleanup_transition(
    from: CleanupState,
    to: CleanupState,
) -> Result<(), DeliveryError> {
    validate_cleanup_state(from.kind, from.operation, from.worktree, from.branch)?;
    validate_cleanup_state(to.kind, to.operation, to.worktree, to.branch)?;
    if from.kind != to.kind {
        return Err(DeliveryError::InvalidStateCombination);
    }

    let valid = match from.kind {
        CleanupKind::RemoveWorktree => remove::transition_is_valid(from, to),
        CleanupKind::DeleteBranch => delete::transition_is_valid(from, to),
    };
    valid
        .then_some(())
        .ok_or(DeliveryError::InvalidStateCombination)
}
