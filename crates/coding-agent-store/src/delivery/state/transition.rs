use std::fmt;

use super::super::{DeliveryError, DeliveryVersion};
use super::cleanup::{
    BranchDisposition, CleanupKind, CleanupOperationState, CleanupState, WorktreeDisposition,
    validate_cleanup_state, validate_cleanup_transition,
};
use super::merge::MergeOperationState;
use super::source::DeliverySourceState;

pub trait DeliveryState: Copy + fmt::Display + PartialEq {
    fn can_transition_to(self, next: Self) -> bool;
    fn can_refresh_in_place(self) -> bool;
}

impl DeliveryState for DeliverySourceState {
    fn can_transition_to(self, next: Self) -> bool {
        self.can_transition_to(next)
    }

    fn can_refresh_in_place(self) -> bool {
        matches!(self, Self::ObjectPending | Self::CommitPending)
    }
}

impl DeliveryState for MergeOperationState {
    fn can_transition_to(self, next: Self) -> bool {
        self.can_transition_to(next)
    }

    fn can_refresh_in_place(self) -> bool {
        false
    }
}

impl DeliveryState for WorktreeDisposition {
    fn can_transition_to(self, next: Self) -> bool {
        self.can_transition_to(next)
    }

    fn can_refresh_in_place(self) -> bool {
        false
    }
}

impl DeliveryState for BranchDisposition {
    fn can_transition_to(self, next: Self) -> bool {
        self.can_transition_to(next)
    }

    fn can_refresh_in_place(self) -> bool {
        false
    }
}

impl DeliveryState for CleanupOperationState {
    fn can_transition_to(self, next: Self) -> bool {
        self.can_transition_to(next)
    }

    fn can_refresh_in_place(self) -> bool {
        self == Self::DeletePending
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateTransition<S> {
    from: Option<S>,
    to: S,
    version: DeliveryVersion,
}

impl<S> StateTransition<S>
where
    S: DeliveryState,
{
    fn try_initial_phase(
        to: S,
        version: DeliveryVersion,
        phase_is_initial: bool,
    ) -> Result<Self, DeliveryError> {
        if version != DeliveryVersion::initial() {
            return Err(DeliveryError::InvalidVersion);
        }
        if !phase_is_initial {
            return Err(DeliveryError::IllegalTransition);
        }
        Ok(Self {
            from: None,
            to,
            version,
        })
    }

    pub fn try_advance(
        from: S,
        to: S,
        previous_version: DeliveryVersion,
        version: DeliveryVersion,
    ) -> Result<Self, DeliveryError> {
        if previous_version.next()? != version {
            return Err(DeliveryError::InvalidVersion);
        }
        if !from.can_transition_to(to) {
            return Err(DeliveryError::IllegalTransition);
        }
        Ok(Self {
            from: Some(from),
            to,
            version,
        })
    }

    /// Records a new durable observation or retry diagnostic without advancing phase.
    ///
    /// Keeping this separate from `can_transition_to` prevents retry metadata from
    /// weakening the phase state machine while still preserving an exact versioned
    /// journal entry for every current-row update.
    pub fn try_observation(
        state: S,
        previous_version: DeliveryVersion,
        version: DeliveryVersion,
    ) -> Result<Self, DeliveryError> {
        if previous_version.next()? != version {
            return Err(DeliveryError::InvalidVersion);
        }
        if !state.can_refresh_in_place() {
            return Err(DeliveryError::IllegalTransition);
        }
        Ok(Self {
            from: Some(state),
            to: state,
            version,
        })
    }

    pub const fn from(&self) -> Option<S> {
        self.from
    }

    pub const fn to(&self) -> S {
        self.to
    }

    pub const fn version(&self) -> DeliveryVersion {
        self.version
    }

    pub fn from_storage_value(&self) -> String {
        self.from
            .map_or_else(|| "absent".to_owned(), |state| state.to_string())
    }
}

/// A versioned cleanup phase transition that cannot be detached from disposition facts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CleanupTransition {
    from: CleanupState,
    to: CleanupState,
    operation: StateTransition<CleanupOperationState>,
}

impl CleanupTransition {
    pub fn try_advance(
        from: CleanupState,
        to: CleanupState,
        previous_version: DeliveryVersion,
        version: DeliveryVersion,
    ) -> Result<Self, DeliveryError> {
        validate_cleanup_transition(from, to)?;
        let operation = if from.operation() == to.operation() {
            StateTransition::try_observation(from.operation(), previous_version, version)?
        } else {
            StateTransition::try_advance(
                from.operation(),
                to.operation(),
                previous_version,
                version,
            )?
        };
        Ok(Self {
            from,
            to,
            operation,
        })
    }

    pub const fn from(&self) -> CleanupState {
        self.from
    }

    pub const fn to(&self) -> CleanupState {
        self.to
    }

    pub const fn operation(&self) -> StateTransition<CleanupOperationState> {
        self.operation
    }
}

impl StateTransition<DeliverySourceState> {
    pub fn try_initial_source(
        to: DeliverySourceState,
        version: DeliveryVersion,
    ) -> Result<Self, DeliveryError> {
        Self::try_initial_phase(to, version, to == DeliverySourceState::ObjectPending)
    }
}

impl StateTransition<MergeOperationState> {
    pub fn try_initial_merge(
        to: MergeOperationState,
        version: DeliveryVersion,
    ) -> Result<Self, DeliveryError> {
        Self::try_initial_phase(to, version, to == MergeOperationState::PreflightPending)
    }
}

impl StateTransition<WorktreeDisposition> {
    pub fn try_initial_worktree_disposition(
        to: WorktreeDisposition,
        version: DeliveryVersion,
    ) -> Result<Self, DeliveryError> {
        Self::try_initial_phase(to, version, to == WorktreeDisposition::RetainedLocked)
    }
}

impl StateTransition<BranchDisposition> {
    pub fn try_initial_branch_disposition(
        to: BranchDisposition,
        version: DeliveryVersion,
    ) -> Result<Self, DeliveryError> {
        Self::try_initial_phase(to, version, to == BranchDisposition::Retained)
    }
}

impl StateTransition<CleanupOperationState> {
    pub fn try_initial_cleanup(
        kind: CleanupKind,
        to: CleanupOperationState,
        worktree: WorktreeDisposition,
        branch: BranchDisposition,
        version: DeliveryVersion,
    ) -> Result<Self, DeliveryError> {
        let phase_is_initial = matches!(
            (kind, to),
            (
                CleanupKind::RemoveWorktree,
                CleanupOperationState::UnlockPending
            ) | (
                CleanupKind::RemoveWorktree,
                CleanupOperationState::RemovePending
            ) | (
                CleanupKind::DeleteBranch,
                CleanupOperationState::DeletePending
            )
        );
        if !phase_is_initial {
            return Err(DeliveryError::IllegalTransition);
        }
        validate_cleanup_state(kind, to, worktree, branch)?;
        Self::try_initial_phase(to, version, true)
    }
}
