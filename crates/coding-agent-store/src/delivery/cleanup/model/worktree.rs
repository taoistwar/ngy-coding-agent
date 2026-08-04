use std::fmt;

use crate::delivery::{CleanupOperationState, DeliveryError};

use super::{
    CleanupOperationAnchor, CleanupReconciliationReason, WorktreeCleanupKnownNotAppliedReason,
};

macro_rules! anchored_request {
    ($name:ident) => {
        #[derive(Clone, PartialEq, Eq)]
        pub struct $name {
            pub(in crate::delivery::cleanup) anchor: CleanupOperationAnchor,
        }

        impl $name {
            pub fn try_new(anchor: CleanupOperationAnchor) -> Result<Self, DeliveryError> {
                Ok(Self { anchor })
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_struct(stringify!($name))
                    .field("anchor", &self.anchor)
                    .finish()
            }
        }
    };
}

anchored_request!(RecordWorktreeUnlockedRequest);
anchored_request!(EnterWorktreeRemovePendingRequest);
anchored_request!(CompleteWorktreeCleanupRequest);

#[derive(Clone, PartialEq, Eq)]
pub struct RecordWorktreeCleanupFailureRequest {
    pub(in crate::delivery::cleanup) anchor: CleanupOperationAnchor,
    pub(in crate::delivery::cleanup) expected_state: CleanupOperationState,
    pub(in crate::delivery::cleanup) reason: WorktreeCleanupKnownNotAppliedReason,
}

impl RecordWorktreeCleanupFailureRequest {
    pub fn try_new(
        anchor: CleanupOperationAnchor,
        expected_state: CleanupOperationState,
        reason: WorktreeCleanupKnownNotAppliedReason,
    ) -> Result<Self, DeliveryError> {
        if !matches!(
            expected_state,
            CleanupOperationState::UnlockPending | CleanupOperationState::RemovePending
        ) || (reason == WorktreeCleanupKnownNotAppliedReason::TargetWorktreeDirty
            && expected_state != CleanupOperationState::RemovePending)
        {
            return Err(DeliveryError::InvalidCommandRequest);
        }
        Ok(Self {
            anchor,
            expected_state,
            reason,
        })
    }
}

impl fmt::Debug for RecordWorktreeCleanupFailureRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecordWorktreeCleanupFailureRequest")
            .field("anchor", &self.anchor)
            .field("expected_state", &self.expected_state)
            .field("reason", &self.reason)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ReconcileWorktreeCleanupRequest {
    pub(in crate::delivery::cleanup) anchor: CleanupOperationAnchor,
    pub(in crate::delivery::cleanup) expected_state: CleanupOperationState,
    pub(in crate::delivery::cleanup) reason: CleanupReconciliationReason,
}

impl ReconcileWorktreeCleanupRequest {
    pub fn try_new(
        anchor: CleanupOperationAnchor,
        expected_state: CleanupOperationState,
        reason: CleanupReconciliationReason,
    ) -> Result<Self, DeliveryError> {
        if !matches!(
            expected_state,
            CleanupOperationState::UnlockPending
                | CleanupOperationState::UnlockedPendingRemove
                | CleanupOperationState::RemovePending
        ) {
            return Err(DeliveryError::InvalidCommandRequest);
        }
        Ok(Self {
            anchor,
            expected_state,
            reason,
        })
    }
}

impl fmt::Debug for ReconcileWorktreeCleanupRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReconcileWorktreeCleanupRequest")
            .field("anchor", &self.anchor)
            .field("expected_state", &self.expected_state)
            .field("reason", &self.reason)
            .finish()
    }
}
