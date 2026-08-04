use std::fmt;

use crate::delivery::{DeliveryError, GitCommitOid};

use super::{
    BranchCleanupKnownNotAppliedReason, CleanupOperationAnchor, CleanupReconciliationReason,
};

#[derive(Clone, PartialEq, Eq)]
pub struct CompleteBranchCleanupRequest {
    pub(in crate::delivery::cleanup) anchor: CleanupOperationAnchor,
}

impl CompleteBranchCleanupRequest {
    pub fn try_new(anchor: CleanupOperationAnchor) -> Result<Self, DeliveryError> {
        Ok(Self { anchor })
    }
}

impl fmt::Debug for CompleteBranchCleanupRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompleteBranchCleanupRequest")
            .field("anchor", &self.anchor)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct RefreshBranchCleanupTargetRequest {
    pub(in crate::delivery::cleanup) anchor: CleanupOperationAnchor,
    pub(in crate::delivery::cleanup) expected_target_head: GitCommitOid,
    pub(in crate::delivery::cleanup) fresh_target_head: GitCommitOid,
}

impl RefreshBranchCleanupTargetRequest {
    pub fn try_new(
        anchor: CleanupOperationAnchor,
        expected_target_head: GitCommitOid,
        fresh_target_head: GitCommitOid,
    ) -> Result<Self, DeliveryError> {
        if expected_target_head == fresh_target_head
            || expected_target_head.algorithm() != fresh_target_head.algorithm()
        {
            return Err(DeliveryError::InvalidCommandRequest);
        }
        Ok(Self {
            anchor,
            expected_target_head,
            fresh_target_head,
        })
    }

    pub const fn expected_target_head(&self) -> &GitCommitOid {
        &self.expected_target_head
    }

    pub const fn fresh_target_head(&self) -> &GitCommitOid {
        &self.fresh_target_head
    }
}

impl fmt::Debug for RefreshBranchCleanupTargetRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RefreshBranchCleanupTargetRequest")
            .field("anchor", &self.anchor)
            .field("target_heads", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct RecordBranchCleanupFailureRequest {
    pub(in crate::delivery::cleanup) anchor: CleanupOperationAnchor,
    pub(in crate::delivery::cleanup) reason: BranchCleanupKnownNotAppliedReason,
}

impl RecordBranchCleanupFailureRequest {
    pub fn try_new(
        anchor: CleanupOperationAnchor,
        reason: BranchCleanupKnownNotAppliedReason,
    ) -> Result<Self, DeliveryError> {
        Ok(Self { anchor, reason })
    }
}

impl fmt::Debug for RecordBranchCleanupFailureRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecordBranchCleanupFailureRequest")
            .field("anchor", &self.anchor)
            .field("reason", &self.reason)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ReconcileBranchCleanupRequest {
    pub(in crate::delivery::cleanup) anchor: CleanupOperationAnchor,
    pub(in crate::delivery::cleanup) reason: CleanupReconciliationReason,
}

impl ReconcileBranchCleanupRequest {
    pub fn try_new(
        anchor: CleanupOperationAnchor,
        reason: CleanupReconciliationReason,
    ) -> Result<Self, DeliveryError> {
        Ok(Self { anchor, reason })
    }
}

impl fmt::Debug for ReconcileBranchCleanupRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReconcileBranchCleanupRequest")
            .field("anchor", &self.anchor)
            .field("reason", &self.reason)
            .finish()
    }
}
