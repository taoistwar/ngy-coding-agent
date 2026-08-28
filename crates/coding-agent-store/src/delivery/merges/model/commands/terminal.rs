use std::fmt;

use coding_agent_domain::TaskId;

use crate::delivery::{DeliveryError, DeliveryOperationId, DeliveryVersion, MergeOperationState};

use super::super::reasons::{MergeKnownNotAppliedReason, MergeReconciliationReason};

#[derive(Clone, PartialEq, Eq)]
pub struct RecordMergeKnownFailureRequest {
    pub(in crate::delivery::merges) task_id: TaskId,
    pub(in crate::delivery::merges) operation_id: DeliveryOperationId,
    pub(in crate::delivery::merges) expected_state: MergeOperationState,
    pub(in crate::delivery::merges) expected_version: DeliveryVersion,
    pub(in crate::delivery::merges) reason: MergeKnownNotAppliedReason,
}

impl RecordMergeKnownFailureRequest {
    pub fn try_new(
        task_id: TaskId,
        operation_id: DeliveryOperationId,
        expected_state: MergeOperationState,
        expected_version: DeliveryVersion,
        reason: MergeKnownNotAppliedReason,
    ) -> Result<Self, DeliveryError> {
        if task_id.as_uuid().is_nil()
            || operation_id.as_uuid().is_nil()
            || !matches!(
                expected_state,
                MergeOperationState::Accepted | MergeOperationState::MergePending
            )
        {
            return Err(DeliveryError::InvalidCommandRequest);
        }
        expected_version.next()?;
        Ok(Self {
            task_id,
            operation_id,
            expected_state,
            expected_version,
            reason,
        })
    }
}

impl fmt::Debug for RecordMergeKnownFailureRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecordMergeKnownFailureRequest")
            .field("task_id", &self.task_id)
            .field("operation_id", &self.operation_id)
            .field("expected_state", &self.expected_state)
            .field("expected_version", &self.expected_version)
            .field("reason", &self.reason)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ReconcileMergeRequest {
    pub(in crate::delivery::merges) task_id: TaskId,
    pub(in crate::delivery::merges) operation_id: DeliveryOperationId,
    pub(in crate::delivery::merges) expected_state: MergeOperationState,
    pub(in crate::delivery::merges) expected_version: DeliveryVersion,
    pub(in crate::delivery::merges) reason: MergeReconciliationReason,
}

impl ReconcileMergeRequest {
    pub fn try_new(
        task_id: TaskId,
        operation_id: DeliveryOperationId,
        expected_state: MergeOperationState,
        expected_version: DeliveryVersion,
        reason: MergeReconciliationReason,
    ) -> Result<Self, DeliveryError> {
        if task_id.as_uuid().is_nil()
            || operation_id.as_uuid().is_nil()
            || !matches!(
                expected_state,
                MergeOperationState::PreflightPending
                    | MergeOperationState::PreflightReady
                    | MergeOperationState::Accepted
                    | MergeOperationState::MergePending
                    | MergeOperationState::AbortPending
            )
            || (expected_state == MergeOperationState::PreflightPending
                && expected_version.get() != 2)
        {
            return Err(DeliveryError::InvalidCommandRequest);
        }
        expected_version.next()?;
        Ok(Self {
            task_id,
            operation_id,
            expected_state,
            expected_version,
            reason,
        })
    }
}

impl fmt::Debug for ReconcileMergeRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReconcileMergeRequest")
            .field("task_id", &self.task_id)
            .field("operation_id", &self.operation_id)
            .field("expected_state", &self.expected_state)
            .field("expected_version", &self.expected_version)
            .field("reason", &self.reason)
            .finish()
    }
}
