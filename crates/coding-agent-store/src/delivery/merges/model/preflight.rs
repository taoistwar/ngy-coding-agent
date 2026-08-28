use std::fmt;

use coding_agent_domain::TaskId;

use crate::delivery::mutation::{
    DeliveryMutationEntity, DeliveryMutationEntityKind, DeliveryMutationKey, DeliveryMutationKind,
    impl_delivery_mutation_request,
};
use crate::delivery::{
    DeliveryError, DeliveryOperationId, DeliveryVersion, GitCommitOid, GitTreeOid,
    MergeOperationState, PreflightStaleReason,
};

use super::{MergeConflictPaths, MergeReconciliationReason, PreflightRejectedReason};

#[derive(Clone, PartialEq, Eq)]
pub enum MergePreflightResult {
    Ready {
        merge_base: GitCommitOid,
        candidate_merge_tree: GitTreeOid,
    },
    Conflict {
        merge_base: GitCommitOid,
        candidate_merge_tree: GitTreeOid,
        paths: MergeConflictPaths,
    },
    Rejected(PreflightRejectedReason),
    Stale(PreflightStaleReason),
    ReconciliationRequired(MergeReconciliationReason),
}

impl fmt::Debug for MergePreflightResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ready { .. } => formatter.write_str("MergePreflightResult::Ready(<redacted>)"),
            Self::Conflict { paths, .. } => formatter
                .debug_tuple("MergePreflightResult::Conflict")
                .field(paths)
                .finish(),
            Self::Rejected(reason) => formatter.debug_tuple("Rejected").field(reason).finish(),
            Self::Stale(reason) => formatter.debug_tuple("Stale").field(reason).finish(),
            Self::ReconciliationRequired(reason) => formatter
                .debug_tuple("ReconciliationRequired")
                .field(reason)
                .finish(),
        }
    }
}

impl MergePreflightResult {
    pub fn ready(
        merge_base: GitCommitOid,
        candidate_merge_tree: GitTreeOid,
    ) -> Result<Self, DeliveryError> {
        if merge_base.algorithm() != candidate_merge_tree.algorithm() {
            return Err(DeliveryError::InvalidCommandRequest);
        }
        Ok(Self::Ready {
            merge_base,
            candidate_merge_tree,
        })
    }

    pub fn conflict(
        merge_base: GitCommitOid,
        candidate_merge_tree: GitTreeOid,
        paths: MergeConflictPaths,
    ) -> Result<Self, DeliveryError> {
        if merge_base.algorithm() != candidate_merge_tree.algorithm() {
            return Err(DeliveryError::InvalidCommandRequest);
        }
        Ok(Self::Conflict {
            merge_base,
            candidate_merge_tree,
            paths,
        })
    }

    pub const fn rejected(reason: PreflightRejectedReason) -> Self {
        Self::Rejected(reason)
    }

    pub const fn stale(reason: PreflightStaleReason) -> Self {
        Self::Stale(reason)
    }

    pub const fn reconciliation_required(reason: MergeReconciliationReason) -> Self {
        Self::ReconciliationRequired(reason)
    }

    pub(in crate::delivery::merges) const fn state(&self) -> MergeOperationState {
        match self {
            Self::Ready { .. } => MergeOperationState::PreflightReady,
            Self::Conflict { .. } => MergeOperationState::Conflict,
            Self::Rejected(_) => MergeOperationState::Rejected,
            Self::Stale(_) => MergeOperationState::Stale,
            Self::ReconciliationRequired(_) => MergeOperationState::ReconciliationRequired,
        }
    }

    pub(in crate::delivery::merges) const fn failure_code(&self) -> Option<&'static str> {
        match self {
            Self::Ready { .. } => None,
            Self::Conflict { .. } => Some("MERGE_CONFLICT"),
            Self::Rejected(reason) => Some(reason.as_failure_code()),
            Self::Stale(reason) => Some(reason.as_failure_code()),
            Self::ReconciliationRequired(reason) => Some(reason.as_failure_code()),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct RecordMergePreflightResultRequest {
    pub(in crate::delivery::merges) task_id: TaskId,
    pub(in crate::delivery::merges) operation_id: DeliveryOperationId,
    pub(in crate::delivery::merges) expected_version: DeliveryVersion,
    pub(in crate::delivery::merges) result: MergePreflightResult,
}

impl fmt::Debug for RecordMergePreflightResultRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecordMergePreflightResultRequest")
            .field("task_id", &self.task_id)
            .field("operation_id", &self.operation_id)
            .field("expected_version", &self.expected_version)
            .field("result", &self.result)
            .finish()
    }
}

impl RecordMergePreflightResultRequest {
    pub fn try_new(
        task_id: TaskId,
        operation_id: DeliveryOperationId,
        expected_version: DeliveryVersion,
        result: MergePreflightResult,
    ) -> Result<Self, DeliveryError> {
        if task_id.as_uuid().is_nil() || operation_id.as_uuid().is_nil() {
            return Err(DeliveryError::InvalidCommandRequest);
        }
        expected_version.next()?;
        Ok(Self {
            task_id,
            operation_id,
            expected_version,
            result,
        })
    }
}

impl_delivery_mutation_request!(RecordMergePreflightResultRequest, |request| {
    DeliveryMutationKey::new(
        DeliveryMutationKind::RecordMergePreflightResult,
        request.task_id,
        vec![DeliveryMutationEntity::operation(
            DeliveryMutationEntityKind::MergeOperation,
            request.operation_id,
            request.expected_version,
        )],
        None,
    )
});
