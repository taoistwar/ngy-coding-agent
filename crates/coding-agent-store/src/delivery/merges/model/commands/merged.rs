use std::fmt;

use coding_agent_domain::TaskId;

use crate::delivery::merges::proof::MergeAppliedProof;
use crate::delivery::{DeliveryError, DeliveryOperationId, DeliveryVersion};

#[derive(Clone, PartialEq, Eq)]
pub struct CompleteMergeRequest {
    pub(in crate::delivery::merges) task_id: TaskId,
    pub(in crate::delivery::merges) operation_id: DeliveryOperationId,
    pub(in crate::delivery::merges) expected_version: DeliveryVersion,
    pub(in crate::delivery::merges) proof: MergeAppliedProof,
}

impl CompleteMergeRequest {
    pub fn try_new(
        task_id: TaskId,
        operation_id: DeliveryOperationId,
        expected_version: DeliveryVersion,
        proof: MergeAppliedProof,
    ) -> Result<Self, DeliveryError> {
        if task_id.as_uuid().is_nil() || operation_id.as_uuid().is_nil() {
            return Err(DeliveryError::InvalidCommandRequest);
        }
        expected_version.next()?;
        Ok(Self {
            task_id,
            operation_id,
            expected_version,
            proof,
        })
    }
}

impl fmt::Debug for CompleteMergeRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompleteMergeRequest")
            .field("task_id", &self.task_id)
            .field("operation_id", &self.operation_id)
            .field("expected_version", &self.expected_version)
            .field("proof", &"<redacted>")
            .finish()
    }
}
