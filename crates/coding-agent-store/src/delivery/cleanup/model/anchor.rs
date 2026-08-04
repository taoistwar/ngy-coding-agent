use std::fmt;

use coding_agent_domain::TaskId;

use crate::delivery::{DeliveryError, DeliveryOperationId, DeliveryVersion};

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct CleanupOperationAnchor {
    pub(in crate::delivery::cleanup) task_id: TaskId,
    pub(in crate::delivery::cleanup) operation_id: DeliveryOperationId,
    pub(in crate::delivery::cleanup) expected_version: DeliveryVersion,
}

impl CleanupOperationAnchor {
    pub fn try_new(
        task_id: TaskId,
        operation_id: DeliveryOperationId,
        expected_version: DeliveryVersion,
    ) -> Result<Self, DeliveryError> {
        if task_id.as_uuid().is_nil() || operation_id.as_uuid().is_nil() {
            return Err(DeliveryError::InvalidCommandRequest);
        }
        expected_version.next()?;
        Ok(Self {
            task_id,
            operation_id,
            expected_version,
        })
    }

    pub const fn task_id(self) -> TaskId {
        self.task_id
    }

    pub const fn operation_id(self) -> DeliveryOperationId {
        self.operation_id
    }

    pub const fn expected_version(self) -> DeliveryVersion {
        self.expected_version
    }
}

impl fmt::Debug for CleanupOperationAnchor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CleanupOperationAnchor")
            .field("task_id", &self.task_id)
            .field("operation_id", &self.operation_id)
            .field("expected_version", &self.expected_version)
            .finish()
    }
}
