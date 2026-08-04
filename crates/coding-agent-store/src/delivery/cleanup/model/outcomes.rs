use crate::delivery::{
    CleanupOperationState, DeliveryCommandReceipt, DeliveryOperationId, DeliveryTimestamp,
    DeliveryVersion, FailureCode,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CleanupAcceptanceOutcome {
    Accepted(DeliveryCommandReceipt),
    Existing(DeliveryCommandReceipt),
    Conflict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupTransitionReceipt {
    pub operation_id: DeliveryOperationId,
    pub version: DeliveryVersion,
    pub state: CleanupOperationState,
    pub failure_code: Option<FailureCode>,
    pub transitioned_at: DeliveryTimestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CleanupTransitionOutcome {
    Applied(CleanupTransitionReceipt),
    Existing(CleanupTransitionReceipt),
    Conflict,
}
