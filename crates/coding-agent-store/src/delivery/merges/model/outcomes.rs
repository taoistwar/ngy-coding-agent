use crate::delivery::{
    DeliveryCommandReceipt, DeliveryOperationId, DeliveryTimestamp, DeliveryVersion, FailureCode,
    MergeOperationState,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcceptMergeOutcome {
    Accepted(DeliveryCommandReceipt),
    Existing(DeliveryCommandReceipt),
    Conflict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeTransitionReceipt {
    pub operation_id: DeliveryOperationId,
    pub version: DeliveryVersion,
    pub state: MergeOperationState,
    pub failure_code: Option<FailureCode>,
    pub transitioned_at: DeliveryTimestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeTransitionOutcome {
    Applied(MergeTransitionReceipt),
    Existing(MergeTransitionReceipt),
    Conflict,
}
