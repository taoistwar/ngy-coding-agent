mod dto;
mod logical;

pub use dto::{
    DeliveryAllowedAction, DeliveryArtifactDispositionProjection, DeliveryBranchDispositionState,
    DeliveryCleanupAcceptance, DeliveryCleanupAcceptanceOutcome, DeliveryCleanupOperationKind,
    DeliveryCleanupOperationProjection, DeliveryCleanupOperationState,
    DeliveryCleanupReceiptDisposition, DeliveryCommandConflict, DeliveryConflictPathEncoding,
    DeliveryConflictPathProjection, DeliveryConflictSummaryProjection, DeliveryEligibility,
    DeliveryEligibilityReason, DeliveryEvidenceProjection, DeliveryMergeAcceptance,
    DeliveryMergeAcceptanceOutcome, DeliveryMergeOperationProjection,
    DeliveryMergeReceiptDisposition, DeliveryOperationProjection, DeliveryOperationQueryOutcome,
    DeliveryPreflightBusyReason, DeliveryPreflightDurability, DeliveryPreflightOperation,
    DeliveryPreflightOutcome, DeliveryPreflightRetry, DeliveryPreflightState,
    DeliveryPreflightUnavailableReason, DeliveryQueryUnavailableReason, DeliverySourceProjection,
    DeliverySourceProjectionState, DeliveryTargetObservation, DeliveryTargetUnavailableReason,
    DeliveryTaskProjection, DeliveryTaskQueryOutcome, DeliveryWorktreeDispositionState,
};

pub(crate) use dto::DeliveryTaskProjectionContext;
pub(crate) use logical::{DeliveryProjectionDecision, project_delivery_task_with_context};
