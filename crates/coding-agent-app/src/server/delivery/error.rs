use std::collections::BTreeMap;

use coding_agent_api::{ApiError, DeliveryApiErrorKind};
use http::StatusCode;

use crate::{
    DeliveryCommandConflict, DeliveryEligibilityReason, DeliveryManagerError,
    DeliveryPreflightBusyReason, DeliveryPreflightUnavailableReason,
    DeliveryQueryUnavailableReason,
};

pub(super) fn manager_error(error: DeliveryManagerError) -> ApiError {
    match error {
        DeliveryManagerError::Closed => delivery_unavailable(),
    }
}

pub(super) fn query_unavailable(_: DeliveryQueryUnavailableReason) -> ApiError {
    delivery_unavailable()
}

pub(super) fn task_not_found() -> ApiError {
    api_error(
        StatusCode::NOT_FOUND,
        "TASK_NOT_FOUND",
        "the task was not found",
        false,
    )
}

pub(super) fn operation_not_found() -> ApiError {
    api_error(
        StatusCode::NOT_FOUND,
        "DELIVERY_OPERATION_NOT_FOUND",
        "the delivery operation was not found",
        false,
    )
}

pub(super) fn invalid_validated_command() -> ApiError {
    api_error(
        StatusCode::UNPROCESSABLE_ENTITY,
        "INVALID_REQUEST",
        "the delivery request is invalid",
        false,
    )
}

pub(super) fn invalid_projection() -> ApiError {
    api_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "INTERNAL_ERROR",
        "the request could not be completed",
        false,
    )
}

pub(super) fn ineligible(reasons: &[DeliveryEligibilityReason]) -> ApiError {
    reasons
        .iter()
        .copied()
        .map(reason_error)
        .min_by_key(|error| reason_priority(&error.code))
        .unwrap_or_else(|| ApiError::delivery(DeliveryApiErrorKind::TaskNotMergeEligible))
}

pub(super) fn cleanup_ineligible(reasons: &[DeliveryEligibilityReason]) -> ApiError {
    if reasons.iter().any(|reason| {
        matches!(
            reason,
            DeliveryEligibilityReason::AttemptArtifactMissing
                | DeliveryEligibilityReason::AlreadyMerged
        )
    }) {
        return ApiError::delivery(DeliveryApiErrorKind::ArtifactCleanupNotAllowed);
    }
    if reasons.contains(&DeliveryEligibilityReason::TaskActive) {
        return ApiError::delivery(DeliveryApiErrorKind::ArtifactProcessStillActive);
    }
    if reasons.contains(&DeliveryEligibilityReason::RuntimeDrift) {
        return ApiError::delivery(DeliveryApiErrorKind::WorktreeIdentityMismatch);
    }
    ineligible(reasons)
}

pub(super) fn conflict(conflict: DeliveryCommandConflict) -> ApiError {
    let kind = match conflict {
        DeliveryCommandConflict::IdempotencyConflict => DeliveryApiErrorKind::IdempotencyConflict,
        DeliveryCommandConflict::EvidenceStale => DeliveryApiErrorKind::DeliveryEvidenceStale,
        DeliveryCommandConflict::SourceChanged => DeliveryApiErrorKind::DeliverySourceChanged,
        DeliveryCommandConflict::PreflightStale => DeliveryApiErrorKind::DeliveryPreflightStale,
        DeliveryCommandConflict::OperationInProgress => {
            DeliveryApiErrorKind::DeliveryOperationInProgress
        }
        DeliveryCommandConflict::TargetBranchMismatch => DeliveryApiErrorKind::TargetBranchMismatch,
        DeliveryCommandConflict::TargetHeadChanged => DeliveryApiErrorKind::TargetHeadChanged,
        DeliveryCommandConflict::MergeConflict => DeliveryApiErrorKind::MergeConflict,
        DeliveryCommandConflict::ArtifactCleanupNotAllowed => {
            DeliveryApiErrorKind::ArtifactCleanupNotAllowed
        }
        DeliveryCommandConflict::ArtifactProcessStillActive => {
            DeliveryApiErrorKind::ArtifactProcessStillActive
        }
        DeliveryCommandConflict::WorktreeIdentityMismatch => {
            DeliveryApiErrorKind::WorktreeIdentityMismatch
        }
        DeliveryCommandConflict::SourceBranchNotMerged => {
            DeliveryApiErrorKind::SourceBranchNotMerged
        }
    };
    ApiError::delivery(kind)
}

pub(super) fn busy(reason: DeliveryPreflightBusyReason) -> ApiError {
    match reason {
        DeliveryPreflightBusyReason::RepositoryBusy
        | DeliveryPreflightBusyReason::WorkerQueueFull => {
            ApiError::delivery(DeliveryApiErrorKind::RepositoryControlBusy)
        }
    }
}

pub(super) fn unavailable(reason: DeliveryPreflightUnavailableReason) -> ApiError {
    match reason {
        DeliveryPreflightUnavailableReason::RepositoryControlUnavailable
        | DeliveryPreflightUnavailableReason::OutcomeUnknown => {
            ApiError::delivery(DeliveryApiErrorKind::RepositoryControlPoisoned)
        }
        DeliveryPreflightUnavailableReason::ProcessProofUnavailable => {
            ApiError::delivery(DeliveryApiErrorKind::ProcessTreeCleanupFailed)
        }
        DeliveryPreflightUnavailableReason::RuntimeUnavailable => {
            ApiError::delivery(DeliveryApiErrorKind::DeliveryReconciliationRequired)
        }
        DeliveryPreflightUnavailableReason::SourceInconsistent => {
            ApiError::delivery(DeliveryApiErrorKind::DeliverySourceInconsistent)
        }
        DeliveryPreflightUnavailableReason::CommandTimedOut => {
            ApiError::delivery(DeliveryApiErrorKind::CommandTimedOut)
        }
        DeliveryPreflightUnavailableReason::ManagerQuiescing
        | DeliveryPreflightUnavailableReason::ServiceNotReady
        | DeliveryPreflightUnavailableReason::StoreUnavailable
        | DeliveryPreflightUnavailableReason::OrchestrationUnavailable => delivery_unavailable(),
    }
}

fn reason_error(reason: DeliveryEligibilityReason) -> ApiError {
    let kind = match reason {
        DeliveryEligibilityReason::TaskNotFound => return task_not_found(),
        DeliveryEligibilityReason::TaskNotCompleted
        | DeliveryEligibilityReason::ReviewNotApproved
        | DeliveryEligibilityReason::ApprovedEvidenceMissing
        | DeliveryEligibilityReason::AttemptArtifactMissing
        | DeliveryEligibilityReason::AttemptArtifactNotReady => {
            DeliveryApiErrorKind::TaskNotMergeEligible
        }
        DeliveryEligibilityReason::TaskActive | DeliveryEligibilityReason::DeliveryOwned => {
            DeliveryApiErrorKind::DeliveryOperationInProgress
        }
        DeliveryEligibilityReason::ProcessCleanupUnproven => {
            DeliveryApiErrorKind::ProcessTreeCleanupFailed
        }
        DeliveryEligibilityReason::TargetBranchDetached => {
            DeliveryApiErrorKind::TargetBranchDetached
        }
        DeliveryEligibilityReason::TargetBranchMismatch => {
            DeliveryApiErrorKind::TargetBranchMismatch
        }
        DeliveryEligibilityReason::TargetWorktreeDirty => DeliveryApiErrorKind::TargetWorktreeDirty,
        DeliveryEligibilityReason::TargetIgnoredPathCollision => {
            DeliveryApiErrorKind::TargetIgnoredPathCollision
        }
        DeliveryEligibilityReason::TargetGitOperationInProgress => {
            DeliveryApiErrorKind::TargetGitOperationInProgress
        }
        DeliveryEligibilityReason::UnsafeGitConfiguration => {
            DeliveryApiErrorKind::UnsafeGitConfiguration
        }
        DeliveryEligibilityReason::UnsupportedGitAttributes => {
            DeliveryApiErrorKind::UnsupportedGitAttributes
        }
        DeliveryEligibilityReason::SourceAlreadyInTarget
        | DeliveryEligibilityReason::AlreadyMerged => DeliveryApiErrorKind::SourceAlreadyInTarget,
        DeliveryEligibilityReason::RuntimeDrift => DeliveryApiErrorKind::DeliveryEvidenceStale,
        DeliveryEligibilityReason::ReconciliationRequired => {
            DeliveryApiErrorKind::DeliveryReconciliationRequired
        }
        DeliveryEligibilityReason::RepositoryBusy => DeliveryApiErrorKind::RepositoryControlBusy,
        DeliveryEligibilityReason::RepositoryUnavailable => {
            DeliveryApiErrorKind::RepositoryControlPoisoned
        }
        DeliveryEligibilityReason::StoreUnavailable
        | DeliveryEligibilityReason::RuntimeObservationUnavailable
        | DeliveryEligibilityReason::ServiceNotReady => return delivery_unavailable(),
    };
    ApiError::delivery(kind)
}

fn reason_priority(code: &str) -> u8 {
    match code {
        "TASK_NOT_FOUND" => 0,
        "REPOSITORY_CONTROL_POISONED" | "DELIVERY_RECONCILIATION_REQUIRED" => 1,
        "PROCESS_TREE_CLEANUP_FAILED" => 2,
        "REPOSITORY_CONTROL_BUSY" | "STORE_DEGRADED" => 3,
        _ => 4,
    }
}

fn delivery_unavailable() -> ApiError {
    api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "STORE_DEGRADED",
        "delivery operations are unavailable",
        true,
    )
}

fn api_error(status: StatusCode, code: &str, message: &str, retryable: bool) -> ApiError {
    ApiError {
        status,
        code: code.to_owned(),
        message: message.to_owned(),
        retryable,
        details: BTreeMap::new(),
    }
}
