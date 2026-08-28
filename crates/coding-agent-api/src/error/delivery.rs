use std::collections::BTreeMap;

use http::StatusCode;

use super::ApiError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryApiErrorKind {
    TaskNotMergeEligible,
    DeliveryEvidenceStale,
    DeliverySourceChanged,
    DeliverySourceInconsistent,
    DeliveryPreflightStale,
    DeliveryOperationInProgress,
    TargetBranchDetached,
    TargetBranchMismatch,
    TargetHeadChanged,
    TargetWorktreeDirty,
    TargetIgnoredPathCollision,
    TargetGitOperationInProgress,
    UnsafeGitConfiguration,
    UnsupportedGitAttributes,
    MergeConflict,
    SourceAlreadyInTarget,
    DeliveryReconciliationRequired,
    ArtifactCleanupNotAllowed,
    ArtifactProcessStillActive,
    WorktreeIdentityMismatch,
    SourceBranchNotMerged,
    IdempotencyConflict,
    RepositoryControlBusy,
    RepositoryControlPoisoned,
    CommandTimedOut,
    ProcessTreeCleanupFailed,
}

impl DeliveryApiErrorKind {
    const fn contract(self) -> (StatusCode, &'static str, &'static str, bool) {
        match self {
            Self::TaskNotMergeEligible => (
                StatusCode::CONFLICT,
                "TASK_NOT_MERGE_ELIGIBLE",
                "the task is not eligible for delivery",
                false,
            ),
            Self::DeliveryEvidenceStale => (
                StatusCode::CONFLICT,
                "DELIVERY_EVIDENCE_STALE",
                "the reviewed evidence is stale",
                false,
            ),
            Self::DeliverySourceChanged => (
                StatusCode::CONFLICT,
                "DELIVERY_SOURCE_CHANGED",
                "the delivery source changed",
                false,
            ),
            Self::DeliverySourceInconsistent => (
                StatusCode::SERVICE_UNAVAILABLE,
                "DELIVERY_SOURCE_INCONSISTENT",
                "the delivery source requires reconciliation",
                false,
            ),
            Self::DeliveryPreflightStale => (
                StatusCode::CONFLICT,
                "DELIVERY_PREFLIGHT_STALE",
                "the delivery preflight is stale",
                false,
            ),
            Self::DeliveryOperationInProgress => (
                StatusCode::CONFLICT,
                "DELIVERY_OPERATION_IN_PROGRESS",
                "a delivery operation is already in progress",
                true,
            ),
            Self::TargetBranchDetached => (
                StatusCode::CONFLICT,
                "TARGET_BRANCH_DETACHED",
                "the target worktree is detached",
                false,
            ),
            Self::TargetBranchMismatch => (
                StatusCode::CONFLICT,
                "TARGET_BRANCH_MISMATCH",
                "the target branch does not match",
                false,
            ),
            Self::TargetHeadChanged => (
                StatusCode::CONFLICT,
                "TARGET_HEAD_CHANGED",
                "the target head changed",
                false,
            ),
            Self::TargetWorktreeDirty => (
                StatusCode::CONFLICT,
                "TARGET_WORKTREE_DIRTY",
                "the target worktree is dirty",
                false,
            ),
            Self::TargetIgnoredPathCollision => (
                StatusCode::CONFLICT,
                "TARGET_IGNORED_PATH_COLLISION",
                "an ignored target path would be overwritten",
                false,
            ),
            Self::TargetGitOperationInProgress => (
                StatusCode::CONFLICT,
                "TARGET_GIT_OPERATION_IN_PROGRESS",
                "another Git operation is in progress",
                true,
            ),
            Self::UnsafeGitConfiguration => (
                StatusCode::CONFLICT,
                "UNSAFE_GIT_CONFIGURATION",
                "the repository Git configuration is unsafe",
                false,
            ),
            Self::UnsupportedGitAttributes => (
                StatusCode::CONFLICT,
                "UNSUPPORTED_GIT_ATTRIBUTES",
                "the repository Git attributes are unsupported",
                false,
            ),
            Self::MergeConflict => (
                StatusCode::CONFLICT,
                "MERGE_CONFLICT",
                "the preflight operation contains conflicts",
                false,
            ),
            Self::SourceAlreadyInTarget => (
                StatusCode::CONFLICT,
                "SOURCE_ALREADY_IN_TARGET",
                "the delivery source is already in the target",
                false,
            ),
            Self::DeliveryReconciliationRequired => (
                StatusCode::SERVICE_UNAVAILABLE,
                "DELIVERY_RECONCILIATION_REQUIRED",
                "the delivery operation requires reconciliation",
                false,
            ),
            Self::ArtifactCleanupNotAllowed => (
                StatusCode::CONFLICT,
                "ARTIFACT_CLEANUP_NOT_ALLOWED",
                "artifact cleanup is not allowed",
                false,
            ),
            Self::ArtifactProcessStillActive => (
                StatusCode::CONFLICT,
                "ARTIFACT_PROCESS_STILL_ACTIVE",
                "an artifact process is still active",
                true,
            ),
            Self::WorktreeIdentityMismatch => (
                StatusCode::CONFLICT,
                "WORKTREE_IDENTITY_MISMATCH",
                "the worktree identity does not match",
                false,
            ),
            Self::SourceBranchNotMerged => (
                StatusCode::CONFLICT,
                "SOURCE_BRANCH_NOT_MERGED",
                "the source branch is not merged into the target",
                false,
            ),
            Self::IdempotencyConflict => (
                StatusCode::CONFLICT,
                "IDEMPOTENCY_CONFLICT",
                "the client request identifier was already used",
                false,
            ),
            Self::RepositoryControlBusy => (
                StatusCode::SERVICE_UNAVAILABLE,
                "REPOSITORY_CONTROL_BUSY",
                "repository control is busy",
                true,
            ),
            Self::RepositoryControlPoisoned => (
                StatusCode::SERVICE_UNAVAILABLE,
                "REPOSITORY_CONTROL_POISONED",
                "repository control requires reconciliation",
                false,
            ),
            Self::CommandTimedOut => (
                StatusCode::GATEWAY_TIMEOUT,
                "COMMAND_TIMED_OUT",
                "the command timed out",
                true,
            ),
            Self::ProcessTreeCleanupFailed => (
                StatusCode::SERVICE_UNAVAILABLE,
                "PROCESS_TREE_CLEANUP_FAILED",
                "process cleanup could not be proven",
                false,
            ),
        }
    }
}

impl ApiError {
    pub fn delivery(kind: DeliveryApiErrorKind) -> Self {
        let (status, code, message, retryable) = kind.contract();
        Self {
            status,
            code: code.to_owned(),
            message: message.to_owned(),
            retryable,
            details: BTreeMap::new(),
        }
    }

    pub(crate) fn invalid_delivery_request() -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            code: "INVALID_REQUEST".to_owned(),
            message: "the delivery request is invalid".to_owned(),
            retryable: false,
            details: BTreeMap::new(),
        }
    }

    pub(crate) fn delivery_backend_unavailable() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "STORE_DEGRADED".to_owned(),
            message: "delivery operations are unavailable".to_owned(),
            retryable: true,
            details: BTreeMap::new(),
        }
    }
}
