use coding_agent_api::{
    ApiResult, DeliveryAllowedActionDto, DeliveryArtifactDispositionDto,
    DeliveryBranchDispositionDto, DeliveryBranchDispositionStateDto, DeliveryCleanupKindDto,
    DeliveryCleanupOperationDto, DeliveryCleanupStateDto, DeliveryConflictPathDto,
    DeliveryConflictPathEncodingDto, DeliveryConflictSummaryDto, DeliveryEligibilityDto,
    DeliveryEligibilityReasonDto, DeliveryEvidenceSummaryDto, DeliveryMergeOperationDto,
    DeliveryMergeStateDto, DeliveryOperationDto, DeliveryOperationFailureDto,
    DeliveryReceiptDispositionDto, DeliverySourceDto, DeliverySourceStateDto,
    DeliveryTargetObservationDto, DeliveryTargetUnavailableReasonDto, DeliveryTaskDto,
    DeliveryWorktreeDispositionDto, DeliveryWorktreeDispositionStateDto,
};

use crate::delivery_api_projection::{
    DeliveryAllowedAction, DeliveryArtifactDispositionProjection, DeliveryBranchDispositionState,
    DeliveryCleanupOperationKind, DeliveryCleanupOperationProjection,
    DeliveryCleanupOperationState, DeliveryCleanupReceiptDisposition, DeliveryConflictPathEncoding,
    DeliveryConflictSummaryProjection, DeliveryEligibility, DeliveryEligibilityReason,
    DeliveryEvidenceProjection, DeliveryMergeOperationProjection, DeliveryMergeReceiptDisposition,
    DeliveryOperationProjection, DeliveryOperationQueryOutcome, DeliveryPreflightDurability,
    DeliveryPreflightState, DeliveryQueryUnavailableReason, DeliverySourceProjection,
    DeliverySourceProjectionState, DeliveryTargetObservation, DeliveryTargetUnavailableReason,
    DeliveryTaskProjection, DeliveryTaskQueryOutcome, DeliveryWorktreeDispositionState,
};

use super::error;

pub(super) fn task_query(outcome: DeliveryTaskQueryOutcome) -> ApiResult<DeliveryTaskDto> {
    match outcome {
        DeliveryTaskQueryOutcome::Found { projection } => task(&projection),
        DeliveryTaskQueryOutcome::NotFound { .. } => Err(error::task_not_found()),
        DeliveryTaskQueryOutcome::Unavailable { reason, .. } => {
            Err(error::query_unavailable(reason))
        }
    }
}

pub(super) fn operation_query(
    outcome: DeliveryOperationQueryOutcome,
) -> ApiResult<DeliveryOperationDto> {
    match outcome {
        DeliveryOperationQueryOutcome::Found { operation } => operation_projection(&operation),
        DeliveryOperationQueryOutcome::NotFound { .. } => Err(error::operation_not_found()),
        DeliveryOperationQueryOutcome::Unavailable { reason, .. } => {
            Err(error::query_unavailable(reason))
        }
    }
}

pub(super) const fn preflight_receipt(
    durability: DeliveryPreflightDurability,
) -> DeliveryReceiptDispositionDto {
    match durability {
        DeliveryPreflightDurability::Created => DeliveryReceiptDispositionDto::Created,
        DeliveryPreflightDurability::Existing => DeliveryReceiptDispositionDto::Existing,
    }
}

pub(super) const fn merge_receipt(
    receipt: DeliveryMergeReceiptDisposition,
) -> DeliveryReceiptDispositionDto {
    match receipt {
        DeliveryMergeReceiptDisposition::Created => DeliveryReceiptDispositionDto::Created,
        DeliveryMergeReceiptDisposition::Existing => DeliveryReceiptDispositionDto::Existing,
    }
}

pub(super) const fn cleanup_receipt(
    receipt: DeliveryCleanupReceiptDisposition,
) -> DeliveryReceiptDispositionDto {
    match receipt {
        DeliveryCleanupReceiptDisposition::Created => DeliveryReceiptDispositionDto::Created,
        DeliveryCleanupReceiptDisposition::Existing => DeliveryReceiptDispositionDto::Existing,
    }
}

fn task(projection: &DeliveryTaskProjection) -> ApiResult<DeliveryTaskDto> {
    Ok(DeliveryTaskDto {
        task_id: projection.task_id().as_uuid(),
        eligibility: eligibility(projection.eligibility()),
        reasons: projection
            .reasons()
            .iter()
            .copied()
            .map(eligibility_reason)
            .collect::<Result<Vec<_>, _>>()?,
        evidence: projection.evidence().map(evidence),
        target: target(projection.target()),
        source: projection.source().map(source),
        latest_merge: projection.latest_merge().map(merge_operation).transpose()?,
        latest_cleanup: projection.latest_cleanup().map(cleanup_operation),
        disposition: projection.disposition().map(disposition),
        allowed_actions: projection
            .allowed_actions()
            .iter()
            .copied()
            .map(allowed_action)
            .collect(),
    })
}

pub(super) fn operation_projection(
    projection: &DeliveryOperationProjection,
) -> ApiResult<DeliveryOperationDto> {
    match projection {
        DeliveryOperationProjection::Merge { details, .. } => details
            .as_ref()
            .ok_or_else(error::invalid_projection)
            .and_then(merge_operation)
            .map(DeliveryOperationDto::Merge),
        DeliveryOperationProjection::Cleanup { details, .. } => details
            .as_ref()
            .ok_or_else(error::invalid_projection)
            .map(cleanup_operation)
            .map(DeliveryOperationDto::Cleanup),
    }
}

const fn eligibility(value: DeliveryEligibility) -> DeliveryEligibilityDto {
    match value {
        DeliveryEligibility::Eligible => DeliveryEligibilityDto::Eligible,
        DeliveryEligibility::Ineligible => DeliveryEligibilityDto::Ineligible,
        DeliveryEligibility::Unavailable => DeliveryEligibilityDto::Unavailable,
    }
}

fn eligibility_reason(value: DeliveryEligibilityReason) -> ApiResult<DeliveryEligibilityReasonDto> {
    Ok(match value {
        DeliveryEligibilityReason::TaskNotFound => return Err(error::invalid_projection()),
        DeliveryEligibilityReason::TaskNotCompleted => {
            DeliveryEligibilityReasonDto::TaskNotCompleted
        }
        DeliveryEligibilityReason::ReviewNotApproved => {
            DeliveryEligibilityReasonDto::ReviewNotApproved
        }
        DeliveryEligibilityReason::ApprovedEvidenceMissing => {
            DeliveryEligibilityReasonDto::ApprovedEvidenceMissing
        }
        DeliveryEligibilityReason::AttemptArtifactMissing => {
            DeliveryEligibilityReasonDto::AttemptArtifactMissing
        }
        DeliveryEligibilityReason::AttemptArtifactNotReady => {
            DeliveryEligibilityReasonDto::AttemptArtifactNotReady
        }
        DeliveryEligibilityReason::TaskActive => DeliveryEligibilityReasonDto::TaskActive,
        DeliveryEligibilityReason::ProcessCleanupUnproven => {
            DeliveryEligibilityReasonDto::ProcessCleanupUnproven
        }
        DeliveryEligibilityReason::TargetBranchDetached => {
            DeliveryEligibilityReasonDto::TargetBranchDetached
        }
        DeliveryEligibilityReason::TargetBranchMismatch => {
            DeliveryEligibilityReasonDto::TargetBranchMismatch
        }
        DeliveryEligibilityReason::TargetWorktreeDirty => {
            DeliveryEligibilityReasonDto::TargetWorktreeDirty
        }
        DeliveryEligibilityReason::TargetIgnoredPathCollision => {
            DeliveryEligibilityReasonDto::TargetIgnoredPathCollision
        }
        DeliveryEligibilityReason::TargetGitOperationInProgress => {
            DeliveryEligibilityReasonDto::TargetGitOperationInProgress
        }
        DeliveryEligibilityReason::UnsafeGitConfiguration => {
            DeliveryEligibilityReasonDto::UnsafeGitConfiguration
        }
        DeliveryEligibilityReason::UnsupportedGitAttributes => {
            DeliveryEligibilityReasonDto::UnsupportedGitAttributes
        }
        DeliveryEligibilityReason::SourceAlreadyInTarget => {
            DeliveryEligibilityReasonDto::SourceAlreadyInTarget
        }
        DeliveryEligibilityReason::RuntimeDrift => DeliveryEligibilityReasonDto::RuntimeDrift,
        DeliveryEligibilityReason::DeliveryOwned => DeliveryEligibilityReasonDto::DeliveryOwned,
        DeliveryEligibilityReason::AlreadyMerged => DeliveryEligibilityReasonDto::AlreadyMerged,
        DeliveryEligibilityReason::ReconciliationRequired => {
            DeliveryEligibilityReasonDto::ReconciliationRequired
        }
        DeliveryEligibilityReason::RepositoryBusy => DeliveryEligibilityReasonDto::RepositoryBusy,
        DeliveryEligibilityReason::RepositoryUnavailable => {
            DeliveryEligibilityReasonDto::RepositoryUnavailable
        }
        DeliveryEligibilityReason::StoreUnavailable => {
            DeliveryEligibilityReasonDto::StoreUnavailable
        }
        DeliveryEligibilityReason::RuntimeObservationUnavailable => {
            DeliveryEligibilityReasonDto::RuntimeObservationUnavailable
        }
        DeliveryEligibilityReason::ServiceNotReady => DeliveryEligibilityReasonDto::ServiceNotReady,
    })
}

const fn allowed_action(value: DeliveryAllowedAction) -> DeliveryAllowedActionDto {
    match value {
        DeliveryAllowedAction::RunPreflight => DeliveryAllowedActionDto::RunPreflight,
        DeliveryAllowedAction::AcceptMerge => DeliveryAllowedActionDto::AcceptMerge,
        DeliveryAllowedAction::RemoveWorktree => DeliveryAllowedActionDto::RemoveWorktree,
        DeliveryAllowedAction::DeleteBranch => DeliveryAllowedActionDto::DeleteBranch,
    }
}

fn evidence(value: &DeliveryEvidenceProjection) -> DeliveryEvidenceSummaryDto {
    DeliveryEvidenceSummaryDto {
        review_generation: value.review_generation(),
        workspace_fingerprint: value.workspace_fingerprint().as_str().to_owned(),
    }
}

fn target(value: &DeliveryTargetObservation) -> DeliveryTargetObservationDto {
    match value {
        DeliveryTargetObservation::Available { branch, head } => {
            DeliveryTargetObservationDto::available(
                branch.as_str().to_owned(),
                head.as_str().to_owned(),
            )
        }
        DeliveryTargetObservation::Unavailable { reason } => {
            DeliveryTargetObservationDto::unavailable(target_unavailable(*reason))
        }
    }
}

const fn target_unavailable(
    value: DeliveryTargetUnavailableReason,
) -> DeliveryTargetUnavailableReasonDto {
    match value {
        DeliveryTargetUnavailableReason::TargetBranchDetached => {
            DeliveryTargetUnavailableReasonDto::Detached
        }
        DeliveryTargetUnavailableReason::TargetBranchMismatch => {
            DeliveryTargetUnavailableReasonDto::BranchMismatch
        }
        DeliveryTargetUnavailableReason::ReconciliationRequired => {
            DeliveryTargetUnavailableReasonDto::RepositoryPoisoned
        }
        DeliveryTargetUnavailableReason::TargetWorktreeDirty
        | DeliveryTargetUnavailableReason::TargetIgnoredPathCollision
        | DeliveryTargetUnavailableReason::TargetGitOperationInProgress
        | DeliveryTargetUnavailableReason::UnsafeGitConfiguration
        | DeliveryTargetUnavailableReason::UnsupportedGitAttributes
        | DeliveryTargetUnavailableReason::SourceAlreadyInTarget
        | DeliveryTargetUnavailableReason::TargetHeadChanged
        | DeliveryTargetUnavailableReason::RuntimeUnavailable
        | DeliveryTargetUnavailableReason::ProcessCleanupUnproven => {
            DeliveryTargetUnavailableReasonDto::ObservationUnavailable
        }
    }
}

fn source(value: &DeliverySourceProjection) -> DeliverySourceDto {
    DeliverySourceDto {
        state: match value.state() {
            DeliverySourceProjectionState::ObjectPending => DeliverySourceStateDto::ObjectPending,
            DeliverySourceProjectionState::CommitPending => DeliverySourceStateDto::CommitPending,
            DeliverySourceProjectionState::Committed => DeliverySourceStateDto::Committed,
            DeliverySourceProjectionState::ReconciliationRequired => {
                DeliverySourceStateDto::ReconciliationRequired
            }
        },
        version: value.version().get(),
        source_ref: value.source_ref().as_str().to_owned(),
        source_oid: value.source_oid().map(|oid| oid.as_str().to_owned()),
    }
}

fn merge_operation(
    value: &DeliveryMergeOperationProjection,
) -> ApiResult<DeliveryMergeOperationDto> {
    Ok(DeliveryMergeOperationDto {
        operation_id: value.operation_id().as_uuid(),
        version: value.version().get(),
        state: merge_state(value.state()),
        review_generation: value.review_generation(),
        workspace_fingerprint: value.workspace_fingerprint().as_str().to_owned(),
        candidate_source_tree: value
            .candidate_source_tree()
            .map(|oid| oid.as_str().to_owned()),
        preflight_source_commit: value
            .preflight_source_commit()
            .map(|oid| oid.as_str().to_owned()),
        source_commit: value.source_commit().map(|oid| oid.as_str().to_owned()),
        target_branch: value.target_branch().as_str().to_owned(),
        target_head: value.target_head().as_str().to_owned(),
        conflicts: value.conflicts().map(conflicts).transpose()?,
        failure: value.failure().map(failure),
    })
}

const fn merge_state(value: DeliveryPreflightState) -> DeliveryMergeStateDto {
    match value {
        DeliveryPreflightState::PreflightPending => DeliveryMergeStateDto::PreflightPending,
        DeliveryPreflightState::PreflightReady => DeliveryMergeStateDto::PreflightReady,
        DeliveryPreflightState::Accepted => DeliveryMergeStateDto::Accepted,
        DeliveryPreflightState::MergePending => DeliveryMergeStateDto::MergePending,
        DeliveryPreflightState::Merged => DeliveryMergeStateDto::Merged,
        DeliveryPreflightState::AbortPending => DeliveryMergeStateDto::AbortPending,
        DeliveryPreflightState::Conflict => DeliveryMergeStateDto::Conflict,
        DeliveryPreflightState::Rejected => DeliveryMergeStateDto::Rejected,
        DeliveryPreflightState::Stale => DeliveryMergeStateDto::Stale,
        DeliveryPreflightState::Superseded => DeliveryMergeStateDto::Superseded,
        DeliveryPreflightState::Failed => DeliveryMergeStateDto::Failed,
        DeliveryPreflightState::ReconciliationRequired => {
            DeliveryMergeStateDto::ReconciliationRequired
        }
    }
}

fn conflicts(value: &DeliveryConflictSummaryProjection) -> ApiResult<DeliveryConflictSummaryDto> {
    Ok(DeliveryConflictSummaryDto {
        path_count: value.path_count(),
        paths: value
            .paths()
            .iter()
            .map(|path| {
                Ok(DeliveryConflictPathDto {
                    encoding: match path.encoding() {
                        DeliveryConflictPathEncoding::Utf8 => DeliveryConflictPathEncodingDto::Utf8,
                        DeliveryConflictPathEncoding::Base64url => {
                            DeliveryConflictPathEncodingDto::Base64url
                        }
                    },
                    path: std::str::from_utf8(path.path_bytes())
                        .map_err(|_| error::invalid_projection())?
                        .to_owned(),
                })
            })
            .collect::<ApiResult<Vec<_>>>()?,
        payload_bytes: value.payload_bytes(),
        truncated: value.truncated(),
    })
}

fn cleanup_operation(value: &DeliveryCleanupOperationProjection) -> DeliveryCleanupOperationDto {
    DeliveryCleanupOperationDto {
        operation_id: value.operation_id().as_uuid(),
        cleanup_kind: cleanup_kind(value.cleanup_kind()),
        version: value.version().get(),
        state: cleanup_state(value.state()),
        expected_disposition_version: value.expected_disposition_version().get(),
        expected_merge_operation_id: value.expected_merge_operation_id().as_uuid(),
        expected_source_ref: value.expected_source_ref().as_str().to_owned(),
        expected_source_oid: value.expected_source_oid().as_str().to_owned(),
        target_branch: value
            .target_branch()
            .map(|branch| branch.as_str().to_owned()),
        target_head: value.target_head().map(|head| head.as_str().to_owned()),
        failure: value.failure().map(failure),
    }
}

const fn cleanup_kind(value: DeliveryCleanupOperationKind) -> DeliveryCleanupKindDto {
    match value {
        DeliveryCleanupOperationKind::RemoveWorktree => DeliveryCleanupKindDto::RemoveWorktree,
        DeliveryCleanupOperationKind::DeleteBranch => DeliveryCleanupKindDto::DeleteBranch,
    }
}

const fn cleanup_state(value: DeliveryCleanupOperationState) -> DeliveryCleanupStateDto {
    match value {
        DeliveryCleanupOperationState::UnlockPending => DeliveryCleanupStateDto::UnlockPending,
        DeliveryCleanupOperationState::UnlockedPendingRemove => {
            DeliveryCleanupStateDto::UnlockedPendingRemove
        }
        DeliveryCleanupOperationState::RemovePending => DeliveryCleanupStateDto::RemovePending,
        DeliveryCleanupOperationState::DeletePending => DeliveryCleanupStateDto::DeletePending,
        DeliveryCleanupOperationState::Completed => DeliveryCleanupStateDto::Completed,
        DeliveryCleanupOperationState::Failed => DeliveryCleanupStateDto::Failed,
        DeliveryCleanupOperationState::ReconciliationRequired => {
            DeliveryCleanupStateDto::ReconciliationRequired
        }
    }
}

fn disposition(value: &DeliveryArtifactDispositionProjection) -> DeliveryArtifactDispositionDto {
    DeliveryArtifactDispositionDto {
        merged_operation_id: value.merged_operation_id().as_uuid(),
        source_ref: value.source_ref().as_str().to_owned(),
        source_oid: value.source_oid().as_str().to_owned(),
        worktree: DeliveryWorktreeDispositionDto {
            state: match value.worktree_state() {
                DeliveryWorktreeDispositionState::RetainedLocked => {
                    DeliveryWorktreeDispositionStateDto::RetainedLocked
                }
                DeliveryWorktreeDispositionState::RetainedUnlocked => {
                    DeliveryWorktreeDispositionStateDto::RetainedUnlocked
                }
                DeliveryWorktreeDispositionState::Removed => {
                    DeliveryWorktreeDispositionStateDto::Removed
                }
                DeliveryWorktreeDispositionState::ReconciliationRequired => {
                    DeliveryWorktreeDispositionStateDto::ReconciliationRequired
                }
            },
            version: value.worktree_version().get(),
            failure: value.worktree_failure().map(failure),
        },
        branch: DeliveryBranchDispositionDto {
            state: match value.branch_state() {
                DeliveryBranchDispositionState::Retained => {
                    DeliveryBranchDispositionStateDto::Retained
                }
                DeliveryBranchDispositionState::Deleted => {
                    DeliveryBranchDispositionStateDto::Deleted
                }
                DeliveryBranchDispositionState::ReconciliationRequired => {
                    DeliveryBranchDispositionStateDto::ReconciliationRequired
                }
            },
            version: value.branch_version().get(),
            failure: value.branch_failure().map(failure),
        },
    }
}

fn failure(code: &str) -> DeliveryOperationFailureDto {
    DeliveryOperationFailureDto {
        code: code.to_owned(),
    }
}

#[allow(dead_code)]
const fn _query_reason_is_exhaustive(value: DeliveryQueryUnavailableReason) {
    match value {
        DeliveryQueryUnavailableReason::StoreUnavailable
        | DeliveryQueryUnavailableReason::OrchestrationUnavailable => {}
    }
}
