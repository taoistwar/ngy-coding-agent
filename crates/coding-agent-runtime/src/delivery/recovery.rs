//! Bound delivery recovery intents and pure classification.
//!
//! The implementation lives beside, rather than inside, source/target
//! mutation so every recovery path starts from fresh authentication. It must
//! never reset, clean, checkout, or otherwise repair a worktree. Persisted
//! values remain inert until their opaque runtime intent is bound to a newly
//! authenticated capability.

use std::fmt;

use coding_agent_core::WorkspaceFingerprint;
use tokio_util::sync::CancellationToken;

use super::merge::revalidate_expected_delivery_merge_object;
use super::source_commit::DeliverySourceCommitInput;
use super::{
    DeliveryAbortAppliedPersistenceBinding, DeliveryAbortCapability, DeliveryAbortError,
    DeliveryAbortOutcome, DeliveryAbortPendingDisposition, DeliveryAbortProof,
    DeliveryAbortProofCapture, DeliveryCandidateTree, DeliveryCommitOid, DeliveryExpectedMerge,
    DeliveryExpectedMergePersistenceBinding, DeliveryKnownMergeConflict, DeliveryMergeAppliedProof,
    DeliveryMergeError, DeliveryMergeInput, DeliveryMergeOutcome, DeliveryMergePendingDisposition,
    DeliveryPersistedMergeRecovery, DeliveryPersistedSourceRecovery, DeliveryPersistedSourceState,
    DeliveryPreflightResult, DeliverySourceAppliedPersistenceBinding, DeliverySourceCapability,
    DeliverySourceCommit, DeliverySourceError, DeliverySourceObjectPersistenceBinding,
    DeliverySourcePendingState, DeliverySourceProvisioner, DeliverySourceRecoveryDisposition,
    DeliveryTargetCapability, DeliveryTargetError, DeliveryTargetProvisioner, DeliveryTreeOid,
    abort_expected_delivery_merge, apply_expected_delivery_merge, build_expected_delivery_merge,
    capture_delivery_abort_proof,
};
use crate::root_capability::DurableDirectoryIdentityV1;
use crate::worktree::CleanupTopologyIntentV1;
use crate::{WorktreeIdentity, WorktreeReservation};

mod abort;
mod capability;
mod merge;
mod projection;

pub use abort::{
    DeliveryPersistedAbortRecoveryObservation, capture_persisted_delivery_abort_recovery,
    classify_delivery_abort_pending, retry_delivery_abort_pending,
    retry_persisted_delivery_abort_pending,
};
pub use capability::{
    DeliveryMergeRecoveryBindingOutcome, DeliveryMergeRecoveryCapability,
    DeliverySourceRecoveryBindingOutcome, DeliverySourceRecoveryCapability,
    DeliverySourceRecoveryIntent, DeliveryTargetRecoveryBindingOutcome,
    DeliveryTargetRecoveryCapability, DeliveryTargetRecoveryIntent,
    bind_persisted_delivery_merge_recovery,
};
pub use merge::{
    capture_delivery_abort_proof_from_recovery, capture_persisted_delivery_abort_proof,
    classify_delivery_merge_pending, classify_persisted_delivery_merge_pending,
    retry_delivery_merge_pending, retry_persisted_delivery_merge_pending,
};
pub use projection::{
    build_expected_persisted_delivery_merge, project_persisted_delivery_source_applied,
    project_persisted_delivery_source_object,
};

pub(super) use capability::{RecoveryObservation, disposition_for};

#[cfg(test)]
use abort::{AbortPendingDecision, AbortPendingObservation, abort_pending_decision_for};
#[cfg(test)]
use merge::{MergePendingDecision, MergePendingObservation, merge_pending_decision_for};

fn is_source_recovery_mismatch(error: DeliverySourceError) -> bool {
    matches!(
        error,
        DeliverySourceError::SourceChanged
            | DeliverySourceError::AuthenticationChanged
            | DeliverySourceError::UnsafeGitConfiguration
            | DeliverySourceError::UnsafeIndex
            | DeliverySourceError::CommandFailed
            | DeliverySourceError::BoundsExceeded
    )
}

async fn committed_source_is_current(
    provisioner: &DeliverySourceProvisioner,
    source: &DeliverySourceCapability,
    candidate: &DeliveryCandidateTree,
    source_commit: &DeliverySourceCommit,
    source_input: &DeliverySourceCommitInput,
    cancellation: CancellationToken,
) -> Result<bool, DeliveryAbortError> {
    match provisioner
        .revalidate_preflight_committed_source(
            source,
            candidate,
            source_commit,
            source_input,
            cancellation,
        )
        .await
    {
        Ok(()) => Ok(true),
        Err(error) => source_observation_failure(error, false),
    }
}

fn source_observation_failure<T>(
    error: DeliverySourceError,
    reconciliation: T,
) -> Result<T, DeliveryAbortError> {
    match error {
        DeliverySourceError::SourceChanged
        | DeliverySourceError::BoundsExceeded
        | DeliverySourceError::UnsafeGitConfiguration
        | DeliverySourceError::AuthenticationChanged
        | DeliverySourceError::UnsafeIndex => Ok(reconciliation),
        error => Err(DeliveryAbortError::Source(error)),
    }
}

fn target_observation_failure<T>(
    error: DeliveryTargetError,
    reconciliation: T,
) -> Result<T, DeliveryAbortError> {
    match error {
        DeliveryTargetError::AuthenticationChanged
        | DeliveryTargetError::TargetDetached
        | DeliveryTargetError::TargetBranchMismatch
        | DeliveryTargetError::TargetHeadChanged
        | DeliveryTargetError::TargetWorktreeDirty
        | DeliveryTargetError::TargetIgnoredPathCollision
        | DeliveryTargetError::TargetGitOperationInProgress
        | DeliveryTargetError::UnsafeGitConfiguration
        | DeliveryTargetError::UnsupportedGitAttributes
        | DeliveryTargetError::BoundsExceeded => Ok(reconciliation),
        error => Err(DeliveryAbortError::Target(error)),
    }
}

fn merge_object_observation_failure<T>(
    error: DeliveryMergeError,
    reconciliation: T,
) -> Result<T, DeliveryAbortError> {
    match error {
        DeliveryMergeError::Source(error) => source_observation_failure(error, reconciliation),
        DeliveryMergeError::Target(error) => target_observation_failure(error, reconciliation),
        DeliveryMergeError::InvalidInput
        | DeliveryMergeError::Preflight(_)
        | DeliveryMergeError::PreflightStale
        | DeliveryMergeError::ExpectedObjectInvalid => Ok(reconciliation),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_machine_admits_only_the_documented_pending_combinations() {
        assert_eq!(
            disposition_for(
                DeliverySourcePendingState::ObjectPending,
                RecoveryObservation::ApprovedPreStage,
            ),
            DeliverySourceRecoveryDisposition::ReplayObject
        );
        assert_eq!(
            disposition_for(
                DeliverySourcePendingState::CommitPending,
                RecoveryObservation::ApprovedPreStage,
            ),
            DeliverySourceRecoveryDisposition::Continue
        );
        assert_eq!(
            disposition_for(
                DeliverySourcePendingState::CommitPending,
                RecoveryObservation::CandidateStaged,
            ),
            DeliverySourceRecoveryDisposition::StageComplete
        );
        assert_eq!(
            disposition_for(
                DeliverySourcePendingState::CommitPending,
                RecoveryObservation::ExpectedApplied,
            ),
            DeliverySourceRecoveryDisposition::Applied
        );
        for observation in [
            RecoveryObservation::CandidateStaged,
            RecoveryObservation::ExpectedApplied,
            RecoveryObservation::Inconsistent,
        ] {
            assert_eq!(
                disposition_for(DeliverySourcePendingState::ObjectPending, observation),
                DeliverySourceRecoveryDisposition::ReconciliationRequired
            );
        }
    }

    #[test]
    fn merge_pending_truth_table_never_treats_unknown_state_as_retryable() {
        assert_eq!(
            merge_pending_decision_for(MergePendingObservation::OldTargetClean),
            MergePendingDecision::RetryExactMerge
        );
        assert_eq!(
            merge_pending_decision_for(MergePendingObservation::ExpectedMergeApplied),
            MergePendingDecision::MergeApplied
        );
        assert_eq!(
            merge_pending_decision_for(MergePendingObservation::Inconsistent),
            MergePendingDecision::ReconciliationRequired
        );
    }

    #[test]
    fn abort_pending_truth_table_requires_the_exact_durable_conflict() {
        assert_eq!(
            abort_pending_decision_for(AbortPendingObservation::ExactConflict),
            AbortPendingDecision::RetryExactAbort
        );
        assert_eq!(
            abort_pending_decision_for(AbortPendingObservation::OldTargetClean),
            AbortPendingDecision::AbortApplied
        );
        assert_eq!(
            abort_pending_decision_for(AbortPendingObservation::Inconsistent),
            AbortPendingDecision::ReconciliationRequired
        );
    }

    #[test]
    fn observation_drift_becomes_reconciliation_but_cancellation_stays_typed() {
        assert_eq!(
            source_observation_failure(
                DeliverySourceError::SourceChanged,
                DeliveryMergePendingDisposition::ReconciliationRequired,
            ),
            Ok(DeliveryMergePendingDisposition::ReconciliationRequired)
        );
        assert_eq!(
            target_observation_failure(
                DeliveryTargetError::TargetWorktreeDirty,
                DeliveryAbortPendingDisposition::ReconciliationRequired,
            ),
            Ok(DeliveryAbortPendingDisposition::ReconciliationRequired)
        );
        assert!(matches!(
            source_observation_failure(DeliverySourceError::Cancelled, ()),
            Err(DeliveryAbortError::Source(DeliverySourceError::Cancelled))
        ));
        assert!(matches!(
            target_observation_failure(DeliveryTargetError::Cancelled, ()),
            Err(DeliveryAbortError::Target(DeliveryTargetError::Cancelled))
        ));
    }
}
