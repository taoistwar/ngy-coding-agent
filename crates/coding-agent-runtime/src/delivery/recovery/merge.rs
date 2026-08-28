use super::*;
/// Observes one durable `MergePending` intent without mutating the object
/// database, target ref, index, or worktree.
///
/// The exact expected object and committed source are re-proved before the
/// target is classified. Only the two documented stable target scenes are
/// resumable: the old clean target may retry the fixed merge, while the exact
/// expected clean target means a prior merge reply was lost. A live conflict
/// has no durable child receipt at this phase and therefore always requires
/// reconciliation.
#[allow(clippy::too_many_arguments)]
pub async fn classify_delivery_merge_pending(
    source_provisioner: &DeliverySourceProvisioner,
    target_provisioner: &DeliveryTargetProvisioner,
    source: &DeliverySourceCapability,
    target_recovery: &DeliveryTargetRecoveryCapability,
    candidate: &DeliveryCandidateTree,
    source_commit: &DeliverySourceCommit,
    source_input: &DeliverySourceCommitInput,
    expected: &DeliveryExpectedMerge,
    cancellation: CancellationToken,
) -> Result<DeliveryMergePendingDisposition, DeliveryAbortError> {
    let target = target_recovery.target();
    let source_provenance = match source.candidate_tree_provenance() {
        Ok(provenance) => provenance,
        Err(error) => {
            return source_observation_failure(
                error,
                DeliveryMergePendingDisposition::ReconciliationRequired,
            );
        }
    };
    if !source_input.matches_identity(source.identity())
        || !expected.is_bound_to(&source_provenance, target, candidate, source_commit)
    {
        return Ok(DeliveryMergePendingDisposition::ReconciliationRequired);
    }
    if !committed_source_is_current(
        source_provisioner,
        source,
        candidate,
        source_commit,
        source_input,
        cancellation.clone(),
    )
    .await?
    {
        return Ok(DeliveryMergePendingDisposition::ReconciliationRequired);
    }
    if let Err(error) = revalidate_expected_delivery_merge_object(
        target_provisioner,
        target,
        expected,
        cancellation.clone(),
    )
    .await
    {
        return merge_object_observation_failure(
            error,
            DeliveryMergePendingDisposition::ReconciliationRequired,
        );
    }

    let observation = match target_provisioner
        .revalidate_delivery_target(target, cancellation.clone())
        .await
    {
        Ok(()) => MergePendingObservation::OldTargetClean,
        Err(DeliveryTargetError::TargetHeadChanged) => {
            match target_provisioner
                .revalidate_applied_delivery_target(target, expected.commit(), cancellation.clone())
                .await
            {
                Ok(()) => MergePendingObservation::ExpectedMergeApplied,
                Err(error) => {
                    return target_observation_failure(
                        error,
                        DeliveryMergePendingDisposition::ReconciliationRequired,
                    );
                }
            }
        }
        Err(error) => {
            return target_observation_failure(
                error,
                DeliveryMergePendingDisposition::ReconciliationRequired,
            );
        }
    };
    if !committed_source_is_current(
        source_provisioner,
        source,
        candidate,
        source_commit,
        source_input,
        cancellation.clone(),
    )
    .await?
    {
        return Ok(DeliveryMergePendingDisposition::ReconciliationRequired);
    }
    if !merge_target_observation_is_current(
        target_provisioner,
        target,
        expected,
        observation,
        cancellation,
    )
    .await?
    {
        return Ok(DeliveryMergePendingDisposition::ReconciliationRequired);
    }
    Ok(match merge_pending_decision_for(observation) {
        MergePendingDecision::RetryExactMerge => DeliveryMergePendingDisposition::RetryExactMerge,
        MergePendingDecision::MergeApplied => {
            let Some(proof) = DeliveryMergeAppliedProof::from_recovery_postcondition(
                expected,
                source,
                target,
                source_commit,
            ) else {
                return Ok(DeliveryMergePendingDisposition::ReconciliationRequired);
            };
            DeliveryMergePendingDisposition::MergeApplied(Box::new(proof))
        }
        #[cfg(test)]
        MergePendingDecision::ReconciliationRequired => {
            DeliveryMergePendingDisposition::ReconciliationRequired
        }
    })
}

/// Re-enters the fixed Task 14 merge mutation through a freshly authenticated
/// recovery binding. Callers route here only after
/// [`classify_delivery_merge_pending`] returns `RetryExactMerge`; the merge
/// implementation still repeats all source, target, object, ancestry, and
/// collision proofs before spawning the child, so retaining the capability
/// cannot bypass a changed scene.
#[allow(clippy::too_many_arguments)]
pub async fn retry_delivery_merge_pending(
    source_provisioner: &DeliverySourceProvisioner,
    target_provisioner: &DeliveryTargetProvisioner,
    source: &DeliverySourceCapability,
    target_recovery: &DeliveryTargetRecoveryCapability,
    candidate: &DeliveryCandidateTree,
    source_commit: &DeliverySourceCommit,
    source_input: &DeliverySourceCommitInput,
    preflight: &DeliveryPreflightResult,
    expected: &DeliveryExpectedMerge,
    cancellation: CancellationToken,
) -> Result<DeliveryMergeOutcome, DeliveryMergeError> {
    apply_expected_delivery_merge(
        source_provisioner,
        target_provisioner,
        source,
        target_recovery.target(),
        candidate,
        source_commit,
        source_input,
        preflight,
        expected,
        cancellation,
    )
    .await
}

/// Captures the exact unexpected-conflict scene produced by a merge resumed
/// through a fresh recovery binding. This preserves the non-forgeable child
/// token and repeats the same source/target proof before any facts can cross
/// the durable `AbortPending` barrier.
#[allow(clippy::too_many_arguments)]
pub async fn capture_delivery_abort_proof_from_recovery(
    source_provisioner: &DeliverySourceProvisioner,
    target_provisioner: &DeliveryTargetProvisioner,
    source: &DeliverySourceCapability,
    target_recovery: &DeliveryTargetRecoveryCapability,
    source_commit: &DeliverySourceCommit,
    expected: &DeliveryExpectedMerge,
    known_conflict: DeliveryKnownMergeConflict,
    cancellation: CancellationToken,
) -> Result<DeliveryAbortProofCapture, DeliveryAbortError> {
    capture_delivery_abort_proof(
        source_provisioner,
        target_provisioner,
        source,
        target_recovery.target(),
        source_commit,
        expected,
        known_conflict,
        cancellation,
    )
    .await
}

/// Classifies a fully rebound persisted merge without exposing its component
/// OIDs or mutation capabilities.
pub async fn classify_persisted_delivery_merge_pending(
    source_provisioner: &DeliverySourceProvisioner,
    target_provisioner: &DeliveryTargetProvisioner,
    recovery: &DeliveryMergeRecoveryCapability,
    cancellation: CancellationToken,
) -> Result<DeliveryMergePendingDisposition, DeliveryAbortError> {
    classify_delivery_merge_pending(
        source_provisioner,
        target_provisioner,
        recovery.source.source(),
        &recovery.target,
        recovery.source.candidate(),
        recovery
            .source
            .expected()
            .expect("merge recovery is bound only with a source commit"),
        recovery.source.input(),
        &recovery.expected,
        cancellation,
    )
    .await
}

/// Retries at most the one fixed merge through a fully rebound persisted
/// capability.  The underlying mutation path still performs its closing
/// source, target, ancestry, collision, and object checks.
pub async fn retry_persisted_delivery_merge_pending(
    source_provisioner: &DeliverySourceProvisioner,
    target_provisioner: &DeliveryTargetProvisioner,
    recovery: &DeliveryMergeRecoveryCapability,
    cancellation: CancellationToken,
) -> Result<DeliveryMergeOutcome, DeliveryMergeError> {
    retry_delivery_merge_pending(
        source_provisioner,
        target_provisioner,
        recovery.source.source(),
        &recovery.target,
        recovery.source.candidate(),
        recovery
            .source
            .expected()
            .expect("merge recovery is bound only with a source commit"),
        recovery.source.input(),
        &recovery.preflight,
        &recovery.expected,
        cancellation,
    )
    .await
}

/// Captures an unexpected-conflict proof after a persisted merge retry while
/// keeping the rebound source/target/object authority opaque.
pub async fn capture_persisted_delivery_abort_proof(
    source_provisioner: &DeliverySourceProvisioner,
    target_provisioner: &DeliveryTargetProvisioner,
    recovery: &DeliveryMergeRecoveryCapability,
    known_conflict: DeliveryKnownMergeConflict,
    cancellation: CancellationToken,
) -> Result<DeliveryAbortProofCapture, DeliveryAbortError> {
    capture_delivery_abort_proof_from_recovery(
        source_provisioner,
        target_provisioner,
        recovery.source.source(),
        &recovery.target,
        recovery
            .source
            .expected()
            .expect("merge recovery is bound only with a source commit"),
        &recovery.expected,
        known_conflict,
        cancellation,
    )
    .await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MergePendingObservation {
    OldTargetClean,
    ExpectedMergeApplied,
    #[cfg(test)]
    Inconsistent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MergePendingDecision {
    RetryExactMerge,
    MergeApplied,
    #[cfg(test)]
    ReconciliationRequired,
}

pub(super) const fn merge_pending_decision_for(
    observation: MergePendingObservation,
) -> MergePendingDecision {
    match observation {
        MergePendingObservation::OldTargetClean => MergePendingDecision::RetryExactMerge,
        MergePendingObservation::ExpectedMergeApplied => MergePendingDecision::MergeApplied,
        #[cfg(test)]
        MergePendingObservation::Inconsistent => MergePendingDecision::ReconciliationRequired,
    }
}

async fn merge_target_observation_is_current(
    provisioner: &DeliveryTargetProvisioner,
    target: &DeliveryTargetCapability,
    expected: &DeliveryExpectedMerge,
    observation: MergePendingObservation,
    cancellation: CancellationToken,
) -> Result<bool, DeliveryAbortError> {
    let result = match observation {
        MergePendingObservation::OldTargetClean => {
            provisioner
                .revalidate_delivery_target(target, cancellation)
                .await
        }
        MergePendingObservation::ExpectedMergeApplied => {
            provisioner
                .revalidate_applied_delivery_target(target, expected.commit(), cancellation)
                .await
        }
        #[cfg(test)]
        MergePendingObservation::Inconsistent => return Ok(false),
    };
    match result {
        Ok(()) => Ok(true),
        Err(error) => target_observation_failure(error, false),
    }
}
