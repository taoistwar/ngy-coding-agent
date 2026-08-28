use super::*;
/// Observes one durable `AbortPending` proof without executing the fixed
/// abort command or any other Git mutation.
///
/// A clean old target is the exact lost-reply postcondition. Otherwise only
/// the same authenticated conflict scene captured by the durable abort proof
/// may authorize an exact abort retry. Autostash, digest drift, another Git
/// operation, or any other mismatch fails closed to reconciliation.
#[allow(clippy::too_many_arguments)]
pub async fn classify_delivery_abort_pending(
    source_provisioner: &DeliverySourceProvisioner,
    target_provisioner: &DeliveryTargetProvisioner,
    source: &DeliverySourceCapability,
    target_recovery: &DeliveryTargetRecoveryCapability,
    source_commit: &DeliverySourceCommit,
    capability: &DeliveryAbortCapability,
    cancellation: CancellationToken,
) -> Result<DeliveryAbortPendingDisposition, DeliveryAbortError> {
    let target = target_recovery.target();
    let proof = capability.proof();
    if !proof.matches_context(source, target, source_commit) {
        return Ok(DeliveryAbortPendingDisposition::ReconciliationRequired);
    }
    if !committed_source_is_current(
        source_provisioner,
        source,
        proof.candidate(),
        source_commit,
        proof.source_input(),
        cancellation.clone(),
    )
    .await?
    {
        return Ok(DeliveryAbortPendingDisposition::ReconciliationRequired);
    }

    let observation = match target_provisioner
        .revalidate_delivery_target(target, cancellation.clone())
        .await
    {
        Ok(()) => AbortPendingObservation::OldTargetClean,
        Err(DeliveryTargetError::TargetGitOperationInProgress) => {
            let observed = match target_provisioner
                .observe_expected_merge_conflict(
                    target,
                    proof.expected_merge_base(),
                    proof.expected_source_parent(),
                    proof.expected_tree(),
                    cancellation.clone(),
                )
                .await
            {
                Ok(Some(observed)) => observed,
                Ok(None) => {
                    return Ok(DeliveryAbortPendingDisposition::ReconciliationRequired);
                }
                Err(error) => {
                    return target_observation_failure(
                        error,
                        DeliveryAbortPendingDisposition::ReconciliationRequired,
                    );
                }
            };
            if !proof.matches_observation(&observed) {
                return Ok(DeliveryAbortPendingDisposition::ReconciliationRequired);
            }
            AbortPendingObservation::ExactConflict
        }
        Err(error) => {
            return target_observation_failure(
                error,
                DeliveryAbortPendingDisposition::ReconciliationRequired,
            );
        }
    };
    if !committed_source_is_current(
        source_provisioner,
        source,
        proof.candidate(),
        source_commit,
        proof.source_input(),
        cancellation.clone(),
    )
    .await?
    {
        return Ok(DeliveryAbortPendingDisposition::ReconciliationRequired);
    }
    if !abort_target_observation_is_current(
        target_provisioner,
        target,
        proof,
        observation,
        cancellation,
    )
    .await?
    {
        return Ok(DeliveryAbortPendingDisposition::ReconciliationRequired);
    }
    Ok(match abort_pending_decision_for(observation) {
        AbortPendingDecision::RetryExactAbort => DeliveryAbortPendingDisposition::RetryExactAbort,
        AbortPendingDecision::AbortApplied => {
            DeliveryAbortPendingDisposition::AbortApplied(proof.applied_proof())
        }
        #[cfg(test)]
        AbortPendingDecision::ReconciliationRequired => {
            DeliveryAbortPendingDisposition::ReconciliationRequired
        }
    })
}

/// Re-enters the one fixed `git merge --abort` command through a freshly
/// authenticated recovery binding. The durable [`DeliveryAbortCapability`]
/// remains mandatory and the abort implementation re-proves the exact
/// conflict (or query-first old-clean postcondition) before any mutation.
#[allow(clippy::too_many_arguments)]
pub async fn retry_delivery_abort_pending(
    source_provisioner: &DeliverySourceProvisioner,
    target_provisioner: &DeliveryTargetProvisioner,
    source: &DeliverySourceCapability,
    target_recovery: &DeliveryTargetRecoveryCapability,
    source_commit: &DeliverySourceCommit,
    capability: &DeliveryAbortCapability,
    cancellation: CancellationToken,
) -> Result<DeliveryAbortOutcome, DeliveryAbortError> {
    abort_expected_delivery_merge(
        source_provisioner,
        target_provisioner,
        source,
        target_recovery.target(),
        source_commit,
        capability,
        cancellation,
    )
    .await
}

/// Fresh, non-mutating classification of an already durable `AbortPending`
/// operation. A live exact conflict returns an inert proof which must still
/// cross the application's exact Store authorizer. An exact clean old target
/// is the lost-reply postcondition and needs no new mutation authority.
pub enum DeliveryPersistedAbortRecoveryObservation {
    Conflict(DeliveryAbortProof),
    Applied(DeliveryAbortAppliedPersistenceBinding),
    ReconciliationRequired,
}

impl fmt::Debug for DeliveryPersistedAbortRecoveryObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Conflict(_) => "DeliveryPersistedAbortRecoveryObservation::Conflict(<opaque>)",
            Self::Applied(_) => "DeliveryPersistedAbortRecoveryObservation::Applied(<opaque>)",
            Self::ReconciliationRequired => {
                "DeliveryPersistedAbortRecoveryObservation::ReconciliationRequired"
            }
        })
    }
}

/// Rehydrates no authority from Store scalars. The returned conflict proof is
/// rebuilt only from the fully rebound merge capability and two matching live
/// conflict observations; the caller must separately prove that its durable
/// `AbortPending` row contains the same persistence projection.
pub async fn capture_persisted_delivery_abort_recovery(
    source_provisioner: &DeliverySourceProvisioner,
    target_provisioner: &DeliveryTargetProvisioner,
    recovery: &DeliveryMergeRecoveryCapability,
    cancellation: CancellationToken,
) -> Result<DeliveryPersistedAbortRecoveryObservation, DeliveryAbortError> {
    let source = recovery.source.source();
    let source_commit = recovery
        .source
        .expected()
        .expect("merge recovery is bound only with a source commit");
    let target = recovery.target.target();
    if !committed_source_is_current(
        source_provisioner,
        source,
        recovery.source.candidate(),
        source_commit,
        recovery.source.input(),
        cancellation.clone(),
    )
    .await?
    {
        return Ok(DeliveryPersistedAbortRecoveryObservation::ReconciliationRequired);
    }

    match target_provisioner
        .revalidate_delivery_target(target, cancellation.clone())
        .await
    {
        Ok(()) => {
            if !committed_source_is_current(
                source_provisioner,
                source,
                recovery.source.candidate(),
                source_commit,
                recovery.source.input(),
                cancellation.clone(),
            )
            .await?
            {
                return Ok(DeliveryPersistedAbortRecoveryObservation::ReconciliationRequired);
            }
            if let Err(error) = target_provisioner
                .revalidate_delivery_target(target, cancellation)
                .await
            {
                return target_observation_failure(
                    error,
                    DeliveryPersistedAbortRecoveryObservation::ReconciliationRequired,
                );
            }
            Ok(DeliveryPersistedAbortRecoveryObservation::Applied(
                DeliveryAbortAppliedPersistenceBinding::new(
                    format!("refs/heads/{}", target.branch_name()),
                    target.head_id().to_owned(),
                    format!("refs/heads/{}", source.branch_name()),
                    source_commit.commit().as_str().to_owned(),
                    source.common_directory_identity().as_hex().to_owned(),
                    source.admin_directory_identity().as_hex().to_owned(),
                    super::super::persistence::encode_lower_hex(source.config_attributes_digest()),
                ),
            ))
        }
        Err(DeliveryTargetError::TargetGitOperationInProgress) => {
            let observed = match target_provisioner
                .observe_expected_merge_conflict(
                    target,
                    recovery.expected.merge_base(),
                    source_commit.commit(),
                    recovery.expected.tree(),
                    cancellation.clone(),
                )
                .await
            {
                Ok(Some(observed)) => observed,
                Ok(None) => {
                    return Ok(DeliveryPersistedAbortRecoveryObservation::ReconciliationRequired);
                }
                Err(error) => {
                    return target_observation_failure(
                        error,
                        DeliveryPersistedAbortRecoveryObservation::ReconciliationRequired,
                    );
                }
            };
            if !committed_source_is_current(
                source_provisioner,
                source,
                recovery.source.candidate(),
                source_commit,
                recovery.source.input(),
                cancellation.clone(),
            )
            .await?
            {
                return Ok(DeliveryPersistedAbortRecoveryObservation::ReconciliationRequired);
            }
            let closing = match target_provisioner
                .observe_expected_merge_conflict(
                    target,
                    recovery.expected.merge_base(),
                    source_commit.commit(),
                    recovery.expected.tree(),
                    cancellation,
                )
                .await
            {
                Ok(Some(closing)) if closing == observed => closing,
                Ok(Some(_)) | Ok(None) => {
                    return Ok(DeliveryPersistedAbortRecoveryObservation::ReconciliationRequired);
                }
                Err(error) => {
                    return target_observation_failure(
                        error,
                        DeliveryPersistedAbortRecoveryObservation::ReconciliationRequired,
                    );
                }
            };
            let Some(proof) = DeliveryAbortProof::from_persisted_recovery_observation(
                source,
                target,
                recovery.source.candidate(),
                source_commit,
                recovery.source.input(),
                &recovery.expected,
                closing,
            )?
            else {
                return Ok(DeliveryPersistedAbortRecoveryObservation::ReconciliationRequired);
            };
            Ok(DeliveryPersistedAbortRecoveryObservation::Conflict(proof))
        }
        Err(error) => target_observation_failure(
            error,
            DeliveryPersistedAbortRecoveryObservation::ReconciliationRequired,
        ),
    }
}

/// Runs the existing query-first abort path through a fully rebound persisted
/// merge capability and an application-authorized durable abort proof.
pub async fn retry_persisted_delivery_abort_pending(
    source_provisioner: &DeliverySourceProvisioner,
    target_provisioner: &DeliveryTargetProvisioner,
    recovery: &DeliveryMergeRecoveryCapability,
    capability: &DeliveryAbortCapability,
    cancellation: CancellationToken,
) -> Result<DeliveryAbortOutcome, DeliveryAbortError> {
    retry_delivery_abort_pending(
        source_provisioner,
        target_provisioner,
        recovery.source.source(),
        &recovery.target,
        recovery
            .source
            .expected()
            .expect("merge recovery is bound only with a source commit"),
        capability,
        cancellation,
    )
    .await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AbortPendingObservation {
    ExactConflict,
    OldTargetClean,
    #[cfg(test)]
    Inconsistent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AbortPendingDecision {
    RetryExactAbort,
    AbortApplied,
    #[cfg(test)]
    ReconciliationRequired,
}

pub(super) const fn abort_pending_decision_for(
    observation: AbortPendingObservation,
) -> AbortPendingDecision {
    match observation {
        AbortPendingObservation::ExactConflict => AbortPendingDecision::RetryExactAbort,
        AbortPendingObservation::OldTargetClean => AbortPendingDecision::AbortApplied,
        #[cfg(test)]
        AbortPendingObservation::Inconsistent => AbortPendingDecision::ReconciliationRequired,
    }
}

async fn abort_target_observation_is_current(
    provisioner: &DeliveryTargetProvisioner,
    target: &DeliveryTargetCapability,
    proof: &DeliveryAbortProof,
    observation: AbortPendingObservation,
    cancellation: CancellationToken,
) -> Result<bool, DeliveryAbortError> {
    match observation {
        AbortPendingObservation::OldTargetClean => {
            match provisioner
                .revalidate_delivery_target(target, cancellation)
                .await
            {
                Ok(()) => Ok(true),
                Err(error) => target_observation_failure(error, false),
            }
        }
        AbortPendingObservation::ExactConflict => {
            match provisioner
                .observe_expected_merge_conflict(
                    target,
                    proof.expected_merge_base(),
                    proof.expected_source_parent(),
                    proof.expected_tree(),
                    cancellation,
                )
                .await
            {
                Ok(Some(observed)) => Ok(proof.matches_observation(&observed)),
                Ok(None) => Ok(false),
                Err(error) => target_observation_failure(error, false),
            }
        }
        #[cfg(test)]
        AbortPendingObservation::Inconsistent => Ok(false),
    }
}
