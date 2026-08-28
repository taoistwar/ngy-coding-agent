use super::*;
/// Produces the Store source-applied proof only after a fresh exact committed
/// postcondition. Known drift returns `None` and never starts a mutation child.
pub async fn project_persisted_delivery_source_applied(
    source_provisioner: &DeliverySourceProvisioner,
    recovery: &DeliverySourceRecoveryCapability,
    cancellation: CancellationToken,
) -> Result<Option<DeliverySourceAppliedPersistenceBinding>, DeliverySourceError> {
    if recovery.pending_state() != DeliverySourcePendingState::CommitPending {
        return Ok(None);
    }
    let Some(expected) = recovery.expected() else {
        return Ok(None);
    };
    match source_provisioner
        .revalidate_preflight_committed_source(
            recovery.source(),
            recovery.candidate(),
            expected,
            recovery.input(),
            cancellation,
        )
        .await
    {
        Ok(()) => {}
        Err(error) if is_source_recovery_mismatch(error) => return Ok(None),
        Err(error) => return Err(error),
    }
    let source = recovery.source();
    let object = super::super::persistence::source_object_persistence_binding(
        source,
        recovery.candidate(),
        expected,
        recovery.input(),
    )?;
    Ok(Some(DeliverySourceAppliedPersistenceBinding::new(
        object,
        format!("refs/heads/{}", source.branch_name()),
        expected.object_id().to_owned(),
        source.common_directory_identity().as_hex().to_owned(),
        source.admin_directory_identity().as_hex().to_owned(),
        super::super::persistence::encode_lower_hex(source.config_attributes_digest()),
    )))
}

/// Replays and projects the exact deterministic object for one already
/// durable `ObjectPending` source. The returned scalar binding is minted only
/// from the freshly rebound recovery capability.
pub async fn project_persisted_delivery_source_object(
    source_provisioner: &DeliverySourceProvisioner,
    recovery: &DeliverySourceRecoveryCapability,
    cancellation: CancellationToken,
) -> Result<Option<DeliverySourceObjectPersistenceBinding>, DeliverySourceError> {
    if recovery.pending_state() != DeliverySourcePendingState::ObjectPending
        || recovery.expected().is_some()
    {
        return Ok(None);
    }
    let expected = source_provisioner
        .replay_source_commit(recovery, cancellation)
        .await?;
    let source = recovery.source();
    super::super::persistence::source_object_persistence_binding(
        source,
        recovery.candidate(),
        &expected,
        recovery.input(),
    )
    .map(Some)
}

/// Builds the expected two-parent merge object from a freshly rebound
/// committed source and target plus the durable preflight object identities.
/// Raw Store values remain inert until all source/target bindings are proven.
#[allow(clippy::too_many_arguments)]
pub async fn build_expected_persisted_delivery_merge(
    source_provisioner: &DeliverySourceProvisioner,
    target_provisioner: &DeliveryTargetProvisioner,
    source: &DeliverySourceRecoveryCapability,
    target: &DeliveryTargetCapability,
    merge_base: &str,
    candidate_merge_tree: &str,
    input: &DeliveryMergeInput,
    cancellation: CancellationToken,
) -> Result<Option<DeliveryExpectedMergePersistenceBinding>, DeliveryMergeError> {
    let Some(source_commit) = source.expected() else {
        return Ok(None);
    };
    let object_format = source.source().probe().object_format();
    let Some(merge_base) = DeliveryCommitOid::try_new(merge_base, object_format) else {
        return Ok(None);
    };
    let Some(candidate_merge_tree) = DeliveryTreeOid::try_new(candidate_merge_tree, object_format)
    else {
        return Ok(None);
    };
    let preflight = DeliveryPreflightResult::ready(
        source_commit.commit().clone(),
        merge_base,
        candidate_merge_tree,
    );
    let expected = build_expected_delivery_merge(
        source_provisioner,
        target_provisioner,
        source.source(),
        target,
        source.candidate(),
        source_commit,
        source.input(),
        &preflight,
        input,
        cancellation,
    )
    .await?;
    expected.persistence_binding().map(Some)
}
