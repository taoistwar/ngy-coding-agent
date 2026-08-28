use std::sync::Arc;

use coding_agent_runtime::{
    DeliveryAbortOutcome, DeliveryAbortPendingAuthorizer, DeliveryAbortPersistenceBinding,
    DeliveryAbortProof, DeliveryAbortProofCapture, DeliveryConflictPathEncoding,
    DeliveryGitObjectFormat, DeliveryMergeError, DeliveryMergeInput, DeliveryMergeOutcome,
    DeliveryMergePendingDisposition, DeliveryPersistedAbortRecoveryObservation,
    DeliveryPersistedMergeRecovery, DeliveryPersistedSourceRecovery, DeliveryPersistedSourceState,
    DeliveryPersistedTargetRecovery, DeliveryPreflightError, DeliverySourceError,
    DeliverySourceRecoveryBindingOutcome, DeliverySourceRecoveryDisposition, DeliveryTargetError,
    DeliveryTargetRecoveryBindingOutcome, authorize_persisted_delivery_abort,
    bind_persisted_delivery_merge_recovery, build_expected_persisted_delivery_merge,
    capture_persisted_delivery_abort_proof, capture_persisted_delivery_abort_recovery,
    classify_persisted_delivery_merge_pending, preflight_prepared_delivery_merge,
    project_persisted_delivery_source_applied, project_persisted_delivery_source_object,
    retry_persisted_delivery_abort_pending, retry_persisted_delivery_merge_pending,
};
use coding_agent_store::{
    AcceptMergeCommandRequest, DeliveryCommitMetadata, DeliveryEligibilitySnapshot,
    DeliveryOperationSnapshot, DeliverySourceRecord, DeliverySourceRetryReason,
    DeliverySourceState, GitObjectAlgorithm, MergeConflictPathEncoding, MergeKnownNotAppliedReason,
    MergeOperationRecord, MergeOperationState, MergeReconciliationReason, PreflightRejectedReason,
    PreflightStaleReason,
};
use tokio_util::sync::CancellationToken;

use super::{
    ProductionDeliveryRegistry, ProductionDeliverySession, approved_fingerprint,
    target_request_from_operation,
};
use crate::delivery_manager::{DeliveryLiveRuntimeRegistrySeal, DeliveryLiveRuntimeSessionSeal};
use crate::{
    DeliveryAcceptAuthenticationError, DeliveryLiveAbortDisposition, DeliveryLiveAbortProof,
    DeliveryLiveExpectedMergeProof, DeliveryLiveMergeAppliedProof, DeliveryLiveMergeDisposition,
    DeliveryLiveRuntimeError, DeliveryLiveRuntimeRegistry, DeliveryLiveRuntimeSession,
    DeliveryLiveSourceAppliedProof, DeliveryLiveSourceObjectProof, DeliveryLiveSourceResult,
    DeliveryRuntimeAuthentication,
};

impl DeliveryLiveRuntimeRegistrySeal for ProductionDeliveryRegistry {}
impl DeliveryLiveRuntimeSessionSeal for ProductionDeliverySession {}

#[async_trait::async_trait]
impl DeliveryLiveRuntimeRegistry for ProductionDeliveryRegistry {
    async fn open_live_session(
        &self,
        snapshot: &DeliveryEligibilitySnapshot,
    ) -> Result<Arc<dyn DeliveryLiveRuntimeSession>, DeliveryLiveRuntimeError> {
        self.open(snapshot)
            .await
            .map(|session| Arc::new(session) as Arc<dyn DeliveryLiveRuntimeSession>)
            .map_err(|_| inconsistent())
    }
}

#[async_trait::async_trait]
impl DeliveryLiveRuntimeSession for ProductionDeliverySession {
    async fn authenticate_accept(
        &self,
        command: &AcceptMergeCommandRequest,
    ) -> Result<DeliveryRuntimeAuthentication, DeliveryAcceptAuthenticationError> {
        let operation = self
            .snapshot
            .ownership
            .merge_operations
            .iter()
            .find(|operation| operation.operation_id == command.preflight_operation_id())
            .ok_or_else(accept_inconsistent)?;
        if operation.state != MergeOperationState::PreflightReady
            || operation.version != command.expected_operation_version()
            || operation.provenance.identity.task_id() != command.task_id()
            || self.snapshot.ownership.source.is_some()
        {
            return Err(accept_inconsistent());
        }
        if operation.target_branch != *command.target_branch() {
            return Err(DeliveryAcceptAuthenticationError::Stale(
                PreflightStaleReason::TargetBranchChanged,
            ));
        }
        if operation.expected_target_head != *command.expected_target_head() {
            return Err(DeliveryAcceptAuthenticationError::Stale(
                PreflightStaleReason::TargetHeadChanged,
            ));
        }
        let preflight_inputs = operation
            .preflight_inputs
            .as_ref()
            .ok_or_else(accept_inconsistent)?;
        let expected_merge_base = operation
            .merge_base
            .as_ref()
            .ok_or_else(accept_inconsistent)?;
        let expected_merge_tree = operation
            .candidate_merge_tree
            .as_ref()
            .ok_or_else(accept_inconsistent)?;
        let request = coding_agent_runtime::DeliveryTargetRequest::try_new(
            command
                .target_branch()
                .as_str()
                .strip_prefix("refs/heads/")
                .ok_or_else(accept_inconsistent)?,
            command.expected_target_head().as_str(),
        )
        .map_err(map_accept_target_error)?;
        let source = self
            .source
            .open_delivery_source(
                &self.reservation,
                approved_fingerprint(&self.snapshot).map_err(|_| accept_inconsistent())?,
                CancellationToken::new(),
            )
            .await
            .map_err(map_accept_source_error)?;
        let target = self
            .target
            .open_delivery_target(&request, CancellationToken::new())
            .await
            .map_err(map_accept_target_error)?;
        let prepared = self
            .source
            .prepare_delivery_preflight_source(&source, CancellationToken::new())
            .await
            .map_err(map_accept_source_error)?;
        if prepared.candidate_tree_id() != preflight_inputs.candidate_tree.as_str()
            || prepared.source_commit_id() != preflight_inputs.preflight_source_commit.as_str()
        {
            return Err(DeliveryAcceptAuthenticationError::Stale(
                PreflightStaleReason::SourceChanged,
            ));
        }
        let fresh = preflight_prepared_delivery_merge(
            self.source.as_ref(),
            self.target.as_ref(),
            &target,
            &source,
            &prepared,
            CancellationToken::new(),
        )
        .await
        .map_err(map_accept_preflight_error)?;
        if !fresh.is_ready() {
            return Err(DeliveryAcceptAuthenticationError::MergeConflict);
        }
        if fresh.source_commit_id() != preflight_inputs.preflight_source_commit.as_str()
            || fresh.merge_base_id() != expected_merge_base.as_str()
            || fresh.candidate_merge_tree_id() != expected_merge_tree.as_str()
        {
            return Err(DeliveryAcceptAuthenticationError::Stale(
                PreflightStaleReason::SourceChanged,
            ));
        }
        let binding = source
            .persistence_binding_for_target(&target)
            .map_err(map_accept_source_error)?;
        DeliveryRuntimeAuthentication::from_persistence_binding(self.coordination_key, &binding)
            .map_err(|_| accept_inconsistent())
    }

    async fn build_source_object(
        &self,
        source: &DeliverySourceRecord,
    ) -> Result<DeliveryLiveSourceObjectProof, DeliveryLiveRuntimeError> {
        let recovery = self.bind_source(source).await?;
        let binding = project_persisted_delivery_source_object(
            self.source.as_ref(),
            &recovery,
            CancellationToken::new(),
        )
        .await
        .map_err(map_source_error)?
        .ok_or_else(inconsistent)?;
        Ok(DeliveryLiveSourceObjectProof::from_runtime(binding))
    }

    async fn apply_source_commit(
        &self,
        source: &DeliverySourceRecord,
    ) -> Result<DeliveryLiveSourceResult, DeliveryLiveRuntimeError> {
        let recovery = self.bind_source(source).await?;
        match self
            .source
            .apply_source_commit(&recovery, CancellationToken::new())
            .await
        {
            Ok(DeliverySourceRecoveryDisposition::Applied) => {
                let binding = project_persisted_delivery_source_applied(
                    self.source.as_ref(),
                    &recovery,
                    CancellationToken::new(),
                )
                .await
                .map_err(map_source_error)?
                .ok_or_else(inconsistent)?;
                Ok(DeliveryLiveSourceResult::applied(
                    DeliveryLiveSourceAppliedProof::from_runtime(binding),
                ))
            }
            Ok(DeliverySourceRecoveryDisposition::ReconciliationRequired)
            | Ok(DeliverySourceRecoveryDisposition::ReplayObject)
            | Ok(DeliverySourceRecoveryDisposition::Continue)
            | Ok(DeliverySourceRecoveryDisposition::StageComplete) => {
                Ok(DeliveryLiveSourceResult::reconciliation_required(
                    MergeReconciliationReason::SourceInconsistent,
                ))
            }
            Err(DeliverySourceError::TimedOut) => Ok(DeliveryLiveSourceResult::known_not_applied(
                DeliverySourceRetryReason::CommandTimedOut,
            )),
            Err(error) => Err(map_source_error(error)),
        }
    }

    async fn build_expected_merge(
        &self,
        operation: &MergeOperationRecord,
        source: &DeliverySourceRecord,
    ) -> Result<DeliveryLiveExpectedMergeProof, DeliveryLiveRuntimeError> {
        let recovery = self.bind_source(source).await?;
        let target_request =
            target_request_from_operation(operation).map_err(|_| inconsistent())?;
        let target = self
            .target
            .open_delivery_target(&target_request, CancellationToken::new())
            .await
            .map_err(map_target_error)?;
        let preflight = operation
            .preflight_inputs
            .as_ref()
            .ok_or_else(inconsistent)?;
        let merge_base = operation.merge_base.as_ref().ok_or_else(inconsistent)?;
        let candidate_merge_tree = operation
            .candidate_merge_tree
            .as_ref()
            .ok_or_else(inconsistent)?;
        let input = merge_input(operation)?;
        let binding = build_expected_persisted_delivery_merge(
            self.source.as_ref(),
            self.target.as_ref(),
            &recovery,
            &target,
            merge_base.as_str(),
            candidate_merge_tree.as_str(),
            &input,
            CancellationToken::new(),
        )
        .await
        .map_err(map_merge_error)?
        .filter(|binding| {
            binding.tree() == candidate_merge_tree.as_str()
                && binding.source_parent()
                    == source
                        .expected_source_commit
                        .as_ref()
                        .map_or("", |oid| oid.as_str())
                && preflight.candidate_tree == source.candidate_tree
        })
        .ok_or_else(inconsistent)?;
        Ok(DeliveryLiveExpectedMergeProof::from_runtime(binding))
    }

    async fn drive_merge_pending(
        &self,
        operation: &MergeOperationRecord,
        source: &DeliverySourceRecord,
    ) -> Result<DeliveryLiveMergeDisposition, DeliveryLiveRuntimeError> {
        let recovery = self.bind_merge(operation, source).await?;
        let initial = classify_persisted_delivery_merge_pending(
            self.source.as_ref(),
            self.target.as_ref(),
            &recovery,
            CancellationToken::new(),
        )
        .await
        .map_err(map_abort_error)?;
        match initial {
            DeliveryMergePendingDisposition::MergeApplied(proof) => {
                Ok(DeliveryLiveMergeDisposition::Applied(Box::new(
                    DeliveryLiveMergeAppliedProof::from_runtime(
                        proof.persistence_binding().clone(),
                    ),
                )))
            }
            DeliveryMergePendingDisposition::ReconciliationRequired => Ok(reconciliation_merge()),
            DeliveryMergePendingDisposition::RetryExactMerge => {
                match retry_persisted_delivery_merge_pending(
                    self.source.as_ref(),
                    self.target.as_ref(),
                    &recovery,
                    CancellationToken::new(),
                )
                .await
                .map_err(map_merge_error)?
                {
                    DeliveryMergeOutcome::Applied => {
                        match classify_persisted_delivery_merge_pending(
                            self.source.as_ref(),
                            self.target.as_ref(),
                            &recovery,
                            CancellationToken::new(),
                        )
                        .await
                        .map_err(map_abort_error)?
                        {
                            DeliveryMergePendingDisposition::MergeApplied(proof) => {
                                Ok(DeliveryLiveMergeDisposition::Applied(Box::new(
                                    DeliveryLiveMergeAppliedProof::from_runtime(
                                        proof.persistence_binding().clone(),
                                    ),
                                )))
                            }
                            _ => Ok(reconciliation_merge()),
                        }
                    }
                    DeliveryMergeOutcome::KnownNotApplied => {
                        Ok(DeliveryLiveMergeDisposition::KnownNotApplied(
                            MergeKnownNotAppliedReason::CommandTimedOut,
                        ))
                    }
                    DeliveryMergeOutcome::ConflictObserved(conflict) => {
                        match capture_persisted_delivery_abort_proof(
                            self.source.as_ref(),
                            self.target.as_ref(),
                            &recovery,
                            conflict,
                            CancellationToken::new(),
                        )
                        .await
                        .map_err(map_abort_error)?
                        {
                            DeliveryAbortProofCapture::Proven(proof) => {
                                let binding = proof
                                    .persistence_binding(abort_child_receipt_id(
                                        operation.operation_id,
                                    ))
                                    .ok_or_else(inconsistent)?;
                                Ok(DeliveryLiveMergeDisposition::Conflict(Box::new(
                                    DeliveryLiveAbortProof::from_runtime(binding),
                                )))
                            }
                            DeliveryAbortProofCapture::ReconciliationRequired => {
                                Ok(reconciliation_merge())
                            }
                        }
                    }
                    DeliveryMergeOutcome::ReconciliationRequired => Ok(reconciliation_merge()),
                }
            }
        }
    }

    async fn drive_abort_pending(
        &self,
        operation: &MergeOperationRecord,
        source: &DeliverySourceRecord,
    ) -> Result<DeliveryLiveAbortDisposition, DeliveryLiveRuntimeError> {
        if operation.state != MergeOperationState::AbortPending {
            return Err(inconsistent());
        }
        let recovery = self.bind_merge(operation, source).await?;
        match capture_persisted_delivery_abort_recovery(
            self.source.as_ref(),
            self.target.as_ref(),
            &recovery,
            CancellationToken::new(),
        )
        .await
        .map_err(map_abort_error)?
        {
            DeliveryPersistedAbortRecoveryObservation::Applied(binding) => {
                Ok(DeliveryLiveAbortDisposition::Applied(
                    crate::DeliveryLiveAbortAppliedProof::from_runtime(binding),
                ))
            }
            DeliveryPersistedAbortRecoveryObservation::ReconciliationRequired => {
                Ok(DeliveryLiveAbortDisposition::ReconciliationRequired(
                    MergeReconciliationReason::DeliveryStateInconsistent,
                ))
            }
            DeliveryPersistedAbortRecoveryObservation::Conflict(proof) => {
                let child_receipt = operation.abort_child_receipt_id.ok_or_else(inconsistent)?;
                let binding = proof
                    .persistence_binding(*child_receipt.as_bytes())
                    .ok_or_else(inconsistent)?;
                if !abort_binding_matches_operation(&binding, operation) {
                    return Ok(DeliveryLiveAbortDisposition::ReconciliationRequired(
                        MergeReconciliationReason::DeliveryStateInconsistent,
                    ));
                }
                let authorizer = ExactAbortPendingAuthorizer {
                    store: self.store.clone(),
                    operation: operation.clone(),
                    binding,
                };
                let capability = authorize_persisted_delivery_abort(proof, &authorizer).await?;
                match retry_persisted_delivery_abort_pending(
                    self.source.as_ref(),
                    self.target.as_ref(),
                    &recovery,
                    &capability,
                    CancellationToken::new(),
                )
                .await
                .map_err(map_abort_error)?
                {
                    DeliveryAbortOutcome::Applied(proof) => {
                        Ok(DeliveryLiveAbortDisposition::Applied(
                            crate::DeliveryLiveAbortAppliedProof::from_runtime(
                                proof.persistence_binding().clone(),
                            ),
                        ))
                    }
                    DeliveryAbortOutcome::KnownNotApplied => {
                        Ok(DeliveryLiveAbortDisposition::Pending)
                    }
                    DeliveryAbortOutcome::ReconciliationRequired => {
                        Ok(DeliveryLiveAbortDisposition::ReconciliationRequired(
                            MergeReconciliationReason::DeliveryStateInconsistent,
                        ))
                    }
                }
            }
        }
    }
}

struct ExactAbortPendingAuthorizer {
    store: coding_agent_store::Store,
    operation: MergeOperationRecord,
    binding: DeliveryAbortPersistenceBinding,
}

#[async_trait::async_trait]
impl DeliveryAbortPendingAuthorizer for ExactAbortPendingAuthorizer {
    type Error = DeliveryLiveRuntimeError;

    async fn authorize_persisted_abort_pending(
        &self,
        proof: &DeliveryAbortProof,
    ) -> Result<(), Self::Error> {
        let child_receipt = self
            .operation
            .abort_child_receipt_id
            .ok_or_else(inconsistent)?;
        if proof
            .persistence_binding(*child_receipt.as_bytes())
            .as_ref()
            != Some(&self.binding)
        {
            return Err(inconsistent());
        }
        match self
            .store
            .delivery_operation_snapshot(self.operation.operation_id)
            .await
            .map_err(|_| DeliveryLiveRuntimeError::Unavailable)?
        {
            Some(DeliveryOperationSnapshot::Merge(current))
                if *current == self.operation
                    && current.state == MergeOperationState::AbortPending =>
            {
                Ok(())
            }
            Some(_) | None => Err(inconsistent()),
        }
    }
}

fn abort_binding_matches_operation(
    binding: &DeliveryAbortPersistenceBinding,
    operation: &MergeOperationRecord,
) -> bool {
    let common_identity = operation.provenance.common_git_identity.storage_parts();
    let admin_identity = operation.provenance.worktree_admin_identity.storage_parts();
    operation.state == MergeOperationState::AbortPending
        && operation
            .abort_child_receipt_id
            .is_some_and(|receipt| *receipt.as_bytes() == binding.child_receipt_id())
        && operation.target_branch.as_str() == binding.target_branch()
        && operation.expected_target_head.as_str() == binding.target_head()
        && operation.provenance.source_branch.as_str() == binding.source_branch()
        && operation
            .source_commit
            .as_ref()
            .is_some_and(|source| source.as_str() == binding.source_oid())
        && operation
            .abort_merge_head
            .as_ref()
            .is_some_and(|head| head.as_str() == binding.merge_head())
        && common_identity
            == (
                binding.common_git_identity_algorithm(),
                binding.common_git_identity_digest(),
            )
        && admin_identity
            == (
                binding.worktree_admin_identity_algorithm(),
                binding.worktree_admin_identity_digest(),
            )
        && operation.provenance.fixed_lock_reason == binding.fixed_lock_reason()
        && operation.provenance.config_attributes_digest.as_str()
            == binding.source_config_attributes_digest()
        && operation
            .abort_index_stages_digest
            .as_ref()
            .is_some_and(|digest| digest.as_str() == binding.index_stages_digest())
        && operation
            .abort_worktree_digest
            .as_ref()
            .is_some_and(|digest| digest.as_str() == binding.worktree_digest())
        && operation.abort_merge_autostash_proof.as_deref() == Some("absent")
        && binding.merge_autostash_is_absent()
        && binding.other_git_operation_is_clear()
        && operation.conflict_path_count == u8::try_from(binding.conflict_paths().len()).ok()
        && operation.conflicts.len() == binding.conflict_paths().len()
        && operation
            .conflicts
            .iter()
            .zip(binding.conflict_paths())
            .enumerate()
            .all(|(ordinal, (persisted, observed))| {
                usize::from(persisted.ordinal) == ordinal
                    && matches!(
                        (persisted.path_encoding, observed.encoding()),
                        (
                            MergeConflictPathEncoding::Utf8,
                            DeliveryConflictPathEncoding::Utf8
                        ) | (
                            MergeConflictPathEncoding::Base64Url,
                            DeliveryConflictPathEncoding::Base64Url
                        )
                    )
                    && persisted.path_value == observed.value().as_bytes()
            })
}

impl ProductionDeliverySession {
    async fn bind_source(
        &self,
        source: &DeliverySourceRecord,
    ) -> Result<Box<coding_agent_runtime::DeliverySourceRecoveryCapability>, DeliveryLiveRuntimeError>
    {
        let persisted = persisted_source(self, source)?;
        match self
            .source
            .bind_persisted_delivery_source_recovery(
                &self.reservation,
                &persisted,
                CancellationToken::new(),
            )
            .await
            .map_err(map_source_error)?
        {
            DeliverySourceRecoveryBindingOutcome::Bound(recovery) => Ok(recovery),
            DeliverySourceRecoveryBindingOutcome::ReconciliationRequired => Err(inconsistent()),
        }
    }

    async fn bind_merge(
        &self,
        operation: &MergeOperationRecord,
        source: &DeliverySourceRecord,
    ) -> Result<Box<coding_agent_runtime::DeliveryMergeRecoveryCapability>, DeliveryLiveRuntimeError>
    {
        let source_recovery = self.bind_source(source).await?;
        let target = persisted_target(operation)?;
        let target_recovery = match self
            .target
            .bind_persisted_delivery_target_recovery(&target, CancellationToken::new())
            .await
            .map_err(map_target_error)?
        {
            DeliveryTargetRecoveryBindingOutcome::Bound(target) => target,
            DeliveryTargetRecoveryBindingOutcome::ReconciliationRequired => {
                return Err(inconsistent());
            }
        };
        let merge = persisted_merge(operation)?;
        match bind_persisted_delivery_merge_recovery(
            self.source.as_ref(),
            self.target.as_ref(),
            *source_recovery,
            *target_recovery,
            &merge,
            CancellationToken::new(),
        )
        .await
        .map_err(map_abort_error)?
        {
            coding_agent_runtime::DeliveryMergeRecoveryBindingOutcome::Bound(recovery) => {
                Ok(recovery)
            }
            coding_agent_runtime::DeliveryMergeRecoveryBindingOutcome::ReconciliationRequired => {
                Err(inconsistent())
            }
        }
    }
}

pub(super) fn persisted_source(
    session: &ProductionDeliverySession,
    source: &DeliverySourceRecord,
) -> Result<DeliveryPersistedSourceRecovery, DeliveryLiveRuntimeError> {
    let state = match source.state {
        DeliverySourceState::ObjectPending => DeliveryPersistedSourceState::ObjectPending,
        DeliverySourceState::CommitPending => DeliveryPersistedSourceState::CommitPending,
        DeliverySourceState::Committed => DeliveryPersistedSourceState::Committed,
        DeliverySourceState::ReconciliationRequired => return Err(inconsistent()),
    };
    let (common_identity_algorithm, common_identity_digest) =
        source.provenance.common_git_identity.storage_parts();
    let (admin_identity_algorithm, admin_identity_digest) =
        source.provenance.worktree_admin_identity.storage_parts();
    DeliveryPersistedSourceRecovery::try_new(
        object_format(source.provenance.base_commit.algorithm()),
        state,
        session.reservation.identity().clone(),
        source.provenance.source_branch.as_str(),
        source.provenance.base_commit.as_str(),
        approved_fingerprint(&session.snapshot).map_err(|_| inconsistent())?,
        source.candidate_tree.as_str(),
        source
            .expected_source_commit
            .as_ref()
            .map(|oid| oid.as_str()),
        source_input(source)?,
        common_identity_algorithm,
        common_identity_digest,
        admin_identity_algorithm,
        admin_identity_digest,
        source.provenance.config_attributes_digest.as_str(),
    )
    .map_err(|_| inconsistent())
}

fn persisted_target(
    operation: &MergeOperationRecord,
) -> Result<DeliveryPersistedTargetRecovery, DeliveryLiveRuntimeError> {
    persisted_target_for(
        operation,
        &operation.target_branch,
        &operation.expected_target_head,
    )
}

pub(super) fn persisted_target_for(
    operation: &MergeOperationRecord,
    target_branch: &coding_agent_store::GitBranchRef,
    target_head: &coding_agent_store::GitCommitOid,
) -> Result<DeliveryPersistedTargetRecovery, DeliveryLiveRuntimeError> {
    let (common_identity_algorithm, common_identity_digest) =
        operation.provenance.common_git_identity.storage_parts();
    DeliveryPersistedTargetRecovery::try_new(
        object_format(target_head.algorithm()),
        target_branch.as_str(),
        target_head.as_str(),
        common_identity_algorithm,
        common_identity_digest,
        operation.target_config_attributes_digest.as_str(),
        operation.target_security_digest.as_str(),
    )
    .map_err(|_| inconsistent())
}

fn persisted_merge(
    operation: &MergeOperationRecord,
) -> Result<DeliveryPersistedMergeRecovery, DeliveryLiveRuntimeError> {
    DeliveryPersistedMergeRecovery::try_new(
        object_format(operation.expected_target_head.algorithm()),
        operation
            .merge_base
            .as_ref()
            .ok_or_else(inconsistent)?
            .as_str(),
        operation
            .candidate_merge_tree
            .as_ref()
            .ok_or_else(inconsistent)?
            .as_str(),
        operation
            .expected_merge_commit
            .as_ref()
            .ok_or_else(inconsistent)?
            .as_str(),
        merge_input(operation)?,
    )
    .map_err(|_| inconsistent())
}

pub(super) fn source_input(
    source: &DeliverySourceRecord,
) -> Result<coding_agent_runtime::DeliverySourceCommitInput, DeliveryLiveRuntimeError> {
    coding_agent_runtime::DeliverySourceCommitInput::try_new(
        &source.provenance.identity.task_id().to_string(),
        u64::from(source.provenance.identity.attempt()),
        metadata_epoch(&source.commit_metadata)?,
    )
    .map_err(map_source_error)
}

fn merge_input(
    operation: &MergeOperationRecord,
) -> Result<DeliveryMergeInput, DeliveryLiveRuntimeError> {
    let metadata = operation.merge_metadata.as_ref().ok_or_else(inconsistent)?;
    DeliveryMergeInput::try_new(
        &operation.provenance.identity.task_id().to_string(),
        u64::from(operation.provenance.identity.attempt()),
        metadata_epoch(metadata)?,
    )
    .map_err(map_merge_error)
}

fn metadata_epoch(metadata: &DeliveryCommitMetadata) -> Result<i64, DeliveryLiveRuntimeError> {
    if metadata.author_date_bytes != metadata.committer_date_bytes {
        return Err(inconsistent());
    }
    let (epoch, zone) = metadata
        .author_date_bytes
        .split_once(' ')
        .ok_or_else(inconsistent)?;
    if zone != "+0000" {
        return Err(inconsistent());
    }
    epoch.parse().map_err(|_| inconsistent())
}

fn abort_child_receipt_id(operation_id: coding_agent_store::DeliveryOperationId) -> [u8; 16] {
    let mut receipt = *operation_id.as_uuid().as_bytes();
    receipt[0] ^= 0xa7;
    receipt[6] = (receipt[6] & 0x0f) | 0x50;
    receipt[8] = (receipt[8] & 0x3f) | 0x80;
    if receipt == [0; 16] {
        receipt[15] = 1;
    }
    receipt
}

pub(super) const fn object_format(algorithm: GitObjectAlgorithm) -> DeliveryGitObjectFormat {
    match algorithm {
        GitObjectAlgorithm::Sha1 => DeliveryGitObjectFormat::Sha1,
        GitObjectAlgorithm::Sha256 => DeliveryGitObjectFormat::Sha256,
    }
}

const fn reconciliation_merge() -> DeliveryLiveMergeDisposition {
    DeliveryLiveMergeDisposition::ReconciliationRequired(
        MergeReconciliationReason::DeliveryStateInconsistent,
    )
}

const fn inconsistent() -> DeliveryLiveRuntimeError {
    DeliveryLiveRuntimeError::ReconciliationRequired(
        MergeReconciliationReason::DeliveryStateInconsistent,
    )
}

fn map_source_error(error: DeliverySourceError) -> DeliveryLiveRuntimeError {
    match error {
        DeliverySourceError::ProcessCleanupUnproven
        | DeliverySourceError::SandboxCleanupUnproven => {
            DeliveryLiveRuntimeError::ProcessCleanupUnproven
        }
        DeliverySourceError::AuthenticationChanged | DeliverySourceError::UnsafeIndex => {
            DeliveryLiveRuntimeError::ReconciliationRequired(
                MergeReconciliationReason::WorktreeIdentityMismatch,
            )
        }
        DeliverySourceError::SourceChanged | DeliverySourceError::ChildOutcomeUnknown => {
            DeliveryLiveRuntimeError::ReconciliationRequired(
                MergeReconciliationReason::SourceInconsistent,
            )
        }
        DeliverySourceError::UnsafeGitConfiguration => {
            DeliveryLiveRuntimeError::ReconciliationRequired(
                MergeReconciliationReason::UnsafeGitConfiguration,
            )
        }
        _ => DeliveryLiveRuntimeError::Unavailable,
    }
}

fn map_target_error(error: DeliveryTargetError) -> DeliveryLiveRuntimeError {
    match error {
        DeliveryTargetError::ProcessCleanupUnproven => {
            DeliveryLiveRuntimeError::ProcessCleanupUnproven
        }
        DeliveryTargetError::UnsafeGitConfiguration => {
            DeliveryLiveRuntimeError::ReconciliationRequired(
                MergeReconciliationReason::UnsafeGitConfiguration,
            )
        }
        DeliveryTargetError::UnsupportedGitAttributes => {
            DeliveryLiveRuntimeError::ReconciliationRequired(
                MergeReconciliationReason::UnsupportedGitAttributes,
            )
        }
        DeliveryTargetError::AuthenticationChanged | DeliveryTargetError::ChildOutcomeUnknown => {
            inconsistent()
        }
        _ => DeliveryLiveRuntimeError::Unavailable,
    }
}

fn map_merge_error(error: DeliveryMergeError) -> DeliveryLiveRuntimeError {
    match error {
        DeliveryMergeError::Source(error) => map_source_error(error),
        DeliveryMergeError::Target(error) => map_target_error(error),
        DeliveryMergeError::Preflight(coding_agent_runtime::DeliveryPreflightError::Source(
            error,
        )) => map_source_error(error),
        DeliveryMergeError::Preflight(coding_agent_runtime::DeliveryPreflightError::Target(
            error,
        )) => map_target_error(error),
        DeliveryMergeError::ExpectedObjectInvalid | DeliveryMergeError::PreflightStale => {
            inconsistent()
        }
        _ => DeliveryLiveRuntimeError::Unavailable,
    }
}

fn map_abort_error(error: coding_agent_runtime::DeliveryAbortError) -> DeliveryLiveRuntimeError {
    match error {
        coding_agent_runtime::DeliveryAbortError::Source(error) => map_source_error(error),
        coding_agent_runtime::DeliveryAbortError::Target(error) => map_target_error(error),
        coding_agent_runtime::DeliveryAbortError::InvalidProof => inconsistent(),
    }
}

fn map_accept_preflight_error(error: DeliveryPreflightError) -> DeliveryAcceptAuthenticationError {
    match error {
        DeliveryPreflightError::Source(error) => map_accept_source_error(error),
        DeliveryPreflightError::Target(error) => map_accept_target_error(error),
        DeliveryPreflightError::SourceAlreadyInTarget => {
            DeliveryAcceptAuthenticationError::Rejected(
                PreflightRejectedReason::SourceAlreadyInTarget,
            )
        }
        DeliveryPreflightError::MalformedMergeTreeOutput | DeliveryPreflightError::Internal => {
            accept_inconsistent()
        }
    }
}

fn map_accept_source_error(error: DeliverySourceError) -> DeliveryAcceptAuthenticationError {
    match error {
        DeliverySourceError::SourceChanged => {
            DeliveryAcceptAuthenticationError::Stale(PreflightStaleReason::SourceChanged)
        }
        DeliverySourceError::UnsafeGitConfiguration => DeliveryAcceptAuthenticationError::Rejected(
            PreflightRejectedReason::UnsafeGitConfiguration,
        ),
        DeliverySourceError::TimedOut => DeliveryAcceptAuthenticationError::CommandTimedOut,
        DeliverySourceError::ProcessCleanupUnproven
        | DeliverySourceError::SandboxCleanupUnproven => {
            DeliveryAcceptAuthenticationError::ProcessCleanupUnproven
        }
        DeliverySourceError::AuthenticationChanged | DeliverySourceError::UnsafeIndex => {
            DeliveryAcceptAuthenticationError::ReconciliationRequired(
                MergeReconciliationReason::WorktreeIdentityMismatch,
            )
        }
        DeliverySourceError::ChildOutcomeUnknown => accept_inconsistent(),
        DeliverySourceError::Cancelled
        | DeliverySourceError::CommandFailed
        | DeliverySourceError::InvalidLimits
        | DeliverySourceError::InvalidEnvironment
        | DeliverySourceError::CommandPolicy
        | DeliverySourceError::BoundsExceeded
        | DeliverySourceError::SandboxUnavailable
        | DeliverySourceError::Internal => DeliveryAcceptAuthenticationError::Unavailable,
    }
}

fn map_accept_target_error(error: DeliveryTargetError) -> DeliveryAcceptAuthenticationError {
    match error {
        DeliveryTargetError::TargetDetached => DeliveryAcceptAuthenticationError::Rejected(
            PreflightRejectedReason::TargetBranchDetached,
        ),
        DeliveryTargetError::TargetBranchMismatch => {
            DeliveryAcceptAuthenticationError::Stale(PreflightStaleReason::TargetBranchChanged)
        }
        DeliveryTargetError::TargetHeadChanged => {
            DeliveryAcceptAuthenticationError::Stale(PreflightStaleReason::TargetHeadChanged)
        }
        DeliveryTargetError::TargetWorktreeDirty => DeliveryAcceptAuthenticationError::Rejected(
            PreflightRejectedReason::TargetWorktreeDirty,
        ),
        DeliveryTargetError::TargetIgnoredPathCollision => {
            DeliveryAcceptAuthenticationError::Rejected(
                PreflightRejectedReason::TargetIgnoredPathCollision,
            )
        }
        DeliveryTargetError::TargetGitOperationInProgress => {
            DeliveryAcceptAuthenticationError::Rejected(
                PreflightRejectedReason::TargetGitOperationInProgress,
            )
        }
        DeliveryTargetError::UnsafeGitConfiguration => DeliveryAcceptAuthenticationError::Rejected(
            PreflightRejectedReason::UnsafeGitConfiguration,
        ),
        DeliveryTargetError::UnsupportedGitAttributes => {
            DeliveryAcceptAuthenticationError::Rejected(
                PreflightRejectedReason::UnsupportedGitAttributes,
            )
        }
        DeliveryTargetError::TimedOut => DeliveryAcceptAuthenticationError::CommandTimedOut,
        DeliveryTargetError::ProcessCleanupUnproven => {
            DeliveryAcceptAuthenticationError::ProcessCleanupUnproven
        }
        DeliveryTargetError::AuthenticationChanged | DeliveryTargetError::ChildOutcomeUnknown => {
            accept_inconsistent()
        }
        DeliveryTargetError::Cancelled
        | DeliveryTargetError::InvalidLimits
        | DeliveryTargetError::InvalidRequest
        | DeliveryTargetError::BoundsExceeded
        | DeliveryTargetError::CommandFailed
        | DeliveryTargetError::Internal => DeliveryAcceptAuthenticationError::Unavailable,
    }
}

const fn accept_inconsistent() -> DeliveryAcceptAuthenticationError {
    DeliveryAcceptAuthenticationError::ReconciliationRequired(
        MergeReconciliationReason::DeliveryStateInconsistent,
    )
}
