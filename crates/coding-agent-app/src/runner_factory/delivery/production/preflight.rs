use std::str::FromStr;
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use coding_agent_runtime::{
    DeliveryConflictPathEncoding, DeliveryPreflightError, DeliverySourceError, DeliveryTargetError,
    DeliveryTargetRequest, PreparedDeliveryPreflightSource, preflight_prepared_delivery_merge,
};
use coding_agent_store::{
    DeliveryEligibilitySnapshot, GitBranchRef, GitCommitOid, GitTreeOid, MergeConflictPaths,
    MergePreflightResult, MergeReconciliationReason, PreflightCommandRequest,
    PreflightRejectedReason, PreflightStaleReason,
};
use tokio_util::sync::CancellationToken;

use super::{
    ProductionBindingError, ProductionDeliveryRegistry, ProductionDeliverySession,
    approved_fingerprint, local_branch_name,
};
use crate::delivery_manager::{DeliveryRuntimeRegistrySeal, DeliveryRuntimeSessionSeal};
use crate::{
    DeliveryPreparedPreflight, DeliveryRuntimeAuthentication, DeliveryRuntimeAuthenticationOutcome,
    DeliveryRuntimeFailure, DeliveryRuntimeObservation,
    DeliveryRuntimeObservationUnavailableReason, DeliveryRuntimeRegistry, DeliveryRuntimeSession,
};

impl DeliveryRuntimeRegistrySeal for ProductionDeliveryRegistry {}
impl DeliveryRuntimeSessionSeal for ProductionDeliverySession {}

#[async_trait::async_trait]
impl DeliveryRuntimeRegistry for ProductionDeliveryRegistry {
    async fn open_session(
        &self,
        snapshot: &DeliveryEligibilitySnapshot,
    ) -> Result<Arc<dyn DeliveryRuntimeSession>, DeliveryRuntimeFailure> {
        self.open(snapshot)
            .await
            .map(|session| Arc::new(session) as Arc<dyn DeliveryRuntimeSession>)
            .map_err(|_| inconsistent())
    }
}

struct PreparedRuntimeState {
    source: PreparedDeliveryPreflightSource,
    target: DeliveryTargetRequest,
}

#[async_trait::async_trait]
impl DeliveryRuntimeSession for ProductionDeliverySession {
    async fn observe(&self) -> Result<DeliveryRuntimeObservation, DeliveryRuntimeFailure> {
        match self
            .target
            .observe_registered_delivery_target(CancellationToken::new())
            .await
        {
            Ok(observation) => Ok(DeliveryRuntimeObservation::Available {
                branch: GitBranchRef::from_str(&format!(
                    "refs/heads/{}",
                    observation.branch_name()
                ))
                .map_err(|_| inconsistent())?,
                head: GitCommitOid::from_str(observation.head_id()).map_err(|_| inconsistent())?,
            }),
            Err(error) => Ok(DeliveryRuntimeObservation::Unavailable {
                reason: observation_reason(error),
            }),
        }
    }

    async fn authenticate_preflight(
        &self,
        command: &PreflightCommandRequest,
    ) -> Result<DeliveryRuntimeAuthenticationOutcome, DeliveryRuntimeFailure> {
        #[cfg(feature = "test-support")]
        if let Some(controller) = self
            .test_process_fault
            .as_ref()
            .and_then(|fault| fault.take_controller())
        {
            return controller
                .scope(self.authenticate_preflight_inner(command))
                .await;
        }
        self.authenticate_preflight_inner(command).await
    }

    async fn prepare_preflight(&self) -> Result<DeliveryPreparedPreflight, DeliveryRuntimeFailure> {
        let target = self
            .target_request
            .lock()
            .map_err(|_| inconsistent())?
            .clone()
            .ok_or_else(inconsistent)?;
        let source = self
            .source
            .open_delivery_source(
                &self.reservation,
                approved_fingerprint(&self.snapshot).map_err(|_| inconsistent())?,
                CancellationToken::new(),
            )
            .await
            .map_err(map_source_error)?;
        let prepared = self
            .source
            .prepare_delivery_preflight_source(&source, CancellationToken::new())
            .await
            .map_err(map_source_error)?;
        let candidate_tree =
            GitTreeOid::from_str(prepared.candidate_tree_id()).map_err(|_| inconsistent())?;
        let source_commit =
            GitCommitOid::from_str(prepared.source_commit_id()).map_err(|_| inconsistent())?;
        Ok(DeliveryPreparedPreflight::new(
            candidate_tree,
            source_commit,
            PreparedRuntimeState {
                source: prepared,
                target,
            },
        ))
    }

    async fn run_preflight(
        &self,
        prepared: &DeliveryPreparedPreflight,
    ) -> Result<MergePreflightResult, DeliveryRuntimeFailure> {
        let state = prepared
            .runtime_state::<PreparedRuntimeState>()
            .ok_or_else(inconsistent)?;
        let source = self
            .source
            .open_delivery_source(
                &self.reservation,
                approved_fingerprint(&self.snapshot).map_err(|_| inconsistent())?,
                CancellationToken::new(),
            )
            .await
            .map_err(map_source_error)?;
        let target = self
            .target
            .open_delivery_target(&state.target, CancellationToken::new())
            .await
            .map_err(map_target_error)?;
        let result = preflight_prepared_delivery_merge(
            self.source.as_ref(),
            self.target.as_ref(),
            &target,
            &source,
            &state.source,
            CancellationToken::new(),
        )
        .await
        .map_err(map_preflight_error)?;
        runtime_preflight_result(result)
    }
}

impl ProductionDeliverySession {
    async fn authenticate_preflight_inner(
        &self,
        command: &PreflightCommandRequest,
    ) -> Result<DeliveryRuntimeAuthenticationOutcome, DeliveryRuntimeFailure> {
        let request = DeliveryTargetRequest::try_new(
            local_branch_name(command.target_branch().as_str()).map_err(|_| inconsistent())?,
            command.expected_target_head().as_str(),
        )
        .map_err(map_target_error)?;
        let source = self
            .source
            .open_delivery_source(
                &self.reservation,
                approved_fingerprint(&self.snapshot).map_err(|_| inconsistent())?,
                CancellationToken::new(),
            )
            .await
            .map_err(map_source_error)?;
        let target = self
            .target
            .open_delivery_target(&request, CancellationToken::new())
            .await
            .map_err(map_target_error)?;
        let binding = source
            .persistence_binding_for_target(&target)
            .map_err(map_source_error)?;
        let authentication = DeliveryRuntimeAuthentication::from_persistence_binding(
            self.coordination_key,
            &binding,
        )?;
        *self.target_request.lock().map_err(|_| inconsistent())? = Some(request);
        Ok(DeliveryRuntimeAuthenticationOutcome::Ready(authentication))
    }
}

fn runtime_preflight_result(
    result: coding_agent_runtime::DeliveryPreflightResult,
) -> Result<MergePreflightResult, DeliveryRuntimeFailure> {
    let merge_base = GitCommitOid::from_str(result.merge_base_id()).map_err(|_| inconsistent())?;
    let merge_tree =
        GitTreeOid::from_str(result.candidate_merge_tree_id()).map_err(|_| inconsistent())?;
    if result.is_ready() {
        return MergePreflightResult::ready(merge_base, merge_tree).map_err(|_| inconsistent());
    }
    let raw_paths = result
        .conflict_paths()
        .ok_or_else(inconsistent)?
        .iter()
        .map(|path| match path.encoding() {
            DeliveryConflictPathEncoding::Utf8 => Ok(path.value().as_bytes().to_vec()),
            DeliveryConflictPathEncoding::Base64Url => URL_SAFE_NO_PAD
                .decode(path.value())
                .map_err(|_| inconsistent()),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let paths = MergeConflictPaths::try_from_raw(raw_paths).map_err(|_| inconsistent())?;
    MergePreflightResult::conflict(merge_base, merge_tree, paths).map_err(|_| inconsistent())
}

fn map_preflight_error(error: DeliveryPreflightError) -> DeliveryRuntimeFailure {
    match error {
        DeliveryPreflightError::Target(error) => map_target_error(error),
        DeliveryPreflightError::Source(error) => map_source_error(error),
        DeliveryPreflightError::SourceAlreadyInTarget => {
            DeliveryRuntimeFailure::Rejected(PreflightRejectedReason::SourceAlreadyInTarget)
        }
        DeliveryPreflightError::MalformedMergeTreeOutput | DeliveryPreflightError::Internal => {
            inconsistent()
        }
    }
}

fn map_source_error(error: DeliverySourceError) -> DeliveryRuntimeFailure {
    match error {
        DeliverySourceError::SourceChanged => {
            DeliveryRuntimeFailure::Stale(PreflightStaleReason::SourceChanged)
        }
        DeliverySourceError::UnsafeGitConfiguration => {
            DeliveryRuntimeFailure::Rejected(PreflightRejectedReason::UnsafeGitConfiguration)
        }
        DeliverySourceError::ProcessCleanupUnproven
        | DeliverySourceError::SandboxCleanupUnproven => {
            DeliveryRuntimeFailure::ProcessCleanupUnproven
        }
        DeliverySourceError::AuthenticationChanged | DeliverySourceError::UnsafeIndex => {
            DeliveryRuntimeFailure::ReconciliationRequired(
                MergeReconciliationReason::WorktreeIdentityMismatch,
            )
        }
        DeliverySourceError::ChildOutcomeUnknown => inconsistent(),
        _ => DeliveryRuntimeFailure::Unavailable,
    }
}

fn map_target_error(error: DeliveryTargetError) -> DeliveryRuntimeFailure {
    match error {
        DeliveryTargetError::TargetDetached => {
            DeliveryRuntimeFailure::Rejected(PreflightRejectedReason::TargetBranchDetached)
        }
        DeliveryTargetError::TargetBranchMismatch => {
            DeliveryRuntimeFailure::Rejected(PreflightRejectedReason::TargetBranchMismatch)
        }
        DeliveryTargetError::TargetHeadChanged => {
            DeliveryRuntimeFailure::Stale(PreflightStaleReason::TargetHeadChanged)
        }
        DeliveryTargetError::TargetWorktreeDirty => {
            DeliveryRuntimeFailure::Rejected(PreflightRejectedReason::TargetWorktreeDirty)
        }
        DeliveryTargetError::TargetIgnoredPathCollision => {
            DeliveryRuntimeFailure::Rejected(PreflightRejectedReason::TargetIgnoredPathCollision)
        }
        DeliveryTargetError::TargetGitOperationInProgress => {
            DeliveryRuntimeFailure::Rejected(PreflightRejectedReason::TargetGitOperationInProgress)
        }
        DeliveryTargetError::UnsafeGitConfiguration => {
            DeliveryRuntimeFailure::Rejected(PreflightRejectedReason::UnsafeGitConfiguration)
        }
        DeliveryTargetError::UnsupportedGitAttributes => {
            DeliveryRuntimeFailure::Rejected(PreflightRejectedReason::UnsupportedGitAttributes)
        }
        DeliveryTargetError::ProcessCleanupUnproven => {
            DeliveryRuntimeFailure::ProcessCleanupUnproven
        }
        DeliveryTargetError::AuthenticationChanged | DeliveryTargetError::ChildOutcomeUnknown => {
            inconsistent()
        }
        _ => DeliveryRuntimeFailure::Unavailable,
    }
}

fn observation_reason(error: DeliveryTargetError) -> DeliveryRuntimeObservationUnavailableReason {
    match error {
        DeliveryTargetError::TargetDetached => {
            DeliveryRuntimeObservationUnavailableReason::TargetBranchDetached
        }
        DeliveryTargetError::TargetBranchMismatch => {
            DeliveryRuntimeObservationUnavailableReason::TargetBranchMismatch
        }
        DeliveryTargetError::TargetHeadChanged => {
            DeliveryRuntimeObservationUnavailableReason::TargetHeadChanged
        }
        DeliveryTargetError::TargetWorktreeDirty => {
            DeliveryRuntimeObservationUnavailableReason::TargetWorktreeDirty
        }
        DeliveryTargetError::TargetIgnoredPathCollision => {
            DeliveryRuntimeObservationUnavailableReason::TargetIgnoredPathCollision
        }
        DeliveryTargetError::TargetGitOperationInProgress => {
            DeliveryRuntimeObservationUnavailableReason::TargetGitOperationInProgress
        }
        DeliveryTargetError::UnsafeGitConfiguration => {
            DeliveryRuntimeObservationUnavailableReason::UnsafeGitConfiguration
        }
        DeliveryTargetError::UnsupportedGitAttributes => {
            DeliveryRuntimeObservationUnavailableReason::UnsupportedGitAttributes
        }
        DeliveryTargetError::ProcessCleanupUnproven => {
            DeliveryRuntimeObservationUnavailableReason::ProcessCleanupUnproven
        }
        DeliveryTargetError::AuthenticationChanged | DeliveryTargetError::ChildOutcomeUnknown => {
            DeliveryRuntimeObservationUnavailableReason::ReconciliationRequired
        }
        _ => DeliveryRuntimeObservationUnavailableReason::RuntimeUnavailable,
    }
}

const fn inconsistent() -> DeliveryRuntimeFailure {
    DeliveryRuntimeFailure::ReconciliationRequired(
        MergeReconciliationReason::DeliveryStateInconsistent,
    )
}

impl From<ProductionBindingError> for DeliveryRuntimeFailure {
    fn from(_: ProductionBindingError) -> Self {
        inconsistent()
    }
}
