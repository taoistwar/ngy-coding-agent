use std::str::FromStr;
use std::sync::Arc;

use coding_agent_runtime::{
    DeliveryBranchCleanupIntent, DeliveryBranchCleanupRefreshProof,
    DeliveryDeletePendingAuthorizer, DeliveryDeletePendingCapability,
    DeliveryDeletePendingDisposition, DeliveryRemovePendingAuthorizer,
    DeliveryRemovePendingCapability, DeliveryRemovePendingDisposition,
    DeliveryUnlockPendingAuthorizer, DeliveryUnlockPendingCapability,
    DeliveryUnlockPendingDisposition, DeliveryUnlockedPendingRemoveAuthorizer,
    DeliveryUnlockedPendingRemoveCapability, DeliveryUnlockedPendingRemoveDisposition,
    DeliveryWorktreeCleanupIntent, authorize_persisted_delivery_branch_delete,
    authorize_persisted_delivery_remove, authorize_persisted_delivery_unlock,
    authorize_persisted_delivery_unlocked_pending_remove,
};
use coding_agent_store::{
    CleanupOperationRecord, CleanupOperationState, CleanupReconciliationReason,
    DeleteBranchCommandRequest, DeliveryEligibilitySnapshot, DeliveryOperationSnapshot,
    GitCommitOid, RemoveWorktreeCommandRequest, Store,
};

mod sealed {
    pub trait CleanupRuntimeRegistry {}
    pub trait CleanupRuntimeSession {}
}

pub(crate) use sealed::{
    CleanupRuntimeRegistry as DeliveryCleanupRuntimeRegistrySeal,
    CleanupRuntimeSession as DeliveryCleanupRuntimeSessionSeal,
};

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub trait DeliveryCleanupRuntimeRegistryTestSeam {}

#[cfg(feature = "test-support")]
impl<T: DeliveryCleanupRuntimeRegistryTestSeam> sealed::CleanupRuntimeRegistry for T {}

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub trait DeliveryCleanupRuntimeSessionTestSeam {}

#[cfg(feature = "test-support")]
impl<T: DeliveryCleanupRuntimeSessionTestSeam> sealed::CleanupRuntimeSession for T {}

/// Identifies whether a fresh cleanup binding precedes acceptance or resumes
/// an already durable operation. The Store values are matching constraints;
/// they never construct runtime authority.
pub enum DeliveryWorktreeCleanupBinding<'a> {
    Acceptance(&'a RemoveWorktreeCommandRequest),
    Persisted(&'a CleanupOperationRecord),
}

pub enum DeliveryBranchCleanupBinding<'a> {
    Acceptance(&'a DeleteBranchCommandRequest),
    Persisted(&'a CleanupOperationRecord),
}

enum WorktreeIntentEvidence {
    Runtime(DeliveryWorktreeCleanupIntent),
    #[cfg(feature = "test-support")]
    Test(u64),
}

/// Opaque worktree-cleanup authority returned only after fresh runtime
/// authentication. A durable Store phase must still authorize every command.
pub struct DeliveryLiveWorktreeCleanupIntent {
    evidence: WorktreeIntentEvidence,
}

impl DeliveryLiveWorktreeCleanupIntent {
    #[allow(dead_code)]
    pub(crate) fn from_runtime(intent: DeliveryWorktreeCleanupIntent) -> Self {
        Self {
            evidence: WorktreeIntentEvidence::Runtime(intent),
        }
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub const fn new_for_test(identity: u64) -> Self {
        Self {
            evidence: WorktreeIntentEvidence::Test(identity),
        }
    }

    pub(super) async fn authorize_unlock(
        self,
        store: &Store,
        operation: &CleanupOperationRecord,
    ) -> Result<DeliveryLiveUnlockPendingCapability, DeliveryLiveCleanupRuntimeError> {
        require_phase(operation, CleanupOperationState::UnlockPending)?;
        match self.evidence {
            WorktreeIntentEvidence::Runtime(intent) => {
                let authorizer = ExactWorktreePhaseAuthorizer::new(
                    store.clone(),
                    operation.clone(),
                    intent.clone(),
                );
                authorize_persisted_delivery_unlock(&authorizer, intent)
                    .await
                    .map(|capability| DeliveryLiveUnlockPendingCapability {
                        evidence: UnlockCapabilityEvidence::Runtime(capability),
                    })
            }
            #[cfg(feature = "test-support")]
            WorktreeIntentEvidence::Test(identity) => {
                verify_exact_operation(store, operation).await?;
                Ok(DeliveryLiveUnlockPendingCapability {
                    evidence: UnlockCapabilityEvidence::Test(identity),
                })
            }
        }
    }

    pub(super) async fn authorize_unlocked_pending_remove(
        self,
        store: &Store,
        operation: &CleanupOperationRecord,
    ) -> Result<DeliveryLiveUnlockedPendingRemoveCapability, DeliveryLiveCleanupRuntimeError> {
        require_phase(operation, CleanupOperationState::UnlockedPendingRemove)?;
        match self.evidence {
            WorktreeIntentEvidence::Runtime(intent) => {
                let authorizer = ExactWorktreePhaseAuthorizer::new(
                    store.clone(),
                    operation.clone(),
                    intent.clone(),
                );
                authorize_persisted_delivery_unlocked_pending_remove(&authorizer, intent)
                    .await
                    .map(|capability| DeliveryLiveUnlockedPendingRemoveCapability {
                        evidence: UnlockedPendingRemoveCapabilityEvidence::Runtime(capability),
                    })
            }
            #[cfg(feature = "test-support")]
            WorktreeIntentEvidence::Test(identity) => {
                verify_exact_operation(store, operation).await?;
                Ok(DeliveryLiveUnlockedPendingRemoveCapability {
                    evidence: UnlockedPendingRemoveCapabilityEvidence::Test(identity),
                })
            }
        }
    }

    pub(super) async fn authorize_remove(
        self,
        store: &Store,
        operation: &CleanupOperationRecord,
    ) -> Result<DeliveryLiveRemovePendingCapability, DeliveryLiveCleanupRuntimeError> {
        require_phase(operation, CleanupOperationState::RemovePending)?;
        match self.evidence {
            WorktreeIntentEvidence::Runtime(intent) => {
                let authorizer = ExactWorktreePhaseAuthorizer::new(
                    store.clone(),
                    operation.clone(),
                    intent.clone(),
                );
                authorize_persisted_delivery_remove(&authorizer, intent)
                    .await
                    .map(|capability| DeliveryLiveRemovePendingCapability {
                        evidence: RemoveCapabilityEvidence::Runtime(capability),
                    })
            }
            #[cfg(feature = "test-support")]
            WorktreeIntentEvidence::Test(identity) => {
                verify_exact_operation(store, operation).await?;
                Ok(DeliveryLiveRemovePendingCapability {
                    evidence: RemoveCapabilityEvidence::Test(identity),
                })
            }
        }
    }
}

impl std::fmt::Debug for DeliveryLiveWorktreeCleanupIntent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DeliveryLiveWorktreeCleanupIntent(<opaque>)")
    }
}

enum BranchIntentEvidence {
    Runtime(DeliveryBranchCleanupIntent),
    #[cfg(feature = "test-support")]
    Test(u64),
}

/// Opaque authority for the independent branch-delete receipt.
pub struct DeliveryLiveBranchCleanupIntent {
    evidence: BranchIntentEvidence,
}

impl DeliveryLiveBranchCleanupIntent {
    #[allow(dead_code)]
    pub(crate) fn from_runtime(intent: DeliveryBranchCleanupIntent) -> Self {
        Self {
            evidence: BranchIntentEvidence::Runtime(intent),
        }
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub const fn new_for_test(identity: u64) -> Self {
        Self {
            evidence: BranchIntentEvidence::Test(identity),
        }
    }

    pub(super) async fn authorize_delete(
        self,
        store: &Store,
        operation: &CleanupOperationRecord,
    ) -> Result<DeliveryLiveDeletePendingCapability, DeliveryLiveCleanupRuntimeError> {
        require_phase(operation, CleanupOperationState::DeletePending)?;
        match self.evidence {
            BranchIntentEvidence::Runtime(intent) => {
                let authorizer = ExactBranchPhaseAuthorizer::new(
                    store.clone(),
                    operation.clone(),
                    intent.clone(),
                );
                authorize_persisted_delivery_branch_delete(&authorizer, intent)
                    .await
                    .map(|capability| DeliveryLiveDeletePendingCapability {
                        evidence: DeleteCapabilityEvidence::Runtime(capability),
                    })
            }
            #[cfg(feature = "test-support")]
            BranchIntentEvidence::Test(identity) => {
                verify_exact_operation(store, operation).await?;
                Ok(DeliveryLiveDeletePendingCapability {
                    evidence: DeleteCapabilityEvidence::Test(identity),
                })
            }
        }
    }
}

impl std::fmt::Debug for DeliveryLiveBranchCleanupIntent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DeliveryLiveBranchCleanupIntent(<opaque>)")
    }
}

enum UnlockCapabilityEvidence {
    Runtime(DeliveryUnlockPendingCapability),
    #[cfg(feature = "test-support")]
    Test(u64),
}

pub struct DeliveryLiveUnlockPendingCapability {
    evidence: UnlockCapabilityEvidence,
}

enum UnlockedPendingRemoveCapabilityEvidence {
    Runtime(DeliveryUnlockedPendingRemoveCapability),
    #[cfg(feature = "test-support")]
    Test(u64),
}

pub struct DeliveryLiveUnlockedPendingRemoveCapability {
    evidence: UnlockedPendingRemoveCapabilityEvidence,
}

enum RemoveCapabilityEvidence {
    Runtime(DeliveryRemovePendingCapability),
    #[cfg(feature = "test-support")]
    Test(u64),
}

pub struct DeliveryLiveRemovePendingCapability {
    evidence: RemoveCapabilityEvidence,
}

enum DeleteCapabilityEvidence {
    Runtime(DeliveryDeletePendingCapability),
    #[cfg(feature = "test-support")]
    Test(u64),
}

pub struct DeliveryLiveDeletePendingCapability {
    evidence: DeleteCapabilityEvidence,
}

macro_rules! opaque_debug {
    ($type:ty, $name:literal) => {
        impl std::fmt::Debug for $type {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str($name)
            }
        }
    };
}

opaque_debug!(
    DeliveryLiveUnlockPendingCapability,
    "DeliveryLiveUnlockPendingCapability(<opaque>)"
);
opaque_debug!(
    DeliveryLiveUnlockedPendingRemoveCapability,
    "DeliveryLiveUnlockedPendingRemoveCapability(<opaque>)"
);
opaque_debug!(
    DeliveryLiveRemovePendingCapability,
    "DeliveryLiveRemovePendingCapability(<opaque>)"
);
opaque_debug!(
    DeliveryLiveDeletePendingCapability,
    "DeliveryLiveDeletePendingCapability(<opaque>)"
);

impl DeliveryLiveUnlockPendingCapability {
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub const fn identity_for_test(&self) -> Option<u64> {
        match &self.evidence {
            UnlockCapabilityEvidence::Test(identity) => Some(*identity),
            UnlockCapabilityEvidence::Runtime(_) => None,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn into_runtime(self) -> Option<DeliveryUnlockPendingCapability> {
        match self.evidence {
            UnlockCapabilityEvidence::Runtime(capability) => Some(capability),
            #[cfg(feature = "test-support")]
            UnlockCapabilityEvidence::Test(_) => None,
        }
    }
}

impl DeliveryLiveUnlockedPendingRemoveCapability {
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub const fn identity_for_test(&self) -> Option<u64> {
        match &self.evidence {
            UnlockedPendingRemoveCapabilityEvidence::Test(identity) => Some(*identity),
            UnlockedPendingRemoveCapabilityEvidence::Runtime(_) => None,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn into_runtime(self) -> Option<DeliveryUnlockedPendingRemoveCapability> {
        match self.evidence {
            UnlockedPendingRemoveCapabilityEvidence::Runtime(capability) => Some(capability),
            #[cfg(feature = "test-support")]
            UnlockedPendingRemoveCapabilityEvidence::Test(_) => None,
        }
    }
}

impl DeliveryLiveRemovePendingCapability {
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub const fn identity_for_test(&self) -> Option<u64> {
        match &self.evidence {
            RemoveCapabilityEvidence::Test(identity) => Some(*identity),
            RemoveCapabilityEvidence::Runtime(_) => None,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn into_runtime(self) -> Option<DeliveryRemovePendingCapability> {
        match self.evidence {
            RemoveCapabilityEvidence::Runtime(capability) => Some(capability),
            #[cfg(feature = "test-support")]
            RemoveCapabilityEvidence::Test(_) => None,
        }
    }
}

impl DeliveryLiveDeletePendingCapability {
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub const fn identity_for_test(&self) -> Option<u64> {
        match &self.evidence {
            DeleteCapabilityEvidence::Test(identity) => Some(*identity),
            DeleteCapabilityEvidence::Runtime(_) => None,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn into_runtime(self) -> Option<DeliveryDeletePendingCapability> {
        match self.evidence {
            DeleteCapabilityEvidence::Runtime(capability) => Some(capability),
            #[cfg(feature = "test-support")]
            DeleteCapabilityEvidence::Test(_) => None,
        }
    }
}

enum RefreshProofEvidence {
    Runtime(DeliveryBranchCleanupRefreshProof),
    #[cfg(feature = "test-support")]
    Test(u64),
}

/// Runtime proof for a legal target advance. Its target must be persisted
/// before consuming the proof and adopting the next runtime generation.
pub struct DeliveryLiveBranchCleanupRefreshProof {
    evidence: RefreshProofEvidence,
    fresh_target_head: GitCommitOid,
}

impl DeliveryLiveBranchCleanupRefreshProof {
    #[allow(dead_code)]
    pub(crate) fn from_runtime(
        proof: DeliveryBranchCleanupRefreshProof,
    ) -> Result<Self, DeliveryLiveCleanupRuntimeError> {
        let fresh_target_head =
            GitCommitOid::from_str(proof.fresh_target_head()).map_err(|_| {
                DeliveryLiveCleanupRuntimeError::ReconciliationRequired(
                    CleanupReconciliationReason::DeliveryStateInconsistent,
                )
            })?;
        Ok(Self {
            evidence: RefreshProofEvidence::Runtime(proof),
            fresh_target_head,
        })
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub const fn new_for_test(identity: u64, fresh_target_head: GitCommitOid) -> Self {
        Self {
            evidence: RefreshProofEvidence::Test(identity),
            fresh_target_head,
        }
    }

    pub const fn fresh_target_head(&self) -> &GitCommitOid {
        &self.fresh_target_head
    }

    pub(super) fn into_refreshed_intent(
        self,
    ) -> Result<DeliveryLiveBranchCleanupIntent, DeliveryLiveCleanupRuntimeError> {
        match self.evidence {
            RefreshProofEvidence::Runtime(proof) => proof
                .into_refreshed_intent()
                .map(DeliveryLiveBranchCleanupIntent::from_runtime)
                .map_err(|_| {
                    DeliveryLiveCleanupRuntimeError::ReconciliationRequired(
                        CleanupReconciliationReason::DeliveryStateInconsistent,
                    )
                }),
            #[cfg(feature = "test-support")]
            RefreshProofEvidence::Test(identity) => Ok(
                DeliveryLiveBranchCleanupIntent::new_for_test(identity.saturating_add(1)),
            ),
        }
    }
}

impl std::fmt::Debug for DeliveryLiveBranchCleanupRefreshProof {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DeliveryLiveBranchCleanupRefreshProof(<opaque>)")
    }
}

pub enum DeliveryLiveDeletePendingDisposition {
    RetryExactDelete,
    Deleted,
    RefreshExpectedTarget(DeliveryLiveBranchCleanupRefreshProof),
    KnownNotAppliedSourceNotMerged,
    KnownNotAppliedCommandTimedOut,
    ReconciliationRequired,
}

impl DeliveryLiveDeletePendingDisposition {
    #[allow(dead_code)]
    pub(crate) fn from_runtime(
        disposition: DeliveryDeletePendingDisposition,
    ) -> Result<Self, DeliveryLiveCleanupRuntimeError> {
        Ok(match disposition {
            DeliveryDeletePendingDisposition::RetryExactDelete => Self::RetryExactDelete,
            DeliveryDeletePendingDisposition::Deleted => Self::Deleted,
            DeliveryDeletePendingDisposition::RefreshExpectedTarget(proof) => {
                Self::RefreshExpectedTarget(DeliveryLiveBranchCleanupRefreshProof::from_runtime(
                    proof,
                )?)
            }
            DeliveryDeletePendingDisposition::KnownNotAppliedSourceNotMerged => {
                Self::KnownNotAppliedSourceNotMerged
            }
            DeliveryDeletePendingDisposition::KnownNotAppliedCommandTimedOut => {
                Self::KnownNotAppliedCommandTimedOut
            }
            DeliveryDeletePendingDisposition::ReconciliationRequired => {
                Self::ReconciliationRequired
            }
        })
    }
}

impl std::fmt::Debug for DeliveryLiveDeletePendingDisposition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::RetryExactDelete => "DeliveryLiveDeletePendingDisposition::RetryExactDelete",
            Self::Deleted => "DeliveryLiveDeletePendingDisposition::Deleted",
            Self::RefreshExpectedTarget(_) => {
                "DeliveryLiveDeletePendingDisposition::RefreshExpectedTarget(<opaque>)"
            }
            Self::KnownNotAppliedSourceNotMerged => {
                "DeliveryLiveDeletePendingDisposition::KnownNotAppliedSourceNotMerged"
            }
            Self::KnownNotAppliedCommandTimedOut => {
                "DeliveryLiveDeletePendingDisposition::KnownNotAppliedCommandTimedOut"
            }
            Self::ReconciliationRequired => {
                "DeliveryLiveDeletePendingDisposition::ReconciliationRequired"
            }
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DeliveryLiveCleanupRuntimeError {
    #[error("delivery cleanup runtime is unavailable")]
    Unavailable,
    #[error("delivery cleanup worktree is dirty")]
    TargetWorktreeDirty,
    #[error("delivery cleanup process-tree state is unproven")]
    ProcessCleanupUnproven,
    #[error("delivery cleanup requires reconciliation")]
    ReconciliationRequired(CleanupReconciliationReason),
}

/// Sealed runtime authority for the two independent cleanup actions. Every
/// persisted phase is rebound and re-authorized before a side effect.
#[async_trait::async_trait]
pub trait DeliveryCleanupRuntimeSession:
    sealed::CleanupRuntimeSession + Send + Sync + 'static
{
    async fn bind_worktree_cleanup(
        &self,
        snapshot: &DeliveryEligibilitySnapshot,
        binding: DeliveryWorktreeCleanupBinding<'_>,
    ) -> Result<DeliveryLiveWorktreeCleanupIntent, DeliveryLiveCleanupRuntimeError>;

    async fn bind_branch_cleanup(
        &self,
        snapshot: &DeliveryEligibilitySnapshot,
        binding: DeliveryBranchCleanupBinding<'_>,
    ) -> Result<DeliveryLiveBranchCleanupIntent, DeliveryLiveCleanupRuntimeError>;

    async fn drive_unlock_pending(
        &self,
        capability: DeliveryLiveUnlockPendingCapability,
    ) -> Result<DeliveryUnlockPendingDisposition, DeliveryLiveCleanupRuntimeError>;

    async fn drive_unlocked_pending_remove(
        &self,
        capability: DeliveryLiveUnlockedPendingRemoveCapability,
    ) -> Result<DeliveryUnlockedPendingRemoveDisposition, DeliveryLiveCleanupRuntimeError>;

    async fn drive_remove_pending(
        &self,
        capability: DeliveryLiveRemovePendingCapability,
    ) -> Result<DeliveryRemovePendingDisposition, DeliveryLiveCleanupRuntimeError>;

    async fn drive_delete_pending(
        &self,
        capability: DeliveryLiveDeletePendingCapability,
    ) -> Result<DeliveryLiveDeletePendingDisposition, DeliveryLiveCleanupRuntimeError>;
}

#[async_trait::async_trait]
pub trait DeliveryCleanupRuntimeRegistry:
    sealed::CleanupRuntimeRegistry + Send + Sync + 'static
{
    async fn open_cleanup_session(
        &self,
        snapshot: &DeliveryEligibilitySnapshot,
    ) -> Result<Arc<dyn DeliveryCleanupRuntimeSession>, DeliveryLiveCleanupRuntimeError>;
}

struct ExactWorktreePhaseAuthorizer {
    store: Store,
    operation: CleanupOperationRecord,
    intent: DeliveryWorktreeCleanupIntent,
}

impl ExactWorktreePhaseAuthorizer {
    fn new(
        store: Store,
        operation: CleanupOperationRecord,
        intent: DeliveryWorktreeCleanupIntent,
    ) -> Self {
        Self {
            store,
            operation,
            intent,
        }
    }

    async fn authorize(
        &self,
        intent: &DeliveryWorktreeCleanupIntent,
    ) -> Result<(), DeliveryLiveCleanupRuntimeError> {
        if !self.intent.is_same_runtime_intent(intent) {
            return Err(DeliveryLiveCleanupRuntimeError::ReconciliationRequired(
                CleanupReconciliationReason::DeliveryStateInconsistent,
            ));
        }
        verify_exact_operation(&self.store, &self.operation).await
    }
}

#[async_trait::async_trait]
impl DeliveryUnlockPendingAuthorizer for ExactWorktreePhaseAuthorizer {
    type Error = DeliveryLiveCleanupRuntimeError;

    async fn authorize_persisted_unlock_pending(
        &self,
        intent: &DeliveryWorktreeCleanupIntent,
    ) -> Result<(), Self::Error> {
        self.authorize(intent).await
    }
}

#[async_trait::async_trait]
impl DeliveryUnlockedPendingRemoveAuthorizer for ExactWorktreePhaseAuthorizer {
    type Error = DeliveryLiveCleanupRuntimeError;

    async fn authorize_persisted_unlocked_pending_remove(
        &self,
        intent: &DeliveryWorktreeCleanupIntent,
    ) -> Result<(), Self::Error> {
        self.authorize(intent).await
    }
}

#[async_trait::async_trait]
impl DeliveryRemovePendingAuthorizer for ExactWorktreePhaseAuthorizer {
    type Error = DeliveryLiveCleanupRuntimeError;

    async fn authorize_persisted_remove_pending(
        &self,
        intent: &DeliveryWorktreeCleanupIntent,
    ) -> Result<(), Self::Error> {
        self.authorize(intent).await
    }
}

struct ExactBranchPhaseAuthorizer {
    store: Store,
    operation: CleanupOperationRecord,
    intent: DeliveryBranchCleanupIntent,
}

impl ExactBranchPhaseAuthorizer {
    fn new(
        store: Store,
        operation: CleanupOperationRecord,
        intent: DeliveryBranchCleanupIntent,
    ) -> Self {
        Self {
            store,
            operation,
            intent,
        }
    }
}

#[async_trait::async_trait]
impl DeliveryDeletePendingAuthorizer for ExactBranchPhaseAuthorizer {
    type Error = DeliveryLiveCleanupRuntimeError;

    async fn authorize_persisted_delete_pending(
        &self,
        intent: &DeliveryBranchCleanupIntent,
    ) -> Result<(), Self::Error> {
        if !self.intent.is_same_runtime_intent(intent) {
            return Err(DeliveryLiveCleanupRuntimeError::ReconciliationRequired(
                CleanupReconciliationReason::DeliveryStateInconsistent,
            ));
        }
        verify_exact_operation(&self.store, &self.operation).await
    }
}

fn require_phase(
    operation: &CleanupOperationRecord,
    expected: CleanupOperationState,
) -> Result<(), DeliveryLiveCleanupRuntimeError> {
    if operation.state == expected && operation.failure_code.is_none() {
        Ok(())
    } else {
        Err(DeliveryLiveCleanupRuntimeError::ReconciliationRequired(
            CleanupReconciliationReason::DeliveryStateInconsistent,
        ))
    }
}

async fn verify_exact_operation(
    store: &Store,
    expected: &CleanupOperationRecord,
) -> Result<(), DeliveryLiveCleanupRuntimeError> {
    match store
        .delivery_operation_snapshot(expected.operation_id)
        .await
    {
        Ok(Some(DeliveryOperationSnapshot::Cleanup(actual))) if actual.as_ref() == expected => {
            Ok(())
        }
        Ok(Some(_)) | Ok(None) => Err(DeliveryLiveCleanupRuntimeError::ReconciliationRequired(
            CleanupReconciliationReason::DeliveryStateInconsistent,
        )),
        Err(_) => Err(DeliveryLiveCleanupRuntimeError::Unavailable),
    }
}
