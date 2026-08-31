mod abort;
mod actor;
mod cleanup;
mod cleanup_runtime;
mod command;
mod dependencies;
mod handle;
mod live_runtime;
mod merge;
mod operation_query;
mod preflight;
mod query;
mod recovery;
mod request;
mod runtime;
mod runtime_stage;
mod shutdown;
mod source;

pub(super) use actor::DeliveryIntakeGate;
use actor::DeliveryManager;
pub(super) use dependencies::DeliveryManagerBackend;
pub use dependencies::DeliveryManagerLiveDependencies;
pub(crate) use dependencies::{DeliveryTaskOwnershipBinding, DeliveryTaskOwnershipInstallError};
pub use handle::DeliveryManagerHandle;
pub use request::{
    DeliveryAcceptRequest, DeliveryDeleteBranchRequest, DeliveryManagerError,
    DeliveryManagerQuiesceSnapshot, DeliveryOperationRecoveryOutcome, DeliveryPreflightRequest,
    DeliveryRemoveWorktreeRequest,
};
pub use shutdown::DeliveryManagerShutdownProof;

pub use cleanup_runtime::{
    DeliveryBranchCleanupBinding, DeliveryCleanupRuntimeRegistry, DeliveryCleanupRuntimeSession,
    DeliveryLiveBranchCleanupIntent, DeliveryLiveBranchCleanupRefreshProof,
    DeliveryLiveCleanupRuntimeError, DeliveryLiveDeletePendingCapability,
    DeliveryLiveDeletePendingDisposition, DeliveryLiveRemovePendingCapability,
    DeliveryLiveUnlockPendingCapability, DeliveryLiveUnlockedPendingRemoveCapability,
    DeliveryLiveWorktreeCleanupIntent, DeliveryWorktreeCleanupBinding,
};
pub(crate) use cleanup_runtime::{
    DeliveryCleanupRuntimeRegistrySeal, DeliveryCleanupRuntimeSessionSeal,
};
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use cleanup_runtime::{
    DeliveryCleanupRuntimeRegistryTestSeam, DeliveryCleanupRuntimeSessionTestSeam,
};
pub use live_runtime::{
    DeliveryAcceptAuthenticationError, DeliveryLiveAbortAppliedProof, DeliveryLiveAbortDisposition,
    DeliveryLiveAbortProof, DeliveryLiveExpectedMergeProof, DeliveryLiveMergeAppliedProof,
    DeliveryLiveMergeDisposition, DeliveryLiveRuntimeError, DeliveryLiveRuntimeRegistry,
    DeliveryLiveRuntimeSession, DeliveryLiveSourceAppliedProof, DeliveryLiveSourceDisposition,
    DeliveryLiveSourceObjectProof, DeliveryLiveSourceResult,
};
pub(crate) use live_runtime::{DeliveryLiveRuntimeRegistrySeal, DeliveryLiveRuntimeSessionSeal};
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use live_runtime::{DeliveryLiveRuntimeRegistryTestSeam, DeliveryLiveRuntimeSessionTestSeam};
pub use operation_query::DeliveryOperationQuery;
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use operation_query::DeliveryOperationQueryTestSeam;
pub use runtime::{
    DeliveryPreparedPreflight, DeliveryProcessProof, DeliveryProcessProofError,
    DeliveryProcessProofProvider, DeliveryRuntimeAuthentication,
    DeliveryRuntimeAuthenticationOutcome, DeliveryRuntimeFailure, DeliveryRuntimeObservation,
    DeliveryRuntimeObservationUnavailableReason, DeliveryRuntimeRegistry, DeliveryRuntimeSession,
};
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use runtime::{
    DeliveryProcessProofProviderTestSeam, DeliveryRuntimeRegistryTestSeam,
    DeliveryRuntimeSessionTestSeam,
};
pub(crate) use runtime::{DeliveryRuntimeRegistrySeal, DeliveryRuntimeSessionSeal};
pub(crate) use runtime::{ProcessLivenessDeliveryProofProvider, delivery_process_scope_id};

#[cfg(test)]
mod tests;
