//! Capability-scoped repository and process runtime.
//!
//! Generic command construction and the process supervisor are deliberately
//! not part of the public API; external callers must use the typed Cargo/Git
//! facades.
//!
//! ```compile_fail
//! use coding_agent_runtime::{ProcessSupervisor, ValidatedCommand};
//! ```

mod atomic_replace;
mod cargo_tools;
mod command_policy;
mod delivery;
mod diff;
mod file_tools;
mod fingerprint;
mod git_tools;
mod native_fs;
mod process_liveness;
// Task 6 wires this private substrate through typed command adapters. Keep the
// generic process entry point unexported so callers cannot bypass validation.
#[allow(dead_code)]
mod process_supervisor;
mod quality_runtime;
mod relative_path;
mod repository_discovery;
mod review_diff;
mod role_engine_factory;
mod role_runtime;
mod root_capability;
mod runtime_session;
mod storage;
mod target_checkout;
mod tool_discovery;
mod worktree;

pub use atomic_replace::{
    AtomicFileReplacer, AtomicReplaceError, AtomicReplaceLimits, AtomicReplaceLimitsError,
    ReplaceDisposition, ReplaceFileResult,
};
pub use cargo_tools::{
    CargoCatalog, CargoPackage, CargoRunResult, CargoRunStatus, CargoToolError, CargoToolLimits,
    CargoTools,
};
pub use command_policy::{CommandPolicyError, ExecutionDirectory, PinnedExecutable};
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use delivery::probe_delivery_git_with_after_initialize_hook_for_test;
pub use delivery::{
    DeliveryAbortAppliedPersistenceBinding, DeliveryAbortAppliedProof, DeliveryAbortCapability,
    DeliveryAbortError, DeliveryAbortOutcome, DeliveryAbortPendingAuthorizer,
    DeliveryAbortPendingDisposition, DeliveryAbortPersistenceBinding, DeliveryAbortProof,
    DeliveryAbortProofCapture, DeliveryBranchCleanupIntent,
    DeliveryBranchCleanupRecoveryBindingOutcome, DeliveryBranchCleanupRefreshProof,
    DeliveryCandidateTree, DeliveryCommitPersistenceMetadata, DeliveryConflictPath,
    DeliveryConflictPathEncoding, DeliveryDeletePendingAuthorizer, DeliveryDeletePendingCapability,
    DeliveryDeletePendingDisposition, DeliveryExpectedMerge,
    DeliveryExpectedMergePersistenceBinding, DeliveryGitObjectFormat, DeliveryGitProbeError,
    DeliveryGitVersion, DeliveryKnownMergeConflict, DeliveryMergeAppliedPersistenceBinding,
    DeliveryMergeAppliedProof, DeliveryMergeError, DeliveryMergeInput, DeliveryMergeOutcome,
    DeliveryMergePendingDisposition, DeliveryMergeRecoveryBindingOutcome,
    DeliveryMergeRecoveryCapability, DeliveryPersistedAbortRecoveryObservation,
    DeliveryPersistedMergeRecovery, DeliveryPersistedSourceRecovery, DeliveryPersistedSourceState,
    DeliveryPersistedTargetRecovery, DeliveryPersistenceBinding, DeliveryPersistenceInputError,
    DeliveryPreflightError, DeliveryPreflightResult, DeliveryPreflightSource,
    DeliveryRemovePendingAuthorizer, DeliveryRemovePendingCapability,
    DeliveryRemovePendingDisposition, DeliverySourceAppliedPersistenceBinding,
    DeliverySourceCapability, DeliverySourceCommit, DeliverySourceCommitInput, DeliverySourceError,
    DeliverySourceLimits, DeliverySourceObjectPersistenceBinding, DeliverySourcePendingState,
    DeliverySourceProvisioner, DeliverySourceRecoveryBindingOutcome,
    DeliverySourceRecoveryCapability, DeliverySourceRecoveryDisposition,
    DeliverySourceRecoveryIntent, DeliveryTargetCapability, DeliveryTargetError,
    DeliveryTargetProvisioner, DeliveryTargetRecoveryBindingOutcome,
    DeliveryTargetRecoveryCapability, DeliveryTargetRecoveryIntent, DeliveryTargetRequest,
    DeliveryUnlockPendingAuthorizer, DeliveryUnlockPendingCapability,
    DeliveryUnlockPendingDisposition, DeliveryUnlockedPendingRemoveAuthorizer,
    DeliveryUnlockedPendingRemoveCapability, DeliveryUnlockedPendingRemoveDisposition,
    DeliveryWorktreeCleanupError, DeliveryWorktreeCleanupIntent,
    DeliveryWorktreeCleanupProvisioner, DeliveryWorktreeCleanupRecoveryBindingOutcome,
    DeliveryWorktreeCleanupRecoveryPhase, PreparedDeliveryPreflightSource, ProbedDeliveryGit,
    RegisteredDeliveryTargetObservation, abort_expected_delivery_merge,
    apply_expected_delivery_merge, authorize_persisted_delivery_abort,
    authorize_persisted_delivery_branch_delete, authorize_persisted_delivery_remove,
    authorize_persisted_delivery_unlock, authorize_persisted_delivery_unlocked_pending_remove,
    bind_persisted_delivery_merge_recovery, build_expected_delivery_merge,
    build_expected_persisted_delivery_merge, capture_delivery_abort_proof,
    capture_delivery_abort_proof_from_recovery, capture_persisted_delivery_abort_proof,
    capture_persisted_delivery_abort_recovery, classify_delivery_abort_pending,
    classify_delivery_merge_pending, classify_persisted_delivery_merge_pending,
    preflight_delivery_merge, preflight_prepared_delivery_merge, probe_delivery_git,
    project_persisted_delivery_source_applied, project_persisted_delivery_source_object,
    retry_delivery_abort_pending, retry_delivery_merge_pending,
    retry_persisted_delivery_abort_pending, retry_persisted_delivery_merge_pending,
};
pub use diff::{DiffCollector, DiffError, DiffLimits};
pub use file_tools::{
    FileEntry, FileEntryKind, FileToolError, FileToolLimits, FileToolLimitsError, FileTools,
    ListFilesResult, NumberedLine, ReadFileResult, SearchMatch, SearchTextResult,
};
pub use fingerprint::{FingerprintError, FingerprintLimits, WorkspaceFingerprinter};
pub use git_tools::{GitRunResult, GitRunStatus, GitToolError, GitToolLimits, GitTools};
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use process_liveness::HeldProcessLivenessTreeForTest;
pub use process_liveness::{
    ProcessCleanupProof, ProcessLivenessDirectory, ProcessLivenessError, ProcessLivenessScope,
    SealedProcessLivenessScope,
};
pub use process_supervisor::{
    CapturedStream, CommandResult, ProcessError, ProcessLimits, ProcessLimitsError,
    ProcessSpawnGuard, acquire_process_spawn_lock,
};
#[doc(hidden)]
pub use process_supervisor::{
    MAX_EXACT_CHILD_INPUT_BYTES_FOR_TEST, ProcessStdinTestObservation, ProcessStdinTestOutcome,
    ProcessStdinTestScenario, exercise_process_stdin_for_test,
};
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use process_supervisor::{
    ProcessFault, ProcessFaultController, ProcessFaultControllerError, ProcessFaultEvent,
    ProcessFaultEventKind, ProcessFaultZeroLiveProof,
};
pub use relative_path::{RelativePath, RelativePathError};
pub use repository_discovery::{RepositoryDiscoveryCommandError, RepositoryDiscoveryCommands};
pub use role_engine_factory::RoleScopedEngineFactory;
pub use role_runtime::RoleScopedRuntime;
pub use root_capability::{DirectoryIdentityError, DirectoryIdentityMarker, RootCapability};
pub use runtime_session::{
    ATTEMPT_IDENTITY_MISMATCH, RuntimeSession, RuntimeSessionError, RuntimeSessionLimits,
};
pub use storage::{
    NativeVolumeSampler, VolumeIdentity, VolumeSample, VolumeSampleError, VolumeSampler,
};
pub use tool_discovery::{ToolDiscoveryError, ToolchainPaths, discover as discover_toolchain};
#[cfg(feature = "test-support")]
pub use worktree::WorktreeSideEffectTestOutcome;
pub use worktree::{
    ProvisionedWorktree, WorktreeArtifactState, WorktreeError, WorktreeIdentity, WorktreeLimits,
    WorktreeObservation, WorktreeObservationOutcome, WorktreeProvisionError, WorktreeProvisioner,
    WorktreeReservation,
};

// Internal-only retained primary-checkout authority used by Task 13. Keeping
// this crate-private prevents callers from obtaining a target Git/worktree
// binding or raw checkout paths.
pub(crate) use target_checkout::{
    RegisteredCheckoutAuthentication, RegisteredCheckoutAuthenticator,
    RegisteredCheckoutCommandContext,
};
