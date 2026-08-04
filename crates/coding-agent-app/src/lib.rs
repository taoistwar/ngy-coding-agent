//! Application composition responsibilities for the coding agent.
//!
//! Store mutation internals are deliberately unavailable to application consumers:
//!
//! ```compile_fail
//! use coding_agent_app::{
//!     StoreWriterBackend, StoreWriterBackendFuture, StoreWriterOperation,
//!     StoreWriterOperationOutcome,
//! };
//! ```

mod artifact_reconciliation;
mod bootstrap_join;
mod coding_agent_runner;
mod event_dispatcher;
#[cfg(any(test, feature = "test-support"))]
mod fake_runner;
mod local_client;
#[cfg(target_os = "macos")]
mod macos_acl;
mod native_dialog;
mod pending_durable;
mod platform;
mod provider_config;
mod repository_control;
mod repository_service;
mod run_context;
mod runner_factory;
mod runtime_config;
mod scheduler;
mod scheduler_api_projection;
mod security;
mod server;
mod service_state;
mod shutdown;
mod single_instance;
mod static_assets;
mod storage_monitor;
mod storage_policy;
mod store_writer;
mod task_manager;
#[cfg(feature = "test-support")]
mod test_support;

pub use artifact_reconciliation::{
    ArtifactMutationDisposition, ArtifactReconciliationDecision, ArtifactReconciliationError,
    ArtifactReconciliationSummary, AttemptArtifactObserver, LiveStoreWriterArtifactAdapter,
    RestartArtifactObservation, StartupDirectStoreArtifactAdapter,
    VerifiedArtifactReconciliationEvidence, WorktreeArtifactObserver, decide_restart_artifact,
    reconcile_restart_artifacts, reconcile_startup_artifacts_direct,
    reconcile_startup_artifacts_grouped,
};
pub use coding_agent_provider::{
    ApiKey, MAX_PROVIDER_CONFIG_BYTES, PROVIDER_CONFIG_INVALID, ProviderConfig,
    ProviderConfigError, ProviderConfigErrorReason, RedactedText, SecretRedactor,
};
#[cfg(feature = "test-support")]
pub use coding_agent_runner::TestTaskRuntimeSession;
pub use coding_agent_runner::{
    AttemptArtifactObservation, AttemptReservation, CodingAgentAttempt, CodingAgentAttemptFactory,
    CodingAgentPreparationControl, CodingAgentRunner, CodingAgentRunnerConfig,
    CodingAgentRunnerConfigError, CodingAttemptError, CodingAttemptProvisionError,
    Project2RuntimeSessionFactory, ProvisionedAgentRuntimeFactory,
    RepositoryWorktreeProvisionerFactory, TaskAgentRuntime, TaskModelProviderFactory,
    TaskModelSession, WorktreeCodingAgentAttemptFactory,
};
pub use event_dispatcher::{EventDispatcherError, EventDispatcherHandle};
#[cfg(any(test, feature = "test-support"))]
pub use fake_runner::{FakeRunnerConfig, FakeTaskRunner};
#[cfg(feature = "test-support")]
pub use fake_runner::{FakeScenario, ScriptedFakeRunner};
#[cfg(target_os = "macos")]
pub use native_dialog::NativeDialogMainThreadHost;
pub use native_dialog::{NativeDialogService, PickerError};
pub use pending_durable::{
    DurableCompletion, DurableDisposition, DurableOperationIdentity, DurableOperationKind,
    KnownNotAppliedError, KnownNotAppliedReason, MutationSequence, MutationSequenceDisposition,
    OutcomeUnknownReason, PendingDurableResult, PendingReplayReceipt, StopIntentBatchIdentityError,
    TaskMutationIdentity,
};
pub use platform::{
    BrowserLaunchError, BrowserLauncher, PlatformPaths, PrivateFile, SystemWallClock, WallClock,
};
pub use provider_config::{
    ProviderConfigLoadError, ProviderConfigLoadErrorKind, load_provider_config,
};
pub use repository_control::{
    RepositoryControlCoordinator, RepositoryControlError, RepositoryControlLease,
    RepositoryControlPoisonReason, RepositoryControlState, RepositoryIdentityResolutionError,
    RepositoryIdentityResolver, VerifiedRepositoryControlState,
};
pub use repository_service::FilesystemRepositoryIdentityResolver;
#[cfg(not(any(test, feature = "test-support")))]
pub(crate) use repository_service::RepositoryDiscovery;
#[cfg(any(test, feature = "test-support"))]
pub use repository_service::{CommandRunner, RepositoryDiscovery};
pub use repository_service::{DiscoveredRepository, RepositoryDiscoveryError};
pub use run_context::RunContext;
#[cfg(any(test, feature = "test-support"))]
pub use runner_factory::FixedStartupRunnerFactory;
pub use runner_factory::{
    PreActorStartupRunnerContext, ProductionStartupRunnerFactory, StartupRunnerContext,
    StartupRunnerFactory, StartupRunnerFactoryError, StartupRunnerSelection,
};
pub use runtime_config::{
    MAX_RUNTIME_CONFIG_BYTES, RUNTIME_CONFIG_INVALID, RuntimeConfig, RuntimeConfigLoadError,
    RuntimeConfigLoadErrorKind, RuntimeStorageConfig, load_runtime_config,
};
#[cfg(feature = "test-support")]
pub use runtime_config::{derive_cargo_jobs_per_task_for_test, load_runtime_config_for_test};
pub use scheduler::{
    CandidateEvaluation, PermitLedger, PermitLedgerError, PermitLedgerSnapshot,
    PermitOwnershipState, PermitOwnershipWitness, PermitToken, QueueReason, QueueReasonSignals,
    QueuedTaskCandidate, RepositoryCoordinationKey, SchedulerAdmissionGates,
    SchedulerConcurrencyLimits, SchedulerLimitError, SchedulerProjectionCandidate,
    SchedulerProjectionSnapshot, SchedulerPublishOutcome, SchedulerPublisherError,
    SchedulerRepositoryStorageState, SchedulerScan, SchedulerScanError, SchedulerStatePublisher,
    SchedulerStorageNotification, SchedulerStorageNotificationSink, SharedPermitOwnership,
    TerminalProcessCleanReleaseProof, TerminalReleaseProofError, advance_membership_watermark,
    is_membership_lifecycle_event, is_terminal_membership_event, project_queue_reason,
    scan_queued_candidates,
};
pub use security::{
    LaunchToken, LauncherSecret, SecurityClock, SecurityError, SecurityManager, SecuritySeed,
    SessionRecord, SystemSecurityClock,
};
pub(crate) use server::MutationDrainOutcome;
pub use server::{ApplicationBackend, MutationGate, MutationGuard, build_runtime_router};
pub use service_state::{
    InvalidServiceTransition, ServiceState, ServiceStateController, ServiceStateSnapshot,
};
pub use shutdown::{
    DegradedCoordinator, DegradedCoordinatorError, DegradedRecoveryResult, ShutdownCoordinator,
    ShutdownOutcome,
};
#[cfg(feature = "test-support")]
pub use single_instance::PrimaryRuntimeTestHandles;
pub use single_instance::{
    AvailableParallelismProbe, BrowserOpener, InstanceLock, ListenerFactory, NativeMessageSink,
    PrimaryRuntime, RuntimeDescriptor, RuntimeDescriptorError, SecondaryRuntime,
    StartupDependencies, StartupError, StartupOutcome, StartupPaths, StartupPhase,
    StartupPhaseController, StoreFactory, launch, run_degraded_shutdown_warning_if_requested,
};
pub use static_assets::StaticAssetService;
pub use storage_monitor::{
    MonitoredRepositoryStorageState, MonitoredStorageScope, MonitoredStorageScopeBinding,
    STORAGE_PROBE_TIMEOUT, STORAGE_SAMPLE_FRESHNESS, STORAGE_SAMPLE_INTERVAL, StorageActivity,
    StorageCriticalNotification, StorageCriticalNotificationSink, StorageMonitorClock,
    StorageMonitorConfig, StorageMonitorError, StorageMonitorHandle, StorageMonitorSnapshot,
    StorageProbeTarget, TokioStorageMonitorClock,
};
pub use storage_policy::{
    ActiveTaskStorage, DATA_CRITICAL_BYTES, DATA_RECOVERY_MARGIN_BYTES,
    GIT_RUNTIME_ADMISSION_BYTES, GIT_RUNTIME_CRITICAL_BYTES, GIT_RUNTIME_RECOVERY_MARGIN_BYTES,
    STORAGE_RECOVERY_SAMPLE_INTERVAL, ScopeStorageClassification, StorageObservation,
    StoragePolicy, StoragePolicyError, StorageScope, StorageScopeBinding, StorageScopeHysteresis,
    StorageScopeState, StorageState, StorageThresholds, VolumeAdmissionRequirements,
    aggregate_storage_state, critical_affected_tasks,
};
pub use store_writer::{
    EventWake, FinalizeReviewedTaskRequest, FinalizeUnreviewedTaskRequest,
    PendingDurableSubmission, RecordReviewRequest, StoreWriterError, StoreWriterHandle,
    StoreWriterSubmission, StoreWriterSubmitError, WriteReceipt,
};
#[cfg(feature = "test-support")]
pub use store_writer::{
    StoreWriterFaultPoint, StoreWriterFaultSpec, StoreWriterOperationKind, StoreWriterPriority,
    StoreWriterSchedulingError, StoreWriterSchedulingHarness, StoreWriterTestConfigError,
    StoreWriterTestController,
};
pub use task_manager::{
    CancelOutcome, QuiesceResult, RunnerEvent, RunnerEventError, RunnerEventSink, RunnerOutcome,
    RunnerShutdownHandle, TaskManagerError, TaskManagerHandle, TaskManagerLaunchResources,
    TaskRunner,
};
#[cfg(feature = "test-support")]
pub use task_manager::{SchedulerProjectionTestSnapshot, TaskManagerSafetySnapshot};
#[cfg(feature = "test-support")]
pub use test_support::{
    ActorPausePoint, LegacyV2Seed, ProcessRuntimeConfig, ProcessRuntimeStorageConfig,
    ProcessStorageSample, ProcessTestConfig, ProcessTestConfigError, ProcessTestEnvironment,
    TEST_APP_DATA_ENV, TEST_BROWSER_PROBE_FILE, TEST_PICKER_PROBE_FILE, TEST_RUNTIME_ENV,
    TEST_SCENARIO_ENV, TEST_STARTUP_RECOVERY_PROBE_FILE, VirtualReleaseSignal,
    VirtualReleaseTarget,
};
