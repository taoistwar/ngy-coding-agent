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
mod coding_agent_runner;
mod event_dispatcher;
mod fake_runner;
mod local_client;
#[cfg(target_os = "macos")]
mod macos_acl;
mod native_dialog;
mod platform;
mod provider_config;
mod repository_service;
mod runner_factory;
mod security;
mod server;
mod service_state;
mod shutdown;
mod single_instance;
mod static_assets;
mod store_writer;
mod task_manager;
#[cfg(feature = "test-support")]
mod test_support;

pub use artifact_reconciliation::{
    ArtifactReconciliationError, ArtifactReconciliationSummary, AttemptArtifactObserver,
    RestartArtifactObservation, WorktreeArtifactObserver, reconcile_restart_artifacts,
};
pub use coding_agent_provider::{
    ApiKey, MAX_PROVIDER_CONFIG_BYTES, PROVIDER_CONFIG_INVALID, ProviderConfig,
    ProviderConfigError, ProviderConfigErrorReason, RedactedText, SecretRedactor,
};
pub use coding_agent_runner::{
    AttemptArtifactObservation, AttemptReservation, CodingAgentAttempt, CodingAgentAttemptFactory,
    CodingAgentRunner, CodingAgentRunnerConfig, CodingAgentRunnerConfigError, CodingAttemptError,
    CodingAttemptProvisionError, Project2RuntimeSessionFactory, ProvisionedAgentRuntimeFactory,
    RepositoryWorktreeProvisionerFactory, TaskAgentRuntime, TaskModelProviderFactory,
    TaskModelSession, WorktreeCodingAgentAttemptFactory,
};
pub use event_dispatcher::{EventDispatcherError, EventDispatcherHandle};
pub use fake_runner::{FakeRunnerConfig, FakeTaskRunner};
#[cfg(feature = "test-support")]
pub use fake_runner::{FakeScenario, ScriptedFakeRunner};
#[cfg(target_os = "macos")]
pub use native_dialog::NativeDialogMainThreadHost;
pub use native_dialog::{NativeDialogService, PickerError};
pub use platform::{
    BrowserLaunchError, BrowserLauncher, PlatformPaths, PrivateFile, SystemWallClock, WallClock,
};
pub use provider_config::{
    ProviderConfigLoadError, ProviderConfigLoadErrorKind, load_provider_config,
};
pub use repository_service::{
    CommandRunner, DiscoveredRepository, RepositoryDiscovery, RepositoryDiscoveryError,
};
pub use runner_factory::{
    FixedStartupRunnerFactory, ProductionStartupRunnerFactory, StartupRunnerContext,
    StartupRunnerFactory, StartupRunnerFactoryError, StartupRunnerSelection,
};
pub use security::{
    LaunchToken, LauncherSecret, SecurityClock, SecurityError, SecurityManager, SecuritySeed,
    SessionRecord, SystemSecurityClock,
};
pub use server::{ApplicationBackend, MutationGate, MutationGuard, build_runtime_router};
pub use service_state::{
    InvalidServiceTransition, ServiceState, ServiceStateController, ServiceStateSnapshot,
};
pub use shutdown::{
    DegradedCoordinator, DegradedCoordinatorError, DegradedRecoveryResult, PendingDurableResult,
    ShutdownCoordinator, ShutdownOutcome,
};
#[cfg(feature = "test-support")]
pub use single_instance::PrimaryRuntimeTestHandles;
pub use single_instance::{
    BrowserOpener, InstanceLock, ListenerFactory, NativeMessageSink, PrimaryRuntime,
    RuntimeDescriptor, RuntimeDescriptorError, SecondaryRuntime, StartupDependencies, StartupError,
    StartupOutcome, StartupPaths, StartupPhase, StartupPhaseController, StoreFactory, launch,
    run_degraded_shutdown_warning_if_requested,
};
pub use static_assets::StaticAssetService;
pub use store_writer::{EventWake, StoreWriterError, StoreWriterHandle, WriteReceipt};
#[cfg(feature = "test-support")]
pub use store_writer::{
    StoreWriterFaultPoint, StoreWriterFaultSpec, StoreWriterOperationKind,
    StoreWriterTestConfigError, StoreWriterTestController,
};
pub use task_manager::{
    CancelOutcome, QuiesceResult, RunContext, RunnerEvent, RunnerEventError, RunnerEventSink,
    RunnerOutcome, RunnerShutdownHandle, TaskManagerError, TaskManagerHandle, TaskRunner,
};
#[cfg(feature = "test-support")]
pub use test_support::{
    ActorPausePoint, ProcessTestConfig, ProcessTestConfigError, ProcessTestEnvironment,
    TEST_APP_DATA_ENV, TEST_BROWSER_PROBE_FILE, TEST_PICKER_PROBE_FILE, TEST_RUNTIME_ENV,
    TEST_SCENARIO_ENV, TEST_STARTUP_RECOVERY_PROBE_FILE, VirtualReleaseSignal,
    VirtualReleaseTarget,
};
