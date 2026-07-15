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

mod event_dispatcher;
mod fake_runner;
mod local_client;
#[cfg(target_os = "macos")]
mod macos_acl;
mod native_dialog;
mod platform;
mod repository_service;
mod security;
mod server;
mod service_state;
mod shutdown;
mod single_instance;
mod store_writer;
mod task_manager;

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
pub use repository_service::{
    CommandRunner, DiscoveredRepository, RepositoryDiscovery, RepositoryDiscoveryError,
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
pub use store_writer::{EventWake, StoreWriterError, StoreWriterHandle, WriteReceipt};
pub use task_manager::{
    CancelOutcome, QuiesceResult, RunContext, RunnerEvent, RunnerEventError, RunnerEventSink,
    RunnerOutcome, RunnerShutdownHandle, TaskManagerError, TaskManagerHandle, TaskRunner,
};
