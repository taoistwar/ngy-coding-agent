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
