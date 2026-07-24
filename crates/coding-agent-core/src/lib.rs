//! Provider- and runtime-neutral coding-agent orchestration contracts.

mod agent_loop;
mod budget;
mod error;
mod event;
mod limits;
mod model;
mod multi_role_orchestrator;
mod ports;
mod quality_state;
mod retained_result;
mod review_diff;
mod role;
mod role_loop;
mod role_transcript;

pub use agent_loop::{
    AgentCancellation, AgentCompletion, AgentFailure, AgentInput, AgentLoop, AgentOutcome,
};
pub use budget::*;
pub use coding_agent_domain::{
    CheckEvidenceStatus, RequiredCheck, RequiredCheckKind, RequiredCheckSelector,
    ReviewCoverageEvidence, WorkspaceDigest, is_valid_cargo_selector,
};
pub use error::{
    PROVIDER_RESPONSE_ROLE_ACTION_NOT_ALLOWED, PROVIDER_RESPONSE_ROLE_OUTPUT_INVALID,
    ProviderError, RuntimeError,
};
pub use event::{
    ActivityEvent, ActivityLevel, AgentEvent, DiffEvent, DiffFile, DiffFileStatus, PlanEvent,
    PlanItem, PlanItemStatus, TestCase, TestEvent, TestStatus,
};
pub use limits::{AgentLimits, AgentLimitsError};
pub use model::{
    ModelMessage, ModelRequest, ModelResponse, ModelToolChoice, TerminalSnapshot, ToolCall,
    ToolCallBatch, ToolRequest, ToolResult, ToolStatus, WorkspaceFingerprint,
};
pub use multi_role_orchestrator::{
    FinalizationGuard, FinalizationGuardError, MultiRoleFailure, MultiRoleInput,
    MultiRoleOrchestrator, MultiRoleOutcome, MultiRoleRunReport, RoleEngineFactory,
};
pub use ports::{
    AgentEventSink, AgentRuntime, ContextRedactor, MAX_VALIDATION_MODEL_RESULT_BYTES,
    ModelProvider, PreparedModelProvider, PreparedProviderRequest, RawProviderResponse,
    RepositoryCheckCatalog, ReviewDiffRuntime, ToolRuntime, ValidationObservation,
    ValidationRuntime,
};
pub use quality_state::{
    CheckRunToken, CheckpointChange, QualityStateError, RequiredCheckLedger, WorkspaceCheckpoint,
    project_test_snapshot, project_unverified_test_snapshot,
};
pub use retained_result::{
    MAX_RETAINED_REVIEW_CHUNK_BATCH_BYTES, MAX_RETAINED_REVIEW_CHUNK_BYTES,
    MAX_RETAINED_REVIEW_COVERAGE_BYTES, MAX_RETAINED_REVIEW_MANIFEST_BYTES,
    MAX_RETAINED_TOOL_CALL_ID_BYTES, RetainedResultError, RetainedToolResult,
    canonical_tool_result_wire_value,
};
pub use review_diff::{
    MAX_REVIEW_DIFF_BATCH_CHUNKS, MAX_REVIEW_DIFF_BATCHES, MAX_REVIEW_DIFF_TYPED_CHUNK_BYTES,
    MAX_REVIEW_DIFF_TYPED_MANIFEST_BYTES, REVIEW_DIFF_MANIFEST_DOMAIN, ReviewDiffBundle,
    ReviewDiffCheckpoint, ReviewDiffChunk, ReviewDiffChunkBatch, ReviewDiffChunkRequest,
    ReviewDiffError, ReviewDiffInputFile, ReviewDiffManifest, ReviewDiffManifestFile,
    validate_review_coverage, validate_terminal_review_coverage,
};
pub use role::{
    ActionRequest, AllowedActions, BlockedReason, BlockedSubmission, CheckSelectorSubmission,
    ControlKind, ControlRequest, ExecutionSubmission, PlanProgressStatus, PlanProgressSubmission,
    PlanProgressUpdate, PlanStepSubmission, PlanSubmission, RequiredAction,
    ReviewFindingSubmission, ReviewSubmission, Role, RoleContractError, RuntimeActionRequest,
    validate_action_batch, validate_role_response,
};
pub use role_loop::{
    BlockedExecutor, BlockedReviewer, DurableCheckpointAck, DurableEventAck, DurableRoleEvent,
    EXECUTOR_SYSTEM_POLICY, ExecutorRoleInput, ExecutorRoleLoop, ExecutorRoleOutcome,
    PLANNER_SYSTEM_POLICY, PlannerRoleInput, PlannerRoleLoop, PlannerRoleOutcome,
    REVIEWER_SYSTEM_POLICY, ReviewerRoleInput, ReviewerRoleLoop, ReviewerRoleOutcome,
    RoleActionRuntime, RoleActivityEvent, RoleEngine, RoleEvent, RoleEventSink, RoleLoopError,
    RoleRuntimeResult, RoleStageFailure, ValidatedExecution, ValidatedPlannerPlan,
    ValidatedReviewDecision,
};
pub use role_transcript::{
    ContinuationHandoff, MAX_ROLE_HANDOFF_BYTES, PlannerHandoff, RoleHandoff, RoleTranscript,
    RoleTranscriptError, executor_rework_banner,
};
