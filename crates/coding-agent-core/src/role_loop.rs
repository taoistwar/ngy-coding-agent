use std::collections::HashSet;
use std::sync::Arc;

use coding_agent_domain::{
    CheckActor, DeliveryReadiness, NewReviewEvidence, PlanItem, PlanItemStatus, PlanSnapshot,
    RequiredCheck, RequiredCheckSelector, ReviewCoverageEvidence, ReviewDecisionSource,
    ReviewFinding, ReviewVerdict, TaskFailure, TaskStatus, TestSnapshot,
};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::{
    ActionRequest, BlockedSubmission, BudgetError, BudgetResource, ContextRedactor, ControlRequest,
    DiffEvent, EarlyRoleBudgetTermination, ModelResponse, ModelToolChoice, PlannerHandoff,
    PreparedModelProvider, ProviderError, RequiredAction, RequiredCheckLedger, RetainedResultError,
    RetainedToolResult, ReviewDiffChunkBatch, ReviewDiffManifest, Role, RoleContractError, RoleRun,
    RoleTranscript, RoleTranscriptError, RuntimeActionRequest, RuntimeError, TaskBudgetLedger,
    TerminalSnapshot, ToolResult, ValidationObservation, WorkspaceCheckpoint, WorkspaceFingerprint,
    project_test_snapshot, validate_action_batch, validate_role_response,
};

pub const PLANNER_SYSTEM_POLICY: &str = "You are Planner #1. Inspect the repository with list_files, read_file, and search_text only. Submit exactly one structured plan with submit_plan, or report a controlled blocker with report_blocked. Never request Git, Cargo, diff coverage, or a write action. A normal assistant final answer is not a valid role terminal.";
pub const EXECUTOR_SYSTEM_POLICY: &str = "You are Executor. Follow the validated Planner plan in the supplied task worktree. You may inspect, edit, run the supplied required Cargo checks, inspect Git status/diff, and update only existing plan-step statuses. A normal assistant final answer is invalid. End only with submit_execution after every step and current required check is complete, or report_blocked.";
pub const REVIEWER_SYSTEM_POLICY: &str = "You are Reviewer. Independently inspect the bounded task handoff and current worktree. You may inspect files, Git status/diff, and run optional Cargo checks, but you cannot edit files or update the plan. End only with submit_review using approved or changes_requested, or report_blocked. Approval requires current passed checks and the complete authoritative diff coverage forced by the system. A normal assistant final answer is invalid.";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleRuntimeResult {
    Tool(ToolResult),
    Validation(ValidationObservation),
    ReviewDiffManifest(ReviewDiffManifest),
    ReviewDiffChunks(ReviewDiffChunkBatch),
}

/// Runtime-neutral, typed execution boundary used by the reusable role loop.
#[async_trait::async_trait]
pub trait RoleActionRuntime: Send + Sync + 'static {
    async fn invoke(
        &self,
        request: RuntimeActionRequest,
        cancellation: CancellationToken,
    ) -> Result<RoleRuntimeResult, RuntimeError>;

    async fn workspace_fingerprint(
        &self,
        cancellation: CancellationToken,
    ) -> Result<WorkspaceFingerprint, RuntimeError>;

    async fn terminal_snapshot(
        &self,
        generation: u64,
        cancellation: CancellationToken,
    ) -> Result<TerminalSnapshot, RuntimeError>;

    async fn terminal_review_diff_manifest(
        &self,
        checkpoint: crate::ReviewDiffCheckpoint,
        cancellation: CancellationToken,
    ) -> Result<ReviewDiffManifest, RuntimeError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleActivityEvent {
    role: Role,
    role_run: u32,
    message: String,
}

impl RoleActivityEvent {
    pub fn try_new(
        role: Role,
        role_run: u32,
        message: impl Into<String>,
    ) -> Result<Self, RoleLoopError> {
        RoleRun::try_new(role, role_run).map_err(RoleLoopError::Budget)?;
        let message = message.into();
        if message.is_empty() || message.len() > 4_096 {
            return Err(RoleLoopError::InvalidPlannerPlan);
        }
        Ok(Self {
            role,
            role_run,
            message,
        })
    }

    pub const fn role(&self) -> Role {
        self.role
    }

    pub const fn role_run(&self) -> u32 {
        self.role_run
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleEvent {
    Activity(RoleActivityEvent),
    Diff(DiffEvent),
    Tests(TestSnapshot),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DurableRoleEvent {
    StructuredPlan(PlanSnapshot),
    PlanUpdated(PlanSnapshot),
    IntermediateReview {
        evidence: NewReviewEvidence,
        after_checkpoint_sequence: u64,
    },
}

/// Trusted acknowledgement returned only after the event sink's durable
/// boundary. Ordinary `emit` has no way to construct or imply this proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableEventAck {
    sequence: u64,
}

impl DurableEventAck {
    pub fn try_new(sequence: u64) -> Result<Self, RuntimeError> {
        if sequence == 0 {
            return Err(RuntimeError::new(
                "INVALID_DURABLE_EVENT_ACK",
                "durable event acknowledgement sequence must be positive",
                false,
            ));
        }
        Ok(Self { sequence })
    }

    pub const fn sequence(self) -> u64 {
        self.sequence
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableCheckpointAck {
    sequence: u64,
    generation: u64,
}

impl DurableCheckpointAck {
    pub fn try_new(sequence: u64, generation: u64) -> Result<Self, RuntimeError> {
        if sequence == 0 || generation > coding_agent_domain::MAX_WORKSPACE_GENERATION {
            return Err(RuntimeError::new(
                "INVALID_DURABLE_CHECKPOINT_ACK",
                "durable checkpoint acknowledgement is invalid",
                false,
            ));
        }
        Ok(Self {
            sequence,
            generation,
        })
    }

    pub const fn sequence(self) -> u64 {
        self.sequence
    }

    pub const fn generation(self) -> u64 {
        self.generation
    }
}

/// Project 3 event port. There is deliberately no default/no-op durable
/// implementation: app composition must provide the real store barrier.
#[async_trait::async_trait]
pub trait RoleEventSink: Send + Sync + 'static {
    async fn emit(
        &self,
        event: RoleEvent,
        cancellation: CancellationToken,
    ) -> Result<(), RuntimeError>;

    async fn emit_durable(
        &self,
        event: DurableRoleEvent,
        cancellation: CancellationToken,
    ) -> Result<DurableEventAck, RuntimeError>;

    async fn flush_checkpoint(
        &self,
        generation: u64,
        cancellation: CancellationToken,
    ) -> Result<DurableCheckpointAck, RuntimeError>;
}

/// Reusable Project 3 role execution boundary.
///
/// Role-specific policy wrappers own prompt/handoff construction and terminal
/// interpretation. This engine owns the shared staged-provider exchange,
/// ordered preflighted runtime dispatch, canonical result retention,
/// transcript append, cancellation mapping, and durable event barriers used by
/// Planner, Executor, and Reviewer alike.
#[derive(Clone)]
pub struct RoleEngine {
    provider: Arc<dyn PreparedModelProvider>,
    runtime: Arc<dyn RoleActionRuntime>,
    events: Arc<dyn RoleEventSink>,
    redactor: Arc<dyn ContextRedactor>,
}

#[derive(Debug)]
struct RequiredRuntimeActionFailure {
    error: RoleLoopError,
    aborted: Option<crate::AbortedRequiredRuntimeAction>,
}

impl RequiredRuntimeActionFailure {
    fn plain(error: impl Into<RoleLoopError>) -> Self {
        Self {
            error: error.into(),
            aborted: None,
        }
    }

    fn aborted(error: RoleLoopError, aborted: crate::AbortedRequiredRuntimeAction) -> Self {
        Self {
            error,
            aborted: Some(aborted),
        }
    }

    fn into_error(self) -> RoleLoopError {
        self.error
    }

    fn into_parts(self) -> (RoleLoopError, Option<crate::AbortedRequiredRuntimeAction>) {
        (self.error, self.aborted)
    }
}

impl From<RoleLoopError> for RequiredRuntimeActionFailure {
    fn from(error: RoleLoopError) -> Self {
        Self::plain(error)
    }
}

impl From<BudgetError> for RequiredRuntimeActionFailure {
    fn from(error: BudgetError) -> Self {
        Self::plain(error)
    }
}

impl From<RoleContractError> for RequiredRuntimeActionFailure {
    fn from(error: RoleContractError) -> Self {
        Self::plain(error)
    }
}

impl From<RetainedResultError> for RequiredRuntimeActionFailure {
    fn from(error: RetainedResultError) -> Self {
        Self::plain(error)
    }
}

impl From<RoleTranscriptError> for RequiredRuntimeActionFailure {
    fn from(error: RoleTranscriptError) -> Self {
        Self::plain(error)
    }
}

impl RoleEngine {
    pub fn new(
        provider: Arc<dyn PreparedModelProvider>,
        runtime: Arc<dyn RoleActionRuntime>,
        events: Arc<dyn RoleEventSink>,
        redactor: Arc<dyn ContextRedactor>,
    ) -> Self {
        Self {
            provider,
            runtime,
            events,
            redactor,
        }
    }

    pub fn redactor(&self) -> &dyn ContextRedactor {
        self.redactor.as_ref()
    }

    pub async fn emit(
        &self,
        event: RoleEvent,
        cancellation: CancellationToken,
    ) -> Result<(), RoleLoopError> {
        if cancellation.is_cancelled() {
            return Err(RoleLoopError::Cancelled);
        }
        self.events
            .emit(event, cancellation.clone())
            .await
            .map_err(map_runtime_error)?;
        if cancellation.is_cancelled() {
            return Err(RoleLoopError::Cancelled);
        }
        Ok(())
    }

    pub async fn emit_durable(
        &self,
        event: DurableRoleEvent,
        cancellation: CancellationToken,
    ) -> Result<DurableEventAck, RoleLoopError> {
        if cancellation.is_cancelled() {
            return Err(RoleLoopError::Cancelled);
        }
        self.events
            .emit_durable(event, cancellation.clone())
            .await
            .map_err(map_runtime_error)
            .and_then(|ack| {
                if cancellation.is_cancelled() {
                    Err(RoleLoopError::Cancelled)
                } else {
                    Ok(ack)
                }
            })
    }

    pub async fn flush_checkpoint(
        &self,
        generation: u64,
        cancellation: CancellationToken,
    ) -> Result<DurableCheckpointAck, RoleLoopError> {
        if cancellation.is_cancelled() {
            return Err(RoleLoopError::Cancelled);
        }
        let ack = self
            .events
            .flush_checkpoint(generation, cancellation.clone())
            .await
            .map_err(map_runtime_error)?;
        if cancellation.is_cancelled() {
            return Err(RoleLoopError::Cancelled);
        }
        if ack.generation() != generation {
            return Err(RoleLoopError::QualityEvidenceMismatch);
        }
        Ok(ack)
    }

    pub async fn exploratory_exchange(
        &self,
        transcript: &RoleTranscript,
        ledger: &mut TaskBudgetLedger,
        lease: &mut crate::RoleBudgetLease,
        cancellation: CancellationToken,
    ) -> Result<(crate::ProviderResponseReceipt, ModelResponse), RoleLoopError> {
        if cancellation.is_cancelled() {
            return Err(RoleLoopError::Cancelled);
        }
        ensure_transcript_matches_lease(transcript, lease)?;
        let request = transcript.request(ModelToolChoice::Auto);
        let prepared = self.provider.prepare(request)?;
        let request_permit =
            ledger.begin_exploratory_prepared_provider_request(lease, prepared.as_ref())?;
        let raw = match prepared.send(cancellation.clone()).await {
            Ok(raw) => raw,
            Err(error) => {
                ledger.record_transport_no_response(lease, request_permit)?;
                if cancellation.is_cancelled() {
                    return Err(RoleLoopError::Cancelled);
                }
                return Err(map_provider_error(error));
            }
        };
        let receipt =
            ledger.record_exploratory_raw_provider_response(lease, request_permit, raw.as_ref())?;
        if receipt.violation().is_some() {
            ledger.discard_invalid_provider_response(lease, &receipt)?;
            return Err(BudgetError::ProviderResponseLimitViolation.into());
        }
        if cancellation.is_cancelled() {
            ledger.discard_invalid_provider_response(lease, &receipt)?;
            return Err(RoleLoopError::Cancelled);
        }
        match raw.decode() {
            Ok(response) => Ok((receipt, response)),
            Err(error) => {
                ledger.discard_invalid_provider_response(lease, &receipt)?;
                Err(map_provider_error(error))
            }
        }
    }

    pub async fn required_exchange(
        &self,
        transcript: &RoleTranscript,
        required: RequiredAction,
        ledger: &mut TaskBudgetLedger,
        lease: &mut crate::RoleBudgetLease,
        action: &mut crate::RequiredActionPermit,
        cancellation: CancellationToken,
    ) -> Result<(crate::ProviderResponseReceipt, ModelResponse), RoleLoopError> {
        if cancellation.is_cancelled() {
            return Err(RoleLoopError::Cancelled);
        }
        ensure_transcript_matches_lease(transcript, lease)?;
        if !action.permits_required_action(&required) {
            return Err(RoleContractError::InvalidRequiredAction.into());
        }
        let request = transcript.request(ModelToolChoice::Required(required));
        let prepared = self.provider.prepare(request)?;
        let request_permit =
            ledger.begin_required_prepared_provider_request(lease, action, prepared.as_ref())?;
        let raw = match prepared.send(cancellation.clone()).await {
            Ok(raw) => raw,
            Err(error) => {
                ledger.record_transport_no_response(lease, request_permit)?;
                if cancellation.is_cancelled() {
                    return Err(RoleLoopError::Cancelled);
                }
                return Err(map_provider_error(error));
            }
        };
        let receipt = ledger.record_required_raw_provider_response(
            lease,
            action,
            request_permit,
            raw.as_ref(),
        )?;
        if receipt.violation().is_some() {
            ledger.discard_invalid_provider_response(lease, &receipt)?;
            return Err(BudgetError::ProviderResponseLimitViolation.into());
        }
        if cancellation.is_cancelled() {
            ledger.discard_invalid_provider_response(lease, &receipt)?;
            return Err(RoleLoopError::Cancelled);
        }
        match raw.decode() {
            Ok(response) => Ok((receipt, response)),
            Err(error) => {
                ledger.discard_invalid_provider_response(lease, &receipt)?;
                Err(map_provider_error(error))
            }
        }
    }

    pub async fn invoke_runtime(
        &self,
        request: RuntimeActionRequest,
        cancellation: CancellationToken,
    ) -> Result<RoleRuntimeResult, RoleLoopError> {
        if cancellation.is_cancelled() {
            return Err(RoleLoopError::Cancelled);
        }
        let result = self
            .runtime
            .invoke(request, cancellation.clone())
            .await
            .map_err(map_runtime_error);
        if cancellation.is_cancelled() {
            return Err(RoleLoopError::Cancelled);
        }
        result
    }

    /// Executes one exact non-terminal required runtime action and closes its
    /// provider response. Validation observations and authoritative review
    /// manifest/chunk values remain typed on return; the same canonical
    /// retained wrapper is charged and appended before the required slot is
    /// completed.
    #[allow(clippy::too_many_arguments)]
    pub async fn execute_required_runtime_action(
        &self,
        transcript: &mut RoleTranscript,
        batch: crate::ToolCallBatch,
        required: &RequiredAction,
        action: crate::RequiredActionPermit,
        ledger: &mut TaskBudgetLedger,
        lease: &mut crate::RoleBudgetLease,
        receipt: &mut crate::ProviderResponseReceipt,
        cancellation: CancellationToken,
    ) -> Result<RoleRuntimeResult, RoleLoopError> {
        self.execute_required_runtime_action_with_failure(
            transcript,
            batch,
            required,
            action,
            ledger,
            lease,
            receipt,
            cancellation,
        )
        .await
        .map_err(RequiredRuntimeActionFailure::into_error)
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_required_runtime_action_with_failure(
        &self,
        transcript: &mut RoleTranscript,
        batch: crate::ToolCallBatch,
        required: &RequiredAction,
        mut action: crate::RequiredActionPermit,
        ledger: &mut TaskBudgetLedger,
        lease: &mut crate::RoleBudgetLease,
        receipt: &mut crate::ProviderResponseReceipt,
        cancellation: CancellationToken,
    ) -> Result<RoleRuntimeResult, RequiredRuntimeActionFailure> {
        if ensure_transcript_matches_lease(transcript, lease).is_err()
            || !action.permits_required_action(required)
            || validate_action_batch(
                transcript.owner().role(),
                &batch,
                &ModelToolChoice::Required(required.clone()),
                self.redactor.as_ref(),
            )
            .is_err()
            || transcript.preflight_runtime_batch(&batch).is_err()
        {
            ledger.discard_invalid_provider_response(lease, receipt)?;
            return Err(RoleContractError::InvalidBatch.into());
        }
        let [call] = batch.calls.as_slice() else {
            ledger.discard_invalid_provider_response(lease, receipt)?;
            return Err(RoleContractError::InvalidBatch.into());
        };
        let ActionRequest::Runtime(request) = &call.request else {
            ledger.discard_invalid_provider_response(lease, receipt)?;
            return Err(RoleContractError::ControlRuntimeBoundary.into());
        };
        if !required.matches(&call.request) {
            ledger.discard_invalid_provider_response(lease, receipt)?;
            return Err(RoleContractError::InvalidRequiredAction.into());
        }

        if let Err(error) = ledger.charge_required_call(lease, &mut action, receipt) {
            ledger.discard_invalid_provider_response(lease, receipt)?;
            return Err(error.into());
        }
        let runtime_result = match self
            .invoke_runtime(request.clone(), cancellation.clone())
            .await
        {
            Ok(result) => result,
            Err(error) => {
                let aborted = ledger.abort_required_runtime_action(lease, action, receipt)?;
                return Err(RequiredRuntimeActionFailure::aborted(error, aborted));
            }
        };
        if cancellation.is_cancelled() {
            ledger.abort_required_runtime_result(lease, &action, receipt)?;
            ledger.finish_provider_response(lease, receipt)?;
            return Err(RoleLoopError::Cancelled.into());
        }
        let retained = match (request, &runtime_result) {
            (
                RuntimeActionRequest::Validation { check },
                RoleRuntimeResult::Validation(observation),
            ) if observation.check() == check => {
                RetainedToolResult::try_from_tool_result_with_limit(
                    &call.id,
                    observation.model_result(),
                    self.redactor.as_ref(),
                    crate::VALIDATION_RETAINED_RESULT_LIMIT,
                )
            }
            (
                RuntimeActionRequest::ReviewDiffManifest {
                    generation,
                    workspace_digest,
                },
                RoleRuntimeResult::ReviewDiffManifest(manifest),
            ) if manifest.generation() == *generation
                && manifest.workspace_digest() == workspace_digest =>
            {
                RetainedToolResult::try_review_manifest(&call.id, manifest, self.redactor.as_ref())
            }
            (
                RuntimeActionRequest::ReviewDiffChunks {
                    generation,
                    workspace_digest,
                    manifest_sha256,
                    start_chunk,
                    count,
                },
                RoleRuntimeResult::ReviewDiffChunks(chunks),
            ) if chunks.generation() == *generation
                && chunks.workspace_digest() == workspace_digest
                && chunks.manifest_sha256() == manifest_sha256
                && chunks.start_chunk() == *start_chunk
                && chunks.chunks().len() == usize::from(*count) =>
            {
                RetainedToolResult::try_review_chunk_batch(&call.id, chunks, self.redactor.as_ref())
            }
            _ => {
                ledger.abort_required_runtime_result(lease, &action, receipt)?;
                ledger.finish_provider_response(lease, receipt)?;
                return Err(RoleLoopError::RuntimeResultMismatch.into());
            }
        };
        let retained = match retained {
            Ok(retained) => retained,
            Err(error) => {
                ledger.abort_required_runtime_result(lease, &action, receipt)?;
                ledger.finish_provider_response(lease, receipt)?;
                return Err(error.into());
            }
        };
        if let Err(error) = ledger.retain_required_tool_result(lease, &mut action, &retained) {
            ledger.abort_required_runtime_result(lease, &action, receipt)?;
            ledger.finish_provider_response(lease, receipt)?;
            return Err(error.into());
        }
        if let RoleRuntimeResult::ReviewDiffManifest(manifest) = &runtime_result {
            let manifest_receipt = match ledger.observe_typed_reviewer_manifest(
                lease,
                &action,
                manifest.chunk_count(),
            ) {
                Ok(receipt) => receipt,
                Err(error) => {
                    ledger.finish_provider_response(lease, receipt)?;
                    return Err(error.into());
                }
            };
            if let Err(error) = ledger.bind_reviewer_manifest(lease, &mut action, manifest_receipt)
            {
                ledger.finish_provider_response(lease, receipt)?;
                return Err(error.into());
            }
        }
        if let Err(error) = transcript.append_runtime_batch(batch, vec![retained]) {
            ledger.finish_provider_response(lease, receipt)?;
            return Err(error.into());
        }
        ledger.finish_provider_response(lease, receipt)?;
        ledger.complete_required_action(lease, action)?;
        Ok(runtime_result)
    }

    /// Executes a complete budget-preflighted runtime batch in provider order.
    ///
    /// Every exit closes the provider response. Runtime/result/transcript
    /// failures release the whole-batch permit before returning; no prefix of
    /// results is retained.
    #[allow(clippy::too_many_arguments)]
    pub async fn execute_preflighted_runtime_batch(
        &self,
        transcript: &mut RoleTranscript,
        batch: crate::ToolCallBatch,
        permit: crate::ExploratoryBatchPermit,
        ledger: &mut TaskBudgetLedger,
        lease: &mut crate::RoleBudgetLease,
        receipt: &crate::ProviderResponseReceipt,
        cancellation: CancellationToken,
    ) -> Result<Vec<RoleRuntimeResult>, RoleLoopError> {
        if ensure_transcript_matches_lease(transcript, lease).is_err()
            || validate_action_batch(
                transcript.owner().role(),
                &batch,
                &ModelToolChoice::Auto,
                self.redactor.as_ref(),
            )
            .is_err()
        {
            ledger.abort_exploratory_runtime_batch(lease, &permit)?;
            ledger.finish_provider_response(lease, receipt)?;
            return Err(RoleContractError::InvalidBatch.into());
        }
        if let Err(error) = transcript.preflight_runtime_batch(&batch) {
            ledger.abort_exploratory_runtime_batch(lease, &permit)?;
            ledger.finish_provider_response(lease, receipt)?;
            return Err(error.into());
        }
        if batch.calls.len() != permit.invocations().len()
            || batch
                .calls
                .iter()
                .zip(permit.invocations())
                .any(|(call, invocation)| {
                    call.id != invocation.tool_call_id()
                        || call.request != ActionRequest::Runtime(invocation.request().clone())
                })
        {
            ledger.abort_exploratory_runtime_batch(lease, &permit)?;
            ledger.finish_provider_response(lease, receipt)?;
            return Err(RoleContractError::InvalidBatch.into());
        }
        let mut results = Vec::with_capacity(permit.invocations().len());
        let mut typed_results = Vec::with_capacity(permit.invocations().len());
        for invocation in permit.invocations() {
            if cancellation.is_cancelled() {
                ledger.abort_exploratory_runtime_batch(lease, &permit)?;
                ledger.finish_provider_response(lease, receipt)?;
                return Err(RoleLoopError::Cancelled);
            }
            let runtime_result = match self
                .invoke_runtime(invocation.request().clone(), cancellation.clone())
                .await
            {
                Ok(result) => result,
                Err(error) => {
                    ledger.abort_exploratory_runtime_batch(lease, &permit)?;
                    ledger.finish_provider_response(lease, receipt)?;
                    return Err(error);
                }
            };
            if cancellation.is_cancelled() {
                ledger.abort_exploratory_runtime_batch(lease, &permit)?;
                ledger.finish_provider_response(lease, receipt)?;
                return Err(RoleLoopError::Cancelled);
            }
            let tool_result = match (invocation.request(), &runtime_result) {
                (RuntimeActionRequest::Tool(_), RoleRuntimeResult::Tool(result)) => result,
                (
                    RuntimeActionRequest::Validation { check },
                    RoleRuntimeResult::Validation(observation),
                ) if observation.check() == check => observation.model_result(),
                (
                    RuntimeActionRequest::ValidationSelector { selector },
                    RoleRuntimeResult::Validation(observation),
                ) if observation.check().selector() == selector => observation.model_result(),
                _ => {
                    ledger.abort_exploratory_runtime_batch(lease, &permit)?;
                    ledger.finish_provider_response(lease, receipt)?;
                    return Err(RoleLoopError::RuntimeResultMismatch);
                }
            };
            let result = match RetainedToolResult::try_from_tool_result_with_limit(
                invocation.tool_call_id(),
                tool_result,
                self.redactor.as_ref(),
                invocation.wrapper_cap(),
            ) {
                Ok(result) => result,
                Err(error) => {
                    ledger.abort_exploratory_runtime_batch(lease, &permit)?;
                    ledger.finish_provider_response(lease, receipt)?;
                    return Err(error.into());
                }
            };
            results.push(result);
            typed_results.push(runtime_result);
        }
        if let Err(error) = ledger.retain_exploratory_batch_results(lease, &permit, &results) {
            ledger.abort_exploratory_runtime_batch(lease, &permit)?;
            ledger.finish_provider_response(lease, receipt)?;
            return Err(error.into());
        }
        if let Err(error) = transcript.append_runtime_batch(batch, results) {
            ledger.finish_provider_response(lease, receipt)?;
            return Err(error.into());
        }
        ledger.finish_provider_response(lease, receipt)?;
        Ok(typed_results)
    }
}

impl std::fmt::Debug for RoleEngine {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RoleEngine")
            .field("provider", &"<retained>")
            .field("runtime", &"<retained>")
            .field("events", &"<retained>")
            .field("redactor", &"<retained>")
            .finish()
    }
}

pub struct PlannerRoleInput<'a> {
    pub task_prompt: &'a str,
    pub repository_context: &'a str,
    pub checkpoint: &'a WorkspaceCheckpoint,
    pub repository_check_catalog: &'a [RequiredCheckSelector],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedPlannerPlan {
    plan: PlanSnapshot,
    required_checks: Vec<RequiredCheck>,
    durable_sequence: u64,
}

impl ValidatedPlannerPlan {
    pub const fn plan(&self) -> &PlanSnapshot {
        &self.plan
    }

    pub fn required_checks(&self) -> &[RequiredCheck] {
        &self.required_checks
    }

    pub const fn durable_sequence(&self) -> u64 {
        self.durable_sequence
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlannerRoleOutcome {
    Submitted(ValidatedPlannerPlan),
    Blocked(BlockedSubmission),
}

#[derive(Debug, Error)]
pub enum RoleLoopError {
    #[error("the role was cancelled")]
    Cancelled,
    #[error(transparent)]
    Provider(#[from] ProviderError),
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error(transparent)]
    Budget(#[from] BudgetError),
    #[error(transparent)]
    Contract(#[from] RoleContractError),
    #[error(transparent)]
    Transcript(#[from] RoleTranscriptError),
    #[error(transparent)]
    Retained(#[from] RetainedResultError),
    #[error(transparent)]
    Quality(#[from] crate::QualityStateError),
    #[error("the Planner submission does not match the trusted repository catalog")]
    InvalidPlannerPlan,
    #[error("the Planner requested an action outside its capability set")]
    PlannerActionNotAllowed,
    #[error("the Executor response is not a valid current execution submission")]
    InvalidExecutorOutput,
    #[error("the Executor requested an action outside its capability set")]
    ExecutorActionNotAllowed,
    #[error("the Reviewer response is not a valid current review submission")]
    InvalidReviewerOutput,
    #[error("the Reviewer requested an action outside its capability set")]
    ReviewerActionNotAllowed,
    #[error("the typed quality observation does not match the current workspace")]
    QualityEvidenceMismatch,
    #[error("the terminal diff was truncated")]
    TerminalDiffTruncated,
    #[error("the role runtime returned a result for another action class")]
    RuntimeResultMismatch,
}

impl RoleLoopError {
    /// Stable Task failure code for a Planner-stage error. Cancellation is a
    /// lifecycle outcome and therefore intentionally has no failure code.
    pub fn planner_failure_code(&self) -> Option<&'static str> {
        match self {
            Self::Cancelled => None,
            Self::Contract(RoleContractError::RedactionMutation)
            | Self::Transcript(RoleTranscriptError::RedactionUnstable)
            | Self::Retained(RetainedResultError::RedactionUnstable) => {
                Some("PROVIDER_SECRET_DETECTED")
            }
            Self::InvalidPlannerPlan => Some("PLANNER_INVALID_OUTPUT"),
            Self::Provider(error) if error.code == crate::PROVIDER_RESPONSE_ROLE_OUTPUT_INVALID => {
                Some("PLANNER_INVALID_OUTPUT")
            }
            Self::PlannerActionNotAllowed
            | Self::Contract(_)
            | Self::Transcript(RoleTranscriptError::InvalidToolBatch) => {
                Some("PLANNER_ACTION_NOT_ALLOWED")
            }
            Self::Provider(error)
                if error.code == crate::PROVIDER_RESPONSE_ROLE_ACTION_NOT_ALLOWED =>
            {
                Some("PLANNER_ACTION_NOT_ALLOWED")
            }
            Self::Budget(BudgetError::RoleLimitExceeded { .. }) => {
                Some("PLANNER_STEP_LIMIT_REACHED")
            }
            Self::Budget(
                BudgetError::ProviderRequestTooLarge { .. }
                | BudgetError::ProviderResponseTooLarge { .. }
                | BudgetError::ProviderResponseExceedsReservation { .. }
                | BudgetError::ProviderResponseLimitViolation,
            )
            | Self::Transcript(RoleTranscriptError::HandoffTooLarge) => {
                Some("PLANNER_CONTEXT_LIMIT_REACHED")
            }
            Self::Budget(_) => Some("PLANNER_TASK_BUDGET_EXHAUSTED"),
            Self::Provider(_) => Some("PLANNER_PROVIDER_FAILED"),
            Self::Runtime(error) if error.code == "COMMAND_TIMED_OUT" => Some("PLANNER_TIMEOUT"),
            Self::Runtime(_) | Self::RuntimeResultMismatch | Self::Retained(_) => {
                Some("PLANNER_RUNTIME_FAILED")
            }
            Self::Quality(crate::QualityStateError::WorkspaceGenerationOverflow) => {
                Some("WORKSPACE_GENERATION_EXHAUSTED")
            }
            Self::TerminalDiffTruncated => Some("TERMINAL_DIFF_TRUNCATED"),
            Self::Quality(_) | Self::QualityEvidenceMismatch => Some("QUALITY_EVIDENCE_MISMATCH"),
            Self::Transcript(_) => Some("PLANNER_CONTEXT_LIMIT_REACHED"),
            Self::InvalidExecutorOutput | Self::InvalidReviewerOutput => {
                Some("PLANNER_INVALID_OUTPUT")
            }
            Self::ExecutorActionNotAllowed | Self::ReviewerActionNotAllowed => {
                Some("PLANNER_ACTION_NOT_ALLOWED")
            }
        }
    }

    /// Stable Task failure code for an Executor-stage error. Cancellation is a
    /// lifecycle outcome and therefore intentionally has no failure code.
    pub fn executor_failure_code(&self) -> Option<&'static str> {
        match self {
            Self::Cancelled => None,
            Self::Contract(RoleContractError::RedactionMutation)
            | Self::Transcript(RoleTranscriptError::RedactionUnstable)
            | Self::Retained(RetainedResultError::RedactionUnstable) => {
                Some("PROVIDER_SECRET_DETECTED")
            }
            Self::InvalidExecutorOutput => Some("EXECUTOR_INVALID_OUTPUT"),
            Self::Provider(error) if error.code == crate::PROVIDER_RESPONSE_ROLE_OUTPUT_INVALID => {
                Some("EXECUTOR_INVALID_OUTPUT")
            }
            Self::ExecutorActionNotAllowed
            | Self::Contract(_)
            | Self::Transcript(RoleTranscriptError::InvalidToolBatch) => {
                Some("EXECUTOR_ACTION_NOT_ALLOWED")
            }
            Self::Provider(error)
                if error.code == crate::PROVIDER_RESPONSE_ROLE_ACTION_NOT_ALLOWED =>
            {
                Some("EXECUTOR_ACTION_NOT_ALLOWED")
            }
            Self::Budget(BudgetError::RoleLimitExceeded { .. }) => {
                Some("EXECUTOR_STEP_LIMIT_REACHED")
            }
            Self::Budget(
                BudgetError::ProviderRequestTooLarge { .. }
                | BudgetError::ProviderResponseTooLarge { .. }
                | BudgetError::ProviderResponseExceedsReservation { .. }
                | BudgetError::ProviderResponseLimitViolation,
            )
            | Self::Transcript(RoleTranscriptError::HandoffTooLarge) => {
                Some("EXECUTOR_CONTEXT_LIMIT_REACHED")
            }
            Self::Budget(_) => Some("EXECUTOR_TASK_BUDGET_EXHAUSTED"),
            Self::Provider(_) => Some("EXECUTOR_PROVIDER_FAILED"),
            Self::Runtime(error) if error.code == "COMMAND_TIMED_OUT" => Some("EXECUTOR_TIMEOUT"),
            Self::Runtime(_) | Self::RuntimeResultMismatch | Self::Retained(_) => {
                Some("EXECUTOR_RUNTIME_FAILED")
            }
            Self::Quality(crate::QualityStateError::WorkspaceGenerationOverflow) => {
                Some("WORKSPACE_GENERATION_EXHAUSTED")
            }
            Self::TerminalDiffTruncated => Some("TERMINAL_DIFF_TRUNCATED"),
            Self::Quality(_) | Self::QualityEvidenceMismatch => Some("QUALITY_EVIDENCE_MISMATCH"),
            Self::Transcript(_) => Some("EXECUTOR_CONTEXT_LIMIT_REACHED"),
            Self::InvalidPlannerPlan | Self::InvalidReviewerOutput => {
                Some("EXECUTOR_INVALID_OUTPUT")
            }
            Self::PlannerActionNotAllowed | Self::ReviewerActionNotAllowed => {
                Some("EXECUTOR_ACTION_NOT_ALLOWED")
            }
        }
    }

    /// Stable Task failure code for a Reviewer-stage error. A valid
    /// `changes_requested` decision is an outcome, never an error code.
    pub fn reviewer_failure_code(&self) -> Option<&'static str> {
        match self {
            Self::Cancelled => None,
            Self::Contract(RoleContractError::RedactionMutation)
            | Self::Transcript(RoleTranscriptError::RedactionUnstable)
            | Self::Retained(RetainedResultError::RedactionUnstable) => {
                Some("PROVIDER_SECRET_DETECTED")
            }
            Self::InvalidReviewerOutput => Some("REVIEWER_INVALID_OUTPUT"),
            Self::Provider(error) if error.code == crate::PROVIDER_RESPONSE_ROLE_OUTPUT_INVALID => {
                Some("REVIEWER_INVALID_OUTPUT")
            }
            Self::ReviewerActionNotAllowed
            | Self::Contract(_)
            | Self::Transcript(RoleTranscriptError::InvalidToolBatch) => {
                Some("REVIEWER_ACTION_NOT_ALLOWED")
            }
            Self::Provider(error)
                if error.code == crate::PROVIDER_RESPONSE_ROLE_ACTION_NOT_ALLOWED =>
            {
                Some("REVIEWER_ACTION_NOT_ALLOWED")
            }
            Self::Budget(BudgetError::RoleLimitExceeded { .. }) => {
                Some("REVIEWER_STEP_LIMIT_REACHED")
            }
            Self::Budget(
                BudgetError::ProviderRequestTooLarge { .. }
                | BudgetError::ProviderResponseTooLarge { .. }
                | BudgetError::ProviderResponseExceedsReservation { .. }
                | BudgetError::ProviderResponseLimitViolation,
            )
            | Self::Transcript(RoleTranscriptError::HandoffTooLarge) => {
                Some("REVIEWER_CONTEXT_LIMIT_REACHED")
            }
            Self::Budget(_) => Some("REVIEWER_TASK_BUDGET_EXHAUSTED"),
            Self::Provider(_) => Some("REVIEWER_PROVIDER_FAILED"),
            Self::Runtime(error) if error.code == "COMMAND_TIMED_OUT" => Some("REVIEWER_TIMEOUT"),
            Self::Runtime(error) if error.code == "REVIEW_DIFF_COVERAGE_LIMIT" => {
                Some("REVIEW_DIFF_COVERAGE_LIMIT")
            }
            Self::Runtime(_) | Self::RuntimeResultMismatch | Self::Retained(_) => {
                Some("REVIEWER_RUNTIME_FAILED")
            }
            Self::Quality(crate::QualityStateError::WorkspaceGenerationOverflow) => {
                Some("WORKSPACE_GENERATION_EXHAUSTED")
            }
            Self::TerminalDiffTruncated => Some("TERMINAL_DIFF_TRUNCATED"),
            Self::Quality(_) | Self::QualityEvidenceMismatch => Some("QUALITY_EVIDENCE_MISMATCH"),
            Self::Transcript(_) => Some("REVIEWER_CONTEXT_LIMIT_REACHED"),
            Self::InvalidPlannerPlan | Self::InvalidExecutorOutput => {
                Some("REVIEWER_INVALID_OUTPUT")
            }
            Self::PlannerActionNotAllowed | Self::ExecutorActionNotAllowed => {
                Some("REVIEWER_ACTION_NOT_ALLOWED")
            }
        }
    }
}

pub struct PlannerRoleLoop {
    engine: RoleEngine,
}

impl PlannerRoleLoop {
    pub fn new(
        provider: Arc<dyn PreparedModelProvider>,
        runtime: Arc<dyn RoleActionRuntime>,
        events: Arc<dyn RoleEventSink>,
        redactor: Arc<dyn ContextRedactor>,
    ) -> Self {
        Self::from_engine(RoleEngine::new(provider, runtime, events, redactor))
    }

    pub const fn from_engine(engine: RoleEngine) -> Self {
        Self { engine }
    }

    pub const fn engine(&self) -> &RoleEngine {
        &self.engine
    }

    pub async fn run(
        &self,
        input: PlannerRoleInput<'_>,
        ledger: &mut TaskBudgetLedger,
        cancellation: CancellationToken,
    ) -> Result<PlannerRoleOutcome, RoleLoopError> {
        if cancellation.is_cancelled() {
            return Err(RoleLoopError::Cancelled);
        }
        let mut lease = ledger.start_planner()?;
        let result = self
            .run_active(input, ledger, &mut lease, cancellation.clone())
            .await;
        match result {
            Ok(outcome) => {
                ledger.finish_role(lease)?;
                Ok(outcome)
            }
            Err(error) => {
                // Every provider path below closes its pending/open exchange
                // before returning. Release the failed role so the shared
                // task ledger cannot retain a stale active lease.
                ledger.abort_role_on_failure(lease)?;
                Err(error)
            }
        }
    }

    async fn run_active(
        &self,
        input: PlannerRoleInput<'_>,
        ledger: &mut TaskBudgetLedger,
        lease: &mut crate::RoleBudgetLease,
        cancellation: CancellationToken,
    ) -> Result<PlannerRoleOutcome, RoleLoopError> {
        let owner = RoleRun::try_new(Role::Planner, 1)?;
        let handoff = PlannerHandoff::try_new(
            input.task_prompt,
            input.repository_context,
            input.checkpoint,
            input.repository_check_catalog,
            self.engine.redactor(),
        )?;
        let mut transcript = RoleTranscript::try_for_planner(
            owner,
            PLANNER_SYSTEM_POLICY,
            handoff,
            self.engine.redactor(),
        )?;
        self.engine
            .emit(
                RoleEvent::Activity(RoleActivityEvent::try_new(
                    Role::Planner,
                    1,
                    "Planner started",
                )?),
                cancellation.clone(),
            )
            .await?;

        loop {
            if cancellation.is_cancelled() {
                return Err(RoleLoopError::Cancelled);
            }
            let exchange = self
                .engine
                .exploratory_exchange(&transcript, ledger, lease, cancellation.clone())
                .await;
            let (mut receipt, response) = match exchange {
                Ok(exchange) => exchange,
                Err(RoleLoopError::Budget(BudgetError::ReservationWouldBeConsumed {
                    resource: BudgetResource::ModelResponses,
                })) => {
                    return self
                        .run_required_planner_terminal(
                            input.repository_check_catalog,
                            &transcript,
                            ledger,
                            lease,
                            cancellation.clone(),
                        )
                        .await;
                }
                Err(error) => return Err(error),
            };

            if let Err(error) = validate_role_response(
                Role::Planner,
                &response,
                &ModelToolChoice::Auto,
                self.engine.redactor(),
            ) {
                ledger.discard_invalid_provider_response(lease, &receipt)?;
                return Err(classify_planner_response_error(&response, error));
            }
            if cancellation.is_cancelled() {
                ledger.discard_invalid_provider_response(lease, &receipt)?;
                return Err(RoleLoopError::Cancelled);
            }
            let ModelResponse::ToolCalls(batch) = response else {
                ledger.discard_invalid_provider_response(lease, &receipt)?;
                return Err(RoleLoopError::InvalidPlannerPlan);
            };
            if let Err(error) = transcript.preflight_runtime_batch(&batch) {
                ledger.discard_invalid_provider_response(lease, &receipt)?;
                return Err(error.into());
            }

            if let [call] = batch.calls.as_slice()
                && let ActionRequest::Control(control) = &call.request
            {
                let control = control.clone();
                return match control {
                    ControlRequest::SubmitPlan(submission) => {
                        let (plan, required_checks) = match validate_planner_submission(
                            &submission,
                            input.repository_check_catalog,
                        ) {
                            Ok(plan) => plan,
                            Err(error) => {
                                ledger.discard_invalid_provider_response(lease, &receipt)?;
                                return Err(error);
                            }
                        };
                        let terminal = match ledger.mint_exploratory_normal_terminal(
                            lease,
                            &receipt,
                            ControlRequest::SubmitPlan(submission),
                        ) {
                            Ok(terminal) => terminal,
                            Err(error) => {
                                ledger.discard_invalid_provider_response(lease, &receipt)?;
                                return Err(error.into());
                            }
                        };
                        let completed = ledger.complete_exploratory_normal_terminal(
                            lease,
                            &mut receipt,
                            terminal,
                        )?;
                        debug_assert!(matches!(completed, ControlRequest::SubmitPlan(_)));
                        let ack = self
                            .engine
                            .emit_durable(
                                DurableRoleEvent::StructuredPlan(plan.clone()),
                                cancellation.clone(),
                            )
                            .await?;
                        Ok(PlannerRoleOutcome::Submitted(ValidatedPlannerPlan {
                            plan,
                            required_checks,
                            durable_sequence: ack.sequence(),
                        }))
                    }
                    ControlRequest::ReportBlocked(blocked) => {
                        ledger.complete_exploratory_early_terminal(
                            lease,
                            &mut receipt,
                            EarlyRoleBudgetTermination::ReportBlocked,
                        )?;
                        Ok(PlannerRoleOutcome::Blocked(blocked))
                    }
                    _ => {
                        ledger.discard_invalid_provider_response(lease, &receipt)?;
                        Err(RoleLoopError::PlannerActionNotAllowed)
                    }
                };
            }

            let permit = match ledger.preflight_exploratory_runtime_batch(
                lease,
                &mut receipt,
                batch.calls.clone(),
            ) {
                Ok(permit) => permit,
                Err(BudgetError::ReservationWouldBeConsumed {
                    resource:
                        BudgetResource::ModelVisibleCalls | BudgetResource::RetainedResultBytes,
                }) => {
                    // The complete rejected Auto batch remains charged as a
                    // received response, but executes nothing and contributes
                    // no transcript history. Converge through the already
                    // reserved Planner terminal instead of failing the task.
                    ledger.discard_invalid_provider_response(lease, &receipt)?;
                    return self
                        .run_required_planner_terminal(
                            input.repository_check_catalog,
                            &transcript,
                            ledger,
                            lease,
                            cancellation.clone(),
                        )
                        .await;
                }
                Err(error) => {
                    ledger.discard_invalid_provider_response(lease, &receipt)?;
                    return Err(error.into());
                }
            };
            let _typed_results = self
                .engine
                .execute_preflighted_runtime_batch(
                    &mut transcript,
                    batch,
                    permit,
                    ledger,
                    lease,
                    &receipt,
                    cancellation.clone(),
                )
                .await?;
            self.engine
                .emit(
                    RoleEvent::Activity(RoleActivityEvent::try_new(
                        Role::Planner,
                        1,
                        "Planner inspection batch completed",
                    )?),
                    cancellation.clone(),
                )
                .await?;
        }
    }

    async fn run_required_planner_terminal(
        &self,
        repository_check_catalog: &[RequiredCheckSelector],
        transcript: &RoleTranscript,
        ledger: &mut TaskBudgetLedger,
        lease: &mut crate::RoleBudgetLease,
        cancellation: CancellationToken,
    ) -> Result<PlannerRoleOutcome, RoleLoopError> {
        let mut action = ledger.begin_required_action(lease)?;
        let required = RequiredAction::terminal_or_blocked(crate::ControlKind::SubmitPlan)?;
        let (mut receipt, response) = self
            .engine
            .required_exchange(
                transcript,
                required.clone(),
                ledger,
                lease,
                &mut action,
                cancellation.clone(),
            )
            .await?;
        if cancellation.is_cancelled() {
            ledger.discard_invalid_provider_response(lease, &receipt)?;
            return Err(RoleLoopError::Cancelled);
        }
        if let Err(error) = validate_role_response(
            Role::Planner,
            &response,
            &ModelToolChoice::Required(required),
            self.engine.redactor(),
        ) {
            ledger.discard_invalid_provider_response(lease, &receipt)?;
            return Err(classify_planner_response_error(&response, error));
        }
        let ModelResponse::ToolCalls(batch) = response else {
            ledger.discard_invalid_provider_response(lease, &receipt)?;
            return Err(RoleLoopError::InvalidPlannerPlan);
        };
        if let Err(error) = transcript.preflight_runtime_batch(&batch) {
            ledger.discard_invalid_provider_response(lease, &receipt)?;
            return Err(error.into());
        }
        let [call] = batch.calls.as_slice() else {
            ledger.discard_invalid_provider_response(lease, &receipt)?;
            return Err(RoleLoopError::PlannerActionNotAllowed);
        };
        let ActionRequest::Control(control) = &call.request else {
            ledger.discard_invalid_provider_response(lease, &receipt)?;
            return Err(RoleLoopError::PlannerActionNotAllowed);
        };
        match control.clone() {
            ControlRequest::SubmitPlan(submission) => {
                let (plan, required_checks) =
                    match validate_planner_submission(&submission, repository_check_catalog) {
                        Ok(plan) => plan,
                        Err(error) => {
                            ledger.discard_invalid_provider_response(lease, &receipt)?;
                            return Err(error);
                        }
                    };
                if let Err(error) = ledger.charge_required_call(lease, &mut action, &mut receipt) {
                    ledger.discard_invalid_provider_response(lease, &receipt)?;
                    return Err(error.into());
                }
                ledger.finish_provider_response(lease, &receipt)?;
                ledger.complete_required_action(lease, action)?;
                let ack = self
                    .engine
                    .emit_durable(DurableRoleEvent::StructuredPlan(plan.clone()), cancellation)
                    .await?;
                Ok(PlannerRoleOutcome::Submitted(ValidatedPlannerPlan {
                    plan,
                    required_checks,
                    durable_sequence: ack.sequence(),
                }))
            }
            ControlRequest::ReportBlocked(blocked) => {
                ledger.complete_required_early_terminal(
                    lease,
                    action,
                    &mut receipt,
                    EarlyRoleBudgetTermination::ReportBlocked,
                )?;
                Ok(PlannerRoleOutcome::Blocked(blocked))
            }
            _ => {
                ledger.discard_invalid_provider_response(lease, &receipt)?;
                Err(RoleLoopError::PlannerActionNotAllowed)
            }
        }
    }
}

fn classify_planner_response_error(
    response: &ModelResponse,
    error: RoleContractError,
) -> RoleLoopError {
    if matches!(error, RoleContractError::RedactionMutation) {
        return RoleLoopError::Contract(error);
    }
    match response {
        ModelResponse::Final { .. } => RoleLoopError::InvalidPlannerPlan,
        ModelResponse::ToolCalls(batch)
            if matches!(
                batch.calls.as_slice(),
                [call]
                    if matches!(
                        call.request,
                        ActionRequest::Control(
                            ControlRequest::SubmitPlan(_) | ControlRequest::ReportBlocked(_)
                        )
                    )
            ) =>
        {
            RoleLoopError::InvalidPlannerPlan
        }
        ModelResponse::ToolCalls(_) => RoleLoopError::PlannerActionNotAllowed,
    }
}

fn validate_planner_submission(
    submission: &crate::PlanSubmission,
    repository_check_catalog: &[RequiredCheckSelector],
) -> Result<(PlanSnapshot, Vec<RequiredCheck>), RoleLoopError> {
    ActionRequest::Control(ControlRequest::SubmitPlan(submission.clone()))
        .validate()
        .map_err(|_| RoleLoopError::InvalidPlannerPlan)?;
    let catalog = repository_check_catalog.iter().collect::<HashSet<_>>();
    let mut required_checks = Vec::with_capacity(submission.initial_required_checks().len());
    for (ordinal, submitted) in submission.initial_required_checks().iter().enumerate() {
        let selector = submitted
            .selector()
            .map_err(|_| RoleLoopError::InvalidPlannerPlan)?;
        if !catalog.contains(&selector) {
            return Err(RoleLoopError::InvalidPlannerPlan);
        }
        required_checks.push(
            RequiredCheck::try_from_selector(format!("check-{:02}", ordinal + 1), selector)
                .map_err(|_| RoleLoopError::InvalidPlannerPlan)?,
        );
    }
    RequiredCheckLedger::try_new(required_checks.clone())
        .map_err(|_| RoleLoopError::InvalidPlannerPlan)?;
    let items = submission
        .steps()
        .iter()
        .enumerate()
        .map(|(ordinal, step)| {
            PlanItem::try_structured(
                format!("step-{:02}", ordinal + 1),
                step.title(),
                step.description(),
                step.acceptance_criteria().to_vec(),
                PlanItemStatus::Pending,
            )
            .map_err(|_| RoleLoopError::InvalidPlannerPlan)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let plan =
        PlanSnapshot::try_structured(1, submission.summary(), items, required_checks.clone())
            .map_err(|_| RoleLoopError::InvalidPlannerPlan)?;
    Ok((plan, required_checks))
}

pub struct ExecutorRoleInput<'a> {
    pub review_round: u32,
    pub task_prompt: &'a str,
    pub repository_context: &'a str,
    pub plan: &'a mut PlanSnapshot,
    pub checkpoint: &'a mut WorkspaceCheckpoint,
    pub required_checks: &'a mut RequiredCheckLedger,
    pub latest_reviewer_findings: Option<&'a [ReviewFinding]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedExecution {
    summary: String,
    workspace_generation: u64,
    workspace_digest: coding_agent_domain::WorkspaceDigest,
    tests: TestSnapshot,
    durable_sequence: u64,
}

impl ValidatedExecution {
    pub fn summary(&self) -> &str {
        &self.summary
    }

    pub const fn workspace_generation(&self) -> u64 {
        self.workspace_generation
    }

    pub const fn workspace_digest(&self) -> &coding_agent_domain::WorkspaceDigest {
        &self.workspace_digest
    }

    pub const fn tests(&self) -> &TestSnapshot {
        &self.tests
    }

    pub const fn durable_sequence(&self) -> u64 {
        self.durable_sequence
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleStageFailure {
    status: TaskStatus,
    delivery_readiness: DeliveryReadiness,
    failure: TaskFailure,
}

impl RoleStageFailure {
    pub const fn status(&self) -> TaskStatus {
        self.status
    }

    pub const fn delivery_readiness(&self) -> DeliveryReadiness {
        self.delivery_readiness
    }

    pub const fn failure(&self) -> &TaskFailure {
        &self.failure
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockedExecutor {
    submission: BlockedSubmission,
    stage_failure: RoleStageFailure,
}

impl BlockedExecutor {
    pub const fn submission(&self) -> &BlockedSubmission {
        &self.submission
    }

    pub const fn stage_failure(&self) -> &RoleStageFailure {
        &self.stage_failure
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutorRoleOutcome {
    Submitted(ValidatedExecution),
    Blocked(BlockedExecutor),
}

enum RequiredExecutorProgress {
    Continue,
    Finished(ExecutorRoleOutcome),
}

enum ExecutorBatchProgress {
    Completed,
    WorkspaceChangedBeforeFirstDispatch,
}

enum ExecutorCheckAttempt {
    Queued(String),
    Running(crate::CheckRunToken),
}

pub struct ExecutorRoleLoop {
    engine: RoleEngine,
}

impl ExecutorRoleLoop {
    pub fn new(
        provider: Arc<dyn PreparedModelProvider>,
        runtime: Arc<dyn RoleActionRuntime>,
        events: Arc<dyn RoleEventSink>,
        redactor: Arc<dyn ContextRedactor>,
    ) -> Self {
        Self::from_engine(RoleEngine::new(provider, runtime, events, redactor))
    }

    pub const fn from_engine(engine: RoleEngine) -> Self {
        Self { engine }
    }

    pub const fn engine(&self) -> &RoleEngine {
        &self.engine
    }

    pub async fn run(
        &self,
        mut input: ExecutorRoleInput<'_>,
        ledger: &mut TaskBudgetLedger,
        cancellation: CancellationToken,
    ) -> Result<ExecutorRoleOutcome, RoleLoopError> {
        if cancellation.is_cancelled() {
            return Err(RoleLoopError::Cancelled);
        }
        RoleRun::try_new(Role::Executor, input.review_round)?;
        self.observe_current_fingerprint(
            input.checkpoint,
            input.required_checks,
            cancellation.clone(),
        )
        .await?;
        let mut lease =
            ledger.start_executor(input.review_round, input.required_checks, input.checkpoint)?;
        let result = self
            .run_active(&mut input, ledger, &mut lease, cancellation.clone())
            .await;
        match result {
            Ok(outcome) => {
                ledger.finish_role(lease)?;
                Ok(outcome)
            }
            Err(error) => {
                ledger.abort_role_on_failure(lease)?;
                Err(error)
            }
        }
    }

    async fn run_active(
        &self,
        input: &mut ExecutorRoleInput<'_>,
        ledger: &mut TaskBudgetLedger,
        lease: &mut crate::RoleBudgetLease,
        cancellation: CancellationToken,
    ) -> Result<ExecutorRoleOutcome, RoleLoopError> {
        let owner = RoleRun::try_new(Role::Executor, input.review_round)?;
        let handoff = crate::ContinuationHandoff::try_for_executor(
            input.review_round,
            input.task_prompt,
            input.repository_context,
            input.plan,
            input.checkpoint,
            input.required_checks,
            input.latest_reviewer_findings,
            self.engine.redactor(),
        )?;
        let policy = format!(
            "{EXECUTOR_SYSTEM_POLICY}\nExecutor role run: {}",
            input.review_round
        );
        let mut transcript = RoleTranscript::try_fresh(
            owner,
            policy,
            crate::RoleHandoff::from(handoff),
            self.engine.redactor(),
        )?;
        let started = if input.review_round == 1 {
            "Executor #1 started".to_owned()
        } else {
            format!(
                "{} (Executor #{}) started",
                crate::executor_rework_banner(input.review_round),
                input.review_round
            )
        };
        self.engine
            .emit(
                RoleEvent::Activity(RoleActivityEvent::try_new(
                    Role::Executor,
                    input.review_round,
                    started,
                )?),
                cancellation.clone(),
            )
            .await?;

        loop {
            if cancellation.is_cancelled() {
                return Err(RoleLoopError::Cancelled);
            }
            let exchange = self
                .engine
                .exploratory_exchange(&transcript, ledger, lease, cancellation.clone())
                .await;
            let (mut receipt, response) = match exchange {
                Ok(exchange) => exchange,
                Err(RoleLoopError::Budget(BudgetError::ReservationWouldBeConsumed {
                    resource: BudgetResource::ModelResponses,
                })) => {
                    match self
                        .run_required_executor_action(
                            input,
                            &mut transcript,
                            ledger,
                            lease,
                            cancellation.clone(),
                        )
                        .await?
                    {
                        RequiredExecutorProgress::Continue => continue,
                        RequiredExecutorProgress::Finished(outcome) => return Ok(outcome),
                    }
                }
                Err(error) => return Err(error),
            };

            let ModelResponse::ToolCalls(batch) = &response else {
                ledger.discard_invalid_provider_response(lease, &receipt)?;
                return Err(RoleLoopError::InvalidExecutorOutput);
            };
            if let Err(error) = validate_role_response(
                Role::Executor,
                &response,
                &ModelToolChoice::Auto,
                self.engine.redactor(),
            ) {
                ledger.discard_invalid_provider_response(lease, &receipt)?;
                if matches!(error, RoleContractError::RedactionMutation) {
                    return Err(error.into());
                }
                return Err(RoleLoopError::ExecutorActionNotAllowed);
            }
            if let Err(error) = transcript.preflight_runtime_batch(batch) {
                ledger.discard_invalid_provider_response(lease, &receipt)?;
                return Err(error.into());
            }
            let batch = batch.clone();

            if let [call] = batch.calls.as_slice()
                && let ActionRequest::Control(control) = &call.request
            {
                match control.clone() {
                    ControlRequest::UpdatePlanProgress(progress) => {
                        self.complete_plan_progress(
                            input.plan,
                            &mut transcript,
                            batch,
                            progress,
                            ledger,
                            lease,
                            &mut receipt,
                            cancellation.clone(),
                        )
                        .await?;
                        continue;
                    }
                    ControlRequest::SubmitExecution(submission) => {
                        let validated = match self
                            .validate_and_flush_execution(
                                submission.summary(),
                                input.plan,
                                input.checkpoint,
                                input.required_checks,
                                ledger,
                                lease,
                                cancellation.clone(),
                            )
                            .await
                        {
                            Ok(validated) => validated,
                            Err(error) => {
                                ledger.discard_invalid_provider_response(lease, &receipt)?;
                                return Err(error);
                            }
                        };
                        let terminal = match ledger.mint_exploratory_normal_terminal(
                            lease,
                            &receipt,
                            ControlRequest::SubmitExecution(submission),
                        ) {
                            Ok(terminal) => terminal,
                            Err(error) => {
                                ledger.discard_invalid_provider_response(lease, &receipt)?;
                                return Err(error.into());
                            }
                        };
                        ledger.complete_exploratory_normal_terminal(
                            lease,
                            &mut receipt,
                            terminal,
                        )?;
                        return Ok(ExecutorRoleOutcome::Submitted(validated));
                    }
                    ControlRequest::ReportBlocked(blocked) => {
                        ledger.complete_exploratory_early_terminal(
                            lease,
                            &mut receipt,
                            EarlyRoleBudgetTermination::ReportBlocked,
                        )?;
                        return Ok(ExecutorRoleOutcome::Blocked(blocked_executor(blocked)));
                    }
                    _ => {
                        ledger.discard_invalid_provider_response(lease, &receipt)?;
                        return Err(RoleLoopError::ExecutorActionNotAllowed);
                    }
                }
            }

            if validate_executor_runtime_batch(&batch, input.required_checks).is_err() {
                ledger.discard_invalid_provider_response(lease, &receipt)?;
                return Err(RoleLoopError::ExecutorActionNotAllowed);
            }
            let preflight_change = match self
                .observe_current_fingerprint(
                    input.checkpoint,
                    input.required_checks,
                    cancellation.clone(),
                )
                .await
            {
                Ok(change) => change,
                Err(error) => {
                    ledger.discard_invalid_provider_response(lease, &receipt)?;
                    return Err(error);
                }
            };
            if matches!(preflight_change, crate::CheckpointChange::Advanced { .. }) {
                ledger.discard_invalid_provider_response(lease, &receipt)?;
                ledger.refresh_executor_required_actions(
                    lease,
                    input.required_checks,
                    input.checkpoint,
                )?;
                match self
                    .run_required_executor_action(
                        input,
                        &mut transcript,
                        ledger,
                        lease,
                        cancellation.clone(),
                    )
                    .await?
                {
                    RequiredExecutorProgress::Continue => continue,
                    RequiredExecutorProgress::Finished(outcome) => return Ok(outcome),
                }
            }
            if executor_batch_may_change_workspace(&batch)
                && let Err(error) =
                    ledger.protect_executor_workspace_change(lease, &receipt, input.required_checks)
            {
                ledger.discard_invalid_provider_response(lease, &receipt)?;
                return Err(error.into());
            }
            let permit = match ledger.preflight_exploratory_runtime_batch(
                lease,
                &mut receipt,
                batch.calls.clone(),
            ) {
                Ok(permit) => permit,
                Err(BudgetError::ReservationWouldBeConsumed {
                    resource:
                        BudgetResource::ModelVisibleCalls | BudgetResource::RetainedResultBytes,
                }) => {
                    ledger.discard_invalid_provider_response(lease, &receipt)?;
                    match self
                        .run_required_executor_action(
                            input,
                            &mut transcript,
                            ledger,
                            lease,
                            cancellation.clone(),
                        )
                        .await?
                    {
                        RequiredExecutorProgress::Continue => continue,
                        RequiredExecutorProgress::Finished(outcome) => return Ok(outcome),
                    }
                }
                Err(error) => {
                    ledger.discard_invalid_provider_response(lease, &receipt)?;
                    return Err(error.into());
                }
            };
            match self
                .execute_executor_runtime_batch(
                    &mut transcript,
                    batch,
                    permit,
                    ledger,
                    lease,
                    &receipt,
                    input.checkpoint,
                    input.required_checks,
                    input.review_round,
                    cancellation.clone(),
                )
                .await?
            {
                ExecutorBatchProgress::Completed => {}
                ExecutorBatchProgress::WorkspaceChangedBeforeFirstDispatch => {
                    ledger.refresh_executor_required_actions(
                        lease,
                        input.required_checks,
                        input.checkpoint,
                    )?;
                    match self
                        .run_required_executor_action(
                            input,
                            &mut transcript,
                            ledger,
                            lease,
                            cancellation.clone(),
                        )
                        .await?
                    {
                        RequiredExecutorProgress::Continue => continue,
                        RequiredExecutorProgress::Finished(outcome) => return Ok(outcome),
                    }
                }
            }
            ledger.refresh_executor_required_actions(
                lease,
                input.required_checks,
                input.checkpoint,
            )?;
            self.engine
                .emit(
                    RoleEvent::Activity(RoleActivityEvent::try_new(
                        Role::Executor,
                        input.review_round,
                        "Executor runtime batch completed",
                    )?),
                    cancellation.clone(),
                )
                .await?;
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn complete_plan_progress(
        &self,
        plan: &mut PlanSnapshot,
        transcript: &mut RoleTranscript,
        batch: crate::ToolCallBatch,
        progress: crate::PlanProgressSubmission,
        ledger: &mut TaskBudgetLedger,
        lease: &mut crate::RoleBudgetLease,
        receipt: &mut crate::ProviderResponseReceipt,
        cancellation: CancellationToken,
    ) -> Result<(), RoleLoopError> {
        let updated = match apply_plan_progress(plan, &progress) {
            Ok(updated) => updated,
            Err(error) => {
                ledger.discard_invalid_provider_response(lease, receipt)?;
                return Err(error);
            }
        };
        let [call] = batch.calls.as_slice() else {
            ledger.discard_invalid_provider_response(lease, receipt)?;
            return Err(RoleLoopError::ExecutorActionNotAllowed);
        };
        let ack_result = ToolResult::text(format!(
            "Plan progress durably updated to revision {}",
            updated.revision()
        ));
        let retained = RetainedToolResult::try_from_tool_result_with_limit(
            &call.id,
            &ack_result,
            self.engine.redactor(),
            crate::PLAN_PROGRESS_RETAINED_RESULT_LIMIT,
        )?;
        let permit =
            match ledger.preflight_exploratory_control_result(lease, receipt, call, &retained) {
                Ok(permit) => permit,
                Err(error) => {
                    ledger.discard_invalid_provider_response(lease, receipt)?;
                    return Err(error.into());
                }
            };
        let durable = self
            .engine
            .emit_durable(
                DurableRoleEvent::PlanUpdated(updated.clone()),
                cancellation.clone(),
            )
            .await;
        if let Err(error) = durable {
            ledger.abort_exploratory_control_result(lease, &permit)?;
            ledger.finish_provider_response(lease, receipt)?;
            return Err(error);
        }

        // The durable store is now authoritative even if a later local
        // transcript operation fails.
        *plan = updated;
        if let Err(error) = ledger.retain_exploratory_control_result(lease, &permit, &retained) {
            ledger.abort_exploratory_control_result(lease, &permit)?;
            ledger.finish_provider_response(lease, receipt)?;
            return Err(error.into());
        }
        if let Err(error) = transcript.append_runtime_batch(batch, vec![retained]) {
            ledger.finish_provider_response(lease, receipt)?;
            return Err(error.into());
        }
        ledger.finish_provider_response(lease, receipt)?;
        Ok(())
    }

    async fn observe_current_fingerprint(
        &self,
        checkpoint: &mut WorkspaceCheckpoint,
        required_checks: &RequiredCheckLedger,
        cancellation: CancellationToken,
    ) -> Result<crate::CheckpointChange, RoleLoopError> {
        if cancellation.is_cancelled() {
            return Err(RoleLoopError::Cancelled);
        }
        let fingerprint = self
            .engine
            .runtime
            .workspace_fingerprint(cancellation.clone())
            .await
            .map_err(map_runtime_error)?;
        if cancellation.is_cancelled() {
            return Err(RoleLoopError::Cancelled);
        }
        let change = checkpoint.observe_stable(fingerprint)?;
        if matches!(change, crate::CheckpointChange::Advanced { .. }) {
            self.engine
                .emit(
                    RoleEvent::Tests(project_test_snapshot(required_checks, checkpoint)),
                    cancellation,
                )
                .await?;
        }
        Ok(change)
    }

    async fn emit_test_snapshot(
        &self,
        required_checks: &RequiredCheckLedger,
        checkpoint: &WorkspaceCheckpoint,
        cancellation: CancellationToken,
    ) -> Result<(), RoleLoopError> {
        self.engine
            .emit(
                RoleEvent::Tests(project_test_snapshot(required_checks, checkpoint)),
                cancellation,
            )
            .await
    }

    async fn abandon_executor_check_attempt(
        &self,
        required_checks: &mut RequiredCheckLedger,
        checkpoint: &WorkspaceCheckpoint,
        attempt: ExecutorCheckAttempt,
        cancellation: CancellationToken,
        original_error: &RoleLoopError,
    ) {
        let abandoned = match attempt {
            ExecutorCheckAttempt::Queued(check_id) => required_checks
                .abandon_queued_check(checkpoint, &check_id)
                .is_ok(),
            ExecutorCheckAttempt::Running(token) => {
                required_checks.abandon_check_run(token).is_ok()
            }
        };
        debug_assert!(
            abandoned,
            "the exact Executor check attempt must remain available for cleanup"
        );
        if abandoned
            && !matches!(original_error, RoleLoopError::Cancelled)
            && !cancellation.is_cancelled()
        {
            let _ = self
                .emit_test_snapshot(required_checks, checkpoint, cancellation)
                .await;
        }
    }

    async fn fail_executor_check<T>(
        &self,
        required_checks: &mut RequiredCheckLedger,
        checkpoint: &WorkspaceCheckpoint,
        attempt: ExecutorCheckAttempt,
        cancellation: CancellationToken,
        error: RoleLoopError,
    ) -> Result<T, RoleLoopError> {
        self.abandon_executor_check_attempt(
            required_checks,
            checkpoint,
            attempt,
            cancellation,
            &error,
        )
        .await;
        Err(error)
    }

    #[allow(clippy::too_many_arguments)]
    async fn fail_executor_check_batch<T>(
        &self,
        required_checks: &mut RequiredCheckLedger,
        checkpoint: &WorkspaceCheckpoint,
        attempt: ExecutorCheckAttempt,
        ledger: &mut TaskBudgetLedger,
        lease: &mut crate::RoleBudgetLease,
        permit: &crate::ExploratoryBatchPermit,
        receipt: &crate::ProviderResponseReceipt,
        cancellation: CancellationToken,
        error: RoleLoopError,
    ) -> Result<T, RoleLoopError> {
        self.abandon_executor_check_attempt(
            required_checks,
            checkpoint,
            attempt,
            cancellation,
            &error,
        )
        .await;
        let batch_cleanup = release_executor_batch(ledger, lease, permit, receipt);
        debug_assert!(
            batch_cleanup.is_ok(),
            "the exact Executor batch and provider response must remain releasable"
        );
        Err(error)
    }

    #[allow(clippy::too_many_arguments)]
    async fn validate_and_flush_execution(
        &self,
        summary: &str,
        plan: &PlanSnapshot,
        checkpoint: &mut WorkspaceCheckpoint,
        required_checks: &RequiredCheckLedger,
        ledger: &TaskBudgetLedger,
        lease: &crate::RoleBudgetLease,
        cancellation: CancellationToken,
    ) -> Result<ValidatedExecution, RoleLoopError> {
        if plan
            .items()
            .iter()
            .any(|item| item.status() != PlanItemStatus::Completed)
            || !required_checks.all_current_checks_passed(checkpoint)
            || ledger
                .pending_reviewer_reservation()
                .is_none_or(|reservation| {
                    reservation.review_round() != lease.role_run()
                        || reservation.amounts().retained_result_bytes()
                            != crate::EXECUTOR_REVIEWER_RETAINED_RESERVATION
                })
        {
            return Err(RoleLoopError::InvalidExecutorOutput);
        }
        if cancellation.is_cancelled() {
            return Err(RoleLoopError::Cancelled);
        }

        let mut terminal = self
            .engine
            .runtime
            .terminal_snapshot(checkpoint.generation(), cancellation.clone())
            .await
            .map_err(map_runtime_error)?;
        if terminal.diff.revision != checkpoint.generation() {
            return Err(RoleLoopError::QualityEvidenceMismatch);
        }
        let change = checkpoint.observe_stable(terminal.fingerprint)?;
        if matches!(change, crate::CheckpointChange::Advanced { .. }) {
            terminal = self
                .engine
                .runtime
                .terminal_snapshot(checkpoint.generation(), cancellation.clone())
                .await
                .map_err(map_runtime_error)?;
            if terminal.fingerprint != checkpoint.fingerprint()
                || terminal.diff.revision != checkpoint.generation()
            {
                return Err(RoleLoopError::QualityEvidenceMismatch);
            }
        }
        let terminal_diff = sanitize_terminal_diff(&terminal.diff, self.engine.redactor())?;
        self.engine
            .emit(RoleEvent::Diff(terminal_diff), cancellation.clone())
            .await?;
        let tests = project_test_snapshot(required_checks, checkpoint);
        self.engine
            .emit(RoleEvent::Tests(tests.clone()), cancellation.clone())
            .await?;
        if matches!(change, crate::CheckpointChange::Advanced { .. })
            || !required_checks.all_current_checks_passed(checkpoint)
        {
            return Err(RoleLoopError::InvalidExecutorOutput);
        }
        let ack = self
            .engine
            .flush_checkpoint(checkpoint.generation(), cancellation.clone())
            .await?;
        if cancellation.is_cancelled() {
            return Err(RoleLoopError::Cancelled);
        }
        Ok(ValidatedExecution {
            summary: summary.to_owned(),
            workspace_generation: checkpoint.generation(),
            workspace_digest: checkpoint.workspace_digest(),
            tests,
            durable_sequence: ack.sequence(),
        })
    }

    async fn run_required_executor_action(
        &self,
        input: &mut ExecutorRoleInput<'_>,
        transcript: &mut RoleTranscript,
        ledger: &mut TaskBudgetLedger,
        lease: &mut crate::RoleBudgetLease,
        cancellation: CancellationToken,
    ) -> Result<RequiredExecutorProgress, RoleLoopError> {
        self.observe_current_fingerprint(
            input.checkpoint,
            input.required_checks,
            cancellation.clone(),
        )
        .await?;
        ledger.refresh_executor_required_actions(lease, input.required_checks, input.checkpoint)?;
        let next = lease
            .next_required_action()
            .cloned()
            .ok_or(BudgetError::NoRequiredActionPending)?;
        match next {
            crate::RequiredBudgetAction::ExecutorCheck { check } => {
                self.run_required_executor_check(
                    input,
                    transcript,
                    ledger,
                    lease,
                    check,
                    cancellation,
                )
                .await?;
                Ok(RequiredExecutorProgress::Continue)
            }
            crate::RequiredBudgetAction::ExecutorTerminal => {
                self.run_required_executor_terminal(input, transcript, ledger, lease, cancellation)
                    .await
            }
            _ => Err(RoleLoopError::ExecutorActionNotAllowed),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_required_executor_check(
        &self,
        input: &mut ExecutorRoleInput<'_>,
        transcript: &mut RoleTranscript,
        ledger: &mut TaskBudgetLedger,
        lease: &mut crate::RoleBudgetLease,
        check: RequiredCheck,
        cancellation: CancellationToken,
    ) -> Result<(), RoleLoopError> {
        let mut action = ledger.begin_required_action(lease)?;
        let required = RequiredAction::Validation(check.clone());
        let (mut receipt, response) = self
            .engine
            .required_exchange(
                transcript,
                required.clone(),
                ledger,
                lease,
                &mut action,
                cancellation.clone(),
            )
            .await?;
        if cancellation.is_cancelled() {
            ledger.discard_invalid_provider_response(lease, &receipt)?;
            return Err(RoleLoopError::Cancelled);
        }
        if let Err(error) = validate_role_response(
            Role::Executor,
            &response,
            &ModelToolChoice::Required(required.clone()),
            self.engine.redactor(),
        ) {
            ledger.discard_invalid_provider_response(lease, &receipt)?;
            if matches!(error, RoleContractError::RedactionMutation) {
                return Err(error.into());
            }
            return Err(RoleLoopError::ExecutorActionNotAllowed);
        }
        let ModelResponse::ToolCalls(batch) = response else {
            ledger.discard_invalid_provider_response(lease, &receipt)?;
            return Err(RoleLoopError::InvalidExecutorOutput);
        };
        if let Err(error) = transcript.preflight_runtime_batch(&batch) {
            ledger.discard_invalid_provider_response(lease, &receipt)?;
            return Err(error.into());
        }

        input
            .required_checks
            .queue_check(input.checkpoint, check.id())?;
        if let Err(error) = self
            .emit_test_snapshot(
                input.required_checks,
                input.checkpoint,
                cancellation.clone(),
            )
            .await
        {
            self.abandon_executor_check_attempt(
                input.required_checks,
                input.checkpoint,
                ExecutorCheckAttempt::Queued(check.id().to_owned()),
                cancellation.clone(),
                &error,
            )
            .await;
            let response_cleanup = ledger
                .discard_invalid_provider_response(lease, &receipt)
                .map_err(RoleLoopError::from);
            debug_assert!(
                response_cleanup.is_ok(),
                "the exact required Executor response must remain discardable"
            );
            return Err(error);
        }
        let token = match input.required_checks.mark_check_running(
            input.checkpoint,
            check.id(),
            CheckActor::Executor,
            input.review_round,
        ) {
            Ok(token) => token,
            Err(mark_error) => {
                let error = RoleLoopError::from(mark_error);
                self.abandon_executor_check_attempt(
                    input.required_checks,
                    input.checkpoint,
                    ExecutorCheckAttempt::Queued(check.id().to_owned()),
                    cancellation.clone(),
                    &error,
                )
                .await;
                let response_cleanup = ledger
                    .discard_invalid_provider_response(lease, &receipt)
                    .map_err(RoleLoopError::from);
                debug_assert!(
                    response_cleanup.is_ok(),
                    "the exact required Executor response must remain discardable"
                );
                return Err(error);
            }
        };
        if let Err(error) = self
            .emit_test_snapshot(
                input.required_checks,
                input.checkpoint,
                cancellation.clone(),
            )
            .await
        {
            self.abandon_executor_check_attempt(
                input.required_checks,
                input.checkpoint,
                ExecutorCheckAttempt::Running(token),
                cancellation.clone(),
                &error,
            )
            .await;
            let response_cleanup = ledger
                .discard_invalid_provider_response(lease, &receipt)
                .map_err(RoleLoopError::from);
            debug_assert!(
                response_cleanup.is_ok(),
                "the exact required Executor response must remain discardable"
            );
            return Err(error);
        }
        let runtime_result = match self
            .engine
            .execute_required_runtime_action(
                transcript,
                batch,
                &required,
                action,
                ledger,
                lease,
                &mut receipt,
                cancellation.clone(),
            )
            .await
        {
            Ok(result) => result,
            Err(error) => {
                return self
                    .fail_executor_check(
                        input.required_checks,
                        input.checkpoint,
                        ExecutorCheckAttempt::Running(token),
                        cancellation,
                        error,
                    )
                    .await;
            }
        };
        let RoleRuntimeResult::Validation(observation) = runtime_result else {
            return self
                .fail_executor_check(
                    input.required_checks,
                    input.checkpoint,
                    ExecutorCheckAttempt::Running(token),
                    cancellation,
                    RoleLoopError::RuntimeResultMismatch,
                )
                .await;
        };
        let change = match self
            .observe_current_fingerprint(
                input.checkpoint,
                input.required_checks,
                cancellation.clone(),
            )
            .await
        {
            Ok(change) => change,
            Err(error) => {
                return self
                    .fail_executor_check(
                        input.required_checks,
                        input.checkpoint,
                        ExecutorCheckAttempt::Running(token),
                        cancellation,
                        error,
                    )
                    .await;
            }
        };
        if matches!(change, crate::CheckpointChange::Advanced { .. })
            || observation.check() != &check
        {
            return self
                .fail_executor_check(
                    input.required_checks,
                    input.checkpoint,
                    ExecutorCheckAttempt::Running(token),
                    cancellation,
                    RoleLoopError::QualityEvidenceMismatch,
                )
                .await;
        }
        let redacted = self
            .engine
            .redactor()
            .redact(observation.model_result().content());
        let (summary, summary_truncated) = bounded_evidence_summary(&redacted);
        // The unchanged post-runtime checkpoint and this linear token rule out
        // every `finish_check` error that could leave an active attempt. If
        // terminal evidence construction itself fails, `finish_check` removes
        // the active attempt before returning the error.
        input.required_checks.finish_check(
            input.checkpoint,
            token,
            observation.status(),
            observation.duration_ms(),
            summary,
            observation.truncated() || summary_truncated,
        )?;
        self.emit_test_snapshot(
            input.required_checks,
            input.checkpoint,
            cancellation.clone(),
        )
        .await?;
        ledger.refresh_executor_required_actions(lease, input.required_checks, input.checkpoint)?;
        Ok(())
    }

    async fn run_required_executor_terminal(
        &self,
        input: &mut ExecutorRoleInput<'_>,
        transcript: &RoleTranscript,
        ledger: &mut TaskBudgetLedger,
        lease: &mut crate::RoleBudgetLease,
        cancellation: CancellationToken,
    ) -> Result<RequiredExecutorProgress, RoleLoopError> {
        let mut action = ledger.begin_required_action(lease)?;
        let required = RequiredAction::terminal_or_blocked(crate::ControlKind::SubmitExecution)?;
        let (mut receipt, response) = self
            .engine
            .required_exchange(
                transcript,
                required.clone(),
                ledger,
                lease,
                &mut action,
                cancellation.clone(),
            )
            .await?;
        if cancellation.is_cancelled() {
            ledger.discard_invalid_provider_response(lease, &receipt)?;
            return Err(RoleLoopError::Cancelled);
        }
        if let Err(error) = validate_role_response(
            Role::Executor,
            &response,
            &ModelToolChoice::Required(required),
            self.engine.redactor(),
        ) {
            ledger.discard_invalid_provider_response(lease, &receipt)?;
            if matches!(error, RoleContractError::RedactionMutation) {
                return Err(error.into());
            }
            return Err(RoleLoopError::ExecutorActionNotAllowed);
        }
        let ModelResponse::ToolCalls(batch) = response else {
            ledger.discard_invalid_provider_response(lease, &receipt)?;
            return Err(RoleLoopError::InvalidExecutorOutput);
        };
        if let Err(error) = transcript.preflight_runtime_batch(&batch) {
            ledger.discard_invalid_provider_response(lease, &receipt)?;
            return Err(error.into());
        }
        let [call] = batch.calls.as_slice() else {
            ledger.discard_invalid_provider_response(lease, &receipt)?;
            return Err(RoleLoopError::ExecutorActionNotAllowed);
        };
        let ActionRequest::Control(control) = &call.request else {
            ledger.discard_invalid_provider_response(lease, &receipt)?;
            return Err(RoleLoopError::ExecutorActionNotAllowed);
        };
        match control.clone() {
            ControlRequest::SubmitExecution(submission) => {
                let validated = match self
                    .validate_and_flush_execution(
                        submission.summary(),
                        input.plan,
                        input.checkpoint,
                        input.required_checks,
                        ledger,
                        lease,
                        cancellation.clone(),
                    )
                    .await
                {
                    Ok(validated) => validated,
                    Err(error) => {
                        ledger.discard_invalid_provider_response(lease, &receipt)?;
                        return Err(error);
                    }
                };
                if let Err(error) = ledger.charge_required_call(lease, &mut action, &mut receipt) {
                    ledger.discard_invalid_provider_response(lease, &receipt)?;
                    return Err(error.into());
                }
                ledger.finish_provider_response(lease, &receipt)?;
                ledger.complete_required_action(lease, action)?;
                Ok(RequiredExecutorProgress::Finished(
                    ExecutorRoleOutcome::Submitted(validated),
                ))
            }
            ControlRequest::ReportBlocked(blocked) => {
                ledger.complete_required_early_terminal(
                    lease,
                    action,
                    &mut receipt,
                    EarlyRoleBudgetTermination::ReportBlocked,
                )?;
                Ok(RequiredExecutorProgress::Finished(
                    ExecutorRoleOutcome::Blocked(blocked_executor(blocked)),
                ))
            }
            _ => {
                ledger.discard_invalid_provider_response(lease, &receipt)?;
                Err(RoleLoopError::ExecutorActionNotAllowed)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_executor_runtime_batch(
        &self,
        transcript: &mut RoleTranscript,
        batch: crate::ToolCallBatch,
        permit: crate::ExploratoryBatchPermit,
        ledger: &mut TaskBudgetLedger,
        lease: &mut crate::RoleBudgetLease,
        receipt: &crate::ProviderResponseReceipt,
        checkpoint: &mut WorkspaceCheckpoint,
        required_checks: &mut RequiredCheckLedger,
        role_run: u32,
        cancellation: CancellationToken,
    ) -> Result<ExecutorBatchProgress, RoleLoopError> {
        if ensure_transcript_matches_lease(transcript, lease).is_err()
            || validate_action_batch(
                Role::Executor,
                &batch,
                &ModelToolChoice::Auto,
                self.engine.redactor(),
            )
            .is_err()
            || transcript.preflight_runtime_batch(&batch).is_err()
            || batch.calls.len() != permit.invocations().len()
            || batch
                .calls
                .iter()
                .zip(permit.invocations())
                .any(|(call, invocation)| {
                    call.id != invocation.tool_call_id()
                        || call.request != ActionRequest::Runtime(invocation.request().clone())
                })
        {
            return fail_executor_batch(
                ledger,
                lease,
                &permit,
                receipt,
                RoleLoopError::ExecutorActionNotAllowed,
            );
        }

        let mut retained_results = Vec::with_capacity(permit.invocations().len());
        for (action_index, invocation) in permit.invocations().iter().enumerate() {
            if cancellation.is_cancelled() {
                return fail_executor_batch(
                    ledger,
                    lease,
                    &permit,
                    receipt,
                    RoleLoopError::Cancelled,
                );
            }
            let before_dispatch = match self
                .observe_current_fingerprint(checkpoint, required_checks, cancellation.clone())
                .await
            {
                Ok(change) => change,
                Err(error) => {
                    return fail_executor_batch(ledger, lease, &permit, receipt, error);
                }
            };
            if matches!(before_dispatch, crate::CheckpointChange::Advanced { .. }) {
                if action_index == 0 {
                    release_executor_batch(ledger, lease, &permit, receipt)?;
                    return Ok(ExecutorBatchProgress::WorkspaceChangedBeforeFirstDispatch);
                }
                return fail_executor_batch(
                    ledger,
                    lease,
                    &permit,
                    receipt,
                    RoleLoopError::QualityEvidenceMismatch,
                );
            }

            let (runtime_request, validation_check) = match invocation.request() {
                RuntimeActionRequest::ValidationSelector { selector } => {
                    let Some(check) = required_checks.check_by_selector(selector).cloned() else {
                        return fail_executor_batch(
                            ledger,
                            lease,
                            &permit,
                            receipt,
                            RoleLoopError::ExecutorActionNotAllowed,
                        );
                    };
                    (
                        RuntimeActionRequest::Validation {
                            check: check.clone(),
                        },
                        Some(check),
                    )
                }
                RuntimeActionRequest::Tool(request) => {
                    (RuntimeActionRequest::Tool(request.clone()), None)
                }
                _ => {
                    return fail_executor_batch(
                        ledger,
                        lease,
                        &permit,
                        receipt,
                        RoleLoopError::ExecutorActionNotAllowed,
                    );
                }
            };

            let token = if let Some(check) = &validation_check {
                if let Err(error) = required_checks.queue_check(checkpoint, check.id()) {
                    return fail_executor_batch(ledger, lease, &permit, receipt, error.into());
                }
                if let Err(error) = self
                    .emit_test_snapshot(required_checks, checkpoint, cancellation.clone())
                    .await
                {
                    return self
                        .fail_executor_check_batch(
                            required_checks,
                            checkpoint,
                            ExecutorCheckAttempt::Queued(check.id().to_owned()),
                            ledger,
                            lease,
                            &permit,
                            receipt,
                            cancellation,
                            error,
                        )
                        .await;
                }
                let token = match required_checks.mark_check_running(
                    checkpoint,
                    check.id(),
                    CheckActor::Executor,
                    role_run,
                ) {
                    Ok(token) => token,
                    Err(mark_error) => {
                        return self
                            .fail_executor_check_batch(
                                required_checks,
                                checkpoint,
                                ExecutorCheckAttempt::Queued(check.id().to_owned()),
                                ledger,
                                lease,
                                &permit,
                                receipt,
                                cancellation,
                                mark_error.into(),
                            )
                            .await;
                    }
                };
                if let Err(error) = self
                    .emit_test_snapshot(required_checks, checkpoint, cancellation.clone())
                    .await
                {
                    return self
                        .fail_executor_check_batch(
                            required_checks,
                            checkpoint,
                            ExecutorCheckAttempt::Running(token),
                            ledger,
                            lease,
                            &permit,
                            receipt,
                            cancellation,
                            error,
                        )
                        .await;
                }
                Some(token)
            } else {
                None
            };
            let mut token = token;

            let runtime_result = match self
                .engine
                .invoke_runtime(runtime_request, cancellation.clone())
                .await
            {
                Ok(result) => result,
                Err(error) => {
                    if let Some(token) = token.take() {
                        return self
                            .fail_executor_check_batch(
                                required_checks,
                                checkpoint,
                                ExecutorCheckAttempt::Running(token),
                                ledger,
                                lease,
                                &permit,
                                receipt,
                                cancellation,
                                error,
                            )
                            .await;
                    }
                    return fail_executor_batch(ledger, lease, &permit, receipt, error);
                }
            };
            let change = match self
                .observe_current_fingerprint(checkpoint, required_checks, cancellation.clone())
                .await
            {
                Ok(change) => change,
                Err(error) => {
                    if let Some(token) = token.take() {
                        return self
                            .fail_executor_check_batch(
                                required_checks,
                                checkpoint,
                                ExecutorCheckAttempt::Running(token),
                                ledger,
                                lease,
                                &permit,
                                receipt,
                                cancellation,
                                error,
                            )
                            .await;
                    }
                    return fail_executor_batch(ledger, lease, &permit, receipt, error);
                }
            };

            if let (Some(check), Some(token)) = (&validation_check, token.take()) {
                let RoleRuntimeResult::Validation(observation) = &runtime_result else {
                    return self
                        .fail_executor_check_batch(
                            required_checks,
                            checkpoint,
                            ExecutorCheckAttempt::Running(token),
                            ledger,
                            lease,
                            &permit,
                            receipt,
                            cancellation,
                            RoleLoopError::RuntimeResultMismatch,
                        )
                        .await;
                };
                if matches!(change, crate::CheckpointChange::Advanced { .. })
                    || observation.check() != check
                {
                    return self
                        .fail_executor_check_batch(
                            required_checks,
                            checkpoint,
                            ExecutorCheckAttempt::Running(token),
                            ledger,
                            lease,
                            &permit,
                            receipt,
                            cancellation,
                            RoleLoopError::QualityEvidenceMismatch,
                        )
                        .await;
                }
                let redacted = self
                    .engine
                    .redactor()
                    .redact(observation.model_result().content());
                let (summary, summary_truncated) = bounded_evidence_summary(&redacted);
                if let Err(error) = required_checks.finish_check(
                    checkpoint,
                    token,
                    observation.status(),
                    observation.duration_ms(),
                    summary,
                    observation.truncated() || summary_truncated,
                ) {
                    return fail_executor_batch(ledger, lease, &permit, receipt, error.into());
                }
                if let Err(error) = self
                    .emit_test_snapshot(required_checks, checkpoint, cancellation.clone())
                    .await
                {
                    return fail_executor_batch(ledger, lease, &permit, receipt, error);
                }
            }

            let tool_result = match (invocation.request(), &runtime_result) {
                (RuntimeActionRequest::Tool(_), RoleRuntimeResult::Tool(result)) => result,
                (
                    RuntimeActionRequest::ValidationSelector { selector },
                    RoleRuntimeResult::Validation(observation),
                ) if observation.check().selector() == selector => observation.model_result(),
                _ => {
                    return fail_executor_batch(
                        ledger,
                        lease,
                        &permit,
                        receipt,
                        RoleLoopError::RuntimeResultMismatch,
                    );
                }
            };
            let retained = match RetainedToolResult::try_from_tool_result_with_limit(
                invocation.tool_call_id(),
                tool_result,
                self.engine.redactor(),
                invocation.wrapper_cap(),
            ) {
                Ok(retained) => retained,
                Err(error) => {
                    return fail_executor_batch(ledger, lease, &permit, receipt, error.into());
                }
            };
            retained_results.push(retained);
        }

        if let Err(error) =
            ledger.retain_exploratory_batch_results(lease, &permit, &retained_results)
        {
            ledger.abort_exploratory_runtime_batch(lease, &permit)?;
            ledger.finish_provider_response(lease, receipt)?;
            return Err(error.into());
        }
        if let Err(error) = transcript.append_runtime_batch(batch, retained_results) {
            ledger.finish_provider_response(lease, receipt)?;
            return Err(error.into());
        }
        ledger.finish_provider_response(lease, receipt)?;
        Ok(ExecutorBatchProgress::Completed)
    }
}

pub struct ReviewerRoleInput<'a> {
    pub review_round: u32,
    pub task_prompt: &'a str,
    pub repository_context: &'a str,
    pub plan: &'a PlanSnapshot,
    pub execution: &'a ValidatedExecution,
    pub checkpoint: &'a mut WorkspaceCheckpoint,
    pub required_checks: &'a mut RequiredCheckLedger,
    pub previous_reviews: &'a [NewReviewEvidence],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedReviewDecision {
    evidence: NewReviewEvidence,
    durable_sequence: u64,
}

impl ValidatedReviewDecision {
    pub const fn evidence(&self) -> &NewReviewEvidence {
        &self.evidence
    }

    pub const fn durable_sequence(&self) -> u64 {
        self.durable_sequence
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockedReviewer {
    submission: BlockedSubmission,
    stage_failure: RoleStageFailure,
}

impl BlockedReviewer {
    pub const fn submission(&self) -> &BlockedSubmission {
        &self.submission
    }

    pub const fn stage_failure(&self) -> &RoleStageFailure {
        &self.stage_failure
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReviewerRoleOutcome {
    Decided(ValidatedReviewDecision),
    Blocked(BlockedReviewer),
}

#[derive(Default)]
struct ReviewerCoverageTracker {
    manifest: Option<ReviewDiffManifest>,
    covered_chunks: Vec<u8>,
    pending_chunks: Vec<u8>,
}

impl ReviewerCoverageTracker {
    fn bind_manifest(&mut self, manifest: ReviewDiffManifest) -> Result<(), RoleLoopError> {
        if self.manifest.is_some() || !self.pending_chunks.is_empty() {
            return Err(RoleLoopError::QualityEvidenceMismatch);
        }
        self.manifest = Some(manifest);
        Ok(())
    }

    fn record_chunk_result(&mut self, batch: &ReviewDiffChunkBatch) -> Result<(), RoleLoopError> {
        let manifest = self
            .manifest
            .as_ref()
            .ok_or(RoleLoopError::QualityEvidenceMismatch)?;
        if batch.generation() != manifest.generation()
            || batch.workspace_digest() != manifest.workspace_digest()
            || batch.manifest_sha256() != manifest.manifest_sha256()
            || !self.pending_chunks.is_empty()
        {
            return Err(RoleLoopError::QualityEvidenceMismatch);
        }
        self.pending_chunks = batch
            .chunks()
            .iter()
            .map(crate::ReviewDiffChunk::index)
            .collect();
        Ok(())
    }

    /// A retained chunk becomes coverage only after the next provider request
    /// carrying its ToolResult has successfully completed transport.
    fn mark_pending_visible(&mut self) {
        for index in self.pending_chunks.drain(..) {
            if !self.covered_chunks.contains(&index) {
                self.covered_chunks.push(index);
            }
        }
        self.covered_chunks.sort_unstable();
    }

    fn manifest(&self) -> Result<&ReviewDiffManifest, RoleLoopError> {
        self.manifest
            .as_ref()
            .ok_or(RoleLoopError::QualityEvidenceMismatch)
    }

    fn evidence(&self) -> Result<Option<ReviewCoverageEvidence>, RoleLoopError> {
        self.manifest
            .as_ref()
            .map(|manifest| manifest.coverage_evidence(self.covered_chunks.clone()))
            .transpose()
            .map_err(|_| RoleLoopError::QualityEvidenceMismatch)
    }
}

enum ReviewSubmissionFinalization {
    Decision(Box<ValidatedReviewDecision>),
    WorkspaceChanged,
}

pub struct ReviewerRoleLoop {
    engine: RoleEngine,
}

impl ReviewerRoleLoop {
    pub fn new(
        provider: Arc<dyn PreparedModelProvider>,
        runtime: Arc<dyn RoleActionRuntime>,
        events: Arc<dyn RoleEventSink>,
        redactor: Arc<dyn ContextRedactor>,
    ) -> Self {
        Self::from_engine(RoleEngine::new(provider, runtime, events, redactor))
    }

    pub const fn from_engine(engine: RoleEngine) -> Self {
        Self { engine }
    }

    pub const fn engine(&self) -> &RoleEngine {
        &self.engine
    }

    pub async fn run(
        &self,
        mut input: ReviewerRoleInput<'_>,
        ledger: &mut TaskBudgetLedger,
        cancellation: CancellationToken,
    ) -> Result<ReviewerRoleOutcome, RoleLoopError> {
        if cancellation.is_cancelled() {
            return Err(RoleLoopError::Cancelled);
        }
        RoleRun::try_new(Role::Reviewer, input.review_round)?;
        if input.execution.workspace_generation() != input.checkpoint.generation()
            || input.execution.workspace_digest() != &input.checkpoint.workspace_digest()
            || input.execution.tests()
                != &project_test_snapshot(input.required_checks, input.checkpoint)
            || !input
                .required_checks
                .all_current_checks_passed(input.checkpoint)
        {
            return Err(RoleLoopError::QualityEvidenceMismatch);
        }

        let mut lease = ledger.start_reviewer(input.review_round)?;
        let result = self
            .run_active(&mut input, ledger, &mut lease, cancellation.clone())
            .await;
        match result {
            Ok(outcome) => {
                ledger.finish_role(lease)?;
                Ok(outcome)
            }
            Err(error) => {
                ledger.abort_role_on_failure(lease)?;
                Err(error)
            }
        }
    }

    async fn run_active(
        &self,
        input: &mut ReviewerRoleInput<'_>,
        ledger: &mut TaskBudgetLedger,
        lease: &mut crate::RoleBudgetLease,
        cancellation: CancellationToken,
    ) -> Result<ReviewerRoleOutcome, RoleLoopError> {
        let handoff = crate::ContinuationHandoff::try_for_reviewer(
            input.review_round,
            input.task_prompt,
            input.repository_context,
            input.plan,
            input.execution.summary(),
            input.checkpoint,
            input.required_checks,
            input.previous_reviews,
            self.engine.redactor(),
        )?;
        let mut transcript = RoleTranscript::try_fresh(
            RoleRun::try_new(Role::Reviewer, input.review_round)?,
            REVIEWER_SYSTEM_POLICY,
            handoff.into(),
            self.engine.redactor(),
        )?;
        let mut coverage = ReviewerCoverageTracker::default();
        let mut added_checks = Vec::new();
        self.engine
            .emit(
                RoleEvent::Activity(RoleActivityEvent::try_new(
                    Role::Reviewer,
                    input.review_round,
                    "Reviewer started",
                )?),
                cancellation.clone(),
            )
            .await?;

        loop {
            if cancellation.is_cancelled() {
                return Err(RoleLoopError::Cancelled);
            }
            if matches!(
                self.observe_reviewer_fingerprint(
                    input.checkpoint,
                    input.required_checks,
                    cancellation.clone(),
                )
                .await?,
                crate::CheckpointChange::Advanced { .. }
            ) {
                return self
                    .complete_system_invalidation(input, ledger, lease, &added_checks, cancellation)
                    .await;
            }

            let exchange = self
                .engine
                .exploratory_exchange(&transcript, ledger, lease, cancellation.clone())
                .await;
            let (mut receipt, response) = match exchange {
                Ok(exchange) => exchange,
                Err(RoleLoopError::Budget(BudgetError::ReservationWouldBeConsumed {
                    resource: BudgetResource::ModelResponses,
                })) => {
                    return self
                        .run_required_reviewer_actions(
                            input,
                            &mut transcript,
                            ledger,
                            lease,
                            &mut coverage,
                            &mut added_checks,
                            cancellation,
                        )
                        .await;
                }
                Err(error) => return Err(error),
            };
            if let Err(error) = validate_role_response(
                Role::Reviewer,
                &response,
                &ModelToolChoice::Auto,
                self.engine.redactor(),
            ) {
                ledger.discard_invalid_provider_response(lease, &receipt)?;
                if error == RoleContractError::RedactionMutation {
                    return Err(error.into());
                }
                return Err(classify_invalid_reviewer_response(&response));
            }
            let ModelResponse::ToolCalls(batch) = response else {
                ledger.discard_invalid_provider_response(lease, &receipt)?;
                return Err(RoleLoopError::InvalidReviewerOutput);
            };
            if let Err(error) = transcript.preflight_runtime_batch(&batch) {
                ledger.discard_invalid_provider_response(lease, &receipt)?;
                return Err(error.into());
            }

            if let [call] = batch.calls.as_slice()
                && let ActionRequest::Control(control) = &call.request
            {
                return match control.clone() {
                    ControlRequest::SubmitReview(submission) => {
                        if submission.verdict() == ReviewVerdict::Approved {
                            ledger.discard_invalid_provider_response(lease, &receipt)?;
                            return Err(RoleLoopError::InvalidReviewerOutput);
                        }
                        let finalization = self
                            .finalize_review_submission(
                                input,
                                &submission,
                                &coverage,
                                &mut added_checks,
                                cancellation.clone(),
                            )
                            .await;
                        match finalization {
                            Err(error) => {
                                ledger.close_interpreted_exploratory_reviewer_control_on_failure(
                                    lease,
                                    &mut receipt,
                                )?;
                                Err(error)
                            }
                            Ok(ReviewSubmissionFinalization::Decision(decision)) => {
                                ledger.complete_exploratory_early_terminal(
                                    lease,
                                    &mut receipt,
                                    EarlyRoleBudgetTermination::ReviewerChangesRequested,
                                )?;
                                Ok(ReviewerRoleOutcome::Decided(*decision))
                            }
                            Ok(ReviewSubmissionFinalization::WorkspaceChanged) => {
                                let decision = self
                                    .build_system_invalidation(input, &added_checks, cancellation)
                                    .await;
                                match decision {
                                    Ok(decision) => {
                                        ledger.complete_exploratory_early_terminal(
                                            lease,
                                            &mut receipt,
                                            EarlyRoleBudgetTermination::ReviewerChangesRequested,
                                        )?;
                                        Ok(ReviewerRoleOutcome::Decided(decision))
                                    }
                                    Err(error) => {
                                        ledger
                                            .close_interpreted_exploratory_reviewer_control_on_failure(
                                                lease,
                                                &mut receipt,
                                            )?;
                                        Err(error)
                                    }
                                }
                            }
                        }
                    }
                    ControlRequest::ReportBlocked(blocked) => {
                        ledger.complete_exploratory_early_terminal(
                            lease,
                            &mut receipt,
                            EarlyRoleBudgetTermination::ReportBlocked,
                        )?;
                        Ok(ReviewerRoleOutcome::Blocked(blocked_reviewer(blocked)))
                    }
                    _ => {
                        ledger.discard_invalid_provider_response(lease, &receipt)?;
                        Err(RoleLoopError::ReviewerActionNotAllowed)
                    }
                };
            }

            let planned_additions =
                match plan_reviewer_batch_additions(&batch, input.required_checks) {
                    Ok(additions) => additions,
                    Err(error) => {
                        ledger.discard_invalid_provider_response(lease, &receipt)?;
                        return Err(error);
                    }
                };
            let preflight_change = match self
                .observe_reviewer_fingerprint(
                    input.checkpoint,
                    input.required_checks,
                    cancellation.clone(),
                )
                .await
            {
                Ok(change) => change,
                Err(error) => {
                    ledger.discard_invalid_provider_response(lease, &receipt)?;
                    return Err(error);
                }
            };
            if matches!(preflight_change, crate::CheckpointChange::Advanced { .. }) {
                ledger.discard_invalid_provider_response(lease, &receipt)?;
                return self
                    .complete_system_invalidation(input, ledger, lease, &added_checks, cancellation)
                    .await;
            }
            let permit = match ledger.preflight_exploratory_runtime_batch(
                lease,
                &mut receipt,
                batch.calls.clone(),
            ) {
                Ok(permit) => permit,
                Err(BudgetError::ReservationWouldBeConsumed {
                    resource:
                        BudgetResource::ModelVisibleCalls | BudgetResource::RetainedResultBytes,
                }) => {
                    ledger.discard_invalid_provider_response(lease, &receipt)?;
                    return self
                        .run_required_reviewer_actions(
                            input,
                            &mut transcript,
                            ledger,
                            lease,
                            &mut coverage,
                            &mut added_checks,
                            cancellation,
                        )
                        .await;
                }
                Err(error) => {
                    ledger.discard_invalid_provider_response(lease, &receipt)?;
                    return Err(error.into());
                }
            };
            if !planned_additions.is_empty() {
                let added = match input
                    .required_checks
                    .append_checks(planned_additions.clone())
                {
                    Ok(added) => added,
                    Err(error) => {
                        return fail_reviewer_batch(ledger, lease, &permit, &receipt, error.into());
                    }
                };
                if added != planned_additions.len() {
                    return fail_reviewer_batch(
                        ledger,
                        lease,
                        &permit,
                        &receipt,
                        RoleLoopError::ReviewerActionNotAllowed,
                    );
                }
                added_checks.extend(planned_additions);
                if let Err(error) = self
                    .emit_test_snapshot(
                        input.required_checks,
                        input.checkpoint,
                        cancellation.clone(),
                    )
                    .await
                {
                    return fail_reviewer_batch(ledger, lease, &permit, &receipt, error);
                }
            }
            let changed = self
                .execute_reviewer_runtime_batch(
                    &mut transcript,
                    batch,
                    permit,
                    ledger,
                    lease,
                    &receipt,
                    input.checkpoint,
                    input.required_checks,
                    input.review_round,
                    cancellation.clone(),
                )
                .await?;
            if changed {
                return self
                    .complete_system_invalidation(input, ledger, lease, &added_checks, cancellation)
                    .await;
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_required_reviewer_actions(
        &self,
        input: &mut ReviewerRoleInput<'_>,
        transcript: &mut RoleTranscript,
        ledger: &mut TaskBudgetLedger,
        lease: &mut crate::RoleBudgetLease,
        coverage: &mut ReviewerCoverageTracker,
        added_checks: &mut Vec<RequiredCheck>,
        cancellation: CancellationToken,
    ) -> Result<ReviewerRoleOutcome, RoleLoopError> {
        loop {
            if matches!(
                self.observe_reviewer_fingerprint(
                    input.checkpoint,
                    input.required_checks,
                    cancellation.clone(),
                )
                .await?,
                crate::CheckpointChange::Advanced { .. }
            ) {
                return self
                    .complete_system_invalidation(input, ledger, lease, added_checks, cancellation)
                    .await;
            }
            let next = lease
                .next_required_action()
                .cloned()
                .ok_or(BudgetError::NoRequiredActionPending)?;
            let required = match next {
                crate::RequiredBudgetAction::ReviewerManifest => {
                    RequiredAction::review_diff_manifest_or_terminal(
                        input.checkpoint.generation(),
                        input.checkpoint.workspace_digest(),
                    )?
                }
                crate::RequiredBudgetAction::ReviewerChunkBatch {
                    start_chunk, count, ..
                } => {
                    let manifest = coverage.manifest()?;
                    RequiredAction::review_diff_chunks_or_terminal(
                        manifest.generation(),
                        manifest.workspace_digest().clone(),
                        manifest.manifest_sha256().to_owned(),
                        start_chunk,
                        count,
                    )?
                }
                crate::RequiredBudgetAction::ReviewerTerminal => {
                    RequiredAction::terminal_or_blocked(crate::ControlKind::SubmitReview)?
                }
                _ => return Err(RoleLoopError::ReviewerActionNotAllowed),
            };
            let mut action = ledger.begin_required_action(lease)?;
            let exchange = self
                .engine
                .required_exchange(
                    transcript,
                    required.clone(),
                    ledger,
                    lease,
                    &mut action,
                    cancellation.clone(),
                )
                .await;
            let (mut receipt, response) = match exchange {
                Ok(exchange) => exchange,
                Err(error) => return Err(error),
            };
            // This successful transport is the first proof that any previous
            // retained chunk result entered a subsequent provider request.
            coverage.mark_pending_visible();
            let post_exchange_change = match self
                .observe_reviewer_fingerprint(
                    input.checkpoint,
                    input.required_checks,
                    cancellation.clone(),
                )
                .await
            {
                Ok(change) => change,
                Err(error) => {
                    ledger.discard_invalid_provider_response(lease, &receipt)?;
                    ledger.abandon_required_action_on_system_invalidation(lease, action)?;
                    return Err(error);
                }
            };
            if matches!(
                post_exchange_change,
                crate::CheckpointChange::Advanced { .. }
            ) {
                ledger.discard_invalid_provider_response(lease, &receipt)?;
                ledger.abandon_required_action_on_system_invalidation(lease, action)?;
                return self
                    .complete_system_invalidation(input, ledger, lease, added_checks, cancellation)
                    .await;
            }
            if let Err(error) = validate_role_response(
                Role::Reviewer,
                &response,
                &ModelToolChoice::Required(required.clone()),
                self.engine.redactor(),
            ) {
                ledger.discard_invalid_provider_response(lease, &receipt)?;
                if error == RoleContractError::RedactionMutation {
                    return Err(error.into());
                }
                return Err(classify_invalid_reviewer_response(&response));
            }
            let ModelResponse::ToolCalls(batch) = response else {
                ledger.discard_invalid_provider_response(lease, &receipt)?;
                return Err(RoleLoopError::InvalidReviewerOutput);
            };
            if let Err(error) = transcript.preflight_runtime_batch(&batch) {
                ledger.discard_invalid_provider_response(lease, &receipt)?;
                return Err(error.into());
            }

            if let [call] = batch.calls.as_slice()
                && let ActionRequest::Control(control) = &call.request
            {
                return match control.clone() {
                    ControlRequest::SubmitReview(submission) => {
                        if submission.verdict() == ReviewVerdict::Approved
                            && next != crate::RequiredBudgetAction::ReviewerTerminal
                        {
                            ledger.close_interpreted_required_reviewer_control_on_failure(
                                lease,
                                &mut action,
                                &mut receipt,
                            )?;
                            return Err(RoleLoopError::InvalidReviewerOutput);
                        }
                        let finalization = self
                            .finalize_review_submission(
                                input,
                                &submission,
                                coverage,
                                added_checks,
                                cancellation.clone(),
                            )
                            .await;
                        match finalization {
                            Err(error) => {
                                ledger.close_interpreted_required_reviewer_control_on_failure(
                                    lease,
                                    &mut action,
                                    &mut receipt,
                                )?;
                                Err(error)
                            }
                            Ok(ReviewSubmissionFinalization::Decision(decision)) => {
                                match submission.verdict() {
                                    ReviewVerdict::Approved => {
                                        if let Err(error) = ledger.charge_required_call(
                                            lease,
                                            &mut action,
                                            &mut receipt,
                                        ) {
                                            ledger.discard_invalid_provider_response(
                                                lease, &receipt,
                                            )?;
                                            return Err(error.into());
                                        }
                                        ledger.finish_provider_response(lease, &receipt)?;
                                        ledger.complete_required_action(lease, action)?;
                                    }
                                    ReviewVerdict::ChangesRequested => {
                                        ledger.complete_required_early_terminal(
                                            lease,
                                            action,
                                            &mut receipt,
                                            EarlyRoleBudgetTermination::ReviewerChangesRequested,
                                        )?;
                                    }
                                }
                                Ok(ReviewerRoleOutcome::Decided(*decision))
                            }
                            Ok(ReviewSubmissionFinalization::WorkspaceChanged) => {
                                let decision = self
                                    .build_system_invalidation(input, added_checks, cancellation)
                                    .await;
                                match decision {
                                    Ok(decision) => {
                                        ledger.complete_required_early_terminal(
                                            lease,
                                            action,
                                            &mut receipt,
                                            EarlyRoleBudgetTermination::ReviewerChangesRequested,
                                        )?;
                                        Ok(ReviewerRoleOutcome::Decided(decision))
                                    }
                                    Err(error) => {
                                        ledger
                                            .close_interpreted_required_reviewer_control_on_failure(
                                                lease,
                                                &mut action,
                                                &mut receipt,
                                            )?;
                                        Err(error)
                                    }
                                }
                            }
                        }
                    }
                    ControlRequest::ReportBlocked(blocked) => {
                        ledger.complete_required_early_terminal(
                            lease,
                            action,
                            &mut receipt,
                            EarlyRoleBudgetTermination::ReportBlocked,
                        )?;
                        Ok(ReviewerRoleOutcome::Blocked(blocked_reviewer(blocked)))
                    }
                    _ => {
                        ledger.discard_invalid_provider_response(lease, &receipt)?;
                        Err(RoleLoopError::ReviewerActionNotAllowed)
                    }
                };
            }

            if matches!(
                next,
                crate::RequiredBudgetAction::ReviewerManifest
                    | crate::RequiredBudgetAction::ReviewerChunkBatch { .. }
            ) {
                let runtime_result = match self
                    .engine
                    .execute_required_runtime_action_with_failure(
                        transcript,
                        batch,
                        &required,
                        action,
                        ledger,
                        lease,
                        &mut receipt,
                        cancellation.clone(),
                    )
                    .await
                {
                    Ok(result) => result,
                    Err(failure) => {
                        let (error, aborted) = failure.into_parts();
                        let Some(aborted) = aborted else {
                            return Err(error);
                        };
                        if !is_workspace_change_role_error(&error) {
                            return Err(error);
                        }
                        if matches!(
                            self.observe_reviewer_fingerprint(
                                input.checkpoint,
                                input.required_checks,
                                cancellation.clone(),
                            )
                            .await,
                            Ok(crate::CheckpointChange::Advanced { .. })
                        ) {
                            let decision = self
                                .build_system_invalidation(input, added_checks, cancellation)
                                .await?;
                            ledger.complete_reviewer_system_invalidation_after_required_failure(
                                lease, aborted,
                            )?;
                            return Ok(ReviewerRoleOutcome::Decided(decision));
                        }
                        return Err(error);
                    }
                };
                let changed = matches!(
                    self.observe_reviewer_fingerprint(
                        input.checkpoint,
                        input.required_checks,
                        cancellation.clone(),
                    )
                    .await?,
                    crate::CheckpointChange::Advanced { .. }
                );
                match runtime_result {
                    RoleRuntimeResult::ReviewDiffManifest(manifest) => {
                        coverage.bind_manifest(manifest)?;
                    }
                    RoleRuntimeResult::ReviewDiffChunks(chunks) => {
                        coverage.record_chunk_result(&chunks)?;
                    }
                    _ => return Err(RoleLoopError::RuntimeResultMismatch),
                }
                if changed {
                    return self
                        .complete_system_invalidation(
                            input,
                            ledger,
                            lease,
                            added_checks,
                            cancellation,
                        )
                        .await;
                }
                continue;
            }

            ledger.discard_invalid_provider_response(lease, &receipt)?;
            return Err(RoleLoopError::ReviewerActionNotAllowed);
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn finalize_review_submission(
        &self,
        input: &mut ReviewerRoleInput<'_>,
        submission: &crate::ReviewSubmission,
        coverage: &ReviewerCoverageTracker,
        added_checks: &mut Vec<RequiredCheck>,
        cancellation: CancellationToken,
    ) -> Result<ReviewSubmissionFinalization, RoleLoopError> {
        let findings = submission
            .findings()
            .iter()
            .enumerate()
            .map(|(index, finding)| {
                ReviewFinding::try_for_review(
                    input.review_round as u8,
                    index + 1,
                    finding.severity(),
                    finding.message(),
                    finding.path().map(str::to_owned),
                    finding.line(),
                )
                .map_err(|_| RoleLoopError::InvalidReviewerOutput)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let terminal_additions =
            plan_reviewer_submission_additions(submission, input.required_checks)?;
        if submission.verdict() == ReviewVerdict::Approved && !terminal_additions.is_empty() {
            return Err(RoleLoopError::InvalidReviewerOutput);
        }
        if matches!(
            self.observe_reviewer_fingerprint(
                input.checkpoint,
                input.required_checks,
                cancellation.clone(),
            )
            .await?,
            crate::CheckpointChange::Advanced { .. }
        ) {
            return Ok(ReviewSubmissionFinalization::WorkspaceChanged);
        }
        let terminal = match self
            .engine
            .runtime
            .terminal_snapshot(input.checkpoint.generation(), cancellation.clone())
            .await
        {
            Ok(terminal) => terminal,
            Err(error) if is_workspace_change_runtime_error(&error) => {
                let original = map_runtime_error(error);
                if matches!(
                    self.observe_reviewer_fingerprint(
                        input.checkpoint,
                        input.required_checks,
                        cancellation.clone(),
                    )
                    .await,
                    Ok(crate::CheckpointChange::Advanced { .. })
                ) {
                    return Ok(ReviewSubmissionFinalization::WorkspaceChanged);
                }
                return Err(original);
            }
            Err(error) => return Err(map_runtime_error(error)),
        };
        if terminal.diff.revision != input.checkpoint.generation() {
            return Err(RoleLoopError::QualityEvidenceMismatch);
        }
        if matches!(
            input.checkpoint.observe_stable(terminal.fingerprint)?,
            crate::CheckpointChange::Advanced { .. }
        ) {
            self.emit_test_snapshot(
                input.required_checks,
                input.checkpoint,
                cancellation.clone(),
            )
            .await?;
            return Ok(ReviewSubmissionFinalization::WorkspaceChanged);
        }
        let terminal_diff = sanitize_terminal_diff(&terminal.diff, self.engine.redactor())?;

        let coverage_evidence = coverage.evidence()?;
        if submission.verdict() == ReviewVerdict::Approved {
            let manifest = coverage.manifest()?;
            crate::validate_review_coverage(manifest, coverage_evidence.as_ref(), true)
                .map_err(|_| RoleLoopError::InvalidReviewerOutput)?;
            let review_checkpoint =
                crate::ReviewDiffCheckpoint::from_workspace_checkpoint(input.checkpoint);
            let terminal_manifest = match self
                .engine
                .runtime
                .terminal_review_diff_manifest(review_checkpoint, cancellation.clone())
                .await
            {
                Ok(manifest) => manifest,
                Err(error) if is_workspace_change_runtime_error(&error) => {
                    let original = map_runtime_error(error);
                    if matches!(
                        self.observe_reviewer_fingerprint(
                            input.checkpoint,
                            input.required_checks,
                            cancellation.clone(),
                        )
                        .await,
                        Ok(crate::CheckpointChange::Advanced { .. })
                    ) {
                        return Ok(ReviewSubmissionFinalization::WorkspaceChanged);
                    }
                    return Err(original);
                }
                Err(error) => return Err(map_runtime_error(error)),
            };
            crate::validate_terminal_review_coverage(
                coverage_evidence
                    .as_ref()
                    .ok_or(RoleLoopError::InvalidReviewerOutput)?,
                &terminal_manifest,
            )
            .map_err(|_| RoleLoopError::QualityEvidenceMismatch)?;
            validate_terminal_diff_manifest(&terminal_diff, &terminal_manifest)?;
        }
        if matches!(
            self.observe_reviewer_fingerprint(
                input.checkpoint,
                input.required_checks,
                cancellation.clone(),
            )
            .await?,
            crate::CheckpointChange::Advanced { .. }
        ) {
            return Ok(ReviewSubmissionFinalization::WorkspaceChanged);
        }

        // Submission-only checks do not become authoritative until every
        // trusted terminal recapture has proved the workspace stable.
        let added_before_terminal = added_checks.len();
        if !terminal_additions.is_empty() {
            let added = input
                .required_checks
                .append_checks(terminal_additions.clone())?;
            if added != terminal_additions.len() {
                return Err(RoleLoopError::InvalidReviewerOutput);
            }
            added_checks.extend(terminal_additions.clone());
        }

        let finalization = async {
            let evidence = NewReviewEvidence::try_new(
                input.review_round as u8,
                ReviewDecisionSource::Reviewer,
                input.checkpoint.generation(),
                input.checkpoint.workspace_digest(),
                submission.verdict(),
                submission.summary(),
                findings,
                added_checks.clone(),
                input.required_checks.checks().to_vec(),
                input.required_checks.current_evidence(input.checkpoint),
                coverage_evidence,
            )
            .map_err(|_| RoleLoopError::InvalidReviewerOutput)?;
            input
                .required_checks
                .validate_review_evidence(input.checkpoint, &evidence)?;
            self.engine
                .emit(RoleEvent::Diff(terminal_diff), cancellation.clone())
                .await?;
            self.emit_test_snapshot(
                input.required_checks,
                input.checkpoint,
                cancellation.clone(),
            )
            .await?;
            let ack = self
                .engine
                .flush_checkpoint(input.checkpoint.generation(), cancellation.clone())
                .await?;
            if matches!(
                self.observe_reviewer_barrier_fingerprint(input.checkpoint, cancellation.clone(),)
                    .await?,
                crate::CheckpointChange::Advanced { .. }
            ) {
                return Ok(ReviewSubmissionFinalization::WorkspaceChanged);
            }
            Ok(ReviewSubmissionFinalization::Decision(Box::new(
                ValidatedReviewDecision {
                    evidence,
                    durable_sequence: ack.sequence(),
                },
            )))
        }
        .await;

        if !terminal_additions.is_empty()
            && !matches!(finalization, Ok(ReviewSubmissionFinalization::Decision(_)))
        {
            input
                .required_checks
                .rollback_unaccepted_review_checks(input.checkpoint, &terminal_additions)?;
            added_checks.truncate(added_before_terminal);
        }
        finalization
    }

    async fn complete_system_invalidation(
        &self,
        input: &mut ReviewerRoleInput<'_>,
        ledger: &mut TaskBudgetLedger,
        lease: &mut crate::RoleBudgetLease,
        added_checks: &[RequiredCheck],
        cancellation: CancellationToken,
    ) -> Result<ReviewerRoleOutcome, RoleLoopError> {
        let decision = self
            .build_system_invalidation(input, added_checks, cancellation)
            .await?;
        ledger.complete_reviewer_system_invalidation(lease)?;
        Ok(ReviewerRoleOutcome::Decided(decision))
    }

    async fn build_system_invalidation(
        &self,
        input: &mut ReviewerRoleInput<'_>,
        added_checks: &[RequiredCheck],
        cancellation: CancellationToken,
    ) -> Result<ValidatedReviewDecision, RoleLoopError> {
        for _ in 0..2 {
            let terminal = self
                .stable_terminal_panel(
                    input.checkpoint,
                    input.required_checks,
                    cancellation.clone(),
                )
                .await?;
            let terminal_diff = sanitize_terminal_diff(&terminal.diff, self.engine.redactor())?;
            let evidence = NewReviewEvidence::try_new(
                input.review_round as u8,
                ReviewDecisionSource::System,
                input.checkpoint.generation(),
                input.checkpoint.workspace_digest(),
                ReviewVerdict::ChangesRequested,
                "Reviewer validation changed the deliverable workspace",
                vec![
                    ReviewFinding::system_workspace_changed(input.review_round as u8)
                        .map_err(|_| RoleLoopError::QualityEvidenceMismatch)?,
                ],
                added_checks.to_vec(),
                input.required_checks.checks().to_vec(),
                Vec::new(),
                None,
            )
            .map_err(|_| RoleLoopError::QualityEvidenceMismatch)?;
            input
                .required_checks
                .validate_review_evidence(input.checkpoint, &evidence)?;
            self.engine
                .emit(RoleEvent::Diff(terminal_diff), cancellation.clone())
                .await?;
            self.emit_test_snapshot(
                input.required_checks,
                input.checkpoint,
                cancellation.clone(),
            )
            .await?;
            let ack = self
                .engine
                .flush_checkpoint(input.checkpoint.generation(), cancellation.clone())
                .await?;
            if matches!(
                self.observe_reviewer_barrier_fingerprint(input.checkpoint, cancellation.clone(),)
                    .await?,
                crate::CheckpointChange::Advanced { .. }
            ) {
                continue;
            }
            return Ok(ValidatedReviewDecision {
                evidence,
                durable_sequence: ack.sequence(),
            });
        }
        Err(RoleLoopError::QualityEvidenceMismatch)
    }

    async fn stable_terminal_panel(
        &self,
        checkpoint: &mut WorkspaceCheckpoint,
        required_checks: &RequiredCheckLedger,
        cancellation: CancellationToken,
    ) -> Result<TerminalSnapshot, RoleLoopError> {
        for _ in 0..2 {
            let terminal = match self
                .engine
                .runtime
                .terminal_snapshot(checkpoint.generation(), cancellation.clone())
                .await
            {
                Ok(terminal) => terminal,
                Err(error) if is_workspace_change_runtime_error(&error) => {
                    let original = map_runtime_error(error);
                    if matches!(
                        self.observe_reviewer_fingerprint(
                            checkpoint,
                            required_checks,
                            cancellation.clone(),
                        )
                        .await,
                        Ok(crate::CheckpointChange::Advanced { .. })
                    ) {
                        continue;
                    }
                    return Err(original);
                }
                Err(error) => return Err(map_runtime_error(error)),
            };
            if terminal.diff.revision != checkpoint.generation() {
                return Err(RoleLoopError::QualityEvidenceMismatch);
            }
            if matches!(
                checkpoint.observe_stable(terminal.fingerprint)?,
                crate::CheckpointChange::Advanced { .. }
            ) {
                self.emit_test_snapshot(required_checks, checkpoint, cancellation.clone())
                    .await?;
                continue;
            }
            return Ok(terminal);
        }
        Err(RoleLoopError::QualityEvidenceMismatch)
    }

    async fn observe_reviewer_fingerprint(
        &self,
        checkpoint: &mut WorkspaceCheckpoint,
        required_checks: &RequiredCheckLedger,
        cancellation: CancellationToken,
    ) -> Result<crate::CheckpointChange, RoleLoopError> {
        if cancellation.is_cancelled() {
            return Err(RoleLoopError::Cancelled);
        }
        let fingerprint = self
            .engine
            .runtime
            .workspace_fingerprint(cancellation.clone())
            .await
            .map_err(map_runtime_error)?;
        if cancellation.is_cancelled() {
            return Err(RoleLoopError::Cancelled);
        }
        let change = checkpoint.observe_stable(fingerprint)?;
        if matches!(change, crate::CheckpointChange::Advanced { .. }) {
            self.emit_test_snapshot(required_checks, checkpoint, cancellation)
                .await?;
        }
        Ok(change)
    }

    async fn observe_reviewer_barrier_fingerprint(
        &self,
        checkpoint: &mut WorkspaceCheckpoint,
        cancellation: CancellationToken,
    ) -> Result<crate::CheckpointChange, RoleLoopError> {
        if cancellation.is_cancelled() {
            return Err(RoleLoopError::Cancelled);
        }
        let fingerprint = self
            .engine
            .runtime
            .workspace_fingerprint(cancellation.clone())
            .await
            .map_err(map_runtime_error)?;
        if cancellation.is_cancelled() {
            return Err(RoleLoopError::Cancelled);
        }
        checkpoint.observe_stable(fingerprint).map_err(Into::into)
    }

    async fn emit_test_snapshot(
        &self,
        required_checks: &RequiredCheckLedger,
        checkpoint: &WorkspaceCheckpoint,
        cancellation: CancellationToken,
    ) -> Result<(), RoleLoopError> {
        self.engine
            .emit(
                RoleEvent::Tests(project_test_snapshot(required_checks, checkpoint)),
                cancellation,
            )
            .await
    }

    async fn abandon_reviewer_check_run(
        &self,
        required_checks: &mut RequiredCheckLedger,
        checkpoint: &WorkspaceCheckpoint,
        token: crate::CheckRunToken,
        cancellation: CancellationToken,
    ) -> Result<(), RoleLoopError> {
        required_checks.abandon_check_run(token)?;
        if !cancellation.is_cancelled() {
            let _ = self
                .emit_test_snapshot(required_checks, checkpoint, cancellation)
                .await;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_reviewer_runtime_batch(
        &self,
        transcript: &mut RoleTranscript,
        batch: crate::ToolCallBatch,
        permit: crate::ExploratoryBatchPermit,
        ledger: &mut TaskBudgetLedger,
        lease: &mut crate::RoleBudgetLease,
        receipt: &crate::ProviderResponseReceipt,
        checkpoint: &mut WorkspaceCheckpoint,
        required_checks: &mut RequiredCheckLedger,
        role_run: u32,
        cancellation: CancellationToken,
    ) -> Result<bool, RoleLoopError> {
        if ensure_transcript_matches_lease(transcript, lease).is_err()
            || validate_action_batch(
                Role::Reviewer,
                &batch,
                &ModelToolChoice::Auto,
                self.engine.redactor(),
            )
            .is_err()
            || transcript.preflight_runtime_batch(&batch).is_err()
            || batch.calls.len() != permit.invocations().len()
        {
            return fail_reviewer_batch(
                ledger,
                lease,
                &permit,
                receipt,
                RoleLoopError::ReviewerActionNotAllowed,
            );
        }
        let execution = async {
            let mut retained_results = Vec::with_capacity(permit.invocations().len());
            for invocation in permit.invocations() {
                if matches!(
                    self.observe_reviewer_fingerprint(
                        checkpoint,
                        required_checks,
                        cancellation.clone(),
                    )
                    .await?,
                    crate::CheckpointChange::Advanced { .. }
                ) {
                    return Ok::<_, RoleLoopError>((true, retained_results));
                }
                let (runtime_request, validation_check) = match invocation.request() {
                    RuntimeActionRequest::ValidationSelector { selector } => {
                        let check = required_checks
                            .check_by_selector(selector)
                            .cloned()
                            .ok_or(RoleLoopError::ReviewerActionNotAllowed)?;
                        (
                            RuntimeActionRequest::Validation {
                                check: check.clone(),
                            },
                            Some(check),
                        )
                    }
                    RuntimeActionRequest::Tool(request) => {
                        (RuntimeActionRequest::Tool(request.clone()), None)
                    }
                    _ => return Err(RoleLoopError::ReviewerActionNotAllowed),
                };
                let mut token = if let Some(check) = &validation_check {
                    required_checks.queue_check(checkpoint, check.id())?;
                    if let Err(error) = self
                        .emit_test_snapshot(required_checks, checkpoint, cancellation.clone())
                        .await
                    {
                        required_checks.abandon_queued_check(checkpoint, check.id())?;
                        return Err(error);
                    }
                    let token = required_checks.mark_check_running(
                        checkpoint,
                        check.id(),
                        CheckActor::Reviewer,
                        role_run,
                    )?;
                    if let Err(error) = self
                        .emit_test_snapshot(required_checks, checkpoint, cancellation.clone())
                        .await
                    {
                        required_checks.abandon_check_run(token)?;
                        return Err(error);
                    }
                    Some(token)
                } else {
                    None
                };
                let runtime_result = match self
                    .engine
                    .invoke_runtime(runtime_request, cancellation.clone())
                    .await
                {
                    Ok(result) => result,
                    Err(error) if is_workspace_change_role_error(&error) => {
                        if let Some(token) = token.take() {
                            self.abandon_reviewer_check_run(
                                required_checks,
                                checkpoint,
                                token,
                                cancellation.clone(),
                            )
                            .await?;
                        }
                        if matches!(
                            self.observe_reviewer_fingerprint(
                                checkpoint,
                                required_checks,
                                cancellation.clone(),
                            )
                            .await,
                            Ok(crate::CheckpointChange::Advanced { .. })
                        ) {
                            return Ok((true, retained_results));
                        }
                        return Err(error);
                    }
                    Err(error) => {
                        if let Some(token) = token.take() {
                            self.abandon_reviewer_check_run(
                                required_checks,
                                checkpoint,
                                token,
                                cancellation.clone(),
                            )
                            .await?;
                        }
                        return Err(error);
                    }
                };
                let post_runtime_change = self
                    .observe_reviewer_fingerprint(checkpoint, required_checks, cancellation.clone())
                    .await;
                let post_runtime_change = match post_runtime_change {
                    Ok(change) => change,
                    Err(error) => {
                        if let Some(token) = token.take() {
                            self.abandon_reviewer_check_run(
                                required_checks,
                                checkpoint,
                                token,
                                cancellation.clone(),
                            )
                            .await?;
                        }
                        return Err(error);
                    }
                };
                if matches!(
                    post_runtime_change,
                    crate::CheckpointChange::Advanced { .. }
                ) {
                    if let Some(token) = token.take() {
                        self.abandon_reviewer_check_run(
                            required_checks,
                            checkpoint,
                            token,
                            cancellation.clone(),
                        )
                        .await?;
                    }
                    return Ok((true, retained_results));
                }
                let tool_result = match (&runtime_result, &validation_check) {
                    (RoleRuntimeResult::Tool(result), None) => result,
                    (RoleRuntimeResult::Validation(observation), Some(check)) => {
                        let token = token.take().ok_or(RoleLoopError::RuntimeResultMismatch)?;
                        if observation.check() != check {
                            self.abandon_reviewer_check_run(
                                required_checks,
                                checkpoint,
                                token,
                                cancellation.clone(),
                            )
                            .await?;
                            return Err(RoleLoopError::RuntimeResultMismatch);
                        }
                        let redacted = self
                            .engine
                            .redactor()
                            .redact(observation.model_result().content());
                        let (summary, summary_truncated) = bounded_evidence_summary(&redacted);
                        required_checks.finish_check(
                            checkpoint,
                            token,
                            observation.status(),
                            observation.duration_ms(),
                            summary,
                            observation.truncated() || summary_truncated,
                        )?;
                        self.emit_test_snapshot(required_checks, checkpoint, cancellation.clone())
                            .await?;
                        observation.model_result()
                    }
                    _ => {
                        if let Some(token) = token.take() {
                            self.abandon_reviewer_check_run(
                                required_checks,
                                checkpoint,
                                token,
                                cancellation.clone(),
                            )
                            .await?;
                        }
                        return Err(RoleLoopError::RuntimeResultMismatch);
                    }
                };
                retained_results.push(RetainedToolResult::try_from_tool_result_with_limit(
                    invocation.tool_call_id(),
                    tool_result,
                    self.engine.redactor(),
                    invocation.wrapper_cap(),
                )?);
            }
            Ok((false, retained_results))
        }
        .await;
        let (changed, retained_results) = match execution {
            Ok(result) => result,
            Err(error) => {
                return fail_reviewer_batch(ledger, lease, &permit, receipt, error);
            }
        };
        if changed {
            release_reviewer_batch(ledger, lease, &permit, receipt)?;
            return Ok(true);
        }
        if let Err(error) =
            ledger.retain_exploratory_batch_results(lease, &permit, &retained_results)
        {
            return fail_reviewer_batch(ledger, lease, &permit, receipt, error.into());
        }
        if let Err(error) = transcript.append_runtime_batch(batch, retained_results) {
            ledger.finish_provider_response(lease, receipt)?;
            return Err(error.into());
        }
        ledger.finish_provider_response(lease, receipt)?;
        Ok(false)
    }
}

fn plan_reviewer_batch_additions(
    batch: &crate::ToolCallBatch,
    required_checks: &RequiredCheckLedger,
) -> Result<Vec<RequiredCheck>, RoleLoopError> {
    let mut selectors = Vec::new();
    let mut seen = HashSet::new();
    for call in &batch.calls {
        match &call.request {
            ActionRequest::Runtime(RuntimeActionRequest::Tool(_)) => {}
            ActionRequest::Runtime(RuntimeActionRequest::ValidationSelector { selector }) => {
                if !seen.insert(selector.clone()) {
                    return Err(RoleLoopError::ReviewerActionNotAllowed);
                }
                selectors.push(selector.clone());
            }
            _ => return Err(RoleLoopError::ReviewerActionNotAllowed),
        }
    }
    plan_reviewer_check_additions(selectors, required_checks)
        .map_err(|_| RoleLoopError::ReviewerActionNotAllowed)
}

fn plan_reviewer_submission_additions(
    submission: &crate::ReviewSubmission,
    required_checks: &RequiredCheckLedger,
) -> Result<Vec<RequiredCheck>, RoleLoopError> {
    let selectors = submission
        .add_required_checks()
        .iter()
        .map(crate::CheckSelectorSubmission::selector)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| RoleLoopError::InvalidReviewerOutput)?;
    plan_reviewer_check_additions(selectors, required_checks)
        .map_err(|_| RoleLoopError::InvalidReviewerOutput)
}

fn plan_reviewer_check_additions(
    selectors: Vec<RequiredCheckSelector>,
    required_checks: &RequiredCheckLedger,
) -> Result<Vec<RequiredCheck>, ()> {
    let mut known = required_checks
        .checks()
        .iter()
        .map(|check| check.selector().clone())
        .collect::<HashSet<_>>();
    let mut additions = Vec::new();
    for selector in selectors {
        if known.insert(selector.clone()) {
            let ordinal = required_checks.checks().len() + additions.len() + 1;
            additions.push(
                RequiredCheck::try_from_selector(format!("check-{ordinal:02}"), selector)
                    .map_err(|_| ())?,
            );
        }
    }
    if required_checks.checks().len() + additions.len() > crate::MAX_REQUIRED_CHECKS {
        return Err(());
    }
    Ok(additions)
}

fn release_reviewer_batch(
    ledger: &mut TaskBudgetLedger,
    lease: &mut crate::RoleBudgetLease,
    permit: &crate::ExploratoryBatchPermit,
    receipt: &crate::ProviderResponseReceipt,
) -> Result<(), RoleLoopError> {
    ledger.abort_exploratory_runtime_batch(lease, permit)?;
    ledger.finish_provider_response(lease, receipt)?;
    Ok(())
}

fn fail_reviewer_batch<T>(
    ledger: &mut TaskBudgetLedger,
    lease: &mut crate::RoleBudgetLease,
    permit: &crate::ExploratoryBatchPermit,
    receipt: &crate::ProviderResponseReceipt,
    error: RoleLoopError,
) -> Result<T, RoleLoopError> {
    release_reviewer_batch(ledger, lease, permit, receipt)?;
    Err(error)
}

fn blocked_reviewer(submission: BlockedSubmission) -> BlockedReviewer {
    let (suffix, retryable, message) = match submission.reason() {
        crate::BlockedReason::MissingRequiredContext => (
            "MISSING_CONTEXT",
            true,
            "Reviewer is missing required context",
        ),
        crate::BlockedReason::ConflictingUserRequirements => (
            "CONFLICTING_REQUIREMENTS",
            false,
            "Reviewer found conflicting user requirements",
        ),
        crate::BlockedReason::RequiresGoalChange => (
            "REQUIRES_GOAL_CHANGE",
            false,
            "Reviewer requires a task goal change",
        ),
        crate::BlockedReason::UnsupportedScope => (
            "UNSUPPORTED_SCOPE",
            false,
            "Reviewer found unsupported task scope",
        ),
    };
    BlockedReviewer {
        submission,
        stage_failure: RoleStageFailure {
            status: TaskStatus::Failed,
            delivery_readiness: DeliveryReadiness::Unreviewed,
            failure: TaskFailure {
                code: format!("REVIEWER_BLOCKED_{suffix}"),
                message: message.to_owned(),
                retryable,
            },
        },
    }
}

fn validate_terminal_diff_manifest(
    diff: &DiffEvent,
    manifest: &ReviewDiffManifest,
) -> Result<(), RoleLoopError> {
    if diff.files.iter().any(|file| file.truncated) {
        return Err(RoleLoopError::TerminalDiffTruncated);
    }
    if diff.revision != manifest.generation() || diff.files.len() != manifest.files().len() {
        return Err(RoleLoopError::QualityEvidenceMismatch);
    }
    for (file, authority) in diff.files.iter().zip(manifest.files()) {
        let mut hasher = Sha256::new();
        hasher.update(file.patch.as_bytes());
        let patch_sha256 = format!("{:x}", hasher.finalize());
        if file.path != authority.path()
            || file.status != authority.status()
            || file.additions != authority.additions()
            || file.deletions != authority.deletions()
            || u64::try_from(file.patch.len()).ok() != Some(authority.patch_bytes())
            || patch_sha256 != authority.patch_sha256()
        {
            return Err(RoleLoopError::QualityEvidenceMismatch);
        }
    }
    Ok(())
}

fn classify_invalid_reviewer_response(response: &ModelResponse) -> RoleLoopError {
    match response {
        ModelResponse::Final { .. } => RoleLoopError::InvalidReviewerOutput,
        ModelResponse::ToolCalls(batch)
            if matches!(
                batch.calls.as_slice(),
                [crate::ToolCall {
                    request: ActionRequest::Control(ControlRequest::SubmitReview(_)),
                    ..
                }]
            ) =>
        {
            RoleLoopError::InvalidReviewerOutput
        }
        ModelResponse::ToolCalls(_) => RoleLoopError::ReviewerActionNotAllowed,
    }
}

fn is_workspace_change_role_error(error: &RoleLoopError) -> bool {
    matches!(
        error,
        RoleLoopError::Runtime(runtime) if is_workspace_change_runtime_error(runtime)
    )
}

fn is_workspace_change_runtime_error(error: &RuntimeError) -> bool {
    matches!(
        error.code.as_str(),
        "WORKSPACE_CHANGED" | "WORKTREE_CHANGED_DURING_DIFF"
    )
}

fn sanitize_terminal_diff(
    diff: &DiffEvent,
    redactor: &dyn ContextRedactor,
) -> Result<DiffEvent, RoleLoopError> {
    let mut safe = diff.clone();
    for file in &mut safe.files {
        if redactor.redact(&file.path) != file.path {
            return Err(RoleLoopError::QualityEvidenceMismatch);
        }
        let patch = redactor.redact(&file.patch);
        if redactor.redact(&patch) != patch {
            return Err(RoleLoopError::QualityEvidenceMismatch);
        }
        file.patch = patch;
    }
    Ok(safe)
}

fn blocked_executor(submission: BlockedSubmission) -> BlockedExecutor {
    let (suffix, retryable, message) = match submission.reason() {
        crate::BlockedReason::MissingRequiredContext => (
            "MISSING_CONTEXT",
            true,
            "Executor is missing required context",
        ),
        crate::BlockedReason::ConflictingUserRequirements => (
            "CONFLICTING_REQUIREMENTS",
            false,
            "Executor found conflicting user requirements",
        ),
        crate::BlockedReason::RequiresGoalChange => (
            "REQUIRES_GOAL_CHANGE",
            false,
            "Executor requires a task goal change",
        ),
        crate::BlockedReason::UnsupportedScope => (
            "UNSUPPORTED_SCOPE",
            false,
            "Executor found unsupported task scope",
        ),
    };
    BlockedExecutor {
        submission,
        stage_failure: RoleStageFailure {
            status: TaskStatus::Failed,
            delivery_readiness: DeliveryReadiness::Unreviewed,
            failure: TaskFailure {
                code: format!("EXECUTOR_BLOCKED_{suffix}"),
                message: message.to_owned(),
                retryable,
            },
        },
    }
}

fn validate_executor_runtime_batch(
    batch: &crate::ToolCallBatch,
    required_checks: &RequiredCheckLedger,
) -> Result<(), RoleLoopError> {
    let mut validation_ids = HashSet::new();
    for call in &batch.calls {
        match &call.request {
            ActionRequest::Runtime(RuntimeActionRequest::Tool(_)) => {}
            ActionRequest::Runtime(RuntimeActionRequest::ValidationSelector { selector }) => {
                let check = required_checks
                    .check_by_selector(selector)
                    .ok_or(RoleLoopError::ExecutorActionNotAllowed)?;
                if !validation_ids.insert(check.id()) {
                    return Err(RoleLoopError::ExecutorActionNotAllowed);
                }
            }
            _ => return Err(RoleLoopError::ExecutorActionNotAllowed),
        }
    }
    Ok(())
}

fn executor_batch_may_change_workspace(batch: &crate::ToolCallBatch) -> bool {
    batch.calls.iter().any(|call| {
        matches!(
            call.request,
            ActionRequest::Runtime(
                RuntimeActionRequest::ValidationSelector { .. }
                    | RuntimeActionRequest::Tool(
                        crate::ToolRequest::ReplaceFile { .. }
                            | crate::ToolRequest::GitStatus
                            | crate::ToolRequest::GitDiff
                    )
            )
        )
    })
}

fn fail_executor_batch<T>(
    ledger: &mut TaskBudgetLedger,
    lease: &mut crate::RoleBudgetLease,
    permit: &crate::ExploratoryBatchPermit,
    receipt: &crate::ProviderResponseReceipt,
    error: RoleLoopError,
) -> Result<T, RoleLoopError> {
    release_executor_batch(ledger, lease, permit, receipt)?;
    Err(error)
}

fn release_executor_batch(
    ledger: &mut TaskBudgetLedger,
    lease: &mut crate::RoleBudgetLease,
    permit: &crate::ExploratoryBatchPermit,
    receipt: &crate::ProviderResponseReceipt,
) -> Result<(), RoleLoopError> {
    ledger.abort_exploratory_runtime_batch(lease, permit)?;
    ledger.finish_provider_response(lease, receipt)?;
    Ok(())
}

fn apply_plan_progress(
    plan: &PlanSnapshot,
    progress: &crate::PlanProgressSubmission,
) -> Result<PlanSnapshot, RoleLoopError> {
    if plan.format_version() != 1 || plan.validate().is_err() {
        return Err(RoleLoopError::InvalidExecutorOutput);
    }
    let updates = progress
        .updates()
        .iter()
        .map(|update| (update.step_id(), update.status()))
        .collect::<std::collections::HashMap<_, _>>();
    if updates.len() != progress.updates().len()
        || updates
            .keys()
            .any(|step_id| !plan.items().iter().any(|item| item.id() == *step_id))
    {
        return Err(RoleLoopError::InvalidExecutorOutput);
    }

    let (_, revision, summary, items, initial_required_checks) = plan.clone().into_parts();
    let items = items
        .into_iter()
        .map(|item| {
            let (id, title, description, acceptance_criteria, current) = item.into_parts();
            let next = match updates.get(id.as_str()).copied() {
                None => current,
                Some(crate::PlanProgressStatus::Running) if current == PlanItemStatus::Pending => {
                    PlanItemStatus::Running
                }
                Some(crate::PlanProgressStatus::Completed)
                    if matches!(current, PlanItemStatus::Pending | PlanItemStatus::Running) =>
                {
                    PlanItemStatus::Completed
                }
                Some(_) => return Err(RoleLoopError::InvalidExecutorOutput),
            };
            PlanItem::try_structured(id, title, description, acceptance_criteria, next)
                .map_err(|_| RoleLoopError::InvalidExecutorOutput)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let revision = revision
        .checked_add(1)
        .filter(|revision| *revision <= coding_agent_domain::MAX_WORKSPACE_GENERATION)
        .ok_or(RoleLoopError::InvalidExecutorOutput)?;
    PlanSnapshot::try_structured(revision, summary, items, initial_required_checks)
        .map_err(|_| RoleLoopError::InvalidExecutorOutput)
}

fn bounded_evidence_summary(value: &str) -> (String, bool) {
    const LIMIT: usize = 2_048;
    if value.len() <= LIMIT {
        return (value.to_owned(), false);
    }
    const MARKER: &str = "\n...[evidence summary truncated]";
    let mut end = LIMIT.saturating_sub(MARKER.len()).min(value.len());
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    let mut bounded = value[..end].to_owned();
    bounded.push_str(MARKER);
    (bounded, true)
}

fn map_provider_error(error: ProviderError) -> RoleLoopError {
    if error.code == "PROVIDER_CANCELLED" {
        RoleLoopError::Cancelled
    } else {
        RoleLoopError::Provider(error)
    }
}

fn map_runtime_error(error: RuntimeError) -> RoleLoopError {
    if error.code == "COMMAND_CANCELLED" {
        RoleLoopError::Cancelled
    } else {
        RoleLoopError::Runtime(error)
    }
}

fn ensure_transcript_matches_lease(
    transcript: &RoleTranscript,
    lease: &crate::RoleBudgetLease,
) -> Result<(), RoleLoopError> {
    let owner = transcript.owner();
    if owner.role() != lease.role() || owner.role_run() != lease.role_run() {
        return Err(RoleContractError::InvalidBatch.into());
    }
    Ok(())
}
