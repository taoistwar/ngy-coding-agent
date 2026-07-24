use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

use coding_agent_domain::{CheckEvidenceStatus, RequiredCheck};

use crate::model::{ToolCall, ToolRequest};
use crate::quality_state::{RequiredCheckLedger, WorkspaceCheckpoint};
use crate::retained_result::RetainedToolResult;
use crate::role::{
    ActionRequest, ControlKind, ControlRequest, RequiredAction, RuntimeActionRequest,
};
use crate::{PreparedProviderRequest, RawProviderResponse};

pub const TASK_RESPONSE_LIMIT: u32 = 60;
pub const TASK_CALL_LIMIT: u32 = 96;
pub const TASK_PROVIDER_BYTE_LIMIT: usize = 8 * 1024 * 1024;
pub const TASK_RETAINED_RESULT_LIMIT: usize = 768 * 1024;

pub const PROVIDER_REQUEST_BYTE_LIMIT: usize = 1024 * 1024;
pub const PROVIDER_RESPONSE_BYTE_LIMIT: usize = 1024 * 1024;
pub const SINGLE_RETAINED_RESULT_LIMIT: usize = 256 * 1024;

pub const PLANNER_ROLE_RESPONSE_LIMIT: u32 = 8;
pub const PLANNER_ROLE_CALL_LIMIT: u32 = 12;
pub const PLANNER_RETAINED_RESULT_LIMIT: usize = 128 * 1024;

pub const EXECUTOR_ROLE_RESPONSE_LIMIT: u32 = 20;
pub const EXECUTOR_ROLE_CALL_LIMIT: u32 = 32;
pub const EXECUTOR_RETAINED_RESULT_LIMIT: usize = 256 * 1024;

pub const REVIEWER_ROLE_RESPONSE_LIMIT: u32 = 10;
pub const REVIEWER_ROLE_CALL_LIMIT: u32 = 16;
pub const REVIEWER_RETAINED_RESULT_LIMIT: usize = 256 * 1024;

pub const REVIEWER_REQUIRED_RESPONSES: u32 = 6;
pub const REVIEWER_REQUIRED_CALLS: u32 = 6;
pub const EXECUTOR_REVIEWER_RETAINED_RESERVATION: usize =
    crate::retained_result::MAX_RETAINED_REVIEW_COVERAGE_BYTES;
pub const VALIDATION_RETAINED_RESULT_LIMIT: usize = 8 * 1024;
pub const PLAN_PROGRESS_RETAINED_RESULT_LIMIT: usize = 8 * 1024;
pub const REVIEW_MANIFEST_RETAINED_RESULT_LIMIT: usize =
    crate::retained_result::MAX_RETAINED_REVIEW_MANIFEST_BYTES;
pub const REVIEW_DIFF_CHUNK_RETAINED_RESULT_LIMIT: usize =
    crate::retained_result::MAX_RETAINED_REVIEW_CHUNK_BYTES;
pub const REVIEW_DIFF_BATCH_RETAINED_RESULT_LIMIT: usize =
    crate::retained_result::MAX_RETAINED_REVIEW_CHUNK_BATCH_BYTES;
pub const MAX_REQUIRED_CHECKS: usize = 16;
pub const MAX_REVIEW_DIFF_CHUNKS: u8 = 8;
pub const MAX_REVIEW_ROUNDS: u32 = 3;
const MIN_EXPLORATORY_RESULT_WRAPPER_BYTES: usize = 1024;

static NEXT_TASK_BUDGET_LEDGER_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BudgetRole {
    Planner,
    Executor,
    Reviewer,
}

impl BudgetRole {
    pub const fn limits(self) -> RoleBudgetLimits {
        match self {
            Self::Planner => RoleBudgetLimits::new(
                PLANNER_ROLE_RESPONSE_LIMIT,
                PLANNER_ROLE_CALL_LIMIT,
                PLANNER_RETAINED_RESULT_LIMIT,
            ),
            Self::Executor => RoleBudgetLimits::new(
                EXECUTOR_ROLE_RESPONSE_LIMIT,
                EXECUTOR_ROLE_CALL_LIMIT,
                EXECUTOR_RETAINED_RESULT_LIMIT,
            ),
            Self::Reviewer => RoleBudgetLimits::new(
                REVIEWER_ROLE_RESPONSE_LIMIT,
                REVIEWER_ROLE_CALL_LIMIT,
                REVIEWER_RETAINED_RESULT_LIMIT,
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoleBudgetLimits {
    model_responses: u32,
    model_visible_calls: u32,
    retained_result_bytes: usize,
}

impl RoleBudgetLimits {
    const fn new(
        model_responses: u32,
        model_visible_calls: u32,
        retained_result_bytes: usize,
    ) -> Self {
        Self {
            model_responses,
            model_visible_calls,
            retained_result_bytes,
        }
    }

    pub const fn model_responses(self) -> u32 {
        self.model_responses
    }

    pub const fn model_visible_calls(self) -> u32 {
        self.model_visible_calls
    }

    pub const fn retained_result_bytes(self) -> usize {
        self.retained_result_bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RoleRun {
    role: BudgetRole,
    role_run: u32,
}

impl RoleRun {
    pub fn try_new(role: BudgetRole, role_run: u32) -> Result<Self, BudgetError> {
        let valid = match role {
            BudgetRole::Planner => role_run == 1,
            BudgetRole::Executor | BudgetRole::Reviewer => {
                (1..=MAX_REVIEW_ROUNDS).contains(&role_run)
            }
        };
        if !valid {
            return Err(BudgetError::InvalidRoleRun { role, role_run });
        }
        Ok(Self { role, role_run })
    }

    pub const fn role(self) -> BudgetRole {
        self.role
    }

    pub const fn role_run(self) -> u32 {
        self.role_run
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TaskBudgetUsage {
    model_responses: u32,
    model_visible_calls: u32,
    provider_bytes: usize,
    retained_result_bytes: usize,
}

impl TaskBudgetUsage {
    pub const fn model_responses(self) -> u32 {
        self.model_responses
    }

    pub const fn model_visible_calls(self) -> u32 {
        self.model_visible_calls
    }

    pub const fn provider_bytes(self) -> usize {
        self.provider_bytes
    }

    pub const fn retained_result_bytes(self) -> usize {
        self.retained_result_bytes
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RoleBudgetUsage {
    model_responses: u32,
    model_visible_calls: u32,
    retained_result_bytes: usize,
}

impl RoleBudgetUsage {
    pub const fn model_responses(self) -> u32 {
        self.model_responses
    }

    pub const fn model_visible_calls(self) -> u32 {
        self.model_visible_calls
    }

    pub const fn retained_result_bytes(self) -> usize {
        self.retained_result_bytes
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BudgetReservation {
    model_responses: u32,
    model_visible_calls: u32,
    retained_result_bytes: usize,
}

impl BudgetReservation {
    const fn new(
        model_responses: u32,
        model_visible_calls: u32,
        retained_result_bytes: usize,
    ) -> Self {
        Self {
            model_responses,
            model_visible_calls,
            retained_result_bytes,
        }
    }

    pub const fn model_responses(self) -> u32 {
        self.model_responses
    }

    pub const fn model_visible_calls(self) -> u32 {
        self.model_visible_calls
    }

    pub const fn retained_result_bytes(self) -> usize {
        self.retained_result_bytes
    }

    fn checked_add(self, other: Self) -> Result<Self, BudgetError> {
        Ok(Self {
            model_responses: self
                .model_responses
                .checked_add(other.model_responses)
                .ok_or(BudgetError::ArithmeticOverflow {
                    resource: BudgetResource::ModelResponses,
                })?,
            model_visible_calls: self
                .model_visible_calls
                .checked_add(other.model_visible_calls)
                .ok_or(BudgetError::ArithmeticOverflow {
                    resource: BudgetResource::ModelVisibleCalls,
                })?,
            retained_result_bytes: self
                .retained_result_bytes
                .checked_add(other.retained_result_bytes)
                .ok_or(BudgetError::ArithmeticOverflow {
                    resource: BudgetResource::RetainedResultBytes,
                })?,
        })
    }
}

const REVIEWER_RESERVATION: BudgetReservation = BudgetReservation::new(
    REVIEWER_REQUIRED_RESPONSES,
    REVIEWER_REQUIRED_CALLS,
    EXECUTOR_REVIEWER_RETAINED_RESERVATION,
);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingReviewerReservation {
    review_round: u32,
    amounts: BudgetReservation,
}

impl PendingReviewerReservation {
    pub const fn review_round(self) -> u32 {
        self.review_round
    }

    pub const fn amounts(self) -> BudgetReservation {
        self.amounts
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoleBudgetTermination {
    Normal,
    ReportBlocked,
    ReviewerChangesRequested,
}

/// A typed early terminal that can only end a role unsuccessfully.
///
/// Normal success and Reviewer approval intentionally have no representation
/// here; those outcomes must complete the full required-action path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EarlyRoleBudgetTermination {
    ReportBlocked,
    ReviewerChangesRequested,
}

impl From<EarlyRoleBudgetTermination> for RoleBudgetTermination {
    fn from(termination: EarlyRoleBudgetTermination) -> Self {
        match termination {
            EarlyRoleBudgetTermination::ReportBlocked => Self::ReportBlocked,
            EarlyRoleBudgetTermination::ReviewerChangesRequested => Self::ReviewerChangesRequested,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinishedRoleBudget {
    usage: RoleBudgetUsage,
    termination: RoleBudgetTermination,
}

impl FinishedRoleBudget {
    pub const fn usage(self) -> RoleBudgetUsage {
        self.usage
    }

    pub const fn termination(self) -> RoleBudgetTermination {
        self.termination
    }
}

/// Opaque proof that a strictly decoded control is the normal terminal for
/// one role.
///
/// Task 9 mints this only after whole-batch preflight. External callers cannot
/// construct or clone it, so they cannot turn an arbitrary exploratory
/// response into normal success.
#[derive(Debug)]
pub struct ExploratoryNormalTerminal {
    ledger_id: u64,
    lease_id: u64,
    role_run: RoleRun,
    request_id: u64,
    required_action: RequiredBudgetAction,
    control: ControlRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequiredBudgetAction {
    PlannerTerminal,
    ExecutorCheck {
        check: RequiredCheck,
    },
    ExecutorTerminal,
    ReviewerManifest,
    ReviewerChunkBatch {
        batch_index: u8,
        start_chunk: u8,
        count: u8,
    },
    ReviewerTerminal,
}

impl RequiredBudgetAction {
    const fn expects_result(&self) -> bool {
        matches!(
            self,
            Self::ExecutorCheck { .. } | Self::ReviewerManifest | Self::ReviewerChunkBatch { .. }
        )
    }

    const fn result_limit(&self) -> Option<usize> {
        match self {
            Self::ExecutorCheck { .. } => Some(VALIDATION_RETAINED_RESULT_LIMIT),
            Self::ReviewerManifest => Some(REVIEW_MANIFEST_RETAINED_RESULT_LIMIT),
            Self::ReviewerChunkBatch { count: 1, .. } => {
                Some(REVIEW_DIFF_CHUNK_RETAINED_RESULT_LIMIT)
            }
            Self::ReviewerChunkBatch { .. } => Some(REVIEW_DIFF_BATCH_RETAINED_RESULT_LIMIT),
            Self::PlannerTerminal | Self::ExecutorTerminal | Self::ReviewerTerminal => None,
        }
    }
}

#[derive(Debug)]
pub struct RequiredActionPermit {
    ledger_id: u64,
    lease_id: u64,
    role_run: RoleRun,
    action_id: u64,
    action: RequiredBudgetAction,
    response_charged: bool,
    call_charged: bool,
    result_id: Option<RetainedResultId>,
    result_bytes: Option<usize>,
    manifest_bound: bool,
}

/// Opaque proof that one exact charged required runtime action failed after
/// its retained-result permit and provider response were safely closed.
#[derive(Debug)]
pub(crate) struct AbortedRequiredRuntimeAction {
    permit: RequiredActionPermit,
}

#[derive(Debug)]
pub struct ReviewerManifestBudgetReceipt {
    lease_id: u64,
    role_run: RoleRun,
    action_id: u64,
    result_id: RetainedResultId,
    chunk_count: u8,
}

impl RequiredActionPermit {
    pub const fn action(&self) -> &RequiredBudgetAction {
        &self.action
    }

    pub const fn response_charged(&self) -> bool {
        self.response_charged
    }

    pub const fn call_charged(&self) -> bool {
        self.call_charged
    }

    /// Proves a public role engine request consumes exactly this reserved slot.
    pub fn permits_required_action(&self, required: &RequiredAction) -> bool {
        match (&self.action, required) {
            (
                RequiredBudgetAction::PlannerTerminal,
                RequiredAction::TerminalOrBlocked(ControlKind::SubmitPlan),
            )
            | (
                RequiredBudgetAction::ExecutorTerminal,
                RequiredAction::TerminalOrBlocked(ControlKind::SubmitExecution),
            )
            | (
                RequiredBudgetAction::ReviewerTerminal,
                RequiredAction::TerminalOrBlocked(ControlKind::SubmitReview),
            ) => true,
            (
                RequiredBudgetAction::ExecutorCheck { check: expected },
                RequiredAction::Validation(actual),
            ) => expected == actual,
            (
                RequiredBudgetAction::ReviewerManifest,
                RequiredAction::ReviewDiffManifest { .. }
                | RequiredAction::ReviewDiffManifestOrTerminal { .. },
            ) => true,
            (
                RequiredBudgetAction::ReviewerChunkBatch {
                    start_chunk, count, ..
                },
                RequiredAction::ReviewDiffChunks {
                    start_chunk: actual_start,
                    count: actual_count,
                    ..
                }
                | RequiredAction::ReviewDiffChunksOrTerminal {
                    start_chunk: actual_start,
                    count: actual_count,
                    ..
                },
            ) => start_chunk == actual_start && count == actual_count,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolResultKind {
    Runtime,
    Validation,
}

#[derive(Debug)]
pub(crate) struct ToolResultPermit {
    lease_id: u64,
    role_run: RoleRun,
    result_id: RetainedResultId,
    kind: ToolResultKind,
}

/// One charged exploratory invocation whose owned request and result permit
/// cannot be separated or substituted by callers.
///
/// Task 8/9 execution adapters can consume this object and execute the exact
/// request it owns. Public callers can inspect only its immutable budget
/// classification and identity, and must present this same object when
/// retaining the result.
#[derive(Debug)]
pub struct BudgetedToolInvocation {
    request: ToolRequest,
    result_permit: ToolResultPermit,
}

/// Opaque whole-batch permit used by the Project 3 role engine.
///
/// It owns the exact provider IDs and runtime actions whose call budget was
/// charged atomically. No runtime action may execute until this value has been
/// minted successfully.
#[derive(Debug)]
pub struct ExploratoryBatchPermit {
    ledger_id: u64,
    lease_id: u64,
    role_run: RoleRun,
    request_id: u64,
    invocations: Vec<BudgetedRoleInvocation>,
}

#[derive(Debug)]
pub struct BudgetedRoleInvocation {
    tool_call_id: String,
    request: RuntimeActionRequest,
    result_permit: ToolResultPermit,
    wrapper_cap: usize,
}

/// Opaque permit for the Executor's one non-terminal control action.
///
/// It binds the received provider response, exact tool-call identity, decoded
/// progress update, and one bounded retained result. The durable event must be
/// acknowledged before the caller may retain that result and append it to the
/// transcript.
#[derive(Debug)]
pub struct ExploratoryControlPermit {
    ledger_id: u64,
    lease_id: u64,
    role_run: RoleRun,
    request_id: u64,
    tool_call_id: String,
    control: ControlRequest,
    result_permit: ToolResultPermit,
    wrapper_bytes: Box<[u8]>,
}

impl ExploratoryControlPermit {
    pub fn tool_call_id(&self) -> &str {
        &self.tool_call_id
    }

    pub const fn control(&self) -> &ControlRequest {
        &self.control
    }
}

impl ExploratoryBatchPermit {
    pub fn invocations(&self) -> &[BudgetedRoleInvocation] {
        &self.invocations
    }
}

impl BudgetedRoleInvocation {
    pub fn tool_call_id(&self) -> &str {
        &self.tool_call_id
    }

    pub const fn request(&self) -> &RuntimeActionRequest {
        &self.request
    }

    pub const fn result_id(&self) -> RetainedResultId {
        self.result_permit.result_id
    }

    pub const fn wrapper_cap(&self) -> usize {
        self.wrapper_cap
    }
}

impl BudgetedToolInvocation {
    #[cfg(test)]
    const fn request(&self) -> &ToolRequest {
        &self.request
    }

    pub const fn kind(&self) -> ToolResultKind {
        self.result_permit.kind
    }

    pub const fn result_id(&self) -> RetainedResultId {
        self.result_permit.result_id
    }

    const fn byte_limit(&self) -> usize {
        match self.result_permit.kind {
            ToolResultKind::Runtime => SINGLE_RETAINED_RESULT_LIMIT,
            ToolResultKind::Validation => VALIDATION_RETAINED_RESULT_LIMIT,
        }
    }

    /// Same-crate handoff for the role execution adapter. Returning both
    /// values together ensures the runtime receives the owned request that
    /// minted this exact result permit.
    #[allow(dead_code)]
    pub(crate) fn into_parts(self) -> (ToolRequest, ToolResultPermit) {
        (self.request, self.result_permit)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RetainedResultId(u64);

impl RetainedResultId {
    pub const fn value(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetainedResultCharge {
    Charged,
    AlreadyCounted,
}

#[derive(Debug)]
pub struct ProviderRequestPermit {
    ledger_id: u64,
    request_id: u64,
    lease_id: u64,
    role_run: RoleRun,
    class: ChargeClass,
}

#[derive(Debug)]
pub struct ProviderResponseReceipt {
    ledger_id: u64,
    request_id: u64,
    lease_id: u64,
    role_run: RoleRun,
    class: ChargeClass,
    encoded_bytes: usize,
    model_visible_calls: u32,
    violation: Option<ProviderResponseViolation>,
}

impl ProviderResponseReceipt {
    pub const fn encoded_bytes(&self) -> usize {
        self.encoded_bytes
    }

    pub const fn role(&self) -> BudgetRole {
        self.role_run.role
    }

    pub const fn role_run(&self) -> u32 {
        self.role_run.role_run
    }

    pub const fn violation(&self) -> Option<ProviderResponseViolation> {
        self.violation
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderResponseViolation {
    ResponseByteLimit,
    ReservedByteLimit,
    TaskProviderByteLimit,
}

#[derive(Debug)]
pub struct RoleBudgetLease {
    lease_id: u64,
    role_run: RoleRun,
    usage: RoleBudgetUsage,
    required_reservation: BudgetReservation,
    required_actions: VecDeque<RequiredBudgetAction>,
    active_required_action: Option<u64>,
    next_action_id: u64,
    pending_tool_results: HashSet<RetainedResultId>,
    pending_batch_retained_reservation: usize,
    reviewer_manifest_bound: bool,
    termination: Option<RoleBudgetTermination>,
}

impl RoleBudgetLease {
    pub const fn role(&self) -> BudgetRole {
        self.role_run.role
    }

    pub const fn role_run(&self) -> u32 {
        self.role_run.role_run
    }

    pub const fn limits(&self) -> RoleBudgetLimits {
        self.role_run.role.limits()
    }

    pub const fn usage(&self) -> RoleBudgetUsage {
        self.usage
    }

    pub const fn required_reservation(&self) -> BudgetReservation {
        self.required_reservation
    }

    pub fn next_required_action(&self) -> Option<&RequiredBudgetAction> {
        self.required_actions.front()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChargeClass {
    Required { action_id: u64 },
    Exploratory,
}

#[derive(Debug, Clone, Copy)]
struct PendingProviderResponse {
    ledger_id: u64,
    request_id: u64,
    lease_id: u64,
    role_run: RoleRun,
    maximum_bytes: usize,
    class: ChargeClass,
}

#[derive(Debug)]
struct RecordedRetainedResult {
    owner: RoleRun,
    wrapper_bytes: Box<[u8]>,
}

#[derive(Debug)]
pub struct TaskBudgetLedger {
    ledger_id: Option<u64>,
    usage: TaskBudgetUsage,
    retained_results: HashMap<RetainedResultId, RecordedRetainedResult>,
    active_role: Option<(u64, RoleRun)>,
    pending_provider_response: Option<PendingProviderResponse>,
    open_provider_response: Option<(u64, u64, RoleRun)>,
    next_lease_id: u64,
    next_provider_request_id: u64,
    next_result_id: u64,
    started_role_runs: HashSet<RoleRun>,
    pending_reviewer: Option<PendingReviewerReservation>,
}

impl Default for TaskBudgetLedger {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskBudgetLedger {
    pub fn new() -> Self {
        let ledger_id = allocate_task_budget_ledger_id(&NEXT_TASK_BUDGET_LEDGER_ID).ok();
        Self::with_ledger_id(ledger_id)
    }

    pub fn try_new() -> Result<Self, BudgetError> {
        let ledger_id = allocate_task_budget_ledger_id(&NEXT_TASK_BUDGET_LEDGER_ID)?;
        Ok(Self::with_ledger_id(Some(ledger_id)))
    }

    fn with_ledger_id(ledger_id: Option<u64>) -> Self {
        Self {
            ledger_id,
            usage: TaskBudgetUsage::default(),
            retained_results: HashMap::new(),
            active_role: None,
            pending_provider_response: None,
            open_provider_response: None,
            next_lease_id: 0,
            next_provider_request_id: 0,
            next_result_id: 0,
            started_role_runs: HashSet::new(),
            pending_reviewer: None,
        }
    }

    pub const fn usage(&self) -> TaskBudgetUsage {
        self.usage
    }

    pub fn active_role(&self) -> Option<RoleRun> {
        self.active_role.map(|(_, role_run)| role_run)
    }

    pub const fn pending_reviewer_reservation(&self) -> Option<PendingReviewerReservation> {
        self.pending_reviewer
    }

    pub fn start_planner(&mut self) -> Result<RoleBudgetLease, BudgetError> {
        self.ensure_role_start_is_idle()?;
        let role_run = RoleRun {
            role: BudgetRole::Planner,
            role_run: 1,
        };
        let reservation = BudgetReservation::new(1, 1, 0);
        self.ensure_role_not_started(role_run)?;
        self.ensure_task_reservation_fits(reservation)?;
        self.activate_role(
            role_run,
            reservation,
            VecDeque::from([RequiredBudgetAction::PlannerTerminal]),
        )
    }

    pub fn start_executor(
        &mut self,
        review_round: u32,
        required_checks: &RequiredCheckLedger,
        checkpoint: &WorkspaceCheckpoint,
    ) -> Result<RoleBudgetLease, BudgetError> {
        validate_review_round(BudgetRole::Executor, review_round)?;
        self.ensure_role_start_is_idle()?;

        let missing_checks = missing_checks(required_checks, checkpoint);
        let missing_count =
            u32::try_from(missing_checks.len()).map_err(|_| BudgetError::ArithmeticOverflow {
                resource: BudgetResource::ModelResponses,
            })?;
        let required_actions =
            missing_count
                .checked_add(1)
                .ok_or(BudgetError::ArithmeticOverflow {
                    resource: BudgetResource::ModelResponses,
                })?;
        let required_retained = missing_checks
            .len()
            .checked_mul(VALIDATION_RETAINED_RESULT_LIMIT)
            .ok_or(BudgetError::ArithmeticOverflow {
                resource: BudgetResource::RetainedResultBytes,
            })?;
        let executor_reservation =
            BudgetReservation::new(required_actions, required_actions, required_retained);
        let total_reservation = executor_reservation.checked_add(REVIEWER_RESERVATION)?;
        let role_run = RoleRun {
            role: BudgetRole::Executor,
            role_run: review_round,
        };
        self.ensure_role_not_started(role_run)?;
        self.ensure_task_reservation_fits(total_reservation)?;
        ensure_role_reservation_fits(role_run.role, executor_reservation)?;

        let mut actions = missing_checks
            .into_iter()
            .map(|check| RequiredBudgetAction::ExecutorCheck { check })
            .collect::<VecDeque<_>>();
        actions.push_back(RequiredBudgetAction::ExecutorTerminal);
        let lease = self.activate_role(role_run, executor_reservation, actions)?;
        self.pending_reviewer = Some(PendingReviewerReservation {
            review_round,
            amounts: REVIEWER_RESERVATION,
        });
        Ok(lease)
    }

    pub fn start_reviewer(&mut self, review_round: u32) -> Result<RoleBudgetLease, BudgetError> {
        self.ensure_ledger_identity()?;
        validate_review_round(BudgetRole::Reviewer, review_round)?;
        self.ensure_no_active_role()?;
        self.ensure_no_provider_exchange()?;
        let pending = self
            .pending_reviewer
            .ok_or(BudgetError::ReviewerReservationMissing { review_round })?;
        if pending.review_round != review_round {
            return Err(BudgetError::ReviewerReservationRoundMismatch {
                expected: pending.review_round,
                observed: review_round,
            });
        }
        let role_run = RoleRun {
            role: BudgetRole::Reviewer,
            role_run: review_round,
        };
        self.ensure_role_not_started(role_run)?;
        self.ensure_task_reservation_fits(pending.amounts)?;
        ensure_role_reservation_fits(role_run.role, pending.amounts)?;

        let lease = self.activate_role(
            role_run,
            pending.amounts,
            VecDeque::from([RequiredBudgetAction::ReviewerManifest]),
        )?;
        self.pending_reviewer = None;
        Ok(lease)
    }

    pub fn abandon_pending_reviewer_reservation(
        &mut self,
        review_round: u32,
    ) -> Result<(), BudgetError> {
        self.ensure_ledger_identity()?;
        validate_review_round(BudgetRole::Reviewer, review_round)?;
        self.ensure_no_active_role()?;
        self.ensure_no_provider_exchange()?;
        let pending = self
            .pending_reviewer
            .ok_or(BudgetError::ReviewerReservationMissing { review_round })?;
        if pending.review_round != review_round {
            return Err(BudgetError::ReviewerReservationRoundMismatch {
                expected: pending.review_round,
                observed: review_round,
            });
        }
        self.pending_reviewer = None;
        Ok(())
    }

    /// Reconciles one active Executor lease with the checks missing at the
    /// current checkpoint without resetting either task or role usage.
    ///
    /// Generation changes can revoke every passed observation after the lease
    /// was started. This method atomically rebuilds the remaining required
    /// check actions in ledger order, keeps the terminal action last, and
    /// continues to protect the pending 184 KiB Reviewer reservation.
    pub fn refresh_executor_required_actions(
        &mut self,
        lease: &mut RoleBudgetLease,
        required_checks: &RequiredCheckLedger,
        checkpoint: &WorkspaceCheckpoint,
    ) -> Result<(), BudgetError> {
        self.ensure_unsealed_lease(lease)?;
        self.ensure_no_provider_exchange()?;
        if lease.role() != BudgetRole::Executor {
            return Err(BudgetError::ExecutorRefreshForWrongRole);
        }
        if lease.active_required_action.is_some() {
            return Err(BudgetError::RequiredActionInProgress);
        }
        if !lease.pending_tool_results.is_empty() {
            return Err(BudgetError::ToolResultPending);
        }
        if lease.pending_batch_retained_reservation != 0 {
            return Err(BudgetError::ExploratoryBatchAlreadyPending);
        }
        let pending_reviewer =
            self.pending_reviewer
                .ok_or(BudgetError::ReviewerReservationMissing {
                    review_round: lease.role_run(),
                })?;
        if pending_reviewer.review_round != lease.role_run() {
            return Err(BudgetError::ReviewerReservationRoundMismatch {
                expected: pending_reviewer.review_round,
                observed: lease.role_run(),
            });
        }

        let missing = missing_checks(required_checks, checkpoint);
        let missing_count =
            u32::try_from(missing.len()).map_err(|_| BudgetError::ArithmeticOverflow {
                resource: BudgetResource::ModelResponses,
            })?;
        let required_actions =
            missing_count
                .checked_add(1)
                .ok_or(BudgetError::ArithmeticOverflow {
                    resource: BudgetResource::ModelResponses,
                })?;
        let required_retained = missing
            .len()
            .checked_mul(VALIDATION_RETAINED_RESULT_LIMIT)
            .ok_or(BudgetError::ArithmeticOverflow {
                resource: BudgetResource::RetainedResultBytes,
            })?;
        let reservation =
            BudgetReservation::new(required_actions, required_actions, required_retained);
        ensure_role_reservation_with_usage_fits(lease, reservation)?;
        self.ensure_task_reservation_fits(reservation.checked_add(pending_reviewer.amounts)?)?;

        lease.required_reservation = reservation;
        lease.required_actions = missing
            .into_iter()
            .map(|check| RequiredBudgetAction::ExecutorCheck { check })
            .chain(std::iter::once(RequiredBudgetAction::ExecutorTerminal))
            .collect();
        Ok(())
    }

    /// Expands the active Executor's remaining reservation to the worst case
    /// before a received exploratory batch is allowed to change workspace
    /// state or replace current validation evidence.
    ///
    /// The provider response is already charged, but no model-visible call or
    /// runtime side effect may have occurred. A later stable observation can
    /// shrink this reservation through [`Self::refresh_executor_required_actions`].
    pub fn protect_executor_workspace_change(
        &mut self,
        lease: &mut RoleBudgetLease,
        receipt: &ProviderResponseReceipt,
        required_checks: &RequiredCheckLedger,
    ) -> Result<(), BudgetError> {
        self.ensure_unsealed_lease(lease)?;
        self.validate_response_receipt(lease, receipt)?;
        if lease.role() != BudgetRole::Executor
            || receipt.class != ChargeClass::Exploratory
            || receipt.violation.is_some()
            || receipt.model_visible_calls != 0
            || lease.active_required_action.is_some()
            || !lease.pending_tool_results.is_empty()
            || lease.pending_batch_retained_reservation != 0
        {
            return Err(BudgetError::InvalidExecutorWorkspaceProtection);
        }
        let pending_reviewer =
            self.pending_reviewer
                .ok_or(BudgetError::ReviewerReservationMissing {
                    review_round: lease.role_run(),
                })?;
        if pending_reviewer.review_round != lease.role_run() {
            return Err(BudgetError::ReviewerReservationRoundMismatch {
                expected: pending_reviewer.review_round,
                observed: lease.role_run(),
            });
        }
        let check_count = u32::try_from(required_checks.checks().len()).map_err(|_| {
            BudgetError::ArithmeticOverflow {
                resource: BudgetResource::ModelResponses,
            }
        })?;
        let required_actions =
            check_count
                .checked_add(1)
                .ok_or(BudgetError::ArithmeticOverflow {
                    resource: BudgetResource::ModelResponses,
                })?;
        let required_retained = required_checks
            .checks()
            .len()
            .checked_mul(VALIDATION_RETAINED_RESULT_LIMIT)
            .ok_or(BudgetError::ArithmeticOverflow {
                resource: BudgetResource::RetainedResultBytes,
            })?;
        let reservation =
            BudgetReservation::new(required_actions, required_actions, required_retained);
        ensure_role_reservation_with_usage_fits(lease, reservation)?;
        self.ensure_task_reservation_fits(reservation.checked_add(pending_reviewer.amounts)?)?;
        lease.required_reservation = reservation;
        lease.required_actions = required_checks
            .checks()
            .iter()
            .cloned()
            .map(|check| RequiredBudgetAction::ExecutorCheck { check })
            .chain(std::iter::once(RequiredBudgetAction::ExecutorTerminal))
            .collect();
        Ok(())
    }

    pub fn finish_role(
        &mut self,
        lease: RoleBudgetLease,
    ) -> Result<FinishedRoleBudget, BudgetError> {
        self.ensure_matching_lease(&lease)?;
        self.ensure_no_provider_exchange()?;
        if lease.active_required_action.is_some() {
            return Err(BudgetError::RequiredActionInProgress);
        }
        if let Some(action) = lease.required_actions.front().cloned() {
            return Err(BudgetError::RequiredActionPending { action });
        }
        if !lease.pending_tool_results.is_empty() {
            return Err(BudgetError::ToolResultPending);
        }
        if lease.pending_batch_retained_reservation != 0 {
            return Err(BudgetError::ExploratoryBatchAlreadyPending);
        }
        let termination = lease.termination.ok_or(BudgetError::RoleNotTerminated)?;
        if lease.role() == BudgetRole::Executor && termination != RoleBudgetTermination::Normal {
            self.pending_reviewer = None;
        }
        self.active_role = None;
        Ok(FinishedRoleBudget {
            usage: lease.usage,
            termination,
        })
    }

    /// Releases a role lease after a provider/runtime/event/cancellation
    /// failure. Provider exchanges must already be closed so a caller cannot
    /// use this path to erase an unaccounted response body.
    pub fn abort_role_on_failure(
        &mut self,
        lease: RoleBudgetLease,
    ) -> Result<RoleBudgetUsage, BudgetError> {
        self.ensure_matching_lease(&lease)?;
        self.ensure_no_provider_exchange()?;
        self.active_role = None;
        self.pending_reviewer = None;
        Ok(lease.usage)
    }

    pub fn begin_required_action(
        &self,
        lease: &mut RoleBudgetLease,
    ) -> Result<RequiredActionPermit, BudgetError> {
        let ledger_id = self.ensure_ledger_identity()?;
        self.ensure_unsealed_lease(lease)?;
        self.ensure_no_provider_exchange()?;
        if lease.active_required_action.is_some() {
            return Err(BudgetError::RequiredActionInProgress);
        }
        if !lease.pending_tool_results.is_empty() {
            return Err(BudgetError::ToolResultPending);
        }
        let action = lease
            .required_actions
            .front()
            .cloned()
            .ok_or(BudgetError::NoRequiredActionPending)?;
        let action_id = lease
            .next_action_id
            .checked_add(1)
            .ok_or(BudgetError::ActionIdExhausted)?;
        lease.next_action_id = action_id;
        lease.active_required_action = Some(action_id);
        Ok(RequiredActionPermit {
            ledger_id,
            lease_id: lease.lease_id,
            role_run: lease.role_run,
            action_id,
            action,
            response_charged: false,
            call_charged: false,
            result_id: None,
            result_bytes: None,
            manifest_bound: false,
        })
    }

    pub fn begin_exploratory_provider_request(
        &mut self,
        lease: &RoleBudgetLease,
        canonical_request: &[u8],
        maximum_response_bytes: usize,
    ) -> Result<ProviderRequestPermit, BudgetError> {
        self.ensure_unsealed_lease(lease)?;
        if lease.active_required_action.is_some() {
            return Err(BudgetError::RequiredActionInProgress);
        }
        self.begin_provider_request_len(
            lease,
            canonical_request.len(),
            maximum_response_bytes,
            ChargeClass::Exploratory,
        )
    }

    /// Exact staged-provider preflight used by the Project 3 role engine.
    pub fn begin_exploratory_prepared_provider_request(
        &mut self,
        lease: &RoleBudgetLease,
        prepared: &dyn PreparedProviderRequest,
    ) -> Result<ProviderRequestPermit, BudgetError> {
        self.ensure_unsealed_lease(lease)?;
        if lease.active_required_action.is_some() {
            return Err(BudgetError::RequiredActionInProgress);
        }
        self.begin_provider_request_len(
            lease,
            prepared.encoded_len(),
            prepared.maximum_response_bytes(),
            ChargeClass::Exploratory,
        )
    }

    pub fn begin_required_provider_request(
        &mut self,
        lease: &RoleBudgetLease,
        permit: &RequiredActionPermit,
        canonical_request: &[u8],
        maximum_response_bytes: usize,
    ) -> Result<ProviderRequestPermit, BudgetError> {
        self.validate_required_permit(lease, permit)?;
        if permit.response_charged {
            return Err(BudgetError::RequiredResponseAlreadyCharged);
        }
        self.begin_provider_request_len(
            lease,
            canonical_request.len(),
            maximum_response_bytes,
            ChargeClass::Required {
                action_id: permit.action_id,
            },
        )
    }

    /// Exact staged-provider preflight for one active required action.
    pub fn begin_required_prepared_provider_request(
        &mut self,
        lease: &RoleBudgetLease,
        permit: &RequiredActionPermit,
        prepared: &dyn PreparedProviderRequest,
    ) -> Result<ProviderRequestPermit, BudgetError> {
        self.validate_required_permit(lease, permit)?;
        if permit.response_charged {
            return Err(BudgetError::RequiredResponseAlreadyCharged);
        }
        self.begin_provider_request_len(
            lease,
            prepared.encoded_len(),
            prepared.maximum_response_bytes(),
            ChargeClass::Required {
                action_id: permit.action_id,
            },
        )
    }

    pub fn record_exploratory_provider_response(
        &mut self,
        lease: &mut RoleBudgetLease,
        request: ProviderRequestPermit,
        encoded_response: &[u8],
    ) -> Result<ProviderResponseReceipt, BudgetError> {
        let pending = self.require_pending_response(lease, &request)?;
        if pending.class != ChargeClass::Exploratory {
            return Err(BudgetError::ProviderResponseClassMismatch);
        }
        self.record_provider_response_len(lease, pending, encoded_response.len())
    }

    pub fn record_exploratory_raw_provider_response(
        &mut self,
        lease: &mut RoleBudgetLease,
        request: ProviderRequestPermit,
        response: &dyn RawProviderResponse,
    ) -> Result<ProviderResponseReceipt, BudgetError> {
        let pending = self.require_pending_response(lease, &request)?;
        if pending.class != ChargeClass::Exploratory {
            return Err(BudgetError::ProviderResponseClassMismatch);
        }
        self.record_provider_response_len(lease, pending, response.encoded_len())
    }

    pub fn record_required_provider_response(
        &mut self,
        lease: &mut RoleBudgetLease,
        permit: &mut RequiredActionPermit,
        request: ProviderRequestPermit,
        encoded_response: &[u8],
    ) -> Result<ProviderResponseReceipt, BudgetError> {
        self.validate_required_permit(lease, permit)?;
        if permit.response_charged {
            return Err(BudgetError::RequiredResponseAlreadyCharged);
        }
        let pending = self.require_pending_response(lease, &request)?;
        if pending.class
            != (ChargeClass::Required {
                action_id: permit.action_id,
            })
        {
            return Err(BudgetError::ProviderResponseClassMismatch);
        }
        let receipt = self.record_provider_response_len(lease, pending, encoded_response.len())?;
        permit.response_charged = true;
        Ok(receipt)
    }

    pub fn record_required_raw_provider_response(
        &mut self,
        lease: &mut RoleBudgetLease,
        permit: &mut RequiredActionPermit,
        request: ProviderRequestPermit,
        response: &dyn RawProviderResponse,
    ) -> Result<ProviderResponseReceipt, BudgetError> {
        self.validate_required_permit(lease, permit)?;
        if permit.response_charged {
            return Err(BudgetError::RequiredResponseAlreadyCharged);
        }
        let pending = self.require_pending_response(lease, &request)?;
        if pending.class
            != (ChargeClass::Required {
                action_id: permit.action_id,
            })
        {
            return Err(BudgetError::ProviderResponseClassMismatch);
        }
        let receipt = self.record_provider_response_len(lease, pending, response.encoded_len())?;
        permit.response_charged = true;
        Ok(receipt)
    }

    pub fn record_transport_no_response(
        &mut self,
        lease: &RoleBudgetLease,
        request: ProviderRequestPermit,
    ) -> Result<(), BudgetError> {
        let pending = self.require_pending_response(lease, &request)?;
        if pending.class != request.class {
            return Err(BudgetError::ProviderResponseClassMismatch);
        }
        self.pending_provider_response = None;
        Ok(())
    }

    pub fn finish_provider_response(
        &mut self,
        lease: &RoleBudgetLease,
        receipt: &ProviderResponseReceipt,
    ) -> Result<(), BudgetError> {
        self.validate_response_receipt(lease, receipt)?;
        if receipt.violation.is_some() {
            return Err(BudgetError::ProviderResponseLimitViolation);
        }
        if !lease.pending_tool_results.is_empty() {
            return Err(BudgetError::ToolResultPending);
        }
        self.open_provider_response = None;
        Ok(())
    }

    pub fn discard_invalid_provider_response(
        &mut self,
        lease: &RoleBudgetLease,
        receipt: &ProviderResponseReceipt,
    ) -> Result<(), BudgetError> {
        self.validate_response_receipt(lease, receipt)?;
        if receipt.model_visible_calls != 0 || !lease.pending_tool_results.is_empty() {
            return Err(BudgetError::InvalidResponseHasSideEffects);
        }
        self.open_provider_response = None;
        Ok(())
    }

    /// Completes a typed non-success terminal control returned by an
    /// exploratory response. This can only produce `ReportBlocked` or a
    /// Reviewer `changes_requested`; it cannot represent normal success or
    /// approval.
    pub fn complete_exploratory_early_terminal(
        &mut self,
        lease: &mut RoleBudgetLease,
        receipt: &mut ProviderResponseReceipt,
        termination: EarlyRoleBudgetTermination,
    ) -> Result<(), BudgetError> {
        self.validate_response_receipt(lease, receipt)?;
        self.validate_early_termination(lease.role(), termination)?;
        if receipt.violation.is_some() {
            return Err(BudgetError::ProviderResponseLimitViolation);
        }
        if receipt.class != ChargeClass::Exploratory {
            return Err(BudgetError::ProviderResponseClassMismatch);
        }
        if receipt.model_visible_calls != 0 || !lease.pending_tool_results.is_empty() {
            return Err(BudgetError::EarlyTerminalHasSideEffects);
        }
        if lease.active_required_action.is_some() {
            return Err(BudgetError::RequiredActionInProgress);
        }
        self.charge_reserved_terminal_call(lease, receipt)?;
        self.apply_early_termination(lease, termination);
        Ok(())
    }

    /// Completes a typed non-success terminal control in place of the active
    /// required action. Actual response/call usage remains charged; only
    /// unused reservations are released.
    pub fn complete_required_early_terminal(
        &mut self,
        lease: &mut RoleBudgetLease,
        permit: RequiredActionPermit,
        receipt: &mut ProviderResponseReceipt,
        termination: EarlyRoleBudgetTermination,
    ) -> Result<(), BudgetError> {
        self.validate_required_permit(lease, &permit)?;
        self.validate_response_receipt(lease, receipt)?;
        self.validate_early_termination(lease.role(), termination)?;
        if receipt.violation.is_some() {
            return Err(BudgetError::ProviderResponseLimitViolation);
        }
        if receipt.class
            != (ChargeClass::Required {
                action_id: permit.action_id,
            })
        {
            return Err(BudgetError::ProviderResponseClassMismatch);
        }
        if !permit.response_charged {
            return Err(BudgetError::RequiredResponseNotCharged);
        }
        if permit.call_charged
            || receipt.model_visible_calls != 0
            || !lease.pending_tool_results.is_empty()
        {
            return Err(BudgetError::EarlyTerminalHasSideEffects);
        }
        self.charge_reserved_terminal_call(lease, receipt)?;
        self.apply_early_termination(lease, termination);
        Ok(())
    }

    /// Releases an unexecuted required action after its provider response was
    /// invalidated by a trusted Reviewer workspace observation.
    ///
    /// The response bytes remain charged, but the ignored model call is never
    /// charged or interpreted as a Reviewer decision.
    pub fn abandon_required_action_on_system_invalidation(
        &mut self,
        lease: &mut RoleBudgetLease,
        permit: RequiredActionPermit,
    ) -> Result<(), BudgetError> {
        self.ensure_unsealed_lease(lease)?;
        self.ensure_no_provider_exchange()?;
        self.validate_required_permit(lease, &permit)?;
        if lease.role() != BudgetRole::Reviewer {
            return Err(BudgetError::ReviewerChangesRequestedByNonReviewer);
        }
        if !permit.response_charged
            || permit.call_charged
            || permit.result_id.is_some()
            || permit.result_bytes.is_some()
            || !lease.pending_tool_results.is_empty()
        {
            return Err(BudgetError::RequiredActionOrderMismatch);
        }
        lease.active_required_action = None;
        Ok(())
    }

    /// Closes one exact failed required runtime action while preserving its
    /// linear permit for a later trusted Reviewer system-invalidation commit.
    pub(crate) fn abort_required_runtime_action(
        &mut self,
        lease: &mut RoleBudgetLease,
        permit: RequiredActionPermit,
        receipt: &ProviderResponseReceipt,
    ) -> Result<AbortedRequiredRuntimeAction, BudgetError> {
        self.abort_required_runtime_result(lease, &permit, receipt)?;
        self.finish_provider_response(lease, receipt)?;
        Ok(AbortedRequiredRuntimeAction { permit })
    }

    /// Atomically consumes the exact failed coverage capability after durable
    /// system evidence has been flushed.
    pub(crate) fn complete_reviewer_system_invalidation_after_required_failure(
        &mut self,
        lease: &mut RoleBudgetLease,
        failed: AbortedRequiredRuntimeAction,
    ) -> Result<(), BudgetError> {
        self.ensure_unsealed_lease(lease)?;
        self.ensure_no_provider_exchange()?;
        self.validate_required_permit(lease, &failed.permit)?;
        if lease.role() != BudgetRole::Reviewer
            || !matches!(
                failed.permit.action,
                RequiredBudgetAction::ReviewerManifest
                    | RequiredBudgetAction::ReviewerChunkBatch { .. }
            )
            || !failed.permit.response_charged
            || !failed.permit.call_charged
            || failed.permit.result_id.is_none()
            || failed.permit.result_bytes.is_some()
            || !lease.pending_tool_results.is_empty()
        {
            return Err(BudgetError::RequiredActionOrderMismatch);
        }
        self.apply_early_termination(lease, EarlyRoleBudgetTermination::ReviewerChangesRequested);
        Ok(())
    }

    /// Completes a Reviewer round from trusted system invalidation without
    /// forging a provider control or charging a synthetic response/call.
    pub fn complete_reviewer_system_invalidation(
        &mut self,
        lease: &mut RoleBudgetLease,
    ) -> Result<(), BudgetError> {
        self.ensure_unsealed_lease(lease)?;
        self.ensure_no_provider_exchange()?;
        if lease.role() != BudgetRole::Reviewer {
            return Err(BudgetError::ReviewerChangesRequestedByNonReviewer);
        }
        if lease.active_required_action.is_some() {
            return Err(BudgetError::RequiredActionInProgress);
        }
        if !lease.pending_tool_results.is_empty() {
            return Err(BudgetError::ToolResultPending);
        }
        if lease.pending_batch_retained_reservation != 0 {
            return Err(BudgetError::ExploratoryBatchAlreadyPending);
        }
        self.apply_early_termination(lease, EarlyRoleBudgetTermination::ReviewerChangesRequested);
        Ok(())
    }

    /// Accounts for a valid Reviewer terminal control whose subsequent trusted
    /// evidence/event finalization failed. The role remains unterminated so
    /// the caller can preserve the original stage error through
    /// `abort_role_on_failure`.
    pub fn close_interpreted_exploratory_reviewer_control_on_failure(
        &mut self,
        lease: &mut RoleBudgetLease,
        receipt: &mut ProviderResponseReceipt,
    ) -> Result<(), BudgetError> {
        self.validate_exploratory_normal_terminal_state(lease, receipt)?;
        if lease.role() != BudgetRole::Reviewer {
            return Err(BudgetError::ReviewerChangesRequestedByNonReviewer);
        }
        self.charge_reserved_terminal_call(lease, receipt)
    }

    /// Required-control counterpart of
    /// `close_interpreted_exploratory_reviewer_control_on_failure`.
    pub fn close_interpreted_required_reviewer_control_on_failure(
        &mut self,
        lease: &mut RoleBudgetLease,
        permit: &mut RequiredActionPermit,
        receipt: &mut ProviderResponseReceipt,
    ) -> Result<(), BudgetError> {
        self.validate_required_permit(lease, permit)?;
        if lease.role() != BudgetRole::Reviewer
            || !matches!(
                permit.action,
                RequiredBudgetAction::ReviewerManifest
                    | RequiredBudgetAction::ReviewerChunkBatch { .. }
                    | RequiredBudgetAction::ReviewerTerminal
            )
            || permit.call_charged
            || permit.result_id.is_some()
            || permit.result_bytes.is_some()
            || !lease.pending_tool_results.is_empty()
        {
            return Err(BudgetError::ReviewerChangesRequestedByNonReviewer);
        }
        self.charge_required_call(lease, permit, receipt)?;
        self.finish_provider_response(lease, receipt)
    }

    /// Mints a non-transferable normal-terminal proof for Task 9 after its
    /// strict whole-batch preflight has accepted the owned control.
    #[allow(dead_code)]
    pub(crate) fn mint_exploratory_normal_terminal(
        &self,
        lease: &RoleBudgetLease,
        receipt: &ProviderResponseReceipt,
        control: ControlRequest,
    ) -> Result<ExploratoryNormalTerminal, BudgetError> {
        self.validate_exploratory_normal_terminal_state(lease, receipt)?;
        let required_action = normal_terminal_action(lease.role(), control.kind())?;
        if lease.required_actions.len() != 1
            || lease.required_actions.front() != Some(&required_action)
        {
            return Err(BudgetError::ExploratoryNormalTerminalNotReady);
        }
        Ok(ExploratoryNormalTerminal {
            ledger_id: self.ensure_ledger_identity()?,
            lease_id: lease.lease_id,
            role_run: lease.role_run,
            request_id: receipt.request_id,
            required_action,
            control,
        })
    }

    /// Completes the role's sole remaining normal terminal from the current
    /// exploratory response.
    ///
    /// The response was already charged as exploratory, so this transition
    /// consumes only the reserved terminal call. It performs no provider
    /// retry, releases the now-unused role reservation, and intentionally
    /// preserves an Executor's pending Reviewer reservation.
    pub fn complete_exploratory_normal_terminal(
        &mut self,
        lease: &mut RoleBudgetLease,
        receipt: &mut ProviderResponseReceipt,
        terminal: ExploratoryNormalTerminal,
    ) -> Result<ControlRequest, BudgetError> {
        self.validate_exploratory_normal_terminal_state(lease, receipt)?;
        let ledger_id = self.ensure_ledger_identity()?;
        if terminal.ledger_id != ledger_id
            || terminal.lease_id != lease.lease_id
            || terminal.role_run != lease.role_run
            || terminal.request_id != receipt.request_id
        {
            return Err(BudgetError::ExploratoryNormalTerminalIdentityMismatch);
        }
        let expected_action = normal_terminal_action(lease.role(), terminal.control.kind())?;
        if terminal.required_action != expected_action
            || lease.required_actions.len() != 1
            || lease.required_actions.front() != Some(&terminal.required_action)
        {
            return Err(BudgetError::ExploratoryNormalTerminalNotReady);
        }

        self.charge_reserved_terminal_call(lease, receipt)?;
        let completed = lease
            .required_actions
            .pop_front()
            .expect("the sole terminal action was validated");
        debug_assert_eq!(completed, terminal.required_action);
        lease.required_reservation = BudgetReservation::default();
        lease.termination = Some(RoleBudgetTermination::Normal);
        Ok(terminal.control)
    }

    pub fn charge_exploratory_tool_call(
        &mut self,
        lease: &mut RoleBudgetLease,
        receipt: &mut ProviderResponseReceipt,
        request: ToolRequest,
    ) -> Result<BudgetedToolInvocation, BudgetError> {
        let kind = if matches!(
            &request,
            ToolRequest::CargoCheck { .. } | ToolRequest::CargoTest { .. }
        ) {
            ToolResultKind::Validation
        } else {
            ToolResultKind::Runtime
        };
        self.charge_exploratory_result_call(lease, receipt, request, kind)
    }

    /// Atomically charges and binds every runtime action in one exploratory
    /// provider batch before the first action can execute.
    ///
    /// Role/schema/redaction validation remains the role engine's first gate;
    /// this second gate independently rejects controls, malformed IDs and
    /// invalid action values while atomically protecting the task/role call
    /// reservations.
    pub fn preflight_exploratory_runtime_batch(
        &mut self,
        lease: &mut RoleBudgetLease,
        receipt: &mut ProviderResponseReceipt,
        calls: Vec<ToolCall>,
    ) -> Result<ExploratoryBatchPermit, BudgetError> {
        self.ensure_matching_lease(lease)?;
        self.validate_response_receipt(lease, receipt)?;
        if receipt.violation.is_some() {
            return Err(BudgetError::ProviderResponseLimitViolation);
        }
        if receipt.class != ChargeClass::Exploratory {
            return Err(BudgetError::ProviderResponseClassMismatch);
        }
        if lease.active_required_action.is_some() {
            return Err(BudgetError::RequiredActionInProgress);
        }
        if !lease.pending_tool_results.is_empty() {
            return Err(BudgetError::ToolResultPending);
        }
        if calls.is_empty() {
            return Err(BudgetError::InvalidExploratoryRuntimeBatch);
        }

        let mut seen_ids = HashSet::with_capacity(calls.len());
        let mut validated = Vec::with_capacity(calls.len());
        for call in calls {
            if call.id.is_empty()
                || call.id.len() > 256
                || call.id.chars().any(char::is_control)
                || !seen_ids.insert(call.id.clone())
                || call.request.validate().is_err()
            {
                return Err(BudgetError::InvalidExploratoryRuntimeBatch);
            }
            let ActionRequest::Runtime(request) = call.request else {
                return Err(BudgetError::InvalidExploratoryRuntimeBatch);
            };
            let kind = match &request {
                RuntimeActionRequest::Tool(
                    ToolRequest::CargoCheck { .. } | ToolRequest::CargoTest { .. },
                )
                | RuntimeActionRequest::Validation { .. }
                | RuntimeActionRequest::ValidationSelector { .. } => ToolResultKind::Validation,
                RuntimeActionRequest::Tool(_) => ToolResultKind::Runtime,
                // Authoritative review coverage actions are never exploratory;
                // their typed required-action permits own the reservation.
                RuntimeActionRequest::ReviewDiffManifest { .. }
                | RuntimeActionRequest::ReviewDiffChunks { .. } => {
                    return Err(BudgetError::InvalidExploratoryRuntimeBatch);
                }
            };
            validated.push((call.id, request, kind));
        }

        let call_count =
            u32::try_from(validated.len()).map_err(|_| BudgetError::ArithmeticOverflow {
                resource: BudgetResource::ModelVisibleCalls,
            })?;
        let (next_task, next_role, next_required) = preview_u32_charge(
            self.usage.model_visible_calls,
            lease.usage.model_visible_calls,
            call_count,
            TASK_CALL_LIMIT,
            lease.limits().model_visible_calls,
            self.pending_model_visible_calls(),
            lease.required_reservation.model_visible_calls,
            ChargeMode::Exploratory,
            lease.role(),
            BudgetResource::ModelVisibleCalls,
        )?;
        let next_receipt_calls = receipt.model_visible_calls.checked_add(call_count).ok_or(
            BudgetError::ArithmeticOverflow {
                resource: BudgetResource::ModelVisibleCalls,
            },
        )?;

        let validation_count = validated
            .iter()
            .filter(|(_, _, kind)| *kind == ToolResultKind::Validation)
            .count();
        let runtime_count = validated.len() - validation_count;
        let validation_reservation = validation_count
            .checked_mul(VALIDATION_RETAINED_RESULT_LIMIT)
            .ok_or(BudgetError::ArithmeticOverflow {
                resource: BudgetResource::RetainedResultBytes,
            })?;
        let task_protected = self
            .pending_retained_result_bytes()
            .checked_add(lease.required_reservation.retained_result_bytes)
            .ok_or(BudgetError::ArithmeticOverflow {
                resource: BudgetResource::RetainedResultBytes,
            })?;
        let task_available = TASK_RETAINED_RESULT_LIMIT
            .checked_sub(self.usage.retained_result_bytes)
            .and_then(|remaining| remaining.checked_sub(task_protected))
            .ok_or(BudgetError::ReservationWouldBeConsumed {
                resource: BudgetResource::RetainedResultBytes,
            })?;
        let role_available = lease
            .limits()
            .retained_result_bytes
            .checked_sub(lease.usage.retained_result_bytes)
            .and_then(|remaining| {
                remaining.checked_sub(lease.required_reservation.retained_result_bytes)
            })
            .ok_or(BudgetError::ReservationWouldBeConsumed {
                resource: BudgetResource::RetainedResultBytes,
            })?;
        let available = task_available.min(role_available);
        let runtime_minimum = runtime_count
            .checked_mul(MIN_EXPLORATORY_RESULT_WRAPPER_BYTES)
            .ok_or(BudgetError::ArithmeticOverflow {
                resource: BudgetResource::RetainedResultBytes,
            })?;
        let minimum = validation_reservation.checked_add(runtime_minimum).ok_or(
            BudgetError::ArithmeticOverflow {
                resource: BudgetResource::RetainedResultBytes,
            },
        )?;
        if minimum > available {
            return Err(BudgetError::ReservationWouldBeConsumed {
                resource: BudgetResource::RetainedResultBytes,
            });
        }
        let runtime_cap = if runtime_count == 0 {
            0
        } else {
            (available - validation_reservation)
                .checked_div(runtime_count)
                .ok_or(BudgetError::ArithmeticOverflow {
                    resource: BudgetResource::RetainedResultBytes,
                })?
                .min(SINGLE_RETAINED_RESULT_LIMIT)
        };
        let batch_retained_reservation = validation_reservation
            .checked_add(runtime_cap.checked_mul(runtime_count).ok_or(
                BudgetError::ArithmeticOverflow {
                    resource: BudgetResource::RetainedResultBytes,
                },
            )?)
            .ok_or(BudgetError::ArithmeticOverflow {
                resource: BudgetResource::RetainedResultBytes,
            })?;

        let mut next_result_id = self.next_result_id;
        let mut invocations = Vec::with_capacity(validated.len());
        for (tool_call_id, request, kind) in validated {
            next_result_id = next_result_id
                .checked_add(1)
                .ok_or(BudgetError::RetainedResultIdExhausted)?;
            let result_id = RetainedResultId(next_result_id);
            invocations.push(BudgetedRoleInvocation {
                tool_call_id,
                request,
                result_permit: ToolResultPermit {
                    lease_id: lease.lease_id,
                    role_run: lease.role_run,
                    result_id,
                    kind,
                },
                wrapper_cap: match kind {
                    ToolResultKind::Runtime => runtime_cap,
                    ToolResultKind::Validation => VALIDATION_RETAINED_RESULT_LIMIT,
                },
            });
        }

        self.usage.model_visible_calls = next_task;
        lease.usage.model_visible_calls = next_role;
        lease.required_reservation.model_visible_calls = next_required;
        receipt.model_visible_calls = next_receipt_calls;
        self.next_result_id = next_result_id;
        lease.pending_batch_retained_reservation = batch_retained_reservation;
        lease.pending_tool_results.extend(
            invocations
                .iter()
                .map(|invocation| invocation.result_permit.result_id),
        );
        Ok(ExploratoryBatchPermit {
            ledger_id: self.ensure_ledger_identity()?,
            lease_id: lease.lease_id,
            role_run: lease.role_run,
            request_id: receipt.request_id,
            invocations,
        })
    }

    /// Charges and binds the one non-terminal Executor control before any
    /// durable plan mutation occurs.
    pub fn preflight_exploratory_control_result(
        &mut self,
        lease: &mut RoleBudgetLease,
        receipt: &mut ProviderResponseReceipt,
        call: &ToolCall,
        result: &RetainedToolResult,
    ) -> Result<ExploratoryControlPermit, BudgetError> {
        self.ensure_unsealed_lease(lease)?;
        self.validate_response_receipt(lease, receipt)?;
        if receipt.violation.is_some() {
            return Err(BudgetError::ProviderResponseLimitViolation);
        }
        if receipt.class != ChargeClass::Exploratory
            || lease.role() != BudgetRole::Executor
            || lease.active_required_action.is_some()
            || !lease.pending_tool_results.is_empty()
            || lease.pending_batch_retained_reservation != 0
            || call.id.is_empty()
            || call.id.len() > 256
            || call.id.chars().any(char::is_control)
            || call.id != result.tool_call_id()
            || !matches!(
                call.request,
                ActionRequest::Control(ControlRequest::UpdatePlanProgress(_))
            )
        {
            return Err(BudgetError::InvalidExploratoryControl);
        }
        let observed = result.wrapper_len();
        if observed > PLAN_PROGRESS_RETAINED_RESULT_LIMIT {
            return Err(BudgetError::ToolResultKindLimitExceeded {
                limit: PLAN_PROGRESS_RETAINED_RESULT_LIMIT,
                observed,
            });
        }

        let result_id = self.preview_next_result_id()?;
        let (next_task_calls, next_role_calls, next_required_calls) = preview_u32_charge(
            self.usage.model_visible_calls,
            lease.usage.model_visible_calls,
            1,
            TASK_CALL_LIMIT,
            lease.limits().model_visible_calls,
            self.pending_model_visible_calls(),
            lease.required_reservation.model_visible_calls,
            ChargeMode::Exploratory,
            lease.role(),
            BudgetResource::ModelVisibleCalls,
        )?;
        let next_receipt_calls =
            receipt
                .model_visible_calls
                .checked_add(1)
                .ok_or(BudgetError::ArithmeticOverflow {
                    resource: BudgetResource::ModelVisibleCalls,
                })?;
        let next_task_bytes = self
            .usage
            .retained_result_bytes
            .checked_add(observed)
            .ok_or(BudgetError::ArithmeticOverflow {
                resource: BudgetResource::RetainedResultBytes,
            })?;
        let next_role_bytes = lease
            .usage
            .retained_result_bytes
            .checked_add(observed)
            .ok_or(BudgetError::ArithmeticOverflow {
                resource: BudgetResource::RetainedResultBytes,
            })?;
        preview_usize_charge_from_sums(
            next_task_bytes,
            next_role_bytes,
            observed,
            TASK_RETAINED_RESULT_LIMIT,
            lease.limits().retained_result_bytes,
            self.pending_retained_result_bytes(),
            lease.required_reservation.retained_result_bytes,
            ChargeMode::Exploratory,
            lease.role(),
            BudgetResource::RetainedResultBytes,
        )?;

        self.usage.model_visible_calls = next_task_calls;
        lease.usage.model_visible_calls = next_role_calls;
        lease.required_reservation.model_visible_calls = next_required_calls;
        receipt.model_visible_calls = next_receipt_calls;
        self.next_result_id = result_id.0;
        lease.pending_tool_results.insert(result_id);
        Ok(ExploratoryControlPermit {
            ledger_id: self.ensure_ledger_identity()?,
            lease_id: lease.lease_id,
            role_run: lease.role_run,
            request_id: receipt.request_id,
            tool_call_id: call.id.clone(),
            control: match &call.request {
                ActionRequest::Control(control) => control.clone(),
                _ => unreachable!("the control shape was validated"),
            },
            result_permit: ToolResultPermit {
                lease_id: lease.lease_id,
                role_run: lease.role_run,
                result_id,
                kind: ToolResultKind::Runtime,
            },
            wrapper_bytes: result.wrapper_bytes().into(),
        })
    }

    pub fn retain_exploratory_control_result(
        &mut self,
        lease: &mut RoleBudgetLease,
        permit: &ExploratoryControlPermit,
        result: &RetainedToolResult,
    ) -> Result<(), BudgetError> {
        self.validate_exploratory_control_permit(lease, permit)?;
        if permit.tool_call_id != result.tool_call_id()
            || permit.wrapper_bytes.as_ref() != result.wrapper_bytes()
        {
            return Err(BudgetError::InvalidExploratoryControl);
        }
        self.retain_result(
            lease,
            permit.result_permit.result_id,
            PLAN_PROGRESS_RETAINED_RESULT_LIMIT,
            result.wrapper_bytes(),
            ChargeMode::Exploratory,
        )?;
        Ok(())
    }

    pub fn abort_exploratory_control_result(
        &mut self,
        lease: &mut RoleBudgetLease,
        permit: &ExploratoryControlPermit,
    ) -> Result<(), BudgetError> {
        self.validate_exploratory_control_permit(lease, permit)?;
        lease
            .pending_tool_results
            .remove(&permit.result_permit.result_id);
        Ok(())
    }

    /// Atomically retains the complete canonical wrappers for a previously
    /// preflighted batch. A mismatch or byte-limit failure records none of the
    /// results and leaves every result permit pending.
    pub fn retain_exploratory_batch_results(
        &mut self,
        lease: &mut RoleBudgetLease,
        permit: &ExploratoryBatchPermit,
        results: &[RetainedToolResult],
    ) -> Result<(), BudgetError> {
        self.ensure_unsealed_lease(lease)?;
        if permit.ledger_id != self.ensure_ledger_identity()?
            || permit.lease_id != lease.lease_id
            || permit.role_run != lease.role_run
            || self.open_provider_response
                != Some((permit.request_id, permit.lease_id, permit.role_run))
            || permit.invocations.len() != results.len()
        {
            return Err(BudgetError::ExploratoryBatchPermitMismatch);
        }
        let expected_reservation =
            permit
                .invocations
                .iter()
                .try_fold(0usize, |total, invocation| {
                    total.checked_add(invocation.wrapper_cap).ok_or(
                        BudgetError::ArithmeticOverflow {
                            resource: BudgetResource::RetainedResultBytes,
                        },
                    )
                })?;
        if expected_reservation != lease.pending_batch_retained_reservation {
            return Err(BudgetError::ExploratoryBatchPermitMismatch);
        }

        let mut total_bytes = 0usize;
        for (invocation, result) in permit.invocations.iter().zip(results) {
            let result_id = invocation.result_permit.result_id;
            if invocation.result_permit.lease_id != lease.lease_id
                || invocation.result_permit.role_run != lease.role_run
                || invocation.tool_call_id != result.tool_call_id()
                || !lease.pending_tool_results.contains(&result_id)
                || self.retained_results.contains_key(&result_id)
            {
                return Err(BudgetError::ExploratoryBatchPermitMismatch);
            }
            let observed = result.wrapper_len();
            let limit = invocation.wrapper_cap;
            if observed > limit {
                return Err(BudgetError::ToolResultKindLimitExceeded { limit, observed });
            }
            total_bytes =
                total_bytes
                    .checked_add(observed)
                    .ok_or(BudgetError::ArithmeticOverflow {
                        resource: BudgetResource::RetainedResultBytes,
                    })?;
        }

        let next_task = self
            .usage
            .retained_result_bytes
            .checked_add(total_bytes)
            .ok_or(BudgetError::ArithmeticOverflow {
                resource: BudgetResource::RetainedResultBytes,
            })?;
        let next_role = lease
            .usage
            .retained_result_bytes
            .checked_add(total_bytes)
            .ok_or(BudgetError::ArithmeticOverflow {
                resource: BudgetResource::RetainedResultBytes,
            })?;
        let (next_task, next_role, next_required) = preview_usize_charge_from_sums(
            next_task,
            next_role,
            total_bytes,
            TASK_RETAINED_RESULT_LIMIT,
            lease.limits().retained_result_bytes,
            self.pending_retained_result_bytes(),
            lease.required_reservation.retained_result_bytes,
            ChargeMode::Exploratory,
            lease.role(),
            BudgetResource::RetainedResultBytes,
        )?;

        for (invocation, result) in permit.invocations.iter().zip(results) {
            let result_id = invocation.result_permit.result_id;
            self.retained_results.insert(
                result_id,
                RecordedRetainedResult {
                    owner: lease.role_run,
                    wrapper_bytes: result.wrapper_bytes().into(),
                },
            );
            lease.pending_tool_results.remove(&result_id);
        }
        self.usage.retained_result_bytes = next_task;
        lease.usage.retained_result_bytes = next_role;
        lease.required_reservation.retained_result_bytes = next_required;
        lease.pending_batch_retained_reservation = 0;
        Ok(())
    }

    /// Releases an unconsumed whole-batch result reservation after runtime,
    /// cancellation, redaction, or encoding failure. Model-visible call usage
    /// remains charged because the provider response was already received.
    pub fn abort_exploratory_runtime_batch(
        &mut self,
        lease: &mut RoleBudgetLease,
        permit: &ExploratoryBatchPermit,
    ) -> Result<(), BudgetError> {
        self.ensure_unsealed_lease(lease)?;
        if permit.ledger_id != self.ensure_ledger_identity()?
            || permit.lease_id != lease.lease_id
            || permit.role_run != lease.role_run
            || self.open_provider_response
                != Some((permit.request_id, permit.lease_id, permit.role_run))
        {
            return Err(BudgetError::ExploratoryBatchPermitMismatch);
        }
        if permit.invocations.iter().any(|invocation| {
            !lease
                .pending_tool_results
                .contains(&invocation.result_permit.result_id)
        }) {
            return Err(BudgetError::ExploratoryBatchPermitMismatch);
        }
        for invocation in &permit.invocations {
            lease
                .pending_tool_results
                .remove(&invocation.result_permit.result_id);
        }
        lease.pending_batch_retained_reservation = 0;
        Ok(())
    }

    pub fn charge_required_call(
        &mut self,
        lease: &mut RoleBudgetLease,
        permit: &mut RequiredActionPermit,
        receipt: &mut ProviderResponseReceipt,
    ) -> Result<(), BudgetError> {
        self.validate_required_permit(lease, permit)?;
        self.validate_response_receipt(lease, receipt)?;
        if receipt.violation.is_some() {
            return Err(BudgetError::ProviderResponseLimitViolation);
        }
        if receipt.class
            != (ChargeClass::Required {
                action_id: permit.action_id,
            })
        {
            return Err(BudgetError::ProviderResponseClassMismatch);
        }
        if !permit.response_charged {
            return Err(BudgetError::RequiredResponseNotCharged);
        }
        if permit.call_charged {
            return Err(BudgetError::RequiredCallAlreadyCharged);
        }
        let next_result_id = if permit.action.expects_result() {
            Some(self.preview_next_result_id()?)
        } else {
            None
        };
        let (next_task, next_role, next_required) = preview_u32_charge(
            self.usage.model_visible_calls,
            lease.usage.model_visible_calls,
            1,
            TASK_CALL_LIMIT,
            lease.limits().model_visible_calls,
            self.pending_model_visible_calls(),
            lease.required_reservation.model_visible_calls,
            ChargeMode::Required,
            lease.role(),
            BudgetResource::ModelVisibleCalls,
        )?;
        let next_receipt_calls =
            receipt
                .model_visible_calls
                .checked_add(1)
                .ok_or(BudgetError::ArithmeticOverflow {
                    resource: BudgetResource::ModelVisibleCalls,
                })?;

        self.usage.model_visible_calls = next_task;
        lease.usage.model_visible_calls = next_role;
        lease.required_reservation.model_visible_calls = next_required;
        receipt.model_visible_calls = next_receipt_calls;
        permit.call_charged = true;
        if let Some(result_id) = next_result_id {
            self.next_result_id = result_id.0;
            lease.pending_tool_results.insert(result_id);
            permit.result_id = Some(result_id);
        }
        Ok(())
    }

    fn retain_exploratory_wrapper_bytes(
        &mut self,
        lease: &mut RoleBudgetLease,
        invocation: &BudgetedToolInvocation,
        wrapper_bytes: &[u8],
    ) -> Result<RetainedResultCharge, BudgetError> {
        self.validate_budgeted_invocation(lease, invocation)?;
        self.retain_result(
            lease,
            invocation.result_permit.result_id,
            invocation.byte_limit(),
            wrapper_bytes,
            ChargeMode::Exploratory,
        )
    }

    /// The public retained-result entry point accepts only the canonical,
    /// redaction-verified wrapper object.
    pub fn retain_exploratory_tool_result(
        &mut self,
        lease: &mut RoleBudgetLease,
        invocation: &BudgetedToolInvocation,
        result: &RetainedToolResult,
    ) -> Result<RetainedResultCharge, BudgetError> {
        self.retain_exploratory_wrapper_bytes(lease, invocation, result.wrapper_bytes())
    }

    fn retain_required_wrapper_bytes(
        &mut self,
        lease: &mut RoleBudgetLease,
        permit: &mut RequiredActionPermit,
        wrapper_bytes: &[u8],
    ) -> Result<RetainedResultCharge, BudgetError> {
        self.validate_required_permit(lease, permit)?;
        if !permit.call_charged {
            return Err(BudgetError::RequiredCallNotCharged);
        }
        let result_id = permit
            .result_id
            .ok_or(BudgetError::RequiredActionHasNoResult)?;
        let limit = permit
            .action
            .result_limit()
            .ok_or(BudgetError::RequiredActionHasNoResult)?;
        let outcome =
            self.retain_result(lease, result_id, limit, wrapper_bytes, ChargeMode::Required)?;
        if outcome == RetainedResultCharge::Charged {
            permit.result_bytes = Some(wrapper_bytes.len());
        }
        Ok(outcome)
    }

    /// Required actions share the same unique canonical retained wrapper.
    pub fn retain_required_tool_result(
        &mut self,
        lease: &mut RoleBudgetLease,
        permit: &mut RequiredActionPermit,
        result: &RetainedToolResult,
    ) -> Result<RetainedResultCharge, BudgetError> {
        self.retain_required_wrapper_bytes(lease, permit, result.wrapper_bytes())
    }

    /// Releases an unretained result permit after a charged required runtime
    /// action fails, is cancelled, or returns the wrong typed result.
    ///
    /// The provider response and model-visible call remain charged. Borrowing
    /// the exact active action permit lets callers close the response without
    /// losing the capability needed to diagnose a mismatched ledger/lease.
    pub fn abort_required_runtime_result(
        &mut self,
        lease: &mut RoleBudgetLease,
        permit: &RequiredActionPermit,
        receipt: &ProviderResponseReceipt,
    ) -> Result<(), BudgetError> {
        self.validate_required_permit(lease, permit)?;
        self.validate_response_receipt(lease, receipt)?;
        if receipt.class
            != (ChargeClass::Required {
                action_id: permit.action_id,
            })
            || !permit.response_charged
        {
            return Err(BudgetError::ProviderResponseClassMismatch);
        }
        if !permit.call_charged {
            return Err(BudgetError::RequiredCallNotCharged);
        }
        let result_id = permit
            .result_id
            .ok_or(BudgetError::RequiredActionHasNoResult)?;
        if permit.result_bytes.is_some()
            || !lease.pending_tool_results.contains(&result_id)
            || self.retained_results.contains_key(&result_id)
        {
            return Err(BudgetError::RetainedResultPermitNotPending { result_id });
        }
        lease.pending_tool_results.remove(&result_id);
        Ok(())
    }

    /// Same-crate hook for Task 7's typed runtime manifest adapter.
    ///
    /// The raw count must never be sourced from provider arguments, provider
    /// text, or decoded `ToolResult` text. External callers cannot mint the
    /// returned receipt.
    #[allow(dead_code)]
    pub(crate) fn observe_typed_reviewer_manifest(
        &self,
        lease: &RoleBudgetLease,
        permit: &RequiredActionPermit,
        chunk_count: u8,
    ) -> Result<ReviewerManifestBudgetReceipt, BudgetError> {
        self.validate_required_permit(lease, permit)?;
        if permit.action != RequiredBudgetAction::ReviewerManifest {
            return Err(BudgetError::NotReviewerManifestAction);
        }
        let result_id = permit
            .result_id
            .filter(|_| permit.result_bytes.is_some())
            .ok_or(BudgetError::RequiredResultNotCharged)?;
        if chunk_count > MAX_REVIEW_DIFF_CHUNKS {
            return Err(BudgetError::TooManyReviewDiffChunks {
                observed: chunk_count,
            });
        }
        Ok(ReviewerManifestBudgetReceipt {
            lease_id: lease.lease_id,
            role_run: lease.role_run,
            action_id: permit.action_id,
            result_id,
            chunk_count,
        })
    }

    /// Releases only the coverage reservation proven unnecessary by a typed,
    /// same-action runtime manifest receipt.
    pub fn bind_reviewer_manifest(
        &mut self,
        lease: &mut RoleBudgetLease,
        permit: &mut RequiredActionPermit,
        manifest: ReviewerManifestBudgetReceipt,
    ) -> Result<(), BudgetError> {
        self.validate_required_permit(lease, permit)?;
        if permit.action != RequiredBudgetAction::ReviewerManifest {
            return Err(BudgetError::NotReviewerManifestAction);
        }
        if permit.manifest_bound || lease.reviewer_manifest_bound {
            return Err(BudgetError::ReviewerManifestAlreadyBound);
        }
        if permit.result_bytes.is_none() {
            return Err(BudgetError::RequiredResultNotCharged);
        }
        if manifest.lease_id != lease.lease_id
            || manifest.role_run != lease.role_run
            || manifest.action_id != permit.action_id
            || Some(manifest.result_id) != permit.result_id
        {
            return Err(BudgetError::ReviewerManifestReceiptMismatch);
        }
        let chunk_count = manifest.chunk_count;

        let full_batches = chunk_count / 2;
        let tail_chunks = chunk_count % 2;
        let batch_count =
            full_batches
                .checked_add(tail_chunks)
                .ok_or(BudgetError::ArithmeticOverflow {
                    resource: BudgetResource::ModelResponses,
                })?;
        let remaining_actions =
            u32::from(batch_count)
                .checked_add(1)
                .ok_or(BudgetError::ArithmeticOverflow {
                    resource: BudgetResource::ModelResponses,
                })?;
        let full_batch_bytes = usize::from(full_batches)
            .checked_mul(REVIEW_DIFF_BATCH_RETAINED_RESULT_LIMIT)
            .ok_or(BudgetError::ArithmeticOverflow {
                resource: BudgetResource::RetainedResultBytes,
            })?;
        let tail_bytes = usize::from(tail_chunks)
            .checked_mul(REVIEW_DIFF_CHUNK_RETAINED_RESULT_LIMIT)
            .ok_or(BudgetError::ArithmeticOverflow {
                resource: BudgetResource::RetainedResultBytes,
            })?;
        let remaining_bytes =
            full_batch_bytes
                .checked_add(tail_bytes)
                .ok_or(BudgetError::ArithmeticOverflow {
                    resource: BudgetResource::RetainedResultBytes,
                })?;
        if remaining_actions > lease.required_reservation.model_responses
            || remaining_actions > lease.required_reservation.model_visible_calls
            || remaining_bytes > lease.required_reservation.retained_result_bytes
        {
            return Err(BudgetError::ReservationInvariantBroken);
        }

        lease.required_reservation.model_responses = remaining_actions;
        lease.required_reservation.model_visible_calls = remaining_actions;
        lease.required_reservation.retained_result_bytes = remaining_bytes;
        for batch_index in 0..batch_count {
            let start_chunk =
                batch_index
                    .checked_mul(2)
                    .ok_or(BudgetError::ArithmeticOverflow {
                        resource: BudgetResource::ModelVisibleCalls,
                    })?;
            let count = (chunk_count - start_chunk).min(2);
            lease
                .required_actions
                .push_back(RequiredBudgetAction::ReviewerChunkBatch {
                    batch_index,
                    start_chunk,
                    count,
                });
        }
        lease
            .required_actions
            .push_back(RequiredBudgetAction::ReviewerTerminal);
        permit.manifest_bound = true;
        lease.reviewer_manifest_bound = true;
        Ok(())
    }

    pub fn complete_required_action(
        &mut self,
        lease: &mut RoleBudgetLease,
        permit: RequiredActionPermit,
    ) -> Result<(), BudgetError> {
        self.validate_required_permit(lease, &permit)?;
        self.ensure_no_provider_exchange()?;
        if !permit.response_charged {
            return Err(BudgetError::RequiredResponseNotCharged);
        }
        if !permit.call_charged {
            return Err(BudgetError::RequiredCallNotCharged);
        }
        if permit.action.expects_result() && permit.result_bytes.is_none() {
            return Err(BudgetError::RequiredResultNotCharged);
        }
        if permit.action == RequiredBudgetAction::ReviewerManifest && !permit.manifest_bound {
            return Err(BudgetError::ReviewerManifestNotBound);
        }
        if !lease.pending_tool_results.is_empty() {
            return Err(BudgetError::ToolResultPending);
        }
        if let Some(limit) = permit.action.result_limit()
            && permit.action != RequiredBudgetAction::ReviewerManifest
        {
            let observed = permit
                .result_bytes
                .ok_or(BudgetError::RequiredResultNotCharged)?;
            let slack = limit
                .checked_sub(observed)
                .ok_or(BudgetError::ReservationInvariantBroken)?;
            lease.required_reservation.retained_result_bytes = lease
                .required_reservation
                .retained_result_bytes
                .checked_sub(slack)
                .ok_or(BudgetError::ReservationInvariantBroken)?;
        }

        let normal_terminal = matches!(
            permit.action,
            RequiredBudgetAction::PlannerTerminal
                | RequiredBudgetAction::ExecutorTerminal
                | RequiredBudgetAction::ReviewerTerminal
        );
        if normal_terminal
            && (lease.required_actions.len() != 1
                || lease.required_reservation != BudgetReservation::default())
        {
            return Err(BudgetError::ReservationInvariantBroken);
        }
        let expected = lease
            .required_actions
            .pop_front()
            .ok_or(BudgetError::NoRequiredActionPending)?;
        if expected != permit.action {
            return Err(BudgetError::RequiredActionOrderMismatch);
        }
        lease.active_required_action = None;
        if normal_terminal {
            lease.termination = Some(RoleBudgetTermination::Normal);
        }
        Ok(())
    }

    fn validate_early_termination(
        &self,
        role: BudgetRole,
        termination: EarlyRoleBudgetTermination,
    ) -> Result<(), BudgetError> {
        match termination {
            EarlyRoleBudgetTermination::ReportBlocked => Ok(()),
            EarlyRoleBudgetTermination::ReviewerChangesRequested
                if role == BudgetRole::Reviewer =>
            {
                Ok(())
            }
            EarlyRoleBudgetTermination::ReviewerChangesRequested => {
                Err(BudgetError::ReviewerChangesRequestedByNonReviewer)
            }
        }
    }

    fn validate_exploratory_normal_terminal_state(
        &self,
        lease: &RoleBudgetLease,
        receipt: &ProviderResponseReceipt,
    ) -> Result<(), BudgetError> {
        self.validate_response_receipt(lease, receipt)?;
        if receipt.violation.is_some() {
            return Err(BudgetError::ProviderResponseLimitViolation);
        }
        if receipt.class != ChargeClass::Exploratory {
            return Err(BudgetError::ProviderResponseClassMismatch);
        }
        if receipt.model_visible_calls != 0 || !lease.pending_tool_results.is_empty() {
            return Err(BudgetError::NormalTerminalHasSideEffects);
        }
        if lease.active_required_action.is_some() {
            return Err(BudgetError::RequiredActionInProgress);
        }
        if lease.pending_batch_retained_reservation != 0 {
            return Err(BudgetError::ExploratoryBatchAlreadyPending);
        }
        Ok(())
    }

    fn charge_reserved_terminal_call(
        &mut self,
        lease: &mut RoleBudgetLease,
        receipt: &mut ProviderResponseReceipt,
    ) -> Result<(), BudgetError> {
        let (next_task, next_role, next_required) = preview_u32_charge(
            self.usage.model_visible_calls,
            lease.usage.model_visible_calls,
            1,
            TASK_CALL_LIMIT,
            lease.limits().model_visible_calls,
            self.pending_model_visible_calls(),
            lease.required_reservation.model_visible_calls,
            ChargeMode::Required,
            lease.role(),
            BudgetResource::ModelVisibleCalls,
        )?;
        let next_receipt_calls =
            receipt
                .model_visible_calls
                .checked_add(1)
                .ok_or(BudgetError::ArithmeticOverflow {
                    resource: BudgetResource::ModelVisibleCalls,
                })?;
        self.usage.model_visible_calls = next_task;
        lease.usage.model_visible_calls = next_role;
        lease.required_reservation.model_visible_calls = next_required;
        receipt.model_visible_calls = next_receipt_calls;
        self.open_provider_response = None;
        Ok(())
    }

    fn apply_early_termination(
        &mut self,
        lease: &mut RoleBudgetLease,
        termination: EarlyRoleBudgetTermination,
    ) {
        lease.required_reservation = BudgetReservation::default();
        lease.required_actions.clear();
        lease.active_required_action = None;
        lease.termination = Some(termination.into());
        self.pending_reviewer = None;
    }

    fn begin_provider_request_len(
        &mut self,
        lease: &RoleBudgetLease,
        encoded_bytes: usize,
        maximum_response_bytes: usize,
        class: ChargeClass,
    ) -> Result<ProviderRequestPermit, BudgetError> {
        let ledger_id = self.ensure_ledger_identity()?;
        self.ensure_unsealed_lease(lease)?;
        self.ensure_no_provider_exchange()?;
        if !lease.pending_tool_results.is_empty() {
            return Err(BudgetError::ToolResultPending);
        }
        let next_request = self.usage.provider_bytes.checked_add(encoded_bytes).ok_or(
            BudgetError::ArithmeticOverflow {
                resource: BudgetResource::ProviderBytes,
            },
        )?;
        let reserved_end = next_request.checked_add(maximum_response_bytes).ok_or(
            BudgetError::ArithmeticOverflow {
                resource: BudgetResource::ProviderBytes,
            },
        )?;
        let mode = match class {
            ChargeClass::Required { .. } => ChargeMode::Required,
            ChargeClass::Exploratory => ChargeMode::Exploratory,
        };
        preview_u32_charge(
            self.usage.model_responses,
            lease.usage.model_responses,
            1,
            TASK_RESPONSE_LIMIT,
            lease.limits().model_responses,
            self.pending_model_responses(),
            lease.required_reservation.model_responses,
            mode,
            lease.role(),
            BudgetResource::ModelResponses,
        )?;
        if encoded_bytes > PROVIDER_REQUEST_BYTE_LIMIT {
            return Err(BudgetError::ProviderRequestTooLarge {
                observed: encoded_bytes,
            });
        }
        if maximum_response_bytes == 0 {
            return Err(BudgetError::ProviderResponseReservationRequired);
        }
        if maximum_response_bytes > PROVIDER_RESPONSE_BYTE_LIMIT {
            return Err(BudgetError::ProviderResponseTooLarge {
                observed: maximum_response_bytes,
            });
        }
        if reserved_end > TASK_PROVIDER_BYTE_LIMIT {
            return Err(BudgetError::TaskLimitExceeded {
                resource: BudgetResource::ProviderBytes,
            });
        }
        let request_id = self
            .next_provider_request_id
            .checked_add(1)
            .ok_or(BudgetError::ProviderRequestIdExhausted)?;

        self.usage.provider_bytes = next_request;
        self.next_provider_request_id = request_id;
        self.pending_provider_response = Some(PendingProviderResponse {
            ledger_id,
            request_id,
            lease_id: lease.lease_id,
            role_run: lease.role_run,
            maximum_bytes: maximum_response_bytes,
            class,
        });
        Ok(ProviderRequestPermit {
            ledger_id,
            request_id,
            lease_id: lease.lease_id,
            role_run: lease.role_run,
            class,
        })
    }

    fn record_provider_response_len(
        &mut self,
        lease: &mut RoleBudgetLease,
        pending: PendingProviderResponse,
        encoded_bytes: usize,
    ) -> Result<ProviderResponseReceipt, BudgetError> {
        let next_provider_bytes = self.usage.provider_bytes.checked_add(encoded_bytes).ok_or(
            BudgetError::ArithmeticOverflow {
                resource: BudgetResource::ProviderBytes,
            },
        )?;
        let mode = match pending.class {
            ChargeClass::Required { .. } => ChargeMode::Required,
            ChargeClass::Exploratory => ChargeMode::Exploratory,
        };
        let (next_task, next_role, next_required) = preview_u32_charge(
            self.usage.model_responses,
            lease.usage.model_responses,
            1,
            TASK_RESPONSE_LIMIT,
            lease.limits().model_responses,
            self.pending_model_responses(),
            lease.required_reservation.model_responses,
            mode,
            lease.role(),
            BudgetResource::ModelResponses,
        )?;
        let violation = if encoded_bytes > PROVIDER_RESPONSE_BYTE_LIMIT {
            Some(ProviderResponseViolation::ResponseByteLimit)
        } else if encoded_bytes > pending.maximum_bytes {
            Some(ProviderResponseViolation::ReservedByteLimit)
        } else if next_provider_bytes > TASK_PROVIDER_BYTE_LIMIT {
            Some(ProviderResponseViolation::TaskProviderByteLimit)
        } else {
            None
        };

        self.usage.provider_bytes = next_provider_bytes;
        self.usage.model_responses = next_task;
        lease.usage.model_responses = next_role;
        lease.required_reservation.model_responses = next_required;
        self.pending_provider_response = None;
        self.open_provider_response =
            Some((pending.request_id, pending.lease_id, pending.role_run));
        Ok(ProviderResponseReceipt {
            ledger_id: pending.ledger_id,
            request_id: pending.request_id,
            lease_id: pending.lease_id,
            role_run: pending.role_run,
            class: pending.class,
            encoded_bytes,
            model_visible_calls: 0,
            violation,
        })
    }

    fn charge_exploratory_result_call(
        &mut self,
        lease: &mut RoleBudgetLease,
        receipt: &mut ProviderResponseReceipt,
        request: ToolRequest,
        kind: ToolResultKind,
    ) -> Result<BudgetedToolInvocation, BudgetError> {
        self.ensure_matching_lease(lease)?;
        self.validate_response_receipt(lease, receipt)?;
        if receipt.violation.is_some() {
            return Err(BudgetError::ProviderResponseLimitViolation);
        }
        if receipt.class != ChargeClass::Exploratory {
            return Err(BudgetError::ProviderResponseClassMismatch);
        }
        if lease.active_required_action.is_some() {
            return Err(BudgetError::RequiredActionInProgress);
        }
        if lease.pending_batch_retained_reservation != 0 {
            return Err(BudgetError::ExploratoryBatchAlreadyPending);
        }
        let result_id = self.preview_next_result_id()?;
        let (next_task, next_role, next_required) = preview_u32_charge(
            self.usage.model_visible_calls,
            lease.usage.model_visible_calls,
            1,
            TASK_CALL_LIMIT,
            lease.limits().model_visible_calls,
            self.pending_model_visible_calls(),
            lease.required_reservation.model_visible_calls,
            ChargeMode::Exploratory,
            lease.role(),
            BudgetResource::ModelVisibleCalls,
        )?;
        let next_receipt_calls =
            receipt
                .model_visible_calls
                .checked_add(1)
                .ok_or(BudgetError::ArithmeticOverflow {
                    resource: BudgetResource::ModelVisibleCalls,
                })?;

        self.usage.model_visible_calls = next_task;
        lease.usage.model_visible_calls = next_role;
        lease.required_reservation.model_visible_calls = next_required;
        self.next_result_id = result_id.0;
        receipt.model_visible_calls = next_receipt_calls;
        lease.pending_tool_results.insert(result_id);
        Ok(BudgetedToolInvocation {
            request,
            result_permit: ToolResultPermit {
                lease_id: lease.lease_id,
                role_run: lease.role_run,
                result_id,
                kind,
            },
        })
    }

    fn retain_result(
        &mut self,
        lease: &mut RoleBudgetLease,
        result_id: RetainedResultId,
        result_limit: usize,
        wrapper_bytes: &[u8],
        mode: ChargeMode,
    ) -> Result<RetainedResultCharge, BudgetError> {
        if let Some(recorded) = self.retained_results.get(&result_id) {
            if recorded.owner != lease.role_run {
                return Err(BudgetError::RetainedResultOwnerConflict { result_id });
            }
            if recorded.wrapper_bytes.as_ref() != wrapper_bytes {
                return Err(BudgetError::RetainedResultContentConflict { result_id });
            }
            return Ok(RetainedResultCharge::AlreadyCounted);
        }
        if lease.pending_batch_retained_reservation != 0 {
            return Err(BudgetError::ExploratoryBatchAlreadyPending);
        }
        if !lease.pending_tool_results.contains(&result_id) {
            return Err(BudgetError::RetainedResultPermitNotPending { result_id });
        }
        let encoded_bytes = wrapper_bytes.len();
        if encoded_bytes > result_limit {
            return Err(BudgetError::ToolResultKindLimitExceeded {
                limit: result_limit,
                observed: encoded_bytes,
            });
        }
        let next_task = self
            .usage
            .retained_result_bytes
            .checked_add(encoded_bytes)
            .ok_or(BudgetError::ArithmeticOverflow {
                resource: BudgetResource::RetainedResultBytes,
            })?;
        let next_role = lease
            .usage
            .retained_result_bytes
            .checked_add(encoded_bytes)
            .ok_or(BudgetError::ArithmeticOverflow {
                resource: BudgetResource::RetainedResultBytes,
            })?;
        let (next_task, next_role, next_required) = preview_usize_charge_from_sums(
            next_task,
            next_role,
            encoded_bytes,
            TASK_RETAINED_RESULT_LIMIT,
            lease.limits().retained_result_bytes,
            self.pending_retained_result_bytes(),
            lease.required_reservation.retained_result_bytes,
            mode,
            lease.role(),
            BudgetResource::RetainedResultBytes,
        )?;
        self.retained_results.insert(
            result_id,
            RecordedRetainedResult {
                owner: lease.role_run,
                wrapper_bytes: wrapper_bytes.into(),
            },
        );
        self.usage.retained_result_bytes = next_task;
        lease.usage.retained_result_bytes = next_role;
        lease.required_reservation.retained_result_bytes = next_required;
        lease.pending_tool_results.remove(&result_id);
        Ok(RetainedResultCharge::Charged)
    }

    fn activate_role(
        &mut self,
        role_run: RoleRun,
        required_reservation: BudgetReservation,
        required_actions: VecDeque<RequiredBudgetAction>,
    ) -> Result<RoleBudgetLease, BudgetError> {
        let lease_id = self
            .next_lease_id
            .checked_add(1)
            .ok_or(BudgetError::LeaseIdExhausted)?;
        let lease = RoleBudgetLease {
            lease_id,
            role_run,
            usage: RoleBudgetUsage::default(),
            required_reservation,
            required_actions,
            active_required_action: None,
            next_action_id: 0,
            pending_tool_results: HashSet::new(),
            pending_batch_retained_reservation: 0,
            reviewer_manifest_bound: false,
            termination: None,
        };
        self.next_lease_id = lease_id;
        self.active_role = Some((lease_id, role_run));
        self.started_role_runs.insert(role_run);
        Ok(lease)
    }

    fn validate_required_permit(
        &self,
        lease: &RoleBudgetLease,
        permit: &RequiredActionPermit,
    ) -> Result<(), BudgetError> {
        self.ensure_unsealed_lease(lease)?;
        if permit.ledger_id != self.ensure_ledger_identity()?
            || permit.lease_id != lease.lease_id
            || permit.role_run != lease.role_run
            || lease.active_required_action != Some(permit.action_id)
        {
            return Err(BudgetError::RequiredPermitMismatch);
        }
        if lease.required_actions.front() != Some(&permit.action) {
            return Err(BudgetError::RequiredActionOrderMismatch);
        }
        Ok(())
    }

    fn validate_budgeted_invocation(
        &self,
        lease: &RoleBudgetLease,
        invocation: &BudgetedToolInvocation,
    ) -> Result<(), BudgetError> {
        let permit = &invocation.result_permit;
        self.ensure_unsealed_lease(lease)?;
        if permit.lease_id != lease.lease_id || permit.role_run != lease.role_run {
            return Err(BudgetError::ToolResultPermitMismatch);
        }
        Ok(())
    }

    fn validate_exploratory_control_permit(
        &self,
        lease: &RoleBudgetLease,
        permit: &ExploratoryControlPermit,
    ) -> Result<(), BudgetError> {
        self.ensure_unsealed_lease(lease)?;
        if permit.ledger_id != self.ensure_ledger_identity()?
            || permit.lease_id != lease.lease_id
            || permit.role_run != lease.role_run
            || self.open_provider_response
                != Some((permit.request_id, permit.lease_id, permit.role_run))
            || permit.result_permit.lease_id != lease.lease_id
            || permit.result_permit.role_run != lease.role_run
            || !lease
                .pending_tool_results
                .contains(&permit.result_permit.result_id)
        {
            return Err(BudgetError::InvalidExploratoryControl);
        }
        Ok(())
    }

    fn validate_response_receipt(
        &self,
        lease: &RoleBudgetLease,
        receipt: &ProviderResponseReceipt,
    ) -> Result<(), BudgetError> {
        self.ensure_unsealed_lease(lease)?;
        if receipt.ledger_id != self.ensure_ledger_identity()?
            || receipt.lease_id != lease.lease_id
            || receipt.role_run != lease.role_run
        {
            return Err(BudgetError::ProviderResponseReceiptMismatch);
        }
        if self.open_provider_response
            != Some((receipt.request_id, receipt.lease_id, receipt.role_run))
        {
            return Err(BudgetError::ProviderResponseReceiptMismatch);
        }
        Ok(())
    }

    fn require_pending_response(
        &self,
        lease: &RoleBudgetLease,
        request: &ProviderRequestPermit,
    ) -> Result<PendingProviderResponse, BudgetError> {
        self.ensure_unsealed_lease(lease)?;
        let pending = self
            .pending_provider_response
            .ok_or(BudgetError::ProviderResponseNotReserved)?;
        if pending.lease_id != lease.lease_id || pending.role_run != lease.role_run {
            return Err(BudgetError::LeaseMismatch {
                active_role: pending.role_run,
                observed_role: lease.role_run,
            });
        }
        if pending.request_id != request.request_id
            || pending.ledger_id != self.ensure_ledger_identity()?
            || pending.ledger_id != request.ledger_id
            || pending.lease_id != request.lease_id
            || pending.role_run != request.role_run
            || pending.class != request.class
        {
            return Err(BudgetError::ProviderRequestPermitMismatch);
        }
        Ok(pending)
    }

    fn ensure_matching_lease(&self, lease: &RoleBudgetLease) -> Result<(), BudgetError> {
        self.ensure_ledger_identity()?;
        match self.active_role {
            Some((lease_id, role_run))
                if lease_id == lease.lease_id && role_run == lease.role_run =>
            {
                Ok(())
            }
            Some((_, role_run)) => Err(BudgetError::LeaseMismatch {
                active_role: role_run,
                observed_role: lease.role_run,
            }),
            None => Err(BudgetError::NoActiveRole),
        }
    }

    fn ensure_unsealed_lease(&self, lease: &RoleBudgetLease) -> Result<(), BudgetError> {
        self.ensure_matching_lease(lease)?;
        if let Some(termination) = lease.termination {
            return Err(BudgetError::RoleAlreadyTerminated { termination });
        }
        Ok(())
    }

    fn ensure_role_start_is_idle(&self) -> Result<(), BudgetError> {
        self.ensure_ledger_identity()?;
        self.ensure_no_active_role()?;
        self.ensure_no_provider_exchange()?;
        if let Some(pending) = self.pending_reviewer {
            return Err(BudgetError::ReviewerReservationAlreadyPending {
                review_round: pending.review_round,
            });
        }
        Ok(())
    }

    fn ensure_ledger_identity(&self) -> Result<u64, BudgetError> {
        self.ledger_id.ok_or(BudgetError::LedgerIdExhausted)
    }

    fn ensure_no_active_role(&self) -> Result<(), BudgetError> {
        if let Some((_, role_run)) = self.active_role {
            return Err(BudgetError::RoleAlreadyActive { role_run });
        }
        Ok(())
    }

    fn ensure_no_provider_exchange(&self) -> Result<(), BudgetError> {
        if self.pending_provider_response.is_some() {
            return Err(BudgetError::ProviderResponsePending);
        }
        if self.open_provider_response.is_some() {
            return Err(BudgetError::ProviderResponseReceiptOpen);
        }
        Ok(())
    }

    fn ensure_role_not_started(&self, role_run: RoleRun) -> Result<(), BudgetError> {
        if self.started_role_runs.contains(&role_run) {
            return Err(BudgetError::RoleRunAlreadyStarted {
                role: role_run.role,
                role_run: role_run.role_run,
            });
        }
        Ok(())
    }

    fn ensure_task_reservation_fits(
        &self,
        reservation: BudgetReservation,
    ) -> Result<(), BudgetError> {
        ensure_u32_reservation_fits(
            self.usage.model_responses,
            reservation.model_responses,
            TASK_RESPONSE_LIMIT,
            BudgetResource::ModelResponses,
        )?;
        ensure_u32_reservation_fits(
            self.usage.model_visible_calls,
            reservation.model_visible_calls,
            TASK_CALL_LIMIT,
            BudgetResource::ModelVisibleCalls,
        )?;
        ensure_usize_reservation_fits(
            self.usage.retained_result_bytes,
            reservation.retained_result_bytes,
            TASK_RETAINED_RESULT_LIMIT,
            BudgetResource::RetainedResultBytes,
        )
    }

    fn pending_model_responses(&self) -> u32 {
        self.pending_reviewer
            .map_or(0, |pending| pending.amounts.model_responses)
    }

    fn pending_model_visible_calls(&self) -> u32 {
        self.pending_reviewer
            .map_or(0, |pending| pending.amounts.model_visible_calls)
    }

    fn pending_retained_result_bytes(&self) -> usize {
        self.pending_reviewer
            .map_or(0, |pending| pending.amounts.retained_result_bytes)
    }

    fn preview_next_result_id(&self) -> Result<RetainedResultId, BudgetError> {
        self.next_result_id
            .checked_add(1)
            .map(RetainedResultId)
            .ok_or(BudgetError::RetainedResultIdExhausted)
    }
}

fn allocate_task_budget_ledger_id(counter: &AtomicU64) -> Result<u64, BudgetError> {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(1)
        })
        .map_err(|_| BudgetError::LedgerIdExhausted)
}

fn missing_checks(
    required_checks: &RequiredCheckLedger,
    checkpoint: &WorkspaceCheckpoint,
) -> Vec<RequiredCheck> {
    let passed = required_checks
        .current_evidence(checkpoint)
        .into_iter()
        .filter(|evidence| evidence.status() == CheckEvidenceStatus::Passed)
        .map(|evidence| evidence.check_id().to_owned())
        .collect::<HashSet<_>>();
    required_checks
        .checks()
        .iter()
        .filter(|check| !passed.contains(check.id()))
        .cloned()
        .collect()
}

fn validate_review_round(role: BudgetRole, role_run: u32) -> Result<(), BudgetError> {
    if !(1..=MAX_REVIEW_ROUNDS).contains(&role_run) {
        return Err(BudgetError::InvalidRoleRun { role, role_run });
    }
    Ok(())
}

fn normal_terminal_action(
    role: BudgetRole,
    kind: ControlKind,
) -> Result<RequiredBudgetAction, BudgetError> {
    match (role, kind) {
        (BudgetRole::Planner, ControlKind::SubmitPlan) => Ok(RequiredBudgetAction::PlannerTerminal),
        (BudgetRole::Executor, ControlKind::SubmitExecution) => {
            Ok(RequiredBudgetAction::ExecutorTerminal)
        }
        (BudgetRole::Reviewer, ControlKind::SubmitReview) => {
            Ok(RequiredBudgetAction::ReviewerTerminal)
        }
        (_, observed) => Err(BudgetError::InvalidExploratoryNormalTerminal { role, observed }),
    }
}

fn ensure_role_reservation_fits(
    role: BudgetRole,
    reservation: BudgetReservation,
) -> Result<(), BudgetError> {
    let limits = role.limits();
    if reservation.model_responses > limits.model_responses {
        return Err(BudgetError::RoleLimitExceeded {
            role,
            resource: BudgetResource::ModelResponses,
        });
    }
    if reservation.model_visible_calls > limits.model_visible_calls {
        return Err(BudgetError::RoleLimitExceeded {
            role,
            resource: BudgetResource::ModelVisibleCalls,
        });
    }
    if reservation.retained_result_bytes > limits.retained_result_bytes {
        return Err(BudgetError::RoleLimitExceeded {
            role,
            resource: BudgetResource::RetainedResultBytes,
        });
    }
    Ok(())
}

fn ensure_role_reservation_with_usage_fits(
    lease: &RoleBudgetLease,
    reservation: BudgetReservation,
) -> Result<(), BudgetError> {
    let limits = lease.limits();
    ensure_u32_reservation_fits(
        lease.usage.model_responses,
        reservation.model_responses,
        limits.model_responses,
        BudgetResource::ModelResponses,
    )
    .map_err(|error| role_reservation_error(lease.role(), error))?;
    ensure_u32_reservation_fits(
        lease.usage.model_visible_calls,
        reservation.model_visible_calls,
        limits.model_visible_calls,
        BudgetResource::ModelVisibleCalls,
    )
    .map_err(|error| role_reservation_error(lease.role(), error))?;
    ensure_usize_reservation_fits(
        lease.usage.retained_result_bytes,
        reservation.retained_result_bytes,
        limits.retained_result_bytes,
        BudgetResource::RetainedResultBytes,
    )
    .map_err(|error| role_reservation_error(lease.role(), error))
}

fn role_reservation_error(role: BudgetRole, error: BudgetError) -> BudgetError {
    match error {
        BudgetError::TaskLimitExceeded { resource } => {
            BudgetError::RoleLimitExceeded { role, resource }
        }
        other => other,
    }
}

#[derive(Debug, Clone, Copy)]
enum ChargeMode {
    Required,
    Exploratory,
}

#[allow(clippy::too_many_arguments)]
fn preview_u32_charge(
    task_current: u32,
    role_current: u32,
    amount: u32,
    task_limit: u32,
    role_limit: u32,
    pending_task_reservation: u32,
    role_reservation: u32,
    mode: ChargeMode,
    role: BudgetRole,
    resource: BudgetResource,
) -> Result<(u32, u32, u32), BudgetError> {
    let next_task = task_current
        .checked_add(amount)
        .ok_or(BudgetError::ArithmeticOverflow { resource })?;
    let next_role = role_current
        .checked_add(amount)
        .ok_or(BudgetError::ArithmeticOverflow { resource })?;
    let next_role_reservation = match mode {
        ChargeMode::Required => role_reservation
            .checked_sub(amount)
            .ok_or(BudgetError::RequiredReservationExceeded { resource })?,
        ChargeMode::Exploratory => role_reservation,
    };
    let protected_task = pending_task_reservation
        .checked_add(next_role_reservation)
        .ok_or(BudgetError::ArithmeticOverflow { resource })?;
    if next_task > task_limit {
        return Err(BudgetError::TaskLimitExceeded { resource });
    }
    if task_limit - next_task < protected_task {
        return Err(BudgetError::ReservationWouldBeConsumed { resource });
    }
    if next_role > role_limit {
        return Err(BudgetError::RoleLimitExceeded { role, resource });
    }
    if role_limit - next_role < next_role_reservation {
        return Err(BudgetError::ReservationWouldBeConsumed { resource });
    }
    Ok((next_task, next_role, next_role_reservation))
}

#[allow(clippy::too_many_arguments)]
fn preview_usize_charge_from_sums(
    next_task: usize,
    next_role: usize,
    amount: usize,
    task_limit: usize,
    role_limit: usize,
    pending_task_reservation: usize,
    role_reservation: usize,
    mode: ChargeMode,
    role: BudgetRole,
    resource: BudgetResource,
) -> Result<(usize, usize, usize), BudgetError> {
    let next_role_reservation = match mode {
        ChargeMode::Required => role_reservation
            .checked_sub(amount)
            .ok_or(BudgetError::RequiredReservationExceeded { resource })?,
        ChargeMode::Exploratory => role_reservation,
    };
    let protected_task = pending_task_reservation
        .checked_add(next_role_reservation)
        .ok_or(BudgetError::ArithmeticOverflow { resource })?;
    if next_task > task_limit {
        return Err(BudgetError::TaskLimitExceeded { resource });
    }
    if task_limit - next_task < protected_task {
        return Err(BudgetError::ReservationWouldBeConsumed { resource });
    }
    if next_role > role_limit {
        return Err(BudgetError::RoleLimitExceeded { role, resource });
    }
    if role_limit - next_role < next_role_reservation {
        return Err(BudgetError::ReservationWouldBeConsumed { resource });
    }
    Ok((next_task, next_role, next_role_reservation))
}

fn ensure_u32_reservation_fits(
    current: u32,
    reservation: u32,
    limit: u32,
    resource: BudgetResource,
) -> Result<(), BudgetError> {
    let required = current
        .checked_add(reservation)
        .ok_or(BudgetError::ArithmeticOverflow { resource })?;
    if required > limit {
        return Err(BudgetError::TaskLimitExceeded { resource });
    }
    Ok(())
}

fn ensure_usize_reservation_fits(
    current: usize,
    reservation: usize,
    limit: usize,
    resource: BudgetResource,
) -> Result<(), BudgetError> {
    let required = current
        .checked_add(reservation)
        .ok_or(BudgetError::ArithmeticOverflow { resource })?;
    if required > limit {
        return Err(BudgetError::TaskLimitExceeded { resource });
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetResource {
    ModelResponses,
    ModelVisibleCalls,
    ProviderBytes,
    RetainedResultBytes,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BudgetError {
    #[error("budget arithmetic overflow for {resource:?}")]
    ArithmeticOverflow { resource: BudgetResource },
    #[error("task budget exhausted for {resource:?}")]
    TaskLimitExceeded { resource: BudgetResource },
    #[error("{role:?} role budget exhausted for {resource:?}")]
    RoleLimitExceeded {
        role: BudgetRole,
        resource: BudgetResource,
    },
    #[error("an exploratory charge would consume the required {resource:?} reservation")]
    ReservationWouldBeConsumed { resource: BudgetResource },
    #[error("the required {resource:?} charge exceeds its reservation")]
    RequiredReservationExceeded { resource: BudgetResource },
    #[error("encoded provider request exceeds the 1 MiB boundary: {observed} bytes")]
    ProviderRequestTooLarge { observed: usize },
    #[error("encoded provider response exceeds the 1 MiB boundary: {observed} bytes")]
    ProviderResponseTooLarge { observed: usize },
    #[error("a non-zero provider response byte reservation is required")]
    ProviderResponseReservationRequired,
    #[error("another provider response is still pending")]
    ProviderResponsePending,
    #[error("a charged provider response receipt is still open")]
    ProviderResponseReceiptOpen,
    #[error("no provider response reservation is active")]
    ProviderResponseNotReserved,
    #[error("provider request permit does not match the pending request")]
    ProviderRequestPermitMismatch,
    #[error("provider response charge class does not match its request permit")]
    ProviderResponseClassMismatch,
    #[error("provider response receipt does not match the active response")]
    ProviderResponseReceiptMismatch,
    #[error("provider response violated a preflight byte boundary")]
    ProviderResponseLimitViolation,
    #[error(
        "encoded provider response exceeds its reservation: reserved {reserved}, observed {observed} bytes"
    )]
    ProviderResponseExceedsReservation { reserved: usize, observed: usize },
    #[error("an invalid provider response already produced side effects")]
    InvalidResponseHasSideEffects,
    #[error("an early non-success terminal response already produced another side effect")]
    EarlyTerminalHasSideEffects,
    #[error("an exploratory normal terminal response already produced another side effect")]
    NormalTerminalHasSideEffects,
    #[error("the exploratory runtime batch is empty, malformed, or contains a control action")]
    InvalidExploratoryRuntimeBatch,
    #[error("the exploratory non-terminal control permit is invalid")]
    InvalidExploratoryControl,
    #[error("the exploratory runtime batch permit does not match the active ledger exchange")]
    ExploratoryBatchPermitMismatch,
    #[error("another exploratory runtime batch result reservation is still pending")]
    ExploratoryBatchAlreadyPending,
    #[error("{observed:?} is not the normal terminal control for {role:?}")]
    InvalidExploratoryNormalTerminal {
        role: BudgetRole,
        observed: ControlKind,
    },
    #[error("the exploratory normal-terminal token belongs to another ledger exchange")]
    ExploratoryNormalTerminalIdentityMismatch,
    #[error("the exploratory normal terminal does not match the sole remaining role action")]
    ExploratoryNormalTerminalNotReady,
    #[error("only Reviewer may produce the early changes-requested terminal transition")]
    ReviewerChangesRequestedByNonReviewer,
    #[error("only an active Executor lease can refresh required checks")]
    ExecutorRefreshForWrongRole,
    #[error("the Executor workspace-change reservation preflight is invalid")]
    InvalidExecutorWorkspaceProtection,
    #[error("tool result exceeds its typed {limit}-byte boundary: {observed} bytes")]
    ToolResultKindLimitExceeded { limit: usize, observed: usize },
    #[error("retained result token {result_id:?} belongs to another role run")]
    RetainedResultOwnerConflict { result_id: RetainedResultId },
    #[error("retained result token {result_id:?} was replayed with different wrapper bytes")]
    RetainedResultContentConflict { result_id: RetainedResultId },
    #[error("retained result token {result_id:?} is not pending")]
    RetainedResultPermitNotPending { result_id: RetainedResultId },
    #[error("tool-result permit does not belong to the active role")]
    ToolResultPermitMismatch,
    #[error("a model-visible tool result has not been retained")]
    ToolResultPending,
    #[error("the role has not completed a typed terminal action")]
    RoleNotTerminated,
    #[error("the role budget is sealed after {termination:?}")]
    RoleAlreadyTerminated { termination: RoleBudgetTermination },
    #[error("another role is active: {role_run:?}")]
    RoleAlreadyActive { role_run: RoleRun },
    #[error("there is no active role")]
    NoActiveRole,
    #[error("role lease mismatch: active {active_role:?}, observed {observed_role:?}")]
    LeaseMismatch {
        active_role: RoleRun,
        observed_role: RoleRun,
    },
    #[error("{role:?} role run {role_run} is outside the supported range")]
    InvalidRoleRun { role: BudgetRole, role_run: u32 },
    #[error("{role:?} role run {role_run} has already started")]
    RoleRunAlreadyStarted { role: BudgetRole, role_run: u32 },
    #[error("reviewer reservation for round {review_round} is already pending")]
    ReviewerReservationAlreadyPending { review_round: u32 },
    #[error("reviewer reservation for round {review_round} is missing")]
    ReviewerReservationMissing { review_round: u32 },
    #[error("reviewer reservation belongs to round {expected}, not requested round {observed}")]
    ReviewerReservationRoundMismatch { expected: u32, observed: u32 },
    #[error("a required action is already in progress")]
    RequiredActionInProgress,
    #[error("required action remains pending: {action:?}")]
    RequiredActionPending { action: RequiredBudgetAction },
    #[error("there is no required action pending")]
    NoRequiredActionPending,
    #[error("required action permit does not match the active role action")]
    RequiredPermitMismatch,
    #[error("required actions were completed out of order")]
    RequiredActionOrderMismatch,
    #[error("required provider response was already charged")]
    RequiredResponseAlreadyCharged,
    #[error("required provider response has not been charged")]
    RequiredResponseNotCharged,
    #[error("required model-visible call was already charged")]
    RequiredCallAlreadyCharged,
    #[error("required model-visible call has not been charged")]
    RequiredCallNotCharged,
    #[error("this required action does not produce a retained result")]
    RequiredActionHasNoResult,
    #[error("required retained result has not been charged")]
    RequiredResultNotCharged,
    #[error("required Reviewer manifest has not been bound to its runtime observation")]
    ReviewerManifestNotBound,
    #[error("required action is not the Reviewer manifest")]
    NotReviewerManifestAction,
    #[error("Reviewer manifest was already bound")]
    ReviewerManifestAlreadyBound,
    #[error("Reviewer manifest budget receipt does not match the active manifest action")]
    ReviewerManifestReceiptMismatch,
    #[error("Reviewer manifest exceeds 8 chunks: {observed}")]
    TooManyReviewDiffChunks { observed: u8 },
    #[error("a reservation invariant was violated")]
    ReservationInvariantBroken,
    #[error("role lease identifier overflow")]
    LeaseIdExhausted,
    #[error("task budget ledger identifier space is exhausted")]
    LedgerIdExhausted,
    #[error("required action identifier overflow")]
    ActionIdExhausted,
    #[error("provider request identifier overflow")]
    ProviderRequestIdExhausted,
    #[error("retained result identifier overflow")]
    RetainedResultIdExhausted,
}

#[cfg(test)]
mod tests {
    use coding_agent_domain::{CheckActor, RequiredCheck};

    use super::*;
    use crate::model::WorkspaceFingerprint;
    use crate::role::ActionRequest;

    fn passed_quality() -> (RequiredCheckLedger, WorkspaceCheckpoint) {
        let check =
            RequiredCheck::try_cargo_test("required", Some("package".to_owned()), None).unwrap();
        let mut checks = RequiredCheckLedger::try_new(vec![check.clone()]).unwrap();
        let mut checkpoint = WorkspaceCheckpoint::new(WorkspaceFingerprint::from_bytes([0x42; 32]));
        checks.queue_check(&mut checkpoint, check.id()).unwrap();
        let token = checks
            .mark_check_running(&checkpoint, check.id(), CheckActor::Executor, 1)
            .unwrap();
        checks
            .finish_check(
                &mut checkpoint,
                token,
                CheckEvidenceStatus::Passed,
                1,
                "passed",
                false,
            )
            .unwrap();
        (checks, checkpoint)
    }

    fn runtime_action() -> ToolRequest {
        ToolRequest::ReadFile {
            path: "src/lib.rs".to_owned(),
            start_line: 1,
            end_line: 1,
        }
    }

    fn open_exploratory_response(
        ledger: &mut TaskBudgetLedger,
        lease: &mut RoleBudgetLease,
    ) -> ProviderResponseReceipt {
        let request = ledger
            .begin_exploratory_provider_request(lease, b"q", 1)
            .unwrap();
        ledger
            .record_exploratory_provider_response(lease, request, b"r")
            .unwrap()
    }

    fn charge_exploratory_responses(
        ledger: &mut TaskBudgetLedger,
        lease: &mut RoleBudgetLease,
        count: u32,
    ) {
        for _ in 0..count {
            let receipt = open_exploratory_response(ledger, lease);
            ledger.finish_provider_response(lease, &receipt).unwrap();
        }
    }

    fn charge_runtime_calls(
        ledger: &mut TaskBudgetLedger,
        lease: &mut RoleBudgetLease,
        count: u32,
        wrapper_bytes: &[u8],
    ) {
        let mut receipt = open_exploratory_response(ledger, lease);
        for _ in 0..count {
            let result = ledger
                .charge_exploratory_tool_call(lease, &mut receipt, runtime_action())
                .unwrap();
            ledger
                .retain_exploratory_wrapper_bytes(lease, &result, wrapper_bytes)
                .unwrap();
        }
        ledger.finish_provider_response(lease, &receipt).unwrap();
    }

    fn charge_runtime_result(
        ledger: &mut TaskBudgetLedger,
        lease: &mut RoleBudgetLease,
        wrapper_bytes: &[u8],
    ) {
        charge_runtime_calls(ledger, lease, 1, wrapper_bytes);
    }

    fn complete_next_required(
        ledger: &mut TaskBudgetLedger,
        lease: &mut RoleBudgetLease,
        wrapper_bytes: Option<&[u8]>,
    ) {
        let mut permit = ledger.begin_required_action(lease).unwrap();
        let request = ledger
            .begin_required_provider_request(lease, &permit, b"q", 1)
            .unwrap();
        let mut receipt = ledger
            .record_required_provider_response(lease, &mut permit, request, b"r")
            .unwrap();
        ledger
            .charge_required_call(lease, &mut permit, &mut receipt)
            .unwrap();
        if let Some(wrapper_bytes) = wrapper_bytes {
            ledger
                .retain_required_wrapper_bytes(lease, &mut permit, wrapper_bytes)
                .unwrap();
        }
        ledger.finish_provider_response(lease, &receipt).unwrap();
        ledger.complete_required_action(lease, permit).unwrap();
    }

    fn complete_planner(ledger: &mut TaskBudgetLedger, lease: &mut RoleBudgetLease) {
        complete_next_required(ledger, lease, None);
    }

    fn complete_executor(ledger: &mut TaskBudgetLedger, lease: &mut RoleBudgetLease) {
        complete_next_required(ledger, lease, None);
    }

    fn complete_reviewer_manifest(
        ledger: &mut TaskBudgetLedger,
        lease: &mut RoleBudgetLease,
        chunk_count: u8,
        manifest_wrapper: &[u8],
    ) {
        let mut permit = ledger.begin_required_action(lease).unwrap();
        assert_eq!(permit.action(), &RequiredBudgetAction::ReviewerManifest);
        let request = ledger
            .begin_required_provider_request(lease, &permit, b"q", 1)
            .unwrap();
        let mut receipt = ledger
            .record_required_provider_response(lease, &mut permit, request, b"r")
            .unwrap();
        ledger
            .charge_required_call(lease, &mut permit, &mut receipt)
            .unwrap();
        ledger
            .retain_required_wrapper_bytes(lease, &mut permit, manifest_wrapper)
            .unwrap();
        let manifest = ledger
            .observe_typed_reviewer_manifest(lease, &permit, chunk_count)
            .unwrap();
        ledger
            .bind_reviewer_manifest(lease, &mut permit, manifest)
            .unwrap();
        ledger.finish_provider_response(lease, &receipt).unwrap();
        ledger.complete_required_action(lease, permit).unwrap();
    }

    fn complete_reviewer(
        ledger: &mut TaskBudgetLedger,
        lease: &mut RoleBudgetLease,
        chunk_count: u8,
        manifest_wrapper: &[u8],
        batch_wrapper: &[u8],
        exploratory_responses: u32,
        exploratory_calls: u32,
    ) {
        complete_reviewer_manifest(ledger, lease, chunk_count, manifest_wrapper);

        charge_exploratory_responses(ledger, lease, exploratory_responses);
        if exploratory_calls > 0 {
            charge_runtime_calls(ledger, lease, exploratory_calls, b"x");
        }

        while let Some(action) = lease.next_required_action() {
            let wrapper = match action {
                RequiredBudgetAction::ReviewerChunkBatch { .. } => Some(batch_wrapper),
                RequiredBudgetAction::ReviewerTerminal => None,
                other => panic!("unexpected Reviewer action: {other:?}"),
            };
            complete_next_required(ledger, lease, wrapper);
        }
    }

    fn fake_required_permit(lease: &RoleBudgetLease) -> RequiredActionPermit {
        RequiredActionPermit {
            ledger_id: u64::MAX,
            lease_id: lease.lease_id,
            role_run: lease.role_run,
            action_id: u64::MAX,
            action: RequiredBudgetAction::ReviewerManifest,
            response_charged: true,
            call_charged: true,
            result_id: Some(RetainedResultId(u64::MAX)),
            result_bytes: Some(1),
            manifest_bound: false,
        }
    }

    fn fake_provider_request(lease: &RoleBudgetLease, class: ChargeClass) -> ProviderRequestPermit {
        ProviderRequestPermit {
            ledger_id: u64::MAX,
            request_id: u64::MAX,
            lease_id: lease.lease_id,
            role_run: lease.role_run,
            class,
        }
    }

    fn assert_normal_sealed<T>(result: Result<T, BudgetError>) {
        assert!(matches!(
            result,
            Err(BudgetError::RoleAlreadyTerminated {
                termination: RoleBudgetTermination::Normal
            })
        ));
    }

    fn decoded_normal_control(role: BudgetRole) -> ControlRequest {
        let (name, arguments) = match role {
            BudgetRole::Planner => (
                "submit_plan",
                r#"{"summary":"plan","steps":[{"title":"step","description":"","acceptance_criteria":["done"]}],"initial_required_checks":[{"kind":"cargo_test","package":null,"integration_test":null}]}"#,
            ),
            BudgetRole::Executor => ("submit_execution", r#"{"summary":"ready"}"#),
            BudgetRole::Reviewer => (
                "submit_review",
                r#"{"verdict":"approved","summary":"ready","findings":[],"add_required_checks":[]}"#,
            ),
        };
        match ActionRequest::decode(role, name, arguments).unwrap() {
            ActionRequest::Control(control) => control,
            other => panic!("expected terminal control, observed {other:?}"),
        }
    }

    fn exploratory_normal_terminal(
        ledger: &TaskBudgetLedger,
        lease: &RoleBudgetLease,
        receipt: &ProviderResponseReceipt,
    ) -> ExploratoryNormalTerminal {
        ledger
            .mint_exploratory_normal_terminal(lease, receipt, decoded_normal_control(lease.role()))
            .unwrap()
    }

    fn ledger_with_pending_reviewer() -> TaskBudgetLedger {
        let (checks, checkpoint) = passed_quality();
        let mut ledger = TaskBudgetLedger::new();
        let mut executor = ledger.start_executor(1, &checks, &checkpoint).unwrap();
        let mut receipt = open_exploratory_response(&mut ledger, &mut executor);
        let terminal = exploratory_normal_terminal(&ledger, &executor, &receipt);
        ledger
            .complete_exploratory_normal_terminal(&mut executor, &mut receipt, terminal)
            .unwrap();
        ledger.finish_role(executor).unwrap();
        ledger
    }

    #[test]
    fn abandon_reviewer_reservation_rejects_missing_reservation() {
        let mut ledger = TaskBudgetLedger::new();

        assert_eq!(
            ledger.abandon_pending_reviewer_reservation(1),
            Err(BudgetError::ReviewerReservationMissing { review_round: 1 })
        );
    }

    #[test]
    fn abandon_reviewer_reservation_rejects_round_mismatch_without_mutation() {
        let mut ledger = ledger_with_pending_reviewer();
        let pending = ledger.pending_reviewer_reservation();

        assert_eq!(
            ledger.abandon_pending_reviewer_reservation(2),
            Err(BudgetError::ReviewerReservationRoundMismatch {
                expected: 1,
                observed: 2,
            })
        );
        assert_eq!(ledger.pending_reviewer_reservation(), pending);
    }

    #[test]
    fn abandon_reviewer_reservation_rejects_an_active_role_without_mutation() {
        let (checks, checkpoint) = passed_quality();
        let mut ledger = TaskBudgetLedger::new();
        let executor = ledger.start_executor(1, &checks, &checkpoint).unwrap();
        let pending = ledger.pending_reviewer_reservation();

        assert!(matches!(
            ledger.abandon_pending_reviewer_reservation(1),
            Err(BudgetError::RoleAlreadyActive { role_run })
                if role_run.role() == BudgetRole::Executor && role_run.role_run() == 1
        ));
        assert_eq!(ledger.pending_reviewer_reservation(), pending);

        ledger.abort_role_on_failure(executor).unwrap();
    }

    #[test]
    fn abandon_reviewer_reservation_releases_the_exact_idle_round() {
        let mut ledger = ledger_with_pending_reviewer();

        ledger.abandon_pending_reviewer_reservation(1).unwrap();

        assert_eq!(ledger.pending_reviewer_reservation(), None);
        assert!(matches!(
            ledger.start_reviewer(1),
            Err(BudgetError::ReviewerReservationMissing { review_round: 1 })
        ));
    }

    #[test]
    fn finishing_a_blocked_executor_releases_its_reviewer_reservation() {
        let (checks, checkpoint) = passed_quality();
        let mut ledger = TaskBudgetLedger::new();
        let mut executor = ledger.start_executor(1, &checks, &checkpoint).unwrap();
        let mut receipt = open_exploratory_response(&mut ledger, &mut executor);

        ledger
            .complete_exploratory_early_terminal(
                &mut executor,
                &mut receipt,
                EarlyRoleBudgetTermination::ReportBlocked,
            )
            .unwrap();
        let finished = ledger.finish_role(executor).unwrap();

        assert_eq!(finished.termination(), RoleBudgetTermination::ReportBlocked);
        assert_eq!(ledger.pending_reviewer_reservation(), None);
    }

    #[test]
    fn auto_normal_terminal_uses_the_current_response_and_seals_all_three_roles() {
        let mut planner_ledger = TaskBudgetLedger::new();
        let mut planner = planner_ledger.start_planner().unwrap();
        let mut planner_receipt = open_exploratory_response(&mut planner_ledger, &mut planner);
        let responses_before = planner_ledger.usage.model_responses();
        let provider_before = planner_ledger.usage.provider_bytes();
        let duplicate = exploratory_normal_terminal(&planner_ledger, &planner, &planner_receipt);
        let terminal = exploratory_normal_terminal(&planner_ledger, &planner, &planner_receipt);
        let control = planner_ledger
            .complete_exploratory_normal_terminal(&mut planner, &mut planner_receipt, terminal)
            .unwrap();
        assert_eq!(control.kind(), ControlKind::SubmitPlan);
        assert_eq!(
            planner_ledger.usage.model_responses(),
            responses_before,
            "the current Auto response must be reused"
        );
        assert_eq!(planner_ledger.usage.provider_bytes(), provider_before);
        assert_eq!(planner_ledger.usage.model_visible_calls(), 1);
        assert_eq!(planner.required_reservation, BudgetReservation::default());
        assert_eq!(planner.termination, Some(RoleBudgetTermination::Normal));
        assert_normal_sealed(planner_ledger.complete_exploratory_normal_terminal(
            &mut planner,
            &mut planner_receipt,
            duplicate,
        ));
        assert_eq!(planner_ledger.usage.model_visible_calls(), 1);
        assert_eq!(planner.required_reservation, BudgetReservation::default());
        assert_eq!(planner.termination, Some(RoleBudgetTermination::Normal));
        let planner_finished = planner_ledger.finish_role(planner).unwrap();
        assert_eq!(
            planner_finished.termination(),
            RoleBudgetTermination::Normal
        );

        let (checks, checkpoint) = passed_quality();
        let mut ledger = TaskBudgetLedger::new();
        let mut executor = ledger.start_executor(1, &checks, &checkpoint).unwrap();
        let pending_reviewer = ledger.pending_reviewer;
        let mut executor_receipt = open_exploratory_response(&mut ledger, &mut executor);
        let executor_responses = ledger.usage.model_responses();
        let terminal = exploratory_normal_terminal(&ledger, &executor, &executor_receipt);
        let control = ledger
            .complete_exploratory_normal_terminal(&mut executor, &mut executor_receipt, terminal)
            .unwrap();
        assert_eq!(control.kind(), ControlKind::SubmitExecution);
        assert_eq!(ledger.usage.model_responses(), executor_responses);
        assert_eq!(executor.usage.model_responses(), 1);
        assert_eq!(executor.usage.model_visible_calls(), 1);
        assert_eq!(executor.required_reservation, BudgetReservation::default());
        assert_eq!(
            ledger.pending_reviewer, pending_reviewer,
            "Executor success must preserve the following Reviewer reservation"
        );
        ledger.finish_role(executor).unwrap();

        let mut reviewer = ledger.start_reviewer(1).unwrap();
        complete_reviewer_manifest(&mut ledger, &mut reviewer, 0, b"manifest");
        assert_eq!(
            reviewer.next_required_action(),
            Some(&RequiredBudgetAction::ReviewerTerminal)
        );
        let mut reviewer_receipt = open_exploratory_response(&mut ledger, &mut reviewer);
        let task_responses = ledger.usage.model_responses();
        let reviewer_responses = reviewer.usage.model_responses();
        let terminal = exploratory_normal_terminal(&ledger, &reviewer, &reviewer_receipt);
        let control = ledger
            .complete_exploratory_normal_terminal(&mut reviewer, &mut reviewer_receipt, terminal)
            .unwrap();
        assert_eq!(control.kind(), ControlKind::SubmitReview);
        assert_eq!(ledger.usage.model_responses(), task_responses);
        assert_eq!(reviewer.usage.model_responses(), reviewer_responses);
        assert_eq!(reviewer.usage.model_visible_calls(), 2);
        assert_eq!(reviewer.required_reservation, BudgetReservation::default());
        assert_eq!(reviewer.termination, Some(RoleBudgetTermination::Normal));
        let reviewer_finished = ledger.finish_role(reviewer).unwrap();
        assert_eq!(
            reviewer_finished.termination(),
            RoleBudgetTermination::Normal
        );
    }

    #[test]
    fn auto_normal_terminal_rejects_checks_manifest_chunks_wrong_role_and_violations_atomically() {
        let check =
            RequiredCheck::try_cargo_test("required", Some("package".to_owned()), None).unwrap();
        let checks = RequiredCheckLedger::try_new(vec![check]).unwrap();
        let checkpoint = WorkspaceCheckpoint::new(WorkspaceFingerprint::from_bytes([0x24; 32]));
        let mut check_ledger = TaskBudgetLedger::new();
        let mut executor = check_ledger
            .start_executor(1, &checks, &checkpoint)
            .unwrap();
        let reservation_before = executor.required_reservation;
        let receipt = open_exploratory_response(&mut check_ledger, &mut executor);
        assert!(matches!(
            check_ledger.mint_exploratory_normal_terminal(
                &executor,
                &receipt,
                decoded_normal_control(BudgetRole::Executor)
            ),
            Err(BudgetError::ExploratoryNormalTerminalNotReady)
        ));
        assert_eq!(check_ledger.usage.model_visible_calls(), 0);
        assert_eq!(executor.required_reservation, reservation_before);
        assert!(matches!(
            executor.next_required_action(),
            Some(RequiredBudgetAction::ExecutorCheck { .. })
        ));
        check_ledger
            .finish_provider_response(&executor, &receipt)
            .unwrap();

        let (checks, checkpoint) = passed_quality();
        let mut review_ledger = TaskBudgetLedger::new();
        let mut executor = review_ledger
            .start_executor(1, &checks, &checkpoint)
            .unwrap();
        complete_executor(&mut review_ledger, &mut executor);
        review_ledger.finish_role(executor).unwrap();
        let mut reviewer = review_ledger.start_reviewer(1).unwrap();
        let manifest_reservation = reviewer.required_reservation;
        let manifest_receipt = open_exploratory_response(&mut review_ledger, &mut reviewer);
        assert!(matches!(
            review_ledger.mint_exploratory_normal_terminal(
                &reviewer,
                &manifest_receipt,
                decoded_normal_control(BudgetRole::Reviewer)
            ),
            Err(BudgetError::ExploratoryNormalTerminalNotReady)
        ));
        assert_eq!(reviewer.required_reservation, manifest_reservation);
        assert_eq!(review_ledger.usage.model_visible_calls(), 1);
        review_ledger
            .finish_provider_response(&reviewer, &manifest_receipt)
            .unwrap();

        complete_reviewer_manifest(&mut review_ledger, &mut reviewer, 1, b"manifest");
        let chunk_reservation = reviewer.required_reservation;
        let chunk_receipt = open_exploratory_response(&mut review_ledger, &mut reviewer);
        assert!(matches!(
            review_ledger.mint_exploratory_normal_terminal(
                &reviewer,
                &chunk_receipt,
                decoded_normal_control(BudgetRole::Reviewer)
            ),
            Err(BudgetError::ExploratoryNormalTerminalNotReady)
        ));
        assert!(matches!(
            reviewer.next_required_action(),
            Some(RequiredBudgetAction::ReviewerChunkBatch { .. })
        ));
        assert_eq!(reviewer.required_reservation, chunk_reservation);
        assert_eq!(review_ledger.usage.model_visible_calls(), 2);
        review_ledger
            .finish_provider_response(&reviewer, &chunk_receipt)
            .unwrap();

        let mut wrong_role_ledger = TaskBudgetLedger::new();
        let mut planner = wrong_role_ledger.start_planner().unwrap();
        let planner_reservation = planner.required_reservation;
        let wrong_role_receipt = open_exploratory_response(&mut wrong_role_ledger, &mut planner);
        assert!(matches!(
            wrong_role_ledger.mint_exploratory_normal_terminal(
                &planner,
                &wrong_role_receipt,
                decoded_normal_control(BudgetRole::Executor)
            ),
            Err(BudgetError::InvalidExploratoryNormalTerminal {
                role: BudgetRole::Planner,
                observed: ControlKind::SubmitExecution
            })
        ));
        assert_eq!(wrong_role_ledger.usage.model_visible_calls(), 0);
        assert_eq!(planner.required_reservation, planner_reservation);
        wrong_role_ledger
            .finish_provider_response(&planner, &wrong_role_receipt)
            .unwrap();

        let mut invalid_ledger = TaskBudgetLedger::new();
        let mut planner = invalid_ledger.start_planner().unwrap();
        let reservation_before = planner.required_reservation;
        let request = invalid_ledger
            .begin_exploratory_provider_request(&planner, b"q", 1)
            .unwrap();
        let mut invalid_receipt = invalid_ledger
            .record_exploratory_provider_response(&mut planner, request, b"xx")
            .unwrap();
        assert!(matches!(
            invalid_ledger.mint_exploratory_normal_terminal(
                &planner,
                &invalid_receipt,
                decoded_normal_control(BudgetRole::Planner)
            ),
            Err(BudgetError::ProviderResponseLimitViolation)
        ));
        let forged_terminal = ExploratoryNormalTerminal {
            ledger_id: invalid_ledger.ensure_ledger_identity().unwrap(),
            lease_id: planner.lease_id,
            role_run: planner.role_run,
            request_id: invalid_receipt.request_id,
            required_action: RequiredBudgetAction::PlannerTerminal,
            control: decoded_normal_control(BudgetRole::Planner),
        };
        assert!(matches!(
            invalid_ledger.complete_exploratory_normal_terminal(
                &mut planner,
                &mut invalid_receipt,
                forged_terminal
            ),
            Err(BudgetError::ProviderResponseLimitViolation)
        ));
        assert_eq!(invalid_ledger.usage.model_visible_calls(), 0);
        assert_eq!(planner.required_reservation, reservation_before);
        assert_eq!(
            planner.next_required_action(),
            Some(&RequiredBudgetAction::PlannerTerminal)
        );
        invalid_ledger
            .discard_invalid_provider_response(&planner, &invalid_receipt)
            .unwrap();
    }

    #[test]
    fn normal_terminal_token_is_bound_to_ledger_role_run_and_exact_receipt() {
        let mut first_ledger = TaskBudgetLedger::new();
        let mut first_planner = first_ledger.start_planner().unwrap();
        let first_receipt = open_exploratory_response(&mut first_ledger, &mut first_planner);
        let cross_ledger_token =
            exploratory_normal_terminal(&first_ledger, &first_planner, &first_receipt);

        let mut second_ledger = TaskBudgetLedger::new();
        let mut second_planner = second_ledger.start_planner().unwrap();
        let mut second_receipt = open_exploratory_response(&mut second_ledger, &mut second_planner);
        assert_ne!(first_ledger.ledger_id, second_ledger.ledger_id);
        assert_eq!(first_planner.lease_id, second_planner.lease_id);
        assert_eq!(first_receipt.request_id, second_receipt.request_id);
        let second_reservation = second_planner.required_reservation;
        assert!(matches!(
            second_ledger.complete_exploratory_normal_terminal(
                &mut second_planner,
                &mut second_receipt,
                cross_ledger_token
            ),
            Err(BudgetError::ExploratoryNormalTerminalIdentityMismatch)
        ));
        assert_eq!(second_ledger.usage.model_visible_calls(), 0);
        assert_eq!(second_planner.required_reservation, second_reservation);
        assert_eq!(second_planner.termination, None);
        let valid_second =
            exploratory_normal_terminal(&second_ledger, &second_planner, &second_receipt);
        second_ledger
            .complete_exploratory_normal_terminal(
                &mut second_planner,
                &mut second_receipt,
                valid_second,
            )
            .unwrap();
        assert_eq!(second_ledger.usage.model_visible_calls(), 1);
        first_ledger
            .finish_provider_response(&first_planner, &first_receipt)
            .unwrap();

        let mut receipt_ledger = TaskBudgetLedger::new();
        let mut planner = receipt_ledger.start_planner().unwrap();
        let first_receipt = open_exploratory_response(&mut receipt_ledger, &mut planner);
        let old_receipt_token =
            exploratory_normal_terminal(&receipt_ledger, &planner, &first_receipt);
        receipt_ledger
            .finish_provider_response(&planner, &first_receipt)
            .unwrap();
        let mut current_receipt = open_exploratory_response(&mut receipt_ledger, &mut planner);
        let reservation_before = planner.required_reservation;
        assert!(matches!(
            receipt_ledger.complete_exploratory_normal_terminal(
                &mut planner,
                &mut current_receipt,
                old_receipt_token
            ),
            Err(BudgetError::ExploratoryNormalTerminalIdentityMismatch)
        ));
        assert_eq!(receipt_ledger.usage.model_visible_calls(), 0);
        assert_eq!(planner.required_reservation, reservation_before);
        let current_token =
            exploratory_normal_terminal(&receipt_ledger, &planner, &current_receipt);
        receipt_ledger
            .complete_exploratory_normal_terminal(&mut planner, &mut current_receipt, current_token)
            .unwrap();

        let (checks, checkpoint) = passed_quality();
        let mut role_ledger = TaskBudgetLedger::new();
        let mut planner = role_ledger.start_planner().unwrap();
        let planner_receipt = open_exploratory_response(&mut role_ledger, &mut planner);
        let old_role_token = exploratory_normal_terminal(&role_ledger, &planner, &planner_receipt);
        role_ledger
            .finish_provider_response(&planner, &planner_receipt)
            .unwrap();
        complete_planner(&mut role_ledger, &mut planner);
        role_ledger.finish_role(planner).unwrap();

        let mut executor = role_ledger.start_executor(1, &checks, &checkpoint).unwrap();
        let pending_reviewer = role_ledger.pending_reviewer;
        let mut executor_receipt = open_exploratory_response(&mut role_ledger, &mut executor);
        let calls_before = role_ledger.usage.model_visible_calls();
        let reservation_before = executor.required_reservation;
        assert!(matches!(
            role_ledger.complete_exploratory_normal_terminal(
                &mut executor,
                &mut executor_receipt,
                old_role_token
            ),
            Err(BudgetError::ExploratoryNormalTerminalIdentityMismatch)
        ));
        assert_eq!(role_ledger.usage.model_visible_calls(), calls_before);
        assert_eq!(executor.required_reservation, reservation_before);
        assert_eq!(role_ledger.pending_reviewer, pending_reviewer);
        let stale_executor_round_token =
            exploratory_normal_terminal(&role_ledger, &executor, &executor_receipt);
        let executor_token =
            exploratory_normal_terminal(&role_ledger, &executor, &executor_receipt);
        role_ledger
            .complete_exploratory_normal_terminal(
                &mut executor,
                &mut executor_receipt,
                executor_token,
            )
            .unwrap();
        role_ledger.finish_role(executor).unwrap();

        let mut reviewer = role_ledger.start_reviewer(1).unwrap();
        let mut reviewer_receipt = open_exploratory_response(&mut role_ledger, &mut reviewer);
        role_ledger
            .complete_exploratory_early_terminal(
                &mut reviewer,
                &mut reviewer_receipt,
                EarlyRoleBudgetTermination::ReviewerChangesRequested,
            )
            .unwrap();
        role_ledger.finish_role(reviewer).unwrap();

        let mut executor = role_ledger.start_executor(2, &checks, &checkpoint).unwrap();
        let mut executor_receipt = open_exploratory_response(&mut role_ledger, &mut executor);
        let calls_before = role_ledger.usage.model_visible_calls();
        let reservation_before = executor.required_reservation;
        let pending_reviewer = role_ledger.pending_reviewer;
        assert!(matches!(
            role_ledger.complete_exploratory_normal_terminal(
                &mut executor,
                &mut executor_receipt,
                stale_executor_round_token
            ),
            Err(BudgetError::ExploratoryNormalTerminalIdentityMismatch)
        ));
        assert_eq!(role_ledger.usage.model_visible_calls(), calls_before);
        assert_eq!(executor.required_reservation, reservation_before);
        assert_eq!(role_ledger.pending_reviewer, pending_reviewer);
        let current_executor_token =
            exploratory_normal_terminal(&role_ledger, &executor, &executor_receipt);
        role_ledger
            .complete_exploratory_normal_terminal(
                &mut executor,
                &mut executor_receipt,
                current_executor_token,
            )
            .unwrap();
    }

    #[test]
    fn ledger_identity_allocation_never_wraps_or_enables_an_unidentified_ledger() {
        let counter = AtomicU64::new(u64::MAX - 1);
        assert_eq!(
            allocate_task_budget_ledger_id(&counter).unwrap(),
            u64::MAX - 1
        );
        assert!(matches!(
            allocate_task_budget_ledger_id(&counter),
            Err(BudgetError::LedgerIdExhausted)
        ));
        assert_eq!(counter.load(Ordering::Relaxed), u64::MAX);

        let mut unidentified = TaskBudgetLedger::with_ledger_id(None);
        assert!(matches!(
            unidentified.start_planner(),
            Err(BudgetError::LedgerIdExhausted)
        ));
    }

    #[test]
    fn owned_invocation_keeps_the_actual_request_and_permit_classification_together() {
        let mut ledger = TaskBudgetLedger::new();
        let mut planner = ledger.start_planner().unwrap();
        let mut receipt = open_exploratory_response(&mut ledger, &mut planner);
        let cargo_invocation = ledger
            .charge_exploratory_tool_call(
                &mut planner,
                &mut receipt,
                ToolRequest::CargoTest {
                    package: Some("package".to_owned()),
                    test: None,
                    timeout_ms: 1,
                },
            )
            .unwrap();
        assert!(matches!(
            cargo_invocation.request(),
            ToolRequest::CargoTest {
                package: Some(package),
                test: None,
                timeout_ms: 1
            } if package == "package"
        ));
        assert_eq!(cargo_invocation.kind(), ToolResultKind::Validation);
        assert!(matches!(
            ledger.retain_exploratory_wrapper_bytes(
                &mut planner,
                &cargo_invocation,
                &vec![0; VALIDATION_RETAINED_RESULT_LIMIT + 1]
            ),
            Err(BudgetError::ToolResultKindLimitExceeded {
                limit: VALIDATION_RETAINED_RESULT_LIMIT,
                ..
            })
        ));
        ledger
            .retain_exploratory_wrapper_bytes(&mut planner, &cargo_invocation, b"validation")
            .unwrap();
        let cargo_result_id = cargo_invocation.result_id();
        let (owned_cargo_request, cargo_result_permit) = cargo_invocation.into_parts();
        assert!(matches!(
            owned_cargo_request,
            ToolRequest::CargoTest {
                package: Some(package),
                test: None,
                timeout_ms: 1
            } if package == "package"
        ));
        assert_eq!(cargo_result_permit.kind, ToolResultKind::Validation);
        assert_eq!(cargo_result_permit.result_id, cargo_result_id);

        let read_invocation = ledger
            .charge_exploratory_tool_call(&mut planner, &mut receipt, runtime_action())
            .unwrap();
        assert!(matches!(
            read_invocation.request(),
            ToolRequest::ReadFile {
                path,
                start_line: 1,
                end_line: 1
            } if path == "src/lib.rs"
        ));
        assert_eq!(read_invocation.kind(), ToolResultKind::Runtime);
        assert_ne!(
            cargo_result_id,
            read_invocation.result_id(),
            "a distinct owned request must mint a distinct result identity"
        );
        ledger
            .retain_exploratory_wrapper_bytes(&mut planner, &read_invocation, b"runtime")
            .unwrap();
        let read_result_id = read_invocation.result_id();
        let (owned_request, result_permit) = read_invocation.into_parts();
        assert!(matches!(
            owned_request,
            ToolRequest::ReadFile {
                path,
                start_line: 1,
                end_line: 1
            } if path == "src/lib.rs"
        ));
        assert_eq!(result_permit.kind, ToolResultKind::Runtime);
        assert_eq!(result_permit.result_id, read_result_id);
    }

    #[test]
    fn reviewer_normal_terminal_seals_every_budget_transition_entry() {
        let (checks, checkpoint) = passed_quality();
        let mut ledger = TaskBudgetLedger::new();
        let mut executor = ledger.start_executor(1, &checks, &checkpoint).unwrap();
        complete_executor(&mut ledger, &mut executor);
        ledger.finish_role(executor).unwrap();
        let mut reviewer = ledger.start_reviewer(1).unwrap();

        let mut stale_receipt = open_exploratory_response(&mut ledger, &mut reviewer);
        let stale_invocation = ledger
            .charge_exploratory_tool_call(&mut reviewer, &mut stale_receipt, runtime_action())
            .unwrap();
        ledger
            .retain_exploratory_wrapper_bytes(&mut reviewer, &stale_invocation, b"kept")
            .unwrap();
        ledger
            .finish_provider_response(&reviewer, &stale_receipt)
            .unwrap();
        complete_reviewer(&mut ledger, &mut reviewer, 0, b"manifest", b"", 0, 0);
        assert_eq!(reviewer.termination, Some(RoleBudgetTermination::Normal));

        assert_normal_sealed(ledger.begin_required_action(&mut reviewer));
        assert_normal_sealed(ledger.begin_exploratory_provider_request(&reviewer, b"q", 1));

        let required = fake_required_permit(&reviewer);
        assert_normal_sealed(ledger.begin_required_provider_request(&reviewer, &required, b"q", 1));
        let exploratory_request = fake_provider_request(&reviewer, ChargeClass::Exploratory);
        assert_normal_sealed(ledger.record_exploratory_provider_response(
            &mut reviewer,
            exploratory_request,
            b"r",
        ));
        let mut required = fake_required_permit(&reviewer);
        let required_class = ChargeClass::Required {
            action_id: required.action_id,
        };
        let required_request = fake_provider_request(&reviewer, required_class);
        assert_normal_sealed(ledger.record_required_provider_response(
            &mut reviewer,
            &mut required,
            required_request,
            b"r",
        ));
        assert_normal_sealed(ledger.record_transport_no_response(
            &reviewer,
            fake_provider_request(&reviewer, ChargeClass::Exploratory),
        ));
        assert_normal_sealed(ledger.finish_provider_response(&reviewer, &stale_receipt));
        assert_normal_sealed(ledger.discard_invalid_provider_response(&reviewer, &stale_receipt));
        assert_normal_sealed(ledger.complete_exploratory_early_terminal(
            &mut reviewer,
            &mut stale_receipt,
            EarlyRoleBudgetTermination::ReviewerChangesRequested,
        ));
        assert_normal_sealed(ledger.charge_exploratory_tool_call(
            &mut reviewer,
            &mut stale_receipt,
            runtime_action(),
        ));
        assert_normal_sealed(ledger.retain_exploratory_wrapper_bytes(
            &mut reviewer,
            &stale_invocation,
            b"kept",
        ));

        let mut required = fake_required_permit(&reviewer);
        assert_normal_sealed(ledger.charge_required_call(
            &mut reviewer,
            &mut required,
            &mut stale_receipt,
        ));
        assert_normal_sealed(ledger.retain_required_wrapper_bytes(
            &mut reviewer,
            &mut required,
            b"kept",
        ));
        assert_normal_sealed(ledger.observe_typed_reviewer_manifest(&reviewer, &required, 0));
        let manifest = ReviewerManifestBudgetReceipt {
            lease_id: reviewer.lease_id,
            role_run: reviewer.role_run,
            action_id: required.action_id,
            result_id: RetainedResultId(u64::MAX),
            chunk_count: 0,
        };
        assert_normal_sealed(ledger.bind_reviewer_manifest(&mut reviewer, &mut required, manifest));
        let early_required = fake_required_permit(&reviewer);
        assert_normal_sealed(ledger.complete_required_early_terminal(
            &mut reviewer,
            early_required,
            &mut stale_receipt,
            EarlyRoleBudgetTermination::ReviewerChangesRequested,
        ));
        let incomplete_required = fake_required_permit(&reviewer);
        assert_normal_sealed(ledger.complete_required_action(&mut reviewer, incomplete_required));

        let finished = ledger.finish_role(reviewer).unwrap();
        assert_eq!(finished.termination(), RoleBudgetTermination::Normal);
    }

    #[test]
    fn typed_manifest_receipt_is_bound_to_the_charged_action_and_shrinks_only_unused_coverage() {
        let (checks, checkpoint) = passed_quality();
        let mut ledger = TaskBudgetLedger::new();
        let mut executor = ledger.start_executor(1, &checks, &checkpoint).unwrap();
        complete_executor(&mut ledger, &mut executor);
        ledger.finish_role(executor).unwrap();
        let mut reviewer = ledger.start_reviewer(1).unwrap();

        let mut permit = ledger.begin_required_action(&mut reviewer).unwrap();
        assert!(matches!(
            ledger.observe_typed_reviewer_manifest(&reviewer, &permit, 3),
            Err(BudgetError::RequiredResultNotCharged)
        ));
        let request = ledger
            .begin_required_provider_request(&reviewer, &permit, b"q", b"manifest".len())
            .unwrap();
        let mut receipt = ledger
            .record_required_provider_response(&mut reviewer, &mut permit, request, b"manifest")
            .unwrap();
        ledger
            .charge_required_call(&mut reviewer, &mut permit, &mut receipt)
            .unwrap();
        ledger
            .retain_required_wrapper_bytes(
                &mut reviewer,
                &mut permit,
                &vec![0; REVIEW_MANIFEST_RETAINED_RESULT_LIMIT],
            )
            .unwrap();
        assert!(matches!(
            ledger.observe_typed_reviewer_manifest(
                &reviewer,
                &permit,
                MAX_REVIEW_DIFF_CHUNKS + 1
            ),
            Err(BudgetError::TooManyReviewDiffChunks {
                observed
            }) if observed == MAX_REVIEW_DIFF_CHUNKS + 1
        ));

        let manifest = ledger
            .observe_typed_reviewer_manifest(&reviewer, &permit, 3)
            .unwrap();
        ledger
            .bind_reviewer_manifest(&mut reviewer, &mut permit, manifest)
            .unwrap();
        assert_eq!(reviewer.required_reservation.model_responses(), 3);
        assert_eq!(reviewer.required_reservation.model_visible_calls(), 3);
        assert_eq!(
            reviewer.required_reservation.retained_result_bytes(),
            REVIEW_DIFF_BATCH_RETAINED_RESULT_LIMIT + REVIEW_DIFF_CHUNK_RETAINED_RESULT_LIMIT
        );
        assert!(
            ledger
                .observe_typed_reviewer_manifest(&reviewer, &permit, 3)
                .is_ok()
        );
        let duplicate = ledger
            .observe_typed_reviewer_manifest(&reviewer, &permit, 3)
            .unwrap();
        assert!(matches!(
            ledger.bind_reviewer_manifest(&mut reviewer, &mut permit, duplicate),
            Err(BudgetError::ReviewerManifestAlreadyBound)
        ));
        assert_eq!(
            reviewer.required_reservation.retained_result_bytes(),
            REVIEW_DIFF_BATCH_RETAINED_RESULT_LIMIT + REVIEW_DIFF_CHUNK_RETAINED_RESULT_LIMIT
        );
    }

    #[test]
    fn exact_task_response_limit_is_shared_across_role_runs() {
        let (checks, checkpoint) = passed_quality();
        let mut ledger = TaskBudgetLedger::new();

        let mut planner = ledger.start_planner().unwrap();
        charge_exploratory_responses(&mut ledger, &mut planner, 7);
        complete_planner(&mut ledger, &mut planner);
        ledger.finish_role(planner).unwrap();

        let mut executor_1 = ledger.start_executor(1, &checks, &checkpoint).unwrap();
        charge_exploratory_responses(&mut ledger, &mut executor_1, 19);
        complete_executor(&mut ledger, &mut executor_1);
        ledger.finish_role(executor_1).unwrap();
        let mut reviewer_1 = ledger.start_reviewer(1).unwrap();
        charge_exploratory_responses(&mut ledger, &mut reviewer_1, 4);
        complete_reviewer(&mut ledger, &mut reviewer_1, 0, b"m", b"", 4, 0);
        ledger.finish_role(reviewer_1).unwrap();

        let mut executor_2 = ledger.start_executor(2, &checks, &checkpoint).unwrap();
        charge_exploratory_responses(&mut ledger, &mut executor_2, 15);
        complete_executor(&mut ledger, &mut executor_2);
        ledger.finish_role(executor_2).unwrap();
        let mut reviewer_2 = ledger.start_reviewer(2).unwrap();
        complete_reviewer(&mut ledger, &mut reviewer_2, 0, b"m", b"", 4, 0);

        assert_eq!(ledger.usage.model_responses(), TASK_RESPONSE_LIMIT);
        assert!(matches!(
            ledger.begin_exploratory_provider_request(&reviewer_2, b"q", 1),
            Err(BudgetError::RoleAlreadyTerminated {
                termination: RoleBudgetTermination::Normal
            })
        ));
        ledger.finish_role(reviewer_2).unwrap();
        assert!(matches!(
            ledger.start_executor(3, &checks, &checkpoint),
            Err(BudgetError::TaskLimitExceeded {
                resource: BudgetResource::ModelResponses
            })
        ));
    }

    #[test]
    fn exact_task_call_limit_counts_runtime_and_required_controls_across_roles() {
        let (checks, checkpoint) = passed_quality();
        let mut ledger = TaskBudgetLedger::new();

        let mut planner = ledger.start_planner().unwrap();
        charge_runtime_calls(&mut ledger, &mut planner, 11, b"x");
        complete_planner(&mut ledger, &mut planner);
        ledger.finish_role(planner).unwrap();

        let mut executor_1 = ledger.start_executor(1, &checks, &checkpoint).unwrap();
        charge_runtime_calls(&mut ledger, &mut executor_1, 31, b"x");
        complete_executor(&mut ledger, &mut executor_1);
        ledger.finish_role(executor_1).unwrap();
        let mut reviewer_1 = ledger.start_reviewer(1).unwrap();
        charge_runtime_calls(&mut ledger, &mut reviewer_1, 10, b"x");
        complete_reviewer(&mut ledger, &mut reviewer_1, 0, b"m", b"", 0, 4);
        ledger.finish_role(reviewer_1).unwrap();

        let mut executor_2 = ledger.start_executor(2, &checks, &checkpoint).unwrap();
        charge_runtime_calls(&mut ledger, &mut executor_2, 29, b"x");
        complete_executor(&mut ledger, &mut executor_2);
        ledger.finish_role(executor_2).unwrap();
        let mut reviewer_2 = ledger.start_reviewer(2).unwrap();
        complete_reviewer(&mut ledger, &mut reviewer_2, 0, b"m", b"", 0, 4);

        assert_eq!(ledger.usage.model_visible_calls(), TASK_CALL_LIMIT);
        ledger.finish_role(reviewer_2).unwrap();
        assert!(matches!(
            ledger.start_executor(3, &checks, &checkpoint),
            Err(BudgetError::TaskLimitExceeded {
                resource: BudgetResource::ModelVisibleCalls
            })
        ));
    }

    #[test]
    fn exact_task_retained_limit_preserves_each_roles_actual_results() {
        let (checks, checkpoint) = passed_quality();
        let mut ledger = TaskBudgetLedger::new();

        let mut planner = ledger.start_planner().unwrap();
        charge_runtime_result(
            &mut ledger,
            &mut planner,
            &vec![0; PLANNER_RETAINED_RESULT_LIMIT],
        );
        complete_planner(&mut ledger, &mut planner);
        ledger.finish_role(planner).unwrap();

        let mut executor_1 = ledger.start_executor(1, &checks, &checkpoint).unwrap();
        charge_runtime_result(
            &mut ledger,
            &mut executor_1,
            &vec![1; EXECUTOR_RETAINED_RESULT_LIMIT],
        );
        complete_executor(&mut ledger, &mut executor_1);
        ledger.finish_role(executor_1).unwrap();
        let mut reviewer_1 = ledger.start_reviewer(1).unwrap();
        complete_reviewer(&mut ledger, &mut reviewer_1, 0, b"m", b"", 0, 0);
        ledger.finish_role(reviewer_1).unwrap();

        let mut executor_2 = ledger.start_executor(2, &checks, &checkpoint).unwrap();
        charge_runtime_result(&mut ledger, &mut executor_2, &vec![2; 200 * 1024 - 1]);
        complete_executor(&mut ledger, &mut executor_2);
        ledger.finish_role(executor_2).unwrap();
        let mut reviewer_2 = ledger.start_reviewer(2).unwrap();
        complete_reviewer(
            &mut ledger,
            &mut reviewer_2,
            MAX_REVIEW_DIFF_CHUNKS,
            &vec![3; REVIEW_MANIFEST_RETAINED_RESULT_LIMIT],
            &vec![4; REVIEW_DIFF_BATCH_RETAINED_RESULT_LIMIT],
            0,
            0,
        );

        assert_eq!(
            ledger.usage.retained_result_bytes(),
            TASK_RETAINED_RESULT_LIMIT
        );
        ledger.finish_role(reviewer_2).unwrap();
        assert!(matches!(
            ledger.start_executor(3, &checks, &checkpoint),
            Err(BudgetError::TaskLimitExceeded {
                resource: BudgetResource::RetainedResultBytes
            })
        ));
        assert_eq!(
            ledger.usage.retained_result_bytes(),
            TASK_RETAINED_RESULT_LIMIT
        );
    }
}
