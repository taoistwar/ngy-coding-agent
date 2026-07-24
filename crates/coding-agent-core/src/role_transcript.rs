use std::collections::BTreeSet;

use coding_agent_domain::{
    CheckEvidence, FindingSeverity, NewReviewEvidence, PlanSnapshot, RequiredCheck,
    RequiredCheckKind, RequiredCheckSelector, ReviewDecisionSource, ReviewFinding, ReviewVerdict,
    WorkspaceDigest,
};
use serde::Serialize;
use thiserror::Error;

use crate::{
    AllowedActions, ContextRedactor, ModelMessage, ModelRequest, ModelToolChoice,
    RequiredCheckLedger, RetainedToolResult, Role, RoleRun, ToolCallBatch, WorkspaceCheckpoint,
};

pub const MAX_ROLE_HANDOFF_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum RoleTranscriptError {
    #[error("the role handoff exceeds its canonical byte bound")]
    HandoffTooLarge,
    #[error("the role handoff or system policy is not redaction-stable")]
    RedactionUnstable,
    #[error("the role transcript tool batch is malformed or reuses an identifier")]
    InvalidToolBatch,
    #[error("the role transcript result order does not match the provider batch")]
    ResultOrderMismatch,
    #[error("the role handoff could not be encoded")]
    EncodingFailed,
}

/// Canonical Planner-only context. It contains no provider response metadata,
/// reasoning, tool calls, tool results, or another role's final text.
#[derive(Clone, PartialEq, Eq)]
pub struct PlannerHandoff {
    canonical_json: String,
}

impl PlannerHandoff {
    pub fn try_new(
        task_prompt: &str,
        repository_context: &str,
        checkpoint: &WorkspaceCheckpoint,
        catalog: &[RequiredCheckSelector],
        redactor: &dyn ContextRedactor,
    ) -> Result<Self, RoleTranscriptError> {
        let task_prompt = redactor.redact(task_prompt);
        let repository_context = redactor.redact(repository_context);
        let mut catalog = catalog
            .iter()
            .map(CanonicalSelector::from)
            .collect::<Vec<_>>();
        catalog.sort_by(|left, right| left.key().cmp(&right.key()));
        catalog.dedup_by(|left, right| left.key() == right.key());
        let handoff = CanonicalPlannerHandoff {
            handoff_version: 1,
            role: "planner",
            task_prompt: &task_prompt,
            repository_context: &repository_context,
            checkpoint: CanonicalCheckpoint {
                generation: checkpoint.generation(),
                workspace_digest: checkpoint.workspace_digest(),
            },
            repository_check_catalog: catalog,
        };
        let encoded =
            serde_json::to_vec(&handoff).map_err(|_| RoleTranscriptError::EncodingFailed)?;
        if encoded.len() > MAX_ROLE_HANDOFF_BYTES {
            return Err(RoleTranscriptError::HandoffTooLarge);
        }
        let canonical_json =
            String::from_utf8(encoded).map_err(|_| RoleTranscriptError::EncodingFailed)?;
        if redactor.redact(&canonical_json) != canonical_json {
            return Err(RoleTranscriptError::RedactionUnstable);
        }
        Ok(Self { canonical_json })
    }

    pub fn canonical_json(&self) -> &str {
        &self.canonical_json
    }

    pub fn encoded_len(&self) -> usize {
        self.canonical_json.len()
    }
}

impl std::fmt::Debug for PlannerHandoff {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PlannerHandoff")
            .field("encoded_len", &self.encoded_len())
            .field("canonical_json", &"<redacted>")
            .finish()
    }
}

/// Canonical inter-role context for Executor and Reviewer runs.
///
/// Construction accepts only bounded domain objects plus redacted task/repo
/// context. It has no raw-string/JSON escape hatch for provider messages,
/// reasoning, request IDs, usage, or another role's final text.
#[derive(Clone, PartialEq, Eq)]
pub struct ContinuationHandoff {
    role: Role,
    canonical_json: String,
}

impl ContinuationHandoff {
    pub fn try_new(
        role: Role,
        task_prompt: &str,
        repository_context: &str,
        plan: &PlanSnapshot,
        checkpoint: &WorkspaceCheckpoint,
        redactor: &dyn ContextRedactor,
    ) -> Result<Self, RoleTranscriptError> {
        if !matches!(role, Role::Executor | Role::Reviewer)
            || plan.format_version() != 1
            || plan.validate().is_err()
        {
            return Err(RoleTranscriptError::InvalidToolBatch);
        }
        let task_prompt = redactor.redact(task_prompt);
        let repository_context = redactor.redact(repository_context);
        let handoff = CanonicalContinuationHandoff {
            handoff_version: 1,
            role: role_name(role),
            task_prompt: &task_prompt,
            repository_context: &repository_context,
            plan,
            checkpoint: CanonicalCheckpoint {
                generation: checkpoint.generation(),
                workspace_digest: checkpoint.workspace_digest(),
            },
        };
        let encoded =
            serde_json::to_vec(&handoff).map_err(|_| RoleTranscriptError::EncodingFailed)?;
        if encoded.len() > MAX_ROLE_HANDOFF_BYTES {
            return Err(RoleTranscriptError::HandoffTooLarge);
        }
        let canonical_json =
            String::from_utf8(encoded).map_err(|_| RoleTranscriptError::EncodingFailed)?;
        // Structured plan authority cannot be silently redacted or omitted.
        // Any secret in the complete composition fails the handoff closed.
        if redactor.redact(&canonical_json) != canonical_json {
            return Err(RoleTranscriptError::RedactionUnstable);
        }
        Ok(Self {
            role,
            canonical_json,
        })
    }

    /// Constructs the complete typed handoff for one fresh Executor round.
    ///
    /// The first round has no rework context. Rounds two and three carry only
    /// the immediately preceding Reviewer's bounded structured findings and a
    /// core-generated banner; no review transcript or provider metadata can
    /// enter this value.
    #[allow(clippy::too_many_arguments)]
    pub fn try_for_executor(
        role_run: u32,
        task_prompt: &str,
        repository_context: &str,
        plan: &PlanSnapshot,
        checkpoint: &WorkspaceCheckpoint,
        required_checks: &RequiredCheckLedger,
        latest_reviewer_findings: Option<&[ReviewFinding]>,
        redactor: &dyn ContextRedactor,
    ) -> Result<Self, RoleTranscriptError> {
        if !(1..=3).contains(&role_run)
            || plan.format_version() != 1
            || plan.validate().is_err()
            || required_checks.checks().is_empty()
            || !required_checks
                .checks()
                .starts_with(plan.initial_required_checks())
            || (role_run == 1 && latest_reviewer_findings.is_some())
            || (role_run > 1
                && latest_reviewer_findings.is_none_or(|findings| {
                    findings.is_empty()
                        || findings.iter().enumerate().any(|(index, finding)| {
                            !finding.matches_review_position((role_run - 1) as u8, index + 1)
                        })
                        || !findings
                            .iter()
                            .any(|finding| finding.severity() == FindingSeverity::Blocking)
                }))
        {
            return Err(RoleTranscriptError::InvalidToolBatch);
        }

        let task_prompt = redactor.redact(task_prompt);
        let repository_context = redactor.redact(repository_context);
        let check_evidence = required_checks.current_evidence(checkpoint);
        let rework = latest_reviewer_findings.map(|findings| CanonicalExecutorRework {
            banner: executor_rework_banner(role_run),
            source_review_round: role_run - 1,
            findings,
        });
        let handoff = CanonicalExecutorHandoff {
            handoff_version: 1,
            role: "executor",
            role_run,
            task_prompt: &task_prompt,
            repository_context: &repository_context,
            plan,
            checkpoint: CanonicalCheckpoint {
                generation: checkpoint.generation(),
                workspace_digest: checkpoint.workspace_digest(),
            },
            required_checks: required_checks.checks(),
            check_evidence,
            rework,
        };
        let encoded =
            serde_json::to_vec(&handoff).map_err(|_| RoleTranscriptError::EncodingFailed)?;
        if encoded.len() > MAX_ROLE_HANDOFF_BYTES {
            return Err(RoleTranscriptError::HandoffTooLarge);
        }
        let canonical_json =
            String::from_utf8(encoded).map_err(|_| RoleTranscriptError::EncodingFailed)?;
        if redactor.redact(&canonical_json) != canonical_json {
            return Err(RoleTranscriptError::RedactionUnstable);
        }
        Ok(Self {
            role: Role::Executor,
            canonical_json,
        })
    }

    /// Constructs one bounded fresh Reviewer handoff without importing any
    /// Executor or previous Reviewer transcript messages.
    #[allow(clippy::too_many_arguments)]
    pub fn try_for_reviewer(
        role_run: u32,
        task_prompt: &str,
        repository_context: &str,
        plan: &PlanSnapshot,
        executor_summary: &str,
        checkpoint: &WorkspaceCheckpoint,
        required_checks: &RequiredCheckLedger,
        previous_reviews: &[NewReviewEvidence],
        redactor: &dyn ContextRedactor,
    ) -> Result<Self, RoleTranscriptError> {
        if !(1..=3).contains(&role_run)
            || plan.format_version() != 1
            || plan.validate().is_err()
            || required_checks.checks().is_empty()
            || !required_checks
                .checks()
                .starts_with(plan.initial_required_checks())
            || previous_reviews.len() != role_run.saturating_sub(1) as usize
            || !review_history_matches(plan, required_checks, previous_reviews)
        {
            return Err(RoleTranscriptError::InvalidToolBatch);
        }

        let task_prompt = redactor.redact(task_prompt);
        let repository_context = redactor.redact(repository_context);
        let executor_summary = redactor.redact(executor_summary);
        let previous_reviews = previous_reviews
            .iter()
            .map(|review| CanonicalPriorReview {
                round: review.round(),
                decision_source: review.decision_source(),
                verdict: review.verdict(),
                findings: review.findings(),
                added_required_checks: review.added_required_checks(),
            })
            .collect::<Vec<_>>();
        let handoff = CanonicalReviewerHandoff {
            handoff_version: 1,
            role: "reviewer",
            role_run,
            task_prompt: &task_prompt,
            repository_context: &repository_context,
            plan,
            executor_summary: &executor_summary,
            checkpoint: CanonicalCheckpoint {
                generation: checkpoint.generation(),
                workspace_digest: checkpoint.workspace_digest(),
            },
            required_checks: required_checks.checks(),
            check_evidence: required_checks.current_evidence(checkpoint),
            previous_reviews,
        };
        let encoded =
            serde_json::to_vec(&handoff).map_err(|_| RoleTranscriptError::EncodingFailed)?;
        if encoded.len() > MAX_ROLE_HANDOFF_BYTES {
            return Err(RoleTranscriptError::HandoffTooLarge);
        }
        let canonical_json =
            String::from_utf8(encoded).map_err(|_| RoleTranscriptError::EncodingFailed)?;
        if redactor.redact(&canonical_json) != canonical_json {
            return Err(RoleTranscriptError::RedactionUnstable);
        }
        Ok(Self {
            role: Role::Reviewer,
            canonical_json,
        })
    }

    pub const fn role(&self) -> Role {
        self.role
    }

    pub fn canonical_json(&self) -> &str {
        &self.canonical_json
    }

    pub fn encoded_len(&self) -> usize {
        self.canonical_json.len()
    }
}

impl std::fmt::Debug for ContinuationHandoff {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ContinuationHandoff")
            .field("role", &self.role)
            .field("encoded_len", &self.encoded_len())
            .field("canonical_json", &"<redacted>")
            .finish()
    }
}

/// Sealed canonical handoff variants accepted by a fresh role transcript.
/// There is intentionally no constructor from arbitrary serialized text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleHandoff {
    Planner(PlannerHandoff),
    Continuation(ContinuationHandoff),
}

impl RoleHandoff {
    pub const fn role(&self) -> Role {
        match self {
            Self::Planner(_) => Role::Planner,
            Self::Continuation(handoff) => handoff.role(),
        }
    }

    pub fn canonical_json(&self) -> &str {
        match self {
            Self::Planner(handoff) => handoff.canonical_json(),
            Self::Continuation(handoff) => handoff.canonical_json(),
        }
    }

    fn into_canonical_json(self) -> String {
        match self {
            Self::Planner(handoff) => handoff.canonical_json,
            Self::Continuation(handoff) => handoff.canonical_json,
        }
    }
}

impl From<PlannerHandoff> for RoleHandoff {
    fn from(value: PlannerHandoff) -> Self {
        Self::Planner(value)
    }
}

impl From<ContinuationHandoff> for RoleHandoff {
    fn from(value: ContinuationHandoff) -> Self {
        Self::Continuation(value)
    }
}

/// Transcript state is owned by exactly one role run. Constructing a new role
/// run always creates a fresh two-message `[system,user]` prefix and a fresh
/// tool-call ID namespace.
#[derive(Debug)]
pub struct RoleTranscript {
    owner: RoleRun,
    messages: Vec<ModelMessage>,
    seen_tool_call_ids: BTreeSet<String>,
}

impl RoleTranscript {
    pub fn try_fresh(
        owner: RoleRun,
        system_policy: impl Into<String>,
        handoff: RoleHandoff,
        redactor: &dyn ContextRedactor,
    ) -> Result<Self, RoleTranscriptError> {
        if owner.role() != handoff.role() {
            return Err(RoleTranscriptError::InvalidToolBatch);
        }
        let system_policy = system_policy.into();
        let handoff_json = handoff.canonical_json();
        if redactor.redact(&system_policy) != system_policy
            || redactor.redact(handoff_json) != handoff_json
            || redactor.redact(&format!("{system_policy}\n{handoff_json}"))
                != format!("{system_policy}\n{handoff_json}")
        {
            return Err(RoleTranscriptError::RedactionUnstable);
        }
        Ok(Self {
            owner,
            messages: vec![
                ModelMessage::system(system_policy),
                ModelMessage::user(handoff.into_canonical_json()),
            ],
            seen_tool_call_ids: BTreeSet::new(),
        })
    }

    pub fn try_for_planner(
        owner: RoleRun,
        system_policy: impl Into<String>,
        handoff: PlannerHandoff,
        redactor: &dyn ContextRedactor,
    ) -> Result<Self, RoleTranscriptError> {
        if owner.role() != Role::Planner || owner.role_run() != 1 {
            return Err(RoleTranscriptError::InvalidToolBatch);
        }
        Self::try_fresh(owner, system_policy, handoff.into(), redactor)
    }

    pub const fn owner(&self) -> RoleRun {
        self.owner
    }

    pub fn request(&self, tool_choice: ModelToolChoice) -> ModelRequest {
        ModelRequest {
            messages: self.messages.clone(),
            allowed_actions: AllowedActions::for_role(self.owner.role()),
            tool_choice,
        }
    }

    /// Atomically appends one same-role provider batch and its same-order
    /// canonical retained results.
    pub fn append_runtime_batch(
        &mut self,
        batch: ToolCallBatch,
        results: Vec<RetainedToolResult>,
    ) -> Result<(), RoleTranscriptError> {
        self.preflight_runtime_batch(&batch)?;
        if batch.calls.len() != results.len() {
            return Err(RoleTranscriptError::ResultOrderMismatch);
        }
        for (call, result) in batch.calls.iter().zip(&results) {
            if call.id != result.tool_call_id() {
                return Err(RoleTranscriptError::ResultOrderMismatch);
            }
        }

        self.seen_tool_call_ids
            .extend(batch.calls.iter().map(|call| call.id.clone()));
        self.messages.push(ModelMessage::AssistantToolCalls(batch));
        self.messages.extend(
            results
                .into_iter()
                .map(RetainedToolResult::into_model_message),
        );
        Ok(())
    }

    /// Same-run ID and capability preflight used before budget mutation or
    /// runtime dispatch.
    pub fn preflight_runtime_batch(
        &self,
        batch: &ToolCallBatch,
    ) -> Result<(), RoleTranscriptError> {
        if batch.calls.is_empty() {
            return Err(RoleTranscriptError::InvalidToolBatch);
        }
        let allowed = AllowedActions::for_role(self.owner.role());
        let mut batch_ids = BTreeSet::new();
        for call in &batch.calls {
            if call.id.is_empty()
                || call.id.len() > 256
                || call.id.chars().any(char::is_control)
                || !batch_ids.insert(call.id.as_str())
                || self.seen_tool_call_ids.contains(&call.id)
                || !allowed.allows_action(&call.request)
            {
                return Err(RoleTranscriptError::InvalidToolBatch);
            }
        }
        Ok(())
    }

    pub fn message_count(&self) -> usize {
        self.messages.len()
    }
}

#[derive(Serialize)]
struct CanonicalPlannerHandoff<'a> {
    handoff_version: u8,
    role: &'static str,
    task_prompt: &'a str,
    repository_context: &'a str,
    checkpoint: CanonicalCheckpoint,
    repository_check_catalog: Vec<CanonicalSelector<'a>>,
}

#[derive(Serialize)]
struct CanonicalContinuationHandoff<'a> {
    handoff_version: u8,
    role: &'static str,
    task_prompt: &'a str,
    repository_context: &'a str,
    plan: &'a PlanSnapshot,
    checkpoint: CanonicalCheckpoint,
}

#[derive(Serialize)]
struct CanonicalExecutorHandoff<'a> {
    handoff_version: u8,
    role: &'static str,
    role_run: u32,
    task_prompt: &'a str,
    repository_context: &'a str,
    plan: &'a PlanSnapshot,
    checkpoint: CanonicalCheckpoint,
    required_checks: &'a [RequiredCheck],
    check_evidence: Vec<CheckEvidence>,
    rework: Option<CanonicalExecutorRework<'a>>,
}

#[derive(Serialize)]
struct CanonicalExecutorRework<'a> {
    banner: String,
    source_review_round: u32,
    findings: &'a [ReviewFinding],
}

#[derive(Serialize)]
struct CanonicalReviewerHandoff<'a> {
    handoff_version: u8,
    role: &'static str,
    role_run: u32,
    task_prompt: &'a str,
    repository_context: &'a str,
    plan: &'a PlanSnapshot,
    executor_summary: &'a str,
    checkpoint: CanonicalCheckpoint,
    required_checks: &'a [RequiredCheck],
    check_evidence: Vec<CheckEvidence>,
    previous_reviews: Vec<CanonicalPriorReview<'a>>,
}

#[derive(Serialize)]
struct CanonicalPriorReview<'a> {
    round: u8,
    decision_source: ReviewDecisionSource,
    verdict: ReviewVerdict,
    findings: &'a [ReviewFinding],
    added_required_checks: &'a [RequiredCheck],
}

fn review_history_matches(
    plan: &PlanSnapshot,
    required_checks: &RequiredCheckLedger,
    previous_reviews: &[NewReviewEvidence],
) -> bool {
    let mut expected_checks = plan.initial_required_checks().to_vec();
    for (index, review) in previous_reviews.iter().enumerate() {
        if review.round() != (index + 1) as u8
            || review.verdict() != ReviewVerdict::ChangesRequested
            || !review
                .findings()
                .iter()
                .any(|finding| finding.severity() == FindingSeverity::Blocking)
            || !review.required_checks().starts_with(&expected_checks)
            || &review.required_checks()[expected_checks.len()..] != review.added_required_checks()
        {
            return false;
        }
        expected_checks = review.required_checks().to_vec();
    }
    required_checks.checks() == expected_checks
}

#[derive(Serialize)]
struct CanonicalCheckpoint {
    generation: u64,
    workspace_digest: WorkspaceDigest,
}

#[derive(Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CanonicalSelector<'a> {
    CargoCheck {
        package: Option<&'a str>,
    },
    CargoTest {
        package: Option<&'a str>,
        integration_test: Option<&'a str>,
    },
}

impl<'a> CanonicalSelector<'a> {
    fn key(&self) -> (u8, Option<&str>, Option<&str>) {
        match self {
            Self::CargoCheck { package } => (0, *package, None),
            Self::CargoTest {
                package,
                integration_test,
            } => (1, *package, *integration_test),
        }
    }
}

impl<'a> From<&'a RequiredCheckSelector> for CanonicalSelector<'a> {
    fn from(selector: &'a RequiredCheckSelector) -> Self {
        match selector.kind() {
            RequiredCheckKind::CargoCheck => Self::CargoCheck {
                package: selector.package(),
            },
            RequiredCheckKind::CargoTest => Self::CargoTest {
                package: selector.package(),
                integration_test: selector.integration_test(),
            },
        }
    }
}

const fn role_name(role: Role) -> &'static str {
    match role {
        Role::Planner => "planner",
        Role::Executor => "executor",
        Role::Reviewer => "reviewer",
    }
}

pub fn executor_rework_banner(role_run: u32) -> String {
    format!("Rework round {}", role_run.saturating_sub(1))
}
