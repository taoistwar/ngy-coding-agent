use std::collections::BTreeSet;

use coding_agent_domain::{
    FindingSeverity, MAX_WORKSPACE_GENERATION, RequiredCheck, RequiredCheckKind,
    RequiredCheckSelector, ReviewFinding, ReviewVerdict, WorkspaceDigest,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::model::is_valid_tool_request;
use crate::{ContextRedactor, ModelResponse, ModelToolChoice, ToolCallBatch, ToolRequest};

const MAX_CONTROL_JSON_BYTES: usize = 64 * 1024;
const MAX_SUMMARY_SCALARS: usize = 4_096;
const MAX_PLAN_STEPS: usize = 32;
const MAX_TITLE_SCALARS: usize = 256;
const MAX_DESCRIPTION_SCALARS: usize = 4_096;
const MAX_ACCEPTANCE_CRITERIA: usize = 8;
const MAX_ACCEPTANCE_CRITERION_SCALARS: usize = 1_024;
const MAX_REQUIRED_CHECKS: usize = 16;
const MAX_FINDINGS: usize = 32;
const MAX_FINDING_MESSAGE_SCALARS: usize = 2_048;
const MAX_PLAN_PROGRESS_UPDATES: usize = 32;
const MAX_TOOL_CALL_ID_BYTES: usize = 256;
const MAX_REVIEW_DIFF_CHUNKS: u8 = 8;
const MAX_REVIEW_DIFF_BATCH_CHUNKS: u8 = 2;

/// The action-contract role is the same identity used by the shared task
/// budget ledger; there is no parallel role enum to drift.
pub use crate::budget::BudgetRole as Role;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActionProfile {
    Legacy,
    Role(Role),
}

/// Immutable model-visible capability set for one provider request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllowedActions {
    profile: ActionProfile,
}

impl AllowedActions {
    /// Transitional Project 2 action set used only by the old `AgentLoop`.
    pub const fn legacy() -> Self {
        Self {
            profile: ActionProfile::Legacy,
        }
    }

    pub const fn for_role(role: Role) -> Self {
        Self {
            profile: ActionProfile::Role(role),
        }
    }

    pub const fn role(&self) -> Option<Role> {
        match self.profile {
            ActionProfile::Legacy => None,
            ActionProfile::Role(role) => Some(role),
        }
    }

    pub fn names(&self) -> Vec<&'static str> {
        match self.profile {
            ActionProfile::Legacy => LEGACY_ACTIONS.to_vec(),
            ActionProfile::Role(Role::Planner) => PLANNER_ACTIONS.to_vec(),
            ActionProfile::Role(Role::Executor) => EXECUTOR_ACTIONS.to_vec(),
            ActionProfile::Role(Role::Reviewer) => REVIEWER_ACTIONS.to_vec(),
        }
    }

    pub fn allows_name(&self, name: &str) -> bool {
        self.names().contains(&name)
    }

    pub const fn is_legacy(&self) -> bool {
        matches!(self.profile, ActionProfile::Legacy)
    }

    pub fn allows_action(&self, request: &ActionRequest) -> bool {
        match self.profile {
            ActionProfile::Legacy => {
                matches!(
                    request,
                    ActionRequest::Runtime(RuntimeActionRequest::Tool(_))
                ) && self.allows_name(request.name())
                    && request.validate().is_ok()
            }
            ActionProfile::Role(role) => {
                self.allows_name(request.name())
                    && role_allows_request_shape(role, request)
                    && request.validate().is_ok()
            }
        }
    }

    pub fn allows_required(&self, required: &RequiredAction) -> bool {
        if required.validate().is_err() || !self.allows_name(required.action_name()) {
            return false;
        }
        match (self.profile, required) {
            (ActionProfile::Legacy, RequiredAction::LegacyCargoTest) => true,
            (
                ActionProfile::Role(Role::Executor | Role::Reviewer),
                RequiredAction::Validation(_),
            ) => true,
            (
                ActionProfile::Role(Role::Reviewer),
                RequiredAction::ReviewDiffManifest { .. }
                | RequiredAction::ReviewDiffManifestOrTerminal { .. }
                | RequiredAction::ReviewDiffChunks { .. }
                | RequiredAction::ReviewDiffChunksOrTerminal { .. },
            ) => true,
            (ActionProfile::Role(role), RequiredAction::Terminal(kind)) => match role {
                Role::Planner => {
                    matches!(kind, ControlKind::SubmitPlan | ControlKind::ReportBlocked)
                }
                Role::Executor => matches!(
                    kind,
                    ControlKind::SubmitExecution | ControlKind::ReportBlocked
                ),
                Role::Reviewer => {
                    matches!(kind, ControlKind::SubmitReview | ControlKind::ReportBlocked)
                }
            },
            (ActionProfile::Role(role), RequiredAction::TerminalOrBlocked(normal_terminal)) => {
                match role {
                    Role::Planner => *normal_terminal == ControlKind::SubmitPlan,
                    Role::Executor => *normal_terminal == ControlKind::SubmitExecution,
                    Role::Reviewer => *normal_terminal == ControlKind::SubmitReview,
                }
            }
            _ => false,
        }
    }
}

const LEGACY_ACTIONS: &[&str] = &[
    "list_files",
    "read_file",
    "search_text",
    "replace_file",
    "cargo_check",
    "cargo_test",
    "git_status",
    "git_diff",
];
const PLANNER_ACTIONS: &[&str] = &[
    "list_files",
    "read_file",
    "search_text",
    "submit_plan",
    "report_blocked",
];
const EXECUTOR_ACTIONS: &[&str] = &[
    "list_files",
    "read_file",
    "search_text",
    "replace_file",
    "cargo_check",
    "cargo_test",
    "git_status",
    "git_diff",
    "update_plan_progress",
    "submit_execution",
    "report_blocked",
];
const REVIEWER_ACTIONS: &[&str] = &[
    "list_files",
    "read_file",
    "search_text",
    "cargo_check",
    "cargo_test",
    "git_status",
    "git_diff",
    "review_diff_manifest",
    "review_diff_chunks",
    "submit_review",
    "report_blocked",
];

/// Provider actions are partitioned before dispatch. A control can never be
/// passed to `ToolRuntime::invoke`, whose input remains the narrower
/// `ToolRequest`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActionRequest {
    Runtime(RuntimeActionRequest),
    Control(ControlRequest),
}

impl ActionRequest {
    pub fn runtime(request: ToolRequest) -> Self {
        Self::Runtime(RuntimeActionRequest::Tool(request))
    }

    pub fn decode(role: Role, name: &str, arguments: &str) -> Result<Self, RoleContractError> {
        decode_action(&AllowedActions::for_role(role), name, arguments)
    }

    pub fn decode_legacy(name: &str, arguments: &str) -> Result<Self, RoleContractError> {
        decode_action(&AllowedActions::legacy(), name, arguments)
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Runtime(request) => request.name(),
            Self::Control(request) => request.kind().name(),
        }
    }

    pub const fn as_tool_request(&self) -> Option<&ToolRequest> {
        match self {
            Self::Runtime(RuntimeActionRequest::Tool(request)) => Some(request),
            _ => None,
        }
    }

    pub fn into_tool_request(self) -> Result<ToolRequest, RoleContractError> {
        match self {
            Self::Runtime(RuntimeActionRequest::Tool(request)) => Ok(request),
            _ => Err(RoleContractError::ControlRuntimeBoundary),
        }
    }

    pub fn canonical_arguments(&self) -> Result<Vec<u8>, RoleContractError> {
        serde_json::to_vec(&self.arguments_value()).map_err(|_| RoleContractError::InvalidPayload)
    }

    /// Revalidates every semantic and canonical-size invariant at the local
    /// execution boundary. This is intentionally required even for values
    /// produced by public `Deserialize` implementations or alternate scripted
    /// providers, which need not have passed through [`Self::decode`].
    pub fn validate(&self) -> Result<(), RoleContractError> {
        match self {
            Self::Runtime(request) => request.validate(),
            Self::Control(request) => request.validate(),
        }
    }

    pub fn is_redaction_stable(&self, redactor: &dyn ContextRedactor) -> bool {
        let mut stable = true;
        self.visit_strings(&mut |value| {
            if redactor.redact(value) != value {
                stable = false;
            }
        });
        stable
    }

    pub fn contains_secret(&self, secret: &str) -> bool {
        let mut found = false;
        self.visit_strings(&mut |value| found |= value.contains(secret));
        found
    }

    pub fn visit_strings(&self, visitor: &mut dyn FnMut(&str)) {
        match self {
            Self::Runtime(request) => request.visit_strings(visitor),
            Self::Control(request) => request.visit_strings(visitor),
        }
    }

    fn arguments_value(&self) -> serde_json::Value {
        match self {
            Self::Runtime(request) => request.arguments_value(),
            Self::Control(request) => request.arguments_value(),
        }
    }
}

impl From<ToolRequest> for ActionRequest {
    fn from(value: ToolRequest) -> Self {
        Self::runtime(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeActionRequest {
    Tool(ToolRequest),
    Validation {
        check: RequiredCheck,
    },
    ValidationSelector {
        selector: RequiredCheckSelector,
    },
    ReviewDiffManifest {
        generation: u64,
        workspace_digest: WorkspaceDigest,
    },
    ReviewDiffChunks {
        generation: u64,
        workspace_digest: WorkspaceDigest,
        manifest_sha256: String,
        start_chunk: u8,
        count: u8,
    },
}

impl RuntimeActionRequest {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Tool(request) => tool_request_name(request),
            Self::Validation { check } => kind_name(check.selector().kind()),
            Self::ValidationSelector { selector } => kind_name(selector.kind()),
            Self::ReviewDiffManifest { .. } => "review_diff_manifest",
            Self::ReviewDiffChunks { .. } => "review_diff_chunks",
        }
    }

    pub const fn exact_check(&self) -> Option<&RequiredCheck> {
        match self {
            Self::Validation { check } => Some(check),
            _ => None,
        }
    }

    fn validate(&self) -> Result<(), RoleContractError> {
        match self {
            Self::Tool(request) if is_valid_tool_request(request) => Ok(()),
            Self::Tool(_) => Err(RoleContractError::InvalidPayload),
            // These domain values can only be created through their validated
            // constructors, so possession of the value is the invariant.
            Self::Validation { .. } | Self::ValidationSelector { .. } => Ok(()),
            Self::ReviewDiffManifest {
                generation,
                workspace_digest,
            } => RequiredAction::review_diff_manifest(*generation, workspace_digest.clone())
                .map(|_| ()),
            Self::ReviewDiffChunks {
                generation,
                workspace_digest,
                manifest_sha256,
                start_chunk,
                count,
            } => RequiredAction::review_diff_chunks(
                *generation,
                workspace_digest.clone(),
                manifest_sha256.clone(),
                *start_chunk,
                *count,
            )
            .map(|_| ()),
        }
    }

    pub const fn proposed_selector(&self) -> Option<&RequiredCheckSelector> {
        match self {
            Self::ValidationSelector { selector } => Some(selector),
            _ => None,
        }
    }

    fn arguments_value(&self) -> serde_json::Value {
        use serde_json::json;
        match self {
            Self::Tool(request) => tool_request_arguments(request),
            Self::Validation { check } => match check.selector().kind() {
                RequiredCheckKind::CargoCheck => {
                    json!({"check_id": check.id(), "package": check.package()})
                }
                RequiredCheckKind::CargoTest => json!({
                    "check_id": check.id(),
                    "package": check.package(),
                    "integration_test": check.integration_test()
                }),
            },
            Self::ValidationSelector { selector } => match selector.kind() {
                RequiredCheckKind::CargoCheck => json!({"package": selector.package()}),
                RequiredCheckKind::CargoTest => json!({
                    "package": selector.package(),
                    "integration_test": selector.integration_test()
                }),
            },
            Self::ReviewDiffManifest {
                generation,
                workspace_digest,
            } => json!({"generation": generation, "workspace_digest": workspace_digest}),
            Self::ReviewDiffChunks {
                generation,
                workspace_digest,
                manifest_sha256,
                start_chunk,
                count,
            } => json!({
                "generation": generation,
                "workspace_digest": workspace_digest,
                "manifest_sha256": manifest_sha256,
                "start_chunk": start_chunk,
                "count": count
            }),
        }
    }

    fn visit_strings(&self, visitor: &mut dyn FnMut(&str)) {
        match self {
            Self::Tool(request) => visit_tool_request_strings(request, visitor),
            Self::Validation { check } => {
                visitor(check.id());
                visit_selector_strings(check.selector(), visitor);
            }
            Self::ValidationSelector { selector } => visit_selector_strings(selector, visitor),
            Self::ReviewDiffManifest {
                workspace_digest, ..
            } => visitor(workspace_digest.value()),
            Self::ReviewDiffChunks {
                workspace_digest,
                manifest_sha256,
                ..
            } => {
                visitor(workspace_digest.value());
                visitor(manifest_sha256);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ControlKind {
    SubmitPlan,
    SubmitExecution,
    SubmitReview,
    ReportBlocked,
    UpdatePlanProgress,
}

impl ControlKind {
    pub const fn name(self) -> &'static str {
        match self {
            Self::SubmitPlan => "submit_plan",
            Self::SubmitExecution => "submit_execution",
            Self::SubmitReview => "submit_review",
            Self::ReportBlocked => "report_blocked",
            Self::UpdatePlanProgress => "update_plan_progress",
        }
    }

    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::UpdatePlanProgress)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlRequest {
    SubmitPlan(PlanSubmission),
    SubmitExecution(ExecutionSubmission),
    SubmitReview(ReviewSubmission),
    ReportBlocked(BlockedSubmission),
    UpdatePlanProgress(PlanProgressSubmission),
}

impl ControlRequest {
    pub const fn kind(&self) -> ControlKind {
        match self {
            Self::SubmitPlan(_) => ControlKind::SubmitPlan,
            Self::SubmitExecution(_) => ControlKind::SubmitExecution,
            Self::SubmitReview(_) => ControlKind::SubmitReview,
            Self::ReportBlocked(_) => ControlKind::ReportBlocked,
            Self::UpdatePlanProgress(_) => ControlKind::UpdatePlanProgress,
        }
    }

    fn validate(&self) -> Result<(), RoleContractError> {
        match self {
            Self::SubmitPlan(value) => value.validate(),
            Self::SubmitExecution(value) => value.validate(),
            Self::SubmitReview(value) => value.validate(),
            Self::ReportBlocked(value) => value.validate(),
            Self::UpdatePlanProgress(value) => value.validate(),
        }
    }

    fn arguments_value(&self) -> serde_json::Value {
        match self {
            Self::SubmitPlan(value) => serde_json::to_value(value),
            Self::SubmitExecution(value) => serde_json::to_value(value),
            Self::SubmitReview(value) => serde_json::to_value(value),
            Self::ReportBlocked(value) => serde_json::to_value(value),
            Self::UpdatePlanProgress(value) => serde_json::to_value(value),
        }
        .expect("validated control is serializable")
    }

    fn visit_strings(&self, visitor: &mut dyn FnMut(&str)) {
        match self {
            Self::SubmitPlan(value) => value.visit_strings(visitor),
            Self::SubmitExecution(value) => visitor(&value.summary),
            Self::SubmitReview(value) => value.visit_strings(visitor),
            Self::ReportBlocked(value) => visitor(&value.summary),
            Self::UpdatePlanProgress(value) => {
                for update in &value.updates {
                    visitor(&update.step_id);
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PlanSubmission {
    summary: String,
    steps: Vec<PlanStepSubmission>,
    initial_required_checks: Vec<CheckSelectorSubmission>,
}

impl PlanSubmission {
    pub fn summary(&self) -> &str {
        &self.summary
    }

    pub fn steps(&self) -> &[PlanStepSubmission] {
        &self.steps
    }

    pub fn initial_required_checks(&self) -> &[CheckSelectorSubmission] {
        &self.initial_required_checks
    }

    fn validate(&self) -> Result<(), RoleContractError> {
        if self.summary.chars().count() > MAX_SUMMARY_SCALARS
            || !(1..=MAX_PLAN_STEPS).contains(&self.steps.len())
            || !(1..=MAX_REQUIRED_CHECKS).contains(&self.initial_required_checks.len())
        {
            return Err(RoleContractError::InvalidPayload);
        }
        for step in &self.steps {
            step.validate()?;
        }
        let mut selectors = BTreeSet::new();
        let mut has_cargo_test = false;
        for check in &self.initial_required_checks {
            let selector = check.selector()?;
            has_cargo_test |= selector.kind() == RequiredCheckKind::CargoTest;
            if !selectors.insert(selector_key(&selector)) {
                return Err(RoleContractError::InvalidPayload);
            }
        }
        if !has_cargo_test {
            return Err(RoleContractError::InvalidPayload);
        }
        validate_control_size(self)
    }

    fn visit_strings(&self, visitor: &mut dyn FnMut(&str)) {
        visitor(&self.summary);
        for step in &self.steps {
            visitor(&step.title);
            visitor(&step.description);
            for criterion in &step.acceptance_criteria {
                visitor(criterion);
            }
        }
        for check in &self.initial_required_checks {
            check.visit_strings(visitor);
        }
    }
}

impl<'de> Deserialize<'de> for PlanSubmission {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            summary: String,
            steps: Vec<PlanStepSubmission>,
            initial_required_checks: Vec<CheckSelectorSubmission>,
        }
        let raw = Raw::deserialize(deserializer)?;
        Ok(Self {
            summary: raw.summary,
            steps: raw.steps,
            initial_required_checks: raw.initial_required_checks,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanStepSubmission {
    title: String,
    description: String,
    acceptance_criteria: Vec<String>,
}

impl PlanStepSubmission {
    pub fn title(&self) -> &str {
        &self.title
    }

    pub fn description(&self) -> &str {
        &self.description
    }

    pub fn acceptance_criteria(&self) -> &[String] {
        &self.acceptance_criteria
    }

    fn validate(&self) -> Result<(), RoleContractError> {
        if self.title.is_empty()
            || self.title.chars().count() > MAX_TITLE_SCALARS
            || self.description.chars().count() > MAX_DESCRIPTION_SCALARS
            || !(1..=MAX_ACCEPTANCE_CRITERIA).contains(&self.acceptance_criteria.len())
            || self.acceptance_criteria.iter().any(|criterion| {
                criterion.is_empty() || criterion.chars().count() > MAX_ACCEPTANCE_CRITERION_SCALARS
            })
        {
            return Err(RoleContractError::InvalidPayload);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CheckSelectorSubmission {
    CargoCheck {
        package: Option<String>,
    },
    CargoTest {
        package: Option<String>,
        integration_test: Option<String>,
    },
}

impl CheckSelectorSubmission {
    pub fn selector(&self) -> Result<RequiredCheckSelector, RoleContractError> {
        match self {
            Self::CargoCheck { package } => RequiredCheckSelector::try_cargo_check(package.clone()),
            Self::CargoTest {
                package,
                integration_test,
            } => RequiredCheckSelector::try_cargo_test(package.clone(), integration_test.clone()),
        }
        .map_err(|_| RoleContractError::InvalidPayload)
    }

    fn visit_strings(&self, visitor: &mut dyn FnMut(&str)) {
        match self {
            Self::CargoCheck { package } => visit_option(package.as_deref(), visitor),
            Self::CargoTest {
                package,
                integration_test,
            } => {
                visit_option(package.as_deref(), visitor);
                visit_option(integration_test.as_deref(), visitor);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionSubmission {
    summary: String,
}

impl ExecutionSubmission {
    pub fn summary(&self) -> &str {
        &self.summary
    }

    fn validate(&self) -> Result<(), RoleContractError> {
        if self.summary.chars().count() > MAX_SUMMARY_SCALARS {
            return Err(RoleContractError::InvalidPayload);
        }
        validate_control_size(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReviewSubmission {
    verdict: ReviewVerdict,
    summary: String,
    findings: Vec<ReviewFindingSubmission>,
    add_required_checks: Vec<CheckSelectorSubmission>,
}

impl ReviewSubmission {
    pub const fn verdict(&self) -> ReviewVerdict {
        self.verdict
    }

    pub const fn is_approved(&self) -> bool {
        matches!(self.verdict, ReviewVerdict::Approved)
    }

    pub fn summary(&self) -> &str {
        &self.summary
    }

    pub fn findings(&self) -> &[ReviewFindingSubmission] {
        &self.findings
    }

    pub fn add_required_checks(&self) -> &[CheckSelectorSubmission] {
        &self.add_required_checks
    }

    fn validate(&self) -> Result<(), RoleContractError> {
        if self.summary.is_empty()
            || self.summary.chars().count() > MAX_SUMMARY_SCALARS
            || self.findings.len() > MAX_FINDINGS
            || self.add_required_checks.len() > MAX_REQUIRED_CHECKS
        {
            return Err(RoleContractError::InvalidPayload);
        }
        let mut has_blocking = false;
        for (index, finding) in self.findings.iter().enumerate() {
            finding.validate(index + 1)?;
            has_blocking |= finding.severity == FindingSeverity::Blocking;
        }
        if matches!(self.verdict, ReviewVerdict::Approved) && has_blocking
            || matches!(self.verdict, ReviewVerdict::ChangesRequested) && !has_blocking
        {
            return Err(RoleContractError::InvalidPayload);
        }
        let mut selectors = BTreeSet::new();
        for check in &self.add_required_checks {
            let selector = check.selector()?;
            if !selectors.insert(selector_key(&selector)) {
                return Err(RoleContractError::InvalidPayload);
            }
        }
        validate_control_size(self)
    }

    fn visit_strings(&self, visitor: &mut dyn FnMut(&str)) {
        visitor(&self.summary);
        for finding in &self.findings {
            visitor(&finding.message);
            visit_option(finding.path.as_deref(), visitor);
        }
        for check in &self.add_required_checks {
            check.visit_strings(visitor);
        }
    }
}

impl<'de> Deserialize<'de> for ReviewSubmission {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            verdict: ReviewVerdict,
            summary: String,
            findings: Vec<ReviewFindingSubmission>,
            add_required_checks: Vec<CheckSelectorSubmission>,
        }
        let raw = Raw::deserialize(deserializer)?;
        Ok(Self {
            verdict: raw.verdict,
            summary: raw.summary,
            findings: raw.findings,
            add_required_checks: raw.add_required_checks,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewFindingSubmission {
    severity: FindingSeverity,
    message: String,
    path: Option<String>,
    line: Option<u64>,
}

impl ReviewFindingSubmission {
    pub const fn severity(&self) -> FindingSeverity {
        self.severity
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }

    pub const fn line(&self) -> Option<u64> {
        self.line
    }

    fn validate(&self, ordinal: usize) -> Result<(), RoleContractError> {
        if self.message.is_empty()
            || self.message.chars().count() > MAX_FINDING_MESSAGE_SCALARS
            || ReviewFinding::try_for_review(
                1,
                ordinal,
                self.severity,
                self.message.clone(),
                self.path.clone(),
                self.line,
            )
            .is_err()
        {
            return Err(RoleContractError::InvalidPayload);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockedReason {
    MissingRequiredContext,
    ConflictingUserRequirements,
    RequiresGoalChange,
    UnsupportedScope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlockedSubmission {
    reason: BlockedReason,
    summary: String,
}

impl BlockedSubmission {
    pub const fn reason(&self) -> BlockedReason {
        self.reason
    }

    pub fn summary(&self) -> &str {
        &self.summary
    }

    fn validate(&self) -> Result<(), RoleContractError> {
        if self.summary.chars().count() > MAX_SUMMARY_SCALARS {
            return Err(RoleContractError::InvalidPayload);
        }
        validate_control_size(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanProgressStatus {
    Running,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PlanProgressSubmission {
    updates: Vec<PlanProgressUpdate>,
}

impl PlanProgressSubmission {
    pub fn updates(&self) -> &[PlanProgressUpdate] {
        &self.updates
    }

    fn validate(&self) -> Result<(), RoleContractError> {
        if !(1..=MAX_PLAN_PROGRESS_UPDATES).contains(&self.updates.len()) {
            return Err(RoleContractError::InvalidPayload);
        }
        let mut ids = BTreeSet::new();
        if self.updates.iter().any(|update| {
            update.step_id.is_empty()
                || update.step_id.len() > MAX_TOOL_CALL_ID_BYTES
                || update.step_id.chars().any(char::is_control)
                || !ids.insert(update.step_id.as_str())
        }) {
            return Err(RoleContractError::InvalidPayload);
        }
        validate_control_size(self)
    }
}

impl<'de> Deserialize<'de> for PlanProgressSubmission {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            updates: Vec<PlanProgressUpdate>,
        }
        Ok(Self {
            updates: Raw::deserialize(deserializer)?.updates,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanProgressUpdate {
    step_id: String,
    status: PlanProgressStatus,
}

impl PlanProgressUpdate {
    pub fn step_id(&self) -> &str {
        &self.step_id
    }

    pub const fn status(&self) -> PlanProgressStatus {
        self.status
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequiredAction {
    /// Project 2 source/wire compatibility during the migration.
    LegacyCargoTest,
    Validation(RequiredCheck),
    ReviewDiffManifest {
        generation: u64,
        workspace_digest: WorkspaceDigest,
    },
    /// Reviewer coverage convergence may either read the exact next manifest
    /// or terminate early with `changes_requested`/`report_blocked`.
    ReviewDiffManifestOrTerminal {
        generation: u64,
        workspace_digest: WorkspaceDigest,
    },
    ReviewDiffChunks {
        generation: u64,
        workspace_digest: WorkspaceDigest,
        manifest_sha256: String,
        start_chunk: u8,
        count: u8,
    },
    /// Reviewer coverage convergence may either read the exact next chunk
    /// range or terminate early with `changes_requested`/`report_blocked`.
    ReviewDiffChunksOrTerminal {
        generation: u64,
        workspace_digest: WorkspaceDigest,
        manifest_sha256: String,
        start_chunk: u8,
        count: u8,
    },
    Terminal(ControlKind),
    /// A convergence-only terminal pair. The provider must return exactly one
    /// normal terminal or `report_blocked`; no runtime action is accepted.
    TerminalOrBlocked(ControlKind),
}

impl RequiredAction {
    pub fn terminal(kind: ControlKind) -> Result<Self, RoleContractError> {
        if !kind.is_terminal() {
            return Err(RoleContractError::InvalidRequiredAction);
        }
        Ok(Self::Terminal(kind))
    }

    pub fn terminal_or_blocked(normal_terminal: ControlKind) -> Result<Self, RoleContractError> {
        if !matches!(
            normal_terminal,
            ControlKind::SubmitPlan | ControlKind::SubmitExecution | ControlKind::SubmitReview
        ) {
            return Err(RoleContractError::InvalidRequiredAction);
        }
        Ok(Self::TerminalOrBlocked(normal_terminal))
    }

    pub fn review_diff_manifest(
        generation: u64,
        workspace_digest: WorkspaceDigest,
    ) -> Result<Self, RoleContractError> {
        if generation > MAX_WORKSPACE_GENERATION {
            return Err(RoleContractError::InvalidRequiredAction);
        }
        Ok(Self::ReviewDiffManifest {
            generation,
            workspace_digest,
        })
    }

    pub fn review_diff_manifest_or_terminal(
        generation: u64,
        workspace_digest: WorkspaceDigest,
    ) -> Result<Self, RoleContractError> {
        if generation > MAX_WORKSPACE_GENERATION {
            return Err(RoleContractError::InvalidRequiredAction);
        }
        Ok(Self::ReviewDiffManifestOrTerminal {
            generation,
            workspace_digest,
        })
    }

    pub fn review_diff_chunks(
        generation: u64,
        workspace_digest: WorkspaceDigest,
        manifest_sha256: String,
        start_chunk: u8,
        count: u8,
    ) -> Result<Self, RoleContractError> {
        if generation > MAX_WORKSPACE_GENERATION
            || !is_lower_hex_64(&manifest_sha256)
            || count == 0
            || count > MAX_REVIEW_DIFF_BATCH_CHUNKS
            || start_chunk
                .checked_add(count)
                .is_none_or(|end| end > MAX_REVIEW_DIFF_CHUNKS)
        {
            return Err(RoleContractError::InvalidRequiredAction);
        }
        Ok(Self::ReviewDiffChunks {
            generation,
            workspace_digest,
            manifest_sha256,
            start_chunk,
            count,
        })
    }

    pub fn review_diff_chunks_or_terminal(
        generation: u64,
        workspace_digest: WorkspaceDigest,
        manifest_sha256: String,
        start_chunk: u8,
        count: u8,
    ) -> Result<Self, RoleContractError> {
        if generation > MAX_WORKSPACE_GENERATION
            || !is_lower_hex_64(&manifest_sha256)
            || count == 0
            || count > MAX_REVIEW_DIFF_BATCH_CHUNKS
            || start_chunk
                .checked_add(count)
                .is_none_or(|end| end > MAX_REVIEW_DIFF_CHUNKS)
        {
            return Err(RoleContractError::InvalidRequiredAction);
        }
        Ok(Self::ReviewDiffChunksOrTerminal {
            generation,
            workspace_digest,
            manifest_sha256,
            start_chunk,
            count,
        })
    }

    pub fn action_name(&self) -> &'static str {
        match self {
            Self::LegacyCargoTest => "cargo_test",
            Self::Validation(check) => kind_name(check.selector().kind()),
            Self::ReviewDiffManifest { .. } | Self::ReviewDiffManifestOrTerminal { .. } => {
                "review_diff_manifest"
            }
            Self::ReviewDiffChunks { .. } | Self::ReviewDiffChunksOrTerminal { .. } => {
                "review_diff_chunks"
            }
            Self::Terminal(kind) | Self::TerminalOrBlocked(kind) => kind.name(),
        }
    }

    pub fn validate(&self) -> Result<(), RoleContractError> {
        match self {
            Self::LegacyCargoTest | Self::Validation(_) => Ok(()),
            Self::ReviewDiffManifest { generation, .. }
            | Self::ReviewDiffManifestOrTerminal { generation, .. }
                if *generation <= MAX_WORKSPACE_GENERATION =>
            {
                Ok(())
            }
            Self::ReviewDiffChunks {
                generation,
                manifest_sha256,
                start_chunk,
                count,
                ..
            }
            | Self::ReviewDiffChunksOrTerminal {
                generation,
                manifest_sha256,
                start_chunk,
                count,
                ..
            } if *generation <= MAX_WORKSPACE_GENERATION
                && is_lower_hex_64(manifest_sha256)
                && *count != 0
                && *count <= MAX_REVIEW_DIFF_BATCH_CHUNKS
                && start_chunk
                    .checked_add(*count)
                    .is_some_and(|end| end <= MAX_REVIEW_DIFF_CHUNKS) =>
            {
                Ok(())
            }
            Self::Terminal(kind) if kind.is_terminal() => Ok(()),
            Self::TerminalOrBlocked(
                ControlKind::SubmitPlan | ControlKind::SubmitExecution | ControlKind::SubmitReview,
            ) => Ok(()),
            _ => Err(RoleContractError::InvalidRequiredAction),
        }
    }

    pub fn matches(&self, request: &ActionRequest) -> bool {
        match (self, request) {
            (
                Self::LegacyCargoTest,
                ActionRequest::Runtime(RuntimeActionRequest::Tool(ToolRequest::CargoTest {
                    ..
                })),
            ) => true,
            (
                Self::Validation(expected),
                ActionRequest::Runtime(RuntimeActionRequest::Validation { check }),
            ) => expected == check,
            (
                Self::ReviewDiffManifest {
                    generation,
                    workspace_digest,
                },
                ActionRequest::Runtime(RuntimeActionRequest::ReviewDiffManifest {
                    generation: actual_generation,
                    workspace_digest: actual_digest,
                }),
            ) => generation == actual_generation && workspace_digest == actual_digest,
            (
                Self::ReviewDiffManifestOrTerminal {
                    generation,
                    workspace_digest,
                },
                ActionRequest::Runtime(RuntimeActionRequest::ReviewDiffManifest {
                    generation: actual_generation,
                    workspace_digest: actual_digest,
                }),
            ) => generation == actual_generation && workspace_digest == actual_digest,
            (Self::ReviewDiffManifestOrTerminal { .. }, ActionRequest::Control(control)) => {
                matches!(
                    control,
                    ControlRequest::SubmitReview(submission)
                        if submission.verdict() == ReviewVerdict::ChangesRequested
                ) || matches!(control, ControlRequest::ReportBlocked(_))
            }
            (
                Self::ReviewDiffChunks {
                    generation,
                    workspace_digest,
                    manifest_sha256,
                    start_chunk,
                    count,
                },
                ActionRequest::Runtime(RuntimeActionRequest::ReviewDiffChunks {
                    generation: actual_generation,
                    workspace_digest: actual_digest,
                    manifest_sha256: actual_manifest,
                    start_chunk: actual_start,
                    count: actual_count,
                }),
            ) => {
                generation == actual_generation
                    && workspace_digest == actual_digest
                    && manifest_sha256 == actual_manifest
                    && start_chunk == actual_start
                    && count == actual_count
            }
            (
                Self::ReviewDiffChunksOrTerminal {
                    generation,
                    workspace_digest,
                    manifest_sha256,
                    start_chunk,
                    count,
                },
                ActionRequest::Runtime(RuntimeActionRequest::ReviewDiffChunks {
                    generation: actual_generation,
                    workspace_digest: actual_digest,
                    manifest_sha256: actual_manifest,
                    start_chunk: actual_start,
                    count: actual_count,
                }),
            ) => {
                generation == actual_generation
                    && workspace_digest == actual_digest
                    && manifest_sha256 == actual_manifest
                    && start_chunk == actual_start
                    && count == actual_count
            }
            (Self::ReviewDiffChunksOrTerminal { .. }, ActionRequest::Control(control)) => {
                matches!(
                    control,
                    ControlRequest::SubmitReview(submission)
                        if submission.verdict() == ReviewVerdict::ChangesRequested
                ) || matches!(control, ControlRequest::ReportBlocked(_))
            }
            (Self::Terminal(expected), ActionRequest::Control(control)) => {
                *expected == control.kind() && expected.is_terminal()
            }
            (Self::TerminalOrBlocked(expected), ActionRequest::Control(control)) => {
                (*expected == control.kind() || control.kind() == ControlKind::ReportBlocked)
                    && matches!(
                        expected,
                        ControlKind::SubmitPlan
                            | ControlKind::SubmitExecution
                            | ControlKind::SubmitReview
                    )
            }
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RoleContractError {
    #[error("the action is not allowed for this role")]
    ActionNotAllowed,
    #[error("the action payload is invalid")]
    InvalidPayload,
    #[error("the control/runtime capability boundary was violated")]
    ControlRuntimeBoundary,
    #[error("the action batch is invalid")]
    InvalidBatch,
    #[error("the action payload changed under redaction")]
    RedactionMutation,
    #[error("the required action is invalid")]
    InvalidRequiredAction,
    #[error("the required action did not match exactly")]
    RequiredActionMismatch,
}

/// Pure, side-effect-free whole-batch preflight. Callers dispatch only after
/// this succeeds, which gives mixed/invalid batches zero execution.
pub fn validate_action_batch(
    role: Role,
    batch: &ToolCallBatch,
    choice: &ModelToolChoice,
    redactor: &dyn ContextRedactor,
) -> Result<(), RoleContractError> {
    if batch.calls.is_empty() {
        return Err(RoleContractError::InvalidBatch);
    }
    if batch
        .assistant_content
        .as_deref()
        .is_some_and(|content| redactor.redact(content) != content)
        || batch
            .reasoning_content
            .as_deref()
            .is_some_and(|content| redactor.redact(content) != content)
    {
        return Err(RoleContractError::RedactionMutation);
    }
    let allowed = AllowedActions::for_role(role);
    let mut ids = BTreeSet::new();
    for call in &batch.calls {
        if call.id.is_empty()
            || call.id.len() > MAX_TOOL_CALL_ID_BYTES
            || call.id.chars().any(char::is_control)
            || !ids.insert(call.id.as_str())
            || !allowed.allows_name(call.request.name())
            || !role_allows_request_shape(role, &call.request)
            || call.request.validate().is_err()
        {
            return Err(RoleContractError::InvalidBatch);
        }
        if redactor.redact(&call.id) != call.id || !call.request.is_redaction_stable(redactor) {
            return Err(RoleContractError::RedactionMutation);
        }
    }
    if batch
        .calls
        .iter()
        .any(|call| matches!(call.request, ActionRequest::Control(_)))
        && batch.calls.len() != 1
    {
        return Err(RoleContractError::InvalidBatch);
    }
    match choice {
        ModelToolChoice::Required(required) => {
            if required.validate().is_err()
                || batch.calls.len() != 1
                || !required.matches(&batch.calls[0].request)
            {
                return Err(RoleContractError::RequiredActionMismatch);
            }
        }
        ModelToolChoice::RequiredCargoTest => {
            if batch.calls.len() != 1
                || !RequiredAction::LegacyCargoTest.matches(&batch.calls[0].request)
            {
                return Err(RoleContractError::RequiredActionMismatch);
            }
        }
        ModelToolChoice::Auto => {
            if batch.calls.iter().any(|call| {
                matches!(
                    call.request,
                    ActionRequest::Runtime(
                        RuntimeActionRequest::Validation { .. }
                            | RuntimeActionRequest::ReviewDiffManifest { .. }
                            | RuntimeActionRequest::ReviewDiffChunks { .. }
                    )
                )
            }) {
                return Err(RoleContractError::RequiredActionMismatch);
            }
        }
        ModelToolChoice::None => return Err(RoleContractError::InvalidBatch),
    }
    Ok(())
}

/// Role runs never terminate with ordinary assistant text. This common
/// response gate runs before any action dispatch.
pub fn validate_role_response(
    role: Role,
    response: &ModelResponse,
    choice: &ModelToolChoice,
    redactor: &dyn ContextRedactor,
) -> Result<(), RoleContractError> {
    match response {
        ModelResponse::ToolCalls(batch) => validate_action_batch(role, batch, choice, redactor),
        ModelResponse::Final { .. } => Err(RoleContractError::InvalidBatch),
    }
}

fn role_allows_request_shape(role: Role, request: &ActionRequest) -> bool {
    match request {
        ActionRequest::Control(control) => match role {
            Role::Planner => matches!(
                control,
                ControlRequest::SubmitPlan(_) | ControlRequest::ReportBlocked(_)
            ),
            Role::Executor => matches!(
                control,
                ControlRequest::SubmitExecution(_)
                    | ControlRequest::ReportBlocked(_)
                    | ControlRequest::UpdatePlanProgress(_)
            ),
            Role::Reviewer => matches!(
                control,
                ControlRequest::SubmitReview(_) | ControlRequest::ReportBlocked(_)
            ),
        },
        ActionRequest::Runtime(RuntimeActionRequest::Tool(tool)) => match role {
            Role::Planner => matches!(
                tool,
                ToolRequest::ListFiles { .. }
                    | ToolRequest::ReadFile { .. }
                    | ToolRequest::SearchText { .. }
            ),
            Role::Executor => matches!(
                tool,
                ToolRequest::ListFiles { .. }
                    | ToolRequest::ReadFile { .. }
                    | ToolRequest::SearchText { .. }
                    | ToolRequest::ReplaceFile { .. }
                    | ToolRequest::GitStatus
                    | ToolRequest::GitDiff
            ),
            Role::Reviewer => matches!(
                tool,
                ToolRequest::ListFiles { .. }
                    | ToolRequest::ReadFile { .. }
                    | ToolRequest::SearchText { .. }
                    | ToolRequest::GitStatus
                    | ToolRequest::GitDiff
            ),
        },
        ActionRequest::Runtime(
            RuntimeActionRequest::Validation { .. }
            | RuntimeActionRequest::ValidationSelector { .. },
        ) => matches!(role, Role::Executor | Role::Reviewer),
        ActionRequest::Runtime(
            RuntimeActionRequest::ReviewDiffManifest { .. }
            | RuntimeActionRequest::ReviewDiffChunks { .. },
        ) => role == Role::Reviewer,
    }
}

fn decode_action(
    allowed: &AllowedActions,
    name: &str,
    arguments: &str,
) -> Result<ActionRequest, RoleContractError> {
    if !allowed.allows_name(name) {
        return Err(RoleContractError::ActionNotAllowed);
    }
    let action = match name {
        "list_files" => {
            let value: ListFilesArguments = parse(arguments)?;
            Ok(ActionRequest::runtime(ToolRequest::ListFiles {
                path: value.path,
                depth: value.depth,
                limit: value.limit,
            }))
        }
        "read_file" => {
            let value: ReadFileArguments = parse(arguments)?;
            Ok(ActionRequest::runtime(ToolRequest::ReadFile {
                path: value.path,
                start_line: value.start_line,
                end_line: value.end_line,
            }))
        }
        "search_text" => {
            let value: SearchTextArguments = parse(arguments)?;
            Ok(ActionRequest::runtime(ToolRequest::SearchText {
                query: value.query,
                path: value.path,
                glob: value.glob,
                limit: value.limit,
            }))
        }
        "replace_file" => {
            let value: ReplaceFileArguments = parse(arguments)?;
            Ok(ActionRequest::runtime(ToolRequest::ReplaceFile {
                path: value.path,
                expected_sha256: value.expected_sha256,
                content: value.content,
            }))
        }
        "cargo_check" if allowed.is_legacy() => {
            let value: LegacyCargoCheckArguments = parse(arguments)?;
            Ok(ActionRequest::runtime(ToolRequest::CargoCheck {
                package: value.package,
                timeout_ms: value.timeout_ms,
            }))
        }
        "cargo_test" if allowed.is_legacy() => {
            let value: LegacyCargoTestArguments = parse(arguments)?;
            Ok(ActionRequest::runtime(ToolRequest::CargoTest {
                package: value.package,
                test: value.test,
                timeout_ms: value.timeout_ms,
            }))
        }
        "cargo_check" => decode_cargo_check(arguments),
        "cargo_test" => decode_cargo_test(arguments),
        "git_status" | "git_diff" => {
            let _: EmptyArguments = parse(arguments)?;
            Ok(ActionRequest::runtime(if name == "git_status" {
                ToolRequest::GitStatus
            } else {
                ToolRequest::GitDiff
            }))
        }
        "review_diff_manifest" => {
            let value: ReviewDiffManifestArguments = parse(arguments)?;
            RequiredAction::review_diff_manifest(value.generation, value.workspace_digest.clone())?;
            Ok(ActionRequest::Runtime(
                RuntimeActionRequest::ReviewDiffManifest {
                    generation: value.generation,
                    workspace_digest: value.workspace_digest,
                },
            ))
        }
        "review_diff_chunks" => {
            let value: ReviewDiffChunksArguments = parse(arguments)?;
            RequiredAction::review_diff_chunks(
                value.generation,
                value.workspace_digest.clone(),
                value.manifest_sha256.clone(),
                value.start_chunk,
                value.count,
            )?;
            Ok(ActionRequest::Runtime(
                RuntimeActionRequest::ReviewDiffChunks {
                    generation: value.generation,
                    workspace_digest: value.workspace_digest,
                    manifest_sha256: value.manifest_sha256,
                    start_chunk: value.start_chunk,
                    count: value.count,
                },
            ))
        }
        "submit_plan" => {
            let value: PlanSubmission = parse(arguments)?;
            value.validate()?;
            Ok(ActionRequest::Control(ControlRequest::SubmitPlan(value)))
        }
        "submit_execution" => {
            let value: ExecutionSubmission = parse(arguments)?;
            value.validate()?;
            Ok(ActionRequest::Control(ControlRequest::SubmitExecution(
                value,
            )))
        }
        "submit_review" => {
            let value: ReviewSubmission = parse(arguments)?;
            value.validate()?;
            Ok(ActionRequest::Control(ControlRequest::SubmitReview(value)))
        }
        "report_blocked" => {
            let value: BlockedSubmission = parse(arguments)?;
            value.validate()?;
            Ok(ActionRequest::Control(ControlRequest::ReportBlocked(value)))
        }
        "update_plan_progress" => {
            let value: PlanProgressSubmission = parse(arguments)?;
            value.validate()?;
            Ok(ActionRequest::Control(ControlRequest::UpdatePlanProgress(
                value,
            )))
        }
        _ => Err(RoleContractError::ActionNotAllowed),
    }?;
    action.validate()?;
    Ok(action)
}

fn decode_cargo_check(arguments: &str) -> Result<ActionRequest, RoleContractError> {
    if let Ok(value) = serde_json::from_str::<ExactCargoCheckArguments>(arguments) {
        let check = RequiredCheck::try_cargo_check(value.check_id, value.package)
            .map_err(|_| RoleContractError::InvalidPayload)?;
        return Ok(ActionRequest::Runtime(RuntimeActionRequest::Validation {
            check,
        }));
    }
    let value: CargoCheckArguments = parse(arguments)?;
    let selector = RequiredCheckSelector::try_cargo_check(value.package)
        .map_err(|_| RoleContractError::InvalidPayload)?;
    Ok(ActionRequest::Runtime(
        RuntimeActionRequest::ValidationSelector { selector },
    ))
}

fn decode_cargo_test(arguments: &str) -> Result<ActionRequest, RoleContractError> {
    if let Ok(value) = serde_json::from_str::<ExactCargoTestArguments>(arguments) {
        let check =
            RequiredCheck::try_cargo_test(value.check_id, value.package, value.integration_test)
                .map_err(|_| RoleContractError::InvalidPayload)?;
        return Ok(ActionRequest::Runtime(RuntimeActionRequest::Validation {
            check,
        }));
    }
    let value: CargoTestArguments = parse(arguments)?;
    let selector = RequiredCheckSelector::try_cargo_test(value.package, value.integration_test)
        .map_err(|_| RoleContractError::InvalidPayload)?;
    Ok(ActionRequest::Runtime(
        RuntimeActionRequest::ValidationSelector { selector },
    ))
}

fn parse<T: DeserializeOwned>(arguments: &str) -> Result<T, RoleContractError> {
    serde_json::from_str(arguments).map_err(|_| RoleContractError::InvalidPayload)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListFilesArguments {
    path: String,
    depth: u32,
    limit: usize,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadFileArguments {
    path: String,
    start_line: u64,
    end_line: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchTextArguments {
    query: String,
    path: String,
    glob: Option<String>,
    limit: usize,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplaceFileArguments {
    path: String,
    expected_sha256: Option<String>,
    content: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyCargoCheckArguments {
    package: Option<String>,
    timeout_ms: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyCargoTestArguments {
    package: Option<String>,
    test: Option<String>,
    timeout_ms: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CargoCheckArguments {
    package: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExactCargoCheckArguments {
    check_id: String,
    package: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CargoTestArguments {
    package: Option<String>,
    integration_test: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExactCargoTestArguments {
    check_id: String,
    package: Option<String>,
    integration_test: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewDiffManifestArguments {
    generation: u64,
    workspace_digest: WorkspaceDigest,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewDiffChunksArguments {
    generation: u64,
    workspace_digest: WorkspaceDigest,
    manifest_sha256: String,
    start_chunk: u8,
    count: u8,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EmptyArguments {}

fn validate_control_size<T: Serialize>(value: &T) -> Result<(), RoleContractError> {
    if serde_json::to_vec(value)
        .map_err(|_| RoleContractError::InvalidPayload)?
        .len()
        > MAX_CONTROL_JSON_BYTES
    {
        return Err(RoleContractError::InvalidPayload);
    }
    Ok(())
}

fn selector_key(selector: &RequiredCheckSelector) -> String {
    format!(
        "{:?}\0{}\0{}",
        selector.kind(),
        selector.package().unwrap_or_default(),
        selector.integration_test().unwrap_or_default()
    )
}

fn kind_name(kind: RequiredCheckKind) -> &'static str {
    match kind {
        RequiredCheckKind::CargoCheck => "cargo_check",
        RequiredCheckKind::CargoTest => "cargo_test",
    }
}

fn is_lower_hex_64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn tool_request_name(request: &ToolRequest) -> &'static str {
    match request {
        ToolRequest::ListFiles { .. } => "list_files",
        ToolRequest::ReadFile { .. } => "read_file",
        ToolRequest::SearchText { .. } => "search_text",
        ToolRequest::ReplaceFile { .. } => "replace_file",
        ToolRequest::CargoCheck { .. } => "cargo_check",
        ToolRequest::CargoTest { .. } => "cargo_test",
        ToolRequest::GitStatus => "git_status",
        ToolRequest::GitDiff => "git_diff",
    }
}

fn tool_request_arguments(request: &ToolRequest) -> serde_json::Value {
    use serde_json::json;
    match request {
        ToolRequest::ListFiles { path, depth, limit } => {
            json!({"path": path, "depth": depth, "limit": limit})
        }
        ToolRequest::ReadFile {
            path,
            start_line,
            end_line,
        } => json!({"path": path, "start_line": start_line, "end_line": end_line}),
        ToolRequest::SearchText {
            query,
            path,
            glob,
            limit,
        } => json!({"query": query, "path": path, "glob": glob, "limit": limit}),
        ToolRequest::ReplaceFile {
            path,
            expected_sha256,
            content,
        } => json!({
            "path": path,
            "expected_sha256": expected_sha256,
            "content": content
        }),
        ToolRequest::CargoCheck {
            package,
            timeout_ms,
        } => json!({"package": package, "timeout_ms": timeout_ms}),
        ToolRequest::CargoTest {
            package,
            test,
            timeout_ms,
        } => json!({"package": package, "test": test, "timeout_ms": timeout_ms}),
        ToolRequest::GitStatus | ToolRequest::GitDiff => json!({}),
    }
}

fn visit_tool_request_strings(request: &ToolRequest, visitor: &mut dyn FnMut(&str)) {
    match request {
        ToolRequest::ListFiles { path, .. } | ToolRequest::ReadFile { path, .. } => visitor(path),
        ToolRequest::SearchText {
            query, path, glob, ..
        } => {
            visitor(query);
            visitor(path);
            visit_option(glob.as_deref(), visitor);
        }
        ToolRequest::ReplaceFile {
            path,
            expected_sha256,
            content,
        } => {
            visitor(path);
            visit_option(expected_sha256.as_deref(), visitor);
            visitor(content);
        }
        ToolRequest::CargoCheck { package, .. } => visit_option(package.as_deref(), visitor),
        ToolRequest::CargoTest { package, test, .. } => {
            visit_option(package.as_deref(), visitor);
            visit_option(test.as_deref(), visitor);
        }
        ToolRequest::GitStatus | ToolRequest::GitDiff => {}
    }
}

fn visit_selector_strings(selector: &RequiredCheckSelector, visitor: &mut dyn FnMut(&str)) {
    visit_option(selector.package(), visitor);
    visit_option(selector.integration_test(), visitor);
}

fn visit_option(value: Option<&str>, visitor: &mut dyn FnMut(&str)) {
    if let Some(value) = value {
        visitor(value);
    }
}
