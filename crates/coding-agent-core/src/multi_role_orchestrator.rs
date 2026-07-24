use std::sync::Arc;

use coding_agent_domain::{
    DeliveryReadiness, NewReviewEvidence, RequiredCheckSelector, ReviewVerdict, TaskFailure,
    TaskStatus,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::{
    BlockedReason, BlockedSubmission, DurableRoleEvent, ExecutorRoleInput, ExecutorRoleLoop,
    ExecutorRoleOutcome, PlannerRoleInput, PlannerRoleLoop, PlannerRoleOutcome,
    RequiredCheckLedger, ReviewDiffCheckpoint, ReviewerRoleInput, ReviewerRoleLoop,
    ReviewerRoleOutcome, Role, RoleEngine, RoleLoopError, RoleRun, RuntimeError, TaskBudgetLedger,
    ValidatedReviewDecision, WorkspaceCheckpoint, WorkspaceDigest, WorkspaceFingerprint,
};

const REVIEW_REJECTED: &str = "REVIEW_REJECTED";
const QUALITY_EVIDENCE_MISMATCH: &str = "QUALITY_EVIDENCE_MISMATCH";
const QUALITY_EVIDENCE_STORE_FAILED: &str = "QUALITY_EVIDENCE_STORE_FAILED";

pub struct MultiRoleInput<'a> {
    pub task_prompt: &'a str,
    pub repository_context: &'a str,
    pub checkpoint: WorkspaceCheckpoint,
    pub repository_check_catalog: &'a [RequiredCheckSelector],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultiRoleFailure {
    role_run: RoleRun,
    failure: TaskFailure,
}

impl MultiRoleFailure {
    pub const fn role_run(&self) -> RoleRun {
        self.role_run
    }

    pub const fn status(&self) -> TaskStatus {
        TaskStatus::Failed
    }

    pub const fn delivery_readiness(&self) -> DeliveryReadiness {
        DeliveryReadiness::Unreviewed
    }

    pub const fn failure(&self) -> &TaskFailure {
        &self.failure
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultiRoleOutcome {
    Approved(ValidatedReviewDecision),
    Rejected {
        decision: ValidatedReviewDecision,
        failure: TaskFailure,
    },
    Failed(MultiRoleFailure),
    Cancelled,
}

impl MultiRoleOutcome {
    pub const fn status(&self) -> TaskStatus {
        match self {
            Self::Approved(_) => TaskStatus::Completed,
            Self::Rejected { .. } | Self::Failed(_) => TaskStatus::Failed,
            Self::Cancelled => TaskStatus::Cancelled,
        }
    }

    pub const fn delivery_readiness(&self) -> DeliveryReadiness {
        match self {
            Self::Approved(_) => DeliveryReadiness::ReviewApproved,
            Self::Rejected { .. } => DeliveryReadiness::ReviewRejected,
            Self::Failed(_) | Self::Cancelled => DeliveryReadiness::Unreviewed,
        }
    }

    pub const fn failure(&self) -> Option<&TaskFailure> {
        match self {
            Self::Rejected { failure, .. } => Some(failure),
            Self::Failed(failure) => Some(failure.failure()),
            Self::Approved(_) | Self::Cancelled => None,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct MultiRoleRunReport {
    outcome: MultiRoleOutcome,
    final_checkpoint: WorkspaceCheckpoint,
    required_checks: Option<RequiredCheckLedger>,
}

impl MultiRoleRunReport {
    fn from_parts(
        mut outcome: MultiRoleOutcome,
        final_checkpoint: WorkspaceCheckpoint,
        required_checks: Option<RequiredCheckLedger>,
    ) -> Self {
        let final_decision = match &outcome {
            MultiRoleOutcome::Approved(decision) | MultiRoleOutcome::Rejected { decision, .. } => {
                Some(decision)
            }
            MultiRoleOutcome::Failed(_) | MultiRoleOutcome::Cancelled => None,
        };
        if let Some(decision) = final_decision
            && (decision.evidence().workspace_generation() != final_checkpoint.generation()
                || decision.evidence().workspace_digest() != &final_checkpoint.workspace_digest())
        {
            let reviewer_run =
                RoleRun::try_new(Role::Reviewer, u32::from(decision.evidence().round()))
                    .expect("validated review evidence always has a valid review round");
            outcome = fixed_failure(reviewer_run, QUALITY_EVIDENCE_MISMATCH, false);
        }
        Self {
            outcome,
            final_checkpoint,
            required_checks,
        }
    }

    pub const fn outcome(&self) -> &MultiRoleOutcome {
        &self.outcome
    }

    pub const fn final_checkpoint(&self) -> &WorkspaceCheckpoint {
        &self.final_checkpoint
    }

    pub const fn required_checks(&self) -> Option<&RequiredCheckLedger> {
        self.required_checks.as_ref()
    }

    pub const fn final_workspace_generation(&self) -> u64 {
        self.final_checkpoint.generation()
    }

    pub fn final_workspace_digest(&self) -> WorkspaceDigest {
        self.final_checkpoint.workspace_digest()
    }

    pub const fn status(&self) -> TaskStatus {
        self.outcome.status()
    }

    pub const fn delivery_readiness(&self) -> DeliveryReadiness {
        self.outcome.delivery_readiness()
    }

    pub const fn failure(&self) -> Option<&TaskFailure> {
        self.outcome.failure()
    }

    pub fn into_parts(
        self,
    ) -> (
        MultiRoleOutcome,
        WorkspaceCheckpoint,
        Option<RequiredCheckLedger>,
    ) {
        (self.outcome, self.final_checkpoint, self.required_checks)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FinalizationGuardError {
    #[error("the attempt worktree identity no longer matches")]
    IdentityMismatch,
    #[error("the deliverable workspace no longer matches the reviewed fingerprint")]
    WorkspaceMismatch,
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
}

/// Trusted app-provided proof that final evidence still belongs to both the
/// provisioned attempt/worktree identity and the exact reviewed workspace.
///
/// There is deliberately no default or no-op implementation.
#[async_trait::async_trait]
pub trait FinalizationGuard: Send + Sync + 'static {
    async fn verify_finalization(
        &self,
        expected_fingerprint: WorkspaceFingerprint,
        cancellation: CancellationToken,
    ) -> Result<(), FinalizationGuardError>;
}

/// Creates one capability-scoped engine for each concrete role run.
///
/// Implementations must retain the same task-owned prepared-provider session,
/// event sink, redactor, and underlying runtime session across every call.
/// Planner and Executor calls receive no review checkpoint; Reviewer calls
/// receive the complete current checkpoint authority.
pub trait RoleEngineFactory: Send + Sync + 'static {
    fn create_engine(
        &self,
        role_run: RoleRun,
        review_checkpoint: Option<ReviewDiffCheckpoint>,
    ) -> Result<RoleEngine, RuntimeError>;
}

/// One-task deterministic Planner -> Executor -> Reviewer orchestrator.
///
/// `run` consumes the orchestrator so its provider task session and shared
/// budget ledger cannot accidentally be reused for a second Task.
pub struct MultiRoleOrchestrator {
    engine_factory: Arc<dyn RoleEngineFactory>,
    finalization_guard: Arc<dyn FinalizationGuard>,
}

impl MultiRoleOrchestrator {
    pub fn new(
        engine_factory: Arc<dyn RoleEngineFactory>,
        finalization_guard: Arc<dyn FinalizationGuard>,
    ) -> Self {
        Self {
            engine_factory,
            finalization_guard,
        }
    }

    pub async fn run(
        self,
        input: MultiRoleInput<'_>,
        cancellation: CancellationToken,
    ) -> MultiRoleRunReport {
        let MultiRoleInput {
            task_prompt,
            repository_context,
            checkpoint: mut final_checkpoint,
            repository_check_catalog,
        } = input;
        let mut required_checks = None;
        let outcome = self
            .run_inner(
                task_prompt,
                repository_context,
                repository_check_catalog,
                &mut final_checkpoint,
                &mut required_checks,
                cancellation,
            )
            .await;
        MultiRoleRunReport::from_parts(outcome, final_checkpoint, required_checks)
    }

    async fn run_inner(
        &self,
        task_prompt: &str,
        repository_context: &str,
        repository_check_catalog: &[RequiredCheckSelector],
        checkpoint: &mut WorkspaceCheckpoint,
        report_required_checks: &mut Option<RequiredCheckLedger>,
        cancellation: CancellationToken,
    ) -> MultiRoleOutcome {
        let planner_run = RoleRun::try_new(Role::Planner, 1)
            .expect("Planner round one is always a valid role run");
        if cancellation.is_cancelled() {
            return MultiRoleOutcome::Cancelled;
        }

        let mut budget = match TaskBudgetLedger::try_new() {
            Ok(budget) => budget,
            Err(error) => {
                return failed_from_role_error(planner_run, RoleLoopError::Budget(error));
            }
        };
        let planner_engine_result = self.engine_factory.create_engine(planner_run, None);
        if cancellation.is_cancelled() {
            return MultiRoleOutcome::Cancelled;
        }
        let planner_engine = match planner_engine_result {
            Ok(engine) => engine,
            Err(error) => return failed_from_factory_error(planner_run, error),
        };
        let planner = PlannerRoleLoop::from_engine(planner_engine);

        let planner_result = planner
            .run(
                PlannerRoleInput {
                    task_prompt,
                    repository_context,
                    checkpoint,
                    repository_check_catalog,
                },
                &mut budget,
                cancellation.clone(),
            )
            .await;
        if cancellation.is_cancelled() {
            return MultiRoleOutcome::Cancelled;
        }
        let submitted_plan = match planner_result {
            Ok(PlannerRoleOutcome::Submitted(plan)) => plan,
            Ok(PlannerRoleOutcome::Blocked(blocked)) => {
                return MultiRoleOutcome::Failed(blocked_failure(planner_run, &blocked));
            }
            Err(RoleLoopError::Cancelled) => return MultiRoleOutcome::Cancelled,
            Err(error) => return failed_from_role_error(planner_run, error),
        };
        if cancellation.is_cancelled() {
            return MultiRoleOutcome::Cancelled;
        }

        let mut plan = submitted_plan.plan().clone();
        let required_checks =
            match RequiredCheckLedger::try_new(submitted_plan.required_checks().to_vec()) {
                Ok(required_checks) => required_checks,
                Err(error) => {
                    return failed_from_role_error(planner_run, RoleLoopError::Quality(error));
                }
            };
        *report_required_checks = Some(required_checks);
        let required_checks = report_required_checks
            .as_mut()
            .expect("required checks were just installed");
        let mut previous_reviews = Vec::<NewReviewEvidence>::new();

        for round in 1..=crate::MAX_REVIEW_ROUNDS {
            let executor_run = match RoleRun::try_new(Role::Executor, round) {
                Ok(role_run) => role_run,
                Err(error) => {
                    return failed_from_role_error(planner_run, RoleLoopError::Budget(error));
                }
            };
            if cancellation.is_cancelled() {
                return MultiRoleOutcome::Cancelled;
            }
            let executor_engine_result = self.engine_factory.create_engine(executor_run, None);
            if cancellation.is_cancelled() {
                return MultiRoleOutcome::Cancelled;
            }
            let executor_engine = match executor_engine_result {
                Ok(engine) => engine,
                Err(error) => return failed_from_factory_error(executor_run, error),
            };
            let executor = ExecutorRoleLoop::from_engine(executor_engine);
            let executor_result = executor
                .run(
                    ExecutorRoleInput {
                        review_round: round,
                        task_prompt,
                        repository_context,
                        plan: &mut plan,
                        checkpoint,
                        required_checks,
                        latest_reviewer_findings: previous_reviews
                            .last()
                            .map(NewReviewEvidence::findings),
                    },
                    &mut budget,
                    cancellation.clone(),
                )
                .await;
            if cancellation.is_cancelled() {
                return cancel_after_executor(&mut budget, round);
            }
            let execution = match executor_result {
                Ok(ExecutorRoleOutcome::Submitted(execution)) => execution,
                Ok(ExecutorRoleOutcome::Blocked(blocked)) => {
                    abandon_exact_pending_reviewer(&mut budget, round);
                    return MultiRoleOutcome::Failed(MultiRoleFailure {
                        role_run: executor_run,
                        failure: blocked.stage_failure().failure().clone(),
                    });
                }
                Err(RoleLoopError::Cancelled) => {
                    abandon_exact_pending_reviewer(&mut budget, round);
                    return MultiRoleOutcome::Cancelled;
                }
                Err(error) => {
                    abandon_exact_pending_reviewer(&mut budget, round);
                    return failed_from_role_error(executor_run, error);
                }
            };

            let reviewer_run = match RoleRun::try_new(Role::Reviewer, round) {
                Ok(role_run) => role_run,
                Err(error) => {
                    abandon_exact_pending_reviewer(&mut budget, round);
                    return failed_from_role_error(executor_run, RoleLoopError::Budget(error));
                }
            };
            if cancellation.is_cancelled() {
                return cancel_after_executor(&mut budget, round);
            }
            let review_checkpoint = ReviewDiffCheckpoint::from_workspace_checkpoint(checkpoint);
            let reviewer_engine_result = self
                .engine_factory
                .create_engine(reviewer_run, Some(review_checkpoint));
            if cancellation.is_cancelled() {
                abandon_exact_pending_reviewer(&mut budget, round);
                return MultiRoleOutcome::Cancelled;
            }
            let reviewer_engine = match reviewer_engine_result {
                Ok(engine) => engine,
                Err(error) => {
                    abandon_exact_pending_reviewer(&mut budget, round);
                    return failed_from_factory_error(reviewer_run, error);
                }
            };
            let reviewer = ReviewerRoleLoop::from_engine(reviewer_engine.clone());
            let reviewer_result = reviewer
                .run(
                    ReviewerRoleInput {
                        review_round: round,
                        task_prompt,
                        repository_context,
                        plan: &plan,
                        execution: &execution,
                        checkpoint,
                        required_checks,
                        previous_reviews: &previous_reviews,
                    },
                    &mut budget,
                    cancellation.clone(),
                )
                .await;
            abandon_exact_pending_reviewer(&mut budget, round);
            if cancellation.is_cancelled() {
                return MultiRoleOutcome::Cancelled;
            }
            let decision = match reviewer_result {
                Ok(ReviewerRoleOutcome::Decided(decision)) => decision,
                Ok(ReviewerRoleOutcome::Blocked(blocked)) => {
                    return MultiRoleOutcome::Failed(MultiRoleFailure {
                        role_run: reviewer_run,
                        failure: blocked.stage_failure().failure().clone(),
                    });
                }
                Err(RoleLoopError::Cancelled) => return MultiRoleOutcome::Cancelled,
                Err(error) => return failed_from_role_error(reviewer_run, error),
            };
            if cancellation.is_cancelled() {
                return MultiRoleOutcome::Cancelled;
            }
            if u32::from(decision.evidence().round()) != round {
                return fixed_failure(reviewer_run, QUALITY_EVIDENCE_MISMATCH, false);
            }

            match decision.evidence().verdict() {
                ReviewVerdict::Approved => {
                    return self
                        .finalize(
                            decision,
                            reviewer_run,
                            checkpoint.fingerprint(),
                            cancellation.clone(),
                        )
                        .await;
                }
                ReviewVerdict::ChangesRequested if round < crate::MAX_REVIEW_ROUNDS => {
                    let panel_sequence = decision.durable_sequence();
                    let evidence = decision.evidence().clone();
                    let durable_result = reviewer_engine
                        .emit_durable(
                            DurableRoleEvent::IntermediateReview {
                                evidence: evidence.clone(),
                                after_checkpoint_sequence: panel_sequence,
                            },
                            cancellation.clone(),
                        )
                        .await;
                    if cancellation.is_cancelled() {
                        return MultiRoleOutcome::Cancelled;
                    }
                    let ack = match durable_result {
                        Ok(ack) => ack,
                        Err(RoleLoopError::Cancelled) => return MultiRoleOutcome::Cancelled,
                        Err(error) => {
                            return fixed_failure(
                                reviewer_run,
                                QUALITY_EVIDENCE_STORE_FAILED,
                                role_error_retryable(&error),
                            );
                        }
                    };
                    if ack.sequence() <= panel_sequence {
                        return fixed_failure(reviewer_run, QUALITY_EVIDENCE_MISMATCH, false);
                    }
                    if cancellation.is_cancelled() {
                        return MultiRoleOutcome::Cancelled;
                    }
                    previous_reviews.push(evidence);
                }
                ReviewVerdict::ChangesRequested => {
                    return self
                        .finalize(
                            decision,
                            reviewer_run,
                            checkpoint.fingerprint(),
                            cancellation.clone(),
                        )
                        .await;
                }
            }
        }

        fixed_failure(planner_run, QUALITY_EVIDENCE_MISMATCH, false)
    }

    async fn finalize(
        &self,
        decision: ValidatedReviewDecision,
        reviewer_run: RoleRun,
        expected_fingerprint: WorkspaceFingerprint,
        cancellation: CancellationToken,
    ) -> MultiRoleOutcome {
        if cancellation.is_cancelled() {
            return MultiRoleOutcome::Cancelled;
        }
        let guard_result = self
            .finalization_guard
            .verify_finalization(expected_fingerprint, cancellation.clone())
            .await;
        if cancellation.is_cancelled() {
            return MultiRoleOutcome::Cancelled;
        }
        match guard_result {
            Ok(()) => {}
            Err(FinalizationGuardError::Runtime(error)) if error.code == "COMMAND_CANCELLED" => {
                return MultiRoleOutcome::Cancelled;
            }
            Err(
                FinalizationGuardError::IdentityMismatch
                | FinalizationGuardError::WorkspaceMismatch,
            ) => {
                return fixed_failure(reviewer_run, QUALITY_EVIDENCE_MISMATCH, false);
            }
            Err(FinalizationGuardError::Runtime(error)) => {
                return failed_from_role_error(reviewer_run, RoleLoopError::Runtime(error));
            }
        }
        if cancellation.is_cancelled() {
            return MultiRoleOutcome::Cancelled;
        }

        match decision.evidence().verdict() {
            ReviewVerdict::Approved => MultiRoleOutcome::Approved(decision),
            ReviewVerdict::ChangesRequested
                if u32::from(decision.evidence().round()) == crate::MAX_REVIEW_ROUNDS =>
            {
                MultiRoleOutcome::Rejected {
                    decision,
                    failure: TaskFailure {
                        code: REVIEW_REJECTED.to_owned(),
                        message: "Automated review requested changes after the final review round"
                            .to_owned(),
                        retryable: true,
                    },
                }
            }
            ReviewVerdict::ChangesRequested => {
                fixed_failure(reviewer_run, QUALITY_EVIDENCE_MISMATCH, false)
            }
        }
    }
}

fn cancel_after_executor(budget: &mut TaskBudgetLedger, review_round: u32) -> MultiRoleOutcome {
    abandon_exact_pending_reviewer(budget, review_round);
    MultiRoleOutcome::Cancelled
}

fn abandon_exact_pending_reviewer(budget: &mut TaskBudgetLedger, review_round: u32) {
    if budget
        .pending_reviewer_reservation()
        .is_some_and(|pending| pending.review_round() == review_round)
    {
        // Cleanup is best-effort at a terminal boundary. It must never replace
        // the original cancellation or role-stage error.
        let _ = budget.abandon_pending_reviewer_reservation(review_round);
    }
}

fn blocked_failure(role_run: RoleRun, blocked: &BlockedSubmission) -> MultiRoleFailure {
    let (suffix, retryable, reason) = match blocked.reason() {
        BlockedReason::MissingRequiredContext => {
            ("MISSING_CONTEXT", true, "is missing required context")
        }
        BlockedReason::ConflictingUserRequirements => (
            "CONFLICTING_REQUIREMENTS",
            false,
            "found conflicting user requirements",
        ),
        BlockedReason::RequiresGoalChange => {
            ("REQUIRES_GOAL_CHANGE", false, "requires a task goal change")
        }
        BlockedReason::UnsupportedScope => {
            ("UNSUPPORTED_SCOPE", false, "found unsupported task scope")
        }
    };
    let role = role_label(role_run.role());
    MultiRoleFailure {
        role_run,
        failure: TaskFailure {
            code: format!("{}_BLOCKED_{suffix}", role.to_ascii_uppercase()),
            message: format!("{role} {reason}"),
            retryable,
        },
    }
}

fn failed_from_role_error(role_run: RoleRun, error: RoleLoopError) -> MultiRoleOutcome {
    if matches!(error, RoleLoopError::Cancelled) {
        return MultiRoleOutcome::Cancelled;
    }
    let retryable = role_error_retryable(&error);
    let code = match role_run.role() {
        Role::Planner => error
            .planner_failure_code()
            .unwrap_or("PLANNER_RUNTIME_FAILED"),
        Role::Executor => error
            .executor_failure_code()
            .unwrap_or("EXECUTOR_RUNTIME_FAILED"),
        Role::Reviewer => error
            .reviewer_failure_code()
            .unwrap_or("REVIEWER_RUNTIME_FAILED"),
    };
    fixed_failure(role_run, code, retryable)
}

fn failed_from_factory_error(role_run: RoleRun, error: RuntimeError) -> MultiRoleOutcome {
    if error.code == "COMMAND_CANCELLED" {
        MultiRoleOutcome::Cancelled
    } else {
        failed_from_role_error(role_run, RoleLoopError::Runtime(error))
    }
}

fn fixed_failure(role_run: RoleRun, code: &str, retryable: bool) -> MultiRoleOutcome {
    MultiRoleOutcome::Failed(MultiRoleFailure {
        role_run,
        failure: TaskFailure {
            code: code.to_owned(),
            message: "The multi-role coding agent could not complete the task".to_owned(),
            retryable,
        },
    })
}

fn role_error_retryable(error: &RoleLoopError) -> bool {
    match error {
        RoleLoopError::Provider(error) => error.retryable,
        RoleLoopError::Runtime(error) => error.retryable,
        _ => false,
    }
}

fn role_label(role: Role) -> &'static str {
    match role {
        Role::Planner => "Planner",
        Role::Executor => "Executor",
        Role::Reviewer => "Reviewer",
    }
}
