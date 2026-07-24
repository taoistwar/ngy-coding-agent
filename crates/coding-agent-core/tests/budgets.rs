use coding_agent_core::{
    ActionRequest, BudgetError, BudgetResource, BudgetRole, BudgetedToolInvocation,
    ContextRedactor, EXECUTOR_RETAINED_RESULT_LIMIT, EXECUTOR_REVIEWER_RETAINED_RESERVATION,
    EXECUTOR_ROLE_CALL_LIMIT, EXECUTOR_ROLE_RESPONSE_LIMIT, EarlyRoleBudgetTermination,
    PLANNER_RETAINED_RESULT_LIMIT, PLANNER_ROLE_CALL_LIMIT, PLANNER_ROLE_RESPONSE_LIMIT,
    ProviderResponseReceipt, ProviderResponseViolation, REVIEWER_REQUIRED_CALLS,
    REVIEWER_REQUIRED_RESPONSES, REVIEWER_RETAINED_RESULT_LIMIT, REVIEWER_ROLE_CALL_LIMIT,
    REVIEWER_ROLE_RESPONSE_LIMIT, RequiredBudgetAction, RequiredCheckLedger, RetainedResultCharge,
    RetainedToolResult, Role, RoleBudgetLease, RoleBudgetTermination, TASK_CALL_LIMIT,
    TASK_PROVIDER_BYTE_LIMIT, TASK_RESPONSE_LIMIT, TASK_RETAINED_RESULT_LIMIT, TaskBudgetLedger,
    ToolCall, ToolRequest, ToolResultKind, ToolStatus, VALIDATION_RETAINED_RESULT_LIMIT,
    WorkspaceCheckpoint, WorkspaceFingerprint,
};
use coding_agent_domain::{CheckActor, CheckEvidenceStatus, RequiredCheck};

struct QualityFixture {
    checks: RequiredCheckLedger,
    checkpoint: WorkspaceCheckpoint,
}

struct IdentityRedactor;

impl ContextRedactor for IdentityRedactor {
    fn redact(&self, content: &str) -> String {
        content.to_owned()
    }
}

fn retained(wrapper_fixture: &[u8]) -> RetainedToolResult {
    RetainedToolResult::try_from_parts(
        "budget-test-result",
        String::from_utf8_lossy(wrapper_fixture),
        ToolStatus::Succeeded,
        false,
        &IdentityRedactor,
    )
    .unwrap()
}

fn retained_with_wrapper_len(target: usize) -> RetainedToolResult {
    let empty = retained(b"");
    assert!(
        target >= empty.wrapper_len(),
        "target wrapper length must fit the canonical wrapper"
    );
    let result = retained(&vec![b'x'; target - empty.wrapper_len()]);
    assert_eq!(result.wrapper_len(), target);
    result
}

fn retained_for_id_with_wrapper_len(tool_call_id: &str, target: usize) -> RetainedToolResult {
    let empty = RetainedToolResult::try_from_parts(
        tool_call_id,
        "",
        ToolStatus::Succeeded,
        false,
        &IdentityRedactor,
    )
    .unwrap();
    assert!(
        target >= empty.wrapper_len(),
        "target wrapper length must fit the canonical wrapper"
    );
    let result = RetainedToolResult::try_from_parts(
        tool_call_id,
        "x".repeat(target - empty.wrapper_len()),
        ToolStatus::Succeeded,
        false,
        &IdentityRedactor,
    )
    .unwrap();
    assert_eq!(result.wrapper_len(), target);
    result
}

fn runtime_action() -> ToolRequest {
    ToolRequest::ReadFile {
        path: "src/lib.rs".to_owned(),
        start_line: 1,
        end_line: 1,
    }
}

fn validation_action() -> ToolRequest {
    ToolRequest::CargoTest {
        package: Some("package-1".to_owned()),
        test: None,
        timeout_ms: 1,
    }
}

fn quality_fixture(check_count: usize, passed_count: usize) -> QualityFixture {
    assert!(check_count > 0);
    assert!(passed_count <= check_count);
    let checks = (0..check_count)
        .map(|index| {
            RequiredCheck::try_cargo_test(
                format!("check-{}", index + 1),
                Some(format!("package-{}", index + 1)),
                None,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let mut ledger = RequiredCheckLedger::try_new(checks.clone()).unwrap();
    let mut checkpoint = WorkspaceCheckpoint::new(WorkspaceFingerprint::from_bytes([0x42; 32]));
    for check in checks.iter().take(passed_count) {
        ledger.queue_check(&mut checkpoint, check.id()).unwrap();
        let token = ledger
            .mark_check_running(&checkpoint, check.id(), CheckActor::Executor, 1)
            .unwrap();
        ledger
            .finish_check(
                &mut checkpoint,
                token,
                CheckEvidenceStatus::Passed,
                1,
                "passed",
                false,
            )
            .unwrap();
    }
    QualityFixture {
        checks: ledger,
        checkpoint,
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

fn charge_exploratory_runtime_calls(
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
            .retain_exploratory_tool_result(lease, &result, &retained(wrapper_bytes))
            .unwrap();
    }
    ledger.finish_provider_response(lease, &receipt).unwrap();
}

fn charge_one_runtime_result(
    ledger: &mut TaskBudgetLedger,
    lease: &mut RoleBudgetLease,
    wrapper_bytes: &[u8],
) -> BudgetedToolInvocation {
    let mut receipt = open_exploratory_response(ledger, lease);
    let result = ledger
        .charge_exploratory_tool_call(lease, &mut receipt, runtime_action())
        .unwrap();
    ledger
        .retain_exploratory_tool_result(lease, &result, &retained(wrapper_bytes))
        .unwrap();
    ledger.finish_provider_response(lease, &receipt).unwrap();
    result
}

fn charge_one_exact_runtime_result(
    ledger: &mut TaskBudgetLedger,
    lease: &mut RoleBudgetLease,
    wrapper_len: usize,
) -> BudgetedToolInvocation {
    let mut receipt = open_exploratory_response(ledger, lease);
    let result = ledger
        .charge_exploratory_tool_call(lease, &mut receipt, runtime_action())
        .unwrap();
    ledger
        .retain_exploratory_tool_result(lease, &result, &retained_with_wrapper_len(wrapper_len))
        .unwrap();
    ledger.finish_provider_response(lease, &receipt).unwrap();
    result
}

fn complete_next_required(
    ledger: &mut TaskBudgetLedger,
    lease: &mut RoleBudgetLease,
    wrapper_bytes: Option<&[u8]>,
) -> RequiredBudgetAction {
    let mut permit = ledger.begin_required_action(lease).unwrap();
    let action = permit.action().clone();
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
            .retain_required_tool_result(lease, &mut permit, &retained(wrapper_bytes))
            .unwrap();
    }
    ledger.finish_provider_response(lease, &receipt).unwrap();
    ledger.complete_required_action(lease, permit).unwrap();
    action
}

fn complete_planner(ledger: &mut TaskBudgetLedger, lease: &mut RoleBudgetLease) {
    assert_eq!(
        complete_next_required(ledger, lease, None),
        RequiredBudgetAction::PlannerTerminal
    );
}

fn complete_executor(ledger: &mut TaskBudgetLedger, lease: &mut RoleBudgetLease) {
    while let Some(action) = lease.next_required_action().cloned() {
        match action {
            RequiredBudgetAction::ExecutorCheck { .. } => {
                complete_next_required(ledger, lease, Some(b"ok"));
            }
            RequiredBudgetAction::ExecutorTerminal => {
                complete_next_required(ledger, lease, None);
            }
            other => panic!("unexpected Executor action: {other:?}"),
        }
    }
}

#[test]
fn production_limits_are_the_approved_fixed_values() {
    assert_eq!(TASK_RESPONSE_LIMIT, 60);
    assert_eq!(TASK_CALL_LIMIT, 96);
    assert_eq!(TASK_PROVIDER_BYTE_LIMIT, 8 * 1024 * 1024);
    assert_eq!(TASK_RETAINED_RESULT_LIMIT, 768 * 1024);

    let planner = BudgetRole::Planner.limits();
    assert_eq!(planner.model_responses(), PLANNER_ROLE_RESPONSE_LIMIT);
    assert_eq!(planner.model_visible_calls(), PLANNER_ROLE_CALL_LIMIT);
    assert_eq!(
        planner.retained_result_bytes(),
        PLANNER_RETAINED_RESULT_LIMIT
    );
    let executor = BudgetRole::Executor.limits();
    assert_eq!(executor.model_responses(), EXECUTOR_ROLE_RESPONSE_LIMIT);
    assert_eq!(executor.model_visible_calls(), EXECUTOR_ROLE_CALL_LIMIT);
    assert_eq!(
        executor.retained_result_bytes(),
        EXECUTOR_RETAINED_RESULT_LIMIT
    );
    let reviewer = BudgetRole::Reviewer.limits();
    assert_eq!(reviewer.model_responses(), REVIEWER_ROLE_RESPONSE_LIMIT);
    assert_eq!(reviewer.model_visible_calls(), REVIEWER_ROLE_CALL_LIMIT);
    assert_eq!(
        reviewer.retained_result_bytes(),
        REVIEWER_RETAINED_RESULT_LIMIT
    );
}

#[test]
fn executor_reservation_is_derived_from_authoritative_current_check_state() {
    let quality = quality_fixture(3, 1);
    let mut ledger = TaskBudgetLedger::new();
    let mut executor = ledger
        .start_executor(1, &quality.checks, &quality.checkpoint)
        .unwrap();

    assert_eq!(executor.required_reservation().model_responses(), 3);
    assert_eq!(executor.required_reservation().model_visible_calls(), 3);
    assert_eq!(
        executor.required_reservation().retained_result_bytes(),
        2 * VALIDATION_RETAINED_RESULT_LIMIT
    );
    assert_eq!(
        executor.next_required_action(),
        Some(&RequiredBudgetAction::ExecutorCheck {
            check: quality.checks.checks()[1].clone()
        })
    );
    let pending = ledger.pending_reviewer_reservation().unwrap();
    assert_eq!(
        pending.amounts().model_responses(),
        REVIEWER_REQUIRED_RESPONSES
    );
    assert_eq!(
        pending.amounts().model_visible_calls(),
        REVIEWER_REQUIRED_CALLS
    );
    assert_eq!(
        pending.amounts().retained_result_bytes(),
        EXECUTOR_REVIEWER_RETAINED_RESERVATION
    );

    charge_exploratory_responses(&mut ledger, &mut executor, 17);
    assert!(matches!(
        ledger.begin_exploratory_provider_request(&executor, b"q", 1),
        Err(BudgetError::ReservationWouldBeConsumed {
            resource: BudgetResource::ModelResponses
        })
    ));
    let permit = ledger.begin_required_action(&mut executor).unwrap();
    assert!(matches!(
        ledger.begin_exploratory_provider_request(&executor, b"q", 1),
        Err(BudgetError::RequiredActionInProgress)
    ));
    assert!(matches!(
        ledger.finish_role(executor),
        Err(BudgetError::RequiredActionInProgress)
    ));
    drop(permit);
}

#[test]
fn finish_requires_a_terminal_and_normal_planner_executor_terminals_seal_the_lease() {
    let mut unfinished_ledger = TaskBudgetLedger::new();
    let unfinished = unfinished_ledger.start_planner().unwrap();
    assert!(matches!(
        unfinished_ledger.finish_role(unfinished),
        Err(BudgetError::RequiredActionPending {
            action: RequiredBudgetAction::PlannerTerminal
        })
    ));

    let mut planner_ledger = TaskBudgetLedger::new();
    let mut planner = planner_ledger.start_planner().unwrap();
    let mut stale_receipt = open_exploratory_response(&mut planner_ledger, &mut planner);
    let stale_invocation = planner_ledger
        .charge_exploratory_tool_call(&mut planner, &mut stale_receipt, runtime_action())
        .unwrap();
    planner_ledger
        .retain_exploratory_tool_result(&mut planner, &stale_invocation, &retained(b"kept"))
        .unwrap();
    planner_ledger
        .finish_provider_response(&planner, &stale_receipt)
        .unwrap();
    complete_planner(&mut planner_ledger, &mut planner);

    assert!(matches!(
        planner_ledger.begin_required_action(&mut planner),
        Err(BudgetError::RoleAlreadyTerminated {
            termination: RoleBudgetTermination::Normal
        })
    ));
    assert!(matches!(
        planner_ledger.begin_exploratory_provider_request(&planner, b"q", 1),
        Err(BudgetError::RoleAlreadyTerminated {
            termination: RoleBudgetTermination::Normal
        })
    ));
    assert!(matches!(
        planner_ledger.finish_provider_response(&planner, &stale_receipt),
        Err(BudgetError::RoleAlreadyTerminated {
            termination: RoleBudgetTermination::Normal
        })
    ));
    assert!(matches!(
        planner_ledger.charge_exploratory_tool_call(
            &mut planner,
            &mut stale_receipt,
            runtime_action()
        ),
        Err(BudgetError::RoleAlreadyTerminated {
            termination: RoleBudgetTermination::Normal
        })
    ));
    assert!(matches!(
        planner_ledger.retain_exploratory_tool_result(
            &mut planner,
            &stale_invocation,
            &retained(b"kept")
        ),
        Err(BudgetError::RoleAlreadyTerminated {
            termination: RoleBudgetTermination::Normal
        })
    ));
    assert!(matches!(
        planner_ledger.complete_exploratory_early_terminal(
            &mut planner,
            &mut stale_receipt,
            EarlyRoleBudgetTermination::ReportBlocked
        ),
        Err(BudgetError::RoleAlreadyTerminated {
            termination: RoleBudgetTermination::Normal
        })
    ));
    let planner_finished = planner_ledger.finish_role(planner).unwrap();
    assert_eq!(
        planner_finished.termination(),
        RoleBudgetTermination::Normal
    );

    let quality = quality_fixture(1, 1);
    let mut executor_ledger = TaskBudgetLedger::new();
    let mut executor = executor_ledger
        .start_executor(1, &quality.checks, &quality.checkpoint)
        .unwrap();
    complete_executor(&mut executor_ledger, &mut executor);
    assert!(matches!(
        executor_ledger.begin_exploratory_provider_request(&executor, b"q", 1),
        Err(BudgetError::RoleAlreadyTerminated {
            termination: RoleBudgetTermination::Normal
        })
    ));
    let executor_finished = executor_ledger.finish_role(executor).unwrap();
    assert_eq!(
        executor_finished.termination(),
        RoleBudgetTermination::Normal
    );
    assert!(executor_ledger.pending_reviewer_reservation().is_some());
    assert!(executor_ledger.start_reviewer(1).is_ok());
}

#[test]
fn typed_early_terminals_release_only_unused_reservations_and_never_mean_success() {
    let mut planner_ledger = TaskBudgetLedger::new();
    let mut planner = planner_ledger.start_planner().unwrap();
    let planner_invocation =
        charge_one_runtime_result(&mut planner_ledger, &mut planner, b"evidence");
    let mut receipt = open_exploratory_response(&mut planner_ledger, &mut planner);
    assert!(matches!(
        planner_ledger.complete_exploratory_early_terminal(
            &mut planner,
            &mut receipt,
            EarlyRoleBudgetTermination::ReviewerChangesRequested
        ),
        Err(BudgetError::ReviewerChangesRequestedByNonReviewer)
    ));
    planner_ledger
        .complete_exploratory_early_terminal(
            &mut planner,
            &mut receipt,
            EarlyRoleBudgetTermination::ReportBlocked,
        )
        .unwrap();
    assert_eq!(
        planner.required_reservation(),
        coding_agent_core::BudgetReservation::default()
    );
    assert!(matches!(
        planner_ledger.begin_exploratory_provider_request(&planner, b"q", 1),
        Err(BudgetError::RoleAlreadyTerminated {
            termination: RoleBudgetTermination::ReportBlocked
        })
    ));
    assert!(matches!(
        planner_ledger.finish_provider_response(&planner, &receipt),
        Err(BudgetError::RoleAlreadyTerminated {
            termination: RoleBudgetTermination::ReportBlocked
        })
    ));
    assert!(matches!(
        planner_ledger.charge_exploratory_tool_call(&mut planner, &mut receipt, runtime_action()),
        Err(BudgetError::RoleAlreadyTerminated {
            termination: RoleBudgetTermination::ReportBlocked
        })
    ));
    assert!(matches!(
        planner_ledger.retain_exploratory_tool_result(
            &mut planner,
            &planner_invocation,
            &retained(b"evidence")
        ),
        Err(BudgetError::RoleAlreadyTerminated {
            termination: RoleBudgetTermination::ReportBlocked
        })
    ));
    let finished = planner_ledger.finish_role(planner).unwrap();
    assert_eq!(finished.termination(), RoleBudgetTermination::ReportBlocked);
    assert_eq!(finished.usage().model_responses(), 2);
    assert_eq!(finished.usage().model_visible_calls(), 2);
    assert_eq!(
        finished.usage().retained_result_bytes(),
        retained(b"evidence").wrapper_len()
    );

    let quality = quality_fixture(3, 0);
    let mut executor_ledger = TaskBudgetLedger::new();
    let mut executor = executor_ledger
        .start_executor(1, &quality.checks, &quality.checkpoint)
        .unwrap();
    let mut permit = executor_ledger
        .begin_required_action(&mut executor)
        .unwrap();
    let request = executor_ledger
        .begin_required_provider_request(&executor, &permit, b"q", 1)
        .unwrap();
    let mut receipt = executor_ledger
        .record_required_provider_response(&mut executor, &mut permit, request, b"r")
        .unwrap();
    executor_ledger
        .complete_required_early_terminal(
            &mut executor,
            permit,
            &mut receipt,
            EarlyRoleBudgetTermination::ReportBlocked,
        )
        .unwrap();
    assert!(executor_ledger.pending_reviewer_reservation().is_none());
    assert_eq!(
        executor.required_reservation(),
        coding_agent_core::BudgetReservation::default()
    );
    assert!(matches!(
        executor_ledger.begin_required_action(&mut executor),
        Err(BudgetError::RoleAlreadyTerminated {
            termination: RoleBudgetTermination::ReportBlocked
        })
    ));
    assert!(matches!(
        executor_ledger.finish_provider_response(&executor, &receipt),
        Err(BudgetError::RoleAlreadyTerminated {
            termination: RoleBudgetTermination::ReportBlocked
        })
    ));
    let finished = executor_ledger.finish_role(executor).unwrap();
    assert_eq!(finished.termination(), RoleBudgetTermination::ReportBlocked);

    let quality = quality_fixture(1, 1);
    let mut review_ledger = TaskBudgetLedger::new();
    let mut executor = review_ledger
        .start_executor(1, &quality.checks, &quality.checkpoint)
        .unwrap();
    complete_executor(&mut review_ledger, &mut executor);
    review_ledger.finish_role(executor).unwrap();
    let mut reviewer = review_ledger.start_reviewer(1).unwrap();
    assert_eq!(
        reviewer.required_reservation().retained_result_bytes(),
        EXECUTOR_REVIEWER_RETAINED_RESERVATION
    );
    let blocking_invocation =
        charge_one_runtime_result(&mut review_ledger, &mut reviewer, b"blocking");
    let mut receipt = open_exploratory_response(&mut review_ledger, &mut reviewer);
    review_ledger
        .complete_exploratory_early_terminal(
            &mut reviewer,
            &mut receipt,
            EarlyRoleBudgetTermination::ReviewerChangesRequested,
        )
        .unwrap();
    assert_eq!(
        reviewer.required_reservation(),
        coding_agent_core::BudgetReservation::default()
    );
    assert!(matches!(
        review_ledger.begin_exploratory_provider_request(&reviewer, b"q", 1),
        Err(BudgetError::RoleAlreadyTerminated {
            termination: RoleBudgetTermination::ReviewerChangesRequested
        })
    ));
    assert!(matches!(
        review_ledger.retain_exploratory_tool_result(
            &mut reviewer,
            &blocking_invocation,
            &retained(b"blocking")
        ),
        Err(BudgetError::RoleAlreadyTerminated {
            termination: RoleBudgetTermination::ReviewerChangesRequested
        })
    ));
    assert!(matches!(
        review_ledger.charge_exploratory_tool_call(&mut reviewer, &mut receipt, runtime_action()),
        Err(BudgetError::RoleAlreadyTerminated {
            termination: RoleBudgetTermination::ReviewerChangesRequested
        })
    ));
    let finished = review_ledger.finish_role(reviewer).unwrap();
    assert_eq!(
        finished.termination(),
        RoleBudgetTermination::ReviewerChangesRequested
    );
    assert_eq!(finished.usage().model_responses(), 2);
    assert_eq!(finished.usage().model_visible_calls(), 2);
    assert_eq!(
        finished.usage().retained_result_bytes(),
        retained(b"blocking").wrapper_len()
    );
    assert_eq!(
        review_ledger.usage().retained_result_bytes(),
        retained(b"blocking").wrapper_len()
    );
    assert!(
        review_ledger
            .start_executor(2, &quality.checks, &quality.checkpoint)
            .is_ok()
    );
}

#[test]
fn retained_result_tokens_bind_call_owner_and_exact_wrapper_content() {
    let mut ledger = TaskBudgetLedger::new();
    let mut planner = ledger.start_planner().unwrap();
    let mut receipt = open_exploratory_response(&mut ledger, &mut planner);
    let first = ledger
        .charge_exploratory_tool_call(&mut planner, &mut receipt, runtime_action())
        .unwrap();
    let second = ledger
        .charge_exploratory_tool_call(&mut planner, &mut receipt, runtime_action())
        .unwrap();
    assert_ne!(first.result_id(), second.result_id());

    assert_eq!(
        ledger
            .retain_exploratory_tool_result(&mut planner, &first, &retained(b"same"))
            .unwrap(),
        RetainedResultCharge::Charged
    );
    assert_eq!(
        ledger
            .retain_exploratory_tool_result(&mut planner, &first, &retained(b"same"))
            .unwrap(),
        RetainedResultCharge::AlreadyCounted
    );
    assert!(matches!(
        ledger.retain_exploratory_tool_result(&mut planner, &first, &retained(b"diff")),
        Err(BudgetError::RetainedResultContentConflict { result_id })
            if result_id == first.result_id()
    ));
    assert_eq!(
        ledger
            .retain_exploratory_tool_result(&mut planner, &second, &retained(b"same"))
            .unwrap(),
        RetainedResultCharge::Charged
    );
    let same_wrapper_bytes = retained(b"same").wrapper_len();
    assert_eq!(
        ledger.usage().retained_result_bytes(),
        same_wrapper_bytes * 2
    );
    ledger.finish_provider_response(&planner, &receipt).unwrap();
    complete_planner(&mut ledger, &mut planner);
    ledger.finish_role(planner).unwrap();

    let quality = quality_fixture(1, 1);
    let mut executor = ledger
        .start_executor(1, &quality.checks, &quality.checkpoint)
        .unwrap();
    assert!(matches!(
        ledger.retain_exploratory_tool_result(&mut executor, &first, &retained(b"same")),
        Err(BudgetError::ToolResultPermitMismatch)
    ));
    assert_eq!(
        ledger.usage().retained_result_bytes(),
        same_wrapper_bytes * 2
    );
}

#[test]
fn every_validation_result_is_limited_to_eight_kib_in_all_roles_and_modes() {
    let quality = quality_fixture(1, 0);
    let mut ledger = TaskBudgetLedger::new();
    let mut executor = ledger
        .start_executor(1, &quality.checks, &quality.checkpoint)
        .unwrap();

    let mut required = ledger.begin_required_action(&mut executor).unwrap();
    let request = ledger
        .begin_required_provider_request(&executor, &required, b"q", 1)
        .unwrap();
    let mut receipt = ledger
        .record_required_provider_response(&mut executor, &mut required, request, b"r")
        .unwrap();
    ledger
        .charge_required_call(&mut executor, &mut required, &mut receipt)
        .unwrap();
    assert!(matches!(
        ledger.retain_required_tool_result(
            &mut executor,
            &mut required,
            &retained_with_wrapper_len(VALIDATION_RETAINED_RESULT_LIMIT + 1)
        ),
        Err(BudgetError::ToolResultKindLimitExceeded {
            limit: VALIDATION_RETAINED_RESULT_LIMIT,
            ..
        })
    ));
    ledger
        .retain_required_tool_result(
            &mut executor,
            &mut required,
            &retained_with_wrapper_len(VALIDATION_RETAINED_RESULT_LIMIT),
        )
        .unwrap();
    ledger
        .finish_provider_response(&executor, &receipt)
        .unwrap();
    ledger
        .complete_required_action(&mut executor, required)
        .unwrap();

    let mut receipt = open_exploratory_response(&mut ledger, &mut executor);
    let validation = ledger
        .charge_exploratory_tool_call(&mut executor, &mut receipt, validation_action())
        .unwrap();
    assert_eq!(validation.kind(), ToolResultKind::Validation);
    assert!(matches!(
        ledger.retain_exploratory_tool_result(
            &mut executor,
            &validation,
            &retained_with_wrapper_len(VALIDATION_RETAINED_RESULT_LIMIT + 1)
        ),
        Err(BudgetError::ToolResultKindLimitExceeded {
            limit: VALIDATION_RETAINED_RESULT_LIMIT,
            ..
        })
    ));
    ledger
        .retain_exploratory_tool_result(
            &mut executor,
            &validation,
            &retained_with_wrapper_len(VALIDATION_RETAINED_RESULT_LIMIT),
        )
        .unwrap();
    let runtime = ledger
        .charge_exploratory_tool_call(&mut executor, &mut receipt, runtime_action())
        .unwrap();
    assert_eq!(runtime.kind(), ToolResultKind::Runtime);
    ledger
        .retain_exploratory_tool_result(
            &mut executor,
            &runtime,
            &retained_with_wrapper_len(VALIDATION_RETAINED_RESULT_LIMIT + 1),
        )
        .unwrap();
    ledger
        .finish_provider_response(&executor, &receipt)
        .unwrap();
    complete_executor(&mut ledger, &mut executor);
    ledger.finish_role(executor).unwrap();

    let mut reviewer = ledger.start_reviewer(1).unwrap();
    let mut receipt = open_exploratory_response(&mut ledger, &mut reviewer);
    let validation = ledger
        .charge_exploratory_tool_call(&mut reviewer, &mut receipt, validation_action())
        .unwrap();
    assert_eq!(validation.kind(), ToolResultKind::Validation);
    assert!(matches!(
        ledger.retain_exploratory_tool_result(
            &mut reviewer,
            &validation,
            &retained_with_wrapper_len(VALIDATION_RETAINED_RESULT_LIMIT + 1)
        ),
        Err(BudgetError::ToolResultKindLimitExceeded {
            limit: VALIDATION_RETAINED_RESULT_LIMIT,
            ..
        })
    ));
    ledger
        .retain_exploratory_tool_result(
            &mut reviewer,
            &validation,
            &retained_with_wrapper_len(VALIDATION_RETAINED_RESULT_LIMIT),
        )
        .unwrap();
}

#[test]
fn whole_batch_preflight_failures_are_atomic_and_do_not_mint_result_ids() {
    let mut ledger = TaskBudgetLedger::new();
    let mut planner = ledger.start_planner().unwrap();
    let mut receipt = open_exploratory_response(&mut ledger, &mut planner);
    let usage_before = ledger.usage();
    let role_usage_before = planner.usage();

    let terminal = ToolCall {
        id: "terminal".to_owned(),
        request: ActionRequest::decode(
            Role::Planner,
            "submit_plan",
            &serde_json::json!({
                "summary": "plan",
                "steps": [{
                    "title": "step",
                    "description": "description",
                    "acceptance_criteria": ["criterion"]
                }],
                "initial_required_checks": [{
                    "kind": "cargo_test",
                    "package": "coding-agent-core",
                    "integration_test": "budgets"
                }]
            })
            .to_string(),
        )
        .unwrap(),
    };
    assert!(matches!(
        ledger.preflight_exploratory_runtime_batch(
            &mut planner,
            &mut receipt,
            vec![ToolCall::runtime("read", runtime_action()), terminal,],
        ),
        Err(BudgetError::InvalidExploratoryRuntimeBatch)
    ));
    assert_eq!(ledger.usage(), usage_before);
    assert_eq!(planner.usage(), role_usage_before);

    let too_many = (0..PLANNER_ROLE_CALL_LIMIT)
        .map(|index| ToolCall::runtime(format!("read-{index}"), runtime_action()))
        .collect();
    assert!(matches!(
        ledger.preflight_exploratory_runtime_batch(&mut planner, &mut receipt, too_many),
        Err(BudgetError::ReservationWouldBeConsumed {
            resource: BudgetResource::ModelVisibleCalls
        })
    ));
    assert_eq!(ledger.usage(), usage_before);
    assert_eq!(planner.usage(), role_usage_before);

    let permit = ledger
        .preflight_exploratory_runtime_batch(
            &mut planner,
            &mut receipt,
            vec![ToolCall::runtime("accepted", runtime_action())],
        )
        .unwrap();
    assert_eq!(permit.invocations()[0].result_id().value(), 1);
    ledger
        .abort_exploratory_runtime_batch(&mut planner, &permit)
        .unwrap();
    ledger.finish_provider_response(&planner, &receipt).unwrap();
    ledger.abort_role_on_failure(planner).unwrap();
}

#[test]
fn whole_batch_dynamic_caps_share_only_the_actual_remaining_role_bytes() {
    let mut ledger = TaskBudgetLedger::new();
    let mut planner = ledger.start_planner().unwrap();
    charge_one_exact_runtime_result(&mut ledger, &mut planner, 120 * 1024);
    let retained_before = ledger.usage().retained_result_bytes();
    let remaining = PLANNER_RETAINED_RESULT_LIMIT - retained_before;
    let mut receipt = open_exploratory_response(&mut ledger, &mut planner);
    let permit = ledger
        .preflight_exploratory_runtime_batch(
            &mut planner,
            &mut receipt,
            vec![
                ToolCall::runtime("first", runtime_action()),
                ToolCall::runtime("second", runtime_action()),
            ],
        )
        .unwrap();
    assert_eq!(permit.invocations().len(), 2);
    assert_eq!(permit.invocations()[0].wrapper_cap(), remaining / 2);
    assert_eq!(permit.invocations()[1].wrapper_cap(), remaining / 2);
    assert_eq!(
        permit
            .invocations()
            .iter()
            .map(|invocation| invocation.wrapper_cap())
            .sum::<usize>(),
        remaining
    );

    let results = permit
        .invocations()
        .iter()
        .map(|invocation| {
            retained_for_id_with_wrapper_len(invocation.tool_call_id(), invocation.wrapper_cap())
        })
        .collect::<Vec<_>>();
    ledger
        .retain_exploratory_batch_results(&mut planner, &permit, &results)
        .unwrap();
    assert_eq!(
        ledger.usage().retained_result_bytes(),
        PLANNER_RETAINED_RESULT_LIMIT
    );
    ledger.finish_provider_response(&planner, &receipt).unwrap();
    ledger.abort_role_on_failure(planner).unwrap();
}

#[test]
fn whole_batch_dynamic_cap_preserves_executor_required_validation_bytes() {
    let quality = quality_fixture(1, 0);
    let mut ledger = TaskBudgetLedger::new();
    let mut executor = ledger
        .start_executor(1, &quality.checks, &quality.checkpoint)
        .unwrap();
    assert_eq!(
        executor.required_reservation().retained_result_bytes(),
        VALIDATION_RETAINED_RESULT_LIMIT
    );
    assert_eq!(
        ledger
            .pending_reviewer_reservation()
            .unwrap()
            .amounts()
            .retained_result_bytes(),
        EXECUTOR_REVIEWER_RETAINED_RESERVATION
    );

    let mut receipt = open_exploratory_response(&mut ledger, &mut executor);
    let permit = ledger
        .preflight_exploratory_runtime_batch(
            &mut executor,
            &mut receipt,
            vec![ToolCall::runtime("read", runtime_action())],
        )
        .unwrap();
    assert_eq!(
        permit.invocations()[0].wrapper_cap(),
        EXECUTOR_RETAINED_RESULT_LIMIT - VALIDATION_RETAINED_RESULT_LIMIT
    );

    ledger
        .abort_exploratory_runtime_batch(&mut executor, &permit)
        .unwrap();
    ledger
        .finish_provider_response(&executor, &receipt)
        .unwrap();
    ledger.abort_role_on_failure(executor).unwrap();
}

#[test]
fn executor_dynamic_refresh_tracks_current_missing_checks_without_resetting_the_lease() {
    let mut quality = quality_fixture(2, 2);
    let mut ledger = TaskBudgetLedger::new();
    let mut executor = ledger
        .start_executor(1, &quality.checks, &quality.checkpoint)
        .unwrap();
    let initial_usage = executor.usage();

    assert_eq!(executor.required_reservation().model_responses(), 1);
    assert_eq!(executor.required_reservation().model_visible_calls(), 1);
    assert_eq!(executor.required_reservation().retained_result_bytes(), 0);
    assert!(matches!(
        executor.next_required_action(),
        Some(RequiredBudgetAction::ExecutorTerminal)
    ));

    quality
        .checkpoint
        .observe_stable(WorkspaceFingerprint::from_bytes([0x43; 32]))
        .unwrap();
    ledger
        .refresh_executor_required_actions(&mut executor, &quality.checks, &quality.checkpoint)
        .unwrap();

    assert_eq!(executor.usage(), initial_usage);
    assert_eq!(executor.required_reservation().model_responses(), 3);
    assert_eq!(executor.required_reservation().model_visible_calls(), 3);
    assert_eq!(
        executor.required_reservation().retained_result_bytes(),
        2 * VALIDATION_RETAINED_RESULT_LIMIT
    );
    assert!(matches!(
        executor.next_required_action(),
        Some(RequiredBudgetAction::ExecutorCheck { check }) if check.id() == "check-1"
    ));
    let pending_reviewer = ledger.pending_reviewer_reservation().unwrap();
    assert_eq!(pending_reviewer.review_round(), 1);
    assert_eq!(
        pending_reviewer.amounts().model_responses(),
        REVIEWER_REQUIRED_RESPONSES
    );
    assert_eq!(
        pending_reviewer.amounts().model_visible_calls(),
        REVIEWER_REQUIRED_CALLS
    );
    assert_eq!(
        pending_reviewer.amounts().retained_result_bytes(),
        EXECUTOR_REVIEWER_RETAINED_RESERVATION
    );

    for check in quality.checks.checks().to_vec() {
        quality
            .checks
            .queue_check(&mut quality.checkpoint, check.id())
            .unwrap();
        let token = quality
            .checks
            .mark_check_running(&quality.checkpoint, check.id(), CheckActor::Executor, 1)
            .unwrap();
        quality
            .checks
            .finish_check(
                &mut quality.checkpoint,
                token,
                CheckEvidenceStatus::Passed,
                1,
                "passed after refresh",
                false,
            )
            .unwrap();
    }
    ledger
        .refresh_executor_required_actions(&mut executor, &quality.checks, &quality.checkpoint)
        .unwrap();

    assert_eq!(executor.usage(), initial_usage);
    assert_eq!(executor.required_reservation().model_responses(), 1);
    assert_eq!(executor.required_reservation().model_visible_calls(), 1);
    assert_eq!(executor.required_reservation().retained_result_bytes(), 0);
    assert!(matches!(
        executor.next_required_action(),
        Some(RequiredBudgetAction::ExecutorTerminal)
    ));
    assert_eq!(
        ledger
            .pending_reviewer_reservation()
            .unwrap()
            .amounts()
            .retained_result_bytes(),
        EXECUTOR_REVIEWER_RETAINED_RESERVATION
    );
    ledger.abort_role_on_failure(executor).unwrap();
}

#[test]
fn executor_workspace_change_protection_expands_before_runtime_and_later_shrinks() {
    let quality = quality_fixture(2, 2);
    let mut ledger = TaskBudgetLedger::new();
    let mut executor = ledger
        .start_executor(1, &quality.checks, &quality.checkpoint)
        .unwrap();
    let receipt = open_exploratory_response(&mut ledger, &mut executor);

    ledger
        .protect_executor_workspace_change(&mut executor, &receipt, &quality.checks)
        .unwrap();
    assert_eq!(executor.required_reservation().model_responses(), 3);
    assert_eq!(executor.required_reservation().model_visible_calls(), 3);
    assert_eq!(
        executor.required_reservation().retained_result_bytes(),
        2 * VALIDATION_RETAINED_RESULT_LIMIT
    );
    assert!(matches!(
        executor.next_required_action(),
        Some(RequiredBudgetAction::ExecutorCheck { check }) if check.id() == "check-1"
    ));
    assert_eq!(
        ledger
            .pending_reviewer_reservation()
            .unwrap()
            .amounts()
            .retained_result_bytes(),
        EXECUTOR_REVIEWER_RETAINED_RESERVATION
    );

    ledger
        .finish_provider_response(&executor, &receipt)
        .unwrap();
    ledger
        .refresh_executor_required_actions(&mut executor, &quality.checks, &quality.checkpoint)
        .unwrap();
    assert_eq!(executor.required_reservation().model_responses(), 1);
    assert_eq!(executor.required_reservation().model_visible_calls(), 1);
    assert_eq!(executor.required_reservation().retained_result_bytes(), 0);
    assert!(matches!(
        executor.next_required_action(),
        Some(RequiredBudgetAction::ExecutorTerminal)
    ));
    ledger.abort_role_on_failure(executor).unwrap();
}

#[test]
fn executor_workspace_change_protection_failure_is_atomic() {
    let quality = quality_fixture(16, 16);
    let mut ledger = TaskBudgetLedger::new();
    let mut executor = ledger
        .start_executor(1, &quality.checks, &quality.checkpoint)
        .unwrap();
    charge_exploratory_responses(&mut ledger, &mut executor, 4);
    let receipt = open_exploratory_response(&mut ledger, &mut executor);
    let reservation_before = executor.required_reservation();
    let action_before = executor.next_required_action().cloned();
    let reviewer_before = ledger.pending_reviewer_reservation();

    assert!(matches!(
        ledger.protect_executor_workspace_change(&mut executor, &receipt, &quality.checks),
        Err(BudgetError::RoleLimitExceeded {
            role: BudgetRole::Executor,
            resource: BudgetResource::ModelResponses
        })
    ));
    assert_eq!(executor.required_reservation(), reservation_before);
    assert_eq!(executor.next_required_action(), action_before.as_ref());
    assert_eq!(ledger.pending_reviewer_reservation(), reviewer_before);
    assert_eq!(
        ledger
            .pending_reviewer_reservation()
            .unwrap()
            .amounts()
            .retained_result_bytes(),
        EXECUTOR_REVIEWER_RETAINED_RESERVATION
    );

    ledger
        .finish_provider_response(&executor, &receipt)
        .unwrap();
    ledger.abort_role_on_failure(executor).unwrap();
}

#[test]
fn executor_control_permit_binds_the_exact_canonical_wrapper_bytes() {
    let quality = quality_fixture(1, 0);
    let mut ledger = TaskBudgetLedger::new();
    let mut executor = ledger
        .start_executor(1, &quality.checks, &quality.checkpoint)
        .unwrap();
    let mut receipt = open_exploratory_response(&mut ledger, &mut executor);
    let call = ToolCall {
        id: "progress-1".to_owned(),
        request: ActionRequest::decode(
            Role::Executor,
            "update_plan_progress",
            r#"{"updates":[{"step_id":"step-1","status":"running"}]}"#,
        )
        .unwrap(),
    };
    let accepted = RetainedToolResult::try_from_parts(
        "progress-1",
        "a",
        ToolStatus::Succeeded,
        false,
        &IdentityRedactor,
    )
    .unwrap();
    let same_length_different_wrapper = RetainedToolResult::try_from_parts(
        "progress-1",
        "b",
        ToolStatus::Succeeded,
        false,
        &IdentityRedactor,
    )
    .unwrap();
    assert_eq!(
        accepted.wrapper_len(),
        same_length_different_wrapper.wrapper_len()
    );
    assert_ne!(
        accepted.wrapper_bytes(),
        same_length_different_wrapper.wrapper_bytes()
    );

    let permit = ledger
        .preflight_exploratory_control_result(&mut executor, &mut receipt, &call, &accepted)
        .unwrap();
    assert!(matches!(
        ledger.retain_exploratory_control_result(
            &mut executor,
            &permit,
            &same_length_different_wrapper,
        ),
        Err(BudgetError::InvalidExploratoryControl)
    ));
    assert_eq!(ledger.usage().retained_result_bytes(), 0);
    assert_eq!(executor.usage().retained_result_bytes(), 0);

    ledger
        .abort_exploratory_control_result(&mut executor, &permit)
        .unwrap();
    ledger
        .finish_provider_response(&executor, &receipt)
        .unwrap();
    ledger.abort_role_on_failure(executor).unwrap();
}

#[test]
fn whole_batch_result_mismatch_is_zero_retained_and_abort_cleans_the_permit() {
    let mut ledger = TaskBudgetLedger::new();
    let mut planner = ledger.start_planner().unwrap();
    let mut receipt = open_exploratory_response(&mut ledger, &mut planner);
    let permit = ledger
        .preflight_exploratory_runtime_batch(
            &mut planner,
            &mut receipt,
            vec![
                ToolCall::runtime("first", runtime_action()),
                ToolCall::runtime("second", runtime_action()),
            ],
        )
        .unwrap();
    let first = retained_for_id_with_wrapper_len(
        permit.invocations()[0].tool_call_id(),
        permit.invocations()[0].wrapper_cap().min(1_024),
    );
    let wrong_second = retained_for_id_with_wrapper_len(
        "wrong-id",
        permit.invocations()[1].wrapper_cap().min(1_024),
    );
    assert!(matches!(
        ledger.retain_exploratory_batch_results(&mut planner, &permit, &[first, wrong_second],),
        Err(BudgetError::ExploratoryBatchPermitMismatch)
    ));
    assert_eq!(ledger.usage().retained_result_bytes(), 0);
    assert_eq!(planner.usage().retained_result_bytes(), 0);

    let first = retained_for_id_with_wrapper_len(
        permit.invocations()[0].tool_call_id(),
        permit.invocations()[0].wrapper_cap().min(1_024),
    );
    let oversized_second = retained_for_id_with_wrapper_len(
        permit.invocations()[1].tool_call_id(),
        permit.invocations()[1].wrapper_cap() + 1,
    );
    assert!(matches!(
        ledger.retain_exploratory_batch_results(
            &mut planner,
            &permit,
            &[first, oversized_second],
        ),
        Err(BudgetError::ToolResultKindLimitExceeded {
            limit,
            observed
        }) if limit == permit.invocations()[1].wrapper_cap()
            && observed == limit + 1
    ));
    assert_eq!(ledger.usage().retained_result_bytes(), 0);
    assert_eq!(planner.usage().retained_result_bytes(), 0);

    ledger
        .abort_exploratory_runtime_batch(&mut planner, &permit)
        .unwrap();
    ledger.finish_provider_response(&planner, &receipt).unwrap();
    ledger.abort_role_on_failure(planner).unwrap();

    let mut other_ledger = TaskBudgetLedger::new();
    let mut other_planner = other_ledger.start_planner().unwrap();
    let mut other_receipt = open_exploratory_response(&mut other_ledger, &mut other_planner);
    let other_permit = other_ledger
        .preflight_exploratory_runtime_batch(
            &mut other_planner,
            &mut other_receipt,
            vec![ToolCall::runtime("bound", runtime_action())],
        )
        .unwrap();
    let mut foreign_ledger = TaskBudgetLedger::new();
    let mut foreign_planner = foreign_ledger.start_planner().unwrap();
    assert!(matches!(
        foreign_ledger.abort_exploratory_runtime_batch(&mut foreign_planner, &other_permit,),
        Err(BudgetError::ExploratoryBatchPermitMismatch)
    ));
    assert_eq!(foreign_ledger.usage().retained_result_bytes(), 0);
    foreign_ledger
        .abort_role_on_failure(foreign_planner)
        .unwrap();
    other_ledger
        .abort_exploratory_runtime_batch(&mut other_planner, &other_permit)
        .unwrap();
    other_ledger
        .finish_provider_response(&other_planner, &other_receipt)
        .unwrap();
    other_ledger.abort_role_on_failure(other_planner).unwrap();
}

#[test]
fn required_result_abort_is_bound_to_ledger_action_result_and_open_request() {
    let quality = quality_fixture(1, 0);
    let mut source = TaskBudgetLedger::new();
    let mut source_executor = source
        .start_executor(1, &quality.checks, &quality.checkpoint)
        .unwrap();
    let mut source_action = source.begin_required_action(&mut source_executor).unwrap();
    let source_request = source
        .begin_required_provider_request(&source_executor, &source_action, b"q", 1)
        .unwrap();
    let mut source_receipt = source
        .record_required_provider_response(
            &mut source_executor,
            &mut source_action,
            source_request,
            b"r",
        )
        .unwrap();
    source
        .charge_required_call(
            &mut source_executor,
            &mut source_action,
            &mut source_receipt,
        )
        .unwrap();

    let mut foreign = TaskBudgetLedger::new();
    let mut foreign_executor = foreign
        .start_executor(1, &quality.checks, &quality.checkpoint)
        .unwrap();
    let mut foreign_action = foreign
        .begin_required_action(&mut foreign_executor)
        .unwrap();
    let foreign_request = foreign
        .begin_required_provider_request(&foreign_executor, &foreign_action, b"q", 1)
        .unwrap();
    let mut foreign_receipt = foreign
        .record_required_provider_response(
            &mut foreign_executor,
            &mut foreign_action,
            foreign_request,
            b"r",
        )
        .unwrap();
    foreign
        .charge_required_call(
            &mut foreign_executor,
            &mut foreign_action,
            &mut foreign_receipt,
        )
        .unwrap();

    assert!(matches!(
        source.abort_required_runtime_result(
            &mut source_executor,
            &source_action,
            &foreign_receipt,
        ),
        Err(BudgetError::ProviderResponseReceiptMismatch)
    ));
    assert!(matches!(
        foreign.abort_required_runtime_result(
            &mut foreign_executor,
            &source_action,
            &foreign_receipt,
        ),
        Err(BudgetError::RequiredPermitMismatch)
    ));
    assert_eq!(source.usage().retained_result_bytes(), 0);
    assert_eq!(foreign.usage().retained_result_bytes(), 0);

    source
        .abort_required_runtime_result(&mut source_executor, &source_action, &source_receipt)
        .unwrap();
    source
        .finish_provider_response(&source_executor, &source_receipt)
        .unwrap();
    source.abort_role_on_failure(source_executor).unwrap();

    foreign
        .abort_required_runtime_result(&mut foreign_executor, &foreign_action, &foreign_receipt)
        .unwrap();
    foreign
        .finish_provider_response(&foreign_executor, &foreign_receipt)
        .unwrap();
    foreign.abort_role_on_failure(foreign_executor).unwrap();
}

#[test]
fn malformed_and_oversized_received_responses_are_charged_before_rejection() {
    let mut ledger = TaskBudgetLedger::new();
    let mut planner = ledger.start_planner().unwrap();

    let request = ledger
        .begin_exploratory_provider_request(&planner, b"req", 4)
        .unwrap();
    let receipt = ledger
        .record_exploratory_provider_response(&mut planner, request, &[0xff, 0x00])
        .unwrap();
    assert_eq!(ledger.usage().provider_bytes(), 5);
    assert_eq!(ledger.usage().model_responses(), 1);
    assert_eq!(ledger.usage().model_visible_calls(), 0);
    assert_eq!(receipt.violation(), None);
    ledger
        .discard_invalid_provider_response(&planner, &receipt)
        .unwrap();

    let request = ledger
        .begin_exploratory_provider_request(&planner, b"sent", 4)
        .unwrap();
    ledger
        .record_transport_no_response(&planner, request)
        .unwrap();
    assert_eq!(ledger.usage().provider_bytes(), 9);
    assert_eq!(ledger.usage().model_responses(), 1);

    let request = ledger
        .begin_exploratory_provider_request(&planner, b"x", 1)
        .unwrap();
    let mut receipt = ledger
        .record_exploratory_provider_response(&mut planner, request, b"xx")
        .unwrap();
    assert_eq!(
        receipt.violation(),
        Some(ProviderResponseViolation::ReservedByteLimit)
    );
    assert_eq!(ledger.usage().provider_bytes(), 12);
    assert_eq!(ledger.usage().model_responses(), 2);
    assert!(matches!(
        ledger.finish_provider_response(&planner, &receipt),
        Err(BudgetError::ProviderResponseLimitViolation)
    ));
    assert!(matches!(
        ledger.charge_exploratory_tool_call(&mut planner, &mut receipt, runtime_action()),
        Err(BudgetError::ProviderResponseLimitViolation)
    ));
    assert!(matches!(
        ledger.complete_exploratory_early_terminal(
            &mut planner,
            &mut receipt,
            EarlyRoleBudgetTermination::ReportBlocked
        ),
        Err(BudgetError::ProviderResponseLimitViolation)
    ));
    assert_eq!(ledger.usage().model_visible_calls(), 0);
    assert_eq!(ledger.usage().retained_result_bytes(), 0);
    ledger
        .discard_invalid_provider_response(&planner, &receipt)
        .unwrap();
}

#[test]
fn violating_required_response_cannot_mint_a_call_or_early_terminal() {
    let mut ledger = TaskBudgetLedger::new();
    let mut planner = ledger.start_planner().unwrap();
    let mut permit = ledger.begin_required_action(&mut planner).unwrap();
    let request = ledger
        .begin_required_provider_request(&planner, &permit, b"q", 1)
        .unwrap();
    let mut receipt = ledger
        .record_required_provider_response(&mut planner, &mut permit, request, b"xx")
        .unwrap();

    assert_eq!(
        receipt.violation(),
        Some(ProviderResponseViolation::ReservedByteLimit)
    );
    assert_eq!(ledger.usage().model_responses(), 1);
    assert_eq!(ledger.usage().provider_bytes(), 3);
    assert!(matches!(
        ledger.charge_required_call(&mut planner, &mut permit, &mut receipt),
        Err(BudgetError::ProviderResponseLimitViolation)
    ));
    assert!(matches!(
        ledger.complete_required_early_terminal(
            &mut planner,
            permit,
            &mut receipt,
            EarlyRoleBudgetTermination::ReportBlocked
        ),
        Err(BudgetError::ProviderResponseLimitViolation)
    ));
    assert_eq!(ledger.usage().model_visible_calls(), 0);
    assert_eq!(ledger.usage().retained_result_bytes(), 0);
    ledger
        .discard_invalid_provider_response(&planner, &receipt)
        .unwrap();
}

#[test]
fn provider_preflight_keeps_room_for_the_bounded_response_and_reaches_exact_task_limit() {
    let mut ledger = TaskBudgetLedger::new();
    let mut planner = ledger.start_planner().unwrap();
    let half_mib = vec![0; 512 * 1024];
    for _ in 0..7 {
        let request = ledger
            .begin_exploratory_provider_request(&planner, &half_mib, half_mib.len())
            .unwrap();
        let receipt = ledger
            .record_exploratory_provider_response(&mut planner, request, &half_mib)
            .unwrap();
        ledger.finish_provider_response(&planner, &receipt).unwrap();
    }
    assert_eq!(ledger.usage().provider_bytes(), 7 * 1024 * 1024);

    let mut terminal = ledger.begin_required_action(&mut planner).unwrap();
    assert!(matches!(
        ledger.begin_required_provider_request(&planner, &terminal, &vec![0; 1024 * 1024], 1),
        Err(BudgetError::TaskLimitExceeded {
            resource: BudgetResource::ProviderBytes
        })
    ));
    let request = ledger
        .begin_required_provider_request(&planner, &terminal, &half_mib, half_mib.len())
        .unwrap();
    let mut receipt = ledger
        .record_required_provider_response(&mut planner, &mut terminal, request, &half_mib)
        .unwrap();
    ledger
        .charge_required_call(&mut planner, &mut terminal, &mut receipt)
        .unwrap();
    ledger.finish_provider_response(&planner, &receipt).unwrap();
    ledger
        .complete_required_action(&mut planner, terminal)
        .unwrap();
    assert_eq!(ledger.usage().provider_bytes(), TASK_PROVIDER_BYTE_LIMIT);
}

#[test]
fn task_usage_is_shared_across_fresh_role_runs_and_transcript_reencoding_is_not_retained_twice() {
    let quality = quality_fixture(1, 1);
    let mut ledger = TaskBudgetLedger::new();

    let mut planner = ledger.start_planner().unwrap();
    let planner_result = charge_one_exact_runtime_result(&mut ledger, &mut planner, 128 * 1024);
    let retained_before = ledger.usage().retained_result_bytes();
    assert_eq!(
        ledger
            .retain_exploratory_tool_result(
                &mut planner,
                &planner_result,
                &retained_with_wrapper_len(128 * 1024)
            )
            .unwrap(),
        RetainedResultCharge::AlreadyCounted
    );
    let request = ledger
        .begin_exploratory_provider_request(&planner, b"transcript", 1)
        .unwrap();
    let receipt = ledger
        .record_exploratory_provider_response(&mut planner, request, b"r")
        .unwrap();
    ledger.finish_provider_response(&planner, &receipt).unwrap();
    assert_eq!(ledger.usage().retained_result_bytes(), retained_before);
    complete_planner(&mut ledger, &mut planner);
    ledger.finish_role(planner).unwrap();

    let mut executor_1 = ledger
        .start_executor(1, &quality.checks, &quality.checkpoint)
        .unwrap();
    charge_one_exact_runtime_result(&mut ledger, &mut executor_1, 256 * 1024);
    complete_executor(&mut ledger, &mut executor_1);
    ledger.finish_role(executor_1).unwrap();
    let before_reviewer = ledger.usage();

    let mut reviewer_1 = ledger.start_reviewer(1).unwrap();
    let mut receipt = open_exploratory_response(&mut ledger, &mut reviewer_1);
    ledger
        .complete_exploratory_early_terminal(
            &mut reviewer_1,
            &mut receipt,
            EarlyRoleBudgetTermination::ReviewerChangesRequested,
        )
        .unwrap();
    let reviewer_finished = ledger.finish_role(reviewer_1).unwrap();
    assert_eq!(
        reviewer_finished.termination(),
        RoleBudgetTermination::ReviewerChangesRequested
    );
    assert_eq!(
        ledger.usage().retained_result_bytes(),
        before_reviewer.retained_result_bytes()
    );
    assert_eq!(
        ledger.usage().model_responses(),
        before_reviewer.model_responses() + 1
    );
    assert_eq!(
        ledger.usage().model_visible_calls(),
        before_reviewer.model_visible_calls() + 1
    );

    let mut executor_2 = ledger
        .start_executor(2, &quality.checks, &quality.checkpoint)
        .unwrap();
    charge_one_exact_runtime_result(&mut ledger, &mut executor_2, 100 * 1024);
    complete_executor(&mut ledger, &mut executor_2);
    ledger.finish_role(executor_2).unwrap();
    assert_eq!(
        ledger.usage().retained_result_bytes(),
        128 * 1024 + 256 * 1024 + 100 * 1024
    );
}

#[test]
fn role_lease_ceilings_fail_atomically_at_the_exact_boundary() {
    let mut response_ledger = TaskBudgetLedger::new();
    let mut planner = response_ledger.start_planner().unwrap();
    charge_exploratory_responses(&mut response_ledger, &mut planner, 7);
    complete_planner(&mut response_ledger, &mut planner);
    assert_eq!(
        planner.usage().model_responses(),
        PLANNER_ROLE_RESPONSE_LIMIT
    );
    assert!(matches!(
        response_ledger.begin_exploratory_provider_request(&planner, b"q", 1),
        Err(BudgetError::RoleAlreadyTerminated {
            termination: RoleBudgetTermination::Normal
        })
    ));
    assert_eq!(
        planner.usage().model_responses(),
        PLANNER_ROLE_RESPONSE_LIMIT
    );

    let mut call_ledger = TaskBudgetLedger::new();
    let mut planner = call_ledger.start_planner().unwrap();
    charge_exploratory_runtime_calls(&mut call_ledger, &mut planner, 11, b"");
    complete_planner(&mut call_ledger, &mut planner);
    assert_eq!(
        planner.usage().model_visible_calls(),
        PLANNER_ROLE_CALL_LIMIT
    );
    assert!(matches!(
        call_ledger.begin_exploratory_provider_request(&planner, b"q", 1),
        Err(BudgetError::RoleAlreadyTerminated {
            termination: RoleBudgetTermination::Normal
        })
    ));
    assert_eq!(
        planner.usage().model_visible_calls(),
        PLANNER_ROLE_CALL_LIMIT
    );

    let mut retained_ledger = TaskBudgetLedger::new();
    let mut planner = retained_ledger.start_planner().unwrap();
    charge_one_exact_runtime_result(
        &mut retained_ledger,
        &mut planner,
        PLANNER_RETAINED_RESULT_LIMIT,
    );
    let mut receipt = open_exploratory_response(&mut retained_ledger, &mut planner);
    let result = retained_ledger
        .charge_exploratory_tool_call(&mut planner, &mut receipt, runtime_action())
        .unwrap();
    assert!(matches!(
        retained_ledger.retain_exploratory_tool_result(&mut planner, &result, &retained(b"x")),
        Err(BudgetError::RoleLimitExceeded {
            role: BudgetRole::Planner,
            resource: BudgetResource::RetainedResultBytes
        })
    ));
    assert_eq!(
        planner.usage().retained_result_bytes(),
        PLANNER_RETAINED_RESULT_LIMIT
    );
}

#[test]
fn checked_provider_arithmetic_and_failed_preflight_are_atomic() {
    let mut ledger = TaskBudgetLedger::new();
    let planner = ledger.start_planner().unwrap();
    assert!(matches!(
        ledger.begin_exploratory_provider_request(&planner, b"x", usize::MAX),
        Err(BudgetError::ArithmeticOverflow {
            resource: BudgetResource::ProviderBytes
        })
    ));
    assert_eq!(
        ledger.usage(),
        coding_agent_core::TaskBudgetUsage::default()
    );
}
