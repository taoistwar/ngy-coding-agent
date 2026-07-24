use coding_agent_core::{
    ActionRequest, AllowedActions, BlockedSubmission, ContextRedactor, ControlKind, ControlRequest,
    ExecutionSubmission, ModelToolChoice, PlanProgressSubmission, PlanSubmission, RequiredAction,
    RequiredCheck, ReviewSubmission, Role, RuntimeActionRequest, ToolCall, ToolCallBatch,
    ToolRequest, validate_action_batch, validate_role_response,
};

struct SecretRedactor;

impl ContextRedactor for SecretRedactor {
    fn redact(&self, content: &str) -> String {
        content.replace("known-secret", "<redacted>")
    }
}

fn batch(calls: Vec<ToolCall>) -> ToolCallBatch {
    ToolCallBatch {
        assistant_content: None,
        reasoning_content: None,
        calls,
    }
}

fn call(id: &str, role: Role, name: &str, arguments: &str) -> ToolCall {
    ToolCall {
        id: id.to_owned(),
        request: ActionRequest::decode(role, name, arguments).expect("valid action fixture"),
    }
}

#[test]
fn role_action_sets_are_exact_and_controls_never_become_tool_runtime_requests() {
    let planner = AllowedActions::for_role(Role::Planner);
    assert_eq!(
        planner.names(),
        [
            "list_files",
            "read_file",
            "search_text",
            "submit_plan",
            "report_blocked",
        ]
    );
    let executor = AllowedActions::for_role(Role::Executor);
    assert!(executor.allows_name("replace_file"));
    assert!(executor.allows_name("update_plan_progress"));
    assert!(!executor.allows_name("submit_review"));
    let reviewer = AllowedActions::for_role(Role::Reviewer);
    assert!(reviewer.allows_name("review_diff_manifest"));
    assert!(reviewer.allows_name("review_diff_chunks"));
    assert!(!reviewer.allows_name("replace_file"));
    assert!(!reviewer.allows_name("update_plan_progress"));

    let control =
        ActionRequest::decode(Role::Executor, "submit_execution", r#"{"summary":"ready"}"#)
            .unwrap();
    assert!(control.as_tool_request().is_none());
}

#[test]
fn terminal_and_progress_controls_are_solo_batches_and_fail_before_execution() {
    let mixed = batch(vec![
        call("runtime", Role::Executor, "git_status", r#"{}"#),
        call(
            "terminal",
            Role::Executor,
            "submit_execution",
            r#"{"summary":"ready"}"#,
        ),
    ]);
    assert!(
        validate_action_batch(
            Role::Executor,
            &mixed,
            &ModelToolChoice::Auto,
            &SecretRedactor,
        )
        .is_err()
    );

    let progress_mixed = batch(vec![
        call(
            "progress",
            Role::Executor,
            "update_plan_progress",
            r#"{"updates":[{"step_id":"step-1","status":"completed"}]}"#,
        ),
        call("runtime", Role::Executor, "git_status", r#"{}"#),
    ]);
    assert!(
        validate_action_batch(
            Role::Executor,
            &progress_mixed,
            &ModelToolChoice::Auto,
            &SecretRedactor,
        )
        .is_err()
    );
}

#[test]
fn strict_control_decoding_rejects_unknown_duplicate_and_nested_bound_violations() {
    for invalid in [
        r#"{"summary":"ok","unknown":true}"#.to_owned(),
        r#"{"summary":"first","summary":"second"}"#.to_owned(),
        format!(r#"{{"summary":"{}"}}"#, "x".repeat(4_097)),
    ] {
        assert!(
            ActionRequest::decode(Role::Executor, "submit_execution", &invalid).is_err(),
            "{invalid}"
        );
    }

    let too_many_criteria = (0..9)
        .map(|index| format!(r#""criterion-{index}""#))
        .collect::<Vec<_>>()
        .join(",");
    let invalid_plan = format!(
        r#"{{"summary":"plan","steps":[{{"title":"step","description":"","acceptance_criteria":[{too_many_criteria}]}}],"initial_required_checks":[{{"kind":"cargo_test","package":null,"integration_test":null}}]}}"#
    );
    assert!(ActionRequest::decode(Role::Planner, "submit_plan", &invalid_plan).is_err());

    assert!(
        ActionRequest::decode(
            Role::Reviewer,
            "submit_review",
            r#"{"verdict":"approved","summary":"no","findings":[{"severity":"unknown","message":"x","path":null,"line":null}],"add_required_checks":[]}"#,
        )
        .is_err()
    );
}

#[test]
fn canonical_arguments_are_stable_and_redaction_mutation_is_rejected() {
    let left = ActionRequest::decode(
        Role::Executor,
        "submit_execution",
        "{ \"summary\" : \"ready\" }",
    )
    .unwrap();
    let right = ActionRequest::decode(Role::Executor, "submit_execution", r#"{"summary":"ready"}"#)
        .unwrap();
    assert_eq!(
        left.canonical_arguments().unwrap(),
        right.canonical_arguments().unwrap()
    );
    assert!(left.is_redaction_stable(&SecretRedactor));

    let secret = ActionRequest::decode(
        Role::Executor,
        "submit_execution",
        r#"{"summary":"known-secret"}"#,
    )
    .unwrap();
    assert!(!secret.is_redaction_stable(&SecretRedactor));
    let secret_batch = batch(vec![ToolCall {
        id: "terminal".to_owned(),
        request: secret,
    }]);
    assert!(
        validate_action_batch(
            Role::Executor,
            &secret_batch,
            &ModelToolChoice::Auto,
            &SecretRedactor,
        )
        .is_err()
    );
}

#[test]
fn typed_required_validation_matches_id_kind_and_complete_selector() {
    let required = RequiredCheck::try_cargo_test(
        "check-7",
        Some("coding-agent-core".to_owned()),
        Some("role_contracts".to_owned()),
    )
    .unwrap();
    let choice = ModelToolChoice::Required(RequiredAction::Validation(required));
    let exact = batch(vec![call(
        "required",
        Role::Executor,
        "cargo_test",
        r#"{"check_id":"check-7","package":"coding-agent-core","integration_test":"role_contracts"}"#,
    )]);
    validate_action_batch(Role::Executor, &exact, &choice, &SecretRedactor).unwrap();

    for arguments in [
        r#"{"check_id":"check-8","package":"coding-agent-core","integration_test":"role_contracts"}"#,
        r#"{"check_id":"check-7","package":"coding-agent-provider","integration_test":"role_contracts"}"#,
        r#"{"check_id":"check-7","package":"coding-agent-core","integration_test":"protocol"}"#,
    ] {
        let wrong = batch(vec![call(
            "required",
            Role::Executor,
            "cargo_test",
            arguments,
        )]);
        assert!(validate_action_batch(Role::Executor, &wrong, &choice, &SecretRedactor).is_err());
    }
}

#[test]
fn coverage_and_terminal_required_actions_match_every_authoritative_field() {
    let digest = coding_agent_core::WorkspaceDigest::try_new("a".repeat(64)).unwrap();
    let coverage =
        RequiredAction::review_diff_chunks(7, digest.clone(), "b".repeat(64), 2, 2).unwrap();
    let exact = batch(vec![call(
        "chunks",
        Role::Reviewer,
        "review_diff_chunks",
        &format!(
            r#"{{"generation":7,"workspace_digest":{{"algorithm":"workspace_fingerprint_v1","value":"{}"}},"manifest_sha256":"{}","start_chunk":2,"count":2}}"#,
            "a".repeat(64),
            "b".repeat(64)
        ),
    )]);
    validate_action_batch(
        Role::Reviewer,
        &exact,
        &ModelToolChoice::Required(coverage),
        &SecretRedactor,
    )
    .unwrap();

    let terminal = batch(vec![call(
        "terminal",
        Role::Reviewer,
        "submit_review",
        r#"{"verdict":"changes_requested","summary":"fix","findings":[{"severity":"blocking","message":"bug","path":null,"line":null}],"add_required_checks":[]}"#,
    )]);
    validate_action_batch(
        Role::Reviewer,
        &terminal,
        &ModelToolChoice::Required(RequiredAction::Terminal(ControlKind::SubmitReview)),
        &SecretRedactor,
    )
    .unwrap();
    assert!(
        validate_action_batch(
            Role::Reviewer,
            &terminal,
            &ModelToolChoice::Required(RequiredAction::Terminal(ControlKind::ReportBlocked)),
            &SecretRedactor,
        )
        .is_err()
    );
}

#[test]
fn empty_duplicate_wrong_role_and_non_exact_required_batches_fail_closed() {
    assert!(
        validate_action_batch(
            Role::Planner,
            &batch(vec![]),
            &ModelToolChoice::Auto,
            &SecretRedactor,
        )
        .is_err()
    );

    let duplicate = batch(vec![
        call(
            "same",
            Role::Planner,
            "list_files",
            r#"{"path":"","depth":1,"limit":1}"#,
        ),
        call(
            "same",
            Role::Planner,
            "read_file",
            r#"{"path":"README.md","start_line":1,"end_line":1}"#,
        ),
    ]);
    assert!(
        validate_action_batch(
            Role::Planner,
            &duplicate,
            &ModelToolChoice::Auto,
            &SecretRedactor,
        )
        .is_err()
    );

    assert!(
        ActionRequest::decode(
            Role::Planner,
            "replace_file",
            r#"{"path":"x","expected_sha256":null,"content":"x"}"#
        )
        .is_err()
    );

    let required = RequiredCheck::try_cargo_test("check-1", None, None).unwrap();
    let choice = ModelToolChoice::Required(RequiredAction::Validation(required));
    assert!(
        validate_action_batch(Role::Executor, &batch(vec![]), &choice, &SecretRedactor,).is_err()
    );
    let two = batch(vec![
        call(
            "one",
            Role::Executor,
            "cargo_test",
            r#"{"check_id":"check-1","package":null,"integration_test":null}"#,
        ),
        call(
            "two",
            Role::Executor,
            "cargo_test",
            r#"{"check_id":"check-1","package":null,"integration_test":null}"#,
        ),
    ]);
    assert!(validate_action_batch(Role::Executor, &two, &choice, &SecretRedactor).is_err());
}

#[test]
fn hand_constructed_legacy_cargo_cannot_bypass_role_validation_authority() {
    for role in [Role::Executor, Role::Reviewer] {
        let bypass = batch(vec![ToolCall {
            id: "bypass".to_owned(),
            request: ActionRequest::runtime(ToolRequest::CargoTest {
                package: None,
                test: None,
                timeout_ms: 999_999,
            }),
        }]);
        assert!(
            validate_action_batch(role, &bypass, &ModelToolChoice::Auto, &SecretRedactor).is_err()
        );
    }
}

#[test]
fn all_control_semantics_and_nested_duplicate_fields_fail_closed() {
    let valid_plan = r#"{"summary":"plan","steps":[{"title":"step","description":"","acceptance_criteria":["done"]}],"initial_required_checks":[{"kind":"cargo_test","package":null,"integration_test":null}]}"#;
    ActionRequest::decode(Role::Planner, "submit_plan", valid_plan).unwrap();

    for invalid in [
        r#"{"summary":"plan","steps":[],"initial_required_checks":[{"kind":"cargo_test","package":null,"integration_test":null}]}"#,
        r#"{"summary":"plan","steps":[{"title":"step","description":"","acceptance_criteria":["done"]}],"initial_required_checks":[{"kind":"cargo_check","package":null}]}"#,
        r#"{"summary":"plan","steps":[{"title":"step","description":"","acceptance_criteria":["done"]}],"initial_required_checks":[{"kind":"cargo_test","package":null,"integration_test":null},{"kind":"cargo_test","package":null,"integration_test":null}]}"#,
        r#"{"summary":"plan","steps":[{"title":"step","title":"duplicate","description":"","acceptance_criteria":["done"]}],"initial_required_checks":[{"kind":"cargo_test","package":null,"integration_test":null}]}"#,
    ] {
        assert!(ActionRequest::decode(Role::Planner, "submit_plan", invalid).is_err());
    }

    assert!(
        ActionRequest::decode(
            Role::Reviewer,
            "submit_review",
            r#"{"verdict":"approved","summary":"bad","findings":[{"severity":"blocking","message":"bug","path":null,"line":null}],"add_required_checks":[]}"#,
        )
        .is_err()
    );
    assert!(
        ActionRequest::decode(
            Role::Reviewer,
            "submit_review",
            r#"{"verdict":"approved","summary":"","findings":[],"add_required_checks":[]}"#,
        )
        .is_err()
    );
    assert!(
        ActionRequest::decode(
            Role::Reviewer,
            "submit_review",
            r#"{"verdict":"changes_requested","summary":"bad","findings":[],"add_required_checks":[]}"#,
        )
        .is_err()
    );
    assert!(
        ActionRequest::decode(
            Role::Reviewer,
            "submit_review",
            r#"{"verdict":"changes_requested","summary":"bad","findings":[{"severity":"blocking","message":"one","message":"two","path":null,"line":null}],"add_required_checks":[]}"#,
        )
        .is_err()
    );
    assert!(
        ActionRequest::decode(
            Role::Planner,
            "report_blocked",
            r#"{"reason":"arbitrary","summary":"no"}"#,
        )
        .is_err()
    );
    assert!(
        ActionRequest::decode(
            Role::Executor,
            "update_plan_progress",
            r#"{"updates":[{"step_id":"step-1","status":"pending"}]}"#,
        )
        .is_err()
    );
    assert!(
        ActionRequest::decode(
            Role::Executor,
            "update_plan_progress",
            r#"{"updates":[{"step_id":"step-1","status":"running"},{"step_id":"step-1","status":"completed"}]}"#,
        )
        .is_err()
    );
}

#[test]
fn canonical_control_document_limit_is_enforced_after_structural_validation() {
    let steps = (0..32)
        .map(|index| {
            serde_json::json!({
                "title": format!("step-{index}"),
                "description": "x".repeat(3_000),
                "acceptance_criteria": ["done"]
            })
        })
        .collect::<Vec<_>>();
    let arguments = serde_json::to_string(&serde_json::json!({
        "summary": "",
        "steps": steps,
        "initial_required_checks": [
            {"kind": "cargo_test", "package": null, "integration_test": null}
        ]
    }))
    .unwrap();
    assert!(arguments.len() > 64 * 1024);
    assert!(ActionRequest::decode(Role::Planner, "submit_plan", &arguments).is_err());
}

#[test]
fn serde_constructed_invalid_controls_fail_the_core_second_gate() {
    let invalid_plan = serde_json::from_value::<PlanSubmission>(serde_json::json!({
        "summary": "plan",
        "steps": [],
        "initial_required_checks": []
    }))
    .unwrap();
    let invalid_execution = serde_json::from_value::<ExecutionSubmission>(serde_json::json!({
        "summary": "x".repeat(4_097)
    }))
    .unwrap();
    let invalid_review = serde_json::from_value::<ReviewSubmission>(serde_json::json!({
        "verdict": "approved",
        "summary": "bad",
        "findings": [{
            "severity": "blocking",
            "message": "must fix",
            "path": null,
            "line": null
        }],
        "add_required_checks": []
    }))
    .unwrap();
    let empty_review = serde_json::from_value::<ReviewSubmission>(serde_json::json!({
        "verdict": "approved",
        "summary": "",
        "findings": [],
        "add_required_checks": []
    }))
    .unwrap();
    let invalid_blocked = serde_json::from_value::<BlockedSubmission>(serde_json::json!({
        "reason": "missing_required_context",
        "summary": "x".repeat(4_097)
    }))
    .unwrap();
    let invalid_progress = serde_json::from_value::<PlanProgressSubmission>(serde_json::json!({
        "updates": []
    }))
    .unwrap();

    let invalid = [
        (
            Role::Planner,
            ActionRequest::Control(ControlRequest::SubmitPlan(invalid_plan)),
        ),
        (
            Role::Executor,
            ActionRequest::Control(ControlRequest::SubmitExecution(invalid_execution)),
        ),
        (
            Role::Reviewer,
            ActionRequest::Control(ControlRequest::SubmitReview(invalid_review)),
        ),
        (
            Role::Reviewer,
            ActionRequest::Control(ControlRequest::SubmitReview(empty_review)),
        ),
        (
            Role::Planner,
            ActionRequest::Control(ControlRequest::ReportBlocked(invalid_blocked)),
        ),
        (
            Role::Executor,
            ActionRequest::Control(ControlRequest::UpdatePlanProgress(invalid_progress)),
        ),
    ];

    for (role, request) in invalid {
        assert!(request.validate().is_err(), "{}", request.name());
        let invalid_batch = batch(vec![ToolCall {
            id: "bypass".to_owned(),
            request,
        }]);
        assert!(
            validate_action_batch(
                role,
                &invalid_batch,
                &ModelToolChoice::Auto,
                &SecretRedactor,
            )
            .is_err()
        );
    }
}

#[test]
fn invalid_public_runtime_arguments_fail_whole_batch_preflight() {
    assert!(
        ActionRequest::decode(
            Role::Planner,
            "list_files",
            r#"{"path":"","depth":0,"limit":0}"#,
        )
        .is_err()
    );

    let invalid_tools = [
        ToolRequest::ListFiles {
            path: String::new(),
            depth: 0,
            limit: 1,
        },
        ToolRequest::ReadFile {
            path: String::new(),
            start_line: 1,
            end_line: 1,
        },
        ToolRequest::SearchText {
            query: String::new(),
            path: String::new(),
            glob: None,
            limit: 1,
        },
        ToolRequest::ReplaceFile {
            path: "src/lib.rs".to_owned(),
            expected_sha256: Some("not-a-digest".to_owned()),
            content: String::new(),
        },
        ToolRequest::CargoCheck {
            package: None,
            timeout_ms: 0,
        },
        ToolRequest::CargoTest {
            package: None,
            test: Some(String::new()),
            timeout_ms: 1,
        },
    ];
    for request in invalid_tools {
        assert!(ActionRequest::runtime(request).validate().is_err());
    }

    let invalid_diff_requests = [
        ActionRequest::Runtime(RuntimeActionRequest::ReviewDiffManifest {
            generation: 9_007_199_254_740_992,
            workspace_digest: coding_agent_core::WorkspaceDigest::try_new("a".repeat(64)).unwrap(),
        }),
        ActionRequest::Runtime(RuntimeActionRequest::ReviewDiffChunks {
            generation: 1,
            workspace_digest: coding_agent_core::WorkspaceDigest::try_new("a".repeat(64)).unwrap(),
            manifest_sha256: "B".repeat(64),
            start_chunk: 0,
            count: 0,
        }),
    ];
    for request in invalid_diff_requests {
        assert!(request.validate().is_err());
    }

    let mixed = batch(vec![
        call(
            "write",
            Role::Executor,
            "replace_file",
            r#"{"path":"src/lib.rs","expected_sha256":null,"content":"changed"}"#,
        ),
        ToolCall {
            id: "invalid-read".to_owned(),
            request: ActionRequest::runtime(ToolRequest::ReadFile {
                path: "README.md".to_owned(),
                start_line: 0,
                end_line: 1,
            }),
        },
    ]);
    assert!(
        validate_action_batch(
            Role::Executor,
            &mixed,
            &ModelToolChoice::Auto,
            &SecretRedactor,
        )
        .is_err()
    );
}

#[test]
fn every_nested_count_scalar_and_enum_bound_is_authoritatively_enforced() {
    let too_many_steps = (0..33)
        .map(|index| {
            serde_json::json!({
                "title": format!("step-{index}"),
                "description": "",
                "acceptance_criteria": ["done"]
            })
        })
        .collect::<Vec<_>>();
    let plan = serde_json::to_string(&serde_json::json!({
        "summary": "",
        "steps": too_many_steps,
        "initial_required_checks": [
            {"kind": "cargo_test", "package": null, "integration_test": null}
        ]
    }))
    .unwrap();
    assert!(ActionRequest::decode(Role::Planner, "submit_plan", &plan).is_err());

    let long_criterion = serde_json::to_string(&serde_json::json!({
        "summary": "",
        "steps": [{
            "title": "step",
            "description": "",
            "acceptance_criteria": ["界".repeat(1_025)]
        }],
        "initial_required_checks": [
            {"kind": "cargo_test", "package": null, "integration_test": null}
        ]
    }))
    .unwrap();
    assert!(ActionRequest::decode(Role::Planner, "submit_plan", &long_criterion).is_err());

    let blocked = serde_json::to_string(&serde_json::json!({
        "reason": "missing_required_context",
        "summary": "界".repeat(4_097)
    }))
    .unwrap();
    assert!(ActionRequest::decode(Role::Planner, "report_blocked", &blocked).is_err());

    let updates = (0..33)
        .map(|index| serde_json::json!({"step_id": format!("step-{index}"), "status": "completed"}))
        .collect::<Vec<_>>();
    let progress = serde_json::to_string(&serde_json::json!({"updates": updates})).unwrap();
    assert!(ActionRequest::decode(Role::Executor, "update_plan_progress", &progress).is_err());
}

#[test]
fn invalid_public_required_variants_and_batch_metadata_redaction_cannot_bypass_preflight() {
    let invalid_required =
        ModelToolChoice::Required(RequiredAction::Terminal(ControlKind::UpdatePlanProgress));
    let progress = batch(vec![call(
        "progress",
        Role::Executor,
        "update_plan_progress",
        r#"{"updates":[{"step_id":"step-1","status":"completed"}]}"#,
    )]);
    assert!(
        validate_action_batch(
            Role::Executor,
            &progress,
            &invalid_required,
            &SecretRedactor,
        )
        .is_err()
    );

    let metadata_secret = ToolCallBatch {
        assistant_content: Some("known-secret".to_owned()),
        reasoning_content: None,
        calls: vec![call("status", Role::Executor, "git_status", r#"{}"#)],
    };
    assert!(
        validate_action_batch(
            Role::Executor,
            &metadata_secret,
            &ModelToolChoice::Auto,
            &SecretRedactor,
        )
        .is_err()
    );
}

#[test]
fn reviewer_coverage_actions_are_usable_only_as_matching_required_turns() {
    let digest = "a".repeat(64);
    let manifest = batch(vec![call(
        "manifest",
        Role::Reviewer,
        "review_diff_manifest",
        &format!(
            r#"{{"generation":1,"workspace_digest":{{"algorithm":"workspace_fingerprint_v1","value":"{digest}"}}}}"#
        ),
    )]);
    assert!(
        validate_action_batch(
            Role::Reviewer,
            &manifest,
            &ModelToolChoice::Auto,
            &SecretRedactor,
        )
        .is_err()
    );
    let workspace_digest = coding_agent_core::WorkspaceDigest::try_new(digest).unwrap();
    validate_action_batch(
        Role::Reviewer,
        &manifest,
        &ModelToolChoice::Required(
            RequiredAction::review_diff_manifest(1, workspace_digest).unwrap(),
        ),
        &SecretRedactor,
    )
    .unwrap();
}

#[test]
fn ordinary_final_text_never_terminates_a_role() {
    assert!(
        validate_role_response(
            Role::Planner,
            &coding_agent_core::ModelResponse::Final {
                content: "unstructured plan".to_owned(),
            },
            &ModelToolChoice::Auto,
            &SecretRedactor,
        )
        .is_err()
    );
}
