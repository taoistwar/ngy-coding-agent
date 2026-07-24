use coding_agent_core::{
    ActionRequest, AllowedActions, ControlKind, ControlRequest, ModelMessage, ModelRequest,
    ModelResponse, ModelToolChoice, RequiredAction, RequiredCheck, Role, RoleLoopError,
    RuntimeActionRequest,
};
use coding_agent_provider::{
    PROVIDER_REQUEST_REJECTED, PROVIDER_RESPONSE_ROLE_ACTION_NOT_ALLOWED,
    PROVIDER_RESPONSE_ROLE_OUTPUT_INVALID, ProviderToolChoiceCompatibility,
    decode_chat_completions_response_for_request, encode_chat_completions_request,
    encode_chat_completions_request_with_compatibility,
};

fn request(role: Role, tool_choice: ModelToolChoice) -> ModelRequest {
    ModelRequest {
        messages: vec![
            ModelMessage::system("role policy"),
            ModelMessage::user("bounded role input"),
        ],
        allowed_actions: AllowedActions::for_role(role),
        tool_choice,
    }
}

fn tool_response(calls: serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": null,
                "tool_calls": calls
            },
            "finish_reason": "tool_calls"
        }]
    }))
    .unwrap()
}

fn call(id: &str, name: &str, arguments: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "type": "function",
        "function": {
            "name": name,
            "arguments": serde_json::to_string(&arguments).unwrap()
        }
    })
}

fn encoded_body(request: &ModelRequest) -> serde_json::Value {
    serde_json::from_slice(
        &encode_chat_completions_request("coding-model", request).expect("encode role request"),
    )
    .unwrap()
}

fn tool_names(body: &serde_json::Value) -> Vec<&str> {
    body["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["function"]["name"].as_str().unwrap())
        .collect()
}

#[test]
fn each_role_exposes_only_its_exact_action_set() {
    let planner = encoded_body(&request(Role::Planner, ModelToolChoice::Auto));
    assert_eq!(
        tool_names(&planner),
        [
            "list_files",
            "read_file",
            "search_text",
            "submit_plan",
            "report_blocked",
        ]
    );

    let executor = encoded_body(&request(Role::Executor, ModelToolChoice::Auto));
    assert_eq!(
        tool_names(&executor),
        [
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
        ]
    );
    let cargo_test = executor["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["function"]["name"] == "cargo_test")
        .unwrap();
    let properties = &cargo_test["function"]["parameters"]["properties"];
    assert!(properties.get("timeout_ms").is_none());
    assert_eq!(
        cargo_test["function"]["parameters"]["required"],
        serde_json::json!(["package", "integration_test"])
    );

    let reviewer = encoded_body(&request(Role::Reviewer, ModelToolChoice::Auto));
    assert_eq!(
        tool_names(&reviewer),
        [
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
        ]
    );
    let submit_review = reviewer["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["function"]["name"] == "submit_review")
        .unwrap();
    assert_eq!(
        submit_review["function"]["parameters"]["properties"]["summary"]["minLength"],
        1
    );
}

#[test]
fn executor_required_validation_schema_uses_exact_consts_in_all_wire_modes() {
    let required = RequiredCheck::try_cargo_test(
        "check-9",
        Some("coding-agent-core".to_owned()),
        Some("role_contracts".to_owned()),
    )
    .unwrap();
    let request = request(
        Role::Executor,
        ModelToolChoice::Required(RequiredAction::Validation(required)),
    );

    for (mode, expected_choice) in [
        (
            ProviderToolChoiceCompatibility::Strict,
            serde_json::json!({
                "type": "function",
                "function": {"name": "cargo_test"}
            }),
        ),
        (
            ProviderToolChoiceCompatibility::RequiredAsRequired,
            serde_json::json!("required"),
        ),
        (
            ProviderToolChoiceCompatibility::RequiredAsAuto,
            serde_json::json!("auto"),
        ),
    ] {
        let encoded =
            encode_chat_completions_request_with_compatibility("coding-model", &request, mode)
                .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(body["tool_choice"], expected_choice);
        assert_eq!(body["tools"].as_array().unwrap().len(), 1);
        let parameters = &body["tools"][0]["function"]["parameters"];
        assert_eq!(parameters["properties"]["check_id"]["const"], "check-9");
        assert_eq!(
            parameters["properties"]["package"]["const"],
            "coding-agent-core"
        );
        assert_eq!(
            parameters["properties"]["integration_test"]["const"],
            "role_contracts"
        );
        assert_eq!(parameters["additionalProperties"], false);
    }
}

#[test]
fn required_coverage_and_terminal_schemas_bind_authoritative_fields() {
    let digest = coding_agent_core::WorkspaceDigest::try_new("a".repeat(64)).unwrap();
    let chunks = request(
        Role::Reviewer,
        ModelToolChoice::Required(
            RequiredAction::review_diff_chunks(3, digest, "b".repeat(64), 4, 2).unwrap(),
        ),
    );
    let body = encoded_body(&chunks);
    let properties = &body["tools"][0]["function"]["parameters"]["properties"];
    assert_eq!(properties["generation"]["const"], 3);
    assert_eq!(
        properties["workspace_digest"]["properties"]["algorithm"]["const"],
        "workspace_fingerprint_v1"
    );
    assert_eq!(
        properties["workspace_digest"]["properties"]["value"]["const"],
        "a".repeat(64)
    );
    assert_eq!(properties["manifest_sha256"]["const"], "b".repeat(64));
    assert_eq!(properties["start_chunk"]["const"], 4);
    assert_eq!(properties["count"]["const"], 2);

    let terminal = request(
        Role::Reviewer,
        ModelToolChoice::Required(RequiredAction::terminal(ControlKind::SubmitReview).unwrap()),
    );
    let body = encoded_body(&terminal);
    assert_eq!(tool_names(&body), ["submit_review"]);
}

#[test]
fn reviewer_coverage_or_terminal_is_exact_and_fail_closed_in_all_wire_modes() {
    let digest = coding_agent_core::WorkspaceDigest::try_new("a".repeat(64)).unwrap();
    let required =
        RequiredAction::review_diff_chunks_or_terminal(3, digest, "b".repeat(64), 4, 2).unwrap();
    let review_request = request(Role::Reviewer, ModelToolChoice::Required(required));
    for (compatibility, expected_choice) in [
        (
            ProviderToolChoiceCompatibility::Strict,
            serde_json::json!("required"),
        ),
        (
            ProviderToolChoiceCompatibility::RequiredAsRequired,
            serde_json::json!("required"),
        ),
        (
            ProviderToolChoiceCompatibility::RequiredAsAuto,
            serde_json::json!("auto"),
        ),
    ] {
        let encoded = encode_chat_completions_request_with_compatibility(
            "coding-model",
            &review_request,
            compatibility,
        )
        .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(body["tool_choice"], expected_choice);
        assert_eq!(
            tool_names(&body),
            ["review_diff_chunks", "submit_review", "report_blocked"]
        );
        let coverage = &body["tools"][0]["function"]["parameters"]["properties"];
        assert_eq!(coverage["generation"]["const"], 3);
        assert_eq!(coverage["manifest_sha256"]["const"], "b".repeat(64));
        assert_eq!(coverage["start_chunk"]["const"], 4);
        assert_eq!(coverage["count"]["const"], 2);
        assert_eq!(
            body["tools"][1]["function"]["parameters"]["properties"]["verdict"]["const"],
            "changes_requested"
        );
    }

    let exact_coverage = tool_response(serde_json::json!([call(
        "chunks",
        "review_diff_chunks",
        serde_json::json!({
            "generation": 3,
            "workspace_digest": {
                "algorithm": "workspace_fingerprint_v1",
                "value": "a".repeat(64)
            },
            "manifest_sha256": "b".repeat(64),
            "start_chunk": 4,
            "count": 2
        }),
    )]));
    decode_chat_completions_response_for_request(&exact_coverage, 64 * 1024, &review_request)
        .unwrap();

    let changes = tool_response(serde_json::json!([call(
        "changes",
        "submit_review",
        serde_json::json!({
            "verdict": "changes_requested",
            "summary": "blocking issue",
            "findings": [{
                "severity": "blocking",
                "message": "fix this",
                "path": "src/lib.rs",
                "line": 1
            }],
            "add_required_checks": []
        }),
    )]));
    decode_chat_completions_response_for_request(&changes, 64 * 1024, &review_request).unwrap();

    let blocked = tool_response(serde_json::json!([call(
        "blocked",
        "report_blocked",
        serde_json::json!({
            "reason": "missing_required_context",
            "summary": "context unavailable"
        }),
    )]));
    decode_chat_completions_response_for_request(&blocked, 64 * 1024, &review_request).unwrap();

    let approved = tool_response(serde_json::json!([call(
        "approved",
        "submit_review",
        serde_json::json!({
            "verdict": "approved",
            "summary": "premature",
            "findings": [],
            "add_required_checks": []
        }),
    )]));
    let error = decode_chat_completions_response_for_request(&approved, 64 * 1024, &review_request)
        .unwrap_err();
    assert_eq!(error.code, PROVIDER_RESPONSE_ROLE_OUTPUT_INVALID);
    assert_eq!(
        RoleLoopError::Provider(error).reviewer_failure_code(),
        Some("REVIEWER_INVALID_OUTPUT")
    );

    for malformed in [
        tool_response(serde_json::json!([call(
            "empty-summary",
            "submit_review",
            serde_json::json!({
                "verdict": "changes_requested",
                "summary": "",
                "findings": [{
                    "severity": "blocking",
                    "message": "fix this",
                    "path": null,
                    "line": null
                }],
                "add_required_checks": []
            }),
        )])),
        tool_response(serde_json::json!([call(
            "changes-without-blocking",
            "submit_review",
            serde_json::json!({
                "verdict": "changes_requested",
                "summary": "invalid relation",
                "findings": [],
                "add_required_checks": []
            }),
        )])),
        tool_response(serde_json::json!([call(
            "approved-with-blocking",
            "submit_review",
            serde_json::json!({
                "verdict": "approved",
                "summary": "invalid relation",
                "findings": [{
                    "severity": "blocking",
                    "message": "cannot approve",
                    "path": null,
                    "line": null
                }],
                "add_required_checks": []
            }),
        )])),
    ] {
        let error =
            decode_chat_completions_response_for_request(&malformed, 64 * 1024, &review_request)
                .unwrap_err();
        assert_eq!(error.code, PROVIDER_RESPONSE_ROLE_OUTPUT_INVALID);
        assert_eq!(
            RoleLoopError::Provider(error).reviewer_failure_code(),
            Some("REVIEWER_INVALID_OUTPUT")
        );
    }

    let wrong_coverage = tool_response(serde_json::json!([call(
        "wrong",
        "review_diff_chunks",
        serde_json::json!({
            "generation": 3,
            "workspace_digest": {
                "algorithm": "workspace_fingerprint_v1",
                "value": "a".repeat(64)
            },
            "manifest_sha256": "b".repeat(64),
            "start_chunk": 5,
            "count": 1
        }),
    )]));
    let error =
        decode_chat_completions_response_for_request(&wrong_coverage, 64 * 1024, &review_request)
            .unwrap_err();
    assert_eq!(error.code, PROVIDER_RESPONSE_ROLE_ACTION_NOT_ALLOWED);

    let multiple = tool_response(serde_json::json!([
        call(
            "changes",
            "submit_review",
            serde_json::json!({
                "verdict": "changes_requested",
                "summary": "blocking issue",
                "findings": [{
                    "severity": "blocking",
                    "message": "fix this",
                    "path": null,
                    "line": null
                }],
                "add_required_checks": []
            })
        ),
        call(
            "blocked",
            "report_blocked",
            serde_json::json!({
                "reason": "missing_required_context",
                "summary": "context unavailable"
            })
        )
    ]));
    let error = decode_chat_completions_response_for_request(&multiple, 64 * 1024, &review_request)
        .unwrap_err();
    assert_eq!(error.code, PROVIDER_RESPONSE_ROLE_ACTION_NOT_ALLOWED);

    let final_text =
        br#"{"choices":[{"index":0,"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}]}"#;
    let error =
        decode_chat_completions_response_for_request(final_text, 64 * 1024, &review_request)
            .unwrap_err();
    assert_eq!(error.code, PROVIDER_RESPONSE_ROLE_OUTPUT_INVALID);

    let manifest = request(
        Role::Reviewer,
        ModelToolChoice::Required(
            RequiredAction::review_diff_manifest_or_terminal(
                3,
                coding_agent_core::WorkspaceDigest::try_new("a".repeat(64)).unwrap(),
            )
            .unwrap(),
        ),
    );
    let body = encoded_body(&manifest);
    assert_eq!(
        tool_names(&body),
        ["review_diff_manifest", "submit_review", "report_blocked"]
    );
}

#[test]
fn planner_terminal_or_blocked_convergence_is_a_typed_two_schema_required_exception() {
    let choice = ModelToolChoice::Required(
        RequiredAction::terminal_or_blocked(ControlKind::SubmitPlan).unwrap(),
    );
    let request = request(Role::Planner, choice);

    for (compatibility, expected_choice) in [
        (
            ProviderToolChoiceCompatibility::Strict,
            serde_json::json!("required"),
        ),
        (
            ProviderToolChoiceCompatibility::RequiredAsRequired,
            serde_json::json!("required"),
        ),
        (
            ProviderToolChoiceCompatibility::RequiredAsAuto,
            serde_json::json!("auto"),
        ),
    ] {
        let encoded = encode_chat_completions_request_with_compatibility(
            "coding-model",
            &request,
            compatibility,
        )
        .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(
            body["tool_choice"], expected_choice,
            "strict is the named-choice exception while RequiredAsAuto remains auto"
        );
        assert_eq!(tool_names(&body), ["submit_plan", "report_blocked"]);
    }

    let plan_arguments = serde_json::json!({
        "summary": "bounded plan",
        "steps": [{
            "title": "implement",
            "description": "implement the requested change",
            "acceptance_criteria": ["the focused test passes"]
        }],
        "initial_required_checks": [{
            "kind": "cargo_test",
            "package": "coding-agent-core",
            "integration_test": "role_loop"
        }]
    });
    let plan = tool_response(serde_json::json!([call(
        "plan",
        "submit_plan",
        plan_arguments,
    )]));
    assert!(decode_chat_completions_response_for_request(&plan, 64 * 1024, &request).is_ok());

    let blocked = tool_response(serde_json::json!([call(
        "blocked",
        "report_blocked",
        serde_json::json!({
            "reason": "missing_required_context",
            "summary": "required context is unavailable"
        }),
    )]));
    decode_chat_completions_response_for_request(&blocked, 64 * 1024, &request).unwrap();

    let runtime = tool_response(serde_json::json!([call(
        "read",
        "read_file",
        serde_json::json!({"path": "src/lib.rs", "start_line": 1, "end_line": 1}),
    )]));
    let wrong_terminal = tool_response(serde_json::json!([call(
        "execution",
        "submit_execution",
        serde_json::json!({"summary": "not a Planner terminal"}),
    )]));
    let multiple = tool_response(serde_json::json!([
        call(
            "plan",
            "submit_plan",
            serde_json::json!({
                "summary": "bounded plan",
                "steps": [{
                    "title": "implement",
                    "description": "implement the requested change",
                    "acceptance_criteria": ["the focused test passes"]
                }],
                "initial_required_checks": [{
                    "kind": "cargo_test",
                    "package": "coding-agent-core",
                    "integration_test": "role_loop"
                }]
            })
        ),
        call(
            "blocked",
            "report_blocked",
            serde_json::json!({
                "reason": "missing_required_context",
                "summary": "required context is unavailable"
            })
        )
    ]));
    let empty = tool_response(serde_json::json!([]));
    let final_text =
        br#"{"choices":[{"index":0,"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}]}"#;
    for rejected in [&runtime[..], &wrong_terminal[..], &multiple[..]] {
        let error = decode_chat_completions_response_for_request(rejected, 64 * 1024, &request)
            .unwrap_err();
        assert_eq!(error.code, PROVIDER_RESPONSE_ROLE_ACTION_NOT_ALLOWED);
    }
    assert!(decode_chat_completions_response_for_request(&empty, 64 * 1024, &request).is_err());
    let error =
        decode_chat_completions_response_for_request(final_text, 64 * 1024, &request).unwrap_err();
    assert_eq!(error.code, PROVIDER_RESPONSE_ROLE_OUTPUT_INVALID);
}

#[test]
fn executor_terminal_or_blocked_convergence_is_exact_in_all_wire_modes() {
    let request = request(
        Role::Executor,
        ModelToolChoice::Required(
            RequiredAction::terminal_or_blocked(ControlKind::SubmitExecution).unwrap(),
        ),
    );
    for (compatibility, expected_choice) in [
        (
            ProviderToolChoiceCompatibility::Strict,
            serde_json::json!("required"),
        ),
        (
            ProviderToolChoiceCompatibility::RequiredAsRequired,
            serde_json::json!("required"),
        ),
        (
            ProviderToolChoiceCompatibility::RequiredAsAuto,
            serde_json::json!("auto"),
        ),
    ] {
        let encoded = encode_chat_completions_request_with_compatibility(
            "coding-model",
            &request,
            compatibility,
        )
        .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(body["tool_choice"], expected_choice);
        assert_eq!(tool_names(&body), ["submit_execution", "report_blocked"]);
    }

    for (name, arguments) in [
        ("submit_execution", serde_json::json!({"summary": "ready"})),
        (
            "report_blocked",
            serde_json::json!({
                "reason": "missing_required_context",
                "summary": "context unavailable"
            }),
        ),
    ] {
        let response = tool_response(serde_json::json!([call("terminal", name, arguments)]));
        decode_chat_completions_response_for_request(&response, 64 * 1024, &request).unwrap();
    }
}

#[test]
fn wrong_role_or_non_terminal_required_actions_fail_before_provider_contact() {
    let wrong_role = request(
        Role::Executor,
        ModelToolChoice::Required(RequiredAction::Terminal(ControlKind::SubmitReview)),
    );
    let error = encode_chat_completions_request("coding-model", &wrong_role).unwrap_err();
    assert_eq!(error.code, PROVIDER_REQUEST_REJECTED);

    let progress = request(
        Role::Executor,
        ModelToolChoice::Required(RequiredAction::Terminal(ControlKind::UpdatePlanProgress)),
    );
    let error = encode_chat_completions_request("coding-model", &progress).unwrap_err();
    assert_eq!(error.code, PROVIDER_REQUEST_REJECTED);
}

#[test]
fn required_calls_fail_on_same_name_wrong_selector_zero_multiple_or_final_text() {
    let required = RequiredCheck::try_cargo_test(
        "check-1",
        Some("coding-agent-core".to_owned()),
        Some("role_contracts".to_owned()),
    )
    .unwrap();
    let request = request(
        Role::Executor,
        ModelToolChoice::Required(RequiredAction::Validation(required)),
    );

    let wrong_selector = tool_response(serde_json::json!([call(
        "one",
        "cargo_test",
        serde_json::json!({
            "check_id": "check-1",
            "package": "coding-agent-provider",
            "integration_test": "role_contracts"
        }),
    )]));
    let error = decode_chat_completions_response_for_request(&wrong_selector, 64 * 1024, &request)
        .unwrap_err();
    assert_eq!(error.code, PROVIDER_RESPONSE_ROLE_ACTION_NOT_ALLOWED);

    let multiple = tool_response(serde_json::json!([
        call(
            "one",
            "cargo_test",
            serde_json::json!({
                "check_id": "check-1",
                "package": "coding-agent-core",
                "integration_test": "role_contracts"
            }),
        ),
        call(
            "two",
            "cargo_test",
            serde_json::json!({
                "check_id": "check-1",
                "package": "coding-agent-core",
                "integration_test": "role_contracts"
            }),
        )
    ]));
    assert!(decode_chat_completions_response_for_request(&multiple, 64 * 1024, &request).is_err());

    let empty = tool_response(serde_json::json!([]));
    assert!(decode_chat_completions_response_for_request(&empty, 64 * 1024, &request).is_err());

    let final_text = br#"{"choices":[{"index":0,"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}]}"#;
    let error =
        decode_chat_completions_response_for_request(final_text, final_text.len(), &request)
            .unwrap_err();
    assert_eq!(error.code, PROVIDER_RESPONSE_ROLE_OUTPUT_INVALID);
}

#[test]
fn wrong_role_mixed_controls_and_duplicate_ids_fail_closed() {
    let planner = request(Role::Planner, ModelToolChoice::Auto);
    let wrong_role = tool_response(serde_json::json!([call(
        "write",
        "replace_file",
        serde_json::json!({
            "path": "src/lib.rs",
            "expected_sha256": null,
            "content": "x"
        }),
    )]));
    assert!(
        decode_chat_completions_response_for_request(&wrong_role, 64 * 1024, &planner).is_err()
    );

    let executor = request(Role::Executor, ModelToolChoice::Auto);
    let mixed = tool_response(serde_json::json!([
        call("status", "git_status", serde_json::json!({})),
        call(
            "terminal",
            "submit_execution",
            serde_json::json!({"summary": "done"})
        )
    ]));
    assert!(decode_chat_completions_response_for_request(&mixed, 64 * 1024, &executor).is_err());

    let duplicate = tool_response(serde_json::json!([
        call("same", "git_status", serde_json::json!({})),
        call("same", "git_diff", serde_json::json!({}))
    ]));
    assert!(
        decode_chat_completions_response_for_request(&duplicate, 64 * 1024, &executor).is_err()
    );
}

#[test]
fn executor_update_plan_progress_decodes_as_control_and_round_trips_one_matching_result() {
    let request = request(Role::Executor, ModelToolChoice::Auto);
    let response = tool_response(serde_json::json!([call(
        "progress-1",
        "update_plan_progress",
        serde_json::json!({
            "updates": [
                {"step_id": "step-1", "status": "running"},
                {"step_id": "step-2", "status": "completed"}
            ]
        }),
    )]));
    let decoded =
        decode_chat_completions_response_for_request(&response, 64 * 1024, &request).unwrap();
    let ModelResponse::ToolCalls(batch) = decoded else {
        panic!("expected control call");
    };
    assert!(matches!(
        &batch.calls[0].request,
        ActionRequest::Control(ControlRequest::UpdatePlanProgress(_))
    ));

    let continued = ModelRequest {
        messages: vec![
            ModelMessage::system("executor policy"),
            ModelMessage::user("executor input"),
            ModelMessage::AssistantToolCalls(batch),
            ModelMessage::tool_result("progress-1", r#"{"status":"plan_updated","revision":2}"#),
        ],
        allowed_actions: AllowedActions::for_role(Role::Executor),
        tool_choice: ModelToolChoice::Auto,
    };
    let body = encoded_body(&continued);
    assert_eq!(body["messages"][2]["tool_calls"][0]["id"], "progress-1");
    assert_eq!(body["messages"][3]["tool_call_id"], "progress-1");
}

#[test]
fn fresh_role_request_does_not_inherit_prior_reasoning_or_tool_metadata() {
    let reviewer = ModelRequest {
        messages: vec![
            ModelMessage::system("reviewer policy"),
            ModelMessage::user("fresh bounded reviewer handoff"),
        ],
        allowed_actions: AllowedActions::for_role(Role::Reviewer),
        tool_choice: ModelToolChoice::Auto,
    };
    let body = encoded_body(&reviewer);
    assert_eq!(body["messages"].as_array().unwrap().len(), 2);
    let rendered = String::from_utf8(serde_json::to_vec(&body).unwrap()).unwrap();
    assert!(!rendered.contains("reasoning_content"));
    assert!(!rendered.contains("tool_call_id"));
    assert!(!rendered.contains("request_id"));

    // A caller cannot smuggle an action from a different role into the fresh
    // transcript while retaining the new role capability set.
    let mut smuggled = reviewer;
    smuggled.messages.push(ModelMessage::AssistantToolCalls(
        coding_agent_core::ToolCallBatch {
            assistant_content: None,
            reasoning_content: Some("opaque-old-role".to_owned()),
            calls: vec![coding_agent_core::ToolCall {
                id: "old".to_owned(),
                request: ActionRequest::decode(
                    Role::Executor,
                    "update_plan_progress",
                    r#"{"updates":[{"step_id":"step-1","status":"completed"}]}"#,
                )
                .unwrap(),
            }],
        },
    ));
    smuggled
        .messages
        .push(ModelMessage::tool_result("old", "plan updated"));
    assert!(encode_chat_completions_request("coding-model", &smuggled).is_err());
}

#[test]
fn exact_required_decode_produces_typed_validation_not_legacy_tool_request() {
    let required =
        RequiredCheck::try_cargo_check("check-2", Some("coding-agent-core".to_owned())).unwrap();
    let request = request(
        Role::Reviewer,
        ModelToolChoice::Required(RequiredAction::Validation(required)),
    );
    let response = tool_response(serde_json::json!([call(
        "check",
        "cargo_check",
        serde_json::json!({
            "check_id": "check-2",
            "package": "coding-agent-core"
        }),
    )]));
    let decoded =
        decode_chat_completions_response_for_request(&response, 64 * 1024, &request).unwrap();
    assert!(matches!(
        decoded,
        ModelResponse::ToolCalls(coding_agent_core::ToolCallBatch { calls, .. })
            if matches!(
                &calls[0].request,
                ActionRequest::Runtime(RuntimeActionRequest::Validation { check })
                    if check.id() == "check-2"
            )
    ));
}

#[test]
fn auto_reviewer_cannot_turn_arbitrary_diff_identity_into_coverage() {
    let request = request(Role::Reviewer, ModelToolChoice::Auto);
    let response = tool_response(serde_json::json!([call(
        "manifest",
        "review_diff_manifest",
        serde_json::json!({
            "generation": 7,
            "workspace_digest": {
                "algorithm": "workspace_fingerprint_v1",
                "value": "a".repeat(64)
            }
        }),
    )]));
    let error =
        decode_chat_completions_response_for_request(&response, 64 * 1024, &request).unwrap_err();
    assert_eq!(error.code, PROVIDER_RESPONSE_ROLE_ACTION_NOT_ALLOWED);
}

#[test]
fn ordinary_final_text_is_rejected_for_role_requests_even_in_auto_mode() {
    let request = request(Role::Planner, ModelToolChoice::Auto);
    let response =
        br#"{"choices":[{"index":0,"message":{"role":"assistant","content":"plain plan"},"finish_reason":"stop"}]}"#;
    let error = decode_chat_completions_response_for_request(response, response.len(), &request)
        .unwrap_err();
    assert_eq!(error.code, PROVIDER_RESPONSE_ROLE_OUTPUT_INVALID);
}

#[test]
fn executor_protocol_decode_classification_maps_to_stable_stage_failure_codes() {
    let request = request(Role::Executor, ModelToolChoice::Auto);
    let final_text =
        br#"{"choices":[{"index":0,"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}]}"#
            .to_vec();
    let empty_batch = tool_response(serde_json::json!([]));
    let invalid_terminal = tool_response(serde_json::json!([call(
        "submit",
        "submit_execution",
        serde_json::json!({}),
    )]));
    let wrong_role_action = tool_response(serde_json::json!([call(
        "plan",
        "submit_plan",
        serde_json::json!({}),
    )]));
    let invalid_action = tool_response(serde_json::json!([call(
        "read",
        "read_file",
        serde_json::json!({}),
    )]));

    for (response, provider_code, executor_code) in [
        (
            final_text,
            PROVIDER_RESPONSE_ROLE_OUTPUT_INVALID,
            "EXECUTOR_INVALID_OUTPUT",
        ),
        (
            empty_batch,
            PROVIDER_RESPONSE_ROLE_OUTPUT_INVALID,
            "EXECUTOR_INVALID_OUTPUT",
        ),
        (
            invalid_terminal,
            PROVIDER_RESPONSE_ROLE_OUTPUT_INVALID,
            "EXECUTOR_INVALID_OUTPUT",
        ),
        (
            wrong_role_action,
            PROVIDER_RESPONSE_ROLE_ACTION_NOT_ALLOWED,
            "EXECUTOR_ACTION_NOT_ALLOWED",
        ),
        (
            invalid_action,
            PROVIDER_RESPONSE_ROLE_ACTION_NOT_ALLOWED,
            "EXECUTOR_ACTION_NOT_ALLOWED",
        ),
    ] {
        let error = decode_chat_completions_response_for_request(&response, 64 * 1024, &request)
            .unwrap_err();
        assert_eq!(error.code, provider_code);
        assert_eq!(
            RoleLoopError::Provider(error).executor_failure_code(),
            Some(executor_code)
        );
    }
}
