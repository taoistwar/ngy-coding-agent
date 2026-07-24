use coding_agent_core::{
    ActionRequest, AllowedActions, ModelMessage, ModelRequest, ModelResponse, ModelToolChoice,
    RetainedToolResult, RuntimeActionRequest, ToolCall, ToolCallBatch, ToolRequest, ToolStatus,
};
use coding_agent_provider::{
    PROVIDER_RATE_LIMITED, PROVIDER_RESPONSE_FINISH_UNSUPPORTED, PROVIDER_RESPONSE_INVALID,
    PROVIDER_RESPONSE_REASONING_REJECTED, PROVIDER_RESPONSE_SCHEMA_UNSUPPORTED,
    PROVIDER_UNAUTHORIZED, SecretRedactor, decode_chat_completions_response,
    encode_chat_completions_request, map_http_status,
};

#[test]
fn request_encodes_supported_messages_and_one_tool_call_id_round_trip() {
    let call = ToolCall::runtime(
        "call-17",
        ToolRequest::ReadFile {
            path: "src/lib.rs".to_owned(),
            start_line: 1,
            end_line: 20,
        },
    );
    let request = ModelRequest {
        messages: vec![
            ModelMessage::system("policy"),
            ModelMessage::user("inspect"),
            ModelMessage::assistant("I will inspect."),
            ModelMessage::AssistantToolCalls(ToolCallBatch {
                assistant_content: None,
                reasoning_content: None,
                calls: vec![call],
            }),
            ModelMessage::tool_result("call-17", "file contents"),
        ],
        allowed_actions: AllowedActions::legacy(),
        tool_choice: ModelToolChoice::Auto,
    };

    let encoded =
        encode_chat_completions_request("coding-model", &request).expect("encode chat request");
    let json: serde_json::Value = serde_json::from_slice(&encoded).expect("request JSON");

    assert_eq!(json["model"], "coding-model");
    assert_eq!(json["tool_choice"], "auto");
    assert_eq!(json["parallel_tool_calls"], false);
    assert_eq!(json["stream"], false);
    assert_eq!(json["messages"][0]["role"], "system");
    assert_eq!(json["messages"][1]["role"], "user");
    assert_eq!(json["messages"][2]["role"], "assistant");
    assert!(json["messages"][3]["content"].is_null());
    assert_eq!(json["messages"][3]["tool_calls"][0]["id"], "call-17");
    assert_eq!(
        json["messages"][3]["tool_calls"][0]["function"]["name"],
        "read_file"
    );
    assert_eq!(json["messages"][4]["role"], "tool");
    assert_eq!(json["messages"][4]["tool_call_id"], "call-17");

    let arguments: serde_json::Value = serde_json::from_str(
        json["messages"][3]["tool_calls"][0]["function"]["arguments"]
            .as_str()
            .expect("arguments string"),
    )
    .expect("arguments JSON");
    assert_eq!(arguments["path"], "src/lib.rs");
    assert_eq!(arguments["start_line"], 1);
    assert_eq!(arguments["end_line"], 20);

    let tools = json["tools"].as_array().expect("tools array");
    assert_eq!(tools.len(), 8);
    assert!(tools.iter().all(|tool| tool["type"] == "function"));
    let list_files = tools
        .iter()
        .find(|tool| tool["function"]["name"] == "list_files")
        .expect("list_files schema");
    assert_eq!(
        list_files["function"]["parameters"]["properties"]["depth"]["minimum"],
        1
    );
}

#[test]
fn retained_wrapper_is_byte_for_byte_the_provider_tool_message_wire() {
    let tool_call_id = r#"call-"quoted"\slash"#;
    let retained = RetainedToolResult::try_from_parts(
        tool_call_id,
        r#"result has "quotes" and a \backslash"#,
        ToolStatus::Succeeded,
        false,
        &SecretRedactor::new(),
    )
    .unwrap();
    let request = ModelRequest {
        messages: vec![
            ModelMessage::AssistantToolCalls(ToolCallBatch {
                assistant_content: None,
                reasoning_content: None,
                calls: vec![ToolCall::runtime(tool_call_id, ToolRequest::GitStatus)],
            }),
            retained.clone().into_model_message(),
        ],
        allowed_actions: AllowedActions::legacy(),
        tool_choice: ModelToolChoice::Auto,
    };

    let encoded = encode_chat_completions_request("coding-model", &request).unwrap();
    let wire: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    let provider_message = serde_json::to_vec(&wire["messages"][1]).unwrap();

    assert_eq!(retained.wrapper_bytes(), provider_message);
    assert!(
        encoded
            .windows(retained.wrapper_len())
            .any(|window| window == retained.wrapper_bytes()),
        "the full provider request must embed the exact wrapper bytes counted by core"
    );
    assert_eq!(
        std::str::from_utf8(retained.wrapper_bytes()).unwrap(),
        format!(
            "{{\"content\":{},\"role\":\"tool\",\"tool_call_id\":{}}}",
            serde_json::to_string(retained.content()).unwrap(),
            serde_json::to_string(tool_call_id).unwrap()
        )
    );
}

#[test]
fn strict_request_encodes_all_supported_tool_choices_exactly() {
    for (tool_choice, expected) in [
        (ModelToolChoice::Auto, serde_json::json!("auto")),
        (ModelToolChoice::None, serde_json::json!("none")),
        (
            ModelToolChoice::RequiredCargoTest,
            serde_json::json!({
                "type": "function",
                "function": {"name": "cargo_test"}
            }),
        ),
    ] {
        let encoded = encode_chat_completions_request(
            "coding-model",
            &ModelRequest {
                messages: vec![ModelMessage::user("finish safely")],
                allowed_actions: AllowedActions::legacy(),
                tool_choice: tool_choice.clone(),
            },
        )
        .expect("encode supported tool choice");
        let json: serde_json::Value = serde_json::from_slice(&encoded).unwrap();

        assert_eq!(json["tool_choice"], expected);
        assert_eq!(json["parallel_tool_calls"], false);
        assert_eq!(
            json["tools"].as_array().map(Vec::len),
            Some(
                if matches!(tool_choice, ModelToolChoice::RequiredCargoTest) {
                    1
                } else {
                    8
                }
            ),
            "required choices expose only their exact action schema"
        );
    }
}

#[test]
fn request_rejects_a_mismatched_or_unpaired_tool_result_id() {
    let call = ToolCall::runtime("call-17", ToolRequest::GitStatus);
    for messages in [
        vec![
            ModelMessage::AssistantToolCalls(ToolCallBatch {
                assistant_content: None,
                reasoning_content: None,
                calls: vec![call.clone()],
            }),
            ModelMessage::tool_result("different-call", "clean"),
        ],
        vec![ModelMessage::tool_result("call-17", "clean")],
        vec![ModelMessage::AssistantToolCalls(ToolCallBatch {
            assistant_content: None,
            reasoning_content: None,
            calls: vec![call.clone()],
        })],
    ] {
        let error = encode_chat_completions_request(
            "coding-model",
            &ModelRequest {
                messages,
                allowed_actions: AllowedActions::legacy(),
                tool_choice: ModelToolChoice::Auto,
            },
        )
        .expect_err("tool result IDs must be paired exactly");
        assert!(!error.retryable);
    }

    let ordered_batch = ToolCallBatch {
        assistant_content: Some("Inspecting.".to_owned()),
        reasoning_content: None,
        calls: vec![
            ToolCall::runtime("first", ToolRequest::GitStatus),
            ToolCall::runtime("second", ToolRequest::GitDiff),
        ],
    };
    let duplicate_batch = ToolCallBatch {
        assistant_content: None,
        reasoning_content: None,
        calls: vec![
            ToolCall::runtime("same", ToolRequest::GitStatus),
            ToolCall::runtime("same", ToolRequest::GitDiff),
        ],
    };
    for messages in [
        vec![
            ModelMessage::AssistantToolCalls(ordered_batch.clone()),
            ModelMessage::tool_result("second", "diff"),
            ModelMessage::tool_result("first", "clean"),
        ],
        vec![
            ModelMessage::AssistantToolCalls(ordered_batch.clone()),
            ModelMessage::tool_result("first", "clean"),
        ],
        vec![ModelMessage::AssistantToolCalls(duplicate_batch)],
        vec![
            ModelMessage::AssistantToolCalls(ordered_batch.clone()),
            ModelMessage::tool_result("first", "clean"),
            ModelMessage::assistant("interleaved"),
            ModelMessage::tool_result("second", "diff"),
        ],
    ] {
        let error = encode_chat_completions_request(
            "coding-model",
            &ModelRequest {
                messages,
                allowed_actions: AllowedActions::legacy(),
                tool_choice: ModelToolChoice::Auto,
            },
        )
        .expect_err("tool result batches must be complete, unique, contiguous, and ordered");
        assert!(!error.retryable);
    }
}

#[test]
fn response_decodes_exactly_one_typed_tool_call_and_preserves_its_id() {
    let response = serde_json::json!({
        "id": "chatcmpl-1",
        "object": "chat.completion",
        "created": 1,
        "model": "coding-model",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "id": "call-17",
                    "type": "function",
                    "function": {
                        "name": "read_file",
                        "arguments": "{\"path\":\"src/lib.rs\",\"start_line\":1,\"end_line\":20}"
                    }
                }]
            },
            "finish_reason": "tool_calls"
        }],
        "usage": {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5}
    });
    let encoded = serde_json::to_vec(&response).unwrap();

    let decoded =
        decode_chat_completions_response(&encoded, 16 * 1024).expect("decode one tool call");
    assert_eq!(
        decoded,
        ModelResponse::ToolCalls(ToolCallBatch {
            assistant_content: None,
            reasoning_content: None,
            calls: vec![ToolCall::runtime(
                "call-17",
                ToolRequest::ReadFile {
                    path: "src/lib.rs".to_owned(),
                    start_line: 1,
                    end_line: 20,
                },
            )],
        })
    );
}

#[test]
fn final_text_response_is_supported() {
    let encoded = br#"{"choices":[{"index":0,"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}]}"#;
    assert_eq!(
        decode_chat_completions_response(encoded, encoded.len()).unwrap(),
        ModelResponse::Final {
            content: "done".to_owned()
        }
    );
}

#[test]
fn disabled_thinking_empty_marker_is_supported_but_reasoning_content_is_rejected() {
    let disabled = br#"{"choices":[{"index":0,"message":{"role":"assistant","content":"done","reasoning_content":null},"finish_reason":"stop"}]}"#;
    assert_eq!(
        decode_chat_completions_response(disabled, disabled.len()).unwrap(),
        ModelResponse::Final {
            content: "done".to_owned()
        }
    );

    let empty = br#"{"choices":[{"index":0,"message":{"role":"assistant","content":"done","reasoning_content":""},"finish_reason":"stop"}]}"#;
    assert_eq!(
        decode_chat_completions_response(empty, empty.len()).unwrap(),
        ModelResponse::Final {
            content: "done".to_owned()
        }
    );

    let reasoning = br#"{"choices":[{"index":0,"message":{"role":"assistant","content":"done","reasoning_content":"known-secret-reasoning"},"finish_reason":"stop"}]}"#;
    let error = decode_chat_completions_response(reasoning, reasoning.len())
        .expect_err("non-null reasoning content remains outside the supported subset");
    assert_eq!(error.code, PROVIDER_RESPONSE_REASONING_REJECTED);
    for rendered in [format!("{error:?}"), format!("{error}")] {
        assert!(!rendered.contains("known-secret-reasoning"));
        assert!(!rendered.contains("reasoning_content"));
    }
}

#[test]
fn tool_call_assistant_content_is_preserved_for_the_next_request() {
    let encoded = br#"{"choices":[{"index":0,"message":{"role":"assistant","content":"I will inspect the repository.","reasoning_content":"","tool_calls":[{"index":0,"id":"call-1","type":"function","function":{"name":"git_status","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}"#;
    let response = decode_chat_completions_response(encoded, encoded.len())
        .expect("DeepSeek-compatible tool call response");
    let ModelResponse::ToolCalls(batch) = response else {
        panic!("expected one tool-call batch");
    };
    assert_eq!(
        batch.assistant_content.as_deref(),
        Some("I will inspect the repository.")
    );
    assert_eq!(batch.calls[0].id, "call-1");

    let request = ModelRequest {
        messages: vec![
            ModelMessage::AssistantToolCalls(batch),
            ModelMessage::tool_result("call-1", "clean"),
        ],
        allowed_actions: AllowedActions::legacy(),
        tool_choice: ModelToolChoice::Auto,
    };
    let round_trip = encode_chat_completions_request("coding-model", &request)
        .expect("encode assistant tool-call context");
    let json: serde_json::Value = serde_json::from_slice(&round_trip).unwrap();
    assert_eq!(
        json["messages"][0]["content"],
        "I will inspect the repository."
    );
    assert!(json["messages"][0].get("reasoning_content").is_none());

    let empty = br#"{"choices":[{"index":0,"message":{"role":"assistant","content":"","tool_calls":[{"index":0,"id":"call-empty","type":"function","function":{"name":"git_status","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}"#;
    let empty_response = decode_chat_completions_response(empty, empty.len())
        .expect("empty assistant tool-call content is preserved");
    let ModelResponse::ToolCalls(empty_batch) = empty_response else {
        panic!("expected one tool-call batch");
    };
    assert_eq!(empty_batch.assistant_content.as_deref(), Some(""));

    let empty_request = ModelRequest {
        messages: vec![
            ModelMessage::AssistantToolCalls(empty_batch),
            ModelMessage::tool_result("call-empty", "clean"),
        ],
        allowed_actions: AllowedActions::legacy(),
        tool_choice: ModelToolChoice::Auto,
    };
    let empty_round_trip = encode_chat_completions_request("coding-model", &empty_request)
        .expect("encode empty assistant tool-call context");
    let empty_json: serde_json::Value = serde_json::from_slice(&empty_round_trip).unwrap();
    assert_eq!(empty_json["messages"][0]["content"], "");
}

#[test]
fn nullable_tool_calls_and_only_array_position_indices_are_supported() {
    let no_call = br#"{"choices":[{"index":0,"message":{"role":"assistant","content":"done","tool_calls":null},"finish_reason":"stop"}]}"#;
    assert_eq!(
        decode_chat_completions_response(no_call, no_call.len()).unwrap(),
        ModelResponse::Final {
            content: "done".to_owned()
        }
    );

    for invalid in [
        br#"{"choices":[{"index":0,"message":{"role":"assistant","content":"","tool_calls":[{"index":1,"id":"call-1","type":"function","function":{"name":"git_status","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}"#.as_slice(),
        br#"{"choices":[{"index":0,"message":{"role":"assistant","content":"","tool_calls":[{"index":0,"id":"same","type":"function","function":{"name":"git_status","arguments":"{}"}},{"index":1,"id":"same","type":"function","function":{"name":"git_diff","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}"#.as_slice(),
        br#"{"choices":[{"index":0,"message":{"role":"assistant","content":"","tool_calls":[{"index":0,"id":"one","type":"function","function":{"name":"git_status","arguments":"{}"}},{"index":0,"id":"two","type":"function","function":{"name":"git_diff","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}"#.as_slice(),
    ] {
        let error = decode_chat_completions_response(invalid, invalid.len())
            .expect_err("tool call indices and IDs must preserve one ordered batch");
        assert_eq!(error.code, PROVIDER_RESPONSE_INVALID);
    }
}

#[test]
fn empty_final_text_is_not_a_supported_completion() {
    let encoded = br#"{"choices":[{"index":0,"message":{"role":"assistant","content":""},"finish_reason":"stop"}]}"#;
    let error = decode_chat_completions_response(encoded, encoded.len()).unwrap_err();
    assert_eq!(error.code, PROVIDER_RESPONSE_INVALID);
    assert!(!error.retryable);
}

#[test]
fn finish_reason_must_exactly_match_the_response_kind() {
    for encoded in [
        br#"{"choices":[{"index":0,"message":{"role":"assistant","content":"partial"},"finish_reason":"length"}]}"#.as_slice(),
        br#"{"choices":[{"index":0,"message":{"role":"assistant","content":"filtered"},"finish_reason":"content_filter"}]}"#.as_slice(),
        br#"{"choices":[{"index":0,"message":{"role":"assistant","content":"done"},"finish_reason":"known-secret-finish"}]}"#.as_slice(),
        br#"{"choices":[{"index":0,"message":{"role":"assistant","content":"done"}}]}"#.as_slice(),
        br#"{"choices":[{"index":0,"message":{"role":"assistant","content":"done"},"finish_reason":null}]}"#.as_slice(),
        br#"{"choices":[{"index":0,"message":{"role":"assistant","content":"done"},"finish_reason":"tool_calls"}]}"#.as_slice(),
        br#"{"choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"one","type":"function","function":{"name":"git_status","arguments":"{}"}}]},"finish_reason":"stop"}]}"#.as_slice(),
    ] {
        let error = decode_chat_completions_response(encoded, encoded.len()).unwrap_err();
        assert_eq!(error.code, PROVIDER_RESPONSE_FINISH_UNSUPPORTED);
        assert!(!error.retryable);
        for rendered in [format!("{error:?}"), format!("{error}")] {
            assert!(!rendered.contains("known-secret-finish"));
            assert!(!rendered.contains("finish_reason"));
        }
    }
}

#[test]
fn provider_rejects_tool_call_ids_above_the_core_256_byte_limit() {
    let response = serde_json::json!({
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "id": "x".repeat(257),
                    "type": "function",
                    "function": {"name": "git_status", "arguments": "{}"}
                }]
            },
            "finish_reason": "tool_calls"
        }]
    });
    let encoded = serde_json::to_vec(&response).unwrap();
    let error = decode_chat_completions_response(&encoded, encoded.len()).unwrap_err();
    assert_eq!(error.code, PROVIDER_RESPONSE_INVALID);
}

#[test]
fn multiple_tool_calls_preserve_response_order_and_round_trip_as_one_message() {
    let encoded = br#"{
      "choices":[{"index":0,"message":{"role":"assistant","content":"Inspecting.","tool_calls":[
        {"index":0,"id":"one","type":"function","function":{"name":"git_status","arguments":"{}"}},
        {"index":1,"id":"two","type":"function","function":{"name":"git_diff","arguments":"{}"}}
      ]},"finish_reason":"tool_calls"}]
    }"#;

    let response = decode_chat_completions_response(encoded, encoded.len())
        .expect("ordered multiple tool calls are supported");
    let ModelResponse::ToolCalls(batch) = response else {
        panic!("expected a tool-call batch");
    };
    assert_eq!(batch.assistant_content.as_deref(), Some("Inspecting."));
    assert_eq!(
        batch
            .calls
            .iter()
            .map(|call| call.id.as_str())
            .collect::<Vec<_>>(),
        ["one", "two"]
    );
    assert!(matches!(
        batch.calls[0].request,
        ActionRequest::Runtime(RuntimeActionRequest::Tool(ToolRequest::GitStatus))
    ));
    assert!(matches!(
        batch.calls[1].request,
        ActionRequest::Runtime(RuntimeActionRequest::Tool(ToolRequest::GitDiff))
    ));

    let request = ModelRequest {
        messages: vec![
            ModelMessage::AssistantToolCalls(batch),
            ModelMessage::tool_result("one", "clean"),
            ModelMessage::tool_result("two", "diff"),
        ],
        allowed_actions: AllowedActions::legacy(),
        tool_choice: ModelToolChoice::Auto,
    };
    let round_trip = encode_chat_completions_request("coding-model", &request)
        .expect("encode one assistant batch with ordered results");
    let json: serde_json::Value = serde_json::from_slice(&round_trip).unwrap();
    assert_eq!(json["messages"].as_array().unwrap().len(), 3);
    assert_eq!(json["messages"][0]["tool_calls"][0]["id"], "one");
    assert_eq!(json["messages"][0]["tool_calls"][1]["id"], "two");
    assert_eq!(json["messages"][1]["tool_call_id"], "one");
    assert_eq!(json["messages"][2]["tool_call_id"], "two");
}

#[test]
fn response_schema_data_errors_are_classified_without_raw_body_echo() {
    for encoded in [
        br#"{"choices":[{"index":0,"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}],"known-secret-field":"known-secret-body"}"#.as_slice(),
        br#"{"choices":[{"index":0,"message":{"role":"assistant","content":"done","known-secret-field":"known-secret-body"},"finish_reason":"stop"}]}"#.as_slice(),
        br#"{"choices":"known-secret-body"}"#.as_slice(),
        br#"{"known-secret-field":"known-secret-body"}"#.as_slice(),
    ] {
        let error = decode_chat_completions_response(encoded, 16 * 1024)
            .expect_err("unsupported response schemas must fail closed");
        assert_eq!(error.code, PROVIDER_RESPONSE_SCHEMA_UNSUPPORTED);
        for rendered in [format!("{error:?}"), format!("{error}")] {
            assert!(!rendered.contains("known-secret-field"));
            assert!(!rendered.contains("known-secret-body"));
        }
    }
}

#[test]
fn malformed_json_keeps_the_generic_code_without_raw_body_echo() {
    let encoded = br#"{"choices":"known-secret-body"#;
    let error = decode_chat_completions_response(encoded, encoded.len())
        .expect_err("malformed JSON must fail closed");
    assert_eq!(error.code, PROVIDER_RESPONSE_INVALID);
    for rendered in [format!("{error:?}"), format!("{error}")] {
        assert!(!rendered.contains("known-secret-body"));
    }
}

#[test]
fn tool_names_and_argument_schemas_keep_the_existing_invalid_code() {
    for encoded in [
        br#"{"choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"one","type":"function","function":{"name":"run_shell","arguments":"{\"command\":\"known-secret-body\"}"}}]},"finish_reason":"tool_calls"}]}"#.as_slice(),
        br#"{"choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"one","type":"function","function":{"name":"git_status","arguments":"{\"known-secret-field\":\"known-secret-body\"}"}}]},"finish_reason":"tool_calls"}]}"#.as_slice(),
        br#"{"choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"index":0,"id":"safe","type":"function","function":{"name":"git_status","arguments":"{}"}},{"index":1,"id":"bad","type":"function","function":{"name":"git_diff","arguments":"{\"known-secret-field\":\"known-secret-body\"}"}}]},"finish_reason":"tool_calls"}]}"#.as_slice(),
    ] {
        let error = decode_chat_completions_response(encoded, 16 * 1024)
            .expect_err("tool validation must keep its existing error code");
        assert_eq!(error.code, PROVIDER_RESPONSE_INVALID);
        for rendered in [format!("{error:?}"), format!("{error}")] {
            assert!(!rendered.contains("known-secret-field"));
            assert!(!rendered.contains("known-secret-body"));
        }
    }
}

#[test]
fn unsupported_response_envelope_values_are_rejected() {
    for encoded in [
        br#"{"object":"not-a-chat-completion","choices":[{"index":0,"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}]}"#.as_slice(),
        br#"{"choices":[{"index":1,"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}]}"#.as_slice(),
    ] {
        let error = decode_chat_completions_response(encoded, encoded.len()).unwrap_err();
        assert_eq!(error.code, PROVIDER_RESPONSE_INVALID);
    }
}

#[test]
fn response_limit_is_checked_before_json_parsing() {
    let body = br#"{"choices":[{"message":{"role":"assistant","content":"known-secret-body"}}]}"#;
    let error = decode_chat_completions_response(body, body.len() - 1)
        .expect_err("oversized response must be rejected");
    assert_eq!(error.code, PROVIDER_RESPONSE_INVALID);
    assert!(!error.retryable);
    assert!(!format!("{error:?}").contains("known-secret-body"));
}

#[test]
fn status_mapping_marks_only_transient_failures_retryable() {
    let unauthorized = map_http_status(401);
    assert_eq!(unauthorized.code, PROVIDER_UNAUTHORIZED);
    assert!(!unauthorized.retryable);

    let rate_limited = map_http_status(429);
    assert_eq!(rate_limited.code, PROVIDER_RATE_LIMITED);
    assert!(rate_limited.retryable);

    for status in [408, 425, 500, 502, 503, 504] {
        assert!(map_http_status(status).retryable, "status {status}");
    }
    for status in [300, 400, 403, 404, 422] {
        assert!(!map_http_status(status).retryable, "status {status}");
    }
}
