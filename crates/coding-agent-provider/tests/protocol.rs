use coding_agent_core::{ModelMessage, ModelRequest, ModelResponse, ToolCall, ToolRequest};
use coding_agent_provider::{
    PROVIDER_RATE_LIMITED, PROVIDER_RESPONSE_INVALID, PROVIDER_UNAUTHORIZED,
    PROVIDER_UNSUPPORTED_MULTIPLE_TOOL_CALLS, decode_chat_completions_response,
    encode_chat_completions_request, map_http_status,
};

#[test]
fn request_encodes_supported_messages_and_one_tool_call_id_round_trip() {
    let call = ToolCall {
        id: "call-17".to_owned(),
        request: ToolRequest::ReadFile {
            path: "src/lib.rs".to_owned(),
            start_line: 1,
            end_line: 20,
        },
    };
    let request = ModelRequest {
        messages: vec![
            ModelMessage::system("policy"),
            ModelMessage::user("inspect"),
            ModelMessage::assistant("I will inspect."),
            ModelMessage::AssistantToolCall(call),
            ModelMessage::tool_result("call-17", "file contents"),
        ],
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
fn request_rejects_a_mismatched_or_unpaired_tool_result_id() {
    let call = ToolCall {
        id: "call-17".to_owned(),
        request: ToolRequest::GitStatus,
    };
    for messages in [
        vec![
            ModelMessage::AssistantToolCall(call.clone()),
            ModelMessage::tool_result("different-call", "clean"),
        ],
        vec![ModelMessage::tool_result("call-17", "clean")],
        vec![ModelMessage::AssistantToolCall(call.clone())],
    ] {
        let error = encode_chat_completions_request("coding-model", &ModelRequest { messages })
            .expect_err("tool result IDs must be paired exactly");
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
        ModelResponse::ToolCall(ToolCall {
            id: "call-17".to_owned(),
            request: ToolRequest::ReadFile {
                path: "src/lib.rs".to_owned(),
                start_line: 1,
                end_line: 20,
            },
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
        br#"{"choices":[{"index":0,"message":{"role":"assistant","content":"done"},"finish_reason":null}]}"#.as_slice(),
        br#"{"choices":[{"index":0,"message":{"role":"assistant","content":"done"},"finish_reason":"tool_calls"}]}"#.as_slice(),
        br#"{"choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"one","type":"function","function":{"name":"git_status","arguments":"{}"}}]},"finish_reason":"stop"}]}"#.as_slice(),
    ] {
        let error = decode_chat_completions_response(encoded, encoded.len()).unwrap_err();
        assert_eq!(error.code, PROVIDER_RESPONSE_INVALID);
        assert!(!error.retryable);
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
fn multiple_tool_calls_have_the_locked_project_two_error() {
    let encoded = br#"{
      "choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[
        {"id":"one","type":"function","function":{"name":"git_status","arguments":"{}"}},
        {"id":"two","type":"function","function":{"name":"git_diff","arguments":"{}"}}
      ]},"finish_reason":"tool_calls"}]
    }"#;

    let error = decode_chat_completions_response(encoded, encoded.len()).unwrap_err();
    assert_eq!(error.code, PROVIDER_UNSUPPORTED_MULTIPLE_TOOL_CALLS);
    assert!(!error.retryable);
}

#[test]
fn unknown_fields_tools_and_arguments_are_rejected_without_raw_body_echo() {
    let fixtures: &[&[u8]] = &[
        br#"{"choices":[{"index":0,"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}],"unknown":"known-secret-body"}"#,
        br#"{"choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"one","type":"function","function":{"name":"run_shell","arguments":"{\"command\":\"known-secret-body\"}"}}]},"finish_reason":"tool_calls"}]}"#,
        br#"{"choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"one","type":"function","function":{"name":"git_status","arguments":"{\"unknown\":\"known-secret-body\"}"}}]},"finish_reason":"tool_calls"}]}"#,
    ];

    for encoded in fixtures {
        let error = decode_chat_completions_response(encoded, 16 * 1024)
            .expect_err("unknown response data must fail closed");
        assert_eq!(error.code, PROVIDER_RESPONSE_INVALID);
        assert!(!format!("{error:?}").contains("known-secret-body"));
        assert!(!format!("{error}").contains("known-secret-body"));
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
