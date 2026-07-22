use std::collections::BTreeSet;

use coding_agent_core::{
    ModelMessage, ModelRequest, ModelResponse, ModelToolChoice, ProviderError, ToolCall,
    ToolCallBatch, ToolRequest,
};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Value, error::Category, json};

use crate::config::{ProviderThinkingMode, ProviderToolChoiceCompatibility};
use crate::error::{
    invalid_request, invalid_response, rejected_response_reasoning, tool_choice_violated,
    unsupported_response_finish, unsupported_response_schema,
};

pub fn encode_chat_completions_request(
    model: &str,
    request: &ModelRequest,
) -> Result<Vec<u8>, ProviderError> {
    encode_chat_completions_request_with_options(
        model,
        request,
        None,
        ProviderToolChoiceCompatibility::Strict,
    )
}

pub(crate) fn encode_chat_completions_request_with_options(
    model: &str,
    request: &ModelRequest,
    thinking_mode: Option<ProviderThinkingMode>,
    tool_choice_compatibility: ProviderToolChoiceCompatibility,
) -> Result<Vec<u8>, ProviderError> {
    if model.is_empty() || model.len() > 256 || model.chars().any(char::is_control) {
        return Err(invalid_request("The provider model is invalid."));
    }

    validate_tool_call_round_trips(&request.messages)?;
    let messages = request
        .messages
        .iter()
        .map(encode_message)
        .collect::<Result<Vec<_>, _>>()?;
    let wire = ChatCompletionsRequestWire {
        model,
        messages,
        tools: tool_definitions_for_request(request.tool_choice, tool_choice_compatibility),
        tool_choice: encode_tool_choice(request.tool_choice, tool_choice_compatibility),
        thinking: thinking_mode.map(|mode| ThinkingWire {
            kind: mode.request_value(),
        }),
        parallel_tool_calls: false,
        stream: false,
    };
    serde_json::to_vec(&wire)
        .map_err(|_| invalid_request("The provider request could not be encoded."))
}

pub fn decode_chat_completions_response(
    encoded: &[u8],
    max_response_bytes: usize,
) -> Result<ModelResponse, ProviderError> {
    decode_chat_completions_response_with_tool_choice(
        encoded,
        max_response_bytes,
        ModelToolChoice::Auto,
        None,
    )
}

pub(crate) fn decode_chat_completions_response_with_tool_choice(
    encoded: &[u8],
    max_response_bytes: usize,
    tool_choice: ModelToolChoice,
    thinking_mode: Option<ProviderThinkingMode>,
) -> Result<ModelResponse, ProviderError> {
    if encoded.len() > max_response_bytes {
        return Err(invalid_response(
            "The provider response exceeded the configured byte limit.",
        ));
    }
    let response: ChatCompletionsResponseWire =
        serde_json::from_slice(encoded).map_err(|error| match error.classify() {
            Category::Data => unsupported_response_schema(),
            Category::Syntax | Category::Eof | Category::Io => {
                invalid_response("The provider response JSON is invalid.")
            }
        })?;
    if response.choices.len() != 1 {
        return Err(invalid_response(
            "The provider response must contain exactly one choice.",
        ));
    }

    let choice = response
        .choices
        .into_iter()
        .next()
        .expect("the exact choice count was validated");
    if choice.index != 0
        || response
            .object
            .as_deref()
            .is_some_and(|object| object != "chat.completion")
    {
        return Err(invalid_response(
            "The provider response envelope is unsupported.",
        ));
    }
    if choice.message.role != "assistant" {
        return Err(invalid_response(
            "The provider response message role is unsupported.",
        ));
    }
    let reasoning_content = choice
        .message
        .reasoning_content
        .filter(|content| !content.is_empty());
    if reasoning_content.is_some() && thinking_mode != Some(ProviderThinkingMode::Enabled) {
        return Err(rejected_response_reasoning());
    }
    let has_tool_calls = !choice.message.tool_calls.is_empty();
    match tool_choice {
        ModelToolChoice::Auto => {}
        ModelToolChoice::None if has_tool_calls => return Err(tool_choice_violated()),
        ModelToolChoice::None => {}
        ModelToolChoice::RequiredCargoTest
            if choice.message.tool_calls.len() != 1
                || choice.message.tool_calls[0].function.name != "cargo_test" =>
        {
            return Err(tool_choice_violated());
        }
        ModelToolChoice::RequiredCargoTest => {}
    }
    if has_tool_calls {
        let assistant_content = choice.message.content;
        if choice.finish_reason.as_deref() != Some("tool_calls") {
            return Err(unsupported_response_finish());
        }
        let mut ids = BTreeSet::new();
        let calls = choice
            .message
            .tool_calls
            .into_iter()
            .enumerate()
            .map(|(expected_index, call)| {
                if call.kind != "function"
                    || call.index.is_some_and(|index| index != expected_index)
                    || call.id.is_empty()
                    || call.id.len() > 256
                    || call.id.chars().any(char::is_control)
                    || !ids.insert(call.id.clone())
                {
                    return Err(invalid_response(
                        "The provider tool call envelope is invalid.",
                    ));
                }
                let request = decode_tool_request(&call.function.name, &call.function.arguments)?;
                Ok(ToolCall {
                    id: call.id,
                    request,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(ModelResponse::ToolCalls(ToolCallBatch {
            assistant_content,
            reasoning_content,
            calls,
        }));
    }

    if choice.finish_reason.as_deref() != Some("stop") {
        return Err(unsupported_response_finish());
    }
    let content = choice
        .message
        .content
        .ok_or_else(|| invalid_response("The provider response has no supported content."))?;
    if content.is_empty() || content.len() > max_response_bytes {
        return Err(invalid_response(
            "The provider final response content is invalid.",
        ));
    }
    Ok(ModelResponse::Final { content })
}

fn validate_tool_call_round_trips(messages: &[ModelMessage]) -> Result<(), ProviderError> {
    let mut pending = Vec::new();
    let mut next_result = 0usize;
    let mut seen = BTreeSet::new();
    for message in messages {
        match message {
            ModelMessage::AssistantToolCalls(batch) if pending.is_empty() => {
                if batch.calls.is_empty()
                    || batch
                        .calls
                        .iter()
                        .any(|call| !seen.insert(call.id.as_str()))
                {
                    return Err(invalid_request(
                        "Provider tool call batches must be non-empty with unique IDs.",
                    ));
                }
                pending.extend(batch.calls.iter().map(|call| call.id.as_str()));
                next_result = 0;
            }
            ModelMessage::ToolResult { tool_call_id, .. }
                if pending
                    .get(next_result)
                    .is_some_and(|expected| *expected == tool_call_id) =>
            {
                next_result += 1;
                if next_result == pending.len() {
                    pending.clear();
                    next_result = 0;
                }
            }
            ModelMessage::AssistantToolCalls(_) | ModelMessage::ToolResult { .. } => {
                return Err(invalid_request(
                    "Provider tool calls and results must have matching ordered IDs.",
                ));
            }
            _ if !pending.is_empty() => {
                return Err(invalid_request(
                    "Provider tool results must immediately follow their tool calls.",
                ));
            }
            _ => {}
        }
    }
    if !pending.is_empty() {
        return Err(invalid_request(
            "A provider tool call batch is missing matching results.",
        ));
    }
    Ok(())
}

#[derive(Serialize)]
struct ChatCompletionsRequestWire<'a> {
    model: &'a str,
    messages: Vec<Value>,
    tools: Vec<Value>,
    tool_choice: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    thinking: Option<ThinkingWire>,
    parallel_tool_calls: bool,
    stream: bool,
}

fn encode_tool_choice(
    choice: ModelToolChoice,
    compatibility: ProviderToolChoiceCompatibility,
) -> Value {
    match (choice, compatibility) {
        (
            ModelToolChoice::RequiredCargoTest,
            ProviderToolChoiceCompatibility::RequiredAsRequired,
        ) => {
            json!("required")
        }
        (ModelToolChoice::RequiredCargoTest, ProviderToolChoiceCompatibility::RequiredAsAuto) => {
            json!("auto")
        }
        (ModelToolChoice::Auto, _) => json!("auto"),
        (ModelToolChoice::None, _) => json!("none"),
        (ModelToolChoice::RequiredCargoTest, _) => {
            json!({"type": "function", "function": {"name": "cargo_test"}})
        }
    }
}

#[derive(Serialize)]
struct ThinkingWire {
    #[serde(rename = "type")]
    kind: &'static str,
}

fn encode_message(message: &ModelMessage) -> Result<Value, ProviderError> {
    match message {
        ModelMessage::System(content) => Ok(json!({"role": "system", "content": content})),
        ModelMessage::User(content) => Ok(json!({"role": "user", "content": content})),
        ModelMessage::Assistant(content) => Ok(json!({"role": "assistant", "content": content})),
        ModelMessage::AssistantToolCalls(batch) => {
            if batch.calls.is_empty() {
                return Err(invalid_request("The provider tool call batch is empty."));
            }
            let calls = batch
                .calls
                .iter()
                .map(|call| {
                    if call.id.is_empty()
                        || call.id.len() > 256
                        || call.id.chars().any(char::is_control)
                    {
                        return Err(invalid_request("The provider tool call ID is invalid."));
                    }
                    let (name, arguments) = encode_tool_request(&call.request);
                    let arguments = serde_json::to_string(&arguments).map_err(|_| {
                        invalid_request("The provider tool call arguments could not be encoded.")
                    })?;
                    Ok(json!({
                        "id": call.id,
                        "type": "function",
                        "function": {"name": name, "arguments": arguments}
                    }))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let mut encoded = json!({
                "role": "assistant",
                "content": batch.assistant_content.as_deref(),
                "tool_calls": calls
            });
            if let Some(reasoning) = batch.reasoning_content.as_deref() {
                encoded["reasoning_content"] = json!(reasoning);
            }
            Ok(encoded)
        }
        ModelMessage::ToolResult {
            tool_call_id,
            content,
        } => {
            if tool_call_id.is_empty()
                || tool_call_id.len() > 256
                || tool_call_id.chars().any(char::is_control)
            {
                return Err(invalid_request("The provider tool result ID is invalid."));
            }
            Ok(json!({
                "role": "tool",
                "tool_call_id": tool_call_id,
                "content": content,
            }))
        }
    }
}

fn encode_tool_request(request: &ToolRequest) -> (&'static str, Value) {
    match request {
        ToolRequest::ListFiles { path, depth, limit } => (
            "list_files",
            json!({"path": path, "depth": depth, "limit": limit}),
        ),
        ToolRequest::ReadFile {
            path,
            start_line,
            end_line,
        } => (
            "read_file",
            json!({"path": path, "start_line": start_line, "end_line": end_line}),
        ),
        ToolRequest::SearchText {
            query,
            path,
            glob,
            limit,
        } => (
            "search_text",
            json!({"query": query, "path": path, "glob": glob, "limit": limit}),
        ),
        ToolRequest::ReplaceFile {
            path,
            expected_sha256,
            content,
        } => (
            "replace_file",
            json!({
                "path": path,
                "expected_sha256": expected_sha256,
                "content": content
            }),
        ),
        ToolRequest::CargoCheck {
            package,
            timeout_ms,
        } => (
            "cargo_check",
            json!({"package": package, "timeout_ms": timeout_ms}),
        ),
        ToolRequest::CargoTest {
            package,
            test,
            timeout_ms,
        } => (
            "cargo_test",
            json!({"package": package, "test": test, "timeout_ms": timeout_ms}),
        ),
        ToolRequest::GitStatus => ("git_status", json!({})),
        ToolRequest::GitDiff => ("git_diff", json!({})),
    }
}

fn decode_tool_request(name: &str, arguments: &str) -> Result<ToolRequest, ProviderError> {
    fn parse<T: for<'de> Deserialize<'de>>(arguments: &str) -> Result<T, ProviderError> {
        serde_json::from_str(arguments)
            .map_err(|_| invalid_response("The provider tool call arguments are invalid."))
    }

    match name {
        "list_files" => {
            let args: ListFilesArguments = parse(arguments)?;
            Ok(ToolRequest::ListFiles {
                path: args.path,
                depth: args.depth,
                limit: args.limit,
            })
        }
        "read_file" => {
            let args: ReadFileArguments = parse(arguments)?;
            Ok(ToolRequest::ReadFile {
                path: args.path,
                start_line: args.start_line,
                end_line: args.end_line,
            })
        }
        "search_text" => {
            let args: SearchTextArguments = parse(arguments)?;
            Ok(ToolRequest::SearchText {
                query: args.query,
                path: args.path,
                glob: args.glob,
                limit: args.limit,
            })
        }
        "replace_file" => {
            let args: ReplaceFileArguments = parse(arguments)?;
            Ok(ToolRequest::ReplaceFile {
                path: args.path,
                expected_sha256: args.expected_sha256,
                content: args.content,
            })
        }
        "cargo_check" => {
            let args: CargoCheckArguments = parse(arguments)?;
            Ok(ToolRequest::CargoCheck {
                package: args.package,
                timeout_ms: args.timeout_ms,
            })
        }
        "cargo_test" => {
            let args: CargoTestArguments = parse(arguments)?;
            Ok(ToolRequest::CargoTest {
                package: args.package,
                test: args.test,
                timeout_ms: args.timeout_ms,
            })
        }
        "git_status" => {
            let _: EmptyArguments = parse(arguments)?;
            Ok(ToolRequest::GitStatus)
        }
        "git_diff" => {
            let _: EmptyArguments = parse(arguments)?;
            Ok(ToolRequest::GitDiff)
        }
        _ => Err(invalid_response("The provider requested an unknown tool.")),
    }
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
    #[serde(default)]
    glob: Option<String>,
    limit: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplaceFileArguments {
    path: String,
    #[serde(default)]
    expected_sha256: Option<String>,
    content: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CargoCheckArguments {
    #[serde(default)]
    package: Option<String>,
    timeout_ms: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CargoTestArguments {
    #[serde(default)]
    package: Option<String>,
    #[serde(default)]
    test: Option<String>,
    timeout_ms: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EmptyArguments {}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChatCompletionsResponseWire {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    object: Option<String>,
    #[serde(default)]
    created: Option<u64>,
    #[serde(default)]
    model: Option<String>,
    choices: Vec<ChoiceWire>,
    #[serde(default)]
    usage: Option<Value>,
    #[serde(default)]
    service_tier: Option<String>,
    #[serde(default)]
    system_fingerprint: Option<String>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChoiceWire {
    index: usize,
    message: AssistantMessageWire,
    #[serde(default)]
    finish_reason: Option<String>,
    #[serde(default)]
    logprobs: Option<Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AssistantMessageWire {
    role: String,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    reasoning_content: Option<String>,
    #[serde(default, deserialize_with = "deserialize_tool_calls")]
    tool_calls: Vec<ToolCallWire>,
}

fn deserialize_tool_calls<'de, D>(deserializer: D) -> Result<Vec<ToolCallWire>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<Vec<ToolCallWire>>::deserialize(deserializer).map(Option::unwrap_or_default)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ToolCallWire {
    #[serde(default)]
    index: Option<usize>,
    id: String,
    #[serde(rename = "type")]
    kind: String,
    function: FunctionCallWire,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FunctionCallWire {
    name: String,
    arguments: String,
}

fn tool_definitions_for_request(
    choice: ModelToolChoice,
    compatibility: ProviderToolChoiceCompatibility,
) -> Vec<Value> {
    if matches!(
        (choice, compatibility),
        (
            ModelToolChoice::RequiredCargoTest,
            ProviderToolChoiceCompatibility::RequiredAsRequired
                | ProviderToolChoiceCompatibility::RequiredAsAuto
        )
    ) {
        vec![cargo_test_tool_definition()]
    } else {
        tool_definitions()
    }
}

fn tool_definitions() -> Vec<Value> {
    vec![
        function_tool(
            "list_files",
            "List repository files below a relative path.",
            json!({
                "path": {"type": "string"},
                "depth": {"type": "integer", "minimum": 1},
                "limit": {"type": "integer", "minimum": 1}
            }),
            &["path", "depth", "limit"],
        ),
        function_tool(
            "read_file",
            "Read an inclusive line range from a UTF-8 repository file.",
            json!({
                "path": {"type": "string"},
                "start_line": {"type": "integer", "minimum": 1},
                "end_line": {"type": "integer", "minimum": 1}
            }),
            &["path", "start_line", "end_line"],
        ),
        function_tool(
            "search_text",
            "Search repository text below a relative path.",
            json!({
                "query": {"type": "string"},
                "path": {"type": "string"},
                "glob": {"type": ["string", "null"]},
                "limit": {"type": "integer", "minimum": 1}
            }),
            &["query", "path", "limit"],
        ),
        function_tool(
            "replace_file",
            "Atomically replace a UTF-8 repository file.",
            json!({
                "path": {"type": "string"},
                "expected_sha256": {"type": ["string", "null"]},
                "content": {"type": "string"}
            }),
            &["path", "content"],
        ),
        function_tool(
            "cargo_check",
            "Run the bounded typed Cargo check operation.",
            json!({
                "package": {"type": ["string", "null"]},
                "timeout_ms": {"type": "integer", "minimum": 1}
            }),
            &["timeout_ms"],
        ),
        cargo_test_tool_definition(),
        function_tool(
            "git_status",
            "Read the sanitized repository status.",
            json!({}),
            &[],
        ),
        function_tool(
            "git_diff",
            "Read the bounded sanitized repository diff.",
            json!({}),
            &[],
        ),
    ]
}

fn cargo_test_tool_definition() -> Value {
    function_tool(
        "cargo_test",
        "Run the bounded typed Cargo test operation.",
        json!({
            "package": {"type": ["string", "null"]},
            "test": {"type": ["string", "null"]},
            "timeout_ms": {"type": "integer", "minimum": 1}
        }),
        &["timeout_ms"],
    )
}

fn function_tool(
    name: &'static str,
    description: &'static str,
    properties: Value,
    required: &[&str],
) -> Value {
    json!({
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": {
                "type": "object",
                "properties": properties,
                "required": required,
                "additionalProperties": false
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enabled_thinking_round_trips_opaque_reasoning_for_tool_calls() {
        let response = br#"{"choices":[{"index":0,"message":{"role":"assistant","content":null,"reasoning_content":"opaque state","tool_calls":[{"id":"call-1","type":"function","function":{"name":"git_status","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}"#;
        let decoded = decode_chat_completions_response_with_tool_choice(
            response,
            response.len(),
            ModelToolChoice::Auto,
            Some(ProviderThinkingMode::Enabled),
        )
        .expect("enabled thinking accepts reasoning state");
        let ModelResponse::ToolCalls(batch) = decoded else {
            panic!("expected tool calls");
        };
        assert_eq!(batch.reasoning_content.as_deref(), Some("opaque state"));

        let encoded = encode_chat_completions_request_with_options(
            "coding-model",
            &ModelRequest {
                messages: vec![
                    ModelMessage::AssistantToolCalls(batch),
                    ModelMessage::tool_result("call-1", "clean"),
                ],
                tool_choice: ModelToolChoice::Auto,
            },
            Some(ProviderThinkingMode::Enabled),
            ProviderToolChoiceCompatibility::Strict,
        )
        .expect("reasoning state is re-encoded");
        let body: Value = serde_json::from_slice(&encoded).expect("request JSON");
        assert_eq!(body["thinking"]["type"], "enabled");
        assert_eq!(body["messages"][0]["reasoning_content"], "opaque state");
    }

    #[test]
    fn required_as_required_changes_only_the_logical_required_wire() {
        for (choice, expected_wire, expected_tool_count) in [
            (ModelToolChoice::Auto, json!("auto"), 8),
            (ModelToolChoice::None, json!("none"), 8),
            (ModelToolChoice::RequiredCargoTest, json!("required"), 1),
        ] {
            let encoded = encode_chat_completions_request_with_options(
                "coding-model",
                &ModelRequest {
                    messages: vec![ModelMessage::user("finish safely")],
                    tool_choice: choice,
                },
                None,
                ProviderToolChoiceCompatibility::RequiredAsRequired,
            )
            .expect("encode required compatibility request");
            let body: Value = serde_json::from_slice(&encoded).expect("request JSON");

            assert_eq!(body["tool_choice"], expected_wire);
            let tools = body["tools"].as_array().expect("tools array");
            assert_eq!(tools.len(), expected_tool_count);
            if choice == ModelToolChoice::RequiredCargoTest {
                assert_eq!(tools[0]["function"]["name"], "cargo_test");
            }
        }
    }

    #[test]
    fn required_as_auto_changes_only_the_logical_required_wire() {
        for (choice, expected_tool_count) in [
            (ModelToolChoice::Auto, 8),
            (ModelToolChoice::None, 8),
            (ModelToolChoice::RequiredCargoTest, 1),
        ] {
            let encoded = encode_chat_completions_request_with_options(
                "coding-model",
                &ModelRequest {
                    messages: vec![ModelMessage::user("finish safely")],
                    tool_choice: choice,
                },
                Some(ProviderThinkingMode::Enabled),
                ProviderToolChoiceCompatibility::RequiredAsAuto,
            )
            .expect("encode auto compatibility request");
            let body: Value = serde_json::from_slice(&encoded).expect("request JSON");

            assert_eq!(
                body["tool_choice"],
                match choice {
                    ModelToolChoice::None => json!("none"),
                    ModelToolChoice::Auto | ModelToolChoice::RequiredCargoTest => json!("auto"),
                }
            );
            assert_eq!(body["thinking"]["type"], "enabled");
            let tools = body["tools"].as_array().expect("tools array");
            assert_eq!(tools.len(), expected_tool_count);
            if choice == ModelToolChoice::RequiredCargoTest {
                assert_eq!(tools[0]["function"]["name"], "cargo_test");
            }
        }
    }
}
