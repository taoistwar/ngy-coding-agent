use coding_agent_core::{
    ModelMessage, ModelRequest, ModelResponse, ProviderError, ToolCall, ToolRequest,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::error::{invalid_request, invalid_response, unsupported_multiple_tool_calls};

pub fn encode_chat_completions_request(
    model: &str,
    request: &ModelRequest,
) -> Result<Vec<u8>, ProviderError> {
    if model.is_empty() || model.len() > 256 || model.chars().any(char::is_control) {
        return Err(invalid_request("The provider model is invalid."));
    }

    let messages = request
        .messages
        .iter()
        .map(encode_message)
        .collect::<Result<Vec<_>, _>>()?;
    validate_tool_call_round_trips(&request.messages)?;
    let wire = ChatCompletionsRequestWire {
        model,
        messages,
        tools: tool_definitions(),
        tool_choice: "auto",
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
    if encoded.len() > max_response_bytes {
        return Err(invalid_response(
            "The provider response exceeded the configured byte limit.",
        ));
    }
    let response: ChatCompletionsResponseWire = serde_json::from_slice(encoded)
        .map_err(|_| invalid_response("The provider response JSON is invalid."))?;
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
    if choice.message.tool_calls.len() > 1 {
        return Err(unsupported_multiple_tool_calls());
    }

    if let Some(call) = choice.message.tool_calls.into_iter().next() {
        if choice
            .message
            .content
            .as_deref()
            .is_some_and(|content| !content.is_empty())
        {
            return Err(invalid_response(
                "The provider response ambiguously contains text and a tool call.",
            ));
        }
        if choice.finish_reason.as_deref() != Some("tool_calls")
            || call.kind != "function"
            || call.id.is_empty()
            || call.id.len() > 256
            || call.id.chars().any(char::is_control)
        {
            return Err(invalid_response(
                "The provider tool call envelope is invalid.",
            ));
        }
        let request = decode_tool_request(&call.function.name, &call.function.arguments)?;
        return Ok(ModelResponse::ToolCall(ToolCall {
            id: call.id,
            request,
        }));
    }

    let content = choice
        .message
        .content
        .ok_or_else(|| invalid_response("The provider response has no supported content."))?;
    if choice.finish_reason.as_deref() != Some("stop")
        || content.is_empty()
        || content.len() > max_response_bytes
    {
        return Err(invalid_response(
            "The provider final response content is invalid.",
        ));
    }
    Ok(ModelResponse::Final { content })
}

fn validate_tool_call_round_trips(messages: &[ModelMessage]) -> Result<(), ProviderError> {
    let mut pending: Option<&str> = None;
    for message in messages {
        match message {
            ModelMessage::AssistantToolCall(call) if pending.is_none() => {
                pending = Some(call.id.as_str());
            }
            ModelMessage::ToolResult { tool_call_id, .. }
                if pending.is_some_and(|expected| expected == tool_call_id) =>
            {
                pending = None;
            }
            ModelMessage::AssistantToolCall(_) | ModelMessage::ToolResult { .. } => {
                return Err(invalid_request(
                    "Provider tool calls and results must have one matching ID.",
                ));
            }
            _ if pending.is_some() => {
                return Err(invalid_request(
                    "A provider tool result must immediately follow its tool call.",
                ));
            }
            _ => {}
        }
    }
    if pending.is_some() {
        return Err(invalid_request(
            "A provider tool call is missing its matching result.",
        ));
    }
    Ok(())
}

#[derive(Serialize)]
struct ChatCompletionsRequestWire<'a> {
    model: &'a str,
    messages: Vec<Value>,
    tools: Vec<Value>,
    tool_choice: &'static str,
    parallel_tool_calls: bool,
    stream: bool,
}

fn encode_message(message: &ModelMessage) -> Result<Value, ProviderError> {
    match message {
        ModelMessage::System(content) => Ok(json!({"role": "system", "content": content})),
        ModelMessage::User(content) => Ok(json!({"role": "user", "content": content})),
        ModelMessage::Assistant(content) => Ok(json!({"role": "assistant", "content": content})),
        ModelMessage::AssistantToolCall(call) => {
            if call.id.is_empty() || call.id.len() > 256 || call.id.chars().any(char::is_control) {
                return Err(invalid_request("The provider tool call ID is invalid."));
            }
            let (name, arguments) = encode_tool_request(&call.request);
            let arguments = serde_json::to_string(&arguments).map_err(|_| {
                invalid_request("The provider tool call arguments could not be encoded.")
            })?;
            Ok(json!({
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "id": call.id,
                    "type": "function",
                    "function": {"name": name, "arguments": arguments}
                }]
            }))
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
    tool_calls: Vec<ToolCallWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ToolCallWire {
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
        function_tool(
            "cargo_test",
            "Run the bounded typed Cargo test operation.",
            json!({
                "package": {"type": ["string", "null"]},
                "test": {"type": ["string", "null"]},
                "timeout_ms": {"type": "integer", "minimum": 1}
            }),
            &["timeout_ms"],
        ),
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
