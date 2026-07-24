use std::collections::BTreeSet;

use coding_agent_core::{
    ActionRequest, AllowedActions, ControlKind, ModelMessage, ModelRequest, ModelResponse,
    ModelToolChoice, ProviderError, RequiredAction, RequiredCheckKind, RuntimeActionRequest,
    ToolCall, ToolCallBatch, canonical_tool_result_wire_value,
};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Value, error::Category, json};

use crate::config::{ProviderThinkingMode, ProviderToolChoiceCompatibility};
use crate::error::{
    invalid_request, invalid_response, invalid_role_output, rejected_response_reasoning,
    role_action_not_allowed, tool_choice_violated, unsupported_response_finish,
    unsupported_response_schema,
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

pub fn encode_chat_completions_request_with_compatibility(
    model: &str,
    request: &ModelRequest,
    compatibility: ProviderToolChoiceCompatibility,
) -> Result<Vec<u8>, ProviderError> {
    encode_chat_completions_request_with_options(model, request, None, compatibility)
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

    validate_model_request_contract(request)?;
    validate_tool_call_round_trips(&request.messages)?;
    let messages = request
        .messages
        .iter()
        .map(encode_message)
        .collect::<Result<Vec<_>, _>>()?;
    let wire = ChatCompletionsRequestWire {
        model,
        messages,
        tools: tool_definitions_for_request(request, tool_choice_compatibility),
        tool_choice: encode_tool_choice(&request.tool_choice, tool_choice_compatibility),
        thinking: thinking_mode.map(|mode| ThinkingWire {
            kind: mode.request_value(),
        }),
        parallel_tool_calls: false,
        stream: false,
    };
    serde_json::to_vec(&wire)
        .map_err(|_| invalid_request("The provider request could not be encoded."))
}

fn validate_model_request_contract(request: &ModelRequest) -> Result<(), ProviderError> {
    match &request.tool_choice {
        ModelToolChoice::Required(required)
            if !request.allowed_actions.allows_required(required) =>
        {
            return Err(invalid_request(
                "The required provider action is not allowed by this request.",
            ));
        }
        ModelToolChoice::RequiredCargoTest if !request.allowed_actions.is_legacy() => {
            return Err(invalid_request(
                "The legacy required action is not allowed by this request.",
            ));
        }
        _ => {}
    }
    if request.messages.iter().any(|message| {
        matches!(
            message,
            ModelMessage::AssistantToolCalls(batch)
                if batch
                    .calls
                    .iter()
                    .any(|call| !request.allowed_actions.allows_action(&call.request))
        )
    }) {
        return Err(invalid_request(
            "The provider transcript contains an action outside the request capability set.",
        ));
    }
    Ok(())
}

pub fn decode_chat_completions_response(
    encoded: &[u8],
    max_response_bytes: usize,
) -> Result<ModelResponse, ProviderError> {
    decode_chat_completions_response_with_tool_choice(
        encoded,
        max_response_bytes,
        &AllowedActions::legacy(),
        &ModelToolChoice::Auto,
        None,
    )
}

pub fn decode_chat_completions_response_for_request(
    encoded: &[u8],
    max_response_bytes: usize,
    request: &ModelRequest,
) -> Result<ModelResponse, ProviderError> {
    decode_chat_completions_response_with_tool_choice(
        encoded,
        max_response_bytes,
        &request.allowed_actions,
        &request.tool_choice,
        None,
    )
}

pub(crate) fn decode_chat_completions_response_with_tool_choice(
    encoded: &[u8],
    max_response_bytes: usize,
    allowed_actions: &AllowedActions,
    tool_choice: &ModelToolChoice,
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
    if allowed_actions.role().is_some() && !has_tool_calls {
        return Err(invalid_role_output());
    }
    match tool_choice {
        ModelToolChoice::Auto => {}
        ModelToolChoice::None if has_tool_calls => {
            return Err(action_contract_violated(allowed_actions));
        }
        ModelToolChoice::None => {}
        ModelToolChoice::Required(RequiredAction::TerminalOrBlocked(normal_terminal))
            if choice.message.tool_calls.len() != 1
                || !matches!(
                    choice.message.tool_calls[0].function.name.as_str(),
                    name if name == normal_terminal.name() || name == "report_blocked"
                ) =>
        {
            return Err(action_contract_violated(allowed_actions));
        }
        ModelToolChoice::Required(RequiredAction::TerminalOrBlocked(_)) => {}
        ModelToolChoice::Required(
            required @ (RequiredAction::ReviewDiffManifestOrTerminal { .. }
            | RequiredAction::ReviewDiffChunksOrTerminal { .. }),
        ) if choice.message.tool_calls.len() != 1
            || !matches!(
                choice.message.tool_calls[0].function.name.as_str(),
                name if name == required.action_name()
                    || name == "submit_review"
                    || name == "report_blocked"
            ) =>
        {
            return Err(action_contract_violated(allowed_actions));
        }
        ModelToolChoice::Required(
            RequiredAction::ReviewDiffManifestOrTerminal { .. }
            | RequiredAction::ReviewDiffChunksOrTerminal { .. },
        ) => {}
        ModelToolChoice::Required(required)
            if choice.message.tool_calls.len() != 1
                || choice.message.tool_calls[0].function.name != required.action_name() =>
        {
            return Err(action_contract_violated(allowed_actions));
        }
        ModelToolChoice::RequiredCargoTest
            if choice.message.tool_calls.len() != 1
                || choice.message.tool_calls[0].function.name != "cargo_test" =>
        {
            return Err(action_contract_violated(allowed_actions));
        }
        ModelToolChoice::Required(_) | ModelToolChoice::RequiredCargoTest => {}
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
                let request = decode_action_request(
                    allowed_actions,
                    &call.function.name,
                    &call.function.arguments,
                )?;
                Ok(ToolCall {
                    id: call.id,
                    request,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        validate_decoded_action_batch(allowed_actions, tool_choice, &calls)?;
        let decoded = ModelResponse::ToolCalls(ToolCallBatch {
            assistant_content,
            reasoning_content,
            calls,
        });
        if !tool_choice.permits(&decoded) {
            return Err(action_contract_violated(allowed_actions));
        }
        return Ok(decoded);
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
    let decoded = ModelResponse::Final { content };
    if allowed_actions.role().is_some() {
        return Err(invalid_role_output());
    }
    if !tool_choice.permits(&decoded) {
        return Err(tool_choice_violated());
    }
    Ok(decoded)
}

fn validate_decoded_action_batch(
    allowed_actions: &AllowedActions,
    tool_choice: &ModelToolChoice,
    calls: &[ToolCall],
) -> Result<(), ProviderError> {
    if calls.is_empty()
        || calls
            .iter()
            .any(|call| !allowed_actions.allows_action(&call.request))
        || (calls
            .iter()
            .any(|call| matches!(call.request, ActionRequest::Control(_)))
            && calls.len() != 1)
    {
        return Err(action_contract_violated(allowed_actions));
    }
    match tool_choice {
        ModelToolChoice::Required(
            RequiredAction::ReviewDiffManifestOrTerminal { .. }
            | RequiredAction::ReviewDiffChunksOrTerminal { .. },
        ) if matches!(
            calls,
            [ToolCall {
                request: ActionRequest::Control(
                    coding_agent_core::ControlRequest::SubmitReview(submission)
                ),
                ..
            }] if submission.is_approved()
        ) =>
        {
            Err(invalid_role_output())
        }
        ModelToolChoice::Required(required)
            if calls.len() != 1 || !required.matches(&calls[0].request) =>
        {
            Err(action_contract_violated(allowed_actions))
        }
        ModelToolChoice::RequiredCargoTest
            if calls.len() != 1 || !RequiredAction::LegacyCargoTest.matches(&calls[0].request) =>
        {
            Err(action_contract_violated(allowed_actions))
        }
        ModelToolChoice::Auto
            if calls.iter().any(|call| {
                matches!(
                    call.request,
                    ActionRequest::Runtime(
                        RuntimeActionRequest::Validation { .. }
                            | RuntimeActionRequest::ReviewDiffManifest { .. }
                            | RuntimeActionRequest::ReviewDiffChunks { .. }
                    )
                )
            }) =>
        {
            Err(action_contract_violated(allowed_actions))
        }
        ModelToolChoice::None => Err(action_contract_violated(allowed_actions)),
        _ => Ok(()),
    }
}

fn action_contract_violated(allowed_actions: &AllowedActions) -> ProviderError {
    if allowed_actions.role().is_some() {
        role_action_not_allowed()
    } else {
        tool_choice_violated()
    }
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
    choice: &ModelToolChoice,
    compatibility: ProviderToolChoiceCompatibility,
) -> Value {
    match (choice, compatibility) {
        (
            ModelToolChoice::Required(
                RequiredAction::TerminalOrBlocked(_)
                | RequiredAction::ReviewDiffManifestOrTerminal { .. }
                | RequiredAction::ReviewDiffChunksOrTerminal { .. },
            ),
            ProviderToolChoiceCompatibility::RequiredAsAuto,
        ) => json!("auto"),
        (
            ModelToolChoice::Required(
                RequiredAction::TerminalOrBlocked(_)
                | RequiredAction::ReviewDiffManifestOrTerminal { .. }
                | RequiredAction::ReviewDiffChunksOrTerminal { .. },
            ),
            ProviderToolChoiceCompatibility::Strict
            | ProviderToolChoiceCompatibility::RequiredAsRequired,
        ) => {
            // A normal terminal and report_blocked are both valid uses of the
            // one reserved convergence call. No named strict choice can
            // express this two-schema union, so strict and required
            // compatibility use the standard exactly-one required choice.
            // RequiredAsAuto retains its configured wire behavior; core still
            // revalidates exactly one returned typed control in every mode.
            json!("required")
        }
        (
            ModelToolChoice::Required(_) | ModelToolChoice::RequiredCargoTest,
            ProviderToolChoiceCompatibility::RequiredAsRequired,
        ) => {
            json!("required")
        }
        (
            ModelToolChoice::Required(_) | ModelToolChoice::RequiredCargoTest,
            ProviderToolChoiceCompatibility::RequiredAsAuto,
        ) => {
            json!("auto")
        }
        (ModelToolChoice::Auto, _) => json!("auto"),
        (ModelToolChoice::None, _) => json!("none"),
        (ModelToolChoice::Required(required), _) => {
            json!({"type": "function", "function": {"name": required.action_name()}})
        }
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
                    let name = call.request.name();
                    let arguments =
                        String::from_utf8(call.request.canonical_arguments().map_err(|_| {
                            invalid_request(
                                "The provider tool call arguments could not be encoded.",
                            )
                        })?)
                        .map_err(|_| {
                            invalid_request(
                                "The provider tool call arguments could not be encoded.",
                            )
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
        } => canonical_tool_result_wire_value(tool_call_id, content)
            .map_err(|_| invalid_request("The provider tool result ID is invalid.")),
    }
}

fn decode_action_request(
    allowed_actions: &AllowedActions,
    name: &str,
    arguments: &str,
) -> Result<ActionRequest, ProviderError> {
    let decoded = match allowed_actions.role() {
        Some(role) => ActionRequest::decode(role, name, arguments),
        None => ActionRequest::decode_legacy(name, arguments),
    };
    decoded.map_err(|_| {
        if is_allowed_role_terminal(allowed_actions, name) {
            invalid_role_output()
        } else if allowed_actions.role().is_some() {
            role_action_not_allowed()
        } else {
            invalid_response("The provider tool call arguments are invalid.")
        }
    })
}

fn is_allowed_role_terminal(allowed_actions: &AllowedActions, name: &str) -> bool {
    allowed_actions.allows_name(name)
        && [
            ControlKind::SubmitPlan,
            ControlKind::SubmitExecution,
            ControlKind::SubmitReview,
            ControlKind::ReportBlocked,
        ]
        .into_iter()
        .any(|kind| kind.name() == name)
}

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
    request: &ModelRequest,
    _compatibility: ProviderToolChoiceCompatibility,
) -> Vec<Value> {
    match &request.tool_choice {
        ModelToolChoice::Required(RequiredAction::TerminalOrBlocked(normal_terminal)) => vec![
            role_action_definition(normal_terminal.name()),
            role_action_definition("report_blocked"),
        ],
        ModelToolChoice::Required(
            required @ (RequiredAction::ReviewDiffManifestOrTerminal { .. }
            | RequiredAction::ReviewDiffChunksOrTerminal { .. }),
        ) => vec![
            required_action_definition(required),
            early_reviewer_submission_definition(),
            role_action_definition("report_blocked"),
        ],
        ModelToolChoice::Required(required) => vec![required_action_definition(required)],
        ModelToolChoice::RequiredCargoTest => vec![legacy_action_definition("cargo_test")],
        ModelToolChoice::Auto | ModelToolChoice::None => request
            .allowed_actions
            .names()
            .into_iter()
            .map(|name| {
                if request.allowed_actions.is_legacy() {
                    legacy_action_definition(name)
                } else {
                    role_action_definition(name)
                }
            })
            .collect(),
    }
}

fn legacy_action_definition(name: &str) -> Value {
    match name {
        "list_files" => function_tool(
            "list_files",
            "List repository files below a relative path.",
            json!({
                "path": {"type": "string"},
                "depth": {"type": "integer", "minimum": 1},
                "limit": {"type": "integer", "minimum": 1}
            }),
            &["path", "depth", "limit"],
        ),
        "read_file" => function_tool(
            "read_file",
            "Read an inclusive line range from a UTF-8 repository file.",
            json!({
                "path": {"type": "string"},
                "start_line": {"type": "integer", "minimum": 1},
                "end_line": {"type": "integer", "minimum": 1}
            }),
            &["path", "start_line", "end_line"],
        ),
        "search_text" => function_tool(
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
        "replace_file" => function_tool(
            "replace_file",
            "Atomically replace a UTF-8 repository file.",
            json!({
                "path": {"type": "string"},
                "expected_sha256": {"type": ["string", "null"]},
                "content": {"type": "string"}
            }),
            &["path", "content"],
        ),
        "cargo_check" => function_tool(
            "cargo_check",
            "Run the bounded typed Cargo check operation.",
            json!({
                "package": {"type": ["string", "null"]},
                "timeout_ms": {"type": "integer", "minimum": 1}
            }),
            &["timeout_ms"],
        ),
        "cargo_test" => function_tool(
            "cargo_test",
            "Run the bounded typed Cargo test operation.",
            json!({
                "package": {"type": ["string", "null"]},
                "test": {"type": ["string", "null"]},
                "timeout_ms": {"type": "integer", "minimum": 1}
            }),
            &["timeout_ms"],
        ),
        "git_status" => function_tool(
            "git_status",
            "Read the sanitized repository status.",
            json!({}),
            &[],
        ),
        "git_diff" => function_tool(
            "git_diff",
            "Read the bounded sanitized repository diff.",
            json!({}),
            &[],
        ),
        _ => unreachable!("legacy action set is closed"),
    }
}

fn role_action_definition(name: &str) -> Value {
    match name {
        "list_files" | "read_file" | "search_text" | "replace_file" | "git_status" | "git_diff" => {
            legacy_action_definition(name)
        }
        "cargo_check" => function_tool(
            "cargo_check",
            "Run a canonical Cargo check selector.",
            json!({"package": nullable_selector_schema()}),
            &["package"],
        ),
        "cargo_test" => function_tool(
            "cargo_test",
            "Run a canonical Cargo test selector.",
            json!({
                "package": nullable_selector_schema(),
                "integration_test": nullable_selector_schema()
            }),
            &["package", "integration_test"],
        ),
        "review_diff_manifest" => function_tool(
            "review_diff_manifest",
            "Read the authoritative manifest for the current review checkpoint.",
            json!({
                "generation": generation_schema(),
                "workspace_digest": digest_schema(None)
            }),
            &["generation", "workspace_digest"],
        ),
        "review_diff_chunks" => function_tool(
            "review_diff_chunks",
            "Read one exact contiguous authoritative diff chunk range.",
            json!({
                "generation": generation_schema(),
                "workspace_digest": digest_schema(None),
                "manifest_sha256": lower_hex_schema(),
                "start_chunk": {"type": "integer", "minimum": 0, "maximum": 7},
                "count": {"type": "integer", "minimum": 1, "maximum": 2}
            }),
            &[
                "generation",
                "workspace_digest",
                "manifest_sha256",
                "start_chunk",
                "count",
            ],
        ),
        "submit_plan" => function_tool(
            "submit_plan",
            "Submit the complete structured plan.",
            json!({
                "summary": bounded_string_schema(4_096, false),
                "steps": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 32,
                    "items": {
                        "type": "object",
                        "properties": {
                            "title": bounded_string_schema(256, true),
                            "description": bounded_string_schema(4_096, false),
                            "acceptance_criteria": {
                                "type": "array",
                                "minItems": 1,
                                "maxItems": 8,
                                "items": bounded_string_schema(1_024, true)
                            }
                        },
                        "required": ["title", "description", "acceptance_criteria"],
                        "additionalProperties": false
                    }
                },
                "initial_required_checks": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 16,
                    "items": check_selector_schema()
                }
            }),
            &["summary", "steps", "initial_required_checks"],
        ),
        "submit_execution" => function_tool(
            "submit_execution",
            "Submit the bounded execution summary.",
            json!({"summary": bounded_string_schema(4_096, false)}),
            &["summary"],
        ),
        "submit_review" => function_tool(
            "submit_review",
            "Submit one structured review verdict.",
            json!({
                "verdict": {"type": "string", "enum": ["approved", "changes_requested"]},
                "summary": bounded_string_schema(4_096, true),
                "findings": {
                    "type": "array",
                    "maxItems": 32,
                    "items": {
                        "type": "object",
                        "properties": {
                            "severity": {"type": "string", "enum": ["blocking", "advisory"]},
                            "message": bounded_string_schema(2_048, true),
                            "path": {"type": ["string", "null"], "maxLength": 4_096},
                            "line": {"type": ["integer", "null"], "minimum": 1, "maximum": 9_007_199_254_740_991_u64}
                        },
                        "required": ["severity", "message", "path", "line"],
                        "additionalProperties": false
                    }
                },
                "add_required_checks": {
                    "type": "array",
                    "maxItems": 16,
                    "items": check_selector_schema()
                }
            }),
            &["verdict", "summary", "findings", "add_required_checks"],
        ),
        "report_blocked" => function_tool(
            "report_blocked",
            "End the current role with one controlled blocked reason.",
            json!({
                "reason": {
                    "type": "string",
                    "enum": [
                        "missing_required_context",
                        "conflicting_user_requirements",
                        "requires_goal_change",
                        "unsupported_scope"
                    ]
                },
                "summary": bounded_string_schema(4_096, false)
            }),
            &["reason", "summary"],
        ),
        "update_plan_progress" => function_tool(
            "update_plan_progress",
            "Atomically advance statuses of existing plan steps.",
            json!({
                "updates": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 32,
                    "items": {
                        "type": "object",
                        "properties": {
                            "step_id": bounded_string_schema(256, true),
                            "status": {"type": "string", "enum": ["running", "completed"]}
                        },
                        "required": ["step_id", "status"],
                        "additionalProperties": false
                    }
                }
            }),
            &["updates"],
        ),
        _ => unreachable!("role action set is closed"),
    }
}

fn required_action_definition(required: &RequiredAction) -> Value {
    match required {
        RequiredAction::LegacyCargoTest => legacy_action_definition("cargo_test"),
        RequiredAction::Validation(check) => match check.selector().kind() {
            RequiredCheckKind::CargoCheck => function_tool(
                "cargo_check",
                "Run the exact required Cargo check.",
                json!({
                    "check_id": {"const": check.id()},
                    "package": {"const": check.package()}
                }),
                &["check_id", "package"],
            ),
            RequiredCheckKind::CargoTest => function_tool(
                "cargo_test",
                "Run the exact required Cargo test.",
                json!({
                    "check_id": {"const": check.id()},
                    "package": {"const": check.package()},
                    "integration_test": {"const": check.integration_test()}
                }),
                &["check_id", "package", "integration_test"],
            ),
        },
        RequiredAction::ReviewDiffManifest {
            generation,
            workspace_digest,
        }
        | RequiredAction::ReviewDiffManifestOrTerminal {
            generation,
            workspace_digest,
        } => function_tool(
            "review_diff_manifest",
            "Read the exact required review diff manifest.",
            json!({
                "generation": {"const": generation},
                "workspace_digest": digest_schema(Some(workspace_digest.value()))
            }),
            &["generation", "workspace_digest"],
        ),
        RequiredAction::ReviewDiffChunks {
            generation,
            workspace_digest,
            manifest_sha256,
            start_chunk,
            count,
        }
        | RequiredAction::ReviewDiffChunksOrTerminal {
            generation,
            workspace_digest,
            manifest_sha256,
            start_chunk,
            count,
        } => function_tool(
            "review_diff_chunks",
            "Read the exact required review diff chunk range.",
            json!({
                "generation": {"const": generation},
                "workspace_digest": digest_schema(Some(workspace_digest.value())),
                "manifest_sha256": {"const": manifest_sha256},
                "start_chunk": {"const": start_chunk},
                "count": {"const": count}
            }),
            &[
                "generation",
                "workspace_digest",
                "manifest_sha256",
                "start_chunk",
                "count",
            ],
        ),
        RequiredAction::Terminal(kind) | RequiredAction::TerminalOrBlocked(kind) => {
            role_action_definition(kind.name())
        }
    }
}

fn early_reviewer_submission_definition() -> Value {
    let mut definition = role_action_definition("submit_review");
    definition["function"]["parameters"]["properties"]["verdict"] =
        json!({"const": "changes_requested"});
    definition
}

fn bounded_string_schema(max: usize, non_empty: bool) -> Value {
    let mut schema = json!({"type": "string", "maxLength": max});
    if non_empty {
        schema["minLength"] = json!(1);
    }
    schema
}

fn nullable_selector_schema() -> Value {
    json!({
        "type": ["string", "null"],
        "minLength": 1,
        "maxLength": 128,
        "pattern": "^[A-Za-z0-9_][A-Za-z0-9_-]*$"
    })
}

fn generation_schema() -> Value {
    json!({
        "type": "integer",
        "minimum": 0,
        "maximum": 9_007_199_254_740_991_u64
    })
}

fn lower_hex_schema() -> Value {
    json!({
        "type": "string",
        "minLength": 64,
        "maxLength": 64,
        "pattern": "^[0-9a-f]{64}$"
    })
}

fn digest_schema(exact_value: Option<&str>) -> Value {
    let value_schema = exact_value.map_or_else(lower_hex_schema, |value| json!({"const": value}));
    json!({
        "type": "object",
        "properties": {
            "algorithm": {"const": "workspace_fingerprint_v1"},
            "value": value_schema
        },
        "required": ["algorithm", "value"],
        "additionalProperties": false
    })
}

fn check_selector_schema() -> Value {
    json!({
        "oneOf": [
            {
                "type": "object",
                "properties": {
                    "kind": {"const": "cargo_check"},
                    "package": nullable_selector_schema()
                },
                "required": ["kind", "package"],
                "additionalProperties": false
            },
            {
                "type": "object",
                "properties": {
                    "kind": {"const": "cargo_test"},
                    "package": nullable_selector_schema(),
                    "integration_test": nullable_selector_schema()
                },
                "required": ["kind", "package", "integration_test"],
                "additionalProperties": false
            }
        ]
    })
}

fn function_tool(
    name: &str,
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
            &AllowedActions::legacy(),
            &ModelToolChoice::Auto,
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
                allowed_actions: AllowedActions::legacy(),
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
                    allowed_actions: AllowedActions::legacy(),
                    tool_choice: choice.clone(),
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
                    allowed_actions: AllowedActions::legacy(),
                    tool_choice: choice.clone(),
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
                    ModelToolChoice::Auto
                    | ModelToolChoice::Required(_)
                    | ModelToolChoice::RequiredCargoTest => json!("auto"),
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
