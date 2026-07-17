use std::sync::{Arc, Mutex};

use coding_agent_core::{
    ActivityEvent, ActivityLevel, AgentEvent, AgentLimits, DiffEvent, ModelMessage, ModelProvider,
    ModelRequest, ModelResponse, PlanEvent, RuntimeError, TestEvent, TestStatus, ToolCall,
    ToolRequest, ToolResult, ToolRuntime,
};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct ScriptedProvider {
    requests: Mutex<Vec<ModelRequest>>,
}

#[async_trait::async_trait]
impl ModelProvider for ScriptedProvider {
    async fn complete(
        &self,
        request: ModelRequest,
        cancellation: CancellationToken,
    ) -> Result<ModelResponse, coding_agent_core::ProviderError> {
        assert!(!cancellation.is_cancelled());
        self.requests.lock().unwrap().push(request);
        Ok(ModelResponse::ToolCall(ToolCall {
            id: "call-1".to_owned(),
            request: ToolRequest::GitStatus,
        }))
    }
}

#[derive(Default)]
struct ScriptedRuntime {
    requests: Mutex<Vec<ToolRequest>>,
}

#[async_trait::async_trait]
impl ToolRuntime for ScriptedRuntime {
    async fn invoke(
        &self,
        request: ToolRequest,
        cancellation: CancellationToken,
    ) -> Result<ToolResult, RuntimeError> {
        assert!(!cancellation.is_cancelled());
        self.requests.lock().unwrap().push(request);
        Ok(ToolResult::text("clean"))
    }
}

#[tokio::test]
async fn provider_and_runtime_ports_preserve_one_tool_call_id() {
    let provider: Arc<dyn ModelProvider> = Arc::new(ScriptedProvider::default());
    let runtime: Arc<dyn ToolRuntime> = Arc::new(ScriptedRuntime::default());
    let cancellation = CancellationToken::new();
    let request = ModelRequest {
        messages: vec![ModelMessage::user("inspect the repository")],
    };

    let response = provider
        .complete(request, cancellation.clone())
        .await
        .unwrap();
    let ModelResponse::ToolCall(call) = response else {
        panic!("scripted provider must return exactly one call");
    };
    let result = runtime
        .invoke(call.request.clone(), cancellation.clone())
        .await
        .unwrap();
    let assistant_message = ModelMessage::AssistantToolCall(call.clone());
    let tool_message = ModelMessage::tool_result(call.id.clone(), result.content());

    assert_eq!(call.id, "call-1");
    assert_eq!(result.content(), "clean");
    assert!(!result.truncated());
    assert!(matches!(
        assistant_message,
        ModelMessage::AssistantToolCall(ToolCall { ref id, .. }) if id == "call-1"
    ));
    assert!(matches!(
        tool_message,
        ModelMessage::ToolResult {
            ref tool_call_id,
            ..
        } if tool_call_id == "call-1"
    ));
}

#[test]
fn limits_reject_every_zero_budget() {
    let valid = AgentLimits::try_new(16, 32, 1_048_576, 262_144).unwrap();
    assert_eq!(valid.max_model_steps(), 16);
    assert_eq!(valid.max_tool_calls(), 32);

    assert!(AgentLimits::try_new(0, 32, 1, 1).is_err());
    assert!(AgentLimits::try_new(16, 0, 1, 1).is_err());
    assert!(AgentLimits::try_new(16, 32, 0, 1).is_err());
    assert!(AgentLimits::try_new(16, 32, 1, 0).is_err());
}

#[test]
fn neutral_events_cover_the_existing_runner_panels() {
    let events = [
        AgentEvent::Plan(PlanEvent {
            revision: 1,
            items: Vec::new(),
        }),
        AgentEvent::Activity(ActivityEvent {
            level: ActivityLevel::Info,
            message: "started".to_owned(),
        }),
        AgentEvent::Diff(DiffEvent {
            revision: 1,
            files: Vec::new(),
        }),
        AgentEvent::Tests(TestEvent {
            revision: 1,
            status: TestStatus::Queued,
            cases: Vec::new(),
        }),
    ];

    assert!(matches!(events[0], AgentEvent::Plan(_)));
    assert!(matches!(events[1], AgentEvent::Activity(_)));
    assert!(matches!(events[2], AgentEvent::Diff(_)));
    assert!(matches!(events[3], AgentEvent::Tests(_)));
}
