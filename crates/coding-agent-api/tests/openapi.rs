use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use coding_agent_api::{
    ApiDoc, CanonicalPathDto, QuitAcceptance, ServiceStateControl, ServiceStateDto,
    StreamResetControl, TaskEventDto, UtcTimestampDto, api_openapi,
};
use coding_agent_domain::{
    CanonicalPath, EventId, PlanItem, PlanItemStatus, PlanSnapshot, TaskEvent, TaskEventPayload,
    TaskId, UtcTimestamp,
};
use serde_json::{Value, json};
use tempfile::tempdir;
use utoipa::OpenApi;

fn openapi_value() -> Value {
    serde_json::to_value(ApiDoc::openapi()).expect("OpenAPI must serialize")
}

fn string_set(values: &Value) -> BTreeSet<String> {
    values
        .as_array()
        .expect("expected array")
        .iter()
        .map(|value| value.as_str().expect("expected string").to_owned())
        .collect()
}

fn property_set(schema: &Value) -> BTreeSet<String> {
    schema["properties"]
        .as_object()
        .expect("schema must have object properties")
        .keys()
        .cloned()
        .collect()
}

fn set(values: &[&str]) -> BTreeSet<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn schema_accepts_null(schema: &Value) -> bool {
    schema["nullable"] == true
        || schema["type"]
            .as_array()
            .is_some_and(|types| types.iter().any(|kind| kind == "null"))
        || ["oneOf", "anyOf"].iter().any(|union| {
            schema[*union]
                .as_array()
                .is_some_and(|items| items.iter().any(|item| item["type"] == "null"))
        })
}

#[test]
fn task_event_schema_is_a_discriminated_union_of_ten_flat_envelopes() {
    let value = openapi_value();
    let schema = &value["components"]["schemas"]["TaskEventDto"];

    assert_eq!(schema["discriminator"]["propertyName"], "kind");
    let variants = schema["oneOf"].as_array().expect("oneOf must be present");
    assert_eq!(variants.len(), 10);
    assert_eq!(
        variants
            .iter()
            .map(|variant| {
                variant["$ref"]
                    .as_str()
                    .expect("event alternatives must be component refs")
                    .rsplit('/')
                    .next()
                    .unwrap()
                    .to_owned()
            })
            .collect::<BTreeSet<_>>(),
        set(&[
            "ActivityAppendedEventDto",
            "DiffUpdatedEventDto",
            "PlanUpdatedEventDto",
            "TaskCancelledEventDto",
            "TaskCompletedEventDto",
            "TaskFailedEventDto",
            "TaskInterruptedEventDto",
            "TaskQueuedEventDto",
            "TaskStartedEventDto",
            "TestUpdatedEventDto",
        ])
    );
}

#[test]
fn task_event_discriminator_explicitly_maps_every_dotted_kind_to_its_envelope() {
    let value = openapi_value();
    let discriminator = &value["components"]["schemas"]["TaskEventDto"]["discriminator"];

    assert_eq!(
        discriminator["mapping"],
        json!({
            "task.queued": "#/components/schemas/TaskQueuedEventDto",
            "task.started": "#/components/schemas/TaskStartedEventDto",
            "plan.updated": "#/components/schemas/PlanUpdatedEventDto",
            "activity.appended": "#/components/schemas/ActivityAppendedEventDto",
            "diff.updated": "#/components/schemas/DiffUpdatedEventDto",
            "test.updated": "#/components/schemas/TestUpdatedEventDto",
            "task.completed": "#/components/schemas/TaskCompletedEventDto",
            "task.failed": "#/components/schemas/TaskFailedEventDto",
            "task.cancelled": "#/components/schemas/TaskCancelledEventDto",
            "task.interrupted": "#/components/schemas/TaskInterruptedEventDto",
        })
    );
}

#[test]
fn event_kind_component_literals_match_runtime_discriminators() {
    let value = openapi_value();
    let schemas = &value["components"]["schemas"];

    for (schema, wire_literal) in [
        ("TaskQueuedKind", "task.queued"),
        ("TaskStartedKind", "task.started"),
        ("PlanUpdatedKind", "plan.updated"),
        ("ActivityAppendedKind", "activity.appended"),
        ("DiffUpdatedKind", "diff.updated"),
        ("TestUpdatedKind", "test.updated"),
        ("TaskCompletedKind", "task.completed"),
        ("TaskFailedKind", "task.failed"),
        ("TaskCancelledKind", "task.cancelled"),
        ("TaskInterruptedKind", "task.interrupted"),
    ] {
        assert_eq!(
            schemas[schema]["enum"],
            json!([wire_literal]),
            "{schema} must document its runtime discriminator literal"
        );
    }
}

#[test]
fn openapi_contains_every_approved_top_level_component_and_no_task_12_paths() {
    let value = openapi_value();
    let schemas = value["components"]["schemas"]
        .as_object()
        .expect("components.schemas must exist");

    for schema in [
        "UtcTimestampDto",
        "CanonicalPathDto",
        "SessionExchangeRequest",
        "AddRepositoryRequest",
        "CreateTaskRequest",
        "RepositoryDto",
        "TaskDto",
        "TaskDetailDto",
        "TaskEventDto",
        "BootstrapResponse",
        "StreamResetControl",
        "ServiceStateControl",
        "SseMessage",
        "ApiErrorResponse",
        "CancellationAcceptedResponse",
        "QuitResponse",
    ] {
        assert!(schemas.contains_key(schema), "missing {schema}");
    }

    assert_eq!(value["paths"], json!({}));
}

#[test]
fn task_and_detail_components_have_exact_required_and_nullable_shapes() {
    let value = openapi_value();
    let schemas = &value["components"]["schemas"];
    let task = &schemas["TaskDto"];

    let task_fields = [
        "id",
        "client_request_id",
        "repository_id",
        "prompt",
        "status",
        "attempt",
        "retry_of",
        "created_at",
        "started_at",
        "finished_at",
        "last_event_id",
        "failure",
    ];
    assert_eq!(property_set(task), set(&task_fields));
    assert!(string_set(&task["required"]).contains("last_event_id"));
    assert!(!schema_accepts_null(&task["properties"]["last_event_id"]));

    let detail = &schemas["TaskDetailDto"];
    assert_eq!(
        property_set(detail),
        set(&[
            "task",
            "plan",
            "activity",
            "diff",
            "tests",
            "timeline",
            "event_cursor",
        ])
    );
    assert!(schema_accepts_null(&detail["properties"]["plan"]));
    assert!(schema_accepts_null(&detail["properties"]["diff"]));
    assert!(schema_accepts_null(&detail["properties"]["tests"]));
    for array in ["activity", "timeline"] {
        assert_eq!(detail["properties"][array]["type"], "array");
        assert!(string_set(&detail["required"]).contains(array));
    }
}

#[test]
fn diff_file_exposes_a_required_truncation_marker() {
    let value = openapi_value();
    let diff_file = &value["components"]["schemas"]["DiffFileDto"];

    assert_eq!(
        property_set(diff_file),
        set(&[
            "path",
            "status",
            "patch",
            "additions",
            "deletions",
            "truncated",
        ])
    );
    assert_eq!(string_set(&diff_file["required"]), property_set(diff_file));
    assert_eq!(diff_file["properties"]["truncated"]["type"], "boolean");
}

#[test]
fn control_components_are_exact_required_shapes_without_persisted_ids() {
    let value = openapi_value();
    let schemas = &value["components"]["schemas"];

    let stream_reset = &schemas["StreamResetControl"];
    let stream_fields = set(&["schema_version", "kind", "latest_event_id"]);
    assert_eq!(property_set(stream_reset), stream_fields);
    assert_eq!(string_set(&stream_reset["required"]), stream_fields);
    assert!(stream_reset["properties"].get("id").is_none());

    let service_state = &schemas["ServiceStateControl"];
    let service_fields = set(&["schema_version", "kind", "state", "generation"]);
    assert_eq!(property_set(service_state), service_fields);
    assert_eq!(string_set(&service_state["required"]), service_fields);
    assert!(service_state["properties"].get("id").is_none());
}

#[test]
fn event_json_is_a_flat_typed_wire_frame() {
    let timestamp = UtcTimestamp::parse_rfc3339("2026-07-15T01:02:03Z").unwrap();
    let event = TaskEvent::new(
        EventId::new(17).unwrap(),
        TaskId::new(),
        TaskEventPayload::PlanUpdated {
            plan: PlanSnapshot {
                revision: 3,
                items: vec![PlanItem {
                    id: "compile".to_owned(),
                    title: "Compile the API".to_owned(),
                    status: PlanItemStatus::Running,
                }],
            },
        },
        timestamp,
    );

    let value = serde_json::to_value(TaskEventDto::from(event)).unwrap();
    assert_eq!(
        value
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>(),
        set(&[
            "id",
            "schema_version",
            "task_id",
            "kind",
            "payload",
            "created_at",
        ])
    );
    assert_eq!(value["id"], 17);
    assert_eq!(value["schema_version"], 1);
    assert_eq!(value["kind"], "plan.updated");
    assert_eq!(value["payload"]["plan"]["revision"], 3);
    assert_eq!(value["payload"]["plan"]["items"][0]["status"], "running");
    assert!(value["payload"].get("id").is_none());
    assert!(value["payload"].get("kind").is_none());
    assert_eq!(value["created_at"], "2026-07-15T01:02:03.000000000Z");
}

#[test]
fn transport_scalars_serialize_only_after_domain_validation() {
    let timestamp = UtcTimestamp::parse_rfc3339("2026-07-15T01:02:03+08:00").unwrap();
    assert_eq!(
        serde_json::to_value(UtcTimestampDto::from(timestamp)).unwrap(),
        "2026-07-14T17:02:03.000000000Z"
    );

    let path = std::env::current_dir().unwrap();
    let canonical = CanonicalPath::try_from_canonical(path.clone()).unwrap();
    assert_eq!(
        serde_json::to_value(CanonicalPathDto::from(canonical)).unwrap(),
        path.to_string_lossy().as_ref()
    );
}

#[test]
fn control_constructors_fix_schema_and_kind_and_never_emit_an_id() {
    let reset = serde_json::to_value(StreamResetControl::new(41)).unwrap();
    assert_eq!(
        reset,
        json!({
            "schema_version": 1,
            "kind": "stream.reset",
            "latest_event_id": 41,
        })
    );

    let state =
        serde_json::to_value(ServiceStateControl::new(ServiceStateDto::StoreDegraded, 9)).unwrap();
    assert_eq!(
        state,
        json!({
            "schema_version": 1,
            "kind": "service.state",
            "state": "store_degraded",
            "generation": 9,
        })
    );
}

#[test]
fn quit_acceptance_trigger_can_be_taken_exactly_once() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let mut acceptance = QuitAcceptance::new(move || {
        observed.fetch_add(1, Ordering::SeqCst);
    });

    let trigger = acceptance.take_trigger().expect("first take succeeds");
    assert!(acceptance.take_trigger().is_none());
    trigger();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

fn exporter_path() -> PathBuf {
    if let Some(path) = option_env!("CARGO_BIN_EXE_export_openapi") {
        return PathBuf::from(path);
    }

    let mut path = std::env::current_exe().expect("test executable path");
    path.pop();
    if path.file_name().is_some_and(|name| name == "deps") {
        path.pop();
    }
    path.push(format!("export_openapi{}", std::env::consts::EXE_SUFFIX));
    path
}

fn run_export(output: &Path) {
    let result = Command::new(exporter_path())
        .arg(output)
        .output()
        .expect("exporter must launch");
    assert!(
        result.status.success(),
        "exporter failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
}

fn canonical_openapi_bytes() -> Vec<u8> {
    let mut bytes = serde_json::to_vec_pretty(&api_openapi()).unwrap();
    bytes.push(b'\n');
    bytes
}

#[test]
fn exporter_atomically_replaces_sentinel_twice_with_canonical_bytes() {
    let directory = tempdir().unwrap();
    let output = directory.path().join("nested").join("openapi.json");
    fs::create_dir_all(output.parent().unwrap()).unwrap();
    fs::write(&output, b"sentinel: incomplete old document").unwrap();
    let expected = canonical_openapi_bytes();

    for _ in 0..2 {
        run_export(&output);
        let actual = fs::read(&output).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(actual.last(), Some(&b'\n'));
        serde_json::from_slice::<Value>(&actual).expect("replacement is complete valid JSON");
    }

    let entries = fs::read_dir(output.parent().unwrap())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    assert_eq!(entries, [output.file_name().unwrap()]);
}

#[test]
fn exporter_requires_exactly_one_output_path() {
    for args in [Vec::<&str>::new(), vec!["one.json", "two.json"]] {
        let result = Command::new(exporter_path()).args(args).output().unwrap();
        assert!(!result.status.success());
    }
}
