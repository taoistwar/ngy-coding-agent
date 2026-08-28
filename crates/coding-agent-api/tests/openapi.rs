use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use coding_agent_api::{
    ApiDoc, CanonicalPathDto, QuitAcceptance, SchedulerAdmissionStateDto, SchedulerLimitsDto,
    SchedulerQueueReasonDto, SchedulerQueuedTaskDto, SchedulerRepositoryStorageDto,
    SchedulerStateDto, SchedulerStopIntentDto, SchedulerStoppingTaskDto, SchedulerStorageDto,
    SchedulerStorageScopeDto, SchedulerStorageStateDto, ServiceStateControl, ServiceStateDto,
    StreamResetControl, TaskEventDto, UtcTimestampDto, api_openapi,
};
use coding_agent_domain::{
    ActivityEntry, ActivityLevel, CanonicalPath, EventId, PlanItem, PlanItemStatus, PlanSnapshot,
    TaskEvent, TaskEventPayload, TaskId, UtcTimestamp,
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

fn assert_exact_required_object(schema: &Value, fields: &[&str]) {
    let fields = set(fields);
    assert_eq!(property_set(schema), fields);
    assert_eq!(string_set(&schema["required"]), fields);
    assert_eq!(schema["additionalProperties"], false);
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
fn task_event_schema_is_a_discriminated_union_of_eleven_flat_envelopes() {
    let value = openapi_value();
    let schema = &value["components"]["schemas"]["TaskEventDto"];

    assert_eq!(schema["discriminator"]["propertyName"], "kind");
    let variants = schema["oneOf"].as_array().expect("oneOf must be present");
    assert_eq!(variants.len(), 11);
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
            "ReviewUpdatedEventDto",
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
            "review.updated": "#/components/schemas/ReviewUpdatedEventDto",
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
        ("ReviewUpdatedKind", "review.updated"),
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
        "RequiredCheckDto",
        "CheckEvidenceDto",
        "ReviewFindingDto",
        "ReviewCoverageDto",
        "ReviewEvidenceDto",
        "ReviewUpdatedEventDto",
        "TaskEventDto",
        "BootstrapResponse",
        "StreamResetControl",
        "ServiceStateControl",
        "SchedulerStateDto",
        "SchedulerAdmissionStateDto",
        "SchedulerLimitsDto",
        "SchedulerQueuedTaskDto",
        "SchedulerQueueReasonDto",
        "SchedulerStoppingTaskDto",
        "SchedulerStopIntentDto",
        "SchedulerStorageDto",
        "SchedulerStorageScopeDto",
        "SchedulerRepositoryStorageDto",
        "SchedulerStorageStateDto",
        "SseMessage",
        "ApiErrorResponse",
        "CancellationAcceptedResponse",
        "QuitResponse",
        "DeliveryPreflightRequest",
        "DeliveryMergeRequest",
        "DeliveryRemoveWorktreeRequest",
        "DeliveryDeleteBranchRequest",
        "DeliveryTaskDto",
        "DeliveryOperationDto",
        "DeliveryCommandResponse",
    ] {
        assert!(schemas.contains_key(schema), "missing {schema}");
    }

    assert_eq!(value["paths"], json!({}));
}

#[test]
fn delivery_openapi_locks_request_bounds_operation_union_and_rest_only_paths() {
    let value = serde_json::to_value(api_openapi()).expect("router OpenAPI must serialize");
    let schemas = &value["components"]["schemas"];

    assert_exact_required_object(
        &schemas["DeliveryPreflightRequest"],
        &["client_request_id", "target_branch", "expected_target_head"],
    );
    assert_exact_required_object(
        &schemas["DeliveryMergeRequest"],
        &[
            "client_request_id",
            "preflight_operation_id",
            "expected_operation_version",
            "expected_review_generation",
            "expected_workspace_fingerprint",
            "target_branch",
            "expected_target_head",
        ],
    );
    assert_exact_required_object(
        &schemas["DeliveryRemoveWorktreeRequest"],
        &[
            "client_request_id",
            "expected_disposition_version",
            "expected_merge_operation_id",
            "expected_source_ref",
            "expected_source_oid",
        ],
    );
    assert_exact_required_object(
        &schemas["DeliveryDeleteBranchRequest"],
        &[
            "client_request_id",
            "expected_disposition_version",
            "expected_merge_operation_id",
            "expected_source_ref",
            "expected_source_oid",
            "target_branch",
            "target_head",
        ],
    );

    let merge = &schemas["DeliveryMergeRequest"]["properties"];
    assert_eq!(merge["expected_operation_version"]["minimum"], 1);
    assert_eq!(
        merge["expected_operation_version"]["maximum"],
        9_007_199_254_740_991_u64
    );
    assert_eq!(merge["expected_review_generation"]["minimum"], 0);
    assert_eq!(merge["expected_workspace_fingerprint"]["minLength"], 64);
    assert_eq!(merge["expected_workspace_fingerprint"]["maxLength"], 64);
    assert_eq!(merge["target_branch"]["maxLength"], 4096);
    assert_eq!(merge["expected_target_head"]["minLength"], 40);
    assert_eq!(merge["expected_target_head"]["maxLength"], 64);

    assert_eq!(
        schemas["DeliveryConflictSummaryDto"]["properties"]["paths"]["maxItems"],
        128
    );
    assert_eq!(
        schemas["DeliveryConflictSummaryDto"]["properties"]["payload_bytes"]["maximum"],
        65_536
    );
    assert_eq!(
        schemas["DeliveryConflictPathDto"]["properties"]["path"]["maxLength"],
        4_096
    );
    assert_eq!(
        schemas["DeliveryOperationDto"]["discriminator"]["propertyName"],
        "kind"
    );
    assert_eq!(
        schemas["DeliveryOperationDto"]["discriminator"]["mapping"],
        json!({
            "merge": "#/components/schemas/DeliveryMergeOperationEnvelopeDto",
            "cleanup": "#/components/schemas/DeliveryCleanupOperationEnvelopeDto",
        })
    );
    assert_eq!(
        schemas["DeliveryOperationDto"]["oneOf"]
            .as_array()
            .expect("operation oneOf")
            .len(),
        2
    );
    assert_exact_required_object(
        &schemas["DeliveryCleanupOperationDto"],
        &[
            "operation_id",
            "cleanup_kind",
            "version",
            "state",
            "expected_disposition_version",
            "expected_merge_operation_id",
            "expected_source_ref",
            "expected_source_oid",
            "target_branch",
            "target_head",
            "failure",
        ],
    );
    assert!(
        schemas["DeliveryCleanupOperationDto"]["properties"]
            .get("kind")
            .is_none(),
        "task delivery embeds the bare cleanup DTO"
    );
    assert_eq!(
        schemas["DeliveryTargetObservationDto"]["oneOf"]
            .as_array()
            .expect("target observation oneOf")
            .len(),
        2
    );
    assert_exact_required_object(
        &schemas["DeliveryAvailableTargetDto"],
        &["available", "branch", "head"],
    );
    assert_exact_required_object(
        &schemas["DeliveryUnavailableTargetDto"],
        &["available", "reason"],
    );
    assert_exact_required_object(
        &schemas["DeliveryTaskDto"],
        &[
            "task_id",
            "eligibility",
            "reasons",
            "evidence",
            "target",
            "source",
            "latest_merge",
            "latest_cleanup",
            "disposition",
            "allowed_actions",
        ],
    );
    assert!(schema_accepts_null(
        &schemas["DeliveryTaskDto"]["properties"]["latest_cleanup"]
    ));
    assert_eq!(
        schemas["DeliveryTaskDto"]["properties"]["latest_cleanup"]["oneOf"][1]["$ref"],
        "#/components/schemas/DeliveryCleanupOperationDto"
    );

    let paths = value["paths"].as_object().expect("router paths");
    for path in [
        "/api/tasks/{task_id}/delivery",
        "/api/delivery-operations/{operation_id}",
        "/api/tasks/{task_id}/merge/preflight",
        "/api/tasks/{task_id}/merge",
        "/api/tasks/{task_id}/cleanup/worktree",
        "/api/tasks/{task_id}/cleanup/branch",
    ] {
        assert!(paths.contains_key(path), "missing delivery path {path}");
    }
    assert_eq!(
        schemas["TaskEventDto"]["oneOf"]
            .as_array()
            .expect("task event oneOf")
            .len(),
        11,
        "delivery polling must not add a task lifecycle/SSE event"
    );
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
        "delivery_readiness",
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
            "reviews",
            "timeline",
            "event_cursor",
        ])
    );
    assert!(schema_accepts_null(&detail["properties"]["plan"]));
    assert!(schema_accepts_null(&detail["properties"]["diff"]));
    assert!(schema_accepts_null(&detail["properties"]["tests"]));
    for array in ["activity", "reviews", "timeline"] {
        assert_eq!(detail["properties"][array]["type"], "array");
        assert!(string_set(&detail["required"]).contains(array));
    }
}

#[test]
fn readiness_plan_and_activity_extensions_are_required_and_legacy_safe() {
    let value = openapi_value();
    let schemas = &value["components"]["schemas"];

    assert_eq!(
        schemas["DeliveryReadinessDto"]["enum"],
        json!(["unreviewed", "review_approved", "review_rejected"])
    );

    let plan = &schemas["PlanSnapshotDto"];
    assert_eq!(
        property_set(plan),
        set(&[
            "format_version",
            "revision",
            "summary",
            "items",
            "initial_required_checks",
        ])
    );
    assert_eq!(string_set(&plan["required"]), property_set(plan));
    assert_eq!(plan["properties"]["format_version"]["minimum"], 0);
    assert_eq!(plan["properties"]["format_version"]["maximum"], 1);
    assert_eq!(
        plan["properties"]["revision"]["maximum"],
        9_007_199_254_740_991_u64
    );
    assert_eq!(plan["properties"]["summary"]["maxLength"], 4096);
    assert_eq!(plan["properties"]["items"]["maxItems"], 32);
    assert_eq!(
        plan["properties"]["initial_required_checks"]["maxItems"],
        16
    );

    let item = &schemas["PlanItemDto"];
    assert_eq!(
        property_set(item),
        set(&[
            "id",
            "title",
            "description",
            "acceptance_criteria",
            "status",
        ])
    );
    assert_eq!(string_set(&item["required"]), property_set(item));
    assert_eq!(item["properties"]["title"]["maxLength"], 256);
    assert_eq!(item["properties"]["description"]["maxLength"], 4096);
    assert_eq!(item["properties"]["acceptance_criteria"]["maxItems"], 8);

    let activity = &schemas["ActivityEntryDto"];
    assert_eq!(
        property_set(activity),
        set(&["id", "level", "actor", "role_run", "message", "created_at"])
    );
    assert_eq!(string_set(&activity["required"]), property_set(activity));
    assert_eq!(
        schemas["ActivityActorDto"]["enum"],
        json!(["system", "planner", "executor", "reviewer"])
    );
    assert!(schema_accepts_null(&activity["properties"]["role_run"]));
    assert_eq!(activity["properties"]["role_run"]["minimum"], 1);
    assert_eq!(
        activity["properties"]["role_run"]["maximum"],
        9_007_199_254_740_991_u64
    );
}

#[test]
fn required_check_schema_is_an_exact_typed_discriminated_union() {
    let value = openapi_value();
    let schemas = &value["components"]["schemas"];
    let required_check = &schemas["RequiredCheckDto"];

    assert_eq!(required_check["discriminator"]["propertyName"], "kind");
    assert_eq!(
        required_check["discriminator"]["mapping"],
        json!({
            "cargo_check": "#/components/schemas/CargoCheckDto",
            "cargo_test": "#/components/schemas/CargoTestDto",
        })
    );
    assert_eq!(
        required_check["oneOf"],
        json!([
            {"$ref": "#/components/schemas/CargoCheckDto"},
            {"$ref": "#/components/schemas/CargoTestDto"},
        ])
    );

    let cargo_check = &schemas["CargoCheckDto"];
    assert_eq!(property_set(cargo_check), set(&["id", "kind", "package"]));
    assert_eq!(
        string_set(&cargo_check["required"]),
        property_set(cargo_check)
    );
    assert!(
        cargo_check["properties"]
            .as_object()
            .unwrap()
            .get("integration_test")
            .is_none()
    );
    assert!(schema_accepts_null(&cargo_check["properties"]["package"]));

    let cargo_test = &schemas["CargoTestDto"];
    assert_eq!(
        property_set(cargo_test),
        set(&["id", "kind", "package", "integration_test"])
    );
    assert_eq!(
        string_set(&cargo_test["required"]),
        property_set(cargo_test)
    );
    for nullable in ["package", "integration_test"] {
        let selector = &cargo_test["properties"][nullable];
        assert!(schema_accepts_null(selector));
        assert_eq!(selector["minLength"], 1);
        assert_eq!(selector["maxLength"], 128);
        assert_eq!(selector["pattern"], "^[A-Za-z0-9_][A-Za-z0-9_-]{0,127}$");
    }
}

#[test]
fn review_check_and_coverage_components_lock_exact_shapes_bounds_and_nullability() {
    let value = openapi_value();
    let schemas = &value["components"]["schemas"];
    let max_safe = json!(9_007_199_254_740_991_u64);

    let digest = &schemas["WorkspaceDigestDto"];
    assert_eq!(
        string_set(&digest["required"]),
        set(&["algorithm", "value"])
    );
    assert_eq!(
        schemas["WorkspaceDigestAlgorithmDto"]["enum"],
        json!(["workspace_fingerprint_v1"])
    );
    assert_eq!(digest["properties"]["value"]["minLength"], 64);
    assert_eq!(digest["properties"]["value"]["maxLength"], 64);
    assert_eq!(digest["properties"]["value"]["pattern"], "^[0-9a-f]{64}$");

    let check = &schemas["CheckEvidenceDto"];
    assert_eq!(
        property_set(check),
        set(&[
            "check_id",
            "actor",
            "role_run",
            "workspace_generation",
            "workspace_digest",
            "status",
            "duration_ms",
            "summary",
            "truncated",
        ])
    );
    assert_eq!(string_set(&check["required"]), property_set(check));
    assert_eq!(
        schemas["CheckActorDto"]["enum"],
        json!(["executor", "reviewer"])
    );
    assert_eq!(
        schemas["CheckEvidenceStatusDto"]["enum"],
        json!(["passed", "failed", "cancelled"])
    );
    assert_eq!(check["properties"]["role_run"]["minimum"], 1);
    assert_eq!(
        check["properties"]["workspace_generation"]["maximum"],
        max_safe
    );
    assert_eq!(check["properties"]["duration_ms"]["maximum"], max_safe);
    assert_eq!(check["properties"]["summary"]["minLength"], 1);
    assert_eq!(check["properties"]["summary"]["maxLength"], 2048);

    let finding = &schemas["ReviewFindingDto"];
    assert_eq!(
        property_set(finding),
        set(&["id", "severity", "message", "path", "line"])
    );
    assert_eq!(string_set(&finding["required"]), property_set(finding));
    assert!(schema_accepts_null(&finding["properties"]["path"]));
    assert!(schema_accepts_null(&finding["properties"]["line"]));
    assert_eq!(finding["properties"]["line"]["minimum"], 1);
    assert_eq!(finding["properties"]["line"]["maximum"], max_safe);
    assert_eq!(finding["properties"]["message"]["maxLength"], 2048);

    let coverage = &schemas["ReviewCoverageDto"];
    assert_eq!(
        property_set(coverage),
        set(&[
            "generation",
            "workspace_digest",
            "manifest_sha256",
            "covered_chunks",
            "total_chunks",
        ])
    );
    assert_eq!(string_set(&coverage["required"]), property_set(coverage));
    assert_eq!(coverage["properties"]["generation"]["maximum"], max_safe);
    assert_eq!(coverage["properties"]["manifest_sha256"]["minLength"], 64);
    assert_eq!(
        coverage["properties"]["manifest_sha256"]["pattern"],
        "^[0-9a-f]{64}$"
    );
    assert_eq!(coverage["properties"]["covered_chunks"]["maxItems"], 8);
    assert_eq!(
        coverage["properties"]["covered_chunks"]["items"]["$ref"],
        "#/components/schemas/ReviewChunkIndexDto"
    );
    assert_eq!(schemas["ReviewChunkIndexDto"]["minimum"], 0);
    assert_eq!(schemas["ReviewChunkIndexDto"]["maximum"], 7);
    assert_eq!(coverage["properties"]["total_chunks"]["minimum"], 0);
    assert_eq!(coverage["properties"]["total_chunks"]["maximum"], 8);

    let review = &schemas["ReviewEvidenceDto"];
    assert_eq!(
        property_set(review),
        set(&[
            "round",
            "decision_source",
            "workspace_generation",
            "workspace_digest",
            "verdict",
            "summary",
            "findings",
            "added_required_checks",
            "required_checks",
            "check_evidence",
            "coverage",
            "created_at",
        ])
    );
    assert_eq!(string_set(&review["required"]), property_set(review));
    assert_eq!(review["properties"]["round"]["minimum"], 1);
    assert_eq!(review["properties"]["round"]["maximum"], 3);
    assert_eq!(
        review["properties"]["workspace_generation"]["maximum"],
        max_safe
    );
    assert_eq!(review["properties"]["summary"]["minLength"], 1);
    assert_eq!(review["properties"]["summary"]["maxLength"], 4096);
    assert_eq!(review["properties"]["findings"]["maxItems"], 32);
    assert_eq!(
        review["properties"]["added_required_checks"]["maxItems"],
        16
    );
    assert_eq!(review["properties"]["required_checks"]["minItems"], 1);
    assert_eq!(review["properties"]["required_checks"]["maxItems"], 16);
    assert_eq!(review["properties"]["check_evidence"]["maxItems"], 16);
    assert!(schema_accepts_null(&review["properties"]["coverage"]));
    assert_eq!(
        schemas["ReviewDecisionSourceDto"]["enum"],
        json!(["reviewer", "system"])
    );
    assert_eq!(
        schemas["ReviewVerdictDto"]["enum"],
        json!(["approved", "changes_requested"])
    );
    assert_eq!(
        schemas["FindingSeverityDto"]["enum"],
        json!(["blocking", "advisory"])
    );
}

#[test]
fn review_updated_payload_is_typed_and_task_event_kind_is_exactly_eleven() {
    let value = openapi_value();
    let schemas = &value["components"]["schemas"];
    let payload = &schemas["ReviewUpdatedPayloadDto"];

    assert_eq!(property_set(payload), set(&["review"]));
    assert_eq!(string_set(&payload["required"]), set(&["review"]));
    assert_eq!(
        payload["properties"]["review"]["$ref"],
        "#/components/schemas/ReviewEvidenceDto"
    );
    assert_eq!(
        schemas["ReviewUpdatedEventDto"]["properties"]["payload"]["$ref"],
        "#/components/schemas/ReviewUpdatedPayloadDto"
    );
    assert_eq!(
        string_set(&schemas["TaskEventKindDto"]["enum"]),
        set(&[
            "task.queued",
            "task.started",
            "plan.updated",
            "activity.appended",
            "diff.updated",
            "test.updated",
            "review.updated",
            "task.completed",
            "task.failed",
            "task.cancelled",
            "task.interrupted",
        ])
    );
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
fn bootstrap_requires_scheduler_and_bounded_alias_watermarks() {
    let value = openapi_value();
    let schemas = &value["components"]["schemas"];
    let bootstrap = &schemas["BootstrapResponse"];

    assert_eq!(
        property_set(bootstrap),
        set(&[
            "csrf_token",
            "repositories",
            "tasks",
            "latest_event_id",
            "server_started_at",
            "service_state",
            "service_state_generation",
            "max_concurrent_tasks",
            "scheduler",
        ])
    );
    assert_eq!(string_set(&bootstrap["required"]), property_set(bootstrap));
    assert_eq!(
        bootstrap["properties"]["scheduler"]["$ref"],
        "#/components/schemas/SchedulerStateDto"
    );
    assert_eq!(bootstrap["properties"]["latest_event_id"]["minimum"], 0);
    assert_eq!(
        bootstrap["properties"]["latest_event_id"]["maximum"],
        9_007_199_254_740_991_u64
    );
    assert_eq!(
        bootstrap["properties"]["service_state_generation"]["minimum"],
        0
    );
    assert_eq!(
        bootstrap["properties"]["service_state_generation"]["maximum"],
        9_007_199_254_740_991_u64
    );
    assert_eq!(
        bootstrap["properties"]["max_concurrent_tasks"]["minimum"],
        1
    );
    assert_eq!(
        bootstrap["properties"]["max_concurrent_tasks"]["maximum"],
        4
    );
}

#[test]
fn scheduler_state_schema_is_exact_and_bounded() {
    let value = openapi_value();
    let schemas = &value["components"]["schemas"];
    let scheduler = &schemas["SchedulerStateDto"];
    assert_exact_required_object(
        scheduler,
        &[
            "schema_version",
            "server_instance_id",
            "server_started_at",
            "generation",
            "as_of_event_id",
            "service_state_generation",
            "admission_state",
            "limits",
            "active_task_count",
            "queued_task_count",
            "queued_tasks",
            "stopping_tasks",
            "storage",
        ],
    );
    assert_eq!(scheduler["properties"]["schema_version"]["minimum"], 1);
    assert_eq!(scheduler["properties"]["schema_version"]["maximum"], 1);
    assert_eq!(
        scheduler["properties"]["server_instance_id"]["format"],
        "uuid"
    );
    assert_eq!(
        scheduler["properties"]["server_instance_id"]["pattern"],
        "^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    );
    for field in ["generation", "as_of_event_id", "service_state_generation"] {
        assert_eq!(scheduler["properties"][field]["minimum"], 0);
        assert_eq!(
            scheduler["properties"][field]["maximum"],
            9_007_199_254_740_991_u64
        );
    }
    assert_eq!(scheduler["properties"]["active_task_count"]["minimum"], 0);
    assert_eq!(scheduler["properties"]["active_task_count"]["maximum"], 4);
    assert_eq!(scheduler["properties"]["queued_task_count"]["minimum"], 0);
    assert_eq!(
        scheduler["properties"]["queued_task_count"]["maximum"],
        u32::MAX
    );
    assert_eq!(scheduler["properties"]["stopping_tasks"]["maxItems"], 4);
}

#[test]
fn scheduler_nested_schemas_are_exact_and_bounded() {
    let value = openapi_value();
    let schemas = &value["components"]["schemas"];
    let limits = &schemas["SchedulerLimitsDto"];
    assert_exact_required_object(
        limits,
        &["global", "per_repository", "queued", "cargo_jobs_per_task"],
    );
    for (field, maximum) in [
        ("global", 4),
        ("per_repository", 4),
        ("queued", 256),
        ("cargo_jobs_per_task", 8),
    ] {
        assert_eq!(limits["properties"][field]["minimum"], 1);
        assert_eq!(limits["properties"][field]["maximum"], maximum);
    }

    let queued = &schemas["SchedulerQueuedTaskDto"];
    assert_exact_required_object(queued, &["task_id", "reason"]);
    assert_eq!(
        queued["properties"]["task_id"]["pattern"],
        "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    );
    assert_eq!(
        schemas["SchedulerQueueReasonDto"]["enum"],
        json!([
            "service_paused",
            "storage_pressure",
            "global_capacity",
            "repository_capacity",
            "repository_control_busy",
        ])
    );

    let stopping = &schemas["SchedulerStoppingTaskDto"];
    assert_exact_required_object(stopping, &["task_id", "intent"]);
    assert_eq!(
        schemas["SchedulerStopIntentDto"]["enum"],
        json!(["user_cancelled", "disk_pressure_critical"])
    );

    assert_exact_required_object(
        &schemas["SchedulerStorageDto"],
        &["state", "data", "runtime", "repositories"],
    );
    assert_exact_required_object(&schemas["SchedulerStorageScopeDto"], &["state"]);
    assert_exact_required_object(
        &schemas["SchedulerRepositoryStorageDto"],
        &["repository_id", "state"],
    );
    assert_eq!(
        schemas["SchedulerStorageStateDto"]["enum"],
        json!(["normal", "pressure", "critical", "unavailable"])
    );
    assert_eq!(
        schemas["SchedulerAdmissionStateDto"]["enum"],
        json!(["running", "paused"])
    );
}

#[test]
fn scheduler_state_serializes_the_exact_path_private_wire_shape() {
    let queued_task_id = uuid::Uuid::parse_str("123e4567-e89b-42d3-a456-426614174001").unwrap();
    let stopping_task_id = uuid::Uuid::parse_str("123e4567-e89b-42d3-a456-426614174002").unwrap();
    let repository_id = uuid::Uuid::parse_str("123e4567-e89b-42d3-a456-426614174003").unwrap();
    let state = SchedulerStateDto {
        schema_version: 1,
        server_instance_id: uuid::Uuid::parse_str("123e4567-e89b-42d3-a456-426614174000").unwrap(),
        server_started_at: UtcTimestamp::parse_rfc3339("2026-07-15T00:00:00Z")
            .unwrap()
            .into(),
        generation: 7,
        as_of_event_id: 41,
        service_state_generation: 3,
        admission_state: SchedulerAdmissionStateDto::Running,
        limits: SchedulerLimitsDto {
            global: 2,
            per_repository: 2,
            queued: 32,
            cargo_jobs_per_task: 4,
        },
        active_task_count: 1,
        queued_task_count: 1,
        queued_tasks: vec![SchedulerQueuedTaskDto {
            task_id: queued_task_id,
            reason: SchedulerQueueReasonDto::StoragePressure,
        }],
        stopping_tasks: vec![SchedulerStoppingTaskDto {
            task_id: stopping_task_id,
            intent: SchedulerStopIntentDto::DiskPressureCritical,
        }],
        storage: SchedulerStorageDto {
            state: SchedulerStorageStateDto::Pressure,
            data: SchedulerStorageScopeDto {
                state: SchedulerStorageStateDto::Pressure,
            },
            runtime: SchedulerStorageScopeDto {
                state: SchedulerStorageStateDto::Normal,
            },
            repositories: vec![SchedulerRepositoryStorageDto {
                repository_id,
                state: SchedulerStorageStateDto::Normal,
            }],
        },
    };

    assert_eq!(
        serde_json::to_value(state).unwrap(),
        json!({
            "schema_version": 1,
            "server_instance_id": "123e4567-e89b-42d3-a456-426614174000",
            "server_started_at": "2026-07-15T00:00:00.000000000Z",
            "generation": 7,
            "as_of_event_id": 41,
            "service_state_generation": 3,
            "admission_state": "running",
            "limits": {
                "global": 2,
                "per_repository": 2,
                "queued": 32,
                "cargo_jobs_per_task": 4,
            },
            "active_task_count": 1,
            "queued_task_count": 1,
            "queued_tasks": [{
                "task_id": queued_task_id,
                "reason": "storage_pressure",
            }],
            "stopping_tasks": [{
                "task_id": stopping_task_id,
                "intent": "disk_pressure_critical",
            }],
            "storage": {
                "state": "pressure",
                "data": {"state": "pressure"},
                "runtime": {"state": "normal"},
                "repositories": [{
                    "repository_id": repository_id,
                    "state": "normal",
                }],
            },
        })
    );
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
fn scheduler_control_components_are_exact_idless_shapes_and_sse_union_members() {
    let value = openapi_value();
    let schemas = &value["components"]["schemas"];

    assert_exact_required_object(
        &schemas["SchedulerStateControl"],
        &[
            "schema_version",
            "kind",
            "server_instance_id",
            "server_started_at",
            "generation",
            "as_of_event_id",
            "service_state_generation",
            "admission_state",
            "limits",
            "active_task_count",
            "queued_task_count",
            "stopping_task_count",
            "repository_storage_count",
            "storage",
            "item_count",
            "chunk_count",
            "snapshot_digest",
        ],
    );
    assert_exact_required_object(
        &schemas["SchedulerStateChunkControl"],
        &[
            "schema_version",
            "kind",
            "server_instance_id",
            "generation",
            "snapshot_digest",
            "chunk_index",
            "chunk_count",
            "items",
        ],
    );
    assert_exact_required_object(
        &schemas["SchedulerControlStorageDto"],
        &["state", "data", "runtime"],
    );
    for schema in ["SchedulerStateControl", "SchedulerStateChunkControl"] {
        assert!(schemas[schema]["properties"].get("id").is_none());
    }
    assert_eq!(
        schemas["SchedulerStateControl"]["properties"]["snapshot_digest"]["pattern"],
        "^[0-9a-f]{64}$"
    );
    assert_eq!(
        schemas["SchedulerStateChunkControl"]["properties"]["items"]["maxItems"],
        128
    );

    let members = schemas["SseMessage"]["oneOf"]
        .as_array()
        .expect("SseMessage oneOf")
        .iter()
        .map(|member| {
            member["$ref"]
                .as_str()
                .expect("component ref")
                .rsplit('/')
                .next()
                .unwrap()
                .to_owned()
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        members,
        set(&[
            "TaskEventDto",
            "StreamResetControl",
            "ServiceStateControl",
            "SchedulerStateControl",
            "SchedulerStateChunkControl",
        ])
    );
}

#[test]
fn scheduler_item_schema_is_an_exact_discriminated_union() {
    let value = openapi_value();
    let schemas = &value["components"]["schemas"];
    let item = &schemas["SchedulerStateItemDto"];

    assert_eq!(item["discriminator"]["propertyName"], "kind");
    assert_eq!(
        item["discriminator"]["mapping"],
        json!({
            "queued_task": "#/components/schemas/SchedulerQueuedTaskItemDto",
            "stopping_task": "#/components/schemas/SchedulerStoppingTaskItemDto",
            "repository_storage": "#/components/schemas/SchedulerRepositoryStorageItemDto",
        })
    );
    assert_exact_required_object(
        &schemas["SchedulerQueuedTaskItemDto"],
        &["kind", "task_id", "reason"],
    );
    assert_exact_required_object(
        &schemas["SchedulerStoppingTaskItemDto"],
        &["kind", "task_id", "intent"],
    );
    assert_exact_required_object(
        &schemas["SchedulerRepositoryStorageItemDto"],
        &["kind", "repository_id", "state"],
    );
}

#[test]
fn event_json_is_a_flat_typed_wire_frame() {
    let timestamp = UtcTimestamp::parse_rfc3339("2026-07-15T01:02:03Z").unwrap();
    let event = TaskEvent::new(
        EventId::new(17).unwrap(),
        TaskId::new(),
        TaskEventPayload::PlanUpdated {
            plan: PlanSnapshot::legacy(
                3,
                vec![PlanItem::legacy(
                    "compile",
                    "Compile the API",
                    PlanItemStatus::Running,
                )],
            ),
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
    assert_eq!(value["payload"]["plan"]["format_version"], 0);
    assert_eq!(value["payload"]["plan"]["revision"], 3);
    assert_eq!(value["payload"]["plan"]["summary"], "");
    assert_eq!(
        value["payload"]["plan"]["initial_required_checks"],
        json!([])
    );
    assert_eq!(value["payload"]["plan"]["items"][0]["status"], "running");
    assert_eq!(value["payload"]["plan"]["items"][0]["description"], "");
    assert_eq!(
        value["payload"]["plan"]["items"][0]["acceptance_criteria"],
        json!([])
    );
    assert!(value["payload"].get("id").is_none());
    assert!(value["payload"].get("kind").is_none());
    assert_eq!(value["created_at"], "2026-07-15T01:02:03.000000000Z");
}

#[test]
fn legacy_activity_is_projected_with_required_system_actor_and_null_role_run() {
    let timestamp = UtcTimestamp::parse_rfc3339("2026-07-15T01:02:03Z").unwrap();
    let event = TaskEvent::new(
        EventId::new(18).unwrap(),
        TaskId::new(),
        TaskEventPayload::ActivityAppended {
            entry: ActivityEntry::legacy("legacy-activity", ActivityLevel::Info, "safe", timestamp),
        },
        timestamp,
    );

    let value = serde_json::to_value(TaskEventDto::from(event)).unwrap();
    assert_eq!(value["payload"]["entry"]["actor"], "system");
    assert_eq!(value["payload"]["entry"]["role_run"], Value::Null);
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
