use coding_agent_domain::{
    ActivityActor, ActivityEntry, ActivityLevel, DomainError, MAX_WORKSPACE_GENERATION, PlanItem,
    PlanItemStatus, PlanSnapshot, RequiredCheck, TaskEvent, TaskEventPayload, UtcTimestamp,
};
use serde_json::{Value, json};

const PLAN_LIMIT: usize = 64 * 1024;

#[test]
fn legacy_plan_and_activity_events_decode_to_safe_v0_defaults() {
    let plan_event: TaskEvent = serde_json::from_value(event_json(
        "plan.updated",
        json!({
            "plan": {
                "revision": 7,
                "items": [{"id": "legacy-step", "title": "legacy", "status": "running"}]
            }
        }),
    ))
    .unwrap();
    let TaskEventPayload::PlanUpdated { plan } = plan_event.payload else {
        panic!("expected plan event");
    };
    assert_eq!(plan.format_version(), 0);
    assert_eq!(plan.summary(), "");
    assert!(plan.initial_required_checks().is_empty());
    assert_eq!(plan.items()[0].description(), "");
    assert!(plan.items()[0].acceptance_criteria().is_empty());
    assert_eq!(
        serde_json::to_value(plan).unwrap(),
        json!({
            "format_version": 0,
            "revision": 7,
            "summary": "",
            "items": [{
                "id": "legacy-step",
                "title": "legacy",
                "description": "",
                "acceptance_criteria": [],
                "status": "running"
            }],
            "initial_required_checks": []
        })
    );

    let activity_event: TaskEvent = serde_json::from_value(event_json(
        "activity.appended",
        json!({
            "entry": {
                "id": "legacy-activity",
                "level": "info",
                "message": "working",
                "created_at": "2026-07-23T00:00:00Z"
            }
        }),
    ))
    .unwrap();
    let TaskEventPayload::ActivityAppended { entry } = activity_event.payload else {
        panic!("expected activity event");
    };
    assert_eq!(entry.actor(), ActivityActor::System);
    assert_eq!(entry.role_run(), None);
    let encoded = serde_json::to_value(entry).unwrap();
    assert_eq!(encoded["actor"], "system");
    assert_eq!(encoded["role_run"], Value::Null);

    let structured_item = PlanItem::try_structured(
        "step-1",
        "title",
        "description",
        vec!["criterion".into()],
        PlanItemStatus::Pending,
    )
    .unwrap();
    let normalized_legacy = PlanSnapshot::legacy(8, vec![structured_item]);
    assert_eq!(normalized_legacy.items()[0].description(), "");
    assert!(
        normalized_legacy.items()[0]
            .acceptance_criteria()
            .is_empty()
    );
}

#[test]
fn structured_plan_has_exact_shape_and_closed_v0_v1_semantics() {
    let plan = PlanSnapshot::try_structured(
        1,
        "Implement the quality loop",
        vec![
            PlanItem::try_structured(
                "step-1",
                "Implement",
                "Build the typed path",
                vec!["Domain tests pass".into()],
                PlanItemStatus::Running,
            )
            .unwrap(),
        ],
        vec![
            RequiredCheck::try_cargo_test(
                "check-1",
                Some("coding-agent-domain".into()),
                Some("quality_loop".into()),
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let encoded = serde_json::to_value(&plan).unwrap();
    assert_eq!(encoded["format_version"], 1);
    assert_eq!(encoded["items"][0]["description"], "Build the typed path");
    assert_eq!(
        encoded["items"][0]["acceptance_criteria"][0],
        "Domain tests pass"
    );
    assert_eq!(encoded["initial_required_checks"][0]["kind"], "cargo_test");
    assert_eq!(
        serde_json::from_value::<PlanSnapshot>(encoded.clone()).unwrap(),
        plan
    );

    for mutate in [
        |value: &mut Value| value["format_version"] = json!(2),
        |value: &mut Value| {
            value.as_object_mut().unwrap().remove("summary");
        },
        |value: &mut Value| {
            value["items"][0]
                .as_object_mut()
                .unwrap()
                .remove("description");
        },
        |value: &mut Value| value["items"][0]["acceptance_criteria"] = json!([]),
        |value: &mut Value| value["initial_required_checks"] = json!([]),
        |value: &mut Value| value["unexpected"] = json!(true),
        |value: &mut Value| value["items"][0]["unexpected"] = json!(true),
    ] {
        let mut invalid = encoded.clone();
        mutate(&mut invalid);
        assert!(serde_json::from_value::<PlanSnapshot>(invalid).is_err());
    }

    let explicit_v0 = json!({
        "format_version": 0,
        "revision": 99,
        "summary": "",
        "items": [{
            "id": "legacy-step",
            "title": "legacy",
            "description": "",
            "acceptance_criteria": [],
            "status": "completed"
        }],
        "initial_required_checks": []
    });
    assert!(serde_json::from_value::<PlanSnapshot>(explicit_v0.clone()).is_ok());
    let mut invalid_v0 = explicit_v0;
    invalid_v0["summary"] = json!("not legacy-safe");
    assert!(serde_json::from_value::<PlanSnapshot>(invalid_v0).is_err());
}

#[test]
fn structured_plan_enforces_nested_counts_bounds_and_canonical_checks() {
    let test = RequiredCheck::try_cargo_test("check-1", Some("workspace".into()), None).unwrap();
    let item = || {
        PlanItem::try_structured(
            "step-1",
            "title",
            "description",
            vec!["criterion".into()],
            PlanItemStatus::Pending,
        )
        .unwrap()
    };

    assert_eq!(
        PlanSnapshot::try_structured(
            MAX_WORKSPACE_GENERATION + 1,
            "summary",
            vec![item()],
            vec![test.clone()],
        ),
        Err(DomainError::InvalidPlan)
    );
    assert!(PlanSnapshot::try_structured(1, "summary", vec![], vec![test.clone()]).is_err());
    assert!(
        PlanSnapshot::try_structured(
            1,
            "summary",
            (0..33).map(|_| item()).collect(),
            vec![test.clone()],
        )
        .is_err()
    );
    assert!(
        PlanItem::try_structured(
            "step-1",
            "",
            "description",
            vec!["criterion".into()],
            PlanItemStatus::Pending,
        )
        .is_err()
    );
    assert!(
        PlanItem::try_structured(
            "step-1",
            "title",
            "description",
            (0..9).map(|index| format!("criterion-{index}")).collect(),
            PlanItemStatus::Pending,
        )
        .is_err()
    );
    assert!(
        PlanItem::try_structured(
            "step-1",
            "title",
            "description",
            vec!["c".repeat(1_025)],
            PlanItemStatus::Pending,
        )
        .is_err()
    );
    assert!(
        PlanSnapshot::try_structured(1, "s".repeat(4_097), vec![item()], vec![test.clone()],)
            .is_err()
    );
    assert!(
        PlanItem::try_structured(
            "step-1",
            "t".repeat(257),
            "description",
            vec!["criterion".into()],
            PlanItemStatus::Pending,
        )
        .is_err()
    );
    assert!(
        PlanItem::try_structured(
            "step-1",
            "title",
            "d".repeat(4_097),
            vec!["criterion".into()],
            PlanItemStatus::Pending,
        )
        .is_err()
    );
    assert!(
        PlanItem::try_structured(
            "step-1",
            "title",
            "description",
            vec!["".into()],
            PlanItemStatus::Pending,
        )
        .is_err()
    );
    assert!(
        PlanSnapshot::try_structured(
            1,
            "summary",
            vec![item()],
            vec![RequiredCheck::try_cargo_check("check-1", None).unwrap()],
        )
        .is_err()
    );
    assert!(
        PlanSnapshot::try_structured(1, "summary", vec![item()], vec![test.clone(), test.clone()],)
            .is_err()
    );
    let same_selector =
        RequiredCheck::try_cargo_test("check-2", Some("workspace".into()), None).unwrap();
    assert!(
        PlanSnapshot::try_structured(
            1,
            "summary",
            vec![item()],
            vec![test.clone(), same_selector],
        )
        .is_err()
    );
    assert!(
        PlanSnapshot::try_structured(
            1,
            "summary",
            vec![item()],
            (1..=17)
                .map(|ordinal| {
                    RequiredCheck::try_cargo_test(
                        format!("check-{ordinal}"),
                        Some(format!("package-{ordinal}")),
                        None,
                    )
                    .unwrap()
                })
                .collect(),
        )
        .is_err()
    );
    assert!(
        PlanSnapshot::try_structured(1, "summary", vec![item(), item()], vec![test.clone()],)
            .is_err()
    );
    let running = |id| {
        PlanItem::try_structured(
            id,
            "title",
            "description",
            vec!["criterion".into()],
            PlanItemStatus::Running,
        )
        .unwrap()
    };
    assert!(
        PlanSnapshot::try_structured(
            1,
            "summary",
            vec![running("step-1"), running("step-2")],
            vec![test],
        )
        .is_err()
    );
}

#[test]
fn structured_plan_canonical_json_limit_is_exact_after_unicode_escaping() {
    let exact = try_plan_with_encoded_size(PLAN_LIMIT).unwrap();
    assert_eq!(serde_json::to_vec(&exact).unwrap().len(), PLAN_LIMIT);
    assert!(try_plan_with_encoded_size(PLAN_LIMIT + 1).is_err());
}

#[test]
fn activity_actor_and_required_nullable_role_run_form_a_closed_matrix() {
    let planner = ActivityEntry::try_new(
        "activity-1",
        ActivityLevel::Info,
        ActivityActor::Planner,
        Some(1),
        "planning",
        timestamp(),
    )
    .unwrap();
    let encoded = serde_json::to_value(&planner).unwrap();
    assert_eq!(encoded["actor"], "planner");
    assert_eq!(encoded["role_run"], 1);
    assert_eq!(
        serde_json::from_value::<ActivityEntry>(encoded.clone()).unwrap(),
        planner
    );

    assert!(
        ActivityEntry::try_new(
            "activity-2",
            ActivityLevel::Info,
            ActivityActor::System,
            Some(1),
            "invalid",
            timestamp(),
        )
        .is_err()
    );
    assert!(
        ActivityEntry::try_new(
            "activity-3",
            ActivityLevel::Info,
            ActivityActor::Reviewer,
            None,
            "invalid",
            timestamp(),
        )
        .is_err()
    );
    assert!(
        ActivityEntry::try_new(
            "activity-4",
            ActivityLevel::Info,
            ActivityActor::Executor,
            Some(0),
            "invalid",
            timestamp(),
        )
        .is_err()
    );

    for field in ["actor", "role_run"] {
        let mut missing = encoded.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<ActivityEntry>(missing).is_err());
    }
    let mut unknown = encoded;
    unknown["unexpected"] = json!(true);
    assert!(serde_json::from_value::<ActivityEntry>(unknown).is_err());
}

fn event_json(kind: &str, payload: Value) -> Value {
    json!({
        "id": 1,
        "schema_version": 1,
        "task_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "kind": kind,
        "payload": payload,
        "created_at": "2026-07-23T00:00:00Z"
    })
}

fn timestamp() -> UtcTimestamp {
    UtcTimestamp::parse_rfc3339("2026-07-23T00:00:00Z").unwrap()
}

fn try_plan_with_encoded_size(target: usize) -> Result<PlanSnapshot, DomainError> {
    let check =
        || RequiredCheck::try_cargo_test("check-1", Some("workspace".into()), None).unwrap();
    let make_items = |descriptions: Vec<String>| {
        descriptions
            .into_iter()
            .enumerate()
            .map(|(index, description)| {
                PlanItem::try_structured(
                    format!("step-{}", index + 1),
                    "title",
                    description,
                    vec!["criterion".into()],
                    PlanItemStatus::Pending,
                )
                .unwrap()
            })
            .collect::<Vec<_>>()
    };
    let base =
        PlanSnapshot::try_structured(1, "", make_items(vec![String::new(); 32]), vec![check()])?;
    let base_size = serde_json::to_vec(&base).unwrap().len();
    let encoded_filler = target
        .checked_sub(base_size)
        .ok_or(DomainError::InvalidPlan)?;
    let descriptions = escaped_plan_fillers(encoded_filler, 32)?;
    PlanSnapshot::try_structured(1, "", make_items(descriptions), vec![check()])
}

fn escaped_plan_fillers(encoded_bytes: usize, slots: usize) -> Result<Vec<String>, DomainError> {
    let controls = encoded_bytes / 6;
    let ascii = encoded_bytes % 6;
    if controls + ascii > slots * 4_096 {
        return Err(DomainError::InvalidPlan);
    }

    let mut controls_left = controls;
    let mut ascii_left = ascii;
    let mut result = Vec::with_capacity(slots);
    for _ in 0..slots {
        let control_count = controls_left.min(4_096);
        controls_left -= control_count;
        let remaining_capacity = 4_096 - control_count;
        let ascii_count = ascii_left.min(remaining_capacity);
        ascii_left -= ascii_count;
        result.push(format!(
            "{}{}",
            "\u{0001}".repeat(control_count),
            "a".repeat(ascii_count)
        ));
    }
    if controls_left == 0 && ascii_left == 0 {
        Ok(result)
    } else {
        Err(DomainError::InvalidPlan)
    }
}
