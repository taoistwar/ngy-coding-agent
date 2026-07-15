use std::path::PathBuf;

use coding_agent_domain::{
    ActivityEntry, ActivityLevel, CanonicalPath, ClientRequestId, DiffSnapshot, DomainError,
    EventCursor, EventId, NewTask, PlanSnapshot, RepositoryId, Task, TaskEvent, TaskEventKind,
    TaskEventPayload, TaskFailure, TaskId, TaskStatus, TestSnapshot, TestStatus, UtcTimestamp,
};
use time::format_description::well_known::Rfc3339;
use time::{Date, Month, OffsetDateTime, Time, UtcOffset};

#[test]
fn task_status_transition_matrix_is_closed() {
    use TaskStatus::*;
    let legal = [
        (Queued, Running),
        (Queued, Cancelled),
        (Queued, Interrupted),
        (Running, Completed),
        (Running, Failed),
        (Running, Cancelled),
        (Running, Interrupted),
    ];
    for from in [Queued, Running, Completed, Failed, Cancelled, Interrupted] {
        for to in [Queued, Running, Completed, Failed, Cancelled, Interrupted] {
            assert_eq!(from.can_transition_to(to), legal.contains(&(from, to)));
        }
    }
}

#[test]
fn only_terminal_tasks_are_retryable() {
    use TaskStatus::*;
    assert!(!Queued.is_retryable());
    assert!(!Running.is_retryable());
    for status in [Completed, Failed, Cancelled, Interrupted] {
        assert!(status.is_retryable());
    }
}

#[test]
fn empty_prompt_is_rejected_after_trimming() {
    let error = NewTask::try_new(ClientRequestId::new(), RepositoryId::new(), " \t\r\n ")
        .expect_err("whitespace-only prompts must be invalid");

    assert_eq!(error, DomainError::InvalidPrompt);
}

#[test]
fn prompt_at_unicode_scalar_limit_is_trimmed_and_accepted() {
    let prompt = format!("  {}  ", "界".repeat(50_000));

    let task = NewTask::try_new(ClientRequestId::new(), RepositoryId::new(), prompt)
        .expect("50,000 Unicode scalar values must be accepted");

    assert_eq!(task.prompt.chars().count(), 50_000);
    assert!(!task.prompt.starts_with(char::is_whitespace));
    assert!(!task.prompt.ends_with(char::is_whitespace));
}

#[test]
fn prompt_over_unicode_scalar_limit_is_rejected() {
    let error = NewTask::try_new(
        ClientRequestId::new(),
        RepositoryId::new(),
        "界".repeat(50_001),
    )
    .expect_err("50,001 Unicode scalar values must be invalid");

    assert_eq!(error, DomainError::InvalidPrompt);
}

#[test]
fn uuid_id_newtypes_round_trip_through_text_and_serde() {
    let repository_id = RepositoryId::new();
    let task_id = TaskId::new();
    let client_request_id = ClientRequestId::new();

    assert_eq!(
        repository_id.to_string().parse::<RepositoryId>().unwrap(),
        repository_id
    );
    assert_eq!(task_id.to_string().parse::<TaskId>().unwrap(), task_id);
    assert_eq!(
        client_request_id
            .to_string()
            .parse::<ClientRequestId>()
            .unwrap(),
        client_request_id
    );
    assert_eq!(
        serde_json::from_str::<RepositoryId>(&serde_json::to_string(&repository_id).unwrap())
            .unwrap(),
        repository_id
    );
}

#[test]
fn event_id_and_cursor_enforce_ranges_and_round_trip() {
    assert_eq!(EventId::new(0), Err(DomainError::InvalidEventId));
    assert_eq!(EventId::new(-1), Err(DomainError::InvalidEventId));
    assert_eq!(EventCursor::new(-1), Err(DomainError::InvalidEventCursor));
    assert_eq!(EventCursor::ZERO.get(), 0);

    let event_id = EventId::new(42).unwrap();
    let cursor = EventCursor::new(42).unwrap();
    assert_eq!(event_id.get(), 42);
    assert_eq!(cursor.get(), 42);
    assert_eq!(
        serde_json::from_str::<EventId>(&serde_json::to_string(&event_id).unwrap()).unwrap(),
        event_id
    );
    assert_eq!(
        serde_json::from_str::<EventCursor>(&serde_json::to_string(&cursor).unwrap()).unwrap(),
        cursor
    );
}

#[test]
fn checked_value_newtypes_reject_invalid_json() {
    assert!(serde_json::from_str::<EventId>("0").is_err());
    assert!(serde_json::from_str::<EventCursor>("-1").is_err());
    assert!(serde_json::from_str::<CanonicalPath>(r#""relative/path""#).is_err());
    assert!(serde_json::from_str::<UtcTimestamp>(r#""not-a-timestamp""#).is_err());
}

#[test]
fn canonical_path_accepts_only_absolute_normalized_paths() {
    let normalized = absolute_path();
    let canonical = CanonicalPath::try_from_canonical(normalized.clone()).unwrap();
    assert_eq!(canonical.as_path(), normalized.as_path());

    assert_eq!(
        CanonicalPath::try_from_canonical(PathBuf::from("relative/path")),
        Err(DomainError::InvalidCanonicalPath)
    );
    assert_eq!(
        CanonicalPath::try_from_canonical(path_with_current_component()),
        Err(DomainError::InvalidCanonicalPath)
    );
    assert_eq!(
        CanonicalPath::try_from_canonical(path_with_parent_component()),
        Err(DomainError::InvalidCanonicalPath)
    );
}

#[test]
fn timestamp_normalizes_to_utc_and_serializes_fixed_width_rfc3339() {
    let source = OffsetDateTime::parse("2026-07-14T08:09:10.123456789+08:00", &Rfc3339).unwrap();

    let timestamp = UtcTimestamp::new(source).unwrap();

    assert_eq!(timestamp.as_offset_date_time().offset(), UtcOffset::UTC);
    assert_eq!(timestamp.to_string(), "2026-07-14T00:09:10.123456789Z");
    assert_eq!(
        serde_json::to_string(&timestamp).unwrap(),
        r#""2026-07-14T00:09:10.123456789Z""#
    );
    assert_eq!(
        serde_json::from_str::<UtcTimestamp>(r#""2026-07-14T08:09:10.123456789+08:00""#).unwrap(),
        timestamp
    );
}

#[test]
fn timestamp_without_fraction_still_serializes_nine_fractional_digits() {
    let timestamp = UtcTimestamp::parse_rfc3339("2026-07-14T00:09:10Z").unwrap();

    assert_eq!(timestamp.to_string(), "2026-07-14T00:09:10.000000000Z");
}

#[test]
fn timestamp_constructor_rejects_years_outside_fixed_width_rfc3339_range() {
    let negative_year = Date::from_calendar_date(-1, Month::January, 1)
        .unwrap()
        .with_time(Time::MIDNIGHT)
        .assume_utc();

    assert_eq!(
        UtcTimestamp::new(negative_year),
        Err(DomainError::InvalidTimestamp)
    );
}

#[test]
fn timestamp_supported_year_boundaries_preserve_lexical_order() {
    let earliest = UtcTimestamp::parse_rfc3339("0000-01-01T00:00:00Z").unwrap();
    let latest = UtcTimestamp::parse_rfc3339("9999-12-31T23:59:59.999999999Z").unwrap();

    assert_eq!(earliest.to_string(), "0000-01-01T00:00:00.000000000Z");
    assert_eq!(latest.to_string(), "9999-12-31T23:59:59.999999999Z");
    assert!(earliest.to_string() < latest.to_string());
}

#[test]
fn stored_task_rejects_zero_attempt() {
    let mut task = task_with_status(TaskStatus::Queued);
    task.attempt = 0;

    assert_eq!(
        Task::try_from_stored(task),
        Err(DomainError::InvalidTaskAttempt)
    );
}

#[test]
fn stored_task_status_invariants_are_exhaustive() {
    use TaskStatus::*;

    for status in [Queued, Running, Completed, Failed, Cancelled, Interrupted] {
        for has_started_at in [false, true] {
            for has_finished_at in [false, true] {
                for has_failure in [false, true] {
                    let mut task = task_with_status(status);
                    task.started_at = has_started_at.then(timestamp);
                    task.finished_at = has_finished_at.then(timestamp);
                    task.failure = has_failure.then(task_failure);

                    let valid = matches!(
                        (status, has_started_at, has_finished_at, has_failure),
                        (Queued, false, false, false)
                            | (Running, true, false, false)
                            | (Completed, true, true, false)
                            | (Failed, true, true, true)
                            | (Cancelled, false | true, true, false)
                            | (Interrupted, false | true, true, true)
                    );

                    assert_eq!(
                        Task::try_from_stored(task).is_ok(),
                        valid,
                        "unexpected validity for {status:?}, started={has_started_at}, finished={has_finished_at}, failure={has_failure}"
                    );
                }
            }
        }
    }
}

#[test]
fn task_event_payload_kind_mapping_is_exhaustive() {
    let payloads = [
        TaskEventPayload::TaskQueued {
            task: task_with_status(TaskStatus::Queued),
        },
        TaskEventPayload::TaskStarted {
            task: task_with_status(TaskStatus::Running),
        },
        TaskEventPayload::PlanUpdated {
            plan: PlanSnapshot {
                revision: 1,
                items: vec![],
            },
        },
        TaskEventPayload::ActivityAppended {
            entry: ActivityEntry {
                id: "activity-1".into(),
                level: ActivityLevel::Info,
                message: "working".into(),
                created_at: timestamp(),
            },
        },
        TaskEventPayload::DiffUpdated {
            diff: DiffSnapshot {
                revision: 1,
                files: vec![],
            },
        },
        TaskEventPayload::TestUpdated {
            tests: TestSnapshot {
                revision: 1,
                status: TestStatus::Running,
                cases: vec![],
            },
        },
        TaskEventPayload::TaskCompleted {
            task: task_with_status(TaskStatus::Completed),
        },
        TaskEventPayload::TaskFailed {
            task: task_with_status(TaskStatus::Failed),
        },
        TaskEventPayload::TaskCancelled {
            task: task_with_status(TaskStatus::Cancelled),
        },
        TaskEventPayload::TaskInterrupted {
            task: task_with_status(TaskStatus::Interrupted),
        },
    ];
    let expected = [
        TaskEventKind::TaskQueued,
        TaskEventKind::TaskStarted,
        TaskEventKind::PlanUpdated,
        TaskEventKind::ActivityAppended,
        TaskEventKind::DiffUpdated,
        TaskEventKind::TestUpdated,
        TaskEventKind::TaskCompleted,
        TaskEventKind::TaskFailed,
        TaskEventKind::TaskCancelled,
        TaskEventKind::TaskInterrupted,
    ];

    for (payload, expected_kind) in payloads.iter().zip(expected) {
        assert_eq!(payload.kind(), expected_kind);
    }
}

#[test]
fn task_event_serialization_is_tagged_and_schema_versioned() {
    let task = task_with_status(TaskStatus::Queued);
    let event = TaskEvent::new(
        EventId::new(7).unwrap(),
        task.id,
        TaskEventPayload::TaskQueued { task },
        timestamp(),
    );

    let value = serde_json::to_value(&event).unwrap();

    assert_eq!(event.schema_version, 1);
    assert_eq!(value["schema_version"], 1);
    assert_eq!(value["kind"], "task.queued");
    assert_eq!(value["payload"]["task"]["status"], "queued");
    assert_eq!(serde_json::from_value::<TaskEvent>(value).unwrap(), event);
}

fn task_with_status(status: TaskStatus) -> Task {
    use TaskStatus::*;

    let (started_at, finished_at, failure) = match status {
        Queued => (None, None, None),
        Running => (Some(timestamp()), None, None),
        Completed => (Some(timestamp()), Some(timestamp()), None),
        Failed => (Some(timestamp()), Some(timestamp()), Some(task_failure())),
        Cancelled => (None, Some(timestamp()), None),
        Interrupted => (None, Some(timestamp()), Some(task_failure())),
    };

    Task {
        id: TaskId::new(),
        client_request_id: ClientRequestId::new(),
        repository_id: RepositoryId::new(),
        prompt: "implement the domain".into(),
        status,
        attempt: 1,
        retry_of: None,
        created_at: timestamp(),
        started_at,
        finished_at,
        last_event_id: EventId::new(1).unwrap(),
        failure,
    }
}

fn task_failure() -> TaskFailure {
    TaskFailure {
        code: "agent_interrupted".into(),
        message: "agent stopped".into(),
        retryable: true,
    }
}

fn timestamp() -> UtcTimestamp {
    UtcTimestamp::parse_rfc3339("2026-07-14T00:00:00Z").unwrap()
}

#[cfg(windows)]
fn absolute_path() -> PathBuf {
    PathBuf::from(r"C:\workspace\repo")
}

#[cfg(not(windows))]
fn absolute_path() -> PathBuf {
    PathBuf::from("/workspace/repo")
}

#[cfg(windows)]
fn path_with_current_component() -> PathBuf {
    PathBuf::from(r"C:\workspace\.\repo")
}

#[cfg(not(windows))]
fn path_with_current_component() -> PathBuf {
    PathBuf::from("/workspace/./repo")
}

#[cfg(windows)]
fn path_with_parent_component() -> PathBuf {
    PathBuf::from(r"C:\workspace\repo\..\other")
}

#[cfg(not(windows))]
fn path_with_parent_component() -> PathBuf {
    PathBuf::from("/workspace/repo/../other")
}
