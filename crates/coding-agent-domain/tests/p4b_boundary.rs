use coding_agent_domain::{TaskEventKind, TaskEventPayload, TaskStatus};

#[test]
fn p4b_does_not_expand_the_six_task_lifecycle_states() {
    let statuses = [
        TaskStatus::Queued,
        TaskStatus::Running,
        TaskStatus::Completed,
        TaskStatus::Failed,
        TaskStatus::Cancelled,
        TaskStatus::Interrupted,
    ];
    let wire = statuses
        .map(|status| serde_json::to_value(status).unwrap())
        .map(|value| value.as_str().unwrap().to_owned());

    assert_eq!(
        wire,
        [
            "queued",
            "running",
            "completed",
            "failed",
            "cancelled",
            "interrupted",
        ]
    );
    assert!(!wire.iter().any(|status| status == "merged"));
    assert!(serde_json::from_str::<TaskStatus>(r#""merged""#).is_err());
}

// Adding a seventh lifecycle state must force an explicit boundary decision here.
#[allow(dead_code)]
const fn characterize_task_status(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Queued => "queued",
        TaskStatus::Running => "running",
        TaskStatus::Completed => "completed",
        TaskStatus::Failed => "failed",
        TaskStatus::Cancelled => "cancelled",
        TaskStatus::Interrupted => "interrupted",
    }
}

#[test]
fn p4b_does_not_add_a_twelfth_persisted_task_event() {
    let kinds = [
        TaskEventKind::TaskQueued,
        TaskEventKind::TaskStarted,
        TaskEventKind::PlanUpdated,
        TaskEventKind::ActivityAppended,
        TaskEventKind::DiffUpdated,
        TaskEventKind::TestUpdated,
        TaskEventKind::ReviewUpdated,
        TaskEventKind::TaskCompleted,
        TaskEventKind::TaskFailed,
        TaskEventKind::TaskCancelled,
        TaskEventKind::TaskInterrupted,
    ];
    let wire = kinds
        .map(|kind| serde_json::to_value(kind).unwrap())
        .map(|value| value.as_str().unwrap().to_owned());

    assert_eq!(
        wire,
        [
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
        ]
    );
    assert!(!wire.iter().any(|kind| kind.starts_with("delivery.")));
    assert!(serde_json::from_str::<TaskEventKind>(r#""delivery.merged""#).is_err());
}

// This exhaustive match is intentionally a compile-time characterization. Adding a
// delivery payload (or any other twelfth payload) forces this boundary test to change.
#[allow(dead_code)]
const fn characterize_payload(payload: &TaskEventPayload) -> TaskEventKind {
    match payload {
        TaskEventPayload::TaskQueued { .. } => TaskEventKind::TaskQueued,
        TaskEventPayload::TaskStarted { .. } => TaskEventKind::TaskStarted,
        TaskEventPayload::PlanUpdated { .. } => TaskEventKind::PlanUpdated,
        TaskEventPayload::ActivityAppended { .. } => TaskEventKind::ActivityAppended,
        TaskEventPayload::DiffUpdated { .. } => TaskEventKind::DiffUpdated,
        TaskEventPayload::TestUpdated { .. } => TaskEventKind::TestUpdated,
        TaskEventPayload::ReviewUpdated { .. } => TaskEventKind::ReviewUpdated,
        TaskEventPayload::TaskCompleted { .. } => TaskEventKind::TaskCompleted,
        TaskEventPayload::TaskFailed { .. } => TaskEventKind::TaskFailed,
        TaskEventPayload::TaskCancelled { .. } => TaskEventKind::TaskCancelled,
        TaskEventPayload::TaskInterrupted { .. } => TaskEventKind::TaskInterrupted,
    }
}
