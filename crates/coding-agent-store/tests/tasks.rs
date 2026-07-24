mod support;

use std::collections::HashSet;
use std::sync::Arc;

use coding_agent_domain::{
    CheckActor, CheckEvidence, CheckEvidenceStatus, ClientRequestId, DeliveryReadiness,
    DomainError, EventCursor, EventId, FindingSeverity, NewReviewEvidence, NewTask, RequiredCheck,
    ReviewCoverageEvidence, ReviewDecisionSource, ReviewEvidence, ReviewFinding, ReviewVerdict,
    Task, TaskEventKind, TaskEventPayload, TaskFailure, TaskId, TaskStatus, UtcTimestamp,
    WorkspaceDigest,
};
use coding_agent_store::{
    AppendEventOutcome, CreateTaskOutcome, RetryTaskOutcome, Store, StoreError, TaskTransition,
    TransitionOutcome,
};
use tokio::sync::Barrier;

#[tokio::test]
async fn create_sets_initial_attempt_retry_link_and_lifecycle_event() {
    let store = support::seeded_store().await;
    let repository = store.list_repositories().await.unwrap().remove(0);
    let outcome = store
        .create_task(support::new_task(repository.id, "  build the feature  "))
        .await
        .unwrap();
    let (task, event_id) = match outcome {
        CreateTaskOutcome::Created { task, event_id } => (task, event_id),
        CreateTaskOutcome::Existing { .. } => panic!("first request must create"),
    };

    assert_eq!(task.prompt, "build the feature");
    assert_eq!(task.attempt, 1);
    assert_eq!(task.retry_of, None);
    assert_eq!(task.status, TaskStatus::Queued);
    assert_eq!(task.delivery_readiness, DeliveryReadiness::Unreviewed);
    assert_eq!(task.last_event_id, event_id);

    let events = store
        .task_events_after(task.id, EventCursor::ZERO, 10)
        .await
        .unwrap();
    assert_eq!(events.events.len(), 1);
    assert_lifecycle_payload_points_to_own_event(&events.events[0]);
    assert_eq!(
        lifecycle_task(&events.events[0])
            .expect("queued event must contain a task")
            .delivery_readiness,
        DeliveryReadiness::Unreviewed
    );
}

#[tokio::test]
async fn repeated_create_is_idempotent_only_for_the_same_repository_and_trimmed_prompt() {
    let store = support::seeded_store().await;
    let first_repository = store.list_repositories().await.unwrap().remove(0);
    let second_repository = support::register_repository(&store, "other").await;
    let request_id = ClientRequestId::new();
    let first = NewTask::try_new(request_id, first_repository.id, " same prompt ").unwrap();
    let created = store.create_task(first.clone()).await.unwrap();

    let repeated = store.create_task(first).await.unwrap();
    assert!(matches!(repeated, CreateTaskOutcome::Existing { .. }));
    assert_eq!(repeated.task().id, created.task().id);
    assert_eq!(
        created.task().delivery_readiness,
        DeliveryReadiness::Unreviewed
    );
    assert_eq!(
        repeated.task().delivery_readiness,
        DeliveryReadiness::Unreviewed
    );

    let changed_prompt = NewTask::try_new(request_id, first_repository.id, "different").unwrap();
    assert!(matches!(
        store.create_task(changed_prompt).await.unwrap_err(),
        StoreError::IdempotencyConflict
    ));

    let changed_repository =
        NewTask::try_new(request_id, second_repository.id, "same prompt").unwrap();
    assert!(matches!(
        store.create_task(changed_repository).await.unwrap_err(),
        StoreError::IdempotencyConflict
    ));

    assert_eq!(event_count(&store).await, 1);
}

#[tokio::test]
async fn transition_and_event_are_one_transaction() {
    let store = support::seeded_store().await;
    let task = support::queued_task(&store).await;
    let changed = store
        .transition_with_event(task.id, TaskStatus::Queued, TaskTransition::Running)
        .await
        .unwrap();
    assert!(matches!(changed, TransitionOutcome::Applied { .. }));
    let detail = store.task_detail(task.id).await.unwrap().unwrap();
    assert_eq!(detail.task.status, TaskStatus::Running);
    assert_eq!(
        detail.task.delivery_readiness,
        DeliveryReadiness::Unreviewed
    );
    assert_eq!(
        detail.timeline.last().unwrap().kind,
        coding_agent_domain::TaskEventKind::TaskStarted
    );
    assert_eq!(
        detail.task.last_event_id,
        detail.timeline.last().unwrap().event_id
    );
}

#[tokio::test]
async fn transitions_set_timestamps_and_failure_fields_from_the_closed_transition() {
    let store = support::seeded_store().await;
    let queued = support::queued_task(&store).await;
    let running = applied_task(
        store
            .transition_with_event(queued.id, TaskStatus::Queued, TaskTransition::Running)
            .await
            .unwrap(),
    );
    let started_at = running.started_at.expect("running task start time");
    assert_eq!(running.finished_at, None);
    assert_eq!(running.failure, None);
    assert_eq!(running.delivery_readiness, DeliveryReadiness::Unreviewed);

    let failure = support::failure("RUNNER_FAILED");
    let failed = applied_task(
        store
            .transition_with_event(
                running.id,
                TaskStatus::Running,
                TaskTransition::Failed(failure.clone()),
            )
            .await
            .unwrap(),
    );
    assert_eq!(failed.started_at, Some(started_at));
    assert!(failed.finished_at.is_some());
    assert_eq!(failed.failure, Some(failure));
    assert_eq!(failed.delivery_readiness, DeliveryReadiness::Unreviewed);

    let cancelled = support::queued_task(&store).await;
    let cancelled = applied_task(
        store
            .transition_with_event(cancelled.id, TaskStatus::Queued, TaskTransition::Cancelled)
            .await
            .unwrap(),
    );
    assert_eq!(cancelled.started_at, None);
    assert!(cancelled.finished_at.is_some());
    assert_eq!(cancelled.failure, None);
    assert_eq!(cancelled.delivery_readiness, DeliveryReadiness::Unreviewed);
}

#[tokio::test]
async fn illegal_transition_is_rejected_before_sql_and_changes_nothing() {
    let store = support::seeded_store().await;
    let task = support::queued_task(&store).await;
    let before_events = event_count(&store).await;

    let error = store
        .transition_with_event(task.id, TaskStatus::Queued, TaskTransition::Completed)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        StoreError::IllegalTransition {
            from: TaskStatus::Queued,
            to: TaskStatus::Completed
        }
    ));

    let after = store.task_detail(task.id).await.unwrap().unwrap().task;
    assert_eq!(after, task);
    assert_eq!(event_count(&store).await, before_events);
}

#[tokio::test]
async fn transition_cas_miss_returns_the_current_task_without_an_event() {
    let store = support::seeded_store().await;
    let task = support::queued_task(&store).await;
    let before_events = event_count(&store).await;

    let error = store
        .transition_with_event(task.id, TaskStatus::Running, TaskTransition::Completed)
        .await
        .unwrap_err();
    assert!(matches!(error, StoreError::InvariantViolation(_)));
    assert_eq!(event_count(&store).await, before_events);

    assert!(matches!(
        store
            .transition_with_event(TaskId::new(), TaskStatus::Queued, TaskTransition::Running)
            .await
            .unwrap_err(),
        StoreError::TaskNotFound
    ));
}

#[tokio::test]
async fn running_events_reject_lifecycle_payloads_and_nonrunning_tasks() {
    let store = support::seeded_store().await;
    let queued = support::queued_task(&store).await;
    let plan = coding_agent_domain::PlanSnapshot::legacy(1, Vec::new());

    let not_running = store
        .append_running_event(
            queued.id,
            TaskEventPayload::PlanUpdated { plan: plan.clone() },
        )
        .await
        .unwrap();
    assert!(matches!(
        not_running,
        AppendEventOutcome::NotRunning { current } if current == queued
    ));

    let running = applied_task(
        store
            .transition_with_event(queued.id, TaskStatus::Queued, TaskTransition::Running)
            .await
            .unwrap(),
    );
    let before_invalid = event_count(&store).await;
    assert!(matches!(
        store
            .append_running_event(
                running.id,
                TaskEventPayload::TaskStarted {
                    task: running.clone()
                }
            )
            .await
            .unwrap_err(),
        StoreError::InvalidRunningEvent
    ));
    assert_eq!(event_count(&store).await, before_invalid);

    let appended = store
        .append_running_event(
            running.id,
            TaskEventPayload::PlanUpdated { plan: plan.clone() },
        )
        .await
        .unwrap();
    let event_id = match appended {
        AppendEventOutcome::Applied { event_id } => event_id,
        AppendEventOutcome::NotRunning { .. } => panic!("running task must accept panel event"),
    };
    let detail = store.task_detail(running.id).await.unwrap().unwrap();
    assert_eq!(detail.plan, Some(plan));
    assert_eq!(detail.task.last_event_id, event_id);

    let completed = support::historical_completed_task(&store, running).await;
    let late = store
        .append_running_event(
            completed.id,
            TaskEventPayload::PlanUpdated {
                plan: coding_agent_domain::PlanSnapshot::legacy(2, Vec::new()),
            },
        )
        .await
        .unwrap();
    assert!(matches!(late, AppendEventOutcome::NotRunning { .. }));
}

#[tokio::test]
async fn retry_is_a_linear_idempotent_chain() {
    let store = support::seeded_store().await;
    let source = support::terminal_task(&store, TaskStatus::Interrupted).await;
    let a = store.retry_task(source.id).await.unwrap();
    let b = store.retry_task(source.id).await.unwrap();
    assert!(matches!(a, RetryTaskOutcome::Created { .. }));
    assert!(matches!(b, RetryTaskOutcome::Existing { .. }));
    assert_eq!(a.task().id, b.task().id);
    assert_eq!(a.task().repository_id, source.repository_id);
    assert_eq!(a.task().prompt, source.prompt);
    assert_eq!(a.task().attempt, source.attempt + 1);
    assert_eq!(a.task().retry_of, Some(source.id));
    assert_ne!(a.task().client_request_id, source.client_request_id);
    assert_eq!(source.delivery_readiness, DeliveryReadiness::Unreviewed);
    assert_eq!(a.task().delivery_readiness, DeliveryReadiness::Unreviewed);
    assert_eq!(b.task().delivery_readiness, DeliveryReadiness::Unreviewed);

    assert!(matches!(
        store.retry_task(a.task().id).await.unwrap_err(),
        StoreError::TaskNotRetryable
    ));
    assert!(matches!(
        store.retry_task(TaskId::new()).await.unwrap_err(),
        StoreError::TaskNotFound
    ));
}

#[tokio::test]
async fn delivery_readiness_is_shared_by_bootstrap_create_and_retry_lookups() {
    let store = support::seeded_store().await;
    let repository = store.list_repositories().await.unwrap().remove(0);
    let approved_input = support::new_task(repository.id, "approved delivery");
    let approved = store
        .create_task(approved_input.clone())
        .await
        .unwrap()
        .task()
        .clone();
    let approved = applied_task(
        store
            .transition_with_event(approved.id, TaskStatus::Queued, TaskTransition::Running)
            .await
            .unwrap(),
    );
    let approved =
        install_delivery_fixture(&store, approved, DeliveryReadiness::ReviewApproved).await;
    store
        .task_events_after(approved.id, EventCursor::ZERO, 100)
        .await
        .expect("approved fixture events must decode");
    store
        .task_detail(approved.id)
        .await
        .expect("approved fixture must form a complete reviewed aggregate");

    let rejected_input = support::new_task(repository.id, "rejected delivery");
    let rejected = store
        .create_task(rejected_input.clone())
        .await
        .unwrap()
        .task()
        .clone();
    let rejected = applied_task(
        store
            .transition_with_event(rejected.id, TaskStatus::Queued, TaskTransition::Running)
            .await
            .unwrap(),
    );
    let rejected =
        install_delivery_fixture(&store, rejected, DeliveryReadiness::ReviewRejected).await;
    store
        .task_detail(rejected.id)
        .await
        .expect("rejected fixture must form a complete reviewed aggregate");

    let snapshot = store.bootstrap_snapshot().await.unwrap();
    for expected in [&approved, &rejected] {
        let stored = snapshot
            .tasks
            .iter()
            .find(|task| task.id == expected.id)
            .expect("finalized task must be present in bootstrap");
        assert_eq!(stored, expected);
    }

    let approved_existing = store.create_task(approved_input).await.unwrap();
    assert!(matches!(
        approved_existing,
        CreateTaskOutcome::Existing { .. }
    ));
    assert_eq!(
        approved_existing.task().delivery_readiness,
        DeliveryReadiness::ReviewApproved
    );
    let rejected_existing = store.create_task(rejected_input).await.unwrap();
    assert!(matches!(
        rejected_existing,
        CreateTaskOutcome::Existing { .. }
    ));
    assert_eq!(
        rejected_existing.task().delivery_readiness,
        DeliveryReadiness::ReviewRejected
    );

    for source in [approved, rejected] {
        let created = store.retry_task(source.id).await.unwrap();
        assert!(matches!(created, RetryTaskOutcome::Created { .. }));
        assert_eq!(
            created.task().delivery_readiness,
            DeliveryReadiness::Unreviewed
        );

        let existing = store.retry_task(source.id).await.unwrap();
        assert!(matches!(existing, RetryTaskOutcome::Existing { .. }));
        assert_eq!(existing.task().id, created.task().id);
        assert_eq!(
            existing.task().delivery_readiness,
            DeliveryReadiness::Unreviewed
        );
    }
}

#[tokio::test]
async fn delivery_readiness_rejects_an_illegal_task_terminal_tuple_on_every_lookup() {
    let store = support::seeded_store().await;
    let repository = store.list_repositories().await.unwrap().remove(0);
    let input = support::new_task(repository.id, "corrupt approved delivery");
    let task = store
        .create_task(input.clone())
        .await
        .unwrap()
        .task()
        .clone();
    let task = applied_task(
        store
            .transition_with_event(task.id, TaskStatus::Queued, TaskTransition::Running)
            .await
            .unwrap(),
    );
    let task = install_delivery_fixture(&store, task, DeliveryReadiness::ReviewApproved).await;
    let events_before = event_count(&store).await;

    let updated = sqlx::query(
        "UPDATE tasks \
         SET status = 'running', finished_at = NULL, failure_json = NULL \
         WHERE id = ?",
    )
    .bind(task.id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
    assert_eq!(updated.rows_affected(), 1);

    assert_invalid_task_state(store.bootstrap_snapshot().await.unwrap_err());
    assert_invalid_task_state(store.create_task(input).await.unwrap_err());
    assert_invalid_task_state(store.retry_task(task.id).await.unwrap_err());
    assert_invalid_task_state(store.task_detail(task.id).await.unwrap_err());
    assert_eq!(event_count(&store).await, events_before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn eight_concurrent_retries_create_one_direct_child_and_event() {
    const CALLS: usize = 8;

    let fixture = support::store_fixture().await;
    support::register_repository(&fixture.store, "concurrent-retry").await;
    let source = support::terminal_task(&fixture.store, TaskStatus::Interrupted).await;
    let barrier = Arc::new(Barrier::new(CALLS + 1));
    let mut calls = Vec::new();
    for _ in 0..CALLS {
        let store = fixture.store.clone();
        let barrier = barrier.clone();
        calls.push(tokio::spawn(async move {
            barrier.wait().await;
            store.retry_task(source.id).await.unwrap()
        }));
    }

    barrier.wait().await;
    let mut outcomes = Vec::new();
    for call in calls {
        outcomes.push(call.await.unwrap());
    }

    let child_ids: HashSet<_> = outcomes.iter().map(|outcome| outcome.task().id).collect();
    assert_eq!(child_ids.len(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, RetryTaskOutcome::Created { .. }))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, RetryTaskOutcome::Existing { .. }))
            .count(),
        CALLS - 1
    );

    let child_id = outcomes[0].task().id;
    let direct_children: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tasks WHERE retry_of = ?")
        .bind(source.id.to_string())
        .fetch_one(fixture.store.pool())
        .await
        .unwrap();
    let child_events: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM task_events WHERE task_id = ?")
            .bind(child_id.to_string())
            .fetch_one(fixture.store.pool())
            .await
            .unwrap();
    assert_eq!(direct_children, 1);
    assert_eq!(child_events, 1);
}

#[tokio::test]
async fn every_lifecycle_payload_contains_the_final_task_pointing_to_its_event() {
    let store = support::seeded_store().await;
    let task = support::queued_task(&store).await;
    let running = applied_task(
        store
            .transition_with_event(task.id, TaskStatus::Queued, TaskTransition::Running)
            .await
            .unwrap(),
    );
    store
        .transition_with_event(
            running.id,
            TaskStatus::Running,
            TaskTransition::Failed(support::failure("FAILED")),
        )
        .await
        .unwrap();
    let retry = store.retry_task(running.id).await.unwrap().task().clone();
    support::terminal_task(&store, TaskStatus::Completed).await;
    support::terminal_task(&store, TaskStatus::Cancelled).await;
    store
        .recover_incomplete(
            support::current_timestamp(),
            support::failure("APP_RESTARTED"),
        )
        .await
        .unwrap();

    let page = store.events_after(EventCursor::ZERO, 100).await.unwrap();
    let lifecycle_kinds: Vec<_> = page
        .events
        .iter()
        .filter_map(|event| match &event.payload {
            TaskEventPayload::TaskQueued { .. }
            | TaskEventPayload::TaskStarted { .. }
            | TaskEventPayload::TaskCompleted { .. }
            | TaskEventPayload::TaskFailed { .. }
            | TaskEventPayload::TaskCancelled { .. }
            | TaskEventPayload::TaskInterrupted { .. } => Some(event.payload.kind()),
            TaskEventPayload::PlanUpdated { .. }
            | TaskEventPayload::ActivityAppended { .. }
            | TaskEventPayload::DiffUpdated { .. }
            | TaskEventPayload::TestUpdated { .. }
            | TaskEventPayload::ReviewUpdated { .. } => None,
        })
        .collect();
    for expected in [
        TaskEventKind::TaskQueued,
        TaskEventKind::TaskStarted,
        TaskEventKind::TaskCompleted,
        TaskEventKind::TaskFailed,
        TaskEventKind::TaskCancelled,
        TaskEventKind::TaskInterrupted,
    ] {
        assert!(lifecycle_kinds.contains(&expected));
    }
    for event in &page.events {
        assert_lifecycle_payload_points_to_own_event(event);
        if let Some(task) = lifecycle_task(event) {
            assert_eq!(task.delivery_readiness, DeliveryReadiness::Unreviewed);
        }
    }

    let raw_links: Vec<(i64, Option<i64>)> = sqlx::query_as(
        "SELECT id, json_extract(payload_json, '$.task.last_event_id') \
         FROM task_events WHERE kind LIKE 'task.%' ORDER BY id",
    )
    .fetch_all(store.pool())
    .await
    .unwrap();
    assert!(
        raw_links
            .iter()
            .all(|(id, embedded)| *embedded == Some(*id))
    );
    assert_eq!(placeholder_count(&store).await, 0);
    assert_eq!(
        store
            .task_detail(retry.id)
            .await
            .unwrap()
            .unwrap()
            .task
            .status,
        TaskStatus::Interrupted
    );
}

#[tokio::test]
async fn recovery_interrupts_incomplete_tasks_in_deterministic_order_and_reports_watermark() {
    let store = support::seeded_store().await;
    let queued = support::queued_task(&store).await;
    let running = support::running_task(&store).await;
    let tied = support::queued_task(&store).await;
    let terminal = support::terminal_task(&store, TaskStatus::Completed).await;
    sqlx::query("UPDATE tasks SET created_at = ? WHERE id = ?")
        .bind("2026-01-01T00:00:00.000000001Z")
        .bind(queued.id.to_string())
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE tasks SET created_at = ? WHERE id = ?")
        .bind("2026-01-01T00:00:00.000000002Z")
        .bind(running.id.to_string())
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE tasks SET created_at = ? WHERE id = ?")
        .bind("2026-01-01T00:00:00.000000002Z")
        .bind(tied.id.to_string())
        .execute(store.pool())
        .await
        .unwrap();

    let now = UtcTimestamp::parse_rfc3339("2026-01-02T03:04:05.000000006Z").unwrap();
    let failure = support::failure("APP_RESTARTED");
    let outcome = store
        .recover_incomplete(now, failure.clone())
        .await
        .unwrap();
    assert_eq!(outcome.interrupted_count, 3);
    assert!(outcome.first_event_id < outcome.last_event_id);
    assert_eq!(
        outcome.high_watermark,
        store.latest_event_id().await.unwrap()
    );
    assert_eq!(
        outcome.last_event_id.unwrap().get(),
        outcome.high_watermark.get()
    );

    let events = store.events_after(EventCursor::ZERO, 100).await.unwrap();
    let interrupted_ids: Vec<_> = events
        .events
        .iter()
        .filter_map(|event| match &event.payload {
            TaskEventPayload::TaskInterrupted { task } => Some(task.id),
            _ => None,
        })
        .collect();
    let mut tied_ids = [running.id, tied.id];
    tied_ids.sort_by_key(ToString::to_string);
    assert_eq!(interrupted_ids, vec![queued.id, tied_ids[0], tied_ids[1]]);

    for task_id in [queued.id, running.id, tied.id] {
        let task = store.task_detail(task_id).await.unwrap().unwrap().task;
        assert_eq!(task.status, TaskStatus::Interrupted);
        assert_eq!(task.finished_at, Some(now));
        assert_eq!(task.failure, Some(failure.clone()));
        assert_eq!(task.delivery_readiness, DeliveryReadiness::Unreviewed);
    }
    assert_eq!(
        store.task_detail(terminal.id).await.unwrap().unwrap().task,
        terminal
    );
    assert_eq!(terminal.delivery_readiness, DeliveryReadiness::Unreviewed);

    let no_op = store.recover_incomplete(now, failure).await.unwrap();
    assert_eq!(no_op.interrupted_count, 0);
    assert_eq!(no_op.first_event_id, None);
    assert_eq!(no_op.last_event_id, None);
    assert_eq!(no_op.high_watermark, outcome.high_watermark);
}

#[tokio::test]
async fn recovery_failure_rolls_back_every_task_and_event() {
    let store = support::seeded_store().await;
    let first = support::queued_task(&store).await;
    let second = support::running_task(&store).await;
    let before_events = event_count(&store).await;
    let before_first = store.task_detail(first.id).await.unwrap().unwrap().task;
    let before_second = store.task_detail(second.id).await.unwrap().unwrap().task;

    sqlx::query(
        "CREATE TRIGGER fail_second_recovery AFTER UPDATE OF last_event_id ON tasks \
         WHEN NEW.status = 'interrupted' AND (\
             SELECT COUNT(*) FROM task_events WHERE kind = 'task.interrupted'\
         ) = 2 \
         BEGIN SELECT RAISE(ABORT, 'fault-recovery'); END",
    )
    .execute(store.pool())
    .await
    .unwrap();

    store
        .recover_incomplete(
            support::current_timestamp(),
            support::failure("APP_RESTARTED"),
        )
        .await
        .unwrap_err();

    assert_eq!(
        store.task_detail(first.id).await.unwrap().unwrap().task,
        before_first
    );
    assert_eq!(
        store.task_detail(second.id).await.unwrap().unwrap().task,
        before_second
    );
    assert_eq!(event_count(&store).await, before_events);
    assert_eq!(placeholder_count(&store).await, 0);
}

#[tokio::test]
async fn faults_at_each_lifecycle_write_stage_roll_back_without_publishable_placeholders() {
    for point in [
        FaultPoint::TaskStateUpdated,
        FaultPoint::PlaceholderInserted,
        FaultPoint::LastEventUpdated,
        FaultPoint::FinalPayloadUpdated,
    ] {
        assert_transition_fault_rolls_back(point).await;
    }
}

#[derive(Debug, Clone, Copy)]
enum FaultPoint {
    TaskStateUpdated,
    PlaceholderInserted,
    LastEventUpdated,
    FinalPayloadUpdated,
}

async fn assert_transition_fault_rolls_back(point: FaultPoint) {
    let store = support::seeded_store().await;
    let task = support::queued_task(&store).await;
    let before_events = event_count(&store).await;
    install_fault(&store, point).await;

    store
        .transition_with_event(task.id, TaskStatus::Queued, TaskTransition::Running)
        .await
        .expect_err("injected transaction fault must escape");

    let after = store.task_detail(task.id).await.unwrap().unwrap().task;
    assert_eq!(after, task, "fault point {point:?}");
    assert_eq!(event_count(&store).await, before_events, "{point:?}");
    assert_eq!(placeholder_count(&store).await, 0, "{point:?}");
    let later_publishable: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_events WHERE id > ? AND payload_json <> '{}'",
    )
    .bind(task.last_event_id.get())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(later_publishable, 0, "fault point {point:?}");
}

async fn install_fault(store: &Store, point: FaultPoint) {
    let trigger = match point {
        FaultPoint::TaskStateUpdated => {
            "CREATE TRIGGER injected_fault BEFORE INSERT ON task_events \
             WHEN NEW.payload_json = '{}' \
             BEGIN SELECT RAISE(ABORT, 'after-task-state'); END"
        }
        FaultPoint::PlaceholderInserted => {
            "CREATE TRIGGER injected_fault BEFORE UPDATE OF last_event_id ON tasks \
             WHEN OLD.last_event_id <> NEW.last_event_id \
             BEGIN SELECT RAISE(ABORT, 'after-placeholder'); END"
        }
        FaultPoint::LastEventUpdated => {
            "CREATE TRIGGER injected_fault BEFORE UPDATE OF payload_json ON task_events \
             WHEN OLD.payload_json = '{}' AND NEW.payload_json <> '{}' \
             BEGIN SELECT RAISE(ABORT, 'after-last-event'); END"
        }
        FaultPoint::FinalPayloadUpdated => {
            sqlx::raw_sql(
                "CREATE TABLE injected_fault_parent (id INTEGER PRIMARY KEY); \
                 CREATE TABLE injected_fault_child (\
                     parent_id INTEGER REFERENCES injected_fault_parent(id) \
                         DEFERRABLE INITIALLY DEFERRED\
                 ); \
                 CREATE TRIGGER injected_fault AFTER UPDATE OF payload_json ON task_events \
                 WHEN OLD.payload_json = '{}' AND NEW.payload_json <> '{}' \
                 BEGIN INSERT INTO injected_fault_child (parent_id) VALUES (1); END;",
            )
            .execute(store.pool())
            .await
            .unwrap();
            return;
        }
    };
    sqlx::query(trigger).execute(store.pool()).await.unwrap();
}

fn applied_task(outcome: TransitionOutcome) -> coding_agent_domain::Task {
    match outcome {
        TransitionOutcome::Applied { task, .. } => task,
        TransitionOutcome::Conflict { .. } => panic!("fixture transition must apply"),
    }
}

async fn install_delivery_fixture(
    store: &Store,
    mut task: Task,
    readiness: DeliveryReadiness,
) -> Task {
    assert_eq!(task.status, TaskStatus::Running);
    assert_eq!(task.delivery_readiness, DeliveryReadiness::Unreviewed);
    let decided_at = UtcTimestamp::parse_rfc3339("2026-07-23T12:34:56Z").unwrap();
    let evidence_history = match readiness {
        DeliveryReadiness::ReviewApproved => {
            vec![canonical_review_evidence(readiness, 1, decided_at)]
        }
        DeliveryReadiness::ReviewRejected => (1..=3)
            .map(|round| canonical_review_evidence(readiness, round, decided_at))
            .collect(),
        DeliveryReadiness::Unreviewed => {
            panic!("delivery fixture requires a final review decision")
        }
    };
    let evidence = evidence_history.last().unwrap().clone();
    let evidence_value = serde_json::to_value(&evidence).unwrap();
    assert_eq!(
        serde_json::from_value::<ReviewEvidence>(evidence_value.clone()).unwrap(),
        evidence
    );

    let verdict = evidence_value["verdict"].as_str().unwrap().to_owned();

    let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await.unwrap();
    let plan_payload = serde_json::to_string(&serde_json::json!({
        "plan": {
            "format_version": 1,
            "revision": 1,
            "summary": "Fixture quality plan",
            "items": [{
                "id": "fixture-step",
                "title": "Validate",
                "description": "Validate the fixture",
                "acceptance_criteria": ["The required check passes"],
                "status": "completed"
            }],
            "initial_required_checks": evidence.required_checks()
        }
    }))
    .unwrap();
    sqlx::query(
        "INSERT INTO task_events (schema_version, task_id, kind, payload_json, created_at) \
         VALUES (1, ?, 'plan.updated', ?, ?)",
    )
    .bind(task.id.to_string())
    .bind(plan_payload)
    .bind(decided_at.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    for review in &evidence_history {
        let review_value = serde_json::to_value(review).unwrap();
        let decision_source = review_value["decision_source"].as_str().unwrap();
        let review_verdict = review_value["verdict"].as_str().unwrap();
        let review_event_id = sqlx::query(
            "INSERT INTO task_events (schema_version, task_id, kind, payload_json, created_at) \
             VALUES (1, ?, 'review.updated', '{\"evidence_ref\":true}', ?)",
        )
        .bind(task.id.to_string())
        .bind(decided_at.to_string())
        .execute(&mut *transaction)
        .await
        .unwrap()
        .last_insert_rowid();

        sqlx::query(
            "INSERT INTO task_review_evidence (\
                 task_id, repository_id, attempt, review_round, workspace_generation, \
                 digest_algorithm, workspace_digest, decision_source, verdict, summary, \
                 findings_json, added_checks_json, required_checks_json, check_evidence_json, \
                 coverage_json, created_at, event_id, event_kind\
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'review.updated')",
        )
        .bind(task.id.to_string())
        .bind(task.repository_id.to_string())
        .bind(i64::from(task.attempt))
        .bind(i64::from(review.round()))
        .bind(i64::try_from(review.workspace_generation()).unwrap())
        .bind(review.workspace_digest().algorithm())
        .bind(review.workspace_digest().value())
        .bind(decision_source)
        .bind(review_verdict)
        .bind(review.summary())
        .bind(serde_json::to_string(review.findings()).unwrap())
        .bind(serde_json::to_string(review.added_required_checks()).unwrap())
        .bind(serde_json::to_string(review.required_checks()).unwrap())
        .bind(serde_json::to_string(review.check_evidence()).unwrap())
        .bind(serde_json::to_string(&review.coverage()).unwrap())
        .bind(decided_at.to_string())
        .bind(review_event_id)
        .execute(&mut *transaction)
        .await
        .unwrap();
    }

    let (status, readiness_text, terminal_kind, failure) = match readiness {
        DeliveryReadiness::ReviewApproved => (
            TaskStatus::Completed,
            "review_approved",
            "task.completed",
            None,
        ),
        DeliveryReadiness::ReviewRejected => (
            TaskStatus::Failed,
            "review_rejected",
            "task.failed",
            Some(TaskFailure {
                code: "REVIEW_REJECTED".to_owned(),
                message: "review rejected after three rounds".to_owned(),
                retryable: true,
            }),
        ),
        DeliveryReadiness::Unreviewed => {
            panic!("delivery fixture requires a final review decision")
        }
    };
    let failure_json = failure
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .unwrap();
    sqlx::query(
        "INSERT INTO task_delivery_state (\
             task_id, readiness, final_review_round, final_verdict, decided_at\
         ) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(task.id.to_string())
    .bind(readiness_text)
    .bind(i64::from(evidence.round()))
    .bind(&verdict)
    .bind(decided_at.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();

    let updated = sqlx::query(
        "UPDATE tasks SET status = ?, finished_at = ?, failure_json = ? \
         WHERE id = ? AND status = 'running'",
    )
    .bind(task_status_text(status))
    .bind(decided_at.to_string())
    .bind(&failure_json)
    .bind(task.id.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    assert_eq!(updated.rows_affected(), 1);

    let terminal_event_id = sqlx::query(
        "INSERT INTO task_events (schema_version, task_id, kind, payload_json, created_at) \
         VALUES (1, ?, ?, '{}', ?)",
    )
    .bind(task.id.to_string())
    .bind(terminal_kind)
    .bind(decided_at.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap()
    .last_insert_rowid();
    let terminal_event_id = EventId::new(terminal_event_id).unwrap();

    task.status = status;
    task.delivery_readiness = readiness;
    task.finished_at = Some(decided_at);
    task.last_event_id = terminal_event_id;
    task.failure = failure;
    task = Task::try_from_stored(task).unwrap();

    let updated = sqlx::query("UPDATE tasks SET last_event_id = ? WHERE id = ?")
        .bind(terminal_event_id.get())
        .bind(task.id.to_string())
        .execute(&mut *transaction)
        .await
        .unwrap();
    assert_eq!(updated.rows_affected(), 1);
    let payload_json = serde_json::to_string(&serde_json::json!({ "task": &task })).unwrap();
    let updated = sqlx::query(
        "UPDATE task_events SET payload_json = ? \
         WHERE id = ? AND payload_json = '{}'",
    )
    .bind(payload_json)
    .bind(terminal_event_id.get())
    .execute(&mut *transaction)
    .await
    .unwrap();
    assert_eq!(updated.rows_affected(), 1);

    transaction.commit().await.unwrap();
    task
}

fn canonical_review_evidence(
    readiness: DeliveryReadiness,
    round: u8,
    created_at: UtcTimestamp,
) -> ReviewEvidence {
    let digest = WorkspaceDigest::try_new("a".repeat(64)).unwrap();
    let required_check = RequiredCheck::try_cargo_test(
        "store-mapper-cargo-test",
        Some("coding-agent-store".to_owned()),
        None,
    )
    .unwrap();
    let check_evidence = CheckEvidence::try_for_check(
        &required_check,
        CheckActor::Executor,
        1,
        0,
        digest.clone(),
        CheckEvidenceStatus::Passed,
        1,
        "passed",
        false,
    )
    .unwrap();
    let (verdict, findings, coverage) = match readiness {
        DeliveryReadiness::ReviewApproved => (
            ReviewVerdict::Approved,
            Vec::new(),
            Some(
                ReviewCoverageEvidence::try_new(0, digest.clone(), "b".repeat(64), vec![0], 1)
                    .unwrap(),
            ),
        ),
        DeliveryReadiness::ReviewRejected => (
            ReviewVerdict::ChangesRequested,
            vec![
                ReviewFinding::try_for_review(
                    round,
                    1,
                    FindingSeverity::Blocking,
                    "A blocking issue remains",
                    None,
                    None,
                )
                .unwrap(),
            ],
            None,
        ),
        DeliveryReadiness::Unreviewed => unreachable!(),
    };
    let new = NewReviewEvidence::try_new(
        round,
        ReviewDecisionSource::Reviewer,
        0,
        digest,
        verdict,
        "Canonical store mapper fixture",
        findings,
        Vec::new(),
        vec![required_check],
        vec![check_evidence],
        coverage,
    )
    .unwrap();
    ReviewEvidence::try_from_new(new, created_at).unwrap()
}

const fn task_status_text(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Queued => "queued",
        TaskStatus::Running => "running",
        TaskStatus::Completed => "completed",
        TaskStatus::Failed => "failed",
        TaskStatus::Cancelled => "cancelled",
        TaskStatus::Interrupted => "interrupted",
    }
}

fn assert_invalid_task_state(error: StoreError) {
    assert!(matches!(
        error,
        StoreError::Domain(DomainError::InvalidTaskState)
    ));
}

fn assert_lifecycle_payload_points_to_own_event(event: &coding_agent_domain::TaskEvent) {
    let Some(task) = lifecycle_task(event) else {
        return;
    };
    assert_eq!(task.id, event.task_id);
    assert_eq!(task.last_event_id, event.id);
}

fn lifecycle_task(event: &coding_agent_domain::TaskEvent) -> Option<&Task> {
    match &event.payload {
        TaskEventPayload::TaskQueued { task }
        | TaskEventPayload::TaskStarted { task }
        | TaskEventPayload::TaskCompleted { task }
        | TaskEventPayload::TaskFailed { task }
        | TaskEventPayload::TaskCancelled { task }
        | TaskEventPayload::TaskInterrupted { task } => Some(task),
        TaskEventPayload::PlanUpdated { .. }
        | TaskEventPayload::ActivityAppended { .. }
        | TaskEventPayload::DiffUpdated { .. }
        | TaskEventPayload::TestUpdated { .. }
        | TaskEventPayload::ReviewUpdated { .. } => None,
    }
}

async fn event_count(store: &Store) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM task_events")
        .fetch_one(store.pool())
        .await
        .unwrap()
}

async fn placeholder_count(store: &Store) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM task_events WHERE payload_json = '{}'")
        .fetch_one(store.pool())
        .await
        .unwrap()
}
