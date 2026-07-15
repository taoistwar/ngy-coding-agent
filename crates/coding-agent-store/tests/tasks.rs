mod support;

use std::collections::HashSet;
use std::sync::Arc;

use coding_agent_domain::{
    ClientRequestId, EventCursor, NewTask, TaskEventKind, TaskEventPayload, TaskId, TaskStatus,
    UtcTimestamp,
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
    assert_eq!(task.last_event_id, event_id);

    let events = store
        .task_events_after(task.id, EventCursor::ZERO, 10)
        .await
        .unwrap();
    assert_eq!(events.events.len(), 1);
    assert_lifecycle_payload_points_to_own_event(&events.events[0]);
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

    let outcome = store
        .transition_with_event(task.id, TaskStatus::Running, TaskTransition::Completed)
        .await
        .unwrap();
    match outcome {
        TransitionOutcome::Conflict { current } => assert_eq!(current, task),
        TransitionOutcome::Applied { .. } => panic!("stale expected state must not apply"),
    }
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
    let plan = coding_agent_domain::PlanSnapshot {
        revision: 1,
        items: Vec::new(),
    };

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

    let completed = applied_task(
        store
            .transition_with_event(running.id, TaskStatus::Running, TaskTransition::Completed)
            .await
            .unwrap(),
    );
    let late = store
        .append_running_event(
            completed.id,
            TaskEventPayload::PlanUpdated {
                plan: coding_agent_domain::PlanSnapshot {
                    revision: 2,
                    items: Vec::new(),
                },
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

    assert!(matches!(
        store.retry_task(a.task().id).await.unwrap_err(),
        StoreError::TaskNotRetryable
    ));
    assert!(matches!(
        store.retry_task(TaskId::new()).await.unwrap_err(),
        StoreError::TaskNotFound
    ));
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
            | TaskEventPayload::TestUpdated { .. } => None,
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
    }
    assert_eq!(
        store.task_detail(terminal.id).await.unwrap().unwrap().task,
        terminal
    );

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

fn assert_lifecycle_payload_points_to_own_event(event: &coding_agent_domain::TaskEvent) {
    let task = match &event.payload {
        TaskEventPayload::TaskQueued { task }
        | TaskEventPayload::TaskStarted { task }
        | TaskEventPayload::TaskCompleted { task }
        | TaskEventPayload::TaskFailed { task }
        | TaskEventPayload::TaskCancelled { task }
        | TaskEventPayload::TaskInterrupted { task } => task,
        TaskEventPayload::PlanUpdated { .. }
        | TaskEventPayload::ActivityAppended { .. }
        | TaskEventPayload::DiffUpdated { .. }
        | TaskEventPayload::TestUpdated { .. } => return,
    };
    assert_eq!(task.id, event.task_id);
    assert_eq!(task.last_event_id, event.id);
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
