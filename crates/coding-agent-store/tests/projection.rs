mod support;

use std::sync::mpsc;
use std::time::Duration;

use coding_agent_domain::{
    ActivityEntry, ActivityLevel, DiffFile, DiffFileStatus, DiffSnapshot, DomainError, EventCursor,
    PlanItem, PlanItemStatus, PlanSnapshot, TaskEventKind, TaskEventPayload, TaskStatus, TestCase,
    TestSnapshot, TestStatus,
};
use coding_agent_store::{
    AppendEventOutcome, Store, StoreError, TaskTransition, TransitionOutcome,
};
use tokio::sync::oneshot;

#[tokio::test]
async fn task_detail_replays_replacing_snapshots_deduplicated_activity_and_lifecycle_timeline() {
    let store = support::seeded_store().await;
    let queued = support::queued_task(&store).await;
    let running = applied_task(
        store
            .transition_with_event(queued.id, TaskStatus::Queued, TaskTransition::Running)
            .await
            .unwrap(),
    );

    let old_plan = plan(1, "first");
    let final_plan = plan(2, "second");
    append(
        &store,
        running.id,
        TaskEventPayload::PlanUpdated { plan: old_plan },
    )
    .await;
    append(
        &store,
        running.id,
        TaskEventPayload::PlanUpdated {
            plan: final_plan.clone(),
        },
    )
    .await;

    let created_at = support::current_timestamp();
    let first_activity = ActivityEntry {
        id: "stable-entry".to_owned(),
        level: ActivityLevel::Info,
        message: "first message".to_owned(),
        created_at,
    };
    append(
        &store,
        running.id,
        TaskEventPayload::ActivityAppended {
            entry: first_activity.clone(),
        },
    )
    .await;
    let final_activity = ActivityEntry {
        id: "stable-entry".to_owned(),
        level: ActivityLevel::Warning,
        message: "replacement message".to_owned(),
        created_at,
    };
    append(
        &store,
        running.id,
        TaskEventPayload::ActivityAppended {
            entry: final_activity.clone(),
        },
    )
    .await;

    append(
        &store,
        running.id,
        TaskEventPayload::DiffUpdated { diff: diff(1) },
    )
    .await;
    let final_diff = diff(2);
    append(
        &store,
        running.id,
        TaskEventPayload::DiffUpdated {
            diff: final_diff.clone(),
        },
    )
    .await;
    append(
        &store,
        running.id,
        TaskEventPayload::TestUpdated { tests: tests(1) },
    )
    .await;
    let final_tests = tests(2);
    append(
        &store,
        running.id,
        TaskEventPayload::TestUpdated {
            tests: final_tests.clone(),
        },
    )
    .await;
    let completed = applied_task(
        store
            .transition_with_event(running.id, TaskStatus::Running, TaskTransition::Completed)
            .await
            .unwrap(),
    );

    let unrelated = support::queued_task(&store).await;
    let detail = store.task_detail(completed.id).await.unwrap().unwrap();
    assert_eq!(detail.task, completed);
    assert_eq!(detail.plan, Some(final_plan));
    assert_eq!(detail.activity, vec![first_activity]);
    assert_eq!(detail.diff, Some(final_diff));
    assert_eq!(detail.tests, Some(final_tests));
    assert_eq!(
        detail
            .timeline
            .iter()
            .map(|entry| entry.kind)
            .collect::<Vec<_>>(),
        vec![
            TaskEventKind::TaskQueued,
            TaskEventKind::TaskStarted,
            TaskEventKind::TaskCompleted,
        ]
    );
    assert!(detail.timeline.iter().all(|entry| entry.failure.is_none()));
    assert_eq!(detail.event_cursor, store.latest_event_id().await.unwrap());
    assert_eq!(detail.event_cursor.get(), unrelated.last_event_id.get());
}

#[tokio::test]
async fn failed_and_interrupted_timeline_entries_project_structured_failure() {
    let store = support::seeded_store().await;
    let failed = support::terminal_task(&store, TaskStatus::Failed).await;
    let interrupted = support::terminal_task(&store, TaskStatus::Interrupted).await;

    for task in [failed, interrupted] {
        let detail = store.task_detail(task.id).await.unwrap().unwrap();
        let final_entry = detail.timeline.last().unwrap();
        assert_eq!(final_entry.failure, task.failure);
    }
}

#[tokio::test]
async fn bootstrap_orders_complete_repository_and_task_lists_with_one_global_watermark() {
    let fixture = support::store_fixture().await;
    let first_repository = support::register_repository(&fixture.store, "first").await;
    let second_repository = support::register_repository(&fixture.store, "second").await;
    let first_task = fixture
        .store
        .create_task(support::new_task(first_repository.id, "first"))
        .await
        .unwrap()
        .task()
        .clone();
    let second_task = fixture
        .store
        .create_task(support::new_task(second_repository.id, "second"))
        .await
        .unwrap()
        .task()
        .clone();

    sqlx::query("UPDATE repositories SET last_opened_at = ? WHERE id = ?")
        .bind("2026-01-01T00:00:00.000000001Z")
        .bind(first_repository.id.to_string())
        .execute(fixture.store.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE repositories SET last_opened_at = ? WHERE id = ?")
        .bind("2026-01-01T00:00:00.000000002Z")
        .bind(second_repository.id.to_string())
        .execute(fixture.store.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE tasks SET created_at = ? WHERE id = ?")
        .bind("2026-01-01T00:00:00.000000001Z")
        .bind(first_task.id.to_string())
        .execute(fixture.store.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE tasks SET created_at = ? WHERE id = ?")
        .bind("2026-01-01T00:00:00.000000002Z")
        .bind(second_task.id.to_string())
        .execute(fixture.store.pool())
        .await
        .unwrap();

    let snapshot = fixture.store.bootstrap_snapshot().await.unwrap();
    assert_eq!(
        snapshot
            .repositories
            .iter()
            .map(|repository| repository.id)
            .collect::<Vec<_>>(),
        vec![second_repository.id, first_repository.id]
    );
    assert_eq!(
        snapshot
            .tasks
            .iter()
            .map(|task| task.id)
            .collect::<Vec<_>>(),
        vec![second_task.id, first_task.id]
    );
    assert_eq!(
        snapshot.latest_event_id,
        fixture.store.latest_event_id().await.unwrap()
    );
}

#[tokio::test]
async fn global_and_task_event_pages_are_ordered_filtered_and_share_a_query_watermark() {
    let store = support::seeded_store().await;
    let first = support::running_task(&store).await;
    append(
        &store,
        first.id,
        TaskEventPayload::ActivityAppended {
            entry: ActivityEntry {
                id: "activity".to_owned(),
                level: ActivityLevel::Info,
                message: "message".to_owned(),
                created_at: support::current_timestamp(),
            },
        },
    )
    .await;
    let second = support::queued_task(&store).await;
    let latest = store.latest_event_id().await.unwrap();

    let first_page = store.events_after(EventCursor::ZERO, 2).await.unwrap();
    assert_eq!(first_page.events.len(), 2);
    assert!(first_page.events[0].id < first_page.events[1].id);
    assert_eq!(first_page.high_watermark, latest);

    let cursor = EventCursor::new(first_page.events[1].id.get()).unwrap();
    let tail = store.events_after(cursor, 100).await.unwrap();
    assert!(
        tail.events
            .iter()
            .all(|event| event.id.get() > cursor.get())
    );
    assert_eq!(tail.high_watermark, latest);

    let task_page = store
        .task_events_after(first.id, EventCursor::ZERO, 100)
        .await
        .unwrap();
    assert!(
        task_page
            .events
            .iter()
            .all(|event| event.task_id == first.id)
    );
    assert!(
        task_page
            .events
            .windows(2)
            .all(|pair| pair[0].id < pair[1].id)
    );
    assert_eq!(task_page.high_watermark, latest);
    assert!(
        !task_page
            .events
            .iter()
            .any(|event| event.task_id == second.id)
    );
}

#[tokio::test]
async fn empty_store_and_missing_task_have_zero_or_none_projections() {
    let store = support::memory_store().await;
    assert_eq!(store.latest_event_id().await.unwrap(), EventCursor::ZERO);
    let snapshot = store.bootstrap_snapshot().await.unwrap();
    assert!(snapshot.repositories.is_empty());
    assert!(snapshot.tasks.is_empty());
    assert_eq!(snapshot.latest_event_id, EventCursor::ZERO);
    assert!(
        store
            .task_detail(coding_agent_domain::TaskId::new())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn lifecycle_payload_decode_rejects_invalid_tasks_and_broken_envelope_links() {
    for corruption in [
        LifecycleCorruption::InvalidTaskState,
        LifecycleCorruption::WrongTaskId,
        LifecycleCorruption::WrongEventId,
        LifecycleCorruption::WrongLifecycleStatus,
    ] {
        assert_lifecycle_corruption_is_rejected(corruption).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_detail_events_and_global_watermark_share_one_read_snapshot() {
    let fixture = support::store_fixture().await;
    support::register_repository(&fixture.store, "coherent-detail").await;
    let task = support::running_task(&fixture.store).await;
    let old_plan = plan(512, "snapshot before concurrent commit");
    let payload_json = serde_json::to_string(&serde_json::json!({ "plan": old_plan })).unwrap();
    let created_at = support::current_timestamp();
    let mut seed = fixture
        .store
        .pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap();
    let inserted = sqlx::query(
        "WITH RECURSIVE seq(n) AS (\
             VALUES(1) UNION ALL SELECT n + 1 FROM seq WHERE n < 512\
         ) \
         INSERT INTO task_events (schema_version, task_id, kind, payload_json, created_at) \
         SELECT 1, ?, 'plan.updated', ?, ? FROM seq",
    )
    .bind(task.id.to_string())
    .bind(payload_json)
    .bind(created_at.to_string())
    .execute(&mut *seed)
    .await
    .unwrap();
    sqlx::query("UPDATE tasks SET last_event_id = ? WHERE id = ?")
        .bind(inserted.last_insert_rowid())
        .bind(task.id.to_string())
        .execute(&mut *seed)
        .await
        .unwrap();
    seed.commit().await.unwrap();
    let before_cursor = fixture.store.latest_event_id().await.unwrap();

    let max_connections = fixture.store.pool().options().get_max_connections() as usize;
    assert!(max_connections >= 2);
    let mut reserved = Vec::with_capacity(max_connections);
    for _ in 0..max_connections {
        reserved.push(fixture.store.pool().acquire().await.unwrap());
    }
    let mut instrumented = reserved.pop().unwrap();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    {
        let mut handle = instrumented.lock_handle().await.unwrap();
        let mut entered_tx = Some(entered_tx);
        let mut release_rx = Some(release_rx);
        handle.set_progress_handler(1_000, move || {
            if let Some(entered_tx) = entered_tx.take() {
                let _ = entered_tx.send(());
                let _ = release_rx.take().unwrap().recv();
            }
            true
        });
    }
    drop(instrumented);

    let detail_store = fixture.store.clone();
    let detail = tokio::spawn(async move { detail_store.task_detail(task.id).await });
    match tokio::time::timeout(Duration::from_secs(5), entered_rx).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => {
            let _ = release_tx.send(());
            panic!("progress gate sender must stay alive");
        }
        Err(_) => {
            let _ = release_tx.send(());
            panic!("event replay must reach the deterministic progress gate");
        }
    }

    drop(reserved.pop().expect("reserve a writer connection"));
    let new_plan = plan(513, "committed after detail snapshot");
    let append_result = tokio::time::timeout(
        Duration::from_secs(5),
        fixture.store.append_running_event(
            task.id,
            TaskEventPayload::PlanUpdated {
                plan: new_plan.clone(),
            },
        ),
    )
    .await;
    release_tx.send(()).unwrap();
    let appended = append_result
        .expect("WAL writer must commit while the read snapshot is open")
        .unwrap();
    let new_event_id = match appended {
        AppendEventOutcome::Applied { event_id } => event_id,
        AppendEventOutcome::NotRunning { .. } => panic!("fixture task must remain running"),
    };
    let detail = tokio::time::timeout(Duration::from_secs(5), detail)
        .await
        .expect("detail replay must finish after gate release")
        .unwrap()
        .unwrap()
        .unwrap();

    assert_eq!(detail.plan, Some(old_plan));
    assert_eq!(detail.event_cursor, before_cursor);
    assert!(new_event_id.get() > before_cursor.get());
    assert_eq!(
        fixture.store.latest_event_id().await.unwrap().get(),
        new_event_id.get()
    );
    assert_ne!(detail.plan, Some(new_plan));
}

#[derive(Debug, Clone, Copy)]
enum LifecycleCorruption {
    InvalidTaskState,
    WrongTaskId,
    WrongEventId,
    WrongLifecycleStatus,
}

async fn assert_lifecycle_corruption_is_rejected(corruption: LifecycleCorruption) {
    let store = support::seeded_store().await;
    let task = support::queued_task(&store).await;
    match corruption {
        LifecycleCorruption::InvalidTaskState => {
            sqlx::query(
                "UPDATE task_events \
                 SET payload_json = json_set(payload_json, '$.task.status', 'running') \
                 WHERE id = ?",
            )
            .bind(task.last_event_id.get())
            .execute(store.pool())
            .await
            .unwrap();
        }
        LifecycleCorruption::WrongTaskId => {
            sqlx::query(
                "UPDATE task_events \
                 SET payload_json = json_set(payload_json, '$.task.id', ?) \
                 WHERE id = ?",
            )
            .bind(coding_agent_domain::TaskId::new().to_string())
            .bind(task.last_event_id.get())
            .execute(store.pool())
            .await
            .unwrap();
        }
        LifecycleCorruption::WrongEventId => {
            sqlx::query(
                "UPDATE task_events \
                 SET payload_json = json_set(payload_json, '$.task.last_event_id', id + 1) \
                 WHERE id = ?",
            )
            .bind(task.last_event_id.get())
            .execute(store.pool())
            .await
            .unwrap();
        }
        LifecycleCorruption::WrongLifecycleStatus => {
            sqlx::query(
                "UPDATE task_events \
                 SET payload_json = json_set(\
                     payload_json, '$.task.status', 'running', '$.task.started_at', created_at\
                 ) \
                 WHERE id = ?",
            )
            .bind(task.last_event_id.get())
            .execute(store.pool())
            .await
            .unwrap();
        }
    }

    let replay_error = store
        .events_after(EventCursor::ZERO, 10)
        .await
        .expect_err("corrupt lifecycle event must not be emitted");
    assert_corruption_error(corruption, replay_error);
    let detail_error = store
        .task_detail(task.id)
        .await
        .expect_err("corrupt lifecycle event must not enter TaskDetail");
    assert_corruption_error(corruption, detail_error);
}

fn assert_corruption_error(corruption: LifecycleCorruption, error: StoreError) {
    match corruption {
        LifecycleCorruption::InvalidTaskState => assert!(matches!(
            error,
            StoreError::Domain(DomainError::InvalidTaskState)
        )),
        LifecycleCorruption::WrongTaskId
        | LifecycleCorruption::WrongEventId
        | LifecycleCorruption::WrongLifecycleStatus => {
            assert!(matches!(error, StoreError::InvariantViolation(_)))
        }
    }
}

async fn append(store: &Store, task_id: coding_agent_domain::TaskId, payload: TaskEventPayload) {
    assert!(matches!(
        store.append_running_event(task_id, payload).await.unwrap(),
        AppendEventOutcome::Applied { .. }
    ));
}

fn applied_task(outcome: TransitionOutcome) -> coding_agent_domain::Task {
    match outcome {
        TransitionOutcome::Applied { task, .. } => task,
        TransitionOutcome::Conflict { .. } => panic!("fixture transition must apply"),
    }
}

fn plan(revision: u64, title: &str) -> PlanSnapshot {
    PlanSnapshot {
        revision,
        items: vec![PlanItem {
            id: "item".to_owned(),
            title: title.to_owned(),
            status: PlanItemStatus::Running,
        }],
    }
}

fn diff(revision: u64) -> DiffSnapshot {
    DiffSnapshot {
        revision,
        files: vec![DiffFile {
            path: "src/lib.rs".to_owned(),
            status: DiffFileStatus::Modified,
            patch: format!("revision {revision}"),
            additions: revision,
            deletions: 0,
        }],
    }
}

fn tests(revision: u64) -> TestSnapshot {
    TestSnapshot {
        revision,
        status: TestStatus::Passed,
        cases: vec![TestCase {
            id: "case".to_owned(),
            name: "case".to_owned(),
            status: TestStatus::Passed,
            duration_ms: revision,
            summary: format!("revision {revision}"),
        }],
    }
}
