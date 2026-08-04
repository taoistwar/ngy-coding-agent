mod support;

use std::sync::mpsc;
use std::time::Duration;

use coding_agent_domain::{
    ActivityActor, ActivityEntry, ActivityLevel, DeliveryReadiness, DiffFile, DiffFileStatus,
    DiffSnapshot, DomainError, EventCursor, PlanItem, PlanItemStatus, PlanSnapshot, TaskEventKind,
    TaskEventPayload, TaskStatus, TestCase, TestSnapshot, TestStatus,
};
use coding_agent_store::{
    AppendEventOutcome, FinalizeStoppedTaskOutcome, FinalizeStoppedTaskRequest,
    PersistStopIntentOutcome, QueueCapacity, StopIntentKind, StopIntentRequest, Store, StoreError,
    TaskTransition, TransitionOutcome,
};
use tokio::sync::oneshot;

#[tokio::test]
async fn projections_validate_running_and_retained_terminal_stop_intents() {
    let store = support::seeded_store().await;
    let running = support::running_task(&store).await;
    let intent_request = StopIntentRequest {
        task_id: running.id,
        expected_repository_id: running.repository_id,
        expected_attempt: running.attempt,
        kind: StopIntentKind::UserCancelled,
    };
    assert!(matches!(
        store.persist_stop_intent(intent_request).await.unwrap(),
        PersistStopIntentOutcome::Applied(_)
    ));
    assert!(store.bootstrap_snapshot().await.is_ok());
    assert!(store.task_detail(running.id).await.unwrap().is_some());

    assert!(matches!(
        store
            .finalize_stopped_task(FinalizeStoppedTaskRequest {
                task_id: running.id,
                expected_repository_id: running.repository_id,
                expected_attempt: running.attempt,
                expected_intent: StopIntentKind::UserCancelled,
            })
            .await
            .unwrap(),
        FinalizeStoppedTaskOutcome::Applied(_)
    ));
    assert!(store.bootstrap_snapshot().await.is_ok());
    assert!(store.task_detail(running.id).await.unwrap().is_some());

    sqlx::query("DROP TRIGGER task_stop_intents_no_update")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task_stop_intents SET requested_at = 'invalid' WHERE task_id = ?")
        .bind(running.id.to_string())
        .execute(store.pool())
        .await
        .unwrap();
    assert!(matches!(
        store.bootstrap_snapshot().await.unwrap_err(),
        StoreError::InvariantViolation(_)
    ));
    assert!(matches!(
        store.scheduler_bootstrap_snapshot().await.unwrap_err(),
        StoreError::InvariantViolation(_)
    ));
    assert!(matches!(
        store.task_detail(running.id).await.unwrap_err(),
        StoreError::InvariantViolation(_)
    ));
}

#[tokio::test]
async fn scheduler_bootstrap_snapshot_has_exact_running_intents_and_membership_watermark() {
    let store = support::seeded_store().await;
    let queued = support::queued_task(&store).await;
    let running = support::running_task(&store).await;
    let running_intent = match store
        .persist_stop_intent(StopIntentRequest {
            task_id: running.id,
            expected_repository_id: running.repository_id,
            expected_attempt: running.attempt,
            kind: StopIntentKind::UserCancelled,
        })
        .await
        .unwrap()
    {
        PersistStopIntentOutcome::Applied(receipt) => receipt,
        other => panic!("running intent must apply, got {other:?}"),
    };
    let stopping = support::running_task(&store).await;
    let stopping_intent = StopIntentRequest {
        task_id: stopping.id,
        expected_repository_id: stopping.repository_id,
        expected_attempt: stopping.attempt,
        kind: StopIntentKind::DiskPressureCritical,
    };
    assert!(matches!(
        store.persist_stop_intent(stopping_intent).await.unwrap(),
        PersistStopIntentOutcome::Applied(_)
    ));
    let terminal = match store
        .finalize_stopped_task(FinalizeStoppedTaskRequest {
            task_id: stopping.id,
            expected_repository_id: stopping.repository_id,
            expected_attempt: stopping.attempt,
            expected_intent: StopIntentKind::DiskPressureCritical,
        })
        .await
        .unwrap()
    {
        FinalizeStoppedTaskOutcome::Applied(receipt) => receipt,
        other => panic!("fixture final stop must apply, got {other:?}"),
    };
    let panel_event_id = match store
        .append_running_event(
            running.id,
            TaskEventPayload::PlanUpdated {
                plan: PlanSnapshot::legacy(1, Vec::new()),
            },
        )
        .await
        .unwrap()
    {
        AppendEventOutcome::Applied { event_id } => event_id,
        AppendEventOutcome::NotRunning { .. } => panic!("fixture task must remain running"),
    };

    let scheduler = store.scheduler_bootstrap_snapshot().await.unwrap();
    let bootstrap = store.bootstrap_snapshot().await.unwrap();
    assert_eq!(scheduler.repositories, bootstrap.repositories);
    assert_eq!(scheduler.tasks, bootstrap.tasks);
    assert_eq!(scheduler.running_stop_intents, vec![running_intent]);
    assert_eq!(scheduler.latest_event_id.get(), panel_event_id.get());
    assert_eq!(
        scheduler.membership_event_id.get(),
        terminal.terminal_event_id.get()
    );
    assert!(scheduler.membership_event_id < scheduler.latest_event_id);
    assert!(scheduler.tasks.iter().any(|task| task.id == queued.id));
    assert!(
        scheduler
            .running_stop_intents
            .iter()
            .all(|intent| intent.task_id != terminal.task.id)
    );
}

#[tokio::test]
async fn scheduler_membership_watermark_advances_only_for_lifecycle_events() {
    let store = support::seeded_store().await;
    let queued = support::queued_task(&store).await;
    let queued_snapshot = store.scheduler_bootstrap_snapshot().await.unwrap();
    assert_eq!(
        queued_snapshot.membership_event_id.get(),
        queued.last_event_id.get()
    );
    assert_eq!(
        queued_snapshot.latest_event_id.get(),
        queued.last_event_id.get()
    );

    let running = applied_task(
        store
            .transition_with_event(queued.id, TaskStatus::Queued, TaskTransition::Running)
            .await
            .unwrap(),
    );
    let started_snapshot = store.scheduler_bootstrap_snapshot().await.unwrap();
    assert_eq!(
        started_snapshot.membership_event_id.get(),
        running.last_event_id.get()
    );
    assert_eq!(
        started_snapshot.latest_event_id.get(),
        running.last_event_id.get()
    );

    let intent = match store
        .persist_stop_intent(StopIntentRequest {
            task_id: running.id,
            expected_repository_id: running.repository_id,
            expected_attempt: running.attempt,
            kind: StopIntentKind::UserCancelled,
        })
        .await
        .unwrap()
    {
        PersistStopIntentOutcome::Applied(receipt) => receipt,
        other => panic!("running intent must apply, got {other:?}"),
    };
    let intent_snapshot = store.scheduler_bootstrap_snapshot().await.unwrap();
    assert_eq!(
        intent_snapshot.membership_event_id,
        started_snapshot.membership_event_id
    );
    assert_eq!(
        intent_snapshot.latest_event_id,
        started_snapshot.latest_event_id
    );
    assert_eq!(intent_snapshot.running_stop_intents, vec![intent]);

    let panel_event_id = match store
        .append_running_event(
            running.id,
            TaskEventPayload::PlanUpdated {
                plan: PlanSnapshot::legacy(1, Vec::new()),
            },
        )
        .await
        .unwrap()
    {
        AppendEventOutcome::Applied { event_id } => event_id,
        AppendEventOutcome::NotRunning { .. } => panic!("fixture task must remain running"),
    };
    let panel_snapshot = store.scheduler_bootstrap_snapshot().await.unwrap();
    assert_eq!(
        panel_snapshot.membership_event_id,
        started_snapshot.membership_event_id
    );
    assert_eq!(panel_snapshot.latest_event_id.get(), panel_event_id.get());
    assert!(panel_snapshot.membership_event_id < panel_snapshot.latest_event_id);
}

#[tokio::test]
async fn scheduler_membership_watermark_is_bounded_by_the_requested_event_cursor() {
    let store = support::seeded_store().await;
    let queued = support::queued_task(&store).await;
    let queued_cursor = EventCursor::new(queued.last_event_id.get()).unwrap();
    let running = applied_task(
        store
            .transition_with_event(queued.id, TaskStatus::Queued, TaskTransition::Running)
            .await
            .unwrap(),
    );
    let started_cursor = EventCursor::new(running.last_event_id.get()).unwrap();
    let panel_event_id = match store
        .append_running_event(
            running.id,
            TaskEventPayload::PlanUpdated {
                plan: PlanSnapshot::legacy(1, Vec::new()),
            },
        )
        .await
        .unwrap()
    {
        AppendEventOutcome::Applied { event_id } => event_id,
        AppendEventOutcome::NotRunning { .. } => panic!("fixture task must remain running"),
    };
    let panel_cursor = EventCursor::new(panel_event_id.get()).unwrap();

    assert_eq!(
        store
            .membership_watermark_through(EventCursor::ZERO)
            .await
            .unwrap(),
        EventCursor::ZERO
    );
    assert_eq!(
        store
            .membership_watermark_through(queued_cursor)
            .await
            .unwrap(),
        queued_cursor
    );
    assert_eq!(
        store
            .membership_watermark_through(started_cursor)
            .await
            .unwrap(),
        started_cursor
    );
    assert_eq!(
        store
            .membership_watermark_through(panel_cursor)
            .await
            .unwrap(),
        started_cursor
    );
}

#[tokio::test]
async fn scheduler_membership_watermark_covers_every_terminal_lifecycle_kind() {
    for status in [
        TaskStatus::Completed,
        TaskStatus::Failed,
        TaskStatus::Cancelled,
        TaskStatus::Interrupted,
    ] {
        let store = support::seeded_store().await;
        let terminal = support::terminal_task(&store, status).await;
        let snapshot = store.scheduler_bootstrap_snapshot().await.unwrap();

        assert_eq!(
            snapshot.membership_event_id.get(),
            terminal.last_event_id.get()
        );
        assert_eq!(snapshot.latest_event_id.get(), terminal.last_event_id.get());
    }
}

#[tokio::test]
async fn scheduler_bootstrap_snapshot_has_deterministic_store_ordering() {
    let fixture = support::store_fixture().await;
    let first_repository = support::register_repository(&fixture.store, "scheduler-first").await;
    let second_repository = support::register_repository(&fixture.store, "scheduler-second").await;
    let tied_repository_timestamp = "2026-01-01T00:00:00.000000001Z";
    sqlx::query("UPDATE repositories SET last_opened_at = ?")
        .bind(tied_repository_timestamp)
        .execute(fixture.store.pool())
        .await
        .unwrap();

    let mut running_tasks = Vec::new();
    for (repository_id, prompt) in [
        (first_repository.id, "first running"),
        (second_repository.id, "second running"),
    ] {
        let queued = fixture
            .store
            .create_task(support::new_task(repository_id, prompt))
            .await
            .unwrap()
            .task()
            .clone();
        running_tasks.push(applied_task(
            fixture
                .store
                .transition_with_event(queued.id, TaskStatus::Queued, TaskTransition::Running)
                .await
                .unwrap(),
        ));
    }
    for task in &running_tasks {
        assert!(matches!(
            fixture
                .store
                .persist_stop_intent(StopIntentRequest {
                    task_id: task.id,
                    expected_repository_id: task.repository_id,
                    expected_attempt: task.attempt,
                    kind: StopIntentKind::UserCancelled,
                })
                .await
                .unwrap(),
            PersistStopIntentOutcome::Applied(_)
        ));
    }

    sqlx::query("DROP TRIGGER task_stop_intents_no_update")
        .execute(fixture.store.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task_stop_intents SET requested_at = ?")
        .bind(tied_repository_timestamp)
        .execute(fixture.store.pool())
        .await
        .unwrap();

    let snapshot = fixture.store.scheduler_bootstrap_snapshot().await.unwrap();
    assert_eq!(snapshot.repositories.len(), 2);
    assert_eq!(snapshot.tasks.len(), 2);
    assert_eq!(snapshot.running_stop_intents.len(), 2);
    assert!(snapshot.repositories.windows(2).all(|pair| {
        pair[0].last_opened_at > pair[1].last_opened_at
            || (pair[0].last_opened_at == pair[1].last_opened_at
                && pair[0].id.to_string() < pair[1].id.to_string())
    }));
    assert!(snapshot.tasks.windows(2).all(|pair| {
        pair[0].created_at > pair[1].created_at
            || (pair[0].created_at == pair[1].created_at
                && pair[0].id.to_string() < pair[1].id.to_string())
    }));
    assert!(snapshot.running_stop_intents.windows(2).all(|pair| {
        pair[0].requested_at < pair[1].requested_at
            || (pair[0].requested_at == pair[1].requested_at
                && pair[0].task_id.to_string() < pair[1].task_id.to_string())
    }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scheduler_bootstrap_snapshot_is_one_consistent_read_during_a_concurrent_commit() {
    let fixture = support::store_fixture().await;
    let repository = support::register_repository(&fixture.store, "coherent-scheduler").await;
    let queued_anchor = fixture
        .store
        .create_task(support::new_task(repository.id, "snapshot anchor"))
        .await
        .unwrap()
        .task()
        .clone();
    let anchor = applied_task(
        fixture
            .store
            .transition_with_event(
                queued_anchor.id,
                TaskStatus::Queued,
                TaskTransition::Running,
            )
            .await
            .unwrap(),
    );
    let payload_json = serde_json::to_string(&serde_json::json!({
        "entry": ActivityEntry::legacy(
            "snapshot-load",
            ActivityLevel::Info,
            "make the read transaction observable",
            support::current_timestamp(),
        )
    }))
    .unwrap();
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
         SELECT 1, ?, 'activity.appended', ?, ? FROM seq",
    )
    .bind(anchor.id.to_string())
    .bind(payload_json)
    .bind(created_at.to_string())
    .execute(&mut *seed)
    .await
    .unwrap();
    sqlx::query("UPDATE tasks SET last_event_id = ? WHERE id = ?")
        .bind(inserted.last_insert_rowid())
        .bind(anchor.id.to_string())
        .execute(&mut *seed)
        .await
        .unwrap();
    seed.commit().await.unwrap();
    let running_intent = match fixture
        .store
        .persist_stop_intent(StopIntentRequest {
            task_id: anchor.id,
            expected_repository_id: anchor.repository_id,
            expected_attempt: anchor.attempt,
            kind: StopIntentKind::UserCancelled,
        })
        .await
        .unwrap()
    {
        PersistStopIntentOutcome::Applied(receipt) => receipt,
        other => panic!("running intent must apply, got {other:?}"),
    };
    let before = fixture.store.scheduler_bootstrap_snapshot().await.unwrap();
    assert_eq!(before.running_stop_intents, vec![running_intent]);

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

    let snapshot_store = fixture.store.clone();
    let snapshot = tokio::spawn(async move { snapshot_store.scheduler_bootstrap_snapshot().await });
    match tokio::time::timeout(Duration::from_secs(5), entered_rx).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => {
            let _ = release_tx.send(());
            panic!("progress gate sender must stay alive");
        }
        Err(_) => {
            let _ = release_tx.send(());
            panic!("scheduler snapshot must reach the deterministic progress gate");
        }
    }

    drop(reserved.pop().expect("reserve a writer connection"));
    let finalize_result = tokio::time::timeout(
        Duration::from_secs(5),
        fixture
            .store
            .finalize_stopped_task(FinalizeStoppedTaskRequest {
                task_id: anchor.id,
                expected_repository_id: anchor.repository_id,
                expected_attempt: anchor.attempt,
                expected_intent: StopIntentKind::UserCancelled,
            }),
    )
    .await;
    release_tx.send(()).unwrap();
    let terminal = match finalize_result
        .expect("WAL writer must commit while the read snapshot is open")
        .unwrap()
    {
        FinalizeStoppedTaskOutcome::Applied(receipt) => receipt,
        other => panic!("final stop must apply, got {other:?}"),
    };
    let snapshot = tokio::time::timeout(Duration::from_secs(5), snapshot)
        .await
        .expect("scheduler snapshot must finish after gate release")
        .unwrap()
        .unwrap();

    assert_eq!(snapshot, before);
    let after = fixture.store.scheduler_bootstrap_snapshot().await.unwrap();
    assert_eq!(
        after
            .tasks
            .iter()
            .find(|task| task.id == anchor.id)
            .unwrap(),
        &terminal.task
    );
    assert!(
        after
            .running_stop_intents
            .iter()
            .all(|intent| intent.task_id != anchor.id)
    );
    assert!(after.latest_event_id > snapshot.latest_event_id);
    assert!(after.membership_event_id > snapshot.membership_event_id);
}

#[tokio::test]
async fn scheduler_bootstrap_snapshot_rejects_numeric_projection_overflow() {
    let store = support::seeded_store().await;
    let queued = support::queued_task(&store).await;
    sqlx::query("UPDATE tasks SET attempt = ? WHERE id = ?")
        .bind(i64::from(u32::MAX) + 1)
        .bind(queued.id.to_string())
        .execute(store.pool())
        .await
        .unwrap();

    assert!(matches!(
        store.scheduler_bootstrap_snapshot().await.unwrap_err(),
        StoreError::InvariantViolation(_)
    ));
}

#[tokio::test]
async fn bootstrap_rejects_an_orphan_stop_intent_outside_the_task_projection() {
    let store = support::seeded_store().await;
    let repository = store.list_repositories().await.unwrap().remove(0);
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("DROP TRIGGER task_stop_intents_running_unreviewed_on_insert")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO task_stop_intents \
             (task_id, repository_id, attempt, kind, requested_at) \
         VALUES (?, ?, 1, 'user_cancelled', ?)",
    )
    .bind(coding_agent_domain::TaskId::new().to_string())
    .bind(repository.id.to_string())
    .bind(support::current_timestamp().to_string())
    .execute(&mut *connection)
    .await
    .unwrap();
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
    drop(connection);

    assert!(matches!(
        store.bootstrap_snapshot().await.unwrap_err(),
        StoreError::InvariantViolation(_)
    ));
    assert!(matches!(
        store.scheduler_bootstrap_snapshot().await.unwrap_err(),
        StoreError::InvariantViolation(_)
    ));
}

#[tokio::test]
async fn scheduler_bootstrap_rejects_global_raw_aliases_and_orphans() {
    for corruption in [
        SchedulerGraphCorruption::RepositoryIdAlias,
        SchedulerGraphCorruption::RepositoryTimestamp,
        SchedulerGraphCorruption::RepositoryIdentityKey,
        SchedulerGraphCorruption::OrphanTaskRepository,
        SchedulerGraphCorruption::EventTaskIdAlias,
        SchedulerGraphCorruption::EventUnknownKind,
        SchedulerGraphCorruption::EventSchemaVersion,
        SchedulerGraphCorruption::EventTimestamp,
        SchedulerGraphCorruption::OrphanReview,
        SchedulerGraphCorruption::DeliveryTaskIdAlias,
    ] {
        let store = support::seeded_store().await;
        install_scheduler_graph_corruption(&store, corruption).await;

        assert!(
            matches!(
                store.scheduler_bootstrap_snapshot().await.unwrap_err(),
                StoreError::InvariantViolation(_)
            ),
            "{corruption:?}"
        );
        assert!(
            matches!(
                store.bootstrap_snapshot().await.unwrap_err(),
                StoreError::InvariantViolation(_)
            ),
            "{corruption:?}"
        );
    }
}

#[tokio::test]
async fn scheduler_bootstrap_rejects_repository_storage_class_corruption() {
    let store = support::seeded_store().await;
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA ignore_check_constraints = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("UPDATE repositories SET selected_path = CAST(selected_path AS BLOB)")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("PRAGMA ignore_check_constraints = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    drop(connection);

    assert!(matches!(
        store.scheduler_bootstrap_snapshot().await.unwrap_err(),
        StoreError::InvariantViolation(_)
    ));
    assert!(matches!(
        store.bootstrap_snapshot().await.unwrap_err(),
        StoreError::InvariantViolation(_)
    ));
}

#[test]
fn queue_capacity_projection_saturates_for_configured_and_unrepresentable_legacy_counts() {
    let max_queued_tasks = support::queue_limit(2);
    for (queued_tasks, expected_available) in [
        (0, 2),
        (1, 1),
        (2, 0),
        (3, 0),
        (u64::from(u32::MAX) + 1, 0),
        (u64::MAX, 0),
    ] {
        let capacity = QueueCapacity {
            queued_tasks,
            max_queued_tasks,
        };
        assert_eq!(
            capacity.available_tasks(),
            expected_available,
            "queued_tasks={queued_tasks}"
        );
    }
}

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
    let first_activity = ActivityEntry::legacy(
        "stable-entry",
        ActivityLevel::Info,
        "first message",
        created_at,
    );
    append(
        &store,
        running.id,
        TaskEventPayload::ActivityAppended {
            entry: first_activity.clone(),
        },
    )
    .await;
    let final_activity = ActivityEntry::legacy(
        "stable-entry",
        ActivityLevel::Warning,
        "replacement message",
        created_at,
    );
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
    let completed = support::historical_completed_task(&store, running).await;

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
async fn raw_project_two_plan_activity_and_lifecycle_payloads_replay_with_legacy_defaults() {
    let store = support::seeded_store().await;
    let queued = support::queued_task(&store).await;
    let running = applied_task(
        store
            .transition_with_event(queued.id, TaskStatus::Queued, TaskTransition::Running)
            .await
            .unwrap(),
    );
    let legacy_plan = PlanSnapshot::legacy(
        7,
        vec![PlanItem::legacy(
            "legacy-step",
            "Project 2 plan item",
            PlanItemStatus::Running,
        )],
    );
    append(
        &store,
        running.id,
        TaskEventPayload::PlanUpdated {
            plan: legacy_plan.clone(),
        },
    )
    .await;
    let activity_created_at =
        coding_agent_domain::UtcTimestamp::parse_rfc3339("2026-07-23T01:02:03Z").unwrap();
    let legacy_activity = ActivityEntry::legacy(
        "legacy-activity",
        ActivityLevel::Info,
        "Project 2 activity",
        activity_created_at,
    );
    append(
        &store,
        running.id,
        TaskEventPayload::ActivityAppended {
            entry: legacy_activity.clone(),
        },
    )
    .await;

    let legacy_plan_json = serde_json::json!({
        "plan": {
            "revision": 7,
            "items": [{
                "id": "legacy-step",
                "title": "Project 2 plan item",
                "status": "running"
            }]
        }
    })
    .to_string();
    let updated = sqlx::query(
        "UPDATE task_events SET payload_json = ? \
         WHERE task_id = ? AND kind = 'plan.updated'",
    )
    .bind(&legacy_plan_json)
    .bind(running.id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
    assert_eq!(updated.rows_affected(), 1);

    let legacy_activity_json = serde_json::json!({
        "entry": {
            "id": "legacy-activity",
            "level": "info",
            "message": "Project 2 activity",
            "created_at": activity_created_at
        }
    })
    .to_string();
    let updated = sqlx::query(
        "UPDATE task_events SET payload_json = ? \
         WHERE task_id = ? AND kind = 'activity.appended'",
    )
    .bind(&legacy_activity_json)
    .bind(running.id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
    assert_eq!(updated.rows_affected(), 1);

    let updated = sqlx::query(
        "UPDATE task_events \
         SET payload_json = json_remove(payload_json, '$.task.delivery_readiness') \
         WHERE task_id = ? AND kind IN ('task.queued', 'task.started')",
    )
    .bind(running.id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
    assert_eq!(updated.rows_affected(), 2);

    let raw_plan: serde_json::Value = serde_json::from_str(&legacy_plan_json).unwrap();
    assert!(raw_plan["plan"].get("format_version").is_none());
    assert!(raw_plan["plan"].get("summary").is_none());
    assert!(raw_plan["plan"].get("initial_required_checks").is_none());
    assert!(raw_plan["plan"]["items"][0].get("description").is_none());
    assert!(
        raw_plan["plan"]["items"][0]
            .get("acceptance_criteria")
            .is_none()
    );
    let raw_activity: serde_json::Value = serde_json::from_str(&legacy_activity_json).unwrap();
    assert!(raw_activity["entry"].get("actor").is_none());
    assert!(raw_activity["entry"].get("role_run").is_none());
    let raw_lifecycle_payloads: Vec<String> = sqlx::query_scalar(
        "SELECT payload_json FROM task_events \
         WHERE task_id = ? AND kind IN ('task.queued', 'task.started') ORDER BY id",
    )
    .bind(running.id.to_string())
    .fetch_all(store.pool())
    .await
    .unwrap();
    assert_eq!(raw_lifecycle_payloads.len(), 2);
    assert!(raw_lifecycle_payloads.iter().all(|payload| {
        serde_json::from_str::<serde_json::Value>(payload).unwrap()["task"]
            .get("delivery_readiness")
            .is_none()
    }));

    let events = store.events_after(EventCursor::ZERO, 100).await.unwrap();
    let replayed_plan = events
        .events
        .iter()
        .find_map(|event| match &event.payload {
            TaskEventPayload::PlanUpdated { plan } => Some(plan),
            _ => None,
        })
        .expect("legacy plan event");
    assert_eq!(replayed_plan, &legacy_plan);
    assert_eq!(replayed_plan.format_version(), 0);
    assert_eq!(replayed_plan.summary(), "");
    assert!(replayed_plan.initial_required_checks().is_empty());
    let replayed_activity = events
        .events
        .iter()
        .find_map(|event| match &event.payload {
            TaskEventPayload::ActivityAppended { entry } => Some(entry),
            _ => None,
        })
        .expect("legacy activity event");
    assert_eq!(replayed_activity, &legacy_activity);
    assert_eq!(replayed_activity.actor(), ActivityActor::System);
    assert_eq!(replayed_activity.role_run(), None);
    for event in &events.events {
        match &event.payload {
            TaskEventPayload::TaskQueued { task } | TaskEventPayload::TaskStarted { task } => {
                assert_eq!(task.delivery_readiness, DeliveryReadiness::Unreviewed);
            }
            _ => {}
        }
    }

    let detail = store.task_detail(running.id).await.unwrap().unwrap();
    assert_eq!(
        detail.task.delivery_readiness,
        DeliveryReadiness::Unreviewed
    );
    assert_eq!(detail.plan, Some(legacy_plan));
    assert_eq!(detail.activity, vec![legacy_activity]);
    assert_eq!(detail.plan.as_ref().unwrap().format_version(), 0);
    assert_eq!(detail.activity[0].actor(), ActivityActor::System);
    assert_eq!(detail.activity[0].role_run(), None);
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
    for (task, created_at) in [
        (&first_task, "2026-01-01T00:00:00.000000001Z"),
        (&second_task, "2026-01-01T00:00:00.000000002Z"),
    ] {
        sqlx::query("UPDATE tasks SET created_at = ? WHERE id = ?")
            .bind(created_at)
            .bind(task.id.to_string())
            .execute(fixture.store.pool())
            .await
            .unwrap();
        sqlx::query(
            "UPDATE task_events \
             SET created_at = ?, \
                 payload_json = json_set(payload_json, '$.task.created_at', ?) \
             WHERE task_id = ? AND kind = 'task.queued'",
        )
        .bind(created_at)
        .bind(created_at)
        .bind(task.id.to_string())
        .execute(fixture.store.pool())
        .await
        .unwrap();
    }

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
            entry: ActivityEntry::legacy(
                "activity",
                ActivityLevel::Info,
                "message",
                support::current_timestamp(),
            ),
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
    let scheduler = store.scheduler_bootstrap_snapshot().await.unwrap();
    assert!(scheduler.repositories.is_empty());
    assert!(scheduler.tasks.is_empty());
    assert!(scheduler.running_stop_intents.is_empty());
    assert_eq!(scheduler.latest_event_id, EventCursor::ZERO);
    assert_eq!(scheduler.membership_event_id, EventCursor::ZERO);
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

#[tokio::test]
async fn projection_storage_class_corruption_is_a_typed_invariant() {
    for corruption in [
        ProjectionStorageCorruption::TaskAttemptBlob,
        ProjectionStorageCorruption::EventPayloadBlob,
    ] {
        let store = support::seeded_store().await;
        let task = support::queued_task(&store).await;
        install_projection_storage_corruption(&store, &task, corruption).await;

        assert!(
            matches!(
                store.bootstrap_snapshot().await.unwrap_err(),
                StoreError::InvariantViolation(_)
            ),
            "{corruption:?}"
        );
        assert!(
            matches!(
                store.scheduler_bootstrap_snapshot().await.unwrap_err(),
                StoreError::InvariantViolation(_)
            ),
            "{corruption:?}"
        );
        assert!(
            matches!(
                store.task_detail(task.id).await.unwrap_err(),
                StoreError::InvariantViolation(_)
            ),
            "{corruption:?}"
        );
        if matches!(corruption, ProjectionStorageCorruption::EventPayloadBlob) {
            assert!(
                matches!(
                    store.events_after(EventCursor::ZERO, 10).await.unwrap_err(),
                    StoreError::InvariantViolation(_)
                ),
                "{corruption:?}"
            );
        }
    }
}

#[tokio::test]
async fn projection_rejects_noncanonical_task_rows_and_lifecycle_history() {
    for corruption in [
        ProjectionCanonicalCorruption::ClientRequestIdUppercase,
        ProjectionCanonicalCorruption::TaskTimestamp,
        ProjectionCanonicalCorruption::FailureJson,
        ProjectionCanonicalCorruption::LifecycleWhitespace,
        ProjectionCanonicalCorruption::DuplicateLifecycle,
    ] {
        let store = support::seeded_store().await;
        let task = match corruption {
            ProjectionCanonicalCorruption::FailureJson => {
                support::terminal_task(&store, TaskStatus::Failed).await
            }
            ProjectionCanonicalCorruption::ClientRequestIdUppercase
            | ProjectionCanonicalCorruption::TaskTimestamp
            | ProjectionCanonicalCorruption::LifecycleWhitespace
            | ProjectionCanonicalCorruption::DuplicateLifecycle => {
                support::queued_task(&store).await
            }
        };
        install_projection_canonical_corruption(&store, &task, corruption).await;

        assert!(
            matches!(
                store.bootstrap_snapshot().await.unwrap_err(),
                StoreError::InvariantViolation(_)
            ),
            "{corruption:?}"
        );
        assert!(
            matches!(
                store.scheduler_bootstrap_snapshot().await.unwrap_err(),
                StoreError::InvariantViolation(_)
            ),
            "{corruption:?}"
        );
        assert!(
            matches!(
                store.task_detail(task.id).await.unwrap_err(),
                StoreError::InvariantViolation(_)
            ),
            "{corruption:?}"
        );
    }
}

#[derive(Debug, Clone, Copy)]
enum SchedulerGraphCorruption {
    RepositoryIdAlias,
    RepositoryTimestamp,
    RepositoryIdentityKey,
    OrphanTaskRepository,
    EventTaskIdAlias,
    EventUnknownKind,
    EventSchemaVersion,
    EventTimestamp,
    OrphanReview,
    DeliveryTaskIdAlias,
}

async fn install_scheduler_graph_corruption(store: &Store, corruption: SchedulerGraphCorruption) {
    match corruption {
        SchedulerGraphCorruption::RepositoryIdAlias => {
            sqlx::query("UPDATE repositories SET id = '{' || id || '}'")
                .execute(store.pool())
                .await
                .unwrap();
        }
        SchedulerGraphCorruption::RepositoryTimestamp => {
            sqlx::query("UPDATE repositories SET created_at = replace(created_at, 'Z', '+00:00')")
                .execute(store.pool())
                .await
                .unwrap();
        }
        SchedulerGraphCorruption::RepositoryIdentityKey => {
            sqlx::query("UPDATE repositories SET git_identity_key = upper(git_identity_key)")
                .execute(store.pool())
                .await
                .unwrap();
        }
        SchedulerGraphCorruption::OrphanTaskRepository => {
            let task = support::queued_task(store).await;
            let mut connection = store.pool().acquire().await.unwrap();
            sqlx::query("PRAGMA foreign_keys = OFF")
                .execute(&mut *connection)
                .await
                .unwrap();
            sqlx::query("UPDATE tasks SET repository_id = ? WHERE id = ?")
                .bind(coding_agent_domain::RepositoryId::new().to_string())
                .bind(task.id.to_string())
                .execute(&mut *connection)
                .await
                .unwrap();
            sqlx::query("PRAGMA foreign_keys = ON")
                .execute(&mut *connection)
                .await
                .unwrap();
        }
        SchedulerGraphCorruption::EventTaskIdAlias => {
            let task = support::queued_task(store).await;
            let mut connection = store.pool().acquire().await.unwrap();
            sqlx::query("PRAGMA foreign_keys = OFF")
                .execute(&mut *connection)
                .await
                .unwrap();
            sqlx::query("UPDATE task_events SET task_id = '{' || task_id || '}' WHERE id = ?")
                .bind(task.last_event_id.get())
                .execute(&mut *connection)
                .await
                .unwrap();
            sqlx::query("PRAGMA foreign_keys = ON")
                .execute(&mut *connection)
                .await
                .unwrap();
        }
        SchedulerGraphCorruption::EventUnknownKind => {
            let task = support::queued_task(store).await;
            sqlx::query("UPDATE task_events SET kind = 'task.unknown' WHERE id = ?")
                .bind(task.last_event_id.get())
                .execute(store.pool())
                .await
                .unwrap();
        }
        SchedulerGraphCorruption::EventSchemaVersion => {
            let task = support::queued_task(store).await;
            sqlx::query("UPDATE task_events SET schema_version = 2 WHERE id = ?")
                .bind(task.last_event_id.get())
                .execute(store.pool())
                .await
                .unwrap();
        }
        SchedulerGraphCorruption::EventTimestamp => {
            let task = support::queued_task(store).await;
            sqlx::query(
                "UPDATE task_events \
                 SET created_at = replace(created_at, 'Z', '+00:00') WHERE id = ?",
            )
            .bind(task.last_event_id.get())
            .execute(store.pool())
            .await
            .unwrap();
        }
        SchedulerGraphCorruption::OrphanReview => {
            let repository = store.list_repositories().await.unwrap().remove(0);
            let mut connection = store.pool().acquire().await.unwrap();
            sqlx::query("PRAGMA foreign_keys = OFF")
                .execute(&mut *connection)
                .await
                .unwrap();
            sqlx::query(
                "INSERT INTO task_review_evidence (\
                     task_id, repository_id, attempt, review_round, workspace_generation, \
                     digest_algorithm, workspace_digest, decision_source, verdict, summary, \
                     findings_json, added_checks_json, required_checks_json, check_evidence_json, \
                     coverage_json, created_at, event_id, event_kind\
                 ) VALUES (?, ?, 1, 1, 0, 'workspace_fingerprint_v1', ?, 'reviewer', \
                     'changes_requested', 'orphan review', '[]', '[]', '[\"check\"]', '[]', \
                     'null', ?, 1, 'review.updated')",
            )
            .bind(coding_agent_domain::TaskId::new().to_string())
            .bind(repository.id.to_string())
            .bind("0".repeat(64))
            .bind(support::current_timestamp().to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
            sqlx::query("PRAGMA foreign_keys = ON")
                .execute(&mut *connection)
                .await
                .unwrap();
        }
        SchedulerGraphCorruption::DeliveryTaskIdAlias => {
            let task = support::queued_task(store).await;
            let mut connection = store.pool().acquire().await.unwrap();
            sqlx::query("PRAGMA foreign_keys = OFF")
                .execute(&mut *connection)
                .await
                .unwrap();
            sqlx::query(
                "INSERT INTO task_delivery_state (\
                     task_id, readiness, final_review_round, final_verdict, decided_at\
                 ) VALUES (?, 'review_approved', 1, 'approved', ?)",
            )
            .bind(format!("{{{}}}", task.id))
            .bind(support::current_timestamp().to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
            sqlx::query("PRAGMA foreign_keys = ON")
                .execute(&mut *connection)
                .await
                .unwrap();
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ProjectionStorageCorruption {
    TaskAttemptBlob,
    EventPayloadBlob,
}

async fn install_projection_storage_corruption(
    store: &Store,
    task: &coding_agent_domain::Task,
    corruption: ProjectionStorageCorruption,
) {
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA ignore_check_constraints = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
    match corruption {
        ProjectionStorageCorruption::TaskAttemptBlob => {
            sqlx::query("UPDATE tasks SET attempt = CAST(attempt AS BLOB) WHERE id = ?")
                .bind(task.id.to_string())
                .execute(&mut *connection)
                .await
                .unwrap();
        }
        ProjectionStorageCorruption::EventPayloadBlob => {
            sqlx::query(
                "UPDATE task_events \
                 SET payload_json = CAST(payload_json AS BLOB) WHERE id = ?",
            )
            .bind(task.last_event_id.get())
            .execute(&mut *connection)
            .await
            .unwrap();
        }
    }
    sqlx::query("PRAGMA ignore_check_constraints = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
}

#[derive(Debug, Clone, Copy)]
enum ProjectionCanonicalCorruption {
    ClientRequestIdUppercase,
    TaskTimestamp,
    FailureJson,
    LifecycleWhitespace,
    DuplicateLifecycle,
}

async fn install_projection_canonical_corruption(
    store: &Store,
    task: &coding_agent_domain::Task,
    corruption: ProjectionCanonicalCorruption,
) {
    match corruption {
        ProjectionCanonicalCorruption::ClientRequestIdUppercase => {
            sqlx::query(
                "UPDATE tasks SET client_request_id = upper(client_request_id) WHERE id = ?",
            )
            .bind(task.id.to_string())
            .execute(store.pool())
            .await
            .unwrap();
        }
        ProjectionCanonicalCorruption::TaskTimestamp => {
            sqlx::query(
                "UPDATE tasks \
                 SET created_at = replace(created_at, 'Z', '+00:00') WHERE id = ?",
            )
            .bind(task.id.to_string())
            .execute(store.pool())
            .await
            .unwrap();
        }
        ProjectionCanonicalCorruption::FailureJson => {
            let failure = task.failure.as_ref().unwrap();
            let reordered = serde_json::json!({
                "retryable": failure.retryable,
                "message": failure.message,
                "code": failure.code,
            });
            let raw = format!(
                "{{\"retryable\":{},\"message\":{},\"code\":{}}}",
                failure.retryable,
                serde_json::to_string(&failure.message).unwrap(),
                serde_json::to_string(&failure.code).unwrap()
            );
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&raw).unwrap(),
                reordered
            );
            sqlx::query("UPDATE tasks SET failure_json = ? WHERE id = ?")
                .bind(raw)
                .bind(task.id.to_string())
                .execute(store.pool())
                .await
                .unwrap();
        }
        ProjectionCanonicalCorruption::LifecycleWhitespace => {
            sqlx::query("UPDATE task_events SET payload_json = ' ' || payload_json WHERE id = ?")
                .bind(task.last_event_id.get())
                .execute(store.pool())
                .await
                .unwrap();
        }
        ProjectionCanonicalCorruption::DuplicateLifecycle => {
            sqlx::query(
                "INSERT INTO task_events \
                     (schema_version, task_id, kind, payload_json, created_at) \
                 SELECT schema_version, task_id, kind, payload_json, created_at \
                 FROM task_events WHERE id = ?",
            )
            .bind(task.last_event_id.get())
            .execute(store.pool())
            .await
            .unwrap();
        }
    }
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
    PlanSnapshot::legacy(
        revision,
        vec![PlanItem::legacy("item", title, PlanItemStatus::Running)],
    )
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
            truncated: false,
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
