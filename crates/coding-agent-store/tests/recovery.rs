mod support;

use coding_agent_domain::{
    CheckActor, CheckEvidence, CheckEvidenceStatus, DeliveryReadiness, EventCursor, EventId,
    FindingSeverity, NewReviewEvidence, PlanItem, PlanItemStatus, PlanSnapshot, RequiredCheck,
    ReviewCoverageEvidence, ReviewDecisionSource, ReviewFinding, ReviewVerdict, Task, TaskEvent,
    TaskEventPayload, TaskId, TaskStatus, UtcTimestamp, WorkspaceDigest,
};
use coding_agent_store::{
    FinalizeReviewedTaskOutcome, FinalizeStoppedTaskOutcome, FinalizeStoppedTaskRequest,
    StopIntentKind, Store, StoreError, TaskTransition, TransitionOutcome,
};

const APP_RESTARTED: &str = "APP_RESTARTED";

#[tokio::test]
async fn cold_recovery_finishes_stops_then_interrupts_running_in_canonical_order() {
    let store = support::seeded_store().await;

    let stop_old = support::running_task(&store).await;
    let stop_tied_left = support::running_task(&store).await;
    let stop_tied_right = support::running_task(&store).await;
    insert_stop_intent(
        &store,
        &stop_old,
        StopIntentKind::UserCancelled,
        "2026-07-27T01:00:00Z",
    )
    .await;
    insert_stop_intent(
        &store,
        &stop_tied_left,
        StopIntentKind::DiskPressureCritical,
        "2026-07-27T02:00:00Z",
    )
    .await;
    insert_stop_intent(
        &store,
        &stop_tied_right,
        StopIntentKind::UserCancelled,
        "2026-07-27T02:00:00Z",
    )
    .await;

    let running_old = support::running_task(&store).await;
    let running_tied_left = support::running_task(&store).await;
    let running_tied_right = support::running_task(&store).await;
    set_task_created_at(&store, &running_old, "2026-07-27T03:00:00Z").await;
    set_task_created_at(&store, &running_tied_left, "2026-07-27T04:00:00Z").await;
    set_task_created_at(&store, &running_tied_right, "2026-07-27T04:00:00Z").await;

    let queued = support::queued_task(&store).await;
    set_task_created_at(&store, &queued, "2026-07-27T00:00:00Z").await;
    let queued_before = store.task_detail(queued.id).await.unwrap().unwrap();
    let before = store.latest_event_id().await.unwrap();

    let receipt = store.recover_after_restart().await.unwrap();
    let page = store.events_after(before, 100).await.unwrap();
    assert_eq!(receipt.finalized_stop_count, 3);
    assert_eq!(receipt.interrupted_count, 3);
    assert_eq!(page.events.len(), 6);
    assert_eq!(
        receipt.first_event_id,
        page.events.first().map(|event| event.id)
    );
    assert_eq!(
        receipt.last_event_id,
        page.events.last().map(|event| event.id)
    );
    assert_eq!(
        receipt.high_watermark,
        store.latest_event_id().await.unwrap()
    );
    assert_eq!(
        receipt.membership_high_watermark,
        membership_high_watermark(&store).await
    );
    assert_eq!(
        receipt.last_event_id.map(EventId::get),
        Some(receipt.high_watermark.get())
    );
    assert!(
        page.events
            .windows(2)
            .all(|pair| pair[1].id.get() == pair[0].id.get() + 1)
    );

    let mut stopped = vec![
        (
            stop_old.clone(),
            UtcTimestamp::parse_rfc3339("2026-07-27T01:00:00Z").unwrap(),
            StopIntentKind::UserCancelled,
        ),
        (
            stop_tied_left.clone(),
            UtcTimestamp::parse_rfc3339("2026-07-27T02:00:00Z").unwrap(),
            StopIntentKind::DiskPressureCritical,
        ),
        (
            stop_tied_right.clone(),
            UtcTimestamp::parse_rfc3339("2026-07-27T02:00:00Z").unwrap(),
            StopIntentKind::UserCancelled,
        ),
    ];
    stopped.sort_by_key(|(task, requested_at, _)| (*requested_at, task.id.as_uuid().as_u128()));

    let mut running = vec![
        (
            running_old.clone(),
            UtcTimestamp::parse_rfc3339("2026-07-27T03:00:00Z").unwrap(),
        ),
        (
            running_tied_left.clone(),
            UtcTimestamp::parse_rfc3339("2026-07-27T04:00:00Z").unwrap(),
        ),
        (
            running_tied_right.clone(),
            UtcTimestamp::parse_rfc3339("2026-07-27T04:00:00Z").unwrap(),
        ),
    ];
    running.sort_by_key(|(task, created_at)| (*created_at, task.id.as_uuid().as_u128()));

    let expected_ids: Vec<_> = stopped
        .iter()
        .map(|(task, _, _)| task.id)
        .chain(running.iter().map(|(task, _)| task.id))
        .collect();
    assert_eq!(
        page.events
            .iter()
            .map(|event| event.task_id)
            .collect::<Vec<_>>(),
        expected_ids
    );

    for (task, _, kind) in stopped {
        let detail = store.task_detail(task.id).await.unwrap().unwrap();
        match kind {
            StopIntentKind::UserCancelled => {
                assert_eq!(detail.task.status, TaskStatus::Cancelled);
                assert_eq!(detail.task.failure, None);
            }
            StopIntentKind::DiskPressureCritical => {
                assert_eq!(detail.task.status, TaskStatus::Failed);
                let failure = detail.task.failure.as_ref().unwrap();
                assert_eq!(failure.code, "DISK_PRESSURE_CRITICAL");
                assert!(failure.retryable);
            }
        }
        assert_terminal_event_matches_task(
            page.events
                .iter()
                .find(|event| event.task_id == task.id)
                .unwrap(),
            &detail.task,
        );
    }

    for (task, _) in running {
        let detail = store.task_detail(task.id).await.unwrap().unwrap();
        assert_eq!(detail.task.status, TaskStatus::Interrupted);
        assert_eq!(
            detail
                .task
                .failure
                .as_ref()
                .map(|failure| failure.code.as_str()),
            Some(APP_RESTARTED)
        );
        assert!(
            detail
                .task
                .failure
                .as_ref()
                .is_some_and(|failure| failure.retryable)
        );
        assert_terminal_event_matches_task(
            page.events
                .iter()
                .find(|event| event.task_id == task.id)
                .unwrap(),
            &detail.task,
        );
    }

    assert_detail_unchanged_except_cursor(
        &store.task_detail(queued.id).await.unwrap().unwrap(),
        &queued_before,
    );
    assert_eq!(intent_count(&store).await, 3);
}

#[tokio::test]
async fn cold_recovery_preflights_every_intent_aggregate_before_its_first_write() {
    for corruption in [
        PreflightCorruption::RunningIntent,
        PreflightCorruption::TerminalIntent,
    ] {
        let store = support::seeded_store().await;
        let would_be_first = support::running_task(&store).await;
        insert_stop_intent(
            &store,
            &would_be_first,
            StopIntentKind::UserCancelled,
            "2026-07-27T01:00:00Z",
        )
        .await;

        let corrupt = support::running_task(&store).await;
        insert_stop_intent(
            &store,
            &corrupt,
            StopIntentKind::DiskPressureCritical,
            "2026-07-27T02:00:00Z",
        )
        .await;
        match corruption {
            PreflightCorruption::RunningIntent => {
                sqlx::query("UPDATE tasks SET prompt = 'corrupt-running-aggregate' WHERE id = ?")
                    .bind(corrupt.id.to_string())
                    .execute(store.pool())
                    .await
                    .unwrap();
            }
            PreflightCorruption::TerminalIntent => {
                let receipt = match store
                    .finalize_stopped_task(finalize_request(
                        &corrupt,
                        StopIntentKind::DiskPressureCritical,
                    ))
                    .await
                    .unwrap()
                {
                    FinalizeStoppedTaskOutcome::Applied(receipt) => receipt,
                    other => panic!("fixture final stop must apply, got {other:?}"),
                };
                sqlx::query(
                    "INSERT INTO task_events \
                         (schema_version, task_id, kind, payload_json, created_at) \
                     SELECT schema_version, task_id, kind, payload_json, created_at \
                     FROM task_events WHERE id = ?",
                )
                .bind(receipt.terminal_event_id.get())
                .execute(store.pool())
                .await
                .unwrap();
            }
        }
        sqlx::query(
            "CREATE TRIGGER recovery_preflight_must_precede_writes \
             BEFORE UPDATE OF status ON tasks \
             WHEN OLD.status = 'running' AND NEW.status != OLD.status \
             BEGIN SELECT RAISE(ABORT, 'recovery-wrote-before-preflight'); END",
        )
        .execute(store.pool())
        .await
        .unwrap();
        let before = support::durable_task_event_snapshot(&store).await;

        assert!(
            matches!(
                store.recover_after_restart().await.unwrap_err(),
                StoreError::InvariantViolation(_)
            ),
            "{corruption:?}"
        );
        assert_eq!(
            support::durable_task_event_snapshot(&store).await,
            before,
            "{corruption:?}"
        );
    }
}

#[tokio::test]
async fn cold_recovery_commit_replay_is_a_noop_with_original_watermarks() {
    let fixture = support::store_fixture().await;
    support::register_repository(&fixture.store, "restart-replay").await;
    let stopped = support::running_task(&fixture.store).await;
    let running = support::running_task(&fixture.store).await;
    let queued = support::queued_task(&fixture.store).await;
    insert_stop_intent(
        &fixture.store,
        &stopped,
        StopIntentKind::DiskPressureCritical,
        "2026-07-27T01:00:00Z",
    )
    .await;
    let queued_before = fixture.store.task_detail(queued.id).await.unwrap().unwrap();

    let applied = fixture.store.recover_after_restart().await.unwrap();
    assert_eq!(applied.finalized_stop_count, 1);
    assert_eq!(applied.interrupted_count, 1);
    let committed = support::durable_task_event_snapshot(&fixture.store).await;
    assert_eq!(applied.high_watermark.get(), committed.high_watermark);
    assert_eq!(
        applied.membership_high_watermark,
        membership_high_watermark(&fixture.store).await
    );
    fixture.store.checkpoint_and_close().await.unwrap();

    let reopened = Store::open(&fixture.database_path).await.unwrap();
    let replay = reopened.recover_after_restart().await.unwrap();
    assert_eq!(replay.finalized_stop_count, 0);
    assert_eq!(replay.interrupted_count, 0);
    assert_eq!(replay.first_event_id, None);
    assert_eq!(replay.last_event_id, None);
    assert_eq!(replay.high_watermark, applied.high_watermark);
    assert_eq!(
        replay.membership_high_watermark,
        applied.membership_high_watermark
    );
    assert_eq!(
        support::durable_task_event_snapshot(&reopened).await,
        committed
    );
    assert_detail_unchanged_except_cursor(
        &reopened.task_detail(queued.id).await.unwrap().unwrap(),
        &queued_before,
    );
    assert_eq!(
        reopened
            .task_detail(stopped.id)
            .await
            .unwrap()
            .unwrap()
            .task
            .status,
        TaskStatus::Failed
    );
    assert_eq!(
        reopened
            .task_detail(running.id)
            .await
            .unwrap()
            .unwrap()
            .task
            .status,
        TaskStatus::Interrupted
    );
}

#[tokio::test]
async fn every_recovery_sql_and_post_write_fault_rolls_back_the_whole_transaction() {
    for fault in [
        RecoveryFault::StopTaskUpdate,
        RecoveryFault::StopEventInsert,
        RecoveryFault::StopCursorUpdate,
        RecoveryFault::StopPayloadUpdate,
        RecoveryFault::StopAfterPayloadTamper,
        RecoveryFault::StopAfterPayloadDelete,
        RecoveryFault::InterruptTaskUpdate,
        RecoveryFault::InterruptEventInsert,
        RecoveryFault::InterruptCursorUpdate,
        RecoveryFault::InterruptPayloadUpdate,
        RecoveryFault::InterruptAfterPayloadTamper,
        RecoveryFault::InterruptAfterPayloadDelete,
        RecoveryFault::DeferredCommit,
    ] {
        let store = support::seeded_store().await;
        let stopped = support::running_task(&store).await;
        let running = support::running_task(&store).await;
        insert_stop_intent(
            &store,
            &stopped,
            StopIntentKind::UserCancelled,
            "2026-07-27T01:00:00Z",
        )
        .await;
        let before = support::durable_task_event_snapshot(&store).await;
        install_recovery_fault(&store, fault).await;

        let result = store.recover_after_restart().await;
        assert!(
            result.is_err(),
            "{fault:?} unexpectedly committed: {result:?}"
        );

        assert_eq!(
            support::durable_task_event_snapshot(&store).await,
            before,
            "{fault:?}"
        );
        assert_eq!(
            store
                .task_detail(stopped.id)
                .await
                .unwrap()
                .unwrap()
                .task
                .status,
            TaskStatus::Running,
            "{fault:?}"
        );
        assert_eq!(
            store
                .task_detail(running.id)
                .await
                .unwrap()
                .unwrap()
                .task
                .status,
            TaskStatus::Running,
            "{fault:?}"
        );
        if matches!(fault, RecoveryFault::DeferredCommit) {
            assert_eq!(
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM recovery_fault_child")
                    .fetch_one(store.pool())
                    .await
                    .unwrap(),
                0
            );
        }
    }
}

#[tokio::test]
async fn final_recovery_graph_validation_rolls_back_later_item_tampering_an_earlier_item() {
    let store = support::seeded_store().await;
    let first = support::running_task(&store).await;
    let second = support::running_task(&store).await;
    set_task_created_at(&store, &first, "2026-07-27T01:00:00Z").await;
    set_task_created_at(&store, &second, "2026-07-27T02:00:00Z").await;
    let sql = format!(
        "CREATE TRIGGER recovery_cross_item_fault \
         AFTER UPDATE OF payload_json ON task_events \
         WHEN NEW.kind = 'task.interrupted' AND NEW.task_id = '{}' \
         BEGIN UPDATE tasks SET prompt = 'tampered-by-later-recovery-item' \
               WHERE id = '{}'; END",
        second.id, first.id
    );
    sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
        .execute(store.pool())
        .await
        .unwrap();
    let before = support::durable_task_event_snapshot(&store).await;

    assert!(matches!(
        store.recover_after_restart().await.unwrap_err(),
        StoreError::InvariantViolation(_)
    ));
    assert_eq!(support::durable_task_event_snapshot(&store).await, before);
}

#[tokio::test]
async fn recovery_post_state_rejects_self_consistent_tampering_of_a_later_running_task() {
    let store = support::seeded_store().await;
    let first = support::running_task(&store).await;
    let second = support::running_task(&store).await;
    set_task_created_at(&store, &first, "2026-07-27T01:00:00Z").await;
    set_task_created_at(&store, &second, "2026-07-27T02:00:00Z").await;
    let sql = format!(
        "CREATE TRIGGER recovery_later_running_tamper \
         AFTER UPDATE OF payload_json ON task_events \
         WHEN NEW.kind = 'task.interrupted' AND NEW.task_id = '{}' \
         BEGIN \
             UPDATE tasks SET prompt = 'self-consistent-trigger-tamper' WHERE id = '{}'; \
             UPDATE task_events \
             SET payload_json = json_set(\
                 payload_json, '$.task.prompt', 'self-consistent-trigger-tamper'\
             ) \
             WHERE task_id = '{}' AND kind IN ('task.queued', 'task.started'); \
         END",
        first.id, second.id, second.id
    );
    sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
        .execute(store.pool())
        .await
        .unwrap();
    let before = support::durable_task_event_snapshot(&store).await;

    assert!(matches!(
        store.recover_after_restart().await.unwrap_err(),
        StoreError::InvariantViolation(_)
    ));
    assert_eq!(support::durable_task_event_snapshot(&store).await, before);
}

#[tokio::test]
async fn cold_recovery_rejects_a_cross_item_trigger_that_terminalizes_the_next_intent() {
    let store = support::seeded_store().await;
    let first = support::running_task(&store).await;
    let second = support::running_task(&store).await;
    insert_stop_intent(
        &store,
        &first,
        StopIntentKind::UserCancelled,
        "2026-07-27T01:00:00Z",
    )
    .await;
    insert_stop_intent(
        &store,
        &second,
        StopIntentKind::UserCancelled,
        "2026-07-27T02:00:00Z",
    )
    .await;

    let terminal = match store
        .finalize_stopped_task(finalize_request(&second, StopIntentKind::UserCancelled))
        .await
        .unwrap()
    {
        FinalizeStoppedTaskOutcome::Applied(receipt) => receipt,
        other => panic!("fixture final stop must apply, got {other:?}"),
    };
    let terminal_guard_sql: String = sqlx::query_scalar(
        "SELECT sql FROM sqlite_master \
         WHERE type = 'trigger' AND name = 'tasks_stop_intent_terminal_on_update'",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    sqlx::raw_sql("DROP TRIGGER tasks_stop_intent_terminal_on_update")
        .execute(store.pool())
        .await
        .unwrap();
    let started_event_id: i64 = sqlx::query_scalar(
        "SELECT id FROM task_events \
         WHERE task_id = ? AND kind = 'task.started'",
    )
    .bind(second.id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE tasks \
         SET status = 'running', finished_at = NULL, failure_json = NULL, last_event_id = ? \
         WHERE id = ?",
    )
    .bind(started_event_id)
    .bind(second.id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query("DELETE FROM task_events WHERE id = ?")
        .bind(terminal.terminal_event_id.get())
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(terminal_guard_sql))
        .execute(store.pool())
        .await
        .unwrap();

    let side_effect_event_id = EventId::new(terminal.terminal_event_id.get() + 2).unwrap();
    let mut side_effect_task = terminal.task;
    side_effect_task.last_event_id = side_effect_event_id;
    let side_effect_payload =
        serde_json::to_string(&serde_json::json!({ "task": &side_effect_task })).unwrap();
    let side_effect_finished_at = side_effect_task.finished_at.unwrap().to_string();
    let sql = format!(
        "CREATE TRIGGER recovery_cross_item_terminalization \
         AFTER UPDATE OF payload_json ON task_events \
         WHEN NEW.kind = 'task.cancelled' AND NEW.task_id = '{}' \
         BEGIN \
             UPDATE tasks \
             SET status = 'cancelled', finished_at = '{}', failure_json = NULL, \
                 last_event_id = {} \
             WHERE id = '{}'; \
             INSERT INTO task_events \
                 (id, schema_version, task_id, kind, payload_json, created_at) \
             VALUES ({}, 1, '{}', 'task.cancelled', '{}', '{}'); \
         END",
        first.id,
        side_effect_finished_at,
        side_effect_event_id.get(),
        second.id,
        side_effect_event_id.get(),
        second.id,
        side_effect_payload.replace('\'', "''"),
        side_effect_finished_at,
    );
    sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
        .execute(store.pool())
        .await
        .unwrap();
    let before = support::durable_task_event_snapshot(&store).await;

    assert!(matches!(
        store.recover_after_restart().await.unwrap_err(),
        StoreError::InvariantViolation(_)
    ));
    assert_eq!(support::durable_task_event_snapshot(&store).await, before);
    assert_eq!(
        store
            .task_detail(first.id)
            .await
            .unwrap()
            .unwrap()
            .task
            .status,
        TaskStatus::Running
    );
    assert_eq!(
        store
            .task_detail(second.id)
            .await
            .unwrap()
            .unwrap()
            .task
            .status,
        TaskStatus::Running
    );
}

#[tokio::test]
async fn interrupt_remaining_after_stops_requires_running_intents_to_be_finalized_first() {
    let store = support::seeded_store().await;
    let stopped = support::running_task(&store).await;
    let running = support::running_task(&store).await;
    let queued = support::queued_task(&store).await;
    let intent = StopIntentKind::UserCancelled;
    insert_stop_intent(&store, &stopped, intent, "2026-07-27T01:00:00Z").await;
    let before = support::durable_task_event_snapshot(&store).await;
    let shutdown_failure = support::failure("APP_SHUTDOWN");

    assert!(matches!(
        store
            .interrupt_remaining_after_stops(shutdown_failure.clone())
            .await
            .unwrap_err(),
        StoreError::InvariantViolation(_)
    ));
    assert_eq!(support::durable_task_event_snapshot(&store).await, before);

    let stopped_receipt = match store
        .finalize_stopped_task(finalize_request(&stopped, intent))
        .await
        .unwrap()
    {
        FinalizeStoppedTaskOutcome::Applied(receipt) => receipt,
        other => panic!("fixture final stop must apply, got {other:?}"),
    };
    let receipt = store
        .interrupt_remaining_after_stops(shutdown_failure.clone())
        .await
        .unwrap();
    assert_eq!(receipt.finalized_stop_count, 0);
    assert_eq!(receipt.interrupted_count, 2);
    assert_eq!(
        receipt.high_watermark,
        store.latest_event_id().await.unwrap()
    );
    assert_eq!(
        receipt.membership_high_watermark,
        membership_high_watermark(&store).await
    );

    let current_stopped = store.task_detail(stopped.id).await.unwrap().unwrap().task;
    assert_eq!(current_stopped, stopped_receipt.task);
    let current_running = store.task_detail(running.id).await.unwrap().unwrap().task;
    assert_eq!(current_running.status, TaskStatus::Interrupted);
    assert_eq!(current_running.failure, Some(shutdown_failure.clone()));
    let current_queued = store.task_detail(queued.id).await.unwrap().unwrap().task;
    assert_eq!(current_queued.status, TaskStatus::Interrupted);
    assert_eq!(current_queued.started_at, None);
    assert_eq!(current_queued.failure, Some(shutdown_failure));
}

#[tokio::test]
async fn shutdown_queued_interrupt_faults_roll_back_all_nonterminal_tasks() {
    for fault in ["task-update", "event-insert"] {
        let store = support::seeded_store().await;
        let running = support::running_task(&store).await;
        let queued = support::queued_task(&store).await;
        set_task_created_at(&store, &running, "2026-07-27T01:00:00Z").await;
        set_task_created_at(&store, &queued, "2026-07-27T02:00:00Z").await;
        let clause = match fault {
            "task-update" => format!(
                "BEFORE UPDATE OF status ON tasks \
                 WHEN OLD.id = '{}' AND NEW.status = 'interrupted'",
                queued.id
            ),
            "event-insert" => format!(
                "BEFORE INSERT ON task_events \
                 WHEN NEW.task_id = '{}' AND NEW.kind = 'task.interrupted'",
                queued.id
            ),
            _ => unreachable!(),
        };
        install_abort_trigger(&store, &clause, fault).await;
        let before = support::durable_task_event_snapshot(&store).await;

        store
            .interrupt_remaining_after_stops(support::failure("APP_SHUTDOWN"))
            .await
            .expect_err("the queued shutdown fault must abort the transaction");

        assert_eq!(
            support::durable_task_event_snapshot(&store).await,
            before,
            "{fault}"
        );
        assert_eq!(
            store
                .task_detail(running.id)
                .await
                .unwrap()
                .unwrap()
                .task
                .status,
            TaskStatus::Running
        );
        assert_eq!(
            store
                .task_detail(queued.id)
                .await
                .unwrap()
                .unwrap()
                .task
                .status,
            TaskStatus::Queued
        );
    }
}

#[tokio::test]
async fn legacy_recover_incomplete_wrapper_is_running_only_and_intent_guarded() {
    let store = support::seeded_store().await;
    let stopped = support::running_task(&store).await;
    let running = support::running_task(&store).await;
    let queued = support::queued_task(&store).await;
    let intent = StopIntentKind::DiskPressureCritical;
    insert_stop_intent(&store, &stopped, intent, "2026-07-27T01:00:00Z").await;
    let before = support::durable_task_event_snapshot(&store).await;
    let wrapper_now = UtcTimestamp::parse_rfc3339("2026-07-28T01:02:03Z").unwrap();
    let wrapper_failure = support::failure("STORE_DEGRADED_RECOVERY");

    assert!(matches!(
        store
            .recover_incomplete(wrapper_now, wrapper_failure.clone())
            .await
            .unwrap_err(),
        StoreError::InvariantViolation(_)
    ));
    assert_eq!(support::durable_task_event_snapshot(&store).await, before);

    let stopped_receipt = match store
        .finalize_stopped_task(finalize_request(&stopped, intent))
        .await
        .unwrap()
    {
        FinalizeStoppedTaskOutcome::Applied(receipt) => receipt,
        other => panic!("fixture final stop must apply, got {other:?}"),
    };
    let queued_before = store.task_detail(queued.id).await.unwrap().unwrap();
    let outcome = store
        .recover_incomplete(wrapper_now, wrapper_failure.clone())
        .await
        .unwrap();
    assert_eq!(outcome.interrupted_count, 1);
    assert_eq!(
        outcome.high_watermark,
        store.latest_event_id().await.unwrap()
    );
    assert_eq!(
        store.task_detail(stopped.id).await.unwrap().unwrap().task,
        stopped_receipt.task
    );
    let interrupted = store.task_detail(running.id).await.unwrap().unwrap().task;
    assert_eq!(interrupted.status, TaskStatus::Interrupted);
    assert_eq!(interrupted.finished_at, Some(wrapper_now));
    assert_eq!(interrupted.failure, Some(wrapper_failure));
    assert_detail_unchanged_except_cursor(
        &store.task_detail(queued.id).await.unwrap().unwrap(),
        &queued_before,
    );
}

#[tokio::test]
async fn cold_recovery_preserves_reviews_readiness_and_existing_terminal_aggregates() {
    let store = support::seeded_store().await;

    let intermediate = running_project3_task(&store).await;
    store
        .record_review(
            intermediate.id,
            intermediate.repository_id,
            intermediate.attempt,
            changes_requested(1),
        )
        .await
        .unwrap();
    let intermediate_before = store.task_detail(intermediate.id).await.unwrap().unwrap();

    let reviewed_running = running_project3_task(&store).await;
    let reviewed = match store
        .finalize_reviewed_task(
            reviewed_running.id,
            reviewed_running.repository_id,
            reviewed_running.attempt,
            approved(1),
        )
        .await
        .unwrap()
    {
        FinalizeReviewedTaskOutcome::Applied { task, .. } => task,
        other => panic!("fixture reviewed finalization must apply, got {other:?}"),
    };
    let reviewed_before = store.task_detail(reviewed.id).await.unwrap().unwrap();
    assert_eq!(
        reviewed_before.task.delivery_readiness,
        DeliveryReadiness::ReviewApproved
    );

    let historical = support::terminal_task(&store, TaskStatus::Completed).await;
    let historical_before = store.task_detail(historical.id).await.unwrap().unwrap();
    let queued = support::queued_task(&store).await;
    let queued_before = store.task_detail(queued.id).await.unwrap().unwrap();
    let intermediate_review_count = review_count(&store, intermediate.id).await;
    let reviewed_review_count = review_count(&store, reviewed.id).await;
    let reviewed_delivery_count = delivery_count(&store, reviewed.id).await;

    let receipt = store.recover_after_restart().await.unwrap();
    assert_eq!(receipt.finalized_stop_count, 0);
    assert_eq!(receipt.interrupted_count, 1);

    let intermediate_after = store.task_detail(intermediate.id).await.unwrap().unwrap();
    assert_eq!(intermediate_after.task.status, TaskStatus::Interrupted);
    assert_eq!(
        intermediate_after
            .task
            .failure
            .as_ref()
            .map(|failure| failure.code.as_str()),
        Some(APP_RESTARTED)
    );
    assert_eq!(
        intermediate_after.task.delivery_readiness,
        DeliveryReadiness::Unreviewed
    );
    assert_eq!(intermediate_after.plan, intermediate_before.plan);
    assert_eq!(intermediate_after.activity, intermediate_before.activity);
    assert_eq!(intermediate_after.diff, intermediate_before.diff);
    assert_eq!(intermediate_after.tests, intermediate_before.tests);
    assert_eq!(intermediate_after.reviews, intermediate_before.reviews);
    assert_eq!(
        review_count(&store, intermediate.id).await,
        intermediate_review_count
    );
    assert_eq!(delivery_count(&store, intermediate.id).await, 0);

    assert_detail_unchanged_except_cursor(
        &store.task_detail(reviewed.id).await.unwrap().unwrap(),
        &reviewed_before,
    );
    assert_eq!(
        review_count(&store, reviewed.id).await,
        reviewed_review_count
    );
    assert_eq!(
        delivery_count(&store, reviewed.id).await,
        reviewed_delivery_count
    );
    assert_detail_unchanged_except_cursor(
        &store.task_detail(historical.id).await.unwrap().unwrap(),
        &historical_before,
    );
    assert_detail_unchanged_except_cursor(
        &store.task_detail(queued.id).await.unwrap().unwrap(),
        &queued_before,
    );
}

#[derive(Debug, Clone, Copy)]
enum PreflightCorruption {
    RunningIntent,
    TerminalIntent,
}

#[derive(Debug, Clone, Copy)]
enum RecoveryFault {
    StopTaskUpdate,
    StopEventInsert,
    StopCursorUpdate,
    StopPayloadUpdate,
    StopAfterPayloadTamper,
    StopAfterPayloadDelete,
    InterruptTaskUpdate,
    InterruptEventInsert,
    InterruptCursorUpdate,
    InterruptPayloadUpdate,
    InterruptAfterPayloadTamper,
    InterruptAfterPayloadDelete,
    DeferredCommit,
}

async fn install_recovery_fault(store: &Store, fault: RecoveryFault) {
    match fault {
        RecoveryFault::StopTaskUpdate => {
            install_abort_trigger(
                store,
                "BEFORE UPDATE OF status ON tasks \
                 WHEN NEW.status = 'cancelled'",
                "stop-task-update",
            )
            .await;
        }
        RecoveryFault::StopEventInsert => {
            install_abort_trigger(
                store,
                "BEFORE INSERT ON task_events \
                 WHEN NEW.kind = 'task.cancelled'",
                "stop-event-insert",
            )
            .await;
        }
        RecoveryFault::StopCursorUpdate => {
            install_abort_trigger(
                store,
                "BEFORE UPDATE OF last_event_id ON tasks \
                 WHEN NEW.status = 'cancelled' AND OLD.last_event_id != NEW.last_event_id",
                "stop-cursor-update",
            )
            .await;
        }
        RecoveryFault::StopPayloadUpdate => {
            install_abort_trigger(
                store,
                "BEFORE UPDATE OF payload_json ON task_events \
                 WHEN NEW.kind = 'task.cancelled' AND OLD.payload_json = '{}'",
                "stop-payload-update",
            )
            .await;
        }
        RecoveryFault::InterruptTaskUpdate => {
            install_abort_trigger(
                store,
                "BEFORE UPDATE OF status ON tasks \
                 WHEN NEW.status = 'interrupted'",
                "interrupt-task-update",
            )
            .await;
        }
        RecoveryFault::InterruptEventInsert => {
            install_abort_trigger(
                store,
                "BEFORE INSERT ON task_events \
                 WHEN NEW.kind = 'task.interrupted'",
                "interrupt-event-insert",
            )
            .await;
        }
        RecoveryFault::InterruptCursorUpdate => {
            install_abort_trigger(
                store,
                "BEFORE UPDATE OF last_event_id ON tasks \
                 WHEN NEW.status = 'interrupted' AND OLD.last_event_id != NEW.last_event_id",
                "interrupt-cursor-update",
            )
            .await;
        }
        RecoveryFault::InterruptPayloadUpdate => {
            install_abort_trigger(
                store,
                "BEFORE UPDATE OF payload_json ON task_events \
                 WHEN NEW.kind = 'task.interrupted' AND OLD.payload_json = '{}'",
                "interrupt-payload-update",
            )
            .await;
        }
        RecoveryFault::StopAfterPayloadTamper | RecoveryFault::InterruptAfterPayloadTamper => {
            let kind = match fault {
                RecoveryFault::StopAfterPayloadTamper => "task.cancelled",
                RecoveryFault::InterruptAfterPayloadTamper => "task.interrupted",
                _ => unreachable!(),
            };
            let sql = format!(
                "CREATE TRIGGER recovery_fault \
                 AFTER UPDATE OF payload_json ON task_events \
                 WHEN NEW.kind = '{kind}' AND OLD.payload_json = '{{}}' \
                 BEGIN UPDATE task_events SET created_at = '2026-01-01T00:00:00Z' \
                       WHERE id = NEW.id; END"
            );
            sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
                .execute(store.pool())
                .await
                .unwrap();
        }
        RecoveryFault::StopAfterPayloadDelete | RecoveryFault::InterruptAfterPayloadDelete => {
            let kind = match fault {
                RecoveryFault::StopAfterPayloadDelete => "task.cancelled",
                RecoveryFault::InterruptAfterPayloadDelete => "task.interrupted",
                _ => unreachable!(),
            };
            let sql = format!(
                "CREATE TRIGGER recovery_fault \
                 AFTER UPDATE OF payload_json ON task_events \
                 WHEN NEW.kind = '{kind}' AND OLD.payload_json = '{{}}' \
                 BEGIN DELETE FROM task_events WHERE id = NEW.id; END"
            );
            sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
                .execute(store.pool())
                .await
                .unwrap();
        }
        RecoveryFault::DeferredCommit => {
            sqlx::raw_sql(
                "CREATE TABLE recovery_fault_parent (id INTEGER PRIMARY KEY); \
                 CREATE TABLE recovery_fault_child (\
                     parent_id INTEGER REFERENCES recovery_fault_parent(id) \
                         DEFERRABLE INITIALLY DEFERRED\
                 ); \
                 CREATE TRIGGER recovery_fault \
                 AFTER UPDATE OF payload_json ON task_events \
                 WHEN NEW.kind = 'task.interrupted' AND OLD.payload_json = '{}' \
                 BEGIN INSERT INTO recovery_fault_child (parent_id) VALUES (1); END;",
            )
            .execute(store.pool())
            .await
            .unwrap();
        }
    }
}

async fn install_abort_trigger(store: &Store, clause: &str, message: &str) {
    let sql = format!(
        "CREATE TRIGGER recovery_fault {clause} \
         BEGIN SELECT RAISE(ABORT, '{message}'); END"
    );
    sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
        .execute(store.pool())
        .await
        .unwrap();
}

async fn insert_stop_intent(store: &Store, task: &Task, kind: StopIntentKind, requested_at: &str) {
    let requested_at = UtcTimestamp::parse_rfc3339(requested_at)
        .unwrap()
        .to_string();
    sqlx::query(
        "INSERT INTO task_stop_intents \
             (task_id, repository_id, attempt, kind, requested_at) \
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(task.id.to_string())
    .bind(task.repository_id.to_string())
    .bind(i64::from(task.attempt))
    .bind(kind.as_str())
    .bind(requested_at)
    .execute(store.pool())
    .await
    .unwrap();
}

fn finalize_request(task: &Task, kind: StopIntentKind) -> FinalizeStoppedTaskRequest {
    FinalizeStoppedTaskRequest {
        task_id: task.id,
        expected_repository_id: task.repository_id,
        expected_attempt: task.attempt,
        expected_intent: kind,
    }
}

async fn set_task_created_at(store: &Store, task: &Task, created_at: &str) {
    let created_at = UtcTimestamp::parse_rfc3339(created_at).unwrap().to_string();
    sqlx::query("UPDATE tasks SET created_at = ? WHERE id = ?")
        .bind(&created_at)
        .bind(task.id.to_string())
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE task_events \
         SET payload_json = json_set(payload_json, '$.task.created_at', ?) \
         WHERE task_id = ? \
           AND kind IN ('task.queued', 'task.started')",
    )
    .bind(&created_at)
    .bind(task.id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_events SET created_at = ? \
         WHERE task_id = ? AND kind = 'task.queued'",
    )
    .bind(&created_at)
    .bind(task.id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
}

fn assert_terminal_event_matches_task(event: &TaskEvent, expected: &Task) {
    let embedded = match &event.payload {
        TaskEventPayload::TaskFailed { task }
        | TaskEventPayload::TaskCancelled { task }
        | TaskEventPayload::TaskInterrupted { task } => task,
        other => panic!("recovery emitted a non-terminal event: {other:?}"),
    };
    assert_eq!(embedded, expected);
    assert_eq!(event.id, expected.last_event_id);
    assert_eq!(Some(event.created_at), expected.finished_at);
}

fn assert_detail_unchanged_except_cursor(
    actual: &coding_agent_store::TaskDetail,
    expected: &coding_agent_store::TaskDetail,
) {
    assert_eq!(actual.task, expected.task);
    assert_eq!(actual.plan, expected.plan);
    assert_eq!(actual.activity, expected.activity);
    assert_eq!(actual.diff, expected.diff);
    assert_eq!(actual.tests, expected.tests);
    assert_eq!(actual.reviews, expected.reviews);
    assert_eq!(actual.timeline, expected.timeline);
}

async fn membership_high_watermark(store: &Store) -> EventCursor {
    let raw: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(id), 0) FROM task_events \
         WHERE kind IN (\
             'task.queued', 'task.started', 'task.completed', \
             'task.failed', 'task.cancelled', 'task.interrupted'\
         )",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    EventCursor::new(raw).unwrap()
}

async fn intent_count(store: &Store) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM task_stop_intents")
        .fetch_one(store.pool())
        .await
        .unwrap()
}

async fn review_count(store: &Store, task_id: TaskId) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM task_review_evidence WHERE task_id = ?")
        .bind(task_id.to_string())
        .fetch_one(store.pool())
        .await
        .unwrap()
}

async fn delivery_count(store: &Store, task_id: TaskId) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM task_delivery_state WHERE task_id = ?")
        .bind(task_id.to_string())
        .fetch_one(store.pool())
        .await
        .unwrap()
}

async fn running_project3_task(store: &Store) -> Task {
    let queued = support::queued_task(store).await;
    let task = match store
        .transition_with_event(queued.id, TaskStatus::Queued, TaskTransition::Running)
        .await
        .unwrap()
    {
        TransitionOutcome::Applied { task, .. } => task,
        TransitionOutcome::Conflict { .. } => panic!("fixture transition must apply"),
    };
    let plan = PlanSnapshot::try_structured(
        1,
        "Implement and review the approved recovery plan",
        vec![
            PlanItem::try_structured(
                "step-1",
                "Implement",
                "Implement the requested recovery behavior",
                vec!["All required checks pass".to_owned()],
                PlanItemStatus::Completed,
            )
            .unwrap(),
        ],
        vec![required_check()],
    )
    .unwrap();
    store
        .append_running_event(task.id, TaskEventPayload::PlanUpdated { plan })
        .await
        .unwrap();
    store.task_detail(task.id).await.unwrap().unwrap().task
}

fn required_check() -> RequiredCheck {
    RequiredCheck::try_cargo_test(
        "recovery-cargo-test",
        Some("coding-agent-store".to_owned()),
        None,
    )
    .unwrap()
}

fn digest(round: u8) -> WorkspaceDigest {
    let digit = char::from(b'a' + round - 1);
    WorkspaceDigest::try_new(digit.to_string().repeat(64)).unwrap()
}

fn passed_check(round: u8, check: &RequiredCheck, digest: &WorkspaceDigest) -> CheckEvidence {
    CheckEvidence::try_for_check(
        check,
        CheckActor::Executor,
        u32::from(round),
        u64::from(round),
        digest.clone(),
        CheckEvidenceStatus::Passed,
        10,
        "cargo test passed",
        false,
    )
    .unwrap()
}

fn changes_requested(round: u8) -> NewReviewEvidence {
    let digest = digest(round);
    let check = required_check();
    NewReviewEvidence::try_new(
        round,
        ReviewDecisionSource::Reviewer,
        u64::from(round),
        digest.clone(),
        ReviewVerdict::ChangesRequested,
        format!("round {round} needs changes"),
        vec![
            ReviewFinding::try_for_review(
                round,
                1,
                FindingSeverity::Blocking,
                "A blocking issue remains",
                Some("src/lib.rs".to_owned()),
                Some(1),
            )
            .unwrap(),
        ],
        Vec::new(),
        vec![check.clone()],
        vec![passed_check(round, &check, &digest)],
        None,
    )
    .unwrap()
}

fn approved(round: u8) -> NewReviewEvidence {
    let digest = digest(round);
    let check = required_check();
    NewReviewEvidence::try_new(
        round,
        ReviewDecisionSource::Reviewer,
        u64::from(round),
        digest.clone(),
        ReviewVerdict::Approved,
        format!("round {round} approved"),
        Vec::new(),
        Vec::new(),
        vec![check.clone()],
        vec![passed_check(round, &check, &digest)],
        Some(
            ReviewCoverageEvidence::try_new(u64::from(round), digest, "f".repeat(64), vec![0], 1)
                .unwrap(),
        ),
    )
    .unwrap()
}
