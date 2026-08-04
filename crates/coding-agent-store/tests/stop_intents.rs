mod support;

use std::sync::Arc;

use coding_agent_domain::{Task, TaskId, TaskStatus, UtcTimestamp};
use coding_agent_store::{
    FinalizeStoppedTaskOutcome, FinalizeStoppedTaskRequest, PersistStopIntentOutcome,
    StopIntentKind, StopIntentReceipt, StopIntentRequest, Store, StoreError, TaskTransition,
    TransitionOutcome,
};
use tokio::sync::Barrier;

#[tokio::test]
async fn single_intent_is_query_first_and_preserves_the_original_receipt() {
    let store = support::seeded_store().await;
    let running = support::running_task(&store).await;
    let request = stop_request(&running, StopIntentKind::UserCancelled);
    let before_events = event_count(&store, running.id).await;

    let applied = applied_intent(store.persist_stop_intent(request).await.unwrap());
    assert_eq!(applied.task_id, running.id);
    assert_eq!(applied.repository_id, running.repository_id);
    assert_eq!(applied.attempt, running.attempt);
    assert_eq!(applied.kind, StopIntentKind::UserCancelled);
    assert_eq!(
        UtcTimestamp::parse_rfc3339(&applied.requested_at.to_string()).unwrap(),
        applied.requested_at
    );
    assert_eq!(event_count(&store, running.id).await, before_events);

    let existing = existing_intent(store.persist_stop_intent(request).await.unwrap());
    assert_eq!(existing, applied);
    assert!(matches!(
        store
            .persist_stop_intent(StopIntentRequest {
                kind: StopIntentKind::DiskPressureCritical,
                ..request
            })
            .await
            .unwrap(),
        PersistStopIntentOutcome::IntentConflict { existing } if existing == applied
    ));
    assert!(matches!(
        store
            .persist_stop_intent(StopIntentRequest {
                expected_attempt: running.attempt + 1,
                ..request
            })
            .await
            .unwrap_err(),
        StoreError::InvariantViolation(_)
    ));
    assert_eq!(load_intents(&store).await, vec![applied]);
    assert_eq!(event_count(&store, running.id).await, before_events);
}

#[tokio::test]
async fn terminal_wins_without_creating_an_intent_and_missing_tasks_stay_errors() {
    let store = support::seeded_store().await;
    let running = support::running_task(&store).await;
    let terminal = applied_task(
        store
            .transition_with_event(
                running.id,
                TaskStatus::Running,
                TaskTransition::Failed(support::failure("RUNNER_WON")),
            )
            .await
            .unwrap(),
    );

    assert!(matches!(
        store
            .persist_stop_intent(stop_request(&running, StopIntentKind::UserCancelled))
            .await
            .unwrap(),
        PersistStopIntentOutcome::TerminalWon { current } if current == terminal
    ));
    assert!(load_intents(&store).await.is_empty());

    let missing = StopIntentRequest {
        task_id: TaskId::new(),
        expected_repository_id: running.repository_id,
        expected_attempt: 0,
        kind: StopIntentKind::UserCancelled,
    };
    assert!(matches!(
        store.persist_stop_intent(missing).await.unwrap_err(),
        StoreError::TaskNotFound
    ));
}

#[tokio::test]
async fn raw_intent_corruption_fails_closed_without_repair() {
    for corruption in [
        RawIntentCorruption::TaskIdUppercase,
        RawIntentCorruption::TaskIdSimple,
        RawIntentCorruption::TaskIdBraced,
        RawIntentCorruption::TaskIdUrn,
        RawIntentCorruption::CanonicalPlusSimpleTaskId,
        RawIntentCorruption::RepositoryIdUppercase,
        RawIntentCorruption::KindUnknown,
        RawIntentCorruption::RequestedAtNonCanonical,
    ] {
        let store = support::seeded_store().await;
        let running = support::running_task(&store).await;
        let request = stop_request(&running, StopIntentKind::UserCancelled);
        applied_intent(store.persist_stop_intent(request).await.unwrap());
        assert!(
            sqlx::query(
                "UPDATE task_stop_intents \
                 SET requested_at = 'not-a-timestamp' WHERE task_id = ?",
            )
            .bind(running.id.to_string())
            .execute(store.pool())
            .await
            .is_err(),
            "immutable trigger must reject ordinary updates"
        );

        install_raw_intent_corruption(&store, &running, corruption).await;
        let before = support::durable_task_event_snapshot(&store).await;
        assert!(
            matches!(
                store.persist_stop_intent(request).await.unwrap_err(),
                StoreError::InvariantViolation(_)
            ),
            "{corruption:?}"
        );
        assert_eq!(
            support::durable_task_event_snapshot(&store).await,
            before,
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
async fn strict_stop_intent_storage_rejects_non_declared_storage_classes() {
    for column in ["task_id", "attempt", "requested_at"] {
        let store = support::seeded_store().await;
        let running = support::running_task(&store).await;
        let request = stop_request(&running, StopIntentKind::UserCancelled);
        let receipt = applied_intent(store.persist_stop_intent(request).await.unwrap());
        sqlx::query("DROP TRIGGER task_stop_intents_no_update")
            .execute(store.pool())
            .await
            .unwrap();
        let before = support::durable_task_event_snapshot(&store).await;
        let sql = format!(
            "UPDATE task_stop_intents SET {column} = CAST({column} AS BLOB) WHERE task_id = ?"
        );
        assert!(
            sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(running.id.to_string())
                .execute(store.pool())
                .await
                .is_err(),
            "{column}"
        );
        assert_eq!(
            support::durable_task_event_snapshot(&store).await,
            before,
            "{column}"
        );
        assert_eq!(
            existing_intent(store.persist_stop_intent(request).await.unwrap()),
            receipt
        );
    }
}

#[tokio::test]
async fn urgent_batch_is_canonical_bounded_and_business_conflicts_do_not_abort_peers() {
    let store = support::seeded_store().await;
    let existing_task = support::running_task(&store).await;
    let conflicting_task = support::running_task(&store).await;
    let terminal_task = support::running_task(&store).await;
    let new_task = support::running_task(&store).await;

    applied_intent(
        store
            .persist_stop_intent(stop_request(&existing_task, StopIntentKind::UserCancelled))
            .await
            .unwrap(),
    );
    applied_intent(
        store
            .persist_stop_intent(stop_request(
                &conflicting_task,
                StopIntentKind::DiskPressureCritical,
            ))
            .await
            .unwrap(),
    );
    let terminal = applied_task(
        store
            .transition_with_event(
                terminal_task.id,
                TaskStatus::Running,
                TaskTransition::Failed(support::failure("TERMINAL_WON")),
            )
            .await
            .unwrap(),
    );

    let batch = store
        .persist_stop_intent_batch(vec![
            stop_request(&new_task, StopIntentKind::DiskPressureCritical),
            stop_request(&terminal_task, StopIntentKind::UserCancelled),
            stop_request(&existing_task, StopIntentKind::UserCancelled),
            stop_request(&conflicting_task, StopIntentKind::UserCancelled),
        ])
        .await
        .unwrap();
    let outcomes = batch.items;
    assert_eq!(outcomes.len(), 4);
    assert!(
        outcomes
            .windows(2)
            .all(|pair| pair[0].request.task_id.to_string() < pair[1].request.task_id.to_string())
    );
    for item in outcomes {
        if item.request.task_id == existing_task.id {
            assert!(matches!(
                item.outcome,
                PersistStopIntentOutcome::Existing(_)
            ));
        } else if item.request.task_id == conflicting_task.id {
            assert!(matches!(
                item.outcome,
                PersistStopIntentOutcome::IntentConflict { existing }
                    if existing.task_id == conflicting_task.id
                        && existing.kind == StopIntentKind::DiskPressureCritical
            ));
        } else if item.request.task_id == terminal_task.id {
            assert!(matches!(
                item.outcome,
                PersistStopIntentOutcome::TerminalWon { current } if current == terminal
            ));
        } else if item.request.task_id == new_task.id {
            assert!(matches!(item.outcome, PersistStopIntentOutcome::Applied(_)));
        } else {
            panic!("batch returned an unknown task");
        }
    }

    let fifth = support::running_task(&store).await;
    let before = support::durable_task_event_snapshot(&store).await;
    let requests = [
        existing_task,
        conflicting_task,
        terminal_task,
        new_task,
        fifth,
    ]
    .iter()
    .map(|task| stop_request(task, StopIntentKind::UserCancelled))
    .collect();
    assert!(matches!(
        store.persist_stop_intent_batch(requests).await.unwrap_err(),
        StoreError::InvariantViolation(_)
    ));
    assert_eq!(support::durable_task_event_snapshot(&store).await, before);
}

#[tokio::test]
async fn a_database_fault_rolls_back_the_entire_urgent_batch() {
    let store = support::seeded_store().await;
    let first = support::running_task(&store).await;
    let second = support::running_task(&store).await;
    let mut requests = vec![
        stop_request(&first, StopIntentKind::UserCancelled),
        stop_request(&second, StopIntentKind::DiskPressureCritical),
    ];
    requests.sort_by_key(|request| request.task_id.to_string());
    let fail_task_id = requests[1].task_id;
    let trigger_sql = format!(
        "CREATE TRIGGER stop_batch_fault BEFORE INSERT ON task_stop_intents \
         WHEN NEW.task_id = '{}' \
         BEGIN SELECT RAISE(ABORT, 'stop-batch-fault'); END",
        fail_task_id
    );
    sqlx::raw_sql(sqlx::AssertSqlSafe(trigger_sql))
        .execute(store.pool())
        .await
        .unwrap();
    let before = support::durable_task_event_snapshot(&store).await;

    store
        .persist_stop_intent_batch(requests)
        .await
        .expect_err("second item must abort the whole transaction");

    assert_eq!(support::durable_task_event_snapshot(&store).await, before);
    assert!(load_intents(&store).await.is_empty());
}

#[tokio::test]
async fn batch_post_classification_and_deferred_commit_failures_roll_back_every_item() {
    for fault in [BatchFault::TamperEarlierItem, BatchFault::DeferredCommit] {
        let store = support::seeded_store().await;
        let first = support::running_task(&store).await;
        let second = support::running_task(&store).await;
        let mut requests = vec![
            stop_request(&first, StopIntentKind::UserCancelled),
            stop_request(&second, StopIntentKind::DiskPressureCritical),
        ];
        requests.sort_by_key(|request| request.task_id.as_uuid().as_u128());
        install_batch_fault(&store, &requests, fault).await;
        let before = support::durable_task_event_snapshot(&store).await;

        store
            .persist_stop_intent_batch(requests)
            .await
            .expect_err("batch fault must roll back the transaction");

        assert_eq!(
            support::durable_task_event_snapshot(&store).await,
            before,
            "{fault:?}"
        );
        assert!(load_intents(&store).await.is_empty());
    }
}

#[tokio::test]
async fn invalid_batch_shape_or_identity_is_rejected_atomically() {
    let store = support::seeded_store().await;
    let first = support::running_task(&store).await;
    let second = support::running_task(&store).await;

    let before = support::durable_task_event_snapshot(&store).await;
    assert!(matches!(
        store
            .persist_stop_intent_batch(Vec::new())
            .await
            .unwrap_err(),
        StoreError::InvariantViolation(_)
    ));
    assert_eq!(support::durable_task_event_snapshot(&store).await, before);

    let duplicate = stop_request(&first, StopIntentKind::UserCancelled);
    assert!(matches!(
        store
            .persist_stop_intent_batch(vec![duplicate, duplicate])
            .await
            .unwrap_err(),
        StoreError::InvariantViolation(_)
    ));
    assert_eq!(support::durable_task_event_snapshot(&store).await, before);

    assert!(matches!(
        store
            .persist_stop_intent_batch(vec![
                stop_request(&first, StopIntentKind::UserCancelled),
                StopIntentRequest {
                    expected_attempt: second.attempt + 1,
                    ..stop_request(&second, StopIntentKind::DiskPressureCritical)
                },
            ])
            .await
            .unwrap_err(),
        StoreError::InvariantViolation(_)
    ));
    assert_eq!(support::durable_task_event_snapshot(&store).await, before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn concurrent_intent_writers_preserve_one_immutable_winner() {
    const CALLS: usize = 6;

    let fixture = support::store_fixture().await;
    support::register_repository(&fixture.store, "intent-race").await;
    let running = support::running_task(&fixture.store).await;
    let first = Store::open(&fixture.database_path).await.unwrap();
    let second = Store::open(&fixture.database_path).await.unwrap();
    let barrier = Arc::new(Barrier::new(CALLS + 1));
    let mut calls = Vec::new();
    for index in 0..CALLS {
        let store = if index % 2 == 0 {
            first.clone()
        } else {
            second.clone()
        };
        let barrier = barrier.clone();
        let request = stop_request(&running, StopIntentKind::UserCancelled);
        calls.push(tokio::spawn(async move {
            barrier.wait().await;
            store.persist_stop_intent(request).await.unwrap()
        }));
    }
    barrier.wait().await;
    let mut outcomes = Vec::new();
    for call in calls {
        outcomes.push(call.await.unwrap());
    }
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, PersistStopIntentOutcome::Applied(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, PersistStopIntentOutcome::Existing(_)))
            .count(),
        CALLS - 1
    );
    let receipts: Vec<_> = outcomes
        .into_iter()
        .map(|outcome| match outcome {
            PersistStopIntentOutcome::Applied(receipt)
            | PersistStopIntentOutcome::Existing(receipt) => receipt,
            other => panic!("same-kind race returned {other:?}"),
        })
        .collect();
    assert!(receipts.iter().all(|receipt| receipt == &receipts[0]));
    assert_eq!(load_intents(&fixture.store).await, vec![receipts[0]]);

    let other = support::running_task(&fixture.store).await;
    let barrier = Arc::new(Barrier::new(3));
    let mut different = Vec::new();
    for (store, kind) in [
        (first, StopIntentKind::UserCancelled),
        (second, StopIntentKind::DiskPressureCritical),
    ] {
        let barrier = barrier.clone();
        let request = stop_request(&other, kind);
        different.push(tokio::spawn(async move {
            barrier.wait().await;
            store.persist_stop_intent(request).await.unwrap()
        }));
    }
    barrier.wait().await;
    let left = different.remove(0).await.unwrap();
    let right = different.remove(0).await.unwrap();
    let winner = match (&left, &right) {
        (PersistStopIntentOutcome::Applied(receipt), _) => *receipt,
        (_, PersistStopIntentOutcome::Applied(receipt)) => *receipt,
        pair => panic!("different-kind race has no applied winner: {pair:?}"),
    };
    assert!(matches!(
        (&left, &right),
        (
            PersistStopIntentOutcome::Applied(_),
            PersistStopIntentOutcome::IntentConflict { existing }
        ) | (
            PersistStopIntentOutcome::IntentConflict { existing },
            PersistStopIntentOutcome::Applied(_)
        ) if *existing == winner
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generic_terminal_and_stop_intent_race_have_one_classification_winner() {
    let fixture = support::store_fixture().await;
    support::register_repository(&fixture.store, "generic-stop-race").await;
    let running = support::running_task(&fixture.store).await;
    let terminal_store = Store::open(&fixture.database_path).await.unwrap();
    let intent_store = Store::open(&fixture.database_path).await.unwrap();
    let barrier = Arc::new(Barrier::new(3));

    let terminal_barrier = barrier.clone();
    let terminal_task = running.clone();
    let terminal = tokio::spawn(async move {
        terminal_barrier.wait().await;
        terminal_store
            .transition_with_event(
                terminal_task.id,
                TaskStatus::Running,
                TaskTransition::Cancelled,
            )
            .await
    });
    let intent_barrier = barrier.clone();
    let intent_task = running.clone();
    let intent = tokio::spawn(async move {
        intent_barrier.wait().await;
        intent_store
            .persist_stop_intent(stop_request(&intent_task, StopIntentKind::UserCancelled))
            .await
    });
    barrier.wait().await;

    let terminal = terminal.await.unwrap();
    let intent = intent.await.unwrap().unwrap();
    match terminal {
        Ok(TransitionOutcome::Applied { task, .. }) => {
            assert!(matches!(
                intent,
                PersistStopIntentOutcome::TerminalWon { current } if current == task
            ));
            assert!(load_intents(&fixture.store).await.is_empty());
        }
        Err(StoreError::InvariantViolation(_)) => {
            assert!(matches!(intent, PersistStopIntentOutcome::Applied(_)));
            assert_eq!(
                fixture
                    .store
                    .task_detail(running.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .task
                    .status,
                TaskStatus::Running
            );
        }
        other => panic!("generic terminal/intent race returned {other:?}"),
    }
}

#[tokio::test]
async fn final_stop_maps_each_intent_to_one_exact_terminal_receipt_and_replays_it() {
    for kind in [
        StopIntentKind::UserCancelled,
        StopIntentKind::DiskPressureCritical,
    ] {
        let store = support::seeded_store().await;
        let running = support::running_task(&store).await;
        let intent = applied_intent(
            store
                .persist_stop_intent(stop_request(&running, kind))
                .await
                .unwrap(),
        );
        let request = finalize_request(&running, kind);

        let applied = applied_final(store.finalize_stopped_task(request).await.unwrap());
        assert_eq!(applied.intent, intent);
        assert_eq!(applied.task.last_event_id, applied.terminal_event_id);
        assert_eq!(
            applied.task.finished_at,
            Some(terminal_created_at(&store, &applied).await)
        );
        match kind {
            StopIntentKind::UserCancelled => {
                assert_eq!(applied.task.status, TaskStatus::Cancelled);
                assert_eq!(applied.task.failure, None);
            }
            StopIntentKind::DiskPressureCritical => {
                assert_eq!(applied.task.status, TaskStatus::Failed);
                let failure = applied.task.failure.as_ref().unwrap();
                assert_eq!(failure.code, "DISK_PRESSURE_CRITICAL");
                assert_eq!(failure.message, "critical disk pressure stopped the task");
                assert!(failure.retryable);
            }
        }
        assert_eq!(terminal_count(&store, running.id).await, 1);
        assert_eq!(load_intents(&store).await, vec![intent]);

        let before = support::durable_task_event_snapshot(&store).await;
        let existing = existing_final(store.finalize_stopped_task(request).await.unwrap());
        assert_eq!(existing, applied);
        assert_eq!(support::durable_task_event_snapshot(&store).await, before);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_final_stop_replays_one_terminal_event_and_receipt() {
    let fixture = support::store_fixture().await;
    support::register_repository(&fixture.store, "final-stop-race").await;
    let running = support::running_task(&fixture.store).await;
    applied_intent(
        fixture
            .store
            .persist_stop_intent(stop_request(&running, StopIntentKind::DiskPressureCritical))
            .await
            .unwrap(),
    );
    let first = Store::open(&fixture.database_path).await.unwrap();
    let second = Store::open(&fixture.database_path).await.unwrap();
    let barrier = Arc::new(Barrier::new(3));
    let mut calls = Vec::new();
    for store in [first, second] {
        let barrier = barrier.clone();
        let request = finalize_request(&running, StopIntentKind::DiskPressureCritical);
        calls.push(tokio::spawn(async move {
            barrier.wait().await;
            store.finalize_stopped_task(request).await.unwrap()
        }));
    }
    barrier.wait().await;
    let left = calls.remove(0).await.unwrap();
    let right = calls.remove(0).await.unwrap();
    assert!(matches!(
        (&left, &right),
        (
            FinalizeStoppedTaskOutcome::Applied(_),
            FinalizeStoppedTaskOutcome::Existing(_)
        ) | (
            FinalizeStoppedTaskOutcome::Existing(_),
            FinalizeStoppedTaskOutcome::Applied(_)
        )
    ));
    let left_receipt = match left {
        FinalizeStoppedTaskOutcome::Applied(receipt)
        | FinalizeStoppedTaskOutcome::Existing(receipt) => receipt,
        FinalizeStoppedTaskOutcome::InvariantConflict => panic!("canonical final stop conflicted"),
    };
    let right_receipt = match right {
        FinalizeStoppedTaskOutcome::Applied(receipt)
        | FinalizeStoppedTaskOutcome::Existing(receipt) => receipt,
        FinalizeStoppedTaskOutcome::InvariantConflict => panic!("canonical final stop conflicted"),
    };
    assert_eq!(left_receipt, right_receipt);
    assert_eq!(terminal_count(&fixture.store, running.id).await, 1);
}

#[tokio::test]
async fn panel_events_may_finish_after_intent_before_the_unique_final_stop() {
    let store = support::seeded_store().await;
    let running = support::running_task(&store).await;
    applied_intent(
        store
            .persist_stop_intent(stop_request(&running, StopIntentKind::UserCancelled))
            .await
            .unwrap(),
    );
    store
        .append_running_event(
            running.id,
            coding_agent_domain::TaskEventPayload::PlanUpdated {
                plan: coding_agent_domain::PlanSnapshot::legacy(1, Vec::new()),
            },
        )
        .await
        .unwrap();

    let receipt = applied_final(
        store
            .finalize_stopped_task(finalize_request(&running, StopIntentKind::UserCancelled))
            .await
            .unwrap(),
    );
    assert_eq!(receipt.task.status, TaskStatus::Cancelled);
    assert_eq!(terminal_count(&store, running.id).await, 1);
}

#[tokio::test]
async fn partial_or_extra_final_stop_tuples_fail_closed_without_repair() {
    for corruption in [
        FinalCorruption::TaskCursor,
        FinalCorruption::TaskStatus,
        FinalCorruption::TaskFailure,
        FinalCorruption::TaskFinishedAt,
        FinalCorruption::IntentKind,
        FinalCorruption::TerminalTimestamp,
        FinalCorruption::TerminalSchema,
        FinalCorruption::TerminalKind,
        FinalCorruption::TerminalTaskId,
        FinalCorruption::TerminalPayload,
        FinalCorruption::TerminalPayloadStorage,
        FinalCorruption::ExtraTerminalEvent,
    ] {
        let store = support::seeded_store().await;
        let running = support::running_task(&store).await;
        let request = finalize_request(&running, StopIntentKind::UserCancelled);
        applied_intent(
            store
                .persist_stop_intent(stop_request(&running, StopIntentKind::UserCancelled))
                .await
                .unwrap(),
        );
        let receipt = applied_final(store.finalize_stopped_task(request).await.unwrap());
        install_final_corruption(&store, &receipt, corruption).await;
        let before = support::durable_task_event_snapshot(&store).await;

        assert!(matches!(
            store.finalize_stopped_task(request).await.unwrap(),
            FinalizeStoppedTaskOutcome::InvariantConflict
        ));
        assert_eq!(
            support::durable_task_event_snapshot(&store).await,
            before,
            "{corruption:?}"
        );
    }
}

#[tokio::test]
async fn disk_final_stop_rejects_every_non_exact_failure_tuple() {
    for corruption in [
        DiskFailureCorruption::Code,
        DiskFailureCorruption::Message,
        DiskFailureCorruption::Retryable,
        DiskFailureCorruption::ExtraField,
        DiskFailureCorruption::NonCanonicalJson,
    ] {
        let store = support::seeded_store().await;
        let running = support::running_task(&store).await;
        let request = finalize_request(&running, StopIntentKind::DiskPressureCritical);
        applied_intent(
            store
                .persist_stop_intent(stop_request(&running, StopIntentKind::DiskPressureCritical))
                .await
                .unwrap(),
        );
        let receipt = applied_final(store.finalize_stopped_task(request).await.unwrap());
        install_disk_failure_corruption(&store, &receipt, corruption).await;
        let before = support::durable_task_event_snapshot(&store).await;

        assert!(
            matches!(
                store.finalize_stopped_task(request).await.unwrap(),
                FinalizeStoppedTaskOutcome::InvariantConflict
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
async fn final_stop_faults_roll_back_every_lifecycle_write_stage() {
    for fault in [
        FinalStopFault::TaskUpdate,
        FinalStopFault::TerminalInsert,
        FinalStopFault::CursorUpdate,
        FinalStopFault::PayloadUpdate,
        FinalStopFault::AfterPayloadTamper,
        FinalStopFault::AfterPayloadDelete,
        FinalStopFault::DeferredCommit,
    ] {
        let store = support::seeded_store().await;
        let running = support::running_task(&store).await;
        applied_intent(
            store
                .persist_stop_intent(stop_request(&running, StopIntentKind::UserCancelled))
                .await
                .unwrap(),
        );
        let before = support::durable_task_event_snapshot(&store).await;
        install_final_stop_fault(&store, fault).await;

        store
            .finalize_stopped_task(finalize_request(&running, StopIntentKind::UserCancelled))
            .await
            .expect_err("injected final-stop fault must escape");

        assert_eq!(
            support::durable_task_event_snapshot(&store).await,
            before,
            "{fault:?}"
        );
    }
}

#[tokio::test]
async fn generic_terminal_writes_cannot_consume_an_intent_but_queued_cancel_stays_direct() {
    let store = support::seeded_store().await;
    let running = support::running_task(&store).await;
    applied_intent(
        store
            .persist_stop_intent(stop_request(&running, StopIntentKind::UserCancelled))
            .await
            .unwrap(),
    );
    let before = support::durable_task_event_snapshot(&store).await;
    assert!(matches!(
        store
            .transition_with_event(running.id, TaskStatus::Running, TaskTransition::Cancelled)
            .await
            .unwrap_err(),
        StoreError::InvariantViolation(_)
    ));
    assert_eq!(support::durable_task_event_snapshot(&store).await, before);

    let queued = support::queued_task(&store).await;
    let cancelled = applied_task(
        store
            .transition_with_event(queued.id, TaskStatus::Queued, TaskTransition::Cancelled)
            .await
            .unwrap(),
    );
    assert_eq!(cancelled.status, TaskStatus::Cancelled);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_stop_intents WHERE task_id = ?",)
            .bind(queued.id.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap(),
        0
    );
}

#[derive(Debug, Clone, Copy)]
enum RawIntentCorruption {
    TaskIdUppercase,
    TaskIdSimple,
    TaskIdBraced,
    TaskIdUrn,
    CanonicalPlusSimpleTaskId,
    RepositoryIdUppercase,
    KindUnknown,
    RequestedAtNonCanonical,
}

async fn install_raw_intent_corruption(
    store: &Store,
    task: &Task,
    corruption: RawIntentCorruption,
) {
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("DROP TRIGGER task_stop_intents_no_update")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("PRAGMA ignore_check_constraints = ON")
        .execute(&mut *connection)
        .await
        .unwrap();

    match corruption {
        RawIntentCorruption::TaskIdUppercase => {
            sqlx::query("UPDATE task_stop_intents SET task_id = upper(task_id) WHERE task_id = ?")
                .bind(task.id.to_string())
                .execute(&mut *connection)
                .await
                .unwrap();
        }
        RawIntentCorruption::TaskIdSimple => {
            sqlx::query(
                "UPDATE task_stop_intents \
                 SET task_id = replace(task_id, '-', '') WHERE task_id = ?",
            )
            .bind(task.id.to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
        }
        RawIntentCorruption::TaskIdBraced => {
            sqlx::query(
                "UPDATE task_stop_intents \
                 SET task_id = '{' || task_id || '}' WHERE task_id = ?",
            )
            .bind(task.id.to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
        }
        RawIntentCorruption::TaskIdUrn => {
            sqlx::query(
                "UPDATE task_stop_intents \
                 SET task_id = 'urn:uuid:' || task_id WHERE task_id = ?",
            )
            .bind(task.id.to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
        }
        RawIntentCorruption::CanonicalPlusSimpleTaskId => {
            sqlx::query("DROP TRIGGER task_stop_intents_running_unreviewed_on_insert")
                .execute(&mut *connection)
                .await
                .unwrap();
            sqlx::query(
                "INSERT INTO task_stop_intents \
                     (task_id, repository_id, attempt, kind, requested_at) \
                 SELECT replace(task_id, '-', ''), repository_id, attempt, kind, requested_at \
                 FROM task_stop_intents WHERE task_id = ?",
            )
            .bind(task.id.to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
        }
        RawIntentCorruption::RepositoryIdUppercase => {
            sqlx::query(
                "UPDATE task_stop_intents SET repository_id = upper(repository_id) \
                 WHERE task_id = ?",
            )
            .bind(task.id.to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
        }
        RawIntentCorruption::KindUnknown => {
            sqlx::query(
                "UPDATE task_stop_intents SET kind = 'unexpected_stop_kind' \
                 WHERE task_id = ?",
            )
            .bind(task.id.to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
        }
        RawIntentCorruption::RequestedAtNonCanonical => {
            sqlx::query(
                "UPDATE task_stop_intents \
                 SET requested_at = replace(requested_at, 'Z', '+00:00') \
                 WHERE task_id = ?",
            )
            .bind(task.id.to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
        }
    }

    sqlx::query("PRAGMA ignore_check_constraints = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
}

#[derive(Debug, Clone, Copy)]
enum FinalCorruption {
    TaskCursor,
    TaskStatus,
    TaskFailure,
    TaskFinishedAt,
    IntentKind,
    TerminalTimestamp,
    TerminalSchema,
    TerminalKind,
    TerminalTaskId,
    TerminalPayload,
    TerminalPayloadStorage,
    ExtraTerminalEvent,
}

#[derive(Debug, Clone, Copy)]
enum DiskFailureCorruption {
    Code,
    Message,
    Retryable,
    ExtraField,
    NonCanonicalJson,
}

async fn install_disk_failure_corruption(
    store: &Store,
    receipt: &coding_agent_store::FinalizeStoppedTaskReceipt,
    corruption: DiskFailureCorruption,
) {
    sqlx::query("DROP TRIGGER tasks_stop_intent_terminal_on_update")
        .execute(store.pool())
        .await
        .unwrap();
    let failure_json = match corruption {
        DiskFailureCorruption::Code => {
            r#"{"code":"OTHER","message":"critical disk pressure stopped the task","retryable":true}"#
        }
        DiskFailureCorruption::Message => {
            r#"{"code":"DISK_PRESSURE_CRITICAL","message":"other","retryable":true}"#
        }
        DiskFailureCorruption::Retryable => {
            r#"{"code":"DISK_PRESSURE_CRITICAL","message":"critical disk pressure stopped the task","retryable":false}"#
        }
        DiskFailureCorruption::ExtraField => {
            r#"{"code":"DISK_PRESSURE_CRITICAL","message":"critical disk pressure stopped the task","retryable":true,"extra":1}"#
        }
        DiskFailureCorruption::NonCanonicalJson => {
            r#"{"retryable":true,"message":"critical disk pressure stopped the task","code":"DISK_PRESSURE_CRITICAL"}"#
        }
    };
    sqlx::query("UPDATE tasks SET failure_json = ? WHERE id = ?")
        .bind(failure_json)
        .bind(receipt.task.id.to_string())
        .execute(store.pool())
        .await
        .unwrap();
}

#[derive(Debug, Clone, Copy)]
enum BatchFault {
    TamperEarlierItem,
    DeferredCommit,
}

async fn install_batch_fault(store: &Store, requests: &[StopIntentRequest], fault: BatchFault) {
    assert_eq!(requests.len(), 2);
    let first_task_id = requests[0].task_id;
    let second_task_id = requests[1].task_id;
    let sql = match fault {
        BatchFault::TamperEarlierItem => format!(
            "CREATE TRIGGER stop_batch_post_fault \
             AFTER INSERT ON task_stop_intents \
             WHEN NEW.task_id = '{second_task_id}' \
             BEGIN \
                 UPDATE tasks SET prompt = 'tampered-by-later-item' \
                 WHERE id = '{first_task_id}'; \
             END;"
        ),
        BatchFault::DeferredCommit => format!(
            "CREATE TABLE stop_batch_parent (id INTEGER PRIMARY KEY); \
             CREATE TABLE stop_batch_child (\
                 parent_id INTEGER REFERENCES stop_batch_parent(id) \
                     DEFERRABLE INITIALLY DEFERRED\
             ); \
             CREATE TRIGGER stop_batch_post_fault \
             AFTER INSERT ON task_stop_intents \
             WHEN NEW.task_id = '{second_task_id}' \
             BEGIN \
                 INSERT INTO stop_batch_child (parent_id) VALUES (1); \
             END;"
        ),
    };
    sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
        .execute(store.pool())
        .await
        .unwrap();
}

async fn install_final_corruption(
    store: &Store,
    receipt: &coding_agent_store::FinalizeStoppedTaskReceipt,
    corruption: FinalCorruption,
) {
    match corruption {
        FinalCorruption::TaskCursor => {
            sqlx::query("UPDATE tasks SET last_event_id = last_event_id - 1 WHERE id = ?")
                .bind(receipt.task.id.to_string())
                .execute(store.pool())
                .await
                .unwrap();
        }
        FinalCorruption::TaskStatus => {
            sqlx::query("DROP TRIGGER tasks_stop_intent_terminal_on_update")
                .execute(store.pool())
                .await
                .unwrap();
            sqlx::query("UPDATE tasks SET status = 'interrupted' WHERE id = ?")
                .bind(receipt.task.id.to_string())
                .execute(store.pool())
                .await
                .unwrap();
        }
        FinalCorruption::TaskFailure => {
            sqlx::query("DROP TRIGGER tasks_stop_intent_terminal_on_update")
                .execute(store.pool())
                .await
                .unwrap();
            sqlx::query(
                "UPDATE tasks \
                 SET failure_json = \
                     '{\"code\":\"UNEXPECTED\",\"message\":\"unexpected\",\"retryable\":false}' \
                 WHERE id = ?",
            )
            .bind(receipt.task.id.to_string())
            .execute(store.pool())
            .await
            .unwrap();
        }
        FinalCorruption::TaskFinishedAt => {
            sqlx::query("DROP TRIGGER tasks_stop_intent_terminal_on_update")
                .execute(store.pool())
                .await
                .unwrap();
            sqlx::query("UPDATE tasks SET finished_at = '2020-01-01T00:00:00Z' WHERE id = ?")
                .bind(receipt.task.id.to_string())
                .execute(store.pool())
                .await
                .unwrap();
        }
        FinalCorruption::IntentKind => {
            sqlx::query("DROP TRIGGER task_stop_intents_no_update")
                .execute(store.pool())
                .await
                .unwrap();
            sqlx::query(
                "UPDATE task_stop_intents \
                 SET kind = 'disk_pressure_critical' WHERE task_id = ?",
            )
            .bind(receipt.task.id.to_string())
            .execute(store.pool())
            .await
            .unwrap();
        }
        FinalCorruption::TerminalTimestamp => {
            sqlx::query(
                "UPDATE task_events \
                 SET created_at = '2020-01-01T00:00:00Z' WHERE id = ?",
            )
            .bind(receipt.terminal_event_id.get())
            .execute(store.pool())
            .await
            .unwrap();
        }
        FinalCorruption::TerminalSchema => {
            sqlx::query("UPDATE task_events SET schema_version = 2 WHERE id = ?")
                .bind(receipt.terminal_event_id.get())
                .execute(store.pool())
                .await
                .unwrap();
        }
        FinalCorruption::TerminalKind => {
            sqlx::query("UPDATE task_events SET kind = 'task.interrupted' WHERE id = ?")
                .bind(receipt.terminal_event_id.get())
                .execute(store.pool())
                .await
                .unwrap();
        }
        FinalCorruption::TerminalTaskId => {
            let mut connection = store.pool().acquire().await.unwrap();
            sqlx::query("PRAGMA foreign_keys = OFF")
                .execute(&mut *connection)
                .await
                .unwrap();
            sqlx::query(
                "UPDATE task_events \
                 SET task_id = '00000000-0000-4000-8000-000000000000' WHERE id = ?",
            )
            .bind(receipt.terminal_event_id.get())
            .execute(&mut *connection)
            .await
            .unwrap();
            sqlx::query("PRAGMA foreign_keys = ON")
                .execute(&mut *connection)
                .await
                .unwrap();
        }
        FinalCorruption::TerminalPayload => {
            sqlx::query("UPDATE task_events SET payload_json = '{}' WHERE id = ?")
                .bind(receipt.terminal_event_id.get())
                .execute(store.pool())
                .await
                .unwrap();
        }
        FinalCorruption::TerminalPayloadStorage => {
            let mut connection = store.pool().acquire().await.unwrap();
            sqlx::query("PRAGMA ignore_check_constraints = ON")
                .execute(&mut *connection)
                .await
                .unwrap();
            sqlx::query(
                "UPDATE task_events \
                 SET payload_json = CAST(payload_json AS BLOB) WHERE id = ?",
            )
            .bind(receipt.terminal_event_id.get())
            .execute(&mut *connection)
            .await
            .unwrap();
            sqlx::query("PRAGMA ignore_check_constraints = OFF")
                .execute(&mut *connection)
                .await
                .unwrap();
        }
        FinalCorruption::ExtraTerminalEvent => {
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
}

#[derive(Debug, Clone, Copy)]
enum FinalStopFault {
    TaskUpdate,
    TerminalInsert,
    CursorUpdate,
    PayloadUpdate,
    AfterPayloadTamper,
    AfterPayloadDelete,
    DeferredCommit,
}

async fn install_final_stop_fault(store: &Store, fault: FinalStopFault) {
    match fault {
        FinalStopFault::TaskUpdate => {
            sqlx::query(
                "CREATE TRIGGER final_stop_fault BEFORE UPDATE OF status ON tasks \
                 WHEN NEW.status = 'cancelled' \
                 BEGIN SELECT RAISE(ABORT, 'final-stop-task'); END",
            )
            .execute(store.pool())
            .await
            .unwrap();
        }
        FinalStopFault::TerminalInsert => {
            sqlx::query(
                "CREATE TRIGGER final_stop_fault BEFORE INSERT ON task_events \
                 WHEN NEW.kind = 'task.cancelled' \
                 BEGIN SELECT RAISE(ABORT, 'final-stop-event'); END",
            )
            .execute(store.pool())
            .await
            .unwrap();
        }
        FinalStopFault::CursorUpdate => {
            sqlx::query(
                "CREATE TRIGGER final_stop_fault BEFORE UPDATE OF last_event_id ON tasks \
                 WHEN NEW.status = 'cancelled' \
                 BEGIN SELECT RAISE(ABORT, 'final-stop-cursor'); END",
            )
            .execute(store.pool())
            .await
            .unwrap();
        }
        FinalStopFault::PayloadUpdate => {
            sqlx::query(
                "CREATE TRIGGER final_stop_fault BEFORE UPDATE OF payload_json ON task_events \
                 WHEN NEW.kind = 'task.cancelled' \
                 BEGIN SELECT RAISE(ABORT, 'final-stop-payload'); END",
            )
            .execute(store.pool())
            .await
            .unwrap();
        }
        FinalStopFault::AfterPayloadTamper => {
            sqlx::query(
                "CREATE TRIGGER final_stop_fault AFTER UPDATE OF payload_json ON task_events \
                 WHEN NEW.kind = 'task.cancelled' \
                 BEGIN UPDATE task_events SET created_at = '2026-01-01T00:00:00Z' \
                       WHERE id = NEW.id; END",
            )
            .execute(store.pool())
            .await
            .unwrap();
        }
        FinalStopFault::AfterPayloadDelete => {
            sqlx::query(
                "CREATE TRIGGER final_stop_fault AFTER UPDATE OF payload_json ON task_events \
                 WHEN NEW.kind = 'task.cancelled' \
                 BEGIN DELETE FROM task_events WHERE id = NEW.id; END",
            )
            .execute(store.pool())
            .await
            .unwrap();
        }
        FinalStopFault::DeferredCommit => {
            sqlx::raw_sql(
                "CREATE TABLE final_stop_parent (id INTEGER PRIMARY KEY); \
                 CREATE TABLE final_stop_child (\
                     parent_id INTEGER REFERENCES final_stop_parent(id) \
                         DEFERRABLE INITIALLY DEFERRED\
                 ); \
                 CREATE TRIGGER final_stop_fault AFTER UPDATE OF payload_json ON task_events \
                 WHEN NEW.kind = 'task.cancelled' \
                 BEGIN INSERT INTO final_stop_child (parent_id) VALUES (1); END;",
            )
            .execute(store.pool())
            .await
            .unwrap();
        }
    }
}

fn stop_request(task: &Task, kind: StopIntentKind) -> StopIntentRequest {
    StopIntentRequest {
        task_id: task.id,
        expected_repository_id: task.repository_id,
        expected_attempt: task.attempt,
        kind,
    }
}

fn finalize_request(task: &Task, kind: StopIntentKind) -> FinalizeStoppedTaskRequest {
    FinalizeStoppedTaskRequest {
        task_id: task.id,
        expected_repository_id: task.repository_id,
        expected_attempt: task.attempt,
        expected_intent: kind,
    }
}

fn applied_intent(outcome: PersistStopIntentOutcome) -> StopIntentReceipt {
    match outcome {
        PersistStopIntentOutcome::Applied(receipt) => receipt,
        other => panic!("intent must apply, got {other:?}"),
    }
}

fn existing_intent(outcome: PersistStopIntentOutcome) -> StopIntentReceipt {
    match outcome {
        PersistStopIntentOutcome::Existing(receipt) => receipt,
        other => panic!("intent must be existing, got {other:?}"),
    }
}

fn applied_final(
    outcome: FinalizeStoppedTaskOutcome,
) -> coding_agent_store::FinalizeStoppedTaskReceipt {
    match outcome {
        FinalizeStoppedTaskOutcome::Applied(receipt) => receipt,
        other => panic!("final stop must apply, got {other:?}"),
    }
}

fn existing_final(
    outcome: FinalizeStoppedTaskOutcome,
) -> coding_agent_store::FinalizeStoppedTaskReceipt {
    match outcome {
        FinalizeStoppedTaskOutcome::Existing(receipt) => receipt,
        other => panic!("final stop must be existing, got {other:?}"),
    }
}

fn applied_task(outcome: TransitionOutcome) -> Task {
    match outcome {
        TransitionOutcome::Applied { task, .. } => task,
        TransitionOutcome::Conflict { .. } => panic!("fixture transition must apply"),
    }
}

async fn load_intents(store: &Store) -> Vec<StopIntentReceipt> {
    sqlx::query_as::<_, (String, String, i64, String, String)>(
        "SELECT task_id, repository_id, attempt, kind, requested_at \
         FROM task_stop_intents ORDER BY task_id",
    )
    .fetch_all(store.pool())
    .await
    .unwrap()
    .into_iter()
    .map(
        |(task_id, repository_id, attempt, kind, requested_at)| StopIntentReceipt {
            task_id: task_id.parse().unwrap(),
            repository_id: repository_id.parse().unwrap(),
            attempt: u32::try_from(attempt).unwrap(),
            kind: match kind.as_str() {
                "user_cancelled" => StopIntentKind::UserCancelled,
                "disk_pressure_critical" => StopIntentKind::DiskPressureCritical,
                _ => panic!("fixture intent kind"),
            },
            requested_at: coding_agent_domain::UtcTimestamp::parse_rfc3339(&requested_at).unwrap(),
        },
    )
    .collect()
}

async fn event_count(store: &Store, task_id: TaskId) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM task_events WHERE task_id = ?")
        .bind(task_id.to_string())
        .fetch_one(store.pool())
        .await
        .unwrap()
}

async fn terminal_count(store: &Store, task_id: TaskId) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_events \
         WHERE task_id = ? \
           AND kind IN ('task.completed','task.failed','task.cancelled','task.interrupted')",
    )
    .bind(task_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap()
}

async fn terminal_created_at(
    store: &Store,
    receipt: &coding_agent_store::FinalizeStoppedTaskReceipt,
) -> coding_agent_domain::UtcTimestamp {
    let raw: String = sqlx::query_scalar("SELECT created_at FROM task_events WHERE id = ?")
        .bind(receipt.terminal_event_id.get())
        .fetch_one(store.pool())
        .await
        .unwrap();
    coding_agent_domain::UtcTimestamp::parse_rfc3339(&raw).unwrap()
}
