mod support;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use coding_agent_domain::{EventCursor, EventId, Task, TaskEventPayload, TaskStatus};
use coding_agent_store::{
    ClaimTaskOutcome, ClaimTaskReceipt, ClaimTaskReconciliationOutcome, ClaimTaskRequest, Store,
    StoreError, TaskTransition, TransitionOutcome,
};
use tokio::sync::Barrier;

#[tokio::test]
async fn first_claim_commits_the_exact_started_tuple_atomically() {
    let store = support::seeded_store().await;
    let queued = support::queued_task(&store).await;
    let request = claim_request(&queued);
    let queued_event_before = raw_event(&store, queued.last_event_id).await;

    let receipt = applied_receipt(store.claim_task(request.clone()).await.unwrap());
    let events = store
        .task_events_after(queued.id, EventCursor::ZERO, 10)
        .await
        .unwrap()
        .events;
    assert_eq!(events.len(), 2);
    assert_eq!(receipt.task.id, queued.id);
    assert_eq!(receipt.task.repository_id, queued.repository_id);
    assert_eq!(receipt.task.attempt, queued.attempt);
    assert_eq!(receipt.task.status, TaskStatus::Running);
    assert_eq!(receipt.task.last_event_id, receipt.started_event_id);
    assert_eq!(receipt.task.started_at, Some(events[1].created_at));

    let detail = store.task_detail(queued.id).await.unwrap().unwrap();
    assert_eq!(detail.task, receipt.task);
    assert_eq!(detail.timeline.len(), 2);
    assert!(matches!(
        &events[1].payload,
        TaskEventPayload::TaskStarted { task } if task == &receipt.task
    ));
    assert_eq!(events[1].id, receipt.started_event_id);
    assert_eq!(events[1].created_at, receipt.task.started_at.unwrap());
    assert_eq!(
        raw_event(&store, queued.last_event_id).await,
        queued_event_before
    );
}

#[tokio::test]
async fn canonical_replay_and_reconciliation_return_the_original_receipt_without_writes() {
    let store = support::seeded_store().await;
    let queued = support::queued_task(&store).await;
    let request = claim_request(&queued);
    let applied = applied_receipt(store.claim_task(request.clone()).await.unwrap());
    let before = support::durable_task_event_snapshot(&store).await;

    let existing = existing_receipt(store.claim_task(request.clone()).await.unwrap());
    assert_eq!(existing, applied);
    let reconciled = existing_reconciliation(store.reconcile_task_claim(&request).await.unwrap());
    assert_eq!(reconciled, applied);
    assert_eq!(support::durable_task_event_snapshot(&store).await, before);

    store
        .append_running_event(
            queued.id,
            TaskEventPayload::PlanUpdated {
                plan: coding_agent_domain::PlanSnapshot::legacy(1, Vec::new()),
            },
        )
        .await
        .unwrap();
    let after_panel = support::durable_task_event_snapshot(&store).await;
    assert_eq!(
        existing_receipt(store.claim_task(request.clone()).await.unwrap()),
        applied
    );
    assert_eq!(
        existing_reconciliation(store.reconcile_task_claim(&request).await.unwrap()),
        applied
    );
    assert_eq!(
        support::durable_task_event_snapshot(&store).await,
        after_panel
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_identical_claims_have_one_linearization_point_and_event() {
    const CALLS: usize = 8;

    let fixture = support::store_fixture().await;
    support::register_repository(&fixture.store, "claim-race").await;
    let queued = support::queued_task(&fixture.store).await;
    let request = claim_request(&queued);
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
        let request = request.clone();
        calls.push(tokio::spawn(async move {
            barrier.wait().await;
            store.claim_task(request).await
        }));
    }

    barrier.wait().await;
    let mut outcomes = Vec::new();
    for call in calls {
        outcomes.push(
            call.await
                .unwrap()
                .expect("claim must not return SQLITE_BUSY"),
        );
    }
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, ClaimTaskOutcome::Applied(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, ClaimTaskOutcome::ExistingApplied(_)))
            .count(),
        CALLS - 1
    );
    let event_ids: HashSet<_> = outcomes
        .iter()
        .map(|outcome| match outcome {
            ClaimTaskOutcome::Applied(receipt) | ClaimTaskOutcome::ExistingApplied(receipt) => {
                receipt.started_event_id
            }
            other => panic!("canonical concurrent claim returned {other:?}"),
        })
        .collect();
    assert_eq!(event_ids.len(), 1);
    let authoritative = fixture
        .store
        .task_detail(queued.id)
        .await
        .unwrap()
        .unwrap()
        .task;
    assert!(outcomes.iter().all(|outcome| match outcome {
        ClaimTaskOutcome::Applied(receipt) | ClaimTaskOutcome::ExistingApplied(receipt) =>
            receipt.task == authoritative,
        _ => false,
    }));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task_events WHERE task_id = ? AND kind = 'task.started'",
        )
        .bind(queued.id.to_string())
        .fetch_one(fixture.store.pool())
        .await
        .unwrap(),
        1
    );
}

#[tokio::test]
async fn reconciliation_proves_queued_and_terminal_tasks_known_not_applied() {
    let store = support::seeded_store().await;

    let queued = support::queued_task(&store).await;
    let queued_request = claim_request(&queued);
    assert_known_not_applied(
        store.reconcile_task_claim(&queued_request).await.unwrap(),
        &queued,
    );

    let cancelled = applied_task(
        store
            .transition_with_event(queued.id, TaskStatus::Queued, TaskTransition::Cancelled)
            .await
            .unwrap(),
    );
    assert_known_not_applied(
        store.reconcile_task_claim(&queued_request).await.unwrap(),
        &cancelled,
    );
    assert_known_not_applied_claim(store.claim_task(queued_request).await.unwrap(), &cancelled);

    let another = support::queued_task(&store).await;
    let another_request = claim_request(&another);
    let running = applied_receipt(store.claim_task(another_request.clone()).await.unwrap()).task;
    let failed = applied_task(
        store
            .transition_with_event(
                running.id,
                TaskStatus::Running,
                TaskTransition::Failed(support::failure("TERMINAL_AFTER_CLAIM")),
            )
            .await
            .unwrap(),
    );
    assert_known_not_applied(
        store.reconcile_task_claim(&another_request).await.unwrap(),
        &failed,
    );

    let missing = ClaimTaskRequest {
        task_id: coding_agent_domain::TaskId::new(),
        expected_repository_id: queued.repository_id,
        expected_attempt: 0,
        expected_queued_event_id: queued.last_event_id,
    };
    assert!(matches!(
        store.reconcile_task_claim(&missing).await.unwrap_err(),
        StoreError::TaskNotFound
    ));
    assert!(matches!(
        store.claim_task(missing).await.unwrap_err(),
        StoreError::TaskNotFound
    ));
}

#[tokio::test]
async fn request_identity_and_queued_cursor_mismatches_are_invariant_conflicts_without_writes() {
    let store = support::seeded_store().await;
    let queued = support::queued_task(&store).await;
    let other = support::queued_task(&store).await;
    let other_repository = support::register_repository(&store, "claim-other").await;
    let base = claim_request(&queued);
    let requests = [
        ClaimTaskRequest {
            expected_repository_id: other_repository.id,
            ..base.clone()
        },
        ClaimTaskRequest {
            expected_attempt: queued.attempt + 1,
            ..base.clone()
        },
        ClaimTaskRequest {
            expected_attempt: 0,
            ..base.clone()
        },
        ClaimTaskRequest {
            expected_queued_event_id: other.last_event_id,
            ..base
        },
    ];

    for request in requests {
        let before = support::durable_task_event_snapshot(&store).await;
        assert!(matches!(
            store.claim_task(request.clone()).await.unwrap(),
            ClaimTaskOutcome::InvariantConflict
        ));
        assert!(matches!(
            store.reconcile_task_claim(&request).await.unwrap(),
            ClaimTaskReconciliationOutcome::InvariantConflict
        ));
        assert_eq!(support::durable_task_event_snapshot(&store).await, before);
    }
}

#[tokio::test]
async fn partial_and_mismatched_queued_or_started_tuples_fail_closed() {
    for corruption in [
        ClaimCorruption::TaskCursor,
        ClaimCorruption::QueuedKind,
        ClaimCorruption::QueuedPayload,
        ClaimCorruption::QueuedPayloadAttempt,
        ClaimCorruption::QueuedStrayStarted,
        ClaimCorruption::RunningTaskCursor,
        ClaimCorruption::RunningStartedAt,
        ClaimCorruption::StartedSchema,
        ClaimCorruption::StartedTaskId,
        ClaimCorruption::StartedKind,
        ClaimCorruption::StartedPayload,
        ClaimCorruption::StartedPayloadAttempt,
        ClaimCorruption::StartedTimestamp,
        ClaimCorruption::MissingStarted,
        ClaimCorruption::DuplicateStarted,
    ] {
        assert_claim_corruption_is_conflict(corruption).await;
    }
}

#[tokio::test]
async fn storage_class_corruption_is_a_typed_invariant_conflict() {
    for corruption in [
        StorageClassCorruption::TaskAttemptBlob,
        StorageClassCorruption::EventSchemaReal,
        StorageClassCorruption::EventPayloadBlob,
    ] {
        let store = support::seeded_store().await;
        let queued = support::queued_task(&store).await;
        let request = claim_request(&queued);
        install_storage_class_corruption(&store, &queued, corruption).await;
        let before = support::durable_task_event_snapshot(&store).await;

        assert!(matches!(
            store.claim_task(request.clone()).await.unwrap(),
            ClaimTaskOutcome::InvariantConflict
        ));
        assert!(matches!(
            store.reconcile_task_claim(&request).await.unwrap(),
            ClaimTaskReconciliationOutcome::InvariantConflict
        ));
        assert_eq!(
            support::durable_task_event_snapshot(&store).await,
            before,
            "{corruption:?}"
        );
    }
}

#[tokio::test]
async fn terminal_current_and_history_corruption_are_invariant_conflicts() {
    for corruption in [
        TerminalCorruption::CurrentKind,
        TerminalCorruption::CurrentPayloadAttempt,
        TerminalCorruption::StartedPayloadAttempt,
        TerminalCorruption::StrayQueuedHistory,
    ] {
        let store = support::seeded_store().await;
        let queued = support::queued_task(&store).await;
        let request = claim_request(&queued);
        let receipt = applied_receipt(store.claim_task(request.clone()).await.unwrap());
        if matches!(corruption, TerminalCorruption::StrayQueuedHistory) {
            sqlx::query(
                "INSERT INTO task_events \
                     (schema_version, task_id, kind, payload_json, created_at) \
                 SELECT 1, task_id, 'task.queued', payload_json, created_at \
                 FROM task_events WHERE id = ?",
            )
            .bind(queued.last_event_id.get())
            .execute(store.pool())
            .await
            .unwrap();
        }
        let terminal = applied_task(
            store
                .transition_with_event(
                    queued.id,
                    TaskStatus::Running,
                    TaskTransition::Failed(support::failure("TERMINAL_CORRUPTION")),
                )
                .await
                .unwrap(),
        );
        install_terminal_corruption(&store, &queued, &receipt, &terminal, corruption).await;
        let before = support::durable_task_event_snapshot(&store).await;

        assert!(matches!(
            store.claim_task(request.clone()).await.unwrap(),
            ClaimTaskOutcome::InvariantConflict
        ));
        assert!(matches!(
            store.reconcile_task_claim(&request).await.unwrap(),
            ClaimTaskReconciliationOutcome::InvariantConflict
        ));
        assert_eq!(
            support::durable_task_event_snapshot(&store).await,
            before,
            "{corruption:?}"
        );
    }
}

#[tokio::test]
async fn claim_faults_roll_back_every_lifecycle_write_stage() {
    for fault in [
        ClaimFault::TaskUpdate,
        ClaimFault::StartedInsert,
        ClaimFault::LastEventUpdate,
        ClaimFault::PayloadUpdate,
        ClaimFault::AfterPayloadTamper,
        ClaimFault::AfterPayloadDelete,
        ClaimFault::DeferredCommit,
    ] {
        let store = support::seeded_store().await;
        let queued = support::queued_task(&store).await;
        let request = claim_request(&queued);
        let before = support::durable_task_event_snapshot(&store).await;
        install_claim_fault(&store, fault).await;

        store
            .claim_task(request)
            .await
            .expect_err("injected claim fault must escape");

        assert_eq!(
            support::durable_task_event_snapshot(&store).await,
            before,
            "{fault:?}"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM task_events WHERE payload_json = '{}'",
            )
            .fetch_one(store.pool())
            .await
            .unwrap(),
            0,
            "{fault:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconciliation_waits_for_an_inflight_claim_before_classifying() {
    let fixture = support::store_fixture().await;
    support::register_repository(&fixture.store, "reconcile-race").await;
    let queued = support::queued_task(&fixture.store).await;
    let request = claim_request(&queued);
    let reconcile_store = Store::open(&fixture.database_path).await.unwrap();
    let mut transaction = fixture
        .store
        .pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap();
    let staged = stage_claim_without_commit(&mut transaction, &queued).await;
    let mut reconciliation = Box::pin(reconcile_store.reconcile_task_claim(&request));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut reconciliation)
            .await
            .is_err(),
        "writer-fenced reconciliation must not classify an uncommitted queued snapshot"
    );
    transaction.commit().await.unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(2), &mut reconciliation)
        .await
        .expect("reconciliation must resume after writer commit")
        .unwrap();
    assert_eq!(existing_reconciliation(outcome), staged);
}

#[tokio::test]
async fn reconciliation_is_strictly_read_only_for_every_disposition() {
    let store = support::seeded_store().await;
    let queued = support::queued_task(&store).await;
    let queued_request = claim_request(&queued);
    install_write_rejection_triggers(&store).await;
    let before = support::durable_task_event_snapshot(&store).await;
    assert_known_not_applied(
        store.reconcile_task_claim(&queued_request).await.unwrap(),
        &queued,
    );
    assert_eq!(support::durable_task_event_snapshot(&store).await, before);

    let running_store = support::seeded_store().await;
    let running_queued = support::queued_task(&running_store).await;
    let running_request = claim_request(&running_queued);
    let receipt = applied_receipt(
        running_store
            .claim_task(running_request.clone())
            .await
            .unwrap(),
    );
    install_write_rejection_triggers(&running_store).await;
    let before = support::durable_task_event_snapshot(&running_store).await;
    assert_eq!(
        existing_reconciliation(
            running_store
                .reconcile_task_claim(&running_request)
                .await
                .unwrap()
        ),
        receipt
    );
    assert_eq!(
        support::durable_task_event_snapshot(&running_store).await,
        before
    );

    let terminal_store = support::seeded_store().await;
    let terminal_queued = support::queued_task(&terminal_store).await;
    let terminal_request = claim_request(&terminal_queued);
    let terminal = applied_task(
        terminal_store
            .transition_with_event(
                terminal_queued.id,
                TaskStatus::Queued,
                TaskTransition::Cancelled,
            )
            .await
            .unwrap(),
    );
    install_write_rejection_triggers(&terminal_store).await;
    let before = support::durable_task_event_snapshot(&terminal_store).await;
    assert_known_not_applied(
        terminal_store
            .reconcile_task_claim(&terminal_request)
            .await
            .unwrap(),
        &terminal,
    );
    assert_eq!(
        support::durable_task_event_snapshot(&terminal_store).await,
        before
    );

    let corrupt_store = support::seeded_store().await;
    let corrupt = support::queued_task(&corrupt_store).await;
    let corrupt_request = claim_request(&corrupt);
    sqlx::query("UPDATE task_events SET payload_json = '{}' WHERE id = ?")
        .bind(corrupt.last_event_id.get())
        .execute(corrupt_store.pool())
        .await
        .unwrap();
    install_write_rejection_triggers(&corrupt_store).await;
    let before = support::durable_task_event_snapshot(&corrupt_store).await;
    assert!(matches!(
        corrupt_store
            .reconcile_task_claim(&corrupt_request)
            .await
            .unwrap(),
        ClaimTaskReconciliationOutcome::InvariantConflict
    ));
    assert_eq!(
        support::durable_task_event_snapshot(&corrupt_store).await,
        before
    );

    let missing = ClaimTaskRequest {
        task_id: coding_agent_domain::TaskId::new(),
        expected_repository_id: corrupt.repository_id,
        expected_attempt: 1,
        expected_queued_event_id: corrupt.last_event_id,
    };
    let before = support::durable_task_event_snapshot(&corrupt_store).await;
    assert!(matches!(
        corrupt_store
            .reconcile_task_claim(&missing)
            .await
            .unwrap_err(),
        StoreError::TaskNotFound
    ));
    assert_eq!(
        support::durable_task_event_snapshot(&corrupt_store).await,
        before
    );
}

#[tokio::test]
async fn legacy_unreviewed_lifecycle_payloads_without_readiness_remain_claimable() {
    let store = support::seeded_store().await;
    let queued = support::queued_task(&store).await;
    let request = claim_request(&queued);
    remove_delivery_readiness(&store, queued.last_event_id).await;

    let receipt = applied_receipt(store.claim_task(request.clone()).await.unwrap());
    remove_delivery_readiness(&store, receipt.started_event_id).await;
    let before = support::durable_task_event_snapshot(&store).await;
    assert_eq!(
        existing_receipt(store.claim_task(request.clone()).await.unwrap()),
        receipt
    );
    assert_eq!(
        existing_reconciliation(store.reconcile_task_claim(&request).await.unwrap()),
        receipt
    );
    assert_eq!(support::durable_task_event_snapshot(&store).await, before);
}

#[derive(Debug, Clone, Copy)]
enum ClaimCorruption {
    TaskCursor,
    QueuedKind,
    QueuedPayload,
    QueuedPayloadAttempt,
    QueuedStrayStarted,
    RunningTaskCursor,
    RunningStartedAt,
    StartedSchema,
    StartedTaskId,
    StartedKind,
    StartedPayload,
    StartedPayloadAttempt,
    StartedTimestamp,
    MissingStarted,
    DuplicateStarted,
}

async fn assert_claim_corruption_is_conflict(corruption: ClaimCorruption) {
    let store = support::seeded_store().await;
    let queued = support::queued_task(&store).await;
    let request = claim_request(&queued);
    let running_corruption = matches!(
        corruption,
        ClaimCorruption::RunningTaskCursor
            | ClaimCorruption::RunningStartedAt
            | ClaimCorruption::StartedSchema
            | ClaimCorruption::StartedTaskId
            | ClaimCorruption::StartedKind
            | ClaimCorruption::StartedPayload
            | ClaimCorruption::StartedPayloadAttempt
            | ClaimCorruption::StartedTimestamp
            | ClaimCorruption::MissingStarted
            | ClaimCorruption::DuplicateStarted
    );
    let started_event_id = if running_corruption {
        Some(applied_receipt(store.claim_task(request.clone()).await.unwrap()).started_event_id)
    } else {
        None
    };

    match corruption {
        ClaimCorruption::TaskCursor => {
            sqlx::query("UPDATE tasks SET last_event_id = last_event_id + 1000 WHERE id = ?")
                .bind(queued.id.to_string())
                .execute(store.pool())
                .await
                .unwrap();
        }
        ClaimCorruption::QueuedKind => {
            sqlx::query("UPDATE task_events SET kind = 'task.started' WHERE id = ?")
                .bind(queued.last_event_id.get())
                .execute(store.pool())
                .await
                .unwrap();
        }
        ClaimCorruption::QueuedPayload => {
            sqlx::query("UPDATE task_events SET payload_json = '{\"task\":{}}' WHERE id = ?")
                .bind(queued.last_event_id.get())
                .execute(store.pool())
                .await
                .unwrap();
        }
        ClaimCorruption::QueuedPayloadAttempt => {
            let mut corrupt = queued.clone();
            corrupt.attempt += 1;
            let payload = serde_json::to_string(&serde_json::json!({ "task": corrupt })).unwrap();
            sqlx::query("UPDATE task_events SET payload_json = ? WHERE id = ?")
                .bind(payload)
                .bind(queued.last_event_id.get())
                .execute(store.pool())
                .await
                .unwrap();
        }
        ClaimCorruption::QueuedStrayStarted => {
            sqlx::query(
                "INSERT INTO task_events (schema_version, task_id, kind, payload_json, created_at) \
                 VALUES (1, ?, 'task.started', '{}', ?)",
            )
            .bind(queued.id.to_string())
            .bind(queued.created_at.to_string())
            .execute(store.pool())
            .await
            .unwrap();
        }
        ClaimCorruption::RunningTaskCursor => {
            sqlx::query("UPDATE tasks SET last_event_id = ? WHERE id = ?")
                .bind(queued.last_event_id.get())
                .bind(queued.id.to_string())
                .execute(store.pool())
                .await
                .unwrap();
        }
        ClaimCorruption::RunningStartedAt => {
            sqlx::query("UPDATE tasks SET started_at = ? WHERE id = ?")
                .bind("2026-01-01T00:00:00Z")
                .bind(queued.id.to_string())
                .execute(store.pool())
                .await
                .unwrap();
        }
        ClaimCorruption::StartedSchema => {
            sqlx::query("UPDATE task_events SET schema_version = 2 WHERE id = ?")
                .bind(started_event_id.unwrap().get())
                .execute(store.pool())
                .await
                .unwrap();
        }
        ClaimCorruption::StartedTaskId => {
            let other = support::queued_task(&store).await;
            sqlx::query("UPDATE task_events SET task_id = ? WHERE id = ?")
                .bind(other.id.to_string())
                .bind(started_event_id.unwrap().get())
                .execute(store.pool())
                .await
                .unwrap();
        }
        ClaimCorruption::StartedKind => {
            sqlx::query("UPDATE task_events SET kind = 'task.queued' WHERE id = ?")
                .bind(started_event_id.unwrap().get())
                .execute(store.pool())
                .await
                .unwrap();
        }
        ClaimCorruption::StartedPayload => {
            sqlx::query("UPDATE task_events SET payload_json = '{}' WHERE id = ?")
                .bind(started_event_id.unwrap().get())
                .execute(store.pool())
                .await
                .unwrap();
        }
        ClaimCorruption::StartedPayloadAttempt => {
            let mut corrupt = store.task_detail(queued.id).await.unwrap().unwrap().task;
            corrupt.attempt += 1;
            let payload = serde_json::to_string(&serde_json::json!({ "task": corrupt })).unwrap();
            sqlx::query("UPDATE task_events SET payload_json = ? WHERE id = ?")
                .bind(payload)
                .bind(started_event_id.unwrap().get())
                .execute(store.pool())
                .await
                .unwrap();
        }
        ClaimCorruption::StartedTimestamp => {
            sqlx::query("UPDATE task_events SET created_at = ? WHERE id = ?")
                .bind("2026-01-01T00:00:00Z")
                .bind(started_event_id.unwrap().get())
                .execute(store.pool())
                .await
                .unwrap();
        }
        ClaimCorruption::MissingStarted => {
            sqlx::query("DELETE FROM task_events WHERE id = ?")
                .bind(started_event_id.unwrap().get())
                .execute(store.pool())
                .await
                .unwrap();
        }
        ClaimCorruption::DuplicateStarted => {
            sqlx::query(
                "INSERT INTO task_events \
                     (schema_version, task_id, kind, payload_json, created_at) \
                 SELECT schema_version, task_id, kind, payload_json, created_at \
                 FROM task_events WHERE id = ?",
            )
            .bind(started_event_id.unwrap().get())
            .execute(store.pool())
            .await
            .unwrap();
        }
    }

    let before = support::durable_task_event_snapshot(&store).await;
    assert!(matches!(
        store.claim_task(request.clone()).await.unwrap(),
        ClaimTaskOutcome::InvariantConflict
    ));
    assert!(matches!(
        store.reconcile_task_claim(&request).await.unwrap(),
        ClaimTaskReconciliationOutcome::InvariantConflict
    ));
    assert_eq!(
        support::durable_task_event_snapshot(&store).await,
        before,
        "{corruption:?}"
    );
}

#[derive(Debug, Clone, Copy)]
enum StorageClassCorruption {
    TaskAttemptBlob,
    EventSchemaReal,
    EventPayloadBlob,
}

async fn install_storage_class_corruption(
    store: &Store,
    queued: &Task,
    corruption: StorageClassCorruption,
) {
    match corruption {
        StorageClassCorruption::TaskAttemptBlob => {
            let mut connection = store.pool().acquire().await.unwrap();
            sqlx::query("PRAGMA ignore_check_constraints = ON")
                .execute(&mut *connection)
                .await
                .unwrap();
            sqlx::query("UPDATE tasks SET attempt = X'31' WHERE id = ?")
                .bind(queued.id.to_string())
                .execute(&mut *connection)
                .await
                .unwrap();
            sqlx::query("PRAGMA ignore_check_constraints = OFF")
                .execute(&mut *connection)
                .await
                .unwrap();
        }
        StorageClassCorruption::EventSchemaReal => {
            sqlx::query("UPDATE task_events SET schema_version = 1.5 WHERE id = ?")
                .bind(queued.last_event_id.get())
                .execute(store.pool())
                .await
                .unwrap();
        }
        StorageClassCorruption::EventPayloadBlob => {
            let mut connection = store.pool().acquire().await.unwrap();
            sqlx::query("PRAGMA ignore_check_constraints = ON")
                .execute(&mut *connection)
                .await
                .unwrap();
            sqlx::query(
                "UPDATE task_events SET payload_json = CAST(payload_json AS BLOB) WHERE id = ?",
            )
            .bind(queued.last_event_id.get())
            .execute(&mut *connection)
            .await
            .unwrap();
            sqlx::query("PRAGMA ignore_check_constraints = OFF")
                .execute(&mut *connection)
                .await
                .unwrap();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TerminalCorruption {
    CurrentKind,
    CurrentPayloadAttempt,
    StartedPayloadAttempt,
    StrayQueuedHistory,
}

async fn install_terminal_corruption(
    store: &Store,
    _queued: &Task,
    receipt: &ClaimTaskReceipt,
    terminal: &Task,
    corruption: TerminalCorruption,
) {
    match corruption {
        TerminalCorruption::CurrentKind => {
            sqlx::query("UPDATE task_events SET kind = 'task.cancelled' WHERE id = ?")
                .bind(terminal.last_event_id.get())
                .execute(store.pool())
                .await
                .unwrap();
        }
        TerminalCorruption::CurrentPayloadAttempt => {
            let mut corrupt = terminal.clone();
            corrupt.attempt += 1;
            let payload = serde_json::to_string(&serde_json::json!({ "task": corrupt })).unwrap();
            sqlx::query("UPDATE task_events SET payload_json = ? WHERE id = ?")
                .bind(payload)
                .bind(terminal.last_event_id.get())
                .execute(store.pool())
                .await
                .unwrap();
        }
        TerminalCorruption::StartedPayloadAttempt => {
            let mut corrupt = receipt.task.clone();
            corrupt.attempt += 1;
            let payload = serde_json::to_string(&serde_json::json!({ "task": corrupt })).unwrap();
            sqlx::query("UPDATE task_events SET payload_json = ? WHERE id = ?")
                .bind(payload)
                .bind(receipt.started_event_id.get())
                .execute(store.pool())
                .await
                .unwrap();
        }
        TerminalCorruption::StrayQueuedHistory => {}
    }
}

#[derive(Debug, Clone, Copy)]
enum ClaimFault {
    TaskUpdate,
    StartedInsert,
    LastEventUpdate,
    PayloadUpdate,
    AfterPayloadTamper,
    AfterPayloadDelete,
    DeferredCommit,
}

async fn install_claim_fault(store: &Store, fault: ClaimFault) {
    match fault {
        ClaimFault::TaskUpdate => {
            sqlx::query(
                "CREATE TRIGGER claim_fault BEFORE UPDATE OF status ON tasks \
                 WHEN NEW.status = 'running' \
                 BEGIN SELECT RAISE(ABORT, 'claim-task-update'); END",
            )
            .execute(store.pool())
            .await
            .unwrap();
        }
        ClaimFault::StartedInsert => {
            sqlx::query(
                "CREATE TRIGGER claim_fault BEFORE INSERT ON task_events \
                 WHEN NEW.kind = 'task.started' \
                 BEGIN SELECT RAISE(ABORT, 'claim-started-insert'); END",
            )
            .execute(store.pool())
            .await
            .unwrap();
        }
        ClaimFault::LastEventUpdate => {
            sqlx::query(
                "CREATE TRIGGER claim_fault BEFORE UPDATE OF last_event_id ON tasks \
                 WHEN NEW.status = 'running' \
                 BEGIN SELECT RAISE(ABORT, 'claim-last-event'); END",
            )
            .execute(store.pool())
            .await
            .unwrap();
        }
        ClaimFault::PayloadUpdate => {
            sqlx::query(
                "CREATE TRIGGER claim_fault BEFORE UPDATE OF payload_json ON task_events \
                 WHEN NEW.kind = 'task.started' \
                 BEGIN SELECT RAISE(ABORT, 'claim-payload'); END",
            )
            .execute(store.pool())
            .await
            .unwrap();
        }
        ClaimFault::AfterPayloadTamper => {
            sqlx::query(
                "CREATE TRIGGER claim_fault AFTER UPDATE OF payload_json ON task_events \
                 WHEN NEW.kind = 'task.started' \
                 BEGIN \
                     UPDATE tasks SET started_at = '2026-01-01T00:00:00Z' \
                     WHERE id = NEW.task_id; \
                 END",
            )
            .execute(store.pool())
            .await
            .unwrap();
        }
        ClaimFault::AfterPayloadDelete => {
            sqlx::query(
                "CREATE TRIGGER claim_fault AFTER UPDATE OF payload_json ON task_events \
                 WHEN NEW.kind = 'task.started' \
                 BEGIN DELETE FROM task_events WHERE id = NEW.id; END",
            )
            .execute(store.pool())
            .await
            .unwrap();
        }
        ClaimFault::DeferredCommit => {
            sqlx::raw_sql(
                "CREATE TABLE claim_fault_parent (id INTEGER PRIMARY KEY); \
                 CREATE TABLE claim_fault_child (\
                     parent_id INTEGER REFERENCES claim_fault_parent(id) \
                         DEFERRABLE INITIALLY DEFERRED\
                 ); \
                 CREATE TRIGGER claim_fault AFTER UPDATE OF payload_json ON task_events \
                 WHEN NEW.kind = 'task.started' \
                 BEGIN INSERT INTO claim_fault_child (parent_id) VALUES (1); END;",
            )
            .execute(store.pool())
            .await
            .unwrap();
        }
    }
}

async fn stage_claim_without_commit(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    queued: &Task,
) -> ClaimTaskReceipt {
    let now = support::current_timestamp();
    let updated = sqlx::query(
        "UPDATE tasks \
         SET status = 'running', started_at = ?, finished_at = NULL, failure_json = NULL \
         WHERE id = ? AND repository_id = ? AND attempt = ? \
           AND status = 'queued' AND last_event_id = ? \
           AND started_at IS NULL AND finished_at IS NULL AND failure_json IS NULL",
    )
    .bind(now.to_string())
    .bind(queued.id.to_string())
    .bind(queued.repository_id.to_string())
    .bind(i64::from(queued.attempt))
    .bind(queued.last_event_id.get())
    .execute(&mut **transaction)
    .await
    .unwrap();
    assert_eq!(updated.rows_affected(), 1);

    let inserted = sqlx::query(
        "INSERT INTO task_events (schema_version, task_id, kind, payload_json, created_at) \
         VALUES (1, ?, 'task.started', '{}', ?)",
    )
    .bind(queued.id.to_string())
    .bind(now.to_string())
    .execute(&mut **transaction)
    .await
    .unwrap();
    let started_event_id = EventId::new(inserted.last_insert_rowid()).unwrap();
    let updated = sqlx::query(
        "UPDATE tasks SET last_event_id = ? \
         WHERE id = ? AND repository_id = ? AND attempt = ? \
           AND status = 'running' AND started_at = ? \
           AND finished_at IS NULL AND failure_json IS NULL \
           AND last_event_id = ?",
    )
    .bind(started_event_id.get())
    .bind(queued.id.to_string())
    .bind(queued.repository_id.to_string())
    .bind(i64::from(queued.attempt))
    .bind(now.to_string())
    .bind(queued.last_event_id.get())
    .execute(&mut **transaction)
    .await
    .unwrap();
    assert_eq!(updated.rows_affected(), 1);

    let mut running = queued.clone();
    running.status = TaskStatus::Running;
    running.started_at = Some(now);
    running.finished_at = None;
    running.last_event_id = started_event_id;
    running.failure = None;
    let running = Task::try_from_stored(running).unwrap();
    let payload = serde_json::to_string(&serde_json::json!({ "task": &running })).unwrap();
    let updated = sqlx::query(
        "UPDATE task_events SET payload_json = ? \
         WHERE id = ? AND task_id = ? AND schema_version = 1 \
           AND kind = 'task.started' AND payload_json = '{}'",
    )
    .bind(payload)
    .bind(started_event_id.get())
    .bind(queued.id.to_string())
    .execute(&mut **transaction)
    .await
    .unwrap();
    assert_eq!(updated.rows_affected(), 1);

    ClaimTaskReceipt {
        task: running,
        started_event_id,
    }
}

async fn remove_delivery_readiness(store: &Store, event_id: EventId) {
    let updated = sqlx::query(
        "UPDATE task_events \
         SET payload_json = json_remove(payload_json, '$.task.delivery_readiness') \
         WHERE id = ?",
    )
    .bind(event_id.get())
    .execute(store.pool())
    .await
    .unwrap();
    assert_eq!(updated.rows_affected(), 1);
}

async fn install_write_rejection_triggers(store: &Store) {
    sqlx::raw_sql(
        "CREATE TRIGGER reject_task_insert BEFORE INSERT ON tasks \
             BEGIN SELECT RAISE(ABORT, 'read-only'); END; \
         CREATE TRIGGER reject_task_update BEFORE UPDATE ON tasks \
             BEGIN SELECT RAISE(ABORT, 'read-only'); END; \
         CREATE TRIGGER reject_task_delete BEFORE DELETE ON tasks \
             BEGIN SELECT RAISE(ABORT, 'read-only'); END; \
         CREATE TRIGGER reject_event_insert BEFORE INSERT ON task_events \
             BEGIN SELECT RAISE(ABORT, 'read-only'); END; \
         CREATE TRIGGER reject_event_update BEFORE UPDATE ON task_events \
             BEGIN SELECT RAISE(ABORT, 'read-only'); END; \
         CREATE TRIGGER reject_event_delete BEFORE DELETE ON task_events \
             BEGIN SELECT RAISE(ABORT, 'read-only'); END;",
    )
    .execute(store.pool())
    .await
    .unwrap();
}

fn claim_request(task: &Task) -> ClaimTaskRequest {
    ClaimTaskRequest {
        task_id: task.id,
        expected_repository_id: task.repository_id,
        expected_attempt: task.attempt,
        expected_queued_event_id: task.last_event_id,
    }
}

fn applied_receipt(outcome: ClaimTaskOutcome) -> ClaimTaskReceipt {
    match outcome {
        ClaimTaskOutcome::Applied(receipt) => receipt,
        other => panic!("claim must apply, got {other:?}"),
    }
}

fn existing_receipt(outcome: ClaimTaskOutcome) -> ClaimTaskReceipt {
    match outcome {
        ClaimTaskOutcome::ExistingApplied(receipt) => receipt,
        other => panic!("claim must be existing-applied, got {other:?}"),
    }
}

fn existing_reconciliation(outcome: ClaimTaskReconciliationOutcome) -> ClaimTaskReceipt {
    match outcome {
        ClaimTaskReconciliationOutcome::ExistingApplied(receipt) => receipt,
        other => panic!("claim must reconcile existing-applied, got {other:?}"),
    }
}

fn assert_known_not_applied(outcome: ClaimTaskReconciliationOutcome, expected: &Task) {
    assert!(matches!(
        outcome,
        ClaimTaskReconciliationOutcome::KnownNotApplied { current } if current == *expected
    ));
}

fn assert_known_not_applied_claim(outcome: ClaimTaskOutcome, expected: &Task) {
    assert!(matches!(
        outcome,
        ClaimTaskOutcome::KnownNotApplied { current } if current == *expected
    ));
}

fn applied_task(outcome: TransitionOutcome) -> Task {
    match outcome {
        TransitionOutcome::Applied { task, .. } => task,
        TransitionOutcome::Conflict { .. } => panic!("fixture transition must apply"),
    }
}

async fn raw_event(
    store: &Store,
    event_id: coding_agent_domain::EventId,
) -> (i64, i64, String, String, String, String) {
    sqlx::query_as(
        "SELECT id, schema_version, task_id, kind, payload_json, created_at \
         FROM task_events WHERE id = ?",
    )
    .bind(event_id.get())
    .fetch_one(store.pool())
    .await
    .unwrap()
}
