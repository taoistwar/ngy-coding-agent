#![cfg(feature = "test-support")]

mod support;

use std::num::NonZeroU64;

use coding_agent_app::{
    DurableOperationKind, MutationSequence, PendingDurableResult, QuiesceResult,
    RecordReviewRequest, RunnerEvent, RunnerEventError, ServiceState, StoreWriterError,
    StoreWriterFaultPoint, StoreWriterFaultSpec, StoreWriterOperationKind, TaskManagerError,
    TaskMutationIdentity,
};
use coding_agent_domain::{
    DeliveryReadiness, PlanSnapshot, TaskEventKind, TaskEventPayload, TaskFailure, TaskStatus,
};
use coding_agent_store::{CreateTaskOutcome, TaskTransition, TransitionOutcome};
use tokio::time::Duration;
use tokio::time::Instant;

#[tokio::test]
async fn pending_review_replays_before_incomplete_tasks_are_recovered() {
    let _test_guard = support::degraded_test_guard().await;
    let (fixture, writer_faults) =
        support::degraded_fixture_with_writer_faults(1, pending_review_replay_faults()).await;
    let evidence = support::changes_requested_review(1);
    let (running, review_gate) = fixture.start_review_task(evidence.clone()).await;
    let queued = fixture.enqueue_task().await;

    review_gate.release.notify_one();
    for attempt in 1..=2 {
        writer_faults
            .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, attempt)
            .await;
        assert_eq!(
            writer_faults.release(StoreWriterFaultPoint::PauseBeforeExecute),
            1
        );
    }
    fixture.wait_for_state(ServiceState::StoreDegraded).await;
    writer_faults
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 3)
        .await;
    let degraded_generation = fixture.state.current().generation;
    let mut live_events = fixture.dispatcher.subscribe();

    assert!(matches!(
        fixture.manager.notify_queued(queued).await,
        Err(TaskManagerError::StoreDegraded)
    ));
    assert_eq!(fixture.load(queued).await.status, TaskStatus::Queued);

    assert_eq!(
        writer_faults.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    fixture.wait_for_state(ServiceState::Ready).await;
    assert!(
        review_gate.result.await.unwrap().is_ok(),
        "the typed replay must resolve the original RecordReview caller"
    );
    let recovered = fixture.load(running).await;
    assert_eq!(recovered.status, TaskStatus::Interrupted);
    assert_eq!(recovered.delivery_readiness, DeliveryReadiness::Unreviewed);
    assert_eq!(fixture.load(queued).await.status, TaskStatus::Interrupted);
    assert_eq!(fixture.runner.started_count(queued), 0);

    let recovery = fixture.next_recovery().await;
    assert_eq!(recovery.recovery.interrupted_count, 2);
    assert_eq!(recovery.replayed_pending_count, 1);
    assert_eq!(
        recovery.ready_generation,
        fixture.state.current().generation
    );
    assert!(recovery.ready_generation > degraded_generation);
    let mut interrupted = Vec::new();
    while let Ok(event) = live_events.try_recv() {
        if event.payload.kind() == TaskEventKind::TaskInterrupted {
            interrupted.push(event.task_id);
        }
    }
    interrupted.sort_by_key(ToString::to_string);
    let mut expected = vec![running, queued];
    expected.sort_by_key(ToString::to_string);
    assert_eq!(
        interrupted, expected,
        "Ready was visible only after live flush"
    );
    let detail = fixture.store.task_detail(running).await.unwrap().unwrap();
    assert_eq!(detail.reviews.len(), 1);
    assert_eq!(detail.reviews[0].verdict(), evidence.verdict());
}

#[tokio::test]
async fn persistent_reviewed_terminal_ambiguity_freezes_and_retains_runner_and_permit() {
    let _test_guard = support::degraded_test_guard().await;
    let (fixture, writer_faults) = support::degraded_fixture_with_writer_faults(
        1,
        [StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseBeforeExecute,
            operation: Some(StoreWriterOperationKind::FinalizeReviewedTask),
            count: 2,
        }],
    )
    .await;
    let running = fixture.start_success_task().await;
    let queued = fixture.enqueue_task().await;
    fixture.install_non_transient_event_failure().await;

    fixture.finish_runner(running).await;
    for attempt in 1..=2 {
        writer_faults
            .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, attempt)
            .await;
        assert_eq!(
            writer_faults.release(StoreWriterFaultPoint::PauseBeforeExecute),
            1
        );
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if matches!(
                fixture.manager.notify_queued(queued).await,
                Err(TaskManagerError::Frozen)
            ) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("persistent reviewed-terminal ambiguity freezes the manager");

    assert!(matches!(
        fixture.manager.safety_snapshot_for_test().await,
        Err(TaskManagerError::Frozen)
    ));
    assert_eq!(fixture.state.current().state, ServiceState::StoreDegraded);
    assert_eq!(fixture.load(running).await.status, TaskStatus::Running);
    assert_eq!(fixture.load(queued).await.status, TaskStatus::Queued);
    assert_eq!(fixture.runner.started_count(queued), 0);
    assert_eq!(
        fixture
            .manager
            .pending_durable_results_for_test()
            .await
            .unwrap(),
        Vec::new(),
        "ambiguous typed terminal ownership is retained in-place, not replayed as a generic pending write"
    );
}

#[tokio::test]
async fn cleanup_unproven_runner_blocks_multi_active_degraded_recovery_and_retains_all_permits() {
    let _test_guard = support::degraded_test_guard().await;
    let fixture = support::degraded_fixture_with_concurrency(2).await;
    let (cleanup_task, cleanup_gate) = fixture.start_cleanup_unproven_task().await;
    let (event_task, event_gate) = fixture
        .start_event_task(RunnerEvent::PlanUpdated(PlanSnapshot::legacy(
            1,
            Vec::new(),
        )))
        .await;

    fixture.fail_all_background_writes().await;
    event_gate.release.notify_one();
    fixture.wait_for_state(ServiceState::StoreDegraded).await;
    assert_eq!(
        event_gate.result.await.unwrap(),
        Err(RunnerEventError::StoreDegraded)
    );
    cleanup_gate.release.notify_one();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let snapshot = fixture.manager.safety_snapshot_for_test().await.unwrap();
            if snapshot.active_count == 2 && snapshot.recovery_release_ready_count == 1 {
                assert_eq!(snapshot.available_permits, 0);
                assert!(!snapshot.degraded_recovery_running);
                assert!(fixture.runner.cleanup_tree_is_held(cleanup_task));
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cleanup-unproven result keeps the coordinator blocked");

    fixture.restore_writes().await;
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    let snapshot = fixture.manager.safety_snapshot_for_test().await.unwrap();
    assert_eq!(snapshot.active_count, 2);
    assert_eq!(snapshot.recovery_release_ready_count, 1);
    assert_eq!(snapshot.available_permits, 0);
    assert!(!snapshot.degraded_recovery_running);
    assert_eq!(fixture.state.current().state, ServiceState::StoreDegraded);
    assert!(fixture.runner.cleanup_tree_is_held(cleanup_task));

    assert!(fixture.runner.release_cleanup_tree(cleanup_task));
    assert!(!fixture.runner.cleanup_tree_is_held(cleanup_task));
    let recovery = tokio::time::timeout(Duration::from_secs(5), fixture.next_recovery())
        .await
        .expect("exact cleanup-tree release starts generic recovery");
    fixture.wait_for_state(ServiceState::Ready).await;
    assert_eq!(recovery.replayed_pending_count, 0);
    assert_eq!(recovery.recovery.interrupted_count, 2);

    let released = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let snapshot = fixture.manager.safety_snapshot_for_test().await.unwrap();
            if snapshot.active_count == 0
                && snapshot.recovery_release_ready_count == 0
                && snapshot.available_permits == 2
                && !snapshot.degraded_recovery_running
            {
                return snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("generic recovery releases both exact active-task permits");
    assert_eq!(released.active_count, 0);
    assert_eq!(released.recovery_release_ready_count, 0);
    assert_eq!(released.available_permits, 2);
    assert!(!released.degraded_recovery_running);
    assert_eq!(
        fixture.load(cleanup_task).await.status,
        TaskStatus::Interrupted
    );
    assert_eq!(
        fixture.load(event_task).await.status,
        TaskStatus::Interrupted
    );
    assert!(!fixture.runner.cleanup_tree_is_held(cleanup_task));
}

#[tokio::test]
async fn ordinary_event_failure_is_not_replayed_as_quality_pending() {
    let _test_guard = support::degraded_test_guard().await;
    let fixture = support::degraded_fixture_with_concurrency(2).await;
    let (event_task, event_gate) = fixture
        .start_event_task(RunnerEvent::PlanUpdated(PlanSnapshot::legacy(
            1,
            Vec::new(),
        )))
        .await;

    fixture.fail_all_background_writes().await;
    event_gate.release.notify_one();
    fixture.wait_for_state(ServiceState::StoreDegraded).await;
    assert_eq!(
        event_gate.result.await.unwrap(),
        Err(RunnerEventError::StoreDegraded)
    );
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }

    fixture.restore_writes().await;
    fixture.wait_for_state(ServiceState::Ready).await;
    let recovery = fixture.next_recovery().await;

    assert_eq!(recovery.replayed_pending_count, 0);
    assert_eq!(recovery.recovery.interrupted_count, 1);
    assert_eq!(
        fixture.load(event_task).await.status,
        TaskStatus::Interrupted
    );
    assert!(
        !fixture
            .event_kinds(event_task)
            .await
            .contains(&TaskEventKind::PlanUpdated)
    );
}

#[tokio::test]
async fn pending_review_accepts_existing_exact_request_before_recovery() {
    let _test_guard = support::degraded_test_guard().await;
    let (fixture, writer_faults) =
        support::degraded_fixture_with_writer_faults(1, pending_review_replay_faults()).await;
    let evidence = support::changes_requested_review(1);
    let (running, review_gate) = fixture.start_review_task(evidence.clone()).await;

    review_gate.release.notify_one();
    for attempt in 1..=2 {
        writer_faults
            .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, attempt)
            .await;
        assert_eq!(
            writer_faults.release(StoreWriterFaultPoint::PauseBeforeExecute),
            1
        );
    }
    fixture.wait_for_state(ServiceState::StoreDegraded).await;
    writer_faults
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 3)
        .await;
    let task = fixture.load(running).await;
    fixture
        .store
        .record_review(task.id, task.repository_id, task.attempt, evidence.clone())
        .await
        .expect("seed the exact review before replay");
    assert_eq!(
        writer_faults.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );

    fixture.wait_for_state(ServiceState::Ready).await;
    let recovery = fixture.next_recovery().await;
    assert_eq!(recovery.replayed_pending_count, 1);
    assert_eq!(recovery.recovery.interrupted_count, 1);
    let task = fixture.load(running).await;
    assert_eq!(task.status, TaskStatus::Interrupted);
    assert_eq!(task.delivery_readiness, DeliveryReadiness::Unreviewed);
    assert!(
        review_gate.result.await.unwrap().is_ok(),
        "an exact Existing replay must resolve the original RecordReview caller"
    );
    let detail = fixture.store.task_detail(running).await.unwrap().unwrap();
    assert_eq!(detail.reviews.len(), 1);
    assert_eq!(detail.reviews[0].verdict(), evidence.verdict());
}

#[tokio::test]
async fn typed_replay_conflict_freezes_and_retains_the_exact_pending_request() {
    let _test_guard = support::degraded_test_guard().await;
    let (fixture, writer_faults) =
        support::degraded_fixture_with_writer_faults(1, pending_review_replay_faults()).await;
    let evidence = support::changes_requested_review(1);
    let (running, review_gate) = fixture.start_review_task(evidence.clone()).await;

    review_gate.release.notify_one();
    for attempt in 1..=2 {
        writer_faults
            .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, attempt)
            .await;
        assert_eq!(
            writer_faults.release(StoreWriterFaultPoint::PauseBeforeExecute),
            1
        );
    }
    fixture.wait_for_state(ServiceState::StoreDegraded).await;
    writer_faults
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 3)
        .await;
    let task = fixture.load(running).await;
    fixture
        .store
        .record_review(
            task.id,
            task.repository_id,
            task.attempt,
            support::changes_requested_review_with_summary(1, "conflicting durable review"),
        )
        .await
        .expect("seed a conflicting durable review");
    assert_eq!(
        writer_faults.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if matches!(
                fixture.manager.notify_queued(task.id).await,
                Err(TaskManagerError::Frozen)
            ) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("typed conflict must freeze the manager");

    let expected = PendingDurableResult::RecordReview {
        identity: TaskMutationIdentity {
            task_id: task.id,
            sequence: MutationSequence::new(NonZeroU64::new(3).unwrap()),
            kind: DurableOperationKind::RecordReview,
        },
        request: RecordReviewRequest {
            task_id: task.id,
            expected_repository_id: task.repository_id,
            expected_attempt: task.attempt,
            evidence,
        },
    };
    assert_eq!(
        fixture
            .manager
            .pending_durable_results_for_test()
            .await
            .unwrap(),
        vec![expected]
    );
    assert!(matches!(
        fixture.manager.safety_snapshot_for_test().await,
        Err(TaskManagerError::Frozen)
    ));
    assert_eq!(fixture.state.current().state, ServiceState::StoreDegraded);
    assert_eq!(
        fixture.load(running).await.status,
        TaskStatus::Running,
        "a typed replay conflict retains the active durable membership"
    );
    assert_eq!(
        review_gate.result.await.unwrap(),
        Err(RunnerEventError::StoreDegraded)
    );
}

#[tokio::test]
async fn deterministic_quality_failure_freezes_without_manufacturing_pending_replay() {
    let _test_guard = support::degraded_test_guard().await;
    let (fixture, _) = support::degraded_fixture_with_writer_faults(
        1,
        [StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::FailBeforeExecute,
            operation: Some(StoreWriterOperationKind::FinalizeReviewedTask),
            count: 1,
        }],
    )
    .await;
    let running = fixture.start_success_task().await;

    fixture.finish_runner(running).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if matches!(
                fixture.manager.notify_queued(running).await,
                Err(TaskManagerError::Frozen)
            ) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("a deterministic quality invariant must freeze the manager");

    assert!(
        fixture
            .manager
            .pending_durable_results_for_test()
            .await
            .unwrap()
            .is_empty(),
        "a deterministic failure must not be converted into an unknown replay"
    );
    assert!(matches!(
        fixture.manager.safety_snapshot_for_test().await,
        Err(TaskManagerError::Frozen)
    ));
    assert_eq!(fixture.load(running).await.status, TaskStatus::Running);
}

#[tokio::test]
async fn repeated_known_busy_uses_real_new_sequences_then_freezes_without_pending() {
    let _test_guard = support::degraded_test_guard().await;
    let (fixture, controller) = support::degraded_fixture_with_writer_faults(
        1,
        [StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::BusyBeforeExecute,
            operation: Some(StoreWriterOperationKind::FinalizeReviewedTask),
            count: 12,
        }],
    )
    .await;
    let running = fixture.start_success_task().await;

    fixture.finish_runner(running).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if matches!(
                fixture.manager.notify_queued(running).await,
                Err(TaskManagerError::Frozen)
            ) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the second known-not-applied reviewed terminal result freezes ownership");

    assert_eq!(fixture.state.current().state, ServiceState::StoreDegraded);
    assert_eq!(fixture.load(running).await.status, TaskStatus::Running);
    assert!(matches!(
        fixture.manager.safety_snapshot_for_test().await,
        Err(TaskManagerError::Frozen)
    ));
    assert!(
        fixture
            .manager
            .pending_durable_results_for_test()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::BusyBeforeExecute,
            StoreWriterOperationKind::FinalizeReviewedTask,
        ),
        12,
        "both sequence identities must enter the real writer retry path"
    );
}

#[tokio::test]
async fn cold_degraded_recovery_batch_releases_only_after_every_runner_is_process_clean() {
    let _test_guard = support::degraded_test_guard().await;
    let (fixture, controller) = support::degraded_fixture_with_writer_faults(
        2,
        [StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::BusyBeforeExecute,
            operation: Some(StoreWriterOperationKind::RecordReview),
            count: 12,
        }],
    )
    .await;
    let (first, first_gate) = fixture
        .start_review_task(support::changes_requested_review(1))
        .await;
    let second = fixture.start_success_task().await;

    first_gate.release.notify_one();
    assert_eq!(
        first_gate
            .result
            .await
            .expect("review gate reports its result"),
        Err(RunnerEventError::StoreDegraded)
    );
    fixture.wait_for_state(ServiceState::StoreDegraded).await;
    let blocked = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = fixture.manager.safety_snapshot_for_test().await.unwrap();
            if snapshot.recovery_release_ready_count == 1 {
                return snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first runner becomes process-clean while recovery remains blocked");
    assert_eq!(blocked.active_count, 2);
    assert_eq!(blocked.recovery_release_ready_count, 1);
    assert_eq!(blocked.available_permits, 0);
    assert!(!blocked.degraded_recovery_running);

    fixture.finish_runner(second).await;
    let recovery = fixture.next_recovery().await;
    fixture.wait_for_state(ServiceState::Ready).await;
    assert_eq!(recovery.replayed_pending_count, 0);
    assert_eq!(recovery.recovery.interrupted_count, 2);
    assert_eq!(fixture.load(first).await.status, TaskStatus::Interrupted);
    assert_eq!(fixture.load(second).await.status, TaskStatus::Interrupted);
    let released = fixture.manager.safety_snapshot_for_test().await.unwrap();
    assert_eq!(released.active_count, 0);
    assert_eq!(released.recovery_release_ready_count, 0);
    assert_eq!(released.available_permits, 2);
    assert!(!released.degraded_recovery_running);
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::BusyBeforeExecute,
            StoreWriterOperationKind::RecordReview,
        ),
        12
    );
}

fn pending_review_replay_faults() -> [StoreWriterFaultSpec; 2] {
    [
        StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseBeforeExecute,
            operation: Some(StoreWriterOperationKind::RecordReview),
            count: 3,
        },
        StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::FailUnknownBeforeExecute,
            operation: Some(StoreWriterOperationKind::RecordReview),
            count: 2,
        },
    ]
}

#[tokio::test]
async fn cold_recovery_interrupts_running_task_but_preserves_intermediate_review() {
    let fixture = support::store_fixture().await;
    let queued = match fixture
        .store
        .create_task(support::new_task(
            fixture.repository.id,
            "cold recovery with review",
        ))
        .await
        .unwrap()
    {
        CreateTaskOutcome::Created { task, .. } | CreateTaskOutcome::Existing { task } => task,
    };
    let running = match fixture
        .store
        .transition_with_event(queued.id, TaskStatus::Queued, TaskTransition::Running)
        .await
        .unwrap()
    {
        TransitionOutcome::Applied { task, .. } => task,
        TransitionOutcome::Conflict { .. } => panic!("cold recovery task must start"),
    };
    fixture
        .store
        .append_running_event(
            running.id,
            TaskEventPayload::PlanUpdated {
                plan: support::fixture_review_plan(),
            },
        )
        .await
        .unwrap();
    fixture
        .store
        .record_review(
            running.id,
            running.repository_id,
            running.attempt,
            support::changes_requested_review(1),
        )
        .await
        .unwrap();

    fixture
        .store
        .recover_incomplete(
            support::timestamp(),
            TaskFailure {
                code: "APP_RESTARTED".to_owned(),
                message: "task was interrupted because the application restarted".to_owned(),
                retryable: true,
            },
        )
        .await
        .unwrap();

    let detail = fixture
        .store
        .task_detail(running.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(detail.task.status, TaskStatus::Interrupted);
    assert_eq!(
        detail.task.delivery_readiness,
        DeliveryReadiness::Unreviewed
    );
    assert_eq!(detail.reviews.len(), 1);
}

#[tokio::test]
async fn non_transient_recovery_failure_stays_degraded_without_recreating_the_database() {
    let _test_guard = support::degraded_test_guard().await;
    let (fixture, _) = support::degraded_fixture_with_writer_faults(
        1,
        [StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::FailBeforeExecute,
            operation: Some(StoreWriterOperationKind::AppendRunningEvent),
            count: 1,
        }],
    )
    .await;
    let (running, event_gate) = fixture
        .start_event_task(RunnerEvent::PlanUpdated(PlanSnapshot::legacy(
            1,
            Vec::new(),
        )))
        .await;
    fixture.install_non_transient_event_failure().await;

    event_gate.release.notify_one();
    fixture.wait_for_state(ServiceState::StoreDegraded).await;
    assert_eq!(
        event_gate.result.await.unwrap(),
        Err(RunnerEventError::StoreDegraded)
    );
    let degraded_generation = fixture.state.current().generation;
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }

    assert_eq!(fixture.state.current().state, ServiceState::StoreDegraded);
    assert_eq!(fixture.state.current().generation, degraded_generation);
    assert_eq!(fixture.load(running).await.status, TaskStatus::Running);
    assert!(fixture.database_path().is_file());
    let trigger_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name = 'fail_degraded_events'",
    )
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(trigger_count, 1, "the existing database was preserved");
}

#[tokio::test]
async fn known_uncommitted_foreground_busy_does_not_enter_degraded_recovery() {
    let _test_guard = support::degraded_test_guard().await;
    let fixture = support::degraded_fixture_with_concurrency(1).await;
    let running = fixture.start_success_task().await;
    let queued = fixture.enqueue_task().await;
    fixture.fail_all_background_writes().await;

    let cancel = tokio::spawn({
        let manager = fixture.manager.clone();
        async move { manager.cancel(queued).await }
    });
    let error = cancel.await.unwrap().unwrap_err();

    assert!(
        matches!(error, TaskManagerError::StoreWriter(StoreWriterError::Busy)),
        "unexpected foreground error: {error:?}"
    );
    assert_eq!(fixture.state.current().state, ServiceState::Ready);
    assert_eq!(fixture.state.current().generation, 0);
    assert_eq!(fixture.load(queued).await.status, TaskStatus::Queued);
    assert_eq!(fixture.load(running).await.status, TaskStatus::Running);
    fixture.restore_writes().await;
}

#[tokio::test]
async fn quiescing_supersedes_recovery_and_never_publishes_ready() {
    let _test_guard = support::degraded_test_guard().await;
    let (fixture, _) = support::degraded_fixture_with_writer_faults(
        1,
        [StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::FailBeforeExecute,
            operation: Some(StoreWriterOperationKind::AppendRunningEvent),
            count: 1,
        }],
    )
    .await;
    let (_running, event_gate) = fixture
        .start_event_task(RunnerEvent::PlanUpdated(PlanSnapshot::legacy(
            1,
            Vec::new(),
        )))
        .await;
    let mut recovery_results = fixture.manager.subscribe_degraded_recovery();
    fixture.install_non_transient_event_failure().await;
    event_gate.release.notify_one();
    fixture.wait_for_state(ServiceState::StoreDegraded).await;
    assert_eq!(
        event_gate.result.await.unwrap(),
        Err(RunnerEventError::StoreDegraded)
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if fixture
                .manager
                .safety_snapshot_for_test()
                .await
                .is_ok_and(|snapshot| snapshot.degraded_recovery_running)
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cold recovery is retrying before quiescing supersedes it");

    let quiesce = tokio::spawn({
        let manager = fixture.manager.clone();
        async move { manager.quiesce_and_interrupt(Instant::now()).await }
    });
    let result = quiesce.await.unwrap().unwrap();
    assert!(matches!(
        result,
        QuiesceResult::Frozen {
            error: StoreWriterError::DeadlineElapsed,
            ..
        }
    ));
    assert_eq!(fixture.state.current().state, ServiceState::Quiescing);

    fixture.remove_non_transient_event_failure().await;
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert_eq!(fixture.state.current().state, ServiceState::Quiescing);
    assert!(matches!(
        recovery_results.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}
