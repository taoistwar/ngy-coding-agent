mod support;

use coding_agent_app::{
    FinalizeReviewedTaskRequest, PendingDurableResult, QuiesceResult, RunnerEvent,
    RunnerEventError, ServiceState, StoreWriterError, StoreWriterFaultPoint, StoreWriterFaultSpec,
    StoreWriterOperationKind, TaskManagerError,
};
use coding_agent_domain::{
    DeliveryReadiness, PlanSnapshot, TaskEventKind, TaskEventPayload, TaskFailure, TaskStatus,
};
use coding_agent_store::{CreateTaskOutcome, TaskTransition, TransitionOutcome};
use tokio::time::Duration;
use tokio::time::Instant;

#[tokio::test]
async fn pending_finalize_replays_before_incomplete_tasks_are_recovered() {
    let _test_guard = support::degraded_test_guard().await;
    let fixture = support::degraded_fixture_with_concurrency(1).await;
    let running = fixture.start_success_task().await;
    let queued = fixture.enqueue_task().await;

    fixture.fail_all_background_writes().await;
    fixture.finish_runner(running).await;
    fixture.wait_for_state(ServiceState::StoreDegraded).await;
    let degraded_generation = fixture.state.current().generation;
    let mut live_events = fixture.dispatcher.subscribe();

    assert!(matches!(
        fixture.manager.notify_queued(queued).await,
        Err(TaskManagerError::StoreDegraded)
    ));
    assert_eq!(fixture.load(queued).await.status, TaskStatus::Queued);

    fixture.restore_writes().await;
    fixture.wait_for_state(ServiceState::Ready).await;
    let finalized = fixture.load(running).await;
    assert_eq!(finalized.status, TaskStatus::Completed);
    assert_eq!(
        finalized.delivery_readiness,
        DeliveryReadiness::ReviewApproved
    );
    assert_eq!(fixture.load(queued).await.status, TaskStatus::Interrupted);
    assert_eq!(fixture.runner.started_count(queued), 0);

    let recovery = fixture.next_recovery().await;
    assert_eq!(recovery.recovery.interrupted_count, 1);
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
    let mut expected = vec![queued];
    expected.sort_by_key(ToString::to_string);
    assert_eq!(
        interrupted, expected,
        "Ready was visible only after live flush"
    );
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
    let terminal_task = fixture.start_success_task().await;

    fixture.fail_all_background_writes().await;
    event_gate.release.notify_one();
    fixture.finish_runner(terminal_task).await;
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

    assert_eq!(recovery.replayed_pending_count, 1);
    assert_eq!(recovery.recovery.interrupted_count, 1);
    assert_eq!(
        fixture.load(event_task).await.status,
        TaskStatus::Interrupted
    );
    let terminal = fixture.load(terminal_task).await;
    assert_eq!(terminal.status, TaskStatus::Completed);
    assert_eq!(
        terminal.delivery_readiness,
        DeliveryReadiness::ReviewApproved
    );
    assert!(
        !fixture
            .event_kinds(event_task)
            .await
            .contains(&TaskEventKind::PlanUpdated)
    );
}

#[tokio::test]
async fn pending_finalize_accepts_existing_exact_request_before_recovery() {
    let _test_guard = support::degraded_test_guard().await;
    let (fixture, writer_faults) =
        support::degraded_fixture_with_writer_faults(1, [pause_two_finalize_attempts()]).await;
    let running = fixture.start_success_task().await;
    fixture.install_non_transient_event_failure().await;

    fixture.finish_runner(running).await;
    writer_faults
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;
    assert_eq!(
        writer_faults.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    fixture.wait_for_state(ServiceState::StoreDegraded).await;
    writer_faults
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 2)
        .await;
    fixture.remove_non_transient_event_failure().await;
    let task = fixture.load(running).await;
    fixture
        .store
        .finalize_reviewed_task(
            task.id,
            task.repository_id,
            task.attempt,
            support::approved_review(),
        )
        .await
        .expect("seed the exact durable result before replay");
    assert_eq!(
        writer_faults.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );

    fixture.wait_for_state(ServiceState::Ready).await;
    let recovery = fixture.next_recovery().await;
    assert_eq!(recovery.replayed_pending_count, 1);
    assert_eq!(recovery.recovery.interrupted_count, 0);
    let task = fixture.load(running).await;
    assert_eq!(task.status, TaskStatus::Completed);
    assert_eq!(task.delivery_readiness, DeliveryReadiness::ReviewApproved);
}

#[tokio::test]
async fn typed_replay_conflict_freezes_and_retains_the_exact_pending_request() {
    let _test_guard = support::degraded_test_guard().await;
    let (fixture, writer_faults) =
        support::degraded_fixture_with_writer_faults(1, [pause_two_finalize_attempts()]).await;
    let running = fixture.start_success_task().await;
    fixture.install_non_transient_event_failure().await;

    fixture.finish_runner(running).await;
    writer_faults
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;
    assert_eq!(
        writer_faults.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    fixture.wait_for_state(ServiceState::StoreDegraded).await;
    writer_faults
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 2)
        .await;
    fixture.remove_non_transient_event_failure().await;
    let task = fixture.load(running).await;
    fixture
        .store
        .finalize_reviewed_task(
            task.id,
            task.repository_id,
            task.attempt,
            support::approved_review_round_with_summary(1, "conflicting durable approval"),
        )
        .await
        .expect("seed a conflicting durable final review");
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

    let expected = PendingDurableResult::FinalizeReviewedTask(FinalizeReviewedTaskRequest {
        task_id: task.id,
        expected_repository_id: task.repository_id,
        expected_attempt: task.attempt,
        evidence: support::approved_review(),
    });
    assert_eq!(
        fixture
            .manager
            .pending_durable_results_for_test()
            .await
            .unwrap(),
        vec![expected]
    );
    assert_eq!(fixture.state.current().state, ServiceState::StoreDegraded);
}

fn pause_two_finalize_attempts() -> StoreWriterFaultSpec {
    StoreWriterFaultSpec {
        point: StoreWriterFaultPoint::PauseBeforeExecute,
        operation: Some(StoreWriterOperationKind::FinalizeReviewedTask),
        count: 2,
    }
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
    let fixture = support::degraded_fixture_with_concurrency(1).await;
    let running = fixture.start_success_task().await;
    fixture.install_non_transient_event_failure().await;

    fixture.finish_runner(running).await;
    fixture.wait_for_state(ServiceState::StoreDegraded).await;
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
    let fixture = support::degraded_fixture_with_concurrency(1).await;
    let running = fixture.start_success_task().await;
    let mut recovery_results = fixture.manager.subscribe_degraded_recovery();
    fixture.fail_all_background_writes().await;
    fixture.finish_runner(running).await;
    fixture.wait_for_state(ServiceState::StoreDegraded).await;

    let quiesce = tokio::spawn({
        let manager = fixture.manager.clone();
        async move { manager.quiesce_and_interrupt(Instant::now()).await }
    });
    let result = quiesce.await.unwrap().unwrap();
    assert!(matches!(
        result,
        QuiesceResult::Frozen {
            error: StoreWriterError::Busy,
            ..
        }
    ));
    assert_eq!(fixture.state.current().state, ServiceState::Quiescing);

    fixture.restore_writes().await;
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert_eq!(fixture.state.current().state, ServiceState::Quiescing);
    assert!(matches!(
        recovery_results.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}
