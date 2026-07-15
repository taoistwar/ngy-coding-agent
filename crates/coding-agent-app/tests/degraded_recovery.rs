mod support;

use coding_agent_app::{
    QuiesceResult, RunnerEvent, RunnerEventError, ServiceState, StoreWriterError, TaskManagerError,
};
use coding_agent_domain::{PlanSnapshot, TaskEventKind, TaskStatus};
use tokio::time::Instant;

#[tokio::test]
async fn terminal_write_failure_stops_claims_until_recovery() {
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
    assert_eq!(fixture.load(running).await.status, TaskStatus::Interrupted);
    assert_eq!(fixture.load(queued).await.status, TaskStatus::Interrupted);
    assert_eq!(fixture.runner.started_count(queued), 0);

    let recovery = fixture.next_recovery().await;
    assert_eq!(recovery.recovery.interrupted_count, 2);
    assert_eq!(recovery.discarded_pending_count, 1);
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
}

#[tokio::test]
async fn event_and_terminal_failures_retain_every_pending_marker() {
    let _test_guard = support::degraded_test_guard().await;
    let fixture = support::degraded_fixture_with_concurrency(2).await;
    let (event_task, event_gate) = fixture
        .start_event_task(RunnerEvent::PlanUpdated(PlanSnapshot {
            revision: 1,
            items: Vec::new(),
        }))
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

    assert_eq!(recovery.discarded_pending_count, 3);
    assert_eq!(recovery.recovery.interrupted_count, 2);
    assert_eq!(
        fixture.load(event_task).await.status,
        TaskStatus::Interrupted
    );
    assert_eq!(
        fixture.load(terminal_task).await.status,
        TaskStatus::Interrupted
    );
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
