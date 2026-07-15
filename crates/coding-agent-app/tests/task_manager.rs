mod support;

use coding_agent_app::{
    CancelOutcome, QuiesceResult, RunnerEvent, RunnerEventError, ServiceState, StoreWriterError,
    TaskManagerError,
};
use coding_agent_domain::{
    ActivityEntry, ActivityLevel, DiffSnapshot, PlanSnapshot, TaskFailure, TaskStatus, TestSnapshot,
};
use tokio::time::{Duration, Instant};

#[tokio::test]
async fn fifth_task_stays_queued_until_a_permit_is_released() {
    let fixture = support::task_manager_fixture(4).await;
    fixture.runner.push_blocking(5);
    let tasks = fixture.enqueue_tasks(5, true).await;

    fixture.wait_for_running(4).await;
    assert_eq!(fixture.load(tasks[4].id).await.status, TaskStatus::Queued);

    fixture.runner.release(tasks[0].id);
    fixture
        .wait_for_status(tasks[4].id, TaskStatus::Running)
        .await;
    assert_eq!(fixture.runner.started_count(tasks[4].id), 1);
}

#[tokio::test]
async fn running_is_never_visible_without_an_active_handle() {
    let fixture = support::task_manager_fixture(1).await;
    fixture.runner.push_blocking(32);
    for _ in 0..32 {
        let task = fixture.enqueue_tasks(1, false).await.remove(0);
        fixture.manager.notify_queued(task.id).await.unwrap();
        let response = fixture.manager.cancel(task.id).await.unwrap();
        assert!(matches!(
            response,
            CancelOutcome::Accepted { .. } | CancelOutcome::Cancelled { .. }
        ));
        fixture
            .wait_for_status(task.id, TaskStatus::Cancelled)
            .await;
        assert!(fixture.runner.started_count(task.id) <= 1);
    }
}

#[tokio::test]
async fn queued_cancel_committed_before_notification_prevents_runner_start() {
    let fixture = support::task_manager_fixture(1).await;
    fixture.runner.push_blocking(1);
    let task = fixture.enqueue_tasks(1, false).await.remove(0);

    let outcome = fixture.manager.cancel(task.id).await.unwrap();
    assert!(matches!(outcome, CancelOutcome::Cancelled { .. }));
    fixture.manager.notify_queued(task.id).await.unwrap();
    fixture.reconcile().await;

    assert_eq!(fixture.load(task.id).await.status, TaskStatus::Cancelled);
    assert_eq!(fixture.runner.started_count(task.id), 0);
}

#[tokio::test]
async fn busy_claim_cleans_provisional_state_and_reconciliation_claims_once() {
    let fixture = support::task_manager_fixture(1).await;
    fixture.runner.push_blocking(1);
    let task = fixture.enqueue_tasks(1, false).await.remove(0);
    fixture.force_claim_busy(true).await;
    fixture.manager.notify_queued(task.id).await.unwrap();
    fixture.manager.notify_queued(task.id).await.unwrap();
    assert_eq!(fixture.load(task.id).await.status, TaskStatus::Queued);
    assert_eq!(fixture.runner.started_count(task.id), 0);

    fixture.force_claim_busy(false).await;
    fixture.manager.notify_queued(task.id).await.unwrap();
    fixture.wait_for_status(task.id, TaskStatus::Running).await;
    assert_eq!(fixture.runner.started_count(task.id), 1);
}

#[tokio::test]
async fn queued_cancel_preserves_known_uncommitted_store_busy() {
    let fixture = support::task_manager_fixture(1).await;
    let task = fixture.enqueue_tasks(1, false).await.remove(0);
    fixture.force_claim_busy(true).await;

    assert!(matches!(
        fixture.manager.cancel(task.id).await,
        Err(TaskManagerError::StoreWriter(StoreWriterError::Busy))
    ));
    assert_eq!(fixture.load(task.id).await.status, TaskStatus::Queued);
    assert_eq!(fixture.state.current().state, ServiceState::Ready);
    fixture.force_claim_busy(false).await;
}

#[tokio::test]
async fn terminal_claim_failure_cleans_provisional_state_and_reconciliation_claims_once() {
    let fixture = support::task_manager_fixture(1).await;
    fixture.runner.push_blocking(1);
    fixture.fail_started_event_inserts(true).await;
    let task = fixture.enqueue_tasks(1, false).await.remove(0);
    fixture.manager.notify_queued(task.id).await.unwrap();
    fixture.manager.notify_queued(task.id).await.unwrap();
    assert_eq!(fixture.load(task.id).await.status, TaskStatus::Queued);
    assert_eq!(fixture.runner.started_count(task.id), 0);

    fixture.fail_started_event_inserts(false).await;
    fixture.manager.notify_queued(task.id).await.unwrap();
    fixture.wait_for_status(task.id, TaskStatus::Running).await;
    assert_eq!(fixture.runner.started_count(task.id), 1);
}

#[tokio::test]
async fn a_failed_fifo_head_claim_cannot_be_overtaken() {
    let fixture = support::task_manager_fixture(1).await;
    fixture.runner.push_blocking(2);
    let tasks = fixture.enqueue_tasks(2, false).await;
    fixture.fail_started_event_for(Some(tasks[0].id)).await;

    fixture.manager.notify_queued(tasks[0].id).await.unwrap();
    assert!(matches!(
        fixture
            .manager
            .cancel(coding_agent_domain::TaskId::new())
            .await,
        Err(TaskManagerError::TaskNotFound)
    ));
    assert_eq!(fixture.load(tasks[0].id).await.status, TaskStatus::Queued);
    assert_eq!(fixture.load(tasks[1].id).await.status, TaskStatus::Queued);
    assert_eq!(fixture.runner.started_count(tasks[1].id), 0);

    fixture.fail_started_event_for(None).await;
    fixture.manager.notify_queued(tasks[0].id).await.unwrap();
    fixture
        .wait_for_status(tasks[0].id, TaskStatus::Running)
        .await;
    assert_eq!(fixture.load(tasks[1].id).await.status, TaskStatus::Queued);
    fixture.runner.release(tasks[0].id);
    fixture
        .wait_for_status(tasks[1].id, TaskStatus::Running)
        .await;
}

#[tokio::test]
async fn reconciliation_claims_a_task_after_its_queue_notification_is_lost() {
    let fixture = support::task_manager_fixture(1).await;
    fixture.runner.push_blocking(1);
    let task = fixture.enqueue_tasks(1, false).await.remove(0);

    assert_eq!(fixture.load(task.id).await.status, TaskStatus::Queued);
    fixture.wait_for_status(task.id, TaskStatus::Running).await;

    assert_eq!(fixture.runner.started_count(task.id), 1);
}

#[tokio::test]
async fn queued_tasks_are_claimed_fifo_by_created_at_then_id() {
    let fixture = support::task_manager_fixture(1).await;
    fixture.runner.push_blocking(4);
    let blocker = fixture.enqueue_tasks(1, true).await.remove(0);
    fixture
        .wait_for_status(blocker.id, TaskStatus::Running)
        .await;
    let queued = fixture.enqueue_tasks(3, false).await;
    fixture
        .set_created_at(queued[0].id, "2026-07-15T00:00:02.000000000Z")
        .await;
    fixture
        .set_created_at(queued[1].id, "2026-07-15T00:00:01.000000000Z")
        .await;
    fixture
        .set_created_at(queued[2].id, "2026-07-15T00:00:01.000000000Z")
        .await;
    for task in &queued {
        fixture.manager.notify_queued(task.id).await.unwrap();
    }

    let mut expected = queued.iter().map(|task| task.id).collect::<Vec<_>>();
    expected.sort_by_key(|id| {
        let created = if *id == queued[0].id { 2 } else { 1 };
        (created, id.to_string())
    });
    let mut actual = Vec::new();
    fixture.runner.release(blocker.id);
    for expected_id in &expected {
        fixture
            .wait_for_status(*expected_id, TaskStatus::Running)
            .await;
        actual.push(*expected_id);
        fixture.runner.release(*expected_id);
    }
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn runner_event_sink_accepts_only_panel_events_and_rejects_late_events() {
    let fixture = support::task_manager_fixture(1).await;
    let late = fixture.runner.push_late_event();
    let task = fixture.enqueue_tasks(1, true).await.remove(0);
    fixture
        .wait_for_status(task.id, TaskStatus::Completed)
        .await;

    late.release.notify_one();
    let result = late.result.lock().await.take().unwrap().await.unwrap();
    assert_eq!(result, Err(RunnerEventError::TaskNotRunning));
    assert_eq!(fixture.load(task.id).await.status, TaskStatus::Completed);
}

#[tokio::test]
async fn each_bounded_sink_variant_returns_its_committed_event_id() {
    let fixture = support::task_manager_fixture(1).await;
    let events = vec![
        RunnerEvent::PlanUpdated(PlanSnapshot {
            revision: 1,
            items: Vec::new(),
        }),
        RunnerEvent::ActivityAppended(ActivityEntry {
            id: "activity-1".to_owned(),
            level: ActivityLevel::Info,
            message: "working".to_owned(),
            created_at: support::timestamp(),
        }),
        RunnerEvent::DiffUpdated(DiffSnapshot {
            revision: 1,
            files: Vec::new(),
        }),
        RunnerEvent::TestUpdated(TestSnapshot {
            revision: 1,
            status: coding_agent_domain::TestStatus::Passed,
            cases: Vec::new(),
        }),
    ];
    let results = fixture.runner.push_events(events);
    let task = fixture.enqueue_tasks(1, true).await.remove(0);
    fixture
        .wait_for_status(task.id, TaskStatus::Completed)
        .await;

    let ids = results.receive().await;
    assert_eq!(ids.len(), 4);
    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
}

#[tokio::test]
async fn completed_and_cancelled_are_decided_by_the_first_terminal_commit() {
    let completion_wins = support::task_manager_fixture(1).await;
    let completion = completion_wins.runner.push_completion_gate();
    let task = completion_wins.enqueue_tasks(1, true).await.remove(0);
    completion_wins
        .wait_for_status(task.id, TaskStatus::Running)
        .await;
    completion.release.notify_one();
    completion_wins
        .wait_for_status(task.id, TaskStatus::Completed)
        .await;
    assert!(matches!(
        completion_wins.manager.cancel(task.id).await,
        Err(TaskManagerError::TaskNotCancellable { .. })
    ));

    let cancel_wins = support::task_manager_fixture(1).await;
    cancel_wins.runner.push_blocking(1);
    let task = cancel_wins.enqueue_tasks(1, true).await.remove(0);
    cancel_wins
        .wait_for_status(task.id, TaskStatus::Running)
        .await;
    assert!(matches!(
        cancel_wins.manager.cancel(task.id).await.unwrap(),
        CancelOutcome::Accepted { .. }
    ));
    cancel_wins
        .wait_for_status(task.id, TaskStatus::Cancelled)
        .await;
    assert_eq!(
        cancel_wins.load(task.id).await.status,
        TaskStatus::Cancelled
    );
}

#[tokio::test]
async fn runner_panic_becomes_failed_and_does_not_affect_another_task() {
    let fixture = support::task_manager_fixture(2).await;
    fixture.runner.push_panic();
    fixture.runner.push_blocking(1);
    let tasks = fixture.enqueue_tasks(2, true).await;

    fixture
        .wait_for_status(tasks[0].id, TaskStatus::Failed)
        .await;
    fixture
        .wait_for_status(tasks[1].id, TaskStatus::Running)
        .await;
    assert_eq!(
        fixture.load(tasks[0].id).await.failure,
        Some(TaskFailure {
            code: "RUNNER_PANICKED".to_owned(),
            message: "task runner panicked".to_owned(),
            retryable: false,
        })
    );
    assert_eq!(fixture.runner.started_count(tasks[1].id), 1);
}

#[tokio::test]
async fn durable_quiesce_interrupts_incomplete_tasks_and_returns_live_handles() {
    let fixture = support::task_manager_fixture(1).await;
    fixture.runner.push_blocking(1);
    let tasks = fixture.enqueue_tasks(2, true).await;
    fixture
        .wait_for_status(tasks[0].id, TaskStatus::Running)
        .await;

    let result = fixture
        .manager
        .quiesce_and_interrupt(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    let (recovery, active) = match result {
        QuiesceResult::Durable { recovery, active } => (recovery, active),
        QuiesceResult::Frozen { .. } => panic!("quiesce should commit"),
    };
    assert_eq!(recovery.interrupted_count, 2);
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].task_id, tasks[0].id);
    assert_eq!(
        fixture.load(tasks[0].id).await.status,
        TaskStatus::Interrupted
    );
    assert_eq!(
        fixture.load(tasks[1].id).await.status,
        TaskStatus::Interrupted
    );
    assert_eq!(fixture.state.current().state, ServiceState::Quiescing);

    let active = active.into_iter().next().unwrap();
    active.cancellation.cancel();
    active.done.await.unwrap();
    assert!(matches!(
        fixture.manager.notify_queued(tasks[1].id).await,
        Err(TaskManagerError::Frozen)
    ));
}

#[tokio::test]
async fn failed_quiesce_is_frozen_and_returns_the_same_active_handles() {
    let fixture = support::task_manager_fixture(1).await;
    fixture.runner.push_blocking(1);
    let tasks = fixture.enqueue_tasks(2, true).await;
    fixture
        .wait_for_status(tasks[0].id, TaskStatus::Running)
        .await;

    let result = fixture
        .manager
        .quiesce_and_interrupt(Instant::now())
        .await
        .unwrap();
    let active = match result {
        QuiesceResult::Frozen { active, error } => {
            assert!(matches!(error, StoreWriterError::Busy));
            active
        }
        QuiesceResult::Durable { .. } => panic!("expired deadline must not commit"),
    };
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].task_id, tasks[0].id);
    assert_eq!(fixture.load(tasks[0].id).await.status, TaskStatus::Running);
    assert_eq!(fixture.load(tasks[1].id).await.status, TaskStatus::Queued);
    assert!(matches!(
        fixture.manager.cancel(tasks[0].id).await,
        Err(TaskManagerError::Frozen)
    ));
}

#[tokio::test]
async fn a_degraded_service_rejects_runner_events_without_persistence() {
    let fixture = support::task_manager_fixture(1).await;
    let append = fixture
        .runner
        .push_event_gate(RunnerEvent::PlanUpdated(PlanSnapshot {
            revision: 7,
            items: Vec::new(),
        }));
    let task = fixture.enqueue_tasks(1, true).await.remove(0);
    fixture.wait_for_status(task.id, TaskStatus::Running).await;
    fixture.state.set(ServiceState::StoreDegraded).unwrap();
    append.release.notify_one();

    assert_eq!(
        append.result.await.unwrap(),
        Err(RunnerEventError::StoreDegraded)
    );
    assert!(fixture.load_detail(task.id).await.plan.is_none());
}

#[tokio::test]
async fn a_degraded_service_does_not_claim_queued_tasks() {
    let fixture = support::task_manager_fixture(1).await;
    fixture.runner.push_blocking(1);
    fixture.state.set(ServiceState::StoreDegraded).unwrap();
    let task = fixture.enqueue_tasks(1, false).await.remove(0);

    assert!(matches!(
        fixture.manager.notify_queued(task.id).await,
        Err(TaskManagerError::StoreDegraded)
    ));
    assert_eq!(fixture.load(task.id).await.status, TaskStatus::Queued);
    assert_eq!(fixture.runner.started_count(task.id), 0);

    fixture.state.set(ServiceState::Ready).unwrap();
    fixture.manager.notify_queued(task.id).await.unwrap();
    fixture.wait_for_status(task.id, TaskStatus::Running).await;
}
