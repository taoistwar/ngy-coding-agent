#![cfg(feature = "test-support")]

mod support;

use coding_agent_app::{
    CancelOutcome, FakeRunnerConfig, QuiesceResult, RunnerEvent, RunnerEventError,
    SchedulerRepositoryStorageState, SchedulerStorageNotification, ServiceState, StorageState,
    StoreWriterError, TaskManagerError,
};
#[cfg(feature = "test-support")]
use coding_agent_app::{
    FakeScenario, StoreWriterFaultPoint, StoreWriterFaultSpec, StoreWriterOperationKind,
    TaskManagerSafetySnapshot,
};
use coding_agent_domain::{
    ActivityEntry, ActivityLevel, DiffFile, DiffFileStatus, DiffSnapshot, PlanItemStatus,
    PlanSnapshot, TaskEventKind, TaskFailure, TaskId, TaskStatus, TestCase, TestSnapshot,
    TestStatus,
};
use coding_agent_store::{StopIntentKind, TaskTransition, TransitionOutcome};
use tokio::time::{Duration, Instant};

#[cfg(feature = "test-support")]
async fn wait_for_safety_snapshot(
    fixture: &support::TaskManagerFixture,
    active_count: usize,
    available_permits: usize,
) -> TaskManagerSafetySnapshot {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = fixture
                .manager
                .safety_snapshot_for_test()
                .await
                .expect("inspect task-manager safety");
            if snapshot.active_count == active_count
                && snapshot.available_permits == available_permits
            {
                return snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "task-manager safety did not reach active={active_count}, available={available_permits}"
        )
    })
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn typed_claim_unknown_is_read_only_and_does_not_spawn() {
    let (fixture, writer_faults) = support::task_manager_fixture_with_writer_faults(
        1,
        1,
        [
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::FailUnknownBeforeExecute,
                operation: Some(StoreWriterOperationKind::StartTask),
                count: 1,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseBeforeExecute,
                operation: Some(StoreWriterOperationKind::ReconcileClaimTask),
                count: 1,
            },
        ],
    )
    .await;
    fixture.runner.push_blocking(1);
    let task = fixture.enqueue_tasks(1, false).await.remove(0);

    fixture.manager.notify_queued(task.id).await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        writer_faults.wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1),
    )
    .await
    .expect("unknown claim reaches the exact read-only reconciliation");

    assert_eq!(fixture.load(task.id).await.status, TaskStatus::Queued);
    assert_eq!(fixture.runner.started_count(task.id), 0);
    let retained = fixture.manager.safety_snapshot_for_test().await.unwrap();
    assert_eq!(retained.active_count, 1);
    assert_eq!(retained.available_permits, 0);

    fixture.state.set(ServiceState::StoreDegraded).unwrap();
    assert_eq!(
        writer_faults.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    wait_for_safety_snapshot(&fixture, 0, 1).await;

    assert_eq!(fixture.load(task.id).await.status, TaskStatus::Queued);
    assert_eq!(fixture.runner.started_count(task.id), 0);
    assert_eq!(
        fixture.event_kinds(task.id).await,
        vec![TaskEventKind::TaskQueued]
    );

    fixture.state.set(ServiceState::Ready).unwrap();
    fixture.manager.notify_queued(task.id).await.unwrap();
    assert_eq!(fixture.runner.wait_for_started_task(0).await, task.id);
    assert_eq!(fixture.runner.started_count(task.id), 1);
    fixture.runner.release(task.id).await;
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn global_two_allows_two_same_repository_roles_to_overlap_and_globally_blocks_the_rest() {
    let (fixture, other_repository) =
        support::gated_two_repository_task_manager_fixture(2, 2).await;
    fixture.runner.push_blocking(4);
    let same_repository = fixture
        .enqueue_tasks_for_repository(fixture.repository.id, "same repository", 3)
        .await;
    for (task, timestamp) in same_repository.iter().zip([
        "2026-07-15T00:00:01.000000000Z",
        "2026-07-15T00:00:02.000000000Z",
        "2026-07-15T00:00:03.000000000Z",
    ]) {
        fixture.set_created_at(task.id, timestamp).await;
    }
    fixture.state.set(ServiceState::Ready).unwrap();
    fixture
        .manager
        .notify_queued(same_repository[0].id)
        .await
        .unwrap();

    let first = fixture.runner.wait_for_started_task(0).await;
    let second = fixture.runner.wait_for_started_task(1).await;
    assert_eq!(
        [first, second],
        [same_repository[0].id, same_repository[1].id]
    );
    assert_eq!(
        fixture.load(same_repository[2].id).await.status,
        TaskStatus::Queued
    );

    fixture.state.set(ServiceState::StoreDegraded).unwrap();
    let other = fixture
        .enqueue_tasks_for_repository(other_repository.id, "other repository", 1)
        .await;
    fixture
        .set_created_at(other[0].id, "2026-07-15T00:00:04.000000000Z")
        .await;
    fixture.state.set(ServiceState::Ready).unwrap();
    fixture.manager.notify_queued(other[0].id).await.unwrap();

    assert_eq!(fixture.load(other[0].id).await.status, TaskStatus::Queued);
    let snapshot = wait_for_safety_snapshot(&fixture, 2, 0).await;
    assert_eq!(snapshot.recovery_release_ready_count, 0);

    fixture.runner.release(first).await;
    fixture.runner.release(second).await;
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn repository_two_skips_the_blocked_fifo_candidate_and_starts_another_repository() {
    let (fixture, other_repository) =
        support::gated_two_repository_task_manager_fixture(3, 2).await;
    fixture.runner.push_blocking(4);
    let same_repository = fixture
        .enqueue_tasks_for_repository(fixture.repository.id, "same repository", 3)
        .await;
    for (task, timestamp) in same_repository.iter().zip([
        "2026-07-15T00:00:01.000000000Z",
        "2026-07-15T00:00:02.000000000Z",
        "2026-07-15T00:00:03.000000000Z",
    ]) {
        fixture.set_created_at(task.id, timestamp).await;
    }
    fixture.state.set(ServiceState::Ready).unwrap();
    fixture
        .manager
        .notify_queued(same_repository[0].id)
        .await
        .unwrap();

    let same_repository_starts = [
        fixture.runner.wait_for_started_task(0).await,
        fixture.runner.wait_for_started_task(1).await,
    ];
    assert_eq!(
        same_repository_starts,
        [same_repository[0].id, same_repository[1].id]
    );
    assert_eq!(
        fixture.load(same_repository[2].id).await.status,
        TaskStatus::Queued
    );

    fixture.state.set(ServiceState::StoreDegraded).unwrap();
    let other = fixture
        .enqueue_tasks_for_repository(other_repository.id, "other repository", 1)
        .await;
    fixture
        .set_created_at(other[0].id, "2026-07-15T00:00:04.000000000Z")
        .await;
    fixture.state.set(ServiceState::Ready).unwrap();
    fixture
        .manager
        .notify_queued(same_repository[2].id)
        .await
        .unwrap();
    assert_eq!(fixture.runner.wait_for_started_task(2).await, other[0].id);

    fixture.runner.release(other[0].id).await;
    wait_for_safety_snapshot(&fixture, 2, 1).await;
    assert_eq!(
        fixture.load(same_repository[2].id).await.status,
        TaskStatus::Queued
    );
    assert_eq!(fixture.runner.started_count(same_repository[2].id), 0);

    fixture.runner.release(same_repository[0].id).await;
    assert_eq!(
        fixture.runner.wait_for_started_task(3).await,
        same_repository[2].id
    );
    fixture.runner.release(same_repository[1].id).await;
    fixture.runner.release(same_repository[2].id).await;
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn claim_completion_wait_does_not_block_mailbox() {
    let (fixture, writer_faults) = support::task_manager_fixture_with_writer_faults(
        1,
        1,
        [StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
            operation: Some(StoreWriterOperationKind::StartTask),
            count: 1,
        }],
    )
    .await;
    fixture.runner.push_blocking(1);
    let task = fixture.enqueue_tasks(1, false).await.remove(0);

    fixture.manager.notify_queued(task.id).await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        writer_faults.wait_until_reached(StoreWriterFaultPoint::PauseAfterCommitBeforeWake, 1),
    )
    .await
    .expect("claim commit reaches the paused completion boundary");
    assert_eq!(fixture.load(task.id).await.status, TaskStatus::Running);
    assert_eq!(fixture.runner.started_count(task.id), 0);

    let snapshot = tokio::time::timeout(
        Duration::from_secs(1),
        fixture.manager.safety_snapshot_for_test(),
    )
    .await
    .expect("safety inspection remains responsive")
    .unwrap();
    assert_eq!(snapshot.active_count, 1);
    assert_eq!(snapshot.available_permits, 0);
    tokio::time::timeout(
        Duration::from_secs(1),
        fixture.manager.notify_queued(TaskId::new()),
    )
    .await
    .expect("high-priority queue notification remains responsive")
    .unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        fixture.manager.safety_snapshot_for_test(),
    )
    .await
    .expect("mailbox processes the notification before the following inspection")
    .unwrap();

    assert_eq!(
        writer_faults.release(StoreWriterFaultPoint::PauseAfterCommitBeforeWake),
        1
    );
    assert_eq!(fixture.runner.wait_for_started_task(0).await, task.id);
    assert_eq!(fixture.runner.started_count(task.id), 1);
    fixture.runner.release(task.id).await;
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn quiesce_during_claim_completion_wait_latches_and_keeps_mailbox_responsive() {
    let (fixture, writer_faults) = support::task_manager_fixture_with_writer_faults(
        1,
        1,
        [StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
            operation: Some(StoreWriterOperationKind::StartTask),
            count: 1,
        }],
    )
    .await;
    fixture.runner.push_blocking(1);
    let task = fixture.enqueue_tasks(1, false).await.remove(0);
    fixture.manager.notify_queued(task.id).await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        writer_faults.wait_until_reached(StoreWriterFaultPoint::PauseAfterCommitBeforeWake, 1),
    )
    .await
    .expect("claim commit reaches the paused completion boundary");

    let quiescing_manager = fixture.manager.clone();
    let quiesce = tokio::spawn(async move {
        quiescing_manager
            .quiesce_and_interrupt(Instant::now() + Duration::from_secs(5))
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !fixture.manager.shutdown_latched_for_test() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the actor accepts and publishes the shutdown latch");
    assert!(
        !quiesce.is_finished(),
        "the quiesce receipt may remain pending behind StoreWriter"
    );

    let safety = tokio::time::timeout(
        Duration::from_secs(1),
        fixture.manager.safety_snapshot_for_test(),
    )
    .await
    .expect("latched shutdown keeps the safety mailbox responsive");
    assert!(matches!(safety, Err(TaskManagerError::Frozen)));
    let notification = tokio::time::timeout(
        Duration::from_secs(1),
        fixture.manager.notify_queued(TaskId::new()),
    )
    .await
    .expect("latched shutdown keeps queue notification responsive");
    assert!(matches!(notification, Err(TaskManagerError::Frozen)));

    assert_eq!(
        writer_faults.release(StoreWriterFaultPoint::PauseAfterCommitBeforeWake),
        1
    );
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), quiesce)
            .await
            .expect("quiesce completes after the writer resumes")
            .expect("quiesce task does not panic")
            .expect("quiesce returns a typed result"),
        QuiesceResult::Durable { .. }
    ));
    assert_eq!(fixture.runner.started_count(task.id), 0);
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn quiesce_during_runner_event_writer_lag_keeps_the_mailbox_responsive() {
    let (fixture, writer_faults) = support::task_manager_fixture_with_writer_faults(
        1,
        1,
        [StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
            operation: Some(StoreWriterOperationKind::AppendRunningEvent),
            count: 1,
        }],
    )
    .await;
    let gate = fixture
        .runner
        .push_event_gate(RunnerEvent::PlanUpdated(support::fixture_review_plan()));
    let task = fixture.enqueue_tasks(1, true).await.remove(0);
    assert_eq!(fixture.runner.wait_for_started_task(0).await, task.id);
    gate.release.notify_one();
    tokio::time::timeout(
        Duration::from_secs(5),
        writer_faults.wait_until_reached(StoreWriterFaultPoint::PauseAfterCommitBeforeWake, 1),
    )
    .await
    .expect("runner event reaches the paused commit-before-reply boundary");

    let safety = tokio::time::timeout(
        Duration::from_secs(1),
        fixture.manager.safety_snapshot_for_test(),
    )
    .await
    .expect("runner event writer lag does not block the safety mailbox")
    .expect("the active safety snapshot remains available");
    assert_eq!(safety.active_count, 1);
    assert_eq!(safety.available_permits, 0);

    let quiescing_manager = fixture.manager.clone();
    let quiesce = tokio::spawn(async move {
        quiescing_manager
            .quiesce_and_interrupt(Instant::now() + Duration::from_secs(5))
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !fixture.manager.shutdown_latched_for_test() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("quiesce is accepted while the runner event reply is pending");
    assert!(
        !quiesce.is_finished(),
        "quiesce still waits for the exact in-flight mutation and runner cleanup"
    );
    assert!(matches!(
        tokio::time::timeout(
            Duration::from_secs(1),
            fixture.manager.notify_queued(task.id),
        )
        .await
        .expect("queue notification receives a prompt shutdown answer"),
        Err(TaskManagerError::Frozen)
    ));

    assert_eq!(
        writer_faults.release(StoreWriterFaultPoint::PauseAfterCommitBeforeWake),
        1
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(5), gate.result)
            .await
            .expect("the runner event receives its exact durable completion")
            .expect("the event result channel remains live")
            .is_ok()
    );
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), quiesce)
            .await
            .expect("quiesce completes after the event mutation and runner return")
            .expect("quiesce task does not panic")
            .expect("quiesce returns a typed result"),
        QuiesceResult::Durable { .. }
    ));
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn quiesce_during_review_writer_lag_keeps_the_mailbox_responsive() {
    let (fixture, writer_faults) = support::task_manager_fixture_with_writer_faults(
        1,
        1,
        [StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
            operation: Some(StoreWriterOperationKind::RecordReview),
            count: 1,
        }],
    )
    .await;
    let gate = fixture
        .runner
        .push_review_gate(support::changes_requested_review(1));
    let task = fixture.enqueue_tasks(1, true).await.remove(0);
    assert_eq!(fixture.runner.wait_for_started_task(0).await, task.id);
    gate.release.notify_one();
    tokio::time::timeout(
        Duration::from_secs(5),
        writer_faults.wait_until_reached(StoreWriterFaultPoint::PauseAfterCommitBeforeWake, 1),
    )
    .await
    .expect("review reaches the paused commit-before-reply boundary");

    let safety = tokio::time::timeout(
        Duration::from_secs(1),
        fixture.manager.safety_snapshot_for_test(),
    )
    .await
    .expect("review writer lag does not block the safety mailbox")
    .expect("the active safety snapshot remains available");
    assert_eq!(safety.active_count, 1);
    assert_eq!(safety.available_permits, 0);

    let quiescing_manager = fixture.manager.clone();
    let quiesce = tokio::spawn(async move {
        quiescing_manager
            .quiesce_and_interrupt(Instant::now() + Duration::from_secs(5))
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !fixture.manager.shutdown_latched_for_test() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("quiesce is accepted while the review reply is pending");
    assert!(
        !quiesce.is_finished(),
        "quiesce still waits for the exact review mutation and runner cleanup"
    );

    assert_eq!(
        writer_faults.release(StoreWriterFaultPoint::PauseAfterCommitBeforeWake),
        1
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(5), gate.result)
            .await
            .expect("the review receives its exact durable completion")
            .expect("the review result channel remains live")
            .is_ok()
    );
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), quiesce)
            .await
            .expect("quiesce completes after review mutation and runner return")
            .expect("quiesce task does not panic")
            .expect("quiesce returns a typed result"),
        QuiesceResult::Durable { .. }
    ));
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn terminal_release_waits_for_writer_receipt_projection_and_process_return() {
    let (fixture, writer_faults) = support::task_manager_fixture_with_writer_faults(
        1,
        1,
        [
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
                operation: Some(StoreWriterOperationKind::FinalizeReviewedTask),
                count: 1,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::DropWakeAfterCommit,
                operation: Some(StoreWriterOperationKind::FinalizeReviewedTask),
                count: 1,
            },
        ],
    )
    .await;
    fixture.runner.push_blocking(2);
    let tasks = fixture.enqueue_tasks(2, true).await;
    assert_eq!(fixture.runner.wait_for_started_task(0).await, tasks[0].id);

    fixture.runner.release(tasks[0].id).await;
    tokio::time::timeout(
        Duration::from_secs(5),
        writer_faults.wait_until_reached(StoreWriterFaultPoint::PauseAfterCommitBeforeWake, 1),
    )
    .await
    .expect("terminal commit reaches the receipt/wake boundary");

    assert_eq!(
        fixture.load(tasks[0].id).await.status,
        TaskStatus::Completed
    );
    assert_eq!(fixture.load(tasks[1].id).await.status, TaskStatus::Queued);
    assert_eq!(fixture.runner.started_count(tasks[1].id), 0);
    let retained = fixture.manager.safety_snapshot_for_test().await.unwrap();
    assert_eq!(retained.active_count, 1);
    assert_eq!(retained.available_permits, 0);

    assert_eq!(
        writer_faults.release(StoreWriterFaultPoint::PauseAfterCommitBeforeWake),
        1
    );
    assert_eq!(fixture.runner.wait_for_started_task(1).await, tasks[1].id);
    assert_eq!(
        fixture
            .event_kinds(tasks[0].id)
            .await
            .into_iter()
            .filter(|kind| *kind == TaskEventKind::TaskCompleted)
            .count(),
        1
    );
    fixture.runner.release(tasks[1].id).await;
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn reviewed_terminal_after_commit_before_reply_reconciles_exactly_once() {
    let (fixture, _writer_faults) = support::task_manager_fixture_with_writer_faults(
        1,
        1,
        [StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
            operation: Some(StoreWriterOperationKind::FinalizeReviewedTask),
            count: 1,
        }],
    )
    .await;
    fixture.runner.push_blocking(2);
    let tasks = fixture.enqueue_tasks(2, true).await;
    assert_eq!(fixture.runner.wait_for_started_task(0).await, tasks[0].id);

    fixture.runner.release(tasks[0].id).await;
    fixture
        .wait_for_status(tasks[0].id, TaskStatus::Completed)
        .await;
    assert_eq!(fixture.runner.wait_for_started_task(1).await, tasks[1].id);
    let kinds = fixture.event_kinds(tasks[0].id).await;
    assert_eq!(
        kinds
            .iter()
            .filter(|kind| **kind == TaskEventKind::ReviewUpdated)
            .count(),
        1
    );
    assert_eq!(
        kinds
            .iter()
            .filter(|kind| **kind == TaskEventKind::TaskCompleted)
            .count(),
        1
    );
    assert!(!kinds.contains(&TaskEventKind::TaskInterrupted));
    fixture.runner.release(tasks[1].id).await;
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn unreviewed_terminal_drop_wake_still_releases_only_after_exact_receipt() {
    let (fixture, _writer_faults) = support::task_manager_fixture_with_writer_faults(
        1,
        1,
        [StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::DropWakeAfterCommit,
            operation: Some(StoreWriterOperationKind::FinishTask),
            count: 1,
        }],
    )
    .await;
    let failure = TaskFailure {
        code: "DROP_WAKE_EXACT_FAILURE".to_owned(),
        message: "the typed terminal receipt survives a dropped dispatcher wake".to_owned(),
        retryable: false,
    };
    fixture.runner.push_failure(failure.clone());
    fixture.runner.push_blocking(1);
    let tasks = fixture.enqueue_tasks(2, true).await;

    fixture
        .wait_for_status(tasks[0].id, TaskStatus::Failed)
        .await;
    assert_eq!(fixture.load(tasks[0].id).await.failure, Some(failure));
    assert_eq!(
        fixture
            .event_kinds(tasks[0].id)
            .await
            .into_iter()
            .filter(|kind| *kind == TaskEventKind::TaskFailed)
            .count(),
        1
    );
    assert_eq!(fixture.runner.wait_for_started_task(1).await, tasks[1].id);
    fixture.runner.release(tasks[1].id).await;
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn persistently_unknown_unreviewed_terminal_freezes_and_retains_ownership() {
    let (fixture, _writer_faults) = support::task_manager_fixture_with_writer_faults(
        1,
        1,
        [StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::FailUnknownBeforeExecute,
            operation: Some(StoreWriterOperationKind::FinishTask),
            count: 4,
        }],
    )
    .await;
    fixture.runner.push_failure(TaskFailure {
        code: "PERSISTENT_UNKNOWN_FAILURE".to_owned(),
        message: "the outcome remains unknown through local exact replay".to_owned(),
        retryable: false,
    });
    fixture.runner.push_blocking(1);
    let tasks = fixture.enqueue_tasks(2, true).await;

    tokio::time::timeout(Duration::from_secs(5), async {
        while !fixture.manager.shutdown_latched_for_test() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("persistent unknown freezes the task manager");
    assert_eq!(fixture.load(tasks[0].id).await.status, TaskStatus::Running);
    assert_eq!(fixture.load(tasks[1].id).await.status, TaskStatus::Queued);
    assert_eq!(fixture.runner.started_count(tasks[1].id), 0);
    assert!(matches!(
        fixture.manager.safety_snapshot_for_test().await,
        Err(TaskManagerError::Frozen)
    ));
    assert!(matches!(
        fixture.manager.notify_queued(tasks[1].id).await,
        Err(TaskManagerError::Frozen)
    ));
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn failed_after_commit_before_reply_preserves_the_exact_terminal_and_releases_the_permit() {
    let (fixture, _writer_faults) = support::task_manager_fixture_with_writer_faults(
        1,
        1,
        [StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
            operation: Some(StoreWriterOperationKind::FinishTask),
            count: 1,
        }],
    )
    .await;
    let expected_failure = TaskFailure {
        code: "EXACT_RUNNER_FAILURE".to_owned(),
        message: "preserve the exact runner failure".to_owned(),
        retryable: false,
    };
    fixture.runner.push_failure(expected_failure.clone());
    fixture.runner.push_blocking(1);
    let tasks = fixture.enqueue_tasks(2, true).await;

    fixture
        .wait_for_status(tasks[0].id, TaskStatus::Failed)
        .await;
    assert_eq!(
        fixture.load(tasks[0].id).await.failure,
        Some(expected_failure)
    );
    assert_eq!(
        fixture
            .event_kinds(tasks[0].id)
            .await
            .into_iter()
            .filter(|kind| *kind == TaskEventKind::TaskFailed)
            .count(),
        1
    );
    assert_eq!(fixture.runner.wait_for_started_task(1).await, tasks[1].id);
    fixture.runner.release(tasks[1].id).await;
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn cancelled_after_commit_before_reply_preserves_the_terminal_and_releases_the_permit() {
    let (fixture, _writer_faults) = support::task_manager_fixture_with_writer_faults(
        1,
        1,
        [StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
            operation: Some(StoreWriterOperationKind::FinalizeStoppedTask),
            count: 1,
        }],
    )
    .await;
    fixture.runner.push_blocking(2);
    let tasks = fixture.enqueue_tasks(2, true).await;
    assert_eq!(fixture.runner.wait_for_started_task(0).await, tasks[0].id);

    assert!(matches!(
        fixture.manager.cancel(tasks[0].id).await.unwrap(),
        CancelOutcome::Accepted { .. }
    ));
    fixture
        .wait_for_status(tasks[0].id, TaskStatus::Cancelled)
        .await;
    assert_eq!(fixture.load(tasks[0].id).await.failure, None);
    assert_eq!(
        fixture
            .event_kinds(tasks[0].id)
            .await
            .into_iter()
            .filter(|kind| *kind == TaskEventKind::TaskCancelled)
            .count(),
        1
    );
    assert_eq!(fixture.runner.wait_for_started_task(1).await, tasks[1].id);
    fixture.runner.release(tasks[1].id).await;
}

#[tokio::test]
async fn running_cancel_waits_for_the_durable_intent_and_keeps_the_mailbox_responsive() {
    let (fixture, controller) = support::task_manager_fixture_with_writer_faults(
        1,
        1,
        [StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
            operation: Some(StoreWriterOperationKind::PersistStopIntentBatch),
            count: 1,
        }],
    )
    .await;
    fixture.runner.push_blocking(1);
    let task = fixture.enqueue_tasks(1, true).await.remove(0);
    fixture.wait_for_status(task.id, TaskStatus::Running).await;

    let cancel = tokio::spawn({
        let manager = fixture.manager.clone();
        async move { manager.cancel(task.id).await }
    });
    tokio::time::timeout(
        Duration::from_secs(5),
        controller.wait_until_reached(StoreWriterFaultPoint::PauseAfterCommitBeforeWake, 1),
    )
    .await
    .expect("running cancel reaches durable stop-intent persistence");
    assert!(
        !cancel.is_finished(),
        "user cancellation is not accepted before the durable reply"
    );
    let snapshot = fixture
        .store
        .scheduler_bootstrap_snapshot()
        .await
        .expect("read the committed stop intent");
    assert!(snapshot.running_stop_intents.iter().any(|intent| {
        intent.task_id == task.id && intent.kind == StopIntentKind::UserCancelled
    }));
    let safety = tokio::time::timeout(
        Duration::from_secs(1),
        fixture.manager.safety_snapshot_for_test(),
    )
    .await
    .expect("the actor mailbox remains responsive while the intent reply is paused")
    .expect("inspect task-manager safety");
    assert_eq!(safety.active_count, 1);
    assert_eq!(safety.available_permits, 0);
    fixture
        .manager
        .notify_storage_critical_for_test(vec![coding_agent_app::MonitoredStorageScope::Data]);

    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseAfterCommitBeforeWake),
        1
    );
    assert!(matches!(
        cancel
            .await
            .expect("join cancel request")
            .expect("cancel task"),
        CancelOutcome::Accepted { .. }
    ));
    fixture
        .wait_for_status(task.id, TaskStatus::Cancelled)
        .await;
    let retained: (i64, String) =
        sqlx::query_as("SELECT COUNT(*), MIN(kind) FROM task_stop_intents WHERE task_id = ?")
            .bind(task.id.to_string())
            .fetch_one(fixture.store.pool())
            .await
            .expect("load retained user-first stop intent");
    assert_eq!(retained, (1, "user_cancelled".to_owned()));
    assert_eq!(
        fixture
            .event_kinds(task.id)
            .await
            .into_iter()
            .filter(|kind| *kind == TaskEventKind::TaskCancelled)
            .count(),
        1
    );
}

#[tokio::test]
async fn disk_critical_uses_a_durable_intent_and_the_exact_retryable_failure() {
    let fixture = support::task_manager_fixture(1).await;
    fixture.runner.push_blocking(1);
    let task = fixture.enqueue_tasks(1, true).await.remove(0);
    fixture.wait_for_status(task.id, TaskStatus::Running).await;
    fixture.wait_for_runner_start(task.id).await;

    fixture
        .manager
        .notify_storage_critical_for_test(vec![coding_agent_app::MonitoredStorageScope::Data]);
    fixture.wait_for_status(task.id, TaskStatus::Failed).await;

    assert_eq!(
        fixture.load(task.id).await.failure,
        Some(TaskFailure {
            code: "DISK_PRESSURE_CRITICAL".to_owned(),
            message: "critical disk pressure stopped the task".to_owned(),
            retryable: true,
        })
    );
    let retained: (i64, String) =
        sqlx::query_as("SELECT COUNT(*), MIN(kind) FROM task_stop_intents WHERE task_id = ?")
            .bind(task.id.to_string())
            .fetch_one(fixture.store.pool())
            .await
            .expect("load retained disk stop intent");
    assert_eq!(retained, (1, "disk_pressure_critical".to_owned()));
    assert_eq!(
        fixture
            .event_kinds(task.id)
            .await
            .into_iter()
            .filter(|kind| *kind == TaskEventKind::TaskFailed)
            .count(),
        1
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn disk_intent_assigned_first_rejects_a_later_user_cancel_with_typed_conflict() {
    let (fixture, controller) = support::task_manager_fixture_with_writer_faults(
        1,
        1,
        [StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
            operation: Some(StoreWriterOperationKind::PersistStopIntentBatch),
            count: 1,
        }],
    )
    .await;
    fixture.runner.push_blocking(1);
    let task = fixture.enqueue_tasks(1, true).await.remove(0);
    fixture.wait_for_status(task.id, TaskStatus::Running).await;
    fixture.wait_for_runner_start(task.id).await;

    fixture
        .manager
        .notify_storage_critical_for_test(vec![coding_agent_app::MonitoredStorageScope::Data]);
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseAfterCommitBeforeWake, 1)
        .await;
    let cancel = fixture.manager.cancel(task.id).await;
    assert!(
        matches!(
            &cancel,
            Err(TaskManagerError::StopAlreadyRequested {
                task: current,
                existing: StopIntentKind::DiskPressureCritical,
            }) if current.id == task.id
        ),
        "unexpected disk-first cancellation outcome: {cancel:?}"
    );

    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseAfterCommitBeforeWake),
        1
    );
    fixture.wait_for_status(task.id, TaskStatus::Failed).await;
}

#[tokio::test]
async fn fifth_task_stays_queued_until_a_permit_is_released() {
    let fixture = support::task_manager_fixture(4).await;
    fixture.runner.push_blocking(5);
    let tasks = fixture.enqueue_tasks(5, true).await;

    fixture.wait_for_running(4).await;
    assert_eq!(fixture.load(tasks[4].id).await.status, TaskStatus::Queued);

    fixture.runner.release(tasks[0].id).await;
    fixture
        .wait_for_status(tasks[4].id, TaskStatus::Running)
        .await;
    fixture.wait_for_runner_start(tasks[4].id).await;
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
    let blocker = fixture.enqueue_tasks(1, true).await.remove(0);
    fixture
        .wait_for_status(blocker.id, TaskStatus::Running)
        .await;
    fixture.wait_for_runner_start(blocker.id).await;
    let task = fixture.enqueue_tasks(1, false).await.remove(0);

    let outcome = fixture.manager.cancel(task.id).await.unwrap();
    let cancelled = match outcome {
        CancelOutcome::Cancelled { task } => task,
        other => panic!("queued cancel must finish synchronously: {other:?}"),
    };
    let projected = fixture.manager.scheduler_projection_for_test();
    let durable = fixture
        .store
        .scheduler_bootstrap_snapshot()
        .await
        .expect("load queued-cancel Store witness");
    let mut durable_tasks = durable
        .tasks
        .iter()
        .map(|task| (task.id, task.status))
        .collect::<Vec<_>>();
    durable_tasks.sort_unstable_by_key(|(task_id, _)| task_id.as_uuid());
    assert_eq!(projected.tasks, durable_tasks);
    assert!(
        projected.as_of_event_id.get() >= cancelled.last_event_id.get(),
        "cancel response must follow exact Scheduler terminal publication"
    );
    assert_eq!(durable.membership_event_id, projected.as_of_event_id);
    fixture.manager.notify_queued(task.id).await.unwrap();
    fixture.runner.release(blocker.id).await;
    fixture
        .wait_for_status(blocker.id, TaskStatus::Completed)
        .await;
    wait_for_safety_snapshot(&fixture, 0, 1).await;
    fixture.reconcile().await;

    assert_eq!(fixture.load(task.id).await.status, TaskStatus::Cancelled);
    assert_eq!(fixture.runner.started_count(task.id), 0);
}

#[tokio::test]
async fn queued_cancel_conflict_replay_publishes_exact_terminal_before_response() {
    let (fixture, writer_faults) = support::task_manager_fixture_with_writer_faults(
        1,
        1,
        [StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseBeforeExecute,
            operation: Some(StoreWriterOperationKind::CancelTask),
            count: 1,
        }],
    )
    .await;
    fixture.runner.push_blocking(1);
    let occupied = fixture.enqueue_tasks(1, true).await.remove(0);
    fixture
        .wait_for_status(occupied.id, TaskStatus::Running)
        .await;
    fixture.wait_for_runner_start(occupied.id).await;

    let task = fixture.enqueue_tasks(1, false).await.remove(0);
    let cancel = tokio::spawn({
        let manager = fixture.manager.clone();
        async move { manager.cancel(task.id).await }
    });
    writer_faults
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;

    let terminal = match fixture
        .store
        .transition_with_event(task.id, TaskStatus::Queued, TaskTransition::Cancelled)
        .await
        .expect("commit the competing queued cancellation")
    {
        TransitionOutcome::Applied { task, .. } => task,
        TransitionOutcome::Conflict { .. } => {
            panic!("the direct competing transition must win")
        }
    };
    assert_eq!(
        writer_faults.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    let outcome = tokio::time::timeout(Duration::from_secs(5), cancel)
        .await
        .expect("conflicting queued cancel returns")
        .expect("join conflicting queued cancel")
        .expect("terminal conflict is an idempotent cancel replay");
    assert!(matches!(outcome, CancelOutcome::Cancelled { task } if task == terminal));

    let projected = fixture.manager.scheduler_projection_for_test();
    let durable = fixture
        .store
        .scheduler_bootstrap_snapshot()
        .await
        .expect("load conflict-replay Store witness");
    let mut durable_tasks = durable
        .tasks
        .iter()
        .map(|task| (task.id, task.status))
        .collect::<Vec<_>>();
    durable_tasks.sort_unstable_by_key(|(task_id, _)| task_id.as_uuid());
    assert_eq!(projected.tasks, durable_tasks);
    assert_eq!(projected.as_of_event_id, durable.membership_event_id);
    assert!(projected.as_of_event_id.get() >= terminal.last_event_id.get());
}

#[tokio::test]
async fn categorical_storage_change_publishes_once_and_identical_replay_is_stable() {
    let fixture = support::task_manager_fixture(1).await;
    let before = fixture.manager.scheduler_projection_for_test();
    let pressure = SchedulerStorageNotification::new(
        StorageState::Pressure,
        StorageState::Pressure,
        StorageState::Normal,
        vec![SchedulerRepositoryStorageState::new(
            fixture.repository.id,
            StorageState::Normal,
        )],
    );
    fixture
        .manager
        .notify_scheduler_storage_for_test(pressure.clone());
    let changed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let current = fixture.manager.scheduler_projection_for_test();
            if current.generation > before.generation {
                break current;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("storage classification publishes a Scheduler generation");

    fixture.manager.notify_scheduler_storage_for_test(pressure);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        fixture.manager.scheduler_projection_for_test().generation,
        changed.generation,
        "identical categorical storage replay must not consume a generation"
    );
}

#[tokio::test]
async fn busy_claim_cleans_provisional_state_and_reconciliation_claims_once() {
    let fixture = support::task_manager_fixture(1).await;
    fixture.state.set(ServiceState::StoreDegraded).unwrap();
    fixture.runner.push_blocking(1);
    let task = fixture.enqueue_tasks(1, false).await.remove(0);
    fixture.force_claim_busy(true).await;
    fixture.state.set(ServiceState::Ready).unwrap();
    fixture.manager.notify_queued(task.id).await.unwrap();
    fixture.manager.notify_queued(task.id).await.unwrap();
    assert_eq!(fixture.load(task.id).await.status, TaskStatus::Queued);
    assert_eq!(fixture.runner.started_count(task.id), 0);

    fixture.force_claim_busy(false).await;
    fixture.manager.notify_queued(task.id).await.unwrap();
    fixture.wait_for_status(task.id, TaskStatus::Running).await;
    fixture.wait_for_runner_start(task.id).await;
    assert_eq!(fixture.runner.started_count(task.id), 1);
}

#[tokio::test]
async fn queued_cancel_preserves_known_uncommitted_store_busy() {
    let fixture = support::task_manager_fixture(1).await;
    fixture.state.set(ServiceState::StoreDegraded).unwrap();
    let task = fixture.enqueue_tasks(1, false).await.remove(0);
    fixture.force_claim_busy(true).await;
    fixture.state.set(ServiceState::Ready).unwrap();

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
    fixture.wait_for_runner_start(task.id).await;
    assert_eq!(fixture.runner.started_count(task.id), 1);
}

#[tokio::test]
async fn a_failed_fifo_head_claim_cannot_be_overtaken() {
    let fixture = support::task_manager_fixture(1).await;
    fixture.runner.push_blocking(2);
    fixture.fail_fifo_head_started_event_inserts(true).await;
    let tasks = fixture.enqueue_tasks(2, false).await;

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

    fixture.fail_fifo_head_started_event_inserts(false).await;
    fixture.manager.notify_queued(tasks[0].id).await.unwrap();
    fixture
        .wait_for_status(tasks[0].id, TaskStatus::Running)
        .await;
    assert_eq!(fixture.load(tasks[1].id).await.status, TaskStatus::Queued);
    fixture.runner.release(tasks[0].id).await;
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
    fixture.wait_for_runner_start(task.id).await;

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

    let mut persisted = Vec::with_capacity(queued.len());
    for task in &queued {
        persisted.push(fixture.load(task.id).await);
    }
    persisted.sort_by_key(|task| (task.created_at, task.id.to_string()));
    let expected = persisted.iter().map(|task| task.id).collect::<Vec<_>>();
    let mut actual = vec![fixture.runner.wait_for_started_task(0).await];
    assert_eq!(actual, [blocker.id]);
    fixture.runner.release(blocker.id).await;
    for (index, expected_id) in expected.iter().enumerate() {
        let actual_id = fixture.runner.wait_for_started_task(index + 1).await;
        actual.push(actual_id);
        assert_eq!(actual_id, *expected_id, "FIFO start {index}");
        assert_eq!(fixture.load(actual_id).await.status, TaskStatus::Running);
        assert_eq!(fixture.runner.started_count(actual_id), 1);
        for pending_id in &expected[index + 1..] {
            assert_eq!(fixture.load(*pending_id).await.status, TaskStatus::Queued);
            assert_eq!(fixture.runner.started_count(*pending_id), 0);
        }
        fixture.runner.release(actual_id).await;
    }
    let expected_with_blocker = std::iter::once(blocker.id)
        .chain(expected)
        .collect::<Vec<_>>();
    assert_eq!(actual, expected_with_blocker);
    assert_eq!(fixture.runner.started_task_ids(), expected_with_blocker);
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
        RunnerEvent::PlanUpdated(PlanSnapshot::legacy(1, Vec::new())),
        RunnerEvent::ActivityAppended(ActivityEntry::legacy(
            "activity-1",
            ActivityLevel::Info,
            "working",
            support::timestamp(),
        )),
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
        completion_wins.manager.cancel(task.id).await.unwrap(),
        CancelOutcome::Finished { task: existing } if existing.id == task.id
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

#[cfg(feature = "test-support")]
#[tokio::test]
async fn approved_finalize_entered_first_makes_later_cancel_not_cancellable() {
    let (fixture, controller) = support::paused_finalize_task_manager_fixture().await;
    let completion = fixture.runner.push_completion_gate();
    let task = fixture.enqueue_tasks(1, true).await.remove(0);
    fixture.wait_for_status(task.id, TaskStatus::Running).await;

    completion.release.notify_one();
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseAfterCommitBeforeWake, 1)
        .await;
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
            StoreWriterOperationKind::FinalizeReviewedTask,
        ),
        1
    );
    let cancel = tokio::spawn({
        let manager = fixture.manager.clone();
        async move { manager.cancel(task.id).await }
    });
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    assert!(!cancel.is_finished());

    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseAfterCommitBeforeWake),
        1
    );
    assert!(matches!(
        cancel.await.unwrap().unwrap(),
        CancelOutcome::Finished { task: existing } if existing.id == task.id
    ));
    let persisted = fixture.load(task.id).await;
    assert_eq!(persisted.status, TaskStatus::Completed);
    assert_eq!(
        persisted.delivery_readiness,
        coding_agent_domain::DeliveryReadiness::ReviewApproved
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
    assert_eq!(fixture.runner.wait_for_started_task(1).await, tasks[1].id);
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
async fn forced_quiesce_retains_the_latest_durable_diff_and_rejects_late_updates() {
    let fixture = support::task_manager_fixture(1).await;
    let durable = DiffSnapshot {
        revision: 7,
        files: vec![DiffFile {
            path: "src/lib.rs".to_owned(),
            status: DiffFileStatus::Modified,
            patch: "+durable\n".to_owned(),
            additions: 1,
            deletions: 0,
            truncated: false,
        }],
    };
    let late = DiffSnapshot {
        revision: 8,
        files: vec![DiffFile {
            path: "src/lib.rs".to_owned(),
            status: DiffFileStatus::Modified,
            patch: "+late\n".to_owned(),
            additions: 1,
            deletions: 0,
            truncated: false,
        }],
    };
    let gate = fixture.runner.push_durable_then_late_event(
        RunnerEvent::DiffUpdated(durable.clone()),
        RunnerEvent::DiffUpdated(late),
    );
    let task = fixture.enqueue_tasks(1, true).await.remove(0);
    fixture.wait_for_status(task.id, TaskStatus::Running).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if fixture.load_detail(task.id).await.diff == Some(durable.clone()) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the pre-quiesce diff becomes durable");

    let active = match fixture
        .manager
        .quiesce_and_interrupt(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap()
    {
        QuiesceResult::Durable { active, .. } => active,
        QuiesceResult::Frozen { .. } => panic!("quiesce should commit"),
    };
    assert_eq!(fixture.load(task.id).await.status, TaskStatus::Interrupted);
    assert_eq!(
        fixture.load_detail(task.id).await.diff,
        Some(durable.clone())
    );

    assert!(matches!(
        gate.result.await.unwrap(),
        Err(RunnerEventError::TaskNotRunning | RunnerEventError::StoreDegraded)
    ));
    active.into_iter().next().unwrap().done.await.unwrap();
    assert_eq!(fixture.load_detail(task.id).await.diff, Some(durable));
}

#[tokio::test]
async fn durable_quiesce_cancels_all_runners_before_recovery() {
    let fixture = support::task_manager_fixture(1).await;
    fixture.runner.push_blocking(1);
    let task = fixture.enqueue_tasks(1, true).await.remove(0);
    fixture.wait_for_status(task.id, TaskStatus::Running).await;

    let result = fixture
        .manager
        .quiesce_and_interrupt(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    let active = match result {
        QuiesceResult::Durable { active, .. } => active,
        QuiesceResult::Frozen { .. } => panic!("quiesce should commit"),
    };

    assert_eq!(active.len(), 1);
    let runner = active.into_iter().next().unwrap();
    let was_cancelled = runner.cancellation.is_cancelled();
    if !was_cancelled {
        runner.cancellation.cancel();
    }
    runner.done.await.unwrap();
    assert!(
        was_cancelled,
        "TaskManager owns active process cleanup and must cancel before recovery"
    );
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
            assert!(matches!(error, StoreWriterError::DeadlineElapsed));
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
        .push_event_gate(RunnerEvent::PlanUpdated(PlanSnapshot::legacy(
            7,
            Vec::new(),
        )));
    let task = fixture.enqueue_tasks(1, true).await.remove(0);
    fixture.wait_for_status(task.id, TaskStatus::Running).await;
    fixture.wait_for_runner_start(task.id).await;
    fixture.state.set(ServiceState::StoreDegraded).unwrap();
    append.release.notify_one();

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), append.result)
            .await
            .expect("degraded runner event receives a bounded rejection")
            .unwrap(),
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

#[tokio::test]
async fn fake_runner_emits_the_approved_panel_sequence() {
    assert_eq!(
        FakeRunnerConfig::default().emission_interval(),
        Duration::from_millis(200)
    );
    let fixture = support::fake_runner_fixture().await;
    let task = fixture.start().await;
    tokio::time::pause();

    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::time::resume();
    fixture.wait_for_terminal(task.id).await;

    assert_eq!(
        fixture.event_kinds(task.id).await,
        vec![
            TaskEventKind::TaskQueued,
            TaskEventKind::TaskStarted,
            TaskEventKind::PlanUpdated,
            TaskEventKind::ActivityAppended,
            TaskEventKind::ActivityAppended,
            TaskEventKind::ActivityAppended,
            TaskEventKind::DiffUpdated,
            TaskEventKind::TestUpdated,
            TaskEventKind::TestUpdated,
            TaskEventKind::ReviewUpdated,
            TaskEventKind::TaskCompleted,
        ]
    );

    let detail = fixture.detail(task.id).await;
    let plan = detail.plan.as_ref().expect("fake runner persists its plan");
    assert_eq!(plan.format_version(), 1);
    assert_eq!(plan.revision(), 1);
    assert_eq!(
        plan.items()
            .iter()
            .map(|item| (item.id(), item.title(), item.status()))
            .collect::<Vec<_>>(),
        vec![
            (
                "fake-plan",
                "Prepare deterministic plan",
                PlanItemStatus::Completed
            ),
            (
                "fake-diff",
                "Generate synthetic diff",
                PlanItemStatus::Completed
            ),
            (
                "fake-tests",
                "Report synthetic tests",
                PlanItemStatus::Completed
            ),
        ]
    );
    assert_eq!(plan.initial_required_checks().len(), 1);
    assert_eq!(plan.initial_required_checks()[0].id(), "fake-cargo-test");
    assert_eq!(
        detail.task.delivery_readiness,
        coding_agent_domain::DeliveryReadiness::ReviewApproved
    );
    assert_eq!(detail.reviews.len(), 1);
    assert_eq!(
        detail
            .activity
            .iter()
            .map(|entry| (entry.id(), entry.message()))
            .collect::<Vec<_>>(),
        vec![
            ("fake-plan-ready", "Prepared deterministic plan"),
            ("fake-diff-ready", "Generated synthetic diff"),
            ("fake-tests-ready", "Started synthetic tests"),
        ]
    );
    assert!(
        detail
            .activity
            .iter()
            .all(|entry| entry.created_at() == detail.task.started_at.unwrap())
    );
    assert!(
        detail
            .activity
            .iter()
            .all(|entry| entry.level() == ActivityLevel::Info)
    );
    assert_eq!(
        detail.diff,
        Some(DiffSnapshot {
            revision: 1,
            files: vec![DiffFile {
                path: "synthetic/example.rs".to_owned(),
                status: DiffFileStatus::Added,
                patch: "diff --git a/synthetic/example.rs b/synthetic/example.rs\nnew file mode 100644\n--- /dev/null\n+++ b/synthetic/example.rs\n@@ -0,0 +1 @@\n+// deterministic fake change\n".to_owned(),
                additions: 1,
                deletions: 0,
                truncated: false,
            }],
        })
    );
    assert_eq!(
        detail.tests,
        Some(TestSnapshot {
            revision: 1,
            status: TestStatus::Passed,
            cases: vec![TestCase {
                id: "fake-test".to_owned(),
                name: "deterministic synthetic check".to_owned(),
                status: TestStatus::Passed,
                duration_ms: 200,
                summary: "Synthetic checks passed".to_owned(),
            }],
        })
    );
    assert_eq!(
        fixture.test_snapshots(task.id).await,
        vec![
            TestSnapshot {
                revision: 1,
                status: TestStatus::Running,
                cases: vec![TestCase {
                    id: "fake-test".to_owned(),
                    name: "deterministic synthetic check".to_owned(),
                    status: TestStatus::Running,
                    duration_ms: 0,
                    summary: "Synthetic checks are running".to_owned(),
                }],
            },
            TestSnapshot {
                revision: 1,
                status: TestStatus::Passed,
                cases: vec![TestCase {
                    id: "fake-test".to_owned(),
                    name: "deterministic synthetic check".to_owned(),
                    status: TestStatus::Passed,
                    duration_ms: 200,
                    summary: "Synthetic checks passed".to_owned(),
                }],
            },
        ]
    );
    let review_generation = detail.reviews[0].workspace_generation();
    assert_eq!(
        detail.diff.as_ref().map(|diff| diff.revision),
        Some(review_generation)
    );
    assert_eq!(
        detail.tests.as_ref().map(|tests| tests.revision),
        Some(review_generation)
    );
}

#[tokio::test]
async fn fake_runner_cancellation_during_an_interval_emits_no_late_panel_event() {
    let fixture =
        support::fake_runner_fixture_with_config(FakeRunnerConfig::new(Duration::from_secs(10)))
            .await;
    let task = fixture.start().await;

    assert!(matches!(
        fixture.cancel(task.id).await,
        CancelOutcome::Accepted { .. }
    ));
    fixture.wait_for_terminal(task.id).await;

    assert_eq!(fixture.load(task.id).await.status, TaskStatus::Cancelled);
    assert_eq!(
        fixture.event_kinds(task.id).await,
        vec![
            TaskEventKind::TaskQueued,
            TaskEventKind::TaskStarted,
            TaskEventKind::PlanUpdated,
            TaskEventKind::TaskCancelled,
        ]
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn scripted_fake_runner_failure_uses_the_fixed_safe_failure() {
    let fixture = support::scripted_fake_runner_fixture([FakeScenario::Failure], 1).await;
    let task = fixture.enqueue(&["ordinary prompt"]).await.remove(0);
    fixture.wait_for_status(task.id, TaskStatus::Failed).await;

    assert_eq!(
        fixture.load(task.id).await.failure,
        Some(TaskFailure {
            code: "FAKE_RUNNER_FAILURE".to_owned(),
            message: "deterministic fake runner failure".to_owned(),
            retryable: true,
        })
    );
    assert_eq!(
        fixture.event_kinds(task.id).await,
        vec![
            TaskEventKind::TaskQueued,
            TaskEventKind::TaskStarted,
            TaskEventKind::TaskFailed,
        ]
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn scripted_fake_runner_blocking_observes_cancellation_without_a_release() {
    let fixture = support::scripted_fake_runner_fixture([FakeScenario::Blocking], 1).await;
    let task = fixture.start("ordinary prompt").await;
    fixture.wait_for_status(task.id, TaskStatus::Running).await;

    assert!(matches!(
        fixture.cancel(task.id).await,
        CancelOutcome::Accepted { .. }
    ));
    fixture
        .wait_for_status(task.id, TaskStatus::Cancelled)
        .await;
    assert!(!fixture.runner.release(task.id));
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn scripted_fake_runner_blocking_release_persists_panels_before_approval() {
    let fixture = support::scripted_fake_runner_fixture([FakeScenario::Blocking], 1).await;
    let task = fixture.start("ordinary prompt").await;

    assert!(fixture.runner.release(task.id));
    fixture
        .wait_for_status(task.id, TaskStatus::Completed)
        .await;

    let detail = fixture.detail(task.id).await;
    let review_generation = detail.reviews[0].workspace_generation();
    assert_eq!(
        detail.diff.as_ref().map(|diff| diff.revision),
        Some(review_generation)
    );
    assert_eq!(
        detail.tests.as_ref().map(|tests| tests.revision),
        Some(review_generation)
    );
    assert_eq!(
        fixture.event_kinds(task.id).await,
        vec![
            TaskEventKind::TaskQueued,
            TaskEventKind::TaskStarted,
            TaskEventKind::PlanUpdated,
            TaskEventKind::DiffUpdated,
            TaskEventKind::TestUpdated,
            TaskEventKind::ReviewUpdated,
            TaskEventKind::TaskCompleted,
        ]
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn cancel_processed_first_overrides_late_approved_outcome() {
    let fixture =
        support::scripted_fake_runner_fixture([FakeScenario::IgnoresCancellation], 1).await;
    let task = fixture.start("ordinary prompt").await;
    fixture.wait_for_status(task.id, TaskStatus::Running).await;

    assert!(matches!(
        fixture.cancel(task.id).await,
        CancelOutcome::Accepted { .. }
    ));
    assert!(matches!(
        fixture.cancel(task.id).await,
        CancelOutcome::Accepted { task: replay } if replay.id == task.id
    ));
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    assert_eq!(fixture.load(task.id).await.status, TaskStatus::Running);
    assert!(fixture.runner.release(task.id));
    fixture
        .wait_for_status(task.id, TaskStatus::Cancelled)
        .await;
    assert_eq!(
        fixture.load(task.id).await.delivery_readiness,
        coding_agent_domain::DeliveryReadiness::Unreviewed
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn scripted_fake_runner_panic_is_isolated_from_another_task() {
    let fixture =
        support::scripted_fake_runner_fixture([FakeScenario::Panic, FakeScenario::Blocking], 2)
            .await;
    let tasks = fixture.enqueue(&["first prompt", "second prompt"]).await;

    fixture
        .wait_for_status(tasks[0].id, TaskStatus::Failed)
        .await;
    fixture
        .wait_for_status(tasks[1].id, TaskStatus::Running)
        .await;
    assert_eq!(
        fixture.load(tasks[0].id).await.failure.unwrap().code,
        "RUNNER_PANICKED"
    );
    fixture.wait_for_runner_start(tasks[1].id).await;
    assert!(fixture.runner.release(tasks[1].id));
    fixture
        .wait_for_status(tasks[1].id, TaskStatus::Completed)
        .await;
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn scripted_fake_runner_consumes_scenarios_in_task_creation_order_not_prompt_text() {
    let fixture =
        support::scripted_fake_runner_fixture([FakeScenario::Failure, FakeScenario::Blocking], 1)
            .await;
    let tasks = fixture
        .enqueue(&["please block forever", "please return a failure"])
        .await;

    fixture
        .wait_for_status(tasks[0].id, TaskStatus::Failed)
        .await;
    fixture
        .wait_for_status(tasks[1].id, TaskStatus::Running)
        .await;
    fixture.wait_for_runner_start(tasks[1].id).await;
    assert_eq!(
        fixture.runner.started_task_ids(),
        vec![tasks[0].id, tasks[1].id]
    );
    assert!(fixture.runner.release(tasks[1].id));
    fixture
        .wait_for_status(tasks[1].id, TaskStatus::Completed)
        .await;
}

#[cfg(feature = "test-support")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scripted_fake_runner_orders_scenarios_by_launch_when_inner_polls_are_reversed() {
    const ITERATIONS: usize = 32;
    let fixture = support::reverse_polled_scripted_fake_runner_fixture(
        (0..ITERATIONS).flat_map(|_| [FakeScenario::Failure, FakeScenario::Blocking]),
        2,
    )
    .await;
    for iteration in 0..ITERATIONS {
        let tasks = fixture.enqueue(&["created first", "created second"]).await;

        let failed = fixture
            .wait_for_one_failed(&[tasks[0].id, tasks[1].id])
            .await;
        assert_eq!(failed, tasks[0].id, "iteration {iteration}");
        fixture
            .wait_for_status(tasks[1].id, TaskStatus::Running)
            .await;
        fixture.wait_for_runner_start(tasks[1].id).await;
        let started = fixture.runner.started_task_ids();
        assert_eq!(
            &started[started.len() - 2..],
            &[tasks[0].id, tasks[1].id],
            "iteration {iteration}"
        );
        assert!(fixture.runner.release(tasks[1].id));
        fixture
            .wait_for_status_with_timeout(
                tasks[1].id,
                TaskStatus::Completed,
                Duration::from_secs(30),
            )
            .await;
    }
}
