mod support;

#[cfg(feature = "test-support")]
use coding_agent_app::FakeScenario;
use coding_agent_app::{
    CancelOutcome, FakeRunnerConfig, QuiesceResult, RunnerEvent, RunnerEventError, ServiceState,
    StoreWriterError, TaskManagerError,
};
use coding_agent_domain::{
    ActivityEntry, ActivityLevel, DiffFile, DiffFileStatus, DiffSnapshot, PlanItem, PlanItemStatus,
    PlanSnapshot, TaskEventKind, TaskFailure, TaskStatus, TestCase, TestSnapshot, TestStatus,
};
use tokio::time::{Duration, Instant};

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

    gate.release.notify_one();
    assert!(matches!(
        gate.result.await.unwrap(),
        Err(RunnerEventError::TaskNotRunning | RunnerEventError::StoreDegraded)
    ));
    active.into_iter().next().unwrap().done.await.unwrap();
    assert_eq!(fixture.load_detail(task.id).await.diff, Some(durable));
}

#[tokio::test]
async fn durable_quiesce_does_not_cancel_runners_before_the_shutdown_owner_decides() {
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
    assert!(!active[0].cancellation.is_cancelled());
    active[0].cancellation.cancel();
    active.into_iter().next().unwrap().done.await.unwrap();
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
            TaskEventKind::TaskCompleted,
        ]
    );

    let detail = fixture.detail(task.id).await;
    assert_eq!(
        detail.plan,
        Some(PlanSnapshot {
            revision: 1,
            items: vec![
                PlanItem {
                    id: "fake-plan".to_owned(),
                    title: "Prepare deterministic plan".to_owned(),
                    status: PlanItemStatus::Completed,
                },
                PlanItem {
                    id: "fake-diff".to_owned(),
                    title: "Generate synthetic diff".to_owned(),
                    status: PlanItemStatus::Completed,
                },
                PlanItem {
                    id: "fake-tests".to_owned(),
                    title: "Report synthetic tests".to_owned(),
                    status: PlanItemStatus::Completed,
                },
            ],
        })
    );
    assert_eq!(
        detail
            .activity
            .iter()
            .map(|entry| (entry.id.as_str(), entry.message.as_str()))
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
            .all(|entry| entry.created_at == detail.task.started_at.unwrap())
    );
    assert!(
        detail
            .activity
            .iter()
            .all(|entry| entry.level == ActivityLevel::Info)
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
            revision: 2,
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
                revision: 2,
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
async fn scripted_fake_runner_can_ignore_cancellation_until_explicit_release() {
    let fixture =
        support::scripted_fake_runner_fixture([FakeScenario::IgnoresCancellation], 1).await;
    let task = fixture.start("ordinary prompt").await;
    fixture.wait_for_status(task.id, TaskStatus::Running).await;

    assert!(matches!(
        fixture.cancel(task.id).await,
        CancelOutcome::Accepted { .. }
    ));
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    assert_eq!(fixture.load(task.id).await.status, TaskStatus::Running);
    assert!(fixture.runner.release(task.id));
    fixture
        .wait_for_status(task.id, TaskStatus::Completed)
        .await;
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
        let started = fixture.runner.started_task_ids();
        assert_eq!(
            &started[started.len() - 2..],
            &[tasks[0].id, tasks[1].id],
            "iteration {iteration}"
        );
        assert!(fixture.runner.release(tasks[1].id));
        fixture
            .wait_for_status(tasks[1].id, TaskStatus::Completed)
            .await;
    }
}
