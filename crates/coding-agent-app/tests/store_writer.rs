mod support;

use std::sync::Arc;

use coding_agent_app::{ServiceState, ServiceStateController, StoreWriterError, StoreWriterHandle};
use coding_agent_domain::{PlanSnapshot, TaskEventPayload, TaskStatus};
use coding_agent_store::{
    AppendEventOutcome, CreateTaskOutcome, RegisterRepositoryOutcome, TaskTransition,
    TransitionOutcome,
};
use support::{FaultControlledStore, InjectedFault};
use tokio::time::{Duration, Instant};

#[tokio::test]
async fn writer_serializes_concurrent_creates() {
    let fixture = support::writer_fixture().await;
    let a = fixture.writer.create_task(
        support::new_task(fixture.repository.id, "a"),
        support::deadline(),
    );
    let b = fixture.writer.create_task(
        support::new_task(fixture.repository.id, "b"),
        support::deadline(),
    );
    let (a, b) = tokio::join!(a, b);

    assert!(a.unwrap().event_id < b.unwrap().event_id);
}

#[tokio::test]
async fn writer_retries_two_busy_attempts_then_commits_once() {
    let fixture = support::store_fixture().await;
    let backend = Arc::new(FaultControlledStore::new(
        fixture.store.clone(),
        [InjectedFault::Busy, InjectedFault::Busy],
    ));
    let wake = Arc::new(support::CountingWake::default());
    let writer = StoreWriterHandle::spawn_with_backend(backend.clone(), wake.clone(), 4);

    let receipt = writer
        .create_task(
            support::new_task(fixture.repository.id, "retry transient busy"),
            support::deadline(),
        )
        .await
        .expect("third attempt commits");

    assert!(matches!(receipt.value, CreateTaskOutcome::Created { .. }));
    assert_eq!(backend.attempts(), 3);
    assert_eq!(
        fixture
            .store
            .bootstrap_snapshot()
            .await
            .unwrap()
            .tasks
            .len(),
        1
    );
    assert_eq!(wake.count(), 1);
}

#[tokio::test]
async fn writer_uses_the_exact_bounded_retry_schedule() {
    let fixture = support::store_fixture().await;
    tokio::time::pause();
    let backend = Arc::new(FaultControlledStore::new(
        fixture.store.clone(),
        [
            InjectedFault::Busy,
            InjectedFault::Busy,
            InjectedFault::Busy,
            InjectedFault::Busy,
            InjectedFault::Busy,
            InjectedFault::Busy,
        ],
    ));
    let writer = StoreWriterHandle::spawn_with_backend(
        backend.clone(),
        Arc::new(support::CountingWake::default()),
        4,
    );
    let request = tokio::spawn({
        let writer = writer.clone();
        let input = support::new_task(fixture.repository.id, "bounded retries");
        async move { writer.create_task(input, support::deadline()).await }
    });

    wait_for_attempts(&backend, 1).await;
    for (index, delay_ms) in [25_u64, 50, 100, 200, 400].into_iter().enumerate() {
        tokio::time::advance(Duration::from_millis(delay_ms - 1)).await;
        tokio::task::yield_now().await;
        assert_eq!(backend.attempts(), index + 1);
        // Tokio's paused timer wheel wakes a deadline on the following 1 ms tick.
        tokio::time::advance(Duration::from_millis(2)).await;
        wait_for_attempts(&backend, index + 2).await;
    }
    let result = request.await.unwrap();

    assert!(matches!(result, Err(StoreWriterError::Busy)));
    assert_eq!(backend.attempts(), 6);
}

#[tokio::test]
async fn deadline_expiring_during_backoff_prevents_the_next_attempt() {
    let fixture = support::store_fixture().await;
    tokio::time::pause();
    let backend = Arc::new(FaultControlledStore::new(
        fixture.store.clone(),
        [InjectedFault::Busy],
    ));
    let wake = Arc::new(support::CountingWake::default());
    let writer = StoreWriterHandle::spawn_with_backend(backend.clone(), wake.clone(), 4);

    let result = writer
        .create_task(
            support::new_task(fixture.repository.id, "deadline during backoff"),
            Instant::now() + Duration::from_millis(10),
        )
        .await;

    assert!(matches!(result, Err(StoreWriterError::Busy)));
    assert_eq!(backend.attempts(), 1);
    assert!(
        fixture
            .store
            .bootstrap_snapshot()
            .await
            .unwrap()
            .tasks
            .is_empty()
    );
    assert_eq!(wake.count(), 0);
}

#[tokio::test]
async fn expired_deadline_skips_transaction_and_leaves_task_uncommitted() {
    let fixture = support::store_fixture().await;
    let backend = Arc::new(FaultControlledStore::new(fixture.store.clone(), []));
    let wake = Arc::new(support::CountingWake::default());
    let writer = StoreWriterHandle::spawn_with_backend(backend.clone(), wake.clone(), 4);

    let result = writer
        .create_task(
            support::new_task(fixture.repository.id, "must not commit"),
            Instant::now(),
        )
        .await;

    assert!(matches!(result, Err(StoreWriterError::Busy)));
    assert_eq!(backend.attempts(), 0);
    assert!(
        fixture
            .store
            .bootstrap_snapshot()
            .await
            .unwrap()
            .tasks
            .is_empty()
    );
    assert_eq!(wake.count(), 0);
}

#[tokio::test]
async fn terminal_rolled_back_write_is_not_retried_or_woken() {
    let fixture = support::store_fixture().await;
    let backend = Arc::new(FaultControlledStore::new(
        fixture.store.clone(),
        [InjectedFault::Terminal],
    ));
    let wake = Arc::new(support::CountingWake::default());
    let writer = StoreWriterHandle::spawn_with_backend(backend.clone(), wake.clone(), 4);

    let result = writer
        .create_task(
            support::new_task(fixture.repository.id, "rolled back"),
            support::deadline(),
        )
        .await;

    assert!(matches!(result, Err(StoreWriterError::Store(_))));
    assert_eq!(backend.attempts(), 1);
    assert!(
        fixture
            .store
            .bootstrap_snapshot()
            .await
            .unwrap()
            .tasks
            .is_empty()
    );
    assert_eq!(wake.count(), 0);
}

#[tokio::test]
async fn repository_only_write_does_not_wake_dispatcher() {
    let fixture = support::writer_fixture().await;

    let receipt = fixture
        .writer
        .register_repository(fixture.repository_input("second"), support::deadline())
        .await
        .unwrap();

    assert!(matches!(
        receipt.value,
        RegisterRepositoryOutcome::Created(_)
    ));
    assert_eq!(receipt.event_id, None);
    assert_eq!(fixture.wake.count(), 0);
}

#[tokio::test]
async fn committed_event_outcomes_wake_once_and_non_events_do_not() {
    let fixture = support::writer_fixture().await;
    let input = support::new_task(fixture.repository.id, "event wake matrix");
    let created = fixture
        .writer
        .create_task(input.clone(), support::deadline())
        .await
        .unwrap();
    let task = created.value.task().clone();
    assert_eq!(fixture.wake.count(), 1);

    let duplicate = fixture
        .writer
        .create_task(input, support::deadline())
        .await
        .unwrap();
    assert_eq!(duplicate.event_id, None);
    assert_eq!(fixture.wake.count(), 1);

    let running = fixture
        .writer
        .transition_with_event(
            task.id,
            TaskStatus::Queued,
            TaskTransition::Running,
            support::deadline(),
        )
        .await
        .unwrap();
    assert!(matches!(running.value, TransitionOutcome::Applied { .. }));
    assert_eq!(fixture.wake.count(), 2);

    let panel = fixture
        .writer
        .append_running_event(
            task.id,
            TaskEventPayload::PlanUpdated {
                plan: PlanSnapshot {
                    revision: 1,
                    items: Vec::new(),
                },
            },
            support::deadline(),
        )
        .await
        .unwrap();
    assert!(matches!(panel.value, AppendEventOutcome::Applied { .. }));
    assert_eq!(fixture.wake.count(), 3);

    let conflict = fixture
        .writer
        .transition_with_event(
            task.id,
            TaskStatus::Queued,
            TaskTransition::Cancelled,
            support::deadline(),
        )
        .await
        .unwrap();
    assert!(matches!(conflict.value, TransitionOutcome::Conflict { .. }));
    assert_eq!(conflict.event_id, None);
    assert_eq!(fixture.wake.count(), 3);
}

#[tokio::test]
async fn retry_task_is_idempotent_and_wakes_only_for_the_new_child_event() {
    let fixture = support::writer_fixture().await;
    let created = fixture
        .writer
        .create_task(
            support::new_task(fixture.repository.id, "retry source"),
            support::deadline(),
        )
        .await
        .unwrap();
    let task = created.value.task().clone();
    fixture
        .writer
        .transition_with_event(
            task.id,
            TaskStatus::Queued,
            TaskTransition::Cancelled,
            support::deadline(),
        )
        .await
        .unwrap();
    let before = fixture.wake.count();

    let first = fixture
        .writer
        .retry_task(task.id, support::deadline())
        .await
        .unwrap();
    let second = fixture
        .writer
        .retry_task(task.id, support::deadline())
        .await
        .unwrap();

    assert!(first.event_id.is_some());
    assert_eq!(second.event_id, None);
    assert_eq!(first.value.task().id, second.value.task().id);
    assert_eq!(fixture.wake.count(), before + 1);
}

#[tokio::test]
async fn panicking_wake_cannot_turn_durable_commit_into_failure() {
    let fixture = support::store_fixture().await;
    let writer =
        StoreWriterHandle::spawn(fixture.store.clone(), Arc::new(support::PanickingWake), 4);

    let receipt = writer
        .create_task(
            support::new_task(fixture.repository.id, "wake panic"),
            support::deadline(),
        )
        .await
        .expect("durable receipt survives wake panic");

    assert!(receipt.event_id.is_some());
    assert_eq!(
        fixture
            .store
            .bootstrap_snapshot()
            .await
            .unwrap()
            .tasks
            .len(),
        1
    );
}

#[tokio::test]
async fn bulk_recovery_preserves_outcome_watermark_and_wakes_once() {
    let fixture = support::writer_fixture().await;
    for prompt in ["recover a", "recover b"] {
        fixture
            .writer
            .create_task(
                support::new_task(fixture.repository.id, prompt),
                support::deadline(),
            )
            .await
            .unwrap();
    }
    let before = fixture.wake.count();

    let receipt = fixture
        .writer
        .recover_incomplete(
            support::timestamp(),
            support::failure("APP_RESTARTED"),
            support::deadline(),
        )
        .await
        .unwrap();

    assert_eq!(receipt.value.interrupted_count, 2);
    assert_eq!(receipt.event_id, receipt.value.last_event_id);
    assert_eq!(
        receipt.value.high_watermark.get(),
        receipt.value.last_event_id.unwrap().get()
    );
    assert_eq!(fixture.wake.count(), before + 1);
}

#[tokio::test]
async fn service_state_generation_never_moves_backwards() {
    let state = ServiceStateController::new(ServiceState::Ready);
    let a = state.set(ServiceState::StoreDegraded).unwrap();
    let b = state.set(ServiceState::Ready).unwrap();
    assert_eq!(a.generation + 1, b.generation);
    assert_eq!(state.current(), b);
}

#[tokio::test]
async fn service_state_same_value_is_unchanged_and_quiescing_is_terminal() {
    let state = ServiceStateController::new(ServiceState::Ready);
    let mut changes = state.subscribe();
    let initial = state.current();
    assert_eq!(state.set(ServiceState::Ready).unwrap(), initial);
    assert!(!changes.has_changed().unwrap());

    let degraded = state.set(ServiceState::StoreDegraded).unwrap();
    assert_eq!(changes.changed().await.unwrap(), ());
    assert_eq!(*changes.borrow_and_update(), degraded);
    let quiescing = state.set(ServiceState::Quiescing).unwrap();
    assert_eq!(quiescing.generation, degraded.generation + 1);
    assert!(state.set(ServiceState::Ready).is_err());
    assert_eq!(state.current(), quiescing);
}

#[tokio::test]
async fn started_attempt_finishes_even_if_request_future_is_dropped() {
    let fixture = support::store_fixture().await;
    let pause = Arc::new(support::PausePoint::default());
    let backend = Arc::new(FaultControlledStore::paused(
        fixture.store.clone(),
        pause.clone(),
    ));
    let writer = StoreWriterHandle::spawn_with_backend(
        backend,
        Arc::new(support::CountingWake::default()),
        4,
    );
    let request = tokio::spawn({
        let writer = writer.clone();
        let input = support::new_task(fixture.repository.id, "detached request");
        async move { writer.create_task(input, support::deadline()).await }
    });
    pause.started.notified().await;
    request.abort();
    pause.release.notify_one();

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if !fixture
                .store
                .bootstrap_snapshot()
                .await
                .unwrap()
                .tasks
                .is_empty()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actor completes an already-started transaction");
}

async fn wait_for_attempts(backend: &FaultControlledStore, expected: usize) {
    for _ in 0..100 {
        if backend.attempts() == expected {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!(
        "store attempts did not reach {expected}; observed {}",
        backend.attempts()
    );
}
