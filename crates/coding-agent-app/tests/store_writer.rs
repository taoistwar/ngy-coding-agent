mod support;

use std::sync::Arc;

use coding_agent_app::{ServiceState, ServiceStateController, StoreWriterError, StoreWriterHandle};
use coding_agent_domain::{PlanSnapshot, TaskEventPayload, TaskStatus};
use coding_agent_store::{
    AppendEventOutcome, RegisterRepositoryOutcome, TaskTransition, TransitionOutcome,
};
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
async fn expired_deadline_skips_transaction_and_leaves_task_uncommitted() {
    let fixture = support::writer_fixture().await;

    let result = fixture
        .writer
        .create_task(
            support::new_task(fixture.repository.id, "must not commit"),
            Instant::now(),
        )
        .await;

    assert!(matches!(result, Err(StoreWriterError::Busy)));
    assert!(
        fixture
            .store
            .bootstrap_snapshot()
            .await
            .unwrap()
            .tasks
            .is_empty()
    );
    assert_eq!(fixture.wake.count(), 0);
}

#[tokio::test]
async fn real_sqlite_busy_exhaustion_is_uncommitted_and_does_not_wake() {
    let fixture = support::writer_fixture().await;
    let options = fixture
        .store
        .pool()
        .connect_options()
        .as_ref()
        .clone()
        .busy_timeout(Duration::ZERO);
    fixture.store.pool().set_connect_options(options);
    let existing_connections = fixture.store.pool().size();
    let mut legacy_connections = Vec::with_capacity(existing_connections as usize);
    for _ in 0..existing_connections {
        legacy_connections.push(
            fixture
                .store
                .pool()
                .acquire()
                .await
                .expect("reserve connection with the original busy timeout"),
        );
    }
    let transaction = fixture
        .store
        .pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("hold the SQLite writer lock");

    let result = fixture
        .writer
        .create_task(
            support::new_task(fixture.repository.id, "real SQLite busy"),
            support::deadline(),
        )
        .await;

    assert!(matches!(result, Err(StoreWriterError::Busy)));
    assert!(
        fixture
            .store
            .bootstrap_snapshot()
            .await
            .unwrap()
            .tasks
            .is_empty()
    );
    assert_eq!(fixture.wake.count(), 0);
    transaction.rollback().await.unwrap();
    drop(legacy_connections);
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
