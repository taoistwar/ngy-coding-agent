#![cfg(feature = "test-support")]

mod support;

use std::time::Duration;

use coding_agent_app::{EventDispatcherError, EventDispatcherHandle};
use coding_agent_domain::{
    DiffFile, DiffFileStatus, DiffSnapshot, EventCursor, ReviewVerdict, TaskEvent, TaskEventKind,
    TaskEventPayload, TaskStatus, TestCase, TestSnapshot, TestStatus,
};
use coding_agent_store::{
    AppendEventOutcome, CreateTaskOutcome, FinalizeReviewedTaskOutcome, TaskTransition,
    TransitionOutcome,
};
use tokio::sync::broadcast::error::TryRecvError;

fn messages(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn activity_message(event: &TaskEvent) -> &str {
    match &event.payload {
        TaskEventPayload::ActivityAppended { entry } => entry.message(),
        payload => panic!("expected activity event, got {:?}", payload.kind()),
    }
}

fn cursor_after(cursor: EventCursor, amount: i64) -> EventCursor {
    EventCursor::new(cursor.get() + amount).expect("construct later event cursor")
}

#[tokio::test]
async fn dispatcher_projects_typed_review_evidence_after_final_panels_and_before_lifecycle() {
    let fixture = support::store_fixture().await;
    let queued = match fixture
        .store
        .create_task(support::new_task(
            fixture.repository.id,
            "project a reviewed result",
        ))
        .await
        .expect("create reviewed dispatcher task")
    {
        CreateTaskOutcome::Created { task, .. } | CreateTaskOutcome::Existing { task } => task,
    };
    let running = match fixture
        .store
        .transition_with_event(queued.id, TaskStatus::Queued, TaskTransition::Running)
        .await
        .expect("start reviewed dispatcher task")
    {
        TransitionOutcome::Applied { task, .. } => task,
        TransitionOutcome::Conflict { .. } => panic!("reviewed dispatcher task must start"),
    };
    let plan = support::fixture_review_plan();
    match fixture
        .store
        .append_running_event(running.id, TaskEventPayload::PlanUpdated { plan })
        .await
        .expect("persist structured review plan")
    {
        AppendEventOutcome::Applied { .. } => {}
        AppendEventOutcome::NotRunning { .. } => {
            panic!("reviewed dispatcher task must remain running")
        }
    }

    let dispatcher = EventDispatcherHandle::spawn(fixture.store.clone(), 16)
        .await
        .expect("spawn reviewed-event dispatcher");
    let mut receiver = dispatcher.subscribe();
    let diff = DiffSnapshot {
        revision: 1,
        files: vec![DiffFile {
            path: "src/lib.rs".to_owned(),
            status: DiffFileStatus::Modified,
            patch: "@@ reviewed patch @@".to_owned(),
            additions: 1,
            deletions: 0,
            truncated: false,
        }],
    };
    let diff_event_id = match fixture
        .store
        .append_running_event(
            running.id,
            TaskEventPayload::DiffUpdated { diff: diff.clone() },
        )
        .await
        .expect("persist final diff panel")
    {
        AppendEventOutcome::Applied { event_id } => event_id,
        AppendEventOutcome::NotRunning { .. } => panic!("final diff must precede finalization"),
    };
    let tests = TestSnapshot {
        revision: 1,
        status: TestStatus::Passed,
        cases: vec![TestCase {
            id: "fixture-cargo-test".to_owned(),
            name: "cargo test".to_owned(),
            status: TestStatus::Passed,
            duration_ms: 10,
            summary: "fixture check passed".to_owned(),
        }],
    };
    let test_event_id = match fixture
        .store
        .append_running_event(
            running.id,
            TaskEventPayload::TestUpdated {
                tests: tests.clone(),
            },
        )
        .await
        .expect("persist final test panel")
    {
        AppendEventOutcome::Applied { event_id } => event_id,
        AppendEventOutcome::NotRunning { .. } => panic!("final tests must precede finalization"),
    };
    let (review, review_event_id, terminal_event_id) = match fixture
        .store
        .finalize_reviewed_task(
            running.id,
            running.repository_id,
            running.attempt,
            support::approved_review(),
        )
        .await
        .expect("atomically finalize approved task")
    {
        FinalizeReviewedTaskOutcome::Applied {
            review,
            review_event_id,
            terminal_event_id,
            ..
        } => (review, review_event_id, terminal_event_id),
        FinalizeReviewedTaskOutcome::Existing { .. } => {
            panic!("first approved finalization must apply")
        }
    };

    dispatcher.wake();
    dispatcher
        .flush_to(EventCursor::new(terminal_event_id.get()).unwrap())
        .await
        .expect("publish through terminal lifecycle");
    let mut events = Vec::new();
    for _ in 0..4 {
        events.push(receiver.recv().await.expect("receive reviewed event"));
    }

    assert_eq!(
        events
            .iter()
            .map(|event| event.payload.kind())
            .collect::<Vec<_>>(),
        [
            TaskEventKind::DiffUpdated,
            TaskEventKind::TestUpdated,
            TaskEventKind::ReviewUpdated,
            TaskEventKind::TaskCompleted,
        ]
    );
    assert_eq!(
        events.iter().map(|event| event.id).collect::<Vec<_>>(),
        [
            diff_event_id,
            test_event_id,
            review_event_id,
            terminal_event_id,
        ]
    );
    assert_eq!(events[0].payload, TaskEventPayload::DiffUpdated { diff });
    assert_eq!(events[1].payload, TaskEventPayload::TestUpdated { tests });
    match &events[2].payload {
        TaskEventPayload::ReviewUpdated { review: projected } => {
            assert_eq!(projected, &review);
            assert_eq!(projected.round(), 1);
            assert_eq!(projected.verdict(), ReviewVerdict::Approved);
            assert_eq!(projected.summary(), "fixture round 1 approved");
        }
        payload => panic!("expected typed review evidence, got {:?}", payload.kind()),
    }
    match &events[3].payload {
        TaskEventPayload::TaskCompleted { task } => {
            assert_eq!(
                task.delivery_readiness,
                coding_agent_domain::DeliveryReadiness::ReviewApproved
            );
            assert_eq!(task.last_event_id, terminal_event_id);
        }
        payload => panic!("expected completed lifecycle, got {:?}", payload.kind()),
    }
}

#[tokio::test]
async fn dispatcher_reads_committed_events_in_id_order() {
    let fixture = support::dispatcher_fixture().await;
    let mut receiver = fixture.dispatcher.subscribe();
    let ids = fixture
        .commit_events_without_wake(&messages(&["first", "second"]))
        .await;

    fixture.dispatcher.wake();
    fixture
        .dispatcher
        .flush_to(EventCursor::new(ids[1].get()).unwrap())
        .await
        .unwrap();

    let first = receiver.recv().await.unwrap();
    let second = receiver.recv().await.unwrap();
    assert_eq!(
        [activity_message(&first), activity_message(&second)],
        ["first", "second"]
    );
    assert_eq!([first.id, second.id], [ids[0], ids[1]]);
    assert!(first.id < second.id);
}

#[tokio::test]
async fn duplicate_wakes_do_not_publish_an_event_twice() {
    let fixture = support::dispatcher_fixture().await;
    let mut receiver = fixture.dispatcher.subscribe();
    let event_id = fixture
        .commit_events_without_wake(&messages(&["only once"]))
        .await[0];

    fixture.dispatcher.wake();
    fixture.dispatcher.wake();
    fixture.dispatcher.wake();
    fixture
        .dispatcher
        .flush_to(EventCursor::new(event_id.get()).unwrap())
        .await
        .unwrap();

    assert_eq!(receiver.recv().await.unwrap().id, event_id);
    tokio::task::yield_now().await;
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
}

#[tokio::test]
async fn periodic_poll_recovers_a_lost_wakeup() {
    let fixture = support::dispatcher_fixture().await;
    let mut receiver = fixture.dispatcher.subscribe();
    let event_id = fixture
        .commit_events_without_wake(&messages(&["lost wakeup"]))
        .await[0];

    let event = tokio::time::timeout(Duration::from_secs(3), receiver.recv())
        .await
        .expect("periodic poll must recover the lost wakeup")
        .unwrap();

    assert_eq!(event.id, event_id);
    assert_eq!(event.payload.kind(), TaskEventKind::ActivityAppended);
}

#[tokio::test]
async fn zero_receivers_still_advance_the_dispatcher_cursor() {
    let fixture = support::dispatcher_fixture().await;
    let event_id = fixture
        .commit_events_without_wake(&messages(&["no listeners"]))
        .await[0];

    fixture.dispatcher.wake();
    fixture
        .dispatcher
        .flush_to(EventCursor::new(event_id.get()).unwrap())
        .await
        .unwrap();

    let mut late_receiver = fixture.dispatcher.subscribe();
    fixture.dispatcher.wake();
    fixture
        .dispatcher
        .flush_to(EventCursor::new(event_id.get()).unwrap())
        .await
        .unwrap();
    tokio::task::yield_now().await;
    assert_eq!(late_receiver.try_recv(), Err(TryRecvError::Empty));
}

#[tokio::test]
async fn dispatcher_drains_more_than_one_store_page_in_order() {
    let fixture = support::dispatcher_fixture().await;
    let mut receiver = fixture.dispatcher.subscribe();
    let expected_messages = (0..300)
        .map(|index| format!("event-{index:03}"))
        .collect::<Vec<_>>();
    let ids = fixture.commit_events_without_wake(&expected_messages).await;

    fixture.dispatcher.wake();
    fixture
        .dispatcher
        .flush_to(EventCursor::new(ids.last().unwrap().get()).unwrap())
        .await
        .unwrap();

    let mut actual_ids = Vec::with_capacity(expected_messages.len());
    let mut actual_messages = Vec::with_capacity(expected_messages.len());
    for _ in 0..expected_messages.len() {
        let event = receiver.recv().await.unwrap();
        actual_ids.push(event.id);
        actual_messages.push(activity_message(&event).to_owned());
    }
    assert_eq!(actual_ids, ids);
    assert_eq!(actual_messages, expected_messages);
    assert!(actual_ids.windows(2).all(|pair| pair[0] < pair[1]));
}

#[tokio::test]
async fn flush_to_polls_immediately_until_its_target_is_published() {
    let fixture = support::dispatcher_fixture().await;
    let mut receiver = fixture.dispatcher.subscribe();
    let event_id = fixture
        .commit_events_without_wake(&messages(&["flush target"]))
        .await[0];

    fixture
        .dispatcher
        .flush_to(EventCursor::new(event_id.get()).unwrap())
        .await
        .unwrap();

    assert_eq!(receiver.recv().await.unwrap().id, event_id);
}

#[tokio::test]
async fn flush_to_current_cursor_succeeds_without_reading_the_store() {
    let fixture = support::dispatcher_fixture().await;
    fixture.store.pool().close().await;

    fixture
        .dispatcher
        .flush_to(fixture.startup_cursor)
        .await
        .unwrap();
}

#[tokio::test]
async fn flush_to_returns_a_store_error_before_reaching_its_target() {
    let fixture = support::dispatcher_fixture().await;
    fixture.store.pool().close().await;

    let error = fixture
        .dispatcher
        .flush_to(cursor_after(fixture.startup_cursor, 1))
        .await
        .unwrap_err();

    assert!(matches!(error, EventDispatcherError::Store(_)));
}

#[tokio::test]
async fn concurrent_flushes_complete_only_as_their_targets_are_published() {
    let fixture = support::dispatcher_fixture().await;
    let first_target = cursor_after(fixture.startup_cursor, 1);
    let second_target = cursor_after(fixture.startup_cursor, 2);
    let first_dispatcher = fixture.dispatcher.clone();
    let second_dispatcher = fixture.dispatcher.clone();
    let first_flush = tokio::spawn(async move { first_dispatcher.flush_to(first_target).await });
    let second_flush = tokio::spawn(async move { second_dispatcher.flush_to(second_target).await });
    tokio::task::yield_now().await;
    assert!(!first_flush.is_finished());
    assert!(!second_flush.is_finished());

    let ids = fixture
        .commit_events_without_wake(&messages(&["first target", "second target"]))
        .await;
    assert_eq!(ids[0].get(), first_target.get());
    assert_eq!(ids[1].get(), second_target.get());
    fixture.dispatcher.wake();

    first_flush.await.unwrap().unwrap();
    second_flush.await.unwrap().unwrap();
}

#[tokio::test]
async fn startup_high_watermark_is_not_replayed_as_a_live_event() {
    let fixture = support::dispatcher_fixture().await;
    let mut receiver = fixture.dispatcher.subscribe();

    fixture.dispatcher.wake();
    fixture
        .dispatcher
        .flush_to(fixture.startup_cursor)
        .await
        .unwrap();
    tokio::task::yield_now().await;

    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
}

#[tokio::test]
async fn explicit_startup_cursor_must_equal_the_durable_high_watermark() {
    let fixture = support::store_fixture().await;
    let high_watermark = fixture
        .store
        .latest_event_id()
        .await
        .expect("read durable high watermark");
    let mismatched = cursor_after(high_watermark, 1);

    let error = match EventDispatcherHandle::spawn_at(fixture.store, 16, mismatched).await {
        Ok(_) => panic!("reject a cursor not returned by the recovery transaction"),
        Err(error) => error,
    };

    assert!(matches!(error, EventDispatcherError::StartupCursorMismatch));
}

#[tokio::test]
async fn close_is_concurrent_idempotent_and_rejects_late_flushes() {
    let fixture = support::dispatcher_fixture().await;
    let first = fixture.dispatcher.clone();
    let second = fixture.dispatcher.clone();

    let first_close = tokio::spawn(async move { first.close().await });
    let second_close = tokio::spawn(async move { second.close().await });

    first_close.await.unwrap().unwrap();
    second_close.await.unwrap().unwrap();
    fixture.dispatcher.close().await.unwrap();
    assert!(matches!(
        fixture.dispatcher.flush_to(fixture.startup_cursor).await,
        Err(EventDispatcherError::Closed)
    ));
}

#[tokio::test]
async fn close_follows_a_reachable_flush_in_fifo_order() {
    let fixture = support::dispatcher_fixture().await;
    let mut receiver = fixture.dispatcher.subscribe();
    let event_id = fixture
        .commit_events_without_wake(&messages(&["flush before close"]))
        .await[0];
    let target = EventCursor::new(event_id.get()).unwrap();
    let flush = fixture.dispatcher.flush_to(target);
    tokio::pin!(flush);
    let first_poll = futures_util::poll!(&mut flush);

    fixture.dispatcher.close().await.unwrap();

    match first_poll {
        std::task::Poll::Ready(result) => result.unwrap(),
        std::task::Poll::Pending => flush.await.unwrap(),
    }
    assert_eq!(receiver.recv().await.unwrap().id, event_id);
}

#[tokio::test]
async fn close_fails_a_pending_unreachable_flush() {
    let fixture = support::dispatcher_fixture().await;
    let target = cursor_after(fixture.startup_cursor, 1);
    let flush = fixture.dispatcher.flush_to(target);
    tokio::pin!(flush);
    assert!(futures_util::poll!(&mut flush).is_pending());

    fixture.dispatcher.close().await.unwrap();

    assert!(matches!(flush.await, Err(EventDispatcherError::Closed)));
}
