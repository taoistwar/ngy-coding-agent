mod support;

use std::time::Duration;

use coding_agent_app::EventDispatcherError;
use coding_agent_domain::{EventCursor, TaskEvent, TaskEventKind, TaskEventPayload};
use tokio::sync::broadcast::error::TryRecvError;

fn messages(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn activity_message(event: &TaskEvent) -> &str {
    match &event.payload {
        TaskEventPayload::ActivityAppended { entry } => &entry.message,
        payload => panic!("expected activity event, got {:?}", payload.kind()),
    }
}

fn cursor_after(cursor: EventCursor, amount: i64) -> EventCursor {
    EventCursor::new(cursor.get() + amount).expect("construct later event cursor")
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
