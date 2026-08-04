use coding_agent_domain::{EventCursor, TaskEventKind, TaskEventPayload, TaskStatus};
use coding_agent_store::{EventPage, RecoveryReceipt, Store, TaskDetail};

use crate::fixture::CompatibilityFixture;
use crate::snapshot::DeliveryRowsSnapshot;
use crate::transitions;

struct RecoveryBefore {
    delivery: DeliveryRowsSnapshot,
    queued: TaskDetail,
    running: TaskDetail,
    stopped: TaskDetail,
    events: EventPage,
    high_watermark: EventCursor,
}

impl RecoveryBefore {
    async fn capture(fixture: &CompatibilityFixture) -> Self {
        let delivery = DeliveryRowsSnapshot::capture(&fixture.store).await;
        assert!(!delivery.is_empty());
        Self {
            delivery,
            queued: task_detail(&fixture.store, fixture.queued_task.id).await,
            running: task_detail(&fixture.store, fixture.panel_task.id).await,
            stopped: task_detail(&fixture.store, fixture.stopped_task.id).await,
            events: fixture
                .store
                .events_after(EventCursor::ZERO, usize::MAX)
                .await
                .unwrap(),
            high_watermark: fixture.store.latest_event_id().await.unwrap(),
        }
    }
}

pub async fn assert_restart_compatibility(fixture: &CompatibilityFixture) {
    let _ = transitions::create_preflight(&fixture.store, &fixture.delivery_task).await;
    let before = RecoveryBefore::capture(fixture).await;

    let receipt = fixture.store.recover_after_restart().await.unwrap();
    assert_receipt(&fixture.store, &receipt, before.high_watermark).await;
    assert_eq!(
        DeliveryRowsSnapshot::capture(&fixture.store).await,
        before.delivery
    );

    let events_after = fixture
        .store
        .events_after(EventCursor::ZERO, usize::MAX)
        .await
        .unwrap();
    assert_recovery_events(&before.events, &events_after, &receipt);
    assert_queued_unchanged(fixture, &before.queued).await;
    assert_running_interrupted(fixture, &before.running, &events_after).await;
    assert_stop_intent_finalized(fixture, &before.stopped).await;
    assert_replay_is_inert(fixture, &before.delivery, &receipt).await;
}

async fn assert_receipt(store: &Store, receipt: &RecoveryReceipt, before: EventCursor) {
    assert_eq!(receipt.finalized_stop_count, 1);
    assert_eq!(receipt.interrupted_count, 1);
    assert_eq!(
        receipt.first_event_id.map(|id| id.get()),
        Some(before.get() + 1)
    );
    assert_eq!(
        receipt.last_event_id.map(|id| id.get()),
        Some(before.get() + 2)
    );
    assert_eq!(receipt.high_watermark.get(), before.get() + 2);
    assert_eq!(
        receipt.last_event_id.map(|id| id.get()),
        Some(receipt.high_watermark.get())
    );
    assert_eq!(
        receipt.membership_high_watermark,
        store
            .membership_watermark_through(receipt.high_watermark)
            .await
            .unwrap()
    );
    assert_eq!(
        store.latest_event_id().await.unwrap(),
        receipt.high_watermark
    );
}

fn assert_recovery_events(before: &EventPage, after: &EventPage, receipt: &RecoveryReceipt) {
    assert_eq!(
        &after.events[..before.events.len()],
        before.events.as_slice()
    );
    assert_eq!(after.events.len(), before.events.len() + 2);
    assert!(matches!(
        &after.events[before.events.len()].payload,
        TaskEventPayload::TaskCancelled { .. }
    ));
    assert!(matches!(
        &after.events[before.events.len() + 1].payload,
        TaskEventPayload::TaskInterrupted { .. }
    ));
    assert_eq!(after.high_watermark, receipt.high_watermark);
}

async fn assert_queued_unchanged(fixture: &CompatibilityFixture, before: &TaskDetail) {
    let after = task_detail(&fixture.store, fixture.queued_task.id).await;
    assert_eq!(after.task, before.task);
    assert_eq!(after.plan, before.plan);
    assert_eq!(after.activity, before.activity);
    assert_eq!(after.diff, before.diff);
    assert_eq!(after.tests, before.tests);
    assert_eq!(after.reviews, before.reviews);
    assert_eq!(after.timeline, before.timeline);
    assert_eq!(after.task.status, TaskStatus::Queued);
}

async fn assert_running_interrupted(
    fixture: &CompatibilityFixture,
    before: &TaskDetail,
    events_after: &EventPage,
) {
    let after = task_detail(&fixture.store, fixture.panel_task.id).await;
    assert_eq!(before.task.status, TaskStatus::Running);
    assert_eq!(after.task.status, TaskStatus::Interrupted);
    assert_eq!(
        after
            .task
            .failure
            .as_ref()
            .map(|failure| failure.code.as_str()),
        Some("APP_RESTARTED")
    );
    assert_eq!(
        after.task.last_event_id,
        events_after.events.last().unwrap().id
    );
    assert_eq!(
        events_after.events.last().unwrap().payload.kind(),
        TaskEventKind::TaskInterrupted
    );
}

async fn assert_stop_intent_finalized(fixture: &CompatibilityFixture, before: &TaskDetail) {
    let after = task_detail(&fixture.store, fixture.stopped_task.id).await;
    assert_eq!(before.task.status, TaskStatus::Running);
    assert_eq!(after.task.status, TaskStatus::Cancelled);
    assert_eq!(after.task.failure, None);
}

async fn assert_replay_is_inert(
    fixture: &CompatibilityFixture,
    delivery_before: &DeliveryRowsSnapshot,
    applied: &RecoveryReceipt,
) {
    let replay = fixture.store.recover_after_restart().await.unwrap();
    assert_eq!(replay.finalized_stop_count, 0);
    assert_eq!(replay.interrupted_count, 0);
    assert_eq!(replay.first_event_id, None);
    assert_eq!(replay.last_event_id, None);
    assert_eq!(replay.high_watermark, applied.high_watermark);
    assert_eq!(
        replay.membership_high_watermark,
        applied.membership_high_watermark
    );
    assert_eq!(
        DeliveryRowsSnapshot::capture(&fixture.store).await,
        *delivery_before
    );
}

async fn task_detail(store: &Store, task_id: coding_agent_domain::TaskId) -> TaskDetail {
    store.task_detail(task_id).await.unwrap().unwrap()
}
