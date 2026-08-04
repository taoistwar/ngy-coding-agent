mod support;

#[path = "delivery_event_compatibility/fixture.rs"]
mod fixture;
#[path = "delivery_event_compatibility/recovery.rs"]
mod recovery;
#[path = "delivery_event_compatibility/snapshot.rs"]
mod snapshot;
#[path = "delivery_event_compatibility/transitions/mod.rs"]
mod transitions;

use fixture::CompatibilityFixture;
use snapshot::{CompatibilitySnapshot, DeliveryRowsSnapshot};

#[tokio::test]
async fn every_delivery_transition_is_inert_for_p4a_rows_and_typed_projections() {
    let fixture = CompatibilityFixture::new().await;
    let baseline = CompatibilitySnapshot::capture(&fixture.store).await;
    baseline.assert_all_event_kinds();

    transitions::exercise_every_delivery_transition(
        &fixture.store,
        &fixture.delivery_task,
        &baseline,
    )
    .await;
}

#[tokio::test]
async fn every_legal_failure_retry_abort_and_reconcile_transition_is_event_inert() {
    transitions::exercise_every_alternate_delivery_transition().await;
}

#[tokio::test]
async fn compatibility_snapshot_detects_and_then_releases_a_test_local_event_coupling() {
    let fixture = CompatibilityFixture::new().await;
    let before = CompatibilitySnapshot::capture(&fixture.store).await;

    fixture.install_event_coupling_trigger().await;
    let first_operation =
        transitions::create_preflight(&fixture.store, &fixture.delivery_task).await;
    let coupled = CompatibilitySnapshot::capture(&fixture.store).await;

    assert_ne!(coupled.durable.tasks, before.durable.tasks);
    assert_ne!(coupled.durable.events, before.durable.events);
    assert_ne!(
        coupled.durable.high_watermark,
        before.durable.high_watermark
    );
    assert_ne!(coupled.event_page, before.event_page);
    assert_ne!(coupled.latest_event_id, before.latest_event_id);

    fixture.remove_event_coupling_trigger().await;
    transitions::record_conflict(&fixture.store, fixture.delivery_task.id, first_operation).await;
    let uncoupled = CompatibilitySnapshot::capture(&fixture.store).await;
    let _ = transitions::create_preflight(&fixture.store, &fixture.delivery_task).await;
    uncoupled
        .assert_unchanged(&fixture.store, "preflight after dropping test trigger")
        .await;
}

#[tokio::test]
async fn restart_recovery_preserves_delivery_rows_and_p4a_queued_semantics() {
    let fixture = CompatibilityFixture::new().await;
    recovery::assert_restart_compatibility(&fixture).await;
}

#[tokio::test]
async fn fresh_v1_through_v5_migration_creates_no_synthetic_delivery_rows() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();

    let delivery = DeliveryRowsSnapshot::capture(&fixture.store).await;
    assert!(
        delivery.is_empty(),
        "fresh migration fabricated delivery state: {delivery:?}"
    );

    fixture.store.migrate().await.unwrap();
    assert_eq!(
        DeliveryRowsSnapshot::capture(&fixture.store).await,
        delivery
    );
}
