use crate::fixture::CompatibilityFixture;
use crate::snapshot::CompatibilitySnapshot;

use super::preflight::{self, AcceptedPreflight};

pub async fn fresh() -> (CompatibilityFixture, CompatibilitySnapshot) {
    let fixture = CompatibilityFixture::new().await;
    let baseline = CompatibilitySnapshot::capture(&fixture.store).await;
    baseline.assert_all_event_kinds();
    (fixture, baseline)
}

pub async fn accepted() -> (
    CompatibilityFixture,
    CompatibilitySnapshot,
    AcceptedPreflight,
) {
    let (fixture, baseline) = fresh().await;
    let accepted =
        preflight::accept_ready_preflight(&fixture.store, &fixture.delivery_task, &baseline).await;
    (fixture, baseline, accepted)
}

pub async fn committed_source() -> (
    CompatibilityFixture,
    CompatibilitySnapshot,
    AcceptedPreflight,
) {
    let (fixture, baseline, accepted) = accepted().await;
    super::source::commit_delivery_source(
        &fixture.store,
        &fixture.delivery_task,
        &accepted,
        &baseline,
    )
    .await;
    (fixture, baseline, accepted)
}

pub async fn merged() -> (
    CompatibilityFixture,
    CompatibilitySnapshot,
    AcceptedPreflight,
) {
    let (fixture, baseline, accepted) = committed_source().await;
    super::merge::complete_delivery_merge(
        &fixture.store,
        &fixture.delivery_task,
        &accepted,
        &baseline,
    )
    .await;
    (fixture, baseline, accepted)
}
