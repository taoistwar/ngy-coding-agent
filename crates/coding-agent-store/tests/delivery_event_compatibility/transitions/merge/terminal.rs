use coding_agent_store::{
    DeliveryVersion, MergeKnownNotAppliedReason, MergeOperationState,
    RecordMergeKnownFailureRequest,
};

use super::super::helpers::applied_merge;
use super::super::scenario;
use super::enter_pending;

pub async fn exercise_known_failure_transitions() {
    fail_accepted().await;
    fail_merge_pending().await;
}

async fn fail_accepted() {
    let (fixture, baseline, accepted) = scenario::committed_source().await;
    record_known_failure(
        &fixture,
        &baseline,
        accepted.operation_id,
        MergeOperationState::Accepted,
        accepted.version,
        "accepted to failed",
    )
    .await;
}

async fn fail_merge_pending() {
    let (fixture, baseline, accepted) = scenario::committed_source().await;
    let version = enter_pending(&fixture.store, &fixture.delivery_task, &accepted, &baseline).await;
    record_known_failure(
        &fixture,
        &baseline,
        accepted.operation_id,
        MergeOperationState::MergePending,
        version,
        "merge pending to failed",
    )
    .await;
}

async fn record_known_failure(
    fixture: &crate::fixture::CompatibilityFixture,
    baseline: &crate::snapshot::CompatibilitySnapshot,
    operation_id: coding_agent_store::DeliveryOperationId,
    state: MergeOperationState,
    version: DeliveryVersion,
    label: &str,
) {
    applied_merge(
        fixture
            .store
            .record_merge_known_failure(
                RecordMergeKnownFailureRequest::try_new(
                    fixture.delivery_task.id,
                    operation_id,
                    state,
                    version,
                    MergeKnownNotAppliedReason::CommandTimedOut,
                )
                .unwrap(),
            )
            .await
            .unwrap(),
    );
    baseline.assert_unchanged(&fixture.store, label).await;
}
