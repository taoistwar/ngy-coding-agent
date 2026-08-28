use coding_agent_store::{
    DeliveryOperationId, DeliveryVersion, MergeOperationState, MergeReconciliationReason,
    ReconcileMergeRequest,
};

use super::super::helpers::applied_merge;
use super::super::preflight;
use super::super::scenario;
use super::abort::begin_abort;
use super::enter_pending;

pub async fn exercise_reconcile_transitions() {
    reconcile_preflight_pending().await;
    reconcile_preflight_ready().await;
    reconcile_accepted().await;
    reconcile_merge_pending().await;
    reconcile_abort_pending().await;
}

async fn reconcile_preflight_pending() {
    let (fixture, baseline) = scenario::fresh().await;
    let operation_id = preflight::create_preflight(&fixture.store, &fixture.delivery_task).await;
    baseline
        .assert_unchanged(&fixture.store, "create preflight before reconcile")
        .await;
    reconcile(
        &fixture,
        &baseline,
        operation_id,
        MergeOperationState::PreflightPending,
        DeliveryVersion::try_new(2).unwrap(),
        "preflight pending to reconciliation required",
    )
    .await;
}

async fn reconcile_preflight_ready() {
    let (fixture, baseline) = scenario::fresh().await;
    let operation_id = preflight::create_preflight(&fixture.store, &fixture.delivery_task).await;
    baseline
        .assert_unchanged(&fixture.store, "create ready preflight before reconcile")
        .await;
    let version =
        preflight::record_ready(&fixture.store, fixture.delivery_task.id, operation_id).await;
    baseline
        .assert_unchanged(
            &fixture.store,
            "preflight pending to ready before reconcile",
        )
        .await;
    reconcile(
        &fixture,
        &baseline,
        operation_id,
        MergeOperationState::PreflightReady,
        version,
        "preflight ready to reconciliation required",
    )
    .await;
}

async fn reconcile_accepted() {
    let (fixture, baseline, accepted) = scenario::committed_source().await;
    reconcile(
        &fixture,
        &baseline,
        accepted.operation_id,
        MergeOperationState::Accepted,
        accepted.version,
        "accepted to reconciliation required",
    )
    .await;
}

async fn reconcile_merge_pending() {
    let (fixture, baseline, accepted) = scenario::committed_source().await;
    let version = enter_pending(&fixture.store, &fixture.delivery_task, &accepted, &baseline).await;
    reconcile(
        &fixture,
        &baseline,
        accepted.operation_id,
        MergeOperationState::MergePending,
        version,
        "merge pending to reconciliation required",
    )
    .await;
}

async fn reconcile_abort_pending() {
    let (fixture, baseline, accepted) = scenario::committed_source().await;
    let pending_version =
        enter_pending(&fixture.store, &fixture.delivery_task, &accepted, &baseline).await;
    let version = begin_abort(
        &fixture.store,
        &fixture.delivery_task,
        accepted.operation_id,
        pending_version,
        &baseline,
    )
    .await;
    reconcile(
        &fixture,
        &baseline,
        accepted.operation_id,
        MergeOperationState::AbortPending,
        version,
        "abort pending to reconciliation required",
    )
    .await;
}

async fn reconcile(
    fixture: &crate::fixture::CompatibilityFixture,
    baseline: &crate::snapshot::CompatibilitySnapshot,
    operation_id: DeliveryOperationId,
    state: MergeOperationState,
    version: DeliveryVersion,
    label: &str,
) {
    applied_merge(
        fixture
            .store
            .reconcile_merge(
                ReconcileMergeRequest::try_new(
                    fixture.delivery_task.id,
                    operation_id,
                    state,
                    version,
                    MergeReconciliationReason::DeliveryStateInconsistent,
                )
                .unwrap(),
            )
            .await
            .unwrap(),
    );
    baseline.assert_unchanged(&fixture.store, label).await;
}
