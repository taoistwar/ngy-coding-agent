// This test crate intentionally uses only the recovery subset of the shared
// merge fixture surface.
#[allow(dead_code, unused_imports)]
mod delivery_merge_support;
mod support;

use coding_agent_app::{
    DeliveryMergeReceiptDisposition, RepositoryControlState, recover_delivery_startup_for_test,
};
use coding_agent_store::{DeliverySourceState, MergeOperationState, MergeReconciliationReason};

use delivery_merge_support::{DeliveryMergeFixture, LiveFault, LiveStage};

#[tokio::test]
async fn startup_recovery_drives_only_the_durable_merge_pending_operation() {
    let mut fixture = DeliveryMergeFixture::new(None).await;
    fixture
        .live_runtime
        .fail_once(LiveStage::ActualMerge, LiveFault::Unavailable);
    let prepared = fixture.prepare_accept().await;
    let accepted = fixture.accept(&prepared).await;
    assert_eq!(accepted.receipt(), DeliveryMergeReceiptDisposition::Created);
    fixture
        .wait_source_state(prepared.task.id, DeliverySourceState::Committed)
        .await;
    fixture
        .wait_operation_state(prepared.operation_id, MergeOperationState::MergePending)
        .await;
    let receipts_before = accept_receipt_count(&fixture).await;

    fixture.restart_manager().await;
    let summary = recover_delivery_startup_for_test(
        &fixture.base.store,
        fixture.manager(),
        fixture.coordinator.as_ref(),
    )
    .await
    .expect("startup delivery recovery converges");

    assert_eq!(summary, (1, 1, 0));
    fixture
        .wait_operation_state(prepared.operation_id, MergeOperationState::Merged)
        .await;
    assert_eq!(accept_receipt_count(&fixture).await, receipts_before);
    fixture.finish().await;
}

#[tokio::test]
async fn startup_recovery_rehydrates_exact_abort_pending_without_a_new_receipt() {
    let mut fixture = DeliveryMergeFixture::new(None).await;
    fixture.live_runtime.use_conflict();
    fixture
        .live_runtime
        .fail_once(LiveStage::Abort, LiveFault::Unavailable);
    let prepared = fixture.prepare_accept().await;
    let accepted = fixture.accept(&prepared).await;
    assert_eq!(accepted.receipt(), DeliveryMergeReceiptDisposition::Created);
    let pending = fixture
        .wait_operation_state(prepared.operation_id, MergeOperationState::AbortPending)
        .await;
    let child_receipt = pending
        .abort_child_receipt_id
        .expect("abort pending has one durable child receipt");
    let receipts_before = accept_receipt_count(&fixture).await;

    fixture.restart_manager().await;
    let summary = recover_delivery_startup_for_test(
        &fixture.base.store,
        fixture.manager(),
        fixture.coordinator.as_ref(),
    )
    .await
    .expect("startup abort recovery converges");

    assert_eq!(summary, (1, 1, 0));
    let conflict = fixture
        .wait_operation_state(prepared.operation_id, MergeOperationState::Conflict)
        .await;
    assert_eq!(conflict.abort_child_receipt_id, Some(child_receipt));
    assert_eq!(accept_receipt_count(&fixture).await, receipts_before);
    fixture.finish().await;
}

#[tokio::test]
async fn startup_recovery_does_not_turn_preflight_ready_into_user_acceptance() {
    let fixture = DeliveryMergeFixture::new(None).await;
    let prepared = fixture.prepare_accept().await;
    let receipts_before = accept_receipt_count(&fixture).await;

    let summary = recover_delivery_startup_for_test(
        &fixture.base.store,
        fixture.manager(),
        fixture.coordinator.as_ref(),
    )
    .await
    .expect("ready preflight is not recovery work");

    assert_eq!(summary, (1, 0, 0));
    assert_eq!(
        fixture.operation(prepared.operation_id).await.state,
        MergeOperationState::PreflightReady
    );
    assert!(fixture.source(prepared.task.id).await.is_none());
    assert_eq!(accept_receipt_count(&fixture).await, receipts_before);
    fixture.finish().await;
}

#[tokio::test]
async fn startup_recovery_leaves_later_same_identity_intent_pending_without_more_git() {
    let mut fixture = DeliveryMergeFixture::new(None).await;
    let earlier = fixture.prepare_accept().await;
    let later = fixture.prepare_accept().await;

    fixture
        .live_runtime
        .fail_once(LiveStage::ActualMerge, LiveFault::Unavailable);
    fixture.accept(&later).await;
    fixture
        .wait_operation_state(later.operation_id, MergeOperationState::MergePending)
        .await;
    fixture
        .wait_repository_state(RepositoryControlState::Available)
        .await;

    fixture.live_runtime.fail_once(
        LiveStage::SourceCommit,
        LiveFault::ReconciliationRequired(MergeReconciliationReason::SourceInconsistent),
    );
    fixture.accept(&earlier).await;
    fixture
        .wait_operation_state(
            earlier.operation_id,
            MergeOperationState::ReconciliationRequired,
        )
        .await;

    fixture
        .restart_manager_with_fresh_repository_control()
        .await;
    let summary = recover_delivery_startup_for_test(
        &fixture.base.store,
        fixture.manager(),
        fixture.coordinator.as_ref(),
    )
    .await
    .expect("one poisoned identity does not fail the rest of startup");

    assert_eq!(summary, (2, 0, 1));
    assert_eq!(
        fixture.operation(later.operation_id).await.state,
        MergeOperationState::MergePending
    );
    fixture
        .wait_repository_state(RepositoryControlState::Poisoned)
        .await;
    fixture.finish().await;
}

async fn accept_receipt_count(fixture: &DeliveryMergeFixture) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_delivery_command_receipts \
         WHERE task_id = ? AND command_kind = 'accept_merge'",
    )
    .bind(
        fixture
            .base
            .store
            .startup_delivery_ownership()
            .await
            .expect("load delivery ownership")
            .first()
            .expect("fixture owns one task")
            .identity
            .task_id()
            .to_string(),
    )
    .fetch_one(fixture.base.store.pool())
    .await
    .expect("count accept receipts")
}
