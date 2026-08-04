use coding_agent_store::{
    AdvanceDeliverySourceObjectRequest, DeliverySourceRetryReason, DeliverySourceState,
    DeliverySourceTransitionOutcome, DeliveryVersion, RecordDeliverySourceRetryRequest,
};

use super::fixtures::{
    accepted_fixture, created_source, object_proof, source_anchor, source_journal_count,
};

#[tokio::test]
async fn exact_object_proof_advances_object_pending_to_commit_pending() {
    let (store, command) = accepted_fixture().await;
    let source = created_source(&store, command.clone()).await;
    let outcome = store
        .advance_delivery_source_object(
            AdvanceDeliverySourceObjectRequest::try_new(
                source_anchor(&command),
                source.version,
                object_proof(&source),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        DeliverySourceTransitionOutcome::Applied(ref receipt)
            if receipt.state == DeliverySourceState::CommitPending
                && receipt.version == DeliveryVersion::try_new(2).unwrap()
    ));
}

#[tokio::test]
async fn old_version_replays_use_exact_target_journal_after_later_progress() {
    let (store, command) = accepted_fixture().await;
    let source = created_source(&store, command.clone()).await;
    let anchor = source_anchor(&command);
    let advance =
        AdvanceDeliverySourceObjectRequest::try_new(anchor, source.version, object_proof(&source))
            .unwrap();
    assert!(matches!(
        store
            .advance_delivery_source_object(advance.clone())
            .await
            .unwrap(),
        DeliverySourceTransitionOutcome::Applied(_)
    ));
    let retry = RecordDeliverySourceRetryRequest::try_new(
        anchor,
        DeliverySourceState::CommitPending,
        DeliveryVersion::try_new(2).unwrap(),
        DeliverySourceRetryReason::CommandTimedOut,
    )
    .unwrap();
    assert!(matches!(
        store.record_delivery_source_retry(retry).await.unwrap(),
        DeliverySourceTransitionOutcome::Applied(_)
    ));
    assert!(matches!(
        store
            .advance_delivery_source_object(advance)
            .await
            .unwrap(),
        DeliverySourceTransitionOutcome::Existing(ref receipt)
            if receipt.version == DeliveryVersion::try_new(2).unwrap()
    ));
    assert_eq!(source_journal_count(&store, command.task_id()).await, 3);
}

#[tokio::test]
async fn retry_replay_remains_existing_after_object_phase_advances() {
    let (store, command) = accepted_fixture().await;
    let source = created_source(&store, command.clone()).await;
    let anchor = source_anchor(&command);
    let retry = RecordDeliverySourceRetryRequest::try_new(
        anchor,
        DeliverySourceState::ObjectPending,
        source.version,
        DeliverySourceRetryReason::CommandTimedOut,
    )
    .unwrap();
    store
        .record_delivery_source_retry(retry.clone())
        .await
        .unwrap();
    store
        .advance_delivery_source_object(
            AdvanceDeliverySourceObjectRequest::try_new(
                anchor,
                DeliveryVersion::try_new(2).unwrap(),
                object_proof(&source),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        store.record_delivery_source_retry(retry).await.unwrap(),
        DeliverySourceTransitionOutcome::Existing(ref receipt)
            if receipt.version == DeliveryVersion::try_new(2).unwrap()
    ));
    assert_eq!(source_journal_count(&store, command.task_id()).await, 3);
}
