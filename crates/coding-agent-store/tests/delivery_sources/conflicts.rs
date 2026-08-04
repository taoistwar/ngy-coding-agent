use coding_agent_domain::TaskId;
use coding_agent_store::{
    AdvanceDeliverySourceObjectRequest, DeliveryOperationId, DeliverySourceAnchor,
    DeliverySourceTransitionOutcome, DeliveryVersion,
};

use super::fixtures::{
    accepted_fixture, created_source, object_proof, source_anchor, source_journal_count,
};

#[tokio::test]
async fn caller_anchor_and_expected_version_mismatches_are_typed_conflicts_without_writes() {
    let (store, command) = accepted_fixture().await;
    let source = created_source(&store, command.clone()).await;
    let exact = source_anchor(&command);
    let anchors = [
        DeliverySourceAnchor::try_new(
            TaskId::new(),
            exact.accepted_operation_id(),
            exact.accepted_receipt_version(),
        )
        .unwrap(),
        DeliverySourceAnchor::try_new(
            exact.task_id(),
            DeliveryOperationId::new(),
            exact.accepted_receipt_version(),
        )
        .unwrap(),
        DeliverySourceAnchor::try_new(
            exact.task_id(),
            exact.accepted_operation_id(),
            DeliveryVersion::try_new(4).unwrap(),
        )
        .unwrap(),
    ];
    for anchor in anchors {
        let request = AdvanceDeliverySourceObjectRequest::try_new(
            anchor,
            source.version,
            object_proof(&source),
        )
        .unwrap();
        assert!(matches!(
            store.advance_delivery_source_object(request).await.unwrap(),
            DeliverySourceTransitionOutcome::Conflict
        ));
        assert_eq!(source_journal_count(&store, command.task_id()).await, 1);
    }

    let wrong_source_version = AdvanceDeliverySourceObjectRequest::try_new(
        exact,
        DeliveryVersion::try_new(2).unwrap(),
        object_proof(&source),
    )
    .unwrap();
    assert!(matches!(
        store
            .advance_delivery_source_object(wrong_source_version)
            .await
            .unwrap(),
        DeliverySourceTransitionOutcome::Conflict
    ));
    assert_eq!(source_journal_count(&store, command.task_id()).await, 1);
}
