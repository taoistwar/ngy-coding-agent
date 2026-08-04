use coding_agent_store::{
    CommitDeliverySourceRequest, DeliverySourceState, DeliverySourceTransitionOutcome,
};

use super::fixtures::{accepted_fixture, commit_pending_fixture};

#[tokio::test]
async fn exact_ref_head_index_worktree_and_object_proof_marks_source_committed() {
    let (store, command) = accepted_fixture().await;
    let (current, anchor, _object, proof) = commit_pending_fixture(&store, &command).await;
    assert!(matches!(
        store
            .commit_delivery_source(
                CommitDeliverySourceRequest::try_new(anchor, current.version, proof).unwrap(),
            )
            .await
            .unwrap(),
        DeliverySourceTransitionOutcome::Applied(ref receipt)
            if receipt.state == DeliverySourceState::Committed
    ));
}
