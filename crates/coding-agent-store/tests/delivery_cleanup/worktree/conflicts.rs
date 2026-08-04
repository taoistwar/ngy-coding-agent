use super::*;

#[tokio::test]
async fn illegal_worktree_phase_is_a_zero_write_conflict() {
    let (store, task, _) = merged_fixture("codex/task7-remove-conflict").await;
    let request = remove_request(&store, &task, ClientRequestId::new()).await;
    let receipt = match store.accept_worktree_cleanup(request).await.unwrap() {
        CleanupAcceptanceOutcome::Accepted(receipt) => receipt,
        other => panic!("expected accepted cleanup, got {other:?}"),
    };
    let before = all_delivery_counts(&store, task.id).await;
    let illegal = EnterWorktreeRemovePendingRequest::try_new(
        CleanupOperationAnchor::try_new(task.id, receipt.operation_id, DeliveryVersion::initial())
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        store.enter_worktree_remove_pending(illegal).await.unwrap(),
        CleanupTransitionOutcome::Conflict
    );
    assert_eq!(all_delivery_counts(&store, task.id).await, before);
}
