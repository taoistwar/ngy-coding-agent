use super::*;

#[tokio::test]
async fn branch_acceptance_is_query_first_and_binds_immutable_target_origin() {
    let (store, task, _) = merged_fixture("codex/task7-branch-accept").await;
    remove_worktree_fully(&store, &task).await;
    let request = delete_request(
        &store,
        &task,
        ClientRequestId::new(),
        target_head("3333333333333333333333333333333333333333"),
    )
    .await;

    let receipt = match store.accept_branch_cleanup(request.clone()).await.unwrap() {
        CleanupAcceptanceOutcome::Accepted(receipt) => receipt,
        other => panic!("expected accepted cleanup, got {other:?}"),
    };
    assert_eq!(
        receipt.accepted_operation_state,
        DeliveryAcceptedOperationState::DeletePending
    );
    let operation = cleanup_operation(&store, &task, receipt.operation_id).await;
    assert_eq!(operation.state, CleanupOperationState::DeletePending);
    assert_eq!(operation.version, DeliveryVersion::initial());
    assert_eq!(
        operation.origin_target_head.as_ref(),
        Some(request.target_head())
    );
    assert_eq!(
        operation.expected_target_head.as_ref(),
        Some(request.target_head())
    );
    assert_eq!(
        operation.expected_target_ref.as_ref(),
        Some(request.target_branch())
    );
    assert_eq!(
        store.accept_branch_cleanup(request).await.unwrap(),
        CleanupAcceptanceOutcome::Existing(receipt)
    );
}

#[tokio::test]
async fn wrong_caller_and_cross_action_receipt_leave_zero_writes() {
    let (store, task, _) = merged_fixture("codex/task7-branch-conflicts").await;
    let shared = ClientRequestId::new();
    remove_worktree_fully_with_client(&store, &task, shared).await;
    let cross_action = delete_request(
        &store,
        &task,
        shared,
        target_head("3333333333333333333333333333333333333333"),
    )
    .await;
    let before_cross = delivery_counts(&store, task.id).await;
    assert!(matches!(
        store.accept_branch_cleanup(cross_action).await,
        Err(StoreError::IdempotencyConflict)
    ));
    assert_eq!(delivery_counts(&store, task.id).await, before_cross);

    let request = delete_request(
        &store,
        &task,
        ClientRequestId::new(),
        target_head("3333333333333333333333333333333333333333"),
    )
    .await;
    let receipt = accepted(store.accept_branch_cleanup(request).await.unwrap());
    let wrong_caller = CompleteBranchCleanupRequest::try_new(
        CleanupOperationAnchor::try_new(
            TaskId::new(),
            receipt.operation_id,
            DeliveryVersion::initial(),
        )
        .unwrap(),
    )
    .unwrap();
    let before_wrong = delivery_counts(&store, task.id).await;
    assert_eq!(
        store.complete_branch_cleanup(wrong_caller).await.unwrap(),
        CleanupTransitionOutcome::Conflict
    );
    assert_eq!(delivery_counts(&store, task.id).await, before_wrong);
}
