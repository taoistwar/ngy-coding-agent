use super::*;

#[tokio::test]
async fn target_refresh_replays_from_immutable_per_version_heads() {
    let (store, task, _) = merged_fixture("codex/task7-branch-refresh").await;
    remove_worktree_fully(&store, &task).await;
    let head_a = target_head("3333333333333333333333333333333333333333");
    let head_b = target_head("4444444444444444444444444444444444444444");
    let head_c = target_head("5555555555555555555555555555555555555555");
    let head_d = target_head("6666666666666666666666666666666666666666");
    let request = delete_request(&store, &task, ClientRequestId::new(), head_a.clone()).await;
    let accepted = accepted(store.accept_branch_cleanup(request).await.unwrap());
    let first_refresh = RefreshBranchCleanupTargetRequest::try_new(
        CleanupOperationAnchor::try_new(task.id, accepted.operation_id, DeliveryVersion::initial())
            .unwrap(),
        head_a.clone(),
        head_b.clone(),
    )
    .unwrap();
    let refreshed_b = applied(
        store
            .refresh_branch_cleanup_target(first_refresh.clone())
            .await
            .unwrap(),
    );
    assert_eq!(refreshed_b.version, DeliveryVersion::try_new(2).unwrap());
    assert_eq!(refreshed_b.state, CleanupOperationState::DeletePending);

    let second_refresh = RefreshBranchCleanupTargetRequest::try_new(
        CleanupOperationAnchor::try_new(task.id, accepted.operation_id, refreshed_b.version)
            .unwrap(),
        head_b.clone(),
        head_c.clone(),
    )
    .unwrap();
    let refreshed_c = applied(
        store
            .refresh_branch_cleanup_target(second_refresh.clone())
            .await
            .unwrap(),
    );
    assert_eq!(refreshed_c.version, DeliveryVersion::try_new(3).unwrap());

    assert_eq!(
        store
            .refresh_branch_cleanup_target(first_refresh.clone())
            .await
            .unwrap(),
        CleanupTransitionOutcome::Existing(refreshed_b.clone())
    );
    let forged_fresh_head = RefreshBranchCleanupTargetRequest::try_new(
        CleanupOperationAnchor::try_new(task.id, accepted.operation_id, DeliveryVersion::initial())
            .unwrap(),
        head_a.clone(),
        head_d.clone(),
    )
    .unwrap();
    let before_conflict = delivery_counts(&store, task.id).await;
    assert_eq!(
        store
            .refresh_branch_cleanup_target(forged_fresh_head)
            .await
            .unwrap(),
        CleanupTransitionOutcome::Conflict
    );
    assert_eq!(delivery_counts(&store, task.id).await, before_conflict);
    let forged_expected_head = RefreshBranchCleanupTargetRequest::try_new(
        CleanupOperationAnchor::try_new(task.id, accepted.operation_id, DeliveryVersion::initial())
            .unwrap(),
        head_d,
        head_b.clone(),
    )
    .unwrap();
    assert_eq!(
        store
            .refresh_branch_cleanup_target(forged_expected_head)
            .await
            .unwrap(),
        CleanupTransitionOutcome::Conflict
    );
    assert_eq!(delivery_counts(&store, task.id).await, before_conflict);
    assert!(
        RefreshBranchCleanupTargetRequest::try_new(
            CleanupOperationAnchor::try_new(task.id, accepted.operation_id, refreshed_c.version)
                .unwrap(),
            head_c.clone(),
            head_c,
        )
        .is_err()
    );

    let complete = CompleteBranchCleanupRequest::try_new(
        CleanupOperationAnchor::try_new(task.id, accepted.operation_id, refreshed_c.version)
            .unwrap(),
    )
    .unwrap();
    let completed = applied(store.complete_branch_cleanup(complete).await.unwrap());
    assert_eq!(completed.version, DeliveryVersion::try_new(4).unwrap());
    assert_eq!(
        store
            .refresh_branch_cleanup_target(first_refresh)
            .await
            .unwrap(),
        CleanupTransitionOutcome::Existing(refreshed_b)
    );
    assert_eq!(
        store
            .refresh_branch_cleanup_target(second_refresh)
            .await
            .unwrap(),
        CleanupTransitionOutcome::Existing(refreshed_c)
    );

    let operation = cleanup_operation(&store, &task, accepted.operation_id).await;
    assert_eq!(operation.origin_target_head.as_ref(), Some(&head_a));
    assert_eq!(operation.target_head_observations.len(), 4);
    assert_eq!(
        operation.target_head_at(DeliveryVersion::try_new(2).unwrap()),
        Some(&head_b)
    );
}
