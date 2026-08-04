use super::*;

#[tokio::test]
async fn branch_completion_changes_fact_and_operation_atomically() {
    let (store, task, _) = merged_fixture("codex/task7-branch-complete").await;
    remove_worktree_fully(&store, &task).await;
    let request = delete_request(
        &store,
        &task,
        ClientRequestId::new(),
        target_head("3333333333333333333333333333333333333333"),
    )
    .await;
    let receipt = accepted(store.accept_branch_cleanup(request.clone()).await.unwrap());
    let complete = CompleteBranchCleanupRequest::try_new(
        CleanupOperationAnchor::try_new(task.id, receipt.operation_id, DeliveryVersion::initial())
            .unwrap(),
    )
    .unwrap();

    let completed = applied(
        store
            .complete_branch_cleanup(complete.clone())
            .await
            .unwrap(),
    );
    assert_eq!(completed.version, DeliveryVersion::try_new(2).unwrap());
    assert_eq!(completed.state, CleanupOperationState::Completed);
    assert_eq!(
        store.complete_branch_cleanup(complete).await.unwrap(),
        CleanupTransitionOutcome::Existing(completed.clone())
    );
    assert_eq!(
        store.accept_branch_cleanup(request).await.unwrap(),
        CleanupAcceptanceOutcome::Existing(receipt)
    );

    let disposition = load_disposition(&store, task.id).await;
    assert_eq!(disposition.worktree_state, WorktreeDisposition::Removed);
    assert_eq!(disposition.branch_state, BranchDisposition::Deleted);
    assert_eq!(
        disposition.branch_cleanup_operation_id,
        Some(completed.operation_id)
    );
    assert_eq!(
        disposition.branch_cleanup_operation_version,
        Some(completed.version)
    );
    assert_eq!(
        disposition.branch_cleanup_operation_state,
        Some(CleanupOperationState::Completed)
    );
    assert_eq!(disposition.branch_updated_at, completed.transitioned_at);
}

#[tokio::test]
async fn known_branch_failure_preserves_fact_and_fresh_receipt_can_retry() {
    let (store, task, _) = merged_fixture("codex/task7-branch-retry").await;
    remove_worktree_fully(&store, &task).await;
    let first_request = delete_request(
        &store,
        &task,
        ClientRequestId::new(),
        target_head("3333333333333333333333333333333333333333"),
    )
    .await;
    let first = accepted(
        store
            .accept_branch_cleanup(first_request.clone())
            .await
            .unwrap(),
    );
    let failure = RecordBranchCleanupFailureRequest::try_new(
        CleanupOperationAnchor::try_new(task.id, first.operation_id, DeliveryVersion::initial())
            .unwrap(),
        BranchCleanupKnownNotAppliedReason::SourceBranchNotMerged,
    )
    .unwrap();
    let failed = applied(
        store
            .record_branch_cleanup_failure(failure.clone())
            .await
            .unwrap(),
    );
    assert_eq!(failed.state, CleanupOperationState::Failed);
    assert_eq!(
        failed.failure_code.as_ref().map(|code| code.as_str()),
        Some("SOURCE_BRANCH_NOT_MERGED")
    );
    let retained = load_disposition(&store, task.id).await;
    assert_eq!(retained.branch_state, BranchDisposition::Retained);
    assert_eq!(retained.branch_version, DeliveryVersion::initial());

    let retry_request = delete_request(
        &store,
        &task,
        ClientRequestId::new(),
        target_head("4444444444444444444444444444444444444444"),
    )
    .await;
    let retry = accepted(store.accept_branch_cleanup(retry_request).await.unwrap());
    assert_ne!(retry.operation_id, first.operation_id);
    let retry_operation = cleanup_operation(&store, &task, retry.operation_id).await;
    assert_eq!(retry_operation.state, CleanupOperationState::DeletePending);
    assert_eq!(
        store.record_branch_cleanup_failure(failure).await.unwrap(),
        CleanupTransitionOutcome::Existing(failed)
    );
    assert_eq!(
        store.accept_branch_cleanup(first_request).await.unwrap(),
        CleanupAcceptanceOutcome::Existing(first)
    );
}

#[tokio::test]
async fn branch_timeout_uses_known_or_unknown_typed_path_from_post_observation() {
    let (known_store, known_task, _) = merged_fixture("codex/task7-branch-timeout-known").await;
    remove_worktree_fully(&known_store, &known_task).await;
    let known_request = delete_request(
        &known_store,
        &known_task,
        ClientRequestId::new(),
        target_head("3333333333333333333333333333333333333333"),
    )
    .await;
    let known_receipt = accepted(
        known_store
            .accept_branch_cleanup(known_request)
            .await
            .unwrap(),
    );
    let known_timeout = RecordBranchCleanupFailureRequest::try_new(
        CleanupOperationAnchor::try_new(
            known_task.id,
            known_receipt.operation_id,
            DeliveryVersion::initial(),
        )
        .unwrap(),
        BranchCleanupKnownNotAppliedReason::CommandTimedOut,
    )
    .unwrap();
    let failed = applied(
        known_store
            .record_branch_cleanup_failure(known_timeout.clone())
            .await
            .unwrap(),
    );
    assert_eq!(
        failed.failure_code.as_ref().map(|code| code.as_str()),
        Some("COMMAND_TIMED_OUT")
    );
    assert_eq!(
        load_disposition(&known_store, known_task.id)
            .await
            .branch_state,
        BranchDisposition::Retained
    );
    assert_eq!(
        known_store
            .record_branch_cleanup_failure(known_timeout)
            .await
            .unwrap(),
        CleanupTransitionOutcome::Existing(failed)
    );
    let changed_reason = RecordBranchCleanupFailureRequest::try_new(
        CleanupOperationAnchor::try_new(
            known_task.id,
            known_receipt.operation_id,
            DeliveryVersion::initial(),
        )
        .unwrap(),
        BranchCleanupKnownNotAppliedReason::SourceBranchNotMerged,
    )
    .unwrap();
    let before_conflict = delivery_counts(&known_store, known_task.id).await;
    assert_eq!(
        known_store
            .record_branch_cleanup_failure(changed_reason)
            .await
            .unwrap(),
        CleanupTransitionOutcome::Conflict
    );
    assert_eq!(
        delivery_counts(&known_store, known_task.id).await,
        before_conflict
    );

    let (unknown_store, unknown_task, _) =
        merged_fixture("codex/task7-branch-timeout-unknown").await;
    remove_worktree_fully(&unknown_store, &unknown_task).await;
    let unknown_request = delete_request(
        &unknown_store,
        &unknown_task,
        ClientRequestId::new(),
        target_head("3333333333333333333333333333333333333333"),
    )
    .await;
    let unknown_receipt = accepted(
        unknown_store
            .accept_branch_cleanup(unknown_request)
            .await
            .unwrap(),
    );
    let unknown_timeout = ReconcileBranchCleanupRequest::try_new(
        CleanupOperationAnchor::try_new(
            unknown_task.id,
            unknown_receipt.operation_id,
            DeliveryVersion::initial(),
        )
        .unwrap(),
        CleanupReconciliationReason::CommandTimedOut,
    )
    .unwrap();
    let reconciled = applied(
        unknown_store
            .reconcile_branch_cleanup(unknown_timeout)
            .await
            .unwrap(),
    );
    assert_eq!(
        reconciled.failure_code.as_ref().map(|code| code.as_str()),
        Some("COMMAND_TIMED_OUT")
    );
    assert_eq!(
        load_disposition(&unknown_store, unknown_task.id)
            .await
            .branch_state,
        BranchDisposition::ReconciliationRequired
    );
}

#[tokio::test]
async fn branch_reconciliation_changes_both_sides_with_one_reason_and_timestamp() {
    let (store, task, _) = merged_fixture("codex/task7-branch-reconcile").await;
    remove_worktree_fully(&store, &task).await;
    let request = delete_request(
        &store,
        &task,
        ClientRequestId::new(),
        target_head("3333333333333333333333333333333333333333"),
    )
    .await;
    let receipt = accepted(store.accept_branch_cleanup(request).await.unwrap());
    let reconcile = ReconcileBranchCleanupRequest::try_new(
        CleanupOperationAnchor::try_new(task.id, receipt.operation_id, DeliveryVersion::initial())
            .unwrap(),
        CleanupReconciliationReason::SourceInconsistent,
    )
    .unwrap();
    let reconciled = applied(
        store
            .reconcile_branch_cleanup(reconcile.clone())
            .await
            .unwrap(),
    );
    assert_eq!(
        reconciled.state,
        CleanupOperationState::ReconciliationRequired
    );
    assert_eq!(
        store.reconcile_branch_cleanup(reconcile).await.unwrap(),
        CleanupTransitionOutcome::Existing(reconciled.clone())
    );
    let disposition = load_disposition(&store, task.id).await;
    assert_eq!(
        disposition.branch_state,
        BranchDisposition::ReconciliationRequired
    );
    assert_eq!(disposition.branch_failure_code, reconciled.failure_code);
    assert_eq!(disposition.branch_updated_at, reconciled.transitioned_at);
}
