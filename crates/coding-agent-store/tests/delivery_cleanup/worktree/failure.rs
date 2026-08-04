use super::*;

#[tokio::test]
async fn failed_unlocked_remove_retries_with_fresh_receipt_directly_in_remove_pending() {
    let (store, task, _) = merged_fixture("codex/task7-remove-retry").await;
    let first_request = remove_request(&store, &task, ClientRequestId::new()).await;
    let first_receipt = match store
        .accept_worktree_cleanup(first_request.clone())
        .await
        .unwrap()
    {
        CleanupAcceptanceOutcome::Accepted(receipt) => receipt,
        other => panic!("expected accepted cleanup, got {other:?}"),
    };
    let unlocked = applied(
        store
            .record_worktree_unlocked(
                RecordWorktreeUnlockedRequest::try_new(
                    CleanupOperationAnchor::try_new(
                        task.id,
                        first_receipt.operation_id,
                        DeliveryVersion::initial(),
                    )
                    .unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap(),
    );
    let pending = applied(
        store
            .enter_worktree_remove_pending(
                EnterWorktreeRemovePendingRequest::try_new(
                    CleanupOperationAnchor::try_new(
                        task.id,
                        first_receipt.operation_id,
                        unlocked.version,
                    )
                    .unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap(),
    );
    let failure_request = RecordWorktreeCleanupFailureRequest::try_new(
        CleanupOperationAnchor::try_new(task.id, first_receipt.operation_id, pending.version)
            .unwrap(),
        CleanupOperationState::RemovePending,
        WorktreeCleanupKnownNotAppliedReason::TargetWorktreeDirty,
    )
    .unwrap();
    let failed = applied(
        store
            .record_worktree_cleanup_failure(failure_request.clone())
            .await
            .unwrap(),
    );
    assert_eq!(failed.state, CleanupOperationState::Failed);
    assert_eq!(
        failed.failure_code.as_ref().map(|code| code.as_str()),
        Some("TARGET_WORKTREE_DIRTY")
    );
    assert_eq!(
        store
            .record_worktree_cleanup_failure(failure_request.clone())
            .await
            .unwrap(),
        CleanupTransitionOutcome::Existing(failed.clone())
    );
    let retained = load_disposition(&store, task.id).await;
    assert_eq!(
        retained.worktree_state,
        WorktreeDisposition::RetainedUnlocked
    );
    assert_eq!(
        retained.worktree_version,
        DeliveryVersion::try_new(2).unwrap()
    );

    let retry_request = remove_request(&store, &task, ClientRequestId::new()).await;
    let retry_receipt = match store.accept_worktree_cleanup(retry_request).await.unwrap() {
        CleanupAcceptanceOutcome::Accepted(receipt) => receipt,
        other => panic!("expected accepted retry, got {other:?}"),
    };
    assert_ne!(retry_receipt.operation_id, first_receipt.operation_id);
    assert_eq!(
        retry_receipt.accepted_operation_state.as_str(),
        "remove_pending"
    );
    let retry = cleanup_operation(&store, &task, retry_receipt.operation_id).await;
    assert_eq!(retry.state, CleanupOperationState::RemovePending);
    assert_eq!(retry.version, DeliveryVersion::initial());
    assert_eq!(
        store
            .record_worktree_cleanup_failure(failure_request)
            .await
            .unwrap(),
        CleanupTransitionOutcome::Existing(failed)
    );
    assert_eq!(
        store.accept_worktree_cleanup(first_request).await.unwrap(),
        CleanupAcceptanceOutcome::Existing(first_receipt)
    );
}

#[tokio::test]
async fn known_failure_and_timeout_use_disjoint_typed_paths() {
    let (store, task, _) = merged_fixture("codex/task7-remove-reason-matrix").await;
    let request = remove_request(&store, &task, ClientRequestId::new()).await;
    let receipt = match store.accept_worktree_cleanup(request).await.unwrap() {
        CleanupAcceptanceOutcome::Accepted(receipt) => receipt,
        other => panic!("expected accepted cleanup, got {other:?}"),
    };
    let anchor =
        CleanupOperationAnchor::try_new(task.id, receipt.operation_id, DeliveryVersion::initial())
            .unwrap();

    assert!(
        RecordWorktreeCleanupFailureRequest::try_new(
            anchor,
            CleanupOperationState::UnlockPending,
            WorktreeCleanupKnownNotAppliedReason::TargetWorktreeDirty,
        )
        .is_err()
    );
    let known_timeout = RecordWorktreeCleanupFailureRequest::try_new(
        anchor,
        CleanupOperationState::UnlockPending,
        WorktreeCleanupKnownNotAppliedReason::CommandTimedOut,
    )
    .unwrap();
    let failed = applied(
        store
            .record_worktree_cleanup_failure(known_timeout)
            .await
            .unwrap(),
    );
    assert_eq!(failed.state, CleanupOperationState::Failed);
    assert_eq!(
        failed.failure_code.as_ref().map(|code| code.as_str()),
        Some("COMMAND_TIMED_OUT")
    );
    let retained = load_disposition(&store, task.id).await;
    assert_eq!(retained.worktree_state, WorktreeDisposition::RetainedLocked);
    assert_eq!(retained.worktree_version, DeliveryVersion::initial());

    let (unknown_store, unknown_task, _) =
        merged_fixture("codex/task7-remove-timeout-unknown").await;
    let unknown_request =
        remove_request(&unknown_store, &unknown_task, ClientRequestId::new()).await;
    let unknown_receipt = match unknown_store
        .accept_worktree_cleanup(unknown_request)
        .await
        .unwrap()
    {
        CleanupAcceptanceOutcome::Accepted(receipt) => receipt,
        other => panic!("expected accepted cleanup, got {other:?}"),
    };
    let unknown_timeout = ReconcileWorktreeCleanupRequest::try_new(
        CleanupOperationAnchor::try_new(
            unknown_task.id,
            unknown_receipt.operation_id,
            DeliveryVersion::initial(),
        )
        .unwrap(),
        CleanupOperationState::UnlockPending,
        CleanupReconciliationReason::CommandTimedOut,
    )
    .unwrap();
    let reconciled = applied(
        unknown_store
            .reconcile_worktree_cleanup(unknown_timeout)
            .await
            .unwrap(),
    );
    assert_eq!(
        reconciled.state,
        CleanupOperationState::ReconciliationRequired
    );
    assert_eq!(
        reconciled.failure_code.as_ref().map(|code| code.as_str()),
        Some("COMMAND_TIMED_OUT")
    );
    assert_eq!(
        load_disposition(&unknown_store, unknown_task.id)
            .await
            .worktree_state,
        WorktreeDisposition::ReconciliationRequired
    );
}
