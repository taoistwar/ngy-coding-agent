use super::*;

#[tokio::test]
async fn worktree_reconciliation_changes_both_facts_and_replays_exactly() {
    let (store, task, _) = merged_fixture("codex/task7-remove-reconcile").await;
    let request = remove_request(&store, &task, ClientRequestId::new()).await;
    let receipt = match store.accept_worktree_cleanup(request).await.unwrap() {
        CleanupAcceptanceOutcome::Accepted(receipt) => receipt,
        other => panic!("expected accepted cleanup, got {other:?}"),
    };
    let reconcile = ReconcileWorktreeCleanupRequest::try_new(
        CleanupOperationAnchor::try_new(task.id, receipt.operation_id, DeliveryVersion::initial())
            .unwrap(),
        CleanupOperationState::UnlockPending,
        CleanupReconciliationReason::WorktreeIdentityMismatch,
    )
    .unwrap();
    let reconciled = applied(
        store
            .reconcile_worktree_cleanup(reconcile.clone())
            .await
            .unwrap(),
    );
    assert_eq!(
        reconciled.state,
        CleanupOperationState::ReconciliationRequired
    );
    assert_eq!(
        reconciled.failure_code.as_ref().map(|code| code.as_str()),
        Some("WORKTREE_IDENTITY_MISMATCH")
    );
    assert_eq!(
        store.reconcile_worktree_cleanup(reconcile).await.unwrap(),
        CleanupTransitionOutcome::Existing(reconciled.clone())
    );
    let disposition = load_disposition(&store, task.id).await;
    assert_eq!(
        disposition.worktree_state,
        WorktreeDisposition::ReconciliationRequired
    );
    assert_eq!(disposition.worktree_failure_code, reconciled.failure_code);
    assert_eq!(
        disposition.worktree_cleanup_operation_version,
        Some(reconciled.version)
    );
    assert_eq!(disposition.worktree_updated_at, reconciled.transitioned_at);
}
