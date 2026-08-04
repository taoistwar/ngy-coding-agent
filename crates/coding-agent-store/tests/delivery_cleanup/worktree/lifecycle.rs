use super::*;

#[tokio::test]
async fn worktree_cleanup_advances_through_exact_fact_pairs() {
    let (store, task, _) = merged_fixture("codex/task7-remove-phases").await;
    let request = remove_request(&store, &task, ClientRequestId::new()).await;
    let accepted = match store.accept_worktree_cleanup(request).await.unwrap() {
        CleanupAcceptanceOutcome::Accepted(receipt) => receipt,
        other => panic!("expected accepted cleanup, got {other:?}"),
    };

    let unlock = RecordWorktreeUnlockedRequest::try_new(
        CleanupOperationAnchor::try_new(task.id, accepted.operation_id, DeliveryVersion::initial())
            .unwrap(),
    )
    .unwrap();
    let unlocked = applied(
        store
            .record_worktree_unlocked(unlock.clone())
            .await
            .unwrap(),
    );
    assert_eq!(unlocked.version, DeliveryVersion::try_new(2).unwrap());
    assert_eq!(unlocked.state, CleanupOperationState::UnlockedPendingRemove);
    assert_eq!(
        store
            .record_worktree_unlocked(unlock.clone())
            .await
            .unwrap(),
        CleanupTransitionOutcome::Existing(unlocked.clone())
    );

    let disposition = load_disposition(&store, task.id).await;
    assert_eq!(
        disposition.worktree_state,
        WorktreeDisposition::RetainedUnlocked
    );
    assert_eq!(
        disposition.worktree_version,
        DeliveryVersion::try_new(2).unwrap()
    );
    assert_eq!(
        disposition.worktree_cleanup_operation_id,
        Some(accepted.operation_id)
    );
    assert_eq!(
        disposition.worktree_cleanup_operation_version,
        Some(unlocked.version)
    );
    assert_eq!(
        disposition.worktree_cleanup_operation_state,
        Some(CleanupOperationState::UnlockedPendingRemove)
    );

    let enter = EnterWorktreeRemovePendingRequest::try_new(
        CleanupOperationAnchor::try_new(task.id, accepted.operation_id, unlocked.version).unwrap(),
    )
    .unwrap();
    let pending = applied(
        store
            .enter_worktree_remove_pending(enter.clone())
            .await
            .unwrap(),
    );
    assert_eq!(pending.version, DeliveryVersion::try_new(3).unwrap());
    assert_eq!(pending.state, CleanupOperationState::RemovePending);
    assert_eq!(
        store
            .enter_worktree_remove_pending(enter.clone())
            .await
            .unwrap(),
        CleanupTransitionOutcome::Existing(pending.clone())
    );

    let phase_only_disposition = load_disposition(&store, task.id).await;
    assert_eq!(
        phase_only_disposition.worktree_version,
        disposition.worktree_version
    );
    assert_eq!(
        phase_only_disposition.worktree_cleanup_operation_version,
        Some(unlocked.version)
    );

    let complete = CompleteWorktreeCleanupRequest::try_new(
        CleanupOperationAnchor::try_new(task.id, accepted.operation_id, pending.version).unwrap(),
    )
    .unwrap();
    let completed = applied(
        store
            .complete_worktree_cleanup(complete.clone())
            .await
            .unwrap(),
    );
    assert_eq!(completed.version, DeliveryVersion::try_new(4).unwrap());
    assert_eq!(completed.state, CleanupOperationState::Completed);
    assert_eq!(
        store.complete_worktree_cleanup(complete).await.unwrap(),
        CleanupTransitionOutcome::Existing(completed.clone())
    );
    assert_eq!(
        store.record_worktree_unlocked(unlock).await.unwrap(),
        CleanupTransitionOutcome::Existing(unlocked.clone())
    );
    assert_eq!(
        store.enter_worktree_remove_pending(enter).await.unwrap(),
        CleanupTransitionOutcome::Existing(pending.clone())
    );

    let completed_disposition = load_disposition(&store, task.id).await;
    assert_eq!(
        completed_disposition.worktree_state,
        WorktreeDisposition::Removed
    );
    assert_eq!(
        completed_disposition.worktree_version,
        DeliveryVersion::try_new(3).unwrap()
    );
    assert_eq!(
        completed_disposition.worktree_cleanup_operation_version,
        Some(completed.version)
    );
    assert_eq!(
        completed_disposition.worktree_cleanup_operation_state,
        Some(CleanupOperationState::Completed)
    );
    assert_eq!(
        journal_counts(&store, accepted.operation_id, task.id).await,
        (4, 3)
    );
}
