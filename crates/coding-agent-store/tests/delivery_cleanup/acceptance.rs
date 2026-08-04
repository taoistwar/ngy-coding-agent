use coding_agent_domain::ClientRequestId;
use coding_agent_store::{
    CleanupAcceptanceOutcome, CleanupOperationState, DeliveryAcceptedOperationState,
    WorktreeDisposition,
};

use super::fixtures::{cleanup_operation, merged_fixture, remove_request};

#[tokio::test]
async fn first_remove_receipt_creates_unlock_pending_without_changing_facts() {
    let (store, task, _) = merged_fixture("codex/task7-remove-accept").await;
    let request = remove_request(&store, &task, ClientRequestId::new()).await;

    let receipt = match store.accept_worktree_cleanup(request).await.unwrap() {
        CleanupAcceptanceOutcome::Accepted(receipt) => receipt,
        other => panic!("expected accepted cleanup, got {other:?}"),
    };

    assert_eq!(
        receipt.accepted_operation_state,
        DeliveryAcceptedOperationState::UnlockPending
    );
    let operation = cleanup_operation(&store, &task, receipt.operation_id).await;
    assert_eq!(operation.state, CleanupOperationState::UnlockPending);
    let disposition = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .disposition
        .unwrap();
    assert_eq!(
        disposition.worktree_state,
        WorktreeDisposition::RetainedLocked
    );
    assert_eq!(disposition.worktree_cleanup_operation_id, None);
}

#[tokio::test]
async fn exact_remove_receipt_replay_returns_existing_without_new_rows() {
    let (store, task, _) = merged_fixture("codex/task7-remove-replay").await;
    let request = remove_request(&store, &task, ClientRequestId::new()).await;
    let first = store
        .accept_worktree_cleanup(request.clone())
        .await
        .unwrap();
    let first_receipt = match first {
        CleanupAcceptanceOutcome::Accepted(receipt) => receipt,
        other => panic!("expected accepted cleanup, got {other:?}"),
    };

    let replay = store.accept_worktree_cleanup(request).await.unwrap();
    let replay_receipt = match replay {
        CleanupAcceptanceOutcome::Existing(receipt) => receipt,
        other => panic!("expected existing cleanup, got {other:?}"),
    };
    assert_eq!(replay_receipt, first_receipt);

    let counts: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM task_cleanup_operations WHERE task_id = ?), \
                (SELECT COUNT(*) FROM task_delivery_command_receipts \
                  WHERE task_id = ? AND command_kind = 'remove_worktree')",
    )
    .bind(task.id.to_string())
    .bind(task.id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(counts, (1, 1));
}
