use coding_agent_domain::{ClientRequestId, Task};
use coding_agent_store::{
    CleanupAcceptanceOutcome, CleanupOperationAnchor, CleanupOperationRecord,
    CompleteWorktreeCleanupRequest, DeleteBranchCommandRequest, DeliveryOperationId,
    DeliveryVersion, EnterWorktreeRemovePendingRequest, GitCommitOid,
    RecordWorktreeUnlockedRequest, RemoveWorktreeCommandRequest, Store,
};

use crate::support::delivery::eligibility::{
    approved_task_on_store, approved_task_with_ready_artifact, create_merged_delivery,
};

pub async fn merged_fixture(branch: &str) -> (Store, Task, DeliveryOperationId) {
    let (store, task) = approved_task_with_ready_artifact(branch).await;
    let eligible = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    let merge_id =
        create_merged_delivery(&store, &task, eligible.evidence_identity.as_ref().unwrap()).await;
    (store, task, merge_id)
}

pub async fn file_backed_merged_fixture(
    branch: &str,
) -> (crate::support::FileStoreFixture, Task, DeliveryOperationId) {
    let fixture = crate::support::file_store().await;
    fixture.store.migrate().await.unwrap();
    crate::support::register_repository(&fixture.store, "task7-cleanup-file").await;
    let (_, task) = approved_task_on_store(fixture.store.clone(), branch, 0).await;
    let eligible = fixture
        .store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    let merge_id = create_merged_delivery(
        &fixture.store,
        &task,
        eligible.evidence_identity.as_ref().unwrap(),
    )
    .await;
    (fixture, task, merge_id)
}

pub async fn remove_request(
    store: &Store,
    task: &Task,
    client_request_id: ClientRequestId,
) -> RemoveWorktreeCommandRequest {
    let snapshot = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    let source = snapshot.source.as_ref().unwrap();
    let disposition = snapshot.disposition.as_ref().unwrap();
    RemoveWorktreeCommandRequest::try_new(
        client_request_id,
        task.id,
        disposition.worktree_version,
        disposition.merged_operation_id,
        source.provenance.source_branch.clone(),
        source.expected_source_commit.clone().unwrap(),
    )
    .unwrap()
}

pub async fn delete_request(
    store: &Store,
    task: &Task,
    client_request_id: ClientRequestId,
    target_head: GitCommitOid,
) -> DeleteBranchCommandRequest {
    let snapshot = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    let source = snapshot.source.as_ref().unwrap();
    let disposition = snapshot.disposition.as_ref().unwrap();
    let merged = snapshot
        .merge_operations
        .iter()
        .find(|operation| operation.operation_id == disposition.merged_operation_id)
        .unwrap();
    DeleteBranchCommandRequest::try_new(
        client_request_id,
        task.id,
        disposition.branch_version,
        disposition.merged_operation_id,
        source.provenance.source_branch.clone(),
        source.expected_source_commit.clone().unwrap(),
        merged.target_branch.clone(),
        target_head,
    )
    .unwrap()
}

pub async fn cleanup_operation(
    store: &Store,
    task: &Task,
    operation_id: DeliveryOperationId,
) -> CleanupOperationRecord {
    store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .cleanup_operations
        .into_iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap()
}

pub async fn remove_worktree_fully(store: &Store, task: &Task) -> DeliveryOperationId {
    remove_worktree_fully_with_client(store, task, ClientRequestId::new()).await
}

pub async fn remove_worktree_fully_with_client(
    store: &Store,
    task: &Task,
    client_request_id: ClientRequestId,
) -> DeliveryOperationId {
    let request = remove_request(store, task, client_request_id).await;
    let receipt = match store.accept_worktree_cleanup(request).await.unwrap() {
        CleanupAcceptanceOutcome::Accepted(receipt) => receipt,
        other => panic!("expected accepted cleanup, got {other:?}"),
    };
    let unlocked = match store
        .record_worktree_unlocked(
            RecordWorktreeUnlockedRequest::try_new(
                CleanupOperationAnchor::try_new(
                    task.id,
                    receipt.operation_id,
                    DeliveryVersion::initial(),
                )
                .unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap()
    {
        coding_agent_store::CleanupTransitionOutcome::Applied(receipt) => receipt,
        other => panic!("expected unlocked cleanup, got {other:?}"),
    };
    let pending = match store
        .enter_worktree_remove_pending(
            EnterWorktreeRemovePendingRequest::try_new(
                CleanupOperationAnchor::try_new(task.id, receipt.operation_id, unlocked.version)
                    .unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap()
    {
        coding_agent_store::CleanupTransitionOutcome::Applied(receipt) => receipt,
        other => panic!("expected pending cleanup, got {other:?}"),
    };
    match store
        .complete_worktree_cleanup(
            CompleteWorktreeCleanupRequest::try_new(
                CleanupOperationAnchor::try_new(task.id, receipt.operation_id, pending.version)
                    .unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap()
    {
        coding_agent_store::CleanupTransitionOutcome::Applied(_) => receipt.operation_id,
        other => panic!("expected completed cleanup, got {other:?}"),
    }
}

pub async fn remove_pending_fixture(
    branch: &str,
) -> (
    crate::support::FileStoreFixture,
    Task,
    DeliveryOperationId,
    DeliveryVersion,
) {
    let (fixture, task, _) = file_backed_merged_fixture(branch).await;
    let store = fixture.store.clone();
    let request = remove_request(&store, &task, ClientRequestId::new()).await;
    let receipt = match store.accept_worktree_cleanup(request).await.unwrap() {
        CleanupAcceptanceOutcome::Accepted(receipt) => receipt,
        other => panic!("expected accepted cleanup, got {other:?}"),
    };
    let unlocked = match store
        .record_worktree_unlocked(
            RecordWorktreeUnlockedRequest::try_new(
                CleanupOperationAnchor::try_new(
                    task.id,
                    receipt.operation_id,
                    DeliveryVersion::initial(),
                )
                .unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap()
    {
        coding_agent_store::CleanupTransitionOutcome::Applied(receipt) => receipt,
        other => panic!("expected unlocked cleanup, got {other:?}"),
    };
    let pending = match store
        .enter_worktree_remove_pending(
            EnterWorktreeRemovePendingRequest::try_new(
                CleanupOperationAnchor::try_new(task.id, receipt.operation_id, unlocked.version)
                    .unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap()
    {
        coding_agent_store::CleanupTransitionOutcome::Applied(receipt) => receipt,
        other => panic!("expected pending cleanup, got {other:?}"),
    };
    (fixture, task, receipt.operation_id, pending.version)
}

pub async fn branch_pending_fixture(
    branch: &str,
) -> (
    crate::support::FileStoreFixture,
    Task,
    DeliveryOperationId,
    DeliveryVersion,
    GitCommitOid,
) {
    let (fixture, task, _) = file_backed_merged_fixture(branch).await;
    let store = fixture.store.clone();
    remove_worktree_fully(&store, &task).await;
    let head: GitCommitOid = "3333333333333333333333333333333333333333".parse().unwrap();
    let request = delete_request(&store, &task, ClientRequestId::new(), head.clone()).await;
    let receipt = match store.accept_branch_cleanup(request).await.unwrap() {
        CleanupAcceptanceOutcome::Accepted(receipt) => receipt,
        other => panic!("expected accepted cleanup, got {other:?}"),
    };
    (
        fixture,
        task,
        receipt.operation_id,
        receipt.accepted_operation_version,
        head,
    )
}
