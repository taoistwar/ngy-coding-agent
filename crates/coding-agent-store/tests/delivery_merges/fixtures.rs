use std::str::FromStr;

use coding_agent_domain::{ClientRequestId, Task};
use coding_agent_store::{
    AcceptMergeCommandRequest, CompleteMergeRequest, CreatePreflightOutcome,
    CreatePreflightRequest, DeliveryOperationId, DeliveryVersion, DirectoryIdentity, GitBranchRef,
    GitCommitOid, GitTreeOid, MergeAppliedProof, MergeAutostashObservation, MergeCommitObjectProof,
    OtherGitOperationObservation, PreflightCommandRequest, Sha256Digest, Store,
};

use crate::support::delivery::eligibility::{
    ADMIN_IDENTITY, CANDIDATE_TREE, COMMON_IDENTITY, CONFIG_DIGEST, MERGE_COMMIT, MERGE_TREE,
    PREFLIGHT_SOURCE, SOURCE_COMMIT, TARGET_HEAD, approved_task_on_store,
    approved_task_with_ready_artifact,
};

pub const TARGET_BRANCH: &str = "refs/heads/main";

pub async fn pending_preflight() -> (Store, Task, DeliveryOperationId) {
    let (store, task) = approved_task_with_ready_artifact("codex/task-merge-store").await;
    let operation_id = create_pending_preflight(&store, &task).await;
    (store, task, operation_id)
}

pub async fn file_backed_pending_preflight()
-> (crate::support::FileStoreFixture, Task, DeliveryOperationId) {
    let fixture = crate::support::file_store().await;
    fixture.store.migrate().await.unwrap();
    crate::support::register_repository(&fixture.store, "task6-file-store").await;
    let (_, task) =
        approved_task_on_store(fixture.store.clone(), "codex/task-merge-store", 0).await;
    let operation_id = create_pending_preflight(&fixture.store, &task).await;
    (fixture, task, operation_id)
}

pub async fn create_pending_preflight(store: &Store, task: &Task) -> DeliveryOperationId {
    create_pending_preflight_with_source(store, task, PREFLIGHT_SOURCE).await
}

pub async fn create_pending_preflight_with_source(
    store: &Store,
    task: &Task,
    preflight_source: &str,
) -> DeliveryOperationId {
    let command = PreflightCommandRequest::try_new(
        ClientRequestId::new(),
        task.id,
        GitBranchRef::from_str(TARGET_BRANCH).unwrap(),
        GitCommitOid::from_str(TARGET_HEAD).unwrap(),
    )
    .unwrap();
    let request = CreatePreflightRequest::try_new(
        command,
        GitTreeOid::from_str(CANDIDATE_TREE).unwrap(),
        GitCommitOid::from_str(preflight_source).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", COMMON_IDENTITY).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", ADMIN_IDENTITY).unwrap(),
        Sha256Digest::from_str(CONFIG_DIGEST).unwrap(),
    )
    .unwrap();
    match store.create_merge_preflight(request).await.unwrap() {
        CreatePreflightOutcome::Created(receipt) => receipt.operation_id,
        other => panic!("expected created preflight, got {other:?}"),
    }
}

pub async fn accept_command(
    store: &Store,
    task: &Task,
    operation_id: DeliveryOperationId,
    client_request_id: ClientRequestId,
) -> AcceptMergeCommandRequest {
    let evidence = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .evidence_identity
        .unwrap();
    AcceptMergeCommandRequest::try_new(
        client_request_id,
        task.id,
        operation_id,
        DeliveryVersion::try_new(2).unwrap(),
        evidence.workspace_generation(),
        evidence.workspace_fingerprint().clone(),
        GitBranchRef::from_str(TARGET_BRANCH).unwrap(),
        GitCommitOid::from_str(TARGET_HEAD).unwrap(),
    )
    .unwrap()
}

pub async fn accepted_with_committed_source() -> (
    Store,
    Task,
    DeliveryOperationId,
    coding_agent_store::DeliveryVersion,
) {
    let (store, task, operation_id) = pending_preflight().await;
    crate::preflight_results::ready(&store, task.id, operation_id).await;
    let command = accept_command(&store, &task, operation_id, ClientRequestId::new()).await;
    let receipt = match store.accept_merge(command).await.unwrap() {
        coding_agent_store::AcceptMergeOutcome::Accepted(receipt) => receipt,
        other => panic!("expected accepted merge, got {other:?}"),
    };
    crate::support::delivery::eligibility::create_committed_source(&store, &task, operation_id)
        .await;
    (
        store,
        task,
        operation_id,
        receipt.accepted_operation_version,
    )
}

pub async fn merge_pending() -> (
    Store,
    Task,
    DeliveryOperationId,
    coding_agent_store::DeliveryVersion,
) {
    use crate::support::delivery::eligibility::{
        MERGE_COMMIT, MERGE_TREE, SOURCE_COMMIT, TARGET_HEAD,
    };

    let (store, task, operation_id, accepted_version) = accepted_with_committed_source().await;
    let operation = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap();
    let proof = coding_agent_store::MergeCommitObjectProof::try_new(
        GitCommitOid::from_str(MERGE_COMMIT).unwrap(),
        GitTreeOid::from_str(MERGE_TREE).unwrap(),
        vec![
            GitCommitOid::from_str(TARGET_HEAD).unwrap(),
            GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        ],
        operation.merge_metadata.unwrap(),
    )
    .unwrap();
    let request = coding_agent_store::EnterMergePendingRequest::try_new(
        task.id,
        operation_id,
        accepted_version,
        proof,
    )
    .unwrap();
    let receipt = match store.enter_merge_pending(request).await.unwrap() {
        coding_agent_store::MergeTransitionOutcome::Applied(receipt) => receipt,
        other => panic!("expected merge pending, got {other:?}"),
    };
    (store, task, operation_id, receipt.version)
}

pub async fn merge_pending_on_store(
    store: &Store,
    task: &Task,
) -> (DeliveryOperationId, DeliveryVersion) {
    let operation_id = create_pending_preflight(store, task).await;
    crate::preflight_results::ready(store, task.id, operation_id).await;
    let command = accept_command(store, task, operation_id, ClientRequestId::new()).await;
    let accepted_version = match store.accept_merge(command).await.unwrap() {
        coding_agent_store::AcceptMergeOutcome::Accepted(receipt) => {
            receipt.accepted_operation_version
        }
        other => panic!("expected accepted merge, got {other:?}"),
    };
    crate::support::delivery::eligibility::create_committed_source(store, task, operation_id).await;
    let operation = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap();
    let proof = MergeCommitObjectProof::try_new(
        GitCommitOid::from_str(MERGE_COMMIT).unwrap(),
        GitTreeOid::from_str(MERGE_TREE).unwrap(),
        vec![
            GitCommitOid::from_str(TARGET_HEAD).unwrap(),
            GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        ],
        operation.merge_metadata.unwrap(),
    )
    .unwrap();
    let request = coding_agent_store::EnterMergePendingRequest::try_new(
        task.id,
        operation_id,
        accepted_version,
        proof,
    )
    .unwrap();
    let pending_version = match store.enter_merge_pending(request).await.unwrap() {
        coding_agent_store::MergeTransitionOutcome::Applied(receipt) => receipt.version,
        other => panic!("expected merge pending, got {other:?}"),
    };
    (operation_id, pending_version)
}

pub async fn complete_merge_request(
    store: &Store,
    task: &Task,
    operation_id: DeliveryOperationId,
    pending_version: DeliveryVersion,
) -> CompleteMergeRequest {
    let operation = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap();
    let object = MergeCommitObjectProof::try_new(
        GitCommitOid::from_str(MERGE_COMMIT).unwrap(),
        GitTreeOid::from_str(MERGE_TREE).unwrap(),
        vec![
            GitCommitOid::from_str(TARGET_HEAD).unwrap(),
            GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        ],
        operation.merge_metadata.unwrap(),
    )
    .unwrap();
    let proof = MergeAppliedProof::try_new(
        object,
        GitBranchRef::from_str(TARGET_BRANCH).unwrap(),
        GitCommitOid::from_str(MERGE_COMMIT).unwrap(),
        GitBranchRef::from_str("refs/heads/codex/task-merge-store").unwrap(),
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", COMMON_IDENTITY).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", ADMIN_IDENTITY).unwrap(),
        "codex-reserved".to_owned(),
        Sha256Digest::from_str(CONFIG_DIGEST).unwrap(),
        GitTreeOid::from_str(MERGE_TREE).unwrap(),
        GitTreeOid::from_str(MERGE_TREE).unwrap(),
        0,
        0,
        0,
        0,
        None,
        MergeAutostashObservation::Absent,
        OtherGitOperationObservation::Clear,
    )
    .unwrap();
    CompleteMergeRequest::try_new(task.id, operation_id, pending_version, proof).unwrap()
}
