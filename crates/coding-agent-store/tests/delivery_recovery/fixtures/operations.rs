use std::str::FromStr;

use coding_agent_domain::{ClientRequestId, Task};
use coding_agent_store::{
    AcceptMergeCommandRequest, AcceptMergeOutcome, AdvanceDeliverySourceObjectRequest,
    BeginMergeAbortRequest, CreateDeliverySourceOutcome, CreateDeliverySourceRequest,
    CreatePreflightOutcome, CreatePreflightRequest, DeliveryOperationId, DeliverySourceAnchor,
    DeliverySourceObjectProof, DeliveryVersion, DirectoryIdentity, EnterMergePendingRequest,
    GitBranchRef, GitCommitOid, GitTreeOid, MergeAbortProof, MergeAutostashObservation,
    MergeCommitObjectProof, MergeTransitionOutcome, OtherGitOperationObservation,
    PreflightCommandRequest, Sha256Digest, Store,
};

use crate::support::delivery::eligibility::{
    ADMIN_IDENTITY, CANDIDATE_TREE, CONFIG_DIGEST, MERGE_COMMIT, MERGE_TREE, PREFLIGHT_SOURCE,
    SOURCE_COMMIT, TARGET_HEAD, approved_task_on_store, create_committed_source,
    mark_preflight_ready,
};

const TARGET_BRANCH: &str = "refs/heads/main";
const INDEX_STAGES: &str = "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1";
const WORKTREE: &str = "b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2";

pub async fn pending_preflight(
    store: &Store,
    branch: &str,
    common_identity: &str,
) -> (Task, DeliveryOperationId) {
    let (_, task) = approved_task_on_store(store.clone(), branch, 0).await;
    let operation_id = create_preflight(store, &task, common_identity).await;
    (task, operation_id)
}

pub async fn accepted(
    store: &Store,
    branch: &str,
    common_identity: &str,
) -> (Task, DeliveryOperationId, AcceptMergeCommandRequest) {
    let (task, operation_id) = pending_preflight(store, branch, common_identity).await;
    let command = accept_existing(store, &task, operation_id).await;
    (task, operation_id, command)
}

pub async fn accept_existing(
    store: &Store,
    task: &Task,
    operation_id: DeliveryOperationId,
) -> AcceptMergeCommandRequest {
    mark_preflight_ready(store, operation_id).await;
    let evidence = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .evidence_identity
        .unwrap();
    let command = AcceptMergeCommandRequest::try_new(
        ClientRequestId::new(),
        task.id,
        operation_id,
        DeliveryVersion::try_new(2).unwrap(),
        evidence.workspace_generation(),
        evidence.workspace_fingerprint().clone(),
        GitBranchRef::from_str(TARGET_BRANCH).unwrap(),
        GitCommitOid::from_str(TARGET_HEAD).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        store.accept_merge(command.clone()).await.unwrap(),
        AcceptMergeOutcome::Accepted(_)
    ));
    command
}

pub async fn object_pending_source(
    store: &Store,
    branch: &str,
    common_identity: &str,
) -> (Task, DeliveryOperationId) {
    let (task, operation_id, command) = accepted(store, branch, common_identity).await;
    assert!(matches!(
        store
            .create_delivery_source(CreateDeliverySourceRequest::try_new(command).unwrap())
            .await
            .unwrap(),
        CreateDeliverySourceOutcome::Created(_)
    ));
    (task, operation_id)
}

pub async fn commit_pending_source(
    store: &Store,
    branch: &str,
    common_identity: &str,
) -> (Task, DeliveryOperationId) {
    let (task, operation_id, command) = accepted(store, branch, common_identity).await;
    let source = match store
        .create_delivery_source(CreateDeliverySourceRequest::try_new(command).unwrap())
        .await
        .unwrap()
    {
        CreateDeliverySourceOutcome::Created(source) => source,
        other => panic!("expected created source, got {other:?}"),
    };
    let anchor =
        DeliverySourceAnchor::try_new(task.id, operation_id, source.origin_accepted_version)
            .unwrap();
    let object = DeliverySourceObjectProof::try_new(
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        source.candidate_tree.clone(),
        vec![source.expected_parent.clone()],
        source.commit_metadata.clone(),
    )
    .unwrap();
    store
        .advance_delivery_source_object(
            AdvanceDeliverySourceObjectRequest::try_new(anchor, source.version, object).unwrap(),
        )
        .await
        .unwrap();
    (task, operation_id)
}

pub async fn committed_source(
    store: &Store,
    branch: &str,
    common_identity: &str,
) -> (Task, DeliveryOperationId) {
    let (task, operation_id, _) = accepted(store, branch, common_identity).await;
    create_committed_source(store, &task, operation_id).await;
    (task, operation_id)
}

pub async fn merge_pending(
    store: &Store,
    branch: &str,
    common_identity: &str,
) -> (Task, DeliveryOperationId, DeliveryVersion) {
    let (task, operation_id, accepted_command) = accepted(store, branch, common_identity).await;
    create_committed_source(store, &task, operation_id).await;
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
    let request = EnterMergePendingRequest::try_new(
        task.id,
        operation_id,
        accepted_command
            .expected_operation_version()
            .next()
            .unwrap(),
        proof,
    )
    .unwrap();
    let version = match store.enter_merge_pending(request).await.unwrap() {
        MergeTransitionOutcome::Applied(receipt) => receipt.version,
        other => panic!("expected merge pending, got {other:?}"),
    };
    (task, operation_id, version)
}

pub async fn abort_pending(
    store: &Store,
    branch: &str,
    common_identity: &str,
) -> (Task, DeliveryOperationId) {
    let (task, operation_id, pending_version) = merge_pending(store, branch, common_identity).await;
    let operation = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap();
    let proof = MergeAbortProof::try_new(
        uuid::Uuid::new_v4(),
        operation.target_branch.clone(),
        operation.expected_target_head.clone(),
        operation.provenance.source_branch.clone(),
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        operation.provenance.common_git_identity.clone(),
        operation.provenance.worktree_admin_identity.clone(),
        operation.provenance.fixed_lock_reason.clone(),
        operation.provenance.config_attributes_digest.clone(),
        Sha256Digest::from_str(INDEX_STAGES).unwrap(),
        Sha256Digest::from_str(WORKTREE).unwrap(),
        MergeAutostashObservation::Absent,
        OtherGitOperationObservation::Clear,
    )
    .unwrap();
    let request =
        BeginMergeAbortRequest::try_new(task.id, operation_id, pending_version, proof).unwrap();
    assert!(matches!(
        store.begin_merge_abort(request).await.unwrap(),
        MergeTransitionOutcome::Applied(_)
    ));
    (task, operation_id)
}

async fn create_preflight(
    store: &Store,
    task: &Task,
    common_identity: &str,
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
        GitCommitOid::from_str(PREFLIGHT_SOURCE).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", common_identity).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", ADMIN_IDENTITY).unwrap(),
        Sha256Digest::from_str(CONFIG_DIGEST).unwrap(),
    )
    .unwrap();
    match store.create_merge_preflight(request).await.unwrap() {
        CreatePreflightOutcome::Created(receipt) => receipt.operation_id,
        other => panic!("expected created preflight, got {other:?}"),
    }
}
