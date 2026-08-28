use std::str::FromStr;

use coding_agent_domain::{ClientRequestId, Task};
use coding_agent_store::{
    AcceptMergeCommandRequest, AdvanceDeliverySourceObjectRequest, CreateDeliverySourceOutcome,
    CreateDeliverySourceRequest, CreatePreflightOutcome, CreatePreflightRequest,
    DeliverySourceAnchor, DeliverySourceAppliedProof, DeliverySourceObjectProof, DeliveryVersion,
    DirectoryIdentity, GitBranchRef, GitCommitOid, PreflightCommandRequest, Sha256Digest,
    SourceWorktreeProof, Store,
};

use crate::support::delivery::eligibility::{
    ADMIN_IDENTITY, CANDIDATE_TREE, COMMON_IDENTITY, CONFIG_DIGEST, PREFLIGHT_SOURCE,
    SOURCE_COMMIT, TARGET_CONFIG_DIGEST, TARGET_HEAD, TARGET_SECURITY_DIGEST,
    approved_task_on_store, approved_task_with_ready_artifact,
};
use crate::support::delivery::merge::{
    accept_merge_operation_with_request_hash, bind_preflight_inputs, mark_preflight_ready,
};

pub const ACCEPT_RECEIPT_ID: &str = "66666666-6666-4666-8666-666666666666";
pub const SOURCE_BRANCH: &str = "refs/heads/codex/task-source";
pub const TARGET_BRANCH: &str = "refs/heads/main";

pub async fn accepted_fixture() -> (Store, AcceptMergeCommandRequest) {
    let (store, task) = approved_task_with_ready_artifact("codex/task-source").await;
    accepted_fixture_for_task(store, task).await
}

pub async fn file_backed_accepted_fixture()
-> (crate::support::FileStoreFixture, AcceptMergeCommandRequest) {
    let fixture = crate::support::file_store().await;
    fixture.store.migrate().await.unwrap();
    crate::support::register_repository(&fixture.store, "task5-file-store").await;
    let (store, task) = approved_task_on_store(fixture.store.clone(), "codex/task-source", 0).await;
    let (_, command) = accepted_fixture_for_task(store, task).await;
    (fixture, command)
}

async fn accepted_fixture_for_task(store: Store, task: Task) -> (Store, AcceptMergeCommandRequest) {
    let evidence = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .evidence_identity
        .unwrap();
    let preflight_command = PreflightCommandRequest::try_new(
        ClientRequestId::new(),
        task.id,
        GitBranchRef::from_str(TARGET_BRANCH).unwrap(),
        GitCommitOid::from_str(TARGET_HEAD).unwrap(),
    )
    .unwrap();
    let preflight = CreatePreflightRequest::try_new(
        preflight_command,
        DirectoryIdentity::try_new("directory_identity_v1", COMMON_IDENTITY).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", ADMIN_IDENTITY).unwrap(),
        Sha256Digest::from_str(CONFIG_DIGEST).unwrap(),
        Sha256Digest::from_str(TARGET_CONFIG_DIGEST).unwrap(),
        Sha256Digest::from_str(TARGET_SECURITY_DIGEST).unwrap(),
    )
    .unwrap();
    let operation_id = match store.create_merge_preflight(preflight).await.unwrap() {
        CreatePreflightOutcome::Created(receipt) => receipt.operation_id,
        other => panic!("expected created preflight, got {other:?}"),
    };
    bind_preflight_inputs(
        &store,
        task.id,
        operation_id,
        CANDIDATE_TREE,
        PREFLIGHT_SOURCE,
    )
    .await;
    mark_preflight_ready(store.pool(), &operation_id.to_string())
        .await
        .unwrap();
    let command = AcceptMergeCommandRequest::try_new(
        ClientRequestId::from_str(ACCEPT_RECEIPT_ID).unwrap(),
        task.id,
        operation_id,
        DeliveryVersion::try_new(3).unwrap(),
        evidence.workspace_generation(),
        evidence.workspace_fingerprint().clone(),
        GitBranchRef::from_str(TARGET_BRANCH).unwrap(),
        GitCommitOid::from_str(TARGET_HEAD).unwrap(),
    )
    .unwrap();
    accept_merge_operation_with_request_hash(
        store.pool(),
        &operation_id.to_string(),
        ACCEPT_RECEIPT_ID,
        command.canonical_request_hash().as_str(),
    )
    .await
    .unwrap();
    (store, command)
}

pub async fn created_source(
    store: &Store,
    command: AcceptMergeCommandRequest,
) -> coding_agent_store::DeliverySourceRecord {
    match store
        .create_delivery_source(CreateDeliverySourceRequest::try_new(command).unwrap())
        .await
        .unwrap()
    {
        CreateDeliverySourceOutcome::Created(source) => source,
        other => panic!("expected created source, got {other:?}"),
    }
}

pub fn source_anchor(command: &AcceptMergeCommandRequest) -> DeliverySourceAnchor {
    DeliverySourceAnchor::try_new(
        command.task_id(),
        command.preflight_operation_id(),
        DeliveryVersion::try_new(4).unwrap(),
    )
    .unwrap()
}

pub fn object_proof(
    source: &coding_agent_store::DeliverySourceRecord,
) -> DeliverySourceObjectProof {
    DeliverySourceObjectProof::try_new(
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        source.candidate_tree.clone(),
        vec![source.expected_parent.clone()],
        source.commit_metadata.clone(),
    )
    .unwrap()
}

pub async fn commit_pending_fixture(
    store: &Store,
    command: &AcceptMergeCommandRequest,
) -> (
    coding_agent_store::DeliverySourceRecord,
    DeliverySourceAnchor,
    DeliverySourceObjectProof,
    DeliverySourceAppliedProof,
) {
    let source = created_source(store, command.clone()).await;
    let anchor = source_anchor(command);
    let object = object_proof(&source);
    store
        .advance_delivery_source_object(
            AdvanceDeliverySourceObjectRequest::try_new(anchor, source.version, object.clone())
                .unwrap(),
        )
        .await
        .unwrap();
    let current = store
        .delivery_ownership_snapshot(command.task_id())
        .await
        .unwrap()
        .unwrap()
        .source
        .unwrap();
    let worktree = SourceWorktreeProof::try_new(
        current.candidate_tree.clone(),
        current.candidate_tree.clone(),
        0,
        0,
        0,
        0,
    )
    .unwrap();
    let commit_oid = GitCommitOid::from_str(SOURCE_COMMIT).unwrap();
    let applied = DeliverySourceAppliedProof::try_new(
        object.clone(),
        current.provenance.source_branch.clone(),
        commit_oid.clone(),
        commit_oid,
        worktree,
        current.provenance.common_git_identity.clone(),
        current.provenance.worktree_admin_identity.clone(),
        current.provenance.fixed_lock_reason.clone(),
        current.provenance.config_attributes_digest.clone(),
    )
    .unwrap();
    (current, anchor, object, applied)
}

pub async fn source_journal_count(store: &Store, task_id: coding_agent_domain::TaskId) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'delivery_source' AND entity_id = ?",
    )
    .bind(task_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap()
}

pub async fn merge_journal_count(
    store: &Store,
    operation_id: coding_agent_store::DeliveryOperationId,
) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'merge_operation' AND entity_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap()
}
