use std::str::FromStr;

use coding_agent_domain::ClientRequestId;
use coding_agent_store::{
    AcceptMergeCommandRequest, AdvanceDeliverySourceObjectRequest, CommitDeliverySourceRequest,
    CreateDeliverySourceOutcome, CreateDeliverySourceRequest, CreatePreflightOutcome,
    CreatePreflightRequest, DeliverySourceAppliedProof, DeliverySourceReconciliationReason,
    DeliverySourceRetryReason, DeliverySourceState, DeliverySourceTransitionOutcome,
    DeliveryVersion, DirectoryIdentity, GitBranchRef, GitCommitOid, GitTreeOid,
    PreflightCommandRequest, ReconcileDeliverySourceOutcome, ReconcileDeliverySourceRequest,
    RecordDeliverySourceRetryRequest, Sha256Digest, SourceWorktreeProof,
};

use super::fixtures::{
    TARGET_BRANCH, accepted_fixture, created_source, object_proof, source_anchor,
    source_journal_count,
};
use crate::support::delivery::eligibility::{
    ADMIN_IDENTITY, CANDIDATE_TREE, COMMON_IDENTITY, CONFIG_DIGEST, DELIVERY_TIMESTAMP,
    SOURCE_COMMIT, TARGET_HEAD,
};
use crate::support::delivery::merge::{
    accept_merge_operation_with_request_hash, mark_preflight_ready,
};

#[tokio::test]
async fn old_operation_replays_survive_a_later_accepted_owner_reconciliation() {
    let (store, first_command) = accepted_fixture().await;
    let source = created_source(&store, first_command.clone()).await;
    let first_anchor = source_anchor(&first_command);

    let retry = RecordDeliverySourceRetryRequest::try_new(
        first_anchor,
        DeliverySourceState::ObjectPending,
        source.version,
        DeliverySourceRetryReason::CommandTimedOut,
    )
    .unwrap();
    assert!(matches!(
        store
            .record_delivery_source_retry(retry.clone())
            .await
            .unwrap(),
        DeliverySourceTransitionOutcome::Applied(_)
    ));
    let object = object_proof(&source);
    let advance = AdvanceDeliverySourceObjectRequest::try_new(
        first_anchor,
        DeliveryVersion::try_new(2).unwrap(),
        object.clone(),
    )
    .unwrap();
    assert!(matches!(
        store
            .advance_delivery_source_object(advance.clone())
            .await
            .unwrap(),
        DeliverySourceTransitionOutcome::Applied(_)
    ));
    let pending = store
        .delivery_ownership_snapshot(first_command.task_id())
        .await
        .unwrap()
        .unwrap()
        .source
        .unwrap();
    let applied = exact_applied_proof(&pending, object);
    let commit = CommitDeliverySourceRequest::try_new(
        first_anchor,
        DeliveryVersion::try_new(3).unwrap(),
        applied,
    )
    .unwrap();
    assert!(matches!(
        store.commit_delivery_source(commit.clone()).await.unwrap(),
        DeliverySourceTransitionOutcome::Applied(_)
    ));
    let first_create = CreateDeliverySourceRequest::try_new(first_command.clone()).unwrap();

    sqlx::query(
        "UPDATE task_merge_operations SET delivery_source_task_id = ?, source_commit_oid = ?, \
             state = 'failed', failure_code = 'TARGET_HEAD_CHANGED', version = 4, updated_at = ? \
         WHERE operation_id = ?",
    )
    .bind(first_command.task_id().to_string())
    .bind(SOURCE_COMMIT)
    .bind(DELIVERY_TIMESTAMP)
    .bind(first_command.preflight_operation_id().to_string())
    .execute(store.pool())
    .await
    .unwrap();

    let second_command = accept_another_operation(&store, &first_command).await;
    assert!(matches!(
        store
            .create_delivery_source(
                CreateDeliverySourceRequest::try_new(second_command.clone()).unwrap(),
            )
            .await
            .unwrap(),
        CreateDeliverySourceOutcome::Existing(ref current)
            if current.state == DeliverySourceState::Committed
    ));
    let second_anchor = source_anchor(&second_command);
    let reconcile = ReconcileDeliverySourceRequest::try_new(
        second_anchor,
        DeliverySourceState::Committed,
        DeliveryVersion::try_new(4).unwrap(),
        DeliveryVersion::try_new(3).unwrap(),
        DeliverySourceReconciliationReason::SourceInconsistent,
    )
    .unwrap();
    assert!(matches!(
        store.reconcile_delivery_source(reconcile).await.unwrap(),
        ReconcileDeliverySourceOutcome::Applied(_)
    ));
    assert_eq!(
        source_journal_count(&store, first_command.task_id()).await,
        5
    );

    let stale_owner_reconcile = ReconcileDeliverySourceRequest::try_new(
        first_anchor,
        DeliverySourceState::Committed,
        DeliveryVersion::try_new(5).unwrap(),
        DeliveryVersion::try_new(4).unwrap(),
        DeliverySourceReconciliationReason::SourceInconsistent,
    )
    .unwrap();
    assert!(matches!(
        store
            .reconcile_delivery_source(stale_owner_reconcile)
            .await
            .unwrap(),
        ReconcileDeliverySourceOutcome::Conflict
    ));

    assert!(matches!(
        store.create_delivery_source(first_create).await.unwrap(),
        CreateDeliverySourceOutcome::Existing(_)
    ));
    assert!(matches!(
        store.record_delivery_source_retry(retry).await.unwrap(),
        DeliverySourceTransitionOutcome::Existing(_)
    ));
    assert!(matches!(
        store.advance_delivery_source_object(advance).await.unwrap(),
        DeliverySourceTransitionOutcome::Existing(_)
    ));
    assert!(matches!(
        store.commit_delivery_source(commit).await.unwrap(),
        DeliverySourceTransitionOutcome::Existing(_)
    ));
    assert_eq!(
        source_journal_count(&store, first_command.task_id()).await,
        5
    );
}

async fn accept_another_operation(
    store: &coding_agent_store::Store,
    first: &AcceptMergeCommandRequest,
) -> AcceptMergeCommandRequest {
    let evidence = store
        .delivery_eligibility_snapshot(first.task_id())
        .await
        .unwrap()
        .unwrap()
        .evidence_identity
        .unwrap();
    let preflight_command = PreflightCommandRequest::try_new(
        ClientRequestId::new(),
        first.task_id(),
        GitBranchRef::from_str(TARGET_BRANCH).unwrap(),
        GitCommitOid::from_str(TARGET_HEAD).unwrap(),
    )
    .unwrap();
    let preflight = CreatePreflightRequest::try_new(
        preflight_command,
        GitTreeOid::from_str(CANDIDATE_TREE).unwrap(),
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", COMMON_IDENTITY).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", ADMIN_IDENTITY).unwrap(),
        Sha256Digest::from_str(CONFIG_DIGEST).unwrap(),
    )
    .unwrap();
    let operation_id = match store.create_merge_preflight(preflight).await.unwrap() {
        CreatePreflightOutcome::Created(receipt) => receipt.operation_id,
        other => panic!("expected second preflight, got {other:?}"),
    };
    mark_preflight_ready(store.pool(), &operation_id.to_string())
        .await
        .unwrap();
    let command = AcceptMergeCommandRequest::try_new(
        ClientRequestId::new(),
        first.task_id(),
        operation_id,
        DeliveryVersion::try_new(2).unwrap(),
        evidence.workspace_generation(),
        evidence.workspace_fingerprint().clone(),
        GitBranchRef::from_str(TARGET_BRANCH).unwrap(),
        GitCommitOid::from_str(TARGET_HEAD).unwrap(),
    )
    .unwrap();
    accept_merge_operation_with_request_hash(
        store.pool(),
        &operation_id.to_string(),
        &command.client_request_id().to_string(),
        command.canonical_request_hash().as_str(),
    )
    .await
    .unwrap();
    command
}

fn exact_applied_proof(
    source: &coding_agent_store::DeliverySourceRecord,
    object: coding_agent_store::DeliverySourceObjectProof,
) -> DeliverySourceAppliedProof {
    let worktree = SourceWorktreeProof::try_new(
        source.candidate_tree.clone(),
        source.candidate_tree.clone(),
        0,
        0,
        0,
        0,
    )
    .unwrap();
    let oid = GitCommitOid::from_str(SOURCE_COMMIT).unwrap();
    DeliverySourceAppliedProof::try_new(
        object,
        source.provenance.source_branch.clone(),
        oid.clone(),
        oid,
        worktree,
        source.provenance.common_git_identity.clone(),
        source.provenance.worktree_admin_identity.clone(),
        source.provenance.fixed_lock_reason.clone(),
        source.provenance.config_attributes_digest.clone(),
    )
    .unwrap()
}
