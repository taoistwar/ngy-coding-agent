use std::str::FromStr;

use coding_agent_domain::ClientRequestId;
use coding_agent_store::{
    AcceptMergeCommandRequest, CommitDeliverySourceRequest, CreateDeliverySourceOutcome,
    CreateDeliverySourceRequest, DeliverySourceState, DeliveryVersion, StoreError,
};

use super::fixtures::{
    SOURCE_BRANCH, accepted_fixture, commit_pending_fixture, source_journal_count,
};
use crate::support::delivery::eligibility::{
    BASE_COMMIT, CANDIDATE_TREE, DELIVERY_TIMESTAMP, SOURCE_COMMIT,
};

#[tokio::test]
async fn accepted_receipt_creates_exact_store_owned_object_pending_source() {
    let (store, command) = accepted_fixture().await;
    let before = store
        .delivery_ownership_snapshot(command.task_id())
        .await
        .unwrap()
        .unwrap();
    let operation = before
        .merge_operations
        .iter()
        .find(|operation| operation.operation_id == command.preflight_operation_id())
        .unwrap()
        .clone();
    let task_id = command.task_id().to_string();
    let outcome = store
        .create_delivery_source(CreateDeliverySourceRequest::try_new(command.clone()).unwrap())
        .await
        .unwrap();
    let source = match outcome {
        CreateDeliverySourceOutcome::Created(source) => source,
        other => panic!("expected a created source, got {other:?}"),
    };
    assert_eq!(source.provenance, operation.provenance);
    assert_eq!(source.provenance.identity.task_id(), command.task_id());
    assert_eq!(
        source.provenance.identity.repository_id(),
        operation.provenance.identity.repository_id()
    );
    assert_eq!(source.provenance.identity.attempt(), 1);
    assert_eq!(
        source.provenance.evidence.algorithm(),
        "evidence_identity_v1"
    );
    assert_eq!(
        source.provenance.evidence.final_review_round(),
        operation.provenance.evidence.final_review_round()
    );
    assert_eq!(
        source.provenance.evidence.final_review_event_id(),
        operation.provenance.evidence.final_review_event_id()
    );
    assert_eq!(
        source.provenance.evidence.workspace_generation(),
        operation.provenance.evidence.workspace_generation()
    );
    assert_eq!(
        source.provenance.evidence.workspace_fingerprint(),
        operation.provenance.evidence.workspace_fingerprint()
    );
    assert_eq!(
        source.provenance.evidence.checks_digest(),
        operation.provenance.evidence.checks_digest()
    );
    assert_eq!(
        source.provenance.evidence.coverage_digest(),
        operation.provenance.evidence.coverage_digest()
    );
    assert_eq!(source.provenance.base_commit.as_str(), BASE_COMMIT);
    assert_eq!(source.provenance.source_branch.as_str(), SOURCE_BRANCH);
    assert_eq!(
        source.provenance.worktree_path,
        operation.provenance.worktree_path
    );
    assert_eq!(
        source.provenance.common_git_identity,
        operation.provenance.common_git_identity
    );
    assert_eq!(
        source.provenance.worktree_admin_identity,
        operation.provenance.worktree_admin_identity
    );
    assert_eq!(
        source.provenance.fixed_lock_reason,
        operation.provenance.fixed_lock_reason
    );
    assert_eq!(
        source.provenance.config_attributes_digest,
        operation.provenance.config_attributes_digest
    );
    assert_eq!(source.candidate_tree.as_str(), CANDIDATE_TREE);
    assert_eq!(
        source.candidate_tree,
        operation.preflight_inputs.unwrap().candidate_tree
    );
    assert_eq!(source.expected_parent.as_str(), BASE_COMMIT);
    assert_eq!(source.expected_source_commit, None);
    assert_eq!(source.commit_metadata.author_name, "Coding Agent");
    assert_eq!(
        source.commit_metadata.author_email,
        "coding-agent@localhost"
    );
    assert_eq!(source.commit_metadata.committer_name, "Coding Agent");
    assert_eq!(
        source.commit_metadata.committer_email,
        "coding-agent@localhost"
    );
    assert_eq!(
        source.commit_metadata.author_date_bytes,
        source.commit_metadata.committer_date_bytes
    );
    assert_eq!(
        source.commit_metadata.author_date_bytes,
        format!(
            "{} +0000",
            source
                .created_at
                .as_utc()
                .as_offset_date_time()
                .unix_timestamp()
        )
    );
    assert_eq!(source.commit_metadata.message_template_version, 1);
    assert_eq!(
        source.commit_metadata.message_bytes,
        format!("coding-agent: deliver task {task_id} attempt 1\n").into_bytes()
    );
    assert_eq!(source.state, DeliverySourceState::ObjectPending);
    assert_eq!(source.failure_code, None);
    assert_eq!(source.version, DeliveryVersion::initial());
    assert_eq!(source.created_at, source.updated_at);
    assert!(source.initial_transition_id > 0);
    assert_eq!(source.initial_transition_id, source.current_transition_id);
}

#[tokio::test]
async fn create_requires_the_exact_accept_receipt_hash_before_source_state() {
    let (store, command) = accepted_fixture().await;
    let missing = accept_command_with(
        &command,
        ClientRequestId::new(),
        command.expected_review_generation(),
    );
    assert!(matches!(
        store
            .create_delivery_source(CreateDeliverySourceRequest::try_new(missing).unwrap())
            .await
            .unwrap(),
        CreateDeliverySourceOutcome::Conflict
    ));

    let same_client_id =
        ClientRequestId::from_str(&command.client_request_id().to_string()).unwrap();
    let wrong_hash = accept_command_with(
        &command,
        same_client_id,
        command.expected_review_generation() + 1,
    );
    assert!(matches!(
        store
            .create_delivery_source(CreateDeliverySourceRequest::try_new(wrong_hash).unwrap())
            .await,
        Err(StoreError::IdempotencyConflict)
    ));
    assert_eq!(source_journal_count(&store, command.task_id()).await, 0);
}

#[tokio::test]
async fn create_replay_is_existing_after_source_commit_and_a_later_legal_merge_state() {
    let (store, command) = accepted_fixture().await;
    let (pending, anchor, _object, applied) = commit_pending_fixture(&store, &command).await;
    store
        .commit_delivery_source(
            CommitDeliverySourceRequest::try_new(anchor, pending.version, applied).unwrap(),
        )
        .await
        .unwrap();
    let replay = CreateDeliverySourceRequest::try_new(command.clone()).unwrap();
    assert!(matches!(
        store.create_delivery_source(replay.clone()).await.unwrap(),
        CreateDeliverySourceOutcome::Existing(ref source)
            if source.state == DeliverySourceState::Committed
    ));
    assert_eq!(source_journal_count(&store, command.task_id()).await, 3);

    sqlx::query(
        "UPDATE task_merge_operations SET delivery_source_task_id = ?, source_commit_oid = ?, \
             state = 'failed', failure_code = 'TARGET_HEAD_CHANGED', version = 5, updated_at = ? \
         WHERE operation_id = ?",
    )
    .bind(command.task_id().to_string())
    .bind(SOURCE_COMMIT)
    .bind(DELIVERY_TIMESTAMP)
    .bind(command.preflight_operation_id().to_string())
    .execute(store.pool())
    .await
    .unwrap();
    assert!(matches!(
        store.create_delivery_source(replay).await.unwrap(),
        CreateDeliverySourceOutcome::Existing(ref source)
            if source.state == DeliverySourceState::Committed
    ));
    assert_eq!(source_journal_count(&store, command.task_id()).await, 3);
}

fn accept_command_with(
    source: &AcceptMergeCommandRequest,
    client_request_id: ClientRequestId,
    expected_review_generation: u64,
) -> AcceptMergeCommandRequest {
    AcceptMergeCommandRequest::try_new(
        client_request_id,
        source.task_id(),
        source.preflight_operation_id(),
        source.expected_operation_version(),
        expected_review_generation,
        source.expected_workspace_fingerprint().clone(),
        source.target_branch().clone(),
        source.expected_target_head().clone(),
    )
    .unwrap()
}
