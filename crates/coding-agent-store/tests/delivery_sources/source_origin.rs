use std::fmt::Debug;
use std::str::FromStr;

use coding_agent_domain::ClientRequestId;
use coding_agent_store::{
    AcceptMergeCommandRequest, AdvanceDeliverySourceObjectRequest, CommitDeliverySourceRequest,
    CreateDeliverySourceRequest, CreatePreflightOutcome, CreatePreflightRequest,
    DeliveryOperationId, DeliverySourceAnchor, DeliverySourceReconciliationReason,
    DeliverySourceRetryReason, DeliverySourceState, DeliveryVersion, DirectoryIdentity,
    GitBranchRef, GitCommitOid, PreflightCommandRequest, ReconcileDeliverySourceRequest,
    RecordDeliverySourceRetryRequest, Sha256Digest, Store, StoreError,
};

use super::fixtures::{
    TARGET_BRANCH, accepted_fixture, commit_pending_fixture, created_source, object_proof,
    source_anchor,
};
use crate::support::delivery::eligibility::{
    ADMIN_IDENTITY, CANDIDATE_TREE, COMMON_IDENTITY, CONFIG_DIGEST, DELIVERY_TIMESTAMP,
    SOURCE_COMMIT, TARGET_CONFIG_DIGEST, TARGET_HEAD, TARGET_SECURITY_DIGEST,
};
use crate::support::delivery::merge::{
    accept_merge_operation_with_request_hash, mark_preflight_ready,
};

const WRONG_REQUEST_HASH: &str = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

#[tokio::test]
async fn orphaned_object_pending_source_owner_blocks_every_source_entrypoint_without_writes() {
    let (store, command) = accepted_fixture().await;
    let source = created_source(&store, command.clone()).await;
    assert_eq!(
        source.origin_accepted_operation_id,
        command.preflight_operation_id()
    );
    assert_eq!(
        source.origin_accept_receipt_id.to_string(),
        command.client_request_id().to_string()
    );
    assert_eq!(
        source.origin_accepted_version,
        DeliveryVersion::try_new(4).unwrap()
    );
    let anchor = source_anchor(&command);
    let create = CreateDeliverySourceRequest::try_new(command.clone()).unwrap();
    let advance =
        AdvanceDeliverySourceObjectRequest::try_new(anchor, source.version, object_proof(&source))
            .unwrap();
    let retry = RecordDeliverySourceRetryRequest::try_new(
        anchor,
        DeliverySourceState::ObjectPending,
        source.version,
        DeliverySourceRetryReason::CommandTimedOut,
    )
    .unwrap();
    let reconcile = ReconcileDeliverySourceRequest::try_new(
        anchor,
        DeliverySourceState::ObjectPending,
        source.version,
        DeliveryVersion::try_new(4).unwrap(),
        DeliverySourceReconciliationReason::SourceInconsistent,
    )
    .unwrap();
    let before = source_storage_snapshot(&store, command.task_id()).await;

    delete_accepted_owner(&store, &command).await;

    assert_ownership_invariant(store.delivery_ownership_snapshot(command.task_id()).await);
    assert_source_invariant(store.create_delivery_source(create).await);
    assert_source_invariant(store.advance_delivery_source_object(advance).await);
    assert_source_invariant(store.record_delivery_source_retry(retry).await);
    assert_source_invariant(store.reconcile_delivery_source(reconcile).await);
    let wrong_owner = DeliverySourceAnchor::try_new(
        command.task_id(),
        DeliveryOperationId::new(),
        DeliveryVersion::try_new(3).unwrap(),
    )
    .unwrap();
    let wrong_owner_advance = AdvanceDeliverySourceObjectRequest::try_new(
        wrong_owner,
        source.version,
        object_proof(&source),
    )
    .unwrap();
    assert_source_invariant(
        store
            .advance_delivery_source_object(wrong_owner_advance)
            .await,
    );
    assert_eq!(
        source_storage_snapshot(&store, command.task_id()).await,
        before
    );
}

#[tokio::test]
async fn orphaned_commit_pending_source_owner_blocks_commit_without_writes() {
    let (store, command) = accepted_fixture().await;
    let (source, anchor, _object, applied) = commit_pending_fixture(&store, &command).await;
    let commit = CommitDeliverySourceRequest::try_new(anchor, source.version, applied).unwrap();
    let before = source_storage_snapshot(&store, command.task_id()).await;

    delete_accepted_owner(&store, &command).await;

    assert_source_invariant(store.commit_delivery_source(commit).await);
    assert_eq!(
        source_storage_snapshot(&store, command.task_id()).await,
        before
    );
}

#[tokio::test]
async fn damaged_accepted_receipt_invalidates_source_origin_without_writes() {
    let (store, command) = accepted_fixture().await;
    let source = created_source(&store, command.clone()).await;
    let advance = AdvanceDeliverySourceObjectRequest::try_new(
        source_anchor(&command),
        source.version,
        object_proof(&source),
    )
    .unwrap();
    let before = source_storage_snapshot(&store, command.task_id()).await;
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::raw_sql("DROP TRIGGER task_delivery_command_receipts_no_update;")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE task_delivery_command_receipts SET canonical_request_hash = ? \
         WHERE client_request_id = ?",
    )
    .bind(WRONG_REQUEST_HASH)
    .bind(command.client_request_id().to_string())
    .execute(&mut *connection)
    .await
    .unwrap();
    drop(connection);

    assert_ownership_invariant(store.delivery_ownership_snapshot(command.task_id()).await);
    assert_source_invariant(store.advance_delivery_source_object(advance).await);
    assert_eq!(
        source_storage_snapshot(&store, command.task_id()).await,
        before
    );
}

#[tokio::test]
async fn damaged_accepted_transition_invalidates_source_origin_without_writes() {
    let (store, command) = accepted_fixture().await;
    let source = created_source(&store, command.clone()).await;
    let advance = AdvanceDeliverySourceObjectRequest::try_new(
        source_anchor(&command),
        source.version,
        object_proof(&source),
    )
    .unwrap();
    let before = source_storage_snapshot(&store, command.task_id()).await;
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::raw_sql("DROP TRIGGER task_delivery_operation_transitions_no_update;")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE task_delivery_operation_transitions \
         SET transitioned_at = '2026-08-04T00:00:01.000000000Z' \
         WHERE entity_kind = 'merge_operation' AND entity_id = ? AND entity_version = 4",
    )
    .bind(command.preflight_operation_id().to_string())
    .execute(&mut *connection)
    .await
    .unwrap();
    drop(connection);

    assert_ownership_invariant(store.delivery_ownership_snapshot(command.task_id()).await);
    assert_source_invariant(store.advance_delivery_source_object(advance).await);
    assert_eq!(
        source_storage_snapshot(&store, command.task_id()).await,
        before
    );
}

#[tokio::test]
async fn hidden_old_operation_corruption_precedes_a_wrong_caller_conflict() {
    let (store, origin) = accepted_fixture().await;
    let (pending, anchor, _object, applied) = commit_pending_fixture(&store, &origin).await;
    let committed = CommitDeliverySourceRequest::try_new(anchor, pending.version, applied).unwrap();
    store.commit_delivery_source(committed).await.unwrap();
    fail_owner(&store, &origin).await;

    let hidden = accept_another(&store, &origin).await;
    fail_owner(&store, &hidden).await;
    let latest_terminal = accept_another(&store, &origin).await;
    fail_owner(&store, &latest_terminal).await;
    let _active = accept_another(&store, &origin).await;
    let current = store
        .delivery_ownership_snapshot(origin.task_id())
        .await
        .unwrap()
        .unwrap()
        .source
        .unwrap();
    let before = source_storage_snapshot(&store, origin.task_id()).await;

    corrupt_operation_provenance(&store, hidden.preflight_operation_id()).await;

    assert_ownership_invariant(store.delivery_ownership_snapshot(origin.task_id()).await);
    let wrong_caller = DeliverySourceAnchor::try_new(
        origin.task_id(),
        DeliveryOperationId::new(),
        DeliveryVersion::try_new(3).unwrap(),
    )
    .unwrap();
    let request = AdvanceDeliverySourceObjectRequest::try_new(
        wrong_caller,
        current.version,
        object_proof(&current),
    )
    .unwrap();
    assert_source_invariant(store.advance_delivery_source_object(request).await);
    assert_eq!(
        source_storage_snapshot(&store, origin.task_id()).await,
        before
    );
}

async fn accept_another(
    store: &Store,
    first: &AcceptMergeCommandRequest,
) -> AcceptMergeCommandRequest {
    let evidence = store
        .delivery_eligibility_snapshot(first.task_id())
        .await
        .unwrap()
        .unwrap()
        .evidence_identity
        .unwrap();
    let preflight = CreatePreflightRequest::try_new(
        PreflightCommandRequest::try_new(
            ClientRequestId::new(),
            first.task_id(),
            GitBranchRef::from_str(TARGET_BRANCH).unwrap(),
            GitCommitOid::from_str(TARGET_HEAD).unwrap(),
        )
        .unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", COMMON_IDENTITY).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", ADMIN_IDENTITY).unwrap(),
        Sha256Digest::from_str(CONFIG_DIGEST).unwrap(),
        Sha256Digest::from_str(TARGET_CONFIG_DIGEST).unwrap(),
        Sha256Digest::from_str(TARGET_SECURITY_DIGEST).unwrap(),
    )
    .unwrap();
    let operation_id = match store.create_merge_preflight(preflight).await.unwrap() {
        CreatePreflightOutcome::Created(receipt) => receipt.operation_id,
        other => panic!("expected a new preflight, got {other:?}"),
    };
    crate::support::delivery::merge::bind_preflight_inputs(
        store,
        first.task_id(),
        operation_id,
        CANDIDATE_TREE,
        SOURCE_COMMIT,
    )
    .await;
    mark_preflight_ready(store.pool(), &operation_id.to_string())
        .await
        .unwrap();
    let command = AcceptMergeCommandRequest::try_new(
        ClientRequestId::new(),
        first.task_id(),
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
        &command.client_request_id().to_string(),
        command.canonical_request_hash().as_str(),
    )
    .await
    .unwrap();
    command
}

async fn fail_owner(store: &Store, command: &AcceptMergeCommandRequest) {
    let updated = sqlx::query(
        "UPDATE task_merge_operations SET delivery_source_task_id = ?, source_commit_oid = ?, \
             state = 'failed', failure_code = 'TARGET_HEAD_CHANGED', version = 5, updated_at = ? \
         WHERE operation_id = ? AND state = 'accepted' AND version = 4",
    )
    .bind(command.task_id().to_string())
    .bind(SOURCE_COMMIT)
    .bind(DELIVERY_TIMESTAMP)
    .bind(command.preflight_operation_id().to_string())
    .execute(store.pool())
    .await
    .unwrap();
    assert_eq!(updated.rows_affected(), 1);
}

async fn corrupt_operation_provenance(store: &Store, operation_id: DeliveryOperationId) {
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::raw_sql(
        "DROP TRIGGER task_merge_operations_immutable_on_update; \
         DROP TRIGGER task_merge_operations_transition_on_update; \
         DROP TRIGGER task_merge_operations_source_consistency_on_update; \
         DROP TRIGGER task_merge_operations_source_reconciliation_on_update; \
         DROP TRIGGER task_merge_operations_journal_on_update;",
    )
    .execute(&mut *connection)
    .await
    .unwrap();
    sqlx::query("UPDATE task_merge_operations SET checks_digest = ? WHERE operation_id = ?")
        .bind(WRONG_REQUEST_HASH)
        .bind(operation_id.to_string())
        .execute(&mut *connection)
        .await
        .unwrap();
}

async fn delete_accepted_owner(
    store: &Store,
    command: &coding_agent_store::AcceptMergeCommandRequest,
) {
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::raw_sql("DROP TRIGGER task_merge_operations_no_delete;")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::raw_sql("DROP TRIGGER task_delivery_command_receipts_no_delete;")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::raw_sql("DROP TRIGGER task_delivery_operation_transitions_no_delete;")
        .execute(&mut *connection)
        .await
        .unwrap();
    let deleted_receipt =
        sqlx::query("DELETE FROM task_delivery_command_receipts WHERE client_request_id = ?")
            .bind(command.client_request_id().to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
    assert_eq!(deleted_receipt.rows_affected(), 1);
    let deleted_transitions = sqlx::query(
        "DELETE FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'merge_operation' AND entity_id = ?",
    )
    .bind(command.preflight_operation_id().to_string())
    .execute(&mut *connection)
    .await
    .unwrap();
    assert_eq!(deleted_transitions.rows_affected(), 4);
    let deleted = sqlx::query("DELETE FROM task_merge_operations WHERE operation_id = ?")
        .bind(command.preflight_operation_id().to_string())
        .execute(&mut *connection)
        .await
        .unwrap();
    assert_eq!(deleted.rows_affected(), 1);
}

async fn source_storage_snapshot(
    store: &Store,
    task_id: coding_agent_domain::TaskId,
) -> (String, i64, i64) {
    sqlx::query_as(
        "SELECT s.state, s.version, ( \
             SELECT COUNT(*) FROM task_delivery_operation_transitions t \
             WHERE t.entity_kind = 'delivery_source' AND t.entity_id = s.task_id \
         ) FROM task_delivery_sources s WHERE s.task_id = ?",
    )
    .bind(task_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap()
}

fn assert_source_invariant<T: Debug>(result: Result<T, StoreError>) {
    match result {
        Err(StoreError::InvariantViolation(_)) => {}
        other => panic!("damaged source origin was not a source invariant: {other:?}"),
    }
}

fn assert_ownership_invariant<T: Debug>(result: Result<T, StoreError>) {
    match result {
        Err(StoreError::InvariantViolation(message)) => {
            assert_eq!(message, "delivery ownership snapshot is inconsistent")
        }
        other => panic!("damaged source origin passed ownership audit: {other:?}"),
    }
}
