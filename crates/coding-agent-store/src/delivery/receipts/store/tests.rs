use std::str::FromStr;

use coding_agent_domain::{ClientRequestId, RepositoryId, TaskId};

use super::{ReceiptWrite, insert_receipt};
use crate::delivery::receipts::model::CanonicalCommandRequest;
use crate::delivery::{
    AcceptMergeCommandRequest, DeleteBranchCommandRequest, DeliveryAcceptedOperationState,
    DeliveryIdentity, DeliveryOperationId, DeliveryTimestamp, DeliveryVersion, GitBranchRef,
    GitCommitOid, RemoveWorktreeCommandRequest, Sha256Digest,
};
use crate::{Store, StoreError};

const TASK_ID: &str = "22222222-2222-4222-8222-222222222222";
const REPOSITORY_ID: &str = "11111111-1111-4111-8111-111111111111";
const MERGE_OPERATION_ID: &str = "44444444-4444-4444-8444-444444444444";
const SOURCE_REF: &str = "refs/heads/codex/task";
const SOURCE_OID: &str = "1111111111111111111111111111111111111111";
const TARGET_HEAD: &str = "dddddddddddddddddddddddddddddddddddddddd";
const FINGERPRINT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const TIMESTAMP: &str = "2026-08-04T00:00:00.000000000Z";

#[tokio::test]
async fn accept_remove_and_delete_each_allow_only_one_receipt_for_an_operation() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(temporary.path().join("receipt-helper-race.sqlite"))
        .await
        .unwrap();
    create_receipt_helper_schema(&store).await;
    let identity = identity();
    let merge_operation_id = DeliveryOperationId::from_str(MERGE_OPERATION_ID).unwrap();
    let accept = |client_request_id| {
        AcceptMergeCommandRequest::try_new(
            client_request_id,
            identity.task_id(),
            merge_operation_id,
            DeliveryVersion::try_new(2).unwrap(),
            7,
            Sha256Digest::from_str(FINGERPRINT).unwrap(),
            GitBranchRef::from_str("refs/heads/main").unwrap(),
            GitCommitOid::from_str(TARGET_HEAD).unwrap(),
        )
        .unwrap()
    };
    assert_receipt_race(
        &store,
        accept(ClientRequestId::new()),
        accept(ClientRequestId::new()),
        identity,
        merge_operation_id,
        DeliveryVersion::try_new(3).unwrap(),
        DeliveryAcceptedOperationState::Accepted,
        "accept_merge",
    )
    .await;

    let remove_operation_id = DeliveryOperationId::new();
    seed_cleanup_anchor(&store, identity, remove_operation_id, merge_operation_id).await;
    let remove = |client_request_id| {
        RemoveWorktreeCommandRequest::try_new(
            client_request_id,
            identity.task_id(),
            DeliveryVersion::initial(),
            merge_operation_id,
            GitBranchRef::from_str(SOURCE_REF).unwrap(),
            GitCommitOid::from_str(SOURCE_OID).unwrap(),
        )
        .unwrap()
    };
    assert_receipt_race(
        &store,
        remove(ClientRequestId::new()),
        remove(ClientRequestId::new()),
        identity,
        remove_operation_id,
        DeliveryVersion::initial(),
        DeliveryAcceptedOperationState::UnlockPending,
        "remove_worktree",
    )
    .await;

    let delete_operation_id = DeliveryOperationId::new();
    seed_cleanup_anchor(&store, identity, delete_operation_id, merge_operation_id).await;
    let delete = |client_request_id| {
        DeleteBranchCommandRequest::try_new(
            client_request_id,
            identity.task_id(),
            DeliveryVersion::initial(),
            merge_operation_id,
            GitBranchRef::from_str(SOURCE_REF).unwrap(),
            GitCommitOid::from_str(SOURCE_OID).unwrap(),
            GitBranchRef::from_str("refs/heads/main").unwrap(),
            GitCommitOid::from_str(TARGET_HEAD).unwrap(),
        )
        .unwrap()
    };
    assert_receipt_race(
        &store,
        delete(ClientRequestId::new()),
        delete(ClientRequestId::new()),
        identity,
        delete_operation_id,
        DeliveryVersion::initial(),
        DeliveryAcceptedOperationState::DeletePending,
        "delete_branch",
    )
    .await;
    store.close().await;
}

#[tokio::test]
async fn cleanup_receipt_write_rejects_a_request_bound_to_another_merged_operation() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(temporary.path().join("cleanup-anchor.sqlite"))
        .await
        .unwrap();
    create_receipt_helper_schema(&store).await;
    let identity = identity();
    let durable_merge = DeliveryOperationId::from_str(MERGE_OPERATION_ID).unwrap();
    let requested_merge = DeliveryOperationId::new();
    let cleanup_operation = DeliveryOperationId::new();
    seed_cleanup_anchor(&store, identity, cleanup_operation, durable_merge).await;
    let request = RemoveWorktreeCommandRequest::try_new(
        ClientRequestId::new(),
        identity.task_id(),
        DeliveryVersion::initial(),
        requested_merge,
        GitBranchRef::from_str(SOURCE_REF).unwrap(),
        GitCommitOid::from_str(SOURCE_OID).unwrap(),
    )
    .unwrap();
    let mut transaction = store.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let write = ReceiptWrite::try_new(
        &request,
        identity,
        cleanup_operation,
        DeliveryVersion::initial(),
        DeliveryAcceptedOperationState::UnlockPending,
        DeliveryTimestamp::from_str(TIMESTAMP).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        insert_receipt(&mut transaction, &write).await,
        Err(StoreError::InvariantViolation(_))
    ));
    transaction.rollback().await.unwrap();
    store.close().await;
}

#[test]
fn receipt_write_rejects_wrong_task_version_and_accept_operation_anchor() {
    let identity = identity();
    let operation_id = DeliveryOperationId::from_str(MERGE_OPERATION_ID).unwrap();
    let request = AcceptMergeCommandRequest::try_new(
        ClientRequestId::new(),
        identity.task_id(),
        operation_id,
        DeliveryVersion::try_new(2).unwrap(),
        7,
        Sha256Digest::from_str(FINGERPRINT).unwrap(),
        GitBranchRef::from_str("refs/heads/main").unwrap(),
        GitCommitOid::from_str(TARGET_HEAD).unwrap(),
    )
    .unwrap();
    let timestamp = DeliveryTimestamp::from_str(TIMESTAMP).unwrap();
    assert!(matches!(
        ReceiptWrite::try_new(
            &request,
            identity,
            DeliveryOperationId::new(),
            DeliveryVersion::try_new(3).unwrap(),
            DeliveryAcceptedOperationState::Accepted,
            timestamp,
        ),
        Err(StoreError::InvariantViolation(_))
    ));
    let wrong_identity =
        DeliveryIdentity::try_new(TaskId::new(), identity.repository_id(), identity.attempt())
            .unwrap();
    assert!(matches!(
        ReceiptWrite::try_new(
            &request,
            wrong_identity,
            operation_id,
            DeliveryVersion::try_new(3).unwrap(),
            DeliveryAcceptedOperationState::Accepted,
            timestamp,
        ),
        Err(StoreError::InvariantViolation(_))
    ));
    assert!(matches!(
        ReceiptWrite::try_new(
            &request,
            identity,
            operation_id,
            DeliveryVersion::try_new(4).unwrap(),
            DeliveryAcceptedOperationState::Accepted,
            timestamp,
        ),
        Err(StoreError::InvariantViolation(_))
    ));
}

#[allow(clippy::too_many_arguments)]
async fn assert_receipt_race<R>(
    store: &Store,
    first: R,
    second: R,
    identity: DeliveryIdentity,
    operation_id: DeliveryOperationId,
    version: DeliveryVersion,
    state: DeliveryAcceptedOperationState,
    command_kind: &str,
) where
    R: CanonicalCommandRequest,
{
    let first_store = store.clone();
    let second_store = store.clone();
    let (first_result, second_result) = tokio::join!(
        insert_one(first_store, first, identity, operation_id, version, state,),
        insert_one(second_store, second, identity, operation_id, version, state,)
    );
    let results = [first_result, second_result];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(StoreError::IdempotencyConflict)))
            .count(),
        1
    );
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_delivery_command_receipts \
         WHERE command_kind = ? AND operation_id = ?",
    )
    .bind(command_kind)
    .bind(operation_id.to_string())
    .fetch_one(&store.pool)
    .await
    .unwrap();
    assert_eq!(count, 1);
}

async fn insert_one<R: CanonicalCommandRequest>(
    store: Store,
    request: R,
    identity: DeliveryIdentity,
    operation_id: DeliveryOperationId,
    version: DeliveryVersion,
    state: DeliveryAcceptedOperationState,
) -> Result<(), StoreError> {
    let mut transaction = store.pool.begin_with("BEGIN IMMEDIATE").await?;
    let write = ReceiptWrite::try_new(
        &request,
        identity,
        operation_id,
        version,
        state,
        DeliveryTimestamp::from_str(TIMESTAMP)?,
    )?;
    match insert_receipt(&mut transaction, &write).await {
        Ok(()) => transaction.commit().await.map_err(StoreError::from),
        Err(error) => {
            transaction.rollback().await?;
            Err(error)
        }
    }
}

async fn create_receipt_helper_schema(store: &Store) {
    sqlx::raw_sql(
        "CREATE TABLE task_delivery_command_receipts ( \
             client_request_id TEXT PRIMARY KEY, command_kind TEXT NOT NULL, task_id TEXT NOT NULL, \
             repository_id TEXT NOT NULL, attempt INTEGER NOT NULL, request_hash_domain TEXT NOT NULL, \
             request_hash_version INTEGER NOT NULL, request_hash_algorithm TEXT NOT NULL, \
             canonical_request_hash TEXT NOT NULL, operation_kind TEXT NOT NULL, operation_id TEXT NOT NULL, \
             merge_operation_id TEXT, cleanup_operation_id TEXT, accepted_operation_version INTEGER NOT NULL, \
             accepted_operation_state TEXT NOT NULL, response_discriminator TEXT NOT NULL, created_at TEXT NOT NULL, \
             UNIQUE(command_kind, operation_id) \
         ); \
         CREATE TABLE task_cleanup_operations ( \
             operation_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, disposition_task_id TEXT NOT NULL \
         ); \
         CREATE TABLE task_artifact_dispositions ( \
             task_id TEXT PRIMARY KEY, merged_operation_id TEXT NOT NULL \
         );",
    )
    .execute(&store.pool)
    .await
    .unwrap();
}

async fn seed_cleanup_anchor(
    store: &Store,
    identity: DeliveryIdentity,
    cleanup_operation_id: DeliveryOperationId,
    merge_operation_id: DeliveryOperationId,
) {
    sqlx::query(
        "INSERT OR IGNORE INTO task_artifact_dispositions (task_id, merged_operation_id) \
         VALUES (?, ?)",
    )
    .bind(identity.task_id().to_string())
    .bind(merge_operation_id.to_string())
    .execute(&store.pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO task_cleanup_operations (operation_id, task_id, disposition_task_id) \
         VALUES (?, ?, ?)",
    )
    .bind(cleanup_operation_id.to_string())
    .bind(identity.task_id().to_string())
    .bind(identity.task_id().to_string())
    .execute(&store.pool)
    .await
    .unwrap();
}

fn identity() -> DeliveryIdentity {
    DeliveryIdentity::try_new(
        TaskId::from_str(TASK_ID).unwrap(),
        RepositoryId::from_str(REPOSITORY_ID).unwrap(),
        1,
    )
    .unwrap()
}
