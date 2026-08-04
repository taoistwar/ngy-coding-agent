use std::str::FromStr;

use coding_agent_domain::ClientRequestId;
use coding_agent_store::{
    AcceptMergeCommandRequest, AdvanceDeliverySourceObjectRequest, CommitDeliverySourceRequest,
    CreatePreflightOutcome, CreatePreflightRequest, DeliveryOperationId,
    DeliverySourceReconciliationReason, DeliverySourceRetryReason, DeliverySourceState,
    DeliverySourceTransitionOutcome, DirectoryIdentity, GitBranchRef, GitCommitOid, GitTreeOid,
    PreflightCommandRequest, ReconcileDeliverySourceOutcome, ReconcileDeliverySourceRequest,
    RecordDeliverySourceRetryRequest, Sha256Digest, Store, StoreError,
};

use super::fixtures::{
    TARGET_BRANCH, accepted_fixture, commit_pending_fixture, created_source, object_proof,
    source_anchor,
};
use crate::support::delivery::eligibility::{
    ADMIN_IDENTITY, CANDIDATE_TREE, COMMON_IDENTITY, CONFIG_DIGEST, DELIVERY_TIMESTAMP,
    SOURCE_COMMIT, TARGET_HEAD,
};
use crate::support::delivery::merge::{
    accept_merge_operation_with_request_hash, mark_preflight_ready,
};

const WRONG_SOURCE_COMMIT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WRONG_REQUEST_HASH: &str = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

#[tokio::test]
async fn paired_source_reconciliation_rejects_an_unaccepted_preflight_owner() {
    let (store, first_command) = accepted_fixture().await;
    let (pending, anchor, _object, applied) = commit_pending_fixture(&store, &first_command).await;
    let replay = CommitDeliverySourceRequest::try_new(anchor, pending.version, applied).unwrap();
    assert!(matches!(
        store.commit_delivery_source(replay.clone()).await.unwrap(),
        DeliverySourceTransitionOutcome::Applied(_)
    ));

    fail_original_owner(&store, &first_command).await;
    let unaccepted_operation = create_unaccepted_preflight_ready(&store, &first_command).await;
    poison_reconciliation_pair(&store, first_command.task_id(), unaccepted_operation).await;

    match store.commit_delivery_source(replay).await {
        Err(StoreError::InvariantViolation(message)) => {
            assert_eq!(message, "delivery source transaction is inconsistent")
        }
        other => panic!("unaccepted reconciliation owner passed source replay: {other:?}"),
    }
    match store
        .delivery_ownership_snapshot(first_command.task_id())
        .await
    {
        Err(StoreError::InvariantViolation(message)) => {
            assert_eq!(message, "delivery ownership snapshot is inconsistent")
        }
        other => panic!("unaccepted reconciliation owner passed ownership audit: {other:?}"),
    }
}

#[tokio::test]
async fn paired_source_reconciliation_revalidates_accept_receipt_hash_and_tuple() {
    let (store, first_command) = accepted_fixture().await;
    let (pending, anchor, _object, applied) = commit_pending_fixture(&store, &first_command).await;
    let replay = CommitDeliverySourceRequest::try_new(anchor, pending.version, applied).unwrap();
    store.commit_delivery_source(replay.clone()).await.unwrap();
    fail_original_owner(&store, &first_command).await;

    let operation_id = create_unaccepted_preflight_ready(&store, &first_command).await;
    let current_owner = accept_preflight_ready(&store, &first_command, operation_id).await;
    let reconcile = ReconcileDeliverySourceRequest::try_new(
        source_anchor(&current_owner),
        DeliverySourceState::Committed,
        coding_agent_store::DeliveryVersion::try_new(3).unwrap(),
        coding_agent_store::DeliveryVersion::try_new(3).unwrap(),
        DeliverySourceReconciliationReason::SourceInconsistent,
    )
    .unwrap();
    assert!(matches!(
        store.reconcile_delivery_source(reconcile).await.unwrap(),
        ReconcileDeliverySourceOutcome::Applied(_)
    ));

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
    .bind(current_owner.client_request_id().to_string())
    .execute(&mut *connection)
    .await
    .unwrap();
    drop(connection);
    assert_current_pair_is_rejected(&store, &first_command, replay.clone()).await;

    let forged_tuple_command = AcceptMergeCommandRequest::try_new(
        ClientRequestId::from_str(&current_owner.client_request_id().to_string()).unwrap(),
        current_owner.task_id(),
        current_owner.preflight_operation_id(),
        coding_agent_store::DeliveryVersion::try_new(1).unwrap(),
        current_owner.expected_review_generation(),
        current_owner.expected_workspace_fingerprint().clone(),
        current_owner.target_branch().clone(),
        current_owner.expected_target_head().clone(),
    )
    .unwrap();
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE task_delivery_command_receipts \
         SET canonical_request_hash = ?, accepted_operation_version = 2 \
         WHERE client_request_id = ?",
    )
    .bind(forged_tuple_command.canonical_request_hash().as_str())
    .bind(current_owner.client_request_id().to_string())
    .execute(&mut *connection)
    .await
    .unwrap();
    drop(connection);
    assert_current_pair_is_rejected(&store, &first_command, replay).await;
}

#[tokio::test]
async fn schema_rejects_an_arbitrary_historical_source_retry_failure_code() {
    let (store, command) = accepted_fixture().await;
    let source = created_source(&store, command.clone()).await;
    let anchor = source_anchor(&command);
    let retry = RecordDeliverySourceRetryRequest::try_new(
        anchor,
        DeliverySourceState::ObjectPending,
        source.version,
        DeliverySourceRetryReason::CommandTimedOut,
    )
    .unwrap();
    let retried_version = match store
        .record_delivery_source_retry(retry.clone())
        .await
        .unwrap()
    {
        DeliverySourceTransitionOutcome::Applied(receipt) => receipt.version,
        other => panic!("expected retry to apply, got {other:?}"),
    };
    store
        .advance_delivery_source_object(
            AdvanceDeliverySourceObjectRequest::try_new(
                anchor,
                retried_version,
                object_proof(&source),
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::raw_sql("DROP TRIGGER task_delivery_operation_transitions_no_update;")
        .execute(&mut *connection)
        .await
        .unwrap();
    let corrupted = sqlx::query(
        "UPDATE task_delivery_operation_transitions SET failure_code = 'ARBITRARY_FAILURE' \
         WHERE entity_kind = 'delivery_source' AND entity_id = ? AND entity_version = 2",
    )
    .bind(command.task_id().to_string())
    .execute(&mut *connection)
    .await;
    assert!(matches!(corrupted, Err(sqlx::Error::Database(_))));
    drop(connection);

    assert!(matches!(
        store.record_delivery_source_retry(retry).await.unwrap(),
        DeliverySourceTransitionOutcome::Existing(_)
    ));
    store
        .delivery_ownership_snapshot(command.task_id())
        .await
        .unwrap();
}

#[tokio::test]
async fn historical_retry_with_a_null_failure_is_rejected_by_the_direct_api() {
    let (store, command) = accepted_fixture().await;
    let source = created_source(&store, command.clone()).await;
    let anchor = source_anchor(&command);
    let retry = RecordDeliverySourceRetryRequest::try_new(
        anchor,
        DeliverySourceState::ObjectPending,
        source.version,
        DeliverySourceRetryReason::CommandTimedOut,
    )
    .unwrap();
    store
        .record_delivery_source_retry(retry.clone())
        .await
        .unwrap();
    store
        .advance_delivery_source_object(
            AdvanceDeliverySourceObjectRequest::try_new(
                anchor,
                coding_agent_store::DeliveryVersion::try_new(2).unwrap(),
                object_proof(&source),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    drop_transition_update_guard(&store).await;
    sqlx::query(
        "UPDATE task_delivery_operation_transitions SET failure_code = NULL \
         WHERE entity_kind = 'delivery_source' AND entity_id = ? AND entity_version = 2",
    )
    .bind(command.task_id().to_string())
    .execute(store.pool())
    .await
    .unwrap();
    assert_source_api_invariant(store.record_delivery_source_retry(retry).await);
}

#[tokio::test]
async fn conflicting_object_journal_is_a_direct_api_invariant() {
    let (store, command) = accepted_fixture().await;
    let source = created_source(&store, command.clone()).await;
    let request = AdvanceDeliverySourceObjectRequest::try_new(
        source_anchor(&command),
        source.version,
        object_proof(&source),
    )
    .unwrap();
    store
        .advance_delivery_source_object(request.clone())
        .await
        .unwrap();
    drop_transition_update_guard(&store).await;
    sqlx::query(
        "UPDATE task_delivery_operation_transitions \
         SET to_state = 'object_pending', failure_code = 'COMMAND_TIMED_OUT' \
         WHERE entity_kind = 'delivery_source' AND entity_id = ? AND entity_version = 2",
    )
    .bind(command.task_id().to_string())
    .execute(store.pool())
    .await
    .unwrap();
    assert_source_api_invariant(store.advance_delivery_source_object(request).await);
}

#[tokio::test]
async fn conflicting_commit_journal_is_a_direct_api_invariant() {
    let (store, command) = accepted_fixture().await;
    let (source, anchor, _object, proof) = commit_pending_fixture(&store, &command).await;
    let request = CommitDeliverySourceRequest::try_new(anchor, source.version, proof).unwrap();
    store.commit_delivery_source(request.clone()).await.unwrap();
    drop_transition_update_guard(&store).await;
    sqlx::query(
        "UPDATE task_delivery_operation_transitions SET failure_code = 'ARBITRARY_FAILURE' \
         WHERE entity_kind = 'delivery_source' AND entity_id = ? AND entity_version = 3",
    )
    .bind(command.task_id().to_string())
    .execute(store.pool())
    .await
    .unwrap();
    assert_source_api_invariant(store.commit_delivery_source(request).await);
}

#[tokio::test]
async fn accepted_owner_with_a_durable_terminal_state_cannot_drive_source_commands() {
    let (store, command) = accepted_fixture().await;
    let source = created_source(&store, command.clone()).await;
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
    sqlx::query(
        "UPDATE task_merge_operations \
         SET state = 'reconciliation_required', \
             failure_code = 'DELIVERY_RECONCILIATION_REQUIRED' \
         WHERE operation_id = ?",
    )
    .bind(command.preflight_operation_id().to_string())
    .execute(&mut *connection)
    .await
    .unwrap();
    drop(connection);

    let request = AdvanceDeliverySourceObjectRequest::try_new(
        source_anchor(&command),
        source.version,
        object_proof(&source),
    )
    .unwrap();
    match store.advance_delivery_source_object(request).await {
        Err(StoreError::InvariantViolation(message)) => {
            assert_eq!(message, "delivery source transaction is inconsistent")
        }
        other => panic!("failed Accepted owner drove source command: {other:?}"),
    }
}

#[tokio::test]
async fn committed_replay_rejects_a_progressed_owner_linked_to_the_wrong_source_oid() {
    let (store, command) = accepted_fixture().await;
    let (pending, anchor, _object, applied) = commit_pending_fixture(&store, &command).await;
    let commit = CommitDeliverySourceRequest::try_new(anchor, pending.version, applied).unwrap();
    assert!(matches!(
        store.commit_delivery_source(commit.clone()).await.unwrap(),
        DeliverySourceTransitionOutcome::Applied(_)
    ));
    sqlx::query(
        "UPDATE task_merge_operations SET delivery_source_task_id = ?, source_commit_oid = ?, \
             state = 'failed', failure_code = 'TARGET_HEAD_CHANGED', version = 4, updated_at = ? \
         WHERE operation_id = ?",
    )
    .bind(command.task_id().to_string())
    .bind(SOURCE_COMMIT)
    .bind(DELIVERY_TIMESTAMP)
    .bind(command.preflight_operation_id().to_string())
    .execute(store.pool())
    .await
    .unwrap();

    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
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
    sqlx::query("UPDATE task_merge_operations SET source_commit_oid = ? WHERE operation_id = ?")
        .bind(WRONG_SOURCE_COMMIT)
        .bind(command.preflight_operation_id().to_string())
        .execute(&mut *connection)
        .await
        .unwrap();
    drop(connection);

    match store.commit_delivery_source(commit).await {
        Err(StoreError::InvariantViolation(message)) => {
            assert_eq!(message, "delivery source transaction is inconsistent")
        }
        other => panic!("wrong progressed owner link was accepted: {other:?}"),
    }
}

#[tokio::test]
async fn accept_receipt_with_a_missing_operation_is_a_source_invariant() {
    let (store, command) = accepted_fixture().await;
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::raw_sql("DROP TRIGGER task_merge_operations_no_delete;")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("DELETE FROM task_merge_operations WHERE operation_id = ?")
        .bind(command.preflight_operation_id().to_string())
        .execute(&mut *connection)
        .await
        .unwrap();
    drop(connection);

    let request = coding_agent_store::CreateDeliverySourceRequest::try_new(command).unwrap();
    match store.create_delivery_source(request).await {
        Err(StoreError::InvariantViolation(message)) => {
            assert_eq!(message, "delivery source transaction is inconsistent")
        }
        other => panic!("orphaned accept receipt was not an invariant: {other:?}"),
    }
}

async fn fail_original_owner(store: &Store, command: &AcceptMergeCommandRequest) {
    let updated = sqlx::query(
        "UPDATE task_merge_operations SET delivery_source_task_id = ?, source_commit_oid = ?, \
             state = 'failed', failure_code = 'TARGET_HEAD_CHANGED', version = 4, updated_at = ? \
         WHERE operation_id = ? AND state = 'accepted' AND version = 3",
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

async fn create_unaccepted_preflight_ready(
    store: &Store,
    first: &AcceptMergeCommandRequest,
) -> DeliveryOperationId {
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
    operation_id
}

async fn accept_preflight_ready(
    store: &Store,
    first: &AcceptMergeCommandRequest,
    operation_id: DeliveryOperationId,
) -> AcceptMergeCommandRequest {
    let evidence = store
        .delivery_eligibility_snapshot(first.task_id())
        .await
        .unwrap()
        .unwrap()
        .evidence_identity
        .unwrap();
    let command = AcceptMergeCommandRequest::try_new(
        ClientRequestId::new(),
        first.task_id(),
        operation_id,
        coding_agent_store::DeliveryVersion::try_new(2).unwrap(),
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

async fn assert_current_pair_is_rejected(
    store: &Store,
    first: &AcceptMergeCommandRequest,
    replay: CommitDeliverySourceRequest,
) {
    assert!(matches!(
        store.commit_delivery_source(replay).await,
        Err(StoreError::InvariantViolation(
            "delivery source transaction is inconsistent"
        ))
    ));
    assert!(matches!(
        store.delivery_ownership_snapshot(first.task_id()).await,
        Err(StoreError::InvariantViolation(
            "delivery ownership snapshot is inconsistent"
        ))
    ));
}

async fn poison_reconciliation_pair(
    store: &Store,
    task_id: coding_agent_domain::TaskId,
    operation_id: DeliveryOperationId,
) {
    let mut transaction = store.pool().begin().await.unwrap();
    let source = sqlx::query(
        "UPDATE task_delivery_sources \
         SET state = 'reconciliation_required', failure_code = 'DELIVERY_SOURCE_INCONSISTENT', \
             version = 4, updated_at = ? \
         WHERE task_id = ? AND state = 'committed' AND version = 3",
    )
    .bind(DELIVERY_TIMESTAMP)
    .bind(task_id.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    assert_eq!(source.rows_affected(), 1);
    let merge = sqlx::query(
        "UPDATE task_merge_operations \
         SET state = 'reconciliation_required', failure_code = 'DELIVERY_SOURCE_INCONSISTENT', \
             version = 3, updated_at = ? \
         WHERE operation_id = ? AND state = 'preflight_ready' AND version = 2",
    )
    .bind(DELIVERY_TIMESTAMP)
    .bind(operation_id.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    assert_eq!(merge.rows_affected(), 1);
    transaction.commit().await.unwrap();
}

async fn drop_transition_update_guard(store: &coding_agent_store::Store) {
    sqlx::query("PRAGMA ignore_check_constraints = ON")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::raw_sql("DROP TRIGGER task_delivery_operation_transitions_no_update;")
        .execute(store.pool())
        .await
        .unwrap();
}

fn assert_source_api_invariant(result: Result<DeliverySourceTransitionOutcome, StoreError>) {
    assert!(matches!(result, Err(StoreError::InvariantViolation(_))));
}
