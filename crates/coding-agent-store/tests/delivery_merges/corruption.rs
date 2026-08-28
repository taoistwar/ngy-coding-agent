use std::str::FromStr;

use coding_agent_domain::{ClientRequestId, TaskId};
use coding_agent_store::{
    DeliverySourceAnchor, DeliverySourceReconciliationReason, DeliverySourceState, DeliveryVersion,
    EnterMergePendingRequest, GitCommitOid, GitTreeOid, MarkPreflightStaleRequest,
    MergeCommitObjectProof, MergeConflictPaths, MergeKnownNotAppliedReason, MergeOperationState,
    MergePreflightResult, PreflightStaleReason, ReconcileDeliverySourceRequest,
    RecordMergeKnownFailureRequest, RecordMergePreflightResultRequest, StoreError,
};
use uuid::Uuid;

use crate::support::delivery::eligibility::{MERGE_BASE, MERGE_TREE};

use super::fixtures::{
    accept_command, accepted_with_committed_source, create_pending_preflight_with_source,
    merge_pending, pending_preflight,
};
use crate::support::delivery::eligibility::{MERGE_COMMIT, SOURCE_COMMIT, TARGET_HEAD};

fn ready_request(
    task_id: coding_agent_domain::TaskId,
    operation_id: coding_agent_store::DeliveryOperationId,
) -> RecordMergePreflightResultRequest {
    RecordMergePreflightResultRequest::try_new(
        task_id,
        operation_id,
        DeliveryVersion::try_new(2).unwrap(),
        MergePreflightResult::ready(
            GitCommitOid::from_str(MERGE_BASE).unwrap(),
            GitTreeOid::from_str(MERGE_TREE).unwrap(),
        )
        .unwrap(),
    )
    .unwrap()
}

fn conflict_request(
    task_id: coding_agent_domain::TaskId,
    operation_id: coding_agent_store::DeliveryOperationId,
    paths: Vec<Vec<u8>>,
) -> RecordMergePreflightResultRequest {
    RecordMergePreflightResultRequest::try_new(
        task_id,
        operation_id,
        DeliveryVersion::try_new(2).unwrap(),
        MergePreflightResult::conflict(
            GitCommitOid::from_str(MERGE_BASE).unwrap(),
            GitTreeOid::from_str(MERGE_TREE).unwrap(),
            MergeConflictPaths::try_from_raw(paths).unwrap(),
        )
        .unwrap(),
    )
    .unwrap()
}

async fn drop_receipt_update_guard(store: &coding_agent_store::Store) {
    sqlx::query("DROP TRIGGER task_delivery_command_receipts_no_update")
        .execute(store.pool())
        .await
        .unwrap();
}

async fn drop_merge_update_guards(store: &coding_agent_store::Store) {
    sqlx::raw_sql(
        "DROP TRIGGER task_merge_operations_immutable_on_update; \
         DROP TRIGGER task_merge_operations_transition_on_update; \
         DROP TRIGGER task_merge_operations_source_consistency_on_update; \
         DROP TRIGGER task_merge_operations_source_reconciliation_on_update; \
         DROP TRIGGER task_merge_operations_journal_on_update;",
    )
    .execute(store.pool())
    .await
    .unwrap();
}

#[tokio::test]
async fn preflight_result_rejects_a_missing_origin_receipt_without_transitioning() {
    let (store, task, operation_id) = pending_preflight().await;
    let receipt_id: String = sqlx::query_scalar(
        "SELECT preflight_receipt_id FROM task_merge_operations WHERE operation_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("DROP TRIGGER task_delivery_command_receipts_no_delete")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("DELETE FROM task_delivery_command_receipts WHERE client_request_id = ?")
        .bind(receipt_id)
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
    drop(connection);

    assert!(matches!(
        store
            .record_merge_preflight_result(ready_request(task.id, operation_id))
            .await,
        Err(StoreError::InvariantViolation(_))
    ));
    let state: (String, i64) =
        sqlx::query_as("SELECT state, version FROM task_merge_operations WHERE operation_id = ?")
            .bind(operation_id.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(state, ("preflight_pending".to_owned(), 2));
}

#[tokio::test]
async fn ownership_replay_rejects_a_corrupted_preflight_request_hash() {
    let (store, task, operation_id) = pending_preflight().await;
    drop_receipt_update_guard(&store).await;
    sqlx::query(
        "UPDATE task_delivery_command_receipts \
         SET canonical_request_hash = ? WHERE operation_id = ? AND command_kind = 'preflight'",
    )
    .bind("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
    .bind(operation_id.to_string())
    .execute(store.pool())
    .await
    .unwrap();

    assert!(matches!(
        store.delivery_ownership_snapshot(task.id).await,
        Err(StoreError::InvariantViolation(_))
    ));
}

#[tokio::test]
async fn wrong_task_classification_cannot_mask_a_corrupted_operation_graph() {
    let (store, _task, operation_id) = pending_preflight().await;
    drop_receipt_update_guard(&store).await;
    sqlx::query(
        "UPDATE task_delivery_command_receipts \
         SET canonical_request_hash = ? WHERE operation_id = ? AND command_kind = 'preflight'",
    )
    .bind("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
    .bind(operation_id.to_string())
    .execute(store.pool())
    .await
    .unwrap();

    assert!(matches!(
        store
            .record_merge_preflight_result(ready_request(TaskId::new(), operation_id))
            .await,
        Err(StoreError::InvariantViolation(_))
    ));
}

#[tokio::test]
async fn stale_entrypoint_audits_the_operation_before_classifying_a_missing_caller_task() {
    let (store, _task, operation_id) = pending_preflight().await;
    drop_receipt_update_guard(&store).await;
    sqlx::query(
        "UPDATE task_delivery_command_receipts \
         SET canonical_request_hash = ? WHERE operation_id = ? AND command_kind = 'preflight'",
    )
    .bind("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
    .bind(operation_id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
    let request = MarkPreflightStaleRequest::try_new(
        TaskId::new(),
        operation_id,
        DeliveryVersion::initial(),
        PreflightStaleReason::TargetHeadChanged,
    )
    .unwrap();
    assert!(matches!(
        store.mark_merge_preflight_stale(request).await,
        Err(StoreError::InvariantViolation(_))
    ));

    let (store, _task, operation_id) = pending_preflight().await;
    let request = MarkPreflightStaleRequest::try_new(
        TaskId::new(),
        operation_id,
        DeliveryVersion::initial(),
        PreflightStaleReason::TargetHeadChanged,
    )
    .unwrap();
    assert!(matches!(
        store.mark_merge_preflight_stale(request).await,
        Err(StoreError::TaskNotFound)
    ));
}

#[tokio::test]
async fn exact_result_replay_rejects_a_preflight_receipt_timestamp_drift() {
    let (store, task, operation_id) = pending_preflight().await;
    let request = ready_request(task.id, operation_id);
    store
        .record_merge_preflight_result(request.clone())
        .await
        .unwrap();
    drop_receipt_update_guard(&store).await;
    sqlx::query(
        "UPDATE task_delivery_command_receipts SET created_at = ? \
         WHERE operation_id = ? AND command_kind = 'preflight'",
    )
    .bind("2026-08-04T00:00:01.000000000Z")
    .bind(operation_id.to_string())
    .execute(store.pool())
    .await
    .unwrap();

    assert!(matches!(
        store.record_merge_preflight_result(request).await,
        Err(StoreError::InvariantViolation(_))
    ));
}

#[tokio::test]
async fn stale_entrypoint_rejects_orphaned_history_when_current_row_is_missing() {
    let (store, task, operation_id) = pending_preflight().await;
    store
        .record_merge_preflight_result(ready_request(task.id, operation_id))
        .await
        .unwrap();
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("DROP TRIGGER task_merge_operations_no_delete")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("DELETE FROM task_merge_operations WHERE operation_id = ?")
        .bind(operation_id.to_string())
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
    drop(connection);

    let request = MarkPreflightStaleRequest::try_new(
        task.id,
        operation_id,
        DeliveryVersion::try_new(3).unwrap(),
        PreflightStaleReason::TargetHeadChanged,
    )
    .unwrap();
    assert!(matches!(
        store.mark_merge_preflight_stale(request).await,
        Err(StoreError::InvariantViolation(_))
    ));
}

#[tokio::test]
async fn schema_rejects_a_partial_preflight_result_group() {
    let (store, task, operation_id) = pending_preflight().await;
    drop_merge_update_guards(&store).await;
    let corrupted =
        sqlx::query("UPDATE task_merge_operations SET merge_base_oid = ? WHERE operation_id = ?")
            .bind(MERGE_BASE)
            .bind(operation_id.to_string())
            .execute(store.pool())
            .await;
    assert!(matches!(corrupted, Err(sqlx::Error::Database(_))));

    store.delivery_ownership_snapshot(task.id).await.unwrap();
}

#[tokio::test]
async fn mutation_rejects_merge_provenance_drift_from_its_review_and_artifact_parent() {
    let (store, task, operation_id) = pending_preflight().await;
    drop_merge_update_guards(&store).await;
    sqlx::query(
        "UPDATE task_merge_operations SET workspace_fingerprint = ? WHERE operation_id = ?",
    )
    .bind("0101010101010101010101010101010101010101010101010101010101010101")
    .bind(operation_id.to_string())
    .execute(store.pool())
    .await
    .unwrap();

    assert!(matches!(
        store
            .record_merge_preflight_result(ready_request(task.id, operation_id))
            .await,
        Err(StoreError::InvariantViolation(_))
    ));
    let row: (String, i64, i64) = sqlx::query_as(
        "SELECT state, version, \
                (SELECT COUNT(*) FROM task_delivery_operation_transitions \
                 WHERE entity_kind = 'merge_operation' AND entity_id = ?) \
         FROM task_merge_operations WHERE operation_id = ?",
    )
    .bind(operation_id.to_string())
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(row, ("preflight_pending".to_owned(), 2, 2));
}

#[tokio::test]
async fn schema_rejects_a_forbidden_abort_group_on_an_accepted_operation() {
    let (store, _task, operation_id, _accepted_version) = accepted_with_committed_source().await;
    drop_merge_update_guards(&store).await;
    let result = sqlx::query(
        "UPDATE task_merge_operations \
         SET abort_child_receipt_id = ?, abort_merge_head_oid = ?, \
              abort_index_stages_digest = ?, abort_worktree_digest = ?, \
              abort_merge_autostash_proof = 'absent', conflict_path_count = 1 \
         WHERE operation_id = ?",
    )
    .bind(Uuid::new_v4().to_string())
    .bind("1212121212121212121212121212121212121212")
    .bind("a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1")
    .bind("b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2")
    .bind(operation_id.to_string())
    .execute(store.pool())
    .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn sealed_zero_path_conflict_rejects_a_late_child_append() {
    let (store, task, operation_id) = pending_preflight().await;
    let request = conflict_request(task.id, operation_id, Vec::new());
    store
        .record_merge_preflight_result(request.clone())
        .await
        .unwrap();
    let inserted = sqlx::query(
        "INSERT INTO task_merge_conflicts \
         (operation_id, ordinal, path_encoding, path_value) VALUES (?, 0, 'utf8', 'late.rs')",
    )
    .bind(operation_id.to_string())
    .execute(store.pool())
    .await;
    assert!(matches!(inserted, Err(sqlx::Error::Database(_))));
    assert!(matches!(
        store.record_merge_preflight_result(request).await.unwrap(),
        coding_agent_store::MergeTransitionOutcome::Existing(_)
    ));
}

#[tokio::test]
async fn loader_rejects_a_missing_row_from_the_sealed_conflict_count() {
    let (store, task, operation_id) = pending_preflight().await;
    let request = conflict_request(task.id, operation_id, vec![b"src/conflict.rs".to_vec()]);
    store.record_merge_preflight_result(request).await.unwrap();
    sqlx::query("DROP TRIGGER task_merge_conflicts_no_delete")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM task_merge_conflicts WHERE operation_id = ?")
        .bind(operation_id.to_string())
        .execute(store.pool())
        .await
        .unwrap();
    assert!(matches!(
        store.delivery_ownership_snapshot(task.id).await,
        Err(StoreError::InvariantViolation(_))
    ));
}

#[tokio::test]
async fn loader_rejects_padded_noncanonical_and_utf8_alias_base64url_rows() {
    let (store, task, operation_id) = pending_preflight().await;
    let request = conflict_request(task.id, operation_id, vec![vec![0xff]]);
    store.record_merge_preflight_result(request).await.unwrap();
    sqlx::query("DROP TRIGGER task_merge_conflicts_no_update")
        .execute(store.pool())
        .await
        .unwrap();
    let padded = sqlx::query(
        "UPDATE task_merge_conflicts \
         SET path_encoding = 'base64url', path_value = '_w==' WHERE operation_id = ?",
    )
    .bind(operation_id.to_string())
    .execute(store.pool())
    .await;
    assert!(matches!(padded, Err(sqlx::Error::Database(_))));
    for invalid_wire in ["_x", "YWxpYXMucnM"] {
        sqlx::query(
            "UPDATE task_merge_conflicts \
             SET path_encoding = 'base64url', path_value = ? WHERE operation_id = ?",
        )
        .bind(invalid_wire)
        .bind(operation_id.to_string())
        .execute(store.pool())
        .await
        .unwrap();
        assert!(matches!(
            store.delivery_ownership_snapshot(task.id).await,
            Err(StoreError::InvariantViolation(_))
        ));
    }
}

#[tokio::test]
async fn ownership_rejects_raw_degenerate_merge_parent_and_object_ids() {
    let (store, task, operation_id, _version) = merge_pending().await;
    drop_merge_update_guards(&store).await;
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA ignore_check_constraints = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE task_merge_operations SET expected_merge_commit_oid = ? WHERE operation_id = ?",
    )
    .bind(SOURCE_COMMIT)
    .bind(operation_id.to_string())
    .execute(&mut *connection)
    .await
    .unwrap();
    sqlx::query("PRAGMA ignore_check_constraints = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    drop(connection);
    assert!(matches!(
        store.delivery_ownership_snapshot(task.id).await,
        Err(StoreError::InvariantViolation(_))
    ));

    let (store, task, operation_id, _version) = merge_pending().await;
    drop_merge_update_guards(&store).await;
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA ignore_check_constraints = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("UPDATE task_merge_operations SET expected_target_head = ? WHERE operation_id = ?")
        .bind(SOURCE_COMMIT)
        .bind(operation_id.to_string())
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("PRAGMA ignore_check_constraints = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    drop(connection);
    assert!(matches!(
        store.delivery_ownership_snapshot(task.id).await,
        Err(StoreError::InvariantViolation(_))
    ));
}

#[tokio::test]
async fn all_row_graph_rejects_active_and_reconciliation_slots_together() {
    let (store, task, first_operation, second_operation) =
        failed_merge_then_second_accepted().await;
    let pending_request = pending_request(&store, &task, second_operation).await;
    let failed_request = RecordMergeKnownFailureRequest::try_new(
        task.id,
        second_operation,
        MergeOperationState::Accepted,
        DeliveryVersion::try_new(4).unwrap(),
        MergeKnownNotAppliedReason::TargetHeadChanged,
    )
    .unwrap();
    let source = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .source
        .unwrap();
    let source_request = ReconcileDeliverySourceRequest::try_new(
        DeliverySourceAnchor::try_new(
            task.id,
            second_operation,
            DeliveryVersion::try_new(4).unwrap(),
        )
        .unwrap(),
        DeliverySourceState::Committed,
        source.version,
        DeliveryVersion::try_new(4).unwrap(),
        DeliverySourceReconciliationReason::SourceInconsistent,
    )
    .unwrap();
    drop_merge_update_guards(&store).await;
    sqlx::query("DROP TRIGGER task_delivery_operation_transitions_no_update")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE task_merge_operations \
         SET state = 'reconciliation_required', failure_code = 'WORKTREE_IDENTITY_MISMATCH' \
         WHERE operation_id = ?",
    )
    .bind(first_operation.to_string())
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_delivery_operation_transitions \
         SET to_state = 'reconciliation_required', failure_code = 'WORKTREE_IDENTITY_MISMATCH' \
         WHERE entity_kind = 'merge_operation' AND entity_id = ? AND entity_version = 6",
    )
    .bind(first_operation.to_string())
    .execute(store.pool())
    .await
    .unwrap();

    let states: Vec<String> = sqlx::query_scalar(
        "SELECT state FROM task_merge_operations \
         WHERE operation_id IN (?, ?) ORDER BY operation_id",
    )
    .bind(first_operation.to_string())
    .bind(second_operation.to_string())
    .fetch_all(store.pool())
    .await
    .unwrap();
    assert!(states.contains(&"accepted".to_owned()));
    assert!(states.contains(&"reconciliation_required".to_owned()));
    let before: (String, i64, i64) = sqlx::query_as(
        "SELECT state, version, \
                (SELECT COUNT(*) FROM task_delivery_operation_transitions \
                 WHERE entity_kind = 'merge_operation' AND entity_id = ?) \
         FROM task_merge_operations WHERE operation_id = ?",
    )
    .bind(second_operation.to_string())
    .bind(second_operation.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert!(matches!(
        store.enter_merge_pending(pending_request).await,
        Err(StoreError::InvariantViolation(_))
    ));
    assert!(matches!(
        store.record_merge_known_failure(failed_request).await,
        Err(StoreError::InvariantViolation(_))
    ));
    assert!(matches!(
        store.reconcile_delivery_source(source_request).await,
        Err(StoreError::InvariantViolation(_))
    ));
    let after: (String, i64, i64) = sqlx::query_as(
        "SELECT state, version, \
                (SELECT COUNT(*) FROM task_delivery_operation_transitions \
                 WHERE entity_kind = 'merge_operation' AND entity_id = ?) \
         FROM task_merge_operations WHERE operation_id = ?",
    )
    .bind(second_operation.to_string())
    .bind(second_operation.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(after, before);
    assert!(matches!(
        store.delivery_ownership_snapshot(task.id).await,
        Err(StoreError::InvariantViolation(_))
    ));
}

#[tokio::test]
async fn all_row_graph_rejects_active_or_reconciliation_alongside_merged() {
    let (store, task, first_operation, second_operation) =
        failed_merge_then_second_accepted().await;
    drop_merge_update_guards(&store).await;
    sqlx::query("DROP TRIGGER task_delivery_operation_transitions_no_update")
        .execute(store.pool())
        .await
        .unwrap();
    let merged_at: String =
        sqlx::query_scalar("SELECT updated_at FROM task_merge_operations WHERE operation_id = ?")
            .bind(first_operation.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap();
    let mut transaction = store.pool().begin().await.unwrap();
    sqlx::query(
        "UPDATE task_merge_operations \
         SET state = 'merged', failure_code = NULL, merged_disposition_task_id = ? \
         WHERE operation_id = ?",
    )
    .bind(task.id.to_string())
    .bind(first_operation.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_delivery_operation_transitions \
         SET to_state = 'merged', failure_code = NULL \
         WHERE entity_kind = 'merge_operation' AND entity_id = ? AND entity_version = 6",
    )
    .bind(first_operation.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO task_artifact_dispositions ( \
             task_id, repository_id, attempt, merged_operation_id, delivery_source_task_id, \
             source_commit_oid, worktree_state, worktree_version, worktree_failure_code, \
             worktree_updated_at, branch_state, branch_version, branch_failure_code, \
             branch_updated_at, created_at \
         ) VALUES (?, ?, ?, ?, ?, ?, 'retained_locked', 1, NULL, ?, \
                   'retained', 1, NULL, ?, ?)",
    )
    .bind(task.id.to_string())
    .bind(task.repository_id.to_string())
    .bind(i64::from(task.attempt))
    .bind(first_operation.to_string())
    .bind(task.id.to_string())
    .bind(SOURCE_COMMIT)
    .bind(&merged_at)
    .bind(&merged_at)
    .bind(&merged_at)
    .execute(&mut *transaction)
    .await
    .unwrap();
    transaction.commit().await.unwrap();

    assert!(matches!(
        store.delivery_ownership_snapshot(task.id).await,
        Err(StoreError::InvariantViolation(_))
    ));

    let accepted_at: String =
        sqlx::query_scalar("SELECT updated_at FROM task_merge_operations WHERE operation_id = ?")
            .bind(second_operation.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap();
    sqlx::query(
        "UPDATE task_merge_operations \
         SET state = 'reconciliation_required', failure_code = 'WORKTREE_IDENTITY_MISMATCH', \
             version = 5, updated_at = ? WHERE operation_id = ?",
    )
    .bind(&accepted_at)
    .bind(second_operation.to_string())
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO task_delivery_operation_transitions ( \
             entity_kind, entity_id, entity_version, from_state, to_state, \
             failure_code, target_config_attributes_digest, target_security_digest, \
             transitioned_at \
         ) VALUES ('merge_operation', ?, 5, 'accepted', 'reconciliation_required', \
                   'WORKTREE_IDENTITY_MISMATCH', \
                   (SELECT target_config_attributes_digest FROM task_merge_operations \
                    WHERE operation_id = ?), \
                   (SELECT target_security_digest FROM task_merge_operations \
                    WHERE operation_id = ?), ?)",
    )
    .bind(second_operation.to_string())
    .bind(second_operation.to_string())
    .bind(second_operation.to_string())
    .bind(&accepted_at)
    .execute(store.pool())
    .await
    .unwrap();
    assert!(matches!(
        store.delivery_ownership_snapshot(task.id).await,
        Err(StoreError::InvariantViolation(_))
    ));
}

async fn failed_merge_then_second_accepted() -> (
    coding_agent_store::Store,
    coding_agent_domain::Task,
    coding_agent_store::DeliveryOperationId,
    coding_agent_store::DeliveryOperationId,
) {
    let (store, task, first_operation, pending_version) = merge_pending().await;
    let failed = RecordMergeKnownFailureRequest::try_new(
        task.id,
        first_operation,
        MergeOperationState::MergePending,
        pending_version,
        MergeKnownNotAppliedReason::TargetHeadChanged,
    )
    .unwrap();
    store.record_merge_known_failure(failed).await.unwrap();
    let second_operation = create_pending_preflight_with_source(&store, &task, SOURCE_COMMIT).await;
    super::preflight_results::ready(&store, task.id, second_operation).await;
    let accept = accept_command(&store, &task, second_operation, ClientRequestId::new()).await;
    assert!(matches!(
        store.accept_merge(accept).await.unwrap(),
        coding_agent_store::AcceptMergeOutcome::Accepted(_)
    ));
    (store, task, first_operation, second_operation)
}

async fn pending_request(
    store: &coding_agent_store::Store,
    task: &coding_agent_domain::Task,
    operation_id: coding_agent_store::DeliveryOperationId,
) -> EnterMergePendingRequest {
    let operation = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap();
    EnterMergePendingRequest::try_new(
        task.id,
        operation_id,
        DeliveryVersion::try_new(4).unwrap(),
        MergeCommitObjectProof::try_new(
            GitCommitOid::from_str(MERGE_COMMIT).unwrap(),
            GitTreeOid::from_str(MERGE_TREE).unwrap(),
            vec![
                GitCommitOid::from_str(TARGET_HEAD).unwrap(),
                GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
            ],
            operation.merge_metadata.unwrap(),
        )
        .unwrap(),
    )
    .unwrap()
}
