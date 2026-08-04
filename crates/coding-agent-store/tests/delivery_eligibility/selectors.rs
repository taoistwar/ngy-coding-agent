use coding_agent_store::{
    DeliveryOperationId, MergeOperationState, PersistentEligibilityBlocker, StoreError,
};

use crate::support::delivery::eligibility::{
    MERGE_COMMIT, approved_task_with_ready_artifact, complete_worktree_cleanup,
    create_branch_cleanup, create_merged_delivery, create_worktree_cleanup, fail_worktree_cleanup,
    finish_preflight_terminal, insert_preflight, reconcile_worktree_cleanup,
};

#[tokio::test]
async fn historical_preflight_terminals_do_not_claim_active_ownership() {
    for state in [
        MergeOperationState::Conflict,
        MergeOperationState::Rejected,
        MergeOperationState::Stale,
        MergeOperationState::Superseded,
    ] {
        let (store, task) = approved_task_with_ready_artifact("codex/task-terminal-kind").await;
        let operation_id = insert_valid_preflight(&store, &task).await;
        finish_preflight_terminal(&store, operation_id, state).await;

        let snapshot = store
            .delivery_eligibility_snapshot(task.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.ownership.merge_operations.len(), 1);
        assert_eq!(snapshot.ownership.merge_operations[0].state, state);
        for absent in [
            PersistentEligibilityBlocker::DeliveryOwned,
            PersistentEligibilityBlocker::AlreadyMerged,
            PersistentEligibilityBlocker::ReconciliationRequired,
        ] {
            assert!(!snapshot.persistent_blockers.contains(&absent));
        }
    }
}

#[tokio::test]
async fn terminal_history_projection_is_bounded_to_the_latest_operation() {
    let (store, task) = approved_task_with_ready_artifact("codex/task-bounded-terminal").await;
    let initial = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    let evidence = initial.evidence_identity.as_ref().unwrap();
    let mut latest = None;
    for _ in 0..24 {
        let operation_id = DeliveryOperationId::new();
        insert_preflight(&store, &task, evidence, operation_id).await;
        finish_preflight_terminal(&store, operation_id, MergeOperationState::Stale).await;
        latest = Some(operation_id);
    }

    let snapshot = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.merge_operations.len(), 1);
    assert_eq!(snapshot.merge_operations[0].operation_id, latest.unwrap());
}

#[tokio::test]
async fn hidden_older_merge_terminal_with_missing_history_fails_closed() {
    let (store, task) = approved_task_with_ready_artifact("codex/task-hidden-merge-history").await;
    let older = insert_valid_preflight(&store, &task).await;
    finish_preflight_terminal(&store, older, MergeOperationState::Stale).await;
    let newer = insert_valid_preflight(&store, &task).await;
    finish_preflight_terminal(&store, newer, MergeOperationState::Stale).await;

    delete_initial_transition(&store, "merge_operation", older).await;

    assert_invariant(
        store
            .delivery_ownership_snapshot(task.id)
            .await
            .unwrap_err(),
    );
}

#[tokio::test]
async fn hidden_older_cleanup_terminal_with_missing_history_fails_closed() {
    let (store, task) =
        approved_task_with_ready_artifact("codex/task-hidden-cleanup-history").await;
    let initial = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    create_merged_delivery(&store, &task, initial.evidence_identity.as_ref().unwrap()).await;
    let older = create_worktree_cleanup(&store, &task).await;
    fail_worktree_cleanup(&store, older).await;
    let newer = create_worktree_cleanup(&store, &task).await;
    fail_worktree_cleanup(&store, newer).await;

    delete_initial_transition(&store, "cleanup_operation", older).await;

    assert_invariant(
        store
            .delivery_ownership_snapshot(task.id)
            .await
            .unwrap_err(),
    );
}

#[tokio::test]
async fn hidden_older_cleanup_terminal_with_corrupted_origin_receipt_fails_closed() {
    let (store, task) =
        approved_task_with_ready_artifact("codex/task-hidden-cleanup-receipt").await;
    let initial = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    create_merged_delivery(&store, &task, initial.evidence_identity.as_ref().unwrap()).await;
    let older = create_worktree_cleanup(&store, &task).await;
    fail_worktree_cleanup(&store, older).await;
    let newer = create_worktree_cleanup(&store, &task).await;
    fail_worktree_cleanup(&store, newer).await;
    let receipt_id: String = sqlx::query_scalar(
        "SELECT origin_receipt_id FROM task_cleanup_operations WHERE operation_id = ?",
    )
    .bind(older.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    sqlx::query("DROP TRIGGER task_delivery_command_receipts_no_update")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE task_delivery_command_receipts SET canonical_request_hash = ? \
         WHERE client_request_id = ?",
    )
    .bind("f".repeat(64))
    .bind(receipt_id)
    .execute(store.pool())
    .await
    .unwrap();

    assert_invariant(
        store
            .delivery_ownership_snapshot(task.id)
            .await
            .unwrap_err(),
    );
}

#[tokio::test]
async fn cleanup_history_requires_each_disposition_fact_to_precede_its_operation_transition() {
    let (store, task) = approved_task_with_ready_artifact("codex/task-cleanup-history-order").await;
    let initial = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    create_merged_delivery(&store, &task, initial.evidence_identity.as_ref().unwrap()).await;
    let operation_id = create_worktree_cleanup(&store, &task).await;
    crate::support::delivery::eligibility::complete_worktree_cleanup(&store, &task, operation_id)
        .await;

    let cleanup_v2: i64 = sqlx::query_scalar(
        "SELECT transition_id FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'cleanup_operation' AND entity_id = ? AND entity_version = 2",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    let disposition_v2: i64 = sqlx::query_scalar(
        "SELECT transition_id FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'worktree_disposition' AND entity_id = ? AND entity_version = 2",
    )
    .bind(task.id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert!(disposition_v2 < cleanup_v2);
    sqlx::query("DROP TRIGGER task_delivery_operation_transitions_no_update")
        .execute(store.pool())
        .await
        .unwrap();
    let mut transaction = store.pool().begin().await.unwrap();
    sqlx::query(
        "UPDATE task_delivery_operation_transitions SET transition_id = -1 WHERE transition_id = ?",
    )
    .bind(disposition_v2)
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_delivery_operation_transitions SET transition_id = ? WHERE transition_id = ?",
    )
    .bind(disposition_v2)
    .bind(cleanup_v2)
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_delivery_operation_transitions SET transition_id = ? WHERE transition_id = -1",
    )
    .bind(cleanup_v2)
    .execute(&mut *transaction)
    .await
    .unwrap();
    transaction.commit().await.unwrap();

    assert_invariant(
        store
            .delivery_ownership_snapshot(task.id)
            .await
            .unwrap_err(),
    );
}

#[tokio::test]
async fn cleanup_reconciliation_requires_the_exact_paired_disposition_reason() {
    let (store, task) = approved_task_with_ready_artifact("codex/task-cleanup-paired-reason").await;
    let initial = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    create_merged_delivery(&store, &task, initial.evidence_identity.as_ref().unwrap()).await;
    let operation_id = create_worktree_cleanup(&store, &task).await;
    reconcile_worktree_cleanup(&store, &task, operation_id).await;

    for drop_trigger in [
        "DROP TRIGGER task_cleanup_operations_immutable_on_update",
        "DROP TRIGGER task_cleanup_operations_transition_on_update",
        "DROP TRIGGER task_cleanup_operations_disposition_on_update",
        "DROP TRIGGER task_cleanup_operations_journal_on_update",
        "DROP TRIGGER task_delivery_operation_transitions_no_update",
    ] {
        sqlx::query(drop_trigger)
            .execute(store.pool())
            .await
            .unwrap();
    }
    sqlx::query(
        "UPDATE task_cleanup_operations SET failure_code = 'PROCESS_TREE_CLEANUP_FAILED' \
         WHERE operation_id = ?",
    )
    .bind(operation_id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_delivery_operation_transitions \
         SET failure_code = 'PROCESS_TREE_CLEANUP_FAILED' \
         WHERE entity_kind = 'cleanup_operation' AND entity_id = ? AND entity_version = 2",
    )
    .bind(operation_id.to_string())
    .execute(store.pool())
    .await
    .unwrap();

    assert_invariant(
        store
            .delivery_ownership_snapshot(task.id)
            .await
            .unwrap_err(),
    );
}

#[tokio::test]
async fn cleanup_fact_transition_requires_the_exact_paired_disposition_timestamp() {
    let (store, task) =
        approved_task_with_ready_artifact("codex/task-cleanup-paired-timestamp").await;
    let initial = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    create_merged_delivery(&store, &task, initial.evidence_identity.as_ref().unwrap()).await;
    let operation_id = create_worktree_cleanup(&store, &task).await;
    reconcile_worktree_cleanup(&store, &task, operation_id).await;

    for drop_trigger in [
        "DROP TRIGGER task_cleanup_operations_transition_on_update",
        "DROP TRIGGER task_cleanup_operations_journal_on_update",
        "DROP TRIGGER task_delivery_operation_transitions_no_update",
    ] {
        sqlx::query(drop_trigger)
            .execute(store.pool())
            .await
            .unwrap();
    }
    let changed_timestamp = "2026-08-04T00:00:01.000000000Z";
    sqlx::query("UPDATE task_cleanup_operations SET updated_at = ? WHERE operation_id = ?")
        .bind(changed_timestamp)
        .bind(operation_id.to_string())
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE task_delivery_operation_transitions SET transitioned_at = ? \
         WHERE entity_kind = 'cleanup_operation' AND entity_id = ? AND entity_version = 2",
    )
    .bind(changed_timestamp)
    .bind(operation_id.to_string())
    .execute(store.pool())
    .await
    .unwrap();

    assert_invariant(
        store
            .delivery_ownership_snapshot(task.id)
            .await
            .unwrap_err(),
    );
}

#[tokio::test]
async fn cleanup_disposition_pointer_requires_the_exact_historical_operation_version() {
    let (store, task) =
        approved_task_with_ready_artifact("codex/task-cleanup-pointer-version").await;
    let initial = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    create_merged_delivery(&store, &task, initial.evidence_identity.as_ref().unwrap()).await;
    let operation_id = create_worktree_cleanup(&store, &task).await;
    complete_worktree_cleanup(&store, &task, operation_id).await;

    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("DROP TRIGGER task_artifact_dispositions_transition_on_update")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE task_artifact_dispositions \
         SET worktree_cleanup_operation_version = 3 WHERE task_id = ?",
    )
    .bind(task.id.to_string())
    .execute(&mut *connection)
    .await
    .unwrap();
    drop(connection);

    assert_invariant(
        store
            .delivery_ownership_snapshot(task.id)
            .await
            .unwrap_err(),
    );
}

#[tokio::test]
async fn hidden_branch_failure_revalidates_the_exact_current_and_journal_matrix() {
    let (store, task) = approved_task_with_ready_artifact("codex/task-branch-failure-matrix").await;
    let initial = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    create_merged_delivery(&store, &task, initial.evidence_identity.as_ref().unwrap()).await;
    let worktree = create_worktree_cleanup(&store, &task).await;
    complete_worktree_cleanup(&store, &task, worktree).await;
    let operation_id = create_branch_cleanup(&store, &task, MERGE_COMMIT).await;
    crate::support::delivery::eligibility::fail_branch_cleanup(&store, operation_id).await;

    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA ignore_check_constraints = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
    for drop_trigger in [
        "DROP TRIGGER task_cleanup_operations_transition_on_update",
        "DROP TRIGGER task_cleanup_operations_journal_on_update",
        "DROP TRIGGER task_delivery_operation_transitions_no_update",
    ] {
        sqlx::query(drop_trigger)
            .execute(&mut *connection)
            .await
            .unwrap();
    }
    sqlx::query(
        "UPDATE task_cleanup_operations SET failure_code = 'TARGET_WORKTREE_DIRTY' \
         WHERE operation_id = ?",
    )
    .bind(operation_id.to_string())
    .execute(&mut *connection)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_delivery_operation_transitions \
         SET failure_code = 'TARGET_WORKTREE_DIRTY' \
         WHERE entity_kind = 'cleanup_operation' AND entity_id = ? AND entity_version = 2",
    )
    .bind(operation_id.to_string())
    .execute(&mut *connection)
    .await
    .unwrap();
    drop(connection);

    assert_invariant(
        store
            .delivery_ownership_snapshot(task.id)
            .await
            .unwrap_err(),
    );
}

#[tokio::test]
async fn task_scoped_reverse_audit_rejects_orphan_cleanup_receipts_and_transitions() {
    let (store, task) =
        approved_task_with_ready_artifact("codex/task-orphan-cleanup-receipt").await;
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("PRAGMA ignore_check_constraints = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("DROP TRIGGER task_delivery_command_receipts_match_operation_on_insert")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO task_delivery_command_receipts (client_request_id, command_kind, task_id, \
             repository_id, attempt, request_hash_domain, request_hash_version, \
             request_hash_algorithm, canonical_request_hash, operation_kind, operation_id, \
             merge_operation_id, cleanup_operation_id, accepted_operation_version, \
             accepted_operation_state, response_discriminator, created_at) \
         VALUES ('aaaaaaaa-1111-4111-8111-111111111111', 'remove_worktree', ?, ?, ?, \
             'coding-agent-delivery-command-request', 1, 'sha256', ?, 'merge_operation', \
             'bbbbbbbb-1111-4111-8111-111111111111', \
             'bbbbbbbb-1111-4111-8111-111111111111', NULL, 1, 'unlock_pending', \
             'worktree_cleanup_accepted', '2026-08-04T00:00:00.000000000Z')",
    )
    .bind(task.id.to_string())
    .bind(task.repository_id.to_string())
    .bind(i64::from(task.attempt))
    .bind("a".repeat(64))
    .execute(&mut *connection)
    .await
    .unwrap();
    drop(connection);
    assert_invariant(
        store
            .delivery_ownership_snapshot(task.id)
            .await
            .unwrap_err(),
    );

    let (store, task) =
        approved_task_with_ready_artifact("codex/task-orphan-cleanup-journal").await;
    sqlx::query("DROP TRIGGER task_delivery_operation_transitions_match_current")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO task_delivery_operation_transitions (entity_kind, entity_id, \
             entity_version, from_state, to_state, failure_code, transitioned_at) \
         VALUES ('cleanup_operation', 'cccccccc-1111-4111-8111-111111111111', 1, \
             'absent', 'unlock_pending', NULL, '2026-08-04T00:00:00.000000000Z')",
    )
    .execute(store.pool())
    .await
    .unwrap();
    assert_invariant(
        store
            .delivery_ownership_snapshot(task.id)
            .await
            .unwrap_err(),
    );
}

#[tokio::test]
async fn delete_cleanup_target_head_history_preserves_every_refresh_version() {
    let (store, task) = approved_task_with_ready_artifact("codex/task-delete-head-history").await;
    let initial = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    create_merged_delivery(&store, &task, initial.evidence_identity.as_ref().unwrap()).await;
    let worktree = create_worktree_cleanup(&store, &task).await;
    complete_worktree_cleanup(&store, &task, worktree).await;
    let operation_id = create_branch_cleanup(&store, &task, MERGE_COMMIT).await;
    for (version, head) in [(2_i64, "8".repeat(40)), (3_i64, "9".repeat(40))] {
        sqlx::query(
            "UPDATE task_cleanup_operations SET expected_target_head = ?, version = ?, \
                 updated_at = ? WHERE operation_id = ?",
        )
        .bind(head)
        .bind(version)
        .bind(crate::support::delivery::eligibility::DELIVERY_TIMESTAMP)
        .bind(operation_id.to_string())
        .execute(store.pool())
        .await
        .unwrap();
    }

    let snapshot = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    let cleanup = snapshot
        .cleanup_operations
        .iter()
        .find(|cleanup| cleanup.operation_id == operation_id)
        .unwrap();
    assert_eq!(cleanup.target_head_observations.len(), 3);
    assert_eq!(
        cleanup
            .target_head_at(coding_agent_store::DeliveryVersion::try_new(1).unwrap())
            .unwrap()
            .as_str(),
        MERGE_COMMIT
    );
    assert_eq!(
        cleanup
            .target_head_at(coding_agent_store::DeliveryVersion::try_new(2).unwrap())
            .unwrap()
            .as_str(),
        "8".repeat(40)
    );
    assert_eq!(
        cleanup
            .target_head_at(coding_agent_store::DeliveryVersion::try_new(3).unwrap())
            .unwrap()
            .as_str(),
        "9".repeat(40)
    );
    sqlx::query("DROP TRIGGER task_cleanup_target_head_observations_no_update")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE task_cleanup_target_head_observations SET target_head = ? \
         WHERE cleanup_operation_id = ? AND operation_version = 2",
    )
    .bind("8".repeat(64))
    .bind(operation_id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
    assert_invariant(
        store
            .delivery_ownership_snapshot(task.id)
            .await
            .unwrap_err(),
    );
}

#[tokio::test]
async fn delete_cleanup_target_head_history_fails_closed_on_missing_mismatch_and_orphan() {
    for corruption in ["missing", "head", "timestamp"] {
        let (store, task) =
            approved_task_with_ready_artifact("codex/task-delete-head-corrupt").await;
        let initial = store
            .delivery_eligibility_snapshot(task.id)
            .await
            .unwrap()
            .unwrap();
        create_merged_delivery(&store, &task, initial.evidence_identity.as_ref().unwrap()).await;
        let worktree = create_worktree_cleanup(&store, &task).await;
        complete_worktree_cleanup(&store, &task, worktree).await;
        let operation_id = create_branch_cleanup(&store, &task, MERGE_COMMIT).await;
        let drop_trigger = match corruption {
            "missing" => "DROP TRIGGER task_cleanup_target_head_observations_no_delete",
            _ => "DROP TRIGGER task_cleanup_target_head_observations_no_update",
        };
        sqlx::query(drop_trigger)
            .execute(store.pool())
            .await
            .unwrap();
        match corruption {
            "missing" => {
                sqlx::query(
                    "DELETE FROM task_cleanup_target_head_observations \
                     WHERE cleanup_operation_id = ? AND operation_version = 1",
                )
                .bind(operation_id.to_string())
                .execute(store.pool())
                .await
                .unwrap();
            }
            "head" => {
                sqlx::query(
                    "UPDATE task_cleanup_target_head_observations SET target_head = ? \
                     WHERE cleanup_operation_id = ? AND operation_version = 1",
                )
                .bind("8".repeat(40))
                .bind(operation_id.to_string())
                .execute(store.pool())
                .await
                .unwrap();
            }
            "timestamp" => {
                sqlx::query(
                    "UPDATE task_cleanup_target_head_observations \
                     SET observed_at = '2026-08-04T00:00:01.000000000Z' \
                     WHERE cleanup_operation_id = ? AND operation_version = 1",
                )
                .bind(operation_id.to_string())
                .execute(store.pool())
                .await
                .unwrap();
            }
            _ => unreachable!(),
        }
        assert_invariant(
            store
                .delivery_ownership_snapshot(task.id)
                .await
                .unwrap_err(),
        );
    }

    let (store, task) = approved_task_with_ready_artifact("codex/task-delete-head-orphan").await;
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("DROP TRIGGER task_cleanup_target_head_observations_match_current")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO task_cleanup_target_head_observations (cleanup_operation_id, \
             operation_version, target_head, observed_at) VALUES ( \
             'dddddddd-1111-4111-8111-111111111111', 1, ?, \
             '2026-08-04T00:00:00.000000000Z')",
    )
    .bind("8".repeat(40))
    .execute(&mut *connection)
    .await
    .unwrap();
    drop(connection);
    assert_invariant(
        store
            .delivery_ownership_snapshot(task.id)
            .await
            .unwrap_err(),
    );
}

#[tokio::test]
async fn duplicate_active_merge_or_cleanup_slots_fail_closed() {
    let (store, task) = approved_task_with_ready_artifact("codex/task-active-merge").await;
    insert_valid_preflight(&store, &task).await;
    sqlx::query("DROP INDEX task_merge_operations_one_active")
        .execute(store.pool())
        .await
        .unwrap();
    insert_valid_preflight(&store, &task).await;
    assert_eligibility_invariant(
        store
            .delivery_eligibility_snapshot(task.id)
            .await
            .unwrap_err(),
    );

    let (store, task) = approved_task_with_ready_artifact("codex/task-active-cleanup").await;
    let initial = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    create_merged_delivery(&store, &task, initial.evidence_identity.as_ref().unwrap()).await;
    create_worktree_cleanup(&store, &task).await;
    sqlx::query("DROP INDEX task_cleanup_operations_one_active_disposition")
        .execute(store.pool())
        .await
        .unwrap();
    create_worktree_cleanup(&store, &task).await;
    assert_invariant(
        store
            .delivery_ownership_snapshot(task.id)
            .await
            .unwrap_err(),
    );
}

async fn insert_valid_preflight(
    store: &coding_agent_store::Store,
    task: &coding_agent_domain::Task,
) -> DeliveryOperationId {
    let snapshot = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    let operation_id = DeliveryOperationId::new();
    insert_preflight(
        store,
        task,
        snapshot.evidence_identity.as_ref().unwrap(),
        operation_id,
    )
    .await;
    operation_id
}

async fn delete_initial_transition(
    store: &coding_agent_store::Store,
    entity_kind: &str,
    operation_id: DeliveryOperationId,
) {
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("DROP TRIGGER task_delivery_operation_transitions_no_delete")
        .execute(&mut *connection)
        .await
        .unwrap();
    let result = sqlx::query(
        "DELETE FROM task_delivery_operation_transitions \
         WHERE entity_kind = ? AND entity_id = ? AND entity_version = 1",
    )
    .bind(entity_kind)
    .bind(operation_id.to_string())
    .execute(&mut *connection)
    .await
    .unwrap();
    assert_eq!(result.rows_affected(), 1);
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
}

fn assert_invariant(error: StoreError) {
    assert!(matches!(error, StoreError::InvariantViolation(_)));
    assert_eq!(
        error.to_string(),
        "store invariant failed: delivery ownership snapshot is inconsistent"
    );
}

fn assert_eligibility_invariant(error: StoreError) {
    assert!(matches!(error, StoreError::InvariantViolation(_)));
    assert_eq!(
        error.to_string(),
        "store invariant failed: delivery eligibility snapshot is inconsistent"
    );
}
