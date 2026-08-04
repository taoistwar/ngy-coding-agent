use crate::corruption_cases::assert_recovery_invariant;
use crate::recovery_fixtures::pending_preflight;
use crate::support::delivery::eligibility::COMMON_IDENTITY;

#[tokio::test]
async fn every_delivery_transition_entity_kind_rejects_an_orphan_journal_row() {
    for (entity_kind, from_state, to_state) in [
        ("delivery_source", "absent", "object_pending"),
        ("merge_operation", "absent", "preflight_pending"),
        ("cleanup_operation", "absent", "unlock_pending"),
        ("worktree_disposition", "absent", "retained_locked"),
        ("branch_disposition", "absent", "retained"),
    ] {
        let store = crate::support::seeded_store().await;
        sqlx::query("DROP TRIGGER task_delivery_operation_transitions_match_current")
            .execute(store.pool())
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO task_delivery_operation_transitions ( \
                 entity_kind, entity_id, entity_version, from_state, to_state, \
                 failure_code, transitioned_at \
             ) VALUES (?, 'aaaaaaaa-1111-4111-8111-111111111111', 1, ?, ?, NULL, \
                 '2026-08-04T00:00:00.000000000Z')",
        )
        .bind(entity_kind)
        .bind(from_state)
        .bind(to_state)
        .execute(store.pool())
        .await
        .unwrap();
        assert_recovery_invariant(&store).await;
    }
}

#[tokio::test]
async fn orphan_command_receipt_is_a_global_failure_without_a_current_operation() {
    let store = crate::support::seeded_store().await;
    let task = crate::support::queued_task(&store).await;
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
    sqlx::query("DROP TRIGGER task_delivery_command_receipts_no_replace")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO task_delivery_command_receipts ( \
             client_request_id, command_kind, task_id, repository_id, attempt, \
             request_hash_domain, request_hash_version, request_hash_algorithm, \
             canonical_request_hash, operation_kind, operation_id, merge_operation_id, \
             cleanup_operation_id, accepted_operation_version, accepted_operation_state, \
             response_discriminator, created_at \
         ) VALUES ( \
             'bbbbbbbb-1111-4111-8111-111111111111', 'preflight', ?, ?, ?, \
             'coding-agent-delivery-command-request', 1, 'sha256', ?, 'merge_operation', \
             'cccccccc-1111-4111-8111-111111111111', \
             'cccccccc-1111-4111-8111-111111111111', NULL, 1, 'preflight_pending', \
             'preflight_created', '2026-08-04T00:00:00.000000000Z')",
    )
    .bind(task.id.to_string())
    .bind(task.repository_id.to_string())
    .bind(i64::from(task.attempt))
    .bind("a".repeat(64))
    .execute(&mut *connection)
    .await
    .unwrap();
    drop(connection);
    assert_recovery_invariant(&store).await;
}

#[tokio::test]
async fn extra_receipt_for_an_existing_operation_fails_reverse_mapping_audit() {
    let store = crate::support::seeded_store().await;
    let (task, operation_id) =
        pending_preflight(&store, "codex/recovery-extra-receipt", COMMON_IDENTITY).await;
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("DROP TRIGGER task_delivery_command_receipts_match_operation_on_insert")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("DROP TRIGGER task_delivery_command_receipts_no_replace")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO task_delivery_command_receipts ( \
             client_request_id, command_kind, task_id, repository_id, attempt, \
             request_hash_domain, request_hash_version, request_hash_algorithm, \
             canonical_request_hash, operation_kind, operation_id, merge_operation_id, \
             cleanup_operation_id, accepted_operation_version, accepted_operation_state, \
             response_discriminator, created_at \
         ) VALUES ( \
             'eeeeeeee-1111-4111-8111-111111111111', 'accept_merge', ?, ?, ?, \
             'coding-agent-delivery-command-request', 1, 'sha256', ?, 'merge_operation', \
             ?, ?, NULL, 2, 'accepted', 'merge_accepted', \
             '2026-08-04T00:00:00.000000000Z')",
    )
    .bind(task.id.to_string())
    .bind(task.repository_id.to_string())
    .bind(i64::from(task.attempt))
    .bind("b".repeat(64))
    .bind(operation_id.to_string())
    .bind(operation_id.to_string())
    .execute(&mut *connection)
    .await
    .unwrap();
    drop(connection);
    assert_recovery_invariant(&store).await;
}
