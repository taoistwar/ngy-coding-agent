use super::{helpers::*, support};

#[tokio::test]
async fn failed_merge_cannot_bind_an_object_pending_source() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    let parents =
        support::delivery::parents::seed_eligible_delivery_parents(fixture.store.pool()).await;
    support::delivery::merge::create_preflight(
        fixture.store.pool(),
        parents.final_review_event_id,
        support::delivery::MERGE_OPERATION_ID,
        support::delivery::PREFLIGHT_RECEIPT_ID,
    )
    .await
    .unwrap();
    support::delivery::merge::mark_preflight_ready(
        fixture.store.pool(),
        support::delivery::MERGE_OPERATION_ID,
    )
    .await
    .unwrap();
    support::delivery::merge::accept_merge(
        fixture.store.pool(),
        support::delivery::MERGE_OPERATION_ID,
    )
    .await
    .unwrap();
    support::delivery::merge::create_source_object_pending(fixture.store.pool())
        .await
        .unwrap();

    let result = sqlx::query(
        "UPDATE task_merge_operations
         SET delivery_source_task_id = ?, source_commit_oid = ?,
             state = 'failed', failure_code = 'TARGET_HEAD_CHANGED',
             version = 5, updated_at = ?
         WHERE operation_id = ?",
    )
    .bind(support::delivery::TASK_ID)
    .bind(support::delivery::SOURCE_COMMIT_OID)
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::MERGE_OPERATION_ID)
    .execute(fixture.store.pool())
    .await;
    assert!(result.is_err());

    let states: (String, String) = sqlx::query_as(
        "SELECT m.state, s.state
         FROM task_merge_operations m JOIN task_delivery_sources s ON s.task_id = m.task_id
         WHERE m.operation_id = ?",
    )
    .bind(support::delivery::MERGE_OPERATION_ID)
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(states, ("accepted".to_owned(), "object_pending".to_owned()));
}

#[tokio::test]
async fn failure_codes_are_coupled_to_failed_reconciliation_and_success_states() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    let parents =
        support::delivery::parents::seed_eligible_delivery_parents(fixture.store.pool()).await;
    support::delivery::merge::create_preflight(
        fixture.store.pool(),
        parents.final_review_event_id,
        support::delivery::MERGE_OPERATION_ID,
        support::delivery::PREFLIGHT_RECEIPT_ID,
    )
    .await
    .unwrap();
    let missing_merge_failure = sqlx::query(
        "UPDATE task_merge_operations
         SET state = 'reconciliation_required', version = 3, updated_at = ?
         WHERE operation_id = ?",
    )
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::MERGE_OPERATION_ID)
    .execute(fixture.store.pool())
    .await;
    assert!(missing_merge_failure.is_err());

    let cleanup_fixture = support::file_store().await;
    cleanup_fixture.store.migrate().await.unwrap();
    support::delivery::merge::seed_merged_delivery(cleanup_fixture.store.pool())
        .await
        .unwrap();
    support::delivery::cleanup::create_remove_cleanup(
        cleanup_fixture.store.pool(),
        support::delivery::CLEANUP_OPERATION_ID,
        support::delivery::CLEANUP_RECEIPT_ID,
    )
    .await
    .unwrap();
    let missing_cleanup_failure = sqlx::query(
        "UPDATE task_cleanup_operations
         SET state = 'failed', version = 2, updated_at = ?
         WHERE operation_id = ?",
    )
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::CLEANUP_OPERATION_ID)
    .execute(cleanup_fixture.store.pool())
    .await;
    assert!(missing_cleanup_failure.is_err());
    let unstable_cleanup_failure = sqlx::query(
        "UPDATE task_cleanup_operations
         SET state = 'failed', failure_code = 'REMOVE_NOT_APPLIED',
             version = 2, updated_at = ? WHERE operation_id = ?",
    )
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::CLEANUP_OPERATION_ID)
    .execute(cleanup_fixture.store.pool())
    .await;
    assert!(unstable_cleanup_failure.is_err());
    sqlx::query(
        "UPDATE task_cleanup_operations
         SET state = 'failed', failure_code = 'COMMAND_TIMED_OUT',
             version = 2, updated_at = ? WHERE operation_id = ?",
    )
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::CLEANUP_OPERATION_ID)
    .execute(cleanup_fixture.store.pool())
    .await
    .unwrap();
}

#[tokio::test]
async fn branch_timeout_known_not_applied_is_failed_in_current_row_and_journal() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    support::delivery::merge::seed_merged_delivery(fixture.store.pool())
        .await
        .unwrap();
    support::delivery::cleanup::advance_disposition_to_removed(fixture.store.pool())
        .await
        .unwrap();
    support::delivery::cleanup::create_delete_cleanup(fixture.store.pool())
        .await
        .unwrap();

    let cross_kind_failure = sqlx::query(
        "UPDATE task_cleanup_operations
         SET state = 'failed', failure_code = 'TARGET_WORKTREE_DIRTY',
             version = 2, updated_at = ? WHERE operation_id = ?",
    )
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::DELETE_CLEANUP_OPERATION_ID)
    .execute(fixture.store.pool())
    .await;
    assert!(cross_kind_failure.is_err());

    sqlx::query(
        "UPDATE task_cleanup_operations
         SET state = 'failed', failure_code = 'COMMAND_TIMED_OUT',
             version = 2, updated_at = ? WHERE operation_id = ?",
    )
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::DELETE_CLEANUP_OPERATION_ID)
    .execute(fixture.store.pool())
    .await
    .unwrap();

    let current: (String, String, i64) = sqlx::query_as(
        "SELECT state, failure_code, expected_disposition_version
         FROM task_cleanup_operations WHERE operation_id = ?",
    )
    .bind(support::delivery::DELETE_CLEANUP_OPERATION_ID)
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(
        current,
        ("failed".to_owned(), "COMMAND_TIMED_OUT".to_owned(), 1)
    );

    let journal: (String, String, String, i64) = sqlx::query_as(
        "SELECT from_state, to_state, failure_code, entity_version
         FROM task_delivery_operation_transitions
         WHERE entity_kind = 'cleanup_operation' AND entity_id = ? AND entity_version = 2",
    )
    .bind(support::delivery::DELETE_CLEANUP_OPERATION_ID)
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(
        journal,
        (
            "delete_pending".to_owned(),
            "failed".to_owned(),
            "COMMAND_TIMED_OUT".to_owned(),
            2,
        )
    );
}

#[tokio::test]
async fn source_and_merge_current_and_journal_failure_contracts_are_exact() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    let source_sql = normalized_schema_sql(fixture.store.pool(), "task_delivery_sources").await;
    let merge_sql = normalized_schema_sql(fixture.store.pool(), "task_merge_operations").await;
    let journal_sql =
        normalized_schema_sql(fixture.store.pool(), "task_delivery_operation_transitions").await;

    assert!(source_sql.contains(
        "state IN ('object_pending', 'commit_pending') AND (failure_code IS NULL OR failure_code = 'COMMAND_TIMED_OUT')"
    ));
    assert!(source_sql.contains(
        "state = 'reconciliation_required' AND failure_code IS NOT NULL AND failure_code IN ( 'DELIVERY_SOURCE_INCONSISTENT', 'PROCESS_TREE_CLEANUP_FAILED' )"
    ));
    for expected in [
        "from_state = 'absent' AND to_state = 'object_pending' AND failure_code IS NULL",
        "from_state = 'object_pending' AND to_state = 'object_pending' AND failure_code IS 'COMMAND_TIMED_OUT'",
        "from_state = 'object_pending' AND to_state = 'commit_pending' AND failure_code IS NULL",
        "from_state = 'commit_pending' AND to_state = 'commit_pending' AND failure_code IS 'COMMAND_TIMED_OUT'",
        "from_state = 'commit_pending' AND to_state = 'committed' AND failure_code IS NULL",
        "from_state IN ('object_pending', 'commit_pending', 'committed') AND to_state = 'reconciliation_required' AND failure_code IS NOT NULL AND failure_code IN ( 'DELIVERY_SOURCE_INCONSISTENT', 'PROCESS_TREE_CLEANUP_FAILED' )",
    ] {
        assert!(
            journal_sql.contains(expected),
            "missing source journal clause: {expected}"
        );
    }
    assert_merge_failure_contract(&merge_sql, "state");
    assert_merge_failure_contract(&journal_sql, "to_state");
}

#[tokio::test]
async fn accepted_to_failed_rejects_a_cross_state_code_and_journals_a_legal_code() {
    let fixture = accepted_committed_source_fixture().await;
    assert!(
        transition_accepted_merge_to_failed(fixture.store.pool(), "MERGE_CONFLICT")
            .await
            .is_err()
    );
    transition_accepted_merge_to_failed(fixture.store.pool(), "TARGET_HEAD_CHANGED")
        .await
        .unwrap();
    let transition: (String, String, Option<String>) = sqlx::query_as(
        "SELECT from_state, to_state, failure_code
         FROM task_delivery_operation_transitions
         WHERE entity_kind = 'merge_operation' AND entity_id = ? AND entity_version = 5",
    )
    .bind(support::delivery::MERGE_OPERATION_ID)
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(
        transition,
        (
            "accepted".to_owned(),
            "failed".to_owned(),
            Some("TARGET_HEAD_CHANGED".to_owned()),
        )
    );
}

#[tokio::test]
async fn delivery_source_current_failure_allowlist_is_exact() {
    let fixture = object_pending_source_fixture().await;
    assert!(
        transition_delivery_source(
            fixture.store.pool(),
            "object_pending",
            Some("TARGET_HEAD_CHANGED"),
            2,
            None,
        )
        .await
        .is_err()
    );
    transition_delivery_source(
        fixture.store.pool(),
        "object_pending",
        Some("COMMAND_TIMED_OUT"),
        2,
        None,
    )
    .await
    .unwrap();
    let mut invalid_reconciliation = fixture.store.pool().begin().await.unwrap();
    assert!(
        sqlx::query(
            "UPDATE task_delivery_sources
             SET state = 'reconciliation_required',
                 failure_code = 'DELIVERY_RECONCILIATION_REQUIRED',
                 version = 3, updated_at = ?
             WHERE task_id = ?",
        )
        .bind(support::delivery::TIMESTAMP)
        .bind(support::delivery::TASK_ID)
        .execute(&mut *invalid_reconciliation)
        .await
        .is_err()
    );
    invalid_reconciliation.rollback().await.unwrap();
}

#[tokio::test]
async fn delivery_source_transition_journal_failure_matrix_is_exact() {
    let fixture = object_pending_source_fixture().await;
    assert!(
        transition_delivery_source(fixture.store.pool(), "object_pending", None, 2, None)
            .await
            .is_err()
    );
    assert!(
        transition_delivery_source(
            fixture.store.pool(),
            "commit_pending",
            Some("COMMAND_TIMED_OUT"),
            2,
            Some(support::delivery::SOURCE_COMMIT_OID),
        )
        .await
        .is_err()
    );
    transition_delivery_source(
        fixture.store.pool(),
        "commit_pending",
        None,
        2,
        Some(support::delivery::SOURCE_COMMIT_OID),
    )
    .await
    .unwrap();
    assert!(
        transition_delivery_source(fixture.store.pool(), "commit_pending", None, 3, None)
            .await
            .is_err()
    );
    transition_delivery_source(
        fixture.store.pool(),
        "commit_pending",
        Some("COMMAND_TIMED_OUT"),
        3,
        None,
    )
    .await
    .unwrap();
    assert!(
        transition_delivery_source(
            fixture.store.pool(),
            "committed",
            Some("COMMAND_TIMED_OUT"),
            4,
            None,
        )
        .await
        .is_err()
    );
    transition_delivery_source(fixture.store.pool(), "committed", None, 4, None)
        .await
        .unwrap();

    let transitions: Vec<(i64, String, String, Option<String>)> = sqlx::query_as(
        "SELECT entity_version, from_state, to_state, failure_code
         FROM task_delivery_operation_transitions
         WHERE entity_kind = 'delivery_source' AND entity_id = ?
         ORDER BY entity_version",
    )
    .bind(support::delivery::TASK_ID)
    .fetch_all(fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(
        transitions,
        vec![
            (1, "absent".to_owned(), "object_pending".to_owned(), None),
            (
                2,
                "object_pending".to_owned(),
                "commit_pending".to_owned(),
                None,
            ),
            (
                3,
                "commit_pending".to_owned(),
                "commit_pending".to_owned(),
                Some("COMMAND_TIMED_OUT".to_owned()),
            ),
            (4, "commit_pending".to_owned(), "committed".to_owned(), None),
        ]
    );
}

#[tokio::test]
async fn accepted_merge_and_pending_source_reconcile_only_as_one_transaction() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    let parents =
        support::delivery::parents::seed_eligible_delivery_parents(fixture.store.pool()).await;
    support::delivery::merge::create_preflight(
        fixture.store.pool(),
        parents.final_review_event_id,
        support::delivery::MERGE_OPERATION_ID,
        support::delivery::PREFLIGHT_RECEIPT_ID,
    )
    .await
    .unwrap();
    support::delivery::merge::mark_preflight_ready(
        fixture.store.pool(),
        support::delivery::MERGE_OPERATION_ID,
    )
    .await
    .unwrap();
    support::delivery::merge::accept_merge(
        fixture.store.pool(),
        support::delivery::MERGE_OPERATION_ID,
    )
    .await
    .unwrap();
    support::delivery::merge::create_source_object_pending(fixture.store.pool())
        .await
        .unwrap();

    let source_only = sqlx::query(
        "UPDATE task_delivery_sources
         SET state = 'reconciliation_required', failure_code = 'DELIVERY_SOURCE_INCONSISTENT',
             version = 2, updated_at = ? WHERE task_id = ?",
    )
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::TASK_ID)
    .execute(fixture.store.pool())
    .await;
    assert!(source_only.is_err());
    let merge_only = sqlx::query(
        "UPDATE task_merge_operations
         SET state = 'reconciliation_required', failure_code = 'DELIVERY_SOURCE_INCONSISTENT',
             version = 5, updated_at = ? WHERE operation_id = ?",
    )
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::MERGE_OPERATION_ID)
    .execute(fixture.store.pool())
    .await;
    assert!(merge_only.is_err());

    let mut transaction = fixture.store.pool().begin().await.unwrap();
    sqlx::query(
        "UPDATE task_delivery_sources
         SET state = 'reconciliation_required', failure_code = 'DELIVERY_SOURCE_INCONSISTENT',
             version = 2, updated_at = ? WHERE task_id = ?",
    )
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::TASK_ID)
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_merge_operations
         SET state = 'reconciliation_required', failure_code = 'DELIVERY_SOURCE_INCONSISTENT',
             version = 5, updated_at = ? WHERE operation_id = ?",
    )
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::MERGE_OPERATION_ID)
    .execute(&mut *transaction)
    .await
    .unwrap();
    transaction.commit().await.unwrap();

    let states: (String, String) = sqlx::query_as(
        "SELECT m.state, s.state
         FROM task_merge_operations m JOIN task_delivery_sources s ON s.task_id = m.task_id
         WHERE m.operation_id = ?",
    )
    .bind(support::delivery::MERGE_OPERATION_ID)
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(
        states,
        (
            "reconciliation_required".to_owned(),
            "reconciliation_required".to_owned()
        )
    );
}

#[tokio::test]
async fn historical_failed_merge_does_not_block_a_new_paired_reconciliation() {
    const NEW_PREFLIGHT_RECEIPT: &str = "55555555-5555-4555-8555-555555555556";
    const NEW_ACCEPT_RECEIPT: &str = "66666666-6666-4666-8666-666666666667";
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    let parents =
        support::delivery::parents::seed_eligible_delivery_parents(fixture.store.pool()).await;
    support::delivery::merge::create_preflight(
        fixture.store.pool(),
        parents.final_review_event_id,
        support::delivery::MERGE_OPERATION_ID,
        support::delivery::PREFLIGHT_RECEIPT_ID,
    )
    .await
    .unwrap();
    support::delivery::merge::mark_preflight_ready(
        fixture.store.pool(),
        support::delivery::MERGE_OPERATION_ID,
    )
    .await
    .unwrap();
    support::delivery::merge::accept_merge(
        fixture.store.pool(),
        support::delivery::MERGE_OPERATION_ID,
    )
    .await
    .unwrap();
    support::delivery::merge::create_committed_source(fixture.store.pool())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE task_merge_operations
         SET delivery_source_task_id = ?, source_commit_oid = ?,
             state = 'failed', failure_code = 'TARGET_HEAD_CHANGED',
             version = 5, updated_at = ? WHERE operation_id = ?",
    )
    .bind(support::delivery::TASK_ID)
    .bind(support::delivery::SOURCE_COMMIT_OID)
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::MERGE_OPERATION_ID)
    .execute(fixture.store.pool())
    .await
    .unwrap();

    support::delivery::merge::create_preflight(
        fixture.store.pool(),
        parents.final_review_event_id,
        support::delivery::SECOND_MERGE_OPERATION_ID,
        NEW_PREFLIGHT_RECEIPT,
    )
    .await
    .unwrap();
    support::delivery::merge::mark_preflight_ready(
        fixture.store.pool(),
        support::delivery::SECOND_MERGE_OPERATION_ID,
    )
    .await
    .unwrap();
    support::delivery::merge::accept_merge_with_receipt(
        fixture.store.pool(),
        support::delivery::SECOND_MERGE_OPERATION_ID,
        NEW_ACCEPT_RECEIPT,
    )
    .await
    .unwrap();

    let mut transaction = fixture.store.pool().begin().await.unwrap();
    sqlx::query(
        "UPDATE task_delivery_sources
         SET state = 'reconciliation_required', failure_code = 'DELIVERY_SOURCE_INCONSISTENT',
             version = 4, updated_at = ? WHERE task_id = ?",
    )
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::TASK_ID)
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_merge_operations
         SET state = 'reconciliation_required', failure_code = 'DELIVERY_SOURCE_INCONSISTENT',
             version = 5, updated_at = ? WHERE operation_id = ?",
    )
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::SECOND_MERGE_OPERATION_ID)
    .execute(&mut *transaction)
    .await
    .unwrap();
    transaction.commit().await.unwrap();

    let operations: Vec<(String, String)> = sqlx::query_as(
        "SELECT operation_id, state FROM task_merge_operations ORDER BY operation_id",
    )
    .fetch_all(fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(
        operations,
        vec![
            (
                support::delivery::MERGE_OPERATION_ID.to_owned(),
                "failed".to_owned()
            ),
            (
                support::delivery::SECOND_MERGE_OPERATION_ID.to_owned(),
                "reconciliation_required".to_owned()
            )
        ]
    );
}

#[tokio::test]
async fn merged_source_ownership_cannot_be_reclassified_as_source_reconciliation() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    support::delivery::merge::seed_merged_delivery(fixture.store.pool())
        .await
        .unwrap();

    let result = sqlx::query(
        "UPDATE task_delivery_sources
         SET state = 'reconciliation_required', failure_code = 'DELIVERY_SOURCE_INCONSISTENT',
             version = 4, updated_at = ? WHERE task_id = ?",
    )
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::TASK_ID)
    .execute(fixture.store.pool())
    .await;

    assert!(result.is_err());
    let state: String =
        sqlx::query_scalar("SELECT state FROM task_delivery_sources WHERE task_id = ?")
            .bind(support::delivery::TASK_ID)
            .fetch_one(fixture.store.pool())
            .await
            .unwrap();
    assert_eq!(state, "committed");
}
