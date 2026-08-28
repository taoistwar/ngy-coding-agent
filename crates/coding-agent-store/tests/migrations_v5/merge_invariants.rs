use super::{helpers::*, support};

#[tokio::test]
async fn preflight_intent_and_prepared_input_schema_contract_is_exact() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    let merge_sql = normalized_schema_sql(fixture.store.pool(), "task_merge_operations").await;
    let immutable_sql = normalized_schema_sql(
        fixture.store.pool(),
        "task_merge_operations_immutable_on_update",
    )
    .await;
    let transition_sql = normalized_schema_sql(
        fixture.store.pool(),
        "task_merge_operations_transition_on_update",
    )
    .await;
    let journal_sql =
        normalized_schema_sql(fixture.store.pool(), "task_delivery_operation_transitions").await;

    assert!(merge_sql.contains(
        "candidate_tree_oid TEXT CHECK ( candidate_tree_oid IS NULL OR ( typeof(candidate_tree_oid) = 'text'"
    ));
    assert!(merge_sql.contains(
        "preflight_source_commit_oid TEXT CHECK ( preflight_source_commit_oid IS NULL OR ( typeof(preflight_source_commit_oid) = 'text'"
    ));
    assert!(merge_sql.contains(
        "target_config_attributes_digest TEXT NOT NULL CHECK ( typeof(target_config_attributes_digest) = 'text' AND length(CAST(target_config_attributes_digest AS BLOB)) = 64 AND target_config_attributes_digest NOT GLOB '*[^0-9a-f]*' )"
    ));
    assert!(merge_sql.contains(
        "target_security_digest TEXT NOT NULL CHECK ( typeof(target_security_digest) = 'text' AND length(CAST(target_security_digest AS BLOB)) = 64 AND target_security_digest NOT GLOB '*[^0-9a-f]*' )"
    ));
    assert!(merge_sql.contains(
        "state = 'preflight_pending' AND version = 1 AND candidate_tree_oid IS NULL AND preflight_source_commit_oid IS NULL"
    ));
    assert!(merge_sql.contains(
        "state IN ('rejected', 'stale', 'reconciliation_required') AND version = 2 AND candidate_tree_oid IS NULL AND preflight_source_commit_oid IS NULL"
    ));
    assert!(merge_sql.contains(
        "candidate_tree_oid IS NOT NULL AND preflight_source_commit_oid IS NOT NULL AND (state != 'preflight_pending' OR version = 2)"
    ));
    assert!(merge_sql.contains(
        "AND NOT ( state IN ('rejected', 'stale', 'reconciliation_required') AND version = 2 )"
    ));
    for sql in [&immutable_sql, &transition_sql] {
        assert!(sql.contains(
            "OLD.state = 'preflight_pending' AND NEW.state = 'preflight_pending' AND OLD.version = 1 AND NEW.version = 2"
        ));
        assert!(sql.contains(
            "OLD.candidate_tree_oid IS NULL AND OLD.preflight_source_commit_oid IS NULL AND NEW.candidate_tree_oid IS NOT NULL AND NEW.preflight_source_commit_oid IS NOT NULL"
        ));
    }
    assert!(immutable_sql.contains(
        "NEW.target_config_attributes_digest IS NOT OLD.target_config_attributes_digest"
    ));
    assert!(immutable_sql.contains("NEW.target_security_digest IS NOT OLD.target_security_digest"));
    assert!(journal_sql.contains(
        "entity_kind = 'merge_operation' AND target_config_attributes_digest IS NOT NULL AND target_security_digest IS NOT NULL"
    ));
    assert!(journal_sql.contains(
        "from_state = 'preflight_pending' AND to_state = 'preflight_pending' AND entity_version = 2"
    ));
    assert!(
        journal_sql.contains("to_state IN ('preflight_ready', 'conflict') AND entity_version = 3")
    );
    assert!(journal_sql.contains(
        "to_state IN ('rejected', 'stale', 'reconciliation_required') AND entity_version IN (2, 3)"
    ));
}

#[tokio::test]
async fn active_terminal_and_reconciliation_merge_uniqueness_is_exact() {
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

    let competing = support::delivery::merge::create_preflight(
        fixture.store.pool(),
        parents.final_review_event_id,
        support::delivery::SECOND_MERGE_OPERATION_ID,
        "55555555-5555-4555-8555-555555555556",
    )
    .await;
    assert!(competing.is_err());
    assert_eq!(
        delivery_row_count(fixture.store.pool(), "task_merge_operations").await,
        1
    );
    assert_eq!(
        delivery_row_count(fixture.store.pool(), "task_delivery_command_receipts").await,
        1
    );

    sqlx::query(
        "UPDATE task_merge_operations
         SET state = 'reconciliation_required', failure_code = 'DELIVERY_RECONCILIATION_REQUIRED',
             version = 3, updated_at = ? WHERE operation_id = ?",
    )
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::MERGE_OPERATION_ID)
    .execute(fixture.store.pool())
    .await
    .unwrap();
    let blocked = support::delivery::merge::create_preflight(
        fixture.store.pool(),
        parents.final_review_event_id,
        support::delivery::SECOND_MERGE_OPERATION_ID,
        "55555555-5555-4555-8555-555555555556",
    )
    .await;
    assert!(blocked.is_err());

    let predicates: Vec<(String, String)> = sqlx::query_as(
        "SELECT name, sql FROM sqlite_schema
         WHERE type = 'index' AND name IN (
             'task_merge_operations_one_active',
             'task_merge_operations_one_merged',
             'task_merge_operations_abort_child_receipt_unique',
             'task_cleanup_operations_one_active_disposition'
         ) ORDER BY name",
    )
    .fetch_all(fixture.store.pool())
    .await
    .unwrap();
    let normalized = predicates
        .into_iter()
        .map(|(name, sql)| (name, sql.split_whitespace().collect::<Vec<_>>().join(" ")))
        .collect::<Vec<_>>();
    assert!(normalized.iter().any(|(name, sql)| {
        name == "task_merge_operations_one_active"
            && sql.contains("'preflight_pending', 'preflight_ready', 'accepted', 'merge_pending', 'abort_pending'")
    }));
    assert!(normalized.iter().any(|(name, sql)| {
        name == "task_merge_operations_one_merged" && sql.ends_with("WHERE state = 'merged'")
    }));
    assert!(normalized.iter().any(|(name, sql)| {
        name == "task_merge_operations_abort_child_receipt_unique"
            && sql.starts_with("CREATE UNIQUE INDEX")
            && sql.contains("ON task_merge_operations (abort_child_receipt_id)")
            && sql.ends_with("WHERE abort_child_receipt_id IS NOT NULL")
    }));
    assert!(normalized.iter().any(|(name, sql)| {
        name == "task_cleanup_operations_one_active_disposition"
            && sql.contains(
                "'unlock_pending', 'unlocked_pending_remove', 'remove_pending', 'delete_pending'",
            )
    }));
}

#[tokio::test]
async fn receipts_journal_and_conflicts_are_append_only_even_via_replace() {
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

    for sql in [
        "UPDATE task_delivery_command_receipts SET canonical_request_hash = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'",
        "DELETE FROM task_delivery_command_receipts",
        "INSERT OR REPLACE INTO task_delivery_command_receipts SELECT * FROM task_delivery_command_receipts",
        "UPDATE task_delivery_operation_transitions SET transitioned_at = '2026-08-05T00:00:00.000000000Z'",
        "DELETE FROM task_delivery_operation_transitions",
        "INSERT OR REPLACE INTO task_delivery_operation_transitions SELECT * FROM task_delivery_operation_transitions",
    ] {
        assert!(
            sqlx::raw_sql(sql)
                .execute(fixture.store.pool())
                .await
                .is_err(),
            "append-only mutation succeeded: {sql}"
        );
    }

    sqlx::query(
        "UPDATE task_merge_operations
         SET state = 'conflict', failure_code = 'MERGE_CONFLICT', version = 3,
             merge_base_oid = ?, candidate_merge_tree_oid = ?,
             conflict_path_count = 1, updated_at = ?
         WHERE operation_id = ?",
    )
    .bind(support::delivery::MERGE_BASE_OID)
    .bind(support::delivery::MERGE_TREE_OID)
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::MERGE_OPERATION_ID)
    .execute(fixture.store.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO task_merge_conflicts
             (operation_id, ordinal, path_encoding, path_value)
         VALUES (?, 0, 'utf8', 'src/lib.rs')",
    )
    .bind(support::delivery::MERGE_OPERATION_ID)
    .execute(fixture.store.pool())
    .await
    .unwrap();
    assert!(
        sqlx::query(
            "INSERT INTO task_merge_conflicts
                 (operation_id, ordinal, path_encoding, path_value)
             VALUES (?, 1, 'utf8', 'src/late.rs')",
        )
        .bind(support::delivery::MERGE_OPERATION_ID)
        .execute(fixture.store.pool())
        .await
        .is_err()
    );
    for sql in [
        "UPDATE task_merge_conflicts SET path_value = 'src/main.rs'",
        "DELETE FROM task_merge_conflicts",
        "INSERT OR REPLACE INTO task_merge_conflicts SELECT * FROM task_merge_conflicts",
    ] {
        assert!(
            sqlx::raw_sql(sql)
                .execute(fixture.store.pool())
                .await
                .is_err(),
            "conflict mutation succeeded: {sql}"
        );
    }
}

#[tokio::test]
async fn preflight_facts_are_first_set_only_by_ready_or_conflict() {
    let fixture = pending_merge_fixture().await;
    for (state, failure_code) in [
        ("rejected", "TASK_NOT_MERGE_ELIGIBLE"),
        ("stale", "TARGET_HEAD_CHANGED"),
        (
            "reconciliation_required",
            "DELIVERY_RECONCILIATION_REQUIRED",
        ),
    ] {
        assert!(
            transition_pending_merge(
                fixture.store.pool(),
                state,
                Some(failure_code),
                Some(support::delivery::MERGE_BASE_OID),
                Some(support::delivery::MERGE_TREE_OID),
            )
            .await
            .is_err(),
            "{state} must not first-set preflight result facts"
        );
    }
    assert!(
        transition_pending_merge(
            fixture.store.pool(),
            "conflict",
            Some("MERGE_CONFLICT"),
            None,
            None,
        )
        .await
        .is_err()
    );
    transition_pending_merge(
        fixture.store.pool(),
        "conflict",
        Some("MERGE_CONFLICT"),
        Some(support::delivery::MERGE_BASE_OID),
        Some(support::delivery::MERGE_TREE_OID),
    )
    .await
    .unwrap();

    let transition: (String, String, Option<String>) = sqlx::query_as(
        "SELECT from_state, to_state, failure_code
         FROM task_delivery_operation_transitions
         WHERE entity_kind = 'merge_operation' AND entity_id = ? AND entity_version = 3",
    )
    .bind(support::delivery::MERGE_OPERATION_ID)
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(
        transition,
        (
            "preflight_pending".to_owned(),
            "conflict".to_owned(),
            Some("MERGE_CONFLICT".to_owned()),
        )
    );
}

#[tokio::test]
async fn abort_child_receipt_is_globally_unique_across_merge_operations() {
    const FIRST_CHILD: &str = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
    const SECOND_CHILD: &str = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";

    let fixture = merge_pending_fixture().await;
    begin_merge_abort(
        fixture.store.pool(),
        support::delivery::MERGE_OPERATION_ID,
        FIRST_CHILD,
    )
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_merge_operations
         SET state = 'conflict', failure_code = 'MERGE_CONFLICT',
             version = 7, updated_at = ? WHERE operation_id = ?",
    )
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::MERGE_OPERATION_ID)
    .execute(fixture.store.pool())
    .await
    .unwrap();

    let final_review_event_id: i64 = sqlx::query_scalar(
        "SELECT final_review_event_id FROM task_merge_operations WHERE operation_id = ?",
    )
    .bind(support::delivery::MERGE_OPERATION_ID)
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();
    support::delivery::merge::create_preflight(
        fixture.store.pool(),
        final_review_event_id,
        support::delivery::SECOND_MERGE_OPERATION_ID,
        "55555555-5555-4555-8555-555555555556",
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
        "66666666-6666-4666-8666-666666666667",
    )
    .await
    .unwrap();
    mark_merge_pending(
        fixture.store.pool(),
        support::delivery::SECOND_MERGE_OPERATION_ID,
    )
    .await
    .unwrap();

    assert!(
        begin_merge_abort(
            fixture.store.pool(),
            support::delivery::SECOND_MERGE_OPERATION_ID,
            FIRST_CHILD,
        )
        .await
        .is_err()
    );
    begin_merge_abort(
        fixture.store.pool(),
        support::delivery::SECOND_MERGE_OPERATION_ID,
        SECOND_CHILD,
    )
    .await
    .unwrap();
    let children: Vec<String> = sqlx::query_scalar(
        "SELECT abort_child_receipt_id FROM task_merge_operations
         WHERE abort_child_receipt_id IS NOT NULL ORDER BY abort_child_receipt_id",
    )
    .fetch_all(fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(
        children,
        vec![FIRST_CHILD.to_owned(), SECOND_CHILD.to_owned()]
    );
}

#[tokio::test]
async fn conflict_path_count_schema_contract_is_exact() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    let merge_sql = normalized_schema_sql(fixture.store.pool(), "task_merge_operations").await;
    let immutable_sql = normalized_schema_sql(
        fixture.store.pool(),
        "task_merge_operations_immutable_on_update",
    )
    .await;
    let conflict_parent_sql = normalized_schema_sql(
        fixture.store.pool(),
        "task_merge_conflicts_parent_on_insert",
    )
    .await;

    assert!(merge_sql.contains(
        "conflict_path_count INTEGER CHECK ( conflict_path_count IS NULL OR ( typeof(conflict_path_count) = 'integer' AND conflict_path_count BETWEEN 0 AND 128 ) )"
    ));
    assert!(merge_sql.contains(
        "( conflict_path_count IS NOT NULL AND ( state IN ('abort_pending', 'conflict') OR ( state = 'reconciliation_required' AND abort_child_receipt_id IS NOT NULL ) ) ) OR ( conflict_path_count IS NULL AND state NOT IN ('abort_pending', 'conflict') )"
    ));
    assert!(merge_sql.contains(
        "abort_child_receipt_id IS NULL OR ( conflict_path_count IS NOT NULL AND conflict_path_count BETWEEN 1 AND 128 )"
    ));
    assert!(immutable_sql.contains(
        "OLD.conflict_path_count IS NOT NULL AND NEW.conflict_path_count IS NOT OLD.conflict_path_count"
    ));
    assert!(immutable_sql.contains(
        "OLD.conflict_path_count IS NULL AND NEW.conflict_path_count IS NOT NULL AND NOT ( (OLD.state = 'preflight_pending' AND NEW.state = 'conflict') OR (OLD.state = 'merge_pending' AND NEW.state = 'abort_pending') )"
    ));
    assert!(conflict_parent_sql.contains(
        "m.state IN ('abort_pending', 'conflict') AND m.conflict_path_count IS NOT NULL AND NEW.ordinal < m.conflict_path_count"
    ));
}

#[tokio::test]
async fn abort_pending_schema_rejects_a_zero_path_conflict_proof() {
    let fixture = merge_pending_fixture().await;
    let result = sqlx::query(
        "UPDATE task_merge_operations
         SET abort_child_receipt_id = ?, abort_merge_head_oid = source_commit_oid,
             abort_index_stages_digest = ?, abort_worktree_digest = ?,
             abort_merge_autostash_proof = 'absent', conflict_path_count = 0,
             state = 'abort_pending', failure_code = NULL, version = 6, updated_at = ?
         WHERE operation_id = ?",
    )
    .bind("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
    .bind(support::delivery::CHECKS_DIGEST)
    .bind(support::delivery::COVERAGE_DIGEST)
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::MERGE_OPERATION_ID)
    .execute(fixture.store.pool())
    .await;

    assert!(result.is_err());
    let state: (String, i64, Option<i64>) = sqlx::query_as(
        "SELECT state, version, conflict_path_count
         FROM task_merge_operations WHERE operation_id = ?",
    )
    .bind(support::delivery::MERGE_OPERATION_ID)
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(state, ("merge_pending".to_owned(), 5, None));
}

#[tokio::test]
async fn fixed_no_ff_merge_object_ids_reject_degenerate_parents_and_commit() {
    let fixture = merge_pending_fixture().await;
    assert!(
        sqlx::query(
            "UPDATE task_merge_operations SET expected_target_head = ? WHERE operation_id = ?",
        )
        .bind(support::delivery::SOURCE_COMMIT_OID)
        .bind(support::delivery::MERGE_OPERATION_ID)
        .execute(fixture.store.pool())
        .await
        .is_err(),
        "equal source and target parents were accepted"
    );
    for value in [
        support::delivery::TARGET_HEAD_OID,
        support::delivery::SOURCE_COMMIT_OID,
    ] {
        assert!(
            sqlx::query(
                "UPDATE task_merge_operations SET expected_merge_commit_oid = ? WHERE operation_id = ?",
            )
            .bind(value)
            .bind(support::delivery::MERGE_OPERATION_ID)
            .execute(fixture.store.pool())
            .await
            .is_err(),
            "merge commit equal to a parent was accepted"
        );
    }
    let merge_sql = normalized_schema_sql(fixture.store.pool(), "task_merge_operations").await;
    assert!(merge_sql.contains(
        "CHECK (source_commit_oid IS NULL OR source_commit_oid != expected_target_head)"
    ));
    assert!(merge_sql.contains(
        "expected_merge_commit_oid != expected_target_head AND ( source_commit_oid IS NULL OR expected_merge_commit_oid != source_commit_oid )"
    ));
}
