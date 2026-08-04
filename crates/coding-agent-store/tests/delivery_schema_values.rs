mod support;

const SHA256_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

#[tokio::test]
async fn raw_sql_rejects_noncanonical_values_or_orphaned_delivery_facts() {
    const ZERO_SHA1: &str = "0000000000000000000000000000000000000000";
    let store = support::memory_store().await;
    for table in [
        "task_delivery_sources",
        "task_merge_operations",
        "task_merge_conflicts",
        "task_artifact_dispositions",
        "task_cleanup_operations",
        "task_delivery_command_receipts",
        "task_delivery_operation_transitions",
    ] {
        let strict: i64 = sqlx::query_scalar(
            "SELECT strict FROM pragma_table_list
             WHERE schema = 'main' AND type = 'table' AND name = ?",
        )
        .bind(table)
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(strict, 1, "{table} is not STRICT");
    }

    let parents = support::delivery::parents::seed_eligible_delivery_parents(store.pool()).await;
    let zero_oid = support::delivery::merge::create_preflight_with_candidate_tree(
        store.pool(),
        parents.final_review_event_id,
        support::delivery::MERGE_OPERATION_ID,
        support::delivery::PREFLIGHT_RECEIPT_ID,
        ZERO_SHA1,
    )
    .await;
    assert!(zero_oid.is_err());
    let invalid_branch = support::delivery::merge::create_preflight_with_target_branch(
        store.pool(),
        parents.final_review_event_id,
        support::delivery::MERGE_OPERATION_ID,
        support::delivery::PREFLIGHT_RECEIPT_ID,
        "refs/heads/../main",
    )
    .await;
    assert!(invalid_branch.is_err());
    let control_branch = support::delivery::merge::create_preflight_with_target_branch(
        store.pool(),
        parents.final_review_event_id,
        support::delivery::MERGE_OPERATION_ID,
        support::delivery::PREFLIGHT_RECEIPT_ID,
        "refs/heads/main\nother",
    )
    .await;
    assert!(control_branch.is_err());
    let empty_counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT
             (SELECT COUNT(*) FROM task_merge_operations),
             (SELECT COUNT(*) FROM task_delivery_command_receipts),
             (SELECT COUNT(*) FROM task_delivery_operation_transitions)",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(empty_counts, (0, 0, 0));

    support::delivery::merge::create_preflight(
        store.pool(),
        parents.final_review_event_id,
        support::delivery::MERGE_OPERATION_ID,
        support::delivery::PREFLIGHT_RECEIPT_ID,
    )
    .await
    .unwrap();
    let alphabetic_timestamp = sqlx::query(
        "UPDATE task_merge_operations
         SET state = 'conflict', version = 2,
             updated_at = 'aaaa-aa-aaTaa:aa:aa.aaaaaaaaaZ'
         WHERE operation_id = ?",
    )
    .bind(support::delivery::MERGE_OPERATION_ID)
    .execute(store.pool())
    .await;
    assert!(alphabetic_timestamp.is_err());
    let immutable_identity = sqlx::query(
        "UPDATE task_merge_operations
         SET workspace_fingerprint = ? WHERE operation_id = ?",
    )
    .bind(SHA256_B)
    .bind(support::delivery::MERGE_OPERATION_ID)
    .execute(store.pool())
    .await;
    assert!(immutable_identity.is_err());
    let orphan_journal = sqlx::query(
        "INSERT INTO task_delivery_operation_transitions (
             entity_kind, entity_id, entity_version, from_state, to_state,
             failure_code, transitioned_at
         ) VALUES (
             'cleanup_operation', ?, 1, 'absent', 'remove_pending', NULL, ?
         )",
    )
    .bind(support::delivery::CLEANUP_OPERATION_ID)
    .bind(support::delivery::TIMESTAMP)
    .execute(store.pool())
    .await;
    assert!(orphan_journal.is_err());

    sqlx::query(
        "UPDATE task_merge_operations
         SET state = 'conflict', failure_code = 'MERGE_CONFLICT',
             merge_base_oid = ?, candidate_merge_tree_oid = ?,
             conflict_path_count = 1, version = 2, updated_at = ?
         WHERE operation_id = ?",
    )
    .bind(support::delivery::MERGE_BASE_OID)
    .bind(support::delivery::MERGE_TREE_OID)
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::MERGE_OPERATION_ID)
    .execute(store.pool())
    .await
    .unwrap();
    let invalid_base64url = sqlx::query(
        "INSERT INTO task_merge_conflicts
             (operation_id, ordinal, path_encoding, path_value)
         VALUES (?, 0, 'base64url', '***')",
    )
    .bind(support::delivery::MERGE_OPERATION_ID)
    .execute(store.pool())
    .await;
    assert!(invalid_base64url.is_err());
    let oversized_path = sqlx::query(
        "INSERT INTO task_merge_conflicts
             (operation_id, ordinal, path_encoding, path_value)
         VALUES (?, 0, 'utf8', ?)",
    )
    .bind(support::delivery::MERGE_OPERATION_ID)
    .bind("x".repeat(4_097))
    .execute(store.pool())
    .await;
    assert!(oversized_path.is_err());
}

#[tokio::test]
async fn raw_sql_rejects_embedded_nul_across_canonical_text_classes() {
    enum CorruptedField {
        OperationId,
        ReceiptId,
        GitOid,
        Digest,
        BranchRef,
        WorktreePath,
        ReceiptHash,
    }

    let cases = [
        (
            CorruptedField::OperationId,
            format!("{}\0evil", support::delivery::MERGE_OPERATION_ID),
        ),
        (
            CorruptedField::ReceiptId,
            format!("{}\0evil", support::delivery::PREFLIGHT_RECEIPT_ID),
        ),
        (
            CorruptedField::GitOid,
            format!("{}\0evil", support::delivery::CANDIDATE_TREE_OID),
        ),
        (
            CorruptedField::Digest,
            format!("{}\0evil", support::delivery::CONFIG_DIGEST),
        ),
        (
            CorruptedField::BranchRef,
            format!("{}\0evil", support::delivery::TARGET_BRANCH),
        ),
        (
            CorruptedField::WorktreePath,
            format!("{}\0evil", support::delivery::WORKTREE_PATH),
        ),
        (
            CorruptedField::ReceiptHash,
            format!("{}\0evil", support::delivery::REQUEST_HASH),
        ),
    ];

    for (field, corrupted) in cases {
        let store = support::memory_store().await;
        let parents =
            support::delivery::parents::seed_eligible_delivery_parents(store.pool()).await;
        let mut fixture = support::delivery::PreflightFixture::valid(
            support::delivery::MERGE_OPERATION_ID,
            support::delivery::PREFLIGHT_RECEIPT_ID,
        );
        match field {
            CorruptedField::OperationId => fixture.operation_id = &corrupted,
            CorruptedField::ReceiptId => fixture.receipt_id = &corrupted,
            CorruptedField::GitOid => fixture.candidate_tree_oid = &corrupted,
            CorruptedField::Digest => fixture.config_attributes_digest = &corrupted,
            CorruptedField::BranchRef => fixture.target_branch = corrupted.as_str().into(),
            CorruptedField::WorktreePath => fixture.artifact_worktree_path = &corrupted,
            CorruptedField::ReceiptHash => fixture.request_hash = &corrupted,
        }

        let result = support::delivery::merge::create_preflight_with_fixture(
            store.pool(),
            parents.final_review_event_id,
            fixture,
        )
        .await;
        assert!(
            result.is_err(),
            "embedded NUL reached a v5 canonical TEXT field"
        );
    }

    let store = support::memory_store().await;
    let parents = support::delivery::parents::seed_eligible_delivery_parents(store.pool()).await;
    support::delivery::merge::create_preflight(
        store.pool(),
        parents.final_review_event_id,
        support::delivery::MERGE_OPERATION_ID,
        support::delivery::PREFLIGHT_RECEIPT_ID,
    )
    .await
    .unwrap();
    support::delivery::merge::mark_preflight_ready(
        store.pool(),
        support::delivery::MERGE_OPERATION_ID,
    )
    .await
    .unwrap();
    support::delivery::merge::accept_merge(store.pool(), support::delivery::MERGE_OPERATION_ID)
        .await
        .unwrap();
    assert!(
        support::delivery::merge::create_source_object_pending_with_date_bytes(
            store.pool(),
            "1785772800\0 +0000",
        )
        .await
        .is_err(),
        "embedded NUL reached Git commit date bytes"
    );

    let failure_store = support::memory_store().await;
    let parents =
        support::delivery::parents::seed_eligible_delivery_parents(failure_store.pool()).await;
    support::delivery::merge::create_preflight(
        failure_store.pool(),
        parents.final_review_event_id,
        support::delivery::MERGE_OPERATION_ID,
        support::delivery::PREFLIGHT_RECEIPT_ID,
    )
    .await
    .unwrap();
    let nul_failure = sqlx::query(
        "UPDATE task_merge_operations
         SET state = 'reconciliation_required', failure_code = ?,
             version = 2, updated_at = ?
         WHERE operation_id = ?",
    )
    .bind("SOURCE_UNKNOWN\0EVIL")
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::MERGE_OPERATION_ID)
    .execute(failure_store.pool())
    .await;
    assert!(
        nul_failure.is_err(),
        "embedded NUL reached current/journal failure codes"
    );

    for (encoding, value) in [("utf8", "src/lib.rs\0evil"), ("base64url", "c3Jj\0evil")] {
        let conflict_store = support::memory_store().await;
        let parents =
            support::delivery::parents::seed_eligible_delivery_parents(conflict_store.pool()).await;
        support::delivery::merge::create_preflight(
            conflict_store.pool(),
            parents.final_review_event_id,
            support::delivery::MERGE_OPERATION_ID,
            support::delivery::PREFLIGHT_RECEIPT_ID,
        )
        .await
        .unwrap();
        sqlx::query(
            "UPDATE task_merge_operations
             SET state = 'conflict', failure_code = 'MERGE_CONFLICT',
                 merge_base_oid = ?, candidate_merge_tree_oid = ?,
                 conflict_path_count = 1, version = 2, updated_at = ?
             WHERE operation_id = ?",
        )
        .bind(support::delivery::MERGE_BASE_OID)
        .bind(support::delivery::MERGE_TREE_OID)
        .bind(support::delivery::TIMESTAMP)
        .bind(support::delivery::MERGE_OPERATION_ID)
        .execute(conflict_store.pool())
        .await
        .unwrap();
        let result = sqlx::query(
            "INSERT INTO task_merge_conflicts
                 (operation_id, ordinal, path_encoding, path_value)
             VALUES (?, 0, ?, ?)",
        )
        .bind(support::delivery::MERGE_OPERATION_ID)
        .bind(encoding)
        .bind(value)
        .execute(conflict_store.pool())
        .await;
        assert!(
            result.is_err(),
            "embedded NUL reached a {encoding} conflict path"
        );
    }
}

#[tokio::test]
async fn raw_sql_rejects_invalid_utf8_at_delivery_text_boundaries() {
    let unicode_store = support::memory_store().await;
    let parents =
        support::delivery::parents::seed_eligible_delivery_parents(unicode_store.pool()).await;
    let mut unicode_fixture = support::delivery::PreflightFixture::valid(
        support::delivery::MERGE_OPERATION_ID,
        support::delivery::PREFLIGHT_RECEIPT_ID,
    );
    unicode_fixture.target_branch = "refs/heads/功能".into();
    support::delivery::merge::create_preflight_with_fixture(
        unicode_store.pool(),
        parents.final_review_event_id,
        unicode_fixture,
    )
    .await
    .unwrap();

    let store = support::memory_store().await;
    let parents = support::delivery::parents::seed_eligible_delivery_parents(store.pool()).await;
    let mut fixture = support::delivery::PreflightFixture::valid(
        support::delivery::MERGE_OPERATION_ID,
        support::delivery::PREFLIGHT_RECEIPT_ID,
    );
    fixture.target_branch = support::delivery::SqlTextFixture::RawBytes(b"refs/heads/main\xff");

    let result = support::delivery::merge::create_preflight_with_fixture(
        store.pool(),
        parents.final_review_event_id,
        fixture,
    )
    .await;
    assert!(result.is_err());

    let conflict_store = support::memory_store().await;
    let parents =
        support::delivery::parents::seed_eligible_delivery_parents(conflict_store.pool()).await;
    support::delivery::merge::create_preflight(
        conflict_store.pool(),
        parents.final_review_event_id,
        support::delivery::MERGE_OPERATION_ID,
        support::delivery::PREFLIGHT_RECEIPT_ID,
    )
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_merge_operations
         SET state = 'conflict', failure_code = 'MERGE_CONFLICT',
             merge_base_oid = ?, candidate_merge_tree_oid = ?,
             conflict_path_count = 1, version = 2, updated_at = ?
         WHERE operation_id = ?",
    )
    .bind(support::delivery::MERGE_BASE_OID)
    .bind(support::delivery::MERGE_TREE_OID)
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::MERGE_OPERATION_ID)
    .execute(conflict_store.pool())
    .await
    .unwrap();
    let invalid_conflict_path = sqlx::query(
        "INSERT INTO task_merge_conflicts
             (operation_id, ordinal, path_encoding, path_value)
         VALUES (?, 0, 'utf8', CAST(? AS TEXT))",
    )
    .bind(support::delivery::MERGE_OPERATION_ID)
    .bind(b"src/\xff".as_slice())
    .execute(conflict_store.pool())
    .await;
    assert!(invalid_conflict_path.is_err());

    let date_store = support::memory_store().await;
    let parents =
        support::delivery::parents::seed_eligible_delivery_parents(date_store.pool()).await;
    support::delivery::merge::create_preflight(
        date_store.pool(),
        parents.final_review_event_id,
        support::delivery::MERGE_OPERATION_ID,
        support::delivery::PREFLIGHT_RECEIPT_ID,
    )
    .await
    .unwrap();
    support::delivery::merge::mark_preflight_ready(
        date_store.pool(),
        support::delivery::MERGE_OPERATION_ID,
    )
    .await
    .unwrap();
    let invalid_commit_date = support::delivery::merge::accept_merge_with_date_bytes(
        date_store.pool(),
        support::delivery::MERGE_OPERATION_ID,
        support::delivery::ACCEPT_RECEIPT_ID,
        support::delivery::SqlTextFixture::RawBytes(b"1785772800 \xff+0000"),
    )
    .await;
    assert!(invalid_commit_date.is_err());
}

#[tokio::test]
async fn raw_sql_rejects_nonexistent_or_out_of_range_timestamps() {
    let invalid_timestamps = [
        "2026-02-30T12:00:00.000000000Z",
        "2025-02-29T12:00:00.000000000Z",
        "2026-01-01T24:00:00.000000000Z",
        "2026-01-01T12:00:60.000000000Z",
    ];

    let store = support::memory_store().await;
    let parents = support::delivery::parents::seed_eligible_delivery_parents(store.pool()).await;
    support::delivery::merge::create_preflight(
        store.pool(),
        parents.final_review_event_id,
        support::delivery::MERGE_OPERATION_ID,
        support::delivery::PREFLIGHT_RECEIPT_ID,
    )
    .await
    .unwrap();
    for timestamp in invalid_timestamps {
        let result = sqlx::query(
            "UPDATE task_merge_operations
             SET state = 'conflict', version = 2, updated_at = ?
             WHERE operation_id = ?",
        )
        .bind(timestamp)
        .bind(support::delivery::MERGE_OPERATION_ID)
        .execute(store.pool())
        .await;
        assert!(
            result.is_err(),
            "accepted invalid current-row timestamp {timestamp}"
        );
    }

    for timestamp in invalid_timestamps {
        let receipt_store = support::memory_store().await;
        let parents =
            support::delivery::parents::seed_eligible_delivery_parents(receipt_store.pool()).await;
        let result = support::delivery::merge::create_preflight_with_receipt_timestamp(
            receipt_store.pool(),
            parents.final_review_event_id,
            support::delivery::MERGE_OPERATION_ID,
            support::delivery::PREFLIGHT_RECEIPT_ID,
            timestamp,
        )
        .await;
        assert!(
            result.is_err(),
            "accepted invalid receipt timestamp {timestamp}"
        );
    }
}

#[tokio::test]
async fn raw_sql_rejects_noncanonical_git_commit_date_bytes() {
    let invalid_dates = [
        "evil +0000",
        "01785772800 +0000",
        "+1785772800 +0000",
        "9223372036854775808 +0000",
        "1785772800  +0000",
        "1785772800 -0000",
    ];

    for date_bytes in invalid_dates {
        let merge_store = support::memory_store().await;
        let parents =
            support::delivery::parents::seed_eligible_delivery_parents(merge_store.pool()).await;
        support::delivery::merge::create_preflight(
            merge_store.pool(),
            parents.final_review_event_id,
            support::delivery::MERGE_OPERATION_ID,
            support::delivery::PREFLIGHT_RECEIPT_ID,
        )
        .await
        .unwrap();
        support::delivery::merge::mark_preflight_ready(
            merge_store.pool(),
            support::delivery::MERGE_OPERATION_ID,
        )
        .await
        .unwrap();
        let merge_result = support::delivery::merge::accept_merge_with_date_bytes(
            merge_store.pool(),
            support::delivery::MERGE_OPERATION_ID,
            support::delivery::ACCEPT_RECEIPT_ID,
            support::delivery::SqlTextFixture::Utf8(date_bytes),
        )
        .await;
        assert!(
            merge_result.is_err(),
            "accepted noncanonical merge Git date {date_bytes}"
        );

        let source_store = support::memory_store().await;
        let parents =
            support::delivery::parents::seed_eligible_delivery_parents(source_store.pool()).await;
        support::delivery::merge::create_preflight(
            source_store.pool(),
            parents.final_review_event_id,
            support::delivery::MERGE_OPERATION_ID,
            support::delivery::PREFLIGHT_RECEIPT_ID,
        )
        .await
        .unwrap();
        support::delivery::merge::mark_preflight_ready(
            source_store.pool(),
            support::delivery::MERGE_OPERATION_ID,
        )
        .await
        .unwrap();
        support::delivery::merge::accept_merge(
            source_store.pool(),
            support::delivery::MERGE_OPERATION_ID,
        )
        .await
        .unwrap();
        let source_result = support::delivery::merge::create_source_object_pending_with_date_bytes(
            source_store.pool(),
            date_bytes,
        )
        .await;
        assert!(
            source_result.is_err(),
            "accepted noncanonical source Git date {date_bytes}"
        );
    }

    for date_bytes in ["0 +0000", "-1 +0000"] {
        let store = support::memory_store().await;
        let parents =
            support::delivery::parents::seed_eligible_delivery_parents(store.pool()).await;
        support::delivery::merge::create_preflight(
            store.pool(),
            parents.final_review_event_id,
            support::delivery::MERGE_OPERATION_ID,
            support::delivery::PREFLIGHT_RECEIPT_ID,
        )
        .await
        .unwrap();
        support::delivery::merge::mark_preflight_ready(
            store.pool(),
            support::delivery::MERGE_OPERATION_ID,
        )
        .await
        .unwrap();
        support::delivery::merge::accept_merge_with_date_bytes(
            store.pool(),
            support::delivery::MERGE_OPERATION_ID,
            support::delivery::ACCEPT_RECEIPT_ID,
            support::delivery::SqlTextFixture::Utf8(date_bytes),
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn raw_sql_rejects_cross_entity_state_and_fact_corruption() {
    let source_store = support::memory_store().await;
    let parents =
        support::delivery::parents::seed_eligible_delivery_parents(source_store.pool()).await;
    support::delivery::merge::create_preflight(
        source_store.pool(),
        parents.final_review_event_id,
        support::delivery::MERGE_OPERATION_ID,
        support::delivery::PREFLIGHT_RECEIPT_ID,
    )
    .await
    .unwrap();
    support::delivery::merge::mark_preflight_ready(
        source_store.pool(),
        support::delivery::MERGE_OPERATION_ID,
    )
    .await
    .unwrap();
    support::delivery::merge::accept_merge(
        source_store.pool(),
        support::delivery::MERGE_OPERATION_ID,
    )
    .await
    .unwrap();
    support::delivery::merge::create_source_object_pending(source_store.pool())
        .await
        .unwrap();
    let unproven_commit = sqlx::query(
        "UPDATE task_delivery_sources
         SET state = 'committed', version = 2, updated_at = ? WHERE task_id = ?",
    )
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::TASK_ID)
    .execute(source_store.pool())
    .await;
    assert!(unproven_commit.is_err());

    let cleanup_store = support::memory_store().await;
    support::delivery::merge::seed_merged_delivery(cleanup_store.pool())
        .await
        .unwrap();
    let disposition_without_cleanup = sqlx::query(
        "UPDATE task_artifact_dispositions
         SET worktree_state = 'retained_unlocked', worktree_version = 2,
             worktree_updated_at = ? WHERE task_id = ?",
    )
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::TASK_ID)
    .execute(cleanup_store.pool())
    .await;
    assert!(disposition_without_cleanup.is_err());
    support::delivery::cleanup::create_remove_cleanup(
        cleanup_store.pool(),
        support::delivery::CLEANUP_OPERATION_ID,
        support::delivery::CLEANUP_RECEIPT_ID,
    )
    .await
    .unwrap();
    let wrong_kind_state = sqlx::query(
        "UPDATE task_cleanup_operations
         SET state = 'delete_pending', version = 2, updated_at = ?
         WHERE operation_id = ?",
    )
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::CLEANUP_OPERATION_ID)
    .execute(cleanup_store.pool())
    .await;
    assert!(wrong_kind_state.is_err());
    let orphan_conflict = sqlx::query(
        "INSERT INTO task_merge_conflicts
             (operation_id, ordinal, path_encoding, path_value)
         VALUES (?, 0, 'utf8', 'src/lib.rs')",
    )
    .bind(support::delivery::MERGE_OPERATION_ID)
    .execute(cleanup_store.pool())
    .await;
    assert!(orphan_conflict.is_err());
}
