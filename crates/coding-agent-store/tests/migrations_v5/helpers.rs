use super::support;

pub(super) async fn pending_merge_fixture() -> support::FileStoreFixture {
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
    fixture
}

pub(super) async fn transition_pending_merge(
    pool: &sqlx::SqlitePool,
    state: &str,
    failure_code: Option<&str>,
    merge_base_oid: Option<&str>,
    candidate_merge_tree_oid: Option<&str>,
) -> Result<sqlx::sqlite::SqliteQueryResult, sqlx::Error> {
    sqlx::query(
        "UPDATE task_merge_operations
         SET state = ?, failure_code = ?, merge_base_oid = ?, candidate_merge_tree_oid = ?,
             conflict_path_count = CASE WHEN ? = 'conflict' THEN 0 ELSE NULL END,
             version = 3, updated_at = ? WHERE operation_id = ?",
    )
    .bind(state)
    .bind(failure_code)
    .bind(merge_base_oid)
    .bind(candidate_merge_tree_oid)
    .bind(state)
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::MERGE_OPERATION_ID)
    .execute(pool)
    .await
}

pub(super) async fn object_pending_source_fixture() -> support::FileStoreFixture {
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
    fixture
}

pub(super) async fn merge_pending_fixture() -> support::FileStoreFixture {
    let fixture = accepted_committed_source_fixture().await;
    mark_merge_pending(fixture.store.pool(), support::delivery::MERGE_OPERATION_ID)
        .await
        .unwrap();
    fixture
}

pub(super) async fn accepted_committed_source_fixture() -> support::FileStoreFixture {
    let fixture = object_pending_source_fixture().await;
    transition_delivery_source(
        fixture.store.pool(),
        "commit_pending",
        None,
        2,
        Some(support::delivery::SOURCE_COMMIT_OID),
    )
    .await
    .unwrap();
    transition_delivery_source(fixture.store.pool(), "committed", None, 3, None)
        .await
        .unwrap();
    fixture
}

pub(super) async fn transition_accepted_merge_to_failed(
    pool: &sqlx::SqlitePool,
    failure_code: &str,
) -> Result<sqlx::sqlite::SqliteQueryResult, sqlx::Error> {
    sqlx::query(
        "UPDATE task_merge_operations
         SET delivery_source_task_id = task_id, source_commit_oid = ?,
             state = 'failed', failure_code = ?, version = 5, updated_at = ?
         WHERE operation_id = ?",
    )
    .bind(support::delivery::SOURCE_COMMIT_OID)
    .bind(failure_code)
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::MERGE_OPERATION_ID)
    .execute(pool)
    .await
}

pub(super) async fn mark_merge_pending(
    pool: &sqlx::SqlitePool,
    operation_id: &str,
) -> Result<sqlx::sqlite::SqliteQueryResult, sqlx::Error> {
    sqlx::query(
        "UPDATE task_merge_operations
         SET delivery_source_task_id = task_id, source_commit_oid = ?,
             expected_merge_commit_oid = ?, state = 'merge_pending', failure_code = NULL,
             version = 5, updated_at = ? WHERE operation_id = ?",
    )
    .bind(support::delivery::SOURCE_COMMIT_OID)
    .bind(support::delivery::MERGE_COMMIT_OID)
    .bind(support::delivery::TIMESTAMP)
    .bind(operation_id)
    .execute(pool)
    .await
}

pub(super) async fn begin_merge_abort(
    pool: &sqlx::SqlitePool,
    operation_id: &str,
    child_receipt_id: &str,
) -> Result<sqlx::sqlite::SqliteQueryResult, sqlx::Error> {
    let mut transaction = pool.begin().await?;
    let updated = sqlx::query(
        "UPDATE task_merge_operations
         SET abort_child_receipt_id = ?, abort_merge_head_oid = source_commit_oid,
             abort_index_stages_digest = ?, abort_worktree_digest = ?,
             abort_merge_autostash_proof = 'absent', conflict_path_count = 1,
             state = 'abort_pending',
             failure_code = NULL, version = 6, updated_at = ? WHERE operation_id = ?",
    )
    .bind(child_receipt_id)
    .bind(support::delivery::CHECKS_DIGEST)
    .bind(support::delivery::COVERAGE_DIGEST)
    .bind(support::delivery::TIMESTAMP)
    .bind(operation_id)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "INSERT INTO task_merge_conflicts
             (operation_id, ordinal, path_encoding, path_value)
         VALUES (?, 0, 'utf8', 'src/conflicted.rs')",
    )
    .bind(operation_id)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(updated)
}

pub(super) async fn transition_delivery_source(
    pool: &sqlx::SqlitePool,
    state: &str,
    failure_code: Option<&str>,
    version: i64,
    expected_source_commit_oid: Option<&str>,
) -> Result<sqlx::sqlite::SqliteQueryResult, sqlx::Error> {
    sqlx::query(
        "UPDATE task_delivery_sources
         SET expected_source_commit_oid = COALESCE(?, expected_source_commit_oid),
             state = ?, failure_code = ?, version = ?, updated_at = ?
         WHERE task_id = ?",
    )
    .bind(expected_source_commit_oid)
    .bind(state)
    .bind(failure_code)
    .bind(version)
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::TASK_ID)
    .execute(pool)
    .await
}

pub(super) async fn delivery_row_count(pool: &sqlx::SqlitePool, table: &str) -> i64 {
    let query = match table {
        "task_delivery_sources" => "SELECT COUNT(*) FROM task_delivery_sources",
        "task_merge_operations" => "SELECT COUNT(*) FROM task_merge_operations",
        "task_merge_conflicts" => "SELECT COUNT(*) FROM task_merge_conflicts",
        "task_artifact_dispositions" => "SELECT COUNT(*) FROM task_artifact_dispositions",
        "task_cleanup_operations" => "SELECT COUNT(*) FROM task_cleanup_operations",
        "task_cleanup_target_head_observations" => {
            "SELECT COUNT(*) FROM task_cleanup_target_head_observations"
        }
        "task_delivery_command_receipts" => "SELECT COUNT(*) FROM task_delivery_command_receipts",
        "task_delivery_operation_transitions" => {
            "SELECT COUNT(*) FROM task_delivery_operation_transitions"
        }
        other => panic!("unsupported delivery table {other}"),
    };
    sqlx::query_scalar(query).fetch_one(pool).await.unwrap()
}

pub(super) async fn normalized_schema_sql(pool: &sqlx::SqlitePool, name: &str) -> String {
    let sql: String = sqlx::query_scalar("SELECT sql FROM sqlite_schema WHERE name = ?")
        .bind(name)
        .fetch_one(pool)
        .await
        .unwrap();
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub(super) fn assert_merge_failure_contract(schema_sql: &str, state_column: &str) {
    assert_schema_has_failure_clause(
        schema_sql,
        state_column,
        "IN",
        &[
            "preflight_pending",
            "preflight_ready",
            "accepted",
            "merge_pending",
            "merged",
            "abort_pending",
            "superseded",
        ],
        false,
    );
    assert_schema_has_failure_clause(
        schema_sql,
        state_column,
        "=",
        &["conflict", "MERGE_CONFLICT"],
        true,
    );
    for (state, failure_codes) in [
        (
            "rejected",
            &[
                "TASK_NOT_MERGE_ELIGIBLE",
                "TARGET_BRANCH_DETACHED",
                "TARGET_BRANCH_MISMATCH",
                "TARGET_WORKTREE_DIRTY",
                "TARGET_IGNORED_PATH_COLLISION",
                "TARGET_GIT_OPERATION_IN_PROGRESS",
                "UNSAFE_GIT_CONFIGURATION",
                "UNSUPPORTED_GIT_ATTRIBUTES",
                "SOURCE_ALREADY_IN_TARGET",
            ][..],
        ),
        (
            "stale",
            &[
                "DELIVERY_EVIDENCE_STALE",
                "TARGET_BRANCH_MISMATCH",
                "TARGET_HEAD_CHANGED",
                "DELIVERY_SOURCE_CHANGED",
            ][..],
        ),
        (
            "failed",
            &[
                "TASK_NOT_MERGE_ELIGIBLE",
                "TARGET_BRANCH_DETACHED",
                "TARGET_BRANCH_MISMATCH",
                "TARGET_WORKTREE_DIRTY",
                "TARGET_IGNORED_PATH_COLLISION",
                "TARGET_GIT_OPERATION_IN_PROGRESS",
                "UNSAFE_GIT_CONFIGURATION",
                "UNSUPPORTED_GIT_ATTRIBUTES",
                "SOURCE_ALREADY_IN_TARGET",
                "TARGET_HEAD_CHANGED",
                "COMMAND_TIMED_OUT",
            ][..],
        ),
        (
            "reconciliation_required",
            &[
                "DELIVERY_RECONCILIATION_REQUIRED",
                "DELIVERY_SOURCE_INCONSISTENT",
                "PROCESS_TREE_CLEANUP_FAILED",
                "WORKTREE_IDENTITY_MISMATCH",
                "UNSAFE_GIT_CONFIGURATION",
                "UNSUPPORTED_GIT_ATTRIBUTES",
            ][..],
        ),
    ] {
        let codes = quoted_values(failure_codes);
        let expected = format!(
            "{state_column} = '{state}' AND failure_code IS NOT NULL AND failure_code IN ( {codes} )"
        );
        assert!(
            schema_sql.contains(&expected),
            "missing exact {state} failure clause: {expected}"
        );
    }
}

pub(super) fn assert_schema_has_failure_clause(
    schema_sql: &str,
    state_column: &str,
    operator: &str,
    values: &[&str],
    exact_failure: bool,
) {
    let expected = if exact_failure {
        format!(
            "{state_column} {operator} '{}' AND failure_code IS NOT NULL AND failure_code = '{}'",
            values[0], values[1]
        )
    } else {
        format!(
            "{state_column} {operator} ( {} ) AND failure_code IS NULL",
            quoted_values(values)
        )
    };
    assert!(
        schema_sql.contains(&expected),
        "missing exact failure clause: {expected}"
    );
}

pub(super) fn quoted_values(values: &[&str]) -> String {
    values
        .iter()
        .map(|value| format!("'{value}'"))
        .collect::<Vec<_>>()
        .join(", ")
}
