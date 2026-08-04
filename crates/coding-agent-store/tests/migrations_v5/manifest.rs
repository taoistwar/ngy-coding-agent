use super::{helpers::delivery_row_count, support};
use coding_agent_store::{DATABASE_SCHEMA_UNSUPPORTED, Store};

const V5_TABLES: [&str; 8] = [
    "task_delivery_sources",
    "task_merge_operations",
    "task_merge_conflicts",
    "task_artifact_dispositions",
    "task_cleanup_operations",
    "task_cleanup_target_head_observations",
    "task_delivery_command_receipts",
    "task_delivery_operation_transitions",
];

#[tokio::test]
async fn empty_database_migrates_to_v5_with_all_strict_delivery_tables() {
    let fixture = support::file_store().await;

    fixture.store.migrate().await.unwrap();

    let versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM schema_migrations ORDER BY version")
            .fetch_all(fixture.store.pool())
            .await
            .unwrap();
    assert_eq!(versions, vec![1, 2, 3, 4, 5]);

    for table in V5_TABLES {
        let strict: i64 = sqlx::query_scalar(
            "SELECT strict FROM pragma_table_list WHERE schema = 'main' AND name = ?",
        )
        .bind(table)
        .fetch_one(fixture.store.pool())
        .await
        .unwrap();
        assert_eq!(strict, 1, "{table} must be STRICT");
    }
}

#[tokio::test]
async fn forged_history_only_v5_fails_closed() {
    let fixture = support::file_store().await;
    sqlx::raw_sql(
        "CREATE TABLE schema_migrations (
             version INTEGER PRIMARY KEY,
             applied_at TEXT NOT NULL
         );
         INSERT INTO schema_migrations(version, applied_at) VALUES
             (1, 'one'), (2, 'two'), (3, 'three'),
             (4, 'four'), (5, 'five');",
    )
    .execute(fixture.store.pool())
    .await
    .unwrap();

    let error = fixture
        .store
        .migrate()
        .await
        .expect_err("history-only v5 must not be trusted as a valid schema");
    assert_eq!(
        error.to_string(),
        coding_agent_store::DATABASE_SCHEMA_UNSUPPORTED
    );
    for table in V5_TABLES {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = ?",
        )
        .bind(table)
        .fetch_one(fixture.store.pool())
        .await
        .unwrap();
        assert_eq!(count, 0, "rejection must not repair forged schema");
    }
}

#[tokio::test]
async fn v5_reopen_and_repeated_migrate_are_exact_no_ops() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    let schema = support::migration_v5::schema_snapshot(fixture.store.pool()).await;
    let history = support::migration_v5::history_snapshot(fixture.store.pool()).await;

    fixture.store.migrate().await.unwrap();
    assert_eq!(
        support::migration_v5::schema_snapshot(fixture.store.pool()).await,
        schema
    );
    assert_eq!(
        support::migration_v5::history_snapshot(fixture.store.pool()).await,
        history
    );

    fixture.store.close().await;
    let reopened = Store::open(&fixture.database_path).await.unwrap();
    reopened.migrate().await.unwrap();
    assert_eq!(
        support::migration_v5::schema_snapshot(reopened.pool()).await,
        schema
    );
    assert_eq!(
        support::migration_v5::history_snapshot(reopened.pool()).await,
        history
    );
}

#[tokio::test]
async fn real_v4_database_upgrades_without_fake_delivery_rows_or_new_sequence() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v4-to-v5.sqlite3");
    support::migration_v5::seed_v4_database(&path).await;
    let store = Store::open(&path).await.unwrap();
    let sequences_before: Vec<(String, i64)> =
        sqlx::query_as("SELECT name, seq FROM sqlite_sequence ORDER BY name")
            .fetch_all(store.pool())
            .await
            .unwrap();

    store.migrate().await.unwrap();

    assert_eq!(
        support::migration_v5::history_snapshot(store.pool())
            .await
            .into_iter()
            .map(|row| row.0)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5]
    );
    for table in V5_TABLES {
        let count = delivery_row_count(store.pool(), table).await;
        assert_eq!(count, 0, "v4 upgrade fabricated a {table} row");
    }
    let sequences_after: Vec<(String, i64)> =
        sqlx::query_as("SELECT name, seq FROM sqlite_sequence ORDER BY name")
            .fetch_all(store.pool())
            .await
            .unwrap();
    assert_eq!(sequences_after, sequences_before);
    assert!(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_optional(store.pool())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn exact_v5_manifest_rejects_weakened_table_trigger_and_index() {
    let corruptions = [
        (
            "task_merge_conflicts",
            "DROP TABLE task_merge_conflicts;
             CREATE TABLE task_merge_conflicts (
                 operation_id TEXT NOT NULL,
                 ordinal INTEGER NOT NULL,
                 path_encoding TEXT NOT NULL,
                 path_value TEXT NOT NULL,
                 PRIMARY KEY (operation_id, ordinal)
             ) STRICT;",
        ),
        (
            "task_merge_operations_transition_on_update",
            "DROP TRIGGER task_merge_operations_transition_on_update;
             CREATE TRIGGER task_merge_operations_transition_on_update
             BEFORE UPDATE ON task_merge_operations BEGIN SELECT 1; END;",
        ),
        (
            "task_merge_operations_one_active",
            "DROP INDEX task_merge_operations_one_active;
             CREATE UNIQUE INDEX task_merge_operations_one_active
             ON task_merge_operations(task_id) WHERE state = 'failed';",
        ),
        (
            "task_merge_operations_abort_child_receipt_unique",
            "DROP INDEX task_merge_operations_abort_child_receipt_unique;
             CREATE INDEX task_merge_operations_abort_child_receipt_unique
             ON task_merge_operations(abort_child_receipt_id)
             WHERE abort_child_receipt_id IS NOT NULL;",
        ),
        (
            "unexpected_v5_trigger",
            "CREATE TRIGGER unexpected_v5_trigger
             AFTER UPDATE ON task_merge_operations BEGIN SELECT 1; END;",
        ),
        (
            "unexpected_v5_index",
            "CREATE INDEX unexpected_v5_index
             ON task_delivery_sources(updated_at);",
        ),
        (
            "task_delivery_sources_no_delete",
            "DROP TRIGGER task_delivery_sources_no_delete;
             CREATE TRIGGER task_delivery_sources_no_delete
             BEFORE DELETE ON task_delivery_sources
             BEGIN
                 SELECT RAISE(ABORT, 'delivery   source current rows are retained');
             END;",
        ),
    ];

    for (object, corruption) in corruptions {
        let fixture = support::file_store().await;
        fixture.store.migrate().await.unwrap();
        sqlx::raw_sql(corruption)
            .execute(fixture.store.pool())
            .await
            .unwrap();
        let corrupted_sql: String =
            sqlx::query_scalar("SELECT sql FROM sqlite_schema WHERE name = ?")
                .bind(object)
                .fetch_one(fixture.store.pool())
                .await
                .unwrap();

        let error = fixture.store.migrate().await.unwrap_err();

        assert_eq!(error.to_string(), DATABASE_SCHEMA_UNSUPPORTED, "{object}");
        let after: String = sqlx::query_scalar("SELECT sql FROM sqlite_schema WHERE name = ?")
            .bind(object)
            .fetch_one(fixture.store.pool())
            .await
            .unwrap();
        assert_eq!(after, corrupted_sql, "manifest audit repaired {object}");
    }
}

#[tokio::test]
async fn every_v5_schema_or_receipt_failure_rolls_back_the_whole_upgrade() {
    let mut conflicts = support::migration_v5::v5_schema_objects();
    conflicts.extend([
        ("migration_receipt", "reject_v5_migration_receipt"),
        ("migration_receipt_ignore", "ignore_v5_migration_receipt"),
        ("migration_receipt_delete", "delete_v5_migration_receipt"),
    ]);

    for (kind, name) in conflicts {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(format!("v5-conflict-{name}.sqlite3"));
        support::migration_v5::seed_v4_database(&path).await;
        support::migration_v5::seed_v5_schema_conflict(&path, kind, name).await;
        let store = Store::open(&path).await.unwrap();
        let schema_before = support::migration_v5::schema_snapshot(store.pool()).await;
        let history_before = support::migration_v5::history_snapshot(store.pool()).await;

        let error = store.migrate().await.unwrap_err();

        assert_eq!(
            support::migration_v5::schema_snapshot(store.pool()).await,
            schema_before,
            "{kind} {name} left a partial v5 schema: {error}"
        );
        assert_eq!(
            support::migration_v5::history_snapshot(store.pool()).await,
            history_before,
            "{kind} {name} changed migration history"
        );
        assert!(
            !error.to_string().contains(&path.display().to_string()),
            "migration error leaked its database path: {error}"
        );
    }
}
