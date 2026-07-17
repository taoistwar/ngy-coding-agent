mod support;

use std::path::Path;

use coding_agent_store::Store;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, SqliteConnection};

#[tokio::test]
async fn migrations_configure_connections_and_are_idempotent() {
    let fixture = support::file_store().await;

    fixture.store.migrate().await.unwrap();
    fixture.store.migrate().await.unwrap();

    let journal_mode: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(fixture.store.pool())
        .await
        .unwrap();
    let foreign_keys: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(fixture.store.pool())
        .await
        .unwrap();
    let busy_timeout: i64 = sqlx::query_scalar("PRAGMA busy_timeout")
        .fetch_one(fixture.store.pool())
        .await
        .unwrap();

    assert_eq!(journal_mode, "wal");
    assert_eq!(foreign_keys, 1);
    assert_eq!(busy_timeout, 5_000);

    let versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM schema_migrations ORDER BY version")
            .fetch_all(fixture.store.pool())
            .await
            .unwrap();
    assert_eq!(versions, vec![1, 2]);

    for table in [
        "schema_migrations",
        "repositories",
        "tasks",
        "task_events",
        "task_attempt_artifacts",
    ] {
        let exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
        )
        .bind(table)
        .fetch_one(fixture.store.pool())
        .await
        .unwrap();
        assert_eq!(exists, 1, "missing table {table}");
    }
}

#[tokio::test]
async fn artifact_migration_has_exact_identity_and_state_constraints() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();

    let table_sql: String = sqlx::query_scalar(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'task_attempt_artifacts'",
    )
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();
    for required in [
        "task_id TEXT PRIMARY KEY",
        "UNIQUE (branch_name)",
        "UNIQUE (worktree_path)",
        "UNIQUE (repository_id, task_id, attempt)",
        "FOREIGN KEY (task_id, repository_id, attempt)",
        "state IN ('reserved', 'ready', 'inconsistent')",
    ] {
        assert!(
            table_sql.contains(required),
            "missing constraint: {required}"
        );
    }

    let parent_index: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master \
         WHERE type = 'index' AND name = 'tasks_id_repository_attempt'",
    )
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(parent_index, 1);
}

#[tokio::test]
async fn failed_migration_rolls_back_without_replacing_the_database() {
    let fixture = support::conflicting_file_store().await;

    fixture.store.migrate().await.unwrap_err();

    assert!(fixture.database_path.exists());
    let marker: String = sqlx::query_scalar("SELECT value FROM migration_marker")
        .fetch_one(fixture.store.pool())
        .await
        .unwrap();
    assert_eq!(marker, "preserve-me");

    let repository_sql: String = sqlx::query_scalar(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'repositories'",
    )
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();
    assert!(repository_sql.contains("broken INTEGER NOT NULL"));

    let migration_table_exists: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'",
    )
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(migration_table_exists, 0);
}

#[tokio::test]
async fn version_one_database_upgrades_to_v2_and_repeat_is_a_no_op() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v1.sqlite3");
    seed_v1(&path, false).await;
    let store = Store::open(&path).await.unwrap();

    store.migrate().await.unwrap();
    store.migrate().await.unwrap();

    let versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM schema_migrations ORDER BY version")
            .fetch_all(store.pool())
            .await
            .unwrap();
    assert_eq!(versions, vec![1, 2]);
    let table_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master \
         WHERE type = 'table' AND name = 'task_attempt_artifacts'",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(table_count, 1);
}

#[tokio::test]
async fn failed_v2_upgrade_rolls_back_every_v2_statement_and_preserves_v1() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("conflicting-v2.sqlite3");
    seed_v1(&path, true).await;
    let store = Store::open(&path).await.unwrap();

    store.migrate().await.unwrap_err();

    let versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM schema_migrations ORDER BY version")
            .fetch_all(store.pool())
            .await
            .unwrap();
    assert_eq!(versions, vec![1]);
    let conflict_sql: String = sqlx::query_scalar(
        "SELECT sql FROM sqlite_master \
         WHERE type = 'table' AND name = 'task_attempt_artifacts'",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert!(conflict_sql.contains("preserve_marker"));
    let rolled_back_index: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master \
         WHERE type = 'index' AND name = 'tasks_id_repository_attempt'",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(rolled_back_index, 0);
}

async fn seed_v1(path: &Path, conflicting_v2_table: bool) {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true);
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    sqlx::raw_sql(include_str!("../migrations/0001_initial.sql"))
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query("INSERT INTO schema_migrations (version, applied_at) VALUES (1, ?)")
        .bind("2026-07-16T00:00:00.000000000Z")
        .execute(&mut connection)
        .await
        .unwrap();
    if conflicting_v2_table {
        sqlx::query("CREATE TABLE task_attempt_artifacts (preserve_marker TEXT NOT NULL)")
            .execute(&mut connection)
            .await
            .unwrap();
    }
    connection.close().await.unwrap();
}
