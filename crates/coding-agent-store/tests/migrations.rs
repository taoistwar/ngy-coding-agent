mod support;

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
    assert_eq!(versions, vec![1]);

    for table in ["schema_migrations", "repositories", "tasks", "task_events"] {
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
