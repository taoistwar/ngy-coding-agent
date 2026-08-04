use std::path::Path;

use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection as _, SqliteConnection, SqlitePool};

pub async fn seed_v4_database(path: &Path) {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true);
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    for (version, migration) in [
        (1_i64, include_str!("../../migrations/0001_initial.sql")),
        (
            2,
            include_str!("../../migrations/0002_task_attempt_artifacts.sql"),
        ),
        (
            3,
            include_str!("../../migrations/0003_multi_role_quality.sql"),
        ),
        (
            4,
            include_str!("../../migrations/0004_concurrent_scheduler.sql"),
        ),
    ] {
        sqlx::raw_sql(migration)
            .execute(&mut connection)
            .await
            .unwrap();
        sqlx::query("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)")
            .bind(version)
            .bind("2026-08-04T00:00:00.000000000Z")
            .execute(&mut connection)
            .await
            .unwrap();
    }
    connection.close().await.unwrap();
}

pub fn v5_schema_objects() -> Vec<(&'static str, &'static str)> {
    include_str!("../../migrations/0005_controlled_delivery.sql")
        .lines()
        .filter_map(|line| {
            line.strip_prefix("CREATE TABLE ")
                .map(|rest| ("table", rest))
                .or_else(|| {
                    line.strip_prefix("CREATE UNIQUE INDEX ")
                        .map(|rest| ("index", rest))
                })
                .or_else(|| {
                    line.strip_prefix("CREATE INDEX ")
                        .map(|rest| ("index", rest))
                })
                .or_else(|| {
                    line.strip_prefix("CREATE TRIGGER ")
                        .map(|rest| ("trigger", rest))
                })
                .map(|(kind, rest)| (kind, rest.split_whitespace().next().unwrap()))
        })
        .collect()
}

pub async fn seed_v5_schema_conflict(path: &Path, kind: &str, name: &str) {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true);
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    let sql = match kind {
        "table" => format!("CREATE TABLE {name} (preserve_marker TEXT NOT NULL)"),
        "index" => format!("CREATE INDEX {name} ON repositories(created_at)"),
        "trigger" => {
            format!("CREATE TRIGGER {name} BEFORE UPDATE ON repositories BEGIN SELECT 1; END")
        }
        "migration_receipt" => format!(
            "CREATE TRIGGER {name} BEFORE INSERT ON schema_migrations \
             WHEN NEW.version = 5 \
             BEGIN SELECT RAISE(ABORT, 'injected v5 receipt failure'); END"
        ),
        "migration_receipt_ignore" => format!(
            "CREATE TRIGGER {name} BEFORE INSERT ON schema_migrations \
             WHEN NEW.version = 5 BEGIN SELECT RAISE(IGNORE); END"
        ),
        "migration_receipt_delete" => format!(
            "CREATE TRIGGER {name} AFTER INSERT ON schema_migrations \
             WHEN NEW.version = 5 \
             BEGIN DELETE FROM schema_migrations WHERE version = 5; END"
        ),
        other => panic!("unsupported v5 schema conflict kind {other}"),
    };
    sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
        .execute(&mut connection)
        .await
        .unwrap();
    connection.close().await.unwrap();
}

pub async fn schema_snapshot(pool: &SqlitePool) -> Vec<(String, String, String, Option<String>)> {
    sqlx::query_as(
        "SELECT type, name, tbl_name, sql
         FROM sqlite_schema
         WHERE name NOT LIKE 'sqlite_%'
         ORDER BY type, name",
    )
    .fetch_all(pool)
    .await
    .unwrap()
}

pub async fn history_snapshot(pool: &SqlitePool) -> Vec<(i64, String)> {
    sqlx::query_as("SELECT version, applied_at FROM schema_migrations ORDER BY version")
        .fetch_all(pool)
        .await
        .unwrap()
}
