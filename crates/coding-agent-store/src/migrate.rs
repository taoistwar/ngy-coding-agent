use coding_agent_domain::UtcTimestamp;
use sqlx::SqlitePool;
use time::OffsetDateTime;

use crate::StoreError;

const MIGRATIONS: &[(i64, &str)] = &[
    (1, include_str!("../migrations/0001_initial.sql")),
    (
        2,
        include_str!("../migrations/0002_task_attempt_artifacts.sql"),
    ),
    (3, include_str!("../migrations/0003_multi_role_quality.sql")),
];

pub(crate) async fn run(pool: &SqlitePool) -> Result<(), StoreError> {
    let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS schema_migrations (\
             version INTEGER PRIMARY KEY,\
             applied_at TEXT NOT NULL\
         )",
    )
    .execute(&mut *transaction)
    .await?;

    let applied_versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM schema_migrations ORDER BY version")
            .fetch_all(&mut *transaction)
            .await?;

    for &(version, sql) in MIGRATIONS {
        if applied_versions.binary_search(&version).is_ok() {
            continue;
        }

        sqlx::raw_sql(sql).execute(&mut *transaction).await?;
        let applied_at = UtcTimestamp::new(OffsetDateTime::now_utc())?.to_string();
        sqlx::query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
            .bind(version)
            .bind(applied_at)
            .execute(&mut *transaction)
            .await?;
    }

    transaction.commit().await?;
    Ok(())
}
