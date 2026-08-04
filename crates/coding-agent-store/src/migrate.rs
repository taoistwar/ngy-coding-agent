use coding_agent_domain::UtcTimestamp;
use sqlx::SqlitePool;
use time::OffsetDateTime;

use crate::StoreError;

const LATEST_DATABASE_MIGRATION_VERSION: i64 = 4;

const MIGRATIONS: &[(i64, &str)] = &[
    (1, include_str!("../migrations/0001_initial.sql")),
    (
        2,
        include_str!("../migrations/0002_task_attempt_artifacts.sql"),
    ),
    (3, include_str!("../migrations/0003_multi_role_quality.sql")),
    (
        4,
        include_str!("../migrations/0004_concurrent_scheduler.sql"),
    ),
];

pub(crate) async fn run(pool: &SqlitePool) -> Result<(), StoreError> {
    validate_embedded_migrations()?;
    let mut transaction = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(StoreError::DatabaseMigration)?;
    let applied_versions = read_validated_history(&mut transaction).await?;

    for &(version, sql) in MIGRATIONS.iter().skip(applied_versions.len()) {
        sqlx::raw_sql(sql)
            .execute(&mut *transaction)
            .await
            .map_err(StoreError::DatabaseMigration)?;
        let applied_at = UtcTimestamp::new(OffsetDateTime::now_utc())?.to_string();
        let receipt =
            sqlx::query("INSERT INTO main.schema_migrations (version, applied_at) VALUES (?, ?)")
                .bind(version)
                .bind(applied_at)
                .execute(&mut *transaction)
                .await
                .map_err(StoreError::DatabaseMigration)?;
        if receipt.rows_affected() != 1 {
            return Err(StoreError::InvariantViolation(
                "database migration receipt was not inserted",
            ));
        }
    }

    let completed_versions = read_validated_history(&mut transaction).await?;
    if completed_versions.len() != MIGRATIONS.len() {
        return Err(StoreError::InvariantViolation(
            "database migration receipt history is incomplete",
        ));
    }

    let foreign_key_violation = sqlx::query("PRAGMA foreign_key_check")
        .fetch_optional(&mut *transaction)
        .await
        .map_err(StoreError::DatabaseMigration)?;
    if foreign_key_violation.is_some() {
        return Err(StoreError::InvariantViolation(
            "database foreign key check failed after migration",
        ));
    }

    transaction
        .commit()
        .await
        .map_err(StoreError::DatabaseMigration)?;
    Ok(())
}

fn validate_embedded_migrations() -> Result<(), StoreError> {
    let latest_version = usize::try_from(LATEST_DATABASE_MIGRATION_VERSION)
        .map_err(|_| StoreError::InvariantViolation("embedded database migrations are invalid"))?;
    let is_contiguous = MIGRATIONS
        .iter()
        .enumerate()
        .all(|(index, (version, _))| i64::try_from(index + 1) == Ok(*version));
    if MIGRATIONS.len() == latest_version && is_contiguous {
        Ok(())
    } else {
        Err(StoreError::InvariantViolation(
            "embedded database migrations are invalid",
        ))
    }
}

async fn read_validated_history(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<Vec<i64>, StoreError> {
    let object: Option<(String, String)> = sqlx::query_as(
        "SELECT name, type \
         FROM main.sqlite_schema \
         WHERE name = 'schema_migrations' COLLATE NOCASE",
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(StoreError::DatabaseMigration)?;
    let Some((object_name, object_type)) = object else {
        return Ok(Vec::new());
    };
    if object_name != "schema_migrations" || object_type != "table" {
        return Err(StoreError::DatabaseSchemaUnsupported);
    }

    let columns: Vec<(String, String, i64, i64, i64)> = sqlx::query_as(
        "SELECT name, upper(type), \"notnull\", pk, hidden \
         FROM pragma_table_xinfo('schema_migrations', 'main') \
         ORDER BY cid",
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(StoreError::DatabaseMigration)?;
    let canonical_columns = [
        ("version", "INTEGER", 0_i64, 1_i64, 0_i64),
        ("applied_at", "TEXT", 1_i64, 0_i64, 0_i64),
    ];
    let has_canonical_shape = columns.len() == canonical_columns.len()
        && columns
            .iter()
            .zip(canonical_columns)
            .all(|(actual, expected)| {
                actual.0 == expected.0
                    && actual.1 == expected.1
                    && actual.2 == expected.2
                    && actual.3 == expected.3
                    && actual.4 == expected.4
            });
    if !has_canonical_shape {
        return Err(StoreError::DatabaseSchemaUnsupported);
    }

    let rows: Vec<(Option<String>, String)> = sqlx::query_as(
        "SELECT CAST(version AS TEXT), typeof(version) \
         FROM main.schema_migrations \
         ORDER BY version, rowid",
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(StoreError::DatabaseMigration)?;
    let latest_version = usize::try_from(LATEST_DATABASE_MIGRATION_VERSION)
        .map_err(|_| StoreError::DatabaseSchemaUnsupported)?;
    if rows.is_empty() || rows.len() > latest_version {
        return Err(StoreError::DatabaseSchemaUnsupported);
    }

    let mut applied_versions = Vec::with_capacity(rows.len());
    for (index, (version, value_type)) in rows.into_iter().enumerate() {
        let expected =
            i64::try_from(index + 1).map_err(|_| StoreError::DatabaseSchemaUnsupported)?;
        let parsed = version
            .as_deref()
            .filter(|_| value_type == "integer")
            .and_then(|value| value.parse::<i64>().ok());
        if parsed != Some(expected) {
            return Err(StoreError::DatabaseSchemaUnsupported);
        }
        applied_versions.push(expected);
    }
    Ok(applied_versions)
}
