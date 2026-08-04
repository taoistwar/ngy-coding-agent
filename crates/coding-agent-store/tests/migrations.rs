mod support;

use std::collections::BTreeMap;
use std::path::Path;

use coding_agent_store::{DATABASE_SCHEMA_UNSUPPORTED, Store};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, Row, SqliteConnection};

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
    assert_eq!(versions, vec![1, 2, 3, 4]);

    for table in [
        "schema_migrations",
        "repositories",
        "tasks",
        "task_events",
        "task_attempt_artifacts",
        "task_review_evidence",
        "task_delivery_state",
        "task_stop_intents",
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

    assert_foreign_keys_clean(fixture.store.pool()).await;
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
async fn v4_schema_has_strict_stop_intents_identity_fk_and_queued_partial_index() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    let pool = fixture.store.pool();

    assert_eq!(strict_table_flag(pool, "task_stop_intents").await, 1);
    let columns = table_columns(pool, "task_stop_intents").await;
    for (name, data_type, primary_key_position) in [
        ("task_id", "TEXT", 1),
        ("repository_id", "TEXT", 0),
        ("attempt", "INTEGER", 0),
        ("kind", "TEXT", 0),
        ("requested_at", "TEXT", 0),
    ] {
        assert_required_column(&columns, name, data_type, primary_key_position);
    }

    let stop_intent_foreign_keys = foreign_keys(pool, "task_stop_intents").await;
    assert!(
        stop_intent_foreign_keys.contains(&ForeignKey {
            parent_table: "tasks".to_owned(),
            columns: vec![
                ("task_id".to_owned(), "id".to_owned()),
                ("repository_id".to_owned(), "repository_id".to_owned()),
                ("attempt".to_owned(), "attempt".to_owned()),
            ],
        }),
        "missing exact task identity foreign key: {stop_intent_foreign_keys:?}"
    );

    let table_sql = normalized_schema_sql(pool, "table", "task_stop_intents").await;
    for required in [
        "strict",
        "typeof(task_id)",
        "typeof(repository_id)",
        "typeof(attempt)",
        "attempt > 0",
        "typeof(kind)",
        "user_cancelled",
        "disk_pressure_critical",
        "typeof(requested_at)",
    ] {
        assert!(
            table_sql.contains(required),
            "task_stop_intents is missing DDL term {required}: {table_sql}"
        );
    }

    let trigger_names: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master \
         WHERE type = 'trigger' AND tbl_name = 'task_stop_intents' ORDER BY name",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    assert_eq!(
        trigger_names,
        vec![
            "task_stop_intents_no_delete",
            "task_stop_intents_no_replace",
            "task_stop_intents_no_update",
            "task_stop_intents_running_unreviewed_on_insert",
        ]
    );

    let queued_index = normalized_schema_sql(pool, "index", "tasks_queued_created_at_id").await;
    assert!(queued_index.contains("tasks (created_at, id)"));
    assert!(queued_index.contains("where status = 'queued'"));
    assert_foreign_keys_clean(pool).await;
}

#[tokio::test]
async fn v4_stop_intent_insert_is_exact_running_unreviewed_and_rows_are_immutable() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    let pool = fixture.store.pool();
    let parents = seed_review_parents(pool).await;

    for (case, repository_id, attempt, kind, requested_at) in [
        (
            "wrong repository",
            "ffffffff-ffff-4fff-8fff-ffffffffffff",
            1_i64,
            "user_cancelled",
            FIXTURE_TIMESTAMP,
        ),
        (
            "zero attempt",
            REVIEW_REPOSITORY_ID,
            0,
            "user_cancelled",
            FIXTURE_TIMESTAMP,
        ),
        (
            "unknown kind",
            REVIEW_REPOSITORY_ID,
            1,
            "other",
            FIXTURE_TIMESTAMP,
        ),
        (
            "empty timestamp",
            REVIEW_REPOSITORY_ID,
            1,
            "user_cancelled",
            "",
        ),
    ] {
        let result = insert_stop_intent(
            pool,
            FIRST_TASK_ID,
            repository_id,
            attempt,
            kind,
            requested_at,
        )
        .await;
        assert_constraint_error(case, result);
        assert_eq!(row_count(pool, "task_stop_intents").await, 0);
    }

    let real_attempt = sqlx::query(
        "INSERT INTO task_stop_intents (
             task_id, repository_id, attempt, kind, requested_at
         ) VALUES (?, ?, 1.5, 'user_cancelled', ?)",
    )
    .bind(FIRST_TASK_ID)
    .bind(REVIEW_REPOSITORY_ID)
    .bind(FIXTURE_TIMESTAMP)
    .execute(pool)
    .await;
    assert_constraint_error("real attempt", real_attempt.map(|_| ()));

    insert_evidence(
        pool,
        EvidenceInsert::approved(FIRST_TASK_ID, parents.first_task_event_id, 1),
    )
    .await
    .unwrap();
    insert_delivery(pool, FIRST_TASK_ID, "review_approved", 1, "approved")
        .await
        .unwrap();
    assert_constraint_error(
        "reviewed task",
        insert_stop_intent(
            pool,
            FIRST_TASK_ID,
            REVIEW_REPOSITORY_ID,
            1,
            "user_cancelled",
            FIXTURE_TIMESTAMP,
        )
        .await,
    );

    sqlx::query(
        "INSERT INTO tasks (
             id, client_request_id, repository_id, prompt, status, attempt,
             retry_of, created_at, started_at, finished_at, last_event_id,
             failure_json
         ) VALUES (?, ?, ?, 'queued fixture', 'queued', 1, NULL, ?, NULL, NULL, 0, NULL)",
    )
    .bind(THIRD_TASK_ID)
    .bind(THIRD_CLIENT_REQUEST_ID)
    .bind(REVIEW_REPOSITORY_ID)
    .bind(FIXTURE_TIMESTAMP)
    .execute(pool)
    .await
    .unwrap();
    assert_constraint_error(
        "queued task",
        insert_stop_intent(
            pool,
            THIRD_TASK_ID,
            REVIEW_REPOSITORY_ID,
            1,
            "user_cancelled",
            FIXTURE_TIMESTAMP,
        )
        .await,
    );
    sqlx::query(
        "UPDATE tasks \
         SET status = 'running', started_at = ?, finished_at = NULL, failure_json = NULL \
         WHERE id = ?",
    )
    .bind(FIXTURE_TIMESTAMP.as_bytes())
    .bind(THIRD_TASK_ID)
    .execute(pool)
    .await
    .unwrap();
    assert_constraint_error(
        "running task with blob started_at",
        insert_stop_intent(
            pool,
            THIRD_TASK_ID,
            REVIEW_REPOSITORY_ID,
            1,
            "user_cancelled",
            FIXTURE_TIMESTAMP,
        )
        .await,
    );
    for (case, started_at, finished_at, failure_json) in [
        ("running task without started_at", None, None, None),
        (
            "running task with finished_at",
            Some(FIXTURE_TIMESTAMP),
            Some(FIXTURE_TIMESTAMP),
            None,
        ),
        (
            "running task with failure",
            Some(FIXTURE_TIMESTAMP),
            None,
            Some(r#"{"code":"CORRUPT","message":"corrupt","retryable":true}"#),
        ),
    ] {
        sqlx::query(
            "UPDATE tasks \
             SET status = 'running', started_at = ?, finished_at = ?, failure_json = ? \
             WHERE id = ?",
        )
        .bind(started_at)
        .bind(finished_at)
        .bind(failure_json)
        .bind(THIRD_TASK_ID)
        .execute(pool)
        .await
        .unwrap();
        assert_constraint_error(
            case,
            insert_stop_intent(
                pool,
                THIRD_TASK_ID,
                REVIEW_REPOSITORY_ID,
                1,
                "user_cancelled",
                FIXTURE_TIMESTAMP,
            )
            .await,
        );
    }

    insert_stop_intent(
        pool,
        SECOND_TASK_ID,
        REVIEW_REPOSITORY_ID,
        1,
        "user_cancelled",
        FIXTURE_TIMESTAMP,
    )
    .await
    .unwrap();
    for (case, sql) in [
        (
            "duplicate insert",
            "INSERT INTO task_stop_intents (
                 task_id, repository_id, attempt, kind, requested_at
             ) VALUES (
                 'dddddddd-dddd-4ddd-8ddd-dddddddddddd',
                 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
                 1, 'user_cancelled', '2026-07-23T00:00:01.000000000Z'
             )",
        ),
        (
            "insert or replace",
            "INSERT OR REPLACE INTO task_stop_intents (
                 task_id, repository_id, attempt, kind, requested_at
             ) VALUES (
                 'dddddddd-dddd-4ddd-8ddd-dddddddddddd',
                 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
                 1, 'disk_pressure_critical', '2026-07-23T00:00:01.000000000Z'
             )",
        ),
        (
            "update upsert",
            "INSERT INTO task_stop_intents (
                 task_id, repository_id, attempt, kind, requested_at
             ) VALUES (
                 'dddddddd-dddd-4ddd-8ddd-dddddddddddd',
                 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
                 1, 'disk_pressure_critical', '2026-07-23T00:00:01.000000000Z'
             ) ON CONFLICT(task_id) DO UPDATE SET kind = excluded.kind",
        ),
        (
            "update",
            "UPDATE task_stop_intents SET requested_at =
                 '2026-07-23T00:00:01.000000000Z'
             WHERE task_id = 'dddddddd-dddd-4ddd-8ddd-dddddddddddd'",
        ),
        (
            "delete",
            "DELETE FROM task_stop_intents
             WHERE task_id = 'dddddddd-dddd-4ddd-8ddd-dddddddddddd'",
        ),
    ] {
        let result = sqlx::raw_sql(sql).execute(pool).await.map(|_| ());
        assert_constraint_error(case, result);
    }
    let stored: (String, String, i64, String, String) = sqlx::query_as(
        "SELECT task_id, repository_id, attempt, kind, requested_at \
         FROM task_stop_intents",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(
        stored,
        (
            SECOND_TASK_ID.to_owned(),
            REVIEW_REPOSITORY_ID.to_owned(),
            1,
            "user_cancelled".to_owned(),
            FIXTURE_TIMESTAMP.to_owned(),
        )
    );
    assert_foreign_keys_clean(pool).await;
}

#[tokio::test]
async fn v4_cross_table_triggers_preserve_stop_winner_and_terminal_classification() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    let pool = fixture.store.pool();
    let parents = seed_review_parents(pool).await;

    insert_parent_event(pool, 105, FIRST_TASK_ID, "review.updated").await;
    insert_evidence(
        pool,
        EvidenceInsert::approved(FIRST_TASK_ID, parents.first_task_event_id, 1),
    )
    .await
    .unwrap();
    insert_stop_intent(
        pool,
        FIRST_TASK_ID,
        REVIEW_REPOSITORY_ID,
        1,
        "user_cancelled",
        FIXTURE_TIMESTAMP,
    )
    .await
    .unwrap();

    assert_constraint_error(
        "review evidence after intent",
        insert_evidence(pool, EvidenceInsert::approved(FIRST_TASK_ID, 105, 2)).await,
    );
    assert_constraint_error(
        "delivery state after intent",
        insert_delivery(pool, FIRST_TASK_ID, "review_approved", 1, "approved").await,
    );
    assert_constraint_error(
        "review marker event after intent",
        sqlx::query(
            "INSERT INTO task_events (
                 id, schema_version, task_id, kind, payload_json, created_at
             ) VALUES (106, 1, ?, 'review.updated', '{\"evidence_ref\":true}', ?)",
        )
        .bind(FIRST_TASK_ID)
        .bind(FIXTURE_TIMESTAMP)
        .execute(pool)
        .await
        .map(|_| ()),
    );
    assert_constraint_error(
        "existing event rewritten to review after intent",
        sqlx::query(
            "UPDATE task_events \
             SET kind = 'review.updated', payload_json = '{\"evidence_ref\":true}' \
             WHERE id = ?",
        )
        .bind(parents.first_task_plan_event_id)
        .execute(pool)
        .await
        .map(|_| ()),
    );
    assert_constraint_error(
        "running start timestamp cannot be rewritten after intent",
        sqlx::query("UPDATE tasks SET started_at = ? WHERE id = ?")
            .bind("2026-07-23T00:00:02.000000000Z")
            .bind(FIRST_TASK_ID)
            .execute(pool)
            .await
            .map(|_| ()),
    );
    sqlx::query("UPDATE tasks SET last_event_id = ? WHERE id = ?")
        .bind(105_i64)
        .bind(FIRST_TASK_ID)
        .execute(pool)
        .await
        .unwrap();

    for (case, status, failure_json) in [
        ("completed", "completed", None),
        (
            "ordinary failure",
            "failed",
            Some(r#"{"code":"RUNNER_FAILED","message":"runner failed","retryable":true}"#),
        ),
        (
            "disk failure for user intent",
            "failed",
            Some(
                r#"{"code":"DISK_PRESSURE_CRITICAL","message":"critical disk pressure stopped the task","retryable":true}"#,
            ),
        ),
        (
            "interrupted",
            "interrupted",
            Some(r#"{"code":"APP_RESTARTED","message":"restart","retryable":true}"#),
        ),
        (
            "cancelled with failure",
            "cancelled",
            Some(r#"{"code":"WRONG","message":"wrong","retryable":false}"#),
        ),
    ] {
        assert_constraint_error(
            case,
            update_task_terminal(pool, FIRST_TASK_ID, status, failure_json).await,
        );
        assert_eq!(task_status(pool, FIRST_TASK_ID).await, "running");
    }
    assert_constraint_error(
        "user terminal timestamp stored as blob",
        sqlx::query(
            "UPDATE tasks SET status = 'cancelled', finished_at = ?, failure_json = NULL \
             WHERE id = ?",
        )
        .bind(FIXTURE_TIMESTAMP.as_bytes())
        .bind(FIRST_TASK_ID)
        .execute(pool)
        .await
        .map(|_| ()),
    );
    update_task_terminal(pool, FIRST_TASK_ID, "cancelled", None)
        .await
        .unwrap();
    assert_eq!(task_status(pool, FIRST_TASK_ID).await, "cancelled");
    assert_constraint_error(
        "terminal user intent cannot return to running",
        sqlx::query(
            "UPDATE tasks SET status = 'running', finished_at = NULL, failure_json = NULL \
             WHERE id = ?",
        )
        .bind(FIRST_TASK_ID)
        .execute(pool)
        .await
        .map(|_| ()),
    );
    sqlx::query("UPDATE tasks SET last_event_id = ? WHERE id = ?")
        .bind(105_i64)
        .bind(FIRST_TASK_ID)
        .execute(pool)
        .await
        .unwrap();
    assert_constraint_error(
        "terminal user timestamp is immutable",
        sqlx::query("UPDATE tasks SET finished_at = ? WHERE id = ?")
            .bind("2026-07-23T00:00:02.000000000Z")
            .bind(FIRST_TASK_ID)
            .execute(pool)
            .await
            .map(|_| ()),
    );

    insert_parent_task(pool, FOURTH_TASK_ID, FOURTH_CLIENT_REQUEST_ID).await;
    insert_stop_intent(
        pool,
        FOURTH_TASK_ID,
        REVIEW_REPOSITORY_ID,
        1,
        "user_cancelled",
        FIXTURE_TIMESTAMP,
    )
    .await
    .unwrap();
    for (case, sql) in [
        (
            "task insert or replace",
            "INSERT OR REPLACE INTO tasks (
                 id, client_request_id, repository_id, prompt, status, attempt,
                 retry_of, created_at, started_at, finished_at, last_event_id,
                 failure_json
             ) VALUES (
                 '12121212-1212-4212-8212-121212121212',
                 '34343434-3434-4434-8434-343434343434',
                 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
                 'replacement', 'interrupted', 1, NULL,
                 '2026-07-23T00:00:00.000000000Z',
                 '2026-07-23T00:00:00.000000000Z',
                 '2026-07-23T00:00:01.000000000Z', 0,
                 '{\"code\":\"WRONG\",\"message\":\"wrong\",\"retryable\":true}'
             )",
        ),
        (
            "task update upsert",
            "INSERT INTO tasks (
                 id, client_request_id, repository_id, prompt, status, attempt,
                 retry_of, created_at, started_at, finished_at, last_event_id,
                 failure_json
             ) VALUES (
                 '12121212-1212-4212-8212-121212121212',
                 '34343434-3434-4434-8434-343434343434',
                 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
                 'upsert', 'running', 1, NULL,
                 '2026-07-23T00:00:00.000000000Z',
                 '2026-07-23T00:00:00.000000000Z',
                 NULL, 0, NULL
             ) ON CONFLICT(id) DO UPDATE SET
                 status = 'interrupted',
                 finished_at = '2026-07-23T00:00:01.000000000Z',
                 failure_json =
                     '{\"code\":\"WRONG\",\"message\":\"wrong\",\"retryable\":true}'",
        ),
    ] {
        assert_constraint_error(case, sqlx::raw_sql(sql).execute(pool).await.map(|_| ()));
        assert_eq!(task_status(pool, FOURTH_TASK_ID).await, "running");
    }
    insert_parent_task(pool, FIFTH_TASK_ID, FIFTH_CLIENT_REQUEST_ID).await;
    assert_constraint_error(
        "update or replace identity collision",
        sqlx::query(
            "UPDATE OR REPLACE tasks \
             SET id = ?, status = 'interrupted', finished_at = ?, failure_json = ? \
             WHERE id = ?",
        )
        .bind(FOURTH_TASK_ID)
        .bind("2026-07-23T00:00:01.000000000Z")
        .bind(r#"{"code":"WRONG","message":"wrong","retryable":true}"#)
        .bind(FIFTH_TASK_ID)
        .execute(pool)
        .await
        .map(|_| ()),
    );
    assert_eq!(task_status(pool, FOURTH_TASK_ID).await, "running");
    assert_eq!(task_status(pool, FIFTH_TASK_ID).await, "running");

    insert_stop_intent(
        pool,
        SECOND_TASK_ID,
        REVIEW_REPOSITORY_ID,
        1,
        "disk_pressure_critical",
        FIXTURE_TIMESTAMP,
    )
    .await
    .unwrap();
    for (case, status, failure_json) in [
        ("cancelled disk task", "cancelled", None),
        (
            "wrong disk code",
            "failed",
            Some(
                r#"{"code":"OTHER","message":"critical disk pressure stopped the task","retryable":true}"#,
            ),
        ),
        (
            "non-retryable disk failure",
            "failed",
            Some(
                r#"{"code":"DISK_PRESSURE_CRITICAL","message":"critical disk pressure stopped the task","retryable":false}"#,
            ),
        ),
        (
            "disk failure with extra field",
            "failed",
            Some(
                r#"{"code":"DISK_PRESSURE_CRITICAL","message":"critical disk pressure stopped the task","retryable":true,"extra":1}"#,
            ),
        ),
        ("malformed disk failure", "failed", Some("{")),
        (
            "disk failure missing message",
            "failed",
            Some(r#"{"code":"DISK_PRESSURE_CRITICAL","retryable":true}"#),
        ),
        (
            "numeric disk retryable",
            "failed",
            Some(
                r#"{"code":"DISK_PRESSURE_CRITICAL","message":"critical disk pressure stopped the task","retryable":1}"#,
            ),
        ),
        (
            "empty disk message",
            "failed",
            Some(r#"{"code":"DISK_PRESSURE_CRITICAL","message":"","retryable":true}"#),
        ),
    ] {
        assert_constraint_error(
            case,
            update_task_terminal(pool, SECOND_TASK_ID, status, failure_json).await,
        );
        assert_eq!(task_status(pool, SECOND_TASK_ID).await, "running");
    }
    assert_constraint_error(
        "disk failure stored as blob",
        sqlx::query(
            "UPDATE tasks SET status = 'failed', finished_at = ?, failure_json = ? \
             WHERE id = ?",
        )
        .bind(FIXTURE_TIMESTAMP)
        .bind(
            br#"{"code":"DISK_PRESSURE_CRITICAL","message":"critical disk pressure stopped the task","retryable":true}"#
                .as_slice(),
        )
        .bind(SECOND_TASK_ID)
        .execute(pool)
        .await
        .map(|_| ()),
    );
    assert_constraint_error(
        "disk terminal timestamp stored as blob",
        sqlx::query(
            "UPDATE tasks SET status = 'failed', finished_at = ?, failure_json = ? \
             WHERE id = ?",
        )
        .bind(FIXTURE_TIMESTAMP.as_bytes())
        .bind(
            r#"{"code":"DISK_PRESSURE_CRITICAL","message":"critical disk pressure stopped the task","retryable":true}"#,
        )
        .bind(SECOND_TASK_ID)
        .execute(pool)
        .await
        .map(|_| ()),
    );
    update_task_terminal(
        pool,
        SECOND_TASK_ID,
        "failed",
        Some(
            r#"{"code":"DISK_PRESSURE_CRITICAL","message":"critical disk pressure stopped the task","retryable":true}"#,
        ),
    )
    .await
    .unwrap();
    let disk_terminal: (String, String) =
        sqlx::query_as("SELECT status, failure_json FROM tasks WHERE id = ?")
            .bind(SECOND_TASK_ID)
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(disk_terminal.0, "failed");
    assert_eq!(
        disk_terminal.1,
        r#"{"code":"DISK_PRESSURE_CRITICAL","message":"critical disk pressure stopped the task","retryable":true}"#
    );
    assert_constraint_error(
        "terminal disk failure bytes are immutable",
        sqlx::query("UPDATE tasks SET failure_json = ? WHERE id = ?")
            .bind(
                r#"{"retryable":true,"message":"critical disk pressure stopped the task","code":"DISK_PRESSURE_CRITICAL"}"#,
            )
            .bind(SECOND_TASK_ID)
            .execute(pool)
            .await
            .map(|_| ()),
    );
    assert_eq!(row_count(pool, "task_stop_intents").await, 3);
    assert_foreign_keys_clean(pool).await;
}

#[tokio::test]
async fn v3_schema_has_strict_columns_composite_keys_and_unique_parent_indexes() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    let pool = fixture.store.pool();

    assert_eq!(strict_table_flag(pool, "task_review_evidence").await, 1);
    assert_eq!(strict_table_flag(pool, "task_delivery_state").await, 1);

    let evidence_columns = table_columns(pool, "task_review_evidence").await;
    for (name, data_type, primary_key_position) in [
        ("task_id", "TEXT", 1),
        ("repository_id", "TEXT", 0),
        ("attempt", "INTEGER", 0),
        ("review_round", "INTEGER", 2),
        ("workspace_generation", "INTEGER", 0),
        ("digest_algorithm", "TEXT", 0),
        ("workspace_digest", "TEXT", 0),
        ("decision_source", "TEXT", 0),
        ("verdict", "TEXT", 0),
        ("summary", "TEXT", 0),
        ("findings_json", "TEXT", 0),
        ("added_checks_json", "TEXT", 0),
        ("required_checks_json", "TEXT", 0),
        ("check_evidence_json", "TEXT", 0),
        ("coverage_json", "TEXT", 0),
        ("created_at", "TEXT", 0),
        ("event_id", "INTEGER", 0),
        ("event_kind", "TEXT", 0),
    ] {
        assert_required_column(&evidence_columns, name, data_type, primary_key_position);
    }

    let delivery_columns = table_columns(pool, "task_delivery_state").await;
    for (name, data_type, primary_key_position) in [
        ("task_id", "TEXT", 1),
        ("readiness", "TEXT", 0),
        ("final_review_round", "INTEGER", 0),
        ("final_verdict", "TEXT", 0),
        ("decided_at", "TEXT", 0),
    ] {
        assert_required_column(&delivery_columns, name, data_type, primary_key_position);
    }

    let evidence_unique_indexes = unique_index_columns(pool, "task_review_evidence").await;
    assert!(
        evidence_unique_indexes.contains(&vec!["event_id".to_owned()]),
        "event_id must be independently unique: {evidence_unique_indexes:?}"
    );
    assert!(
        evidence_unique_indexes.contains(&vec![
            "task_id".to_owned(),
            "review_round".to_owned(),
            "verdict".to_owned(),
        ]),
        "delivery parent tuple must be unique: {evidence_unique_indexes:?}"
    );
    let event_unique_indexes = unique_index_columns(pool, "task_events").await;
    assert!(
        event_unique_indexes.contains(&vec![
            "id".to_owned(),
            "task_id".to_owned(),
            "kind".to_owned(),
        ]),
        "review evidence event parent tuple must be unique: {event_unique_indexes:?}"
    );

    let evidence_foreign_keys = foreign_keys(pool, "task_review_evidence").await;
    assert!(
        evidence_foreign_keys.contains(&ForeignKey {
            parent_table: "tasks".to_owned(),
            columns: vec![
                ("task_id".to_owned(), "id".to_owned()),
                ("repository_id".to_owned(), "repository_id".to_owned()),
                ("attempt".to_owned(), "attempt".to_owned()),
            ],
        }),
        "missing task identity foreign key: {evidence_foreign_keys:?}"
    );
    assert!(
        evidence_foreign_keys.contains(&ForeignKey {
            parent_table: "task_events".to_owned(),
            columns: vec![
                ("event_id".to_owned(), "id".to_owned()),
                ("task_id".to_owned(), "task_id".to_owned()),
                ("event_kind".to_owned(), "kind".to_owned()),
            ],
        }),
        "missing event tuple foreign key: {evidence_foreign_keys:?}"
    );
    let delivery_foreign_keys = foreign_keys(pool, "task_delivery_state").await;
    assert!(
        delivery_foreign_keys.contains(&ForeignKey {
            parent_table: "task_review_evidence".to_owned(),
            columns: vec![
                ("task_id".to_owned(), "task_id".to_owned()),
                ("final_review_round".to_owned(), "review_round".to_owned(),),
                ("final_verdict".to_owned(), "verdict".to_owned()),
            ],
        }),
        "missing verdict-aware delivery foreign key: {delivery_foreign_keys:?}"
    );

    let evidence_sql = normalized_schema_sql(pool, "table", "task_review_evidence").await;
    for required in [
        "strict",
        "typeof(attempt)",
        "typeof(review_round)",
        "typeof(workspace_generation)",
        "typeof(event_id)",
        "json_valid",
        "json_type",
        "9007199254740991",
        "workspace_fingerprint_v1",
        "*[^0-9a-f]*",
        "review.updated",
        "reviewer",
        "system",
        "approved",
        "changes_requested",
    ] {
        assert!(
            evidence_sql.contains(required),
            "task_review_evidence is missing DDL term {required}: {evidence_sql}"
        );
    }
    let delivery_sql = normalized_schema_sql(pool, "table", "task_delivery_state").await;
    for required in [
        "strict",
        "typeof(final_review_round)",
        "review_approved",
        "review_rejected",
        "approved",
        "changes_requested",
    ] {
        assert!(
            delivery_sql.contains(required),
            "task_delivery_state is missing DDL term {required}: {delivery_sql}"
        );
    }

    assert_immutable_trigger_shape(pool, "task_review_evidence").await;
    assert_immutable_trigger_shape(pool, "task_delivery_state").await;
}

#[tokio::test]
async fn v3_evidence_constraints_reject_invalid_source_json_ranges_digest_and_event_tuple() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    let pool = fixture.store.pool();
    normalized_schema_sql(pool, "table", "task_review_evidence").await;
    let parents = seed_review_parents(pool).await;

    let mut known_good =
        EvidenceInsert::changes_requested(SECOND_TASK_ID, parents.second_task_event_id);
    known_good.review_round = 1;
    known_good.summary = "\0";
    insert_evidence(pool, known_good).await.unwrap();

    let baseline = EvidenceInsert::changes_requested(FIRST_TASK_ID, parents.first_task_event_id);
    let mut invalid_rows = Vec::new();

    let mut system_approved = baseline;
    system_approved.decision_source = "system";
    system_approved.verdict = "approved";
    system_approved.coverage_json = Some("{}");
    invalid_rows.push(("system + approved", system_approved));

    let mut empty_summary = baseline;
    empty_summary.summary = "";
    invalid_rows.push(("empty summary", empty_summary));

    let mut sql_null_coverage = baseline;
    sql_null_coverage.coverage_json = None;
    invalid_rows.push(("SQL NULL coverage", sql_null_coverage));

    for (name, field, value) in [
        ("malformed findings JSON", "findings", "["),
        ("object findings JSON", "findings", "{}"),
        ("malformed added checks JSON", "added_checks", "["),
        ("object added checks JSON", "added_checks", "{}"),
        ("malformed required checks JSON", "required_checks", "["),
        ("object required checks JSON", "required_checks", "{}"),
        ("malformed check evidence JSON", "check_evidence", "["),
        ("object check evidence JSON", "check_evidence", "{}"),
        ("malformed coverage JSON", "coverage", "{"),
        ("array coverage JSON", "coverage", "[]"),
    ] {
        let mut row = baseline;
        match field {
            "findings" => row.findings_json = value,
            "added_checks" => row.added_checks_json = value,
            "required_checks" => row.required_checks_json = value,
            "check_evidence" => row.check_evidence_json = value,
            "coverage" => row.coverage_json = Some(value),
            _ => unreachable!(),
        }
        invalid_rows.push((name, row));
    }

    for (name, round) in [("round zero", 0), ("round four", 4)] {
        let mut row = baseline;
        row.review_round = round;
        invalid_rows.push((name, row));
    }
    for (name, generation) in [
        ("negative generation", -1),
        (
            "generation above JavaScript safe integer",
            9_007_199_254_740_992,
        ),
    ] {
        let mut row = baseline;
        row.workspace_generation = generation;
        invalid_rows.push((name, row));
    }

    let mut unknown_algorithm = baseline;
    unknown_algorithm.digest_algorithm = "sha256";
    invalid_rows.push(("unknown digest algorithm", unknown_algorithm));
    for (name, digest) in [
        (
            "short digest",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ),
        (
            "uppercase digest",
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        ),
        (
            "non hexadecimal digest",
            "gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg",
        ),
    ] {
        let mut row = baseline;
        row.workspace_digest = digest;
        invalid_rows.push((name, row));
    }

    let mut wrong_task_event = baseline;
    wrong_task_event.event_id = parents.second_task_unreferenced_event_id;
    invalid_rows.push(("event belongs to another task", wrong_task_event));
    let mut wrong_kind_event = baseline;
    wrong_kind_event.event_id = parents.first_task_plan_event_id;
    invalid_rows.push(("event has another kind", wrong_kind_event));
    let mut mutable_event_kind = baseline;
    mutable_event_kind.event_kind = "plan.updated";
    invalid_rows.push(("evidence event kind is not fixed", mutable_event_kind));

    for (case, row) in invalid_rows {
        let error = insert_evidence(pool, row)
            .await
            .expect_err("invalid evidence unexpectedly inserted");
        assert!(
            matches!(&error, sqlx::Error::Database(_)),
            "{case} failed for a non-constraint reason: {error}"
        );
        assert_eq!(
            row_count(pool, "task_review_evidence").await,
            1,
            "{case} inserted despite constraint; result was {error}"
        );
    }

    assert_foreign_keys_clean(pool).await;
}

#[tokio::test]
async fn v3_evidence_json_counts_and_encoded_size_are_bounded() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    let pool = fixture.store.pool();
    let parents = seed_review_parents(pool).await;

    insert_evidence(
        pool,
        EvidenceInsert::changes_requested(SECOND_TASK_ID, parents.second_task_event_id),
    )
    .await
    .unwrap();
    let baseline = EvidenceInsert::changes_requested(FIRST_TASK_ID, parents.first_task_event_id);

    let too_many_findings = json_array_of_empty_objects(33);
    assert_invalid_evidence(
        pool,
        "33 findings",
        EvidenceInsert {
            findings_json: &too_many_findings,
            ..baseline
        },
        1,
    )
    .await;

    let too_many_added_checks = json_array_of_empty_objects(17);
    assert_invalid_evidence(
        pool,
        "17 added checks",
        EvidenceInsert {
            added_checks_json: &too_many_added_checks,
            ..baseline
        },
        1,
    )
    .await;

    assert_invalid_evidence(
        pool,
        "zero required checks",
        EvidenceInsert {
            required_checks_json: "[]",
            ..baseline
        },
        1,
    )
    .await;
    let too_many_required_checks = json_array_of_empty_objects(17);
    assert_invalid_evidence(
        pool,
        "17 required checks",
        EvidenceInsert {
            required_checks_json: &too_many_required_checks,
            ..baseline
        },
        1,
    )
    .await;

    let too_many_check_evidence = json_array_of_empty_objects(17);
    assert_invalid_evidence(
        pool,
        "17 check evidence entries",
        EvidenceInsert {
            check_evidence_json: &too_many_check_evidence,
            ..baseline
        },
        1,
    )
    .await;

    let oversized_summary = "x".repeat(132 * 1024);
    assert_invalid_evidence(
        pool,
        "encoded evidence above 128 KiB",
        EvidenceInsert {
            summary: &oversized_summary,
            ..baseline
        },
        1,
    )
    .await;

    assert_foreign_keys_clean(pool).await;
}

#[tokio::test]
async fn v3_review_events_require_the_exact_non_empty_marker() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    let pool = fixture.store.pool();
    let parents = seed_review_parents(pool).await;

    for (case, schema_version, payload) in [
        ("empty object", 1, "{}"),
        ("non-canonical whitespace", 1, r#"{"evidence_ref": true}"#),
        ("wrong schema version", 2, r#"{"evidence_ref":true}"#),
    ] {
        let result = sqlx::query(
            "INSERT INTO task_events (
                 schema_version, task_id, kind, payload_json, created_at
             ) VALUES (?, ?, 'review.updated', ?, ?)",
        )
        .bind(schema_version)
        .bind(FIRST_TASK_ID)
        .bind(payload)
        .bind(FIXTURE_TIMESTAMP)
        .execute(pool)
        .await;
        assert!(
            matches!(result, Err(sqlx::Error::Database(_))),
            "{case} review marker unexpectedly accepted: {result:?}"
        );
    }

    sqlx::query("UPDATE task_events SET payload_json = '{}' WHERE id = ?")
        .bind(parents.first_task_event_id)
        .execute(pool)
        .await
        .unwrap_err();
    sqlx::query(
        "UPDATE task_events SET kind = 'review.updated' \
         WHERE id = ?",
    )
    .bind(parents.first_task_plan_event_id)
    .execute(pool)
    .await
    .unwrap_err();
    let preserved: (i64, String, String) =
        sqlx::query_as("SELECT schema_version, kind, payload_json FROM task_events WHERE id = ?")
            .bind(parents.first_task_event_id)
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(
        preserved,
        (
            1,
            "review.updated".to_owned(),
            r#"{"evidence_ref":true}"#.to_owned(),
        )
    );
    assert_foreign_keys_clean(pool).await;
}

#[tokio::test]
async fn v3_delivery_constraints_enforce_readiness_verdict_and_final_round_matrix() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    let pool = fixture.store.pool();
    let parents = seed_review_parents(pool).await;

    insert_evidence(
        pool,
        EvidenceInsert::approved(FIRST_TASK_ID, parents.first_task_event_id, 1),
    )
    .await
    .unwrap();
    let mut changes_requested =
        EvidenceInsert::changes_requested(SECOND_TASK_ID, parents.second_task_event_id);
    changes_requested.review_round = 2;
    insert_evidence(pool, changes_requested).await.unwrap();

    for (case, task_id, readiness, round, verdict) in [
        (
            "unknown readiness",
            FIRST_TASK_ID,
            "unreviewed",
            1,
            "approved",
        ),
        (
            "rejected readiness with approved verdict",
            FIRST_TASK_ID,
            "review_rejected",
            1,
            "approved",
        ),
        (
            "approved readiness with changes-requested verdict",
            SECOND_TASK_ID,
            "review_approved",
            2,
            "changes_requested",
        ),
        (
            "rejected readiness before round three",
            SECOND_TASK_ID,
            "review_rejected",
            2,
            "changes_requested",
        ),
        (
            "delivery without matching evidence",
            "ffffffff-ffff-4fff-8fff-ffffffffffff",
            "review_approved",
            1,
            "approved",
        ),
    ] {
        let result = insert_delivery(pool, task_id, readiness, round, verdict).await;
        assert!(
            matches!(result, Err(sqlx::Error::Database(_))),
            "{case} unexpectedly inserted: {result:?}"
        );
    }

    insert_delivery(pool, FIRST_TASK_ID, "review_approved", 1, "approved")
        .await
        .unwrap();
    assert_eq!(row_count(pool, "task_delivery_state").await, 1);
    assert_foreign_keys_clean(pool).await;
}

#[tokio::test]
async fn v3_evidence_and_delivery_rows_are_immutable() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    let pool = fixture.store.pool();
    let parents = seed_review_parents(pool).await;

    let evidence = EvidenceInsert::approved(FIRST_TASK_ID, parents.first_task_event_id, 1);
    insert_evidence(pool, evidence).await.unwrap();

    sqlx::query(
        "UPDATE task_review_evidence SET summary = 'mutated' \
         WHERE task_id = ? AND review_round = 1",
    )
    .bind(FIRST_TASK_ID)
    .execute(pool)
    .await
    .unwrap_err();
    sqlx::query(
        "DELETE FROM task_review_evidence \
         WHERE task_id = ? AND review_round = 1",
    )
    .bind(FIRST_TASK_ID)
    .execute(pool)
    .await
    .unwrap_err();
    sqlx::query(
        "REPLACE INTO task_review_evidence (
             task_id, repository_id, attempt, review_round,
             workspace_generation, digest_algorithm, workspace_digest,
             decision_source, verdict, summary, findings_json,
             added_checks_json, required_checks_json, check_evidence_json,
             coverage_json, created_at, event_id, event_kind
         )
         SELECT task_id, repository_id, attempt, review_round,
                workspace_generation, digest_algorithm, workspace_digest,
                decision_source, verdict, 'replaced-again', findings_json,
                added_checks_json, required_checks_json, check_evidence_json,
                coverage_json, created_at, event_id, event_kind
         FROM task_review_evidence
         WHERE task_id = ? AND review_round = 1",
    )
    .bind(FIRST_TASK_ID)
    .execute(pool)
    .await
    .unwrap_err();
    sqlx::query(
        "INSERT OR REPLACE INTO task_review_evidence (
             task_id, repository_id, attempt, review_round,
             workspace_generation, digest_algorithm, workspace_digest,
             decision_source, verdict, summary, findings_json,
             added_checks_json, required_checks_json, check_evidence_json,
             coverage_json, created_at, event_id, event_kind
         )
         SELECT task_id, repository_id, attempt, review_round,
                workspace_generation, digest_algorithm, workspace_digest,
                decision_source, verdict, 'replaced', findings_json,
                added_checks_json, required_checks_json, check_evidence_json,
                coverage_json, created_at, event_id, event_kind
         FROM task_review_evidence
         WHERE task_id = ? AND review_round = 1",
    )
    .bind(FIRST_TASK_ID)
    .execute(pool)
    .await
    .unwrap_err();
    let summary: String = sqlx::query_scalar(
        "SELECT summary FROM task_review_evidence \
         WHERE task_id = ? AND review_round = 1",
    )
    .bind(FIRST_TASK_ID)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(summary, "fixture review");

    insert_delivery(pool, FIRST_TASK_ID, "review_approved", 1, "approved")
        .await
        .unwrap();
    sqlx::query("UPDATE task_delivery_state SET decided_at = 'mutated' WHERE task_id = ?")
        .bind(FIRST_TASK_ID)
        .execute(pool)
        .await
        .unwrap_err();
    sqlx::query("DELETE FROM task_delivery_state WHERE task_id = ?")
        .bind(FIRST_TASK_ID)
        .execute(pool)
        .await
        .unwrap_err();
    sqlx::query(
        "INSERT OR REPLACE INTO task_delivery_state (
             task_id, readiness, final_review_round, final_verdict, decided_at
         )
         SELECT task_id, readiness, final_review_round, final_verdict, 'replaced'
         FROM task_delivery_state
         WHERE task_id = ?",
    )
    .bind(FIRST_TASK_ID)
    .execute(pool)
    .await
    .unwrap_err();
    sqlx::query(
        "REPLACE INTO task_delivery_state (
             task_id, readiness, final_review_round, final_verdict, decided_at
         )
         SELECT task_id, readiness, final_review_round, final_verdict, 'replaced-again'
         FROM task_delivery_state
         WHERE task_id = ?",
    )
    .bind(FIRST_TASK_ID)
    .execute(pool)
    .await
    .unwrap_err();
    let delivery: (String, i64, String, String) = sqlx::query_as(
        "SELECT readiness, final_review_round, final_verdict, decided_at \
         FROM task_delivery_state WHERE task_id = ?",
    )
    .bind(FIRST_TASK_ID)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(
        delivery,
        (
            "review_approved".to_owned(),
            1,
            "approved".to_owned(),
            FIXTURE_TIMESTAMP.to_owned(),
        )
    );

    assert_foreign_keys_clean(pool).await;
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
async fn version_one_database_upgrades_to_v4_and_repeat_is_a_no_op() {
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
    assert_eq!(versions, vec![1, 2, 3, 4]);
    for table in [
        "task_attempt_artifacts",
        "task_review_evidence",
        "task_delivery_state",
        "task_stop_intents",
    ] {
        assert_eq!(schema_object_count(store.pool(), "table", table).await, 1);
    }
    assert_eq!(row_count(store.pool(), "task_attempt_artifacts").await, 0);
    assert_eq!(row_count(store.pool(), "task_review_evidence").await, 0);
    assert_eq!(row_count(store.pool(), "task_delivery_state").await, 0);
    assert_eq!(row_count(store.pool(), "task_stop_intents").await, 0);
    assert_legacy_rows_preserved(store.pool()).await;
    assert_foreign_keys_clean(store.pool()).await;
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

#[tokio::test]
async fn version_two_database_upgrades_to_v4_without_rewriting_existing_rows() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v2.sqlite3");
    seed_v2(&path, false).await;
    let store = Store::open(&path).await.unwrap();

    store.migrate().await.unwrap();
    store.migrate().await.unwrap();

    let versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM schema_migrations ORDER BY version")
            .fetch_all(store.pool())
            .await
            .unwrap();
    assert_eq!(versions, vec![1, 2, 3, 4]);
    assert_eq!(row_count(store.pool(), "task_attempt_artifacts").await, 1);
    assert_eq!(row_count(store.pool(), "task_review_evidence").await, 0);
    assert_eq!(row_count(store.pool(), "task_delivery_state").await, 0);
    assert_eq!(row_count(store.pool(), "task_stop_intents").await, 0);

    let artifact: (String, String, i64, String) = sqlx::query_as(
        "SELECT task_id, repository_id, attempt, state \
         FROM task_attempt_artifacts \
         WHERE task_id = '22222222-2222-4222-8222-222222222222'",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(
        artifact,
        (
            "22222222-2222-4222-8222-222222222222".to_owned(),
            "11111111-1111-4111-8111-111111111111".to_owned(),
            1,
            "ready".to_owned(),
        )
    );
    assert_legacy_rows_preserved(store.pool()).await;
    assert_foreign_keys_clean(store.pool()).await;
}

#[tokio::test]
async fn failed_v3_upgrade_rolls_back_every_v3_statement_and_preserves_v2() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("conflicting-v3.sqlite3");
    seed_v2(&path, true).await;
    let store = Store::open(&path).await.unwrap();

    store.migrate().await.unwrap_err();

    let versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM schema_migrations ORDER BY version")
            .fetch_all(store.pool())
            .await
            .unwrap();
    assert_eq!(versions, vec![1, 2]);
    let conflict_sql: String = sqlx::query_scalar(
        "SELECT sql FROM sqlite_master \
         WHERE type = 'table' AND name = 'task_delivery_state'",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert!(conflict_sql.contains("preserve_marker"));
    assert_eq!(
        schema_object_count(store.pool(), "table", "task_review_evidence").await,
        0
    );
    assert_eq!(row_count(store.pool(), "task_attempt_artifacts").await, 1);
    assert!(
        !unique_index_columns(store.pool(), "task_events")
            .await
            .contains(&vec![
                "id".to_owned(),
                "task_id".to_owned(),
                "kind".to_owned(),
            ]),
        "the v3 event parent index must roll back"
    );
    let v3_trigger_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master \
         WHERE type = 'trigger' AND (
             tbl_name = 'task_review_evidence'
             OR tbl_name = 'task_delivery_state'
             OR name LIKE 'task_events_review_%'
             OR name LIKE 'tasks_reviewed_terminal_%'
         )",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(v3_trigger_count, 0);
    assert_legacy_rows_preserved(store.pool()).await;
    assert_foreign_keys_clean(store.pool()).await;
}

#[tokio::test]
async fn version_three_database_upgrades_to_v4_without_rewriting_quality_or_event_rows() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v3.sqlite3");
    seed_v3(&path).await;
    let store = Store::open(&path).await.unwrap();
    let parents = seed_review_parents(store.pool()).await;
    seed_all_persisted_event_kinds(store.pool(), FIRST_TASK_ID).await;
    insert_evidence(
        store.pool(),
        EvidenceInsert::approved(FIRST_TASK_ID, parents.first_task_event_id, 1),
    )
    .await
    .unwrap();
    insert_delivery(
        store.pool(),
        FIRST_TASK_ID,
        "review_approved",
        1,
        "approved",
    )
    .await
    .unwrap();
    insert_evidence(
        store.pool(),
        EvidenceInsert::changes_requested(SECOND_TASK_ID, parents.second_task_event_id),
    )
    .await
    .unwrap();

    let quality_before = quality_rows_snapshot(store.pool()).await;
    let events_before = event_rows_snapshot(store.pool()).await;
    let event_kinds_before: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT kind FROM task_events ORDER BY kind")
            .fetch_all(store.pool())
            .await
            .unwrap();
    assert_eq!(event_kinds_before, persisted_event_kinds());
    let event_schema_versions_before: Vec<i64> = sqlx::query_scalar(
        "SELECT DISTINCT schema_version FROM task_events ORDER BY schema_version",
    )
    .fetch_all(store.pool())
    .await
    .unwrap();
    assert_eq!(event_schema_versions_before, vec![1]);
    store.migrate().await.unwrap();
    let schema_after_first = schema_snapshot(store.pool()).await;
    store.close().await;

    let store = Store::open(&path).await.unwrap();
    store.migrate().await.unwrap();

    let versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM schema_migrations ORDER BY version")
            .fetch_all(store.pool())
            .await
            .unwrap();
    assert_eq!(versions, vec![1, 2, 3, 4]);
    assert_eq!(quality_rows_snapshot(store.pool()).await, quality_before);
    assert_eq!(event_rows_snapshot(store.pool()).await, events_before);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_events WHERE schema_version != 1")
            .fetch_one(store.pool())
            .await
            .unwrap(),
        0
    );
    assert_eq!(schema_snapshot(store.pool()).await, schema_after_first);
    assert_eq!(row_count(store.pool(), "task_stop_intents").await, 0);
    assert_foreign_keys_clean(store.pool()).await;
}

#[tokio::test]
async fn every_v4_statement_failure_rolls_back_the_entire_upgrade() {
    let conflicts = [
        ("table", "task_stop_intents"),
        ("trigger", "task_stop_intents_running_unreviewed_on_insert"),
        ("trigger", "task_stop_intents_no_replace"),
        ("trigger", "task_stop_intents_no_update"),
        ("trigger", "task_stop_intents_no_delete"),
        ("trigger", "tasks_stop_intent_no_replace"),
        ("trigger", "tasks_stop_intent_identity_collision_on_update"),
        ("trigger", "tasks_stop_intent_terminal_on_update"),
        ("trigger", "task_review_evidence_stop_intent_on_insert"),
        ("trigger", "task_delivery_state_stop_intent_on_insert"),
        ("trigger", "task_events_stop_intent_review_on_insert"),
        ("trigger", "task_events_stop_intent_review_on_update"),
        ("index", "tasks_queued_created_at_id"),
        ("migration_receipt", "reject_v4_migration_receipt"),
        ("migration_receipt_ignore", "ignore_v4_migration_receipt"),
        ("migration_receipt_delete", "delete_v4_migration_receipt"),
    ];

    for (kind, name) in conflicts {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(format!("v4-conflict-{name}.sqlite3"));
        seed_v3(&path).await;
        seed_schema_conflict(&path, kind, name).await;
        let store = Store::open(&path).await.unwrap();
        let before = schema_snapshot(store.pool()).await;

        let error = store
            .migrate()
            .await
            .expect_err("conflicting v4 object unexpectedly migrated");

        assert_eq!(
            schema_snapshot(store.pool()).await,
            before,
            "{kind} {name} left partial v4 schema after {error}"
        );
        let versions: Vec<i64> =
            sqlx::query_scalar("SELECT version FROM schema_migrations ORDER BY version")
                .fetch_all(store.pool())
                .await
                .unwrap();
        assert_eq!(versions, vec![1, 2, 3], "{kind} {name}");
        assert_legacy_rows_preserved(store.pool()).await;
        assert!(
            !error.to_string().contains(&path.display().to_string()),
            "migration error leaked database path: {error}"
        );
    }
}

#[tokio::test]
async fn invalid_migration_histories_fail_closed_before_any_schema_write() {
    let cases = [
        ("empty existing history", CANONICAL_HISTORY_SCHEMA, ""),
        (
            "future version",
            CANONICAL_HISTORY_SCHEMA,
            "INSERT INTO schema_migrations VALUES
                 (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five');",
        ),
        (
            "zero version",
            CANONICAL_HISTORY_SCHEMA,
            "INSERT INTO schema_migrations VALUES (0, 'zero');",
        ),
        (
            "negative version",
            CANONICAL_HISTORY_SCHEMA,
            "INSERT INTO schema_migrations VALUES (-1, 'negative');",
        ),
        (
            "missing version one",
            CANONICAL_HISTORY_SCHEMA,
            "INSERT INTO schema_migrations VALUES (2, 'two');",
        ),
        (
            "internal gap",
            CANONICAL_HISTORY_SCHEMA,
            "INSERT INTO schema_migrations VALUES (1, 'one'), (3, 'three');",
        ),
        (
            "duplicate version",
            "CREATE TABLE schema_migrations (version INTEGER, applied_at TEXT NOT NULL);",
            "INSERT INTO schema_migrations VALUES (1, 'first'), (1, 'duplicate');",
        ),
        (
            "text version",
            "CREATE TABLE schema_migrations (version TEXT, applied_at TEXT NOT NULL);",
            "INSERT INTO schema_migrations VALUES ('1', 'text');",
        ),
        (
            "mixed case history name",
            "CREATE TABLE Schema_Migrations (
                 version INTEGER PRIMARY KEY,
                 applied_at TEXT NOT NULL
             );",
            "INSERT INTO Schema_Migrations VALUES (99, 'future');",
        ),
        (
            "null version",
            "CREATE TABLE schema_migrations (version INTEGER, applied_at TEXT NOT NULL);",
            "INSERT INTO schema_migrations VALUES (NULL, 'null');",
        ),
    ];

    for (case, schema, rows) in cases {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(format!(
            "invalid-history-{}.sqlite3",
            case.replace(' ', "-")
        ));
        seed_migration_history(&path, schema, rows).await;
        let store = Store::open(&path).await.unwrap();
        let schema_before = schema_snapshot(store.pool()).await;
        let history_before = migration_history_snapshot(store.pool()).await;

        let error = store
            .migrate()
            .await
            .expect_err("invalid migration history unexpectedly accepted");

        assert_eq!(
            error.to_string(),
            DATABASE_SCHEMA_UNSUPPORTED,
            "{case} returned an unstable error: {error:?}"
        );
        assert_eq!(
            schema_snapshot(store.pool()).await,
            schema_before,
            "{case} changed schema before rejecting history"
        );
        assert_eq!(
            migration_history_snapshot(store.pool()).await,
            history_before,
            "{case} rewrote migration history"
        );
        assert_eq!(
            schema_object_count(store.pool(), "table", "repositories").await,
            0,
            "{case} created application schema"
        );
        assert!(
            !error.to_string().contains(&path.display().to_string()),
            "{case} leaked database path"
        );
    }
}

#[tokio::test]
async fn temporary_migration_table_cannot_shadow_main_history() {
    let store = Store::open(":memory:").await.unwrap();
    sqlx::raw_sql(
        "CREATE TEMP TABLE schema_migrations (
             version INTEGER PRIMARY KEY,
             applied_at TEXT NOT NULL
         );
         INSERT INTO temp.schema_migrations VALUES (99, 'temporary');",
    )
    .execute(store.pool())
    .await
    .unwrap();

    store.migrate().await.unwrap();

    let main_versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM main.schema_migrations ORDER BY version")
            .fetch_all(store.pool())
            .await
            .unwrap();
    let temporary_versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM temp.schema_migrations ORDER BY version")
            .fetch_all(store.pool())
            .await
            .unwrap();
    assert_eq!(main_versions, vec![1, 2, 3, 4]);
    assert_eq!(temporary_versions, vec![99]);
}

#[tokio::test]
async fn v4_upgrade_checks_existing_foreign_keys_before_commit() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v3-corrupt-foreign-key.sqlite3");
    seed_v3(&path).await;
    corrupt_v3_foreign_key(&path).await;
    let store = Store::open(&path).await.unwrap();
    let before = schema_snapshot(store.pool()).await;

    store
        .migrate()
        .await
        .expect_err("foreign-key corruption unexpectedly migrated");

    assert_eq!(schema_snapshot(store.pool()).await, before);
    let versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM schema_migrations ORDER BY version")
            .fetch_all(store.pool())
            .await
            .unwrap();
    assert_eq!(versions, vec![1, 2, 3]);
    assert_eq!(
        schema_object_count(store.pool(), "table", "task_stop_intents").await,
        0
    );
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
    sqlx::raw_sql(
        "INSERT INTO repositories (
             id, selected_path, display_name, git_root, cargo_workspace_root,
             git_identity_key, cargo_identity_key, created_at, last_opened_at
         ) VALUES (
             '11111111-1111-4111-8111-111111111111',
             'C:/legacy/selected', 'legacy repository',
             'C:/legacy/git', 'C:/legacy/git', 'legacy-git', 'legacy-cargo',
             '2026-07-16T00:00:00.000000000Z',
             '2026-07-16T00:00:00.000000000Z'
         );
         INSERT INTO tasks (
             id, client_request_id, repository_id, prompt, status, attempt,
             retry_of, created_at, started_at, finished_at, last_event_id,
             failure_json
         ) VALUES (
             '22222222-2222-4222-8222-222222222222',
             '33333333-3333-4333-8333-333333333333',
             '11111111-1111-4111-8111-111111111111',
             'preserve this completed task', 'completed', 1, NULL,
             '2026-07-16T00:00:00.000000000Z',
             '2026-07-16T00:00:01.000000000Z',
             '2026-07-16T00:00:02.000000000Z', 41, NULL
         );
         INSERT INTO task_events (
             id, schema_version, task_id, kind, payload_json, created_at
         ) VALUES (
             41, 1, '22222222-2222-4222-8222-222222222222', 'plan.updated',
             '{\"plan\":{\"revision\":7,\"items\":[{\"id\":\"legacy-step\",\"title\":\"legacy\",\"status\":\"completed\"}]}}',
             '2026-07-16T00:00:01.000000000Z'
         );",
    )
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

async fn seed_v2(path: &Path, conflicting_v3_table: bool) {
    seed_v1(path, false).await;

    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true);
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    sqlx::raw_sql(include_str!(
        "../migrations/0002_task_attempt_artifacts.sql"
    ))
    .execute(&mut connection)
    .await
    .unwrap();
    sqlx::query("INSERT INTO schema_migrations (version, applied_at) VALUES (2, ?)")
        .bind("2026-07-17T00:00:00.000000000Z")
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO task_attempt_artifacts (
             task_id, repository_id, attempt, base_commit, branch_name,
             worktree_path, state, failure_code, created_at, updated_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)",
    )
    .bind("22222222-2222-4222-8222-222222222222")
    .bind("11111111-1111-4111-8111-111111111111")
    .bind(1_i64)
    .bind("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    .bind("coding-agent/legacy-task")
    .bind("C:/legacy/worktree")
    .bind("ready")
    .bind("2026-07-16T00:00:00.000000000Z")
    .bind("2026-07-16T00:00:00.000000000Z")
    .execute(&mut connection)
    .await
    .unwrap();
    if conflicting_v3_table {
        sqlx::query("CREATE TABLE task_delivery_state (preserve_marker TEXT NOT NULL)")
            .execute(&mut connection)
            .await
            .unwrap();
    }
    connection.close().await.unwrap();
}

async fn seed_v3(path: &Path) {
    seed_v2(path, false).await;

    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true);
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    sqlx::raw_sql(include_str!("../migrations/0003_multi_role_quality.sql"))
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query("INSERT INTO schema_migrations (version, applied_at) VALUES (3, ?)")
        .bind("2026-07-23T00:00:00.000000000Z")
        .execute(&mut connection)
        .await
        .unwrap();
    connection.close().await.unwrap();
}

async fn seed_schema_conflict(path: &Path, kind: &str, name: &str) {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true);
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    let sql = match kind {
        "table" => format!("CREATE TABLE {name} (preserve_marker TEXT NOT NULL)"),
        "trigger" => format!(
            "CREATE TRIGGER {name} BEFORE UPDATE ON repositories \
             BEGIN SELECT 1; END"
        ),
        "index" => format!("CREATE INDEX {name} ON repositories(created_at)"),
        "migration_receipt" => format!(
            "CREATE TRIGGER {name} BEFORE INSERT ON schema_migrations \
             WHEN NEW.version = 4 \
             BEGIN SELECT RAISE(ABORT, 'injected v4 receipt failure'); END"
        ),
        "migration_receipt_ignore" => format!(
            "CREATE TRIGGER {name} BEFORE INSERT ON schema_migrations \
             WHEN NEW.version = 4 \
             BEGIN SELECT RAISE(IGNORE); END"
        ),
        "migration_receipt_delete" => format!(
            "CREATE TRIGGER {name} AFTER INSERT ON schema_migrations \
             WHEN NEW.version = 4 \
             BEGIN DELETE FROM schema_migrations WHERE version = 4; END"
        ),
        other => panic!("unsupported conflict kind {other}"),
    };
    sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
        .execute(&mut connection)
        .await
        .unwrap();
    connection.close().await.unwrap();
}

async fn seed_migration_history(path: &Path, schema: &str, rows: &str) {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true);
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(schema.to_owned()))
        .execute(&mut connection)
        .await
        .unwrap();
    if !rows.is_empty() {
        sqlx::raw_sql(sqlx::AssertSqlSafe(rows.to_owned()))
            .execute(&mut connection)
            .await
            .unwrap();
    }
    connection.close().await.unwrap();
}

async fn corrupt_v3_foreign_key(path: &Path) {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .foreign_keys(false);
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    sqlx::query(
        "UPDATE task_attempt_artifacts \
         SET repository_id = 'ffffffff-ffff-4fff-8fff-ffffffffffff' \
         WHERE task_id = '22222222-2222-4222-8222-222222222222'",
    )
    .execute(&mut connection)
    .await
    .unwrap();
    connection.close().await.unwrap();
}

async fn assert_legacy_rows_preserved(pool: &sqlx::SqlitePool) {
    let task: (String, String, i64, i64) = sqlx::query_as(
        "SELECT repository_id, status, attempt, last_event_id \
         FROM tasks WHERE id = '22222222-2222-4222-8222-222222222222'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(
        task,
        (
            "11111111-1111-4111-8111-111111111111".to_owned(),
            "completed".to_owned(),
            1,
            41,
        )
    );
    let event: (String, String) =
        sqlx::query_as("SELECT kind, payload_json FROM task_events WHERE id = 41")
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(event.0, "plan.updated");
    assert_eq!(
        event.1,
        "{\"plan\":{\"revision\":7,\"items\":[{\"id\":\"legacy-step\",\"title\":\"legacy\",\"status\":\"completed\"}]}}"
    );
}

async fn assert_foreign_keys_clean(pool: &sqlx::SqlitePool) {
    let violations = sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(pool)
        .await
        .unwrap();
    assert!(
        violations.is_empty(),
        "foreign key violations: {violations:?}"
    );
}

async fn schema_snapshot(pool: &sqlx::SqlitePool) -> Vec<(String, String, String, Option<String>)> {
    sqlx::query_as(
        "SELECT type, name, tbl_name, sql \
         FROM sqlite_master \
         WHERE name NOT LIKE 'sqlite_%' \
         ORDER BY type, name",
    )
    .fetch_all(pool)
    .await
    .unwrap()
}

async fn migration_history_snapshot(
    pool: &sqlx::SqlitePool,
) -> Vec<(Option<String>, String, String)> {
    sqlx::query_as(
        "SELECT CAST(version AS TEXT), typeof(version), applied_at \
         FROM schema_migrations ORDER BY rowid",
    )
    .fetch_all(pool)
    .await
    .unwrap()
}

async fn quality_rows_snapshot(pool: &sqlx::SqlitePool) -> (Vec<String>, Vec<String>) {
    let evidence = sqlx::query_scalar(
        "SELECT json_array(
             task_id, repository_id, attempt, review_round,
             workspace_generation, digest_algorithm, workspace_digest,
             decision_source, verdict, summary, findings_json,
             added_checks_json, required_checks_json, check_evidence_json,
             coverage_json, created_at, event_id, event_kind
         )
         FROM task_review_evidence ORDER BY task_id, review_round",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    let delivery = sqlx::query_scalar(
        "SELECT json_array(
             task_id, readiness, final_review_round, final_verdict, decided_at
         )
         FROM task_delivery_state ORDER BY task_id",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    (evidence, delivery)
}

async fn event_rows_snapshot(pool: &sqlx::SqlitePool) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT json_array(
             id, schema_version, task_id, kind, payload_json, created_at
         )
         FROM task_events ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .unwrap()
}

async fn schema_object_count(pool: &sqlx::SqlitePool, kind: &str, name: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE type = ? AND name = ?")
        .bind(kind)
        .bind(name)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn row_count(pool: &sqlx::SqlitePool, table: &str) -> i64 {
    let query = match table {
        "task_attempt_artifacts" => "SELECT COUNT(*) FROM task_attempt_artifacts",
        "task_review_evidence" => "SELECT COUNT(*) FROM task_review_evidence",
        "task_delivery_state" => "SELECT COUNT(*) FROM task_delivery_state",
        "task_stop_intents" => "SELECT COUNT(*) FROM task_stop_intents",
        other => panic!("unsupported fixture table {other}"),
    };
    sqlx::query_scalar(query).fetch_one(pool).await.unwrap()
}

async fn insert_stop_intent(
    pool: &sqlx::SqlitePool,
    task_id: &str,
    repository_id: &str,
    attempt: i64,
    kind: &str,
    requested_at: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO task_stop_intents (
             task_id, repository_id, attempt, kind, requested_at
         ) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(task_id)
    .bind(repository_id)
    .bind(attempt)
    .bind(kind)
    .bind(requested_at)
    .execute(pool)
    .await
    .map(|_| ())
}

async fn update_task_terminal(
    pool: &sqlx::SqlitePool,
    task_id: &str,
    status: &str,
    failure_json: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE tasks SET status = ?, finished_at = ?, failure_json = ? \
         WHERE id = ?",
    )
    .bind(status)
    .bind(FIXTURE_TIMESTAMP)
    .bind(failure_json)
    .bind(task_id)
    .execute(pool)
    .await
    .map(|_| ())
}

async fn task_status(pool: &sqlx::SqlitePool, task_id: &str) -> String {
    sqlx::query_scalar("SELECT status FROM tasks WHERE id = ?")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

fn assert_constraint_error(case: &str, result: Result<(), sqlx::Error>) {
    let error = match result {
        Ok(()) => panic!("{case} unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(
        matches!(&error, sqlx::Error::Database(_)),
        "{case} failed for a non-constraint reason: {error}"
    );
}

const REVIEW_REPOSITORY_ID: &str = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const FIRST_TASK_ID: &str = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
const SECOND_TASK_ID: &str = "dddddddd-dddd-4ddd-8ddd-dddddddddddd";
const THIRD_TASK_ID: &str = "ffffffff-ffff-4fff-8fff-ffffffffffff";
const THIRD_CLIENT_REQUEST_ID: &str = "99999999-9999-4999-8999-999999999999";
const FOURTH_TASK_ID: &str = "12121212-1212-4212-8212-121212121212";
const FOURTH_CLIENT_REQUEST_ID: &str = "34343434-3434-4434-8434-343434343434";
const FIFTH_TASK_ID: &str = "56565656-5656-4656-8656-565656565656";
const FIFTH_CLIENT_REQUEST_ID: &str = "78787878-7878-4878-8878-787878787878";
const FIXTURE_TIMESTAMP: &str = "2026-07-23T00:00:00.000000000Z";
const WORKSPACE_DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const REQUIRED_CHECKS_JSON: &str =
    r#"[{"kind":"cargo_test","id":"tests","package":null,"integration_test":null}]"#;
const CHECK_EVIDENCE_JSON: &str = r#"[{"check_id":"tests","actor":"executor","role_run":1,"workspace_generation":0,"workspace_digest":{"algorithm":"workspace_fingerprint_v1","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"status":"passed","duration_ms":1,"summary":"ok","truncated":false}]"#;
const BLOCKING_FINDINGS_JSON: &str = r#"[{"id":"review-1-finding-1","severity":"blocking","message":"fix required","path":null,"line":null}]"#;
const COMPLETE_COVERAGE_JSON: &str = r#"{"generation":0,"workspace_digest":{"algorithm":"workspace_fingerprint_v1","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"manifest_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","covered_chunks":[0],"total_chunks":1}"#;
const CANONICAL_HISTORY_SCHEMA: &str = "CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);";

#[derive(Debug)]
struct ReviewParents {
    first_task_event_id: i64,
    first_task_plan_event_id: i64,
    second_task_event_id: i64,
    second_task_unreferenced_event_id: i64,
}

async fn seed_review_parents(pool: &sqlx::SqlitePool) -> ReviewParents {
    sqlx::query(
        "INSERT INTO repositories (
             id, selected_path, display_name, git_root, cargo_workspace_root,
             git_identity_key, cargo_identity_key, created_at, last_opened_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(REVIEW_REPOSITORY_ID)
    .bind("C:/review/selected")
    .bind("review repository")
    .bind("C:/review/git")
    .bind("C:/review/git")
    .bind("review-git")
    .bind("review-cargo")
    .bind(FIXTURE_TIMESTAMP)
    .bind(FIXTURE_TIMESTAMP)
    .execute(pool)
    .await
    .unwrap();

    insert_parent_task(pool, FIRST_TASK_ID, "cccccccc-cccc-4ccc-8ccc-cccccccccccc").await;
    insert_parent_task(pool, SECOND_TASK_ID, "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee").await;

    insert_parent_event(pool, 101, FIRST_TASK_ID, "review.updated").await;
    insert_parent_event(pool, 102, FIRST_TASK_ID, "plan.updated").await;
    insert_parent_event(pool, 103, SECOND_TASK_ID, "review.updated").await;
    insert_parent_event(pool, 104, SECOND_TASK_ID, "review.updated").await;

    ReviewParents {
        first_task_event_id: 101,
        first_task_plan_event_id: 102,
        second_task_event_id: 103,
        second_task_unreferenced_event_id: 104,
    }
}

async fn insert_parent_task(pool: &sqlx::SqlitePool, task_id: &str, client_request_id: &str) {
    sqlx::query(
        "INSERT INTO tasks (
             id, client_request_id, repository_id, prompt, status, attempt,
             retry_of, created_at, started_at, finished_at, last_event_id,
             failure_json
         ) VALUES (?, ?, ?, 'review fixture', 'running', 1, NULL, ?, ?, NULL, 0, NULL)",
    )
    .bind(task_id)
    .bind(client_request_id)
    .bind(REVIEW_REPOSITORY_ID)
    .bind(FIXTURE_TIMESTAMP)
    .bind(FIXTURE_TIMESTAMP)
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_parent_event(pool: &sqlx::SqlitePool, event_id: i64, task_id: &str, kind: &str) {
    let payload = if kind == "review.updated" {
        r#"{"evidence_ref":true}"#
    } else {
        "{}"
    };
    sqlx::query(
        "INSERT INTO task_events (
             id, schema_version, task_id, kind, payload_json, created_at
         ) VALUES (?, 1, ?, ?, ?, ?)",
    )
    .bind(event_id)
    .bind(task_id)
    .bind(kind)
    .bind(payload)
    .bind(FIXTURE_TIMESTAMP)
    .execute(pool)
    .await
    .unwrap();
}

async fn seed_all_persisted_event_kinds(pool: &sqlx::SqlitePool, task_id: &str) {
    for (offset, kind) in persisted_event_kinds().into_iter().enumerate() {
        let payload = if kind == "review.updated" {
            r#"{"evidence_ref":true}"#
        } else {
            r#"{"preserve":"event-bytes"}"#
        };
        sqlx::query(
            "INSERT INTO task_events (
                 id, schema_version, task_id, kind, payload_json, created_at
             ) VALUES (?, 1, ?, ?, ?, ?)",
        )
        .bind(200_i64 + i64::try_from(offset).unwrap())
        .bind(task_id)
        .bind(kind)
        .bind(payload)
        .bind(FIXTURE_TIMESTAMP)
        .execute(pool)
        .await
        .unwrap();
    }
}

fn persisted_event_kinds() -> Vec<String> {
    [
        "activity.appended",
        "diff.updated",
        "plan.updated",
        "review.updated",
        "task.cancelled",
        "task.completed",
        "task.failed",
        "task.interrupted",
        "task.queued",
        "task.started",
        "test.updated",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

#[derive(Debug, Clone, Copy)]
struct EvidenceInsert<'a> {
    task_id: &'a str,
    repository_id: &'a str,
    attempt: i64,
    review_round: i64,
    workspace_generation: i64,
    digest_algorithm: &'a str,
    workspace_digest: &'a str,
    decision_source: &'a str,
    verdict: &'a str,
    summary: &'a str,
    findings_json: &'a str,
    added_checks_json: &'a str,
    required_checks_json: &'a str,
    check_evidence_json: &'a str,
    coverage_json: Option<&'a str>,
    created_at: &'a str,
    event_id: i64,
    event_kind: &'a str,
}

impl EvidenceInsert<'static> {
    fn changes_requested(task_id: &'static str, event_id: i64) -> Self {
        Self {
            task_id,
            repository_id: REVIEW_REPOSITORY_ID,
            attempt: 1,
            review_round: 1,
            workspace_generation: 0,
            digest_algorithm: "workspace_fingerprint_v1",
            workspace_digest: WORKSPACE_DIGEST,
            decision_source: "reviewer",
            verdict: "changes_requested",
            summary: "fixture review",
            findings_json: BLOCKING_FINDINGS_JSON,
            added_checks_json: REQUIRED_CHECKS_JSON,
            required_checks_json: REQUIRED_CHECKS_JSON,
            check_evidence_json: CHECK_EVIDENCE_JSON,
            coverage_json: Some("null"),
            created_at: FIXTURE_TIMESTAMP,
            event_id,
            event_kind: "review.updated",
        }
    }

    fn approved(task_id: &'static str, event_id: i64, review_round: i64) -> Self {
        let mut evidence = Self::changes_requested(task_id, event_id);
        evidence.review_round = review_round;
        evidence.verdict = "approved";
        evidence.findings_json = "[]";
        evidence.coverage_json = Some(COMPLETE_COVERAGE_JSON);
        evidence
    }
}

async fn insert_evidence(
    pool: &sqlx::SqlitePool,
    evidence: EvidenceInsert<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO task_review_evidence (
             task_id, repository_id, attempt, review_round,
             workspace_generation, digest_algorithm, workspace_digest,
             decision_source, verdict, summary, findings_json,
             added_checks_json, required_checks_json, check_evidence_json,
             coverage_json, created_at, event_id, event_kind
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(evidence.task_id)
    .bind(evidence.repository_id)
    .bind(evidence.attempt)
    .bind(evidence.review_round)
    .bind(evidence.workspace_generation)
    .bind(evidence.digest_algorithm)
    .bind(evidence.workspace_digest)
    .bind(evidence.decision_source)
    .bind(evidence.verdict)
    .bind(evidence.summary)
    .bind(evidence.findings_json)
    .bind(evidence.added_checks_json)
    .bind(evidence.required_checks_json)
    .bind(evidence.check_evidence_json)
    .bind(evidence.coverage_json)
    .bind(evidence.created_at)
    .bind(evidence.event_id)
    .bind(evidence.event_kind)
    .execute(pool)
    .await
    .map(|_| ())
}

async fn insert_delivery(
    pool: &sqlx::SqlitePool,
    task_id: &str,
    readiness: &str,
    final_review_round: i64,
    final_verdict: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO task_delivery_state (
             task_id, readiness, final_review_round, final_verdict, decided_at
         ) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(task_id)
    .bind(readiness)
    .bind(final_review_round)
    .bind(final_verdict)
    .bind(FIXTURE_TIMESTAMP)
    .execute(pool)
    .await
    .map(|_| ())
}

fn json_array_of_empty_objects(count: usize) -> String {
    format!("[{}]", vec!["{}"; count].join(","))
}

async fn assert_invalid_evidence(
    pool: &sqlx::SqlitePool,
    case: &str,
    evidence: EvidenceInsert<'_>,
    expected_row_count: i64,
) {
    let result = insert_evidence(pool, evidence).await;
    assert!(
        matches!(result, Err(sqlx::Error::Database(_))),
        "{case} unexpectedly inserted or failed for a non-constraint reason: {result:?}"
    );
    assert_eq!(
        row_count(pool, "task_review_evidence").await,
        expected_row_count,
        "{case} changed the evidence table"
    );
}

#[derive(Debug)]
struct ColumnInfo {
    name: String,
    data_type: String,
    not_null: bool,
    primary_key_position: i64,
}

async fn table_columns(pool: &sqlx::SqlitePool, table: &str) -> Vec<ColumnInfo> {
    let query = match table {
        "task_review_evidence" => {
            "SELECT name, type AS data_type, \"notnull\" AS not_null, pk \
             FROM pragma_table_info('task_review_evidence') ORDER BY cid"
        }
        "task_delivery_state" => {
            "SELECT name, type AS data_type, \"notnull\" AS not_null, pk \
             FROM pragma_table_info('task_delivery_state') ORDER BY cid"
        }
        "task_stop_intents" => {
            "SELECT name, type AS data_type, \"notnull\" AS not_null, pk \
             FROM pragma_table_info('task_stop_intents') ORDER BY cid"
        }
        other => panic!("unsupported fixture table {other}"),
    };
    sqlx::query(query)
        .fetch_all(pool)
        .await
        .unwrap()
        .into_iter()
        .map(|row| ColumnInfo {
            name: row.get("name"),
            data_type: row.get("data_type"),
            not_null: row.get::<i64, _>("not_null") == 1,
            primary_key_position: row.get("pk"),
        })
        .collect()
}

fn assert_required_column(
    columns: &[ColumnInfo],
    name: &str,
    data_type: &str,
    primary_key_position: i64,
) {
    let column = columns
        .iter()
        .find(|column| column.name == name)
        .unwrap_or_else(|| panic!("missing column {name}: {columns:?}"));
    assert_eq!(
        column.data_type.to_ascii_uppercase(),
        data_type,
        "wrong declared type for {name}"
    );
    assert!(column.not_null, "{name} must be NOT NULL");
    assert_eq!(
        column.primary_key_position, primary_key_position,
        "wrong primary-key position for {name}"
    );
}

async fn strict_table_flag(pool: &sqlx::SqlitePool, table: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT strict FROM pragma_table_list \
         WHERE schema = 'main' AND name = ?",
    )
    .bind(table)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn unique_index_columns(pool: &sqlx::SqlitePool, table: &str) -> Vec<Vec<String>> {
    let query = match table {
        "task_review_evidence" => {
            "SELECT il.name AS index_name, ii.seqno, ii.name AS column_name \
             FROM pragma_index_list('task_review_evidence') AS il \
             JOIN pragma_index_info(il.name) AS ii \
             WHERE il.\"unique\" = 1 \
             ORDER BY il.name, ii.seqno"
        }
        "task_events" => {
            "SELECT il.name AS index_name, ii.seqno, ii.name AS column_name \
             FROM pragma_index_list('task_events') AS il \
             JOIN pragma_index_info(il.name) AS ii \
             WHERE il.\"unique\" = 1 \
             ORDER BY il.name, ii.seqno"
        }
        other => panic!("unsupported fixture table {other}"),
    };
    let mut indexes: BTreeMap<String, Vec<(i64, String)>> = BTreeMap::new();
    for row in sqlx::query(query).fetch_all(pool).await.unwrap() {
        indexes
            .entry(row.get("index_name"))
            .or_default()
            .push((row.get("seqno"), row.get("column_name")));
    }
    indexes
        .into_values()
        .map(|mut columns| {
            columns.sort_by_key(|(sequence, _)| *sequence);
            columns.into_iter().map(|(_, name)| name).collect()
        })
        .collect()
}

#[derive(Debug, PartialEq, Eq)]
struct ForeignKey {
    parent_table: String,
    columns: Vec<(String, String)>,
}

type SequencedForeignKeyColumns = Vec<(i64, String, String)>;

async fn foreign_keys(pool: &sqlx::SqlitePool, table: &str) -> Vec<ForeignKey> {
    let query = match table {
        "task_review_evidence" => {
            "SELECT id, seq, \"table\" AS parent_table, \
                    \"from\" AS child_column, \"to\" AS parent_column \
             FROM pragma_foreign_key_list('task_review_evidence') \
             ORDER BY id, seq"
        }
        "task_delivery_state" => {
            "SELECT id, seq, \"table\" AS parent_table, \
                    \"from\" AS child_column, \"to\" AS parent_column \
             FROM pragma_foreign_key_list('task_delivery_state') \
             ORDER BY id, seq"
        }
        "task_stop_intents" => {
            "SELECT id, seq, \"table\" AS parent_table, \
                    \"from\" AS child_column, \"to\" AS parent_column \
             FROM pragma_foreign_key_list('task_stop_intents') \
             ORDER BY id, seq"
        }
        other => panic!("unsupported fixture table {other}"),
    };
    let mut keys: BTreeMap<i64, (String, SequencedForeignKeyColumns)> = BTreeMap::new();
    for row in sqlx::query(query).fetch_all(pool).await.unwrap() {
        let id: i64 = row.get("id");
        let entry = keys
            .entry(id)
            .or_insert_with(|| (row.get("parent_table"), Vec::new()));
        entry.1.push((
            row.get("seq"),
            row.get("child_column"),
            row.get("parent_column"),
        ));
    }
    keys.into_values()
        .map(|(parent_table, mut columns)| {
            columns.sort_by_key(|(sequence, _, _)| *sequence);
            ForeignKey {
                parent_table,
                columns: columns
                    .into_iter()
                    .map(|(_, child, parent)| (child, parent))
                    .collect(),
            }
        })
        .collect()
}

async fn normalized_schema_sql(pool: &sqlx::SqlitePool, kind: &str, name: &str) -> String {
    let sql: String =
        sqlx::query_scalar("SELECT sql FROM sqlite_master WHERE type = ? AND name = ?")
            .bind(kind)
            .bind(name)
            .fetch_one(pool)
            .await
            .unwrap();
    sql.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

async fn assert_immutable_trigger_shape(pool: &sqlx::SqlitePool, table: &str) {
    let no_replace = format!("{table}_no_replace");
    let no_update = format!("{table}_no_update");
    let no_delete = format!("{table}_no_delete");
    let triggers: Vec<String> = sqlx::query_scalar(
        "SELECT sql FROM sqlite_master \
         WHERE type = 'trigger' AND tbl_name = ? \
           AND name IN (?, ?, ?) \
         ORDER BY name",
    )
    .bind(table)
    .bind(no_replace)
    .bind(no_update)
    .bind(no_delete)
    .fetch_all(pool)
    .await
    .unwrap();
    assert_eq!(
        triggers.len(),
        3,
        "{table} must have INSERT, UPDATE, and DELETE abort triggers"
    );
    let triggers: Vec<String> = triggers
        .into_iter()
        .map(|sql| {
            sql.split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .to_ascii_lowercase()
        })
        .collect();
    for operation in [" insert ", " update ", " delete "] {
        let trigger = triggers
            .iter()
            .find(|trigger| trigger.contains(operation))
            .unwrap_or_else(|| panic!("missing {operation} trigger for {table}: {triggers:?}"));
        assert!(
            trigger.contains("raise") && trigger.contains("abort"),
            "{table} {operation} trigger must abort: {trigger}"
        );
    }
}
