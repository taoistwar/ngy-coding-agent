use super::V5_MIGRATION_SQL;
use crate::StoreError;

const V5_STRICT_TABLES: [&str; 8] = [
    "task_delivery_sources",
    "task_merge_operations",
    "task_merge_conflicts",
    "task_artifact_dispositions",
    "task_cleanup_operations",
    "task_cleanup_target_head_observations",
    "task_delivery_command_receipts",
    "task_delivery_operation_transitions",
];

const V5_INDEXES: [(&str, &str); 5] = [
    ("task_merge_operations_one_active", "task_merge_operations"),
    ("task_merge_operations_one_merged", "task_merge_operations"),
    (
        "task_merge_operations_abort_child_receipt_unique",
        "task_merge_operations",
    ),
    (
        "task_cleanup_operations_one_active_disposition",
        "task_cleanup_operations",
    ),
    (
        "task_delivery_operation_transitions_initial_order",
        "task_delivery_operation_transitions",
    ),
];

const V5_TRIGGERS: [(&str, &str); 61] = [
    (
        "task_delivery_sources_branch_canonical_on_insert",
        "task_delivery_sources",
    ),
    (
        "task_merge_operations_branches_canonical_on_insert",
        "task_merge_operations",
    ),
    (
        "task_merge_operations_commit_dates_canonical_on_update",
        "task_merge_operations",
    ),
    (
        "task_cleanup_operations_branches_canonical_on_insert",
        "task_cleanup_operations",
    ),
    (
        "task_delivery_operation_transitions_no_replace",
        "task_delivery_operation_transitions",
    ),
    (
        "task_delivery_operation_transitions_match_current",
        "task_delivery_operation_transitions",
    ),
    (
        "task_delivery_operation_transitions_no_update",
        "task_delivery_operation_transitions",
    ),
    (
        "task_delivery_operation_transitions_no_delete",
        "task_delivery_operation_transitions",
    ),
    (
        "task_delivery_sources_initial_on_insert",
        "task_delivery_sources",
    ),
    (
        "task_delivery_sources_ownership_on_insert",
        "task_delivery_sources",
    ),
    ("task_delivery_sources_no_replace", "task_delivery_sources"),
    (
        "task_delivery_sources_immutable_on_update",
        "task_delivery_sources",
    ),
    (
        "task_delivery_sources_transition_on_update",
        "task_delivery_sources",
    ),
    (
        "task_delivery_sources_merge_consistency_on_update",
        "task_delivery_sources",
    ),
    ("task_delivery_sources_no_delete", "task_delivery_sources"),
    (
        "task_delivery_sources_journal_on_insert",
        "task_delivery_sources",
    ),
    (
        "task_delivery_sources_journal_on_update",
        "task_delivery_sources",
    ),
    (
        "task_merge_operations_initial_on_insert",
        "task_merge_operations",
    ),
    (
        "task_merge_operations_eligibility_on_insert",
        "task_merge_operations",
    ),
    (
        "task_merge_operations_blocked_on_insert",
        "task_merge_operations",
    ),
    ("task_merge_operations_no_replace", "task_merge_operations"),
    (
        "task_merge_operations_immutable_on_update",
        "task_merge_operations",
    ),
    (
        "task_merge_operations_transition_on_update",
        "task_merge_operations",
    ),
    (
        "task_merge_operations_source_consistency_on_update",
        "task_merge_operations",
    ),
    (
        "task_merge_operations_source_reconciliation_on_update",
        "task_merge_operations",
    ),
    ("task_merge_operations_no_delete", "task_merge_operations"),
    (
        "task_merge_operations_journal_on_insert",
        "task_merge_operations",
    ),
    (
        "task_merge_operations_journal_on_update",
        "task_merge_operations",
    ),
    (
        "task_merge_conflicts_parent_on_insert",
        "task_merge_conflicts",
    ),
    (
        "task_merge_conflicts_text_canonical_on_insert",
        "task_merge_conflicts",
    ),
    (
        "task_merge_conflicts_bounds_on_insert",
        "task_merge_conflicts",
    ),
    ("task_merge_conflicts_no_replace", "task_merge_conflicts"),
    ("task_merge_conflicts_no_update", "task_merge_conflicts"),
    ("task_merge_conflicts_no_delete", "task_merge_conflicts"),
    (
        "task_artifact_dispositions_initial_on_insert",
        "task_artifact_dispositions",
    ),
    (
        "task_artifact_dispositions_ownership_on_insert",
        "task_artifact_dispositions",
    ),
    (
        "task_artifact_dispositions_no_replace",
        "task_artifact_dispositions",
    ),
    (
        "task_artifact_dispositions_immutable_on_update",
        "task_artifact_dispositions",
    ),
    (
        "task_artifact_dispositions_transition_on_update",
        "task_artifact_dispositions",
    ),
    (
        "task_artifact_dispositions_no_delete",
        "task_artifact_dispositions",
    ),
    (
        "task_artifact_dispositions_worktree_journal_on_insert",
        "task_artifact_dispositions",
    ),
    (
        "task_artifact_dispositions_branch_journal_on_insert",
        "task_artifact_dispositions",
    ),
    (
        "task_artifact_dispositions_worktree_journal_on_update",
        "task_artifact_dispositions",
    ),
    (
        "task_artifact_dispositions_branch_journal_on_update",
        "task_artifact_dispositions",
    ),
    (
        "task_cleanup_operations_initial_on_insert",
        "task_cleanup_operations",
    ),
    (
        "task_cleanup_operations_ownership_on_insert",
        "task_cleanup_operations",
    ),
    (
        "task_cleanup_operations_no_replace",
        "task_cleanup_operations",
    ),
    (
        "task_cleanup_operations_immutable_on_update",
        "task_cleanup_operations",
    ),
    (
        "task_cleanup_operations_transition_on_update",
        "task_cleanup_operations",
    ),
    (
        "task_cleanup_operations_disposition_on_update",
        "task_cleanup_operations",
    ),
    (
        "task_cleanup_operations_no_delete",
        "task_cleanup_operations",
    ),
    (
        "task_cleanup_target_head_observations_match_current",
        "task_cleanup_target_head_observations",
    ),
    (
        "task_cleanup_target_head_observations_no_replace",
        "task_cleanup_target_head_observations",
    ),
    (
        "task_cleanup_target_head_observations_no_update",
        "task_cleanup_target_head_observations",
    ),
    (
        "task_cleanup_target_head_observations_no_delete",
        "task_cleanup_target_head_observations",
    ),
    (
        "task_cleanup_operations_journal_on_insert",
        "task_cleanup_operations",
    ),
    (
        "task_cleanup_operations_journal_on_update",
        "task_cleanup_operations",
    ),
    (
        "task_delivery_command_receipts_match_operation_on_insert",
        "task_delivery_command_receipts",
    ),
    (
        "task_delivery_command_receipts_no_replace",
        "task_delivery_command_receipts",
    ),
    (
        "task_delivery_command_receipts_no_update",
        "task_delivery_command_receipts",
    ),
    (
        "task_delivery_command_receipts_no_delete",
        "task_delivery_command_receipts",
    ),
];

pub(super) async fn validate_v5_schema_manifest(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<(), StoreError> {
    for table in V5_STRICT_TABLES {
        validate_schema_object(transaction, "table", table, table).await?;
        let strict: Option<i64> = sqlx::query_scalar(
            "SELECT strict FROM pragma_table_list \
             WHERE schema = 'main' AND type = 'table' AND name = ? COLLATE BINARY",
        )
        .bind(table)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(StoreError::DatabaseMigration)?;
        if strict != Some(1) {
            return Err(StoreError::DatabaseSchemaUnsupported);
        }
    }
    for (index, table) in V5_INDEXES {
        validate_schema_object(transaction, "index", index, table).await?;
    }
    for (trigger, table) in V5_TRIGGERS {
        validate_schema_object(transaction, "trigger", trigger, table).await?;
    }
    validate_v5_schema_object_set(transaction).await?;
    Ok(())
}

async fn validate_v5_schema_object_set(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<(), StoreError> {
    let actual: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT type, name, tbl_name
         FROM main.sqlite_schema
         WHERE type IN ('index', 'trigger')
           AND sql IS NOT NULL
           AND tbl_name IN (
               'task_delivery_sources', 'task_merge_operations',
               'task_merge_conflicts', 'task_artifact_dispositions',
               'task_cleanup_operations', 'task_cleanup_target_head_observations',
               'task_delivery_command_receipts',
               'task_delivery_operation_transitions'
           )
         ORDER BY type, name",
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(StoreError::DatabaseMigration)?;
    let mut expected = V5_INDEXES
        .into_iter()
        .map(|(name, table)| ("index".to_owned(), name.to_owned(), table.to_owned()))
        .chain(
            V5_TRIGGERS
                .into_iter()
                .map(|(name, table)| ("trigger".to_owned(), name.to_owned(), table.to_owned())),
        )
        .collect::<Vec<_>>();
    expected.sort();
    if actual != expected {
        return Err(StoreError::DatabaseSchemaUnsupported);
    }
    Ok(())
}

async fn validate_schema_object(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    expected_type: &str,
    expected_name: &str,
    expected_table: &str,
) -> Result<(), StoreError> {
    let object: Option<(String, String, String, Option<String>)> = sqlx::query_as(
        "SELECT type, name, tbl_name, sql FROM main.sqlite_schema \
         WHERE name = ? COLLATE NOCASE",
    )
    .bind(expected_name)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(StoreError::DatabaseMigration)?;
    if object
        .as_ref()
        .map(|value| (&*value.0, &*value.1, &*value.2))
        != Some((expected_type, expected_name, expected_table))
    {
        return Err(StoreError::DatabaseSchemaUnsupported);
    }
    let expected_sql = expected_v5_schema_statement(expected_type, expected_name)
        .ok_or(StoreError::DatabaseSchemaUnsupported)?;
    let actual_sql = object
        .and_then(|value| value.3)
        .ok_or(StoreError::DatabaseSchemaUnsupported)?;
    if normalize_schema_sql(&actual_sql) != normalize_schema_sql(expected_sql) {
        return Err(StoreError::DatabaseSchemaUnsupported);
    }
    Ok(())
}

fn expected_v5_schema_statement(object_type: &str, object_name: &str) -> Option<&'static str> {
    let prefixes = match object_type {
        "table" => [Some("CREATE TABLE "), None],
        "index" => [Some("CREATE UNIQUE INDEX "), Some("CREATE INDEX ")],
        "trigger" => [Some("CREATE TRIGGER "), None],
        _ => return None,
    };
    for prefix in prefixes.into_iter().flatten() {
        let start_marker = format!("{prefix}{object_name}");
        let Some(start) = V5_MIGRATION_SQL.find(&start_marker) else {
            continue;
        };
        let remaining = &V5_MIGRATION_SQL[start..];
        let end_marker = match object_type {
            "table" => ") STRICT;",
            "trigger" => "END;",
            "index" => ";",
            _ => return None,
        };
        let end = remaining.find(end_marker)? + end_marker.len();
        return Some(&remaining[..end]);
    }
    None
}

fn normalize_schema_sql(sql: &str) -> String {
    sql.trim().trim_end_matches(';').to_owned()
}
