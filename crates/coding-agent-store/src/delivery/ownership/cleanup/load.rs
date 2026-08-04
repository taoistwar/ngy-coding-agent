use coding_agent_domain::TaskId;
use sqlx::SqliteConnection;

use crate::StoreError;

use super::super::super::{CleanupOperationRecord, DeliveryOperationId, DirectoryIdentity};
use super::super::decode::{
    canonical_path, identity_from_row, integer, optional_text, parse_cleanup_state, parse_optional,
    parse_value, parse_version, text,
};
use super::super::ownership_invariant;
use super::super::transitions::transition_bounds;

pub(in crate::delivery::ownership) async fn load_all_cleanup_operations(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<Vec<CleanupOperationRecord>, StoreError> {
    audit_orphan_cleanup_receipts(&mut *connection, task_id).await?;
    audit_orphan_cleanup_transitions(&mut *connection).await?;
    audit_orphan_target_head_observations(&mut *connection, task_id).await?;
    let operation_ids: Vec<String> = sqlx::query_scalar(
        "SELECT operation_id FROM task_cleanup_operations WHERE task_id = ? ORDER BY operation_id",
    )
    .bind(task_id.to_string())
    .fetch_all(&mut *connection)
    .await?;
    let mut operations = Vec::with_capacity(operation_ids.len());
    for operation_id in operation_ids {
        operations
            .push(load_cleanup_operation(&mut *connection, parse_value(operation_id)?).await?);
    }
    operations.sort_by_key(|operation| operation.initial_transition_id);
    Ok(operations)
}

async fn audit_orphan_target_head_observations(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<(), StoreError> {
    let invalid: i64 = sqlx::query_scalar(
        "SELECT EXISTS( \
             SELECT 1 FROM task_cleanup_target_head_observations observation \
             LEFT JOIN task_cleanup_operations cleanup \
               ON cleanup.operation_id = observation.cleanup_operation_id \
             LEFT JOIN task_delivery_operation_transitions transition_row \
               ON transition_row.entity_kind = 'cleanup_operation' \
              AND transition_row.entity_id = observation.cleanup_operation_id \
              AND transition_row.entity_version = observation.operation_version \
             WHERE cleanup.operation_id IS NULL \
                OR (cleanup.task_id = ? AND ( \
                       cleanup.kind != 'delete_branch' \
                    OR observation.operation_version > cleanup.version \
                    OR transition_row.transition_id IS NULL \
                    OR transition_row.transitioned_at != observation.observed_at \
                )) \
         )",
    )
    .bind(task_id.to_string())
    .fetch_one(connection)
    .await?;
    if invalid == 0 {
        Ok(())
    } else {
        Err(ownership_invariant())
    }
}

async fn audit_orphan_cleanup_transitions(
    connection: &mut SqliteConnection,
) -> Result<(), StoreError> {
    let invalid: i64 = sqlx::query_scalar(
        "SELECT EXISTS( \
             SELECT 1 FROM task_delivery_operation_transitions transition_row \
             LEFT JOIN task_cleanup_operations cleanup \
               ON cleanup.operation_id = transition_row.entity_id \
             WHERE transition_row.entity_kind = 'cleanup_operation' \
               AND cleanup.operation_id IS NULL \
         )",
    )
    .fetch_one(connection)
    .await?;
    if invalid == 0 {
        Ok(())
    } else {
        Err(ownership_invariant())
    }
}

pub(in crate::delivery::ownership) async fn load_cleanup_operation(
    connection: &mut SqliteConnection,
    operation_id: DeliveryOperationId,
) -> Result<CleanupOperationRecord, StoreError> {
    let row = sqlx::query(
        "SELECT operation_id, task_id, repository_id, attempt, kind, origin_receipt_id, \
                disposition_task_id, expected_worktree_path, expected_admin_identity_algorithm, \
                expected_admin_identity_digest, expected_common_git_identity_algorithm, \
                expected_common_git_identity_digest, expected_source_ref, expected_source_oid, \
                expected_disposition_version, expected_target_ref, expected_target_head, \
                origin_target_head, state, failure_code, version, created_at, updated_at \
         FROM task_cleanup_operations WHERE operation_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_optional(&mut *connection)
    .await?
    .ok_or_else(ownership_invariant)?;
    if parse_value::<DeliveryOperationId>(text(&row, "operation_id")?)? != operation_id {
        return Err(ownership_invariant());
    }
    let state = parse_cleanup_state(text(&row, "state")?)?;
    let version = parse_version(integer(&row, "version")?)?;
    let history = transition_bounds(
        &mut *connection,
        "cleanup_operation",
        &operation_id.to_string(),
        version,
        state.as_str(),
        optional_text(&row, "failure_code")?.as_deref(),
        &text(&row, "updated_at")?,
    )
    .await?;
    let kind = text(&row, "kind")?
        .parse()
        .map_err(|_| ownership_invariant())?;
    let target_head_observations =
        load_target_head_observations(&mut *connection, operation_id).await?;
    Ok(CleanupOperationRecord {
        operation_id,
        identity: identity_from_row(&row)?,
        kind,
        origin_receipt_id: parse_value(text(&row, "origin_receipt_id")?)?,
        disposition_task_id: text(&row, "disposition_task_id")?
            .parse()
            .map_err(|_| ownership_invariant())?,
        expected_worktree_path: canonical_path(text(&row, "expected_worktree_path")?)?,
        expected_admin_identity: DirectoryIdentity::try_new(
            &text(&row, "expected_admin_identity_algorithm")?,
            &text(&row, "expected_admin_identity_digest")?,
        )
        .map_err(|_| ownership_invariant())?,
        expected_common_git_identity: DirectoryIdentity::try_new(
            &text(&row, "expected_common_git_identity_algorithm")?,
            &text(&row, "expected_common_git_identity_digest")?,
        )
        .map_err(|_| ownership_invariant())?,
        expected_source_ref: parse_value(text(&row, "expected_source_ref")?)?,
        expected_source_oid: parse_value(text(&row, "expected_source_oid")?)?,
        expected_disposition_version: parse_version(integer(
            &row,
            "expected_disposition_version",
        )?)?,
        expected_target_ref: parse_optional(optional_text(&row, "expected_target_ref")?)?,
        expected_target_head: parse_optional(optional_text(&row, "expected_target_head")?)?,
        origin_target_head: parse_optional(optional_text(&row, "origin_target_head")?)?,
        target_head_observations,
        state,
        failure_code: parse_optional(optional_text(&row, "failure_code")?)?,
        version,
        created_at: parse_value(text(&row, "created_at")?)?,
        updated_at: parse_value(text(&row, "updated_at")?)?,
        initial_transition_id: history.initial_transition_id,
        current_transition_id: history.current_transition_id,
    })
}

async fn load_target_head_observations(
    connection: &mut SqliteConnection,
    operation_id: DeliveryOperationId,
) -> Result<Vec<super::super::super::CleanupTargetHeadObservationRecord>, StoreError> {
    let rows: Vec<(i64, String, String)> = sqlx::query_as(
        "SELECT operation_version, target_head, observed_at \
         FROM task_cleanup_target_head_observations \
         WHERE cleanup_operation_id = ? ORDER BY operation_version",
    )
    .bind(operation_id.to_string())
    .fetch_all(connection)
    .await?;
    rows.into_iter()
        .map(|(version, target_head, observed_at)| {
            Ok(super::super::super::CleanupTargetHeadObservationRecord {
                operation_version: parse_version(version)?,
                target_head: parse_value(target_head)?,
                observed_at: parse_value(observed_at)?,
            })
        })
        .collect()
}

async fn audit_orphan_cleanup_receipts(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<(), StoreError> {
    let invalid: i64 = sqlx::query_scalar(
        "SELECT EXISTS( \
             SELECT 1 FROM task_delivery_command_receipts receipt \
             LEFT JOIN task_cleanup_operations cleanup \
               ON cleanup.operation_id = receipt.operation_id \
              AND cleanup.operation_id = receipt.cleanup_operation_id \
              AND cleanup.origin_receipt_id = receipt.client_request_id \
              AND cleanup.task_id = receipt.task_id \
              AND cleanup.repository_id = receipt.repository_id \
              AND cleanup.attempt = receipt.attempt \
              AND cleanup.kind = receipt.command_kind \
             WHERE receipt.task_id = ? \
               AND (receipt.command_kind IN ('remove_worktree', 'delete_branch') \
                    OR receipt.operation_kind = 'cleanup_operation') \
               AND (receipt.operation_kind != 'cleanup_operation' \
                    OR receipt.merge_operation_id IS NOT NULL \
                    OR receipt.cleanup_operation_id IS NULL \
                    OR cleanup.operation_id IS NULL) \
         )",
    )
    .bind(task_id.to_string())
    .fetch_one(connection)
    .await?;
    if invalid == 0 {
        Ok(())
    } else {
        Err(ownership_invariant())
    }
}
