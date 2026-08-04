use coding_agent_domain::TaskId;
use sqlx::{Row, SqliteConnection};

use crate::StoreError;
use crate::delivery::{DeliveryCommitMetadata, DeliveryOperationId, MergeOperationRecord};

use super::super::decode::{
    integer, optional_integer, optional_text, parse_merge_state, parse_optional,
    parse_optional_task_id, parse_optional_uuid, parse_value, parse_version, positive_u32,
    provenance_from_row, text,
};
use super::super::ownership_invariant;
use super::super::shape::validate_merge_current_shape;
use super::super::transitions::transition_bounds;
use super::conflicts::load_merge_conflicts;
use super::cross_rows::validate_merge_cross_rows;
use super::history::validate_merge_historical_shape;

pub(in crate::delivery::ownership) async fn select_merge_operation_ids(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<Vec<DeliveryOperationId>, StoreError> {
    let rows: Vec<(String, String, i64, Option<i64>)> = sqlx::query_as(
        "WITH candidates AS ( \
             SELECT m.operation_id, \
                    CASE \
                      WHEN m.state IN ('preflight_pending', 'preflight_ready', 'accepted', \
                                       'merge_pending', 'abort_pending') THEN 'active' \
                      WHEN m.state = 'merged' THEN 'merged' \
                      WHEN m.state = 'reconciliation_required' THEN 'reconciliation' \
                      ELSE 'latest_terminal' \
                    END AS slot, \
                    t.transition_id AS initial_transition_id \
             FROM task_merge_operations m \
             LEFT JOIN task_delivery_operation_transitions t \
               ON t.entity_kind = 'merge_operation' \
              AND t.entity_id = m.operation_id AND t.entity_version = 1 \
             WHERE m.task_id = ? \
         ), ranked AS ( \
             SELECT operation_id, slot, initial_transition_id, \
                    ROW_NUMBER() OVER (PARTITION BY slot ORDER BY initial_transition_id DESC) AS slot_rank \
             FROM candidates \
         ) \
         SELECT operation_id, slot, slot_rank, initial_transition_id FROM ranked \
         WHERE (slot IN ('active', 'merged', 'reconciliation') AND slot_rank <= 2) \
            OR (slot = 'latest_terminal' AND slot_rank = 1) \
         ORDER BY initial_transition_id",
    )
    .bind(task_id.to_string())
    .fetch_all(&mut *connection)
    .await?;
    if rows.iter().any(|(_, slot, rank, initial)| {
        initial.is_none()
            || (*rank > 1 && matches!(slot.as_str(), "active" | "merged" | "reconciliation"))
    }) {
        return Err(ownership_invariant());
    }
    rows.into_iter()
        .map(|(id, _, _, _)| parse_value(id))
        .collect()
}

pub(in crate::delivery::ownership) async fn select_all_merge_operation_ids(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<Vec<DeliveryOperationId>, StoreError> {
    let rows: Vec<(String, Option<i64>)> = sqlx::query_as(
        "SELECT m.operation_id, t.transition_id \
         FROM task_merge_operations m \
         LEFT JOIN task_delivery_operation_transitions t \
           ON t.entity_kind = 'merge_operation' AND t.entity_id = m.operation_id \
          AND t.entity_version = 1 \
         WHERE m.task_id = ? ORDER BY t.transition_id",
    )
    .bind(task_id.to_string())
    .fetch_all(connection)
    .await?;
    if rows
        .iter()
        .any(|(_, transition_id)| transition_id.is_none())
    {
        return Err(ownership_invariant());
    }
    rows.into_iter()
        .map(|(operation_id, _)| parse_value(operation_id))
        .collect()
}

pub(in crate::delivery::ownership) async fn load_merge_operation(
    connection: &mut SqliteConnection,
    operation_id: DeliveryOperationId,
) -> Result<MergeOperationRecord, StoreError> {
    let operation = load_merge_operation_local(&mut *connection, operation_id).await?;
    validate_merge_cross_rows(connection, &operation).await?;
    Ok(operation)
}

pub(super) async fn load_merge_operation_local(
    connection: &mut SqliteConnection,
    operation_id: DeliveryOperationId,
) -> Result<MergeOperationRecord, StoreError> {
    let row = sqlx::query(
        "SELECT operation_id, task_id, repository_id, attempt, evidence_algorithm, \
                final_review_round, final_review_event_id, workspace_generation, \
                workspace_fingerprint, checks_digest, coverage_digest, artifact_base_commit, \
                artifact_source_branch, artifact_worktree_path, common_git_identity_algorithm, \
                common_git_identity_digest, worktree_admin_identity_algorithm, \
                worktree_admin_identity_digest, fixed_lock_reason, candidate_tree_oid, \
                preflight_source_commit_oid, delivery_source_task_id, source_commit_oid, \
                preflight_receipt_id, accept_receipt_id, target_branch, expected_target_head, \
                config_attributes_digest, merge_base_oid, candidate_merge_tree_oid, \
                merge_author_name, merge_author_email, merge_committer_name, \
                merge_committer_email, merge_author_date_bytes, merge_committer_date_bytes, \
                merge_message_template_version, merge_message_bytes, expected_merge_commit_oid, \
                abort_child_receipt_id, abort_merge_head_oid, abort_index_stages_digest, \
                abort_worktree_digest, abort_merge_autostash_proof, merged_disposition_task_id, \
                conflict_path_count, state, failure_code, version, created_at, updated_at \
         FROM task_merge_operations WHERE operation_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_optional(&mut *connection)
    .await?
    .ok_or_else(ownership_invariant)?;
    if parse_value::<DeliveryOperationId>(text(&row, "operation_id")?)? != operation_id {
        return Err(ownership_invariant());
    }
    let provenance = provenance_from_row(&row)?;
    let state = parse_merge_state(text(&row, "state")?)?;
    let version = parse_version(integer(&row, "version")?)?;
    let history = transition_bounds(
        &mut *connection,
        "merge_operation",
        &operation_id.to_string(),
        version,
        state.as_str(),
        optional_text(&row, "failure_code")?.as_deref(),
        &text(&row, "updated_at")?,
    )
    .await?;
    let conflict_path_count = optional_integer(&row, "conflict_path_count")?
        .map(|value| u8::try_from(value).map_err(|_| ownership_invariant()))
        .transpose()?;
    let conflicts = load_merge_conflicts(
        &mut *connection,
        operation_id,
        conflict_path_count.map(usize::from),
    )
    .await?;
    let operation = MergeOperationRecord {
        operation_id,
        provenance,
        candidate_tree: parse_value(text(&row, "candidate_tree_oid")?)?,
        preflight_source_commit: parse_value(text(&row, "preflight_source_commit_oid")?)?,
        delivery_source_task_id: parse_optional_task_id(optional_text(
            &row,
            "delivery_source_task_id",
        )?)?,
        source_commit: parse_optional(optional_text(&row, "source_commit_oid")?)?,
        preflight_receipt_id: parse_value(text(&row, "preflight_receipt_id")?)?,
        accept_receipt_id: parse_optional(optional_text(&row, "accept_receipt_id")?)?,
        target_branch: parse_value(text(&row, "target_branch")?)?,
        expected_target_head: parse_value(text(&row, "expected_target_head")?)?,
        merge_base: parse_optional(optional_text(&row, "merge_base_oid")?)?,
        candidate_merge_tree: parse_optional(optional_text(&row, "candidate_merge_tree_oid")?)?,
        merge_metadata: optional_merge_metadata(&row)?,
        expected_merge_commit: parse_optional(optional_text(&row, "expected_merge_commit_oid")?)?,
        abort_child_receipt_id: parse_optional_uuid(optional_text(
            &row,
            "abort_child_receipt_id",
        )?)?,
        abort_merge_head: parse_optional(optional_text(&row, "abort_merge_head_oid")?)?,
        abort_index_stages_digest: parse_optional(optional_text(
            &row,
            "abort_index_stages_digest",
        )?)?,
        abort_worktree_digest: parse_optional(optional_text(&row, "abort_worktree_digest")?)?,
        abort_merge_autostash_proof: optional_text(&row, "abort_merge_autostash_proof")?,
        merged_disposition_task_id: parse_optional_task_id(optional_text(
            &row,
            "merged_disposition_task_id",
        )?)?,
        conflict_path_count,
        conflicts,
        state,
        failure_code: parse_optional(optional_text(&row, "failure_code")?)?,
        version,
        created_at: parse_value(text(&row, "created_at")?)?,
        updated_at: parse_value(text(&row, "updated_at")?)?,
        initial_transition_id: history.initial_transition_id,
        current_transition_id: history.current_transition_id,
    };
    validate_merge_current_shape(&operation)?;
    validate_merge_historical_shape(&mut *connection, &operation).await?;
    Ok(operation)
}

fn optional_merge_metadata(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<Option<DeliveryCommitMetadata>, StoreError> {
    let strings = [
        "merge_author_name",
        "merge_author_email",
        "merge_committer_name",
        "merge_committer_email",
        "merge_author_date_bytes",
        "merge_committer_date_bytes",
    ]
    .map(|column| optional_text(row, column))
    .into_iter()
    .collect::<Result<Vec<_>, _>>()?;
    let template = optional_integer(row, "merge_message_template_version")?;
    let bytes: Option<Vec<u8>> = row
        .try_get("merge_message_bytes")
        .map_err(|_| ownership_invariant())?;
    let present = strings.iter().filter(|value| value.is_some()).count()
        + usize::from(template.is_some())
        + usize::from(bytes.is_some());
    match present {
        0 => Ok(None),
        8 => Ok(Some(DeliveryCommitMetadata {
            author_name: strings[0].clone().ok_or_else(ownership_invariant)?,
            author_email: strings[1].clone().ok_or_else(ownership_invariant)?,
            committer_name: strings[2].clone().ok_or_else(ownership_invariant)?,
            committer_email: strings[3].clone().ok_or_else(ownership_invariant)?,
            author_date_bytes: strings[4].clone().ok_or_else(ownership_invariant)?,
            committer_date_bytes: strings[5].clone().ok_or_else(ownership_invariant)?,
            message_template_version: positive_u32(template.ok_or_else(ownership_invariant)?)?,
            message_bytes: bytes.ok_or_else(ownership_invariant)?,
        })),
        _ => Err(ownership_invariant()),
    }
}
