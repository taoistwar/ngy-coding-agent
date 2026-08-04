use coding_agent_domain::TaskId;
use sqlx::SqliteConnection;

use crate::StoreError;

use super::super::{
    ArtifactDispositionRecord, DeliverySourceRecord, MergeOperationRecord, MergeOperationState,
};
use super::decode::{
    identity_from_row, integer, optional_integer, optional_text, parse_branch_state,
    parse_cleanup_state, parse_optional, parse_task_id, parse_value, parse_version,
    parse_worktree_state, text,
};
use super::ownership_invariant;
use super::transitions::transition_bounds;

pub(super) async fn load_disposition(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<Option<ArtifactDispositionRecord>, StoreError> {
    let row = sqlx::query(
        "SELECT task_id, repository_id, attempt, merged_operation_id, \
                delivery_source_task_id, source_commit_oid, worktree_cleanup_operation_id, \
                worktree_cleanup_operation_version, worktree_cleanup_operation_state, \
                branch_cleanup_operation_id, branch_cleanup_operation_version, \
                branch_cleanup_operation_state, worktree_state, worktree_version, \
                worktree_failure_code, worktree_updated_at, branch_state, branch_version, \
                branch_failure_code, branch_updated_at, created_at \
         FROM task_artifact_dispositions WHERE task_id = ?",
    )
    .bind(task_id.to_string())
    .fetch_optional(&mut *connection)
    .await?;
    let Some(row) = row else { return Ok(None) };
    let identity = identity_from_row(&row)?;
    let worktree_state = parse_worktree_state(text(&row, "worktree_state")?)?;
    let worktree_version = parse_version(integer(&row, "worktree_version")?)?;
    let worktree_history = transition_bounds(
        &mut *connection,
        "worktree_disposition",
        &task_id.to_string(),
        worktree_version,
        worktree_state.as_str(),
        optional_text(&row, "worktree_failure_code")?.as_deref(),
        &text(&row, "worktree_updated_at")?,
    )
    .await?;
    let branch_state = parse_branch_state(text(&row, "branch_state")?)?;
    let branch_version = parse_version(integer(&row, "branch_version")?)?;
    let branch_history = transition_bounds(
        &mut *connection,
        "branch_disposition",
        &task_id.to_string(),
        branch_version,
        branch_state.as_str(),
        optional_text(&row, "branch_failure_code")?.as_deref(),
        &text(&row, "branch_updated_at")?,
    )
    .await?;
    let disposition = ArtifactDispositionRecord {
        identity,
        merged_operation_id: parse_value(text(&row, "merged_operation_id")?)?,
        delivery_source_task_id: parse_task_id(text(&row, "delivery_source_task_id")?)?,
        source_commit: parse_value(text(&row, "source_commit_oid")?)?,
        worktree_cleanup_operation_id: parse_optional(optional_text(
            &row,
            "worktree_cleanup_operation_id",
        )?)?,
        worktree_cleanup_operation_version: optional_integer(
            &row,
            "worktree_cleanup_operation_version",
        )?
        .map(parse_version)
        .transpose()?,
        worktree_cleanup_operation_state: optional_text(&row, "worktree_cleanup_operation_state")?
            .map(parse_cleanup_state)
            .transpose()?,
        branch_cleanup_operation_id: parse_optional(optional_text(
            &row,
            "branch_cleanup_operation_id",
        )?)?,
        branch_cleanup_operation_version: optional_integer(
            &row,
            "branch_cleanup_operation_version",
        )?
        .map(parse_version)
        .transpose()?,
        branch_cleanup_operation_state: optional_text(&row, "branch_cleanup_operation_state")?
            .map(parse_cleanup_state)
            .transpose()?,
        worktree_state,
        worktree_version,
        worktree_failure_code: parse_optional(optional_text(&row, "worktree_failure_code")?)?,
        worktree_updated_at: parse_value(text(&row, "worktree_updated_at")?)?,
        branch_state,
        branch_version,
        branch_failure_code: parse_optional(optional_text(&row, "branch_failure_code")?)?,
        branch_updated_at: parse_value(text(&row, "branch_updated_at")?)?,
        created_at: parse_value(text(&row, "created_at")?)?,
        worktree_initial_transition_id: worktree_history.initial_transition_id,
        worktree_current_transition_id: worktree_history.current_transition_id,
        branch_initial_transition_id: branch_history.initial_transition_id,
        branch_current_transition_id: branch_history.current_transition_id,
    };
    validate_disposition_links(&disposition)?;
    Ok(Some(disposition))
}

fn validate_disposition_links(disposition: &ArtifactDispositionRecord) -> Result<(), StoreError> {
    let worktree_count = usize::from(disposition.worktree_cleanup_operation_id.is_some())
        + usize::from(disposition.worktree_cleanup_operation_version.is_some())
        + usize::from(disposition.worktree_cleanup_operation_state.is_some());
    let branch_count = usize::from(disposition.branch_cleanup_operation_id.is_some())
        + usize::from(disposition.branch_cleanup_operation_version.is_some())
        + usize::from(disposition.branch_cleanup_operation_state.is_some());
    if matches!(worktree_count, 0 | 3) && matches!(branch_count, 0 | 3) {
        Ok(())
    } else {
        Err(ownership_invariant())
    }
}

pub(in crate::delivery) async fn validate_merged_disposition_origin(
    connection: &mut SqliteConnection,
    operation: &MergeOperationRecord,
    source: &DeliverySourceRecord,
    disposition: &ArtifactDispositionRecord,
) -> Result<(), StoreError> {
    let task_id = operation.provenance.identity.task_id();
    let rows: Vec<(String, i64, String, String, Option<String>, String)> = sqlx::query_as(
        "SELECT entity_kind, transition_id, from_state, to_state, failure_code, transitioned_at \
         FROM task_delivery_operation_transitions \
         WHERE entity_kind IN ('worktree_disposition', 'branch_disposition') \
           AND entity_id = ? AND entity_version = 1",
    )
    .bind(task_id.to_string())
    .fetch_all(connection)
    .await?;
    let exact_journal = rows.len() == 2
        && rows
            .iter()
            .all(|(kind, id, from, to, failure, transitioned_at)| {
                *id > operation.current_transition_id
                    && from == "absent"
                    && failure.is_none()
                    && transitioned_at == &operation.updated_at.to_string()
                    && matches!(
                        (kind.as_str(), to.as_str()),
                        ("worktree_disposition", "retained_locked")
                            | ("branch_disposition", "retained")
                    )
            });
    let exact = operation.state == MergeOperationState::Merged
        && operation.merged_disposition_task_id == Some(task_id)
        && disposition.identity == operation.provenance.identity
        && disposition.merged_operation_id == operation.operation_id
        && disposition.delivery_source_task_id == task_id
        && disposition.source_commit
            == *source
                .expected_source_commit
                .as_ref()
                .ok_or_else(ownership_invariant)?
        && disposition.created_at == operation.updated_at
        && disposition.worktree_initial_transition_id > operation.current_transition_id
        && disposition.branch_initial_transition_id > operation.current_transition_id
        && exact_journal;
    if exact {
        Ok(())
    } else {
        Err(ownership_invariant())
    }
}
