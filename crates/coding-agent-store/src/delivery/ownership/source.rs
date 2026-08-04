use coding_agent_domain::TaskId;
use sqlx::SqliteConnection;

use crate::StoreError;

use super::super::DeliverySourceRecord;
use super::decode::{
    integer, optional_text, parse_optional, parse_source_state, parse_value, parse_version,
    provenance_from_row, required_commit_metadata, text,
};
use super::shape::validate_source_current_shape;
use super::transitions::transition_bounds;

pub(super) async fn load_source(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<Option<DeliverySourceRecord>, StoreError> {
    let row = sqlx::query(
        "SELECT task_id, repository_id, attempt, evidence_algorithm, final_review_round, \
                final_review_event_id, workspace_generation, workspace_fingerprint, \
                checks_digest, coverage_digest, artifact_base_commit, artifact_source_branch, \
                artifact_worktree_path, common_git_identity_algorithm, \
                common_git_identity_digest, worktree_admin_identity_algorithm, \
                worktree_admin_identity_digest, fixed_lock_reason, config_attributes_digest, \
                origin_accepted_operation_id, origin_accept_receipt_id, \
                origin_accepted_version, candidate_tree_oid, expected_parent_oid, \
                expected_source_commit_oid, \
                author_name, author_email, committer_name, committer_email, author_date_bytes, \
                committer_date_bytes, commit_message_template_version, commit_message_bytes, \
                state, failure_code, version, created_at, updated_at \
         FROM task_delivery_sources WHERE task_id = ?",
    )
    .bind(task_id.to_string())
    .fetch_optional(&mut *connection)
    .await?;
    let Some(row) = row else { return Ok(None) };
    let provenance = provenance_from_row(&row)?;
    let state = parse_source_state(text(&row, "state")?)?;
    let version = parse_version(integer(&row, "version")?)?;
    let history = transition_bounds(
        &mut *connection,
        "delivery_source",
        &task_id.to_string(),
        version,
        state.as_str(),
        optional_text(&row, "failure_code")?.as_deref(),
        &text(&row, "updated_at")?,
    )
    .await?;
    let source = DeliverySourceRecord {
        provenance,
        origin_accepted_operation_id: parse_value(text(&row, "origin_accepted_operation_id")?)?,
        origin_accept_receipt_id: parse_value(text(&row, "origin_accept_receipt_id")?)?,
        origin_accepted_version: parse_version(integer(&row, "origin_accepted_version")?)?,
        candidate_tree: parse_value(text(&row, "candidate_tree_oid")?)?,
        expected_parent: parse_value(text(&row, "expected_parent_oid")?)?,
        expected_source_commit: parse_optional(optional_text(&row, "expected_source_commit_oid")?)?,
        commit_metadata: required_commit_metadata(&row, false)?,
        state,
        failure_code: parse_optional(optional_text(&row, "failure_code")?)?,
        version,
        created_at: parse_value(text(&row, "created_at")?)?,
        updated_at: parse_value(text(&row, "updated_at")?)?,
        initial_transition_id: history.initial_transition_id,
        current_transition_id: history.current_transition_id,
    };
    validate_source_current_shape(&source)?;
    Ok(Some(source))
}
