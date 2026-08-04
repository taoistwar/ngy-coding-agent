use std::path::PathBuf;
use std::str::FromStr;

use coding_agent_domain::{CanonicalPath, EventId, TaskId};
use sqlx::Row;

use crate::StoreError;

use super::super::{
    BranchDisposition, CleanupOperationState, DeliveryArtifactProvenance, DeliveryCommitMetadata,
    DeliveryIdentity, DeliverySourceState, DeliveryVersion, DirectoryIdentity, EvidenceIdentityV1,
    MergeOperationState, WorktreeDisposition,
};
use super::ownership_invariant;

pub(super) fn provenance_from_row(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<DeliveryArtifactProvenance, StoreError> {
    let identity = identity_from_row(row)?;
    let evidence = evidence_from_row(row, identity)?;
    Ok(DeliveryArtifactProvenance {
        identity,
        evidence,
        base_commit: parse_value(text(row, "artifact_base_commit")?)?,
        source_branch: parse_value(text(row, "artifact_source_branch")?)?,
        worktree_path: canonical_path(text(row, "artifact_worktree_path")?)?,
        common_git_identity: DirectoryIdentity::try_new(
            &text(row, "common_git_identity_algorithm")?,
            &text(row, "common_git_identity_digest")?,
        )
        .map_err(|_| ownership_invariant())?,
        worktree_admin_identity: DirectoryIdentity::try_new(
            &text(row, "worktree_admin_identity_algorithm")?,
            &text(row, "worktree_admin_identity_digest")?,
        )
        .map_err(|_| ownership_invariant())?,
        fixed_lock_reason: text(row, "fixed_lock_reason")?,
        config_attributes_digest: parse_value(text(row, "config_attributes_digest")?)?,
    })
}

pub(super) fn identity_from_row(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<DeliveryIdentity, StoreError> {
    let attempt = positive_u32(integer(row, "attempt")?)?;
    DeliveryIdentity::try_from_text(
        &text(row, "task_id")?,
        &text(row, "repository_id")?,
        attempt,
    )
    .map_err(|_| ownership_invariant())
}

fn evidence_from_row(
    row: &sqlx::sqlite::SqliteRow,
    identity: DeliveryIdentity,
) -> Result<EvidenceIdentityV1, StoreError> {
    if text(row, "evidence_algorithm")? != super::super::EVIDENCE_IDENTITY_ALGORITHM_V1 {
        return Err(ownership_invariant());
    }
    EvidenceIdentityV1::try_new(
        identity,
        u8::try_from(integer(row, "final_review_round")?).map_err(|_| ownership_invariant())?,
        EventId::new(integer(row, "final_review_event_id")?).map_err(|_| ownership_invariant())?,
        u64::try_from(integer(row, "workspace_generation")?).map_err(|_| ownership_invariant())?,
        parse_value(text(row, "workspace_fingerprint")?)?,
        parse_value(text(row, "checks_digest")?)?,
        parse_value(text(row, "coverage_digest")?)?,
    )
    .map_err(|_| ownership_invariant())
}

pub(super) fn required_commit_metadata(
    row: &sqlx::sqlite::SqliteRow,
    merge: bool,
) -> Result<DeliveryCommitMetadata, StoreError> {
    let (
        author_name,
        author_email,
        committer_name,
        committer_email,
        author_date,
        committer_date,
        version_name,
        bytes_name,
    ) = if merge {
        (
            "merge_author_name",
            "merge_author_email",
            "merge_committer_name",
            "merge_committer_email",
            "merge_author_date_bytes",
            "merge_committer_date_bytes",
            "merge_message_template_version",
            "merge_message_bytes",
        )
    } else {
        (
            "author_name",
            "author_email",
            "committer_name",
            "committer_email",
            "author_date_bytes",
            "committer_date_bytes",
            "commit_message_template_version",
            "commit_message_bytes",
        )
    };
    Ok(DeliveryCommitMetadata {
        author_name: text(row, author_name)?,
        author_email: text(row, author_email)?,
        committer_name: text(row, committer_name)?,
        committer_email: text(row, committer_email)?,
        author_date_bytes: text(row, author_date)?,
        committer_date_bytes: text(row, committer_date)?,
        message_template_version: positive_u32(integer(row, version_name)?)?,
        message_bytes: row.try_get(bytes_name).map_err(|_| ownership_invariant())?,
    })
}

pub(super) fn text(row: &sqlx::sqlite::SqliteRow, column: &str) -> Result<String, StoreError> {
    row.try_get(column).map_err(|_| ownership_invariant())
}

pub(super) fn optional_text(
    row: &sqlx::sqlite::SqliteRow,
    column: &str,
) -> Result<Option<String>, StoreError> {
    row.try_get(column).map_err(|_| ownership_invariant())
}

pub(super) fn integer(row: &sqlx::sqlite::SqliteRow, column: &str) -> Result<i64, StoreError> {
    row.try_get(column).map_err(|_| ownership_invariant())
}

pub(super) fn optional_integer(
    row: &sqlx::sqlite::SqliteRow,
    column: &str,
) -> Result<Option<i64>, StoreError> {
    row.try_get(column).map_err(|_| ownership_invariant())
}

pub(super) fn positive_u32(value: i64) -> Result<u32, StoreError> {
    let value = u32::try_from(value).map_err(|_| ownership_invariant())?;
    if value == 0 {
        Err(ownership_invariant())
    } else {
        Ok(value)
    }
}

pub(super) fn parse_version(value: i64) -> Result<DeliveryVersion, StoreError> {
    DeliveryVersion::try_new(u64::try_from(value).map_err(|_| ownership_invariant())?)
        .map_err(|_| ownership_invariant())
}

pub(super) fn parse_value<T: FromStr>(value: String) -> Result<T, StoreError> {
    value.parse().map_err(|_| ownership_invariant())
}

pub(super) fn parse_optional<T: FromStr>(value: Option<String>) -> Result<Option<T>, StoreError> {
    value.map(parse_value).transpose()
}

pub(super) fn parse_task_id(value: String) -> Result<TaskId, StoreError> {
    value.parse().map_err(|_| ownership_invariant())
}

pub(super) fn parse_optional_task_id(value: Option<String>) -> Result<Option<TaskId>, StoreError> {
    value.map(parse_task_id).transpose()
}

pub(super) fn parse_uuid(value: String) -> Result<uuid::Uuid, StoreError> {
    let parsed = uuid::Uuid::parse_str(&value).map_err(|_| ownership_invariant())?;
    if parsed.is_nil() || parsed.hyphenated().to_string() != value {
        Err(ownership_invariant())
    } else {
        Ok(parsed)
    }
}

pub(super) fn parse_optional_uuid(value: Option<String>) -> Result<Option<uuid::Uuid>, StoreError> {
    value.map(parse_uuid).transpose()
}

pub(super) fn canonical_path(value: String) -> Result<CanonicalPath, StoreError> {
    CanonicalPath::try_from_canonical(PathBuf::from(value)).map_err(|_| ownership_invariant())
}

pub(super) fn parse_source_state(value: String) -> Result<DeliverySourceState, StoreError> {
    value.parse().map_err(|_| ownership_invariant())
}

pub(super) fn parse_merge_state(value: String) -> Result<MergeOperationState, StoreError> {
    value.parse().map_err(|_| ownership_invariant())
}

pub(super) fn parse_cleanup_state(value: String) -> Result<CleanupOperationState, StoreError> {
    value.parse().map_err(|_| ownership_invariant())
}

pub(super) fn parse_worktree_state(value: String) -> Result<WorktreeDisposition, StoreError> {
    value.parse().map_err(|_| ownership_invariant())
}

pub(super) fn parse_branch_state(value: String) -> Result<BranchDisposition, StoreError> {
    value.parse().map_err(|_| ownership_invariant())
}
