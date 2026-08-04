use std::fmt;

use coding_agent_domain::{Task, TaskId};
use sqlx::SqliteConnection;

use crate::artifacts::load_artifact;
use crate::{AttemptArtifactState, StoreError, TaskAttemptArtifact};

use super::{
    ArtifactDispositionRecord, CleanupOperationRecord, DeliverySourceRecord, DeliverySourceState,
    EvidenceIdentityV1, MergeOperationRecord, MergeOperationState,
};

const OWNERSHIP_INVARIANT: &str = "delivery ownership snapshot is inconsistent";

mod accepted;
mod audit;
mod cleanup;
mod decode;
mod disposition;
mod merge;
mod origin;
mod shape;
mod source;
mod source_origin;
mod transitions;
mod validate;

pub(super) use accepted::{
    AcceptReceiptAudit, audit_accept_receipt, reconciliation_accept_origin_is_exact,
};
use audit::audit_operation_journals;
use cleanup::{
    load_all_cleanup_operations, load_cleanup_operation, project_cleanup_operations,
    validate_cleanup_slot_exclusivity,
};
use disposition::load_disposition;
pub(super) use disposition::validate_merged_disposition_origin;
use merge::{load_merge_operation, select_all_merge_operation_ids, select_merge_operation_ids};
pub(super) use origin::{PreflightReceiptAudit, audit_preflight_receipt};
use source::load_source;
use source_origin::validate_source_origin;
use validate::validate_artifact_parent;
use validate::{validate_merge_slot_exclusivity, validate_ownership_graph};

pub(super) async fn validate_source_merge_reconciliation_pair(
    connection: &mut SqliteConnection,
    source: &DeliverySourceRecord,
    operation: &MergeOperationRecord,
) -> Result<(), StoreError> {
    if !source_merge_reconciliation_values_match(source, operation) {
        return Err(ownership_invariant());
    }
    let source_from: Option<String> = sqlx::query_scalar(
        "SELECT from_state FROM task_delivery_operation_transitions \
         WHERE transition_id = ? AND entity_kind = 'delivery_source' \
           AND entity_id = ? AND entity_version = ? \
           AND to_state = 'reconciliation_required'",
    )
    .bind(source.current_transition_id)
    .bind(source.provenance.identity.task_id().to_string())
    .bind(i64::try_from(source.version.get()).map_err(|_| ownership_invariant())?)
    .fetch_optional(connection)
    .await?;
    let ordered = match source_from.as_deref() {
        Some("object_pending" | "commit_pending") => {
            source.current_transition_id < operation.current_transition_id
        }
        Some("committed") => operation.current_transition_id < source.current_transition_id,
        _ => false,
    };
    if ordered {
        Ok(())
    } else {
        Err(ownership_invariant())
    }
}

pub(super) fn source_merge_reconciliation_values_match(
    source: &DeliverySourceRecord,
    operation: &MergeOperationRecord,
) -> bool {
    source.state == DeliverySourceState::ReconciliationRequired
        && operation.state == MergeOperationState::ReconciliationRequired
        && source.failure_code.is_some()
        && source.failure_code == operation.failure_code
        && source.updated_at == operation.updated_at
        && source.current_transition_id > 0
        && operation.current_transition_id > 0
}

#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryOwnershipSnapshot {
    pub artifact: Option<TaskAttemptArtifact>,
    pub source: Option<DeliverySourceRecord>,
    pub merge_operations: Vec<MergeOperationRecord>,
    pub disposition: Option<ArtifactDispositionRecord>,
    pub cleanup_operations: Vec<CleanupOperationRecord>,
}

impl fmt::Debug for DeliveryOwnershipSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliveryOwnershipSnapshot")
            .field("artifact_present", &self.artifact.is_some())
            .field("source_present", &self.source.is_some())
            .field("merge_operation_count", &self.merge_operations.len())
            .field("disposition_present", &self.disposition.is_some())
            .field("cleanup_operation_count", &self.cleanup_operations.len())
            .field("delivery_owned", &self.is_delivery_owned())
            .field("merged_facts_present", &self.has_merged_facts())
            .field(
                "blocking_owned_state_present",
                &self.has_blocking_owned_state(),
            )
            .field("reconciliation_required", &self.requires_reconciliation())
            .finish()
    }
}

impl DeliveryOwnershipSnapshot {
    pub fn is_delivery_owned(&self) -> bool {
        self.source.is_some()
            || !self.merge_operations.is_empty()
            || self.disposition.is_some()
            || !self.cleanup_operations.is_empty()
    }

    pub fn has_merged_facts(&self) -> bool {
        self.disposition.is_some()
            || self
                .merge_operations
                .iter()
                .any(|operation| operation.state == MergeOperationState::Merged)
    }

    pub(crate) fn has_blocking_owned_state(&self) -> bool {
        self.source.as_ref().is_some_and(|source| {
            matches!(
                source.state,
                DeliverySourceState::ObjectPending | DeliverySourceState::CommitPending
            )
        }) || self.merge_operations.iter().any(|operation| {
            operation.state == MergeOperationState::PreflightPending
                || operation.state.is_side_effect_active()
        }) || self
            .cleanup_operations
            .iter()
            .any(|operation| operation.state.is_side_effect_active())
    }

    pub fn requires_reconciliation(&self) -> bool {
        self.source
            .as_ref()
            .is_some_and(|source| source.state.is_reconciliation())
            || self
                .merge_operations
                .iter()
                .any(|operation| operation.state.is_reconciliation())
            || self.disposition.as_ref().is_some_and(|disposition| {
                disposition.worktree_state.is_reconciliation()
                    || disposition.branch_state.is_reconciliation()
            })
            || self
                .cleanup_operations
                .iter()
                .any(|operation| operation.state.is_reconciliation())
    }
}

pub(crate) async fn load_delivery_ownership(
    connection: &mut SqliteConnection,
    task: &Task,
    expected_evidence: Option<&EvidenceIdentityV1>,
    approved_tuple: bool,
) -> Result<DeliveryOwnershipSnapshot, StoreError> {
    let artifact = load_artifact(&mut *connection, task.id).await?;
    if let Some(artifact) = artifact.as_ref() {
        validate_artifact_parent(task, artifact)?;
    }
    let has_delivery = has_delivery_rows(&mut *connection, task.id).await?;
    if has_delivery {
        validate_delivery_parent(expected_evidence, approved_tuple, artifact.as_ref())?;
    }
    audit_operation_journals(&mut *connection, task.id).await?;
    let source = load_source(&mut *connection, task.id).await?;
    let disposition = load_disposition(&mut *connection, task.id).await?;
    let all_merge_operations = load_task_merge_operation_set(&mut *connection, task.id).await?;
    let projected_merge_ids = select_merge_operation_ids(&mut *connection, task.id).await?;
    let merge_operations = projected_merge_ids
        .into_iter()
        .map(|operation_id| {
            all_merge_operations
                .iter()
                .find(|operation| operation.operation_id == operation_id)
                .cloned()
                .ok_or_else(ownership_invariant)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let all_cleanup_operations = load_all_cleanup_operations(&mut *connection, task.id).await?;
    validate_cleanup_slot_exclusivity(&all_cleanup_operations)?;
    let cleanup_operations =
        project_cleanup_operations(&all_cleanup_operations, disposition.as_ref());
    if has_delivery {
        validate_ownership_graph(
            &mut *connection,
            task,
            expected_evidence.expect("delivery parent validation requires evidence"),
            artifact
                .as_ref()
                .expect("delivery parent validation requires artifact"),
            source.as_ref(),
            &all_merge_operations,
            disposition.as_ref(),
            &all_cleanup_operations,
        )
        .await?;
    }
    Ok(DeliveryOwnershipSnapshot {
        artifact,
        source,
        merge_operations,
        disposition,
        cleanup_operations,
    })
}

pub(super) async fn load_merge_operation_exact(
    connection: &mut SqliteConnection,
    operation_id: super::DeliveryOperationId,
) -> Result<MergeOperationRecord, StoreError> {
    let stored_task: String =
        sqlx::query_scalar("SELECT task_id FROM task_merge_operations WHERE operation_id = ?")
            .bind(operation_id.to_string())
            .fetch_optional(&mut *connection)
            .await?
            .ok_or_else(ownership_invariant)?;
    let task_id = stored_task
        .parse::<TaskId>()
        .map_err(|_| ownership_invariant())?;
    match super::eligibility::load_snapshot(&mut *connection, task_id).await {
        Ok(Some(_)) => {}
        Ok(None) => return Err(ownership_invariant()),
        Err(error @ StoreError::Database(_)) => return Err(error),
        Err(_) => return Err(ownership_invariant()),
    }
    load_merge_operation(&mut *connection, operation_id).await
}

async fn load_task_merge_operation_set(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<Vec<MergeOperationRecord>, StoreError> {
    let operation_ids = select_all_merge_operation_ids(&mut *connection, task_id).await?;
    let mut operations = Vec::with_capacity(operation_ids.len());
    for operation_id in operation_ids {
        operations.push(load_merge_operation(&mut *connection, operation_id).await?);
    }
    validate_merge_slot_exclusivity(&operations)?;
    Ok(operations)
}

pub(super) async fn load_source_exact(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<Option<DeliverySourceRecord>, StoreError> {
    Ok(super::eligibility::load_snapshot(connection, task_id)
        .await?
        .and_then(|snapshot| snapshot.ownership.source))
}

pub(super) async fn load_disposition_exact(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<Option<ArtifactDispositionRecord>, StoreError> {
    load_disposition(connection, task_id).await
}

pub(super) async fn load_cleanup_operation_exact(
    connection: &mut SqliteConnection,
    operation_id: super::DeliveryOperationId,
) -> Result<CleanupOperationRecord, StoreError> {
    let stored_task: String =
        sqlx::query_scalar("SELECT task_id FROM task_cleanup_operations WHERE operation_id = ?")
            .bind(operation_id.to_string())
            .fetch_optional(&mut *connection)
            .await?
            .ok_or_else(ownership_invariant)?;
    let task_id = stored_task
        .parse::<TaskId>()
        .map_err(|_| ownership_invariant())?;
    match super::eligibility::load_snapshot(&mut *connection, task_id).await {
        Ok(Some(_)) => {}
        Ok(None) => return Err(ownership_invariant()),
        Err(error @ StoreError::Database(_)) => return Err(error),
        Err(_) => return Err(ownership_invariant()),
    }
    load_cleanup_operation(connection, operation_id).await
}

async fn has_delivery_rows(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<bool, StoreError> {
    let present: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM task_delivery_sources WHERE task_id = ?) \
          OR EXISTS(SELECT 1 FROM task_merge_operations WHERE task_id = ?) \
          OR EXISTS(SELECT 1 FROM task_artifact_dispositions WHERE task_id = ?) \
          OR EXISTS(SELECT 1 FROM task_cleanup_operations WHERE task_id = ?)",
    )
    .bind(task_id.to_string())
    .bind(task_id.to_string())
    .bind(task_id.to_string())
    .bind(task_id.to_string())
    .fetch_one(connection)
    .await?;
    Ok(present == 1)
}

fn validate_delivery_parent(
    evidence: Option<&EvidenceIdentityV1>,
    approved_tuple: bool,
    artifact: Option<&TaskAttemptArtifact>,
) -> Result<(), StoreError> {
    let valid_artifact =
        artifact.is_some_and(|artifact| artifact.state == AttemptArtifactState::Ready);
    if approved_tuple && evidence.is_some() && valid_artifact {
        Ok(())
    } else {
        Err(ownership_invariant())
    }
}

pub(super) fn ownership_invariant() -> StoreError {
    StoreError::InvariantViolation(OWNERSHIP_INVARIANT)
}
