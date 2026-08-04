use std::str::FromStr;

use crate::tasks::current_timestamp;
use crate::{Store, StoreError};

use super::{CreatePreflightOutcome, CreatePreflightRequest, invalid_preflight_request};
use crate::delivery::eligibility::load_snapshot;
use crate::delivery::ownership::load_merge_operation_exact;
use crate::delivery::receipts::{ReceiptWrite, insert_receipt, lookup_receipt};
use crate::delivery::transitions::{ReadyPreflightTransition, transition_ready_preflight};
use crate::delivery::{
    DeliveryAcceptedOperationState, DeliveryEligibilitySnapshot, DeliveryIdentity,
    DeliveryOperationId, DeliveryTimestamp, DeliveryVersion, EvidenceIdentityV1, GitBranchRef,
    GitCommitOid, MergeOperationState,
};

const PREFLIGHT_INVARIANT: &str = "delivery preflight creation is inconsistent";

impl Store {
    pub async fn create_merge_preflight(
        &self,
        request: CreatePreflightRequest,
    ) -> Result<CreatePreflightOutcome, StoreError> {
        let mut initial_transaction = self.pool.begin().await?;
        if let Some(receipt) = try_existing_preflight(&mut initial_transaction, &request).await? {
            initial_transaction.commit().await?;
            return Ok(CreatePreflightOutcome::Existing(receipt));
        }
        initial_transaction.commit().await?;

        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        if let Some(receipt) = try_existing_preflight(&mut transaction, &request).await? {
            transaction.commit().await?;
            return Ok(CreatePreflightOutcome::Existing(receipt));
        }

        let snapshot = load_snapshot(&mut transaction, request.command().task_id())
            .await?
            .ok_or(StoreError::TaskNotFound)?;
        classify_snapshot(&snapshot)?;
        let prepared = PreparedPreflight::try_from_snapshot(&request, &snapshot)?;
        let timestamp: DeliveryTimestamp = current_timestamp()?.to_string().parse()?;
        supersede_ready_preflight(&mut transaction, &snapshot, timestamp).await?;

        let operation_id = DeliveryOperationId::new();
        insert_preflight_operation(
            &mut transaction,
            &request,
            &prepared,
            operation_id,
            timestamp,
        )
        .await?;
        let receipt_write = ReceiptWrite::try_new(
            request.command(),
            prepared.identity,
            operation_id,
            DeliveryVersion::initial(),
            DeliveryAcceptedOperationState::PreflightPending,
            timestamp,
        )?;
        insert_receipt(&mut transaction, &receipt_write).await?;
        let receipt = lookup_receipt(&mut transaction, request.command())
            .await?
            .ok_or_else(preflight_invariant)?;
        verify_inserted_snapshot(&mut transaction, operation_id, &receipt).await?;
        transaction.commit().await?;
        Ok(CreatePreflightOutcome::Created(receipt))
    }
}

async fn try_existing_preflight(
    connection: &mut sqlx::SqliteConnection,
    request: &CreatePreflightRequest,
) -> Result<Option<crate::delivery::DeliveryCommandReceipt>, StoreError> {
    let Some(receipt) = lookup_receipt(connection, request.command()).await? else {
        return Ok(None);
    };
    let operation = load_merge_operation_exact(connection, receipt.operation_id).await?;
    if operation.operation_id != receipt.operation_id
        || operation.preflight_receipt_id != receipt.client_request_id
        || operation.provenance.identity != receipt.identity
        || operation.provenance.identity.task_id() != request.command().task_id()
    {
        return Err(preflight_invariant());
    }
    Ok(Some(receipt))
}

struct PreparedPreflight {
    identity: DeliveryIdentity,
    evidence: EvidenceIdentityV1,
    artifact_base: GitCommitOid,
    artifact_source_branch: GitBranchRef,
    artifact_worktree_path: String,
}

impl PreparedPreflight {
    fn try_from_snapshot(
        request: &CreatePreflightRequest,
        snapshot: &DeliveryEligibilitySnapshot,
    ) -> Result<Self, StoreError> {
        let evidence = snapshot
            .evidence_identity
            .clone()
            .ok_or(StoreError::TaskNotMergeEligible)?;
        let artifact = snapshot
            .ownership
            .artifact
            .as_ref()
            .ok_or(StoreError::TaskNotMergeEligible)?;
        let artifact_base =
            GitCommitOid::from_str(&artifact.base_commit).map_err(|_| preflight_invariant())?;
        let artifact_source_branch = format!("refs/heads/{}", artifact.branch_name)
            .parse::<GitBranchRef>()
            .map_err(|_| preflight_invariant())?;
        let input_matches_repository = artifact_base.algorithm() == request.object_algorithm()
            && artifact_base != *request.preflight_source_commit()
            && artifact_source_branch != *request.command().target_branch();
        if !input_matches_repository {
            return Err(invalid_preflight_request());
        }
        let identity = DeliveryIdentity::try_new(
            snapshot.task.id,
            snapshot.task.repository_id,
            snapshot.task.attempt,
        )
        .map_err(|_| preflight_invariant())?;
        if let Some(source) = snapshot.ownership.source.as_ref()
            && (source.candidate_tree != *request.candidate_tree()
                || source.expected_source_commit.as_ref()
                    != Some(request.preflight_source_commit())
                || source.provenance.identity != identity
                || source.provenance.evidence != evidence
                || source.provenance.base_commit != artifact_base
                || source.provenance.source_branch != artifact_source_branch
                || source.provenance.worktree_path != artifact.worktree_path
                || source.provenance.common_git_identity != *request.common_git_identity()
                || source.provenance.worktree_admin_identity != *request.worktree_admin_identity()
                || source.provenance.fixed_lock_reason != "codex-reserved"
                || source.provenance.config_attributes_digest
                    != *request.config_attributes_digest())
        {
            return Err(invalid_preflight_request());
        }
        Ok(Self {
            identity,
            evidence,
            artifact_base,
            artifact_source_branch,
            artifact_worktree_path: artifact.worktree_path.to_string(),
        })
    }
}

fn classify_snapshot(snapshot: &DeliveryEligibilitySnapshot) -> Result<(), StoreError> {
    if snapshot.ownership.requires_reconciliation() {
        return Err(StoreError::DeliveryReconciliationRequired);
    }
    if snapshot.ownership.has_merged_facts() {
        return Err(StoreError::TaskNotMergeEligible);
    }
    if snapshot.ownership.has_blocking_owned_state() {
        return Err(StoreError::DeliveryOperationInProgress);
    }
    if !snapshot.persistent_blockers.is_empty() {
        return Err(StoreError::TaskNotMergeEligible);
    }
    Ok(())
}

async fn supersede_ready_preflight(
    connection: &mut sqlx::SqliteConnection,
    snapshot: &DeliveryEligibilitySnapshot,
    timestamp: DeliveryTimestamp,
) -> Result<(), StoreError> {
    let mut ready = snapshot
        .ownership
        .merge_operations
        .iter()
        .filter(|operation| operation.state == MergeOperationState::PreflightReady);
    let Some(operation) = ready.next() else {
        return Ok(());
    };
    if ready.next().is_some() {
        return Err(preflight_invariant());
    }
    let transition = ReadyPreflightTransition::Superseded;
    let applied = transition_ready_preflight(
        connection,
        operation.operation_id,
        operation.provenance.identity,
        operation.version,
        &transition,
        timestamp,
    )
    .await?;
    if applied.is_none() {
        return Err(preflight_invariant());
    }
    Ok(())
}

async fn insert_preflight_operation(
    connection: &mut sqlx::SqliteConnection,
    request: &CreatePreflightRequest,
    prepared: &PreparedPreflight,
    operation_id: DeliveryOperationId,
    timestamp: DeliveryTimestamp,
) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT INTO task_merge_operations ( \
             operation_id, task_id, repository_id, attempt, evidence_algorithm, \
             final_review_round, final_review_event_id, workspace_generation, \
             workspace_fingerprint, checks_digest, coverage_digest, artifact_base_commit, \
             artifact_source_branch, artifact_worktree_path, common_git_identity_algorithm, \
             common_git_identity_digest, worktree_admin_identity_algorithm, \
             worktree_admin_identity_digest, fixed_lock_reason, candidate_tree_oid, \
             preflight_source_commit_oid, delivery_source_task_id, source_commit_oid, \
             preflight_receipt_id, accept_receipt_id, target_branch, expected_target_head, \
             config_attributes_digest, merge_base_oid, candidate_merge_tree_oid, \
             merge_author_name, merge_author_email, merge_committer_name, merge_committer_email, \
             merge_author_date_bytes, merge_committer_date_bytes, merge_message_template_version, \
             merge_message_bytes, expected_merge_commit_oid, abort_child_receipt_id, \
             abort_merge_head_oid, abort_index_stages_digest, abort_worktree_digest, \
             abort_merge_autostash_proof, merged_disposition_task_id, state, failure_code, \
             version, created_at, updated_at \
         ) VALUES ( \
             ?, ?, ?, ?, 'evidence_identity_v1', ?, ?, ?, ?, ?, ?, ?, ?, ?, \
             'directory_identity_v1', ?, 'directory_identity_v1', ?, 'codex-reserved', ?, ?, \
             NULL, NULL, ?, NULL, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, \
             NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, \
             'preflight_pending', NULL, 1, ?, ? \
         )",
    )
    .bind(operation_id.to_string())
    .bind(prepared.identity.task_id().to_string())
    .bind(prepared.identity.repository_id().to_string())
    .bind(i64::from(prepared.identity.attempt()))
    .bind(i64::from(prepared.evidence.final_review_round()))
    .bind(prepared.evidence.final_review_event_id().get())
    .bind(
        i64::try_from(prepared.evidence.workspace_generation())
            .map_err(|_| preflight_invariant())?,
    )
    .bind(prepared.evidence.workspace_fingerprint().as_str())
    .bind(prepared.evidence.checks_digest().as_str())
    .bind(prepared.evidence.coverage_digest().as_str())
    .bind(prepared.artifact_base.as_str())
    .bind(prepared.artifact_source_branch.as_str())
    .bind(&prepared.artifact_worktree_path)
    .bind(request.common_git_identity().digest.as_str())
    .bind(request.worktree_admin_identity().digest.as_str())
    .bind(request.candidate_tree().as_str())
    .bind(request.preflight_source_commit().as_str())
    .bind(request.command().client_request_id().to_string())
    .bind(request.command().target_branch().as_str())
    .bind(request.command().expected_target_head().as_str())
    .bind(request.config_attributes_digest().as_str())
    .bind(timestamp.to_string())
    .bind(timestamp.to_string())
    .execute(&mut *connection)
    .await?;
    Ok(())
}

async fn verify_inserted_snapshot(
    connection: &mut sqlx::SqliteConnection,
    operation_id: DeliveryOperationId,
    receipt: &crate::delivery::DeliveryCommandReceipt,
) -> Result<(), StoreError> {
    let snapshot = load_snapshot(connection, receipt.identity.task_id())
        .await?
        .ok_or_else(preflight_invariant)?;
    let operation = snapshot
        .ownership
        .merge_operations
        .iter()
        .find(|operation| operation.operation_id == operation_id)
        .ok_or_else(preflight_invariant)?;
    if operation.state != MergeOperationState::PreflightPending
        || operation.version != DeliveryVersion::initial()
        || operation.preflight_receipt_id != receipt.client_request_id
    {
        return Err(preflight_invariant());
    }
    Ok(())
}

fn preflight_invariant() -> StoreError {
    StoreError::InvariantViolation(PREFLIGHT_INVARIANT)
}
