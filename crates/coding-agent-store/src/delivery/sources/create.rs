use crate::delivery::ownership::load_merge_operation_exact;
use crate::delivery::receipts::lookup_receipt;
use crate::delivery::{
    DeliveryAcceptedOperationState, DeliveryCommitMetadata, DeliverySourceRecord,
    DeliverySourceState, DeliveryTimestamp, DeliveryVersion, MergeOperationRecord,
    MergeOperationState,
};
use crate::tasks::current_timestamp;
use crate::{Store, StoreError};

use super::load::load_source_context;
use super::model::DeliverySourceAnchor;
use super::model::{CreateDeliverySourceOutcome, CreateDeliverySourceRequest};
use super::validate::{validate_current_source_reconciliation, validate_replay_anchor};
use super::{
    SOURCE_AUTHOR_EMAIL, SOURCE_AUTHOR_NAME, SOURCE_MESSAGE_TEMPLATE_VERSION, source_invariant,
};

impl Store {
    pub async fn create_delivery_source(
        &self,
        request: CreateDeliverySourceRequest,
    ) -> Result<CreateDeliverySourceOutcome, StoreError> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;

        // The receipt lookup is deliberately the first business query. It preserves
        // the global client UUID/hash conflict semantics before looking at source state.
        let receipt = match lookup_receipt(&mut transaction, request.accept_command()).await {
            Ok(receipt) => receipt,
            Err(error @ StoreError::Database(_)) | Err(error @ StoreError::IdempotencyConflict) => {
                return Err(error);
            }
            Err(_) => return Err(source_invariant()),
        };
        let Some(receipt) = receipt else {
            match load_source_context(&mut transaction, request.accept_command().task_id()).await {
                Ok(_) => {}
                Err(error @ StoreError::Database(_)) => return Err(error),
                Err(_) => return Err(source_invariant()),
            }
            transaction.commit().await?;
            return Ok(CreateDeliverySourceOutcome::Conflict);
        };
        if receipt.accepted_operation_state != DeliveryAcceptedOperationState::Accepted {
            return Err(source_invariant());
        }

        let operation =
            match load_merge_operation_exact(&mut transaction, receipt.operation_id).await {
                Ok(operation) => operation,
                Err(error @ StoreError::Database(_)) => return Err(error),
                Err(_) => return Err(source_invariant()),
            };
        validate_accept_anchor(&request, &receipt, &operation)?;
        let anchor = DeliverySourceAnchor::try_new(
            receipt.identity.task_id(),
            receipt.operation_id,
            receipt.accepted_operation_version,
        )
        .map_err(|_| source_invariant())?;

        if let Some(source) =
            load_source_context(&mut transaction, request.accept_command().task_id()).await?
        {
            validate_replay_anchor(&source, &operation, anchor)?;
            validate_current_source_reconciliation(&mut transaction, &source).await?;
            validate_existing_source(&source, &operation)?;
            transaction.commit().await?;
            return Ok(CreateDeliverySourceOutcome::Existing(source));
        }

        if operation.state != MergeOperationState::Accepted
            || operation.version != receipt.accepted_operation_version
            || operation.failure_code.is_some()
        {
            transaction.commit().await?;
            return Ok(CreateDeliverySourceOutcome::Conflict);
        }

        let timestamp: DeliveryTimestamp = current_timestamp()?.to_string().parse()?;
        let metadata = source_metadata(&operation, timestamp);
        insert_object_pending(&mut transaction, &operation, &metadata, timestamp).await?;
        let source = load_source_context(&mut transaction, request.accept_command().task_id())
            .await?
            .ok_or_else(source_invariant)?;
        validate_existing_source(&source, &operation)?;
        if source.state != DeliverySourceState::ObjectPending
            || source.version != DeliveryVersion::initial()
            || source.failure_code.is_some()
        {
            return Err(source_invariant());
        }
        transaction.commit().await?;
        Ok(CreateDeliverySourceOutcome::Created(source))
    }
}

fn validate_accept_anchor(
    request: &CreateDeliverySourceRequest,
    receipt: &crate::delivery::DeliveryCommandReceipt,
    operation: &MergeOperationRecord,
) -> Result<(), StoreError> {
    let command = request.accept_command();
    let valid = receipt.identity.task_id() == command.task_id()
        && receipt.operation_id == command.preflight_operation_id()
        && operation.operation_id == receipt.operation_id
        && operation.provenance.identity == receipt.identity
        && operation.accept_receipt_id == Some(receipt.client_request_id)
        && operation.target_branch == *command.target_branch()
        && operation.expected_target_head == *command.expected_target_head()
        && operation.provenance.evidence.workspace_generation()
            == command.expected_review_generation()
        && operation.provenance.evidence.workspace_fingerprint()
            == command.expected_workspace_fingerprint();
    if valid {
        Ok(())
    } else {
        Err(source_invariant())
    }
}

fn validate_existing_source(
    source: &DeliverySourceRecord,
    operation: &MergeOperationRecord,
) -> Result<(), StoreError> {
    let valid = source.provenance == operation.provenance
        && source.candidate_tree == operation.candidate_tree
        && source.expected_parent == operation.provenance.base_commit
        && source.commit_metadata.author_name == SOURCE_AUTHOR_NAME
        && source.commit_metadata.author_email == SOURCE_AUTHOR_EMAIL
        && source.commit_metadata.committer_name == SOURCE_AUTHOR_NAME
        && source.commit_metadata.committer_email == SOURCE_AUTHOR_EMAIL
        && source.commit_metadata.message_template_version == SOURCE_MESSAGE_TEMPLATE_VERSION
        && source.commit_metadata.message_bytes == source_message(operation);
    if valid {
        Ok(())
    } else {
        Err(StoreError::DeliveryReconciliationRequired)
    }
}

fn source_metadata(
    operation: &MergeOperationRecord,
    timestamp: DeliveryTimestamp,
) -> DeliveryCommitMetadata {
    let date = format!(
        "{} +0000",
        timestamp.as_utc().as_offset_date_time().unix_timestamp()
    );
    DeliveryCommitMetadata {
        author_name: SOURCE_AUTHOR_NAME.to_owned(),
        author_email: SOURCE_AUTHOR_EMAIL.to_owned(),
        committer_name: SOURCE_AUTHOR_NAME.to_owned(),
        committer_email: SOURCE_AUTHOR_EMAIL.to_owned(),
        author_date_bytes: date.clone(),
        committer_date_bytes: date,
        message_template_version: SOURCE_MESSAGE_TEMPLATE_VERSION,
        message_bytes: source_message(operation),
    }
}

fn source_message(operation: &MergeOperationRecord) -> Vec<u8> {
    format!(
        "coding-agent: deliver task {} attempt {}\n",
        operation.provenance.identity.task_id(),
        operation.provenance.identity.attempt()
    )
    .into_bytes()
}

async fn insert_object_pending(
    connection: &mut sqlx::SqliteConnection,
    operation: &MergeOperationRecord,
    metadata: &DeliveryCommitMetadata,
    timestamp: DeliveryTimestamp,
) -> Result<(), StoreError> {
    let identity = operation.provenance.identity;
    let evidence = &operation.provenance.evidence;
    let result = sqlx::query(
        "INSERT INTO task_delivery_sources ( \
             task_id, repository_id, attempt, evidence_algorithm, final_review_round, \
             final_review_event_id, workspace_generation, workspace_fingerprint, checks_digest, \
             coverage_digest, artifact_base_commit, artifact_source_branch, \
             artifact_worktree_path, common_git_identity_algorithm, common_git_identity_digest, \
             worktree_admin_identity_algorithm, worktree_admin_identity_digest, fixed_lock_reason, \
             config_attributes_digest, origin_accepted_operation_id, origin_accept_receipt_id, \
             origin_accepted_version, candidate_tree_oid, expected_parent_oid, \
             expected_source_commit_oid, author_name, author_email, committer_name, \
             committer_email, author_date_bytes, committer_date_bytes, \
             commit_message_template_version, commit_message_bytes, state, failure_code, version, \
             created_at, updated_at \
         ) VALUES ( \
             ?, ?, ?, 'evidence_identity_v1', ?, ?, ?, ?, ?, ?, ?, ?, ?, \
             'directory_identity_v1', ?, 'directory_identity_v1', ?, ?, ?, ?, ?, ?, ?, ?, NULL, \
             ?, ?, ?, ?, ?, ?, ?, ?, 'object_pending', NULL, 1, ?, ? \
         )",
    )
    .bind(identity.task_id().to_string())
    .bind(identity.repository_id().to_string())
    .bind(i64::from(identity.attempt()))
    .bind(i64::from(evidence.final_review_round()))
    .bind(evidence.final_review_event_id().get())
    .bind(i64::try_from(evidence.workspace_generation()).map_err(|_| source_invariant())?)
    .bind(evidence.workspace_fingerprint().as_str())
    .bind(evidence.checks_digest().as_str())
    .bind(evidence.coverage_digest().as_str())
    .bind(operation.provenance.base_commit.as_str())
    .bind(operation.provenance.source_branch.as_str())
    .bind(operation.provenance.worktree_path.to_string())
    .bind(operation.provenance.common_git_identity.digest.as_str())
    .bind(operation.provenance.worktree_admin_identity.digest.as_str())
    .bind(&operation.provenance.fixed_lock_reason)
    .bind(operation.provenance.config_attributes_digest.as_str())
    .bind(operation.operation_id.to_string())
    .bind(
        operation
            .accept_receipt_id
            .ok_or_else(source_invariant)?
            .to_string(),
    )
    .bind(i64::try_from(operation.version.get()).map_err(|_| source_invariant())?)
    .bind(operation.candidate_tree.as_str())
    .bind(operation.provenance.base_commit.as_str())
    .bind(&metadata.author_name)
    .bind(&metadata.author_email)
    .bind(&metadata.committer_name)
    .bind(&metadata.committer_email)
    .bind(&metadata.author_date_bytes)
    .bind(&metadata.committer_date_bytes)
    .bind(i64::from(metadata.message_template_version))
    .bind(&metadata.message_bytes)
    .bind(timestamp.to_string())
    .bind(timestamp.to_string())
    .execute(&mut *connection)
    .await?;
    if result.rows_affected() != 1 {
        return Err(source_invariant());
    }
    Ok(())
}
