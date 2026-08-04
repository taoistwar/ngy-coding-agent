use crate::StoreError;
use crate::delivery::ownership::load_merge_operation_exact;
use crate::delivery::receipts::{ReceiptWrite, insert_receipt, lookup_receipt};
use crate::delivery::{
    AcceptMergeCommandRequest, DeliveryAcceptedOperationState, DeliveryCommandReceipt,
    DeliveryCommitMetadata, DeliveryTimestamp, MergeOperationRecord, MergeOperationState,
};
use crate::tasks::current_timestamp;

use super::super::merge_invariant;
use super::super::replay::version_i64;
use super::replay::validate_accept_binding;

const MERGE_AUTHOR_NAME: &str = "Coding Agent";
const MERGE_AUTHOR_EMAIL: &str = "coding-agent@localhost";
const MERGE_MESSAGE_TEMPLATE_VERSION: u32 = 1;

pub(super) async fn apply_fresh_accept(
    connection: &mut sqlx::SqliteConnection,
    request: &AcceptMergeCommandRequest,
    operation: &MergeOperationRecord,
) -> Result<DeliveryCommandReceipt, StoreError> {
    let accepted_version = request.expected_operation_version().next()?;
    let timestamp: DeliveryTimestamp = current_timestamp()?.to_string().parse()?;
    let metadata = merge_metadata(operation, timestamp);
    let updated = sqlx::query(
        "UPDATE task_merge_operations \
         SET accept_receipt_id = ?, merge_author_name = ?, merge_author_email = ?, \
             merge_committer_name = ?, merge_committer_email = ?, \
             merge_author_date_bytes = ?, merge_committer_date_bytes = ?, \
             merge_message_template_version = ?, merge_message_bytes = ?, \
             state = 'accepted', failure_code = NULL, version = ?, updated_at = ? \
         WHERE operation_id = ? AND task_id = ? AND repository_id = ? AND attempt = ? \
           AND state = 'preflight_ready' AND version = ? AND accept_receipt_id IS NULL",
    )
    .bind(request.client_request_id().to_string())
    .bind(&metadata.author_name)
    .bind(&metadata.author_email)
    .bind(&metadata.committer_name)
    .bind(&metadata.committer_email)
    .bind(&metadata.author_date_bytes)
    .bind(&metadata.committer_date_bytes)
    .bind(i64::from(metadata.message_template_version))
    .bind(&metadata.message_bytes)
    .bind(version_i64(accepted_version)?)
    .bind(timestamp.to_string())
    .bind(operation.operation_id.to_string())
    .bind(operation.provenance.identity.task_id().to_string())
    .bind(operation.provenance.identity.repository_id().to_string())
    .bind(i64::from(operation.provenance.identity.attempt()))
    .bind(version_i64(request.expected_operation_version())?)
    .execute(&mut *connection)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(merge_invariant());
    }

    let write = ReceiptWrite::try_new(
        request,
        operation.provenance.identity,
        operation.operation_id,
        accepted_version,
        DeliveryAcceptedOperationState::Accepted,
        timestamp,
    )?;
    insert_receipt(&mut *connection, &write).await?;
    let receipt = lookup_receipt(&mut *connection, request)
        .await?
        .ok_or_else(merge_invariant)?;
    let accepted = load_merge_operation_exact(&mut *connection, operation.operation_id).await?;
    validate_accept_binding(request, &accepted)?;
    if accepted.merge_metadata.as_ref() != Some(&metadata)
        || accepted.state != MergeOperationState::Accepted
        || accepted.version != accepted_version
        || accepted.updated_at != timestamp
        || receipt.created_at != timestamp
    {
        return Err(merge_invariant());
    }
    Ok(receipt)
}

fn merge_metadata(
    operation: &MergeOperationRecord,
    timestamp: DeliveryTimestamp,
) -> DeliveryCommitMetadata {
    let date = format!(
        "{} +0000",
        timestamp.as_utc().as_offset_date_time().unix_timestamp()
    );
    DeliveryCommitMetadata {
        author_name: MERGE_AUTHOR_NAME.to_owned(),
        author_email: MERGE_AUTHOR_EMAIL.to_owned(),
        committer_name: MERGE_AUTHOR_NAME.to_owned(),
        committer_email: MERGE_AUTHOR_EMAIL.to_owned(),
        author_date_bytes: date.clone(),
        committer_date_bytes: date,
        message_template_version: MERGE_MESSAGE_TEMPLATE_VERSION,
        message_bytes: format!(
            "coding-agent: merge task {} attempt {}\n",
            operation.provenance.identity.task_id(),
            operation.provenance.identity.attempt()
        )
        .into_bytes(),
    }
}
