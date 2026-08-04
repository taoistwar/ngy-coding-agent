use sqlx::SqliteConnection;

use super::decode::decode_exact_receipt;
use crate::delivery::receipts::model::{CanonicalCommandRequest, DeliveryCommandReceipt};
use crate::delivery::{DeliveryCommand, DeliveryCommandLookup};
use crate::{Store, StoreError};

impl Store {
    pub async fn lookup_delivery_command(
        &self,
        command: &DeliveryCommand,
    ) -> Result<DeliveryCommandLookup, StoreError> {
        let mut connection = self.pool.acquire().await?;
        Ok(match lookup_receipt(&mut connection, command).await? {
            Some(receipt) => DeliveryCommandLookup::Existing(receipt),
            None => DeliveryCommandLookup::Missing,
        })
    }
}

pub(crate) async fn lookup_receipt(
    connection: &mut SqliteConnection,
    request: &impl CanonicalCommandRequest,
) -> Result<Option<DeliveryCommandReceipt>, StoreError> {
    let key = request.command_request_key();
    let row = sqlx::query(
        "SELECT r.client_request_id, r.command_kind, r.task_id, r.repository_id, r.attempt, \
                r.request_hash_domain, r.request_hash_version, r.request_hash_algorithm, \
                r.canonical_request_hash, r.operation_kind, r.operation_id, \
                r.merge_operation_id, r.cleanup_operation_id, \
                r.accepted_operation_version, r.accepted_operation_state, \
                r.response_discriminator, r.created_at, \
                (SELECT d.merged_operation_id \
                   FROM task_cleanup_operations c \
                   JOIN task_artifact_dispositions d ON d.task_id = c.disposition_task_id \
                  WHERE r.operation_kind = 'cleanup_operation' \
                    AND c.operation_id = r.operation_id) AS cleanup_merged_operation_id, \
                t.transition_id AS historical_transition_id, \
                t.from_state AS historical_from_state, \
                t.failure_code AS historical_failure_code, \
                t.transitioned_at AS historical_transitioned_at, \
                CASE r.command_kind \
                  WHEN 'preflight' THEN EXISTS( \
                    SELECT 1 FROM task_merge_operations m \
                    WHERE m.operation_id = r.operation_id \
                      AND m.preflight_receipt_id = r.client_request_id \
                      AND m.task_id = r.task_id AND m.repository_id = r.repository_id \
                      AND m.attempt = r.attempt \
                  ) \
                  WHEN 'accept_merge' THEN EXISTS( \
                    SELECT 1 FROM task_merge_operations m \
                    WHERE m.operation_id = r.operation_id \
                      AND m.accept_receipt_id = r.client_request_id \
                      AND m.task_id = r.task_id AND m.repository_id = r.repository_id \
                      AND m.attempt = r.attempt \
                  ) \
                  WHEN 'remove_worktree' THEN EXISTS( \
                    SELECT 1 FROM task_cleanup_operations c \
                    WHERE c.operation_id = r.operation_id AND c.kind = 'remove_worktree' \
                      AND c.origin_receipt_id = r.client_request_id \
                      AND c.task_id = r.task_id AND c.repository_id = r.repository_id \
                      AND c.attempt = r.attempt \
                  ) \
                  WHEN 'delete_branch' THEN EXISTS( \
                    SELECT 1 FROM task_cleanup_operations c \
                    WHERE c.operation_id = r.operation_id AND c.kind = 'delete_branch' \
                      AND c.origin_receipt_id = r.client_request_id \
                      AND c.task_id = r.task_id AND c.repository_id = r.repository_id \
                      AND c.attempt = r.attempt \
                  ) \
                  ELSE 0 \
                END AS immutable_pointer_matches \
         FROM task_delivery_command_receipts r \
         LEFT JOIN task_delivery_operation_transitions t \
           ON t.entity_kind = r.operation_kind AND t.entity_id = r.operation_id \
          AND t.entity_version = r.accepted_operation_version \
          AND t.to_state = r.accepted_operation_state \
         WHERE r.client_request_id = ?",
    )
    .bind(key.client_request_id.to_string())
    .fetch_optional(&mut *connection)
    .await?;
    row.map(|row| decode_exact_receipt(&row, &key)).transpose()
}
