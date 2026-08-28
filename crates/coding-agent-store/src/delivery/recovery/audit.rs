use coding_agent_domain::TaskId;
use sqlx::SqliteConnection;

use crate::StoreError;
use crate::delivery::{DeliveryIdentity, DeliveryOwnershipSnapshot, DirectoryIdentity};

pub(super) struct AuditedDeliveryOwnership {
    pub(super) identity: DeliveryIdentity,
    pub(super) expected_common_git_identity: DirectoryIdentity,
    pub(super) ownership: DeliveryOwnershipSnapshot,
}

pub(super) async fn load_all(
    connection: &mut SqliteConnection,
) -> Result<Vec<AuditedDeliveryOwnership>, StoreError> {
    audit_global_orphans(&mut *connection).await?;
    let task_ids = delivery_task_ids(&mut *connection).await?;
    let mut audited = Vec::with_capacity(task_ids.len());
    for stored_task_id in task_ids {
        let task_id = stored_task_id
            .parse::<TaskId>()
            .map_err(|_| recovery_invariant())?;
        let snapshot = super::super::eligibility::load_snapshot(&mut *connection, task_id)
            .await?
            .ok_or_else(recovery_invariant)?;
        if !snapshot.ownership.is_delivery_owned() {
            return Err(recovery_invariant());
        }
        let expected_common_git_identity = common_git_identity(&mut *connection, task_id).await?;
        let identity = DeliveryIdentity::try_new(
            snapshot.task.id,
            snapshot.task.repository_id,
            snapshot.task.attempt,
        )
        .map_err(|_| recovery_invariant())?;
        audited.push(AuditedDeliveryOwnership {
            identity,
            expected_common_git_identity,
            ownership: snapshot.ownership,
        });
    }
    Ok(audited)
}

async fn delivery_task_ids(connection: &mut SqliteConnection) -> Result<Vec<String>, StoreError> {
    sqlx::query_scalar(
        "SELECT task_id FROM task_delivery_sources \
         UNION SELECT task_id FROM task_merge_operations \
         UNION SELECT task_id FROM task_artifact_dispositions \
         UNION SELECT task_id FROM task_cleanup_operations \
         UNION SELECT task_id FROM task_delivery_command_receipts \
         ORDER BY task_id",
    )
    .fetch_all(connection)
    .await
    .map_err(StoreError::from)
}

async fn common_git_identity(
    connection: &mut SqliteConnection,
    task_id: TaskId,
) -> Result<DirectoryIdentity, StoreError> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT common_git_identity_algorithm, common_git_identity_digest \
         FROM task_delivery_sources WHERE task_id = ? \
         UNION \
         SELECT common_git_identity_algorithm, common_git_identity_digest \
         FROM task_merge_operations WHERE task_id = ? \
         UNION \
         SELECT expected_common_git_identity_algorithm, expected_common_git_identity_digest \
         FROM task_cleanup_operations WHERE task_id = ?",
    )
    .bind(task_id.to_string())
    .bind(task_id.to_string())
    .bind(task_id.to_string())
    .fetch_all(connection)
    .await?;
    let [(algorithm, digest)] = rows.as_slice() else {
        return Err(recovery_invariant());
    };
    DirectoryIdentity::try_new(algorithm, digest).map_err(|_| recovery_invariant())
}

async fn audit_global_orphans(connection: &mut SqliteConnection) -> Result<(), StoreError> {
    let invalid: i64 = sqlx::query_scalar(
        "SELECT EXISTS( \
             SELECT 1 \
             FROM task_delivery_operation_transitions transition_row \
             LEFT JOIN task_delivery_sources source \
               ON transition_row.entity_kind = 'delivery_source' \
              AND source.task_id = transition_row.entity_id \
             LEFT JOIN task_merge_operations merge_operation \
               ON transition_row.entity_kind = 'merge_operation' \
              AND merge_operation.operation_id = transition_row.entity_id \
             LEFT JOIN task_cleanup_operations cleanup \
               ON transition_row.entity_kind = 'cleanup_operation' \
              AND cleanup.operation_id = transition_row.entity_id \
             LEFT JOIN task_artifact_dispositions disposition \
               ON transition_row.entity_kind IN ( \
                    'worktree_disposition', 'branch_disposition' \
                  ) \
              AND disposition.task_id = transition_row.entity_id \
             WHERE transition_row.entity_kind NOT IN ( \
                       'delivery_source', 'merge_operation', 'cleanup_operation', \
                       'worktree_disposition', 'branch_disposition' \
                   ) \
                OR (transition_row.entity_kind = 'delivery_source' \
                    AND source.task_id IS NULL) \
                OR (transition_row.entity_kind = 'merge_operation' \
                    AND merge_operation.operation_id IS NULL) \
                OR (transition_row.entity_kind = 'cleanup_operation' \
                    AND cleanup.operation_id IS NULL) \
                OR (transition_row.entity_kind IN ( \
                        'worktree_disposition', 'branch_disposition' \
                    ) AND disposition.task_id IS NULL) \
             UNION ALL \
             SELECT 1 \
             FROM task_merge_conflicts conflict \
             LEFT JOIN task_merge_operations merge_operation \
               ON merge_operation.operation_id = conflict.operation_id \
             WHERE merge_operation.operation_id IS NULL \
             UNION ALL \
             SELECT 1 \
             FROM task_cleanup_target_head_observations observation \
             LEFT JOIN task_cleanup_operations cleanup \
               ON cleanup.operation_id = observation.cleanup_operation_id \
             LEFT JOIN task_delivery_operation_transitions transition_row \
               ON transition_row.entity_kind = 'cleanup_operation' \
              AND transition_row.entity_id = observation.cleanup_operation_id \
              AND transition_row.entity_version = observation.operation_version \
             WHERE cleanup.operation_id IS NULL \
                OR transition_row.transition_id IS NULL \
             UNION ALL \
             SELECT 1 \
             FROM task_delivery_command_receipts receipt \
             LEFT JOIN task_merge_operations merge_operation \
               ON merge_operation.operation_id = receipt.operation_id \
             LEFT JOIN task_cleanup_operations cleanup \
               ON cleanup.operation_id = receipt.operation_id \
             WHERE COALESCE(( \
                 ( \
                     receipt.command_kind = 'preflight' \
                     AND receipt.operation_kind = 'merge_operation' \
                     AND receipt.operation_id = receipt.merge_operation_id \
                     AND receipt.cleanup_operation_id IS NULL \
                     AND receipt.cleanup_merged_operation_id IS NULL \
                     AND merge_operation.operation_id IS NOT NULL \
                     AND merge_operation.preflight_receipt_id = receipt.client_request_id \
                     AND merge_operation.task_id = receipt.task_id \
                     AND merge_operation.repository_id = receipt.repository_id \
                     AND merge_operation.attempt = receipt.attempt \
                 ) \
                 OR ( \
                     receipt.command_kind = 'accept_merge' \
                     AND receipt.operation_kind = 'merge_operation' \
                     AND receipt.operation_id = receipt.merge_operation_id \
                     AND receipt.cleanup_operation_id IS NULL \
                     AND receipt.cleanup_merged_operation_id IS NULL \
                     AND merge_operation.operation_id IS NOT NULL \
                     AND merge_operation.accept_receipt_id = receipt.client_request_id \
                     AND merge_operation.task_id = receipt.task_id \
                     AND merge_operation.repository_id = receipt.repository_id \
                     AND merge_operation.attempt = receipt.attempt \
                 ) \
                 OR ( \
                     receipt.command_kind IN ('remove_worktree', 'delete_branch') \
                     AND receipt.operation_kind = 'cleanup_operation' \
                     AND receipt.operation_id = receipt.cleanup_operation_id \
                     AND receipt.merge_operation_id IS NULL \
                     AND receipt.cleanup_merged_operation_id IS NOT NULL \
                     AND cleanup.operation_id IS NOT NULL \
                     AND cleanup.origin_receipt_id = receipt.client_request_id \
                     AND cleanup.kind = receipt.command_kind \
                     AND cleanup.task_id = receipt.task_id \
                     AND cleanup.repository_id = receipt.repository_id \
                     AND cleanup.attempt = receipt.attempt \
                     AND EXISTS ( \
                         SELECT 1 FROM task_artifact_dispositions disposition \
                         WHERE disposition.task_id = cleanup.disposition_task_id \
                           AND disposition.merged_operation_id \
                               = receipt.cleanup_merged_operation_id \
                     ) \
                 ) \
             ), 0) = 0 \
         )",
    )
    .fetch_one(connection)
    .await?;
    if invalid == 0 {
        Ok(())
    } else {
        Err(recovery_invariant())
    }
}

fn recovery_invariant() -> StoreError {
    StoreError::InvariantViolation("delivery recovery snapshot is inconsistent")
}
