use std::str::FromStr;

use coding_agent_domain::{ClientRequestId, TaskId};
use coding_agent_store::{
    DeleteBranchCommandRequest, DeliveryOperationId, DeliveryVersion, GitBranchRef, GitCommitOid,
    RemoveWorktreeCommandRequest,
};
use sqlx::SqlitePool;

use super::*;

pub async fn create_remove_cleanup(
    pool: &SqlitePool,
    operation_id: &str,
    receipt_id: &str,
) -> Result<(), sqlx::Error> {
    create_cleanup(
        pool,
        CleanupFixture {
            operation_id,
            receipt_id,
            kind: "remove_worktree",
            state: "unlock_pending",
            expected_disposition_version: 1,
            expected_target_ref: None,
            expected_target_head: None,
            response_discriminator: "worktree_cleanup_accepted",
        },
    )
    .await
}

pub async fn advance_disposition_to_removed(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    create_remove_cleanup(pool, CLEANUP_OPERATION_ID, CLEANUP_RECEIPT_ID).await?;
    complete_remove_cleanup(pool).await
}

pub async fn complete_remove_cleanup(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    let mut transaction = pool.begin().await?;
    sqlx::query(
        "UPDATE task_artifact_dispositions
         SET worktree_state = 'retained_unlocked', worktree_version = 2,
             worktree_cleanup_operation_id = ?,
             worktree_cleanup_operation_version = 2,
             worktree_cleanup_operation_state = 'unlocked_pending_remove',
             worktree_updated_at = ? WHERE task_id = ?",
    )
    .bind(CLEANUP_OPERATION_ID)
    .bind(TIMESTAMP)
    .bind(TASK_ID)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "UPDATE task_cleanup_operations
         SET state = 'unlocked_pending_remove', expected_disposition_version = 2,
             version = 2, updated_at = ? WHERE operation_id = ?",
    )
    .bind(TIMESTAMP)
    .bind(CLEANUP_OPERATION_ID)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;

    sqlx::query(
        "UPDATE task_cleanup_operations
         SET state = 'remove_pending', version = 3, updated_at = ?
         WHERE operation_id = ?",
    )
    .bind(TIMESTAMP)
    .bind(CLEANUP_OPERATION_ID)
    .execute(pool)
    .await?;

    let mut transaction = pool.begin().await?;
    sqlx::query(
        "UPDATE task_artifact_dispositions
         SET worktree_state = 'removed', worktree_version = 3,
             worktree_cleanup_operation_id = ?,
             worktree_cleanup_operation_version = 4,
             worktree_cleanup_operation_state = 'completed',
             worktree_updated_at = ? WHERE task_id = ?",
    )
    .bind(CLEANUP_OPERATION_ID)
    .bind(TIMESTAMP)
    .bind(TASK_ID)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "UPDATE task_cleanup_operations
         SET state = 'completed', expected_disposition_version = 3,
             version = 4, updated_at = ? WHERE operation_id = ?",
    )
    .bind(TIMESTAMP)
    .bind(CLEANUP_OPERATION_ID)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await
}

pub async fn create_delete_cleanup(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    create_delete_cleanup_for_target(pool, TARGET_BRANCH, MERGE_COMMIT_OID).await
}

pub async fn create_delete_cleanup_for_target(
    pool: &SqlitePool,
    target_ref: &str,
    target_head: &str,
) -> Result<(), sqlx::Error> {
    create_cleanup(
        pool,
        CleanupFixture {
            operation_id: DELETE_CLEANUP_OPERATION_ID,
            receipt_id: DELETE_CLEANUP_RECEIPT_ID,
            kind: "delete_branch",
            state: "delete_pending",
            expected_disposition_version: 1,
            expected_target_ref: Some(target_ref),
            expected_target_head: Some(target_head),
            response_discriminator: "branch_cleanup_accepted",
        },
    )
    .await
}

pub async fn complete_delete_cleanup(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    let mut transaction = pool.begin().await?;
    sqlx::query(
        "UPDATE task_artifact_dispositions
         SET branch_state = 'deleted', branch_version = 2,
             branch_cleanup_operation_id = ?,
             branch_cleanup_operation_version = 2,
             branch_cleanup_operation_state = 'completed',
             branch_updated_at = ? WHERE task_id = ?",
    )
    .bind(DELETE_CLEANUP_OPERATION_ID)
    .bind(TIMESTAMP)
    .bind(TASK_ID)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "UPDATE task_cleanup_operations
         SET state = 'completed', expected_disposition_version = 2,
             version = 2, updated_at = ? WHERE operation_id = ?",
    )
    .bind(TIMESTAMP)
    .bind(DELETE_CLEANUP_OPERATION_ID)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await
}

#[derive(Debug, Clone, Copy)]
struct CleanupFixture<'a> {
    operation_id: &'a str,
    receipt_id: &'a str,
    kind: &'a str,
    state: &'a str,
    expected_disposition_version: i64,
    expected_target_ref: Option<&'a str>,
    expected_target_head: Option<&'a str>,
    response_discriminator: &'a str,
}

async fn create_cleanup(pool: &SqlitePool, fixture: CleanupFixture<'_>) -> Result<(), sqlx::Error> {
    let request_hash = cleanup_request_hash(fixture);
    let mut transaction = pool.begin().await?;
    insert_cleanup_operation(&mut transaction, fixture).await?;
    insert_cleanup_receipt(&mut transaction, fixture, &request_hash).await?;
    transaction.commit().await
}

fn cleanup_request_hash(fixture: CleanupFixture<'_>) -> String {
    let client_request_id = ClientRequestId::from_str(fixture.receipt_id).unwrap();
    let task_id = TaskId::from_str(TASK_ID).unwrap();
    let version = DeliveryVersion::try_new(fixture.expected_disposition_version as u64).unwrap();
    let merge_operation_id = DeliveryOperationId::from_str(MERGE_OPERATION_ID).unwrap();
    let source_ref = GitBranchRef::from_str(SOURCE_BRANCH).unwrap();
    let source_oid = GitCommitOid::from_str(SOURCE_COMMIT_OID).unwrap();
    match fixture.kind {
        "remove_worktree" => RemoveWorktreeCommandRequest::try_new(
            client_request_id,
            task_id,
            version,
            merge_operation_id,
            source_ref,
            source_oid,
        )
        .unwrap()
        .canonical_request_hash()
        .to_string(),
        "delete_branch" => DeleteBranchCommandRequest::try_new(
            client_request_id,
            task_id,
            version,
            merge_operation_id,
            source_ref,
            source_oid,
            GitBranchRef::from_str(fixture.expected_target_ref.unwrap()).unwrap(),
            GitCommitOid::from_str(fixture.expected_target_head.unwrap()).unwrap(),
        )
        .unwrap()
        .canonical_request_hash()
        .to_string(),
        _ => panic!("unsupported cleanup fixture kind"),
    }
}

async fn insert_cleanup_operation(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    fixture: CleanupFixture<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO task_cleanup_operations (
             operation_id, task_id, repository_id, attempt, kind,
             origin_receipt_id, disposition_task_id, expected_worktree_path,
             expected_admin_identity_algorithm, expected_admin_identity_digest,
             expected_common_git_identity_algorithm,
             expected_common_git_identity_digest, expected_source_ref,
             expected_source_oid, expected_disposition_version,
             expected_target_ref, expected_target_head, origin_target_head, state, failure_code,
             version, created_at, updated_at
         ) VALUES (
             ?, ?, ?, 1, ?, ?, ?, ?,
             'directory_identity_v1', ?, 'directory_identity_v1', ?, ?, ?, ?,
             ?, ?, ?, ?, NULL, 1, ?, ?
         )",
    )
    .bind(fixture.operation_id)
    .bind(TASK_ID)
    .bind(REPOSITORY_ID)
    .bind(fixture.kind)
    .bind(fixture.receipt_id)
    .bind(TASK_ID)
    .bind(WORKTREE_PATH)
    .bind(ADMIN_IDENTITY_DIGEST)
    .bind(COMMON_IDENTITY_DIGEST)
    .bind(SOURCE_BRANCH)
    .bind(SOURCE_COMMIT_OID)
    .bind(fixture.expected_disposition_version)
    .bind(fixture.expected_target_ref)
    .bind(fixture.expected_target_head)
    .bind(fixture.expected_target_head)
    .bind(fixture.state)
    .bind(TIMESTAMP)
    .bind(TIMESTAMP)
    .execute(&mut **transaction)
    .await
    .map(|_| ())
}

async fn insert_cleanup_receipt(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    fixture: CleanupFixture<'_>,
    request_hash: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO task_delivery_command_receipts (
             client_request_id, command_kind, task_id, repository_id, attempt,
             request_hash_domain, request_hash_version, request_hash_algorithm,
             canonical_request_hash, operation_kind, operation_id,
             merge_operation_id, cleanup_operation_id, cleanup_merged_operation_id,
             accepted_operation_version, accepted_operation_state,
             response_discriminator, created_at
         ) VALUES (
             ?, ?, ?, ?, 1,
             'coding-agent-delivery-command-request', 1, 'sha256', ?,
             'cleanup_operation', ?, NULL, ?, ?, 1, ?, ?, ?
         )",
    )
    .bind(fixture.receipt_id)
    .bind(fixture.kind)
    .bind(TASK_ID)
    .bind(REPOSITORY_ID)
    .bind(request_hash)
    .bind(fixture.operation_id)
    .bind(fixture.operation_id)
    .bind(MERGE_OPERATION_ID)
    .bind(fixture.state)
    .bind(fixture.response_discriminator)
    .bind(TIMESTAMP)
    .execute(&mut **transaction)
    .await
    .map(|_| ())
}
