use std::str::FromStr;

use coding_agent_domain::{ClientRequestId, Task};
use coding_agent_store::{
    DeleteBranchCommandRequest, DeliveryOperationId, DeliveryVersion, EvidenceIdentityV1,
    GitBranchRef, GitCommitOid, RemoveWorktreeCommandRequest, Store,
};

use super::{
    ADMIN_IDENTITY, COMMON_IDENTITY, DELIVERY_TIMESTAMP, MERGE_COMMIT, SOURCE_COMMIT, accept_merge,
    create_committed_source, insert_preflight, mark_preflight_ready,
};

pub async fn create_merged_delivery(
    store: &Store,
    task: &Task,
    evidence: &EvidenceIdentityV1,
) -> DeliveryOperationId {
    let operation_id = DeliveryOperationId::new();
    insert_preflight(store, task, evidence, operation_id).await;
    mark_preflight_ready(store, operation_id).await;
    accept_merge(store, task, operation_id).await;
    create_committed_source(store, task, operation_id).await;
    finish_merged_delivery(store, task, operation_id).await;
    operation_id
}

pub async fn finish_merged_delivery(store: &Store, task: &Task, operation_id: DeliveryOperationId) {
    mark_merge_pending(store, task, operation_id).await;
    complete_merge_with_disposition(store, task, operation_id).await;
}

pub async fn create_worktree_cleanup(store: &Store, task: &Task) -> DeliveryOperationId {
    create_worktree_cleanup_with_operation_id(store, task, DeliveryOperationId::new()).await
}

pub async fn create_worktree_cleanup_with_operation_id(
    store: &Store,
    task: &Task,
    operation_id: DeliveryOperationId,
) -> DeliveryOperationId {
    let (state, version): (String, i64) = sqlx::query_as(
        "SELECT worktree_state, worktree_version FROM task_artifact_dispositions \
         WHERE task_id = ?",
    )
    .bind(task.id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    create_cleanup(
        store,
        task,
        operation_id,
        CleanupFixture::worktree(&state, version),
    )
    .await
}

pub async fn complete_worktree_cleanup(
    store: &Store,
    task: &Task,
    operation_id: DeliveryOperationId,
) {
    let mut transaction = store.pool().begin().await.unwrap();
    sqlx::query(
        "UPDATE task_artifact_dispositions SET worktree_state = 'retained_unlocked', \
             worktree_version = 2, worktree_cleanup_operation_id = ?, \
             worktree_cleanup_operation_version = 2, \
             worktree_cleanup_operation_state = 'unlocked_pending_remove', \
             worktree_updated_at = ? WHERE task_id = ?",
    )
    .bind(operation_id.to_string())
    .bind(DELIVERY_TIMESTAMP)
    .bind(task.id.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_cleanup_operations SET state = 'unlocked_pending_remove', \
             expected_disposition_version = 2, version = 2, updated_at = ? \
         WHERE operation_id = ?",
    )
    .bind(DELIVERY_TIMESTAMP)
    .bind(operation_id.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    transaction.commit().await.unwrap();

    sqlx::query(
        "UPDATE task_cleanup_operations SET state = 'remove_pending', version = 3, \
             updated_at = ? WHERE operation_id = ?",
    )
    .bind(DELIVERY_TIMESTAMP)
    .bind(operation_id.to_string())
    .execute(store.pool())
    .await
    .unwrap();

    let mut transaction = store.pool().begin().await.unwrap();
    sqlx::query(
        "UPDATE task_artifact_dispositions SET worktree_state = 'removed', \
             worktree_version = 3, worktree_cleanup_operation_id = ?, \
             worktree_cleanup_operation_version = 4, \
             worktree_cleanup_operation_state = 'completed', worktree_updated_at = ? \
         WHERE task_id = ?",
    )
    .bind(operation_id.to_string())
    .bind(DELIVERY_TIMESTAMP)
    .bind(task.id.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_cleanup_operations SET state = 'completed', \
             expected_disposition_version = 3, version = 4, updated_at = ? \
         WHERE operation_id = ?",
    )
    .bind(DELIVERY_TIMESTAMP)
    .bind(operation_id.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    transaction.commit().await.unwrap();
}

pub async fn reconcile_worktree_cleanup(
    store: &Store,
    task: &Task,
    operation_id: DeliveryOperationId,
) {
    let (worktree_version, cleanup_version): (i64, i64) = sqlx::query_as(
        "SELECT disposition.worktree_version, cleanup.version \
         FROM task_artifact_dispositions disposition \
         JOIN task_cleanup_operations cleanup ON cleanup.disposition_task_id = disposition.task_id \
         WHERE cleanup.operation_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    let mut transaction = store.pool().begin().await.unwrap();
    sqlx::query(
        "UPDATE task_artifact_dispositions SET worktree_state = 'reconciliation_required', \
             worktree_version = ?, worktree_failure_code = 'DELIVERY_RECONCILIATION_REQUIRED', \
             worktree_cleanup_operation_id = ?, worktree_cleanup_operation_version = ?, \
             worktree_cleanup_operation_state = 'reconciliation_required', \
             worktree_updated_at = ? WHERE task_id = ?",
    )
    .bind(worktree_version + 1)
    .bind(operation_id.to_string())
    .bind(cleanup_version + 1)
    .bind(DELIVERY_TIMESTAMP)
    .bind(task.id.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_cleanup_operations SET state = 'reconciliation_required', \
             failure_code = 'DELIVERY_RECONCILIATION_REQUIRED', expected_disposition_version = ?, \
             version = ?, updated_at = ? WHERE operation_id = ?",
    )
    .bind(worktree_version + 1)
    .bind(cleanup_version + 1)
    .bind(DELIVERY_TIMESTAMP)
    .bind(operation_id.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    transaction.commit().await.unwrap();
}

pub async fn fail_worktree_cleanup(store: &Store, operation_id: DeliveryOperationId) {
    let (task_id, state, version, disposition_version): (String, String, i64, i64) =
        sqlx::query_as(
            "SELECT task_id, state, version, expected_disposition_version \
             FROM task_cleanup_operations WHERE operation_id = ?",
        )
        .bind(operation_id.to_string())
        .fetch_one(store.pool())
        .await
        .unwrap();
    let (state, version) = if state == "unlock_pending" {
        let mut transaction = store.pool().begin().await.unwrap();
        sqlx::query(
            "UPDATE task_artifact_dispositions SET worktree_state = 'retained_unlocked', \
                 worktree_version = ?, worktree_cleanup_operation_id = ?, \
                 worktree_cleanup_operation_version = ?, \
                 worktree_cleanup_operation_state = 'unlocked_pending_remove', \
                 worktree_updated_at = ? WHERE task_id = ?",
        )
        .bind(disposition_version + 1)
        .bind(operation_id.to_string())
        .bind(version + 1)
        .bind(DELIVERY_TIMESTAMP)
        .bind(&task_id)
        .execute(&mut *transaction)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE task_cleanup_operations SET state = 'unlocked_pending_remove', \
                 expected_disposition_version = ?, version = ?, updated_at = ? \
             WHERE operation_id = ?",
        )
        .bind(disposition_version + 1)
        .bind(version + 1)
        .bind(DELIVERY_TIMESTAMP)
        .bind(operation_id.to_string())
        .execute(&mut *transaction)
        .await
        .unwrap();
        transaction.commit().await.unwrap();
        sqlx::query(
            "UPDATE task_cleanup_operations SET state = 'remove_pending', \
                 version = ?, updated_at = ? WHERE operation_id = ?",
        )
        .bind(version + 2)
        .bind(DELIVERY_TIMESTAMP)
        .bind(operation_id.to_string())
        .execute(store.pool())
        .await
        .unwrap();
        ("remove_pending".to_owned(), version + 2)
    } else {
        (state, version)
    };
    assert_eq!(state, "remove_pending");
    sqlx::query(
        "UPDATE task_cleanup_operations SET state = 'failed', \
             failure_code = 'TARGET_WORKTREE_DIRTY', version = ?, updated_at = ? \
         WHERE operation_id = ?",
    )
    .bind(version + 1)
    .bind(DELIVERY_TIMESTAMP)
    .bind(operation_id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
}

pub async fn create_branch_cleanup(
    store: &Store,
    task: &Task,
    target_head: &str,
) -> DeliveryOperationId {
    create_cleanup(
        store,
        task,
        DeliveryOperationId::new(),
        CleanupFixture::branch(target_head),
    )
    .await
}

pub async fn complete_branch_cleanup(
    store: &Store,
    task: &Task,
    operation_id: DeliveryOperationId,
) {
    let mut transaction = store.pool().begin().await.unwrap();
    sqlx::query(
        "UPDATE task_artifact_dispositions SET branch_state = 'deleted', branch_version = 2, \
             branch_cleanup_operation_id = ?, branch_cleanup_operation_version = 2, \
             branch_cleanup_operation_state = 'completed', branch_updated_at = ? \
         WHERE task_id = ?",
    )
    .bind(operation_id.to_string())
    .bind(DELIVERY_TIMESTAMP)
    .bind(task.id.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_cleanup_operations SET state = 'completed', \
             expected_disposition_version = 2, version = 2, updated_at = ? \
         WHERE operation_id = ?",
    )
    .bind(DELIVERY_TIMESTAMP)
    .bind(operation_id.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    transaction.commit().await.unwrap();
}

pub async fn fail_branch_cleanup(store: &Store, operation_id: DeliveryOperationId) {
    sqlx::query(
        "UPDATE task_cleanup_operations SET state = 'failed', \
             failure_code = 'SOURCE_BRANCH_NOT_MERGED', version = 2, updated_at = ? \
         WHERE operation_id = ?",
    )
    .bind(DELIVERY_TIMESTAMP)
    .bind(operation_id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
}

pub async fn reconcile_branch_cleanup(
    store: &Store,
    task: &Task,
    operation_id: DeliveryOperationId,
) {
    let mut transaction = store.pool().begin().await.unwrap();
    sqlx::query(
        "UPDATE task_artifact_dispositions SET branch_state = 'reconciliation_required', \
             branch_version = 2, branch_failure_code = 'DELIVERY_RECONCILIATION_REQUIRED', \
             branch_cleanup_operation_id = ?, branch_cleanup_operation_version = 2, \
             branch_cleanup_operation_state = 'reconciliation_required', branch_updated_at = ? \
         WHERE task_id = ?",
    )
    .bind(operation_id.to_string())
    .bind(DELIVERY_TIMESTAMP)
    .bind(task.id.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_cleanup_operations SET state = 'reconciliation_required', \
             failure_code = 'DELIVERY_RECONCILIATION_REQUIRED', expected_disposition_version = 2, \
             version = 2, updated_at = ? WHERE operation_id = ?",
    )
    .bind(DELIVERY_TIMESTAMP)
    .bind(operation_id.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    transaction.commit().await.unwrap();
}

async fn mark_merge_pending(store: &Store, task: &Task, operation_id: DeliveryOperationId) {
    sqlx::query(
        "UPDATE task_merge_operations SET delivery_source_task_id = ?, source_commit_oid = ?, \
             expected_merge_commit_oid = ?, state = 'merge_pending', version = 4, updated_at = ? \
         WHERE operation_id = ?",
    )
    .bind(task.id.to_string())
    .bind(SOURCE_COMMIT)
    .bind(MERGE_COMMIT)
    .bind(DELIVERY_TIMESTAMP)
    .bind(operation_id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
}

async fn complete_merge_with_disposition(
    store: &Store,
    task: &Task,
    operation_id: DeliveryOperationId,
) {
    let mut transaction = store.pool().begin().await.unwrap();
    sqlx::query(
        "UPDATE task_merge_operations SET state = 'merged', merged_disposition_task_id = ?, \
             version = 5, updated_at = ? WHERE operation_id = ?",
    )
    .bind(task.id.to_string())
    .bind(DELIVERY_TIMESTAMP)
    .bind(operation_id.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO task_artifact_dispositions (task_id, repository_id, attempt, \
             merged_operation_id, delivery_source_task_id, source_commit_oid, worktree_state, \
             worktree_version, worktree_failure_code, worktree_updated_at, branch_state, \
             branch_version, branch_failure_code, branch_updated_at, created_at) \
         VALUES (?, ?, ?, ?, ?, ?, 'retained_locked', 1, NULL, ?, \
             'retained', 1, NULL, ?, ?)",
    )
    .bind(task.id.to_string())
    .bind(task.repository_id.to_string())
    .bind(i64::from(task.attempt))
    .bind(operation_id.to_string())
    .bind(task.id.to_string())
    .bind(SOURCE_COMMIT)
    .bind(DELIVERY_TIMESTAMP)
    .bind(DELIVERY_TIMESTAMP)
    .bind(DELIVERY_TIMESTAMP)
    .execute(&mut *transaction)
    .await
    .unwrap();
    transaction.commit().await.unwrap();
}

#[derive(Debug, Clone, Copy)]
struct CleanupFixture<'a> {
    kind: &'static str,
    state: &'static str,
    expected_disposition_version: i64,
    expected_target_ref: Option<&'static str>,
    expected_target_head: Option<&'a str>,
    command_kind: &'static str,
    response_discriminator: &'static str,
}

impl CleanupFixture<'_> {
    fn worktree(worktree_state: &str, version: i64) -> Self {
        let state = match worktree_state {
            "retained_locked" => "unlock_pending",
            "retained_unlocked" => "remove_pending",
            _ => panic!("unsupported worktree cleanup fixture state"),
        };
        Self {
            kind: "remove_worktree",
            state,
            expected_disposition_version: version,
            expected_target_ref: None,
            expected_target_head: None,
            command_kind: "remove_worktree",
            response_discriminator: "worktree_cleanup_accepted",
        }
    }

    fn branch(target_head: &str) -> CleanupFixture<'_> {
        CleanupFixture {
            kind: "delete_branch",
            state: "delete_pending",
            expected_disposition_version: 1,
            expected_target_ref: Some("refs/heads/main"),
            expected_target_head: Some(target_head),
            command_kind: "delete_branch",
            response_discriminator: "branch_cleanup_accepted",
        }
    }
}

async fn create_cleanup(
    store: &Store,
    task: &Task,
    operation_id: DeliveryOperationId,
    fixture: CleanupFixture<'_>,
) -> DeliveryOperationId {
    let receipt_id = uuid::Uuid::new_v4();
    let artifact = store.load_attempt_artifact(task.id).await.unwrap().unwrap();
    let source_ref = format!("refs/heads/{}", artifact.branch_name);
    let merged_operation_id: String = sqlx::query_scalar(
        "SELECT merged_operation_id FROM task_artifact_dispositions WHERE task_id = ?",
    )
    .bind(task.id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    let origin_version =
        DeliveryVersion::try_new(fixture.expected_disposition_version as u64).unwrap();
    let request_hash = match fixture.kind {
        "remove_worktree" => RemoveWorktreeCommandRequest::try_new(
            ClientRequestId::from_str(&receipt_id.to_string()).unwrap(),
            task.id,
            origin_version,
            DeliveryOperationId::from_str(&merged_operation_id).unwrap(),
            GitBranchRef::from_str(&source_ref).unwrap(),
            GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        )
        .unwrap()
        .canonical_request_hash(),
        "delete_branch" => DeleteBranchCommandRequest::try_new(
            ClientRequestId::from_str(&receipt_id.to_string()).unwrap(),
            task.id,
            origin_version,
            DeliveryOperationId::from_str(&merged_operation_id).unwrap(),
            GitBranchRef::from_str(&source_ref).unwrap(),
            GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
            GitBranchRef::from_str(fixture.expected_target_ref.unwrap()).unwrap(),
            GitCommitOid::from_str(fixture.expected_target_head.unwrap()).unwrap(),
        )
        .unwrap()
        .canonical_request_hash(),
        _ => panic!("unsupported cleanup fixture kind"),
    };
    let mut transaction = store.pool().begin().await.unwrap();
    sqlx::query(
        "INSERT INTO task_cleanup_operations (operation_id, task_id, repository_id, attempt, \
             kind, origin_receipt_id, disposition_task_id, expected_worktree_path, \
             expected_admin_identity_algorithm, expected_admin_identity_digest, \
             expected_common_git_identity_algorithm, expected_common_git_identity_digest, \
             expected_source_ref, expected_source_oid, expected_disposition_version, \
             expected_target_ref, expected_target_head, origin_target_head, state, failure_code, version, \
             created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'directory_identity_v1', ?, \
             'directory_identity_v1', ?, ?, ?, ?, ?, ?, ?, ?, NULL, 1, ?, ?)",
    )
    .bind(operation_id.to_string())
    .bind(task.id.to_string())
    .bind(task.repository_id.to_string())
    .bind(i64::from(task.attempt))
    .bind(fixture.kind)
    .bind(receipt_id.to_string())
    .bind(task.id.to_string())
    .bind(artifact.worktree_path.to_string())
    .bind(ADMIN_IDENTITY)
    .bind(COMMON_IDENTITY)
    .bind(source_ref)
    .bind(SOURCE_COMMIT)
    .bind(fixture.expected_disposition_version)
    .bind(fixture.expected_target_ref)
    .bind(fixture.expected_target_head)
    .bind(fixture.expected_target_head)
    .bind(fixture.state)
    .bind(DELIVERY_TIMESTAMP)
    .bind(DELIVERY_TIMESTAMP)
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO task_delivery_command_receipts (client_request_id, command_kind, task_id, \
             repository_id, attempt, request_hash_domain, request_hash_version, \
             request_hash_algorithm, canonical_request_hash, operation_kind, operation_id, \
             merge_operation_id, cleanup_operation_id, accepted_operation_version, \
             accepted_operation_state, response_discriminator, created_at) VALUES (?, ?, ?, ?, ?, \
             'coding-agent-delivery-command-request', 1, 'sha256', ?, 'cleanup_operation', ?, \
             NULL, ?, 1, ?, ?, ?)",
    )
    .bind(receipt_id.to_string())
    .bind(fixture.command_kind)
    .bind(task.id.to_string())
    .bind(task.repository_id.to_string())
    .bind(i64::from(task.attempt))
    .bind(request_hash.as_str())
    .bind(operation_id.to_string())
    .bind(operation_id.to_string())
    .bind(fixture.state)
    .bind(fixture.response_discriminator)
    .bind(DELIVERY_TIMESTAMP)
    .execute(&mut *transaction)
    .await
    .unwrap();
    transaction.commit().await.unwrap();
    operation_id
}
