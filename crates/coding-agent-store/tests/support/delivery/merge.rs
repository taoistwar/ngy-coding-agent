use sqlx::SqlitePool;

use super::parents::seed_eligible_delivery_parents;
use super::*;

pub async fn bind_preflight_inputs(
    store: &coding_agent_store::Store,
    task_id: coding_agent_domain::TaskId,
    operation_id: coding_agent_store::DeliveryOperationId,
    candidate_tree: &str,
    preflight_source_commit: &str,
) {
    use std::str::FromStr as _;

    let request = coding_agent_store::BindMergePreflightInputsRequest::try_new(
        task_id,
        operation_id,
        coding_agent_store::DeliveryVersion::initial(),
        coding_agent_store::GitTreeOid::from_str(candidate_tree).unwrap(),
        coding_agent_store::GitCommitOid::from_str(preflight_source_commit).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        store.bind_merge_preflight_inputs(request).await.unwrap(),
        coding_agent_store::MergeTransitionOutcome::Applied(_)
    ));
}

pub async fn create_preflight(
    pool: &SqlitePool,
    final_review_event_id: i64,
    operation_id: &str,
    receipt_id: &str,
) -> Result<(), sqlx::Error> {
    create_preflight_with_fixture(
        pool,
        final_review_event_id,
        PreflightFixture::valid(operation_id, receipt_id),
    )
    .await
}

pub async fn create_preflight_with_candidate_tree(
    pool: &SqlitePool,
    final_review_event_id: i64,
    operation_id: &str,
    receipt_id: &str,
    candidate_tree_oid: &str,
) -> Result<(), sqlx::Error> {
    let mut fixture = PreflightFixture::valid(operation_id, receipt_id);
    fixture.candidate_tree_oid = candidate_tree_oid;
    create_preflight_with_fixture(pool, final_review_event_id, fixture).await
}

pub async fn create_preflight_with_target_branch(
    pool: &SqlitePool,
    final_review_event_id: i64,
    operation_id: &str,
    receipt_id: &str,
    target_branch: &str,
) -> Result<(), sqlx::Error> {
    let mut fixture = PreflightFixture::valid(operation_id, receipt_id);
    fixture.target_branch = target_branch.into();
    create_preflight_with_fixture(pool, final_review_event_id, fixture).await
}

pub async fn create_preflight_with_receipt_timestamp(
    pool: &SqlitePool,
    final_review_event_id: i64,
    operation_id: &str,
    receipt_id: &str,
    receipt_created_at: &str,
) -> Result<(), sqlx::Error> {
    let mut fixture = PreflightFixture::valid(operation_id, receipt_id);
    fixture.receipt_created_at = receipt_created_at;
    create_preflight_with_fixture(pool, final_review_event_id, fixture).await
}

pub async fn create_preflight_with_fixture(
    pool: &SqlitePool,
    final_review_event_id: i64,
    fixture: PreflightFixture<'_>,
) -> Result<(), sqlx::Error> {
    let mut transaction = pool.begin().await?;
    insert_preflight_operation(&mut transaction, final_review_event_id, fixture).await?;
    insert_merge_receipt(&mut transaction, merge_receipt_from_preflight(fixture)).await?;
    bind_preflight_inputs_raw(&mut transaction, fixture).await?;
    transaction.commit().await
}

async fn insert_preflight_operation(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    final_review_event_id: i64,
    fixture: PreflightFixture<'_>,
) -> Result<(), sqlx::Error> {
    let query = sqlx::query(
        "INSERT INTO task_merge_operations (
             operation_id, task_id, repository_id, attempt,
             evidence_algorithm, final_review_round, final_review_event_id,
             workspace_generation, workspace_fingerprint, checks_digest,
             coverage_digest, artifact_base_commit, artifact_source_branch,
             artifact_worktree_path, common_git_identity_algorithm,
             common_git_identity_digest, worktree_admin_identity_algorithm,
             worktree_admin_identity_digest, fixed_lock_reason,
             candidate_tree_oid, preflight_source_commit_oid,
             delivery_source_task_id, source_commit_oid, preflight_receipt_id,
             accept_receipt_id, target_branch, expected_target_head,
             config_attributes_digest, target_config_attributes_digest,
             target_security_digest, merge_base_oid, candidate_merge_tree_oid,
             merge_author_name, merge_author_email, merge_committer_name,
             merge_committer_email, merge_author_date_bytes,
             merge_committer_date_bytes, merge_message_template_version,
             merge_message_bytes, expected_merge_commit_oid,
             abort_child_receipt_id, abort_merge_head_oid,
             abort_index_stages_digest, abort_worktree_digest,
             abort_merge_autostash_proof, merged_disposition_task_id,
             state, failure_code, version, created_at, updated_at
         ) VALUES (
             ?, ?, ?, 1,
             'evidence_identity_v1', 1, ?,
             7, ?, ?,
             ?, ?, ?,
             ?, 'directory_identity_v1',
             ?, 'directory_identity_v1',
             ?, 'codex-reserved',
             NULL, NULL,
             NULL, NULL, ?,
             NULL, CAST(? AS TEXT), ?,
             ?, ?, ?, NULL, NULL,
             NULL, NULL, NULL,
             NULL, NULL,
             NULL, NULL,
             NULL, NULL,
             NULL, NULL,
             NULL, NULL,
             NULL, NULL,
             'preflight_pending', NULL, 1, ?, ?
         )",
    )
    .bind(fixture.operation_id)
    .bind(TASK_ID)
    .bind(REPOSITORY_ID)
    .bind(final_review_event_id)
    .bind(fixture.workspace_fingerprint)
    .bind(CHECKS_DIGEST)
    .bind(COVERAGE_DIGEST)
    .bind(BASE_OID)
    .bind(SOURCE_BRANCH)
    .bind(fixture.artifact_worktree_path)
    .bind(COMMON_IDENTITY_DIGEST)
    .bind(ADMIN_IDENTITY_DIGEST)
    .bind(fixture.receipt_id);
    let query = match fixture.target_branch {
        SqlTextFixture::Utf8(value) => query.bind(value),
        SqlTextFixture::RawBytes(value) => query.bind(value),
    };
    query
        .bind(TARGET_HEAD_OID)
        .bind(fixture.config_attributes_digest)
        .bind(fixture.target_config_attributes_digest)
        .bind(fixture.target_security_digest)
        .bind(TIMESTAMP)
        .bind(TIMESTAMP)
        .execute(&mut **transaction)
        .await
        .map(|_| ())
}

async fn bind_preflight_inputs_raw(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    fixture: PreflightFixture<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE task_merge_operations \
         SET candidate_tree_oid = ?, preflight_source_commit_oid = ?, \
             version = 2, updated_at = ? \
         WHERE operation_id = ? AND state = 'preflight_pending' AND version = 1 \
           AND candidate_tree_oid IS NULL AND preflight_source_commit_oid IS NULL",
    )
    .bind(fixture.candidate_tree_oid)
    .bind(PREFLIGHT_SOURCE_OID)
    .bind(TIMESTAMP)
    .bind(fixture.operation_id)
    .execute(&mut **transaction)
    .await
    .map(|_| ())
}

fn merge_receipt_from_preflight(fixture: PreflightFixture<'_>) -> MergeReceiptFixture<'_> {
    MergeReceiptFixture {
        receipt_id: fixture.receipt_id,
        command_kind: fixture.command_kind,
        operation_id: fixture.operation_id,
        accepted_version: fixture.accepted_version,
        accepted_state: fixture.accepted_state,
        response_discriminator: fixture.response_discriminator,
        request_hash: fixture.request_hash,
        created_at: fixture.receipt_created_at,
    }
}

pub async fn mark_preflight_ready(
    pool: &SqlitePool,
    operation_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE task_merge_operations
         SET state = 'preflight_ready', version = 3,
             merge_base_oid = ?, candidate_merge_tree_oid = ?, updated_at = ?
         WHERE operation_id = ? AND state = 'preflight_pending' AND version = 2",
    )
    .bind(MERGE_BASE_OID)
    .bind(MERGE_TREE_OID)
    .bind(TIMESTAMP)
    .bind(operation_id)
    .execute(pool)
    .await
    .map(|_| ())
}

pub async fn accept_merge(pool: &SqlitePool, operation_id: &str) -> Result<(), sqlx::Error> {
    accept_merge_with_receipt(pool, operation_id, ACCEPT_RECEIPT_ID).await
}

pub async fn accept_merge_with_receipt(
    pool: &SqlitePool,
    operation_id: &str,
    receipt_id: &str,
) -> Result<(), sqlx::Error> {
    accept_merge_with_date_bytes_and_request_hash(
        pool,
        operation_id,
        receipt_id,
        SqlTextFixture::Utf8("1785801600 +0000"),
        REQUEST_HASH,
    )
    .await
}

pub async fn accept_merge_with_date_bytes(
    pool: &SqlitePool,
    operation_id: &str,
    receipt_id: &str,
    date_bytes: SqlTextFixture<'_>,
) -> Result<(), sqlx::Error> {
    accept_merge_with_date_bytes_and_request_hash(
        pool,
        operation_id,
        receipt_id,
        date_bytes,
        REQUEST_HASH,
    )
    .await
}

pub async fn accept_merge_with_request_hash(
    pool: &SqlitePool,
    operation_id: &str,
    receipt_id: &str,
    request_hash: &str,
) -> Result<(), sqlx::Error> {
    accept_merge_with_date_bytes_and_request_hash(
        pool,
        operation_id,
        receipt_id,
        SqlTextFixture::Utf8("1785801600 +0000"),
        request_hash,
    )
    .await
}

pub async fn accept_merge_operation_with_request_hash(
    pool: &SqlitePool,
    operation_id: &str,
    receipt_id: &str,
    request_hash: &str,
) -> Result<(), sqlx::Error> {
    let mut transaction = pool.begin().await?;
    sqlx::query(
        "UPDATE task_merge_operations \
         SET state = 'accepted', version = 4, accept_receipt_id = ?, \
             merge_author_name = 'Coding Agent', \
             merge_author_email = 'coding-agent@localhost', \
             merge_committer_name = 'Coding Agent', \
             merge_committer_email = 'coding-agent@localhost', \
             merge_author_date_bytes = '1785801600 +0000', \
             merge_committer_date_bytes = '1785801600 +0000', \
             merge_message_template_version = 1, \
             merge_message_bytes = CAST('coding-agent: merge task ' || task_id || \
                 ' attempt ' || attempt || char(10) AS BLOB), updated_at = ? \
         WHERE operation_id = ? AND state = 'preflight_ready' AND version = 3",
    )
    .bind(receipt_id)
    .bind(TIMESTAMP)
    .bind(operation_id)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "INSERT INTO task_delivery_command_receipts ( \
             client_request_id, command_kind, task_id, repository_id, attempt, \
             request_hash_domain, request_hash_version, request_hash_algorithm, \
             canonical_request_hash, operation_kind, operation_id, merge_operation_id, \
             cleanup_operation_id, accepted_operation_version, accepted_operation_state, \
             response_discriminator, created_at \
         ) \
         SELECT ?, 'accept_merge', task_id, repository_id, attempt, \
             'coding-agent-delivery-command-request', 1, 'sha256', ?, \
             'merge_operation', operation_id, operation_id, NULL, 4, 'accepted', \
             'merge_accepted', ? \
         FROM task_merge_operations WHERE operation_id = ?",
    )
    .bind(receipt_id)
    .bind(request_hash)
    .bind(TIMESTAMP)
    .bind(operation_id)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await
}

async fn accept_merge_with_date_bytes_and_request_hash(
    pool: &SqlitePool,
    operation_id: &str,
    receipt_id: &str,
    date_bytes: SqlTextFixture<'_>,
    request_hash: &str,
) -> Result<(), sqlx::Error> {
    let mut transaction = pool.begin().await?;
    let query = sqlx::query(
        "UPDATE task_merge_operations
         SET state = 'accepted', version = 4, accept_receipt_id = ?,
             merge_author_name = 'Coding Agent',
             merge_author_email = 'coding-agent@localhost',
             merge_committer_name = 'Coding Agent',
             merge_committer_email = 'coding-agent@localhost',
             merge_author_date_bytes = CAST(? AS TEXT),
             merge_committer_date_bytes = CAST(? AS TEXT),
             merge_message_template_version = 1,
             merge_message_bytes = CAST('coding-agent: merge task ' || task_id || \
                 ' attempt ' || attempt || char(10) AS BLOB),
             updated_at = ?
         WHERE operation_id = ?",
    )
    .bind(receipt_id);
    let query = match date_bytes {
        SqlTextFixture::Utf8(value) => query.bind(value),
        SqlTextFixture::RawBytes(value) => query.bind(value),
    };
    let query = match date_bytes {
        SqlTextFixture::Utf8(value) => query.bind(value),
        SqlTextFixture::RawBytes(value) => query.bind(value),
    };
    query
        .bind(TIMESTAMP)
        .bind(operation_id)
        .execute(&mut *transaction)
        .await?;
    insert_merge_receipt(
        &mut transaction,
        MergeReceiptFixture {
            receipt_id,
            command_kind: "accept_merge",
            operation_id,
            accepted_version: 4,
            accepted_state: "accepted",
            response_discriminator: "merge_accepted",
            request_hash,
            created_at: TIMESTAMP,
        },
    )
    .await?;
    transaction.commit().await
}

pub async fn create_committed_source(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    create_source_object_pending(pool).await?;
    sqlx::query(
        "UPDATE task_delivery_sources
         SET expected_source_commit_oid = ?, state = 'commit_pending',
             version = 2, updated_at = ?
         WHERE task_id = ?",
    )
    .bind(SOURCE_COMMIT_OID)
    .bind(TIMESTAMP)
    .bind(TASK_ID)
    .execute(pool)
    .await?;
    sqlx::query(
        "UPDATE task_delivery_sources
         SET state = 'committed', version = 3, updated_at = ?
         WHERE task_id = ?",
    )
    .bind(TIMESTAMP)
    .bind(TASK_ID)
    .execute(pool)
    .await
    .map(|_| ())
}

pub async fn create_source_object_pending(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    create_source_object_pending_with_date_bytes(pool, "1785801600 +0000").await
}

pub async fn create_source_object_pending_with_date_bytes(
    pool: &SqlitePool,
    date_bytes: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO task_delivery_sources (
             task_id, repository_id, attempt, evidence_algorithm,
             final_review_round, final_review_event_id, workspace_generation,
             workspace_fingerprint, checks_digest, coverage_digest,
             artifact_base_commit, artifact_source_branch,
             artifact_worktree_path, common_git_identity_algorithm,
             common_git_identity_digest, worktree_admin_identity_algorithm,
             worktree_admin_identity_digest, fixed_lock_reason,
             config_attributes_digest, origin_accepted_operation_id,
             origin_accept_receipt_id, origin_accepted_version, candidate_tree_oid,
             expected_parent_oid, expected_source_commit_oid,
             author_name, author_email, committer_name, committer_email,
             author_date_bytes, committer_date_bytes,
             commit_message_template_version, commit_message_bytes,
             state, failure_code, version, created_at, updated_at
         )
         SELECT
             task_id, repository_id, attempt, evidence_algorithm,
             final_review_round, final_review_event_id, workspace_generation,
             workspace_fingerprint, checks_digest, coverage_digest,
             artifact_base_commit, artifact_source_branch,
             artifact_worktree_path, common_git_identity_algorithm,
             common_git_identity_digest, worktree_admin_identity_algorithm,
             worktree_admin_identity_digest, fixed_lock_reason,
             config_attributes_digest, operation_id, accept_receipt_id, version, candidate_tree_oid,
             artifact_base_commit, NULL,
             'Coding Agent', 'coding-agent@localhost',
             'Coding Agent', 'coding-agent@localhost',
             ?, ?,
             1, CAST('coding-agent: deliver task ' || task_id || ' attempt ' || attempt || char(10) AS BLOB),
             'object_pending', NULL, 1, ?, ?
         FROM task_merge_operations
         WHERE operation_id = ?",
    )
    .bind(date_bytes)
    .bind(date_bytes)
    .bind(TIMESTAMP)
    .bind(TIMESTAMP)
    .bind(MERGE_OPERATION_ID)
    .execute(pool)
    .await
    .map(|_| ())
}

pub async fn mark_merge_pending(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE task_merge_operations
         SET delivery_source_task_id = ?, source_commit_oid = ?,
             expected_merge_commit_oid = ?, state = 'merge_pending',
             version = 5, updated_at = ?
         WHERE operation_id = ?",
    )
    .bind(TASK_ID)
    .bind(SOURCE_COMMIT_OID)
    .bind(MERGE_COMMIT_OID)
    .bind(TIMESTAMP)
    .bind(MERGE_OPERATION_ID)
    .execute(pool)
    .await
    .map(|_| ())
}

pub async fn complete_merge_with_disposition(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    let mut transaction = pool.begin().await?;
    sqlx::query(
        "UPDATE task_merge_operations
         SET state = 'merged', merged_disposition_task_id = ?,
             version = 6, updated_at = ?
         WHERE operation_id = ?",
    )
    .bind(TASK_ID)
    .bind(TIMESTAMP)
    .bind(MERGE_OPERATION_ID)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "INSERT INTO task_artifact_dispositions (
             task_id, repository_id, attempt, merged_operation_id,
             delivery_source_task_id, source_commit_oid,
             worktree_state, worktree_version, worktree_failure_code,
             worktree_updated_at, branch_state, branch_version,
             branch_failure_code, branch_updated_at, created_at
         ) VALUES (
             ?, ?, 1, ?, ?, ?,
             'retained_locked', 1, NULL, ?,
             'retained', 1, NULL, ?, ?
         )",
    )
    .bind(TASK_ID)
    .bind(REPOSITORY_ID)
    .bind(MERGE_OPERATION_ID)
    .bind(TASK_ID)
    .bind(SOURCE_COMMIT_OID)
    .bind(TIMESTAMP)
    .bind(TIMESTAMP)
    .bind(TIMESTAMP)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await
}

pub async fn seed_merged_delivery(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    let parents = seed_eligible_delivery_parents(pool).await;
    create_preflight(
        pool,
        parents.final_review_event_id,
        MERGE_OPERATION_ID,
        PREFLIGHT_RECEIPT_ID,
    )
    .await?;
    mark_preflight_ready(pool, MERGE_OPERATION_ID).await?;
    accept_merge(pool, MERGE_OPERATION_ID).await?;
    create_committed_source(pool).await?;
    mark_merge_pending(pool).await?;
    complete_merge_with_disposition(pool).await
}

#[derive(Debug, Clone, Copy)]
struct MergeReceiptFixture<'a> {
    receipt_id: &'a str,
    command_kind: &'a str,
    operation_id: &'a str,
    accepted_version: i64,
    accepted_state: &'a str,
    response_discriminator: &'a str,
    request_hash: &'a str,
    created_at: &'a str,
}

async fn insert_merge_receipt(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    fixture: MergeReceiptFixture<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO task_delivery_command_receipts (
             client_request_id, command_kind, task_id, repository_id, attempt,
             request_hash_domain, request_hash_version, request_hash_algorithm,
             canonical_request_hash, operation_kind, operation_id,
             merge_operation_id, cleanup_operation_id,
             accepted_operation_version, accepted_operation_state,
             response_discriminator, created_at
         ) VALUES (
             ?, ?, ?, ?, 1,
             'coding-agent-delivery-command-request', 1, 'sha256', ?,
             'merge_operation', ?, ?, NULL, ?, ?, ?, ?
         )",
    )
    .bind(fixture.receipt_id)
    .bind(fixture.command_kind)
    .bind(TASK_ID)
    .bind(REPOSITORY_ID)
    .bind(fixture.request_hash)
    .bind(fixture.operation_id)
    .bind(fixture.operation_id)
    .bind(fixture.accepted_version)
    .bind(fixture.accepted_state)
    .bind(fixture.response_discriminator)
    .bind(fixture.created_at)
    .execute(&mut **transaction)
    .await
    .map(|_| ())
}
