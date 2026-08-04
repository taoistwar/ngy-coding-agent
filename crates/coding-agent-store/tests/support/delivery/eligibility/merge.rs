use std::str::FromStr;

use coding_agent_domain::{ClientRequestId, Task};
use coding_agent_store::{
    AcceptMergeCommandRequest, DeliveryOperationId, DeliveryTimestamp, DeliveryVersion,
    EvidenceIdentityV1, GitBranchRef, GitCommitOid, MergeOperationState, PreflightCommandRequest,
    Store,
};

use super::{
    ADMIN_IDENTITY, CANDIDATE_TREE, COMMON_IDENTITY, CONFIG_DIGEST, DELIVERY_TIMESTAMP, MERGE_BASE,
    MERGE_TREE, PREFLIGHT_SOURCE, SOURCE_COMMIT, TARGET_HEAD,
};

pub async fn insert_preflight(
    store: &Store,
    task: &Task,
    evidence: &EvidenceIdentityV1,
    operation_id: DeliveryOperationId,
) {
    let artifact = store.load_attempt_artifact(task.id).await.unwrap().unwrap();
    let receipt_id = ClientRequestId::new();
    let command = PreflightCommandRequest::try_new(
        receipt_id,
        task.id,
        GitBranchRef::from_str("refs/heads/main").unwrap(),
        GitCommitOid::from_str(TARGET_HEAD).unwrap(),
    )
    .unwrap();
    let source_ref = format!("refs/heads/{}", artifact.branch_name);
    let mut transaction = store.pool().begin().await.unwrap();
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
             NULL, NULL, ?, NULL, 'refs/heads/main', ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, \
             NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, \
             'preflight_pending', NULL, 1, ?, ? \
         )",
    )
    .bind(operation_id.to_string())
    .bind(task.id.to_string())
    .bind(task.repository_id.to_string())
    .bind(i64::from(task.attempt))
    .bind(i64::from(evidence.final_review_round()))
    .bind(evidence.final_review_event_id().get())
    .bind(i64::try_from(evidence.workspace_generation()).unwrap())
    .bind(evidence.workspace_fingerprint().as_str())
    .bind(evidence.checks_digest().as_str())
    .bind(evidence.coverage_digest().as_str())
    .bind(&artifact.base_commit)
    .bind(source_ref)
    .bind(artifact.worktree_path.to_string())
    .bind(COMMON_IDENTITY)
    .bind(ADMIN_IDENTITY)
    .bind(CANDIDATE_TREE)
    .bind(PREFLIGHT_SOURCE)
    .bind(receipt_id.to_string())
    .bind(TARGET_HEAD)
    .bind(CONFIG_DIGEST)
    .bind(DELIVERY_TIMESTAMP)
    .bind(DELIVERY_TIMESTAMP)
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO task_delivery_command_receipts ( \
             client_request_id, command_kind, task_id, repository_id, attempt, \
             request_hash_domain, request_hash_version, request_hash_algorithm, \
             canonical_request_hash, operation_kind, operation_id, merge_operation_id, \
             cleanup_operation_id, accepted_operation_version, accepted_operation_state, \
             response_discriminator, created_at \
         ) VALUES (?, 'preflight', ?, ?, ?, 'coding-agent-delivery-command-request', 1, \
             'sha256', ?, 'merge_operation', ?, ?, NULL, 1, 'preflight_pending', \
             'preflight_created', ?)",
    )
    .bind(receipt_id.to_string())
    .bind(task.id.to_string())
    .bind(task.repository_id.to_string())
    .bind(i64::from(task.attempt))
    .bind(command.canonical_request_hash().as_str())
    .bind(operation_id.to_string())
    .bind(operation_id.to_string())
    .bind(DELIVERY_TIMESTAMP)
    .execute(&mut *transaction)
    .await
    .unwrap();
    transaction.commit().await.unwrap();
}

pub async fn mark_preflight_ready(store: &Store, operation_id: DeliveryOperationId) {
    sqlx::query(
        "UPDATE task_merge_operations SET state = 'preflight_ready', version = 2, \
             merge_base_oid = ?, candidate_merge_tree_oid = ?, updated_at = ? \
         WHERE operation_id = ?",
    )
    .bind(MERGE_BASE)
    .bind(MERGE_TREE)
    .bind(DELIVERY_TIMESTAMP)
    .bind(operation_id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
}

pub async fn finish_preflight_terminal(
    store: &Store,
    operation_id: DeliveryOperationId,
    state: MergeOperationState,
) {
    finish_preflight_terminal_with_conflict_count(store, operation_id, state, 0).await;
}

pub async fn finish_preflight_conflict(
    store: &Store,
    operation_id: DeliveryOperationId,
    conflict_path_count: u8,
) {
    finish_preflight_terminal_with_conflict_count(
        store,
        operation_id,
        MergeOperationState::Conflict,
        conflict_path_count,
    )
    .await;
}

async fn finish_preflight_terminal_with_conflict_count(
    store: &Store,
    operation_id: DeliveryOperationId,
    state: MergeOperationState,
    conflict_path_count: u8,
) {
    let version = if state == MergeOperationState::Superseded {
        mark_preflight_ready(store, operation_id).await;
        3
    } else {
        assert!(matches!(
            state,
            MergeOperationState::Conflict
                | MergeOperationState::Rejected
                | MergeOperationState::Stale
        ));
        2
    };
    let (failure_code, merge_base, merge_tree) = match state {
        MergeOperationState::Conflict => {
            (Some("MERGE_CONFLICT"), Some(MERGE_BASE), Some(MERGE_TREE))
        }
        MergeOperationState::Rejected => (Some("TASK_NOT_MERGE_ELIGIBLE"), None, None),
        MergeOperationState::Stale => (Some("TARGET_HEAD_CHANGED"), None, None),
        MergeOperationState::Superseded => (None, Some(MERGE_BASE), Some(MERGE_TREE)),
        _ => unreachable!(),
    };
    let conflict_path_count =
        (state == MergeOperationState::Conflict).then_some(i64::from(conflict_path_count));
    sqlx::query(
        "UPDATE task_merge_operations SET state = ?, failure_code = ?, \
             conflict_path_count = ?, \
             merge_base_oid = COALESCE(merge_base_oid, ?), \
             candidate_merge_tree_oid = COALESCE(candidate_merge_tree_oid, ?), \
             version = ?, updated_at = ? \
         WHERE operation_id = ?",
    )
    .bind(state.as_str())
    .bind(failure_code)
    .bind(conflict_path_count)
    .bind(merge_base)
    .bind(merge_tree)
    .bind(version)
    .bind(DELIVERY_TIMESTAMP)
    .bind(operation_id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
}

pub async fn accept_merge(store: &Store, task: &Task, operation_id: DeliveryOperationId) {
    let command = exact_accept_command(store, task, operation_id).await;
    let mut transaction = store.pool().begin().await.unwrap();
    sqlx::query(
        "UPDATE task_merge_operations SET state = 'accepted', version = 3, \
             accept_receipt_id = ?, merge_author_name = 'Coding Agent', \
             merge_author_email = 'coding-agent@localhost', \
             merge_committer_name = 'Coding Agent', \
             merge_committer_email = 'coding-agent@localhost', \
             merge_author_date_bytes = '1785801600 +0000', \
             merge_committer_date_bytes = '1785801600 +0000', \
             merge_message_template_version = 1, \
             merge_message_bytes = CAST('coding-agent: merge task ' || task_id || \
                 ' attempt ' || attempt || char(10) AS BLOB), updated_at = ? \
         WHERE operation_id = ?",
    )
    .bind(command.client_request_id().to_string())
    .bind(DELIVERY_TIMESTAMP)
    .bind(operation_id.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO task_delivery_command_receipts ( \
             client_request_id, command_kind, task_id, repository_id, attempt, \
             request_hash_domain, request_hash_version, request_hash_algorithm, \
             canonical_request_hash, operation_kind, operation_id, merge_operation_id, \
             cleanup_operation_id, accepted_operation_version, accepted_operation_state, \
             response_discriminator, created_at \
         ) VALUES (?, 'accept_merge', ?, ?, ?, 'coding-agent-delivery-command-request', 1, \
             'sha256', ?, 'merge_operation', ?, ?, NULL, 3, 'accepted', 'merge_accepted', ?)",
    )
    .bind(command.client_request_id().to_string())
    .bind(task.id.to_string())
    .bind(task.repository_id.to_string())
    .bind(i64::from(task.attempt))
    .bind(command.canonical_request_hash().as_str())
    .bind(operation_id.to_string())
    .bind(operation_id.to_string())
    .bind(DELIVERY_TIMESTAMP)
    .execute(&mut *transaction)
    .await
    .unwrap();
    transaction.commit().await.unwrap();
}

pub async fn try_accept_merge_ready(
    store: &Store,
    task: &Task,
    operation_id: DeliveryOperationId,
) -> bool {
    let command = exact_accept_command(store, task, operation_id).await;
    let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await.unwrap();
    let updated = sqlx::query(
        "UPDATE task_merge_operations SET state = 'accepted', version = 3, \
             accept_receipt_id = ?, merge_author_name = 'Coding Agent', \
             merge_author_email = 'coding-agent@localhost', \
             merge_committer_name = 'Coding Agent', \
             merge_committer_email = 'coding-agent@localhost', \
             merge_author_date_bytes = '1785801600 +0000', \
             merge_committer_date_bytes = '1785801600 +0000', \
             merge_message_template_version = 1, \
             merge_message_bytes = CAST('coding-agent: merge task ' || task_id || \
                 ' attempt ' || attempt || char(10) AS BLOB), updated_at = ? \
         WHERE operation_id = ? AND task_id = ? \
           AND state = 'preflight_ready' AND version = 2",
    )
    .bind(command.client_request_id().to_string())
    .bind(DELIVERY_TIMESTAMP)
    .bind(operation_id.to_string())
    .bind(task.id.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    if updated.rows_affected() == 0 {
        transaction.commit().await.unwrap();
        return false;
    }
    assert_eq!(updated.rows_affected(), 1);
    sqlx::query(
        "INSERT INTO task_delivery_command_receipts ( \
             client_request_id, command_kind, task_id, repository_id, attempt, \
             request_hash_domain, request_hash_version, request_hash_algorithm, \
             canonical_request_hash, operation_kind, operation_id, merge_operation_id, \
             cleanup_operation_id, accepted_operation_version, accepted_operation_state, \
             response_discriminator, created_at \
         ) VALUES (?, 'accept_merge', ?, ?, ?, 'coding-agent-delivery-command-request', 1, \
             'sha256', ?, 'merge_operation', ?, ?, NULL, 3, 'accepted', 'merge_accepted', ?)",
    )
    .bind(command.client_request_id().to_string())
    .bind(task.id.to_string())
    .bind(task.repository_id.to_string())
    .bind(i64::from(task.attempt))
    .bind(command.canonical_request_hash().as_str())
    .bind(operation_id.to_string())
    .bind(operation_id.to_string())
    .bind(DELIVERY_TIMESTAMP)
    .execute(&mut *transaction)
    .await
    .unwrap();
    transaction.commit().await.unwrap();
    true
}

async fn exact_accept_command(
    store: &Store,
    task: &Task,
    operation_id: DeliveryOperationId,
) -> AcceptMergeCommandRequest {
    let evidence = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .evidence_identity
        .unwrap();
    AcceptMergeCommandRequest::try_new(
        ClientRequestId::new(),
        task.id,
        operation_id,
        DeliveryVersion::try_new(2).unwrap(),
        evidence.workspace_generation(),
        evidence.workspace_fingerprint().clone(),
        GitBranchRef::from_str("refs/heads/main").unwrap(),
        GitCommitOid::from_str(TARGET_HEAD).unwrap(),
    )
    .unwrap()
}

pub async fn create_committed_source(
    store: &Store,
    task: &Task,
    operation_id: DeliveryOperationId,
) {
    let source_timestamp: String =
        sqlx::query_scalar("SELECT updated_at FROM task_merge_operations WHERE operation_id = ?")
            .bind(operation_id.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap();
    let parsed_timestamp: DeliveryTimestamp = source_timestamp.parse().unwrap();
    let source_date = format!(
        "{} +0000",
        parsed_timestamp
            .as_utc()
            .as_offset_date_time()
            .unix_timestamp()
    );
    sqlx::query(
        "INSERT INTO task_delivery_sources ( \
             task_id, repository_id, attempt, evidence_algorithm, final_review_round, \
             final_review_event_id, workspace_generation, workspace_fingerprint, checks_digest, \
             coverage_digest, artifact_base_commit, artifact_source_branch, artifact_worktree_path, \
             common_git_identity_algorithm, common_git_identity_digest, \
             worktree_admin_identity_algorithm, worktree_admin_identity_digest, fixed_lock_reason, \
             config_attributes_digest, origin_accepted_operation_id, origin_accept_receipt_id, \
             origin_accepted_version, candidate_tree_oid, expected_parent_oid, \
             expected_source_commit_oid, author_name, author_email, committer_name, committer_email, \
             author_date_bytes, committer_date_bytes, commit_message_template_version, \
             commit_message_bytes, state, failure_code, version, created_at, updated_at \
         ) SELECT task_id, repository_id, attempt, evidence_algorithm, final_review_round, \
             final_review_event_id, workspace_generation, workspace_fingerprint, checks_digest, \
             coverage_digest, artifact_base_commit, artifact_source_branch, artifact_worktree_path, \
             common_git_identity_algorithm, common_git_identity_digest, \
             worktree_admin_identity_algorithm, worktree_admin_identity_digest, fixed_lock_reason, \
             config_attributes_digest, operation_id, accept_receipt_id, version, \
             candidate_tree_oid, artifact_base_commit, NULL, \
             'Coding Agent', 'coding-agent@localhost', 'Coding Agent', 'coding-agent@localhost', \
             ?, ?, 1, \
             CAST('coding-agent: deliver task ' || task_id || ' attempt ' || attempt || char(10) AS BLOB), \
             'object_pending', NULL, 1, ?, ? \
         FROM task_merge_operations WHERE operation_id = ? AND task_id = ?",
    )
    .bind(&source_date)
    .bind(&source_date)
    .bind(&source_timestamp)
    .bind(&source_timestamp)
    .bind(operation_id.to_string())
    .bind(task.id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_delivery_sources SET expected_source_commit_oid = ?, \
             state = 'commit_pending', version = 2, updated_at = ? WHERE task_id = ?",
    )
    .bind(SOURCE_COMMIT)
    .bind(&source_timestamp)
    .bind(task.id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_delivery_sources SET state = 'committed', version = 3, updated_at = ? \
         WHERE task_id = ?",
    )
    .bind(&source_timestamp)
    .bind(task.id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
}

pub async fn fail_accepted_merge(store: &Store, task: &Task, operation_id: DeliveryOperationId) {
    sqlx::query(
        "UPDATE task_merge_operations SET delivery_source_task_id = ?, source_commit_oid = ?, \
             state = 'failed', failure_code = 'TARGET_HEAD_CHANGED', version = 4, updated_at = ? \
         WHERE operation_id = ?",
    )
    .bind(task.id.to_string())
    .bind(SOURCE_COMMIT)
    .bind(DELIVERY_TIMESTAMP)
    .bind(operation_id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
}
