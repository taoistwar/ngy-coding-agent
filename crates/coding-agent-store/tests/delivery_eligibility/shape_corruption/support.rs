use coding_agent_domain::Task;
use coding_agent_store::{DeliveryOperationId, DeliverySourceState, MergeOperationState, Store};
use sqlx::{Sqlite, pool::PoolConnection};

use crate::support::delivery::eligibility::{
    DELIVERY_TIMESTAMP, MERGE_COMMIT, SOURCE_COMMIT, accept_merge,
    approved_task_with_ready_artifact, fail_accepted_merge, finish_preflight_terminal,
    insert_preflight, mark_preflight_ready,
};

const SHA256_OID: &str = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
const ABORT_INDEX_DIGEST: &str = "abababababababababababababababababababababababababababababababab";
const ABORT_WORKTREE_DIGEST: &str =
    "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
const CORRUPT_RECEIPT_ID: &str = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";

#[derive(Debug, Clone, Copy)]
pub enum SourceShape {
    ObjectPending,
    CommitPending,
    Committed,
    ReconciliationRequired,
}

impl SourceShape {
    pub const ALL: [Self; 4] = [
        Self::ObjectPending,
        Self::CommitPending,
        Self::Committed,
        Self::ReconciliationRequired,
    ];

    pub const fn state(self) -> DeliverySourceState {
        match self {
            Self::ObjectPending => DeliverySourceState::ObjectPending,
            Self::CommitPending => DeliverySourceState::CommitPending,
            Self::Committed => DeliverySourceState::Committed,
            Self::ReconciliationRequired => DeliverySourceState::ReconciliationRequired,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum SourceInvariantCorruption {
    PendingFailureNotAllowlisted,
    ReconciliationFailureNotAllowlisted,
    ReconciliationFailureMismatch,
    ReconciliationTransitionOrder,
}

impl SourceInvariantCorruption {
    pub const ALL: [Self; 4] = [
        Self::PendingFailureNotAllowlisted,
        Self::ReconciliationFailureNotAllowlisted,
        Self::ReconciliationFailureMismatch,
        Self::ReconciliationTransitionOrder,
    ];

    pub const fn fixture_shape(self) -> SourceShape {
        match self {
            Self::PendingFailureNotAllowlisted => SourceShape::ObjectPending,
            Self::ReconciliationFailureNotAllowlisted
            | Self::ReconciliationFailureMismatch
            | Self::ReconciliationTransitionOrder => SourceShape::ReconciliationRequired,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum MergeShape {
    PreflightPending,
    PreflightReady,
    Accepted,
    MergePending,
    Merged,
    AbortPending,
    Conflict,
    Rejected,
    Stale,
    Superseded,
    Failed,
    ReconciliationRequired,
}

impl MergeShape {
    pub const ALL: [Self; 12] = [
        Self::PreflightPending,
        Self::PreflightReady,
        Self::Accepted,
        Self::MergePending,
        Self::Merged,
        Self::AbortPending,
        Self::Conflict,
        Self::Rejected,
        Self::Stale,
        Self::Superseded,
        Self::Failed,
        Self::ReconciliationRequired,
    ];

    pub const fn state(self) -> MergeOperationState {
        match self {
            Self::PreflightPending => MergeOperationState::PreflightPending,
            Self::PreflightReady => MergeOperationState::PreflightReady,
            Self::Accepted => MergeOperationState::Accepted,
            Self::MergePending => MergeOperationState::MergePending,
            Self::Merged => MergeOperationState::Merged,
            Self::AbortPending => MergeOperationState::AbortPending,
            Self::Conflict => MergeOperationState::Conflict,
            Self::Rejected => MergeOperationState::Rejected,
            Self::Stale => MergeOperationState::Stale,
            Self::Superseded => MergeOperationState::Superseded,
            Self::Failed => MergeOperationState::Failed,
            Self::ReconciliationRequired => MergeOperationState::ReconciliationRequired,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum GitOidField {
    ArtifactBase,
    CandidateTree,
    PreflightSource,
    ExpectedTargetHead,
    SourceCommit,
    MergeBase,
    CandidateMergeTree,
    ExpectedMergeCommit,
    AbortMergeHead,
}

#[derive(Debug, Clone, Copy)]
pub enum MetadataCorruption {
    SourceAuthor,
    SourceDateMismatch,
    MergeEmail,
    SourceTemplate,
    SourceMessageMismatch,
    MergeDate,
    SourceEmptyMessage,
    MergeOversizedMessage,
}

impl MetadataCorruption {
    pub const ALL: [Self; 8] = [
        Self::SourceAuthor,
        Self::SourceDateMismatch,
        Self::MergeEmail,
        Self::SourceTemplate,
        Self::SourceMessageMismatch,
        Self::MergeDate,
        Self::SourceEmptyMessage,
        Self::MergeOversizedMessage,
    ];

    const fn targets_source(self) -> bool {
        matches!(
            self,
            Self::SourceAuthor
                | Self::SourceDateMismatch
                | Self::SourceTemplate
                | Self::SourceMessageMismatch
                | Self::SourceEmptyMessage
        )
    }
}

impl GitOidField {
    pub const ALL: [Self; 9] = [
        Self::ArtifactBase,
        Self::CandidateTree,
        Self::PreflightSource,
        Self::ExpectedTargetHead,
        Self::SourceCommit,
        Self::MergeBase,
        Self::CandidateMergeTree,
        Self::ExpectedMergeCommit,
        Self::AbortMergeHead,
    ];

    pub const fn fixture_shape(self) -> MergeShape {
        match self {
            Self::AbortMergeHead => MergeShape::AbortPending,
            _ => MergeShape::MergePending,
        }
    }
}

pub async fn source_fixture(shape: SourceShape) -> (Store, Task, DeliveryOperationId) {
    let (store, task, operation_id) = accepted_fixture().await;
    insert_object_pending_source(&store, &task, operation_id).await;
    match shape {
        SourceShape::ObjectPending => {}
        SourceShape::CommitPending => mark_source_commit_pending(&store, &task).await,
        SourceShape::Committed => {
            mark_source_commit_pending(&store, &task).await;
            mark_source_committed(&store, &task).await;
        }
        SourceShape::ReconciliationRequired => {
            mark_source_commit_pending(&store, &task).await;
            mark_source_committed(&store, &task).await;
            reconcile_source_and_merge(&store, &task, operation_id).await;
        }
    }
    (store, task, operation_id)
}

pub async fn merge_fixture(shape: MergeShape) -> (Store, Task, DeliveryOperationId) {
    let (store, task, operation_id) = preflight_fixture().await;
    match shape {
        MergeShape::PreflightPending => {}
        MergeShape::PreflightReady => mark_preflight_ready(&store, operation_id).await,
        MergeShape::Accepted => prepare_accepted(&store, &task, operation_id).await,
        MergeShape::MergePending => prepare_merge_pending(&store, &task, operation_id).await,
        MergeShape::Merged => {
            prepare_merge_pending(&store, &task, operation_id).await;
            complete_merge(&store, &task, operation_id).await;
        }
        MergeShape::AbortPending => {
            prepare_merge_pending(&store, &task, operation_id).await;
            mark_abort_pending(&store, operation_id).await;
        }
        MergeShape::Conflict => {
            finish_preflight_terminal(&store, operation_id, MergeOperationState::Conflict).await;
        }
        MergeShape::Rejected => {
            finish_preflight_terminal(&store, operation_id, MergeOperationState::Rejected).await;
        }
        MergeShape::Stale => {
            finish_preflight_terminal(&store, operation_id, MergeOperationState::Stale).await;
        }
        MergeShape::Superseded => {
            finish_preflight_terminal(&store, operation_id, MergeOperationState::Superseded).await;
        }
        MergeShape::Failed => {
            prepare_accepted(&store, &task, operation_id).await;
            crate::support::delivery::eligibility::create_committed_source(
                &store,
                &task,
                operation_id,
            )
            .await;
            fail_accepted_merge(&store, &task, operation_id).await;
        }
        MergeShape::ReconciliationRequired => {
            sqlx::query(
                "UPDATE task_merge_operations SET state = 'reconciliation_required', \
                     failure_code = 'DELIVERY_RECONCILIATION_REQUIRED', \
                     version = 2, updated_at = ? \
                 WHERE operation_id = ?",
            )
            .bind(DELIVERY_TIMESTAMP)
            .bind(operation_id.to_string())
            .execute(store.pool())
            .await
            .unwrap();
        }
    }
    (store, task, operation_id)
}

pub async fn metadata_fixture(
    corruption: MetadataCorruption,
) -> (Store, Task, DeliveryOperationId) {
    if corruption.targets_source() {
        source_fixture(SourceShape::Committed).await
    } else {
        merge_fixture(MergeShape::Accepted).await
    }
}

pub async fn corrupt_source_shape(store: &Store, task: &Task, shape: SourceShape) {
    let mut connection = corruption_connection(store, true, false).await;
    match shape {
        SourceShape::ObjectPending => {
            sqlx::query(
                "UPDATE task_delivery_sources SET expected_source_commit_oid = ? \
                 WHERE task_id = ?",
            )
            .bind(SOURCE_COMMIT)
            .bind(task.id.to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
        }
        SourceShape::CommitPending | SourceShape::Committed => {
            sqlx::query(
                "UPDATE task_delivery_sources SET expected_source_commit_oid = NULL \
                 WHERE task_id = ?",
            )
            .bind(task.id.to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
        }
        SourceShape::ReconciliationRequired => {
            sqlx::query("UPDATE task_delivery_sources SET failure_code = NULL WHERE task_id = ?")
                .bind(task.id.to_string())
                .execute(&mut *connection)
                .await
                .unwrap();
            sqlx::query(
                "UPDATE task_delivery_operation_transitions SET failure_code = NULL \
                 WHERE entity_kind = 'delivery_source' AND entity_id = ? \
                   AND entity_version = 4",
            )
            .bind(task.id.to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
        }
    }
}

pub async fn corrupt_source_invariant(
    store: &Store,
    task: &Task,
    operation_id: DeliveryOperationId,
    corruption: SourceInvariantCorruption,
) {
    let mut connection = corruption_connection(store, true, true).await;
    match corruption {
        SourceInvariantCorruption::PendingFailureNotAllowlisted => {
            sqlx::query(
                "UPDATE task_delivery_sources SET failure_code = 'ARBITRARY_FAILURE' \
                 WHERE task_id = ?",
            )
            .bind(task.id.to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
            sqlx::query(
                "UPDATE task_delivery_operation_transitions \
                 SET failure_code = 'ARBITRARY_FAILURE' \
                 WHERE entity_kind = 'delivery_source' AND entity_id = ? AND entity_version = 1",
            )
            .bind(task.id.to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
        }
        SourceInvariantCorruption::ReconciliationFailureNotAllowlisted => {
            sqlx::query(
                "UPDATE task_delivery_sources SET failure_code = 'ARBITRARY_FAILURE' \
                 WHERE task_id = ?",
            )
            .bind(task.id.to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
            sqlx::query(
                "UPDATE task_merge_operations SET failure_code = 'ARBITRARY_FAILURE' \
                 WHERE operation_id = ?",
            )
            .bind(operation_id.to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
            for (entity_kind, entity_id) in [
                ("delivery_source", task.id.to_string()),
                ("merge_operation", operation_id.to_string()),
            ] {
                sqlx::query(
                    "UPDATE task_delivery_operation_transitions \
                     SET failure_code = 'ARBITRARY_FAILURE' \
                     WHERE entity_kind = ? AND entity_id = ? AND entity_version = 4",
                )
                .bind(entity_kind)
                .bind(entity_id)
                .execute(&mut *connection)
                .await
                .unwrap();
            }
        }
        SourceInvariantCorruption::ReconciliationFailureMismatch => {
            sqlx::query(
                "UPDATE task_merge_operations \
                 SET failure_code = 'PROCESS_TREE_CLEANUP_FAILED' WHERE operation_id = ?",
            )
            .bind(operation_id.to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
            sqlx::query(
                "UPDATE task_delivery_operation_transitions \
                 SET failure_code = 'PROCESS_TREE_CLEANUP_FAILED' \
                 WHERE entity_kind = 'merge_operation' AND entity_id = ? AND entity_version = 4",
            )
            .bind(operation_id.to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
        }
        SourceInvariantCorruption::ReconciliationTransitionOrder => {
            let source_transition: i64 = sqlx::query_scalar(
                "SELECT transition_id FROM task_delivery_operation_transitions \
                 WHERE entity_kind = 'delivery_source' AND entity_id = ? AND entity_version = 4",
            )
            .bind(task.id.to_string())
            .fetch_one(&mut *connection)
            .await
            .unwrap();
            let merge_transition: i64 = sqlx::query_scalar(
                "SELECT transition_id FROM task_delivery_operation_transitions \
                 WHERE entity_kind = 'merge_operation' AND entity_id = ? AND entity_version = 4",
            )
            .bind(operation_id.to_string())
            .fetch_one(&mut *connection)
            .await
            .unwrap();
            assert!(merge_transition < source_transition);
            sqlx::query(
                "UPDATE task_delivery_operation_transitions SET transition_id = -1 \
                 WHERE transition_id = ?",
            )
            .bind(source_transition)
            .execute(&mut *connection)
            .await
            .unwrap();
            sqlx::query(
                "UPDATE task_delivery_operation_transitions SET transition_id = ? \
                 WHERE transition_id = ?",
            )
            .bind(source_transition)
            .bind(merge_transition)
            .execute(&mut *connection)
            .await
            .unwrap();
            sqlx::query(
                "UPDATE task_delivery_operation_transitions SET transition_id = ? \
                 WHERE transition_id = -1",
            )
            .bind(merge_transition)
            .execute(&mut *connection)
            .await
            .unwrap();
        }
    }
}

pub async fn corrupt_merge_shape(
    store: &Store,
    task: &Task,
    operation_id: DeliveryOperationId,
    shape: MergeShape,
) {
    let mut connection = corruption_connection(store, false, true).await;
    let operation_id = operation_id.to_string();
    match shape {
        MergeShape::PreflightPending
        | MergeShape::Rejected
        | MergeShape::Stale
        | MergeShape::Superseded => {
            sqlx::query(
                "UPDATE task_merge_operations SET accept_receipt_id = ? WHERE operation_id = ?",
            )
            .bind(CORRUPT_RECEIPT_ID)
            .bind(operation_id)
            .execute(&mut *connection)
            .await
            .unwrap();
        }
        MergeShape::PreflightReady => {
            sqlx::query(
                "UPDATE task_merge_operations SET merge_base_oid = NULL WHERE operation_id = ?",
            )
            .bind(operation_id)
            .execute(&mut *connection)
            .await
            .unwrap();
        }
        MergeShape::Accepted => {
            sqlx::query(
                "UPDATE task_merge_operations SET accept_receipt_id = NULL WHERE operation_id = ?",
            )
            .bind(operation_id)
            .execute(&mut *connection)
            .await
            .unwrap();
        }
        MergeShape::MergePending | MergeShape::Failed => {
            sqlx::query(
                "UPDATE task_merge_operations SET delivery_source_task_id = NULL, \
                     source_commit_oid = NULL WHERE operation_id = ?",
            )
            .bind(operation_id)
            .execute(&mut *connection)
            .await
            .unwrap();
        }
        MergeShape::Merged => {
            sqlx::query(
                "UPDATE task_merge_operations SET expected_merge_commit_oid = NULL \
                 WHERE operation_id = ?",
            )
            .bind(operation_id)
            .execute(&mut *connection)
            .await
            .unwrap();
        }
        MergeShape::AbortPending => {
            sqlx::query(
                "UPDATE task_merge_operations SET abort_child_receipt_id = NULL, \
                     abort_merge_head_oid = NULL, abort_index_stages_digest = NULL, \
                     abort_worktree_digest = NULL, abort_merge_autostash_proof = NULL \
                 WHERE operation_id = ?",
            )
            .bind(operation_id)
            .execute(&mut *connection)
            .await
            .unwrap();
        }
        MergeShape::Conflict | MergeShape::ReconciliationRequired => {
            sqlx::query(
                "UPDATE task_merge_operations SET merged_disposition_task_id = ? \
                 WHERE operation_id = ?",
            )
            .bind(task.id.to_string())
            .bind(operation_id)
            .execute(&mut *connection)
            .await
            .unwrap();
        }
    }
}

pub async fn corrupt_git_oid_algorithm(
    store: &Store,
    task: &Task,
    operation_id: DeliveryOperationId,
    field: GitOidField,
) {
    let mut connection = corruption_connection(store, true, true).await;
    let operation_id = operation_id.to_string();
    match field {
        GitOidField::ArtifactBase => {
            sqlx::query("UPDATE task_attempt_artifacts SET base_commit = ? WHERE task_id = ?")
                .bind(SHA256_OID)
                .bind(task.id.to_string())
                .execute(&mut *connection)
                .await
                .unwrap();
            sqlx::query(
                "UPDATE task_delivery_sources SET artifact_base_commit = ?, \
                     expected_parent_oid = ? WHERE task_id = ?",
            )
            .bind(SHA256_OID)
            .bind(SHA256_OID)
            .bind(task.id.to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
            sqlx::query(
                "UPDATE task_merge_operations SET artifact_base_commit = ? WHERE operation_id = ?",
            )
            .bind(SHA256_OID)
            .bind(operation_id)
            .execute(&mut *connection)
            .await
            .unwrap();
        }
        GitOidField::CandidateTree => {
            sqlx::query(
                "UPDATE task_delivery_sources SET candidate_tree_oid = ? WHERE task_id = ?",
            )
            .bind(SHA256_OID)
            .bind(task.id.to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
            sqlx::query(
                "UPDATE task_merge_operations SET candidate_tree_oid = ? WHERE operation_id = ?",
            )
            .bind(SHA256_OID)
            .bind(operation_id)
            .execute(&mut *connection)
            .await
            .unwrap();
        }
        GitOidField::PreflightSource => {
            update_merge_oid(
                &mut connection,
                &operation_id,
                "preflight_source_commit_oid",
            )
            .await;
        }
        GitOidField::ExpectedTargetHead => {
            update_merge_oid(&mut connection, &operation_id, "expected_target_head").await;
        }
        GitOidField::SourceCommit => {
            sqlx::query(
                "UPDATE task_delivery_sources SET expected_source_commit_oid = ? WHERE task_id = ?",
            )
            .bind(SHA256_OID)
            .bind(task.id.to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
            update_merge_oid(&mut connection, &operation_id, "source_commit_oid").await;
        }
        GitOidField::MergeBase => {
            update_merge_oid(&mut connection, &operation_id, "merge_base_oid").await;
        }
        GitOidField::CandidateMergeTree => {
            update_merge_oid(&mut connection, &operation_id, "candidate_merge_tree_oid").await;
        }
        GitOidField::ExpectedMergeCommit => {
            update_merge_oid(&mut connection, &operation_id, "expected_merge_commit_oid").await;
        }
        GitOidField::AbortMergeHead => {
            sqlx::query(
                "UPDATE task_delivery_sources SET expected_source_commit_oid = ? WHERE task_id = ?",
            )
            .bind(SHA256_OID)
            .bind(task.id.to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
            update_merge_oid(&mut connection, &operation_id, "source_commit_oid").await;
            update_merge_oid(&mut connection, &operation_id, "abort_merge_head_oid").await;
        }
    }
}

pub async fn corrupt_metadata(
    store: &Store,
    task: &Task,
    operation_id: DeliveryOperationId,
    corruption: MetadataCorruption,
) {
    let mut connection = corruption_connection(
        store,
        corruption.targets_source(),
        !corruption.targets_source(),
    )
    .await;
    match corruption {
        MetadataCorruption::SourceAuthor => {
            sqlx::query("UPDATE task_delivery_sources SET author_name = 'Wrong' WHERE task_id = ?")
                .bind(task.id.to_string())
                .execute(&mut *connection)
                .await
                .unwrap();
        }
        MetadataCorruption::SourceDateMismatch => {
            sqlx::query(
                "UPDATE task_delivery_sources SET author_date_bytes = '1785801601 +0000', \
                     committer_date_bytes = '1785801601 +0000' WHERE task_id = ?",
            )
            .bind(task.id.to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
        }
        MetadataCorruption::MergeEmail => {
            sqlx::query(
                "UPDATE task_merge_operations SET merge_author_email = 'wrong@example.com' \
                 WHERE operation_id = ?",
            )
            .bind(operation_id.to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
        }
        MetadataCorruption::SourceTemplate => {
            sqlx::query(
                "UPDATE task_delivery_sources SET commit_message_template_version = 2 \
                 WHERE task_id = ?",
            )
            .bind(task.id.to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
        }
        MetadataCorruption::SourceMessageMismatch => {
            sqlx::query(
                "UPDATE task_delivery_sources \
                 SET commit_message_bytes = CAST('coding-agent: deliver another task attempt 1' AS BLOB) \
                 WHERE task_id = ?",
            )
            .bind(task.id.to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
        }
        MetadataCorruption::MergeDate => {
            sqlx::query(
                "UPDATE task_merge_operations SET merge_author_date_bytes = 'invalid', \
                     merge_committer_date_bytes = 'invalid' WHERE operation_id = ?",
            )
            .bind(operation_id.to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
        }
        MetadataCorruption::SourceEmptyMessage => {
            sqlx::query(
                "UPDATE task_delivery_sources SET commit_message_bytes = x'' WHERE task_id = ?",
            )
            .bind(task.id.to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
        }
        MetadataCorruption::MergeOversizedMessage => {
            sqlx::query(
                "UPDATE task_merge_operations SET merge_message_bytes = zeroblob(513) \
                 WHERE operation_id = ?",
            )
            .bind(operation_id.to_string())
            .execute(&mut *connection)
            .await
            .unwrap();
        }
    }
}

async fn accepted_fixture() -> (Store, Task, DeliveryOperationId) {
    let (store, task, operation_id) = preflight_fixture().await;
    prepare_accepted(&store, &task, operation_id).await;
    (store, task, operation_id)
}

async fn preflight_fixture() -> (Store, Task, DeliveryOperationId) {
    let (store, task) = approved_task_with_ready_artifact("codex/task-shape-corruption").await;
    let snapshot = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    let operation_id = DeliveryOperationId::new();
    insert_preflight(
        &store,
        &task,
        snapshot.evidence_identity.as_ref().unwrap(),
        operation_id,
    )
    .await;
    (store, task, operation_id)
}

async fn prepare_accepted(store: &Store, task: &Task, operation_id: DeliveryOperationId) {
    mark_preflight_ready(store, operation_id).await;
    accept_merge(store, task, operation_id).await;
}

async fn prepare_merge_pending(store: &Store, task: &Task, operation_id: DeliveryOperationId) {
    prepare_accepted(store, task, operation_id).await;
    crate::support::delivery::eligibility::create_committed_source(store, task, operation_id).await;
    mark_merge_pending(store, task, operation_id).await;
}

async fn insert_object_pending_source(
    store: &Store,
    task: &Task,
    operation_id: DeliveryOperationId,
) {
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
             '1785801600 +0000', '1785801600 +0000', 1, \
             CAST('coding-agent: deliver task ' || task_id || ' attempt ' || attempt || char(10) AS BLOB), \
             'object_pending', NULL, 1, ?, ? \
         FROM task_merge_operations WHERE operation_id = ? AND task_id = ?",
    )
    .bind(DELIVERY_TIMESTAMP)
    .bind(DELIVERY_TIMESTAMP)
    .bind(operation_id.to_string())
    .bind(task.id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
}

async fn mark_source_commit_pending(store: &Store, task: &Task) {
    sqlx::query(
        "UPDATE task_delivery_sources SET expected_source_commit_oid = ?, \
             state = 'commit_pending', version = 2, updated_at = ? WHERE task_id = ?",
    )
    .bind(SOURCE_COMMIT)
    .bind(DELIVERY_TIMESTAMP)
    .bind(task.id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
}

async fn mark_source_committed(store: &Store, task: &Task) {
    sqlx::query(
        "UPDATE task_delivery_sources SET state = 'committed', version = 3, updated_at = ? \
         WHERE task_id = ?",
    )
    .bind(DELIVERY_TIMESTAMP)
    .bind(task.id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
}

async fn reconcile_source_and_merge(store: &Store, task: &Task, operation_id: DeliveryOperationId) {
    let mut transaction = store.pool().begin().await.unwrap();
    sqlx::query(
        "UPDATE task_merge_operations SET state = 'reconciliation_required', \
             failure_code = 'DELIVERY_SOURCE_INCONSISTENT', version = 4, updated_at = ? \
         WHERE operation_id = ?",
    )
    .bind(DELIVERY_TIMESTAMP)
    .bind(operation_id.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_delivery_sources SET state = 'reconciliation_required', \
             failure_code = 'DELIVERY_SOURCE_INCONSISTENT', version = 4, updated_at = ? \
         WHERE task_id = ?",
    )
    .bind(DELIVERY_TIMESTAMP)
    .bind(task.id.to_string())
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

async fn mark_abort_pending(store: &Store, operation_id: DeliveryOperationId) {
    sqlx::query(
        "UPDATE task_merge_operations SET state = 'abort_pending', version = 5, \
             abort_child_receipt_id = ?, abort_merge_head_oid = source_commit_oid, \
             abort_index_stages_digest = ?, abort_worktree_digest = ?, \
             abort_merge_autostash_proof = 'absent', updated_at = ? WHERE operation_id = ?",
    )
    .bind(uuid::Uuid::new_v4().to_string())
    .bind(ABORT_INDEX_DIGEST)
    .bind(ABORT_WORKTREE_DIGEST)
    .bind(DELIVERY_TIMESTAMP)
    .bind(operation_id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
}

async fn complete_merge(store: &Store, task: &Task, operation_id: DeliveryOperationId) {
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

async fn corruption_connection(store: &Store, source: bool, merge: bool) -> PoolConnection<Sqlite> {
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("PRAGMA ignore_check_constraints = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
    if source {
        sqlx::raw_sql(
            "DROP TRIGGER task_delivery_sources_immutable_on_update; \
             DROP TRIGGER task_delivery_sources_transition_on_update; \
             DROP TRIGGER task_delivery_sources_merge_consistency_on_update; \
             DROP TRIGGER task_delivery_sources_journal_on_update; \
             DROP TRIGGER task_delivery_operation_transitions_no_update;",
        )
        .execute(&mut *connection)
        .await
        .unwrap();
    }
    if merge {
        sqlx::raw_sql(
            "DROP TRIGGER task_merge_operations_immutable_on_update; \
             DROP TRIGGER task_merge_operations_transition_on_update; \
             DROP TRIGGER task_merge_operations_source_consistency_on_update; \
             DROP TRIGGER task_merge_operations_source_reconciliation_on_update; \
             DROP TRIGGER task_merge_operations_journal_on_update;",
        )
        .execute(&mut *connection)
        .await
        .unwrap();
    }
    connection
}

async fn update_merge_oid(
    connection: &mut PoolConnection<Sqlite>,
    operation_id: &str,
    column: &str,
) {
    let sql = match column {
        "preflight_source_commit_oid" => {
            "UPDATE task_merge_operations SET preflight_source_commit_oid = ? WHERE operation_id = ?"
        }
        "expected_target_head" => {
            "UPDATE task_merge_operations SET expected_target_head = ? WHERE operation_id = ?"
        }
        "source_commit_oid" => {
            "UPDATE task_merge_operations SET source_commit_oid = ? WHERE operation_id = ?"
        }
        "merge_base_oid" => {
            "UPDATE task_merge_operations SET merge_base_oid = ? WHERE operation_id = ?"
        }
        "candidate_merge_tree_oid" => {
            "UPDATE task_merge_operations SET candidate_merge_tree_oid = ? WHERE operation_id = ?"
        }
        "expected_merge_commit_oid" => {
            "UPDATE task_merge_operations SET expected_merge_commit_oid = ? WHERE operation_id = ?"
        }
        "abort_merge_head_oid" => {
            "UPDATE task_merge_operations SET abort_merge_head_oid = ? WHERE operation_id = ?"
        }
        _ => unreachable!(),
    };
    sqlx::query(sql)
        .bind(SHA256_OID)
        .bind(operation_id)
        .execute(&mut **connection)
        .await
        .unwrap();
}
