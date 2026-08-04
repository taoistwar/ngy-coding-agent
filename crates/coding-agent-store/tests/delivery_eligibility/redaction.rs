use coding_agent_domain::TaskId;
use coding_agent_store::{DeliveryOperationId, Store, StoreError};

use crate::support::delivery::eligibility::{
    ADMIN_IDENTITY, COMMON_IDENTITY, CONFIG_DIGEST, approved_task_with_ready_artifact,
    create_merged_delivery, create_worktree_cleanup, finish_preflight_conflict, insert_preflight,
};

const CONFLICT_PATH_SENTINEL: &str = "private/conflict-path-sentinel.rs";
const TASK_STATUS_SENTINEL: &str = "private/task-status-sentinel";
const READINESS_SENTINEL: &str = "private/readiness-sentinel";
const ARTIFACT_STATE_SENTINEL: &str = "private/artifact-state-sentinel";
const SNAPSHOT_INVARIANT: &str =
    "store invariant failed: delivery eligibility snapshot is inconsistent";
const OWNERSHIP_INVARIANT: &str =
    "store invariant failed: delivery ownership snapshot is inconsistent";

#[tokio::test]
async fn eligibility_and_public_delivery_debug_redact_sensitive_snapshot_values() {
    let (store, task) = approved_task_with_ready_artifact("codex/debug-redaction").await;
    let initial = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    let evidence = initial.evidence_identity.as_ref().unwrap().clone();
    create_merged_delivery(&store, &task, &evidence).await;
    create_worktree_cleanup(&store, &task).await;

    let snapshot = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    let ownership = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    let artifact = ownership.artifact.as_ref().unwrap();
    let source = ownership.source.as_ref().unwrap();
    let merge = ownership.merge_operations.first().unwrap();
    let disposition = ownership.disposition.as_ref().unwrap();
    let cleanup = ownership.cleanup_operations.first().unwrap();
    let source_message = format!("{:?}", source.commit_metadata.message_bytes.as_slice());
    let merge_message = format!(
        "{:?}",
        merge
            .merge_metadata
            .as_ref()
            .unwrap()
            .message_bytes
            .as_slice()
    );
    let artifact_path = artifact.worktree_path.to_string();
    let sensitive = [
        snapshot.task.prompt.as_str(),
        snapshot.final_review.as_ref().unwrap().summary(),
        artifact_path.as_str(),
        evidence.workspace_fingerprint().as_str(),
        evidence.checks_digest().as_str(),
        evidence.coverage_digest().as_str(),
        COMMON_IDENTITY,
        ADMIN_IDENTITY,
        CONFIG_DIGEST,
        source_message.as_str(),
        merge_message.as_str(),
    ];
    let rendered = [
        format!("{snapshot:?}"),
        format!("{ownership:?}"),
        format!("{artifact:?}"),
        format!("{source:?}"),
        format!("{:?}", source.provenance),
        format!("{:?}", source.commit_metadata),
        format!("{merge:?}"),
        format!("{:?}", merge.provenance),
        format!("{:?}", merge.merge_metadata.as_ref().unwrap()),
        format!("{disposition:?}"),
        format!("{cleanup:?}"),
    ];

    for output in &rendered {
        for secret in sensitive {
            assert!(
                !output.contains(secret),
                "Debug output leaked sensitive sentinel {secret:?}: {output}"
            );
        }
    }
}

#[tokio::test]
async fn conflict_record_and_ownership_debug_redact_conflict_path_bytes() {
    let (store, task) = approved_task_with_ready_artifact("codex/conflict-redaction").await;
    let evidence = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .evidence_identity
        .unwrap();
    let operation_id = DeliveryOperationId::new();
    insert_preflight(&store, &task, &evidence, operation_id).await;
    finish_preflight_conflict(&store, operation_id, 1).await;
    sqlx::query(
        "INSERT INTO task_merge_conflicts (operation_id, ordinal, path_encoding, path_value) \
         VALUES (?, 0, 'utf8', ?)",
    )
    .bind(operation_id.to_string())
    .bind(CONFLICT_PATH_SENTINEL)
    .execute(store.pool())
    .await
    .unwrap();

    let ownership = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    let conflict = &ownership.merge_operations[0].conflicts[0];
    let encoded_sentinel = format!("{:?}", CONFLICT_PATH_SENTINEL.as_bytes());
    for output in [format!("{ownership:?}"), format!("{conflict:?}")] {
        assert!(!output.contains(CONFLICT_PATH_SENTINEL));
        assert!(!output.contains(&encoded_sentinel));
    }
}

#[tokio::test]
async fn snapshot_decoder_errors_are_fixed_and_do_not_echo_corrupted_values() {
    let (status_store, status_task) =
        approved_task_with_ready_artifact("codex/status-redaction").await;
    force_checked_value(
        &status_store,
        "UPDATE tasks SET status = ? WHERE id = ?",
        TASK_STATUS_SENTINEL,
        status_task.id,
    )
    .await;
    assert_snapshot_errors_redacted(&status_store, status_task.id, TASK_STATUS_SENTINEL).await;

    let (readiness_store, readiness_task) =
        approved_task_with_ready_artifact("codex/readiness-redaction").await;
    sqlx::query("DROP TRIGGER task_delivery_state_no_update")
        .execute(readiness_store.pool())
        .await
        .unwrap();
    force_checked_value(
        &readiness_store,
        "UPDATE task_delivery_state SET readiness = ? WHERE task_id = ?",
        READINESS_SENTINEL,
        readiness_task.id,
    )
    .await;
    assert_snapshot_errors_redacted(&readiness_store, readiness_task.id, READINESS_SENTINEL).await;

    let (artifact_store, artifact_task) =
        approved_task_with_ready_artifact("codex/artifact-state-redaction").await;
    force_checked_value(
        &artifact_store,
        "UPDATE task_attempt_artifacts SET state = ? WHERE task_id = ?",
        ARTIFACT_STATE_SENTINEL,
        artifact_task.id,
    )
    .await;
    assert_snapshot_errors_redacted(&artifact_store, artifact_task.id, ARTIFACT_STATE_SENTINEL)
        .await;
}

#[tokio::test]
async fn snapshot_read_keeps_sqlx_operational_errors_typed() {
    let (store, task) = approved_task_with_ready_artifact("codex/sqlx-error").await;
    store.pool().close().await;

    let error = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        StoreError::Database(sqlx::Error::PoolClosed)
    ));
}

async fn force_checked_value(store: &Store, sql: &'static str, value: &str, task_id: TaskId) {
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA ignore_check_constraints = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query(sql)
        .bind(value)
        .bind(task_id.to_string())
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("PRAGMA ignore_check_constraints = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
}

async fn assert_snapshot_errors_redacted(store: &Store, task_id: TaskId, sentinel: &str) {
    let eligibility_error = store
        .delivery_eligibility_snapshot(task_id)
        .await
        .unwrap_err();
    let ownership_error = store
        .delivery_ownership_snapshot(task_id)
        .await
        .unwrap_err();
    for (error, expected) in [
        (eligibility_error, SNAPSHOT_INVARIANT),
        (ownership_error, OWNERSHIP_INVARIANT),
    ] {
        let message = error.to_string();
        assert_eq!(message, expected);
        assert!(!message.contains(sentinel));
    }
}
