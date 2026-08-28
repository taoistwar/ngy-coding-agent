mod support;

#[path = "delivery_eligibility/cleanup.rs"]
mod cleanup_cases;
#[path = "delivery_eligibility/compatibility.rs"]
mod compatibility_cases;
#[path = "delivery_eligibility/corruption.rs"]
mod corruption_cases;
#[path = "delivery_eligibility/evidence.rs"]
mod evidence_cases;
#[path = "delivery_eligibility/redaction.rs"]
mod redaction_cases;
#[path = "delivery_eligibility/selectors.rs"]
mod selector_cases;
#[path = "delivery_eligibility/shape_corruption.rs"]
mod shape_corruption_cases;

use std::sync::mpsc;
use std::time::Duration;

use coding_agent_domain::{ReviewVerdict, TaskStatus};
use coding_agent_store::{
    AttemptArtifactState, DeliveryOperationId, MergeOperationState, PersistentEligibilityBlocker,
    StoreError,
};
use tokio::sync::oneshot;

use support::delivery::eligibility::{
    ADMIN_IDENTITY, COMMON_IDENTITY, DELIVERY_TIMESTAMP, FixtureArtifactState, accept_merge,
    approved_task_on_store, approved_task_with_artifact_state, approved_task_with_ready_artifact,
    create_committed_source, fail_accepted_merge, insert_preflight, mark_preflight_ready,
    rejected_task,
};

#[tokio::test]
async fn approved_task_snapshot_derives_evidence_and_preserves_short_branch_storage() {
    let (store, task) = approved_task_with_ready_artifact("codex/task-eligibility").await;
    let stored_before: String =
        sqlx::query_scalar("SELECT branch_name FROM task_attempt_artifacts WHERE task_id = ?")
            .bind(task.id.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap();

    let snapshot = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .expect("approved task snapshot");

    assert_eq!(snapshot.task, task);
    assert_eq!(snapshot.final_review.as_ref().unwrap().round(), 1);
    let evidence = snapshot.evidence_identity.as_ref().unwrap();
    assert_eq!(evidence.identity().task_id(), task.id);
    assert_eq!(evidence.identity().repository_id(), task.repository_id);
    assert_eq!(evidence.identity().attempt(), task.attempt);
    assert_eq!(evidence.final_review_round(), 1);
    assert_eq!(
        evidence.checks_digest().as_str(),
        "694a0edeac6267aa0498462a95205cc84afb5dc3350d83479a890c65062bb63d"
    );
    assert_eq!(
        evidence.coverage_digest().as_str(),
        "826b0ac5c390abf76fe4f2d392fb0091ec9be665e930097dba9b9341d7fa1fc2"
    );
    assert!(!snapshot.ownership.is_delivery_owned());
    assert_eq!(
        snapshot.ownership.artifact.as_ref().unwrap().state,
        AttemptArtifactState::Ready
    );
    assert!(snapshot.ownership.source.is_none());
    assert!(snapshot.ownership.merge_operations.is_empty());
    assert!(snapshot.ownership.disposition.is_none());
    assert!(snapshot.ownership.cleanup_operations.is_empty());

    let ownership = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .expect("task ownership snapshot");
    assert_eq!(ownership, snapshot.ownership);
    let stored_after: String =
        sqlx::query_scalar("SELECT branch_name FROM task_attempt_artifacts WHERE task_id = ?")
            .bind(task.id.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(stored_before, "codex/task-eligibility");
    assert_eq!(stored_after, stored_before);
}

#[tokio::test]
async fn exact_preflight_provenance_owns_the_task_without_rewriting_the_short_branch() {
    let (store, task) = approved_task_with_ready_artifact("codex/task-owned").await;
    let before = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    let operation_id = DeliveryOperationId::new();
    insert_preflight(
        &store,
        &task,
        before.evidence_identity.as_ref().unwrap(),
        operation_id,
    )
    .await;

    let snapshot = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    assert!(snapshot.ownership.is_delivery_owned());
    assert_eq!(snapshot.ownership.merge_operations.len(), 1);
    assert_eq!(
        snapshot.ownership.merge_operations[0].operation_id,
        operation_id
    );
    assert_eq!(
        snapshot.ownership.merge_operations[0].state,
        MergeOperationState::PreflightPending
    );
    assert_eq!(
        snapshot.ownership.merge_operations[0]
            .provenance
            .source_branch
            .as_str(),
        "refs/heads/codex/task-owned"
    );
    let stored: String =
        sqlx::query_scalar("SELECT branch_name FROM task_attempt_artifacts WHERE task_id = ?")
            .bind(task.id.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(stored, "codex/task-owned");
}

#[tokio::test]
async fn persistent_blockers_distinguish_business_states_without_claiming_runtime_eligibility() {
    for status in [
        TaskStatus::Queued,
        TaskStatus::Running,
        TaskStatus::Failed,
        TaskStatus::Cancelled,
        TaskStatus::Interrupted,
    ] {
        let store = support::seeded_store().await;
        let task = match status {
            TaskStatus::Queued => support::queued_task(&store).await,
            TaskStatus::Running => support::running_task(&store).await,
            TaskStatus::Failed | TaskStatus::Cancelled | TaskStatus::Interrupted => {
                support::terminal_task(&store, status).await
            }
            TaskStatus::Completed => unreachable!(),
        };
        let snapshot = store
            .delivery_eligibility_snapshot(task.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.task.status, status);
        assert_eq!(
            snapshot.persistent_blockers,
            vec![
                PersistentEligibilityBlocker::TaskNotCompleted,
                PersistentEligibilityBlocker::ReviewNotApproved,
                PersistentEligibilityBlocker::ApprovedEvidenceMissing,
                PersistentEligibilityBlocker::AttemptArtifactMissing,
            ]
        );
    }

    let historical_store = support::seeded_store().await;
    let running = support::running_task(&historical_store).await;
    let unreviewed = support::historical_completed_task(&historical_store, running).await;
    let unreviewed_snapshot = historical_store
        .delivery_eligibility_snapshot(unreviewed.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !unreviewed_snapshot
            .persistent_blockers
            .contains(&PersistentEligibilityBlocker::TaskNotCompleted)
    );
    assert!(
        unreviewed_snapshot
            .persistent_blockers
            .contains(&PersistentEligibilityBlocker::ReviewNotApproved)
    );
    assert!(
        unreviewed_snapshot
            .persistent_blockers
            .contains(&PersistentEligibilityBlocker::ApprovedEvidenceMissing)
    );

    let (approved_store, approved_without_artifact) =
        approved_task_with_ready_artifact("codex/task-artifact-absent").await;
    sqlx::query("DELETE FROM task_attempt_artifacts WHERE task_id = ?")
        .bind(approved_without_artifact.id.to_string())
        .execute(approved_store.pool())
        .await
        .unwrap();
    let absent_artifact = approved_store
        .delivery_eligibility_snapshot(approved_without_artifact.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        absent_artifact.persistent_blockers,
        vec![PersistentEligibilityBlocker::AttemptArtifactMissing]
    );

    for (state, expected) in [
        (
            FixtureArtifactState::Reserved,
            AttemptArtifactState::Reserved,
        ),
        (
            FixtureArtifactState::Inconsistent,
            AttemptArtifactState::Inconsistent,
        ),
    ] {
        let (store, task) = approved_task_with_artifact_state("codex/task-not-ready", state).await;
        let snapshot = store
            .delivery_eligibility_snapshot(task.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            snapshot.ownership.artifact.as_ref().unwrap().state,
            expected
        );
        assert_eq!(
            snapshot.persistent_blockers,
            vec![PersistentEligibilityBlocker::AttemptArtifactNotReady]
        );
    }

    let (store, rejected) = rejected_task().await;
    let snapshot = store
        .delivery_eligibility_snapshot(rejected.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        snapshot.final_review.as_ref().unwrap().verdict(),
        ReviewVerdict::ChangesRequested
    );
    assert!(snapshot.evidence_identity.is_none());
    assert!(
        snapshot
            .persistent_blockers
            .contains(&PersistentEligibilityBlocker::ReviewNotApproved)
    );
    assert!(
        snapshot
            .persistent_blockers
            .contains(&PersistentEligibilityBlocker::ApprovedEvidenceMissing)
    );
}

#[tokio::test]
async fn ready_and_historical_terminal_merge_facts_do_not_create_a_generic_owned_blocker() {
    let (store, task) = approved_task_with_ready_artifact("codex/task-terminal").await;
    let initial = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    let operation_id = DeliveryOperationId::new();
    insert_preflight(
        &store,
        &task,
        initial.evidence_identity.as_ref().unwrap(),
        operation_id,
    )
    .await;
    mark_preflight_ready(&store, operation_id).await;
    let ready = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        ready.ownership.merge_operations[0].state,
        MergeOperationState::PreflightReady
    );
    assert!(
        !ready
            .persistent_blockers
            .contains(&PersistentEligibilityBlocker::DeliveryOwned)
    );

    accept_merge(&store, &task, operation_id).await;
    create_committed_source(&store, &task, operation_id).await;
    fail_accepted_merge(&store, &task, operation_id).await;
    let terminal = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        terminal
            .ownership
            .source
            .as_ref()
            .unwrap()
            .state
            .to_string(),
        "committed"
    );
    assert_eq!(
        terminal.ownership.merge_operations[0].state,
        MergeOperationState::Failed
    );
    assert!(
        !terminal
            .persistent_blockers
            .contains(&PersistentEligibilityBlocker::DeliveryOwned)
    );
}

#[tokio::test]
async fn operation_order_uses_initial_transition_id_for_old_terminal_and_new_active() {
    let (store, task) = approved_task_with_ready_artifact("codex/task-operation-order").await;
    let initial = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    let evidence = initial.evidence_identity.as_ref().unwrap();
    let old_terminal = DeliveryOperationId::new();
    insert_preflight(&store, &task, evidence, old_terminal).await;
    mark_preflight_ready(&store, old_terminal).await;
    accept_merge(&store, &task, old_terminal).await;
    create_committed_source(&store, &task, old_terminal).await;
    fail_accepted_merge(&store, &task, old_terminal).await;

    let new_active = DeliveryOperationId::new();
    insert_preflight(&store, &task, evidence, new_active).await;
    let snapshot = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        snapshot
            .ownership
            .merge_operations
            .iter()
            .map(|operation| (operation.operation_id, operation.state))
            .collect::<Vec<_>>(),
        vec![
            (old_terminal, MergeOperationState::Failed),
            (new_active, MergeOperationState::PreflightPending),
        ]
    );
    assert!(
        snapshot.ownership.merge_operations[0].initial_transition_id
            < snapshot.ownership.merge_operations[1].initial_transition_id
    );
    let timestamps: Vec<String> = sqlx::query_scalar(
        "SELECT transitioned_at FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'merge_operation' AND entity_version = 1 \
         AND entity_id IN (?, ?) ORDER BY transition_id",
    )
    .bind(old_terminal.to_string())
    .bind(new_active.to_string())
    .fetch_all(store.pool())
    .await
    .unwrap();
    assert_eq!(timestamps, vec![DELIVERY_TIMESTAMP; 2]);
}

#[tokio::test]
async fn old_failed_merge_is_validated_before_later_source_and_merge_reconciliation() {
    let (store, task) = approved_task_with_ready_artifact("codex/task-reconciliation").await;
    let initial = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    let evidence = initial.evidence_identity.as_ref().unwrap();
    let old_failed = DeliveryOperationId::new();
    insert_preflight(&store, &task, evidence, old_failed).await;
    mark_preflight_ready(&store, old_failed).await;
    accept_merge(&store, &task, old_failed).await;
    create_committed_source(&store, &task, old_failed).await;
    fail_accepted_merge(&store, &task, old_failed).await;

    let reconciliation = DeliveryOperationId::new();
    insert_preflight(&store, &task, evidence, reconciliation).await;
    mark_preflight_ready(&store, reconciliation).await;
    accept_merge(&store, &task, reconciliation).await;
    let mut transaction = store.pool().begin().await.unwrap();
    sqlx::query(
        "UPDATE task_merge_operations SET state = 'reconciliation_required', \
             failure_code = 'DELIVERY_SOURCE_INCONSISTENT', version = 5, updated_at = ? WHERE operation_id = ?",
    )
    .bind(DELIVERY_TIMESTAMP)
    .bind(reconciliation.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_delivery_sources SET state = 'reconciliation_required', \
             failure_code = 'DELIVERY_SOURCE_INCONSISTENT', version = 4, updated_at = ? WHERE task_id = ?",
    )
    .bind(DELIVERY_TIMESTAMP)
    .bind(task.id.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    transaction.commit().await.unwrap();

    let snapshot = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        snapshot
            .ownership
            .merge_operations
            .iter()
            .map(|operation| (operation.operation_id, operation.state))
            .collect::<Vec<_>>(),
        vec![
            (old_failed, MergeOperationState::Failed),
            (reconciliation, MergeOperationState::ReconciliationRequired),
        ]
    );
    assert!(
        snapshot
            .persistent_blockers
            .contains(&PersistentEligibilityBlocker::ReconciliationRequired)
    );
}

#[tokio::test]
async fn delivery_join_rejects_ambiguous_or_invalid_short_branch_without_leaking_secrets() {
    for corrupt_branch in ["refs/heads/codex/task-owned", ".hidden"] {
        let (store, task) = approved_task_with_ready_artifact("codex/task-owned").await;
        let before = store
            .delivery_eligibility_snapshot(task.id)
            .await
            .unwrap()
            .unwrap();
        insert_preflight(
            &store,
            &task,
            before.evidence_identity.as_ref().unwrap(),
            DeliveryOperationId::new(),
        )
        .await;
        sqlx::query("UPDATE task_attempt_artifacts SET branch_name = ? WHERE task_id = ?")
            .bind(corrupt_branch)
            .bind(task.id.to_string())
            .execute(store.pool())
            .await
            .unwrap();

        let error = store
            .delivery_eligibility_snapshot(task.id)
            .await
            .unwrap_err();
        assert!(matches!(error, StoreError::InvariantViolation(_)));
        let message = error.to_string();
        assert!(!message.contains("approved delivery prompt secret"));
        assert!(!message.contains("artifacts"));
        assert!(!message.contains(COMMON_IDENTITY));
        assert!(!message.contains(ADMIN_IDENTITY));
        assert!(!message.contains(before.evidence_identity.unwrap().checks_digest().as_str()));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eligibility_uses_one_deferred_wal_snapshot_across_a_concurrent_delivery_commit() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    support::register_repository(&fixture.store, "eligibility-gate").await;
    let (store, task) =
        approved_task_on_store(fixture.store.clone(), "codex/task-gated", 512).await;
    let before = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    assert!(!before.ownership.is_delivery_owned());
    let evidence = before.evidence_identity.clone().unwrap();

    let max_connections = store.pool().options().get_max_connections() as usize;
    assert!(max_connections >= 2);
    let mut reserved = Vec::with_capacity(max_connections);
    for _ in 0..max_connections {
        reserved.push(store.pool().acquire().await.unwrap());
    }
    let mut instrumented = reserved.pop().unwrap();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    {
        let mut handle = instrumented.lock_handle().await.unwrap();
        let mut entered_tx = Some(entered_tx);
        let mut release_rx = Some(release_rx);
        handle.set_progress_handler(1_000, move || {
            if let Some(entered_tx) = entered_tx.take() {
                let _ = entered_tx.send(());
                let _ = release_rx.take().unwrap().recv();
            }
            true
        });
    }
    drop(instrumented);

    let snapshot_store = store.clone();
    let snapshot =
        tokio::spawn(async move { snapshot_store.delivery_eligibility_snapshot(task.id).await });
    tokio::time::timeout(Duration::from_secs(5), entered_rx)
        .await
        .expect("snapshot reaches progress gate")
        .expect("progress gate remains alive");

    drop(reserved.pop().expect("reserve a writer connection"));
    let writer_store = store.clone();
    let writer_task = task.clone();
    let write = tokio::spawn(async move {
        insert_preflight(
            &writer_store,
            &writer_task,
            &evidence,
            DeliveryOperationId::new(),
        )
        .await;
    });
    tokio::time::timeout(Duration::from_secs(5), write)
        .await
        .expect("WAL writer commits while reader is paused")
        .unwrap();
    release_tx.send(()).unwrap();

    let during = tokio::time::timeout(Duration::from_secs(5), snapshot)
        .await
        .expect("snapshot completes after gate release")
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(during, before);
    let after = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    assert!(after.ownership.is_delivery_owned());
    assert_eq!(after.ownership.merge_operations.len(), 1);
}
