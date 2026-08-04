use coding_agent_store::{DeliveryOperationId, StoreError};

use crate::support::delivery::eligibility::{
    MergeCopyCorruption, accept_merge, approved_task_on_store, approved_task_with_prior_rejection,
    approved_task_with_ready_artifact, corrupt_approved_review_without_coverage,
    corrupt_artifact_attempt, corrupt_artifact_state, corrupt_merge_copy, corrupt_merge_evidence,
    corrupt_transition_ids, corrupt_transition_state_pair, create_committed_source,
    delete_artifact_parent, insert_preflight, mark_preflight_ready,
};

#[tokio::test]
async fn artifact_corruption_fails_closed_even_before_the_first_delivery_row() {
    for corrupt_branch in ["refs/heads/codex/already-qualified", ".hidden"] {
        let (store, task) = approved_task_with_ready_artifact("codex/task-parent-corrupt").await;
        sqlx::query("UPDATE task_attempt_artifacts SET branch_name = ? WHERE task_id = ?")
            .bind(corrupt_branch)
            .bind(task.id.to_string())
            .execute(store.pool())
            .await
            .unwrap();
        assert_invariant(
            store
                .delivery_eligibility_snapshot(task.id)
                .await
                .unwrap_err(),
        );
    }

    let (store, task) = approved_task_with_ready_artifact("codex/task-zero-base").await;
    sqlx::query("UPDATE task_attempt_artifacts SET base_commit = ? WHERE task_id = ?")
        .bind("0".repeat(40))
        .bind(task.id.to_string())
        .execute(store.pool())
        .await
        .unwrap();
    assert_invariant(
        store
            .delivery_eligibility_snapshot(task.id)
            .await
            .unwrap_err(),
    );

    let (store, task) = approved_task_with_ready_artifact("codex/task-attempt-corrupt").await;
    corrupt_artifact_attempt(&store, &task).await;
    assert_invariant(
        store
            .delivery_eligibility_snapshot(task.id)
            .await
            .unwrap_err(),
    );
}

#[tokio::test]
async fn delivery_rows_reject_non_ready_mismatched_or_missing_artifact_parents() {
    for state in ["reserved", "inconsistent"] {
        let (store, task) = approved_task_with_ready_artifact("codex/task-not-ready-parent").await;
        insert_valid_preflight(&store, &task).await;
        corrupt_artifact_state(&store, &task, state).await;
        assert_invariant(
            store
                .delivery_eligibility_snapshot(task.id)
                .await
                .unwrap_err(),
        );
    }

    let (store, task) = approved_task_with_ready_artifact("codex/task-mismatched-parent").await;
    insert_valid_preflight(&store, &task).await;
    corrupt_artifact_attempt(&store, &task).await;
    assert_invariant(
        store
            .delivery_ownership_snapshot(task.id)
            .await
            .unwrap_err(),
    );

    let (store, task) = approved_task_with_ready_artifact("codex/task-missing-parent").await;
    insert_valid_preflight(&store, &task).await;
    delete_artifact_parent(&store, &task).await;
    assert_invariant(
        store
            .delivery_eligibility_snapshot(task.id)
            .await
            .unwrap_err(),
    );
}

#[tokio::test]
async fn every_stale_evidence_component_in_a_delivery_row_fails_closed() {
    for column in [
        "workspace_generation",
        "workspace_fingerprint",
        "checks_digest",
        "coverage_digest",
    ] {
        let (store, task) = approved_task_with_ready_artifact("codex/task-stale-evidence").await;
        let operation_id = insert_valid_preflight(&store, &task).await;
        corrupt_merge_evidence(&store, operation_id, column).await;

        let error = store
            .delivery_eligibility_snapshot(task.id)
            .await
            .unwrap_err();
        assert_invariant(error);
    }
}

#[tokio::test]
async fn an_approved_review_without_coverage_fails_typed_decoding() {
    let (store, task) = approved_task_with_ready_artifact("codex/task-missing-coverage").await;
    corrupt_approved_review_without_coverage(&store, &task).await;

    assert_invariant(
        store
            .delivery_eligibility_snapshot(task.id)
            .await
            .unwrap_err(),
    );
}

#[tokio::test]
async fn transition_history_requires_positive_monotonic_ids_and_legal_state_pairs() {
    for corrupt_current_negative in [true, false] {
        let (store, task) = approved_task_with_ready_artifact("codex/task-transition-order").await;
        let operation_id = insert_valid_preflight(&store, &task).await;
        mark_preflight_ready(&store, operation_id).await;
        corrupt_transition_ids(&store, operation_id, corrupt_current_negative).await;
        assert_invariant(
            store
                .delivery_eligibility_snapshot(task.id)
                .await
                .unwrap_err(),
        );
    }

    let (store, task) = approved_task_with_ready_artifact("codex/task-transition-pair").await;
    let operation_id = insert_valid_preflight(&store, &task).await;
    mark_preflight_ready(&store, operation_id).await;
    corrupt_transition_state_pair(&store, operation_id).await;
    assert_invariant(
        store
            .delivery_eligibility_snapshot(task.id)
            .await
            .unwrap_err(),
    );
}

#[tokio::test]
async fn evidence_identity_uses_the_exact_final_review_round_and_event() {
    let (store, task) = approved_task_with_prior_rejection("codex/task-final-review").await;
    let review_rows: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT review_round, event_id FROM task_review_evidence \
         WHERE task_id = ? ORDER BY review_round",
    )
    .bind(task.id.to_string())
    .fetch_all(store.pool())
    .await
    .unwrap();
    assert_eq!(review_rows.len(), 2);
    let snapshot = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.final_review.as_ref().unwrap().round(), 2);
    let evidence = snapshot.evidence_identity.as_ref().unwrap();
    assert_eq!(evidence.final_review_round(), 2);
    assert_eq!(evidence.final_review_event_id().get(), review_rows[1].1);

    let operation_id = DeliveryOperationId::new();
    insert_preflight(&store, &task, evidence, operation_id).await;
    corrupt_merge_copy(
        &store,
        operation_id,
        MergeCopyCorruption::PriorReviewIdentity(review_rows[0].1),
    )
    .await;
    assert_invariant(
        store
            .delivery_eligibility_snapshot(task.id)
            .await
            .unwrap_err(),
    );
}

#[tokio::test]
async fn nonfinal_and_cross_task_review_event_copies_fail_closed() {
    for only_round_drift in [true, false] {
        let (store, task) = approved_task_with_prior_rejection("codex/task-review-copy").await;
        let review_rows: Vec<(i64, i64)> = sqlx::query_as(
            "SELECT review_round, event_id FROM task_review_evidence \
             WHERE task_id = ? ORDER BY review_round",
        )
        .bind(task.id.to_string())
        .fetch_all(store.pool())
        .await
        .unwrap();
        let operation_id = insert_valid_preflight(&store, &task).await;
        let corruption = if only_round_drift {
            MergeCopyCorruption::FinalReviewRound
        } else {
            MergeCopyCorruption::FinalReviewEventId(review_rows[0].1)
        };
        corrupt_merge_copy(&store, operation_id, corruption).await;
        assert_invariant(
            store
                .delivery_eligibility_snapshot(task.id)
                .await
                .unwrap_err(),
        );
    }

    let (store, task) = approved_task_with_prior_rejection("codex/task-cross-review").await;
    let operation_id = insert_valid_preflight(&store, &task).await;
    let (store, other_task) =
        approved_task_on_store(store, "codex/task-cross-review-other", 0).await;
    let other_event: i64 = sqlx::query_scalar(
        "SELECT event_id FROM task_review_evidence \
         WHERE task_id = ? ORDER BY review_round DESC LIMIT 1",
    )
    .bind(other_task.id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    corrupt_merge_copy(
        &store,
        operation_id,
        MergeCopyCorruption::FinalReviewEventId(other_event),
    )
    .await;
    assert_invariant(
        store
            .delivery_eligibility_snapshot(task.id)
            .await
            .unwrap_err(),
    );
}

#[tokio::test]
async fn merge_artifact_provenance_must_be_an_exact_copy() {
    for case in 0..3 {
        let (store, task) = approved_task_with_ready_artifact("codex/task-artifact-copy").await;
        let operation_id = insert_valid_preflight(&store, &task).await;
        let repository_root = store
            .list_repositories()
            .await
            .unwrap()
            .remove(0)
            .git_root
            .to_string();
        let corruption = match case {
            0 => MergeCopyCorruption::ArtifactBaseCommit,
            1 => MergeCopyCorruption::ArtifactSourceBranch,
            _ => MergeCopyCorruption::ArtifactWorktreePath(&repository_root),
        };
        corrupt_merge_copy(&store, operation_id, corruption).await;
        assert_invariant(
            store
                .delivery_eligibility_snapshot(task.id)
                .await
                .unwrap_err(),
        );
    }
}

#[tokio::test]
async fn source_and_merge_must_copy_identity_and_candidate_tree_exactly() {
    for corruption in [
        MergeCopyCorruption::CommonGitIdentity,
        MergeCopyCorruption::WorktreeAdminIdentity,
        MergeCopyCorruption::CandidateTree,
    ] {
        let (store, task) = approved_task_with_ready_artifact("codex/task-source-copy").await;
        let operation_id = insert_valid_preflight(&store, &task).await;
        mark_preflight_ready(&store, operation_id).await;
        accept_merge(&store, &task, operation_id).await;
        create_committed_source(&store, &task, operation_id).await;
        corrupt_merge_copy(&store, operation_id, corruption).await;
        assert_invariant(
            store
                .delivery_eligibility_snapshot(task.id)
                .await
                .unwrap_err(),
        );
    }
}

async fn insert_valid_preflight(
    store: &coding_agent_store::Store,
    task: &coding_agent_domain::Task,
) -> DeliveryOperationId {
    let snapshot = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    let operation_id = DeliveryOperationId::new();
    insert_preflight(
        store,
        task,
        snapshot.evidence_identity.as_ref().unwrap(),
        operation_id,
    )
    .await;
    operation_id
}

fn assert_invariant(error: StoreError) {
    assert!(matches!(error, StoreError::InvariantViolation(_)));
    let message = error.to_string();
    for secret in [
        "approved delivery prompt secret",
        "task-stale-evidence",
        "c1c1c1c1c1c1c1c1",
        "694a0edeac6267aa",
    ] {
        assert!(!message.contains(secret));
    }
}
