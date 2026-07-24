mod support;

use coding_agent_domain::{
    CheckActor, CheckEvidence, CheckEvidenceStatus, DeliveryReadiness, EventCursor, EventId,
    FindingSeverity, NewReviewEvidence, PlanItem, PlanItemStatus, PlanSnapshot, RequiredCheck,
    ReviewCoverageEvidence, ReviewDecisionSource, ReviewEvidence, ReviewFinding, ReviewVerdict,
    Task, TaskEvent, TaskEventKind, TaskEventPayload, TaskId, TaskStatus, UtcTimestamp,
    WorkspaceDigest,
};
use coding_agent_store::{
    FinalizeReviewedTaskOutcome, RecordReviewOutcome, RetryTaskOutcome, Store, StoreError,
    TaskTransition, TransitionOutcome,
};

const REVIEW_MARKER: &str = r#"{"evidence_ref":true}"#;

#[tokio::test]
async fn round_one_and_two_reviews_are_immutable_nonterminal_events() {
    let store = support::seeded_store().await;
    let task = running_project3_task(&store).await;

    for round in 1..=2 {
        let evidence = changes_requested(round, format!("round {round} needs work"));
        let outcome = store
            .record_review(task.id, task.repository_id, task.attempt, evidence.clone())
            .await
            .unwrap();
        let (review, event_id) = applied_record(outcome);

        assert_eq!(review.round(), round);
        assert_eq!(review.verdict(), ReviewVerdict::ChangesRequested);
        assert_eq!(review.summary(), evidence.summary());

        let detail = store.task_detail(task.id).await.unwrap().unwrap();
        assert_eq!(detail.task.status, TaskStatus::Running);
        assert_eq!(
            detail.task.delivery_readiness,
            DeliveryReadiness::Unreviewed
        );
        assert_eq!(detail.task.last_event_id, event_id);
        assert_eq!(
            detail
                .reviews
                .iter()
                .map(ReviewEvidence::round)
                .collect::<Vec<_>>(),
            (1..=round).collect::<Vec<_>>()
        );
        assert_eq!(
            review_event_payload(&store, event_id.get()).await,
            REVIEW_MARKER
        );
        assert_eq!(delivery_count(&store, task.id).await, 0);
    }
}

#[tokio::test]
async fn third_changes_requested_review_atomically_rejects_the_task() {
    let store = support::seeded_store().await;
    let task = running_project3_task(&store).await;
    seed_prior_rounds(&store, &task, 3).await;

    let outcome = store
        .finalize_reviewed_task(
            task.id,
            task.repository_id,
            task.attempt,
            changes_requested(3, "third review still has a blocker"),
        )
        .await
        .unwrap();
    let (final_task, review, review_event_id, terminal_event_id) = applied_final(outcome);

    assert_eq!(review.round(), 3);
    assert_eq!(review.verdict(), ReviewVerdict::ChangesRequested);
    assert_eq!(final_task.status, TaskStatus::Failed);
    assert_eq!(
        final_task.delivery_readiness,
        DeliveryReadiness::ReviewRejected
    );
    let failure = final_task.failure.as_ref().expect("rejection failure");
    assert_eq!(failure.code, "REVIEW_REJECTED");
    assert!(failure.retryable);
    assert_eq!(terminal_event_id.get(), review_event_id.get() + 1);
    assert_eq!(final_task.last_event_id, terminal_event_id);
    assert_final_rows_share_one_timestamp(
        &store,
        task.id,
        3,
        review_event_id.get(),
        terminal_event_id.get(),
    )
    .await;
    assert_terminal_event_order(
        &store,
        task.id,
        review_event_id.get(),
        terminal_event_id.get(),
        TaskEventKind::TaskFailed,
    )
    .await;
}

#[tokio::test]
async fn an_approved_review_can_finalize_any_round() {
    for round in 1..=3 {
        let store = support::seeded_store().await;
        let task = running_project3_task(&store).await;
        seed_prior_rounds(&store, &task, round).await;

        let outcome = store
            .finalize_reviewed_task(task.id, task.repository_id, task.attempt, approved(round))
            .await
            .unwrap();
        let (final_task, review, review_event_id, terminal_event_id) = applied_final(outcome);

        assert_eq!(review.round(), round);
        assert_eq!(review.verdict(), ReviewVerdict::Approved);
        assert_eq!(final_task.status, TaskStatus::Completed);
        assert_eq!(
            final_task.delivery_readiness,
            DeliveryReadiness::ReviewApproved
        );
        assert_eq!(final_task.failure, None);
        assert_eq!(terminal_event_id.get(), review_event_id.get() + 1);
        assert_eq!(final_task.last_event_id, terminal_event_id);
        assert_terminal_event_order(
            &store,
            task.id,
            review_event_id.get(),
            terminal_event_id.get(),
            TaskEventKind::TaskCompleted,
        )
        .await;

        let detail = store.task_detail(task.id).await.unwrap().unwrap();
        assert_eq!(detail.reviews.len(), usize::from(round));
        assert_eq!(detail.reviews.last(), Some(&review));
    }
}

#[tokio::test]
async fn review_writes_are_existing_first_and_conflicting_replays_fail_closed() {
    let store = support::seeded_store().await;
    let task = running_project3_task(&store).await;
    let request = changes_requested(1, "stable request");

    let first = store
        .record_review(task.id, task.repository_id, task.attempt, request.clone())
        .await
        .unwrap();
    let (stored_review, stored_event_id) = applied_record(first);

    // Existing-first remains valid after an unrelated terminal transition.
    let failed = store
        .transition_with_event(
            task.id,
            TaskStatus::Running,
            TaskTransition::Failed(support::failure("RUNNER_FAILED")),
        )
        .await
        .unwrap();
    assert!(matches!(failed, TransitionOutcome::Applied { .. }));
    let event_count_before_replay = event_count(&store, task.id).await;

    let replay = store
        .record_review(task.id, task.repository_id, task.attempt, request)
        .await
        .unwrap();
    match replay {
        RecordReviewOutcome::Existing { review, event_id } => {
            assert_eq!(review, stored_review);
            assert_eq!(event_id, stored_event_id);
        }
        RecordReviewOutcome::Applied { .. } => panic!("same canonical input must be Existing"),
    }
    assert_eq!(
        event_count(&store, task.id).await,
        event_count_before_replay
    );

    let error = store
        .record_review(
            task.id,
            task.repository_id,
            task.attempt,
            changes_requested(1, "different canonical input"),
        )
        .await
        .unwrap_err();
    assert_invariant_conflict(error);
    assert_eq!(
        event_count(&store, task.id).await,
        event_count_before_replay
    );

    assert_invariant_conflict(
        store
            .finalize_reviewed_task(
                task.id,
                task.repository_id,
                task.attempt,
                changes_requested(1, "stable request"),
            )
            .await
            .unwrap_err(),
    );
}

#[tokio::test]
async fn finalization_replay_is_existing_first_and_partial_tuple_is_a_conflict() {
    let store = support::seeded_store().await;
    let task = running_project3_task(&store).await;
    let request = approved(1);
    let first = store
        .finalize_reviewed_task(task.id, task.repository_id, task.attempt, request.clone())
        .await
        .unwrap();
    let (stored_task, stored_review, review_event_id, terminal_event_id) = applied_final(first);
    let event_count_before_replay = event_count(&store, task.id).await;

    let replay = store
        .finalize_reviewed_task(task.id, task.repository_id, task.attempt, request)
        .await
        .unwrap();
    match replay {
        FinalizeReviewedTaskOutcome::Existing {
            task,
            review,
            review_event_id: replay_review_event_id,
            terminal_event_id: replay_terminal_event_id,
        } => {
            assert_eq!(task, stored_task);
            assert_eq!(review, stored_review);
            assert_eq!(replay_review_event_id, review_event_id);
            assert_eq!(replay_terminal_event_id, terminal_event_id);
        }
        FinalizeReviewedTaskOutcome::Applied { .. } => {
            panic!("same final canonical input must be Existing")
        }
    }
    assert_eq!(
        event_count(&store, task.id).await,
        event_count_before_replay
    );

    let changed = approved_with_summary(1, "different final request");
    assert_invariant_conflict(
        store
            .finalize_reviewed_task(task.id, task.repository_id, task.attempt, changed)
            .await
            .unwrap_err(),
    );
    assert_eq!(
        event_count(&store, task.id).await,
        event_count_before_replay
    );

    // Removing one component simulates a crash-corrupted partial final tuple.
    sqlx::query("DROP TRIGGER task_delivery_state_no_delete")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM task_delivery_state WHERE task_id = ?")
        .bind(task.id.to_string())
        .execute(store.pool())
        .await
        .unwrap();
    assert_invariant_conflict(
        store
            .finalize_reviewed_task(task.id, task.repository_id, task.attempt, approved(1))
            .await
            .unwrap_err(),
    );
    assert_eq!(
        event_count(&store, task.id).await,
        event_count_before_replay
    );
}

#[tokio::test]
async fn reviewed_terminal_aggregate_requires_the_terminal_lifecycle_event() {
    let store = support::seeded_store().await;
    let task = running_project3_task(&store).await;
    let (_, _, review_event_id, terminal_event_id) = applied_final(
        store
            .finalize_reviewed_task(task.id, task.repository_id, task.attempt, approved(1))
            .await
            .unwrap(),
    );

    sqlx::query("DELETE FROM task_events WHERE id = ?")
        .bind(terminal_event_id.get())
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE tasks SET last_event_id = ? WHERE id = ?")
        .bind(review_event_id.get())
        .bind(task.id.to_string())
        .execute(store.pool())
        .await
        .unwrap();

    assert_invariant_conflict(store.task_detail(task.id).await.unwrap_err());
    assert_invariant_conflict(store.bootstrap_snapshot().await.unwrap_err());
}

#[tokio::test]
async fn partial_nonterminal_tuple_attempt_mismatch_and_round_gap_are_conflicts() {
    let store = support::seeded_store().await;
    let task = running_project3_task(&store).await;
    let (_, review_event_id) = applied_record(
        store
            .record_review(
                task.id,
                task.repository_id,
                task.attempt,
                changes_requested(1, "durable round one"),
            )
            .await
            .unwrap(),
    );
    let plan_event_id = review_event_id.get() - 1;
    sqlx::query("UPDATE tasks SET last_event_id = ? WHERE id = ?")
        .bind(plan_event_id)
        .bind(task.id.to_string())
        .execute(store.pool())
        .await
        .unwrap();
    assert_invariant_conflict(
        store
            .record_review(
                task.id,
                task.repository_id,
                task.attempt,
                changes_requested(1, "durable round one"),
            )
            .await
            .unwrap_err(),
    );

    let clean_store = support::seeded_store().await;
    let clean_task = running_project3_task(&clean_store).await;
    assert_invariant_conflict(
        clean_store
            .record_review(
                clean_task.id,
                coding_agent_domain::RepositoryId::new(),
                clean_task.attempt,
                changes_requested(1, "wrong repository"),
            )
            .await
            .unwrap_err(),
    );
    assert_invariant_conflict(
        clean_store
            .record_review(
                clean_task.id,
                clean_task.repository_id,
                clean_task.attempt + 1,
                changes_requested(1, "wrong attempt"),
            )
            .await
            .unwrap_err(),
    );
    assert_invariant_conflict(
        clean_store
            .record_review(
                clean_task.id,
                clean_task.repository_id,
                clean_task.attempt,
                changes_requested(2, "round one was skipped"),
            )
            .await
            .unwrap_err(),
    );
    assert_eq!(event_count(&clean_store, clean_task.id).await, 3);
}

#[tokio::test]
async fn review_checks_are_append_only_from_the_plan_with_an_exact_added_delta() {
    let store = support::seeded_store().await;
    let task = running_project3_task(&store).await;
    let initial = required_check();
    let added = RequiredCheck::try_cargo_test(
        "review-added-check",
        Some("coding-agent-store".to_owned()),
        Some("review_extra".to_owned()),
    )
    .unwrap();

    assert_invariant_conflict(
        store
            .record_review(
                task.id,
                task.repository_id,
                task.attempt,
                changes_requested_with_checks(
                    1,
                    "plan check was replaced",
                    vec![added.clone()],
                    Vec::new(),
                ),
            )
            .await
            .unwrap_err(),
    );
    assert_eq!(review_count(&store, task.id).await, 0);

    store
        .record_review(
            task.id,
            task.repository_id,
            task.attempt,
            changes_requested_with_checks(
                1,
                "one exact addition",
                vec![initial.clone(), added.clone()],
                vec![added.clone()],
            ),
        )
        .await
        .unwrap();
    assert_invariant_conflict(
        store
            .record_review(
                task.id,
                task.repository_id,
                task.attempt,
                changes_requested_with_checks(
                    2,
                    "the added check disappeared",
                    vec![initial],
                    Vec::new(),
                ),
            )
            .await
            .unwrap_err(),
    );
    assert_eq!(review_count(&store, task.id).await, 1);

    sqlx::query("DROP TRIGGER task_review_evidence_no_update")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE task_review_evidence SET added_checks_json = '[]' \
         WHERE task_id = ? AND review_round = 1",
    )
    .bind(task.id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
    assert_invariant_conflict(store.task_detail(task.id).await.unwrap_err());
}

#[tokio::test]
async fn existing_and_new_reviews_revalidate_the_typed_plan_event() {
    let replaced_store = support::seeded_store().await;
    let replaced_task = running_project3_task(&replaced_store).await;
    let request = changes_requested(1, "stable before plan replacement");
    replaced_store
        .record_review(
            replaced_task.id,
            replaced_task.repository_id,
            replaced_task.attempt,
            request.clone(),
        )
        .await
        .unwrap();
    let replacement_check = RequiredCheck::try_cargo_test(
        "replacement-plan-check",
        Some("coding-agent-store".to_owned()),
        None,
    )
    .unwrap();
    let replacement_plan = PlanSnapshot::try_structured(
        1,
        "A different but valid plan",
        vec![
            PlanItem::try_structured(
                "replacement-step",
                "Replace",
                "Exercise aggregate validation",
                vec!["Replacement check passes".to_owned()],
                PlanItemStatus::Completed,
            )
            .unwrap(),
        ],
        vec![replacement_check],
    )
    .unwrap();
    sqlx::query(
        "UPDATE task_events SET payload_json = ? \
         WHERE task_id = ? AND kind = 'plan.updated'",
    )
    .bind(serde_json::to_string(&serde_json::json!({ "plan": replacement_plan })).unwrap())
    .bind(replaced_task.id.to_string())
    .execute(replaced_store.pool())
    .await
    .unwrap();
    assert_invariant_conflict(
        replaced_store
            .record_review(
                replaced_task.id,
                replaced_task.repository_id,
                replaced_task.attempt,
                request,
            )
            .await
            .unwrap_err(),
    );
    assert_invariant_conflict(
        replaced_store
            .record_review(
                replaced_task.id,
                replaced_task.repository_id,
                replaced_task.attempt,
                changes_requested(2, "must not extend a replaced plan"),
            )
            .await
            .unwrap_err(),
    );

    let schema_store = support::seeded_store().await;
    let schema_task = running_project3_task(&schema_store).await;
    let schema_request = changes_requested(1, "stable before schema corruption");
    schema_store
        .record_review(
            schema_task.id,
            schema_task.repository_id,
            schema_task.attempt,
            schema_request.clone(),
        )
        .await
        .unwrap();
    sqlx::query(
        "UPDATE task_events SET schema_version = 2 \
         WHERE task_id = ? AND kind = 'plan.updated'",
    )
    .bind(schema_task.id.to_string())
    .execute(schema_store.pool())
    .await
    .unwrap();
    assert_invariant_conflict(
        schema_store
            .record_review(
                schema_task.id,
                schema_task.repository_id,
                schema_task.attempt,
                schema_request,
            )
            .await
            .unwrap_err(),
    );
    assert_invariant_conflict(
        schema_store
            .record_review(
                schema_task.id,
                schema_task.repository_id,
                schema_task.attempt,
                changes_requested(2, "must not trust schema version two"),
            )
            .await
            .unwrap_err(),
    );
}

#[tokio::test]
async fn an_approved_review_cannot_be_followed_by_another_round() {
    let store = support::seeded_store().await;
    let task = running_project3_task(&store).await;
    store
        .record_review(
            task.id,
            task.repository_id,
            task.attempt,
            changes_requested(1, "round one before corruption"),
        )
        .await
        .unwrap();
    store
        .record_review(
            task.id,
            task.repository_id,
            task.attempt,
            changes_requested(2, "round two before corruption"),
        )
        .await
        .unwrap();

    let approved = serde_json::to_value(approved(1)).unwrap();
    sqlx::query("DROP TRIGGER task_review_evidence_no_update")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE task_review_evidence SET \
             workspace_generation = ?, digest_algorithm = ?, workspace_digest = ?, \
             decision_source = ?, verdict = ?, summary = ?, findings_json = ?, \
             added_checks_json = ?, required_checks_json = ?, check_evidence_json = ?, \
             coverage_json = ? \
         WHERE task_id = ? AND review_round = 1",
    )
    .bind(approved["workspace_generation"].as_u64().unwrap() as i64)
    .bind(approved["workspace_digest"]["algorithm"].as_str().unwrap())
    .bind(approved["workspace_digest"]["value"].as_str().unwrap())
    .bind(approved["decision_source"].as_str().unwrap())
    .bind(approved["verdict"].as_str().unwrap())
    .bind(approved["summary"].as_str().unwrap())
    .bind(canonical_value_field(&approved, "findings"))
    .bind(canonical_value_field(&approved, "added_required_checks"))
    .bind(canonical_value_field(&approved, "required_checks"))
    .bind(canonical_value_field(&approved, "check_evidence"))
    .bind(canonical_value_field(&approved, "coverage"))
    .bind(task.id.to_string())
    .execute(store.pool())
    .await
    .unwrap();

    assert_invariant_conflict(store.task_detail(task.id).await.unwrap_err());
    assert_invariant_conflict(store.bootstrap_snapshot().await.unwrap_err());
    assert_invariant_conflict(
        store
            .finalize_reviewed_task(
                task.id,
                task.repository_id,
                task.attempt,
                changes_requested(3, "must not continue after approval"),
            )
            .await
            .unwrap_err(),
    );
}

#[tokio::test]
async fn existing_review_rejects_timestamp_and_forward_or_cross_task_cursor_corruption() {
    let timestamp_store = support::seeded_store().await;
    let timestamp_task = running_project3_task(&timestamp_store).await;
    let request = changes_requested(1, "stable replay");
    let (_, review_event_id) = applied_record(
        timestamp_store
            .record_review(
                timestamp_task.id,
                timestamp_task.repository_id,
                timestamp_task.attempt,
                request.clone(),
            )
            .await
            .unwrap(),
    );
    sqlx::query("DROP TRIGGER task_events_review_no_update")
        .execute(timestamp_store.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task_events SET created_at = ? WHERE id = ?")
        .bind("2026-07-23T23:59:59.000000000Z")
        .bind(review_event_id.get())
        .execute(timestamp_store.pool())
        .await
        .unwrap();
    assert_invariant_conflict(
        timestamp_store
            .record_review(
                timestamp_task.id,
                timestamp_task.repository_id,
                timestamp_task.attempt,
                request,
            )
            .await
            .unwrap_err(),
    );

    let cursor_store = support::seeded_store().await;
    let cursor_task = running_project3_task(&cursor_store).await;
    let cursor_request = changes_requested(1, "stable cursor replay");
    cursor_store
        .record_review(
            cursor_task.id,
            cursor_task.repository_id,
            cursor_task.attempt,
            cursor_request.clone(),
        )
        .await
        .unwrap();
    let unrelated = support::queued_task(&cursor_store).await;
    sqlx::query("UPDATE tasks SET last_event_id = ? WHERE id = ?")
        .bind(unrelated.last_event_id.get())
        .bind(cursor_task.id.to_string())
        .execute(cursor_store.pool())
        .await
        .unwrap();
    assert_invariant_conflict(
        cursor_store
            .record_review(
                cursor_task.id,
                cursor_task.repository_id,
                cursor_task.attempt,
                cursor_request,
            )
            .await
            .unwrap_err(),
    );
    assert_invariant_conflict(
        cursor_store
            .record_review(
                cursor_task.id,
                cursor_task.repository_id,
                cursor_task.attempt,
                changes_requested(2, "must not heal a corrupt cursor"),
            )
            .await
            .unwrap_err(),
    );
    assert_invariant_conflict(cursor_store.task_detail(cursor_task.id).await.unwrap_err());
}

#[tokio::test]
async fn generic_running_to_completed_is_not_a_production_bypass() {
    let store = support::seeded_store().await;
    let task = running_project3_task(&store).await;
    let before_events = event_count(&store, task.id).await;

    let error = store
        .transition_with_event(task.id, TaskStatus::Running, TaskTransition::Completed)
        .await
        .expect_err("generic transition must not create Completed + Unreviewed");
    assert!(matches!(
        error,
        StoreError::InvariantViolation(_) | StoreError::IllegalTransition { .. }
    ));
    sqlx::query(
        "UPDATE tasks SET status = 'completed', finished_at = ? \
         WHERE id = ? AND status = 'running'",
    )
    .bind(
        UtcTimestamp::parse_rfc3339("2026-07-23T12:00:00Z")
            .unwrap()
            .to_string(),
    )
    .bind(task.id.to_string())
    .execute(store.pool())
    .await
    .expect_err("SQLite must reject an unreviewed completion bypass");

    let after = store.task_detail(task.id).await.unwrap().unwrap();
    assert_eq!(after.task.status, TaskStatus::Running);
    assert_eq!(after.task.delivery_readiness, DeliveryReadiness::Unreviewed);
    assert_eq!(event_count(&store, task.id).await, before_events);
}

#[tokio::test]
async fn every_finalization_sql_step_rolls_back_the_whole_quality_tuple() {
    let failpoints = [
        ("review_event", "task_events", "NEW.kind = 'review.updated'"),
        ("evidence", "task_review_evidence", "1"),
        (
            "terminal_task",
            "tasks",
            "NEW.status IN ('completed', 'failed') AND OLD.status = 'running'",
        ),
        ("delivery", "task_delivery_state", "1"),
        (
            "lifecycle_event",
            "task_events",
            "NEW.kind IN ('task.completed', 'task.failed')",
        ),
    ];

    for (name, table, condition) in failpoints {
        let store = support::seeded_store().await;
        let task = running_project3_task(&store).await;
        let before = atomic_state(&store, task.id).await;
        install_insert_or_update_fault(&store, name, table, condition).await;

        store
            .finalize_reviewed_task(task.id, task.repository_id, task.attempt, approved(1))
            .await
            .expect_err(name);
        assert_eq!(atomic_state(&store, task.id).await, before, "{name}");
    }

    for (name, sql) in [
        (
            "last_event",
            "CREATE TRIGGER injected_review_fault \
             BEFORE UPDATE OF last_event_id ON tasks \
             WHEN NEW.status IN ('completed','failed') \
             BEGIN SELECT RAISE(ABORT, 'injected last_event fault'); END",
        ),
        (
            "terminal_payload",
            "CREATE TRIGGER injected_review_fault \
             BEFORE UPDATE OF payload_json ON task_events \
             WHEN NEW.kind IN ('task.completed','task.failed') \
             BEGIN SELECT RAISE(ABORT, 'injected terminal payload fault'); END",
        ),
    ] {
        let store = support::seeded_store().await;
        let task = running_project3_task(&store).await;
        let before = atomic_state(&store, task.id).await;
        sqlx::query(sql).execute(store.pool()).await.unwrap();

        store
            .finalize_reviewed_task(task.id, task.repository_id, task.attempt, approved(1))
            .await
            .expect_err(name);
        assert_eq!(atomic_state(&store, task.id).await, before, "{name}");
    }
}

#[tokio::test]
async fn every_nonterminal_review_sql_step_rolls_back_event_evidence_and_cursor() {
    for (name, trigger_sql) in [
        (
            "record_review_event",
            "CREATE TRIGGER injected_review_fault \
             BEFORE INSERT ON task_events WHEN NEW.kind = 'review.updated' \
             BEGIN SELECT RAISE(ABORT, 'injected review event fault'); END",
        ),
        (
            "record_evidence",
            "CREATE TRIGGER injected_review_fault \
             BEFORE INSERT ON task_review_evidence \
             BEGIN SELECT RAISE(ABORT, 'injected evidence fault'); END",
        ),
        (
            "record_last_event",
            "CREATE TRIGGER injected_review_fault \
             BEFORE UPDATE OF last_event_id ON tasks \
             WHEN NEW.status = 'running' AND NEW.last_event_id != OLD.last_event_id \
             BEGIN SELECT RAISE(ABORT, 'injected last event fault'); END",
        ),
    ] {
        let store = support::seeded_store().await;
        let task = running_project3_task(&store).await;
        let before = atomic_state(&store, task.id).await;
        sqlx::query(trigger_sql)
            .execute(store.pool())
            .await
            .unwrap();

        store
            .record_review(
                task.id,
                task.repository_id,
                task.attempt,
                changes_requested(1, "must roll back"),
            )
            .await
            .expect_err(name);
        assert_eq!(atomic_state(&store, task.id).await, before, "{name}");
    }
}

#[tokio::test]
async fn recovery_preserves_intermediate_reviews_and_retry_starts_unreviewed() {
    let store = support::seeded_store().await;
    let task = running_project3_task(&store).await;
    store
        .record_review(
            task.id,
            task.repository_id,
            task.attempt,
            changes_requested(1, "preserve this review"),
        )
        .await
        .unwrap();

    store
        .recover_incomplete(
            UtcTimestamp::parse_rfc3339("2026-07-23T12:00:00Z").unwrap(),
            support::failure("RECOVERED_AFTER_RESTART"),
        )
        .await
        .unwrap();
    let interrupted = store.task_detail(task.id).await.unwrap().unwrap();
    assert_eq!(interrupted.task.status, TaskStatus::Interrupted);
    assert_eq!(
        interrupted.task.delivery_readiness,
        DeliveryReadiness::Unreviewed
    );
    assert_eq!(interrupted.reviews.len(), 1);

    let retry = store.retry_task(task.id).await.unwrap();
    let retry_task = retry.task().clone();
    assert!(matches!(retry, RetryTaskOutcome::Created { .. }));
    let retry_detail = store.task_detail(retry_task.id).await.unwrap().unwrap();
    assert!(retry_detail.reviews.is_empty());
    assert_eq!(
        retry_detail.task.delivery_readiness,
        DeliveryReadiness::Unreviewed
    );
}

#[tokio::test]
async fn recovery_does_not_rewrite_an_already_committed_final_tuple() {
    let store = support::seeded_store().await;
    let task = running_project3_task(&store).await;
    let final_task = applied_final(
        store
            .finalize_reviewed_task(task.id, task.repository_id, task.attempt, approved(1))
            .await
            .unwrap(),
    )
    .0;
    let before_events = event_count(&store, task.id).await;

    let recovery = store
        .recover_incomplete(
            UtcTimestamp::parse_rfc3339("2026-07-23T12:00:00Z").unwrap(),
            support::failure("RECOVERED_AFTER_RESTART"),
        )
        .await
        .unwrap();
    assert_eq!(recovery.interrupted_count, 0);

    let after = store.task_detail(task.id).await.unwrap().unwrap();
    assert_eq!(after.task, final_task);
    assert_eq!(after.reviews.len(), 1);
    assert_eq!(event_count(&store, task.id).await, before_events);
}

#[tokio::test]
async fn near_128k_evidence_stays_under_the_192k_review_event_limit() {
    let store = support::seeded_store().await;
    let task = running_project3_task(&store).await;
    let evidence = large_changes_requested();
    let encoded_new = serde_json::to_vec(&evidence).unwrap();
    assert!(
        encoded_new.len() > 120 * 1024 && encoded_new.len() <= 126 * 1024,
        "fixture must exercise the upper evidence boundary: {} bytes",
        encoded_new.len()
    );

    let (_, event_id) = applied_record(
        store
            .record_review(task.id, task.repository_id, task.attempt, evidence)
            .await
            .unwrap(),
    );
    let event = store
        .task_events_after(task.id, EventCursor::new(event_id.get() - 1).unwrap(), 1)
        .await
        .unwrap()
        .events
        .pop()
        .unwrap();
    let encoded_event = serde_json::to_vec(&event).unwrap();
    assert!(encoded_event.len() <= 192 * 1024);

    let canonical_row_bytes: i64 = sqlx::query_scalar(
        "SELECT length(CAST(json_object(\
             'round', review_round,\
             'decision_source', decision_source,\
             'workspace_generation', workspace_generation,\
             'workspace_digest', json_object('algorithm', digest_algorithm, 'value', workspace_digest),\
             'verdict', verdict,\
             'summary', summary,\
             'findings', json(findings_json),\
             'added_required_checks', json(added_checks_json),\
             'required_checks', json(required_checks_json),\
             'check_evidence', json(check_evidence_json),\
             'coverage', json(coverage_json),\
             'created_at', created_at\
         ) AS BLOB)) FROM task_review_evidence WHERE event_id = ?",
    )
    .bind(event_id.get())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert!(canonical_row_bytes <= 128 * 1024);
}

#[test]
fn oversize_evidence_and_wire_event_are_rejected_before_persistence() {
    let round = 1;
    let digest = digest(round);
    let check = required_check();
    let findings = (1..=32)
        .map(|ordinal| {
            ReviewFinding::try_for_review(
                round,
                ordinal,
                if ordinal == 1 {
                    FindingSeverity::Blocking
                } else {
                    FindingSeverity::Advisory
                },
                "\u{0001}".repeat(2_048),
                None,
                None,
            )
            .unwrap()
        })
        .collect();
    assert!(
        NewReviewEvidence::try_new(
            round,
            ReviewDecisionSource::Reviewer,
            u64::from(round),
            digest.clone(),
            ReviewVerdict::ChangesRequested,
            "oversize evidence",
            findings,
            Vec::new(),
            vec![check.clone()],
            vec![passed_check(round, &check, &digest)],
            None,
        )
        .is_err()
    );

    let review = ReviewEvidence::try_from_new(
        changes_requested(1, "valid before wire corruption"),
        UtcTimestamp::parse_rfc3339("2026-07-23T12:34:56Z").unwrap(),
    )
    .unwrap();
    let event = TaskEvent::new(
        EventId::new(1).unwrap(),
        TaskId::new(),
        TaskEventPayload::ReviewUpdated { review },
        UtcTimestamp::parse_rfc3339("2026-07-23T12:34:56Z").unwrap(),
    );
    let mut wire = serde_json::to_value(event).unwrap();
    wire["payload"]["review"]["summary"] = serde_json::Value::String("x".repeat(192 * 1024));
    assert!(serde_json::to_vec(&wire).unwrap().len() > 192 * 1024);
    assert!(serde_json::from_value::<TaskEvent>(wire).is_err());
}

#[tokio::test]
async fn noncanonical_typed_row_and_non_marker_event_fail_closed_on_read() {
    let store = support::seeded_store().await;
    let task = running_project3_task(&store).await;
    let (_, event_id) = applied_record(
        store
            .record_review(
                task.id,
                task.repository_id,
                task.attempt,
                changes_requested(1, "canonical row"),
            )
            .await
            .unwrap(),
    );

    sqlx::query("DROP TRIGGER task_review_evidence_no_update")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE task_review_evidence SET findings_json = ' [ ' || \
         substr(findings_json, 2, length(findings_json) - 2) || ' ] ' \
         WHERE event_id = ?",
    )
    .bind(event_id.get())
    .execute(store.pool())
    .await
    .unwrap();

    assert_invariant_conflict(
        store
            .task_events_after(task.id, EventCursor::ZERO, 100)
            .await
            .unwrap_err(),
    );
    assert_invariant_conflict(store.task_detail(task.id).await.unwrap_err());

    let marker_store = support::seeded_store().await;
    let marker_task = running_project3_task(&marker_store).await;
    let (_, marker_event_id) = applied_record(
        marker_store
            .record_review(
                marker_task.id,
                marker_task.repository_id,
                marker_task.attempt,
                changes_requested(1, "canonical marker"),
            )
            .await
            .unwrap(),
    );
    sqlx::query("DROP TRIGGER task_events_review_marker_on_update")
        .execute(marker_store.pool())
        .await
        .unwrap();
    sqlx::query("DROP TRIGGER task_events_review_no_update")
        .execute(marker_store.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task_events SET payload_json = ? WHERE id = ?")
        .bind(format!(
            r#"{{"evidence_ref":true,"padding":"{}"}}"#,
            "x".repeat(192 * 1024)
        ))
        .bind(marker_event_id.get())
        .execute(marker_store.pool())
        .await
        .unwrap();
    assert_invariant_conflict(
        marker_store
            .task_events_after(marker_task.id, EventCursor::ZERO, 100)
            .await
            .unwrap_err(),
    );
}

async fn running_project3_task(store: &Store) -> Task {
    let queued = support::queued_task(store).await;
    let task = match store
        .transition_with_event(queued.id, TaskStatus::Queued, TaskTransition::Running)
        .await
        .unwrap()
    {
        TransitionOutcome::Applied { task, .. } => task,
        TransitionOutcome::Conflict { .. } => panic!("fixture transition must apply"),
    };
    let plan = PlanSnapshot::try_structured(
        1,
        "Implement and review the approved plan",
        vec![
            PlanItem::try_structured(
                "step-1",
                "Implement",
                "Implement the requested behavior",
                vec!["All required checks pass".to_owned()],
                PlanItemStatus::Completed,
            )
            .unwrap(),
        ],
        vec![required_check()],
    )
    .unwrap();
    store
        .append_running_event(task.id, TaskEventPayload::PlanUpdated { plan })
        .await
        .unwrap();
    store.task_detail(task.id).await.unwrap().unwrap().task
}

async fn seed_prior_rounds(store: &Store, task: &Task, final_round: u8) {
    for round in 1..final_round {
        store
            .record_review(
                task.id,
                task.repository_id,
                task.attempt,
                changes_requested(round, format!("round {round} blocker")),
            )
            .await
            .unwrap();
    }
}

fn required_check() -> RequiredCheck {
    RequiredCheck::try_cargo_test(
        "project3-cargo-test",
        Some("coding-agent-store".to_owned()),
        None,
    )
    .unwrap()
}

fn canonical_value_field(value: &serde_json::Value, field: &str) -> String {
    serde_json::to_string(
        value
            .get(field)
            .unwrap_or_else(|| panic!("missing canonical review field {field}")),
    )
    .unwrap()
}

fn digest(round: u8) -> WorkspaceDigest {
    let digit = char::from(b'a' + round - 1);
    WorkspaceDigest::try_new(digit.to_string().repeat(64)).unwrap()
}

fn passed_check(round: u8, check: &RequiredCheck, digest: &WorkspaceDigest) -> CheckEvidence {
    CheckEvidence::try_for_check(
        check,
        CheckActor::Executor,
        u32::from(round),
        u64::from(round),
        digest.clone(),
        CheckEvidenceStatus::Passed,
        10,
        "cargo test passed",
        false,
    )
    .unwrap()
}

fn changes_requested(round: u8, summary: impl Into<String>) -> NewReviewEvidence {
    changes_requested_with_checks(round, summary, vec![required_check()], Vec::new())
}

fn changes_requested_with_checks(
    round: u8,
    summary: impl Into<String>,
    required_checks: Vec<RequiredCheck>,
    added_required_checks: Vec<RequiredCheck>,
) -> NewReviewEvidence {
    let digest = digest(round);
    let check_evidence = required_checks
        .iter()
        .map(|check| passed_check(round, check, &digest))
        .collect();
    NewReviewEvidence::try_new(
        round,
        ReviewDecisionSource::Reviewer,
        u64::from(round),
        digest.clone(),
        ReviewVerdict::ChangesRequested,
        summary,
        vec![
            ReviewFinding::try_for_review(
                round,
                1,
                FindingSeverity::Blocking,
                "A blocking issue remains",
                Some("src/lib.rs".to_owned()),
                Some(1),
            )
            .unwrap(),
        ],
        added_required_checks,
        required_checks,
        check_evidence,
        None,
    )
    .unwrap()
}

fn approved(round: u8) -> NewReviewEvidence {
    approved_with_summary(round, format!("round {round} approved"))
}

fn approved_with_summary(round: u8, summary: impl Into<String>) -> NewReviewEvidence {
    let digest = digest(round);
    let check = required_check();
    NewReviewEvidence::try_new(
        round,
        ReviewDecisionSource::Reviewer,
        u64::from(round),
        digest.clone(),
        ReviewVerdict::Approved,
        summary,
        Vec::new(),
        Vec::new(),
        vec![check.clone()],
        vec![passed_check(round, &check, &digest)],
        Some(
            ReviewCoverageEvidence::try_new(u64::from(round), digest, "f".repeat(64), vec![0], 1)
                .unwrap(),
        ),
    )
    .unwrap()
}

fn large_changes_requested() -> NewReviewEvidence {
    let round = 1;
    let digest = digest(round);
    let check = required_check();
    let mut message_scalars = 700;
    loop {
        let findings = (1..=32)
            .map(|ordinal| {
                ReviewFinding::try_for_review(
                    round,
                    ordinal,
                    if ordinal == 1 {
                        FindingSeverity::Blocking
                    } else {
                        FindingSeverity::Advisory
                    },
                    "\u{0001}".repeat(message_scalars),
                    None,
                    None,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        if let Ok(candidate) = NewReviewEvidence::try_new(
            round,
            ReviewDecisionSource::Reviewer,
            u64::from(round),
            digest.clone(),
            ReviewVerdict::ChangesRequested,
            "large escaped evidence",
            findings,
            Vec::new(),
            vec![check.clone()],
            vec![passed_check(round, &check, &digest)],
            None,
        ) {
            let encoded_len = serde_json::to_vec(&candidate).unwrap().len();
            if encoded_len > 120 * 1024 && encoded_len <= 126 * 1024 {
                return candidate;
            }
        }
        message_scalars -= 1;
        assert!(message_scalars > 500, "could not build near-limit fixture");
    }
}

fn applied_record(outcome: RecordReviewOutcome) -> (ReviewEvidence, coding_agent_domain::EventId) {
    match outcome {
        RecordReviewOutcome::Applied { review, event_id } => (review, event_id),
        RecordReviewOutcome::Existing { .. } => panic!("first review write must apply"),
    }
}

fn applied_final(
    outcome: FinalizeReviewedTaskOutcome,
) -> (
    Task,
    ReviewEvidence,
    coding_agent_domain::EventId,
    coding_agent_domain::EventId,
) {
    match outcome {
        FinalizeReviewedTaskOutcome::Applied {
            task,
            review,
            review_event_id,
            terminal_event_id,
        } => (task, review, review_event_id, terminal_event_id),
        FinalizeReviewedTaskOutcome::Existing { .. } => {
            panic!("first final review write must apply")
        }
    }
}

async fn review_event_payload(store: &Store, event_id: i64) -> String {
    sqlx::query_scalar("SELECT payload_json FROM task_events WHERE id = ?")
        .bind(event_id)
        .fetch_one(store.pool())
        .await
        .unwrap()
}

async fn event_count(store: &Store, task_id: coding_agent_domain::TaskId) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM task_events WHERE task_id = ?")
        .bind(task_id.to_string())
        .fetch_one(store.pool())
        .await
        .unwrap()
}

async fn delivery_count(store: &Store, task_id: coding_agent_domain::TaskId) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM task_delivery_state WHERE task_id = ?")
        .bind(task_id.to_string())
        .fetch_one(store.pool())
        .await
        .unwrap()
}

async fn review_count(store: &Store, task_id: coding_agent_domain::TaskId) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM task_review_evidence WHERE task_id = ?")
        .bind(task_id.to_string())
        .fetch_one(store.pool())
        .await
        .unwrap()
}

async fn assert_terminal_event_order(
    store: &Store,
    task_id: coding_agent_domain::TaskId,
    review_event_id: i64,
    terminal_event_id: i64,
    terminal_kind: TaskEventKind,
) {
    let events = store
        .task_events_after(task_id, EventCursor::new(review_event_id - 1).unwrap(), 2)
        .await
        .unwrap()
        .events;
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].id.get(), review_event_id);
    assert_eq!(events[0].payload.kind(), TaskEventKind::ReviewUpdated);
    assert_eq!(events[1].id.get(), terminal_event_id);
    assert_eq!(events[1].payload.kind(), terminal_kind);
}

async fn assert_final_rows_share_one_timestamp(
    store: &Store,
    task_id: coding_agent_domain::TaskId,
    round: u8,
    review_event_id: i64,
    terminal_event_id: i64,
) {
    let timestamps: (String, String, String, String, String) = sqlx::query_as(
        "SELECT r.created_at, d.decided_at, t.finished_at, re.created_at, te.created_at \
         FROM task_review_evidence r \
         JOIN task_delivery_state d ON d.task_id = r.task_id \
         JOIN tasks t ON t.id = r.task_id \
         JOIN task_events re ON re.id = r.event_id \
         JOIN task_events te ON te.id = t.last_event_id \
         WHERE r.task_id = ? AND r.review_round = ? AND re.id = ? AND te.id = ?",
    )
    .bind(task_id.to_string())
    .bind(i64::from(round))
    .bind(review_event_id)
    .bind(terminal_event_id)
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(timestamps.0, timestamps.1);
    assert_eq!(timestamps.0, timestamps.2);
    assert_eq!(timestamps.0, timestamps.3);
    assert_eq!(timestamps.0, timestamps.4);
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AtomicState {
    task_status: String,
    readiness_rows: i64,
    review_rows: i64,
    review_events: i64,
    terminal_events: i64,
    total_events: i64,
    last_event_id: i64,
}

async fn atomic_state(store: &Store, task_id: coding_agent_domain::TaskId) -> AtomicState {
    let (task_status, last_event_id): (String, i64) =
        sqlx::query_as("SELECT status, last_event_id FROM tasks WHERE id = ?")
            .bind(task_id.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap();
    AtomicState {
        task_status,
        readiness_rows: delivery_count(store, task_id).await,
        review_rows: sqlx::query_scalar(
            "SELECT COUNT(*) FROM task_review_evidence WHERE task_id = ?",
        )
        .bind(task_id.to_string())
        .fetch_one(store.pool())
        .await
        .unwrap(),
        review_events: sqlx::query_scalar(
            "SELECT COUNT(*) FROM task_events WHERE task_id = ? AND kind = 'review.updated'",
        )
        .bind(task_id.to_string())
        .fetch_one(store.pool())
        .await
        .unwrap(),
        terminal_events: sqlx::query_scalar(
            "SELECT COUNT(*) FROM task_events \
             WHERE task_id = ? AND kind IN ('task.completed','task.failed')",
        )
        .bind(task_id.to_string())
        .fetch_one(store.pool())
        .await
        .unwrap(),
        total_events: event_count(store, task_id).await,
        last_event_id,
    }
}

async fn install_insert_or_update_fault(store: &Store, name: &str, table: &str, condition: &str) {
    let operation = if table == "tasks" { "UPDATE" } else { "INSERT" };
    let sql = format!(
        "CREATE TRIGGER injected_review_fault BEFORE {operation} ON {table} \
         WHEN {condition} BEGIN SELECT RAISE(ABORT, 'injected {name} fault'); END"
    );
    sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
        .execute(store.pool())
        .await
        .unwrap();
}

fn assert_invariant_conflict(error: StoreError) {
    assert!(
        matches!(error, StoreError::InvariantViolation(_)),
        "expected invariant conflict, got {error:?}"
    );
}
