#[cfg(feature = "test-support")]
fn staged_review_evidence() -> NewReviewEvidence {
    let generation = 1;
    let digest =
        WorkspaceDigest::try_new("a".repeat(64)).expect("construct staged-review workspace digest");
    let check = crate::fake_runner::fake_plan()
        .initial_required_checks()
        .first()
        .cloned()
        .expect("the fake plan has one required check");
    let evidence = CheckEvidence::try_for_check(
        &check,
        CheckActor::Executor,
        1,
        generation,
        digest.clone(),
        CheckEvidenceStatus::Passed,
        200,
        "staged-review executor check passed",
        false,
    )
    .expect("construct staged-review check evidence");
    let finding = ReviewFinding::try_for_review(
        1,
        1,
        FindingSeverity::Blocking,
        "staged-review fixture requests one bounded correction",
        Some("synthetic/example.rs".to_owned()),
        Some(1),
    )
    .expect("construct staged-review blocking finding");
    NewReviewEvidence::try_new(
        1,
        ReviewDecisionSource::Reviewer,
        generation,
        digest,
        ReviewVerdict::ChangesRequested,
        "staged-review fixture requested changes",
        vec![finding],
        Vec::new(),
        vec![check],
        vec![evidence],
        None,
    )
    .expect("construct staged-review evidence")
}

#[cfg(feature = "test-support")]
async fn wait_for_single_pending_stop_intent(manager: &TaskManagerHandle) -> PendingDurableResult {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let pending = manager
                .pending_durable_results_for_test()
                .await
                .expect("inspect actor-owned pending durable results");
            if let [pending @ PendingDurableResult::PersistStopIntentBatch { .. }] =
                pending.as_slice()
            {
                return pending.clone();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the exact stop-intent pending result remains actor-owned")
}

#[cfg(feature = "test-support")]
async fn wait_for_single_pending_final_stop(manager: &TaskManagerHandle) -> PendingDurableResult {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let pending = manager
                .pending_durable_results_for_test()
                .await
                .expect("inspect actor-owned pending durable results");
            if let [pending @ PendingDurableResult::FinalizeStoppedTask { .. }] = pending.as_slice()
            {
                return pending.clone();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the exact final-stop pending result remains actor-owned")
}

#[cfg(feature = "test-support")]
async fn wait_for_single_pending_record_review(
    manager: &TaskManagerHandle,
) -> PendingDurableResult {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let pending = manager
                .pending_durable_results_for_test()
                .await
                .expect("inspect actor-owned pending durable results");
            if let [pending @ PendingDurableResult::RecordReview { .. }] = pending.as_slice() {
                return pending.clone();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the exact RecordReview pending result remains actor-owned")
}

#[cfg(feature = "test-support")]
async fn committed_stop_intent(store: &Store, task_id: TaskId) -> StopIntentReceipt {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = store
                .scheduler_bootstrap_snapshot()
                .await
                .expect("load stop-intent bootstrap snapshot");
            if let Some(receipt) = snapshot
                .running_stop_intents
                .into_iter()
                .find(|receipt| receipt.task_id == task_id)
            {
                return receipt;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the stop intent commits before its replay reply is released")
}

#[cfg(feature = "test-support")]
fn exact_late_stop_completion(
    pending: &PendingDurableResult,
    receipt: StopIntentReceipt,
) -> (
    DurableOperationIdentity,
    DurableCompletion<StopIntentBatchReceipt>,
) {
    let PendingDurableResult::PersistStopIntentBatch { identity, requests } = pending else {
        panic!("expected a persisted stop-intent batch");
    };
    assert_eq!(requests.len(), 1, "late-receipt fixture uses one task");
    assert!(stop_receipt_matches_request(receipt, requests[0]));
    let batch = StopIntentBatchReceipt {
        items: vec![coding_agent_store::StopIntentBatchItem {
            request: requests[0],
            outcome: PersistStopIntentOutcome::Existing(receipt),
        }],
    };
    (
        identity.clone(),
        DurableCompletion {
            identity: identity.clone(),
            sequence_disposition: MutationSequenceDisposition::AdvanceNext,
            disposition: DurableDisposition::Confirmed(batch),
        },
    )
}

#[cfg(feature = "test-support")]
#[derive(Debug, Clone, Copy)]
enum OriginalFirstReplayFollowup {
    Unknown,
    Exact,
}
