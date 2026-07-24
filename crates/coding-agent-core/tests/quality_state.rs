use coding_agent_core::{
    CheckpointChange, QualityStateError, RequiredCheckLedger, WorkspaceCheckpoint,
    WorkspaceFingerprint, project_test_snapshot, project_unverified_test_snapshot,
};
use coding_agent_domain::{
    CheckActor, CheckEvidence, CheckEvidenceStatus, FindingSeverity, MAX_WORKSPACE_GENERATION,
    NewReviewEvidence, RequiredCheck, ReviewCoverageEvidence, ReviewDecisionSource, ReviewFinding,
    ReviewVerdict, TestStatus,
};

fn fingerprint(byte: u8) -> WorkspaceFingerprint {
    WorkspaceFingerprint::from_bytes([byte; 32])
}

fn cargo_test(id: &str, package: Option<&str>, integration_test: Option<&str>) -> RequiredCheck {
    RequiredCheck::try_cargo_test(
        id,
        package.map(str::to_owned),
        integration_test.map(str::to_owned),
    )
    .unwrap()
}

fn cargo_check(id: &str, package: Option<&str>) -> RequiredCheck {
    RequiredCheck::try_cargo_check(id, package.map(str::to_owned)).unwrap()
}

fn run_to_terminal(
    ledger: &mut RequiredCheckLedger,
    checkpoint: &mut WorkspaceCheckpoint,
    check: &RequiredCheck,
    status: CheckEvidenceStatus,
    duration_ms: u64,
    summary: &str,
) {
    ledger.queue_check(checkpoint, check.id()).unwrap();
    let token = ledger
        .mark_check_running(checkpoint, check.id(), CheckActor::Executor, 1)
        .unwrap();
    ledger
        .finish_check(checkpoint, token, status, duration_ms, summary, false)
        .unwrap();
}

fn changes_requested_review(
    checkpoint: &WorkspaceCheckpoint,
    required_checks: Vec<RequiredCheck>,
    check_evidence: Vec<CheckEvidence>,
) -> NewReviewEvidence {
    NewReviewEvidence::try_new(
        1,
        ReviewDecisionSource::Reviewer,
        checkpoint.generation(),
        checkpoint.workspace_digest(),
        ReviewVerdict::ChangesRequested,
        "changes required",
        vec![
            ReviewFinding::try_for_review(
                1,
                1,
                FindingSeverity::Blocking,
                "fix the failure",
                None,
                None,
            )
            .unwrap(),
        ],
        Vec::new(),
        required_checks,
        check_evidence,
        None,
    )
    .unwrap()
}

fn approved_review(
    checkpoint: &WorkspaceCheckpoint,
    required_checks: Vec<RequiredCheck>,
    check_evidence: Vec<CheckEvidence>,
) -> NewReviewEvidence {
    let digest = checkpoint.workspace_digest();
    let coverage = ReviewCoverageEvidence::try_new(
        checkpoint.generation(),
        digest.clone(),
        "a".repeat(64),
        Vec::new(),
        0,
    )
    .unwrap();
    NewReviewEvidence::try_new(
        1,
        ReviewDecisionSource::Reviewer,
        checkpoint.generation(),
        digest,
        ReviewVerdict::Approved,
        "approved",
        Vec::new(),
        Vec::new(),
        required_checks,
        check_evidence,
        Some(coverage),
    )
    .unwrap()
}

#[test]
fn checkpoint_starts_at_zero_tracks_stable_changes_and_never_reuses_old_evidence() {
    let check = cargo_test("check-1", None, None);
    let mut ledger = RequiredCheckLedger::try_new(vec![check.clone()]).unwrap();
    let mut checkpoint = WorkspaceCheckpoint::new(fingerprint(0xaa));

    assert_eq!(checkpoint.generation(), 0);
    assert_eq!(
        checkpoint.workspace_digest().algorithm(),
        "workspace_fingerprint_v1"
    );
    assert_eq!(
        checkpoint.workspace_digest().value(),
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    );
    assert_eq!(
        checkpoint.observe_stable(fingerprint(0xaa)).unwrap(),
        CheckpointChange::Unchanged
    );

    run_to_terminal(
        &mut ledger,
        &mut checkpoint,
        &check,
        CheckEvidenceStatus::Passed,
        12,
        "passed on A",
    );
    assert!(ledger.all_current_checks_passed(&checkpoint));
    assert_eq!(checkpoint.current_observations().count(), 1);

    assert_eq!(
        checkpoint.observe_stable(fingerprint(0xbb)).unwrap(),
        CheckpointChange::Advanced { generation: 1 }
    );
    assert_eq!(checkpoint.current_observations().count(), 0);
    assert!(!ledger.all_current_checks_passed(&checkpoint));

    assert_eq!(
        checkpoint.observe_stable(fingerprint(0xaa)).unwrap(),
        CheckpointChange::Advanced { generation: 2 }
    );
    assert_eq!(checkpoint.generation(), 2);
    assert_eq!(checkpoint.current_observations().count(), 0);
}

#[test]
fn checkpoint_generation_is_bounded_by_the_javascript_safe_integer() {
    let mut checkpoint =
        WorkspaceCheckpoint::try_at_generation(MAX_WORKSPACE_GENERATION, fingerprint(0xaa))
            .unwrap();

    assert_eq!(
        checkpoint.observe_stable(fingerprint(0xbb)),
        Err(QualityStateError::WorkspaceGenerationOverflow)
    );
    assert_eq!(checkpoint.generation(), MAX_WORKSPACE_GENERATION);
    assert_eq!(checkpoint.fingerprint(), fingerprint(0xaa));

    assert_eq!(
        WorkspaceCheckpoint::try_at_generation(MAX_WORKSPACE_GENERATION + 1, fingerprint(0xaa)),
        Err(QualityStateError::InvalidWorkspaceGeneration)
    );
}

#[test]
fn required_checks_are_append_only_deduplicated_and_bounded() {
    assert_eq!(
        RequiredCheckLedger::try_new(vec![cargo_check("check-1", None)]),
        Err(QualityStateError::CargoTestRequired)
    );

    let initial = cargo_test("check-1", None, None);
    let duplicate_selector = cargo_test("another-id", None, None);
    assert_eq!(
        RequiredCheckLedger::try_new(vec![initial.clone(), duplicate_selector]),
        Err(QualityStateError::DuplicateCheckSelector)
    );

    let mut ledger = RequiredCheckLedger::try_new(vec![initial.clone()]).unwrap();
    assert_eq!(
        ledger
            .append_checks(vec![cargo_test("ignored-id", None, None)])
            .unwrap(),
        0
    );
    assert_eq!(ledger.checks(), &[initial]);

    let additions = (2..=16)
        .map(|ordinal| {
            cargo_check(
                &format!("check-{ordinal}"),
                Some(&format!("package-{ordinal}")),
            )
        })
        .collect();
    assert_eq!(ledger.append_checks(additions).unwrap(), 15);
    assert_eq!(ledger.checks().len(), 16);

    let before = ledger.checks().to_vec();
    assert_eq!(
        ledger.append_checks(vec![cargo_check("check-17", Some("package-17"))]),
        Err(QualityStateError::TooManyRequiredChecks)
    );
    assert_eq!(ledger.checks(), before);
}

#[test]
fn append_is_atomic_when_an_id_is_reused_for_a_different_selector() {
    let initial = cargo_test("check-1", None, None);
    let mut ledger = RequiredCheckLedger::try_new(vec![initial.clone()]).unwrap();

    assert_eq!(
        ledger.append_checks(vec![
            cargo_check("check-2", Some("alpha")),
            cargo_check("check-1", Some("beta")),
        ]),
        Err(QualityStateError::DuplicateCheckId)
    );
    assert_eq!(ledger.checks(), &[initial]);
}

#[test]
fn rerun_revokes_old_pass_before_running_and_latest_terminal_result_wins() {
    let check = cargo_test("check-1", Some("core"), Some("quality_state"));
    let mut ledger = RequiredCheckLedger::try_new(vec![check.clone()]).unwrap();
    let mut checkpoint = WorkspaceCheckpoint::new(fingerprint(0xaa));

    run_to_terminal(
        &mut ledger,
        &mut checkpoint,
        &check,
        CheckEvidenceStatus::Passed,
        10,
        "first pass",
    );
    assert!(ledger.all_current_checks_passed(&checkpoint));
    assert_eq!(ledger.approval_evidence(&checkpoint).unwrap().len(), 1);

    ledger.queue_check(&mut checkpoint, check.id()).unwrap();
    assert!(!ledger.all_current_checks_passed(&checkpoint));
    assert_eq!(
        ledger.approval_evidence(&checkpoint),
        Err(QualityStateError::ChecksNotPassed)
    );
    let queued = project_test_snapshot(&ledger, &checkpoint);
    assert_eq!(queued.status, TestStatus::Queued);
    assert_eq!(queued.cases[0].duration_ms, 0);
    assert_eq!(
        queued.cases[0].summary,
        "Awaiting current-generation evidence"
    );

    let failed_token = ledger
        .mark_check_running(&checkpoint, check.id(), CheckActor::Executor, 1)
        .unwrap();
    let running = project_test_snapshot(&ledger, &checkpoint);
    assert_eq!(running.status, TestStatus::Running);
    assert_eq!(running.cases[0].duration_ms, 0);
    assert_eq!(running.cases[0].summary, "Check is running");

    ledger
        .finish_check(
            &mut checkpoint,
            failed_token,
            CheckEvidenceStatus::Failed,
            20,
            "latest failure",
            false,
        )
        .unwrap();
    let failed = project_test_snapshot(&ledger, &checkpoint);
    assert_eq!(failed.status, TestStatus::Failed);
    assert_eq!(failed.cases[0].duration_ms, 20);
    assert_eq!(failed.cases[0].summary, "latest failure");

    run_to_terminal(
        &mut ledger,
        &mut checkpoint,
        &check,
        CheckEvidenceStatus::Cancelled,
        30,
        "latest cancellation",
    );
    let cancelled = project_test_snapshot(&ledger, &checkpoint);
    assert_eq!(cancelled.status, TestStatus::Cancelled);
    assert_eq!(cancelled.cases[0].duration_ms, 30);
    assert_eq!(cancelled.cases[0].summary, "latest cancellation");
    assert_eq!(
        checkpoint.current_observation(check.id()).unwrap().status(),
        CheckEvidenceStatus::Cancelled
    );
}

#[test]
fn current_review_evidence_is_ordered_and_does_not_hide_terminal_failures() {
    let checks = vec![
        cargo_test("passed", None, None),
        cargo_check("failed", Some("core")),
        cargo_check("missing", Some("store")),
    ];
    let mut ledger = RequiredCheckLedger::try_new(checks.clone()).unwrap();
    let mut checkpoint = WorkspaceCheckpoint::new(fingerprint(0xaa));
    run_to_terminal(
        &mut ledger,
        &mut checkpoint,
        &checks[0],
        CheckEvidenceStatus::Passed,
        1,
        "passed",
    );
    run_to_terminal(
        &mut ledger,
        &mut checkpoint,
        &checks[1],
        CheckEvidenceStatus::Failed,
        2,
        "failed",
    );

    let evidence = ledger.current_evidence(&checkpoint);
    assert_eq!(
        evidence
            .iter()
            .map(CheckEvidence::check_id)
            .collect::<Vec<_>>(),
        vec!["passed", "failed"]
    );
    assert_eq!(evidence[1].status(), CheckEvidenceStatus::Failed);

    ledger.queue_check(&mut checkpoint, checks[0].id()).unwrap();
    assert_eq!(
        ledger
            .current_evidence(&checkpoint)
            .iter()
            .map(CheckEvidence::check_id)
            .collect::<Vec<_>>(),
        vec!["failed"]
    );
}

#[test]
fn review_binder_requires_exact_ledger_order_and_all_current_terminal_results() {
    let checks = vec![
        cargo_test("passed", None, None),
        cargo_check("failed", Some("core")),
        cargo_check("cancelled", Some("store")),
    ];
    let mut ledger = RequiredCheckLedger::try_new(checks.clone()).unwrap();
    let mut checkpoint = WorkspaceCheckpoint::new(fingerprint(0xaa));
    run_to_terminal(
        &mut ledger,
        &mut checkpoint,
        &checks[0],
        CheckEvidenceStatus::Passed,
        1,
        "passed",
    );
    run_to_terminal(
        &mut ledger,
        &mut checkpoint,
        &checks[1],
        CheckEvidenceStatus::Failed,
        2,
        "failed",
    );
    run_to_terminal(
        &mut ledger,
        &mut checkpoint,
        &checks[2],
        CheckEvidenceStatus::Cancelled,
        3,
        "cancelled",
    );

    let current = ledger.current_evidence(&checkpoint);
    let exact = changes_requested_review(&checkpoint, ledger.checks().to_vec(), current.clone());
    assert_eq!(ledger.validate_review_evidence(&checkpoint, &exact), Ok(()));

    let omitted_failure = changes_requested_review(
        &checkpoint,
        ledger.checks().to_vec(),
        vec![current[0].clone(), current[2].clone()],
    );
    assert_eq!(
        ledger.validate_review_evidence(&checkpoint, &omitted_failure),
        Err(QualityStateError::ReviewCheckEvidenceMismatch)
    );

    let incomplete_required_set = changes_requested_review(
        &checkpoint,
        vec![checks[0].clone(), checks[2].clone()],
        vec![current[0].clone(), current[2].clone()],
    );
    assert_eq!(
        ledger.validate_review_evidence(&checkpoint, &incomplete_required_set),
        Err(QualityStateError::ReviewRequiredChecksMismatch)
    );
}

#[test]
fn review_binder_rejects_superseded_same_generation_evidence() {
    let check = cargo_test("check-1", None, None);
    let mut ledger = RequiredCheckLedger::try_new(vec![check.clone()]).unwrap();
    let mut checkpoint = WorkspaceCheckpoint::new(fingerprint(0xaa));
    run_to_terminal(
        &mut ledger,
        &mut checkpoint,
        &check,
        CheckEvidenceStatus::Passed,
        1,
        "passed",
    );

    let old_pass = ledger.current_evidence(&checkpoint);
    let approved = approved_review(&checkpoint, ledger.checks().to_vec(), old_pass.clone());
    assert_eq!(
        ledger.validate_review_evidence(&checkpoint, &approved),
        Ok(())
    );

    run_to_terminal(
        &mut ledger,
        &mut checkpoint,
        &check,
        CheckEvidenceStatus::Failed,
        2,
        "latest failure",
    );
    assert_eq!(checkpoint.generation(), 0);
    assert_eq!(
        ledger.validate_review_evidence(&checkpoint, &approved),
        Err(QualityStateError::ReviewCheckEvidenceMismatch)
    );

    let replayed_old_pass =
        changes_requested_review(&checkpoint, ledger.checks().to_vec(), old_pass);
    assert_eq!(
        ledger.validate_review_evidence(&checkpoint, &replayed_old_pass),
        Err(QualityStateError::ReviewCheckEvidenceMismatch)
    );

    let latest = changes_requested_review(
        &checkpoint,
        ledger.checks().to_vec(),
        ledger.current_evidence(&checkpoint),
    );
    assert_eq!(
        ledger.validate_review_evidence(&checkpoint, &latest),
        Ok(())
    );
}

#[test]
fn review_binder_rejects_evidence_from_an_old_workspace_generation() {
    let check = cargo_test("check-1", None, None);
    let mut ledger = RequiredCheckLedger::try_new(vec![check.clone()]).unwrap();
    let mut checkpoint = WorkspaceCheckpoint::new(fingerprint(0xaa));
    run_to_terminal(
        &mut ledger,
        &mut checkpoint,
        &check,
        CheckEvidenceStatus::Passed,
        1,
        "passed",
    );
    let old_review = approved_review(
        &checkpoint,
        ledger.checks().to_vec(),
        ledger.current_evidence(&checkpoint),
    );

    checkpoint.observe_stable(fingerprint(0xbb)).unwrap();
    assert_eq!(
        ledger.validate_review_evidence(&checkpoint, &old_review),
        Err(QualityStateError::ReviewCheckpointMismatch)
    );
}

#[test]
fn finish_check_uses_token_actor_role_and_rejects_zero_role_run() {
    let check = cargo_test("check-1", None, None);
    let mut ledger = RequiredCheckLedger::try_new(vec![check.clone()]).unwrap();
    let mut checkpoint = WorkspaceCheckpoint::new(fingerprint(0xaa));
    ledger.queue_check(&mut checkpoint, check.id()).unwrap();

    assert_eq!(
        ledger.mark_check_running(&checkpoint, check.id(), CheckActor::Reviewer, 0),
        Err(QualityStateError::InvalidCheckRoleRun)
    );
    let token = ledger
        .mark_check_running(&checkpoint, check.id(), CheckActor::Reviewer, 3)
        .unwrap();
    ledger
        .finish_check(
            &mut checkpoint,
            token,
            CheckEvidenceStatus::Passed,
            11,
            "reviewer pass",
            true,
        )
        .unwrap();

    let observation = checkpoint.current_observation(check.id()).unwrap();
    assert_eq!(observation.check_id(), check.id());
    assert_eq!(observation.actor(), CheckActor::Reviewer);
    assert_eq!(observation.role_run(), 3);
    assert_eq!(observation.summary(), "reviewer pass");
    assert!(observation.truncated());
}

#[test]
fn invalid_terminal_fields_fail_closed_without_leaving_an_unfinishable_active_run() {
    let check = cargo_test("check-1", None, None);
    let mut ledger = RequiredCheckLedger::try_new(vec![check.clone()]).unwrap();
    let mut checkpoint = WorkspaceCheckpoint::new(fingerprint(0xaa));
    ledger.queue_check(&mut checkpoint, check.id()).unwrap();
    let token = ledger
        .mark_check_running(&checkpoint, check.id(), CheckActor::Executor, 1)
        .unwrap();

    assert_eq!(
        ledger.finish_check(
            &mut checkpoint,
            token,
            CheckEvidenceStatus::Passed,
            1,
            "x".repeat(2_049),
            false,
        ),
        Err(QualityStateError::InvalidTerminalObservation)
    );
    assert_eq!(
        project_test_snapshot(&ledger, &checkpoint).status,
        TestStatus::Queued
    );
    ledger.queue_check(&mut checkpoint, check.id()).unwrap();
}

#[test]
fn projector_uses_creation_order_canonical_names_and_fixed_aggregate_priority() {
    let checks = vec![
        cargo_test("test", None, None),
        cargo_check("failed", Some("core")),
        cargo_test("cancelled", Some("store"), Some("reviews")),
        cargo_check("queued", None),
        cargo_test("running", Some("app"), None),
    ];
    let mut ledger = RequiredCheckLedger::try_new(checks.clone()).unwrap();
    let mut checkpoint = WorkspaceCheckpoint::new(fingerprint(0xaa));

    run_to_terminal(
        &mut ledger,
        &mut checkpoint,
        &checks[0],
        CheckEvidenceStatus::Passed,
        1,
        "ok",
    );
    run_to_terminal(
        &mut ledger,
        &mut checkpoint,
        &checks[1],
        CheckEvidenceStatus::Failed,
        2,
        "failed",
    );
    run_to_terminal(
        &mut ledger,
        &mut checkpoint,
        &checks[2],
        CheckEvidenceStatus::Cancelled,
        3,
        "cancelled",
    );
    ledger.queue_check(&mut checkpoint, checks[4].id()).unwrap();
    let running_token = ledger
        .mark_check_running(&checkpoint, checks[4].id(), CheckActor::Reviewer, 1)
        .unwrap();

    let snapshot = project_test_snapshot(&ledger, &checkpoint);
    assert_eq!(snapshot.revision, 0);
    assert_eq!(snapshot.status, TestStatus::Running);
    assert_eq!(
        snapshot
            .cases
            .iter()
            .map(|case| case.id.as_str())
            .collect::<Vec<_>>(),
        vec!["test", "failed", "cancelled", "queued", "running"]
    );
    assert_eq!(
        snapshot
            .cases
            .iter()
            .map(|case| case.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "cargo_test[package=workspace;integration_test=all]",
            "cargo_check[package=core]",
            "cargo_test[package=store;integration_test=reviews]",
            "cargo_check[package=workspace]",
            "cargo_test[package=app;integration_test=all]",
        ]
    );

    ledger
        .finish_check(
            &mut checkpoint,
            running_token,
            CheckEvidenceStatus::Passed,
            5,
            "ok",
            false,
        )
        .unwrap();
    assert_eq!(
        project_test_snapshot(&ledger, &checkpoint).status,
        TestStatus::Failed
    );

    run_to_terminal(
        &mut ledger,
        &mut checkpoint,
        &checks[1],
        CheckEvidenceStatus::Passed,
        6,
        "fixed",
    );
    assert_eq!(
        project_test_snapshot(&ledger, &checkpoint).status,
        TestStatus::Cancelled
    );

    run_to_terminal(
        &mut ledger,
        &mut checkpoint,
        &checks[2],
        CheckEvidenceStatus::Passed,
        7,
        "ok",
    );
    assert_eq!(
        project_test_snapshot(&ledger, &checkpoint).status,
        TestStatus::Queued
    );

    run_to_terminal(
        &mut ledger,
        &mut checkpoint,
        &checks[3],
        CheckEvidenceStatus::Passed,
        8,
        "ok",
    );
    assert_eq!(
        project_test_snapshot(&ledger, &checkpoint).status,
        TestStatus::Passed
    );
    assert!(ledger.all_current_checks_passed(&checkpoint));
}

#[test]
fn stale_running_attempt_cannot_write_evidence_into_a_new_generation() {
    let check = cargo_test("check-1", None, None);
    let mut ledger = RequiredCheckLedger::try_new(vec![check.clone()]).unwrap();
    let mut checkpoint = WorkspaceCheckpoint::new(fingerprint(0xaa));

    ledger.queue_check(&mut checkpoint, check.id()).unwrap();
    let stale_token = ledger
        .mark_check_running(&checkpoint, check.id(), CheckActor::Executor, 1)
        .unwrap();
    checkpoint.observe_stable(fingerprint(0xbb)).unwrap();
    ledger.queue_check(&mut checkpoint, check.id()).unwrap();
    let current_token = ledger
        .mark_check_running(&checkpoint, check.id(), CheckActor::Executor, 2)
        .unwrap();

    assert_eq!(
        ledger.finish_check(
            &mut checkpoint,
            stale_token,
            CheckEvidenceStatus::Passed,
            1,
            "stale",
            false,
        ),
        Err(QualityStateError::ObservationCheckpointMismatch)
    );
    assert!(checkpoint.current_observation(check.id()).is_none());
    let snapshot = project_test_snapshot(&ledger, &checkpoint);
    assert_eq!(snapshot.revision, 1);
    assert_eq!(snapshot.status, TestStatus::Running);

    ledger
        .finish_check(
            &mut checkpoint,
            current_token,
            CheckEvidenceStatus::Passed,
            2,
            "current",
            false,
        )
        .unwrap();
    assert!(ledger.all_current_checks_passed(&checkpoint));
}

#[test]
fn token_from_another_ledger_cannot_complete_or_remove_the_current_attempt() {
    let check = cargo_test("check-1", None, None);
    let mut stale_ledger = RequiredCheckLedger::try_new(vec![check.clone()]).unwrap();
    let mut stale_checkpoint = WorkspaceCheckpoint::new(fingerprint(0xaa));
    stale_ledger
        .queue_check(&mut stale_checkpoint, check.id())
        .unwrap();
    let stale_token = stale_ledger
        .mark_check_running(&stale_checkpoint, check.id(), CheckActor::Executor, 1)
        .unwrap();

    let mut ledger = RequiredCheckLedger::try_new(vec![check.clone()]).unwrap();
    let mut checkpoint = WorkspaceCheckpoint::new(fingerprint(0xaa));
    ledger.queue_check(&mut checkpoint, check.id()).unwrap();
    let current_token = ledger
        .mark_check_running(&checkpoint, check.id(), CheckActor::Executor, 1)
        .unwrap();

    assert_eq!(
        ledger.finish_check(
            &mut checkpoint,
            stale_token,
            CheckEvidenceStatus::Passed,
            1,
            "foreign",
            false,
        ),
        Err(QualityStateError::StaleCheckRunToken)
    );
    assert_eq!(
        project_test_snapshot(&ledger, &checkpoint).status,
        TestStatus::Running
    );
    ledger
        .finish_check(
            &mut checkpoint,
            current_token,
            CheckEvidenceStatus::Passed,
            2,
            "current",
            false,
        )
        .unwrap();
}

#[test]
fn appending_a_check_updates_the_snapshot_without_changing_generation() {
    let first = cargo_test("check-1", None, None);
    let second = cargo_check("check-2", None);
    let mut ledger = RequiredCheckLedger::try_new(vec![first.clone()]).unwrap();
    let mut checkpoint = WorkspaceCheckpoint::new(fingerprint(0xaa));
    run_to_terminal(
        &mut ledger,
        &mut checkpoint,
        &first,
        CheckEvidenceStatus::Passed,
        1,
        "ok",
    );

    let before = project_test_snapshot(&ledger, &checkpoint);
    assert_eq!(before.revision, 0);
    assert_eq!(before.status, TestStatus::Passed);

    assert_eq!(ledger.append_checks(vec![second]).unwrap(), 1);
    let after = project_test_snapshot(&ledger, &checkpoint);
    assert_eq!(after.revision, 0);
    assert_eq!(after.status, TestStatus::Queued);
    assert_eq!(after.cases.len(), 2);
}

#[test]
fn unverified_terminal_projection_never_reuses_passed_or_running_state() {
    let passed = cargo_test("check-1", None, None);
    let running = cargo_check("check-2", None);
    let mut ledger = RequiredCheckLedger::try_new(vec![passed.clone(), running.clone()]).unwrap();
    let mut checkpoint = WorkspaceCheckpoint::new(fingerprint(0xaa));
    run_to_terminal(
        &mut ledger,
        &mut checkpoint,
        &passed,
        CheckEvidenceStatus::Passed,
        1,
        "ok",
    );
    ledger.queue_check(&mut checkpoint, running.id()).unwrap();
    ledger
        .mark_check_running(&checkpoint, running.id(), CheckActor::Executor, 1)
        .unwrap();

    let snapshot = project_unverified_test_snapshot(&ledger, checkpoint.generation());

    assert_eq!(snapshot.status, TestStatus::Queued);
    assert!(
        snapshot
            .cases
            .iter()
            .all(|case| case.status == TestStatus::Queued && case.duration_ms == 0)
    );
}
