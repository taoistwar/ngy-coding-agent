use coding_agent_domain::{
    CheckActor, CheckEvidence, CheckEvidenceStatus, DomainError, EventId, FindingSeverity,
    MAX_CARGO_SELECTOR_BYTES, MAX_WORKSPACE_GENERATION, NewReviewEvidence, RequiredCheck,
    RequiredCheckKind, RequiredCheckSelector, ReviewCoverageEvidence, ReviewDecisionSource,
    ReviewEvidence, ReviewFinding, ReviewVerdict, TaskEvent, TaskEventKind, TaskEventPayload,
    TaskId, UtcTimestamp, WorkspaceDigest, is_valid_cargo_selector,
};

const REVIEW_EVIDENCE_LIMIT: usize = 128 * 1024;

#[test]
fn workspace_digest_and_generation_are_exact_and_json_safe() {
    let digest = WorkspaceDigest::try_new("a".repeat(64)).unwrap();
    assert_eq!(digest.algorithm(), "workspace_fingerprint_v1");
    assert_eq!(digest.value(), "a".repeat(64));
    assert_eq!(MAX_WORKSPACE_GENERATION, 9_007_199_254_740_991);

    for invalid in ["a".repeat(63), "a".repeat(65), "A".repeat(64)] {
        assert_eq!(
            WorkspaceDigest::try_new(invalid),
            Err(DomainError::InvalidQualityEvidence)
        );
    }

    let encoded = serde_json::to_value(&digest).unwrap();
    assert_eq!(encoded["algorithm"], "workspace_fingerprint_v1");
    assert_eq!(encoded["value"], "a".repeat(64));
    assert_eq!(
        serde_json::from_value::<WorkspaceDigest>(encoded).unwrap(),
        digest
    );
    assert!(
        serde_json::from_value::<WorkspaceDigest>(serde_json::json!({
            "algorithm": "workspace_fingerprint_v2",
            "value": "a".repeat(64)
        }))
        .is_err()
    );
}

#[test]
fn required_check_selectors_are_canonical_and_typed() {
    let check = RequiredCheck::try_cargo_check("check-1", Some("workspace".into())).unwrap();
    let test = RequiredCheck::try_cargo_test(
        "check-2",
        Some("workspace".into()),
        Some("integration".into()),
    )
    .unwrap();

    assert_eq!(check.id(), "check-1");
    assert!(!check.is_cargo_test());
    assert_eq!(check.package(), Some("workspace"));
    assert_eq!(test.integration_test(), Some("integration"));
    assert!(test.is_cargo_test());
    assert_eq!(test.selector().kind(), RequiredCheckKind::CargoTest);
    assert_eq!(
        test.selector(),
        &RequiredCheckSelector::try_cargo_test(
            Some("workspace".into()),
            Some("integration".into()),
        )
        .unwrap()
    );

    assert_eq!(
        RequiredCheck::try_cargo_test("check-3", None, Some("integration".into())),
        Err(DomainError::InvalidQualityEvidence)
    );
    assert_eq!(
        RequiredCheck::try_cargo_check("", None),
        Err(DomainError::InvalidQualityEvidence)
    );
    assert!(is_valid_cargo_selector(
        &"p".repeat(MAX_CARGO_SELECTOR_BYTES)
    ));
    for invalid in [
        "p".repeat(MAX_CARGO_SELECTOR_BYTES + 1),
        "--package".into(),
        "with space".into(),
        "path/name".into(),
        "name=value".into(),
        "unicode-工具".into(),
    ] {
        assert_eq!(
            RequiredCheck::try_cargo_check("check-invalid", Some(invalid)),
            Err(DomainError::InvalidQualityEvidence)
        );
    }
    let workspace_test = RequiredCheck::try_cargo_test("check-workspace", None, None).unwrap();
    assert_eq!(
        serde_json::to_value(&workspace_test).unwrap(),
        serde_json::json!({
            "id": "check-workspace",
            "kind": "cargo_test",
            "package": null,
            "integration_test": null
        })
    );
    assert!(
        serde_json::from_value::<RequiredCheck>(serde_json::json!({
            "id": "check-3",
            "kind": "cargo_test",
            "package": null,
            "integration_test": "integration"
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<RequiredCheck>(serde_json::json!({
            "id": "check-4",
            "kind": "cargo_check"
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<RequiredCheck>(serde_json::json!({
            "id": "check-5",
            "kind": "cargo_check",
            "package": null,
            "extra": true
        }))
        .is_err()
    );
}

#[test]
fn check_evidence_enforces_actor_role_generation_and_utf8_summary_bounds() {
    let digest = digest();
    let check = checks().remove(0);
    let evidence = CheckEvidence::try_for_check(
        &check,
        CheckActor::Executor,
        1,
        MAX_WORKSPACE_GENERATION,
        digest.clone(),
        CheckEvidenceStatus::Passed,
        42,
        "ok",
        false,
    )
    .unwrap();
    assert_eq!(evidence.check_id(), "check-1");
    assert_eq!(evidence.role_run(), 1);
    assert_eq!(evidence.workspace_generation(), MAX_WORKSPACE_GENERATION);
    assert_eq!(evidence.workspace_digest(), &digest);
    let mut empty_summary_wire = serde_json::to_value(&evidence).unwrap();
    empty_summary_wire["summary"] = serde_json::Value::String(String::new());
    assert!(serde_json::from_value::<CheckEvidence>(empty_summary_wire).is_err());

    assert_eq!(
        CheckEvidence::try_for_check(
            &check,
            CheckActor::Executor,
            1,
            0,
            digest.clone(),
            CheckEvidenceStatus::Passed,
            0,
            "",
            false,
        ),
        Err(DomainError::InvalidQualityEvidence)
    );

    assert_eq!(
        CheckEvidence::try_for_check(
            &check,
            CheckActor::Reviewer,
            0,
            0,
            digest.clone(),
            CheckEvidenceStatus::Failed,
            0,
            "failed",
            false,
        ),
        Err(DomainError::InvalidQualityEvidence)
    );
    assert_eq!(
        CheckEvidence::try_for_check(
            &check,
            CheckActor::Reviewer,
            1,
            MAX_WORKSPACE_GENERATION + 1,
            digest.clone(),
            CheckEvidenceStatus::Cancelled,
            0,
            "cancelled",
            false,
        ),
        Err(DomainError::InvalidQualityEvidence)
    );
    assert!(
        CheckEvidence::try_for_check(
            &check,
            CheckActor::Executor,
            1,
            0,
            digest.clone(),
            CheckEvidenceStatus::Passed,
            0,
            "界".repeat(683),
            true,
        )
        .is_err()
    );
    assert!(
        CheckEvidence::try_for_check(
            &check,
            CheckActor::Executor,
            1,
            0,
            digest.clone(),
            CheckEvidenceStatus::Passed,
            u64::MAX,
            "passed",
            false,
        )
        .is_err()
    );
}

#[test]
fn findings_validate_path_line_and_message_relationships() {
    let finding = ReviewFinding::try_for_review(
        1,
        1,
        FindingSeverity::Blocking,
        "fix this",
        Some("src/lib.rs".into()),
        Some(12),
    )
    .unwrap();
    assert_eq!(finding.path(), Some("src/lib.rs"));
    assert_eq!(finding.line(), Some(12));
    assert!(finding.matches_review_position(1, 1));
    assert!(!finding.matches_review_position(2, 1));
    assert!(!finding.matches_review_position(1, 2));
    assert!(
        ReviewFinding::try_for_review(
            1,
            0,
            FindingSeverity::Blocking,
            "invalid ordinal",
            None,
            None,
        )
        .is_err()
    );
    assert_eq!(
        serde_json::from_value::<ReviewFinding>(serde_json::json!({
            "id": "review-1-finding-1",
            "severity": "advisory",
            "message": "explicit nulls",
            "path": null,
            "line": null
        }))
        .unwrap()
        .path(),
        None
    );

    for invalid in ["/rooted", "../escape", "src\\lib.rs", ".git/config"] {
        assert!(
            ReviewFinding::try_for_review(
                1,
                1,
                FindingSeverity::Blocking,
                "fix this",
                Some(invalid.into()),
                None,
            )
            .is_err()
        );
    }
    assert!(
        ReviewFinding::try_for_review(1, 1, FindingSeverity::Blocking, "fix this", None, Some(1),)
            .is_err()
    );
    assert!(
        serde_json::from_value::<ReviewFinding>(serde_json::json!({
            "id": "review-1-finding-1",
            "severity": "blocking",
            "message": "missing nullable fields"
        }))
        .is_err()
    );
    assert!(
        ReviewFinding::try_for_review(1, 1, FindingSeverity::Blocking, "", None, None,).is_err()
    );
    assert!(
        ReviewFinding::try_for_review(
            1,
            1,
            FindingSeverity::Blocking,
            "unsafe line",
            Some("src/lib.rs".into()),
            Some(u64::MAX),
        )
        .is_err()
    );
}

#[test]
fn coverage_is_sorted_unique_bounded_and_can_prove_completeness() {
    let complete =
        ReviewCoverageEvidence::try_new(2, digest(), "b".repeat(64), vec![0, 1, 2], 3).unwrap();
    assert!(complete.is_complete());

    assert!(ReviewCoverageEvidence::try_new(2, digest(), "b".repeat(64), vec![1, 0], 2,).is_err());
    assert!(ReviewCoverageEvidence::try_new(2, digest(), "b".repeat(64), vec![0, 0], 2,).is_err());
    assert!(ReviewCoverageEvidence::try_new(2, digest(), "b".repeat(64), vec![8], 9,).is_err());
}

#[test]
fn approved_review_requires_complete_coverage_and_every_current_pass() {
    let new = valid_review(
        ReviewDecisionSource::Reviewer,
        ReviewVerdict::Approved,
        vec![],
    );
    let evidence = ReviewEvidence::try_from_new(new, timestamp()).unwrap();
    assert_eq!(evidence.round(), 1);
    assert_eq!(evidence.verdict(), ReviewVerdict::Approved);
    assert_eq!(evidence.created_at(), timestamp());

    let round_trip =
        serde_json::from_value::<ReviewEvidence>(serde_json::to_value(&evidence).unwrap()).unwrap();
    assert_eq!(round_trip, evidence);
    let mut empty_summary = serde_json::to_value(&evidence).unwrap();
    empty_summary["summary"] = serde_json::Value::String(String::new());
    assert!(serde_json::from_value::<ReviewEvidence>(empty_summary).is_err());
    let mut missing_coverage = serde_json::to_value(&evidence).unwrap();
    missing_coverage.as_object_mut().unwrap().remove("coverage");
    assert!(serde_json::from_value::<ReviewEvidence>(missing_coverage).is_err());
    let mut unknown_field = serde_json::to_value(&evidence).unwrap();
    unknown_field
        .as_object_mut()
        .unwrap()
        .insert("unexpected".into(), serde_json::json!(true));
    assert!(serde_json::from_value::<ReviewEvidence>(unknown_field).is_err());

    assert!(
        NewReviewEvidence::try_new(
            1,
            ReviewDecisionSource::Reviewer,
            0,
            digest(),
            ReviewVerdict::Approved,
            "",
            vec![],
            vec![],
            checks(),
            passed_evidence(),
            Some(complete_coverage()),
        )
        .is_err()
    );

    let blocking =
        ReviewFinding::try_for_review(1, 1, FindingSeverity::Blocking, "blocking", None, None)
            .unwrap();
    assert!(
        NewReviewEvidence::try_new(
            1,
            ReviewDecisionSource::Reviewer,
            0,
            digest(),
            ReviewVerdict::Approved,
            "approved",
            vec![blocking],
            vec![],
            checks(),
            passed_evidence(),
            Some(complete_coverage()),
        )
        .is_err()
    );

    assert!(
        NewReviewEvidence::try_new(
            1,
            ReviewDecisionSource::Reviewer,
            0,
            digest(),
            ReviewVerdict::Approved,
            "approved",
            vec![],
            vec![],
            checks(),
            vec![],
            Some(complete_coverage()),
        )
        .is_err()
    );
}

#[test]
fn changes_requested_and_system_decisions_have_closed_semantics() {
    assert!(
        NewReviewEvidence::try_new(
            1,
            ReviewDecisionSource::Reviewer,
            0,
            digest(),
            ReviewVerdict::ChangesRequested,
            "needs work",
            vec![],
            vec![],
            checks(),
            passed_evidence(),
            None,
        )
        .is_err()
    );

    let reviewer_finding =
        ReviewFinding::try_for_review(1, 1, FindingSeverity::Blocking, "needs work", None, None)
            .unwrap();
    let reviewer_changes = NewReviewEvidence::try_new(
        1,
        ReviewDecisionSource::Reviewer,
        0,
        digest(),
        ReviewVerdict::ChangesRequested,
        "needs work",
        vec![reviewer_finding],
        vec![],
        checks(),
        passed_evidence(),
        None,
    )
    .unwrap();
    let persisted = ReviewEvidence::try_from_new(reviewer_changes, timestamp()).unwrap();
    let mut missing_nullable_coverage = serde_json::to_value(persisted).unwrap();
    missing_nullable_coverage
        .as_object_mut()
        .unwrap()
        .remove("coverage");
    assert!(serde_json::from_value::<ReviewEvidence>(missing_nullable_coverage).is_err());

    let blocking = ReviewFinding::system_workspace_changed(1).unwrap();
    assert!(
        NewReviewEvidence::try_new(
            1,
            ReviewDecisionSource::System,
            0,
            digest(),
            ReviewVerdict::ChangesRequested,
            "invalidated",
            vec![blocking.clone()],
            vec![],
            checks(),
            vec![],
            None,
        )
        .is_ok()
    );
    assert!(
        NewReviewEvidence::try_new(
            1,
            ReviewDecisionSource::System,
            0,
            digest(),
            ReviewVerdict::Approved,
            "invalid",
            vec![blocking.clone()],
            vec![],
            checks(),
            passed_evidence(),
            Some(complete_coverage()),
        )
        .is_err()
    );
    assert!(
        NewReviewEvidence::try_new(
            1,
            ReviewDecisionSource::System,
            0,
            digest(),
            ReviewVerdict::ChangesRequested,
            "invalidated",
            vec![blocking],
            vec![],
            checks(),
            vec![],
            Some(complete_coverage()),
        )
        .is_err()
    );
    let arbitrary = ReviewFinding::try_for_review(
        1,
        1,
        FindingSeverity::Blocking,
        "arbitrary system reason",
        None,
        None,
    )
    .unwrap();
    assert!(
        NewReviewEvidence::try_new(
            1,
            ReviewDecisionSource::System,
            0,
            digest(),
            ReviewVerdict::ChangesRequested,
            "invalidated",
            vec![arbitrary],
            vec![],
            checks(),
            vec![],
            None,
        )
        .is_err()
    );
}

#[test]
fn review_rejects_duplicate_or_mismatched_checks_and_finding_ids() {
    let mut duplicated = checks();
    duplicated.push(duplicated[0].clone());
    assert!(
        NewReviewEvidence::try_new(
            1,
            ReviewDecisionSource::Reviewer,
            0,
            digest(),
            ReviewVerdict::Approved,
            "approved",
            vec![],
            vec![],
            duplicated,
            passed_evidence(),
            Some(complete_coverage()),
        )
        .is_err()
    );

    let wrong_round =
        ReviewFinding::try_for_review(2, 1, FindingSeverity::Blocking, "wrong round", None, None)
            .unwrap();
    assert!(
        NewReviewEvidence::try_new(
            1,
            ReviewDecisionSource::Reviewer,
            0,
            digest(),
            ReviewVerdict::ChangesRequested,
            "needs work",
            vec![wrong_round],
            vec![],
            checks(),
            vec![],
            None,
        )
        .is_err()
    );
}

#[test]
fn canonical_json_size_limit_is_exact_for_new_and_persisted_evidence() {
    let exact_new = try_review_with_encoded_size(REVIEW_EVIDENCE_LIMIT).unwrap();
    assert_eq!(
        serde_json::to_vec(&exact_new).unwrap().len(),
        REVIEW_EVIDENCE_LIMIT
    );
    assert!(ReviewEvidence::try_from_new(exact_new, timestamp()).is_err());
    assert!(try_review_with_encoded_size(REVIEW_EVIDENCE_LIMIT + 1).is_err());

    let probe = try_review_with_encoded_size(64 * 1024).unwrap();
    let probe_size = serde_json::to_vec(&probe).unwrap().len();
    let persisted = ReviewEvidence::try_from_new(probe, timestamp()).unwrap();
    let persisted_overhead = serde_json::to_vec(&persisted).unwrap().len() - probe_size;

    let exact_persisted_new =
        try_review_with_encoded_size(REVIEW_EVIDENCE_LIMIT - persisted_overhead).unwrap();
    let exact_persisted = ReviewEvidence::try_from_new(exact_persisted_new, timestamp()).unwrap();
    assert_eq!(
        serde_json::to_vec(&exact_persisted).unwrap().len(),
        REVIEW_EVIDENCE_LIMIT
    );

    let one_over_persisted =
        try_review_with_encoded_size(REVIEW_EVIDENCE_LIMIT - persisted_overhead + 1).unwrap();
    assert!(ReviewEvidence::try_from_new(one_over_persisted, timestamp()).is_err());
}

#[test]
fn review_updated_is_the_schema_v1_eleventh_event_and_round_trips_under_the_wire_limit() {
    let kinds = [
        TaskEventKind::TaskQueued,
        TaskEventKind::TaskStarted,
        TaskEventKind::PlanUpdated,
        TaskEventKind::ActivityAppended,
        TaskEventKind::DiffUpdated,
        TaskEventKind::TestUpdated,
        TaskEventKind::ReviewUpdated,
        TaskEventKind::TaskCompleted,
        TaskEventKind::TaskFailed,
        TaskEventKind::TaskCancelled,
        TaskEventKind::TaskInterrupted,
    ];
    assert_eq!(kinds.len(), 11);
    let review = ReviewEvidence::try_from_new(
        valid_review(
            ReviewDecisionSource::Reviewer,
            ReviewVerdict::Approved,
            vec![],
        ),
        timestamp(),
    )
    .unwrap();
    let event = TaskEvent::new(
        EventId::new(1).unwrap(),
        TaskId::new(),
        TaskEventPayload::ReviewUpdated {
            review: review.clone(),
        },
        timestamp(),
    );

    assert_eq!(event.schema_version, 1);
    assert_eq!(event.payload.kind(), TaskEventKind::ReviewUpdated);
    let encoded = serde_json::to_vec(&event).unwrap();
    assert!(encoded.len() <= 192 * 1024);
    let value = serde_json::to_value(&event).unwrap();
    assert_eq!(value["kind"], "review.updated");
    assert_eq!(value["payload"]["review"]["round"], 1);
    assert_eq!(serde_json::from_value::<TaskEvent>(value).unwrap(), event);
}

#[test]
fn added_checks_and_check_evidence_preserve_required_ledger_order() {
    let first = RequiredCheck::try_cargo_test("check-1", Some("workspace".into()), None).unwrap();
    let second = RequiredCheck::try_cargo_check("check-2", Some("workspace".into())).unwrap();
    let blocking =
        ReviewFinding::try_for_review(1, 1, FindingSeverity::Blocking, "needs work", None, None)
            .unwrap();
    assert!(
        NewReviewEvidence::try_new(
            1,
            ReviewDecisionSource::Reviewer,
            0,
            digest(),
            ReviewVerdict::ChangesRequested,
            "needs work",
            vec![blocking],
            vec![second.clone(), first.clone()],
            vec![first.clone(), second.clone()],
            vec![],
            None,
        )
        .is_err()
    );

    let second_evidence = CheckEvidence::try_for_check(
        &second,
        CheckActor::Executor,
        1,
        0,
        digest(),
        CheckEvidenceStatus::Passed,
        1,
        "passed",
        false,
    )
    .unwrap();
    assert!(
        NewReviewEvidence::try_new(
            1,
            ReviewDecisionSource::Reviewer,
            0,
            digest(),
            ReviewVerdict::Approved,
            "approved",
            vec![],
            vec![],
            vec![first, second],
            vec![second_evidence, passed_evidence().remove(0)],
            Some(complete_coverage()),
        )
        .is_err()
    );
}

fn valid_review(
    source: ReviewDecisionSource,
    verdict: ReviewVerdict,
    findings: Vec<ReviewFinding>,
) -> NewReviewEvidence {
    NewReviewEvidence::try_new(
        1,
        source,
        0,
        digest(),
        verdict,
        "reviewed",
        findings,
        vec![],
        checks(),
        passed_evidence(),
        Some(complete_coverage()),
    )
    .unwrap()
}

fn digest() -> WorkspaceDigest {
    WorkspaceDigest::try_new("a".repeat(64)).unwrap()
}

fn checks() -> Vec<RequiredCheck> {
    vec![RequiredCheck::try_cargo_test("check-1", Some("workspace".into()), None).unwrap()]
}

fn passed_evidence() -> Vec<CheckEvidence> {
    let checks = checks();
    vec![
        CheckEvidence::try_for_check(
            &checks[0],
            CheckActor::Executor,
            1,
            0,
            digest(),
            CheckEvidenceStatus::Passed,
            10,
            "passed",
            false,
        )
        .unwrap(),
    ]
}

fn try_review_with_encoded_size(target: usize) -> Result<NewReviewEvidence, DomainError> {
    let required_checks = (1..=16)
        .map(|ordinal| {
            RequiredCheck::try_cargo_test(
                format!("check-{ordinal}"),
                Some(format!("package-{ordinal}")),
                None,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let base_evidence = required_checks
        .iter()
        .map(|check| {
            CheckEvidence::try_for_check(
                check,
                CheckActor::Executor,
                1,
                0,
                digest(),
                CheckEvidenceStatus::Failed,
                1,
                "x",
                false,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let finding =
        ReviewFinding::try_for_review(1, 1, FindingSeverity::Blocking, "blocking", None, None)
            .unwrap();
    let base = NewReviewEvidence::try_new(
        1,
        ReviewDecisionSource::Reviewer,
        0,
        digest(),
        ReviewVerdict::ChangesRequested,
        "x",
        vec![finding.clone()],
        vec![],
        required_checks.clone(),
        base_evidence,
        None,
    )?;
    let base_size = serde_json::to_vec(&base).unwrap().len();
    let encoded_filler_bytes = target
        .checked_sub(base_size)
        .ok_or(DomainError::InvalidQualityEvidence)?;
    let summaries = escaped_fillers(encoded_filler_bytes, required_checks.len())?;
    let evidence = required_checks
        .iter()
        .zip(summaries)
        .map(|(check, summary)| {
            CheckEvidence::try_for_check(
                check,
                CheckActor::Executor,
                1,
                0,
                digest(),
                CheckEvidenceStatus::Failed,
                1,
                summary,
                false,
            )
            .unwrap()
        })
        .collect();
    NewReviewEvidence::try_new(
        1,
        ReviewDecisionSource::Reviewer,
        0,
        digest(),
        ReviewVerdict::ChangesRequested,
        "x",
        vec![finding],
        vec![],
        required_checks,
        evidence,
        None,
    )
}

fn escaped_fillers(encoded_bytes: usize, slots: usize) -> Result<Vec<String>, DomainError> {
    let controls = encoded_bytes / 6;
    let ascii = encoded_bytes % 6;
    if controls + ascii > slots * 2_047 {
        return Err(DomainError::InvalidQualityEvidence);
    }

    let mut controls_left = controls;
    let mut ascii_left = ascii;
    let mut result = Vec::with_capacity(slots);
    for _ in 0..slots {
        let control_count = controls_left.min(2_047);
        controls_left -= control_count;
        let remaining_capacity = 2_047 - control_count;
        let ascii_count = ascii_left.min(remaining_capacity);
        ascii_left -= ascii_count;
        result.push(format!(
            "a{}{}",
            "\u{0001}".repeat(control_count),
            "a".repeat(ascii_count)
        ));
    }
    if controls_left == 0 && ascii_left == 0 {
        Ok(result)
    } else {
        Err(DomainError::InvalidQualityEvidence)
    }
}

fn complete_coverage() -> ReviewCoverageEvidence {
    ReviewCoverageEvidence::try_new(0, digest(), "b".repeat(64), vec![0], 1).unwrap()
}

fn timestamp() -> UtcTimestamp {
    UtcTimestamp::parse_rfc3339("2026-07-23T00:00:00Z").unwrap()
}
