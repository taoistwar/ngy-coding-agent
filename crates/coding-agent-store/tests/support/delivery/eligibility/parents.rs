use coding_agent_domain::{
    CanonicalPath, CheckActor, CheckEvidence, CheckEvidenceStatus, FindingSeverity,
    NewReviewEvidence, PlanItem, PlanItemStatus, PlanSnapshot, RequiredCheck,
    ReviewCoverageEvidence, ReviewDecisionSource, ReviewFinding, ReviewVerdict, Task,
    TaskEventPayload, TaskStatus, WorkspaceDigest,
};
use coding_agent_store::{
    AttemptArtifactIdentity, FinalizeReviewedTaskOutcome, ReserveAttemptArtifact, Store,
    TaskTransition, TransitionOutcome,
};

use crate::support;

use super::BASE_COMMIT;

#[derive(Debug, Clone, Copy)]
pub enum FixtureArtifactState {
    Reserved,
    Ready,
    Inconsistent,
}

#[derive(Debug, Clone, Copy)]
pub enum ApprovedEvidenceVariant {
    Baseline,
    RequiredCheck,
    CheckEvidence,
    Coverage,
}

pub async fn approved_task_with_ready_artifact(branch_name: &str) -> (Store, Task) {
    approved_task_with_artifact_state(branch_name, FixtureArtifactState::Ready).await
}

pub async fn approved_task_with_prior_rejection(branch_name: &str) -> (Store, Task) {
    let store = support::seeded_store().await;
    approved_task_on_store_with_artifact_state(
        store,
        branch_name,
        0,
        FixtureArtifactState::Ready,
        2,
        ApprovedEvidenceVariant::Baseline,
    )
    .await
}

pub async fn approved_task_with_evidence_variant(
    branch_name: &str,
    variant: ApprovedEvidenceVariant,
) -> (Store, Task) {
    let store = support::seeded_store().await;
    approved_task_on_store_with_artifact_state(
        store,
        branch_name,
        0,
        FixtureArtifactState::Ready,
        1,
        variant,
    )
    .await
}

pub async fn approved_task_with_artifact_state(
    branch_name: &str,
    artifact_state: FixtureArtifactState,
) -> (Store, Task) {
    let store = support::seeded_store().await;
    approved_task_on_store_with_artifact_state(
        store,
        branch_name,
        0,
        artifact_state,
        1,
        ApprovedEvidenceVariant::Baseline,
    )
    .await
}

pub async fn approved_task_on_store(
    store: Store,
    branch_name: &str,
    noise_events: u32,
) -> (Store, Task) {
    approved_task_on_store_with_artifact_state(
        store,
        branch_name,
        noise_events,
        FixtureArtifactState::Ready,
        1,
        ApprovedEvidenceVariant::Baseline,
    )
    .await
}

async fn approved_task_on_store_with_artifact_state(
    store: Store,
    branch_name: &str,
    noise_events: u32,
    artifact_state: FixtureArtifactState,
    approved_round: u8,
    evidence_variant: ApprovedEvidenceVariant,
) -> (Store, Task) {
    let repository = store.list_repositories().await.unwrap().remove(0);
    let queued = support::queued_task(&store).await;
    let running = match store
        .transition_with_event(queued.id, TaskStatus::Queued, TaskTransition::Running)
        .await
        .unwrap()
    {
        TransitionOutcome::Applied { task, .. } => task,
        TransitionOutcome::Conflict { .. } => panic!("fixture transition must apply"),
    };
    let check = match evidence_variant {
        ApprovedEvidenceVariant::RequiredCheck => RequiredCheck::try_cargo_test(
            "project4-delivery-test-variant",
            Some("coding-agent-store".to_owned()),
            Some("delivery_eligibility".to_owned()),
        )
        .unwrap(),
        _ => required_check(),
    };
    let plan = PlanSnapshot::try_structured(
        1,
        "Implement the approved delivery",
        vec![
            PlanItem::try_structured(
                "step-1",
                "Implement",
                "Implement the approved behavior",
                vec!["All required checks pass".to_owned()],
                PlanItemStatus::Completed,
            )
            .unwrap(),
        ],
        vec![check.clone()],
    )
    .unwrap();
    store
        .append_running_event(running.id, TaskEventPayload::PlanUpdated { plan })
        .await
        .unwrap();
    let running = store.task_detail(running.id).await.unwrap().unwrap().task;
    if noise_events > 0 {
        insert_noise_events(&store, &running, noise_events).await;
    }
    let running = store.task_detail(running.id).await.unwrap().unwrap().task;
    let identity = AttemptArtifactIdentity {
        task_id: running.id,
        repository_id: repository.id,
        attempt: running.attempt,
    };
    store
        .reserve_attempt_artifact(ReserveAttemptArtifact {
            identity,
            base_commit: BASE_COMMIT.to_owned(),
            branch_name: branch_name.to_owned(),
            worktree_path: CanonicalPath::try_from_canonical(
                repository
                    .git_root
                    .as_path()
                    .join("artifacts")
                    .join(running.id.to_string()),
            )
            .unwrap(),
        })
        .await
        .unwrap();
    match artifact_state {
        FixtureArtifactState::Reserved => {}
        FixtureArtifactState::Ready => {
            store.mark_attempt_artifact_ready(identity).await.unwrap();
        }
        FixtureArtifactState::Inconsistent => {
            store
                .mark_attempt_artifact_inconsistent(identity, "WORKTREE_STATE_INCONSISTENT")
                .await
                .unwrap();
        }
    }
    if approved_round > 1 {
        store
            .record_review(
                running.id,
                repository.id,
                running.attempt,
                rejected_evidence(1, &check),
            )
            .await
            .unwrap();
    }
    let digit = char::from(b'a' + approved_round - 1);
    let digest = WorkspaceDigest::try_new(digit.to_string().repeat(64)).unwrap();
    let check_evidence = match evidence_variant {
        ApprovedEvidenceVariant::CheckEvidence => CheckEvidence::try_for_check(
            &check,
            CheckActor::Executor,
            u32::from(approved_round),
            u64::from(approved_round),
            digest.clone(),
            CheckEvidenceStatus::Passed,
            27,
            "delivery eligibility passed with variant evidence",
            false,
        )
        .unwrap(),
        _ => passed_check_for_round(approved_round, &check, &digest),
    };
    let coverage = match evidence_variant {
        ApprovedEvidenceVariant::Coverage => ReviewCoverageEvidence::try_new(
            u64::from(approved_round),
            digest.clone(),
            "e".repeat(64),
            vec![0, 1],
            2,
        )
        .unwrap(),
        _ => ReviewCoverageEvidence::try_new(
            u64::from(approved_round),
            digest.clone(),
            "f".repeat(64),
            vec![0],
            1,
        )
        .unwrap(),
    };
    let evidence = NewReviewEvidence::try_new(
        approved_round,
        ReviewDecisionSource::Reviewer,
        u64::from(approved_round),
        digest.clone(),
        ReviewVerdict::Approved,
        "approved delivery prompt secret",
        Vec::new(),
        Vec::new(),
        vec![check.clone()],
        vec![check_evidence],
        Some(coverage),
    )
    .unwrap();
    let finalized = store
        .finalize_reviewed_task(running.id, repository.id, running.attempt, evidence)
        .await
        .unwrap();
    let task = match finalized {
        FinalizeReviewedTaskOutcome::Applied { task, .. }
        | FinalizeReviewedTaskOutcome::Existing { task, .. } => task,
    };
    (store, task)
}

async fn insert_noise_events(store: &Store, running: &Task, noise_events: u32) {
    let payload = serde_json::to_string(&serde_json::json!({
        "entry": coding_agent_domain::ActivityEntry::legacy(
            "eligibility-gate",
            coding_agent_domain::ActivityLevel::Info,
            "make the read transaction observable",
            support::current_timestamp(),
        )
    }))
    .unwrap();
    let now = support::current_timestamp().to_string();
    let mut transaction = store.pool().begin().await.unwrap();
    let inserted = sqlx::query(
        "WITH RECURSIVE seq(n) AS (VALUES(1) UNION ALL SELECT n + 1 FROM seq WHERE n < ?) \
         INSERT INTO task_events (schema_version, task_id, kind, payload_json, created_at) \
         SELECT 1, ?, 'activity.appended', ?, ? FROM seq",
    )
    .bind(i64::from(noise_events))
    .bind(running.id.to_string())
    .bind(payload)
    .bind(now)
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query("UPDATE tasks SET last_event_id = ? WHERE id = ?")
        .bind(inserted.last_insert_rowid())
        .bind(running.id.to_string())
        .execute(&mut *transaction)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
}

pub async fn rejected_task() -> (Store, Task) {
    let store = support::seeded_store().await;
    let queued = support::queued_task(&store).await;
    let running = match store
        .transition_with_event(queued.id, TaskStatus::Queued, TaskTransition::Running)
        .await
        .unwrap()
    {
        TransitionOutcome::Applied { task, .. } => task,
        TransitionOutcome::Conflict { .. } => panic!("fixture transition must apply"),
    };
    let check = required_check();
    let plan = PlanSnapshot::try_structured(
        1,
        "Reject the delivery",
        vec![
            PlanItem::try_structured(
                "step-1",
                "Review",
                "Exercise rejected review state",
                vec!["reviewed".to_owned()],
                PlanItemStatus::Completed,
            )
            .unwrap(),
        ],
        vec![check.clone()],
    )
    .unwrap();
    store
        .append_running_event(running.id, TaskEventPayload::PlanUpdated { plan })
        .await
        .unwrap();
    let running = store.task_detail(running.id).await.unwrap().unwrap().task;
    for round in 1..3 {
        store
            .record_review(
                running.id,
                running.repository_id,
                running.attempt,
                rejected_evidence(round, &check),
            )
            .await
            .unwrap();
    }
    let outcome = store
        .finalize_reviewed_task(
            running.id,
            running.repository_id,
            running.attempt,
            rejected_evidence(3, &check),
        )
        .await
        .unwrap();
    let task = match outcome {
        FinalizeReviewedTaskOutcome::Applied { task, .. }
        | FinalizeReviewedTaskOutcome::Existing { task, .. } => task,
    };
    (store, task)
}

fn rejected_evidence(round: u8, check: &RequiredCheck) -> NewReviewEvidence {
    let digit = char::from(b'a' + round - 1);
    let digest = WorkspaceDigest::try_new(digit.to_string().repeat(64)).unwrap();
    NewReviewEvidence::try_new(
        round,
        ReviewDecisionSource::Reviewer,
        u64::from(round),
        digest.clone(),
        ReviewVerdict::ChangesRequested,
        format!("round {round} rejected"),
        vec![
            ReviewFinding::try_for_review(
                round,
                1,
                FindingSeverity::Blocking,
                "blocking delivery finding",
                None,
                None,
            )
            .unwrap(),
        ],
        Vec::new(),
        vec![check.clone()],
        vec![passed_check_for_round(round, check, &digest)],
        None,
    )
    .unwrap()
}

fn required_check() -> RequiredCheck {
    RequiredCheck::try_cargo_test(
        "project4-delivery-test",
        Some("coding-agent-store".to_owned()),
        Some("delivery_eligibility".to_owned()),
    )
    .unwrap()
}

fn passed_check_for_round(
    round: u8,
    check: &RequiredCheck,
    digest: &WorkspaceDigest,
) -> CheckEvidence {
    CheckEvidence::try_for_check(
        check,
        CheckActor::Executor,
        u32::from(round),
        u64::from(round),
        digest.clone(),
        CheckEvidenceStatus::Passed,
        10,
        "delivery eligibility passed",
        false,
    )
    .unwrap()
}
