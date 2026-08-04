use coding_agent_store::Sha256Digest;

use crate::support::delivery::eligibility::{
    ApprovedEvidenceVariant, approved_task_with_evidence_variant,
};

#[tokio::test]
async fn evidence_digests_are_sensitive_only_to_their_framed_inputs() {
    let baseline = evidence_digests(ApprovedEvidenceVariant::Baseline, "baseline").await;
    let required_check =
        evidence_digests(ApprovedEvidenceVariant::RequiredCheck, "required-check").await;
    let check_evidence =
        evidence_digests(ApprovedEvidenceVariant::CheckEvidence, "check-evidence").await;
    let coverage = evidence_digests(ApprovedEvidenceVariant::Coverage, "coverage").await;

    assert_ne!(required_check.0, baseline.0);
    assert_eq!(required_check.1, baseline.1);
    assert_ne!(check_evidence.0, baseline.0);
    assert_eq!(check_evidence.1, baseline.1);
    assert_eq!(coverage.0, baseline.0);
    assert_ne!(coverage.1, baseline.1);
    assert_ne!(baseline.0, baseline.1);
}

async fn evidence_digests(
    variant: ApprovedEvidenceVariant,
    suffix: &str,
) -> (Sha256Digest, Sha256Digest) {
    let branch = format!("codex/task-evidence-{suffix}");
    let (store, task) = approved_task_with_evidence_variant(&branch, variant).await;
    let snapshot = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    let identity = snapshot.evidence_identity.unwrap();
    (
        identity.checks_digest().clone(),
        identity.coverage_digest().clone(),
    )
}
