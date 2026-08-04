use std::str::FromStr;

use coding_agent_store::{
    AdvanceDeliverySourceObjectRequest, CreateDeliverySourceRequest, DeliverySourceAppliedProof,
    DeliverySourceObjectProof, DeliverySourceTransitionOutcome, GitBranchRef, GitCommitOid,
    SourceWorktreeProof,
};

use super::fixtures::{accepted_fixture, created_source, source_anchor};
use crate::support::delivery::eligibility::SOURCE_COMMIT;

const SECRET_MARKER: &str = "task5-secret-marker-must-not-leak";

#[tokio::test]
async fn source_requests_proofs_outcomes_and_invariants_redact_untrusted_values() {
    let (store, command) = accepted_fixture().await;
    let source = created_source(&store, command.clone()).await;
    let mut metadata = source.commit_metadata.clone();
    metadata.message_bytes = SECRET_MARKER.as_bytes().to_vec();
    let object = DeliverySourceObjectProof::try_new(
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        source.candidate_tree.clone(),
        vec![source.expected_parent.clone()],
        metadata,
    )
    .unwrap();
    let request = AdvanceDeliverySourceObjectRequest::try_new(
        source_anchor(&command),
        source.version,
        object.clone(),
    )
    .unwrap();
    assert_redacted(&format!("{object:?}"));
    assert_redacted(&format!("{request:?}"));
    let outcome = store.advance_delivery_source_object(request).await.unwrap();
    assert!(matches!(outcome, DeliverySourceTransitionOutcome::Conflict));
    assert_redacted(&format!("{outcome:?}"));

    let create = CreateDeliverySourceRequest::try_new(command.clone()).unwrap();
    let create_debug = format!("{create:?}");
    assert!(!create_debug.contains(command.canonical_request_hash().as_str()));
    assert!(create_debug.contains("<redacted>"));

    let oid = GitCommitOid::from_str(SOURCE_COMMIT).unwrap();
    let worktree = SourceWorktreeProof::try_new(
        source.candidate_tree.clone(),
        source.candidate_tree.clone(),
        0,
        0,
        0,
        0,
    )
    .unwrap();
    let applied = DeliverySourceAppliedProof::try_new(
        object,
        GitBranchRef::from_str("refs/heads/redaction-check").unwrap(),
        oid.clone(),
        oid,
        worktree,
        source.provenance.common_git_identity.clone(),
        source.provenance.worktree_admin_identity.clone(),
        SECRET_MARKER.to_owned(),
        source.provenance.config_attributes_digest.clone(),
    )
    .unwrap();
    assert_redacted(&format!("{applied:?}"));
}

fn assert_redacted(value: &str) {
    assert!(!value.contains(SECRET_MARKER));
    assert!(value.contains("<redacted>") || value.contains("Conflict"));
}
