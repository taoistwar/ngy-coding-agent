mod delivery_source_support;

use std::convert::Infallible;
use std::path::Path;
#[cfg(feature = "test-support")]
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
#[cfg(feature = "test-support")]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(feature = "test-support")]
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
#[cfg(feature = "test-support")]
use coding_agent_runtime::{
    DeliveryAbortCapability, DeliveryAbortPendingDisposition, DeliveryCandidateTree,
    DeliveryMergeInput, DeliveryMergePendingDisposition, DeliveryPreflightResult,
    DeliveryPreflightSource, DeliverySourceCommitInput, DeliverySourcePendingState,
    DeliverySourceRecoveryDisposition, DeliverySourceRecoveryIntent,
    DeliveryTargetRecoveryCapability, DeliveryTargetRecoveryIntent, DeliveryTargetRequest,
    ProcessLimits, WorktreeProvisioner, build_expected_delivery_merge,
    capture_delivery_abort_proof_from_recovery, classify_delivery_abort_pending,
    classify_delivery_merge_pending, preflight_delivery_merge, retry_delivery_abort_pending,
    retry_delivery_merge_pending,
};
use coding_agent_runtime::{
    DeliveryAbortOutcome, DeliveryAbortPendingAuthorizer, DeliveryAbortProof,
    DeliveryAbortProofCapture, DeliveryExpectedMerge, DeliveryKnownMergeConflict,
    DeliveryMergeOutcome, DeliverySourceCapability, DeliverySourceCommit,
    DeliverySourceProvisioner, DeliveryTargetCapability, DeliveryTargetProvisioner,
    abort_expected_delivery_merge, authorize_persisted_delivery_abort,
    capture_delivery_abort_proof,
};
use delivery_source_support::{Fixture, git_line, git_ok};
#[cfg(feature = "test-support")]
use delivery_source_support::{
    RepositorySnapshot, ReviewedDirtySource, delivery_source_limits, git_with_stdin,
};
use tokio_util::sync::CancellationToken;

const TASK_ID: &str = "123e4567-e89b-12d3-a456-426614174215";
#[cfg(feature = "test-support")]
const EPOCH_SECONDS: i64 = 1_700_000_015;
#[cfg(feature = "test-support")]
const CONFLICT_BASE: &[u8] = b"base line 01\nline 02\nline 03\nline 04\nline 05\nline 06\nline 07\nline 08\nline 09\nline 10\nline 11\nline 12\nline 13\nline 14\nline 15\nline 16\nline 17\nline 18\nline 19\nbase line 20\n";
#[cfg(feature = "test-support")]
const CONFLICT_SOURCE: &[u8] = b"source line 01\nline 02\nline 03\nline 04\nline 05\nline 06\nline 07\nline 08\nline 09\nline 10\nline 11\nline 12\nline 13\nline 14\nline 15\nline 16\nline 17\nline 18\nline 19\nbase line 20\n";
#[cfg(feature = "test-support")]
const CONFLICT_TARGET: &[u8] = b"base line 01\nline 02\nline 03\nline 04\nline 05\nline 06\nline 07\nline 08\nline 09\nline 10\nline 11\nline 12\nline 13\nline 14\nline 15\nline 16\nline 17\nline 18\nline 19\ntarget line 20\n";
#[cfg(feature = "test-support")]
const CLEANLY_APPLIED_BYTES: &[u8] = b"source-only path applied beside the conflict\n";

#[derive(Default)]
struct RecordingAbortPendingAuthorizer {
    called: AtomicBool,
}

#[async_trait]
impl DeliveryAbortPendingAuthorizer for RecordingAbortPendingAuthorizer {
    type Error = Infallible;

    async fn authorize_persisted_abort_pending(
        &self,
        proof: &DeliveryAbortProof,
    ) -> Result<(), Self::Error> {
        assert!(!proof.conflict_paths().is_empty());
        assert!(!self.called.swap(true, AtomicOrdering::SeqCst));
        Ok(())
    }
}

/// The complete MergePending observation space allowed by the Task 15 design.
/// The runtime classifier owns the underlying authenticated observations; this
/// test-side model prevents a future public API from collapsing it to a bool.
#[derive(Clone, Copy)]
enum MergePendingScene {
    OldHeadClean,
    ExactExpectedMergeClean,
    ConflictWithoutDurableChildReceipt,
    Other,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ExpectedMergePendingDisposition {
    RetryExactMerge,
    MergeApplied,
    ReconciliationRequired,
}

const fn expected_merge_pending_disposition(
    scene: MergePendingScene,
) -> ExpectedMergePendingDisposition {
    match scene {
        MergePendingScene::OldHeadClean => ExpectedMergePendingDisposition::RetryExactMerge,
        MergePendingScene::ExactExpectedMergeClean => ExpectedMergePendingDisposition::MergeApplied,
        MergePendingScene::ConflictWithoutDurableChildReceipt | MergePendingScene::Other => {
            ExpectedMergePendingDisposition::ReconciliationRequired
        }
    }
}

/// The complete AbortPending observation space allowed by the Task 15 design.
/// In particular, an autostash that exists or cannot be freshly proved absent
/// can never authorize an abort retry.
#[derive(Clone, Copy)]
enum AbortPendingScene {
    ExactDurableConflictWithoutAutostash,
    OldHeadCleanWithoutMergeState,
    AutostashPresent,
    AutostashUnobservable,
    ConflictDigestChanged,
    Other,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ExpectedAbortPendingDisposition {
    RetryExactAbort,
    AbortApplied,
    ReconciliationRequired,
}

const fn expected_abort_pending_disposition(
    scene: AbortPendingScene,
) -> ExpectedAbortPendingDisposition {
    match scene {
        AbortPendingScene::ExactDurableConflictWithoutAutostash => {
            ExpectedAbortPendingDisposition::RetryExactAbort
        }
        AbortPendingScene::OldHeadCleanWithoutMergeState => {
            ExpectedAbortPendingDisposition::AbortApplied
        }
        AbortPendingScene::AutostashPresent
        | AbortPendingScene::AutostashUnobservable
        | AbortPendingScene::ConflictDigestChanged
        | AbortPendingScene::Other => ExpectedAbortPendingDisposition::ReconciliationRequired,
    }
}

#[test]
fn merge_pending_and_abort_pending_keep_separate_exact_recovery_truth_tables() {
    for (scene, expected) in [
        (
            MergePendingScene::OldHeadClean,
            ExpectedMergePendingDisposition::RetryExactMerge,
        ),
        (
            MergePendingScene::ExactExpectedMergeClean,
            ExpectedMergePendingDisposition::MergeApplied,
        ),
        (
            MergePendingScene::ConflictWithoutDurableChildReceipt,
            ExpectedMergePendingDisposition::ReconciliationRequired,
        ),
        (
            MergePendingScene::Other,
            ExpectedMergePendingDisposition::ReconciliationRequired,
        ),
    ] {
        assert!(expected_merge_pending_disposition(scene) == expected);
    }

    for (scene, expected) in [
        (
            AbortPendingScene::ExactDurableConflictWithoutAutostash,
            ExpectedAbortPendingDisposition::RetryExactAbort,
        ),
        (
            AbortPendingScene::OldHeadCleanWithoutMergeState,
            ExpectedAbortPendingDisposition::AbortApplied,
        ),
        (
            AbortPendingScene::AutostashPresent,
            ExpectedAbortPendingDisposition::ReconciliationRequired,
        ),
        (
            AbortPendingScene::AutostashUnobservable,
            ExpectedAbortPendingDisposition::ReconciliationRequired,
        ),
        (
            AbortPendingScene::ConflictDigestChanged,
            ExpectedAbortPendingDisposition::ReconciliationRequired,
        ),
        (
            AbortPendingScene::Other,
            ExpectedAbortPendingDisposition::ReconciliationRequired,
        ),
    ] {
        assert!(expected_abort_pending_disposition(scene) == expected);
    }
}

/// Deliberately accepts the opaque token only by moving it out of a real
/// child outcome.  This integration test has no raw OID, digest, path, or
/// constructor route that could manufacture a token from a fixture.
fn take_known_conflict(outcome: DeliveryMergeOutcome) -> Option<DeliveryKnownMergeConflict> {
    let DeliveryMergeOutcome::ConflictObserved(known_conflict) = outcome else {
        return None;
    };
    Some(known_conflict)
}

#[test]
fn only_conflict_observed_can_cross_the_opaque_abort_boundary() {
    assert!(take_known_conflict(DeliveryMergeOutcome::Applied).is_none());
    assert!(take_known_conflict(DeliveryMergeOutcome::KnownNotApplied).is_none());
    assert!(take_known_conflict(DeliveryMergeOutcome::ReconciliationRequired).is_none());

    // This function pointer is an external-crate API check: the only positive
    // path to the token is the `ConflictObserved(token)` pattern above.
    let _: fn(DeliveryMergeOutcome) -> Option<DeliveryKnownMergeConflict> = take_known_conflict;
}

/// Compile-contract wiring for the real child path.  The caller must obtain
/// `merge_outcome` from `apply_expected_delivery_merge`; a direct Git fixture
/// is intentionally unable to fabricate its `DeliveryKnownMergeConflict`.
///
/// This consumes the token during proof capture so one known child receipt
/// cannot be replayed into multiple abort attempts.
#[allow(clippy::too_many_arguments, dead_code)]
async fn capture_then_abort_from_minted_conflict(
    fixture: &Fixture,
    source_provisioner: &DeliverySourceProvisioner,
    target_provisioner: &DeliveryTargetProvisioner,
    source: &DeliverySourceCapability,
    target: &DeliveryTargetCapability,
    source_commit: &DeliverySourceCommit,
    expected: &DeliveryExpectedMerge,
    merge_outcome: DeliveryMergeOutcome,
) {
    let known_conflict = take_known_conflict(merge_outcome)
        .expect("only an actual known conflict child may enter abort proof capture");
    let capture = capture_delivery_abort_proof(
        source_provisioner,
        target_provisioner,
        source,
        target,
        source_commit,
        expected,
        known_conflict,
        CancellationToken::new(),
    )
    .await
    .expect("a stable, authenticated conflict must capture an abort proof");
    let DeliveryAbortProofCapture::Proven(proof) = capture else {
        panic!("a durable known conflict may not be silently downgraded before abort");
    };
    assert!(proof.persistence_binding([0; 16]).is_none());
    let persisted_abort = proof
        .persistence_binding([0x42; 16])
        .expect("a real conflict proof accepts one non-nil child receipt identity");
    assert_eq!(persisted_abort.child_receipt_id(), [0x42; 16]);
    assert_eq!(
        persisted_abort.target_branch(),
        format!("refs/heads/{}", target.branch_name())
    );
    assert_eq!(persisted_abort.target_head(), target.head_id());
    assert_eq!(
        persisted_abort.source_branch(),
        format!("refs/heads/{}", source.branch_name())
    );
    assert_eq!(persisted_abort.source_oid(), source_commit.object_id());
    assert_eq!(persisted_abort.merge_head(), source_commit.object_id());
    assert_eq!(
        persisted_abort.common_git_identity_algorithm(),
        "directory_identity_v1"
    );
    assert_eq!(
        persisted_abort.worktree_admin_identity_algorithm(),
        "directory_identity_v1"
    );
    assert_eq!(persisted_abort.fixed_lock_reason(), "codex-reserved");
    assert_eq!(persisted_abort.common_git_identity_digest().len(), 64);
    assert_eq!(persisted_abort.worktree_admin_identity_digest().len(), 64);
    assert_eq!(persisted_abort.source_config_attributes_digest().len(), 64);
    assert_eq!(persisted_abort.index_stages_digest().len(), 64);
    assert_eq!(persisted_abort.worktree_digest().len(), 64);
    assert!(persisted_abort.merge_autostash_is_absent());
    assert!(persisted_abort.other_git_operation_is_clear());
    assert_eq!(persisted_abort.conflict_paths(), proof.conflict_paths());
    assert_eq!(
        format!("{persisted_abort:?}"),
        "DeliveryAbortPersistenceBinding(<redacted>)"
    );
    let authorizer = RecordingAbortPendingAuthorizer::default();
    let capability = authorize_persisted_delivery_abort(proof, &authorizer)
        .await
        .unwrap();

    let outcome = abort_expected_delivery_merge(
        source_provisioner,
        target_provisioner,
        source,
        target,
        source_commit,
        &capability,
        CancellationToken::new(),
    )
    .await
    .expect("the fixed abort child must report a typed outcome");
    let DeliveryAbortOutcome::Applied(applied) = outcome else {
        panic!("the exact abort postcondition must return an opaque applied proof");
    };
    assert_eq!(applied.target_branch(), target.branch_name());
    assert_eq!(applied.target_head_id(), target.head_id());
    assert_eq!(applied.source_commit_id(), source_commit.object_id());
    assert!(!applied.conflict_paths().is_empty());
    let applied_persistence = applied.persistence_binding();
    assert_eq!(applied_persistence.target_head(), target.head_id());
    assert_eq!(applied_persistence.source_oid(), source_commit.object_id());
    assert!(applied_persistence.merge_head_is_absent());
    assert!(applied_persistence.merge_autostash_is_absent());
    assert!(applied_persistence.other_git_operation_is_clear());
    assert!(authorizer.called.load(AtomicOrdering::SeqCst));
    assert_exact_abort_postconditions(fixture, source, target, source_commit);
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn actual_runtime_conflict_is_proved_aborted_and_query_first_idempotent() {
    let PreparedRuntimeConflictScene {
        fixture,
        prepared,
        target,
        target_recovery_intent,
        preflight,
        expected,
        old_target_head,
        source_ref,
        source_head,
        attributes,
    } = PreparedRuntimeConflictScene::new("abort-actual-runtime-conflict").await;
    let mut source_before = prepared.source.snapshot(&fixture.repository);
    let source_lock_before = std::fs::read(prepared.source.admin_directory.join("locked")).unwrap();
    assert!(!attributes.exists());

    let mut recovery_target_provisioner =
        delivery_target_provisioner(&fixture, &prepared.source.worktrees);
    let recovery_target = recovery_target_provisioner
        .open_delivery_target_for_recovery(&target_recovery_intent, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        classify_delivery_merge_pending(
            &prepared.source_provisioner,
            &recovery_target_provisioner,
            &prepared.opened,
            &recovery_target,
            &prepared.candidate,
            &prepared.source_commit,
            &prepared.source_input,
            &expected,
            CancellationToken::new(),
        )
        .await
        .unwrap(),
        DeliveryMergePendingDisposition::RetryExactMerge
    );

    // The last pre-spawn boundary is deliberately after every preflight and
    // collision proof. The built-in binary driver converts the otherwise
    // clean text merge into a real exit-1 conflict without an external merge
    // helper. Restoring the attributes file before outcome classification
    // lets the classifier reauthenticate the original security snapshot.
    let hook_state = Arc::new(AtomicUsize::new(0));
    recovery_target_provisioner.set_actual_merge_boundary_hook_for_tests({
        let hook_state = Arc::clone(&hook_state);
        let attributes = attributes.clone();
        move |phase| match phase {
            "after-last-collision-recheck-before-actual-merge-spawn" => {
                assert_eq!(hook_state.fetch_add(1, Ordering::SeqCst), 0);
                std::fs::write(&attributes, b"tracked.txt merge=binary\n").unwrap();
            }
            "after-actual-merge-child-before-outcome-proof" => {
                assert_eq!(hook_state.fetch_add(1, Ordering::SeqCst), 1);
                std::fs::remove_file(&attributes).unwrap();
            }
            _ => {}
        }
    });

    report_abort_e2e_phase("run actual merge child and prove mixed conflict");
    let merge_outcome = retry_delivery_merge_pending(
        &prepared.source_provisioner,
        &recovery_target_provisioner,
        &prepared.opened,
        &recovery_target,
        &prepared.candidate,
        &prepared.source_commit,
        &prepared.source_input,
        &preflight,
        &expected,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(hook_state.load(Ordering::SeqCst), 2);
    assert!(!attributes.exists());
    let known_conflict = take_known_conflict(merge_outcome)
        .expect("the actual fixed merge child must mint the opaque conflict token");
    assert_eq!(
        format!("{known_conflict:?}"),
        "DeliveryKnownMergeConflict(<opaque>)"
    );
    assert_eq!(
        git_line(&fixture.repository, &["rev-parse", "HEAD"]),
        old_target_head
    );
    assert_eq!(
        git_line(&fixture.repository, &["rev-parse", "MERGE_HEAD"]),
        source_head
    );
    assert_eq!(
        git_line(
            &fixture.repository,
            &["diff", "--name-only", "--diff-filter=U"]
        ),
        "tracked.txt"
    );
    assert_eq!(
        std::fs::read(fixture.repository.join("cleanly-applied.txt")).unwrap(),
        CLEANLY_APPLIED_BYTES,
        "the same merge child must also leave its clean stage-0 path in the conflict scene"
    );
    assert_eq!(prepared.source.snapshot(&fixture.repository), source_before);
    assert_eq!(
        std::fs::read(prepared.source.admin_directory.join("locked")).unwrap(),
        source_lock_before
    );

    report_abort_e2e_phase("capture and authorize durable abort proof");
    let capture = capture_delivery_abort_proof_from_recovery(
        &prepared.source_provisioner,
        &recovery_target_provisioner,
        &prepared.opened,
        &recovery_target,
        &prepared.source_commit,
        &expected,
        known_conflict,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let DeliveryAbortProofCapture::Proven(proof) = capture else {
        panic!("the stable actual-child conflict must yield a durable abort proof");
    };
    assert_eq!(proof.conflict_paths().len(), 1);
    let proof_debug = format!("{proof:?}");
    assert_eq!(format!("{proof:?}"), proof_debug);
    assert!(proof_debug.contains("conflict_path_count: 1"));
    assert!(proof_debug.contains("<redacted>"));
    assert!(!proof_debug.contains("tracked.txt"));
    let authorizer = RecordingAbortPendingAuthorizer::default();
    let capability = authorize_persisted_delivery_abort(proof, &authorizer)
        .await
        .unwrap();
    assert!(authorizer.called.load(AtomicOrdering::SeqCst));

    report_abort_e2e_phase("prove exact abort scene before negative mutations");
    assert_eq!(
        classify_delivery_abort_pending(
            &prepared.source_provisioner,
            &recovery_target_provisioner,
            &prepared.opened,
            &recovery_target,
            &prepared.source_commit,
            &capability,
            CancellationToken::new(),
        )
        .await
        .unwrap(),
        DeliveryAbortPendingDisposition::RetryExactAbort,
        "the freshly captured durable proof must classify its unchanged scene as retryable"
    );
    let target_conflict_before = RawAbortConflictScene::capture(&fixture.repository);
    assert_eq!(prepared.source.snapshot(&fixture.repository), source_before);

    // Every retry re-proves the exact durable scene. An external autostash,
    // worktree edit, untracked file, or source drift must preserve that
    // external state and return reconciliation without spawning `merge
    // --abort`.
    let merge_head_path = fixture.repository.join(".git/MERGE_HEAD");
    let conflict_path = fixture.repository.join("tracked.txt");
    let original_conflict_bytes = std::fs::read(&conflict_path).unwrap();
    let autostash_path = fixture.repository.join(".git/MERGE_AUTOSTASH");
    report_abort_e2e_phase("negative scene: external autostash");
    std::fs::write(&autostash_path, b"external autostash marker\n").unwrap();
    assert_abort_retry_reconciles_without_spawning_abort(
        &prepared,
        &recovery_target_provisioner,
        &recovery_target,
        &capability,
    )
    .await;
    assert_eq!(
        std::fs::read(&autostash_path).unwrap(),
        b"external autostash marker\n"
    );
    assert_eq!(
        git_line(&fixture.repository, &["rev-parse", "MERGE_HEAD"]),
        source_head
    );
    std::fs::remove_file(&autostash_path).unwrap();
    assert_restored_abort_scene(
        "external autostash cleanup",
        &fixture,
        &prepared,
        &source_before,
        &target_conflict_before,
    );

    report_abort_e2e_phase("negative scene: conflict worktree digest drift");
    let mut externally_changed_conflict = original_conflict_bytes.clone();
    externally_changed_conflict.extend_from_slice(b"external conflict edit\n");
    std::fs::write(&conflict_path, &externally_changed_conflict).unwrap();
    assert_abort_retry_reconciles_without_spawning_abort(
        &prepared,
        &recovery_target_provisioner,
        &recovery_target,
        &capability,
    )
    .await;
    assert_eq!(
        std::fs::read(&conflict_path).unwrap(),
        externally_changed_conflict
    );
    assert!(merge_head_path.exists());
    std::fs::write(&conflict_path, &original_conflict_bytes).unwrap();
    assert_restored_abort_scene(
        "conflict worktree cleanup",
        &fixture,
        &prepared,
        &source_before,
        &target_conflict_before,
    );

    let external_untracked = fixture.repository.join("external-untracked.txt");
    report_abort_e2e_phase("negative scene: extra untracked path");
    std::fs::write(&external_untracked, b"must never be cleaned\n").unwrap();
    assert_abort_retry_reconciles_without_spawning_abort(
        &prepared,
        &recovery_target_provisioner,
        &recovery_target,
        &capability,
    )
    .await;
    assert_eq!(
        std::fs::read(&external_untracked).unwrap(),
        b"must never be cleaned\n"
    );
    assert!(merge_head_path.exists());
    std::fs::remove_file(&external_untracked).unwrap();
    assert_restored_abort_scene(
        "untracked-path cleanup",
        &fixture,
        &prepared,
        &source_before,
        &target_conflict_before,
    );

    let source_drift_path = prepared.source.worktree_path().join("cleanly-applied.txt");
    let source_drift_before = std::fs::read(&source_drift_path).unwrap();
    report_abort_e2e_phase("negative scene: source drift");
    std::fs::write(&source_drift_path, b"external source drift\n").unwrap();
    assert_abort_retry_reconciles_without_spawning_abort(
        &prepared,
        &recovery_target_provisioner,
        &recovery_target,
        &capability,
    )
    .await;
    assert_eq!(
        std::fs::read(&source_drift_path).unwrap(),
        b"external source drift\n"
    );
    assert!(merge_head_path.exists());
    std::fs::write(&source_drift_path, source_drift_before).unwrap();
    assert_restored_abort_scene(
        "source bytes cleanup before stat-cache refresh",
        &fixture,
        &prepared,
        &source_before,
        &target_conflict_before,
    );
    // `require_applied_source_state` deliberately includes the real index's
    // stat-cache state. Rewriting a tracked source file invalidates that cache
    // even when the original bytes are written back. Restore the same
    // post-commit invariant established by the source-commit path before the
    // happy abort is allowed to continue.
    git_ok(
        prepared.source.worktree_path(),
        &["update-index", "--refresh", "-q"],
    );
    git_ok(
        prepared.source.worktree_path(),
        &["diff-files", "--quiet", "--"],
    );
    source_before = prepared.source.snapshot(&fixture.repository);
    assert_eq!(
        RawAbortConflictScene::capture(&fixture.repository),
        target_conflict_before,
        "source stat-cache cleanup must not change the target conflict scene"
    );

    // The original target capability is no longer used for any mutation.
    // The fresh recovery binding must still classify the exact durable scene.
    report_abort_e2e_phase("classify exact durable conflict and run abort child");
    assert_eq!(
        classify_delivery_abort_pending(
            &prepared.source_provisioner,
            &recovery_target_provisioner,
            &prepared.opened,
            &recovery_target,
            &prepared.source_commit,
            &capability,
            CancellationToken::new(),
        )
        .await
        .unwrap(),
        DeliveryAbortPendingDisposition::RetryExactAbort
    );

    let first_abort = retry_delivery_abort_pending(
        &prepared.source_provisioner,
        &recovery_target_provisioner,
        &prepared.opened,
        &recovery_target,
        &prepared.source_commit,
        &capability,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let DeliveryAbortOutcome::Applied(first_applied) = first_abort else {
        panic!("the first exact abort must return a proven applied postcondition");
    };
    assert_eq!(first_applied.target_branch(), target.branch_name());
    assert_eq!(first_applied.target_head_id(), old_target_head);
    assert_eq!(first_applied.source_commit_id(), source_head);
    assert_eq!(first_applied.conflict_paths().len(), 1);
    assert!(!fixture.repository.join("cleanly-applied.txt").exists());
    assert_exact_abort_postconditions(&fixture, &prepared.opened, &target, &prepared.source_commit);
    assert_eq!(prepared.source.snapshot(&fixture.repository), source_before);
    assert_eq!(
        std::fs::read(prepared.source.admin_directory.join("locked")).unwrap(),
        source_lock_before
    );
    assert_eq!(
        git_line(&fixture.repository, &["rev-parse", &source_ref]),
        source_head
    );
    report_abort_e2e_phase("classify applied abort and replay query-first recovery");
    let applied_disposition = classify_delivery_abort_pending(
        &prepared.source_provisioner,
        &recovery_target_provisioner,
        &prepared.opened,
        &recovery_target,
        &prepared.source_commit,
        &capability,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let DeliveryAbortPendingDisposition::AbortApplied(classified_applied) = applied_disposition
    else {
        panic!("the old clean target must carry an opaque abort-applied proof");
    };
    assert_eq!(classified_applied.target_head_id(), old_target_head);
    assert_eq!(classified_applied.source_commit_id(), source_head);
    assert_eq!(classified_applied.conflict_paths().len(), 1);

    // With no MERGE_HEAD left, a second raw `git merge --abort` child cannot
    // succeed. Applied here therefore exercises the runtime's query-first
    // lost-reply path and reuses the same opaque proof without another child.
    let replayed_abort = retry_delivery_abort_pending(
        &prepared.source_provisioner,
        &recovery_target_provisioner,
        &prepared.opened,
        &recovery_target,
        &prepared.source_commit,
        &capability,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let DeliveryAbortOutcome::Applied(replayed_applied) = replayed_abort else {
        panic!("query-first recovery must return the proven applied postcondition");
    };
    assert_eq!(replayed_applied.target_head_id(), old_target_head);
    assert_eq!(replayed_applied.source_commit_id(), source_head);
    assert_eq!(replayed_applied.conflict_paths().len(), 1);
    assert!(!fixture.repository.join("cleanly-applied.txt").exists());
    assert_exact_abort_postconditions(&fixture, &prepared.opened, &target, &prepared.source_commit);
    assert_eq!(prepared.source.snapshot(&fixture.repository), source_before);
    assert_eq!(
        std::fs::read(prepared.source.admin_directory.join("locked")).unwrap(),
        source_lock_before
    );
    assert!(!attributes.exists());
    report_abort_e2e_phase("complete");
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn actual_merge_stage3_mode_tampering_cannot_mint_abort_token() {
    let PreparedRuntimeConflictScene {
        fixture,
        prepared,
        target: _,
        target_recovery_intent,
        preflight,
        expected,
        old_target_head,
        source_ref: _,
        source_head,
        attributes,
    } = PreparedRuntimeConflictScene::new("abort-stage3-mode-tampering").await;
    let source_before = prepared.source.snapshot(&fixture.repository);
    let source_lock_before = std::fs::read(prepared.source.admin_directory.join("locked")).unwrap();
    let source_tree_path = format!("{source_head}:tracked.txt");
    let expected_stage3_oid = git_line(&fixture.repository, &["rev-parse", &source_tree_path]);
    assert_eq!(
        git_line(
            &fixture.repository,
            &["ls-tree", &source_head, "--", "tracked.txt"]
        ),
        format!("100644 blob {expected_stage3_oid}\ttracked.txt"),
        "the fixture source must bind stage 3 to the non-executable mode"
    );

    let mut recovery_target_provisioner =
        delivery_target_provisioner(&fixture, &prepared.source.worktrees);
    let recovery_target = recovery_target_provisioner
        .open_delivery_target_for_recovery(&target_recovery_intent, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        classify_delivery_merge_pending(
            &prepared.source_provisioner,
            &recovery_target_provisioner,
            &prepared.opened,
            &recovery_target,
            &prepared.candidate,
            &prepared.source_commit,
            &prepared.source_input,
            &expected,
            CancellationToken::new(),
        )
        .await
        .unwrap(),
        DeliveryMergePendingDisposition::RetryExactMerge
    );

    let hook_state = Arc::new(AtomicUsize::new(0));
    let post_child_worktree = Arc::new(Mutex::new(None));
    recovery_target_provisioner.set_actual_merge_boundary_hook_for_tests({
        let hook_state = Arc::clone(&hook_state);
        let post_child_worktree = Arc::clone(&post_child_worktree);
        let attributes = attributes.clone();
        let repository = fixture.repository.clone();
        let expected_stage3_oid = expected_stage3_oid.clone();
        move |phase| match phase {
            "after-last-collision-recheck-before-actual-merge-spawn" => {
                assert_eq!(hook_state.fetch_add(1, Ordering::SeqCst), 0);
                std::fs::write(&attributes, b"tracked.txt merge=binary\n").unwrap();
            }
            "after-actual-merge-child-before-outcome-proof" => {
                assert_eq!(hook_state.fetch_add(1, Ordering::SeqCst), 1);
                std::fs::remove_file(&attributes).unwrap();

                // Keep the source blob but substitute an executable stage-3
                // mode. Both values are individually canonical, so only exact
                // base/target/source attribution can reject this scene.
                let tampered_stage3 = format!("100755 {expected_stage3_oid} 3\ttracked.txt\0");
                git_with_stdin(
                    &repository,
                    &["update-index", "--add", "--replace", "-z", "--index-info"],
                    tampered_stage3.as_bytes(),
                );
                *post_child_worktree.lock().unwrap() =
                    Some(std::fs::read(repository.join("tracked.txt")).unwrap());
            }
            _ => {}
        }
    });

    report_abort_e2e_phase("run actual merge with post-child stage3 mode tampering");
    let merge_outcome = retry_delivery_merge_pending(
        &prepared.source_provisioner,
        &recovery_target_provisioner,
        &prepared.opened,
        &recovery_target,
        &prepared.candidate,
        &prepared.source_commit,
        &prepared.source_input,
        &preflight,
        &expected,
        CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_eq!(hook_state.load(Ordering::SeqCst), 2);
    assert!(!attributes.exists());
    assert!(matches!(
        &merge_outcome,
        DeliveryMergeOutcome::ReconciliationRequired
    ));
    assert!(
        take_known_conflict(merge_outcome).is_none(),
        "a stage-attribution mismatch must not expose an abort-capable token"
    );
    assert_eq!(
        git_line(&fixture.repository, &["rev-parse", "HEAD"]),
        old_target_head
    );
    assert_eq!(
        git_line(&fixture.repository, &["rev-parse", "MERGE_HEAD"]),
        source_head
    );
    let tampered_stage3 = format!("100755 {expected_stage3_oid} 3\ttracked.txt");
    let index = git_line(
        &fixture.repository,
        &["ls-files", "--stage", "--", "tracked.txt"],
    );
    assert_eq!(
        index.lines().count(),
        3,
        "reconciliation must preserve the complete unmerged index: {index}"
    );
    assert!(
        index.lines().any(|line| line == tampered_stage3.as_str()),
        "runtime outcome proof must preserve the externally tampered stage 3: {index}"
    );
    assert_eq!(
        git_line(
            &fixture.repository,
            &["diff", "--name-only", "--diff-filter=U"]
        ),
        "tracked.txt",
        "the stage tamper must remain an unresolved conflict"
    );
    let post_child_worktree = post_child_worktree.lock().unwrap().take().unwrap();
    assert_eq!(
        std::fs::read(fixture.repository.join("tracked.txt")).unwrap(),
        post_child_worktree,
        "reconciliation must not rewrite the conflicted worktree"
    );
    assert_eq!(
        std::fs::read(fixture.repository.join("cleanly-applied.txt")).unwrap(),
        CLEANLY_APPLIED_BYTES,
        "the merge scene must remain present because no abort is authorized"
    );
    assert_eq!(prepared.source.snapshot(&fixture.repository), source_before);
    assert_eq!(
        std::fs::read(prepared.source.admin_directory.join("locked")).unwrap(),
        source_lock_before
    );
}

/// A real Git conflict is used here only to validate the on-disk scene and
/// fixed `merge --abort` postcondition that the opaque runtime proof must
/// authorize. It never manufactures a runtime conflict token.
#[tokio::test]
async fn real_git_conflict_has_the_exact_clean_abort_postcondition() {
    let fixture = Fixture::new("abort-real-conflict").await;
    let source = fixture.reviewed_dirty_source(TASK_ID).await;
    git_ok(source.worktree_path(), &["add", "--all"]);
    git_ok(
        source.worktree_path(),
        &[
            "commit",
            "--quiet",
            "--no-gpg-sign",
            "-m",
            "source side of real abort conflict",
        ],
    );
    let source_ref = format!("refs/heads/{}", source.reservation.branch_name());
    let source_head = git_line(&fixture.repository, &["rev-parse", &source_ref]);

    std::fs::write(
        fixture.repository.join("tracked.txt"),
        b"target side of real abort conflict\n",
    )
    .unwrap();
    git_ok(&fixture.repository, &["add", "--", "tracked.txt"]);
    git_ok(
        &fixture.repository,
        &[
            "commit",
            "--quiet",
            "--no-gpg-sign",
            "-m",
            "target side of real abort conflict",
        ],
    );
    let old_target_head = git_line(&fixture.repository, &["rev-parse", "HEAD"]);

    run_real_conflicting_merge(&fixture.repository, &source_head);
    assert_eq!(
        git_line(&fixture.repository, &["rev-parse", "HEAD"]),
        old_target_head
    );
    assert_eq!(
        std::fs::read_to_string(fixture.repository.join(".git/MERGE_HEAD"))
            .unwrap()
            .trim(),
        source_head
    );
    assert!(!git_line(&fixture.repository, &["status", "--porcelain=v2"]).is_empty());

    git_ok(&fixture.repository, &["merge", "--abort"]);
    assert_eq!(
        git_line(&fixture.repository, &["rev-parse", "HEAD"]),
        old_target_head
    );
    assert_eq!(
        git_line(&fixture.repository, &["rev-parse", &source_ref]),
        source_head
    );
    assert_clean_target_without_merge_state(&fixture.repository);
}

#[cfg(feature = "test-support")]
struct PreparedRuntimeConflictScene {
    fixture: Fixture,
    prepared: PreparedRuntimeConflictSource,
    target: DeliveryTargetCapability,
    target_recovery_intent: DeliveryTargetRecoveryIntent,
    preflight: DeliveryPreflightResult,
    expected: DeliveryExpectedMerge,
    old_target_head: String,
    source_ref: String,
    source_head: String,
    attributes: PathBuf,
}

#[cfg(feature = "test-support")]
impl PreparedRuntimeConflictScene {
    async fn new(name: &str) -> Self {
        report_abort_e2e_phase("prepare fixture and source commit");
        let fixture = Fixture::new(name).await;

        // Give the common base enough structure for ordinary text merge to
        // prove that the source and target edits do not overlap.
        std::fs::write(fixture.repository.join("tracked.txt"), CONFLICT_BASE).unwrap();
        git_ok(&fixture.repository, &["add", "--", "tracked.txt"]);
        git_ok(
            &fixture.repository,
            &[
                "commit",
                "--quiet",
                "--no-gpg-sign",
                "-m",
                "three-line conflict base",
            ],
        );

        let prepared = PreparedRuntimeConflictSource::new(&fixture).await;
        std::fs::write(fixture.repository.join("tracked.txt"), CONFLICT_TARGET).unwrap();
        git_ok(&fixture.repository, &["add", "--", "tracked.txt"]);
        git_ok(
            &fixture.repository,
            &[
                "commit",
                "--quiet",
                "--no-gpg-sign",
                "-m",
                "non-overlapping target edit",
            ],
        );

        let fixture_target_head = git_line(&fixture.repository, &["rev-parse", "HEAD"]);
        let fixture_source_ref = format!("refs/heads/{}", prepared.opened.branch_name());
        let fixture_source_head =
            git_line(&fixture.repository, &["rev-parse", &fixture_source_ref]);
        let raw_preflight = Command::new("git")
            .arg("--no-pager")
            .arg("-c")
            .arg("core.hooksPath=")
            .arg("-c")
            .arg("commit.gpgSign=false")
            .arg("-C")
            .arg(&fixture.repository)
            .args([
                "merge-tree",
                "--write-tree",
                "--messages",
                "--name-only",
                "-z",
                &fixture_target_head,
                &fixture_source_head,
            ])
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", null_device())
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
            .unwrap();
        assert_eq!(
            raw_preflight.status.code(),
            Some(0),
            "the fixture itself must be clean before exercising runtime preflight: {}",
            String::from_utf8_lossy(&raw_preflight.stderr)
        );

        report_abort_e2e_phase("build authenticated target and expected merge");
        let target_provisioner = delivery_target_provisioner(&fixture, &prepared.source.worktrees);
        let target = target_provisioner
            .open_delivery_target(&delivery_target_request(&fixture), CancellationToken::new())
            .await
            .unwrap();
        let target_recovery_intent = DeliveryTargetRecoveryIntent::from_live(&target);
        let preflight = preflight_delivery_merge(
            &prepared.source_provisioner,
            &target_provisioner,
            &target,
            DeliveryPreflightSource::committed(
                &prepared.opened,
                &prepared.candidate,
                &prepared.source_commit,
                &prepared.source_input,
            ),
            CancellationToken::new(),
        )
        .await
        .unwrap_or_else(|error| {
            let phase = match error {
                coding_agent_runtime::DeliveryPreflightError::Target(error) => {
                    format!("target:{}", error.code())
                }
                coding_agent_runtime::DeliveryPreflightError::Source(error) => {
                    format!("source:{}", error.code())
                }
                coding_agent_runtime::DeliveryPreflightError::SourceAlreadyInTarget => {
                    "source-already-in-target".to_owned()
                }
                coding_agent_runtime::DeliveryPreflightError::MalformedMergeTreeOutput => {
                    "malformed-merge-tree-output".to_owned()
                }
                coding_agent_runtime::DeliveryPreflightError::Internal => "internal".to_owned(),
            };
            panic!(
                "the non-overlapping text edits must be preflight-clean: {} ({phase})",
                error.code(),
            )
        });
        let merge_input = DeliveryMergeInput::try_new(TASK_ID, 1, EPOCH_SECONDS).unwrap();
        let expected = build_expected_delivery_merge(
            &prepared.source_provisioner,
            &target_provisioner,
            &prepared.opened,
            &target,
            &prepared.candidate,
            &prepared.source_commit,
            &prepared.source_input,
            &preflight,
            &merge_input,
            CancellationToken::new(),
        )
        .await
        .unwrap();

        let old_target_head = target.head_id().to_owned();
        let source_ref = format!("refs/heads/{}", prepared.opened.branch_name());
        let source_head = prepared.source_commit.object_id().to_owned();
        let attributes = fixture.repository.join(".git/info/attributes");
        assert!(!attributes.exists());

        Self {
            fixture,
            prepared,
            target,
            target_recovery_intent,
            preflight,
            expected,
            old_target_head,
            source_ref,
            source_head,
            attributes,
        }
    }
}

#[cfg(feature = "test-support")]
struct PreparedRuntimeConflictSource {
    source: ReviewedDirtySource,
    source_provisioner: DeliverySourceProvisioner,
    opened: DeliverySourceCapability,
    candidate: DeliveryCandidateTree,
    source_commit: DeliverySourceCommit,
    source_input: DeliverySourceCommitInput,
}

#[cfg(feature = "test-support")]
impl PreparedRuntimeConflictSource {
    async fn new(fixture: &Fixture) -> Self {
        let mut source = fixture.reviewed_dirty_source(TASK_ID).await;
        std::fs::write(source.worktree_path().join("tracked.txt"), CONFLICT_SOURCE).unwrap();
        std::fs::write(
            source.worktree_path().join("cleanly-applied.txt"),
            CLEANLY_APPLIED_BYTES,
        )
        .unwrap();
        std::fs::remove_file(source.worktree_path().join("review-note.txt")).unwrap();
        source.approved_fingerprint = fixture.current_fingerprint(&source).await;

        let source_provisioner = fixture.delivery_source(&source.worktrees).unwrap();
        let opened = source_provisioner
            .open_delivery_source(
                &source.reservation,
                source.approved_fingerprint,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let candidate = source_provisioner
            .build_candidate_tree(&opened, CancellationToken::new())
            .await
            .unwrap();
        let source_input = DeliverySourceCommitInput::try_new(TASK_ID, 1, EPOCH_SECONDS).unwrap();
        let source_commit = source_provisioner
            .build_source_commit(&opened, &candidate, &source_input, CancellationToken::new())
            .await
            .unwrap();
        let intent = DeliverySourceRecoveryIntent::from_source(
            DeliverySourcePendingState::CommitPending,
            &opened,
            &candidate,
            Some(&source_commit),
            source_input.clone(),
        )
        .unwrap();
        let recovery = source_provisioner
            .open_delivery_source_for_recovery(
                &source.reservation,
                &intent,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            source_provisioner
                .apply_source_commit(&recovery, CancellationToken::new())
                .await
                .unwrap(),
            DeliverySourceRecoveryDisposition::Applied
        );
        drop(recovery);

        Self {
            source,
            source_provisioner,
            opened,
            candidate,
            source_commit,
            source_input,
        }
    }
}

#[cfg(feature = "test-support")]
async fn assert_abort_retry_reconciles_without_spawning_abort(
    prepared: &PreparedRuntimeConflictSource,
    target_provisioner: &DeliveryTargetProvisioner,
    target: &DeliveryTargetRecoveryCapability,
    capability: &DeliveryAbortCapability,
) {
    // `retry_delivery_abort_pending` is itself query-first: it re-proves the
    // complete source/target scene before it may spawn `git merge --abort`.
    // Calling the public classifier immediately beforehand would duplicate
    // that expensive authenticated observation without strengthening the
    // negative evidence below.
    let outcome = retry_delivery_abort_pending(
        &prepared.source_provisioner,
        target_provisioner,
        &prepared.opened,
        target,
        &prepared.source_commit,
        capability,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(matches!(
        outcome,
        DeliveryAbortOutcome::ReconciliationRequired
    ));
}

#[cfg(feature = "test-support")]
fn report_abort_e2e_phase(phase: &str) {
    eprintln!("[delivery-abort-e2e] {phase}");
}

#[cfg(feature = "test-support")]
#[derive(Debug, PartialEq, Eq)]
struct RawAbortConflictScene {
    unmerged_index: Vec<u8>,
    complete_index: Vec<u8>,
    porcelain: Vec<u8>,
    tracked_worktree: Vec<u8>,
    cleanly_applied_worktree: Vec<u8>,
    git_state: Vec<(&'static str, Option<Vec<u8>>)>,
}

#[cfg(feature = "test-support")]
impl RawAbortConflictScene {
    fn capture(repository: &Path) -> Self {
        Self {
            unmerged_index: controlled_git_read(
                repository,
                &["ls-files", "--unmerged", "-z", "--"],
            ),
            complete_index: controlled_git_read(repository, &["ls-files", "--stage", "-z", "--"]),
            porcelain: controlled_git_read(
                repository,
                &[
                    "status",
                    "--porcelain=v2",
                    "--untracked-files=all",
                    "-z",
                    "--",
                ],
            ),
            tracked_worktree: std::fs::read(repository.join("tracked.txt")).unwrap(),
            cleanly_applied_worktree: std::fs::read(repository.join("cleanly-applied.txt"))
                .unwrap(),
            git_state: [
                "AUTO_MERGE",
                "MERGE_AUTOSTASH",
                "MERGE_HEAD",
                "MERGE_MODE",
                "MERGE_MSG",
                "ORIG_HEAD",
            ]
            .into_iter()
            .map(|name| {
                let path = repository.join(".git").join(name);
                let bytes = match std::fs::read(path) {
                    Ok(bytes) => Some(bytes),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                    Err(error) => panic!("failed to read target Git state {name}: {error}"),
                };
                (name, bytes)
            })
            .collect(),
        }
    }
}

#[cfg(feature = "test-support")]
fn assert_restored_abort_scene(
    phase: &str,
    fixture: &Fixture,
    prepared: &PreparedRuntimeConflictSource,
    expected_source: &RepositorySnapshot,
    expected_target: &RawAbortConflictScene,
) {
    assert_eq!(
        prepared.source.snapshot(&fixture.repository),
        *expected_source,
        "{phase} did not restore the raw source ref/index/worktree snapshot"
    );
    assert_eq!(
        RawAbortConflictScene::capture(&fixture.repository),
        *expected_target,
        "{phase} did not restore the raw target conflict scene"
    );
}

#[cfg(feature = "test-support")]
fn controlled_git_read(repository: &Path, arguments: &[&str]) -> Vec<u8> {
    let output = Command::new("git")
        .arg("--no-pager")
        .arg("--no-optional-locks")
        .arg("-c")
        .arg("core.hooksPath=")
        .arg("-c")
        .arg("commit.gpgSign=false")
        .arg("-C")
        .arg(repository)
        .args(arguments)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", null_device())
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "read-only Git diagnostic failed: git -C {} {}\nstderr: {}",
        repository.display(),
        arguments.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

#[cfg(feature = "test-support")]
fn delivery_target_provisioner(
    fixture: &Fixture,
    worktrees: &WorktreeProvisioner,
) -> DeliveryTargetProvisioner {
    DeliveryTargetProvisioner::from_worktree_provisioner(
        worktrees,
        Arc::clone(&fixture.delivery_git),
        &fixture.runtime_directory,
        fixture.task_process_scope(),
        abort_test_process_limits(),
        delivery_source_limits(),
    )
    .unwrap()
}

#[cfg(feature = "test-support")]
fn delivery_target_request(fixture: &Fixture) -> DeliveryTargetRequest {
    DeliveryTargetRequest::try_new(
        git_line(&fixture.repository, &["symbolic-ref", "--short", "HEAD"]),
        git_line(&fixture.repository, &["rev-parse", "HEAD"]),
    )
    .unwrap()
}

#[cfg(feature = "test-support")]
fn abort_test_process_limits() -> ProcessLimits {
    ProcessLimits::try_new(
        512 * 1024,
        512 * 1024,
        std::time::Duration::from_secs(30),
        std::time::Duration::from_secs(5),
    )
    .unwrap()
}

fn assert_exact_abort_postconditions(
    fixture: &Fixture,
    source: &DeliverySourceCapability,
    target: &DeliveryTargetCapability,
    source_commit: &DeliverySourceCommit,
) {
    assert_eq!(
        git_line(&fixture.repository, &["rev-parse", "HEAD"]),
        target.head_id()
    );
    let source_ref = format!("refs/heads/{}", source.branch_name());
    assert_eq!(
        git_line(&fixture.repository, &["rev-parse", &source_ref]),
        source_commit.object_id()
    );
    assert_clean_target_without_merge_state(&fixture.repository);
}

fn run_real_conflicting_merge(repository: &Path, source_commit: &str) {
    let output = Command::new("git")
        .arg("--no-pager")
        .arg("-C")
        .arg(repository)
        .args([
            "merge",
            "--no-ff",
            "--no-commit",
            "--no-edit",
            "--no-gpg-sign",
            "--no-autostash",
            "--",
            source_commit,
        ])
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", null_device())
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(1),
        "real fixture merge must produce a known conflict: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_clean_target_without_merge_state(repository: &Path) {
    assert!(git_line(repository, &["status", "--porcelain=v2"]).is_empty());
    for entry in [
        "AUTO_MERGE",
        "MERGE_AUTOSTASH",
        "MERGE_HEAD",
        "MERGE_MODE",
        "MERGE_MSG",
    ] {
        assert!(
            !repository.join(".git").join(entry).exists(),
            "unexpected merge state entry {entry}"
        );
    }
}

#[cfg(windows)]
fn null_device() -> &'static str {
    "NUL"
}

#[cfg(unix)]
fn null_device() -> &'static str {
    "/dev/null"
}
