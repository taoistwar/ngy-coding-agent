#![cfg(feature = "test-support")]

mod delivery_source_support;

use std::convert::Infallible;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use coding_agent_runtime::{
    DeliveryAbortCapability, DeliveryAbortOutcome, DeliveryAbortPendingAuthorizer,
    DeliveryAbortProof, DeliveryAbortProofCapture, DeliveryBranchCleanupIntent,
    DeliveryCandidateTree, DeliveryDeletePendingAuthorizer, DeliveryDeletePendingCapability,
    DeliveryDeletePendingDisposition, DeliveryMergeInput, DeliveryMergeOutcome,
    DeliveryPreflightResult, DeliveryPreflightSource, DeliveryRemovePendingAuthorizer,
    DeliveryRemovePendingCapability, DeliveryRemovePendingDisposition, DeliverySourceCapability,
    DeliverySourceCommit, DeliverySourceCommitInput, DeliverySourcePendingState,
    DeliverySourceProvisioner, DeliverySourceRecoveryDisposition, DeliverySourceRecoveryIntent,
    DeliveryTargetCapability, DeliveryTargetProvisioner, DeliveryTargetRequest,
    DeliveryUnlockPendingAuthorizer, DeliveryUnlockPendingCapability,
    DeliveryUnlockPendingDisposition, DeliveryWorktreeCleanupIntent,
    DeliveryWorktreeCleanupProvisioner, FingerprintLimits, ProcessFault, ProcessFaultController,
    ProcessFaultEventKind, ProcessLimits, SealedProcessLivenessScope, WorktreeProvisioner,
    abort_expected_delivery_merge, apply_expected_delivery_merge,
    authorize_persisted_delivery_abort, authorize_persisted_delivery_branch_delete,
    authorize_persisted_delivery_remove, authorize_persisted_delivery_unlock,
    build_expected_delivery_merge, capture_delivery_abort_proof, preflight_delivery_merge,
};
use delivery_source_support::{
    Fixture, ReviewedDirtySource, delivery_source_limits, git_line, git_ok,
};
use tokio_util::sync::CancellationToken;

const EPOCH_SECONDS: i64 = 1_700_000_018;
const ZERO_LIVE_TIMEOUT: Duration = Duration::from_secs(5);
const ACTUAL_MERGE_CHILD_ORDINAL: u64 = 419;
const ABORT_CHILD_ORDINAL: u64 = 69;
const UNLOCK_CHILD_ORDINAL: u64 = 77;
const REMOVE_CHILD_ORDINAL: u64 = 77;
const ATOMIC_DELETE_CHILD_ORDINAL: u64 = 41;
const CONFLICT_BASE: &[u8] = b"base line 01\nline 02\nline 03\nline 04\nline 05\nline 06\nline 07\nline 08\nline 09\nline 10\nline 11\nline 12\nline 13\nline 14\nline 15\nline 16\nline 17\nline 18\nline 19\nbase line 20\n";
const CONFLICT_SOURCE: &[u8] = b"source line 01\nline 02\nline 03\nline 04\nline 05\nline 06\nline 07\nline 08\nline 09\nline 10\nline 11\nline 12\nline 13\nline 14\nline 15\nline 16\nline 17\nline 18\nline 19\nbase line 20\n";
const CONFLICT_TARGET: &[u8] = b"base line 01\nline 02\nline 03\nline 04\nline 05\nline 06\nline 07\nline 08\nline 09\nline 10\nline 11\nline 12\nline 13\nline 14\nline 15\nline 16\nline 17\nline 18\nline 19\ntarget line 20\n";
const CLEANLY_APPLIED_BYTES: &[u8] = b"source-only path applied beside the conflict\n";
struct PreparedMerge {
    fixture: Fixture,
    source: ReviewedDirtySource,
    source_provisioner: DeliverySourceProvisioner,
    opened: DeliverySourceCapability,
    candidate: DeliveryCandidateTree,
    source_commit: DeliverySourceCommit,
    source_input: DeliverySourceCommitInput,
    target_provisioner: DeliveryTargetProvisioner,
    target: DeliveryTargetCapability,
    preflight: DeliveryPreflightResult,
    expected: coding_agent_runtime::DeliveryExpectedMerge,
    old_target_head: String,
}

impl PreparedMerge {
    async fn new(name: &str, task_id: &str) -> Self {
        let fixture = Fixture::new(name).await;
        let source = fixture.reviewed_dirty_source(task_id).await;
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
        let source_input = DeliverySourceCommitInput::try_new(task_id, 1, EPOCH_SECONDS).unwrap();
        let source_commit = source_provisioner
            .build_source_commit(&opened, &candidate, &source_input, CancellationToken::new())
            .await
            .unwrap();
        let recovery_intent = DeliverySourceRecoveryIntent::from_source(
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
                &recovery_intent,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            source_provisioner
                .apply_source_commit(&recovery, CancellationToken::new())
                .await
                .unwrap(),
            DeliverySourceRecoveryDisposition::Applied,
        );
        drop(recovery);

        let target_provisioner = target_provisioner(&fixture, &source.worktrees);
        let target = target_provisioner
            .open_delivery_target(&target_request(&fixture), CancellationToken::new())
            .await
            .unwrap();
        let old_target_head = target.head_id().to_owned();
        let preflight = preflight_delivery_merge(
            &source_provisioner,
            &target_provisioner,
            &target,
            DeliveryPreflightSource::committed(&opened, &candidate, &source_commit, &source_input),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let merge_input = DeliveryMergeInput::try_new(task_id, 1, EPOCH_SECONDS).unwrap();
        let expected = build_expected_delivery_merge(
            &source_provisioner,
            &target_provisioner,
            &opened,
            &target,
            &candidate,
            &source_commit,
            &source_input,
            &preflight,
            &merge_input,
            CancellationToken::new(),
        )
        .await
        .unwrap();

        Self {
            fixture,
            source,
            source_provisioner,
            opened,
            candidate,
            source_commit,
            source_input,
            target_provisioner,
            target,
            preflight,
            expected,
            old_target_head,
        }
    }

    async fn apply(
        &self,
    ) -> Result<DeliveryMergeOutcome, coding_agent_runtime::DeliveryMergeError> {
        apply_expected_delivery_merge(
            &self.source_provisioner,
            &self.target_provisioner,
            &self.opened,
            &self.target,
            &self.candidate,
            &self.source_commit,
            &self.source_input,
            &self.preflight,
            &self.expected,
            CancellationToken::new(),
        )
        .await
    }

    fn reset_target(&self) {
        git_ok(
            &self.fixture.repository,
            &["reset", "--hard", "--quiet", &self.old_target_head],
        );
    }
}

struct PreparedAbort {
    fixture: Fixture,
    source: ReviewedDirtySource,
    source_provisioner: DeliverySourceProvisioner,
    opened: DeliverySourceCapability,
    source_commit: DeliverySourceCommit,
    target_provisioner: DeliveryTargetProvisioner,
    target: DeliveryTargetCapability,
    capability: DeliveryAbortCapability,
    attributes: std::path::PathBuf,
}

impl PreparedAbort {
    async fn new(name: &str, task_id: &str) -> Self {
        let fixture = Fixture::new(name).await;
        std::fs::write(fixture.repository.join("tracked.txt"), CONFLICT_BASE).unwrap();
        git_ok(&fixture.repository, &["add", "--", "tracked.txt"]);
        git_ok(
            &fixture.repository,
            &[
                "commit",
                "--quiet",
                "--no-gpg-sign",
                "-m",
                "mutation fault conflict base",
            ],
        );

        let mut source = fixture.reviewed_dirty_source(task_id).await;
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
        let source_input = DeliverySourceCommitInput::try_new(task_id, 1, EPOCH_SECONDS).unwrap();
        let source_commit = source_provisioner
            .build_source_commit(&opened, &candidate, &source_input, CancellationToken::new())
            .await
            .unwrap();
        let recovery_intent = DeliverySourceRecoveryIntent::from_source(
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
                &recovery_intent,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            source_provisioner
                .apply_source_commit(&recovery, CancellationToken::new())
                .await
                .unwrap(),
            DeliverySourceRecoveryDisposition::Applied,
        );
        drop(recovery);

        std::fs::write(fixture.repository.join("tracked.txt"), CONFLICT_TARGET).unwrap();
        git_ok(&fixture.repository, &["add", "--", "tracked.txt"]);
        git_ok(
            &fixture.repository,
            &[
                "commit",
                "--quiet",
                "--no-gpg-sign",
                "-m",
                "mutation fault target edit",
            ],
        );

        let mut target_provisioner = target_provisioner(&fixture, &source.worktrees);
        let target = target_provisioner
            .open_delivery_target(&target_request(&fixture), CancellationToken::new())
            .await
            .unwrap();
        let preflight = preflight_delivery_merge(
            &source_provisioner,
            &target_provisioner,
            &target,
            DeliveryPreflightSource::committed(&opened, &candidate, &source_commit, &source_input),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(preflight.is_ready());
        let merge_input = DeliveryMergeInput::try_new(task_id, 1, EPOCH_SECONDS).unwrap();
        let expected = build_expected_delivery_merge(
            &source_provisioner,
            &target_provisioner,
            &opened,
            &target,
            &candidate,
            &source_commit,
            &source_input,
            &preflight,
            &merge_input,
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let attributes = fixture.repository.join(".git/info/attributes");
        target_provisioner.set_actual_merge_boundary_hook_for_tests({
            let attributes = attributes.clone();
            move |phase| match phase {
                "after-last-collision-recheck-before-actual-merge-spawn" => {
                    std::fs::write(&attributes, b"tracked.txt merge=binary\n").unwrap();
                }
                "after-actual-merge-child-before-outcome-proof" => {
                    std::fs::remove_file(&attributes).unwrap();
                }
                _ => {}
            }
        });
        let outcome = apply_expected_delivery_merge(
            &source_provisioner,
            &target_provisioner,
            &opened,
            &target,
            &candidate,
            &source_commit,
            &source_input,
            &preflight,
            &expected,
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let DeliveryMergeOutcome::ConflictObserved(conflict) = outcome else {
            panic!("binary-driver actual merge must yield a known conflict");
        };
        let capture = capture_delivery_abort_proof(
            &source_provisioner,
            &target_provisioner,
            &opened,
            &target,
            &source_commit,
            &expected,
            conflict,
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let DeliveryAbortProofCapture::Proven(proof) = capture else {
            panic!("stable runtime conflict must yield an abort proof");
        };
        let capability = authorize_persisted_delivery_abort(proof, &AcceptAbortProof)
            .await
            .unwrap();

        Self {
            fixture,
            source,
            source_provisioner,
            opened,
            source_commit,
            target_provisioner,
            target,
            capability,
            attributes,
        }
    }

    async fn abort(
        &self,
    ) -> Result<DeliveryAbortOutcome, coding_agent_runtime::DeliveryAbortError> {
        abort_expected_delivery_merge(
            &self.source_provisioner,
            &self.target_provisioner,
            &self.opened,
            &self.target,
            &self.source_commit,
            &self.capability,
            CancellationToken::new(),
        )
        .await
    }

    fn recreate_conflict(&self, task_id: &str) {
        if self.fixture.repository.join(".git/MERGE_HEAD").exists() {
            git_ok(&self.fixture.repository, &["merge", "--abort"]);
        }
        assert_target_clean(&self.fixture.repository);
        std::fs::write(&self.attributes, b"tracked.txt merge=binary\n").unwrap();
        run_matching_conflicting_merge(
            &self.fixture.repository,
            self.source_commit.object_id(),
            task_id,
        );
        std::fs::remove_file(&self.attributes).unwrap();
    }
}

struct AcceptAbortProof;

#[async_trait]
impl DeliveryAbortPendingAuthorizer for AcceptAbortProof {
    type Error = Infallible;

    async fn authorize_persisted_abort_pending(
        &self,
        _proof: &DeliveryAbortProof,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

struct PreparedWorktreeCleanup {
    cleanup: DeliveryWorktreeCleanupProvisioner,
    source_provisioner: DeliverySourceProvisioner,
    intent: DeliveryWorktreeCleanupIntent,
    sealed_worker: SealedProcessLivenessScope,
    source_commit: String,
    source: ReviewedDirtySource,
    fixture: Fixture,
}

impl PreparedWorktreeCleanup {
    async fn new(name: &str, task_id: &str) -> Self {
        let fixture = Fixture::new(name).await;
        let source = fixture.reviewed_dirty_source(task_id).await;
        let worker_process_scope = source.worker_process_scope.clone();
        let delivery_process_scope = delivery_process_scope(&worker_process_scope);
        let source_provisioner = source_provisioner_for_cleanup(
            &fixture,
            &source.worktrees,
            delivery_process_scope.clone(),
        );
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
        let source_input = DeliverySourceCommitInput::try_new(task_id, 1, EPOCH_SECONDS).unwrap();
        let source_commit = source_provisioner
            .build_source_commit(&opened, &candidate, &source_input, CancellationToken::new())
            .await
            .unwrap();
        let recovery_intent = DeliverySourceRecoveryIntent::from_source(
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
                &recovery_intent,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            source_provisioner
                .apply_source_commit(&recovery, CancellationToken::new())
                .await
                .unwrap(),
            DeliverySourceRecoveryDisposition::Applied,
        );
        drop(recovery);
        git_ok(
            &fixture.repository,
            &[
                "merge",
                "--quiet",
                "--no-ff",
                "--no-edit",
                "--message",
                "mutation fault cleanup fixture merge",
                source_commit.object_id(),
            ],
        );

        let cleanup = DeliveryWorktreeCleanupProvisioner::from_worktree_provisioner(
            &source.worktrees,
            Arc::clone(&fixture.delivery_git),
            &fixture.runtime_directory,
            delivery_process_scope,
            process_limits(),
            delivery_source_limits(),
        )
        .unwrap();
        let sealed_worker = worker_process_scope
            .seal_task_scope(worker_task_id())
            .unwrap();
        let intent = cleanup
            .capture_intent(
                &source_provisioner,
                &source.reservation,
                opened,
                &candidate,
                &source_commit,
                &source_input,
                &sealed_worker,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let source_commit = source_commit.object_id().to_owned();

        Self {
            cleanup,
            source_provisioner,
            intent,
            sealed_worker,
            source_commit,
            source,
            fixture,
        }
    }

    async fn retry_unlock(
        &self,
        capability: DeliveryUnlockPendingCapability,
    ) -> Result<DeliveryUnlockPendingDisposition, coding_agent_runtime::DeliveryWorktreeCleanupError>
    {
        self.cleanup
            .retry_delivery_unlock_pending(
                &self.source_provisioner,
                capability,
                &self.sealed_worker,
                CancellationToken::new(),
            )
            .await
    }

    async fn retry_remove(
        &self,
        capability: DeliveryRemovePendingCapability,
    ) -> Result<DeliveryRemovePendingDisposition, coding_agent_runtime::DeliveryWorktreeCleanupError>
    {
        self.cleanup
            .retry_delivery_remove_pending(
                &self.source_provisioner,
                capability,
                &self.sealed_worker,
                CancellationToken::new(),
            )
            .await
    }

    fn raw_unlock(&self) {
        worktree_command(&self.fixture, &self.source, &["unlock"]);
    }

    fn is_locked(&self) -> bool {
        self.source.admin_directory.join("locked").is_file()
    }
}

struct AcceptCleanupIntent {
    expected: DeliveryWorktreeCleanupIntent,
}

impl AcceptCleanupIntent {
    fn new(intent: &DeliveryWorktreeCleanupIntent) -> Self {
        Self {
            expected: intent.clone(),
        }
    }

    fn require_exact(&self, intent: &DeliveryWorktreeCleanupIntent) -> Result<(), &'static str> {
        if self.expected.is_same_runtime_intent(intent) {
            Ok(())
        } else {
            Err("cleanup intent mismatch")
        }
    }
}

#[async_trait]
impl DeliveryUnlockPendingAuthorizer for AcceptCleanupIntent {
    type Error = &'static str;

    async fn authorize_persisted_unlock_pending(
        &self,
        intent: &DeliveryWorktreeCleanupIntent,
    ) -> Result<(), Self::Error> {
        self.require_exact(intent)
    }
}

#[async_trait]
impl DeliveryRemovePendingAuthorizer for AcceptCleanupIntent {
    type Error = &'static str;

    async fn authorize_persisted_remove_pending(
        &self,
        intent: &DeliveryWorktreeCleanupIntent,
    ) -> Result<(), Self::Error> {
        self.require_exact(intent)
    }
}

struct PreparedBranchCleanup {
    cleanup: DeliveryWorktreeCleanupProvisioner,
    intent: DeliveryBranchCleanupIntent,
    sealed_worker: SealedProcessLivenessScope,
    source_commit: String,
    source_ref: String,
    target_head: String,
    target_ref: String,
    source: ReviewedDirtySource,
    fixture: Fixture,
}

impl PreparedBranchCleanup {
    async fn new(name: &str, task_id: &str) -> Self {
        let prepared = PreparedWorktreeCleanup::new(name, task_id).await;
        let source_ref = format!("refs/heads/{}", prepared.source.reservation.branch_name());
        let target_branch = git_line(
            &prepared.fixture.repository,
            &["symbolic-ref", "--short", "HEAD"],
        );
        let target_ref = format!("refs/heads/{target_branch}");
        let target_head = git_line(&prepared.fixture.repository, &["rev-parse", "HEAD"]);
        prepared.raw_unlock();
        worktree_command(&prepared.fixture, &prepared.source, &["remove"]);
        assert!(!prepared.source.worktree_path().exists());
        assert!(!prepared.source.admin_directory.exists());

        let target_provisioner = DeliveryTargetProvisioner::from_worktree_provisioner(
            &prepared.source.worktrees,
            Arc::clone(&prepared.fixture.delivery_git),
            &prepared.fixture.runtime_directory,
            delivery_process_scope(&prepared.source.worker_process_scope),
            process_limits(),
            delivery_source_limits(),
        )
        .unwrap();
        let target = target_provisioner
            .open_delivery_target(
                &DeliveryTargetRequest::try_new(&target_branch, &target_head).unwrap(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let intent = prepared
            .cleanup
            .capture_branch_cleanup_intent(
                &prepared.source_provisioner,
                prepared.intent,
                target,
                &prepared.sealed_worker,
                CancellationToken::new(),
            )
            .await
            .unwrap();

        Self {
            cleanup: prepared.cleanup,
            intent,
            sealed_worker: prepared.sealed_worker,
            source_commit: prepared.source_commit,
            source_ref,
            target_head,
            target_ref,
            source: prepared.source,
            fixture: prepared.fixture,
        }
    }

    async fn retry_delete(
        &self,
        capability: DeliveryDeletePendingCapability,
    ) -> Result<DeliveryDeletePendingDisposition, coding_agent_runtime::DeliveryWorktreeCleanupError>
    {
        self.cleanup
            .retry_delivery_delete_pending(
                capability,
                &self.sealed_worker,
                CancellationToken::new(),
            )
            .await
    }

    fn restore_source_ref(&self) {
        git_ok(
            &self.fixture.repository,
            &["update-ref", &self.source_ref, &self.source_commit],
        );
    }

    fn source_ref_oid(&self) -> Option<String> {
        let object_id = git_line(
            &self.fixture.repository,
            &["for-each-ref", "--format=%(objectname)", &self.source_ref],
        );
        (!object_id.is_empty()).then_some(object_id)
    }
}

struct AcceptBranchCleanupIntent {
    expected: DeliveryBranchCleanupIntent,
}

#[async_trait]
impl DeliveryDeletePendingAuthorizer for AcceptBranchCleanupIntent {
    type Error = &'static str;

    async fn authorize_persisted_delete_pending(
        &self,
        intent: &DeliveryBranchCleanupIntent,
    ) -> Result<(), Self::Error> {
        if self.expected.is_same_runtime_intent(intent) {
            Ok(())
        } else {
            Err("branch cleanup intent mismatch")
        }
    }
}

#[tokio::test]
async fn actual_merge_before_spawn_is_known_not_applied_and_zero_live() {
    assert_actual_merge_fault(
        "mutation-fault-merge-before-spawn",
        "123e4567-e89b-12d3-a456-426614174501",
        ProcessFault::BeforeSpawn,
    )
    .await;
}

macro_rules! actual_merge_fault_test {
    ($test:ident, $fixture:literal, $task_id:literal, $fault:expr) => {
        #[tokio::test]
        async fn $test() {
            assert_actual_merge_fault($fixture, $task_id, $fault).await;
        }
    };
}

actual_merge_fault_test!(
    actual_merge_after_spawn_unknown_truth_table_and_zero_live,
    "mutation-fault-merge-after-spawn",
    "123e4567-e89b-12d3-a456-426614174502",
    ProcessFault::AfterSpawnUnknown
);
actual_merge_fault_test!(
    actual_merge_stdout_overflow_truth_table_and_zero_live,
    "mutation-fault-merge-stdout-overflow",
    "123e4567-e89b-12d3-a456-426614174503",
    ProcessFault::StdoutOverflow
);
actual_merge_fault_test!(
    actual_merge_deadline_truth_table_and_zero_live,
    "mutation-fault-merge-deadline",
    "123e4567-e89b-12d3-a456-426614174504",
    ProcessFault::Deadline
);
actual_merge_fault_test!(
    actual_merge_wait_unknown_truth_table_and_zero_live,
    "mutation-fault-merge-wait-unknown",
    "123e4567-e89b-12d3-a456-426614174505",
    ProcessFault::WaitUnknown
);
actual_merge_fault_test!(
    actual_merge_channel_unknown_truth_table_and_zero_live,
    "mutation-fault-merge-channel-unknown",
    "123e4567-e89b-12d3-a456-426614174506",
    ProcessFault::ChannelUnknown
);
actual_merge_fault_test!(
    actual_merge_kill_failure_truth_table_and_zero_live,
    "mutation-fault-merge-kill-failure",
    "123e4567-e89b-12d3-a456-426614174507",
    ProcessFault::KillFailure
);
actual_merge_fault_test!(
    actual_merge_cleanup_failure_truth_table_and_zero_live,
    "mutation-fault-merge-cleanup-failure",
    "123e4567-e89b-12d3-a456-426614174508",
    ProcessFault::CleanupFailure
);

async fn assert_actual_merge_fault(name: &str, task_id: &str, fault: ProcessFault) {
    let mut prepared = PreparedMerge::new(name, task_id).await;
    let controller = ProcessFaultController::for_child(ACTUAL_MERGE_CHILD_ORDINAL, fault).unwrap();
    prepared
        .target_provisioner
        .set_actual_merge_boundary_hook_for_tests({
            let controller = controller.clone();
            move |phase| {
                if phase == "after-last-collision-recheck-before-actual-merge-spawn" {
                    let admitted = controller
                        .events()
                        .into_iter()
                        .filter(|event| event.kind() == ProcessFaultEventKind::Admitted)
                        .count() as u64;
                    assert_eq!(
                        admitted + 1,
                        ACTUAL_MERGE_CHILD_ORDINAL,
                        "actual merge child ordinal drifted before {fault:?}"
                    );
                }
            }
        });

    let outcome = controller.scope(prepared.apply()).await.unwrap();
    let observed_children =
        assert_controlled_child_was_reaped(&controller, ACTUAL_MERGE_CHILD_ORDINAL, fault).await;
    assert_closing_observation_policy(ACTUAL_MERGE_CHILD_ORDINAL, observed_children, fault);
    assert_fault_event_order(
        &controller,
        ACTUAL_MERGE_CHILD_ORDINAL,
        observed_children,
        fault,
    );
    assert_actual_merge_fault_truth(&prepared, fault, &outcome);
    clear_proven_stale_fixture_index_lock(
        &prepared.fixture.repository,
        fault,
        matches!(outcome, DeliveryMergeOutcome::ReconciliationRequired),
    );

    prepared.reset_target();
    assert_eq!(
        git_line(&prepared.fixture.repository, &["rev-parse", "HEAD"]),
        prepared.old_target_head,
        "{fault:?}",
    );
    assert_eq!(
        git_line(
            &prepared.fixture.repository,
            &["status", "--porcelain=v2", "--untracked-files=all"],
        ),
        "",
        "{fault:?}",
    );
    assert_zero_live(&prepared.source).await;
}

fn assert_actual_merge_fault_truth(
    prepared: &PreparedMerge,
    fault: ProcessFault,
    outcome: &DeliveryMergeOutcome,
) {
    match fault {
        ProcessFault::BeforeSpawn => {
            assert!(matches!(outcome, DeliveryMergeOutcome::KnownNotApplied));
        }
        ProcessFault::StdoutOverflow | ProcessFault::ChannelUnknown => {
            assert!(matches!(outcome, DeliveryMergeOutcome::Applied));
        }
        ProcessFault::KillFailure | ProcessFault::CleanupFailure => {
            assert!(matches!(
                outcome,
                DeliveryMergeOutcome::ReconciliationRequired
            ));
        }
        ProcessFault::AfterSpawnUnknown | ProcessFault::Deadline | ProcessFault::WaitUnknown => {
            assert!(matches!(
                outcome,
                DeliveryMergeOutcome::KnownNotApplied
                    | DeliveryMergeOutcome::Applied
                    | DeliveryMergeOutcome::ReconciliationRequired
            ));
        }
    }

    match outcome {
        DeliveryMergeOutcome::KnownNotApplied => {
            assert_eq!(
                git_line(&prepared.fixture.repository, &["rev-parse", "HEAD"]),
                prepared.old_target_head,
                "{fault:?}",
            );
            assert_target_clean(&prepared.fixture.repository);
        }
        DeliveryMergeOutcome::Applied => {
            assert_eq!(
                git_line(&prepared.fixture.repository, &["rev-parse", "HEAD"]),
                prepared.expected.object_id(),
                "{fault:?}",
            );
            assert_target_clean(&prepared.fixture.repository);
        }
        DeliveryMergeOutcome::ReconciliationRequired => {}
        DeliveryMergeOutcome::ConflictObserved(_) => {
            panic!("{fault:?}: a clean preflight fault may not mint a conflict proof")
        }
    }
}

#[tokio::test]
async fn abort_before_spawn_is_known_not_applied_and_zero_live() {
    assert_abort_fault(
        "mutation-fault-abort-before-spawn",
        "123e4567-e89b-12d3-a456-426614174511",
        ProcessFault::BeforeSpawn,
    )
    .await;
}

macro_rules! abort_fault_test {
    ($test:ident, $fixture:literal, $task_id:literal, $fault:expr) => {
        #[tokio::test]
        async fn $test() {
            assert_abort_fault($fixture, $task_id, $fault).await;
        }
    };
}

abort_fault_test!(
    abort_after_spawn_unknown_truth_table_and_zero_live,
    "mutation-fault-abort-after-spawn",
    "123e4567-e89b-12d3-a456-426614174512",
    ProcessFault::AfterSpawnUnknown
);
abort_fault_test!(
    abort_stdout_overflow_truth_table_and_zero_live,
    "mutation-fault-abort-stdout-overflow",
    "123e4567-e89b-12d3-a456-426614174513",
    ProcessFault::StdoutOverflow
);
abort_fault_test!(
    abort_deadline_truth_table_and_zero_live,
    "mutation-fault-abort-deadline",
    "123e4567-e89b-12d3-a456-426614174514",
    ProcessFault::Deadline
);
abort_fault_test!(
    abort_wait_unknown_truth_table_and_zero_live,
    "mutation-fault-abort-wait-unknown",
    "123e4567-e89b-12d3-a456-426614174515",
    ProcessFault::WaitUnknown
);
abort_fault_test!(
    abort_channel_unknown_truth_table_and_zero_live,
    "mutation-fault-abort-channel-unknown",
    "123e4567-e89b-12d3-a456-426614174516",
    ProcessFault::ChannelUnknown
);
abort_fault_test!(
    abort_kill_failure_truth_table_and_zero_live,
    "mutation-fault-abort-kill-failure",
    "123e4567-e89b-12d3-a456-426614174517",
    ProcessFault::KillFailure
);
abort_fault_test!(
    abort_cleanup_failure_truth_table_and_zero_live,
    "mutation-fault-abort-cleanup-failure",
    "123e4567-e89b-12d3-a456-426614174518",
    ProcessFault::CleanupFailure
);

async fn assert_abort_fault(name: &str, task_id: &str, fault: ProcessFault) {
    let mut prepared = PreparedAbort::new(name, task_id).await;
    let controller = ProcessFaultController::for_child(ABORT_CHILD_ORDINAL, fault).unwrap();
    prepared
        .target_provisioner
        .set_actual_merge_boundary_hook_for_tests({
            let controller = controller.clone();
            move |phase| {
                if phase == "before-actual-abort-spawn" {
                    let admitted = controller
                        .events()
                        .into_iter()
                        .filter(|event| event.kind() == ProcessFaultEventKind::Admitted)
                        .count() as u64;
                    assert_eq!(
                        admitted + 1,
                        ABORT_CHILD_ORDINAL,
                        "abort child ordinal drifted before {fault:?}"
                    );
                }
            }
        });

    let outcome = controller.scope(prepared.abort()).await.unwrap();
    let observed_children =
        assert_controlled_child_was_reaped(&controller, ABORT_CHILD_ORDINAL, fault).await;
    assert_closing_observation_policy(ABORT_CHILD_ORDINAL, observed_children, fault);
    assert_fault_event_order(&controller, ABORT_CHILD_ORDINAL, observed_children, fault);
    assert_abort_fault_truth(&prepared, fault, &outcome);
    clear_proven_stale_fixture_index_lock(
        &prepared.fixture.repository,
        fault,
        matches!(outcome, DeliveryAbortOutcome::ReconciliationRequired),
    );

    prepared.recreate_conflict(task_id);
    assert_eq!(
        git_line(&prepared.fixture.repository, &["rev-parse", "MERGE_HEAD"]),
        prepared.source_commit.object_id(),
        "{fault:?}",
    );
    assert_zero_live(&prepared.source).await;
}

fn assert_abort_fault_truth(
    prepared: &PreparedAbort,
    fault: ProcessFault,
    outcome: &DeliveryAbortOutcome,
) {
    match fault {
        ProcessFault::BeforeSpawn => {
            assert!(matches!(outcome, DeliveryAbortOutcome::KnownNotApplied));
        }
        ProcessFault::StdoutOverflow | ProcessFault::ChannelUnknown => {
            assert!(matches!(outcome, DeliveryAbortOutcome::Applied(_)));
        }
        ProcessFault::KillFailure | ProcessFault::CleanupFailure => {
            assert!(matches!(
                outcome,
                DeliveryAbortOutcome::ReconciliationRequired
            ));
        }
        ProcessFault::AfterSpawnUnknown | ProcessFault::Deadline | ProcessFault::WaitUnknown => {
            assert!(matches!(
                outcome,
                DeliveryAbortOutcome::KnownNotApplied
                    | DeliveryAbortOutcome::Applied(_)
                    | DeliveryAbortOutcome::ReconciliationRequired
            ));
        }
    }

    match outcome {
        DeliveryAbortOutcome::KnownNotApplied => {
            assert_eq!(
                git_line(&prepared.fixture.repository, &["rev-parse", "MERGE_HEAD"]),
                prepared.source_commit.object_id(),
                "{fault:?}",
            );
        }
        DeliveryAbortOutcome::Applied(_) => {
            assert_target_clean(&prepared.fixture.repository);
        }
        DeliveryAbortOutcome::ReconciliationRequired => {}
    }
}

macro_rules! unlock_fault_test {
    ($test:ident, $fixture:literal, $task_id:literal, $fault:expr) => {
        #[tokio::test]
        async fn $test() {
            assert_unlock_fault($fixture, $task_id, $fault).await;
        }
    };
}

unlock_fault_test!(
    unlock_before_spawn_truth_table_and_zero_live,
    "mutation-fault-unlock-before-spawn",
    "123e4567-e89b-12d3-a456-426614174521",
    ProcessFault::BeforeSpawn
);
unlock_fault_test!(
    unlock_after_spawn_unknown_truth_table_and_zero_live,
    "mutation-fault-unlock-after-spawn",
    "123e4567-e89b-12d3-a456-426614174522",
    ProcessFault::AfterSpawnUnknown
);
unlock_fault_test!(
    unlock_stdout_overflow_truth_table_and_zero_live,
    "mutation-fault-unlock-stdout-overflow",
    "123e4567-e89b-12d3-a456-426614174523",
    ProcessFault::StdoutOverflow
);
unlock_fault_test!(
    unlock_deadline_truth_table_and_zero_live,
    "mutation-fault-unlock-deadline",
    "123e4567-e89b-12d3-a456-426614174524",
    ProcessFault::Deadline
);
unlock_fault_test!(
    unlock_wait_unknown_truth_table_and_zero_live,
    "mutation-fault-unlock-wait-unknown",
    "123e4567-e89b-12d3-a456-426614174525",
    ProcessFault::WaitUnknown
);
unlock_fault_test!(
    unlock_channel_unknown_truth_table_and_zero_live,
    "mutation-fault-unlock-channel-unknown",
    "123e4567-e89b-12d3-a456-426614174526",
    ProcessFault::ChannelUnknown
);
unlock_fault_test!(
    unlock_kill_failure_truth_table_and_zero_live,
    "mutation-fault-unlock-kill-failure",
    "123e4567-e89b-12d3-a456-426614174527",
    ProcessFault::KillFailure
);
unlock_fault_test!(
    unlock_cleanup_failure_truth_table_and_zero_live,
    "mutation-fault-unlock-cleanup-failure",
    "123e4567-e89b-12d3-a456-426614174528",
    ProcessFault::CleanupFailure
);

async fn assert_unlock_fault(name: &str, task_id: &str, fault: ProcessFault) {
    let mut prepared = PreparedWorktreeCleanup::new(name, task_id).await;
    let controller = ProcessFaultController::for_child(UNLOCK_CHILD_ORDINAL, fault).unwrap();
    prepared.cleanup.set_cleanup_boundary_hook_for_tests({
        let controller = controller.clone();
        move |phase| {
            if phase == "before-actual-unlock-spawn" {
                let admitted = controller
                    .events()
                    .into_iter()
                    .filter(|event| event.kind() == ProcessFaultEventKind::Admitted)
                    .count() as u64;
                assert_eq!(
                    admitted + 1,
                    UNLOCK_CHILD_ORDINAL,
                    "unlock child ordinal drifted before {fault:?}"
                );
            }
        }
    });

    let result = controller
        .scope(prepared.retry_unlock(unlock_capability(&prepared.intent).await))
        .await;
    let observed_children =
        assert_controlled_child_was_reaped(&controller, UNLOCK_CHILD_ORDINAL, fault).await;
    assert_fault_event_order(&controller, UNLOCK_CHILD_ORDINAL, observed_children, fault);
    assert_unlock_fault_truth(&prepared, fault, result);
    assert_zero_live(&prepared.source).await;
}

fn assert_unlock_fault_truth(
    prepared: &PreparedWorktreeCleanup,
    fault: ProcessFault,
    result: Result<
        DeliveryUnlockPendingDisposition,
        coding_agent_runtime::DeliveryWorktreeCleanupError,
    >,
) {
    assert!(prepared.source.worktree_path().is_dir(), "{fault:?}");
    assert!(prepared.source.admin_directory.is_dir(), "{fault:?}");
    let locked = prepared.is_locked();
    match fault {
        ProcessFault::BeforeSpawn => {
            assert_eq!(
                result,
                Ok(DeliveryUnlockPendingDisposition::RetryExactUnlock)
            );
            assert!(locked);
        }
        ProcessFault::StdoutOverflow | ProcessFault::ChannelUnknown => {
            assert_eq!(result, Ok(DeliveryUnlockPendingDisposition::UnlockApplied));
            assert!(!locked);
        }
        ProcessFault::KillFailure | ProcessFault::CleanupFailure => {
            assert_eq!(
                result,
                Err(coding_agent_runtime::DeliveryWorktreeCleanupError::ProcessCleanupUnproven)
            );
            assert!(!locked);
        }
        ProcessFault::AfterSpawnUnknown | ProcessFault::Deadline | ProcessFault::WaitUnknown => {
            match result {
                Ok(DeliveryUnlockPendingDisposition::RetryExactUnlock) if locked => {}
                Ok(DeliveryUnlockPendingDisposition::UnlockApplied) if !locked => {}
                result => panic!(
                    "{fault:?}: query-first unlock result {result:?} mismatched locked={locked}"
                ),
            }
        }
    }
}

macro_rules! remove_fault_test {
    ($test:ident, $fixture:literal, $task_id:literal, $fault:expr) => {
        #[tokio::test]
        async fn $test() {
            assert_remove_fault($fixture, $task_id, $fault).await;
        }
    };
}

remove_fault_test!(
    remove_before_spawn_truth_table_and_zero_live,
    "mutation-fault-remove-before-spawn",
    "123e4567-e89b-12d3-a456-426614174531",
    ProcessFault::BeforeSpawn
);
remove_fault_test!(
    remove_after_spawn_unknown_truth_table_and_zero_live,
    "mutation-fault-remove-after-spawn",
    "123e4567-e89b-12d3-a456-426614174532",
    ProcessFault::AfterSpawnUnknown
);
remove_fault_test!(
    remove_stdout_overflow_truth_table_and_zero_live,
    "mutation-fault-remove-stdout-overflow",
    "123e4567-e89b-12d3-a456-426614174533",
    ProcessFault::StdoutOverflow
);
remove_fault_test!(
    remove_deadline_truth_table_and_zero_live,
    "mutation-fault-remove-deadline",
    "123e4567-e89b-12d3-a456-426614174534",
    ProcessFault::Deadline
);
remove_fault_test!(
    remove_wait_unknown_truth_table_and_zero_live,
    "mutation-fault-remove-wait-unknown",
    "123e4567-e89b-12d3-a456-426614174535",
    ProcessFault::WaitUnknown
);
remove_fault_test!(
    remove_channel_unknown_truth_table_and_zero_live,
    "mutation-fault-remove-channel-unknown",
    "123e4567-e89b-12d3-a456-426614174536",
    ProcessFault::ChannelUnknown
);
remove_fault_test!(
    remove_kill_failure_truth_table_and_zero_live,
    "mutation-fault-remove-kill-failure",
    "123e4567-e89b-12d3-a456-426614174537",
    ProcessFault::KillFailure
);
remove_fault_test!(
    remove_cleanup_failure_truth_table_and_zero_live,
    "mutation-fault-remove-cleanup-failure",
    "123e4567-e89b-12d3-a456-426614174538",
    ProcessFault::CleanupFailure
);

async fn assert_remove_fault(name: &str, task_id: &str, fault: ProcessFault) {
    let prepared = PreparedWorktreeCleanup::new(name, task_id).await;
    prepared.raw_unlock();
    let controller = ProcessFaultController::for_child(REMOVE_CHILD_ORDINAL, fault).unwrap();

    let result = controller
        .scope(prepared.retry_remove(remove_capability(&prepared.intent).await))
        .await;
    let observed_children =
        assert_controlled_child_was_reaped(&controller, REMOVE_CHILD_ORDINAL, fault).await;
    assert_fault_event_order(&controller, REMOVE_CHILD_ORDINAL, observed_children, fault);
    assert_remove_fault_truth(&prepared, fault, result);
    assert_zero_live(&prepared.source).await;
}

fn assert_remove_fault_truth(
    prepared: &PreparedWorktreeCleanup,
    fault: ProcessFault,
    result: Result<
        DeliveryRemovePendingDisposition,
        coding_agent_runtime::DeliveryWorktreeCleanupError,
    >,
) {
    let worktree_exists = prepared.source.worktree_path().exists();
    let admin_exists = prepared.source.admin_directory.exists();
    assert_eq!(
        worktree_exists, admin_exists,
        "{fault:?}: remove must not leave a partial worktree/admin identity"
    );
    match fault {
        ProcessFault::BeforeSpawn => {
            assert_eq!(
                result,
                Ok(DeliveryRemovePendingDisposition::RetryExactRemove)
            );
            assert!(worktree_exists);
        }
        ProcessFault::StdoutOverflow | ProcessFault::ChannelUnknown => {
            assert_eq!(result, Ok(DeliveryRemovePendingDisposition::Removed));
            assert!(!worktree_exists);
        }
        ProcessFault::KillFailure | ProcessFault::CleanupFailure => {
            assert_eq!(
                result,
                Err(coding_agent_runtime::DeliveryWorktreeCleanupError::ProcessCleanupUnproven)
            );
            assert!(!worktree_exists);
        }
        ProcessFault::AfterSpawnUnknown | ProcessFault::Deadline | ProcessFault::WaitUnknown => {
            match result {
                Ok(DeliveryRemovePendingDisposition::RetryExactRemove) if worktree_exists => {}
                Ok(DeliveryRemovePendingDisposition::Removed) if !worktree_exists => {}
                result => panic!(
                    "{fault:?}: query-first remove result {result:?} mismatched present={worktree_exists}"
                ),
            }
        }
    }
}

macro_rules! delete_fault_test {
    ($test:ident, $fixture:literal, $task_id:literal, $fault:expr) => {
        #[tokio::test]
        async fn $test() {
            assert_delete_fault($fixture, $task_id, $fault).await;
        }
    };
}

delete_fault_test!(
    delete_before_spawn_truth_table_and_zero_live,
    "mutation-fault-delete-before-spawn",
    "123e4567-e89b-12d3-a456-426614174541",
    ProcessFault::BeforeSpawn
);
delete_fault_test!(
    delete_after_spawn_unknown_truth_table_and_zero_live,
    "mutation-fault-delete-after-spawn",
    "123e4567-e89b-12d3-a456-426614174542",
    ProcessFault::AfterSpawnUnknown
);
delete_fault_test!(
    delete_stdout_overflow_truth_table_and_zero_live,
    "mutation-fault-delete-stdout-overflow",
    "123e4567-e89b-12d3-a456-426614174543",
    ProcessFault::StdoutOverflow
);
delete_fault_test!(
    delete_deadline_truth_table_and_zero_live,
    "mutation-fault-delete-deadline",
    "123e4567-e89b-12d3-a456-426614174544",
    ProcessFault::Deadline
);
delete_fault_test!(
    delete_wait_unknown_truth_table_and_zero_live,
    "mutation-fault-delete-wait-unknown",
    "123e4567-e89b-12d3-a456-426614174545",
    ProcessFault::WaitUnknown
);
delete_fault_test!(
    delete_channel_unknown_truth_table_and_zero_live,
    "mutation-fault-delete-channel-unknown",
    "123e4567-e89b-12d3-a456-426614174546",
    ProcessFault::ChannelUnknown
);
delete_fault_test!(
    delete_kill_failure_truth_table_and_zero_live,
    "mutation-fault-delete-kill-failure",
    "123e4567-e89b-12d3-a456-426614174547",
    ProcessFault::KillFailure
);
delete_fault_test!(
    delete_cleanup_failure_truth_table_and_zero_live,
    "mutation-fault-delete-cleanup-failure",
    "123e4567-e89b-12d3-a456-426614174548",
    ProcessFault::CleanupFailure
);

async fn assert_delete_fault(name: &str, task_id: &str, fault: ProcessFault) {
    let mut prepared = PreparedBranchCleanup::new(name, task_id).await;
    let controller = ProcessFaultController::for_child(ATOMIC_DELETE_CHILD_ORDINAL, fault).unwrap();
    prepared
        .cleanup
        .set_branch_cleanup_boundary_hook_for_tests({
            let controller = controller.clone();
            move |phase| {
                if phase == "before-atomic-branch-delete-spawn" {
                    let admitted = controller
                        .events()
                        .into_iter()
                        .filter(|event| event.kind() == ProcessFaultEventKind::Admitted)
                        .count() as u64;
                    assert_eq!(
                        admitted + 1,
                        ATOMIC_DELETE_CHILD_ORDINAL,
                        "atomic delete child ordinal drifted before {fault:?}"
                    );
                }
            }
        });

    let result = controller
        .scope(prepared.retry_delete(delete_capability(&prepared.intent).await))
        .await;
    let observed_children =
        assert_controlled_child_was_reaped(&controller, ATOMIC_DELETE_CHILD_ORDINAL, fault).await;
    assert_fault_event_order(
        &controller,
        ATOMIC_DELETE_CHILD_ORDINAL,
        observed_children,
        fault,
    );
    assert_delete_fault_truth(&prepared, fault, result);
    assert_eq!(
        git_line(
            &prepared.fixture.repository,
            &["rev-parse", &prepared.target_ref]
        ),
        prepared.target_head,
        "{fault:?}",
    );
    prepared.restore_source_ref();
    assert_eq!(
        prepared.source_ref_oid().as_deref(),
        Some(prepared.source_commit.as_str()),
        "{fault:?}",
    );
    assert_zero_live(&prepared.source).await;
}

fn assert_delete_fault_truth(
    prepared: &PreparedBranchCleanup,
    fault: ProcessFault,
    result: Result<
        DeliveryDeletePendingDisposition,
        coding_agent_runtime::DeliveryWorktreeCleanupError,
    >,
) {
    let source_present = prepared.source_ref_oid().is_some();
    match fault {
        ProcessFault::BeforeSpawn => {
            assert!(matches!(
                result,
                Ok(DeliveryDeletePendingDisposition::RetryExactDelete)
            ));
            assert!(source_present);
        }
        ProcessFault::StdoutOverflow | ProcessFault::ChannelUnknown => {
            assert!(matches!(
                result,
                Ok(DeliveryDeletePendingDisposition::Deleted)
            ));
            assert!(!source_present);
        }
        ProcessFault::KillFailure | ProcessFault::CleanupFailure => {
            assert_eq!(
                result.unwrap_err(),
                coding_agent_runtime::DeliveryWorktreeCleanupError::ProcessCleanupUnproven
            );
            assert!(!source_present);
        }
        ProcessFault::AfterSpawnUnknown | ProcessFault::WaitUnknown => match result {
            Ok(DeliveryDeletePendingDisposition::ReconciliationRequired) if source_present => {}
            Ok(DeliveryDeletePendingDisposition::Deleted) if !source_present => {}
            result => panic!(
                "{fault:?}: query-first delete result {result:?} mismatched present={source_present}"
            ),
        },
        ProcessFault::Deadline => match result {
            Ok(DeliveryDeletePendingDisposition::KnownNotAppliedCommandTimedOut)
                if source_present => {}
            Ok(DeliveryDeletePendingDisposition::Deleted) if !source_present => {}
            result => panic!(
                "{fault:?}: query-first delete result {result:?} mismatched present={source_present}"
            ),
        },
    }
}

fn clear_proven_stale_fixture_index_lock(
    repository: &Path,
    fault: ProcessFault,
    reconciliation_required: bool,
) {
    if !matches!(
        fault,
        ProcessFault::AfterSpawnUnknown
            | ProcessFault::Deadline
            | ProcessFault::WaitUnknown
            | ProcessFault::KillFailure
            | ProcessFault::CleanupFailure
    ) {
        return;
    }

    let index_lock = repository.join(".git/index.lock");
    let metadata = match std::fs::symlink_metadata(&index_lock) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => panic!("{fault:?}: inspect fixture index lock after zero-live: {error}"),
    };
    assert!(
        metadata.file_type().is_file(),
        "{fault:?}: fixture index lock must be a plain file"
    );
    assert!(
        reconciliation_required,
        "{fault:?}: a stale fixture index lock requires reconciliation"
    );
    std::fs::remove_file(index_lock)
        .unwrap_or_else(|error| panic!("{fault:?}: remove proven stale fixture lock: {error}"));
}

#[test]
fn proven_stale_fixture_index_lock_is_removed_only_for_after_spawn_faults() {
    let temp = tempfile::tempdir().unwrap();
    let git_directory = temp.path().join(".git");
    std::fs::create_dir(&git_directory).unwrap();
    let index_lock = git_directory.join("index.lock");
    std::fs::write(&index_lock, b"fixture lock").unwrap();

    clear_proven_stale_fixture_index_lock(temp.path(), ProcessFault::BeforeSpawn, true);
    assert!(index_lock.is_file());

    clear_proven_stale_fixture_index_lock(temp.path(), ProcessFault::Deadline, true);
    assert!(!index_lock.exists());
}

#[test]
#[should_panic(expected = "a stale fixture index lock requires reconciliation")]
fn stale_fixture_index_lock_rejects_a_definitive_outcome() {
    let temp = tempfile::tempdir().unwrap();
    let git_directory = temp.path().join(".git");
    std::fs::create_dir(&git_directory).unwrap();
    std::fs::write(git_directory.join("index.lock"), b"fixture lock").unwrap();

    clear_proven_stale_fixture_index_lock(temp.path(), ProcessFault::Deadline, false);
}

#[test]
#[should_panic(expected = "fixture index lock must be a plain file")]
fn stale_fixture_index_lock_rejects_a_non_file() {
    let temp = tempfile::tempdir().unwrap();
    let index_lock = temp.path().join(".git/index.lock");
    std::fs::create_dir_all(&index_lock).unwrap();

    clear_proven_stale_fixture_index_lock(temp.path(), ProcessFault::Deadline, true);
}

async fn assert_controlled_child_was_reaped(
    controller: &ProcessFaultController,
    fault_child_ordinal: u64,
    fault: ProcessFault,
) -> u64 {
    let proof = controller
        .prove_zero_live(ZERO_LIVE_TIMEOUT)
        .await
        .unwrap_or_else(|error| {
            panic!(
                "{} after child {fault_child_ordinal} {fault:?}",
                error.code()
            )
        });
    assert!(
        proof.observed_children() >= fault_child_ordinal,
        "{fault:?}: the injected child must be admitted"
    );
    assert_eq!(proof.checked_scopes(), proof.observed_children() as usize);
    proof.observed_children()
}

fn assert_closing_observation_policy(
    fault_child_ordinal: u64,
    observed_children: u64,
    fault: ProcessFault,
) {
    match fault {
        ProcessFault::BeforeSpawn | ProcessFault::KillFailure | ProcessFault::CleanupFailure => {
            assert_eq!(
                observed_children, fault_child_ordinal,
                "{fault:?}: a proven pre-spawn or cleanup-unproven result must start no observation child"
            );
        }
        ProcessFault::AfterSpawnUnknown
        | ProcessFault::StdoutOverflow
        | ProcessFault::Deadline
        | ProcessFault::WaitUnknown
        | ProcessFault::ChannelUnknown => {
            assert!(
                observed_children > fault_child_ordinal,
                "{fault:?}: cleanup-confirmed unknown outcome must use fresh observation children"
            );
        }
    }
}

fn assert_fault_event_order(
    controller: &ProcessFaultController,
    fault_child_ordinal: u64,
    observed_children: u64,
    fault: ProcessFault,
) {
    let mut expected = Vec::new();
    for ordinal in 1..=observed_children {
        expected.push((ordinal, ProcessFaultEventKind::Admitted));
        if ordinal == fault_child_ordinal {
            expected.push((ordinal, ProcessFaultEventKind::Injected(fault)));
        }
        expected.push((ordinal, ProcessFaultEventKind::Returned));
    }
    assert_eq!(
        controller
            .events()
            .into_iter()
            .map(|event| (event.child_ordinal(), event.kind()))
            .collect::<Vec<_>>(),
        expected,
        "{fault:?}",
    );
}

async fn assert_zero_live(source: &ReviewedDirtySource) {
    tokio::time::timeout(ZERO_LIVE_TIMEOUT, async {
        loop {
            if source.worker_process_scope.active_tree_count() == 0
                && source.worker_process_scope.cleanup_proof().unwrap()
                    == coding_agent_runtime::ProcessCleanupProof::Confirmed
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all delivery process trees must be gone");
}

fn target_provisioner(
    fixture: &Fixture,
    worktrees: &WorktreeProvisioner,
) -> DeliveryTargetProvisioner {
    DeliveryTargetProvisioner::from_worktree_provisioner(
        worktrees,
        Arc::clone(&fixture.delivery_git),
        &fixture.runtime_directory,
        fixture.task_process_scope(),
        process_limits(),
        delivery_source_limits(),
    )
    .unwrap()
}

fn target_request(fixture: &Fixture) -> DeliveryTargetRequest {
    DeliveryTargetRequest::try_new(
        git_line(&fixture.repository, &["symbolic-ref", "--short", "HEAD"]),
        git_line(&fixture.repository, &["rev-parse", "HEAD"]),
    )
    .unwrap()
}

fn process_limits() -> ProcessLimits {
    ProcessLimits::try_new(
        512 * 1024,
        512 * 1024,
        Duration::from_secs(30),
        Duration::from_secs(5),
    )
    .unwrap()
}

fn fingerprint_limits() -> FingerprintLimits {
    FingerprintLimits::try_new(
        Duration::from_secs(10),
        4_096,
        2 * 1024 * 1024,
        32 * 1024 * 1024,
    )
    .unwrap()
}

fn delivery_process_scope(
    worker_process_scope: &coding_agent_runtime::ProcessLivenessScope,
) -> coding_agent_runtime::ProcessLivenessScope {
    let mut task_id = [0x35; 16];
    task_id[6] = 0x45;
    task_id[8] = 0xb5;
    worker_process_scope.sibling_task_scope(task_id).unwrap()
}

fn worker_task_id() -> [u8; 16] {
    let mut task_id = [0x25; 16];
    task_id[6] = 0x45;
    task_id[8] = 0xa5;
    task_id
}

fn source_provisioner_for_cleanup(
    fixture: &Fixture,
    worktrees: &WorktreeProvisioner,
    delivery_process_scope: coding_agent_runtime::ProcessLivenessScope,
) -> DeliverySourceProvisioner {
    DeliverySourceProvisioner::from_worktree_provisioner(
        worktrees,
        Arc::clone(&fixture.delivery_git),
        &fixture.runtime_directory,
        delivery_process_scope,
        process_limits(),
        delivery_source_limits(),
        fingerprint_limits(),
    )
    .unwrap()
}

async fn unlock_capability(
    intent: &DeliveryWorktreeCleanupIntent,
) -> DeliveryUnlockPendingCapability {
    authorize_persisted_delivery_unlock(&AcceptCleanupIntent::new(intent), intent.clone())
        .await
        .unwrap()
}

async fn remove_capability(
    intent: &DeliveryWorktreeCleanupIntent,
) -> DeliveryRemovePendingCapability {
    authorize_persisted_delivery_remove(&AcceptCleanupIntent::new(intent), intent.clone())
        .await
        .unwrap()
}

async fn delete_capability(
    intent: &DeliveryBranchCleanupIntent,
) -> DeliveryDeletePendingCapability {
    authorize_persisted_delivery_branch_delete(
        &AcceptBranchCleanupIntent {
            expected: intent.clone(),
        },
        intent.clone(),
    )
    .await
    .unwrap()
}

fn worktree_command(fixture: &Fixture, source: &ReviewedDirtySource, operation: &[&str]) {
    let path = source.worktree_path().to_string_lossy();
    let mut arguments = vec!["worktree"];
    arguments.extend_from_slice(operation);
    arguments.extend(["--", path.as_ref()]);
    git_ok(&fixture.repository, &arguments);
}

fn run_matching_conflicting_merge(repository: &Path, source_commit: &str, task_id: &str) {
    let message = format!("coding-agent: merge task {task_id} attempt 1");
    let mut command = Command::new("git");
    command
        .arg("--no-pager")
        .arg("-c")
        .arg(format!("core.hooksPath={}", null_device()))
        .arg("-C")
        .arg(repository)
        .args([
            "merge",
            "--no-ff",
            "--strategy=ort",
            "--no-edit",
            "--no-verify",
            "--no-verify-signatures",
            "--no-gpg-sign",
            "--no-autostash",
            "--no-rerere-autoupdate",
            "--no-overwrite-ignore",
            "--no-log",
            "--no-stat",
            "--cleanup=verbatim",
            "-m",
            &message,
            "--",
            source_commit,
        ])
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", null_device())
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let child = {
        let _spawn_guard = coding_agent_runtime::acquire_process_spawn_lock();
        command.spawn().unwrap()
    };
    let output = child.wait_with_output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(1),
        "matching merge must conflict: {}",
        String::from_utf8_lossy(&output.stderr),
    );
}

fn assert_target_clean(repository: &Path) {
    assert!(git_line(repository, &["status", "--porcelain=v2"]).is_empty());
    for entry in [
        "AUTO_MERGE",
        "MERGE_AUTOSTASH",
        "MERGE_HEAD",
        "MERGE_MODE",
        "MERGE_MSG",
    ] {
        assert!(!repository.join(".git").join(entry).exists(), "{entry}");
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
