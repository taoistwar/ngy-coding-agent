mod delivery_source_support;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "test-support")]
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use coding_agent_runtime::{
    DeliveryCandidateTree, DeliveryMergeInput, DeliveryMergeOutcome, DeliveryPreflightSource,
    DeliveryRemovePendingAuthorizer, DeliveryRemovePendingCapability,
    DeliveryRemovePendingDisposition, DeliverySourceCapability, DeliverySourceCommit,
    DeliverySourceCommitInput, DeliverySourcePendingState, DeliverySourceProvisioner,
    DeliverySourceRecoveryDisposition, DeliverySourceRecoveryIntent, DeliveryTargetProvisioner,
    DeliveryTargetRequest, DeliveryUnlockPendingAuthorizer, DeliveryUnlockPendingCapability,
    DeliveryUnlockPendingDisposition, DeliveryUnlockedPendingRemoveAuthorizer,
    DeliveryUnlockedPendingRemoveCapability, DeliveryUnlockedPendingRemoveDisposition,
    DeliveryWorktreeCleanupError, DeliveryWorktreeCleanupIntent,
    DeliveryWorktreeCleanupProvisioner, FingerprintLimits, ProcessLimits, ProcessLivenessDirectory,
    SealedProcessLivenessScope, WorktreeProvisioner, apply_expected_delivery_merge,
    authorize_persisted_delivery_remove, authorize_persisted_delivery_unlock,
    authorize_persisted_delivery_unlocked_pending_remove, build_expected_delivery_merge,
    preflight_delivery_merge,
};
use delivery_source_support::{
    Fixture, ReviewedDirtySource, delivery_source_limits, git_line, git_ok,
};
use tokio_util::sync::CancellationToken;

const EPOCH_SECONDS: i64 = 1_700_000_016;

struct PreparedCleanup {
    cleanup: DeliveryWorktreeCleanupProvisioner,
    source_provisioner: DeliverySourceProvisioner,
    intent: DeliveryWorktreeCleanupIntent,
    sealed_worker: SealedProcessLivenessScope,
    source_commit: String,
    merged_target: String,
    source: ReviewedDirtySource,
    fixture: Fixture,
}

impl PreparedCleanup {
    async fn new(name: &str, task_id: &str) -> Self {
        let fixture = Fixture::new(name).await;
        let source = fixture.reviewed_dirty_source(task_id).await;

        Self::from_reviewed_source(fixture, source, task_id).await
    }

    async fn new_with_approved_tracked_deletion(name: &str, task_id: &str) -> Self {
        let fixture = Fixture::new(name).await;
        let mut source = fixture.reviewed_dirty_source(task_id).await;
        git_ok(
            source.worktree_path(),
            &["rm", "--quiet", "--force", "--", "tracked.txt"],
        );
        source.approved_fingerprint = fixture.current_fingerprint(&source).await;

        Self::from_reviewed_source(fixture, source, task_id).await
    }

    async fn from_reviewed_source(
        fixture: Fixture,
        source: ReviewedDirtySource,
        task_id: &str,
    ) -> Self {
        let worker_process_scope = source.worker_process_scope.clone();
        let delivery_process_scope = delivery_process_scope(&worker_process_scope);
        let source_provisioner =
            source_provisioner(&fixture, &source.worktrees, delivery_process_scope.clone());
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
        apply_source_commit(
            &source_provisioner,
            &source,
            &opened,
            &candidate,
            &source_commit,
            &source_input,
        )
        .await;

        let merged_target = apply_merge(
            &fixture,
            &source,
            &source_provisioner,
            &opened,
            &candidate,
            &source_commit,
            &source_input,
            delivery_process_scope.clone(),
            task_id,
        )
        .await;
        assert_eq!(
            git_line(
                source.worktree_path(),
                &["status", "--porcelain=v2", "--untracked-files=all"],
            ),
            "",
            "cleanup capture starts from an exact clean committed source",
        );
        assert_eq!(
            std::fs::read(source.admin_directory.join("locked")).unwrap(),
            b"codex-reserved\n",
            "cleanup capture starts while the owned worktree is still locked",
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
        let source_commit_id = source_commit.object_id().to_owned();
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

        Self {
            cleanup,
            source_provisioner,
            intent,
            sealed_worker,
            source_commit: source_commit_id,
            merged_target,
            source,
            fixture,
        }
    }

    async fn classify_unlock(
        &self,
        capability: &DeliveryUnlockPendingCapability,
    ) -> Result<DeliveryUnlockPendingDisposition, DeliveryWorktreeCleanupError> {
        self.cleanup
            .classify_delivery_unlock_pending(
                &self.source_provisioner,
                capability,
                &self.sealed_worker,
                CancellationToken::new(),
            )
            .await
    }

    async fn retry_unlock(
        &self,
        capability: DeliveryUnlockPendingCapability,
    ) -> Result<DeliveryUnlockPendingDisposition, DeliveryWorktreeCleanupError> {
        self.cleanup
            .retry_delivery_unlock_pending(
                &self.source_provisioner,
                capability,
                &self.sealed_worker,
                CancellationToken::new(),
            )
            .await
    }

    async fn classify_unlocked_pending_remove(
        &self,
        capability: &DeliveryUnlockedPendingRemoveCapability,
    ) -> Result<DeliveryUnlockedPendingRemoveDisposition, DeliveryWorktreeCleanupError> {
        self.cleanup
            .classify_delivery_unlocked_pending_remove(
                &self.source_provisioner,
                capability,
                &self.sealed_worker,
                CancellationToken::new(),
            )
            .await
    }

    async fn classify_remove(
        &self,
        capability: &DeliveryRemovePendingCapability,
    ) -> Result<DeliveryRemovePendingDisposition, DeliveryWorktreeCleanupError> {
        self.cleanup
            .classify_delivery_remove_pending(
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
    ) -> Result<DeliveryRemovePendingDisposition, DeliveryWorktreeCleanupError> {
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

    fn raw_lock(&self) {
        self.raw_lock_with_reason("codex-reserved");
    }

    fn raw_lock_with_reason(&self, reason: &str) {
        let reason = format!("--reason={reason}");
        worktree_command(&self.fixture, &self.source, &["lock", reason.as_str()]);
    }

    fn raw_remove(&self) {
        worktree_command(&self.fixture, &self.source, &["remove"]);
    }
}

#[derive(Debug)]
struct PersistedCleanupAuthorization {
    expected: DeliveryWorktreeCleanupIntent,
}

impl PersistedCleanupAuthorization {
    fn for_intent(intent: &DeliveryWorktreeCleanupIntent) -> Self {
        Self {
            expected: intent.clone(),
        }
    }

    fn require_exact(&self, intent: &DeliveryWorktreeCleanupIntent) -> Result<(), &'static str> {
        if self.expected.is_same_runtime_intent(intent) {
            Ok(())
        } else {
            Err("cleanup intent did not match the durably accepted runtime intent")
        }
    }
}

#[async_trait]
impl DeliveryUnlockPendingAuthorizer for PersistedCleanupAuthorization {
    type Error = &'static str;

    async fn authorize_persisted_unlock_pending(
        &self,
        intent: &DeliveryWorktreeCleanupIntent,
    ) -> Result<(), Self::Error> {
        self.require_exact(intent)
    }
}

#[async_trait]
impl DeliveryUnlockedPendingRemoveAuthorizer for PersistedCleanupAuthorization {
    type Error = &'static str;

    async fn authorize_persisted_unlocked_pending_remove(
        &self,
        intent: &DeliveryWorktreeCleanupIntent,
    ) -> Result<(), Self::Error> {
        self.require_exact(intent)
    }
}

#[async_trait]
impl DeliveryRemovePendingAuthorizer for PersistedCleanupAuthorization {
    type Error = &'static str;

    async fn authorize_persisted_remove_pending(
        &self,
        intent: &DeliveryWorktreeCleanupIntent,
    ) -> Result<(), Self::Error> {
        self.require_exact(intent)
    }
}

#[tokio::test]
async fn captures_only_a_merged_committed_locked_clean_source_and_redacts_the_intent() {
    let prepared = PreparedCleanup::new(
        "cleanup-capture-locked",
        "123e4567-e89b-12d3-a456-426614174301",
    )
    .await;
    assert_eq!(
        format!("{:?}", prepared.intent),
        "DeliveryWorktreeCleanupIntent(<opaque>)",
    );
    assert_eq!(
        format!("{:?}", prepared.cleanup),
        "DeliveryWorktreeCleanupProvisioner(<opaque>)",
    );
    assert_eq!(
        git_line(&prepared.fixture.repository, &["rev-parse", "HEAD"]),
        prepared.merged_target,
    );
    assert_eq!(
        git_line(
            &prepared.fixture.repository,
            &[
                "rev-parse",
                &format!("refs/heads/{}", prepared.source.reservation.branch_name()),
            ],
        ),
        prepared.source_commit,
    );
    assert_eq!(
        std::fs::read(prepared.source.admin_directory.join("locked")).unwrap(),
        b"codex-reserved\n",
    );

    let capability = unlock_capability(&prepared.intent).await;
    assert_eq!(
        format!("{capability:?}"),
        "DeliveryUnlockPendingCapability(<opaque>)",
    );
    assert_eq!(
        prepared.classify_unlock(&capability).await.unwrap(),
        DeliveryUnlockPendingDisposition::RetryExactUnlock,
    );
}

#[tokio::test]
async fn cleanup_capture_accepts_a_clean_committed_approved_tracked_deletion() {
    let prepared = PreparedCleanup::new_with_approved_tracked_deletion(
        "cleanup-capture-tracked-deletion",
        "123e4567-e89b-12d3-a456-426614174320",
    )
    .await;
    let deleted = prepared.source.worktree_path().join("tracked.txt");

    assert!(!deleted.exists());
    assert_eq!(
        git_line(
            &prepared.fixture.repository,
            &[
                "ls-tree",
                "--name-only",
                &prepared.source_commit,
                "--",
                "tracked.txt",
            ],
        ),
        "",
        "the committed cleanup source must retain the approved deletion",
    );
    assert_eq!(
        git_line(
            prepared.source.worktree_path(),
            &["status", "--porcelain=v2", "--untracked-files=all"],
        ),
        "",
    );
    assert_eq!(
        prepared
            .classify_unlock(&unlock_capability(&prepared.intent).await)
            .await
            .unwrap(),
        DeliveryUnlockPendingDisposition::RetryExactUnlock,
        "cleanup must authenticate the committed scene instead of comparing its fingerprint and attributes with the approved pre-stage scene",
    );
}

#[tokio::test]
async fn cleanup_retries_preserve_in_progress_git_operation_state_without_mutation() {
    for (case_name, task_id, directory_state) in [
        ("merge-head", "123e4567-e89b-12d3-a456-426614174321", false),
        ("rebase-apply", "123e4567-e89b-12d3-a456-426614174322", true),
    ] {
        let fixture_name = format!("cleanup-operation-state-{case_name}");
        let prepared = PreparedCleanup::new(&fixture_name, task_id).await;
        let expected_sentinel = if directory_state {
            b"rebase operation sentinel\n".to_vec()
        } else {
            format!("{}\n", prepared.source_commit).into_bytes()
        };
        let sentinel = if directory_state {
            let operation = prepared.source.admin_directory.join(case_name);
            std::fs::create_dir(&operation).unwrap();
            operation.join("cleanup-sentinel")
        } else {
            prepared.source.admin_directory.join("MERGE_HEAD")
        };
        std::fs::write(&sentinel, &expected_sentinel).unwrap();

        assert_eq!(
            prepared
                .retry_unlock(unlock_capability(&prepared.intent).await)
                .await
                .unwrap(),
            DeliveryUnlockPendingDisposition::ReconciliationRequired,
            "UnlockPending must not mutate a source with {case_name}",
        );
        assert_eq!(
            std::fs::read(prepared.source.admin_directory.join("locked")).unwrap(),
            b"codex-reserved\n",
            "UnlockPending must preserve the exact owned lock for {case_name}",
        );
        assert_eq!(std::fs::read(&sentinel).unwrap(), expected_sentinel);
        assert!(prepared.source.worktree_path().is_dir());
        assert!(prepared.source.admin_directory.is_dir());

        prepared.raw_unlock();
        assert_eq!(std::fs::read(&sentinel).unwrap(), expected_sentinel);
        assert_eq!(
            prepared
                .retry_remove(remove_capability(&prepared.intent).await)
                .await
                .unwrap(),
            DeliveryRemovePendingDisposition::ReconciliationRequired,
            "RemovePending must not mutate a source with {case_name}",
        );
        assert_eq!(std::fs::read(&sentinel).unwrap(), expected_sentinel);
        assert!(prepared.source.worktree_path().is_dir());
        assert!(prepared.source.admin_directory.is_dir());
    }
}

#[tokio::test]
async fn cleanup_constructor_rejects_a_delivery_scope_from_another_liveness_instance() {
    let fixture = Fixture::new("cleanup-foreign-delivery-scope").await;
    let worker = fixture.task_process_scope();
    let worktrees = fixture.worktree_provisioner_with_scope(worker);
    let foreign_worker = foreign_instance_worker_process_scope(&fixture.runtime_directory);
    let foreign_delivery = delivery_process_scope(&foreign_worker);

    let result = DeliveryWorktreeCleanupProvisioner::from_worktree_provisioner(
        &worktrees,
        Arc::clone(&fixture.delivery_git),
        &fixture.runtime_directory,
        foreign_delivery,
        process_limits(),
        delivery_source_limits(),
    );

    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("foreign liveness instance was accepted"),
    };
    assert!(
        matches!(error, DeliveryWorktreeCleanupError::InvalidConfiguration),
        "unexpected stable code: {}",
        error.code()
    );
}

#[tokio::test]
async fn cleanup_recovery_rejects_a_confirmed_proof_from_a_different_worker_scope() {
    let prepared = PreparedCleanup::new(
        "cleanup-wrong-worker-proof",
        "123e4567-e89b-12d3-a456-426614174315",
    )
    .await;
    let (foreign_worker, foreign_task_id) =
        foreign_worker_process_scope(&prepared.fixture.runtime_directory);
    let foreign_proof = foreign_worker.seal_task_scope(foreign_task_id).unwrap();
    let capability = unlock_capability(&prepared.intent).await;

    assert_eq!(
        prepared
            .cleanup
            .classify_delivery_unlock_pending(
                &prepared.source_provisioner,
                &capability,
                &foreign_proof,
                CancellationToken::new(),
            )
            .await
            .unwrap(),
        DeliveryUnlockPendingDisposition::ReconciliationRequired,
    );
    assert_eq!(
        std::fs::read(prepared.source.admin_directory.join("locked")).unwrap(),
        b"codex-reserved\n",
    );
}

#[tokio::test]
async fn unlock_pending_is_query_first_and_a_reply_lost_retry_does_not_spawn_twice() {
    #[allow(unused_mut)]
    let mut prepared = PreparedCleanup::new(
        "cleanup-unlock-reply-lost",
        "123e4567-e89b-12d3-a456-426614174302",
    )
    .await;
    #[cfg(feature = "test-support")]
    let spawns = Arc::new(AtomicUsize::new(0));
    #[cfg(feature = "test-support")]
    let observed = Arc::clone(&spawns);
    #[cfg(feature = "test-support")]
    prepared
        .cleanup
        .set_cleanup_boundary_hook_for_tests(move |phase| {
            if phase == "after-query-before-unlock-spawn" {
                observed.fetch_add(1, Ordering::SeqCst);
            }
        });
    let capability = unlock_capability(&prepared.intent).await;

    assert_eq!(
        prepared.classify_unlock(&capability).await.unwrap(),
        DeliveryUnlockPendingDisposition::RetryExactUnlock,
    );
    let _lost_reply = prepared.retry_unlock(capability).await.unwrap();
    assert_eq!(
        prepared
            .retry_unlock(unlock_capability(&prepared.intent).await)
            .await
            .unwrap(),
        DeliveryUnlockPendingDisposition::UnlockApplied,
    );
    #[cfg(feature = "test-support")]
    assert_eq!(spawns.load(Ordering::SeqCst), 1);
    assert!(!prepared.source.admin_directory.join("locked").exists());
    assert!(prepared.source.worktree_path().is_dir());
    assert!(prepared.source.admin_directory.is_dir());
    assert_eq!(
        git_line(
            prepared.source.worktree_path(),
            &["status", "--porcelain=v2", "--untracked-files=all"],
        ),
        "",
    );
}

#[tokio::test]
async fn unlock_pending_accepts_exact_unlocked_dirty_while_unlocked_pending_remove_reconciles() {
    let prepared = PreparedCleanup::new(
        "cleanup-unlocked-exact",
        "123e4567-e89b-12d3-a456-426614174303",
    )
    .await;
    prepared.raw_unlock();
    let capability = unlocked_pending_remove_capability(&prepared.intent).await;
    assert_eq!(
        prepared
            .classify_unlocked_pending_remove(&capability)
            .await
            .unwrap(),
        DeliveryUnlockedPendingRemoveDisposition::EnterRemovePending,
    );

    let retained = prepared.source.worktree_path().join("late-user-file.txt");
    std::fs::write(&retained, b"must survive\n").unwrap();
    assert_eq!(
        prepared
            .classify_unlock(&unlock_capability(&prepared.intent).await)
            .await
            .unwrap(),
        DeliveryUnlockPendingDisposition::UnlockApplied,
    );
    assert_eq!(
        prepared
            .classify_unlocked_pending_remove(&capability)
            .await
            .unwrap(),
        DeliveryUnlockedPendingRemoveDisposition::ReconciliationRequired,
    );
    assert_eq!(
        prepared
            .classify_remove(&remove_capability(&prepared.intent).await)
            .await
            .unwrap(),
        DeliveryRemovePendingDisposition::KnownNotAppliedDirty,
    );
    assert_eq!(std::fs::read(retained).unwrap(), b"must survive\n");
}

#[tokio::test]
async fn unlock_pending_still_unlocks_an_exact_locked_dirty_source_without_removing_content() {
    let prepared = PreparedCleanup::new(
        "cleanup-locked-dirty-unlock",
        "123e4567-e89b-12d3-a456-426614174318",
    )
    .await;
    let retained = prepared
        .source
        .worktree_path()
        .join("late-before-unlock.txt");
    std::fs::write(&retained, b"arrived after cleanup acceptance\n").unwrap();
    let capability = unlock_capability(&prepared.intent).await;

    assert_eq!(
        prepared.classify_unlock(&capability).await.unwrap(),
        DeliveryUnlockPendingDisposition::RetryExactUnlock,
    );
    assert_eq!(
        prepared.retry_unlock(capability).await.unwrap(),
        DeliveryUnlockPendingDisposition::UnlockApplied,
    );
    assert_eq!(
        std::fs::read(&retained).unwrap(),
        b"arrived after cleanup acceptance\n",
    );
    assert!(!prepared.source.admin_directory.join("locked").exists());
    assert_eq!(
        prepared
            .classify_unlocked_pending_remove(
                &unlocked_pending_remove_capability(&prepared.intent).await,
            )
            .await
            .unwrap(),
        DeliveryUnlockedPendingRemoveDisposition::ReconciliationRequired,
    );
}

#[tokio::test]
async fn remove_pending_preserves_tracked_unstaged_and_staged_changes() {
    let prepared = PreparedCleanup::new(
        "cleanup-remove-tracked-dirty",
        "123e4567-e89b-12d3-a456-426614174316",
    )
    .await;
    prepared.raw_unlock();
    let capability = remove_capability(&prepared.intent).await;
    let tracked = prepared.source.worktree_path().join("tracked.txt");
    let index = prepared.source.admin_directory.join("index");
    let committed_index = std::fs::read(&index).unwrap();
    let dirty_contents = b"tracked cleanup edit must survive\n";

    std::fs::write(&tracked, dirty_contents).unwrap();
    let unstaged_status = git_line(
        prepared.source.worktree_path(),
        &["status", "--porcelain=v2", "--untracked-files=all"],
    );
    assert!(
        unstaged_status.starts_with("1 .M ") && unstaged_status.ends_with(" tracked.txt"),
        "fixture must first prove a tracked unstaged change: {unstaged_status}",
    );
    assert_eq!(
        prepared.classify_remove(&capability).await.unwrap(),
        DeliveryRemovePendingDisposition::KnownNotAppliedDirty,
    );
    assert_eq!(std::fs::read(&tracked).unwrap(), dirty_contents);
    assert_eq!(std::fs::read(&index).unwrap(), committed_index);
    assert!(prepared.source.worktree_path().is_dir());
    assert!(prepared.source.admin_directory.is_dir());

    git_ok(
        prepared.source.worktree_path(),
        &["add", "--", "tracked.txt"],
    );
    let staged_index = std::fs::read(&index).unwrap();
    assert_ne!(staged_index, committed_index);
    let staged_status = git_line(
        prepared.source.worktree_path(),
        &["status", "--porcelain=v2", "--untracked-files=all"],
    );
    assert!(
        staged_status.starts_with("1 M. ") && staged_status.ends_with(" tracked.txt"),
        "fixture must then prove a staged tracked change: {staged_status}",
    );
    assert_eq!(
        prepared.classify_remove(&capability).await.unwrap(),
        DeliveryRemovePendingDisposition::KnownNotAppliedDirty,
    );
    assert_eq!(std::fs::read(&tracked).unwrap(), dirty_contents);
    assert_eq!(std::fs::read(&index).unwrap(), staged_index);
    assert_eq!(
        git_line(
            prepared.source.worktree_path(),
            &["diff", "--cached", "--name-only", "--", "tracked.txt"],
        ),
        "tracked.txt",
    );
    assert!(prepared.source.worktree_path().is_dir());
    assert!(prepared.source.admin_directory.is_dir());
}

#[tokio::test]
async fn remove_pending_preserves_ignored_untracked_content() {
    let prepared = PreparedCleanup::new(
        "cleanup-remove-ignored-dirty",
        "123e4567-e89b-12d3-a456-426614174319",
    )
    .await;
    prepared.raw_unlock();
    let exclude = prepared.fixture.repository.join(".git/info/exclude");
    let original_exclude = std::fs::read(&exclude).unwrap_or_default();
    let mut updated_exclude = original_exclude.clone();
    updated_exclude.extend_from_slice(b"\nignored-cleanup.txt\n");
    std::fs::write(&exclude, &updated_exclude).unwrap();
    let retained = prepared.source.worktree_path().join("ignored-cleanup.txt");
    std::fs::write(&retained, b"ignored user content must survive\n").unwrap();
    assert_eq!(
        git_line(
            prepared.source.worktree_path(),
            &["status", "--porcelain=v2", "--untracked-files=all"],
        ),
        "",
        "ordinary status deliberately hides the ignored node",
    );

    assert_eq!(
        prepared
            .classify_remove(&remove_capability(&prepared.intent).await)
            .await
            .unwrap(),
        DeliveryRemovePendingDisposition::KnownNotAppliedDirty,
    );
    assert_eq!(
        std::fs::read(&retained).unwrap(),
        b"ignored user content must survive\n",
    );
    assert_eq!(std::fs::read(&exclude).unwrap(), updated_exclude);
    assert!(prepared.source.worktree_path().is_dir());
    assert!(prepared.source.admin_directory.is_dir());
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn remove_pending_rechecks_after_query_and_non_force_preserves_late_untracked_content() {
    let mut prepared = PreparedCleanup::new(
        "cleanup-remove-late-dirty",
        "123e4567-e89b-12d3-a456-426614174304",
    )
    .await;
    prepared.raw_unlock();
    let retained = prepared.source.worktree_path().join("late-untracked.txt");
    let injected = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&injected);
    prepared.cleanup.set_cleanup_boundary_hook_for_tests({
        let retained = retained.clone();
        move |phase| {
            if phase == "after-query-before-remove-spawn" {
                std::fs::write(&retained, b"arrived after clean query\n").unwrap();
                observed.fetch_add(1, Ordering::SeqCst);
            }
        }
    });
    assert_eq!(
        prepared
            .retry_remove(remove_capability(&prepared.intent).await)
            .await
            .unwrap(),
        DeliveryRemovePendingDisposition::KnownNotAppliedDirty,
    );
    assert_eq!(injected.load(Ordering::SeqCst), 1);
    assert_eq!(
        std::fs::read(&retained).unwrap(),
        b"arrived after clean query\n",
    );
    assert!(prepared.source.worktree_path().is_dir());
    assert!(prepared.source.admin_directory.is_dir());
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn remove_pending_preserves_ignored_content_arriving_at_final_spawn_boundary() {
    let fixture = Fixture::new("cleanup-remove-final-ignored-race").await;
    let source = fixture
        .reviewed_dirty_source("123e4567-e89b-12d3-a456-426614174326")
        .await;
    let exclude = fixture.repository.join(".git/info/exclude");
    let mut updated_exclude = std::fs::read(&exclude).unwrap_or_default();
    updated_exclude.extend_from_slice(b"\nlate-ignored-cleanup.txt\n");
    std::fs::write(&exclude, updated_exclude).unwrap();
    let mut prepared = PreparedCleanup::from_reviewed_source(
        fixture,
        source,
        "123e4567-e89b-12d3-a456-426614174326",
    )
    .await;
    prepared.raw_unlock();
    let retained = prepared
        .source
        .worktree_path()
        .join("late-ignored-cleanup.txt");
    let injected = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&injected);
    prepared.cleanup.set_cleanup_boundary_hook_for_tests({
        let retained = retained.clone();
        move |phase| {
            if phase == "before-actual-remove-spawn" {
                std::fs::write(&retained, b"arrived at final spawn boundary\n").unwrap();
                observed.fetch_add(1, Ordering::SeqCst);
            }
        }
    });

    assert_eq!(
        prepared
            .retry_remove(remove_capability(&prepared.intent).await)
            .await
            .unwrap(),
        DeliveryRemovePendingDisposition::KnownNotAppliedDirty,
    );
    assert_eq!(injected.load(Ordering::SeqCst), 1);
    assert_eq!(
        std::fs::read(&retained).unwrap(),
        b"arrived at final spawn boundary\n",
    );
    assert!(prepared.source.worktree_path().is_dir());
    assert!(prepared.source.admin_directory.is_dir());
}

#[tokio::test]
async fn remove_pending_real_remove_is_idempotent_when_the_first_reply_is_lost() {
    #[allow(unused_mut)]
    let mut prepared = PreparedCleanup::new(
        "cleanup-remove-reply-lost",
        "123e4567-e89b-12d3-a456-426614174305",
    )
    .await;
    prepared.raw_unlock();
    #[cfg(feature = "test-support")]
    let spawns = Arc::new(AtomicUsize::new(0));
    #[cfg(feature = "test-support")]
    let observed = Arc::clone(&spawns);
    #[cfg(feature = "test-support")]
    prepared
        .cleanup
        .set_cleanup_boundary_hook_for_tests(move |phase| {
            if phase == "after-query-before-remove-spawn" {
                observed.fetch_add(1, Ordering::SeqCst);
            }
        });
    let capability = remove_capability(&prepared.intent).await;

    assert_eq!(
        prepared.classify_remove(&capability).await.unwrap(),
        DeliveryRemovePendingDisposition::RetryExactRemove,
    );
    let _lost_reply = prepared.retry_remove(capability).await.unwrap();
    assert_eq!(
        prepared
            .retry_remove(remove_capability(&prepared.intent).await)
            .await
            .unwrap(),
        DeliveryRemovePendingDisposition::Removed,
    );
    #[cfg(feature = "test-support")]
    assert_eq!(spawns.load(Ordering::SeqCst), 1);
    assert!(!prepared.source.worktree_path().exists());
    assert!(!prepared.source.admin_directory.exists());
    assert!(prepared.fixture.repository.join(".git").is_dir());
    assert_eq!(
        git_line(
            &prepared.fixture.repository,
            &[
                "rev-parse",
                &format!("refs/heads/{}", prepared.source.reservation.branch_name()),
            ],
        ),
        prepared.source_commit,
        "worktree cleanup must not delete the committed source branch",
    );
}

#[tokio::test]
async fn native_non_force_remove_accepts_the_reserved_ignored_cargo_target_subtree() {
    let prepared = PreparedCleanup::new(
        "cleanup-native-ignored-cargo-target",
        "123e4567-e89b-12d3-a456-426614174325",
    )
    .await;
    let outside = prepared.fixture.root.join("outside-sentinel.txt");
    std::fs::write(&outside, b"outside stays\n").unwrap();
    std::fs::write(
        prepared.fixture.repository.join(".git/info/exclude"),
        b"nested/rust/target/\n",
    )
    .unwrap();
    let cargo_output = prepared
        .source
        .worktree_path()
        .join("nested/rust/target/debug/build-output.bin");
    std::fs::create_dir_all(cargo_output.parent().unwrap()).unwrap();
    std::fs::write(&cargo_output, b"runtime-owned Cargo output\n").unwrap();

    assert_eq!(
        git_line(
            prepared.source.worktree_path(),
            &["status", "--porcelain=v2", "--untracked-files=all"],
        ),
        "",
        "the reserved Cargo target remains Git-clean",
    );
    assert_eq!(
        git_line(
            prepared.source.worktree_path(),
            &[
                "ls-files",
                "--others",
                "--ignored",
                "--exclude-standard",
                "--directory",
                "--",
            ],
        ),
        "nested/rust/target/",
        "the only ignored-untracked subtree is the authenticated Cargo target",
    );
    let target_head = git_line(&prepared.fixture.repository, &["rev-parse", "HEAD"]);
    let source_ref = format!("refs/heads/{}", prepared.source.reservation.branch_name());
    let source_oid = git_line(&prepared.fixture.repository, &["rev-parse", &source_ref]);

    prepared.raw_unlock();
    prepared.raw_remove();

    assert!(!prepared.source.worktree_path().exists());
    assert!(!prepared.source.admin_directory.exists());
    assert_eq!(std::fs::read(&outside).unwrap(), b"outside stays\n");
    assert_eq!(
        git_line(&prepared.fixture.repository, &["rev-parse", "HEAD"]),
        target_head
    );
    assert_eq!(
        git_line(&prepared.fixture.repository, &["rev-parse", &source_ref]),
        source_oid
    );
}

#[tokio::test]
async fn exact_absence_is_phase_specific_and_only_remove_pending_accepts_it() {
    let prepared = PreparedCleanup::new(
        "cleanup-phase-absence",
        "123e4567-e89b-12d3-a456-426614174306",
    )
    .await;
    prepared.raw_unlock();
    prepared.raw_remove();
    assert!(!prepared.source.worktree_path().exists());
    assert!(!prepared.source.admin_directory.exists());

    assert_eq!(
        prepared
            .classify_unlock(&unlock_capability(&prepared.intent).await)
            .await
            .unwrap(),
        DeliveryUnlockPendingDisposition::ReconciliationRequired,
    );
    assert_eq!(
        prepared
            .classify_unlocked_pending_remove(
                &unlocked_pending_remove_capability(&prepared.intent).await,
            )
            .await
            .unwrap(),
        DeliveryUnlockedPendingRemoveDisposition::ReconciliationRequired,
    );
    assert_eq!(
        prepared
            .classify_remove(&remove_capability(&prepared.intent).await)
            .await
            .unwrap(),
        DeliveryRemovePendingDisposition::Removed,
    );
}

#[tokio::test]
async fn one_sided_absence_never_authorizes_a_cleanup_command() {
    let worktree_absent = PreparedCleanup::new(
        "cleanup-partial-worktree",
        "123e4567-e89b-12d3-a456-426614174307",
    )
    .await;
    let retained_worktree = worktree_absent.fixture.root.join("retained-worktree");
    std::fs::rename(worktree_absent.source.worktree_path(), &retained_worktree).unwrap();
    assert!(worktree_absent.source.admin_directory.is_dir());
    assert_all_phases_reconcile(&worktree_absent).await;
    assert!(retained_worktree.join("tracked.txt").is_file());

    let admin_absent = PreparedCleanup::new(
        "cleanup-partial-admin",
        "123e4567-e89b-12d3-a456-426614174308",
    )
    .await;
    let retained_admin = admin_absent.fixture.root.join("retained-admin");
    std::fs::rename(&admin_absent.source.admin_directory, &retained_admin).unwrap();
    assert!(admin_absent.source.worktree_path().is_dir());
    assert_all_phases_reconcile(&admin_absent).await;
    assert!(retained_admin.join("HEAD").is_file());
}

#[tokio::test]
async fn relocking_is_accepted_only_by_the_unlock_pending_phase() {
    let prepared =
        PreparedCleanup::new("cleanup-relocked", "123e4567-e89b-12d3-a456-426614174309").await;
    prepared.raw_unlock();
    prepared.raw_lock();

    assert_eq!(
        prepared
            .classify_unlock(&unlock_capability(&prepared.intent).await)
            .await
            .unwrap(),
        DeliveryUnlockPendingDisposition::RetryExactUnlock,
    );
    assert_eq!(
        prepared
            .classify_unlocked_pending_remove(
                &unlocked_pending_remove_capability(&prepared.intent).await,
            )
            .await
            .unwrap(),
        DeliveryUnlockedPendingRemoveDisposition::ReconciliationRequired,
    );
    assert_eq!(
        prepared
            .classify_remove(&remove_capability(&prepared.intent).await)
            .await
            .unwrap(),
        DeliveryRemovePendingDisposition::ReconciliationRequired,
    );

    prepared.raw_unlock();
    prepared.raw_lock_with_reason("external-owner");
    assert_all_phases_reconcile(&prepared).await;
    assert!(
        prepared
            .source
            .worktree_path()
            .join("tracked.txt")
            .is_file()
    );
}

#[tokio::test]
async fn worktree_admin_and_gitdir_path_identity_drift_fail_closed_and_preserve_files() {
    let worktree_drift = PreparedCleanup::new(
        "cleanup-worktree-identity-drift",
        "123e4567-e89b-12d3-a456-426614174310",
    )
    .await;
    worktree_drift.raw_unlock();
    let retained_worktree = worktree_drift.fixture.root.join("original-worktree");
    replace_directory_with_logically_identical_copy(
        worktree_drift.source.worktree_path(),
        &retained_worktree,
    );
    let foreign = worktree_drift.source.worktree_path().join("foreign.txt");
    std::fs::write(&foreign, b"replacement content\n").unwrap();
    assert_eq!(
        worktree_drift
            .classify_remove(&remove_capability(&worktree_drift.intent).await)
            .await
            .unwrap(),
        DeliveryRemovePendingDisposition::ReconciliationRequired,
    );
    assert_eq!(std::fs::read(foreign).unwrap(), b"replacement content\n");

    let admin_drift = PreparedCleanup::new(
        "cleanup-admin-identity-drift",
        "123e4567-e89b-12d3-a456-426614174311",
    )
    .await;
    admin_drift.raw_unlock();
    let retained_admin = admin_drift.fixture.root.join("original-admin");
    replace_directory_with_logically_identical_copy(
        &admin_drift.source.admin_directory,
        &retained_admin,
    );
    assert_eq!(
        admin_drift
            .classify_remove(&remove_capability(&admin_drift.intent).await)
            .await
            .unwrap(),
        DeliveryRemovePendingDisposition::ReconciliationRequired,
    );
    assert!(
        admin_drift
            .source
            .worktree_path()
            .join("tracked.txt")
            .is_file()
    );

    let path_drift = PreparedCleanup::new(
        "cleanup-gitdir-path-drift",
        "123e4567-e89b-12d3-a456-426614174312",
    )
    .await;
    path_drift.raw_unlock();
    let pointer = path_drift.source.worktree_path().join(".git");
    std::fs::write(&pointer, b"gitdir: deliberately-wrong-admin\n").unwrap();
    assert_eq!(
        path_drift
            .classify_remove(&remove_capability(&path_drift.intent).await)
            .await
            .unwrap(),
        DeliveryRemovePendingDisposition::ReconciliationRequired,
    );
    assert_eq!(
        std::fs::read(pointer).unwrap(),
        b"gitdir: deliberately-wrong-admin\n",
    );
}

#[tokio::test]
async fn source_ref_and_common_git_identity_drift_fail_closed_without_removal() {
    let source_drift = PreparedCleanup::new(
        "cleanup-source-ref-drift",
        "123e4567-e89b-12d3-a456-426614174313",
    )
    .await;
    source_drift.raw_unlock();
    let source_ref = format!(
        "refs/heads/{}",
        source_drift.source.reservation.branch_name()
    );
    git_ok(
        &source_drift.fixture.repository,
        &[
            "update-ref",
            &source_ref,
            source_drift.source.reservation.base_commit(),
        ],
    );
    assert_eq!(
        source_drift
            .classify_remove(&remove_capability(&source_drift.intent).await)
            .await
            .unwrap(),
        DeliveryRemovePendingDisposition::ReconciliationRequired,
    );
    assert!(
        source_drift
            .source
            .worktree_path()
            .join("tracked.txt")
            .is_file()
    );

    let common_drift = PreparedCleanup::new(
        "cleanup-common-identity-drift",
        "123e4567-e89b-12d3-a456-426614174314",
    )
    .await;
    common_drift.raw_unlock();
    let common = common_drift.fixture.repository.join(".git");
    let retained_common = common_drift.fixture.root.join("original-common-git");
    replace_directory_with_logically_identical_copy(&common, &retained_common);
    let result = common_drift
        .classify_remove(&remove_capability(&common_drift.intent).await)
        .await;
    assert!(
        matches!(
            &result,
            Ok(DeliveryRemovePendingDisposition::ReconciliationRequired)
                | Err(DeliveryWorktreeCleanupError::AuthenticationChanged)
                | Err(DeliveryWorktreeCleanupError::SourceChanged)
        ),
        "common-directory replacement must fail closed: {result:?}",
    );
    assert!(
        common_drift
            .source
            .worktree_path()
            .join("tracked.txt")
            .is_file()
    );
}

#[tokio::test]
async fn raw_source_branch_ref_to_annotated_tag_reconciles_when_present_and_absent() {
    let prepared = PreparedCleanup::new(
        "cleanup-source-ref-tag-object",
        "123e4567-e89b-12d3-a456-426614174317",
    )
    .await;
    prepared.raw_unlock();
    let source_ref = format!("refs/heads/{}", prepared.source.reservation.branch_name());
    let tag_name = "cleanup-source-ref-tag-object";
    let tag_ref = format!("refs/tags/{tag_name}");
    git_ok(
        &prepared.fixture.repository,
        &[
            "tag",
            "--annotate",
            "--message",
            "cleanup source ref tag object",
            tag_name,
            &prepared.source_commit,
        ],
    );
    let tag_object = git_line(&prepared.fixture.repository, &["rev-parse", &tag_ref]);
    assert_ne!(tag_object, prepared.source_commit);
    assert_eq!(
        git_line(
            &prepared.fixture.repository,
            &["rev-parse", &format!("{tag_ref}^{{commit}}")],
        ),
        prepared.source_commit,
        "the annotated tag must peel to the expected source commit",
    );

    write_loose_ref(&prepared.fixture.repository, &source_ref, &tag_object);
    assert_eq!(
        git_line(&prepared.fixture.repository, &["rev-parse", &source_ref]),
        tag_object,
        "the raw source branch ref must name the tag object",
    );
    assert_eq!(
        git_line(
            &prepared.fixture.repository,
            &["rev-parse", &format!("{source_ref}^{{commit}}")],
        ),
        prepared.source_commit,
        "the drift must be invisible to a peeled-only comparison",
    );
    assert_eq!(
        prepared
            .classify_remove(&remove_capability(&prepared.intent).await)
            .await
            .unwrap(),
        DeliveryRemovePendingDisposition::ReconciliationRequired,
    );
    assert!(
        prepared
            .source
            .worktree_path()
            .join("tracked.txt")
            .is_file()
    );
    assert!(prepared.source.admin_directory.is_dir());

    write_loose_ref(
        &prepared.fixture.repository,
        &source_ref,
        &prepared.source_commit,
    );
    prepared.raw_remove();
    assert!(!prepared.source.worktree_path().exists());
    assert!(!prepared.source.admin_directory.exists());

    write_loose_ref(&prepared.fixture.repository, &source_ref, &tag_object);
    assert_eq!(
        prepared
            .classify_remove(&remove_capability(&prepared.intent).await)
            .await
            .unwrap(),
        DeliveryRemovePendingDisposition::ReconciliationRequired,
    );
    assert_eq!(
        git_line(&prepared.fixture.repository, &["rev-parse", &source_ref]),
        tag_object,
        "the absent classification must preserve the mismatched raw ref",
    );

    git_ok(
        &prepared.fixture.repository,
        &["update-ref", "-d", &source_ref, &tag_object],
    );
    assert_eq!(
        prepared
            .classify_remove(&remove_capability(&prepared.intent).await)
            .await
            .unwrap(),
        DeliveryRemovePendingDisposition::ReconciliationRequired,
        "a missing source ref is a reconciliation fact, not a command failure",
    );

    let victim_ref = "refs/heads/cleanup-symbolic-victim";
    git_ok(
        &prepared.fixture.repository,
        &["update-ref", victim_ref, &prepared.source_commit],
    );
    git_ok(
        &prepared.fixture.repository,
        &["symbolic-ref", &source_ref, victim_ref],
    );
    assert_eq!(
        prepared
            .classify_remove(&remove_capability(&prepared.intent).await)
            .await
            .unwrap(),
        DeliveryRemovePendingDisposition::ReconciliationRequired,
        "an absent worktree must not treat a recursively resolved symbolic source ref as exact",
    );
    assert_eq!(
        git_line(&prepared.fixture.repository, &["rev-parse", victim_ref]),
        prepared.source_commit,
        "cleanup must not touch the symbolic ref target",
    );
}

async fn unlock_capability(
    intent: &DeliveryWorktreeCleanupIntent,
) -> DeliveryUnlockPendingCapability {
    authorize_persisted_delivery_unlock(
        &PersistedCleanupAuthorization::for_intent(intent),
        intent.clone(),
    )
    .await
    .unwrap()
}

async fn unlocked_pending_remove_capability(
    intent: &DeliveryWorktreeCleanupIntent,
) -> DeliveryUnlockedPendingRemoveCapability {
    authorize_persisted_delivery_unlocked_pending_remove(
        &PersistedCleanupAuthorization::for_intent(intent),
        intent.clone(),
    )
    .await
    .unwrap()
}

async fn remove_capability(
    intent: &DeliveryWorktreeCleanupIntent,
) -> DeliveryRemovePendingCapability {
    authorize_persisted_delivery_remove(
        &PersistedCleanupAuthorization::for_intent(intent),
        intent.clone(),
    )
    .await
    .unwrap()
}

async fn assert_all_phases_reconcile(prepared: &PreparedCleanup) {
    assert_eq!(
        prepared
            .classify_unlock(&unlock_capability(&prepared.intent).await)
            .await
            .unwrap(),
        DeliveryUnlockPendingDisposition::ReconciliationRequired,
    );
    assert_eq!(
        prepared
            .classify_unlocked_pending_remove(
                &unlocked_pending_remove_capability(&prepared.intent).await,
            )
            .await
            .unwrap(),
        DeliveryUnlockedPendingRemoveDisposition::ReconciliationRequired,
    );
    assert_eq!(
        prepared
            .classify_remove(&remove_capability(&prepared.intent).await)
            .await
            .unwrap(),
        DeliveryRemovePendingDisposition::ReconciliationRequired,
    );
}

#[allow(clippy::too_many_arguments)]
async fn apply_source_commit(
    source_provisioner: &DeliverySourceProvisioner,
    source: &ReviewedDirtySource,
    opened: &DeliverySourceCapability,
    candidate: &DeliveryCandidateTree,
    source_commit: &DeliverySourceCommit,
    input: &DeliverySourceCommitInput,
) {
    let intent = DeliverySourceRecoveryIntent::from_source(
        DeliverySourcePendingState::CommitPending,
        opened,
        candidate,
        Some(source_commit),
        input.clone(),
    )
    .unwrap();
    let recovery = source_provisioner
        .open_delivery_source_for_recovery(&source.reservation, &intent, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        source_provisioner
            .apply_source_commit(&recovery, CancellationToken::new())
            .await
            .unwrap(),
        DeliverySourceRecoveryDisposition::Applied,
    );
}

#[allow(clippy::too_many_arguments)]
async fn apply_merge(
    fixture: &Fixture,
    source: &ReviewedDirtySource,
    source_provisioner: &DeliverySourceProvisioner,
    opened: &DeliverySourceCapability,
    candidate: &DeliveryCandidateTree,
    source_commit: &DeliverySourceCommit,
    source_input: &DeliverySourceCommitInput,
    delivery_process_scope: coding_agent_runtime::ProcessLivenessScope,
    task_id: &str,
) -> String {
    let target_provisioner = DeliveryTargetProvisioner::from_worktree_provisioner(
        &source.worktrees,
        Arc::clone(&fixture.delivery_git),
        &fixture.runtime_directory,
        delivery_process_scope,
        process_limits(),
        delivery_source_limits(),
    )
    .unwrap();
    let target = target_provisioner
        .open_delivery_target(&target_request(fixture), CancellationToken::new())
        .await
        .unwrap();
    let preflight = preflight_delivery_merge(
        source_provisioner,
        &target_provisioner,
        &target,
        DeliveryPreflightSource::committed(opened, candidate, source_commit, source_input),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let merge_input = DeliveryMergeInput::try_new(task_id, 1, EPOCH_SECONDS).unwrap();
    let expected = build_expected_delivery_merge(
        source_provisioner,
        &target_provisioner,
        opened,
        &target,
        candidate,
        source_commit,
        source_input,
        &preflight,
        &merge_input,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let expected_id = expected.object_id().to_owned();
    assert_eq!(
        apply_expected_delivery_merge(
            source_provisioner,
            &target_provisioner,
            opened,
            &target,
            candidate,
            source_commit,
            source_input,
            &preflight,
            &expected,
            CancellationToken::new(),
        )
        .await
        .unwrap(),
        DeliveryMergeOutcome::Applied,
    );
    // Every target capability and its dependent checkout handles are local to
    // this helper and close before cleanup intent capture begins.
    expected_id
}

fn source_provisioner(
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

fn target_request(fixture: &Fixture) -> DeliveryTargetRequest {
    DeliveryTargetRequest::try_new(
        git_line(&fixture.repository, &["symbolic-ref", "--short", "HEAD"]),
        git_line(&fixture.repository, &["rev-parse", "HEAD"]),
    )
    .unwrap()
}

fn worktree_command(fixture: &Fixture, source: &ReviewedDirtySource, operation: &[&str]) {
    let path = source.worktree_path().to_string_lossy();
    let mut arguments = vec!["worktree"];
    arguments.extend_from_slice(operation);
    arguments.extend(["--", path.as_ref()]);
    git_ok(&fixture.repository, &arguments);
}

fn delivery_process_scope(
    worker_process_scope: &coding_agent_runtime::ProcessLivenessScope,
) -> coding_agent_runtime::ProcessLivenessScope {
    let mut task_id = [0x35; 16];
    task_id[6] = 0x45;
    task_id[8] = 0xb5;
    worker_process_scope.sibling_task_scope(task_id).unwrap()
}

fn foreign_worker_process_scope(
    runtime_directory: &Path,
) -> (coding_agent_runtime::ProcessLivenessScope, [u8; 16]) {
    let mut instance_id = [0x15; 16];
    instance_id[6] = 0x45;
    instance_id[8] = 0x95;
    let mut task_id = [0x45; 16];
    task_id[6] = 0x45;
    task_id[8] = 0x85;
    let scope =
        ProcessLivenessDirectory::open(runtime_directory.join(".process-liveness-test-runtime"))
            .unwrap()
            .instance_scope(instance_id)
            .unwrap()
            .task_scope(task_id)
            .unwrap();
    (scope, task_id)
}

fn foreign_instance_worker_process_scope(
    runtime_directory: &Path,
) -> coding_agent_runtime::ProcessLivenessScope {
    let mut instance_id = [0x16; 16];
    instance_id[6] = 0x46;
    instance_id[8] = 0x96;
    ProcessLivenessDirectory::open(runtime_directory.join(".process-liveness-test-runtime"))
        .unwrap()
        .instance_scope(instance_id)
        .unwrap()
        .task_scope(worker_task_id())
        .unwrap()
}

fn worker_task_id() -> [u8; 16] {
    let mut task_id = [0x25; 16];
    task_id[6] = 0x45;
    task_id[8] = 0xa5;
    task_id
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

fn replace_directory_with_logically_identical_copy(directory: &Path, retained_original: &Path) {
    assert!(!retained_original.exists());
    std::fs::rename(directory, retained_original).unwrap();
    copy_directory_tree(retained_original, directory);
}

fn write_loose_ref(repository: &Path, ref_name: &str, object_id: &str) {
    let ref_path = repository.join(".git").join(ref_name);
    std::fs::create_dir_all(ref_path.parent().unwrap()).unwrap();
    std::fs::write(ref_path, format!("{object_id}\n")).unwrap();
}

fn copy_directory_tree(source: &Path, destination: &Path) {
    std::fs::create_dir(destination).unwrap();
    let mut entries = std::fs::read_dir(source)
        .unwrap()
        .map(|entry| entry.unwrap())
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let file_type = entry.file_type().unwrap();
        let replacement = destination.join(entry.file_name());
        if file_type.is_dir() {
            copy_directory_tree(&entry.path(), &replacement);
        } else {
            assert!(file_type.is_file());
            std::fs::copy(entry.path(), replacement).unwrap();
        }
    }
}
