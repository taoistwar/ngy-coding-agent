mod delivery_source_support;

use std::sync::Arc;
#[cfg(feature = "test-support")]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use coding_agent_runtime::{
    DeliveryBranchCleanupIntent, DeliveryBranchCleanupRecoveryBindingOutcome,
    DeliveryCandidateTree, DeliveryDeletePendingAuthorizer, DeliveryDeletePendingCapability,
    DeliveryDeletePendingDisposition, DeliveryPersistedSourceRecovery,
    DeliveryPersistedSourceState, DeliveryPersistedTargetRecovery, DeliveryPersistenceBinding,
    DeliveryRemovePendingAuthorizer, DeliveryRemovePendingDisposition, DeliverySourceCapability,
    DeliverySourceCommit, DeliverySourceCommitInput, DeliverySourcePendingState,
    DeliverySourceProvisioner, DeliverySourceRecoveryDisposition, DeliverySourceRecoveryIntent,
    DeliveryTargetProvisioner, DeliveryTargetRequest, DeliveryUnlockPendingAuthorizer,
    DeliveryUnlockPendingDisposition, DeliveryUnlockedPendingRemoveAuthorizer,
    DeliveryUnlockedPendingRemoveDisposition, DeliveryWorktreeCleanupIntent,
    DeliveryWorktreeCleanupProvisioner, DeliveryWorktreeCleanupRecoveryBindingOutcome,
    DeliveryWorktreeCleanupRecoveryPhase, FingerprintLimits, ProcessLimits,
    SealedProcessLivenessScope, WorktreeProvisioner, authorize_persisted_delivery_branch_delete,
    authorize_persisted_delivery_remove, authorize_persisted_delivery_unlock,
    authorize_persisted_delivery_unlocked_pending_remove,
};
use delivery_source_support::{
    Fixture, ReviewedDirtySource, delivery_source_limits, git_line, git_ok,
};
use tokio_util::sync::CancellationToken;

const EPOCH_SECONDS: i64 = 1_700_000_022;

struct PreparedRecovery {
    cleanup: DeliveryWorktreeCleanupProvisioner,
    source_provisioner: DeliverySourceProvisioner,
    target_provisioner: DeliveryTargetProvisioner,
    persisted_source: DeliveryPersistedSourceRecovery,
    persisted_target: DeliveryPersistedTargetRecovery,
    persistence: DeliveryPersistenceBinding,
    source_input: DeliverySourceCommitInput,
    candidate_tree: String,
    source_commit: String,
    target_head: String,
    sealed_worker: SealedProcessLivenessScope,
    source: ReviewedDirtySource,
    fixture: Fixture,
}

impl PreparedRecovery {
    async fn new(name: &str, task_id: &str) -> Self {
        let fixture = Fixture::new(name).await;
        let source = fixture.reviewed_dirty_source(task_id).await;
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

        git_ok(
            &fixture.repository,
            &[
                "merge",
                "--quiet",
                "--no-ff",
                "--no-edit",
                "--message",
                "persisted cleanup recovery fixture merge",
                source_commit.object_id(),
            ],
        );
        let target_branch = git_line(&fixture.repository, &["symbolic-ref", "--short", "HEAD"]);
        let target_head = git_line(&fixture.repository, &["rev-parse", "HEAD"]);
        let target_provisioner = DeliveryTargetProvisioner::from_worktree_provisioner(
            &source.worktrees,
            Arc::clone(&fixture.delivery_git),
            &fixture.runtime_directory,
            delivery_process_scope.clone(),
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
        let persistence = opened.persistence_binding_for_target(&target).unwrap();
        let persisted_source = committed_source(
            &persistence,
            candidate.object_id(),
            source_commit.object_id(),
            source_input.clone(),
            persistence.common_git_identity_digest(),
            persistence.worktree_admin_identity_digest(),
            persistence.source_config_attributes_digest(),
        );
        let persisted_target = DeliveryPersistedTargetRecovery::try_new(
            persistence.object_format(),
            persistence.target_branch(),
            persistence.expected_target_head(),
            persistence.common_git_identity_algorithm(),
            persistence.common_git_identity_digest(),
            persistence.target_config_attributes_digest(),
            persistence.target_security_digest(),
        )
        .unwrap();
        let candidate_tree = candidate.object_id().to_owned();
        let source_commit_id = source_commit.object_id().to_owned();
        drop(target);
        drop(opened);

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
        Self {
            cleanup,
            source_provisioner,
            target_provisioner,
            persisted_source,
            persisted_target,
            persistence,
            source_input,
            candidate_tree,
            source_commit: source_commit_id,
            target_head,
            sealed_worker,
            source,
            fixture,
        }
    }

    async fn bind_worktree(
        &self,
        persisted: &DeliveryPersistedSourceRecovery,
    ) -> DeliveryWorktreeCleanupRecoveryBindingOutcome {
        self.bind_worktree_phase(
            persisted,
            DeliveryWorktreeCleanupRecoveryPhase::UnlockPending,
        )
        .await
    }

    async fn bind_worktree_phase(
        &self,
        persisted: &DeliveryPersistedSourceRecovery,
        phase: DeliveryWorktreeCleanupRecoveryPhase,
    ) -> DeliveryWorktreeCleanupRecoveryBindingOutcome {
        self.cleanup
            .bind_persisted_delivery_worktree_cleanup(
                phase,
                &self.source_provisioner,
                &self.target_provisioner,
                &self.source.reservation,
                persisted,
                &self.persisted_target,
                &self.sealed_worker,
                CancellationToken::new(),
            )
            .await
            .unwrap()
    }

    async fn bind_branch(&self) -> DeliveryBranchCleanupRecoveryBindingOutcome {
        self.cleanup
            .bind_persisted_delivery_branch_cleanup(
                &self.source_provisioner,
                &self.target_provisioner,
                &self.source.reservation,
                &self.persisted_source,
                &self.persisted_target,
                &self.sealed_worker,
                CancellationToken::new(),
            )
            .await
            .unwrap()
    }

    fn raw_unlock(&self) {
        worktree_command(&self.fixture, &self.source, &["unlock"]);
    }

    fn raw_remove(&self) {
        worktree_command(&self.fixture, &self.source, &["remove"]);
    }

    fn source_oid(&self) -> String {
        git_line(
            &self.fixture.repository,
            &["rev-parse", self.persistence.source_branch()],
        )
    }
}

#[derive(Debug)]
struct AllowPersistedPhase;

#[async_trait]
impl DeliveryRemovePendingAuthorizer for AllowPersistedPhase {
    type Error = &'static str;

    async fn authorize_persisted_remove_pending(
        &self,
        _: &DeliveryWorktreeCleanupIntent,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
impl DeliveryDeletePendingAuthorizer for AllowPersistedPhase {
    type Error = &'static str;

    async fn authorize_persisted_delete_pending(
        &self,
        _: &DeliveryBranchCleanupIntent,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
impl DeliveryUnlockPendingAuthorizer for AllowPersistedPhase {
    type Error = &'static str;

    async fn authorize_persisted_unlock_pending(
        &self,
        _: &DeliveryWorktreeCleanupIntent,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
impl DeliveryUnlockedPendingRemoveAuthorizer for AllowPersistedPhase {
    type Error = &'static str;

    async fn authorize_persisted_unlocked_pending_remove(
        &self,
        _: &DeliveryWorktreeCleanupIntent,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[tokio::test]
async fn persisted_dirty_rebind_defers_to_each_phase_without_remove_child() {
    let mut prepared = PreparedRecovery::new(
        "cleanup-recovery-persisted-dirty-phases",
        "123e4567-e89b-12d3-a456-426614174421",
    )
    .await;
    let retained = prepared
        .source
        .worktree_path()
        .join("late-persisted-user-file.txt");
    std::fs::write(&retained, b"must survive persisted recovery\n").unwrap();
    #[cfg(feature = "test-support")]
    let unlock_spawns = Arc::new(AtomicUsize::new(0));
    #[cfg(feature = "test-support")]
    let remove_spawns = Arc::new(AtomicUsize::new(0));
    #[cfg(feature = "test-support")]
    prepared.cleanup.set_cleanup_boundary_hook_for_tests({
        let unlock_spawns = Arc::clone(&unlock_spawns);
        let remove_spawns = Arc::clone(&remove_spawns);
        move |phase| match phase {
            "before-actual-unlock-spawn" => {
                unlock_spawns.fetch_add(1, Ordering::SeqCst);
            }
            "before-actual-remove-spawn" => {
                remove_spawns.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        }
    });

    let fresh_error = prepared
        .cleanup
        .bind_delivery_worktree_cleanup_acceptance(
            &prepared.source_provisioner,
            &prepared.target_provisioner,
            &prepared.source.reservation,
            &prepared.persisted_source,
            &prepared.persisted_target,
            &prepared.sealed_worker,
            CancellationToken::new(),
        )
        .await
        .expect_err("fresh cleanup acceptance must reject the same dirty source");
    assert_eq!(fresh_error.code(), "TARGET_WORKTREE_DIRTY");

    let locked = match prepared
        .bind_worktree_phase(
            &prepared.persisted_source,
            DeliveryWorktreeCleanupRecoveryPhase::UnlockPending,
        )
        .await
    {
        DeliveryWorktreeCleanupRecoveryBindingOutcome::Bound(intent) => intent,
        DeliveryWorktreeCleanupRecoveryBindingOutcome::ReconciliationRequired => {
            panic!("authenticated locked dirty recovery must bind for UnlockPending")
        }
    };
    let unlock = authorize_persisted_delivery_unlock(&AllowPersistedPhase, locked)
        .await
        .unwrap();
    assert_eq!(
        prepared
            .cleanup
            .retry_delivery_unlock_pending(
                &prepared.source_provisioner,
                unlock,
                &prepared.sealed_worker,
                CancellationToken::new(),
            )
            .await
            .unwrap(),
        DeliveryUnlockPendingDisposition::UnlockApplied,
    );
    #[cfg(feature = "test-support")]
    assert_eq!(unlock_spawns.load(Ordering::SeqCst), 1);

    let unlocked = match prepared
        .bind_worktree_phase(
            &prepared.persisted_source,
            DeliveryWorktreeCleanupRecoveryPhase::UnlockedPendingRemove,
        )
        .await
    {
        DeliveryWorktreeCleanupRecoveryBindingOutcome::Bound(intent) => intent,
        DeliveryWorktreeCleanupRecoveryBindingOutcome::ReconciliationRequired => {
            panic!("authenticated unlocked dirty recovery must reach the phase decision")
        }
    };
    let bridge = authorize_persisted_delivery_unlocked_pending_remove(
        &AllowPersistedPhase,
        unlocked.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        prepared
            .cleanup
            .classify_delivery_unlocked_pending_remove(
                &prepared.source_provisioner,
                &bridge,
                &prepared.sealed_worker,
                CancellationToken::new(),
            )
            .await
            .unwrap(),
        DeliveryUnlockedPendingRemoveDisposition::ReconciliationRequired,
    );
    let remove_intent = match prepared
        .bind_worktree_phase(
            &prepared.persisted_source,
            DeliveryWorktreeCleanupRecoveryPhase::RemovePending,
        )
        .await
    {
        DeliveryWorktreeCleanupRecoveryBindingOutcome::Bound(intent) => intent,
        DeliveryWorktreeCleanupRecoveryBindingOutcome::ReconciliationRequired => {
            panic!("authenticated unlocked dirty recovery must reach RemovePending decision")
        }
    };
    let remove = authorize_persisted_delivery_remove(&AllowPersistedPhase, remove_intent)
        .await
        .unwrap();
    assert_eq!(
        prepared
            .cleanup
            .retry_delivery_remove_pending(
                &prepared.source_provisioner,
                remove,
                &prepared.sealed_worker,
                CancellationToken::new(),
            )
            .await
            .unwrap(),
        DeliveryRemovePendingDisposition::KnownNotAppliedDirty,
    );
    #[cfg(feature = "test-support")]
    assert_eq!(remove_spawns.load(Ordering::SeqCst), 0);
    assert_eq!(
        std::fs::read(&retained).unwrap(),
        b"must survive persisted recovery\n"
    );

    let dirty_then_clean = match prepared
        .bind_worktree_phase(
            &prepared.persisted_source,
            DeliveryWorktreeCleanupRecoveryPhase::RemovePending,
        )
        .await
    {
        DeliveryWorktreeCleanupRecoveryBindingOutcome::Bound(intent) => intent,
        DeliveryWorktreeCleanupRecoveryBindingOutcome::ReconciliationRequired => {
            panic!("dirty RemovePending proof must bind before the scene changes")
        }
    };
    std::fs::remove_file(&retained).unwrap();
    let remove = authorize_persisted_delivery_remove(&AllowPersistedPhase, dirty_then_clean)
        .await
        .unwrap();
    assert_eq!(
        prepared
            .cleanup
            .retry_delivery_remove_pending(
                &prepared.source_provisioner,
                remove,
                &prepared.sealed_worker,
                CancellationToken::new(),
            )
            .await
            .unwrap(),
        DeliveryRemovePendingDisposition::ReconciliationRequired,
        "a dirty recovery proof must not become remove authority after the scene becomes clean",
    );
    #[cfg(feature = "test-support")]
    assert_eq!(remove_spawns.load(Ordering::SeqCst), 0);
    assert!(prepared.source.worktree_path().is_dir());
    assert!(
        prepared
            .source
            .worktree_path()
            .join("tracked.txt")
            .is_file()
    );
}

#[tokio::test]
async fn locked_unlocked_absent_and_removed_restart_bind_without_phase_authority() {
    let prepared = PreparedRecovery::new(
        "cleanup-recovery-topology-phases",
        "123e4567-e89b-12d3-a456-426614174422",
    )
    .await;

    let locked_outcome = prepared.bind_worktree(&prepared.persisted_source).await;
    assert_eq!(
        format!("{locked_outcome:?}"),
        "DeliveryWorktreeCleanupRecoveryBindingOutcome::Bound(<opaque>)"
    );
    let locked = match locked_outcome {
        DeliveryWorktreeCleanupRecoveryBindingOutcome::Bound(intent) => intent,
        DeliveryWorktreeCleanupRecoveryBindingOutcome::ReconciliationRequired => {
            panic!("exact locked restart must bind")
        }
    };
    assert_eq!(
        format!("{locked:?}"),
        "DeliveryWorktreeCleanupIntent(<opaque>)"
    );
    let locked = authorize_persisted_delivery_unlock(&AllowPersistedPhase, locked)
        .await
        .unwrap();
    assert!(matches!(
        prepared
            .cleanup
            .classify_delivery_unlock_pending(
                &prepared.source_provisioner,
                &locked,
                &prepared.sealed_worker,
                CancellationToken::new(),
            )
            .await
            .unwrap(),
        DeliveryUnlockPendingDisposition::RetryExactUnlock
    ));
    drop(locked);

    prepared.raw_unlock();
    let unlocked = match prepared.bind_worktree(&prepared.persisted_source).await {
        DeliveryWorktreeCleanupRecoveryBindingOutcome::Bound(intent) => intent,
        DeliveryWorktreeCleanupRecoveryBindingOutcome::ReconciliationRequired => {
            panic!("exact unlocked restart must bind")
        }
    };
    let unlock_phase = authorize_persisted_delivery_unlock(&AllowPersistedPhase, unlocked.clone())
        .await
        .unwrap();
    assert!(matches!(
        prepared
            .cleanup
            .classify_delivery_unlock_pending(
                &prepared.source_provisioner,
                &unlock_phase,
                &prepared.sealed_worker,
                CancellationToken::new(),
            )
            .await
            .unwrap(),
        DeliveryUnlockPendingDisposition::UnlockApplied
    ));
    let remove_bridge =
        authorize_persisted_delivery_unlocked_pending_remove(&AllowPersistedPhase, unlocked)
            .await
            .unwrap();
    assert!(matches!(
        prepared
            .cleanup
            .classify_delivery_unlocked_pending_remove(
                &prepared.source_provisioner,
                &remove_bridge,
                &prepared.sealed_worker,
                CancellationToken::new(),
            )
            .await
            .unwrap(),
        DeliveryUnlockedPendingRemoveDisposition::EnterRemovePending
    ));

    prepared.raw_remove();
    // Exact absence deliberately reuses the authentic historical Store
    // digest: the removed worktree/admin no longer contains a current
    // worktree-specific config/attributes scene to replay. The binder still
    // re-proves the common topology, absent admin aliases, source ref, and
    // exact committed object before returning an intent.
    let absent = match prepared.bind_worktree(&prepared.persisted_source).await {
        DeliveryWorktreeCleanupRecoveryBindingOutcome::Bound(intent) => intent,
        DeliveryWorktreeCleanupRecoveryBindingOutcome::ReconciliationRequired => {
            panic!("exact absent restart must bind")
        }
    };
    let remove = authorize_persisted_delivery_remove(&AllowPersistedPhase, absent)
        .await
        .unwrap();
    assert!(matches!(
        prepared
            .cleanup
            .classify_delivery_remove_pending(
                &prepared.source_provisioner,
                &remove,
                &prepared.sealed_worker,
                CancellationToken::new(),
            )
            .await
            .unwrap(),
        DeliveryRemovePendingDisposition::Removed
    ));
}

#[tokio::test]
async fn persisted_common_admin_source_and_partial_drift_fail_closed_without_mutation() {
    let prepared = PreparedRecovery::new(
        "cleanup-recovery-persisted-drift",
        "123e4567-e89b-12d3-a456-426614174423",
    )
    .await;
    let wrong_common = different_digest(prepared.persistence.common_git_identity_digest(), "11");
    let wrong_admin = different_digest(prepared.persistence.worktree_admin_identity_digest(), "22");
    let wrong_config =
        different_digest(prepared.persistence.source_config_attributes_digest(), "33");
    let persisted = [
        (
            "common identity digest",
            committed_source(
                &prepared.persistence,
                &prepared.candidate_tree,
                &prepared.source_commit,
                prepared.source_input.clone(),
                &wrong_common,
                prepared.persistence.worktree_admin_identity_digest(),
                prepared.persistence.source_config_attributes_digest(),
            ),
        ),
        (
            "admin identity digest",
            committed_source(
                &prepared.persistence,
                &prepared.candidate_tree,
                &prepared.source_commit,
                prepared.source_input.clone(),
                prepared.persistence.common_git_identity_digest(),
                &wrong_admin,
                prepared.persistence.source_config_attributes_digest(),
            ),
        ),
        (
            "source config attributes digest",
            committed_source(
                &prepared.persistence,
                &prepared.candidate_tree,
                &prepared.source_commit,
                prepared.source_input.clone(),
                prepared.persistence.common_git_identity_digest(),
                prepared.persistence.worktree_admin_identity_digest(),
                &wrong_config,
            ),
        ),
        (
            "expected source commit",
            committed_source(
                &prepared.persistence,
                &prepared.candidate_tree,
                prepared.persistence.source_base_commit(),
                prepared.source_input.clone(),
                prepared.persistence.common_git_identity_digest(),
                prepared.persistence.worktree_admin_identity_digest(),
                prepared.persistence.source_config_attributes_digest(),
            ),
        ),
    ];
    let before = prepared.source.snapshot(&prepared.fixture.repository);
    for (case, drifted) in &persisted {
        assert!(
            matches!(
                prepared.bind_worktree(drifted).await,
                DeliveryWorktreeCleanupRecoveryBindingOutcome::ReconciliationRequired
            ),
            "persisted drift case unexpectedly bound: {case}"
        );
    }
    assert_eq!(
        prepared.source.snapshot(&prepared.fixture.repository),
        before
    );

    git_ok(
        &prepared.fixture.repository,
        &[
            "update-ref",
            prepared.persistence.source_branch(),
            prepared.persistence.source_base_commit(),
        ],
    );
    let after_external_source_drift = prepared.source.snapshot(&prepared.fixture.repository);
    assert!(matches!(
        prepared.bind_worktree(&prepared.persisted_source).await,
        DeliveryWorktreeCleanupRecoveryBindingOutcome::ReconciliationRequired
    ));
    assert_eq!(
        prepared.source.snapshot(&prepared.fixture.repository),
        after_external_source_drift,
        "source-ref drift is observed without repair or mutation"
    );

    let renamed = PreparedRecovery::new(
        "cleanup-recovery-renamed-admin",
        "123e4567-e89b-12d3-a456-426614174424",
    )
    .await;
    let renamed_path = renamed
        .source
        .admin_directory
        .with_file_name("renamed-admin");
    std::fs::rename(&renamed.source.admin_directory, &renamed_path).unwrap();
    assert!(matches!(
        renamed.bind_worktree(&renamed.persisted_source).await,
        DeliveryWorktreeCleanupRecoveryBindingOutcome::ReconciliationRequired
    ));
    assert!(renamed.source.worktree_path().join("tracked.txt").exists());
    assert_eq!(renamed.source_oid(), renamed.source_commit);

    let partial = PreparedRecovery::new(
        "cleanup-recovery-partial",
        "123e4567-e89b-12d3-a456-426614174425",
    )
    .await;
    std::fs::remove_dir_all(partial.source.worktree_path()).unwrap();
    assert!(partial.source.admin_directory.exists());
    assert!(matches!(
        partial.bind_worktree(&partial.persisted_source).await,
        DeliveryWorktreeCleanupRecoveryBindingOutcome::ReconciliationRequired
    ));
    assert!(partial.source.admin_directory.exists());
    assert_eq!(partial.source_oid(), partial.source_commit);
}

#[tokio::test]
async fn recovered_branch_refresh_revokes_every_old_delete_capability() {
    let prepared = PreparedRecovery::new(
        "cleanup-recovery-branch-refresh",
        "123e4567-e89b-12d3-a456-426614174426",
    )
    .await;
    prepared.raw_unlock();
    prepared.raw_remove();
    let branch_outcome = prepared.bind_branch().await;
    assert_eq!(
        format!("{branch_outcome:?}"),
        "DeliveryBranchCleanupRecoveryBindingOutcome::Bound(<opaque>)"
    );
    let intent = match branch_outcome {
        DeliveryBranchCleanupRecoveryBindingOutcome::Bound(intent) => intent,
        DeliveryBranchCleanupRecoveryBindingOutcome::ReconciliationRequired => {
            panic!("exact removed worktree and target must bind for branch cleanup")
        }
    };
    assert_eq!(
        format!("{intent:?}"),
        "DeliveryBranchCleanupIntent(<opaque>)"
    );
    let first_old = delete_capability(&intent).await;
    let second_old = delete_capability(&intent).await;

    std::fs::write(
        prepared.fixture.repository.join("forward.txt"),
        b"legal target forward\n",
    )
    .unwrap();
    git_ok(&prepared.fixture.repository, &["add", "--", "forward.txt"]);
    git_ok(
        &prepared.fixture.repository,
        &[
            "commit",
            "--quiet",
            "--no-gpg-sign",
            "-m",
            "legal target forward",
        ],
    );
    let fresh_target = git_line(&prepared.fixture.repository, &["rev-parse", "HEAD"]);
    let refresh = match prepared
        .cleanup
        .classify_delivery_delete_pending(
            &first_old,
            &prepared.sealed_worker,
            CancellationToken::new(),
        )
        .await
        .unwrap()
    {
        DeliveryDeletePendingDisposition::RefreshExpectedTarget(refresh) => refresh,
        _ => panic!("legal target forward must require a persisted refresh"),
    };
    assert_eq!(refresh.fresh_target_head(), fresh_target);
    let refreshed = refresh
        .into_refreshed_intent()
        .expect("persisted refresh adopts the next generation");

    git_ok(
        &prepared.fixture.repository,
        &["reset", "--hard", "--quiet", &prepared.target_head],
    );
    assert!(matches!(
        prepared
            .cleanup
            .retry_delivery_delete_pending(
                second_old,
                &prepared.sealed_worker,
                CancellationToken::new(),
            )
            .await
            .unwrap(),
        DeliveryDeletePendingDisposition::ReconciliationRequired
    ));
    assert_eq!(prepared.source_oid(), prepared.source_commit);

    git_ok(
        &prepared.fixture.repository,
        &["reset", "--hard", "--quiet", &fresh_target],
    );
    let fresh = delete_capability(&refreshed).await;
    assert!(matches!(
        prepared
            .cleanup
            .retry_delivery_delete_pending(
                fresh,
                &prepared.sealed_worker,
                CancellationToken::new(),
            )
            .await
            .unwrap(),
        DeliveryDeletePendingDisposition::Deleted
    ));
    assert_eq!(
        git_line(
            &prepared.fixture.repository,
            &[
                "for-each-ref",
                "--format=%(objectname)",
                prepared.persistence.source_branch(),
            ],
        ),
        ""
    );
}

async fn delete_capability(
    intent: &DeliveryBranchCleanupIntent,
) -> DeliveryDeletePendingCapability {
    authorize_persisted_delivery_branch_delete(&AllowPersistedPhase, intent.clone())
        .await
        .unwrap()
}

#[allow(clippy::too_many_arguments)]
fn committed_source(
    persistence: &DeliveryPersistenceBinding,
    candidate_tree: &str,
    expected_source_commit: &str,
    source_input: DeliverySourceCommitInput,
    common_digest: &str,
    admin_digest: &str,
    config_digest: &str,
) -> DeliveryPersistedSourceRecovery {
    DeliveryPersistedSourceRecovery::try_new(
        persistence.object_format(),
        DeliveryPersistedSourceState::Committed,
        persistence.source_identity().clone(),
        persistence.source_branch(),
        persistence.source_base_commit(),
        persistence.approved_fingerprint(),
        candidate_tree,
        Some(expected_source_commit),
        source_input,
        persistence.common_git_identity_algorithm(),
        common_digest,
        persistence.worktree_admin_identity_algorithm(),
        admin_digest,
        config_digest,
    )
    .unwrap()
}

fn different_digest(current: &str, byte: &str) -> String {
    let candidate = byte.repeat(32);
    if current == candidate {
        "ff".repeat(32)
    } else {
        candidate
    }
}

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
    let mut task_id = [0x36; 16];
    task_id[6] = 0x46;
    task_id[8] = 0xb6;
    worker_process_scope.sibling_task_scope(task_id).unwrap()
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
