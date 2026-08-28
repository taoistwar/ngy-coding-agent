mod delivery_source_support;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "test-support")]
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use coding_agent_runtime::{
    DeliveryBranchCleanupIntent, DeliveryCandidateTree, DeliveryDeletePendingAuthorizer,
    DeliveryDeletePendingCapability, DeliveryDeletePendingDisposition, DeliverySourceCapability,
    DeliverySourceCommit, DeliverySourceCommitInput, DeliverySourcePendingState,
    DeliverySourceProvisioner, DeliverySourceRecoveryDisposition, DeliverySourceRecoveryIntent,
    DeliveryTargetProvisioner, DeliveryTargetRequest, DeliveryWorktreeCleanupProvisioner,
    FingerprintLimits, ProcessLimits, SealedProcessLivenessScope, WorktreeProvisioner,
    authorize_persisted_delivery_branch_delete,
};
use delivery_source_support::{
    Fixture, ReviewedDirtySource, delivery_source_limits, git_line, git_ok,
};
use tokio_util::sync::CancellationToken;

const EPOCH_SECONDS: i64 = 1_700_000_017;

struct PreparedBranchCleanup {
    cleanup: DeliveryWorktreeCleanupProvisioner,
    intent: DeliveryBranchCleanupIntent,
    sealed_worker: SealedProcessLivenessScope,
    source_commit: String,
    source_branch: String,
    source_ref: String,
    base_commit: String,
    target_ref: String,
    target_head: String,
    fixture: Fixture,
}

impl PreparedBranchCleanup {
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

        let source_commit_id = source_commit.object_id().to_owned();
        let source_branch = source.reservation.branch_name().to_owned();
        let source_ref = format!("refs/heads/{source_branch}");
        let base_commit = source.reservation.base_commit().to_owned();
        let target_branch = git_line(&fixture.repository, &["symbolic-ref", "--short", "HEAD"]);
        let target_ref = format!("refs/heads/{target_branch}");
        git_ok(
            &fixture.repository,
            &[
                "merge",
                "--quiet",
                "--no-ff",
                "--no-edit",
                "--message",
                "branch cleanup fixture merge",
                &source_commit_id,
            ],
        );
        let target_head = git_line(&fixture.repository, &["rev-parse", "HEAD"]);
        assert_ne!(target_head, source_commit_id);

        let cleanup = DeliveryWorktreeCleanupProvisioner::from_worktree_provisioner(
            &source.worktrees,
            Arc::clone(&fixture.delivery_git),
            &fixture.runtime_directory,
            delivery_process_scope.clone(),
            process_limits(),
            delivery_source_limits(),
        )
        .unwrap();
        let sealed_worker = worker_process_scope
            .seal_task_scope(worker_task_id())
            .unwrap();
        let worktree_intent = cleanup
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

        worktree_command(&fixture, &source, &["unlock"]);
        worktree_command(&fixture, &source, &["remove"]);
        assert!(!source.worktree_path().exists());
        assert!(!source.admin_directory.exists());

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
            .open_delivery_target(
                &DeliveryTargetRequest::try_new(&target_branch, &target_head).unwrap(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let intent = cleanup
            .capture_branch_cleanup_intent(
                &source_provisioner,
                worktree_intent,
                target,
                &sealed_worker,
                CancellationToken::new(),
            )
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "capture branch-cleanup intent failed with stable code {}",
                    error.code()
                )
            });

        Self {
            cleanup,
            intent,
            sealed_worker,
            source_commit: source_commit_id,
            source_branch,
            source_ref,
            base_commit,
            target_ref,
            target_head,
            fixture,
        }
    }

    async fn classify(
        &self,
        capability: &DeliveryDeletePendingCapability,
    ) -> DeliveryDeletePendingDisposition {
        self.cleanup
            .classify_delivery_delete_pending(
                capability,
                &self.sealed_worker,
                CancellationToken::new(),
            )
            .await
            .unwrap()
    }

    async fn retry(
        &self,
        capability: DeliveryDeletePendingCapability,
    ) -> DeliveryDeletePendingDisposition {
        self.cleanup
            .retry_delivery_delete_pending(
                capability,
                &self.sealed_worker,
                CancellationToken::new(),
            )
            .await
            .unwrap()
    }

    fn source_oid(&self) -> String {
        git_line(&self.fixture.repository, &["rev-parse", &self.source_ref])
    }

    fn target_oid(&self) -> String {
        git_line(&self.fixture.repository, &["rev-parse", &self.target_ref])
    }

    fn assert_source_deleted(&self) {
        assert_eq!(
            git_line(
                &self.fixture.repository,
                &["for-each-ref", "--format=%(objectname)", &self.source_ref],
            ),
            "",
        );
    }
}

struct PersistedDeleteAuthorization {
    expected: DeliveryBranchCleanupIntent,
}

impl PersistedDeleteAuthorization {
    fn for_intent(intent: &DeliveryBranchCleanupIntent) -> Self {
        Self {
            expected: intent.clone(),
        }
    }
}

#[async_trait]
impl DeliveryDeletePendingAuthorizer for PersistedDeleteAuthorization {
    type Error = &'static str;

    async fn authorize_persisted_delete_pending(
        &self,
        intent: &DeliveryBranchCleanupIntent,
    ) -> Result<(), Self::Error> {
        if self.expected.is_same_runtime_intent(intent) {
            Ok(())
        } else {
            Err("branch cleanup intent did not match the persisted delete operation")
        }
    }
}

#[tokio::test]
async fn exact_source_and_target_retry_one_atomic_delete_transaction() {
    let prepared = PreparedBranchCleanup::new(
        "branch-cleanup-exact-delete",
        "123e4567-e89b-12d3-a456-426614174401",
    )
    .await;
    let capability = delete_capability(&prepared.intent).await;

    assert!(matches!(
        prepared.classify(&capability).await,
        DeliveryDeletePendingDisposition::RetryExactDelete
    ));
    assert!(matches!(
        prepared.retry(capability).await,
        DeliveryDeletePendingDisposition::Deleted
    ));
    prepared.assert_source_deleted();
    assert_eq!(prepared.target_oid(), prepared.target_head);
}

#[tokio::test]
async fn reply_lost_retry_observes_deleted_and_spawns_only_once() {
    #[allow(unused_mut)]
    let mut prepared = PreparedBranchCleanup::new(
        "branch-cleanup-reply-lost",
        "123e4567-e89b-12d3-a456-426614174402",
    )
    .await;
    #[cfg(feature = "test-support")]
    let spawns = Arc::new(AtomicUsize::new(0));
    #[cfg(feature = "test-support")]
    {
        let observed = Arc::clone(&spawns);
        prepared
            .cleanup
            .set_branch_cleanup_boundary_hook_for_tests(move |phase| {
                if phase == "after-branch-query-before-delete-spawn" {
                    observed.fetch_add(1, Ordering::SeqCst);
                }
            });
    }

    let _lost_reply = prepared
        .retry(delete_capability(&prepared.intent).await)
        .await;
    prepared.assert_source_deleted();
    assert_eq!(prepared.target_oid(), prepared.target_head);
    let capability = delete_capability(&prepared.intent).await;
    assert!(matches!(
        prepared.classify(&capability).await,
        DeliveryDeletePendingDisposition::Deleted
    ));
    assert!(matches!(
        prepared.retry(capability).await,
        DeliveryDeletePendingDisposition::Deleted
    ));
    #[cfg(feature = "test-support")]
    assert_eq!(spawns.load(Ordering::SeqCst), 1);
    prepared.assert_source_deleted();
    assert_eq!(prepared.target_oid(), prepared.target_head);
}

#[tokio::test]
async fn source_absent_is_query_first_deleted_without_a_spawn() {
    #[allow(unused_mut)]
    let mut prepared = PreparedBranchCleanup::new(
        "branch-cleanup-already-absent",
        "123e4567-e89b-12d3-a456-426614174403",
    )
    .await;
    git_ok(
        &prepared.fixture.repository,
        &[
            "update-ref",
            "--no-deref",
            "-d",
            &prepared.source_ref,
            &prepared.source_commit,
        ],
    );
    #[cfg(feature = "test-support")]
    let spawns = Arc::new(AtomicUsize::new(0));
    #[cfg(feature = "test-support")]
    {
        let observed = Arc::clone(&spawns);
        prepared
            .cleanup
            .set_branch_cleanup_boundary_hook_for_tests(move |phase| {
                if phase == "after-branch-query-before-delete-spawn" {
                    observed.fetch_add(1, Ordering::SeqCst);
                }
            });
    }

    let capability = delete_capability(&prepared.intent).await;
    assert!(matches!(
        prepared.classify(&capability).await,
        DeliveryDeletePendingDisposition::Deleted
    ));
    assert!(matches!(
        prepared.retry(capability).await,
        DeliveryDeletePendingDisposition::Deleted
    ));
    #[cfg(feature = "test-support")]
    assert_eq!(spawns.load(Ordering::SeqCst), 0);
    prepared.assert_source_deleted();
    assert_eq!(prepared.target_oid(), prepared.target_head);
}

#[tokio::test]
async fn legal_target_forward_requires_a_persisted_refreshed_intent() {
    #[allow(unused_mut)]
    let mut prepared = PreparedBranchCleanup::new(
        "branch-cleanup-target-forward",
        "123e4567-e89b-12d3-a456-426614174404",
    )
    .await;
    #[cfg(feature = "test-support")]
    let spawns = Arc::new(AtomicUsize::new(0));
    #[cfg(feature = "test-support")]
    {
        let observed = Arc::clone(&spawns);
        prepared
            .cleanup
            .set_branch_cleanup_boundary_hook_for_tests(move |phase| {
                if phase == "after-branch-query-before-delete-spawn" {
                    observed.fetch_add(1, Ordering::SeqCst);
                }
            });
    }
    let fresh_target = commit_target_forward(&prepared, "legal-forward");
    let first_old_capability = delete_capability(&prepared.intent).await;
    let second_old_capability = delete_capability(&prepared.intent).await;
    let refresh = match prepared.classify(&first_old_capability).await {
        DeliveryDeletePendingDisposition::RefreshExpectedTarget(refresh) => refresh,
        _ => panic!("expected a legal target-forward refresh proof"),
    };
    assert_eq!(refresh.fresh_target_head(), fresh_target.as_str());
    let refreshed_intent = refresh
        .into_refreshed_intent()
        .expect("the persisted refresh adopts the next runtime generation");

    // Adopting B revokes every capability minted for the older A generation.
    // Even if an external writer later resets the target back to A, a sibling
    // old capability cannot recover deletion authority.
    git_ok(
        &prepared.fixture.repository,
        &["reset", "--hard", "--quiet", &prepared.target_head],
    );

    assert!(matches!(
        prepared.retry(second_old_capability).await,
        DeliveryDeletePendingDisposition::ReconciliationRequired
    ));
    #[cfg(feature = "test-support")]
    assert_eq!(spawns.load(Ordering::SeqCst), 0);
    assert_eq!(prepared.source_oid(), prepared.source_commit);
    assert_eq!(prepared.target_oid(), prepared.target_head);

    git_ok(
        &prepared.fixture.repository,
        &["reset", "--hard", "--quiet", fresh_target.as_str()],
    );

    let refreshed_capability = delete_capability(&refreshed_intent).await;
    assert!(matches!(
        prepared.classify(&refreshed_capability).await,
        DeliveryDeletePendingDisposition::RetryExactDelete
    ));
    assert!(matches!(
        prepared.retry(refreshed_capability).await,
        DeliveryDeletePendingDisposition::Deleted
    ));
    #[cfg(feature = "test-support")]
    assert_eq!(spawns.load(Ordering::SeqCst), 1);
    prepared.assert_source_deleted();
    assert_eq!(prepared.target_oid(), fresh_target);
}

#[tokio::test]
async fn target_common_config_and_attributes_drift_reconcile_before_delete() {
    #[allow(unused_mut)]
    let mut prepared = PreparedBranchCleanup::new(
        "branch-cleanup-target-security-drift",
        "123e4567-e89b-12d3-a456-426614174412",
    )
    .await;
    #[cfg(feature = "test-support")]
    let spawns = Arc::new(AtomicUsize::new(0));
    #[cfg(feature = "test-support")]
    {
        let observed = Arc::clone(&spawns);
        prepared
            .cleanup
            .set_branch_cleanup_boundary_hook_for_tests(move |phase| {
                if phase == "after-branch-query-before-delete-spawn" {
                    observed.fetch_add(1, Ordering::SeqCst);
                }
            });
    }

    let common_git = prepared.fixture.repository.join(".git");
    let config = common_git.join("config");
    let original_config = std::fs::read(&config).unwrap();
    let mut drifted_config = original_config.clone();
    drifted_config.extend_from_slice(b"\n[merge]\n\tverifySignatures = true\n");
    std::fs::write(&config, drifted_config).unwrap();

    assert!(matches!(
        prepared
            .retry(delete_capability(&prepared.intent).await)
            .await,
        DeliveryDeletePendingDisposition::ReconciliationRequired
    ));
    assert_eq!(prepared.source_oid(), prepared.source_commit);
    assert_eq!(prepared.target_oid(), prepared.target_head);
    #[cfg(feature = "test-support")]
    assert_eq!(spawns.load(Ordering::SeqCst), 0);

    std::fs::write(&config, original_config).unwrap();
    let attributes = common_git.join("info").join("attributes");
    let mut drifted_attributes = std::fs::read(&attributes).unwrap_or_default();
    drifted_attributes.extend_from_slice(b"\n* -text\n");
    std::fs::write(&attributes, drifted_attributes).unwrap();

    assert!(matches!(
        prepared
            .retry(delete_capability(&prepared.intent).await)
            .await,
        DeliveryDeletePendingDisposition::ReconciliationRequired
    ));
    assert_eq!(prepared.source_oid(), prepared.source_commit);
    assert_eq!(prepared.target_oid(), prepared.target_head);
    #[cfg(feature = "test-support")]
    assert_eq!(spawns.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn target_that_no_longer_contains_source_is_known_not_applied() {
    let prepared = PreparedBranchCleanup::new(
        "branch-cleanup-not-merged",
        "123e4567-e89b-12d3-a456-426614174405",
    )
    .await;
    git_ok(
        &prepared.fixture.repository,
        &["reset", "--hard", "--quiet", &prepared.base_commit],
    );

    assert!(matches!(
        prepared
            .retry(delete_capability(&prepared.intent).await)
            .await,
        DeliveryDeletePendingDisposition::KnownNotAppliedSourceNotMerged
    ));
    assert_eq!(prepared.source_oid(), prepared.source_commit);
    assert_eq!(prepared.target_oid(), prepared.base_commit);
}

#[tokio::test]
async fn missing_target_with_exact_source_is_known_not_applied_without_a_spawn() {
    #[allow(unused_mut)]
    let mut prepared = PreparedBranchCleanup::new(
        "branch-cleanup-target-missing",
        "123e4567-e89b-12d3-a456-426614174413",
    )
    .await;
    git_ok(
        &prepared.fixture.repository,
        &[
            "update-ref",
            "--no-deref",
            "-d",
            &prepared.target_ref,
            &prepared.target_head,
        ],
    );
    #[cfg(feature = "test-support")]
    let spawns = Arc::new(AtomicUsize::new(0));
    #[cfg(feature = "test-support")]
    {
        let observed = Arc::clone(&spawns);
        prepared
            .cleanup
            .set_branch_cleanup_boundary_hook_for_tests(move |phase| {
                if phase == "after-branch-query-before-delete-spawn" {
                    observed.fetch_add(1, Ordering::SeqCst);
                }
            });
    }

    assert!(matches!(
        prepared
            .retry(delete_capability(&prepared.intent).await)
            .await,
        DeliveryDeletePendingDisposition::KnownNotAppliedSourceNotMerged
    ));
    #[cfg(feature = "test-support")]
    assert_eq!(spawns.load(Ordering::SeqCst), 0);
    assert_eq!(prepared.source_oid(), prepared.source_commit);
    assert_eq!(
        git_line(
            &prepared.fixture.repository,
            &[
                "for-each-ref",
                "--format=%(objectname)",
                &prepared.target_ref,
            ],
        ),
        "",
    );
}

#[tokio::test]
async fn symbolic_source_and_target_refs_reconcile_without_touching_victims() {
    let prepared = PreparedBranchCleanup::new(
        "branch-cleanup-symbolic-refs",
        "123e4567-e89b-12d3-a456-426614174406",
    )
    .await;
    let target_victim = "refs/heads/task17-target-symbolic-victim";
    git_ok(
        &prepared.fixture.repository,
        &["update-ref", target_victim, &prepared.target_head],
    );
    git_ok(
        &prepared.fixture.repository,
        &["symbolic-ref", &prepared.target_ref, target_victim],
    );
    assert!(matches!(
        prepared
            .retry(delete_capability(&prepared.intent).await)
            .await,
        DeliveryDeletePendingDisposition::ReconciliationRequired
    ));
    assert_eq!(
        git_line(&prepared.fixture.repository, &["rev-parse", target_victim]),
        prepared.target_head,
    );
    assert_eq!(prepared.source_oid(), prepared.source_commit);

    write_loose_ref(
        &prepared.fixture.repository,
        &prepared.target_ref,
        &prepared.target_head,
    );
    let source_victim = "refs/heads/task17-source-symbolic-victim";
    git_ok(
        &prepared.fixture.repository,
        &["update-ref", source_victim, &prepared.source_commit],
    );
    git_ok(
        &prepared.fixture.repository,
        &["symbolic-ref", &prepared.source_ref, source_victim],
    );
    assert!(matches!(
        prepared
            .retry(delete_capability(&prepared.intent).await)
            .await,
        DeliveryDeletePendingDisposition::ReconciliationRequired
    ));
    assert_eq!(
        git_line(&prepared.fixture.repository, &["rev-parse", source_victim]),
        prepared.source_commit,
    );
    assert_eq!(prepared.target_oid(), prepared.target_head);
}

#[tokio::test]
async fn raw_source_and_target_tag_objects_reconcile_without_deleting_refs() {
    let prepared = PreparedBranchCleanup::new(
        "branch-cleanup-tag-objects",
        "123e4567-e89b-12d3-a456-426614174407",
    )
    .await;
    let target_tag = annotated_tag_object(
        &prepared.fixture.repository,
        "task17-target-tag-object",
        &prepared.target_head,
    );
    write_loose_ref(
        &prepared.fixture.repository,
        &prepared.target_ref,
        &target_tag,
    );
    assert!(matches!(
        prepared
            .retry(delete_capability(&prepared.intent).await)
            .await,
        DeliveryDeletePendingDisposition::ReconciliationRequired
    ));
    assert_eq!(prepared.target_oid(), target_tag);
    assert_eq!(prepared.source_oid(), prepared.source_commit);

    write_loose_ref(
        &prepared.fixture.repository,
        &prepared.target_ref,
        &prepared.target_head,
    );
    let source_tag = annotated_tag_object(
        &prepared.fixture.repository,
        "task17-source-tag-object",
        &prepared.source_commit,
    );
    write_loose_ref(
        &prepared.fixture.repository,
        &prepared.source_ref,
        &source_tag,
    );
    assert!(matches!(
        prepared
            .retry(delete_capability(&prepared.intent).await)
            .await,
        DeliveryDeletePendingDisposition::ReconciliationRequired
    ));
    assert_eq!(prepared.source_oid(), source_tag);
    assert_eq!(prepared.target_oid(), prepared.target_head);
}

#[tokio::test]
async fn another_worktree_checking_out_source_forces_reconciliation() {
    let prepared = PreparedBranchCleanup::new(
        "branch-cleanup-source-checked-out",
        "123e4567-e89b-12d3-a456-426614174408",
    )
    .await;
    let other_checkout = prepared.fixture.root.join("other-source-checkout");
    let other_checkout_arg = git_path_argument(&other_checkout);
    git_ok(
        &prepared.fixture.repository,
        &[
            "worktree",
            "add",
            "--quiet",
            "--",
            &other_checkout_arg,
            &prepared.source_branch,
        ],
    );

    assert!(matches!(
        prepared
            .retry(delete_capability(&prepared.intent).await)
            .await,
        DeliveryDeletePendingDisposition::ReconciliationRequired
    ));
    assert_eq!(prepared.source_oid(), prepared.source_commit);
    assert_eq!(prepared.target_oid(), prepared.target_head);
    assert!(other_checkout.join(".git").is_file());
}

#[tokio::test]
async fn target_git_operation_started_after_capture_forces_reconciliation() {
    let prepared = PreparedBranchCleanup::new(
        "branch-cleanup-target-operation",
        "123e4567-e89b-12d3-a456-426614174411",
    )
    .await;
    let merge_head = prepared.fixture.repository.join(".git/MERGE_HEAD");
    let sentinel = format!("{}\n", prepared.source_commit);
    std::fs::write(&merge_head, &sentinel).unwrap();

    assert!(matches!(
        prepared
            .retry(delete_capability(&prepared.intent).await)
            .await,
        DeliveryDeletePendingDisposition::ReconciliationRequired
    ));
    assert_eq!(prepared.source_oid(), prepared.source_commit);
    assert_eq!(prepared.target_oid(), prepared.target_head);
    assert_eq!(std::fs::read_to_string(merge_head).unwrap(), sentinel);
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn target_drift_after_final_query_makes_atomic_transaction_delete_nothing() {
    let mut prepared = PreparedBranchCleanup::new(
        "branch-cleanup-target-cas-drift",
        "123e4567-e89b-12d3-a456-426614174409",
    )
    .await;
    let fresh_target = commit_target_forward(&prepared, "cas-target-forward");
    git_ok(
        &prepared.fixture.repository,
        &["reset", "--hard", "--quiet", &prepared.target_head],
    );
    let repository = prepared.fixture.repository.clone();
    let injected_target = fresh_target.clone();
    let spawns = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&spawns);
    prepared
        .cleanup
        .set_branch_cleanup_boundary_hook_for_tests(move |phase| {
            if phase == "after-branch-query-before-delete-spawn"
                && observed.fetch_add(1, Ordering::SeqCst) == 0
            {
                git_ok(
                    &repository,
                    &["reset", "--hard", "--quiet", &injected_target],
                );
            }
        });

    assert!(matches!(
        prepared
            .retry(delete_capability(&prepared.intent).await)
            .await,
        DeliveryDeletePendingDisposition::RefreshExpectedTarget(_)
    ));
    assert_eq!(spawns.load(Ordering::SeqCst), 1);
    assert_eq!(prepared.source_oid(), prepared.source_commit);
    assert_eq!(prepared.target_oid(), fresh_target);
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn source_drift_after_final_query_makes_atomic_transaction_delete_nothing() {
    let mut prepared = PreparedBranchCleanup::new(
        "branch-cleanup-source-cas-drift",
        "123e4567-e89b-12d3-a456-426614174410",
    )
    .await;
    let repository = prepared.fixture.repository.clone();
    let source_ref = prepared.source_ref.clone();
    let expected_source = prepared.source_commit.clone();
    let injected_source = prepared.base_commit.clone();
    let spawns = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&spawns);
    prepared
        .cleanup
        .set_branch_cleanup_boundary_hook_for_tests(move |phase| {
            if phase == "after-branch-query-before-delete-spawn"
                && observed.fetch_add(1, Ordering::SeqCst) == 0
            {
                git_ok(
                    &repository,
                    &[
                        "update-ref",
                        "--no-deref",
                        &source_ref,
                        &injected_source,
                        &expected_source,
                    ],
                );
            }
        });

    assert!(matches!(
        prepared
            .retry(delete_capability(&prepared.intent).await)
            .await,
        DeliveryDeletePendingDisposition::ReconciliationRequired
    ));
    assert_eq!(spawns.load(Ordering::SeqCst), 1);
    assert_eq!(prepared.source_oid(), prepared.base_commit);
    assert_eq!(prepared.target_oid(), prepared.target_head);
}

async fn delete_capability(
    intent: &DeliveryBranchCleanupIntent,
) -> DeliveryDeletePendingCapability {
    authorize_persisted_delivery_branch_delete(
        &PersistedDeleteAuthorization::for_intent(intent),
        intent.clone(),
    )
    .await
    .unwrap()
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

fn commit_target_forward(prepared: &PreparedBranchCleanup, marker: &str) -> String {
    let path = prepared.fixture.repository.join(format!("{marker}.txt"));
    std::fs::write(&path, format!("{marker}\n")).unwrap();
    let file_name = path.file_name().unwrap().to_string_lossy();
    git_ok(
        &prepared.fixture.repository,
        &["add", "--", file_name.as_ref()],
    );
    git_ok(
        &prepared.fixture.repository,
        &["commit", "--quiet", "--no-gpg-sign", "--message", marker],
    );
    git_line(&prepared.fixture.repository, &["rev-parse", "HEAD"])
}

fn annotated_tag_object(repository: &Path, name: &str, target: &str) -> String {
    git_ok(
        repository,
        &[
            "tag",
            "--annotate",
            "--message",
            "branch cleanup raw tag object",
            name,
            target,
        ],
    );
    git_line(repository, &["rev-parse", &format!("refs/tags/{name}")])
}

fn write_loose_ref(repository: &Path, ref_name: &str, object_id: &str) {
    let ref_path = repository.join(".git").join(ref_name);
    std::fs::create_dir_all(ref_path.parent().unwrap()).unwrap();
    std::fs::write(ref_path, format!("{object_id}\n")).unwrap();
}

fn git_path_argument(path: &Path) -> String {
    let native = path.to_string_lossy();
    #[cfg(windows)]
    {
        native
            .strip_prefix(r"\\?\")
            .unwrap_or(native.as_ref())
            .to_owned()
    }
    #[cfg(not(windows))]
    {
        native.into_owned()
    }
}
