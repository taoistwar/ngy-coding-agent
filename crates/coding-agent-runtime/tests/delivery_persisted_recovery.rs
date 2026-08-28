mod delivery_source_support;

use std::sync::Arc;

use coding_agent_core::WorkspaceFingerprint;
use coding_agent_runtime::{
    DeliveryGitObjectFormat, DeliveryMergeInput, DeliveryMergePendingDisposition,
    DeliveryMergeRecoveryBindingOutcome, DeliveryPersistedMergeRecovery,
    DeliveryPersistedSourceRecovery, DeliveryPersistedSourceState, DeliveryPersistedTargetRecovery,
    DeliveryPreflightSource, DeliverySourceCommitInput, DeliverySourcePendingState,
    DeliverySourceRecoveryBindingOutcome, DeliverySourceRecoveryDisposition,
    DeliverySourceRecoveryIntent, DeliveryTargetProvisioner, DeliveryTargetRecoveryBindingOutcome,
    ProcessLimits, WorktreeIdentity, bind_persisted_delivery_merge_recovery,
    build_expected_delivery_merge, classify_persisted_delivery_merge_pending,
    preflight_delivery_merge, project_persisted_delivery_source_applied,
    retry_persisted_delivery_merge_pending,
};
use delivery_source_support::{Fixture, delivery_source_limits};
use tokio_util::sync::CancellationToken;

const TASK_ID: &str = "123e4567-e89b-12d3-a456-426614174921";
const EPOCH_SECONDS: i64 = 1_700_000_921;

#[test]
fn persisted_scalar_views_reject_malformed_or_cross_state_inputs() {
    let identity = WorktreeIdentity::try_new("persisted-view-repository", TASK_ID, 1).unwrap();
    let source_input = DeliverySourceCommitInput::try_new(TASK_ID, 1, EPOCH_SECONDS).unwrap();
    let merge_input = DeliveryMergeInput::try_new(TASK_ID, 1, EPOCH_SECONDS).unwrap();
    let oid = "1".repeat(40);
    let tree = "2".repeat(40);
    let digest = "ab".repeat(32);
    let source_ref = format!("refs/heads/{}", identity.branch_name());

    assert!(
        DeliveryPersistedSourceRecovery::try_new(
            DeliveryGitObjectFormat::Sha1,
            DeliveryPersistedSourceState::ObjectPending,
            identity.clone(),
            &source_ref,
            &oid,
            WorkspaceFingerprint::from_bytes([4; 32]),
            &tree,
            Some(&oid),
            source_input.clone(),
            "directory_identity_v1",
            &digest,
            "directory_identity_v1",
            &digest,
            &digest,
        )
        .is_err(),
        "ObjectPending cannot smuggle an expected source commit"
    );
    assert!(
        DeliveryPersistedSourceRecovery::try_new(
            DeliveryGitObjectFormat::Sha1,
            DeliveryPersistedSourceState::CommitPending,
            identity,
            "refs/heads/not-the-reserved-source",
            &oid,
            WorkspaceFingerprint::from_bytes([4; 32]),
            &tree,
            Some(&oid),
            source_input,
            "directory_identity_v1",
            &digest,
            "directory_identity_v1",
            &digest,
            &digest,
        )
        .is_err(),
        "the source ref must be derived from the persisted identity"
    );
    assert!(
        DeliveryPersistedTargetRecovery::try_new(
            DeliveryGitObjectFormat::Sha1,
            "main",
            &oid,
            "directory_identity_v1",
            &digest,
            &digest,
            &digest,
        )
        .is_err(),
        "target recovery accepts only a fully qualified local branch ref"
    );
    assert!(
        DeliveryPersistedTargetRecovery::try_new(
            DeliveryGitObjectFormat::Sha1,
            "refs/heads/main",
            &oid,
            "directory_identity_v1",
            &digest.to_ascii_uppercase(),
            &digest,
            &digest,
        )
        .is_err(),
        "durable digests must be canonical lowercase hex"
    );
    assert!(
        DeliveryPersistedMergeRecovery::try_new(
            DeliveryGitObjectFormat::Sha1,
            "1".repeat(39),
            &tree,
            &oid,
            merge_input,
        )
        .is_err(),
        "merge recovery rejects an OID from the wrong object format"
    );
}

#[tokio::test]
async fn persisted_source_target_and_merge_bind_only_through_fresh_authority() {
    let fixture = Fixture::new("persisted-recovery-binding").await;
    let source = fixture.reviewed_dirty_source(TASK_ID).await;
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
    let source_object_projection = source_provisioner
        .project_delivery_source_object(
            &opened,
            &candidate,
            &source_commit,
            &source_input,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        source_object_projection.expected_source_commit(),
        source_commit.object_id()
    );
    assert_eq!(source_object_projection.tree(), candidate.object_id());
    assert_eq!(source_object_projection.parent(), opened.base_commit());
    assert_eq!(
        source_object_projection.metadata().author_name(),
        "Coding Agent"
    );
    assert_eq!(
        source_object_projection.metadata().author_email(),
        "coding-agent@localhost"
    );
    assert_eq!(
        source_object_projection.metadata().author_date_bytes(),
        format!("{EPOCH_SECONDS} +0000").as_bytes()
    );
    assert_eq!(
        source_object_projection
            .metadata()
            .message_template_version(),
        1
    );
    assert_eq!(
        source_object_projection.metadata().message_bytes(),
        format!("coding-agent: deliver task {TASK_ID} attempt 1\n").as_bytes()
    );
    let target_provisioner = target_provisioner(&fixture, &source.worktrees);
    let target = target_provisioner
        .observe_registered_delivery_target(CancellationToken::new())
        .await
        .unwrap();
    let persistence = opened
        .persistence_binding_for_target(target.capability())
        .unwrap();
    let wrong_candidate_type = DeliveryPersistedSourceRecovery::try_new(
        persistence.object_format(),
        DeliveryPersistedSourceState::ObjectPending,
        persistence.source_identity().clone(),
        persistence.source_branch(),
        persistence.source_base_commit(),
        persistence.approved_fingerprint(),
        source_commit.object_id(),
        Option::<&str>::None,
        source_input.clone(),
        persistence.common_git_identity_algorithm(),
        persistence.common_git_identity_digest(),
        persistence.worktree_admin_identity_algorithm(),
        persistence.worktree_admin_identity_digest(),
        persistence.source_config_attributes_digest(),
    )
    .unwrap();
    let wrong_source_shape = DeliveryPersistedSourceRecovery::try_new(
        persistence.object_format(),
        DeliveryPersistedSourceState::CommitPending,
        persistence.source_identity().clone(),
        persistence.source_branch(),
        persistence.source_base_commit(),
        persistence.approved_fingerprint(),
        candidate.object_id(),
        Some(opened.base_commit()),
        source_input.clone(),
        persistence.common_git_identity_algorithm(),
        persistence.common_git_identity_digest(),
        persistence.worktree_admin_identity_algorithm(),
        persistence.worktree_admin_identity_digest(),
        persistence.source_config_attributes_digest(),
    )
    .unwrap();
    let before_invalid_source_binds = source.snapshot(&fixture.repository);
    for persisted in [&wrong_candidate_type, &wrong_source_shape] {
        assert!(matches!(
            source_provisioner
                .bind_persisted_delivery_source_recovery(
                    &source.reservation,
                    persisted,
                    CancellationToken::new(),
                )
                .await
                .unwrap(),
            DeliverySourceRecoveryBindingOutcome::ReconciliationRequired
        ));
    }
    assert_eq!(
        source.snapshot(&fixture.repository),
        before_invalid_source_binds,
        "wrong candidate type and source commit shape must not mutate"
    );
    let commit_intent = DeliverySourceRecoveryIntent::from_source(
        DeliverySourcePendingState::CommitPending,
        &opened,
        &candidate,
        Some(&source_commit),
        source_input.clone(),
    )
    .unwrap();
    let commit_recovery = source_provisioner
        .open_delivery_source_for_recovery(
            &source.reservation,
            &commit_intent,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        source_provisioner
            .apply_source_commit(&commit_recovery, CancellationToken::new())
            .await
            .unwrap(),
        DeliverySourceRecoveryDisposition::Applied,
    );

    let preflight = preflight_delivery_merge(
        &source_provisioner,
        &target_provisioner,
        target.capability(),
        DeliveryPreflightSource::committed(&opened, &candidate, &source_commit, &source_input),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(preflight.is_ready());
    let merge_input = DeliveryMergeInput::try_new(TASK_ID, 1, EPOCH_SECONDS).unwrap();
    let expected = build_expected_delivery_merge(
        &source_provisioner,
        &target_provisioner,
        &opened,
        target.capability(),
        &candidate,
        &source_commit,
        &source_input,
        &preflight,
        &merge_input,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let expected_projection = expected.persistence_binding().unwrap();
    assert_eq!(
        expected_projection.expected_merge_commit(),
        expected.object_id()
    );
    assert_eq!(
        expected_projection.tree(),
        preflight.candidate_merge_tree_id()
    );
    assert_eq!(
        expected_projection.source_parent(),
        source_commit.object_id()
    );
    assert_eq!(expected_projection.metadata().message_template_version(), 1);
    assert_eq!(
        expected_projection.metadata().message_bytes(),
        format!("coding-agent: merge task {TASK_ID} attempt 1\n").as_bytes()
    );

    let persisted_source = DeliveryPersistedSourceRecovery::try_new(
        persistence.object_format(),
        DeliveryPersistedSourceState::Committed,
        persistence.source_identity().clone(),
        persistence.source_branch(),
        persistence.source_base_commit(),
        persistence.approved_fingerprint(),
        candidate.object_id(),
        Some(source_commit.object_id()),
        source_input,
        persistence.common_git_identity_algorithm(),
        persistence.common_git_identity_digest(),
        persistence.worktree_admin_identity_algorithm(),
        persistence.worktree_admin_identity_digest(),
        persistence.source_config_attributes_digest(),
    )
    .unwrap();
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
    let wrong_target_config = if persistence.target_config_attributes_digest() == "11".repeat(32) {
        "22".repeat(32)
    } else {
        "11".repeat(32)
    };
    let drifted_target = DeliveryPersistedTargetRecovery::try_new(
        persistence.object_format(),
        persistence.target_branch(),
        persistence.expected_target_head(),
        persistence.common_git_identity_algorithm(),
        persistence.common_git_identity_digest(),
        &wrong_target_config,
        persistence.target_security_digest(),
    )
    .unwrap();
    let wrong_merge_shape = DeliveryPersistedMergeRecovery::try_new(
        persistence.object_format(),
        preflight.merge_base_id(),
        preflight.candidate_merge_tree_id(),
        source_commit.object_id(),
        merge_input.clone(),
    )
    .unwrap();
    let persisted_merge = DeliveryPersistedMergeRecovery::try_new(
        persistence.object_format(),
        preflight.merge_base_id(),
        preflight.candidate_merge_tree_id(),
        expected.object_id(),
        merge_input,
    )
    .unwrap();

    drop(commit_recovery);
    drop(target);
    drop(opened);
    let wrong_merge_source = match source_provisioner
        .bind_persisted_delivery_source_recovery(
            &source.reservation,
            &persisted_source,
            CancellationToken::new(),
        )
        .await
        .unwrap()
    {
        DeliverySourceRecoveryBindingOutcome::Bound(bound) => bound,
        DeliverySourceRecoveryBindingOutcome::ReconciliationRequired => {
            panic!("exact persisted source must bind before merge-shape rejection")
        }
    };
    let wrong_merge_target = match target_provisioner
        .bind_persisted_delivery_target_recovery(&persisted_target, CancellationToken::new())
        .await
        .unwrap()
    {
        DeliveryTargetRecoveryBindingOutcome::Bound(bound) => bound,
        DeliveryTargetRecoveryBindingOutcome::ReconciliationRequired => {
            panic!("exact persisted target must bind before merge-shape rejection")
        }
    };
    let before_wrong_merge_bind = source.snapshot(&fixture.repository);
    assert!(matches!(
        bind_persisted_delivery_merge_recovery(
            &source_provisioner,
            &target_provisioner,
            *wrong_merge_source,
            *wrong_merge_target,
            &wrong_merge_shape,
            CancellationToken::new(),
        )
        .await
        .unwrap(),
        DeliveryMergeRecoveryBindingOutcome::ReconciliationRequired
    ));
    assert_eq!(
        source.snapshot(&fixture.repository),
        before_wrong_merge_bind,
        "a wrong expected merge object shape must not mutate"
    );
    let rebound_source = match source_provisioner
        .bind_persisted_delivery_source_recovery(
            &source.reservation,
            &persisted_source,
            CancellationToken::new(),
        )
        .await
        .unwrap()
    {
        DeliverySourceRecoveryBindingOutcome::Bound(bound) => bound,
        DeliverySourceRecoveryBindingOutcome::ReconciliationRequired => {
            panic!("exact persisted source must bind")
        }
    };
    let source_applied = project_persisted_delivery_source_applied(
        &source_provisioner,
        &rebound_source,
        CancellationToken::new(),
    )
    .await
    .unwrap()
    .expect("exact committed source projects an applied proof");
    assert_eq!(source_applied.source_ref_oid(), source_commit.object_id());
    assert_eq!(source_applied.index_tree(), candidate.object_id());
    let rebound_target = match target_provisioner
        .bind_persisted_delivery_target_recovery(&persisted_target, CancellationToken::new())
        .await
        .unwrap()
    {
        DeliveryTargetRecoveryBindingOutcome::Bound(bound) => bound,
        DeliveryTargetRecoveryBindingOutcome::ReconciliationRequired => {
            panic!("exact persisted target must bind")
        }
    };
    let recovery = match bind_persisted_delivery_merge_recovery(
        &source_provisioner,
        &target_provisioner,
        *rebound_source,
        *rebound_target,
        &persisted_merge,
        CancellationToken::new(),
    )
    .await
    .unwrap()
    {
        DeliveryMergeRecoveryBindingOutcome::Bound(bound) => bound,
        DeliveryMergeRecoveryBindingOutcome::ReconciliationRequired => {
            panic!("exact expected merge must bind")
        }
    };
    assert!(matches!(
        classify_persisted_delivery_merge_pending(
            &source_provisioner,
            &target_provisioner,
            &recovery,
            CancellationToken::new(),
        )
        .await
        .unwrap(),
        DeliveryMergePendingDisposition::RetryExactMerge
    ));

    // The target binder must preserve the persisted config digest as the old
    // baseline. It may bind directory/security authority, but the first phase
    // classifier must observe the mismatch and refuse a merge child.
    let drifted_source = match source_provisioner
        .bind_persisted_delivery_source_recovery(
            &source.reservation,
            &persisted_source,
            CancellationToken::new(),
        )
        .await
        .unwrap()
    {
        DeliverySourceRecoveryBindingOutcome::Bound(bound) => bound,
        DeliverySourceRecoveryBindingOutcome::ReconciliationRequired => {
            panic!("the duplicate exact source binding must remain valid")
        }
    };
    let drifted_target = match target_provisioner
        .bind_persisted_delivery_target_recovery(&drifted_target, CancellationToken::new())
        .await
        .unwrap()
    {
        DeliveryTargetRecoveryBindingOutcome::Bound(bound) => bound,
        DeliveryTargetRecoveryBindingOutcome::ReconciliationRequired => {
            panic!("config baseline drift is classified after authority binding")
        }
    };
    let drifted_recovery = match bind_persisted_delivery_merge_recovery(
        &source_provisioner,
        &target_provisioner,
        *drifted_source,
        *drifted_target,
        &persisted_merge,
        CancellationToken::new(),
    )
    .await
    .unwrap()
    {
        DeliveryMergeRecoveryBindingOutcome::Bound(bound) => bound,
        DeliveryMergeRecoveryBindingOutcome::ReconciliationRequired => {
            panic!("expected object shape is independent from config-baseline drift")
        }
    };
    assert!(matches!(
        classify_persisted_delivery_merge_pending(
            &source_provisioner,
            &target_provisioner,
            &drifted_recovery,
            CancellationToken::new(),
        )
        .await
        .unwrap(),
        DeliveryMergePendingDisposition::ReconciliationRequired
    ));
    assert!(matches!(
        retry_persisted_delivery_merge_pending(
            &source_provisioner,
            &target_provisioner,
            &recovery,
            CancellationToken::new(),
        )
        .await
        .unwrap(),
        coding_agent_runtime::DeliveryMergeOutcome::Applied
    ));
    let applied = classify_persisted_delivery_merge_pending(
        &source_provisioner,
        &target_provisioner,
        &recovery,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let DeliveryMergePendingDisposition::MergeApplied(applied) = applied else {
        panic!("exact expected target commit must project a merge-applied proof");
    };
    let applied = applied.persistence_binding();
    assert_eq!(applied.target_head(), expected.object_id());
    assert_eq!(applied.source_oid(), source_commit.object_id());
    assert_eq!(applied.index_tree(), preflight.candidate_merge_tree_id());
    assert!(applied.merge_head_is_absent());
    assert!(applied.merge_autostash_is_absent());
    assert!(applied.other_git_operation_is_clear());
}

#[tokio::test]
async fn persisted_digest_drift_is_typed_reconciliation_without_mutation() {
    let fixture = Fixture::new("persisted-recovery-digest-drift").await;
    let source = fixture.reviewed_dirty_source(TASK_ID).await;
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
    let input = DeliverySourceCommitInput::try_new(TASK_ID, 1, EPOCH_SECONDS).unwrap();
    let target_provisioner = target_provisioner(&fixture, &source.worktrees);
    let target = target_provisioner
        .observe_registered_delivery_target(CancellationToken::new())
        .await
        .unwrap();
    let binding = opened
        .persistence_binding_for_target(target.capability())
        .unwrap();
    let wrong_common_digest = if binding.common_git_identity_digest() == "11".repeat(32) {
        "22".repeat(32)
    } else {
        "11".repeat(32)
    };
    let wrong_source_config = if binding.source_config_attributes_digest() == "33".repeat(32) {
        "44".repeat(32)
    } else {
        "33".repeat(32)
    };
    let wrong_target_security = if binding.target_security_digest() == "55".repeat(32) {
        "66".repeat(32)
    } else {
        "55".repeat(32)
    };
    let wrong_common_source = DeliveryPersistedSourceRecovery::try_new(
        binding.object_format(),
        DeliveryPersistedSourceState::ObjectPending,
        binding.source_identity().clone(),
        binding.source_branch(),
        binding.source_base_commit(),
        binding.approved_fingerprint(),
        candidate.object_id(),
        Option::<&str>::None,
        input,
        binding.common_git_identity_algorithm(),
        &wrong_common_digest,
        binding.worktree_admin_identity_algorithm(),
        binding.worktree_admin_identity_digest(),
        binding.source_config_attributes_digest(),
    )
    .unwrap();
    let wrong_config_source = DeliveryPersistedSourceRecovery::try_new(
        binding.object_format(),
        DeliveryPersistedSourceState::ObjectPending,
        binding.source_identity().clone(),
        binding.source_branch(),
        binding.source_base_commit(),
        binding.approved_fingerprint(),
        candidate.object_id(),
        Option::<&str>::None,
        DeliverySourceCommitInput::try_new(TASK_ID, 1, EPOCH_SECONDS).unwrap(),
        binding.common_git_identity_algorithm(),
        binding.common_git_identity_digest(),
        binding.worktree_admin_identity_algorithm(),
        binding.worktree_admin_identity_digest(),
        &wrong_source_config,
    )
    .unwrap();
    let persisted_target = DeliveryPersistedTargetRecovery::try_new(
        binding.object_format(),
        binding.target_branch(),
        binding.expected_target_head(),
        binding.common_git_identity_algorithm(),
        binding.common_git_identity_digest(),
        binding.target_config_attributes_digest(),
        &wrong_target_security,
    )
    .unwrap();
    let before = source.snapshot(&fixture.repository);

    assert!(matches!(
        source_provisioner
            .bind_persisted_delivery_source_recovery(
                &source.reservation,
                &wrong_common_source,
                CancellationToken::new(),
            )
            .await
            .unwrap(),
        DeliverySourceRecoveryBindingOutcome::ReconciliationRequired
    ));
    assert!(matches!(
        source_provisioner
            .bind_persisted_delivery_source_recovery(
                &source.reservation,
                &wrong_config_source,
                CancellationToken::new(),
            )
            .await
            .unwrap(),
        DeliverySourceRecoveryBindingOutcome::ReconciliationRequired
    ));
    assert!(matches!(
        target_provisioner
            .bind_persisted_delivery_target_recovery(&persisted_target, CancellationToken::new(),)
            .await
            .unwrap(),
        DeliveryTargetRecoveryBindingOutcome::ReconciliationRequired
    ));
    assert_eq!(source.snapshot(&fixture.repository), before);
}

fn target_provisioner(
    fixture: &Fixture,
    worktrees: &coding_agent_runtime::WorktreeProvisioner,
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

fn process_limits() -> ProcessLimits {
    ProcessLimits::try_new(
        512 * 1024,
        512 * 1024,
        std::time::Duration::from_secs(30),
        std::time::Duration::from_secs(5),
    )
    .unwrap()
}
