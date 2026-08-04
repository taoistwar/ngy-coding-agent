use std::str::FromStr;

use coding_agent_domain::{EventId, RepositoryId, TaskId};
use coding_agent_store::{
    BranchDisposition, CleanupKind, CleanupOperationState, CleanupState, CleanupTransition,
    DeliveryError, DeliveryIdentity, DeliveryOperationId, DeliverySourceState, DeliveryTimestamp,
    DeliveryVersion, DirectoryIdentity, EvidenceIdentityV1, FailureCode, GitBranchRef,
    GitCommitOid, GitObjectAlgorithm, GitOid, GitTreeOid, MergeOperationState, Sha256Digest,
    StateTransition, WorktreeDisposition, validate_cleanup_state, validate_cleanup_transition,
    validate_merge_source_state,
};

const SHA1: &str = "0123456789abcdef0123456789abcdef01234567";
const SHA256_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SHA256_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const SHA256_C: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

#[test]
fn delivery_states_use_exact_wire_values() {
    assert_exact_wire_values(&[
        (DeliverySourceState::ObjectPending, "object_pending"),
        (DeliverySourceState::CommitPending, "commit_pending"),
        (DeliverySourceState::Committed, "committed"),
        (
            DeliverySourceState::ReconciliationRequired,
            "reconciliation_required",
        ),
    ]);
    assert_exact_wire_values(&[
        (MergeOperationState::PreflightPending, "preflight_pending"),
        (MergeOperationState::PreflightReady, "preflight_ready"),
        (MergeOperationState::Accepted, "accepted"),
        (MergeOperationState::MergePending, "merge_pending"),
        (MergeOperationState::Merged, "merged"),
        (MergeOperationState::AbortPending, "abort_pending"),
        (MergeOperationState::Conflict, "conflict"),
        (MergeOperationState::Rejected, "rejected"),
        (MergeOperationState::Stale, "stale"),
        (MergeOperationState::Superseded, "superseded"),
        (MergeOperationState::Failed, "failed"),
        (
            MergeOperationState::ReconciliationRequired,
            "reconciliation_required",
        ),
    ]);
    assert_exact_wire_values(&[
        (WorktreeDisposition::RetainedLocked, "retained_locked"),
        (WorktreeDisposition::RetainedUnlocked, "retained_unlocked"),
        (WorktreeDisposition::Removed, "removed"),
        (
            WorktreeDisposition::ReconciliationRequired,
            "reconciliation_required",
        ),
    ]);
    assert_exact_wire_values(&[
        (BranchDisposition::Retained, "retained"),
        (BranchDisposition::Deleted, "deleted"),
        (
            BranchDisposition::ReconciliationRequired,
            "reconciliation_required",
        ),
    ]);
    assert_exact_wire_values(&[
        (CleanupKind::RemoveWorktree, "remove_worktree"),
        (CleanupKind::DeleteBranch, "delete_branch"),
    ]);
    assert_exact_wire_values(&[
        (CleanupOperationState::UnlockPending, "unlock_pending"),
        (
            CleanupOperationState::UnlockedPendingRemove,
            "unlocked_pending_remove",
        ),
        (CleanupOperationState::RemovePending, "remove_pending"),
        (CleanupOperationState::DeletePending, "delete_pending"),
        (CleanupOperationState::Completed, "completed"),
        (CleanupOperationState::Failed, "failed"),
        (
            CleanupOperationState::ReconciliationRequired,
            "reconciliation_required",
        ),
    ]);

    assert!(DeliverySourceState::from_str("ObjectPending").is_err());
    assert!(MergeOperationState::from_str("MERGED").is_err());
    assert!(WorktreeDisposition::from_str("remove_pending").is_err());
    assert!(CleanupOperationState::from_str("retained_unlocked").is_err());
}

#[test]
fn source_transition_matrix_is_closed_and_one_way() {
    use DeliverySourceState::*;

    let states = [
        ObjectPending,
        CommitPending,
        Committed,
        ReconciliationRequired,
    ];
    let legal = [
        (ObjectPending, CommitPending),
        (ObjectPending, ReconciliationRequired),
        (CommitPending, Committed),
        (CommitPending, ReconciliationRequired),
        (Committed, ReconciliationRequired),
    ];

    assert_closed_transition_matrix(&states, &legal, |from, to| from.can_transition_to(to));
    assert!(ObjectPending.is_side_effect_active());
    assert!(CommitPending.is_side_effect_active());
    assert!(Committed.is_terminal());
    assert!(ReconciliationRequired.is_reconciliation());
    assert!(!ReconciliationRequired.is_terminal());
}

#[test]
fn merge_transition_matrix_and_classifications_are_closed() {
    use MergeOperationState::*;

    let states = [
        PreflightPending,
        PreflightReady,
        Accepted,
        MergePending,
        Merged,
        AbortPending,
        Conflict,
        Rejected,
        Stale,
        Superseded,
        Failed,
        ReconciliationRequired,
    ];
    let legal = [
        (PreflightPending, PreflightReady),
        (PreflightPending, Conflict),
        (PreflightPending, Rejected),
        (PreflightPending, Stale),
        (PreflightPending, ReconciliationRequired),
        (PreflightReady, Accepted),
        (PreflightReady, Stale),
        (PreflightReady, Superseded),
        (PreflightReady, ReconciliationRequired),
        (Accepted, MergePending),
        (Accepted, Failed),
        (Accepted, ReconciliationRequired),
        (MergePending, Merged),
        (MergePending, AbortPending),
        (MergePending, Failed),
        (MergePending, ReconciliationRequired),
        (AbortPending, Conflict),
        (AbortPending, ReconciliationRequired),
    ];

    assert_closed_transition_matrix(&states, &legal, |from, to| from.can_transition_to(to));
    for state in states {
        assert_eq!(
            state.is_open(),
            matches!(state, PreflightPending | PreflightReady)
        );
        assert_eq!(
            state.is_side_effect_active(),
            matches!(state, Accepted | MergePending | AbortPending)
        );
        assert_eq!(
            state.is_terminal(),
            matches!(
                state,
                Merged | Conflict | Rejected | Stale | Superseded | Failed
            )
        );
        assert_eq!(state.is_reconciliation(), state == ReconciliationRequired);
    }
}

#[test]
fn disposition_and_cleanup_transition_matrices_are_closed() {
    use BranchDisposition::{Deleted, ReconciliationRequired as BranchReconciliation, Retained};
    use CleanupOperationState::{
        Completed, DeletePending, Failed, ReconciliationRequired as CleanupReconciliation,
        RemovePending, UnlockPending, UnlockedPendingRemove,
    };
    use WorktreeDisposition::{
        ReconciliationRequired as WorktreeReconciliation, Removed, RetainedLocked, RetainedUnlocked,
    };

    assert_closed_transition_matrix(
        &[
            RetainedLocked,
            RetainedUnlocked,
            Removed,
            WorktreeReconciliation,
        ],
        &[
            (RetainedLocked, RetainedUnlocked),
            (RetainedLocked, WorktreeReconciliation),
            (RetainedUnlocked, Removed),
            (RetainedUnlocked, WorktreeReconciliation),
            (Removed, WorktreeReconciliation),
        ],
        |from, to| from.can_transition_to(to),
    );
    assert_closed_transition_matrix(
        &[Retained, Deleted, BranchReconciliation],
        &[
            (Retained, Deleted),
            (Retained, BranchReconciliation),
            (Deleted, BranchReconciliation),
        ],
        |from, to| from.can_transition_to(to),
    );

    let cleanup_states = [
        UnlockPending,
        UnlockedPendingRemove,
        RemovePending,
        DeletePending,
        Completed,
        Failed,
        CleanupReconciliation,
    ];
    let cleanup_legal = [
        (UnlockPending, UnlockedPendingRemove),
        (UnlockPending, Failed),
        (UnlockPending, CleanupReconciliation),
        (UnlockedPendingRemove, RemovePending),
        (UnlockedPendingRemove, CleanupReconciliation),
        (RemovePending, Completed),
        (RemovePending, Failed),
        (RemovePending, CleanupReconciliation),
        (DeletePending, Completed),
        (DeletePending, Failed),
        (DeletePending, CleanupReconciliation),
    ];
    assert_closed_transition_matrix(&cleanup_states, &cleanup_legal, |from, to| {
        from.can_transition_to(to)
    });
    for state in cleanup_states {
        assert_eq!(
            state.is_side_effect_active(),
            matches!(
                state,
                UnlockPending | UnlockedPendingRemove | RemovePending | DeletePending
            )
        );
        assert_eq!(state.is_terminal(), matches!(state, Completed | Failed));
        assert_eq!(state.is_reconciliation(), state == CleanupReconciliation);
    }
}

#[test]
fn canonical_identifiers_and_delivery_identity_reject_ambiguous_input() {
    let canonical = "d9428888-122b-4c6f-bb9c-2c3b11e0a73d";
    let operation_id = DeliveryOperationId::from_str(canonical).unwrap();
    assert_eq!(operation_id.to_string(), canonical);
    assert_eq!(
        serde_json::to_string(&operation_id).unwrap(),
        format!("\"{canonical}\"")
    );
    assert_eq!(
        serde_json::from_str::<DeliveryOperationId>(&format!("\"{canonical}\"")).unwrap(),
        operation_id
    );
    for invalid in [
        "D9428888-122B-4C6F-BB9C-2C3B11E0A73D",
        "d9428888122b4c6fbb9c2c3b11e0a73d",
        "{d9428888-122b-4c6f-bb9c-2c3b11e0a73d}",
        "00000000-0000-0000-0000-000000000000",
    ] {
        assert_eq!(
            DeliveryOperationId::from_str(invalid),
            Err(DeliveryError::InvalidUuid)
        );
    }

    let task_id = TaskId::from_str("4f8cc1c4-c7fc-4e32-9338-d567f8da4f2d").unwrap();
    let repository_id = RepositoryId::from_str("51ae0bb2-9c50-4c76-980f-fd80723f083f").unwrap();
    assert_eq!(
        DeliveryIdentity::try_new(task_id, repository_id, 0),
        Err(DeliveryError::InvalidIdentity)
    );
    let identity = DeliveryIdentity::try_new(task_id, repository_id, 3).unwrap();
    assert_eq!(identity.task_id(), task_id);
    assert_eq!(identity.repository_id(), repository_id);
    assert_eq!(identity.attempt(), 3);
}

#[test]
fn object_ids_and_sha256_digests_are_exact_lowercase_hex() {
    let sha1 = GitOid::from_str(SHA1).unwrap();
    assert_eq!(sha1.algorithm(), GitObjectAlgorithm::Sha1);
    assert_eq!(sha1.as_str(), SHA1);
    let sha256 = GitOid::from_str(SHA256_A).unwrap();
    assert_eq!(sha256.algorithm(), GitObjectAlgorithm::Sha256);
    assert_eq!(sha256.as_str(), SHA256_A);

    for invalid in [
        "0123456789ABCDEF0123456789abcdef01234567",
        "0123456789abcdef0123456789abcdef0123456",
        "g123456789abcdef0123456789abcdef01234567",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    ] {
        assert_eq!(GitOid::from_str(invalid), Err(DeliveryError::InvalidGitOid));
    }
    for null_oid in ["0".repeat(40), "0".repeat(64)] {
        assert_eq!(
            GitOid::from_str(&null_oid),
            Err(DeliveryError::InvalidGitOid)
        );
        assert_eq!(
            GitCommitOid::from_str(&null_oid),
            Err(DeliveryError::InvalidGitOid)
        );
        assert_eq!(
            GitTreeOid::from_str(&null_oid),
            Err(DeliveryError::InvalidGitOid)
        );
    }

    let tree = GitTreeOid::from_str(SHA1).unwrap();
    let parent = GitCommitOid::from_str(SHA1).unwrap();
    assert_eq!(tree.as_str(), SHA1);
    assert_eq!(parent.as_str(), SHA1);
    assert_eq!(tree.algorithm(), parent.algorithm());

    let digest = Sha256Digest::from_str(SHA256_B).unwrap();
    assert_eq!(digest.as_str(), SHA256_B);
    assert_eq!(
        Sha256Digest::from_str(SHA1),
        Err(DeliveryError::InvalidSha256Digest)
    );
}

#[test]
fn branch_timestamp_version_and_failure_code_are_canonical_and_bounded() {
    let branch = GitBranchRef::from_str("refs/heads/功能/交付").unwrap();
    assert_eq!(branch.as_str(), "refs/heads/功能/交付");
    for invalid in [
        "main",
        "refs/tags/v1",
        "refs/heads/",
        "refs/heads/.hidden",
        "refs/heads/main.lock",
        "refs/heads/a..b",
        "refs/heads/a@{b",
        "refs/heads/a b",
        "refs/heads/a\\b",
        "refs/heads/-unsafe",
    ] {
        assert_eq!(
            GitBranchRef::from_str(invalid),
            Err(DeliveryError::InvalidGitBranchRef),
            "{invalid} must be rejected"
        );
    }

    let timestamp = DeliveryTimestamp::from_str("2026-08-04T01:02:03.000000000Z").unwrap();
    assert_eq!(timestamp.to_string(), "2026-08-04T01:02:03.000000000Z");
    assert_eq!(
        DeliveryTimestamp::from_str("2026-08-04T01:02:03Z"),
        Err(DeliveryError::InvalidTimestamp)
    );
    assert_eq!(
        DeliveryTimestamp::from_str("2026-08-04T09:02:03.000000000+08:00"),
        Err(DeliveryError::InvalidTimestamp)
    );

    assert_eq!(DeliveryVersion::initial().get(), 1);
    assert_eq!(DeliveryVersion::initial().next().unwrap().get(), 2);
    assert_eq!(
        DeliveryVersion::try_new(0),
        Err(DeliveryError::InvalidVersion)
    );
    assert_eq!(DeliveryVersion::MAX, 9_007_199_254_740_991);
    assert_eq!(
        DeliveryVersion::try_new(DeliveryVersion::MAX + 1),
        Err(DeliveryError::InvalidVersion)
    );
    assert_eq!(
        DeliveryVersion::try_new(DeliveryVersion::MAX)
            .unwrap()
            .next(),
        Err(DeliveryError::InvalidVersion)
    );

    let code = FailureCode::from_str("TARGET_HEAD_CHANGED").unwrap();
    assert_eq!(code.as_str(), "TARGET_HEAD_CHANGED");
    for invalid in ["", "lowercase", "HAS-HYPHEN", "_LEADING", "TRAILING_"] {
        assert_eq!(
            FailureCode::from_str(invalid),
            Err(DeliveryError::InvalidFailureCode)
        );
    }
    assert_eq!(
        FailureCode::from_str(&"A".repeat(FailureCode::MAX_BYTES + 1)),
        Err(DeliveryError::InvalidFailureCode)
    );
}

#[test]
fn evidence_and_directory_identities_have_exact_algorithms_and_fields() {
    let identity = DeliveryIdentity::try_new(
        TaskId::from_str("4f8cc1c4-c7fc-4e32-9338-d567f8da4f2d").unwrap(),
        RepositoryId::from_str("51ae0bb2-9c50-4c76-980f-fd80723f083f").unwrap(),
        2,
    )
    .unwrap();
    let evidence = EvidenceIdentityV1::try_new(
        identity,
        3,
        EventId::new(41).unwrap(),
        7,
        Sha256Digest::from_str(SHA256_A).unwrap(),
        Sha256Digest::from_str(SHA256_B).unwrap(),
        Sha256Digest::from_str(SHA256_C).unwrap(),
    )
    .unwrap();
    assert_eq!(evidence.algorithm(), "evidence_identity_v1");
    assert_eq!(evidence.identity(), identity);
    assert_eq!(evidence.final_review_round(), 3);
    assert_eq!(evidence.final_review_event_id().get(), 41);
    assert_eq!(evidence.workspace_generation(), 7);
    assert_eq!(evidence.workspace_fingerprint().as_str(), SHA256_A);
    assert_eq!(evidence.checks_digest().as_str(), SHA256_B);
    assert_eq!(evidence.coverage_digest().as_str(), SHA256_C);

    let encoded = serde_json::to_value(&evidence).unwrap();
    assert_eq!(encoded["algorithm"], "evidence_identity_v1");
    assert_eq!(encoded["attempt"], 2);
    assert_eq!(encoded["final_review_round"], 3);
    assert_eq!(encoded["final_review_event_id"], 41);
    assert_eq!(encoded["workspace_generation"], 7);
    assert_eq!(encoded["workspace_fingerprint"], SHA256_A);
    assert_eq!(encoded["checks_digest"], SHA256_B);
    assert_eq!(encoded["coverage_digest"], SHA256_C);
    assert_eq!(
        serde_json::from_value::<EvidenceIdentityV1>(encoded).unwrap(),
        evidence
    );

    assert_eq!(
        EvidenceIdentityV1::try_new(
            identity,
            0,
            EventId::new(41).unwrap(),
            7,
            Sha256Digest::from_str(SHA256_A).unwrap(),
            Sha256Digest::from_str(SHA256_B).unwrap(),
            Sha256Digest::from_str(SHA256_C).unwrap(),
        ),
        Err(DeliveryError::InvalidEvidenceIdentity)
    );

    let directory = DirectoryIdentity::try_new("directory_identity_v1", SHA256_A).unwrap();
    assert_eq!(directory.algorithm(), "directory_identity_v1");
    let debug = format!("{directory:?}");
    assert!(debug.contains("directory_identity_v1"));
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains(SHA256_A));
    assert_eq!(
        DirectoryIdentity::try_new("directory_identity_v2", SHA256_A),
        Err(DeliveryError::InvalidDirectoryIdentity)
    );
    assert_eq!(
        DirectoryIdentity::try_new("directory_identity_v1", SHA1),
        Err(DeliveryError::InvalidSha256Digest)
    );
}

#[test]
fn directory_identity_has_no_general_purpose_serde_surface() {
    trait AmbiguousIfSerialize<Marker> {
        fn marker() {}
    }
    impl<T: ?Sized> AmbiguousIfSerialize<()> for T {}
    impl<T: ?Sized + serde::Serialize> AmbiguousIfSerialize<u8> for T {}

    trait AmbiguousIfDeserialize<Marker> {
        fn marker() {}
    }
    impl<T: ?Sized> AmbiguousIfDeserialize<()> for T {}
    impl<T> AmbiguousIfDeserialize<u8> for T where T: for<'de> serde::Deserialize<'de> {}

    let _ = <DirectoryIdentity as AmbiguousIfSerialize<_>>::marker;
    let _ = <DirectoryIdentity as AmbiguousIfDeserialize<_>>::marker;
}

#[test]
fn transition_journal_versions_start_at_one_and_advance_exactly_once() {
    let initial = StateTransition::try_initial_source(
        DeliverySourceState::ObjectPending,
        DeliveryVersion::initial(),
    )
    .unwrap();
    assert_eq!(initial.from(), None);
    assert_eq!(initial.from_storage_value(), "absent");
    assert_eq!(initial.to(), DeliverySourceState::ObjectPending);
    assert_eq!(initial.version().get(), 1);

    assert_eq!(
        StateTransition::try_initial_source(
            DeliverySourceState::CommitPending,
            DeliveryVersion::initial(),
        ),
        Err(DeliveryError::IllegalTransition)
    );
    assert_eq!(
        StateTransition::try_initial_source(
            DeliverySourceState::ObjectPending,
            DeliveryVersion::try_new(2).unwrap(),
        ),
        Err(DeliveryError::InvalidVersion)
    );

    let remove_retry = StateTransition::try_initial_cleanup(
        CleanupKind::RemoveWorktree,
        CleanupOperationState::RemovePending,
        WorktreeDisposition::RetainedUnlocked,
        BranchDisposition::Retained,
        DeliveryVersion::initial(),
    )
    .unwrap();
    assert_eq!(remove_retry.from_storage_value(), "absent");
    assert_eq!(remove_retry.to(), CleanupOperationState::RemovePending);
    assert!(
        validate_cleanup_state(
            CleanupKind::RemoveWorktree,
            remove_retry.to(),
            WorktreeDisposition::RetainedUnlocked,
            BranchDisposition::Retained,
        )
        .is_ok()
    );
    assert_eq!(
        StateTransition::try_initial_cleanup(
            CleanupKind::RemoveWorktree,
            CleanupOperationState::UnlockedPendingRemove,
            WorktreeDisposition::RetainedUnlocked,
            BranchDisposition::Retained,
            DeliveryVersion::initial(),
        ),
        Err(DeliveryError::IllegalTransition)
    );
    assert_eq!(
        StateTransition::try_initial_cleanup(
            CleanupKind::RemoveWorktree,
            CleanupOperationState::RemovePending,
            WorktreeDisposition::RetainedLocked,
            BranchDisposition::Retained,
            DeliveryVersion::initial(),
        ),
        Err(DeliveryError::InvalidStateCombination)
    );
    assert_eq!(
        StateTransition::try_initial_cleanup(
            CleanupKind::DeleteBranch,
            CleanupOperationState::DeletePending,
            WorktreeDisposition::Removed,
            BranchDisposition::Deleted,
            DeliveryVersion::initial(),
        ),
        Err(DeliveryError::InvalidStateCombination)
    );

    let next = StateTransition::try_advance(
        DeliverySourceState::ObjectPending,
        DeliverySourceState::CommitPending,
        DeliveryVersion::initial(),
        DeliveryVersion::try_new(2).unwrap(),
    )
    .unwrap();
    assert_eq!(next.from(), Some(DeliverySourceState::ObjectPending));
    assert_eq!(next.to(), DeliverySourceState::CommitPending);
    assert_eq!(next.version().get(), 2);
    assert_eq!(
        StateTransition::try_advance(
            DeliverySourceState::ObjectPending,
            DeliverySourceState::CommitPending,
            DeliveryVersion::initial(),
            DeliveryVersion::try_new(3).unwrap(),
        ),
        Err(DeliveryError::InvalidVersion)
    );
    assert_eq!(
        StateTransition::try_advance(
            DeliverySourceState::CommitPending,
            DeliverySourceState::ObjectPending,
            DeliveryVersion::try_new(2).unwrap(),
            DeliveryVersion::try_new(3).unwrap(),
        ),
        Err(DeliveryError::IllegalTransition)
    );

    let observation = StateTransition::try_observation(
        DeliverySourceState::CommitPending,
        DeliveryVersion::try_new(2).unwrap(),
        DeliveryVersion::try_new(3).unwrap(),
    )
    .unwrap();
    assert_eq!(observation.from(), Some(DeliverySourceState::CommitPending));
    assert_eq!(observation.to(), DeliverySourceState::CommitPending);
    assert_eq!(observation.version().get(), 3);
    assert_eq!(
        StateTransition::try_observation(
            DeliverySourceState::Committed,
            DeliveryVersion::try_new(3).unwrap(),
            DeliveryVersion::try_new(4).unwrap(),
        ),
        Err(DeliveryError::IllegalTransition)
    );
    assert!(
        StateTransition::try_observation(
            CleanupOperationState::DeletePending,
            DeliveryVersion::initial(),
            DeliveryVersion::try_new(2).unwrap(),
        )
        .is_ok()
    );
    assert_eq!(
        StateTransition::try_observation(
            CleanupOperationState::Failed,
            DeliveryVersion::initial(),
            DeliveryVersion::try_new(2).unwrap(),
        ),
        Err(DeliveryError::IllegalTransition)
    );
}

#[test]
fn cross_entity_state_combinations_fail_closed() {
    use BranchDisposition::{Deleted, Retained};
    use CleanupKind::{DeleteBranch, RemoveWorktree};
    use CleanupOperationState::{
        Completed, DeletePending, Failed, ReconciliationRequired, RemovePending, UnlockPending,
        UnlockedPendingRemove,
    };
    use DeliverySourceState::{
        CommitPending, Committed, ObjectPending, ReconciliationRequired as SourceReconciliation,
    };
    use MergeOperationState::{
        Accepted, Failed as MergeFailed, MergePending,
        ReconciliationRequired as MergeReconciliation,
    };
    use WorktreeDisposition::{
        ReconciliationRequired as WorktreeReconciliation, Removed, RetainedLocked, RetainedUnlocked,
    };

    assert_eq!(
        validate_merge_source_state(MergeFailed, Some(ObjectPending)),
        Err(DeliveryError::InvalidStateCombination)
    );
    assert_eq!(
        validate_merge_source_state(MergeFailed, Some(CommitPending)),
        Err(DeliveryError::InvalidStateCombination)
    );
    assert_eq!(
        validate_merge_source_state(MergeFailed, None),
        Err(DeliveryError::InvalidStateCombination)
    );
    assert!(validate_merge_source_state(MergeFailed, Some(Committed)).is_ok());
    assert!(validate_merge_source_state(Accepted, Some(ObjectPending)).is_ok());
    assert_eq!(
        validate_merge_source_state(MergePending, Some(CommitPending)),
        Err(DeliveryError::InvalidStateCombination)
    );
    assert_eq!(
        validate_merge_source_state(MergePending, None),
        Err(DeliveryError::InvalidStateCombination)
    );
    for pending in [ObjectPending, CommitPending] {
        assert_eq!(
            validate_merge_source_state(MergeReconciliation, Some(pending)),
            Err(DeliveryError::InvalidStateCombination)
        );
    }
    assert!(validate_merge_source_state(MergeReconciliation, None).is_ok());
    assert!(validate_merge_source_state(MergeReconciliation, Some(Committed)).is_ok());
    assert!(validate_merge_source_state(MergeReconciliation, Some(SourceReconciliation)).is_ok());

    for (state, disposition) in [
        (UnlockPending, RetainedLocked),
        (UnlockedPendingRemove, RetainedUnlocked),
        (RemovePending, RetainedUnlocked),
        (Completed, Removed),
        (Failed, RetainedLocked),
        (Failed, RetainedUnlocked),
        (ReconciliationRequired, WorktreeReconciliation),
    ] {
        assert!(validate_cleanup_state(RemoveWorktree, state, disposition, Retained).is_ok());
    }
    for (state, branch) in [
        (DeletePending, Retained),
        (Completed, Deleted),
        (Failed, Retained),
        (
            ReconciliationRequired,
            BranchDisposition::ReconciliationRequired,
        ),
    ] {
        assert!(validate_cleanup_state(DeleteBranch, state, Removed, branch).is_ok());
    }

    for invalid in [
        validate_cleanup_state(RemoveWorktree, DeletePending, RetainedLocked, Retained),
        validate_cleanup_state(DeleteBranch, UnlockPending, Removed, Retained),
        validate_cleanup_state(RemoveWorktree, RemovePending, RetainedLocked, Retained),
        validate_cleanup_state(DeleteBranch, Completed, Removed, Retained),
        validate_cleanup_state(DeleteBranch, DeletePending, RetainedUnlocked, Retained),
        validate_cleanup_state(RemoveWorktree, UnlockPending, RetainedLocked, Deleted),
        validate_cleanup_state(
            RemoveWorktree,
            RemovePending,
            RetainedUnlocked,
            BranchDisposition::ReconciliationRequired,
        ),
    ] {
        assert_eq!(invalid, Err(DeliveryError::InvalidStateCombination));
    }
}

#[test]
fn cleanup_transitions_advance_operation_and_proven_facts_as_one_closed_matrix() {
    use BranchDisposition::{Deleted, ReconciliationRequired as BranchReconciliation, Retained};
    use CleanupKind::{DeleteBranch, RemoveWorktree};
    use CleanupOperationState::{
        Completed, DeletePending, Failed, ReconciliationRequired as CleanupReconciliation,
        RemovePending, UnlockPending, UnlockedPendingRemove,
    };
    use WorktreeDisposition::{
        ReconciliationRequired as WorktreeReconciliation, Removed, RetainedLocked, RetainedUnlocked,
    };

    let states = [
        cleanup_state(RemoveWorktree, UnlockPending, RetainedLocked, Retained),
        cleanup_state(
            RemoveWorktree,
            UnlockedPendingRemove,
            RetainedUnlocked,
            Retained,
        ),
        cleanup_state(RemoveWorktree, RemovePending, RetainedUnlocked, Retained),
        cleanup_state(RemoveWorktree, Completed, Removed, Retained),
        cleanup_state(RemoveWorktree, Failed, RetainedLocked, Retained),
        cleanup_state(RemoveWorktree, Failed, RetainedUnlocked, Retained),
        cleanup_state(
            RemoveWorktree,
            CleanupReconciliation,
            WorktreeReconciliation,
            Retained,
        ),
        cleanup_state(DeleteBranch, DeletePending, Removed, Retained),
        cleanup_state(DeleteBranch, Completed, Removed, Deleted),
        cleanup_state(DeleteBranch, Failed, Removed, Retained),
        cleanup_state(
            DeleteBranch,
            CleanupReconciliation,
            Removed,
            BranchReconciliation,
        ),
    ];
    let legal = [
        (0, 1),
        (0, 4),
        (0, 6),
        (1, 2),
        (1, 6),
        (2, 3),
        (2, 5),
        (2, 6),
        (7, 7),
        (7, 8),
        (7, 9),
        (7, 10),
    ];

    for (from_index, from) in states.iter().copied().enumerate() {
        for (to_index, to) in states.iter().copied().enumerate() {
            let expected = legal.contains(&(from_index, to_index));
            assert_eq!(
                from.can_transition_to(to),
                expected,
                "unexpected coupled cleanup transition: {from:?} -> {to:?}"
            );
            assert_eq!(
                validate_cleanup_transition(from, to).is_ok(),
                expected,
                "validator disagreed for {from:?} -> {to:?}"
            );
            assert_eq!(
                CleanupTransition::try_advance(
                    from,
                    to,
                    DeliveryVersion::initial(),
                    DeliveryVersion::try_new(2).unwrap(),
                )
                .is_ok(),
                expected,
                "versioned constructor disagreed for {from:?} -> {to:?}"
            );
        }
    }

    // Unlock failure proves that the worktree remains locked; an unlocked failure
    // belongs only to the later RemovePending phase.
    assert_eq!(
        validate_cleanup_transition(states[0], states[5]),
        Err(DeliveryError::InvalidStateCombination)
    );
    assert!(validate_cleanup_transition(states[2], states[5]).is_ok());

    let transition = CleanupTransition::try_advance(
        states[0],
        states[1],
        DeliveryVersion::initial(),
        DeliveryVersion::try_new(2).unwrap(),
    )
    .unwrap();
    assert_eq!(transition.from(), states[0]);
    assert_eq!(transition.to(), states[1]);
    assert_eq!(transition.operation().version().get(), 2);

    let delete_refresh = CleanupTransition::try_advance(
        states[7],
        states[7],
        DeliveryVersion::try_new(2).unwrap(),
        DeliveryVersion::try_new(3).unwrap(),
    )
    .unwrap();
    assert_eq!(delete_refresh.operation().from(), Some(DeletePending));
    assert_eq!(delete_refresh.operation().to(), DeletePending);

    assert_eq!(
        CleanupState::try_new(RemoveWorktree, UnlockPending, RetainedUnlocked, Retained),
        Err(DeliveryError::InvalidStateCombination)
    );
    assert_eq!(
        CleanupState::try_new(RemoveWorktree, UnlockPending, RetainedLocked, Deleted),
        Err(DeliveryError::InvalidStateCombination)
    );
    assert_eq!(
        CleanupState::try_new(DeleteBranch, DeletePending, RetainedUnlocked, Retained),
        Err(DeliveryError::InvalidStateCombination)
    );
}

fn assert_exact_wire_values<T>(cases: &[(T, &str)])
where
    T: Copy
        + std::fmt::Debug
        + PartialEq
        + serde::Serialize
        + serde::de::DeserializeOwned
        + FromStr<Err = DeliveryError>
        + std::fmt::Display,
{
    for (value, wire) in cases {
        assert_eq!(value.to_string(), *wire);
        assert_eq!(serde_json::to_string(value).unwrap(), format!("\"{wire}\""));
        assert_eq!(wire.parse::<T>().unwrap(), *value);
        assert_eq!(
            serde_json::from_str::<T>(&format!("\"{wire}\"")).unwrap(),
            *value
        );
    }
}

fn assert_closed_transition_matrix<T>(
    states: &[T],
    legal: &[(T, T)],
    can_transition: impl Fn(T, T) -> bool,
) where
    T: Copy + std::fmt::Debug + PartialEq,
{
    for &from in states {
        for &to in states {
            assert_eq!(
                can_transition(from, to),
                legal.contains(&(from, to)),
                "unexpected transition classification: {from:?} -> {to:?}"
            );
        }
    }
}

fn cleanup_state(
    kind: CleanupKind,
    state: CleanupOperationState,
    worktree: WorktreeDisposition,
    branch: BranchDisposition,
) -> CleanupState {
    CleanupState::try_new(kind, state, worktree, branch).unwrap()
}
