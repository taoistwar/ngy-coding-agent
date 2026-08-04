use std::str::FromStr;

use coding_agent_domain::{ClientRequestId, Task};
use coding_agent_store::{
    AcceptMergeOutcome, BeginMergeAbortRequest, DeliveryOperationId, DeliveryVersion,
    DirectoryIdentity, GitBranchRef, GitCommitOid, MergeAbortProof, MergeAutostashObservation,
    MergeKnownNotAppliedReason, MergeOperationRecord, MergeOperationState,
    MergeReconciliationReason, MergeTransitionOutcome, OtherGitOperationObservation,
    ReconcileMergeRequest, RecordMergeKnownFailureRequest, Sha256Digest, Store,
};
use uuid::Uuid;

use crate::support::delivery::eligibility::{
    ADMIN_IDENTITY, COMMON_IDENTITY, CONFIG_DIGEST, MERGE_BASE, MERGE_COMMIT, MERGE_TREE,
    SOURCE_COMMIT, TARGET_HEAD,
};

use super::fixtures::{
    TARGET_BRANCH, accept_command, accepted_with_committed_source, merge_pending, pending_preflight,
};

const INDEX_STAGES: &str = "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1";
const WORKTREE: &str = "b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2";

#[derive(Clone, Copy)]
enum KnownFailureOrigin {
    Accepted,
    MergePending,
}

#[tokio::test]
async fn every_known_zero_effect_reason_is_exact_from_both_legal_origins() {
    let cases = [
        (
            MergeKnownNotAppliedReason::TaskNotMergeEligible,
            "TASK_NOT_MERGE_ELIGIBLE",
        ),
        (
            MergeKnownNotAppliedReason::TargetBranchDetached,
            "TARGET_BRANCH_DETACHED",
        ),
        (
            MergeKnownNotAppliedReason::TargetBranchMismatch,
            "TARGET_BRANCH_MISMATCH",
        ),
        (
            MergeKnownNotAppliedReason::TargetWorktreeDirty,
            "TARGET_WORKTREE_DIRTY",
        ),
        (
            MergeKnownNotAppliedReason::TargetIgnoredPathCollision,
            "TARGET_IGNORED_PATH_COLLISION",
        ),
        (
            MergeKnownNotAppliedReason::TargetGitOperationInProgress,
            "TARGET_GIT_OPERATION_IN_PROGRESS",
        ),
        (
            MergeKnownNotAppliedReason::UnsafeGitConfiguration,
            "UNSAFE_GIT_CONFIGURATION",
        ),
        (
            MergeKnownNotAppliedReason::UnsupportedGitAttributes,
            "UNSUPPORTED_GIT_ATTRIBUTES",
        ),
        (
            MergeKnownNotAppliedReason::SourceAlreadyInTarget,
            "SOURCE_ALREADY_IN_TARGET",
        ),
        (
            MergeKnownNotAppliedReason::TargetHeadChanged,
            "TARGET_HEAD_CHANGED",
        ),
        (
            MergeKnownNotAppliedReason::CommandTimedOut,
            "COMMAND_TIMED_OUT",
        ),
    ];

    for (reason, expected_code) in cases {
        assert_known_failure(reason, expected_code, KnownFailureOrigin::Accepted).await;
        assert_known_failure(reason, expected_code, KnownFailureOrigin::MergePending).await;
    }
}

#[tokio::test]
async fn every_merge_reconciliation_reason_is_exact_from_merge_pending() {
    let cases = [
        (
            MergeReconciliationReason::DeliveryStateInconsistent,
            "DELIVERY_RECONCILIATION_REQUIRED",
        ),
        (
            MergeReconciliationReason::SourceInconsistent,
            "DELIVERY_SOURCE_INCONSISTENT",
        ),
        (
            MergeReconciliationReason::ProcessTreeCleanupFailed,
            "PROCESS_TREE_CLEANUP_FAILED",
        ),
        (
            MergeReconciliationReason::WorktreeIdentityMismatch,
            "WORKTREE_IDENTITY_MISMATCH",
        ),
        (
            MergeReconciliationReason::UnsafeGitConfiguration,
            "UNSAFE_GIT_CONFIGURATION",
        ),
        (
            MergeReconciliationReason::UnsupportedGitAttributes,
            "UNSUPPORTED_GIT_ATTRIBUTES",
        ),
    ];

    for (reason, expected_code) in cases {
        let (store, task, operation_id, pending_version) = merge_pending().await;
        let request = ReconcileMergeRequest::try_new(
            task.id,
            operation_id,
            MergeOperationState::MergePending,
            pending_version,
            reason,
        )
        .unwrap();
        let applied = applied_receipt(store.reconcile_merge(request.clone()).await.unwrap());
        assert_eq!(applied.state, MergeOperationState::ReconciliationRequired);
        assert_eq!(applied.version, pending_version.next().unwrap());
        assert_eq!(
            applied.failure_code.as_ref().map(|code| code.as_str()),
            Some(expected_code)
        );
        assert_eq!(
            existing_receipt(store.reconcile_merge(request).await.unwrap()),
            applied
        );

        let operation = operation(&store, task.id, operation_id).await;
        assert_reconciliation_shape(
            &operation,
            expected_code,
            pending_version.next().unwrap(),
            true,
            true,
            false,
        );
    }
}

#[tokio::test]
async fn reconcile_merge_accepts_every_documented_nonterminal_origin() {
    let (store, task, operation_id) = pending_preflight().await;
    assert_legal_reconciliation(
        store,
        task,
        operation_id,
        MergeOperationState::PreflightPending,
        DeliveryVersion::initial(),
        ReconciliationShape::default(),
    )
    .await;

    let (store, task, operation_id) = pending_preflight().await;
    crate::preflight_results::ready(&store, task.id, operation_id).await;
    assert_legal_reconciliation(
        store,
        task,
        operation_id,
        MergeOperationState::PreflightReady,
        DeliveryVersion::try_new(2).unwrap(),
        ReconciliationShape {
            has_preflight_result: true,
            ..ReconciliationShape::default()
        },
    )
    .await;

    let (store, task, operation_id, accepted_version) = accepted_without_source().await;
    assert_legal_reconciliation(
        store,
        task,
        operation_id,
        MergeOperationState::Accepted,
        accepted_version,
        ReconciliationShape {
            has_preflight_result: true,
            has_accept_metadata: true,
            ..ReconciliationShape::default()
        },
    )
    .await;

    let (store, task, operation_id, pending_version) = merge_pending().await;
    assert_legal_reconciliation(
        store,
        task,
        operation_id,
        MergeOperationState::MergePending,
        pending_version,
        ReconciliationShape {
            has_preflight_result: true,
            has_accept_metadata: true,
            has_source_and_expected_merge: true,
            ..ReconciliationShape::default()
        },
    )
    .await;

    let (store, task, operation_id, abort_version) = abort_pending().await;
    assert_legal_reconciliation(
        store,
        task,
        operation_id,
        MergeOperationState::AbortPending,
        abort_version,
        ReconciliationShape {
            has_preflight_result: true,
            has_accept_metadata: true,
            has_source_and_expected_merge: true,
            has_abort_proof: true,
        },
    )
    .await;
}

#[test]
fn terminal_request_constructors_reject_illegal_origin_states() {
    let task_id = coding_agent_domain::TaskId::new();
    let operation_id = DeliveryOperationId::new();
    let version = DeliveryVersion::initial();

    for state in [
        MergeOperationState::PreflightPending,
        MergeOperationState::PreflightReady,
        MergeOperationState::Merged,
        MergeOperationState::AbortPending,
        MergeOperationState::Conflict,
        MergeOperationState::Rejected,
        MergeOperationState::Stale,
        MergeOperationState::Superseded,
        MergeOperationState::Failed,
        MergeOperationState::ReconciliationRequired,
    ] {
        assert!(
            RecordMergeKnownFailureRequest::try_new(
                task_id,
                operation_id,
                state,
                version,
                MergeKnownNotAppliedReason::CommandTimedOut,
            )
            .is_err(),
            "known failure constructor accepted {state:?}"
        );
    }

    for state in [
        MergeOperationState::Merged,
        MergeOperationState::Conflict,
        MergeOperationState::Rejected,
        MergeOperationState::Stale,
        MergeOperationState::Superseded,
        MergeOperationState::Failed,
        MergeOperationState::ReconciliationRequired,
    ] {
        assert!(
            ReconcileMergeRequest::try_new(
                task_id,
                operation_id,
                state,
                version,
                MergeReconciliationReason::DeliveryStateInconsistent,
            )
            .is_err(),
            "reconciliation constructor accepted {state:?}"
        );
    }
}

#[tokio::test]
async fn wrong_terminal_state_and_version_are_zero_write_conflicts() {
    let (store, task, operation_id, pending_version) = merge_pending().await;
    let before = operation(&store, task.id, operation_id).await;
    let journal_count_before = transition_count(&store, operation_id).await;

    let wrong_state = RecordMergeKnownFailureRequest::try_new(
        task.id,
        operation_id,
        MergeOperationState::Accepted,
        pending_version,
        MergeKnownNotAppliedReason::CommandTimedOut,
    )
    .unwrap();
    assert!(matches!(
        store.record_merge_known_failure(wrong_state).await.unwrap(),
        MergeTransitionOutcome::Conflict
    ));

    let wrong_version = ReconcileMergeRequest::try_new(
        task.id,
        operation_id,
        MergeOperationState::MergePending,
        pending_version.next().unwrap(),
        MergeReconciliationReason::DeliveryStateInconsistent,
    )
    .unwrap();
    assert!(matches!(
        store.reconcile_merge(wrong_version).await.unwrap(),
        MergeTransitionOutcome::Conflict
    ));

    assert_eq!(operation(&store, task.id, operation_id).await, before);
    assert_eq!(
        transition_count(&store, operation_id).await,
        journal_count_before
    );
}

async fn assert_known_failure(
    reason: MergeKnownNotAppliedReason,
    expected_code: &str,
    origin: KnownFailureOrigin,
) {
    let (store, task, operation_id, version, expected_merge) = match origin {
        KnownFailureOrigin::Accepted => {
            let (store, task, operation_id, version) = accepted_with_committed_source().await;
            (store, task, operation_id, version, None)
        }
        KnownFailureOrigin::MergePending => {
            let (store, task, operation_id, version) = merge_pending().await;
            (store, task, operation_id, version, Some(MERGE_COMMIT))
        }
    };
    let expected_state = match origin {
        KnownFailureOrigin::Accepted => MergeOperationState::Accepted,
        KnownFailureOrigin::MergePending => MergeOperationState::MergePending,
    };
    let request = RecordMergeKnownFailureRequest::try_new(
        task.id,
        operation_id,
        expected_state,
        version,
        reason,
    )
    .unwrap();
    let applied = applied_receipt(
        store
            .record_merge_known_failure(request.clone())
            .await
            .unwrap(),
    );
    assert_eq!(applied.state, MergeOperationState::Failed);
    assert_eq!(applied.version, version.next().unwrap());
    assert_eq!(
        applied.failure_code.as_ref().map(|code| code.as_str()),
        Some(expected_code)
    );
    assert_eq!(
        existing_receipt(store.record_merge_known_failure(request).await.unwrap()),
        applied
    );

    let operation = operation(&store, task.id, operation_id).await;
    assert_eq!(operation.state, MergeOperationState::Failed);
    assert_eq!(operation.version, version.next().unwrap());
    assert_eq!(
        operation.failure_code.as_ref().map(|code| code.as_str()),
        Some(expected_code)
    );
    assert_eq!(operation.delivery_source_task_id, Some(task.id));
    assert_eq!(
        operation.source_commit.as_ref().map(|oid| oid.as_str()),
        Some(SOURCE_COMMIT)
    );
    assert_eq!(
        operation.merge_base.as_ref().map(|oid| oid.as_str()),
        Some(MERGE_BASE)
    );
    assert_eq!(
        operation
            .candidate_merge_tree
            .as_ref()
            .map(|oid| oid.as_str()),
        Some(MERGE_TREE)
    );
    assert!(operation.accept_receipt_id.is_some());
    assert!(operation.merge_metadata.is_some());
    assert_eq!(
        operation
            .expected_merge_commit
            .as_ref()
            .map(|oid| oid.as_str()),
        expected_merge
    );
    assert!(operation.abort_child_receipt_id.is_none());
    assert!(operation.abort_merge_head.is_none());
    assert!(operation.merged_disposition_task_id.is_none());
    assert_eq!(operation.conflict_path_count, None);
    assert!(operation.conflicts.is_empty());
}

#[derive(Default)]
struct ReconciliationShape {
    has_preflight_result: bool,
    has_accept_metadata: bool,
    has_source_and_expected_merge: bool,
    has_abort_proof: bool,
}

async fn assert_legal_reconciliation(
    store: Store,
    task: Task,
    operation_id: DeliveryOperationId,
    state: MergeOperationState,
    version: DeliveryVersion,
    shape: ReconciliationShape,
) {
    let request = ReconcileMergeRequest::try_new(
        task.id,
        operation_id,
        state,
        version,
        MergeReconciliationReason::DeliveryStateInconsistent,
    )
    .unwrap();
    let applied = applied_receipt(store.reconcile_merge(request.clone()).await.unwrap());
    assert_eq!(applied.state, MergeOperationState::ReconciliationRequired);
    assert_eq!(applied.version, version.next().unwrap());
    assert_eq!(
        applied.failure_code.as_ref().map(|code| code.as_str()),
        Some("DELIVERY_RECONCILIATION_REQUIRED")
    );
    assert_eq!(
        existing_receipt(store.reconcile_merge(request).await.unwrap()),
        applied
    );

    let operation = operation(&store, task.id, operation_id).await;
    assert_reconciliation_shape(
        &operation,
        "DELIVERY_RECONCILIATION_REQUIRED",
        version.next().unwrap(),
        shape.has_preflight_result,
        shape.has_source_and_expected_merge,
        shape.has_abort_proof,
    );
    assert_eq!(
        operation.merge_metadata.is_some(),
        shape.has_accept_metadata
    );
    assert_eq!(
        operation.accept_receipt_id.is_some(),
        shape.has_accept_metadata
    );
}

fn assert_reconciliation_shape(
    operation: &MergeOperationRecord,
    expected_code: &str,
    expected_version: DeliveryVersion,
    has_preflight_result: bool,
    has_source_and_expected_merge: bool,
    has_abort_proof: bool,
) {
    assert_eq!(operation.state, MergeOperationState::ReconciliationRequired);
    assert_eq!(operation.version, expected_version);
    assert_eq!(
        operation.failure_code.as_ref().map(|code| code.as_str()),
        Some(expected_code)
    );
    assert_eq!(operation.merge_base.is_some(), has_preflight_result);
    assert_eq!(
        operation.candidate_merge_tree.is_some(),
        has_preflight_result
    );
    assert_eq!(
        operation.delivery_source_task_id.is_some(),
        has_source_and_expected_merge
    );
    assert_eq!(
        operation.source_commit.is_some(),
        has_source_and_expected_merge
    );
    assert_eq!(
        operation.expected_merge_commit.is_some(),
        has_source_and_expected_merge
    );
    assert_eq!(operation.abort_child_receipt_id.is_some(), has_abort_proof);
    assert_eq!(operation.abort_merge_head.is_some(), has_abort_proof);
    assert_eq!(
        operation.abort_index_stages_digest.is_some(),
        has_abort_proof
    );
    assert_eq!(operation.abort_worktree_digest.is_some(), has_abort_proof);
    assert_eq!(
        operation.abort_merge_autostash_proof.is_some(),
        has_abort_proof
    );
    assert!(operation.merged_disposition_task_id.is_none());
    assert_eq!(operation.conflict_path_count, None);
    assert!(operation.conflicts.is_empty());
}

async fn accepted_without_source() -> (Store, Task, DeliveryOperationId, DeliveryVersion) {
    let (store, task, operation_id) = pending_preflight().await;
    crate::preflight_results::ready(&store, task.id, operation_id).await;
    let request = accept_command(&store, &task, operation_id, ClientRequestId::new()).await;
    let version = match store.accept_merge(request).await.unwrap() {
        AcceptMergeOutcome::Accepted(receipt) => receipt.accepted_operation_version,
        other => panic!("expected accepted merge, got {other:?}"),
    };
    (store, task, operation_id, version)
}

async fn abort_pending() -> (Store, Task, DeliveryOperationId, DeliveryVersion) {
    let (store, task, operation_id, pending_version) = merge_pending().await;
    let proof = MergeAbortProof::try_new(
        Uuid::new_v4(),
        GitBranchRef::from_str(TARGET_BRANCH).unwrap(),
        GitCommitOid::from_str(TARGET_HEAD).unwrap(),
        GitBranchRef::from_str("refs/heads/codex/task-merge-store").unwrap(),
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", COMMON_IDENTITY).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", ADMIN_IDENTITY).unwrap(),
        "codex-reserved".to_owned(),
        Sha256Digest::from_str(CONFIG_DIGEST).unwrap(),
        Sha256Digest::from_str(INDEX_STAGES).unwrap(),
        Sha256Digest::from_str(WORKTREE).unwrap(),
        MergeAutostashObservation::Absent,
        OtherGitOperationObservation::Clear,
    )
    .unwrap();
    let request =
        BeginMergeAbortRequest::try_new(task.id, operation_id, pending_version, proof).unwrap();
    let version = applied_receipt(store.begin_merge_abort(request).await.unwrap()).version;
    (store, task, operation_id, version)
}

async fn operation(
    store: &Store,
    task_id: coding_agent_domain::TaskId,
    operation_id: DeliveryOperationId,
) -> MergeOperationRecord {
    store
        .delivery_ownership_snapshot(task_id)
        .await
        .unwrap()
        .unwrap()
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap()
}

async fn transition_count(store: &Store, operation_id: DeliveryOperationId) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'merge_operation' AND entity_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap()
}

fn applied_receipt(outcome: MergeTransitionOutcome) -> coding_agent_store::MergeTransitionReceipt {
    match outcome {
        MergeTransitionOutcome::Applied(receipt) => receipt,
        other => panic!("expected applied transition, got {other:?}"),
    }
}

fn existing_receipt(outcome: MergeTransitionOutcome) -> coding_agent_store::MergeTransitionReceipt {
    match outcome {
        MergeTransitionOutcome::Existing(receipt) => receipt,
        other => panic!("expected existing transition, got {other:?}"),
    }
}
