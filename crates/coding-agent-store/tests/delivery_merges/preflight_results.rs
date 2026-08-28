use std::str::FromStr;

use coding_agent_store::{
    DeliveryVersion, GitCommitOid, GitTreeOid, MergeConflictPathEncoding, MergeConflictPaths,
    MergeOperationState, MergePreflightResult, MergeReconciliationReason, MergeTransitionOutcome,
    PreflightRejectedReason, PreflightStaleReason, RecordMergePreflightResultRequest,
};

use crate::support::delivery::eligibility::{MERGE_BASE, MERGE_TREE};

use super::fixtures::pending_preflight;

pub async fn ready(
    store: &coding_agent_store::Store,
    task_id: coding_agent_domain::TaskId,
    operation_id: coding_agent_store::DeliveryOperationId,
) {
    let result = MergePreflightResult::ready(
        GitCommitOid::from_str(MERGE_BASE).unwrap(),
        GitTreeOid::from_str(MERGE_TREE).unwrap(),
    )
    .unwrap();
    let request = RecordMergePreflightResultRequest::try_new(
        task_id,
        operation_id,
        DeliveryVersion::try_new(2).unwrap(),
        result,
    )
    .unwrap();
    assert!(matches!(
        store.record_merge_preflight_result(request).await.unwrap(),
        MergeTransitionOutcome::Applied(_)
    ));
}

#[tokio::test]
async fn zero_path_conflict_is_durable_and_replays_exactly() {
    let (store, task, operation_id) = pending_preflight().await;
    let result = MergePreflightResult::conflict(
        GitCommitOid::from_str(MERGE_BASE).unwrap(),
        GitTreeOid::from_str(MERGE_TREE).unwrap(),
        MergeConflictPaths::try_from_raw(Vec::new()).unwrap(),
    )
    .unwrap();
    let request = RecordMergePreflightResultRequest::try_new(
        task.id,
        operation_id,
        DeliveryVersion::try_new(2).unwrap(),
        result,
    )
    .unwrap();
    assert!(matches!(
        store
            .record_merge_preflight_result(request.clone())
            .await
            .unwrap(),
        MergeTransitionOutcome::Applied(_)
    ));
    assert!(matches!(
        store.record_merge_preflight_result(request).await.unwrap(),
        MergeTransitionOutcome::Existing(_)
    ));
    let operation = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap();
    assert_eq!(operation.state, MergeOperationState::Conflict);
    assert!(operation.conflicts.is_empty());
    assert_eq!(operation.merge_base.unwrap().as_str(), MERGE_BASE);
    assert_eq!(operation.candidate_merge_tree.unwrap().as_str(), MERGE_TREE);
}

#[tokio::test]
async fn backslash_and_drive_looking_git_path_bytes_round_trip_as_utf8() {
    let (store, task, operation_id) = pending_preflight().await;
    let raw = vec![
        br"dir\name".to_vec(),
        br"C:\name".to_vec(),
        br"C:/name".to_vec(),
    ];
    let result = MergePreflightResult::conflict(
        GitCommitOid::from_str(MERGE_BASE).unwrap(),
        GitTreeOid::from_str(MERGE_TREE).unwrap(),
        MergeConflictPaths::try_from_raw(raw.clone()).unwrap(),
    )
    .unwrap();
    let request = RecordMergePreflightResultRequest::try_new(
        task.id,
        operation_id,
        DeliveryVersion::try_new(2).unwrap(),
        result,
    )
    .unwrap();
    store.record_merge_preflight_result(request).await.unwrap();
    let operation = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap();
    assert_eq!(operation.conflicts.len(), raw.len());
    for (record, expected) in operation.conflicts.iter().zip(raw) {
        assert_eq!(record.path_encoding, MergeConflictPathEncoding::Utf8);
        assert_eq!(record.path_value, expected);
    }
}

#[tokio::test]
async fn clean_preflight_result_advances_pending_to_ready_with_exact_shape() {
    let (store, task, operation_id) = pending_preflight().await;
    let result = MergePreflightResult::ready(
        GitCommitOid::from_str(MERGE_BASE).unwrap(),
        GitTreeOid::from_str(MERGE_TREE).unwrap(),
    )
    .unwrap();
    let request = RecordMergePreflightResultRequest::try_new(
        task.id,
        operation_id,
        DeliveryVersion::try_new(2).unwrap(),
        result,
    )
    .unwrap();

    let receipt = match store.record_merge_preflight_result(request).await.unwrap() {
        MergeTransitionOutcome::Applied(receipt) => receipt,
        other => panic!("expected applied preflight result, got {other:?}"),
    };
    assert_eq!(receipt.state, MergeOperationState::PreflightReady);
    assert_eq!(receipt.version, DeliveryVersion::try_new(3).unwrap());
    assert_eq!(receipt.failure_code, None);

    let operation = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap();
    assert_eq!(operation.merge_base.unwrap().as_str(), MERGE_BASE);
    assert_eq!(operation.candidate_merge_tree.unwrap().as_str(), MERGE_TREE);
}

#[tokio::test]
async fn every_rejected_reason_is_durable_and_replays_exactly() {
    let cases = [
        (
            PreflightRejectedReason::TaskNotMergeEligible,
            "TASK_NOT_MERGE_ELIGIBLE",
        ),
        (
            PreflightRejectedReason::TargetBranchDetached,
            "TARGET_BRANCH_DETACHED",
        ),
        (
            PreflightRejectedReason::TargetBranchMismatch,
            "TARGET_BRANCH_MISMATCH",
        ),
        (
            PreflightRejectedReason::TargetWorktreeDirty,
            "TARGET_WORKTREE_DIRTY",
        ),
        (
            PreflightRejectedReason::TargetIgnoredPathCollision,
            "TARGET_IGNORED_PATH_COLLISION",
        ),
        (
            PreflightRejectedReason::TargetGitOperationInProgress,
            "TARGET_GIT_OPERATION_IN_PROGRESS",
        ),
        (
            PreflightRejectedReason::UnsafeGitConfiguration,
            "UNSAFE_GIT_CONFIGURATION",
        ),
        (
            PreflightRejectedReason::UnsupportedGitAttributes,
            "UNSUPPORTED_GIT_ATTRIBUTES",
        ),
        (
            PreflightRejectedReason::SourceAlreadyInTarget,
            "SOURCE_ALREADY_IN_TARGET",
        ),
    ];
    for (reason, failure_code) in cases {
        assert_terminal_preflight_result(
            MergePreflightResult::rejected(reason),
            MergeOperationState::Rejected,
            failure_code,
        )
        .await;
    }
}

#[tokio::test]
async fn every_stale_reason_is_durable_and_replays_exactly() {
    let cases = [
        (
            PreflightStaleReason::EvidenceStale,
            "DELIVERY_EVIDENCE_STALE",
        ),
        (
            PreflightStaleReason::TargetBranchChanged,
            "TARGET_BRANCH_MISMATCH",
        ),
        (
            PreflightStaleReason::TargetHeadChanged,
            "TARGET_HEAD_CHANGED",
        ),
        (
            PreflightStaleReason::SourceChanged,
            "DELIVERY_SOURCE_CHANGED",
        ),
    ];
    for (reason, failure_code) in cases {
        assert_terminal_preflight_result(
            MergePreflightResult::stale(reason),
            MergeOperationState::Stale,
            failure_code,
        )
        .await;
    }
}

#[tokio::test]
async fn every_preflight_reconciliation_reason_is_durable_and_replays_exactly() {
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
    for (reason, failure_code) in cases {
        assert_terminal_preflight_result(
            MergePreflightResult::reconciliation_required(reason),
            MergeOperationState::ReconciliationRequired,
            failure_code,
        )
        .await;
    }
}

#[tokio::test]
async fn a_different_terminal_reason_is_a_zero_write_conflict() {
    let (store, task, operation_id) = pending_preflight().await;
    let applied = RecordMergePreflightResultRequest::try_new(
        task.id,
        operation_id,
        DeliveryVersion::try_new(2).unwrap(),
        MergePreflightResult::rejected(PreflightRejectedReason::TargetWorktreeDirty),
    )
    .unwrap();
    store.record_merge_preflight_result(applied).await.unwrap();
    let mismatched = RecordMergePreflightResultRequest::try_new(
        task.id,
        operation_id,
        DeliveryVersion::try_new(2).unwrap(),
        MergePreflightResult::rejected(PreflightRejectedReason::TargetBranchDetached),
    )
    .unwrap();
    assert!(matches!(
        store
            .record_merge_preflight_result(mismatched)
            .await
            .unwrap(),
        MergeTransitionOutcome::Conflict
    ));
    let row: (String, i64, String) = sqlx::query_as(
        "SELECT state, version, failure_code FROM task_merge_operations WHERE operation_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(
        row,
        ("rejected".to_owned(), 3, "TARGET_WORKTREE_DIRTY".to_owned())
    );
}

#[tokio::test]
async fn conflict_path_constructor_enforces_count_wire_total_and_raw_path_boundaries() {
    let exactly_128 = (0..128)
        .map(|index| format!("path-{index}").into_bytes())
        .collect::<Vec<_>>();
    assert_eq!(
        MergeConflictPaths::try_from_raw(exactly_128).unwrap().len(),
        128
    );
    let too_many = (0..129)
        .map(|index| format!("path-{index}").into_bytes())
        .collect::<Vec<_>>();
    assert!(MergeConflictPaths::try_from_raw(too_many).is_err());

    assert!(MergeConflictPaths::try_from_raw(vec![vec![b'a'; 4096]]).is_ok());
    assert!(MergeConflictPaths::try_from_raw(vec![vec![b'a'; 4097]]).is_err());
    assert!(MergeConflictPaths::try_from_raw(vec![vec![0xff; 3072]]).is_ok());
    assert!(MergeConflictPaths::try_from_raw(vec![vec![0xff; 3073]]).is_err());
    assert!(MergeConflictPaths::try_from_raw(vec![vec![0xff; 1024 * 1024]]).is_err());

    let exactly_64k = (0..16)
        .map(|index| {
            let mut path = vec![b'a' + index; 4096];
            path[1] = b'x';
            path
        })
        .collect::<Vec<_>>();
    assert!(MergeConflictPaths::try_from_raw(exactly_64k.clone()).is_ok());
    let mut over_64k = exactly_64k;
    over_64k.push(b"z".to_vec());
    assert!(MergeConflictPaths::try_from_raw(over_64k).is_err());

    for invalid in [
        Vec::new(),
        b"a\0b".to_vec(),
        b"/absolute".to_vec(),
        b"a//b".to_vec(),
        b"a/./b".to_vec(),
        b"a/../b".to_vec(),
    ] {
        assert!(MergeConflictPaths::try_from_raw(vec![invalid]).is_err());
    }
    assert!(MergeConflictPaths::try_from_raw(vec![b"same".to_vec(), b"same".to_vec()]).is_err());
}

#[tokio::test]
async fn invalid_utf8_path_round_trips_as_canonical_base64url() {
    let (store, task, operation_id) = pending_preflight().await;
    let raw = vec![0xff, b'a', 0xfe];
    let request = RecordMergePreflightResultRequest::try_new(
        task.id,
        operation_id,
        DeliveryVersion::try_new(2).unwrap(),
        MergePreflightResult::conflict(
            GitCommitOid::from_str(MERGE_BASE).unwrap(),
            GitTreeOid::from_str(MERGE_TREE).unwrap(),
            MergeConflictPaths::try_from_raw(vec![raw]).unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    store.record_merge_preflight_result(request).await.unwrap();
    let conflict = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap()
        .conflicts
        .pop()
        .unwrap();
    assert_eq!(conflict.path_encoding, MergeConflictPathEncoding::Base64Url);
    assert!(!conflict.path_value.contains(&b'='));
}

async fn assert_terminal_preflight_result(
    result: MergePreflightResult,
    expected_state: MergeOperationState,
    expected_failure: &str,
) {
    let (store, task, operation_id) = pending_preflight().await;
    let request = RecordMergePreflightResultRequest::try_new(
        task.id,
        operation_id,
        DeliveryVersion::try_new(2).unwrap(),
        result,
    )
    .unwrap();
    assert!(matches!(
        store
            .record_merge_preflight_result(request.clone())
            .await
            .unwrap(),
        MergeTransitionOutcome::Applied(_)
    ));
    assert!(matches!(
        store.record_merge_preflight_result(request).await.unwrap(),
        MergeTransitionOutcome::Existing(_)
    ));
    let operation = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap();
    assert_eq!(operation.state, expected_state);
    assert_eq!(operation.failure_code.unwrap().as_str(), expected_failure);
    assert_eq!(operation.conflict_path_count, None);
    assert!(operation.conflicts.is_empty());
}
