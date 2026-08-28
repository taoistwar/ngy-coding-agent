use std::str::FromStr;

use coding_agent_domain::{ClientRequestId, Task};
use coding_agent_store::{
    BindMergePreflightInputsRequest, CreatePreflightOutcome, CreatePreflightRequest, DeliveryError,
    DeliveryRecoveryAction, DeliveryRecoveryDisposition, DeliveryRecoveryQuery, DeliveryVersion,
    DirectoryIdentity, FailUnboundMergePreflightRequest, GitBranchRef, GitCommitOid, GitTreeOid,
    MergeOperationState, MergePreflightResult, MergeReconciliationReason, MergeTransitionOutcome,
    PreflightCommandRequest, PreflightRejectedReason, PreflightStaleReason,
    RecordMergePreflightResultRequest, Sha256Digest, Store, StoreError,
    UnboundMergePreflightFailure,
};

use crate::support::delivery::eligibility::{
    ADMIN_IDENTITY, CANDIDATE_TREE, COMMON_IDENTITY, CONFIG_DIGEST, MERGE_BASE, MERGE_TREE,
    PREFLIGHT_SOURCE, TARGET_CONFIG_DIGEST, TARGET_HEAD, TARGET_SECURITY_DIGEST,
    approved_task_with_ready_artifact,
};

async fn intent() -> (Store, Task, coding_agent_store::DeliveryOperationId) {
    let (store, task) = approved_task_with_ready_artifact("codex/preflight-intent").await;
    let command = PreflightCommandRequest::try_new(
        ClientRequestId::new(),
        task.id,
        GitBranchRef::from_str("refs/heads/main").unwrap(),
        GitCommitOid::from_str(TARGET_HEAD).unwrap(),
    )
    .unwrap();
    let request = CreatePreflightRequest::try_new(
        command,
        DirectoryIdentity::try_new("directory_identity_v1", COMMON_IDENTITY).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", ADMIN_IDENTITY).unwrap(),
        Sha256Digest::from_str(CONFIG_DIGEST).unwrap(),
        Sha256Digest::from_str(TARGET_CONFIG_DIGEST).unwrap(),
        Sha256Digest::from_str(TARGET_SECURITY_DIGEST).unwrap(),
    )
    .unwrap();
    let operation_id = match store.create_merge_preflight(request).await.unwrap() {
        CreatePreflightOutcome::Created(receipt) => receipt.operation_id,
        other => panic!("expected durable intent, got {other:?}"),
    };
    (store, task, operation_id)
}

fn bind_request(
    task: &Task,
    operation_id: coding_agent_store::DeliveryOperationId,
) -> BindMergePreflightInputsRequest {
    BindMergePreflightInputsRequest::try_new(
        task.id,
        operation_id,
        DeliveryVersion::initial(),
        GitTreeOid::from_str(CANDIDATE_TREE).unwrap(),
        GitCommitOid::from_str(PREFLIGHT_SOURCE).unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn create_persists_an_unbound_intent_and_receipt_atomically() {
    let (store, task, operation_id) = intent().await;
    let ownership = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    let operation = ownership
        .merge_operations
        .iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap();
    assert_eq!(operation.state, MergeOperationState::PreflightPending);
    assert_eq!(operation.version, DeliveryVersion::initial());
    assert!(operation.preflight_inputs.is_none());
    assert_eq!(
        operation.target_config_attributes_digest.as_str(),
        TARGET_CONFIG_DIGEST
    );
    assert_eq!(
        operation.target_security_digest.as_str(),
        TARGET_SECURITY_DIGEST
    );
    assert_ne!(
        operation.provenance.config_attributes_digest,
        operation.target_config_attributes_digest
    );

    let receipt_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_delivery_command_receipts \
         WHERE merge_operation_id = ? AND command_kind = 'preflight'",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(receipt_count, 1);
}

#[tokio::test]
async fn create_replay_requires_the_exact_authenticated_provenance_baseline() {
    let (store, task) = approved_task_with_ready_artifact("codex/preflight-replay-baseline").await;
    let command = PreflightCommandRequest::try_new(
        ClientRequestId::new(),
        task.id,
        GitBranchRef::from_str("refs/heads/main").unwrap(),
        GitCommitOid::from_str(TARGET_HEAD).unwrap(),
    )
    .unwrap();
    let common = DirectoryIdentity::try_new("directory_identity_v1", COMMON_IDENTITY).unwrap();
    let admin = DirectoryIdentity::try_new("directory_identity_v1", ADMIN_IDENTITY).unwrap();
    let source = Sha256Digest::from_str(CONFIG_DIGEST).unwrap();
    let target_config = Sha256Digest::from_str(TARGET_CONFIG_DIGEST).unwrap();
    let target_security = Sha256Digest::from_str(TARGET_SECURITY_DIGEST).unwrap();
    let original = CreatePreflightRequest::try_new(
        command.clone(),
        common.clone(),
        admin.clone(),
        source.clone(),
        target_config.clone(),
        target_security.clone(),
    )
    .unwrap();
    assert!(matches!(
        store
            .create_merge_preflight(original.clone())
            .await
            .unwrap(),
        CreatePreflightOutcome::Created(_)
    ));
    assert!(matches!(
        store.create_merge_preflight(original).await.unwrap(),
        CreatePreflightOutcome::Existing(_)
    ));

    for changed in [
        CreatePreflightRequest::try_new(
            command.clone(),
            common.clone(),
            admin.clone(),
            target_config.clone(),
            target_config.clone(),
            target_security.clone(),
        )
        .unwrap(),
        CreatePreflightRequest::try_new(
            command.clone(),
            common.clone(),
            admin.clone(),
            source.clone(),
            source.clone(),
            target_security.clone(),
        )
        .unwrap(),
        CreatePreflightRequest::try_new(
            command,
            common,
            admin,
            source.clone(),
            target_config,
            source,
        )
        .unwrap(),
    ] {
        assert!(matches!(
            store.create_merge_preflight(changed).await,
            Err(StoreError::Delivery(DeliveryError::InvalidCommandRequest))
        ));
    }
}

#[tokio::test]
async fn bind_is_version_cas_exactly_replayable_and_immutable() {
    let (store, task, operation_id) = intent().await;
    let request = bind_request(&task, operation_id);
    let applied = store
        .bind_merge_preflight_inputs(request.clone())
        .await
        .unwrap();
    let applied_receipt = match applied {
        MergeTransitionOutcome::Applied(receipt) => receipt,
        other => panic!("expected applied input binding, got {other:?}"),
    };
    let replay = store.bind_merge_preflight_inputs(request).await.unwrap();
    assert_eq!(
        replay,
        MergeTransitionOutcome::Existing(applied_receipt.clone())
    );

    let conflicting = BindMergePreflightInputsRequest::try_new(
        task.id,
        operation_id,
        DeliveryVersion::initial(),
        GitTreeOid::from_str(MERGE_TREE).unwrap(),
        GitCommitOid::from_str(PREFLIGHT_SOURCE).unwrap(),
    )
    .unwrap();
    assert_eq!(
        store
            .bind_merge_preflight_inputs(conflicting)
            .await
            .unwrap(),
        MergeTransitionOutcome::Conflict
    );
    assert!(
        sqlx::query(
            "UPDATE task_merge_operations SET candidate_tree_oid = ? WHERE operation_id = ?"
        )
        .bind(MERGE_TREE)
        .bind(operation_id.to_string())
        .execute(store.pool())
        .await
        .is_err()
    );
    assert!(
        sqlx::query(
            "UPDATE task_merge_operations SET target_security_digest = ? WHERE operation_id = ?",
        )
        .bind(CONFIG_DIGEST)
        .bind(operation_id.to_string())
        .execute(store.pool())
        .await
        .is_err()
    );

    let operation = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap();
    let inputs = operation.preflight_inputs.unwrap();
    assert_eq!(inputs.candidate_tree.as_str(), CANDIDATE_TREE);
    assert_eq!(inputs.preflight_source_commit.as_str(), PREFLIGHT_SOURCE);
    assert_eq!(operation.version, DeliveryVersion::try_new(2).unwrap());
    let transition_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'merge_operation' AND entity_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(transition_count, 2);
}

#[test]
fn bind_request_rejects_any_version_other_than_the_unbound_intent_version() {
    assert!(
        BindMergePreflightInputsRequest::try_new(
            coding_agent_domain::TaskId::new(),
            coding_agent_store::DeliveryOperationId::new(),
            DeliveryVersion::try_new(2).unwrap(),
            GitTreeOid::from_str(CANDIDATE_TREE).unwrap(),
            GitCommitOid::from_str(PREFLIGHT_SOURCE).unwrap(),
        )
        .is_err()
    );
}

#[tokio::test]
async fn result_requires_bound_inputs_and_then_advances_from_prepared_pending() {
    let (store, task, operation_id) = intent().await;
    let result = MergePreflightResult::ready(
        GitCommitOid::from_str(MERGE_BASE).unwrap(),
        GitTreeOid::from_str(MERGE_TREE).unwrap(),
    )
    .unwrap();
    let unbound = RecordMergePreflightResultRequest::try_new(
        task.id,
        operation_id,
        DeliveryVersion::initial(),
        result.clone(),
    )
    .unwrap();
    assert_eq!(
        store.record_merge_preflight_result(unbound).await.unwrap(),
        MergeTransitionOutcome::Conflict
    );
    assert!(matches!(
        store
            .bind_merge_preflight_inputs(bind_request(&task, operation_id))
            .await
            .unwrap(),
        MergeTransitionOutcome::Applied(_)
    ));
    let prepared = RecordMergePreflightResultRequest::try_new(
        task.id,
        operation_id,
        DeliveryVersion::try_new(2).unwrap(),
        result,
    )
    .unwrap();
    let applied = store.record_merge_preflight_result(prepared).await.unwrap();
    assert!(matches!(applied, MergeTransitionOutcome::Applied(_)));
}

#[tokio::test]
async fn unbound_terminal_failures_are_v2_exact_replayable_and_keep_inputs_absent() {
    for (failure, expected_state, expected_code) in [
        (
            UnboundMergePreflightFailure::Rejected(PreflightRejectedReason::TargetWorktreeDirty),
            "rejected",
            "TARGET_WORKTREE_DIRTY",
        ),
        (
            UnboundMergePreflightFailure::Stale(PreflightStaleReason::TargetHeadChanged),
            "stale",
            "TARGET_HEAD_CHANGED",
        ),
        (
            UnboundMergePreflightFailure::ReconciliationRequired(
                MergeReconciliationReason::DeliveryStateInconsistent,
            ),
            "reconciliation_required",
            "DELIVERY_RECONCILIATION_REQUIRED",
        ),
    ] {
        let (store, task, operation_id) = intent().await;
        let request = FailUnboundMergePreflightRequest::try_new(
            task.id,
            operation_id,
            DeliveryVersion::initial(),
            failure,
        )
        .unwrap();
        let applied = match store
            .fail_unbound_merge_preflight(request.clone())
            .await
            .unwrap()
        {
            MergeTransitionOutcome::Applied(receipt) => receipt,
            other => panic!("expected applied unbound failure, got {other:?}"),
        };
        assert_eq!(applied.version, DeliveryVersion::try_new(2).unwrap());
        assert_eq!(
            store.fail_unbound_merge_preflight(request).await.unwrap(),
            MergeTransitionOutcome::Existing(applied)
        );

        let row: (String, String, i64, Option<String>, Option<String>) = sqlx::query_as(
            "SELECT state, failure_code, version, candidate_tree_oid, \
                    preflight_source_commit_oid \
             FROM task_merge_operations WHERE operation_id = ?",
        )
        .bind(operation_id.to_string())
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(
            row,
            (
                expected_state.to_owned(),
                expected_code.to_owned(),
                2,
                None,
                None,
            )
        );
        let journal: (String, String, String, i64) = sqlx::query_as(
            "SELECT from_state, to_state, failure_code, entity_version \
             FROM task_delivery_operation_transitions \
             WHERE entity_kind = 'merge_operation' AND entity_id = ? AND entity_version = 2",
        )
        .bind(operation_id.to_string())
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(
            journal,
            (
                "preflight_pending".to_owned(),
                expected_state.to_owned(),
                expected_code.to_owned(),
                2,
            )
        );
        assert_eq!(
            store
                .bind_merge_preflight_inputs(bind_request(&task, operation_id))
                .await
                .unwrap(),
            MergeTransitionOutcome::Conflict
        );
    }
}

#[tokio::test]
async fn unbound_failure_changed_replay_is_conflict_and_non_initial_version_is_invalid() {
    let (store, task, operation_id) = intent().await;
    let rejected = FailUnboundMergePreflightRequest::try_new(
        task.id,
        operation_id,
        DeliveryVersion::initial(),
        UnboundMergePreflightFailure::Rejected(PreflightRejectedReason::TargetWorktreeDirty),
    )
    .unwrap();
    assert!(matches!(
        store.fail_unbound_merge_preflight(rejected).await.unwrap(),
        MergeTransitionOutcome::Applied(_)
    ));
    let changed = FailUnboundMergePreflightRequest::try_new(
        task.id,
        operation_id,
        DeliveryVersion::initial(),
        UnboundMergePreflightFailure::Stale(PreflightStaleReason::TargetHeadChanged),
    )
    .unwrap();
    assert_eq!(
        store.fail_unbound_merge_preflight(changed).await.unwrap(),
        MergeTransitionOutcome::Conflict
    );
    assert!(
        FailUnboundMergePreflightRequest::try_new(
            task.id,
            operation_id,
            DeliveryVersion::try_new(2).unwrap(),
            UnboundMergePreflightFailure::Stale(PreflightStaleReason::TargetHeadChanged),
        )
        .is_err()
    );
}

#[tokio::test]
async fn unbound_terminal_history_remains_valid_after_a_later_source_is_bound() {
    let (store, task, first_operation_id) = intent().await;
    let fail = FailUnboundMergePreflightRequest::try_new(
        task.id,
        first_operation_id,
        DeliveryVersion::initial(),
        UnboundMergePreflightFailure::Rejected(PreflightRejectedReason::TargetWorktreeDirty),
    )
    .unwrap();
    assert!(matches!(
        store.fail_unbound_merge_preflight(fail).await.unwrap(),
        MergeTransitionOutcome::Applied(_)
    ));

    let second_operation_id = super::fixtures::create_pending_preflight(&store, &task).await;
    crate::preflight_results::ready(&store, task.id, second_operation_id).await;
    let accept =
        super::fixtures::accept_command(&store, &task, second_operation_id, ClientRequestId::new())
            .await;
    assert!(matches!(
        store.accept_merge(accept).await.unwrap(),
        coding_agent_store::AcceptMergeOutcome::Accepted(_)
    ));
    crate::support::delivery::eligibility::create_committed_source(
        &store,
        &task,
        second_operation_id,
    )
    .await;

    let ownership = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    assert!(ownership.source.is_some());
    assert!(ownership.merge_operations.iter().any(|operation| {
        operation.operation_id == first_operation_id && operation.preflight_inputs.is_none()
    }));
    assert!(ownership.merge_operations.iter().any(|operation| {
        operation.operation_id == second_operation_id && operation.preflight_inputs.is_some()
    }));
}

#[tokio::test]
async fn recovery_distinguishes_unbound_and_prepared_pending() {
    let (store, task, operation_id) = intent().await;
    let identity = DirectoryIdentity::try_new("directory_identity_v1", COMMON_IDENTITY).unwrap();
    let batch = store
        .delivery_recovery_batch(&DeliveryRecoveryQuery::first(identity.clone()))
        .await
        .unwrap();
    let debug = format!("{:?}", batch.entries[0].disposition);
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains(TARGET_CONFIG_DIGEST));
    assert!(!debug.contains(TARGET_SECURITY_DIGEST));
    assert!(matches!(
        &batch.entries[0].disposition,
        DeliveryRecoveryDisposition::Recover(DeliveryRecoveryAction::PreflightPending {
            operation_id: current,
            version,
            inputs: None,
            target_config_attributes_digest,
            target_security_digest,
        }) if *current == operation_id
            && *version == DeliveryVersion::initial()
            && target_config_attributes_digest.as_str() == TARGET_CONFIG_DIGEST
            && target_security_digest.as_str() == TARGET_SECURITY_DIGEST
    ));

    store
        .bind_merge_preflight_inputs(bind_request(&task, operation_id))
        .await
        .unwrap();
    let batch = store
        .delivery_recovery_batch(&DeliveryRecoveryQuery::first(identity))
        .await
        .unwrap();
    assert!(matches!(
        &batch.entries[0].disposition,
        DeliveryRecoveryDisposition::Recover(DeliveryRecoveryAction::PreflightPending {
            operation_id: current,
            version,
            inputs: Some(inputs),
            target_config_attributes_digest,
            target_security_digest,
        }) if *current == operation_id
            && *version == DeliveryVersion::try_new(2).unwrap()
            && inputs.candidate_tree.as_str() == CANDIDATE_TREE
            && inputs.preflight_source_commit.as_str() == PREFLIGHT_SOURCE
            && target_config_attributes_digest.as_str() == TARGET_CONFIG_DIGEST
            && target_security_digest.as_str() == TARGET_SECURITY_DIGEST
    ));
}

#[tokio::test]
async fn ownership_fails_closed_when_current_target_provenance_diverges_from_history() {
    let (store, task, operation_id) = intent().await;
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::raw_sql(
        "DROP TRIGGER task_merge_operations_immutable_on_update; \
         DROP TRIGGER task_merge_operations_transition_on_update; \
         DROP TRIGGER task_merge_operations_journal_on_update;",
    )
    .execute(&mut *connection)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_merge_operations SET target_security_digest = ? WHERE operation_id = ?",
    )
    .bind(CONFIG_DIGEST)
    .bind(operation_id.to_string())
    .execute(&mut *connection)
    .await
    .unwrap();
    drop(connection);
    assert!(matches!(
        store.delivery_ownership_snapshot(task.id).await,
        Err(StoreError::InvariantViolation(_))
    ));
}

#[tokio::test]
async fn ownership_fails_closed_on_a_half_bound_preflight_input_pair() {
    let (store, task, operation_id) = intent().await;
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::raw_sql(
        "DROP TRIGGER task_merge_operations_immutable_on_update; \
         DROP TRIGGER task_merge_operations_transition_on_update; \
         DROP TRIGGER task_merge_operations_journal_on_update;",
    )
    .execute(&mut *connection)
    .await
    .unwrap();
    sqlx::query("PRAGMA ignore_check_constraints = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("UPDATE task_merge_operations SET candidate_tree_oid = ? WHERE operation_id = ?")
        .bind(CANDIDATE_TREE)
        .bind(operation_id.to_string())
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("PRAGMA ignore_check_constraints = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    drop(connection);

    assert!(matches!(
        store.delivery_ownership_snapshot(task.id).await,
        Err(StoreError::InvariantViolation(_))
    ));
}

#[tokio::test]
async fn schema_rejects_bound_inputs_on_the_unbound_terminal_version() {
    let (store, _task, operation_id) = intent().await;
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::raw_sql(
        "DROP TRIGGER task_merge_operations_immutable_on_update; \
         DROP TRIGGER task_merge_operations_transition_on_update; \
         DROP TRIGGER task_merge_operations_journal_on_update;",
    )
    .execute(&mut *connection)
    .await
    .unwrap();
    let corrupted = sqlx::query(
        "UPDATE task_merge_operations \
         SET candidate_tree_oid = ?, preflight_source_commit_oid = ?, \
             state = 'rejected', failure_code = 'TARGET_WORKTREE_DIRTY', version = 2 \
         WHERE operation_id = ?",
    )
    .bind(CANDIDATE_TREE)
    .bind(PREFLIGHT_SOURCE)
    .bind(operation_id.to_string())
    .execute(&mut *connection)
    .await;
    assert!(corrupted.is_err());
}
