#![cfg(feature = "test-support")]

// The focused fixtures expose exact durable phase boundaries without widening
// production APIs. The child-process cases below provide the matching real
// Git/Cargo/provider crash and side-effect proof.

// This crash matrix consumes only the durable-boundary subset of the shared
// cleanup fixture surface.
#[allow(dead_code)]
mod delivery_cleanup_support;
#[allow(dead_code, unused_imports)]
mod delivery_merge_support;
mod support;

use std::sync::Arc;

use coding_agent_app::{
    DeliveryMergeReceiptDisposition, DeliveryOperationRecoveryOutcome, InstanceLock,
    ProcessDeliveryProcessFault, ProcessDeliveryProviderScenario, RepositoryControlState,
    StoreWriterFaultPoint, StoreWriterFaultSpec, StoreWriterOperationKind,
    StoreWriterTestController,
};
use coding_agent_store::{CleanupOperationState, DeliverySourceState, MergeOperationState};

use delivery_cleanup_support::{CleanupCall, CleanupFault, CleanupStage, DeliveryCleanupFixture};
use delivery_merge_support::{DeliveryMergeFixture, LiveCall, LiveFault, LiveStage};
use support::delivery::process::{
    ProcessDeliveryFixture, ShutdownFenceObservation, observe_shutdown_fence,
};

static PROCESS_E2E_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn shutdown_fence_observation_reads_descriptor_after_async_listener_probe() {
    let root = tempfile::tempdir().expect("create shutdown observation fixture");
    let descriptor = root.path().join("instance.json");
    std::fs::write(&descriptor, b"published").expect("publish fixture descriptor");

    let stale_descriptor_closed = !descriptor.exists();
    let listener_closed = async {
        std::fs::remove_file(&descriptor).expect("unpublish descriptor during listener probe");
        true
    }
    .await;
    assert_eq!(
        ShutdownFenceObservation {
            descriptor_closed: stale_descriptor_closed,
            listener_closed,
        },
        ShutdownFenceObservation {
            descriptor_closed: false,
            listener_closed: true,
        },
        "the old sampling order can combine a stale descriptor observation with a closed listener"
    );

    std::fs::write(&descriptor, b"published").expect("republish fixture descriptor");
    let observation = observe_shutdown_fence(&descriptor, async {
        std::fs::remove_file(&descriptor).expect("unpublish descriptor during listener probe");
        true
    })
    .await;
    assert_eq!(
        observation,
        ShutdownFenceObservation {
            descriptor_closed: true,
            listener_closed: true,
        },
        "the descriptor is sampled only after the async listener probe completes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn production_process_keeps_offline_provider_alive_until_task_is_approved() {
    let _guard = PROCESS_E2E_LOCK.lock().await;
    let mut fixture = ProcessDeliveryFixture::new();
    let session = fixture.start_ready(None).await;

    let (task_id, _) = fixture.create_approved_task(&session).await;

    assert_eq!(fixture.git_line(&["rev-parse", "HEAD"]), fixture.base_head);
    assert_eq!(
        fixture.git_line(&["rev-list", "--count", "--all"]),
        "1",
        "approval must not create the controlled-delivery source or merge commit"
    );
    assert!(
        fixture
            .persisted_source_state_optional(&task_id)
            .await
            .is_none(),
        "approval alone must not create a delivery source"
    );
    assert_eq!(
        fixture.source_ignored_untracked(&task_id).await,
        b"target/\0",
        "offline Cargo validation leaves only the fixture-owned ignored target subtree"
    );

    fixture.shutdown(&session).await;
    let root = fixture.finish();
    assert!(
        !root.exists(),
        "the process fixture removes its private root"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_delivery_cleanup_unproven_shutdown_keeps_primary_lock_and_delivery_owner() {
    let _guard = PROCESS_E2E_LOCK.lock().await;
    let mut fixture = ProcessDeliveryFixture::new_with_scenario_and_fault(
        ProcessDeliveryProviderScenario::Approve,
        ProcessDeliveryProcessFault::AuthenticatePreflightFirstChildCleanupFailure,
    );
    let session = fixture.start_ready(None).await;
    let (task_id, _) = fixture.create_approved_task(&session).await;
    let source_ref = fixture.source_ref(&task_id).await;
    let source_worktree = fixture.source_worktree_path(&task_id).await;
    let source_bytes = std::fs::read(source_worktree.join("src/lib.rs"))
        .expect("read approved source before process fault");
    let target_head = fixture.git_line(&["rev-parse", "HEAD"]);
    let target_status = fixture.git_status();

    let (status, first) = fixture.post_preflight(&session, &task_id).await;
    assert_eq!(status, http::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(first["code"], "PROCESS_TREE_CLEANUP_FAILED");
    assert_eq!(first["retryable"], false);
    assert_eq!(fixture.receipt_count(&task_id, "preflight").await, 0);
    assert!(
        fixture
            .persisted_source_state_optional(&task_id)
            .await
            .is_none(),
        "faulted authentication cannot create a delivery source"
    );

    // Preflight's cleanup-unproven terminal path intentionally retains the
    // exact unpoisoned repository lease and its global Git slot. This differs
    // from the generic retained-poison coordinator path: ordinary delivery
    // admission must observe Busy until process exit, while reconciliation is
    // not manufactured from a known failure classification.
    let (status, second) = fixture.post_preflight(&session, &task_id).await;
    assert_eq!(status, http::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(second["code"], "REPOSITORY_CONTROL_BUSY");
    assert_eq!(second["retryable"], true);
    assert_eq!(fixture.receipt_count(&task_id, "preflight").await, 0);
    let projection = fixture.delivery_projection(&session, &task_id).await;
    assert_eq!(projection["eligibility"], "unavailable");
    assert!(
        projection["reasons"]
            .as_array()
            .expect("delivery projection reasons are an array")
            .iter()
            .any(|reason| reason == "repository_busy"),
        "retained delivery ownership is visible in the delivery projection: {projection}"
    );
    assert_eq!(fixture.git_line(&["rev-parse", "HEAD"]), target_head);
    assert_eq!(fixture.git_status(), target_status);
    assert_eq!(
        fixture.git_ref(&source_ref).as_deref(),
        Some(fixture.base_head.as_str())
    );
    assert_eq!(fixture.git_line(&["rev-list", "--count", "--all"]), "1");
    assert_eq!(
        std::fs::read(source_worktree.join("src/lib.rs"))
            .expect("read approved source after process fault"),
        source_bytes,
        "faulted authentication cannot alter reviewed source bytes"
    );

    fixture.request_quit(&session).await;
    fixture.wait_for_shutdown_fence(&session).await;
    assert!(
        InstanceLock::try_acquire(fixture.instance_lock_path().as_path())
            .expect("probe primary lock while delivery ownership is retained")
            .is_none(),
        "the live primary lock remains the final process-exit fence"
    );
    let retained_primary_pid = fixture.child_pid();
    fixture.hard_kill().await;

    fixture.clear_process_fault();
    let recovered = fixture.start_ready(None).await;
    let recovered_projection = fixture.delivery_projection(&recovered, &task_id).await;
    assert_eq!(
        recovered_projection["eligibility"], "eligible",
        "a clean restart clears only the dead process-local retained owner: {recovered_projection}; killed_primary_pid={retained_primary_pid}"
    );
    assert_eq!(recovered_projection["reasons"], serde_json::json!([]));
    assert_eq!(fixture.receipt_count(&task_id, "preflight").await, 0);
    assert_eq!(fixture.git_line(&["rev-parse", "HEAD"]), target_head);
    assert_eq!(fixture.git_status(), target_status);

    fixture.shutdown(&recovered).await;
    let root = fixture.finish();
    assert!(
        !root.exists(),
        "cleanup-unproven shutdown fixture removes its private root after the clean restart"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_process_complete_merge_reply_loss_preserves_cleanup_authority() {
    let _guard = PROCESS_E2E_LOCK.lock().await;
    let mut fixture = ProcessDeliveryFixture::new();
    let session = fixture
        .start_ready(Some(StoreWriterOperationKind::CompleteMerge))
        .await;
    let (task_id, _) = fixture.create_approved_task(&session).await;
    let source_ref = fixture.source_ref(&task_id).await;
    let ready = fixture.preflight_ready(&session, &task_id).await;
    fixture.accept_merge(&session, &task_id, &ready).await;
    fixture.wait_for_store_pause().await;
    fixture.hard_kill().await;
    assert_eq!(fixture.persisted_merge_state(&task_id).await, "merged");
    let merge_head = fixture.git_line(&["rev-parse", "HEAD"]);

    let session = fixture
        .start_ready(Some(StoreWriterOperationKind::AcceptWorktreeCleanup))
        .await;
    fixture.wait_merge_state(&session, &task_id, "merged").await;
    let worktree_body = fixture.worktree_cleanup_body(&session, &task_id).await;
    let (status, body) = fixture
        .post_worktree_cleanup(&session, &task_id, &worktree_body)
        .await;
    assert_eq!(status, http::StatusCode::CONFLICT);
    assert_eq!(body["code"], "TARGET_WORKTREE_DIRTY");
    assert_eq!(body["retryable"], false);
    fixture.assert_ignored_cargo_output_was_the_dirty_predicate();
    assert_eq!(fixture.receipt_count(&task_id, "remove_worktree").await, 0);
    assert!(
        fixture
            .persisted_cleanup_state_optional(&task_id, "remove_worktree")
            .await
            .is_none(),
        "dirty first admission writes no cleanup operation"
    );
    fixture.clean_fixture_cargo_outputs(&task_id).await;
    let request = fixture.spawn_mutation(
        session.clone(),
        format!("/api/tasks/{task_id}/cleanup/worktree"),
        worktree_body,
    );
    if let Err(error) = fixture.wait_for_store_pause_or_mutation(request).await {
        let diagnostic = fixture.diagnostic_snapshot(&session, &task_id).await;
        panic!(
            "lost CompleteMerge reply broke cleanup authority: {error}; diagnostic={diagnostic}"
        );
    }
    fixture.hard_kill().await;
    assert_eq!(
        fixture
            .persisted_cleanup_state(&task_id, "remove_worktree")
            .await,
        "unlock_pending"
    );
    assert_eq!(fixture.receipt_count(&task_id, "remove_worktree").await, 1);

    let session = fixture.start_ready(None).await;
    fixture
        .wait_worktree_state(&session, &task_id, "removed")
        .await;
    assert_eq!(fixture.git_line(&["rev-parse", "HEAD"]), merge_head);
    assert!(fixture.git_ref(&source_ref).is_some());
    fixture.shutdown(&session).await;
    let root = fixture.finish();
    assert!(
        !root.exists(),
        "focused cleanup process fixture removes its private root"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_process_hard_kill_restarts_converge_each_source_merge_and_cleanup_phase_once() {
    let _guard = PROCESS_E2E_LOCK.lock().await;
    let mut fixture = ProcessDeliveryFixture::new();

    let session = fixture
        .start_ready(Some(StoreWriterOperationKind::CreateDeliverySource))
        .await;
    let (task_id, _) = fixture.create_approved_task(&session).await;
    let source_ref = fixture.source_ref(&task_id).await;
    let source_worktree = fixture.source_worktree_path(&task_id).await;
    assert_eq!(
        fixture.git_ref(&source_ref).as_deref(),
        Some(fixture.base_head.as_str())
    );
    assert_eq!(fixture.git_line(&["rev-list", "--count", "--all"]), "1");

    let ready = fixture.preflight_ready(&session, &task_id).await;
    fixture.accept_merge(&session, &task_id, &ready).await;
    fixture.wait_for_store_pause().await;
    fixture.hard_kill().await;
    assert_eq!(
        fixture.persisted_source_state(&task_id).await,
        "object_pending"
    );
    assert_eq!(fixture.persisted_merge_state(&task_id).await, "accepted");
    assert_eq!(
        fixture.git_ref(&source_ref).as_deref(),
        Some(fixture.base_head.as_str())
    );
    assert_eq!(fixture.git_line(&["rev-parse", "HEAD"]), fixture.base_head);

    fixture
        .start_until_store_pause(StoreWriterOperationKind::AdvanceDeliverySourceObject)
        .await;
    fixture.hard_kill().await;
    assert_eq!(
        fixture.persisted_source_state(&task_id).await,
        "commit_pending"
    );
    assert_eq!(
        fixture.git_ref(&source_ref).as_deref(),
        Some(fixture.base_head.as_str())
    );
    assert_eq!(fixture.git_line(&["rev-parse", "HEAD"]), fixture.base_head);

    fixture
        .start_until_store_pause(StoreWriterOperationKind::EnterMergePending)
        .await;
    fixture.hard_kill().await;
    assert_eq!(fixture.persisted_source_state(&task_id).await, "committed");
    assert_eq!(
        fixture.persisted_merge_state(&task_id).await,
        "merge_pending"
    );
    let source_commit = fixture
        .git_ref(&source_ref)
        .expect("source commit is reachable after CommitPending recovery");
    assert_ne!(source_commit, fixture.base_head);
    assert_eq!(fixture.git_line(&["rev-parse", "HEAD"]), fixture.base_head);

    fixture
        .start_until_store_pause(StoreWriterOperationKind::CompleteMerge)
        .await;
    fixture.hard_kill().await;
    assert_eq!(fixture.persisted_merge_state(&task_id).await, "merged");
    let merge_commit = fixture.git_line(&["rev-parse", "HEAD"]);
    assert_eq!(
        fixture.git_line(&["show", "-s", "--format=%P", "HEAD"]),
        format!("{} {source_commit}", fixture.base_head),
        "the one real merge side effect is an exact no-ff two-parent commit"
    );
    assert_eq!(fixture.git_line(&["rev-list", "--count", "HEAD"]), "3");
    assert!(fixture.git_status().is_empty());
    assert_eq!(
        std::fs::read_to_string(fixture.repository_path.join("src/lib.rs"))
            .expect("read merged process target"),
        "pub fn fixture_value() -> u32 { 43 }\n// approved offline delivery\n"
    );

    fixture.clean_fixture_cargo_outputs(&task_id).await;

    let session = fixture
        .start_ready(Some(StoreWriterOperationKind::AcceptWorktreeCleanup))
        .await;
    fixture.wait_merge_state(&session, &task_id, "merged").await;
    assert_eq!(fixture.git_line(&["rev-parse", "HEAD"]), merge_commit);
    let worktree_body = fixture.worktree_cleanup_body(&session, &task_id).await;
    let worktree_request = fixture.spawn_mutation(
        session.clone(),
        format!("/api/tasks/{task_id}/cleanup/worktree"),
        worktree_body,
    );
    if let Err(error) = fixture
        .wait_for_store_pause_or_mutation(worktree_request)
        .await
    {
        let diagnostic = fixture.diagnostic_snapshot(&session, &task_id).await;
        panic!("worktree cleanup acceptance failed: {error}; diagnostic={diagnostic}");
    }
    fixture.hard_kill().await;
    assert_eq!(
        fixture
            .persisted_cleanup_state(&task_id, "remove_worktree")
            .await,
        "unlock_pending"
    );
    assert!(source_worktree.exists());
    assert_eq!(fixture.receipt_count(&task_id, "remove_worktree").await, 1);

    fixture
        .start_until_store_pause(StoreWriterOperationKind::RecordWorktreeUnlocked)
        .await;
    fixture.hard_kill().await;
    assert_eq!(
        fixture
            .persisted_cleanup_state(&task_id, "remove_worktree")
            .await,
        "unlocked_pending_remove"
    );
    assert!(source_worktree.exists());

    fixture
        .start_until_store_pause(StoreWriterOperationKind::EnterWorktreeRemovePending)
        .await;
    fixture.hard_kill().await;
    assert_eq!(
        fixture
            .persisted_cleanup_state(&task_id, "remove_worktree")
            .await,
        "remove_pending"
    );
    assert!(source_worktree.exists());

    fixture
        .start_until_store_pause(StoreWriterOperationKind::CompleteWorktreeCleanup)
        .await;
    fixture.hard_kill().await;
    assert_eq!(
        fixture
            .persisted_cleanup_state(&task_id, "remove_worktree")
            .await,
        "completed"
    );
    assert!(!source_worktree.exists());

    let session = fixture
        .start_ready(Some(StoreWriterOperationKind::AcceptBranchCleanup))
        .await;
    fixture
        .wait_worktree_state(&session, &task_id, "removed")
        .await;
    let branch_body = fixture.branch_cleanup_body(&session, &task_id).await;
    let branch_request = fixture.spawn_mutation(
        session.clone(),
        format!("/api/tasks/{task_id}/cleanup/branch"),
        branch_body,
    );
    if let Err(error) = fixture
        .wait_for_store_pause_or_mutation(branch_request)
        .await
    {
        let diagnostic = fixture.diagnostic_snapshot(&session, &task_id).await;
        panic!("branch cleanup acceptance failed: {error}; diagnostic={diagnostic}");
    }
    fixture.hard_kill().await;
    assert_eq!(
        fixture
            .persisted_cleanup_state(&task_id, "delete_branch")
            .await,
        "delete_pending"
    );
    assert_eq!(
        fixture.git_ref(&source_ref).as_deref(),
        Some(source_commit.as_str())
    );
    assert_eq!(fixture.receipt_count(&task_id, "delete_branch").await, 1);

    fixture
        .start_until_store_pause(StoreWriterOperationKind::CompleteBranchCleanup)
        .await;
    fixture.hard_kill().await;
    assert_eq!(
        fixture
            .persisted_cleanup_state(&task_id, "delete_branch")
            .await,
        "completed"
    );
    assert_eq!(fixture.git_ref(&source_ref), None);

    let session = fixture.start_ready(None).await;
    fixture
        .wait_branch_state(&session, &task_id, "deleted")
        .await;
    assert_eq!(fixture.git_line(&["rev-parse", "HEAD"]), merge_commit);
    assert_eq!(fixture.git_line(&["rev-list", "--count", "HEAD"]), "3");
    assert!(fixture.git_status().is_empty());
    assert_eq!(fixture.receipt_count(&task_id, "accept_merge").await, 1);
    assert_eq!(fixture.receipt_count(&task_id, "remove_worktree").await, 1);
    assert_eq!(fixture.receipt_count(&task_id, "delete_branch").await, 1);
    for (entity, state, expected_count) in [
        ("delivery_source", "object_pending", 1),
        ("delivery_source", "commit_pending", 1),
        ("delivery_source", "committed", 1),
        ("merge_operation", "merge_pending", 1),
        ("merge_operation", "merged", 1),
        ("cleanup_operation", "unlock_pending", 1),
        ("cleanup_operation", "unlocked_pending_remove", 1),
        ("cleanup_operation", "remove_pending", 1),
        ("cleanup_operation", "delete_pending", 1),
        ("cleanup_operation", "completed", 2),
    ] {
        assert_eq!(
            fixture.transition_count(&task_id, entity, state).await,
            expected_count,
            "hard-kill recovery must persist the exact {entity}->{state} transition count"
        );
    }

    fixture.shutdown(&session).await;
    let root = fixture.finish();
    assert!(
        !root.exists(),
        "hard-kill matrix removes every private artifact"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_process_abort_pending_hard_kill_runs_one_exact_git_abort_after_restart() {
    let _guard = PROCESS_E2E_LOCK.lock().await;
    let mut fixture =
        ProcessDeliveryFixture::new_with_scenario(ProcessDeliveryProviderScenario::RuntimeConflict);
    let session = fixture
        .start_ready(Some(StoreWriterOperationKind::BeginMergeAbort))
        .await;
    let (task_id, _) = fixture.create_approved_task(&session).await;
    let source_ref = fixture.source_ref(&task_id).await;
    let target_head = fixture.commit_runtime_conflict_target();
    let ready = fixture.preflight_ready(&session, &task_id).await;

    fixture.accept_merge(&session, &task_id, &ready).await;
    fixture.wait_for_store_pause().await;
    fixture.assert_runtime_conflict_attribute_restored();
    assert_eq!(fixture.git_line(&["rev-parse", "HEAD"]), target_head);
    let source_commit = fixture
        .git_ref(&source_ref)
        .expect("runtime-conflict source ref is committed");
    assert_eq!(
        fixture.git_line(&["rev-parse", "MERGE_HEAD"]),
        source_commit,
        "the real fixed merge child leaves its exact conflict scene"
    );
    fixture.hard_kill().await;

    assert_eq!(
        fixture.persisted_merge_state(&task_id).await,
        "abort_pending"
    );
    let abort_child_receipt = fixture
        .persisted_abort_child_receipt_id(&task_id)
        .await
        .expect("AbortPending durably binds the one conflicting merge child");
    assert_eq!(fixture.abort_spawn_count(), 0);
    assert_eq!(
        fixture
            .transition_count(&task_id, "merge_operation", "abort_pending")
            .await,
        1
    );

    fixture
        .start_until_store_pause(StoreWriterOperationKind::CompleteMergeAbort)
        .await;
    assert_eq!(fixture.persisted_merge_state(&task_id).await, "conflict");
    assert_eq!(
        fixture.persisted_abort_child_receipt_id(&task_id).await,
        Some(abort_child_receipt.clone())
    );
    assert_eq!(fixture.abort_spawn_count(), 1);
    assert_eq!(fixture.git_line(&["rev-parse", "HEAD"]), target_head);
    assert_eq!(
        fixture.git_ref(&source_ref).as_deref(),
        Some(source_commit.as_str())
    );
    fixture.assert_runtime_conflict_target_restored();
    assert!(fixture.git_status().is_empty());
    fixture.hard_kill().await;

    let session = fixture.start_ready(None).await;
    fixture
        .wait_merge_state(&session, &task_id, "conflict")
        .await;
    assert_eq!(fixture.abort_spawn_count(), 1);
    assert_eq!(
        fixture.persisted_abort_child_receipt_id(&task_id).await,
        Some(abort_child_receipt)
    );
    assert_eq!(fixture.receipt_count(&task_id, "accept_merge").await, 1);
    assert_eq!(
        fixture
            .transition_count(&task_id, "merge_operation", "abort_pending")
            .await,
        1
    );
    assert_eq!(
        fixture
            .transition_count(&task_id, "merge_operation", "conflict")
            .await,
        1
    );
    assert_eq!(fixture.git_line(&["rev-parse", "HEAD"]), target_head);
    assert_eq!(
        fixture.git_ref(&source_ref).as_deref(),
        Some(source_commit.as_str())
    );
    fixture.assert_runtime_conflict_target_restored();
    assert!(fixture.git_status().is_empty());

    fixture.shutdown(&session).await;
    let root = fixture.finish();
    assert!(
        !root.exists(),
        "AbortPending process fixture removes its private root"
    );
}

#[tokio::test]
async fn source_and_merge_pending_states_restart_from_exact_durable_phase() {
    for (stage, source_state, operation_state) in [
        (
            LiveStage::SourceObject,
            Some(DeliverySourceState::ObjectPending),
            MergeOperationState::Accepted,
        ),
        (
            LiveStage::SourceCommit,
            Some(DeliverySourceState::CommitPending),
            MergeOperationState::Accepted,
        ),
        (
            LiveStage::ActualMerge,
            None,
            MergeOperationState::MergePending,
        ),
    ] {
        let mut fixture = DeliveryMergeFixture::new(None).await;
        fixture
            .live_runtime
            .fail_once(stage, LiveFault::Unavailable);
        let prepared = fixture.prepare_accept().await;
        let accepted = fixture.accept(&prepared).await;
        assert_eq!(accepted.receipt(), DeliveryMergeReceiptDisposition::Created);
        if let Some(source_state) = source_state {
            fixture
                .wait_source_state(prepared.task.id, source_state)
                .await;
        }
        fixture
            .wait_operation_state(prepared.operation_id, operation_state)
            .await;
        fixture
            .wait_repository_state(RepositoryControlState::Available)
            .await;

        fixture.restart_manager().await;
        assert_eq!(
            fixture
                .manager()
                .recover_operation_for_test(prepared.operation_id)
                .await
                .expect("restarted delivery manager remains open"),
            DeliveryOperationRecoveryOutcome::Converged
        );
        fixture
            .wait_operation_state(prepared.operation_id, MergeOperationState::Merged)
            .await;
        fixture
            .wait_source_state(prepared.task.id, DeliverySourceState::Committed)
            .await;
        fixture.finish().await;
    }
}

#[tokio::test]
async fn abort_pending_restart_reuses_the_one_durable_abort_child() {
    let mut fixture = DeliveryMergeFixture::new(None).await;
    fixture.live_runtime.use_conflict();
    fixture
        .live_runtime
        .fail_once(LiveStage::Abort, LiveFault::Unavailable);
    let prepared = fixture.prepare_accept().await;
    fixture.accept(&prepared).await;
    let pending = fixture
        .wait_operation_state(prepared.operation_id, MergeOperationState::AbortPending)
        .await;
    let child_receipt = pending
        .abort_child_receipt_id
        .expect("abort pending persists one exact child receipt");

    fixture
        .wait_repository_state(RepositoryControlState::Available)
        .await;
    fixture.restart_manager().await;
    assert_eq!(
        fixture
            .manager()
            .recover_operation_for_test(prepared.operation_id)
            .await
            .expect("restarted abort manager remains open"),
        DeliveryOperationRecoveryOutcome::Converged
    );
    let conflict = fixture
        .wait_operation_state(prepared.operation_id, MergeOperationState::Conflict)
        .await;
    assert_eq!(conflict.abort_child_receipt_id, Some(child_receipt));
    fixture.finish().await;
}

#[tokio::test]
async fn merge_completion_reply_loss_replays_store_receipt_without_second_merge() {
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
            operation: Some(StoreWriterOperationKind::CompleteMerge),
            count: 1,
        }])
        .expect("valid merge reply-loss script"),
    );
    let fixture = DeliveryMergeFixture::new(Some(controller.clone())).await;
    let prepared = fixture.prepare_accept().await;
    fixture.accept(&prepared).await;
    fixture
        .wait_operation_state(prepared.operation_id, MergeOperationState::Merged)
        .await;

    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::FailAfterCommitBeforeReply,
            StoreWriterOperationKind::CompleteMerge,
        ),
        1
    );
    assert_eq!(
        fixture
            .live_runtime
            .calls()
            .into_iter()
            .filter(|call| *call == LiveCall::ActualMerge)
            .count(),
        1,
        "a lost StoreWriter reply must not repeat the merge side effect"
    );
    fixture.finish().await;
}

#[tokio::test]
async fn every_merge_phase_store_reply_loss_converges_without_repeating_side_effect() {
    for (operation, conflict, side_effect) in [
        (
            StoreWriterOperationKind::CreateDeliverySource,
            false,
            LiveCall::SourceObject,
        ),
        (
            StoreWriterOperationKind::AdvanceDeliverySourceObject,
            false,
            LiveCall::SourceCommit,
        ),
        (
            StoreWriterOperationKind::EnterMergePending,
            false,
            LiveCall::ActualMerge,
        ),
        (
            StoreWriterOperationKind::BeginMergeAbort,
            true,
            LiveCall::Abort,
        ),
    ] {
        let controller = Arc::new(
            StoreWriterTestController::try_new([StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
                operation: Some(operation),
                count: 1,
            }])
            .expect("valid merge phase reply-loss script"),
        );
        let mut fixture = DeliveryMergeFixture::new(Some(controller.clone())).await;
        if conflict {
            fixture.live_runtime.use_conflict();
        }
        let prepared = fixture.prepare_accept().await;
        fixture.accept(&prepared).await;
        let expected = if conflict {
            MergeOperationState::Conflict
        } else {
            MergeOperationState::Merged
        };
        fixture
            .wait_operation_state(prepared.operation_id, expected)
            .await;

        assert_eq!(
            controller.hit_count(StoreWriterFaultPoint::FailAfterCommitBeforeReply, operation),
            1,
            "the exact StoreWriter boundary must be exercised once"
        );
        assert_eq!(
            fixture
                .live_runtime
                .calls()
                .into_iter()
                .filter(|call| *call == side_effect)
                .count(),
            1,
            "query-first reply-loss recovery must not repeat {side_effect:?}"
        );

        fixture.restart_manager().await;
        assert_eq!(
            fixture
                .manager()
                .recover_operation_for_test(prepared.operation_id)
                .await
                .expect("post-reply-loss restart remains open"),
            DeliveryOperationRecoveryOutcome::Converged
        );
        assert_eq!(
            fixture
                .live_runtime
                .calls()
                .into_iter()
                .filter(|call| *call == side_effect)
                .count(),
            1,
            "restart must preserve at-most-once {side_effect:?}"
        );
        fixture.finish().await;
    }
}

#[tokio::test]
async fn unlock_remove_and_delete_pending_states_restart_to_completion() {
    for (stage, pending) in [
        (CleanupStage::Unlock, CleanupOperationState::UnlockPending),
        (
            CleanupStage::EnterRemove,
            CleanupOperationState::UnlockedPendingRemove,
        ),
        (CleanupStage::Remove, CleanupOperationState::RemovePending),
    ] {
        let mut fixture = DeliveryCleanupFixture::new(None).await;
        fixture.runtime.fail_once(stage, CleanupFault::Unavailable);
        let accepted = fixture.remove().await;
        fixture
            .wait_operation_state(accepted.operation_id(), pending)
            .await;
        fixture
            .wait_repository_state(RepositoryControlState::Available)
            .await;
        fixture.restart_manager().await;
        assert_eq!(
            fixture
                .manager()
                .recover_operation_for_test(accepted.operation_id())
                .await
                .expect("restarted worktree cleanup manager remains open"),
            DeliveryOperationRecoveryOutcome::Converged
        );
        fixture
            .wait_operation_state(accepted.operation_id(), CleanupOperationState::Completed)
            .await;
        fixture.finish().await;
    }

    let mut fixture = DeliveryCleanupFixture::new(None).await;
    let worktree = fixture.remove().await;
    fixture
        .wait_operation_state(worktree.operation_id(), CleanupOperationState::Completed)
        .await;
    fixture
        .wait_repository_state(RepositoryControlState::Available)
        .await;
    fixture
        .runtime
        .fail_once(CleanupStage::Delete, CleanupFault::Unavailable);
    let branch = fixture.delete().await;
    fixture
        .wait_operation_state(branch.operation_id(), CleanupOperationState::DeletePending)
        .await;
    fixture
        .wait_repository_state(RepositoryControlState::Available)
        .await;
    fixture.restart_manager().await;
    assert_eq!(
        fixture
            .manager()
            .recover_operation_for_test(branch.operation_id())
            .await
            .expect("restarted branch cleanup manager remains open"),
        DeliveryOperationRecoveryOutcome::Converged
    );
    fixture
        .wait_operation_state(branch.operation_id(), CleanupOperationState::Completed)
        .await;
    fixture.finish().await;
}

#[tokio::test]
async fn cleanup_completion_reply_loss_does_not_repeat_remove_or_delete() {
    let controller = Arc::new(
        StoreWriterTestController::try_new([
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
                operation: Some(StoreWriterOperationKind::CompleteWorktreeCleanup),
                count: 1,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
                operation: Some(StoreWriterOperationKind::CompleteBranchCleanup),
                count: 1,
            },
        ])
        .expect("valid cleanup reply-loss script"),
    );
    let fixture = DeliveryCleanupFixture::new(Some(controller.clone())).await;
    let worktree = fixture.remove().await;
    fixture
        .wait_operation_state(worktree.operation_id(), CleanupOperationState::Completed)
        .await;
    fixture
        .wait_repository_state(RepositoryControlState::Available)
        .await;
    let branch = fixture.delete().await;
    fixture
        .wait_operation_state(branch.operation_id(), CleanupOperationState::Completed)
        .await;

    for operation in [
        StoreWriterOperationKind::CompleteWorktreeCleanup,
        StoreWriterOperationKind::CompleteBranchCleanup,
    ] {
        assert_eq!(
            controller.hit_count(StoreWriterFaultPoint::FailAfterCommitBeforeReply, operation),
            1
        );
    }
    assert_eq!(
        count_cleanup_calls(&fixture, |call| matches!(call, CleanupCall::Remove(_))),
        1
    );
    assert_eq!(
        count_cleanup_calls(&fixture, |call| matches!(call, CleanupCall::Delete(_))),
        1
    );
    fixture.finish().await;
}

#[tokio::test]
async fn every_cleanup_phase_store_reply_loss_converges_without_repeating_side_effect() {
    for (operation, side_effect) in [
        (
            StoreWriterOperationKind::AcceptWorktreeCleanup,
            CleanupStage::Unlock,
        ),
        (
            StoreWriterOperationKind::RecordWorktreeUnlocked,
            CleanupStage::Unlock,
        ),
        (
            StoreWriterOperationKind::EnterWorktreeRemovePending,
            CleanupStage::Remove,
        ),
    ] {
        let controller = Arc::new(
            StoreWriterTestController::try_new([StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
                operation: Some(operation),
                count: 1,
            }])
            .expect("valid worktree phase reply-loss script"),
        );
        let mut fixture = DeliveryCleanupFixture::new(Some(controller.clone())).await;
        let accepted = fixture.remove().await;
        fixture
            .wait_operation_state(accepted.operation_id(), CleanupOperationState::Completed)
            .await;

        assert_eq!(
            controller.hit_count(StoreWriterFaultPoint::FailAfterCommitBeforeReply, operation),
            1
        );
        assert_eq!(cleanup_side_effect_count(&fixture, side_effect), 1);
        fixture.restart_manager().await;
        assert_eq!(
            fixture
                .manager()
                .recover_operation_for_test(accepted.operation_id())
                .await
                .expect("post-worktree-reply-loss restart remains open"),
            DeliveryOperationRecoveryOutcome::Converged
        );
        assert_eq!(cleanup_side_effect_count(&fixture, side_effect), 1);
        fixture.finish().await;
    }

    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
            operation: Some(StoreWriterOperationKind::AcceptBranchCleanup),
            count: 1,
        }])
        .expect("valid branch acceptance reply-loss script"),
    );
    let mut fixture = DeliveryCleanupFixture::new(Some(controller.clone())).await;
    let worktree = fixture.remove().await;
    fixture
        .wait_operation_state(worktree.operation_id(), CleanupOperationState::Completed)
        .await;
    fixture
        .wait_repository_state(RepositoryControlState::Available)
        .await;
    let branch = fixture.delete().await;
    fixture
        .wait_operation_state(branch.operation_id(), CleanupOperationState::Completed)
        .await;
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::FailAfterCommitBeforeReply,
            StoreWriterOperationKind::AcceptBranchCleanup,
        ),
        1
    );
    assert_eq!(cleanup_side_effect_count(&fixture, CleanupStage::Delete), 1);
    fixture.restart_manager().await;
    assert_eq!(
        fixture
            .manager()
            .recover_operation_for_test(branch.operation_id())
            .await
            .expect("post-branch-reply-loss restart remains open"),
        DeliveryOperationRecoveryOutcome::Converged
    );
    assert_eq!(cleanup_side_effect_count(&fixture, CleanupStage::Delete), 1);
    fixture.finish().await;
}

fn count_cleanup_calls(
    fixture: &DeliveryCleanupFixture,
    predicate: impl Fn(&CleanupCall) -> bool,
) -> usize {
    fixture
        .runtime
        .calls()
        .iter()
        .filter(|call| predicate(call))
        .count()
}

fn cleanup_side_effect_count(fixture: &DeliveryCleanupFixture, stage: CleanupStage) -> usize {
    count_cleanup_calls(fixture, |call| match stage {
        CleanupStage::Unlock => matches!(call, CleanupCall::Unlock(_)),
        CleanupStage::Remove => matches!(call, CleanupCall::Remove(_)),
        CleanupStage::Delete => matches!(call, CleanupCall::Delete(_)),
        _ => panic!("{stage:?} is not a cleanup side-effect stage"),
    })
}
