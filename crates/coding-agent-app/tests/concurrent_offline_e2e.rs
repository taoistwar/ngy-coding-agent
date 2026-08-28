#![cfg(feature = "test-support")]

mod support;

use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use coding_agent_app::{
    DeliveryCleanupAcceptanceOutcome, DeliveryCleanupReceiptDisposition,
    DeliveryMergeAcceptanceOutcome, DeliveryMergeReceiptDisposition, DeliveryPreflightBusyReason,
    RepositoryControlState,
};
use coding_agent_domain::{TaskStatus, TestStatus};
use coding_agent_store::{AttemptArtifactState, CleanupOperationState};
use futures_util::FutureExt as _;

use support::concurrent_e2e::{ConcurrentE2eFixture, wait_for_repository_control_settlement};

static E2E_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn repository_control_wait_crosses_terminal_visibility_window() {
    let observations = AtomicUsize::new(0);

    let settled = wait_for_repository_control_settlement(Duration::from_secs(1), || {
        if observations.fetch_add(1, Ordering::SeqCst) == 0 {
            RepositoryControlState::Busy
        } else {
            RepositoryControlState::Available
        }
    })
    .await;

    assert_eq!(settled, Some(RepositoryControlState::Available));
    assert_eq!(observations.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn task_reservation_serializes_real_delivery_merge_and_cleanup() {
    let _guard = E2E_LOCK.lock().await;
    let mut fixture = ConcurrentE2eFixture::new(1, 1, 1, 1).await;
    fixture.start_delivery_manager().await;

    let scenario = AssertUnwindSafe(run_reservation_delivery_competition(&fixture))
        .catch_unwind()
        .await;
    fixture.release_provision_pause();
    fixture.finish().await;
    if let Err(payload) = scenario {
        std::panic::resume_unwind(payload);
    }
}

async fn run_reservation_delivery_competition(fixture: &ConcurrentE2eFixture) {
    let delivery_task = fixture
        .enqueue_for_repository(0, &["produce the approved delivery source"])
        .await
        .pop()
        .expect("one delivery task");
    fixture.wait_for_blocked_role_loops(1).await;
    assert_eq!(
        fixture.artifact(delivery_task.id).await.state,
        AttemptArtifactState::Ready,
        "the production role loop starts only after durable readiness"
    );
    fixture.release_role_loops();
    let completed = fixture.wait_for_terminal(delivery_task.id).await;
    assert_eq!(completed.status, TaskStatus::Completed);
    assert_eq!(
        fixture
            .task_detail(delivery_task.id)
            .await
            .tests
            .expect("approved delivery task tests")
            .status,
        TestStatus::Passed
    );
    fixture.assert_repository_control_available(0);

    let (merge_operation_id, accept_command) =
        fixture.prepare_delivery_accept(delivery_task.id).await;

    fixture.arm_next_provision_pause();
    let merge_competitor = fixture
        .enqueue_for_repository(0, &["hold the repository while merge admission competes"])
        .await
        .pop()
        .expect("one merge competitor");
    fixture.wait_for_provision_pause().await;
    assert_eq!(
        fixture.artifact(merge_competitor.id).await.state,
        AttemptArtifactState::Reserved,
        "competition must occur after the reservation is durable but before readiness"
    );
    fixture.assert_repository_control_busy(0);
    let before_merge = fixture
        .delivery_side_effect_snapshot(delivery_task.id)
        .await;
    before_merge.assert_no_command_receipts();
    assert_eq!(
        fixture.accept_delivery_merge(accept_command.clone()).await,
        DeliveryMergeAcceptanceOutcome::Busy(DeliveryPreflightBusyReason::RepositoryBusy)
    );
    assert_eq!(
        fixture
            .delivery_side_effect_snapshot(delivery_task.id)
            .await,
        before_merge,
        "a merge admission that loses to a task reservation must be side-effect free"
    );

    fixture.release_provision_pause();
    assert_eq!(
        fixture.wait_for_terminal(merge_competitor.id).await.status,
        TaskStatus::Completed
    );
    fixture.assert_repository_control_available(0);
    let merge_acceptance = match fixture.accept_delivery_merge(accept_command).await {
        DeliveryMergeAcceptanceOutcome::Durable(acceptance) => acceptance,
        other => panic!("released merge admission must become durable, got {other:?}"),
    };
    assert_eq!(merge_acceptance.operation_id(), merge_operation_id);
    assert_eq!(
        merge_acceptance.receipt(),
        DeliveryMergeReceiptDisposition::Created
    );
    let merged = fixture.wait_for_delivery_merge(merge_operation_id).await;
    fixture
        .assert_exact_no_ff_delivery_merge(delivery_task.id, &merged)
        .await;

    // Cargo validation intentionally writes ignored `target/` output in the
    // approved source worktree. Remove only that fixture-owned subtree so the
    // production cleanup admission observes a genuinely clean worktree.
    fixture
        .clean_delivery_runtime_outputs(delivery_task.id)
        .await;
    let remove_request = fixture.delivery_remove_request(delivery_task.id).await;

    fixture.arm_next_provision_pause();
    let cleanup_competitor = fixture
        .enqueue_for_repository(0, &["hold the repository while cleanup admission competes"])
        .await
        .pop()
        .expect("one cleanup competitor");
    fixture.wait_for_provision_pause().await;
    assert_eq!(
        fixture.artifact(cleanup_competitor.id).await.state,
        AttemptArtifactState::Reserved
    );
    fixture.assert_repository_control_busy(0);
    let before_cleanup = fixture
        .delivery_side_effect_snapshot(delivery_task.id)
        .await;
    before_cleanup.assert_receipts(1, 0);
    assert_eq!(
        fixture
            .remove_delivery_worktree(remove_request.clone())
            .await,
        DeliveryCleanupAcceptanceOutcome::Busy(DeliveryPreflightBusyReason::RepositoryBusy)
    );
    assert_eq!(
        fixture
            .delivery_side_effect_snapshot(delivery_task.id)
            .await,
        before_cleanup,
        "cleanup admission that loses to a task reservation must write no receipt or Git bytes"
    );

    fixture.release_provision_pause();
    assert_eq!(
        fixture
            .wait_for_terminal(cleanup_competitor.id)
            .await
            .status,
        TaskStatus::Completed
    );
    fixture.assert_repository_control_available(0);
    let cleanup_acceptance = match fixture.remove_delivery_worktree(remove_request).await {
        DeliveryCleanupAcceptanceOutcome::Durable(acceptance) => acceptance,
        other => panic!("released cleanup admission must become durable, got {other:?}"),
    };
    assert_eq!(
        cleanup_acceptance.receipt(),
        DeliveryCleanupReceiptDisposition::Created
    );
    let cleanup = fixture
        .wait_for_delivery_cleanup(cleanup_acceptance.operation_id())
        .await;
    assert_eq!(cleanup.state, CleanupOperationState::Completed);
    let artifact = fixture.artifact(delivery_task.id).await;
    assert!(
        !artifact.worktree_path.as_path().exists(),
        "successful cleanup must remove the exact delivery worktree"
    );
    fixture
        .assert_delivery_cleanup_completed(delivery_task.id)
        .await;
    fixture.wait_for_repository_control_available(0).await;
    fixture.assert_no_live_process_trees();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_repository_real_worktrees_overlap_roles_but_serialize_control() {
    let _guard = E2E_LOCK.lock().await;
    let fixture = ConcurrentE2eFixture::new(2, 2, 1, 2).await;
    fixture.dirty_repository(0);
    let tasks = fixture
        .enqueue_for_repository(
            0,
            &[
                "change and validate the first isolated task",
                "change and validate the second isolated task",
            ],
        )
        .await;

    fixture.wait_for_blocked_role_loops(2).await;

    for task in &tasks {
        assert_eq!(fixture.task(task.id).await.status, TaskStatus::Running);
        assert_eq!(
            fixture.artifact(task.id).await.state,
            AttemptArtifactState::Ready,
            "the role barrier must only be reachable after durable worktree readiness"
        );
    }
    fixture.assert_distinct_isolated_artifacts(&tasks).await;
    fixture.assert_repository_control_available(0);
    assert_eq!(
        fixture.maximum_overlapping_control_operations(),
        1,
        "same-identity worktree control operations must be strictly serialized"
    );
    assert_eq!(
        fixture.maximum_overlapping_role_loops(),
        2,
        "both tasks must be inside the real post-ready role loop together"
    );
    fixture
        .assert_original_dirty_state_isolated(0, &tasks)
        .await;

    fixture.release_role_loops();
    for task in &tasks {
        let terminal = fixture.wait_for_terminal(task.id).await;
        assert_eq!(terminal.status, TaskStatus::Completed);
        let detail = fixture.task_detail(task.id).await;
        assert_eq!(
            detail.tests.expect("completed task tests").status,
            TestStatus::Passed,
            "each isolated worktree must execute a real Cargo test"
        );
    }
    fixture
        .assert_original_dirty_state_isolated(0, &tasks)
        .await;
    fixture.assert_no_live_process_trees();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn third_same_repository_waits_while_another_repository_skips_ahead() {
    let _guard = E2E_LOCK.lock().await;
    let fixture = ConcurrentE2eFixture::new(3, 2, 2, 3).await;
    let same_repository = fixture
        .enqueue_for_repository(
            0,
            &[
                "run same-repository task one",
                "run same-repository task two",
                "run same-repository task three",
            ],
        )
        .await;
    let other_repository = fixture
        .enqueue_for_repository(1, &["run the independent-repository task"])
        .await
        .pop()
        .expect("one independent-repository task");

    fixture.wait_for_blocked_role_loops(3).await;

    assert_eq!(
        fixture.task(same_repository[0].id).await.status,
        TaskStatus::Running
    );
    assert_eq!(
        fixture.task(same_repository[1].id).await.status,
        TaskStatus::Running
    );
    assert_eq!(
        fixture.task(same_repository[2].id).await.status,
        TaskStatus::Queued,
        "the third same-repository task must wait for a repository permit"
    );
    assert_eq!(
        fixture.task(other_repository.id).await.status,
        TaskStatus::Running,
        "a different repository must skip the repository-blocked queue entry"
    );
    assert_eq!(fixture.maximum_overlapping_role_loops(), 3);
    for task in [&same_repository[0], &same_repository[1], &other_repository] {
        assert_eq!(
            fixture.artifact(task.id).await.state,
            AttemptArtifactState::Ready
        );
    }

    for task in same_repository
        .iter()
        .chain(std::iter::once(&other_repository))
    {
        fixture.cancel(task.id).await;
    }
    fixture.release_role_loops();
    for task in same_repository
        .iter()
        .chain(std::iter::once(&other_repository))
    {
        let terminal = fixture.wait_for_terminal(task.id).await;
        assert_eq!(
            terminal.status,
            TaskStatus::Cancelled,
            "task {} did not honor deterministic E2E cleanup: {:?}",
            task.id,
            terminal.failure
        );
    }
    fixture.assert_no_live_process_trees();
    fixture.finish().await;
}
