#![cfg(feature = "test-support")]

mod support;

use coding_agent_domain::{TaskStatus, TestStatus};
use coding_agent_store::AttemptArtifactState;

use support::concurrent_e2e::ConcurrentE2eFixture;

static E2E_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
}
