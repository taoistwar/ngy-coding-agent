use super::*;

#[cfg(feature = "test-support")]
#[tokio::test]
async fn actor_reports_only_exact_active_map_ownership() {
    let fixture = running_hard_freeze_fixture("inspect exact active ownership").await;

    assert_eq!(
        fixture
            .manager
            .active_ownership(TaskId::new())
            .await
            .expect("query unrelated task ownership"),
        TaskActiveOwnership::Inactive
    );
    assert_eq!(
        fixture
            .manager
            .active_ownership(fixture.task.id)
            .await
            .expect("query running task ownership"),
        TaskActiveOwnership::Active {
            repository_id: fixture.repository.id,
            attempt: fixture.task.attempt,
        }
    );

    fixture.runner.release.notify_one();
    wait_for_status(&fixture.store, fixture.task.id, TaskStatus::Failed).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if fixture
                .manager
                .active_ownership(fixture.task.id)
                .await
                .is_ok_and(|ownership| ownership == TaskActiveOwnership::Inactive)
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal projection releases exact active ownership");
}
