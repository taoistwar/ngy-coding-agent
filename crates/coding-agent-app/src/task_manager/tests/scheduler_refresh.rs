use super::*;

#[tokio::test]
async fn stale_service_publish_does_not_ack_a_newer_storage_generation() {
    let temp_dir = tempfile::tempdir().expect("create scheduler refresh fixture");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open scheduler refresh store");
    store
        .migrate()
        .await
        .expect("migrate scheduler refresh store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn scheduler refresh dispatcher");
    let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 8);
    let service = ServiceStateController::new(ServiceState::Ready);
    let manager = TaskManagerHandle::spawn(
        store.clone(),
        writer,
        dispatcher,
        service.clone(),
        Arc::new(EarlyCancelledRunner),
        test_task_manager_launch_resources_for_repository(1, 1, &repository, temp_dir.path()),
        8,
    );
    let mut refresh_pause =
        super::super::scheduler_refresh::install_scheduler_refresh_pause_for_test(
            manager.scheduler_server_instance_id_for_test(),
        );

    service
        .set(ServiceState::StoreDegraded)
        .expect("start a service-state scheduler refresh");
    refresh_pause.wait_until_reached().await;

    let queued = match store
        .create_task(
            NewTask::try_new(
                ClientRequestId::new(),
                repository.id,
                "storage generation must force an exact reread",
            )
            .expect("construct scheduler refresh task"),
        )
        .await
        .expect("create scheduler refresh task")
    {
        CreateTaskOutcome::Created { task, .. } | CreateTaskOutcome::Existing { task } => task,
    };
    manager.notify_scheduler_storage_for_test(SchedulerStorageNotification::new(
        StorageState::Normal,
        StorageState::Normal,
        StorageState::Normal,
        vec![crate::scheduler::SchedulerRepositoryStorageState::new(
            repository.id,
            StorageState::Normal,
        )],
    ));
    refresh_pause.resume();

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let projection = manager.scheduler_projection_for_test();
            if projection.service_paused
                && projection.as_of_event_id.get() == queued.last_event_id.get()
                && projection.tasks == vec![(queued.id, TaskStatus::Queued)]
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the storage generation forces a post-service exact reread");
}
