use super::*;

#[tokio::test]
async fn all_claim_pause_phases_preserve_running_token_invariant() {
    for phase in [
        ClaimPhase::PermitAcquired,
        ClaimPhase::HandleRegistered,
        ClaimPhase::RunningCommitted,
    ] {
        let temp_dir = tempfile::tempdir().expect("create claim-pause fixture directory");
        let store = Store::open(temp_dir.path().join("store.sqlite3"))
            .await
            .expect("open claim-pause store");
        store.migrate().await.expect("migrate claim-pause store");
        let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
        let dispatcher = EventDispatcherHandle::spawn(store.clone(), 64)
            .await
            .expect("spawn claim-pause dispatcher");
        let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 8);
        let runner = Arc::new(CancellingRunner::default());
        let hooks = Arc::new(ClaimTestHooks::new(phase));
        let launch_resources =
            test_task_manager_launch_resources_for_repository(1, 1, &repository, temp_dir.path());
        let projection_repository_control = launch_resources.repository_control();
        let manager = TaskManagerHandle::spawn_with_claim_hooks(
            (
                store.clone(),
                writer.clone(),
                dispatcher,
                ServiceStateController::new(ServiceState::Ready),
            ),
            runner.clone(),
            launch_resources,
            8,
            hooks.clone(),
        );
        let created = writer
            .create_task(
                NewTask::try_new(ClientRequestId::new(), repository.id, "claim pause")
                    .expect("construct claim-pause task"),
                background_deadline(),
            )
            .await
            .expect("create claim-pause task");
        let task = match created.value {
            CreateTaskOutcome::Created { task, .. } | CreateTaskOutcome::Existing { task } => task,
        };

        manager.notify_queued(task.id).await.expect("notify actor");
        hooks.wait_until_reached().await;
        let paused = store
            .task_detail(task.id)
            .await
            .expect("read paused task")
            .expect("paused task exists")
            .task;
        match phase {
            ClaimPhase::PermitAcquired => {
                assert_eq!(paused.status, TaskStatus::Queued);
                assert_eq!(hooks.active_count(), 0);
                assert_eq!(
                    manager
                        .scheduler_state_reader
                        .current()
                        .public_state()
                        .active_task_count_for_test(),
                    0,
                    "a provisional permit is not public active membership"
                );
                let store_snapshot = store
                    .scheduler_bootstrap_snapshot()
                    .await
                    .expect("load the provisional Store projection");
                let permit_snapshot = hooks.permit_snapshot();
                let projected = SchedulerStoreState::from_complete_snapshot(
                    uuid::Uuid::new_v4(),
                    SchedulerPublicLimits::compatibility_defaults(
                        SchedulerConcurrencyLimits::try_new(1, 1)
                            .expect("valid provisional projection limits"),
                    ),
                    &store_snapshot,
                    SchedulerRuntimeProjection {
                        service_paused: false,
                        permit_ledger: &permit_snapshot,
                        repository_control: projection_repository_control.as_ref(),
                        storage: None,
                    },
                )
                .expect("recompute public state with the live provisional permit");
                assert_eq!(
                    projected.active_task_count_for_test(),
                    0,
                    "recomputing with a provisional permit must keep it invisible"
                );
            }
            ClaimPhase::HandleRegistered => {
                assert_eq!(paused.status, TaskStatus::Queued);
                assert_eq!(hooks.active_count(), 1);
            }
            ClaimPhase::RunningCommitted => {
                assert_eq!(paused.status, TaskStatus::Running);
                assert_eq!(hooks.active_count(), 1);
            }
            ClaimPhase::ActorLivenessAcquired
            | ClaimPhase::ClaimRetainedForReconciliation
            | ClaimPhase::TerminalDispatched
            | ClaimPhase::PendingReplayBeforeActorDelivery => {
                unreachable!("the claim-phase matrix excludes terminal dispatch")
            }
        }

        let cancel = tokio::spawn({
            let manager = manager.clone();
            async move { manager.cancel(task.id).await }
        });
        if phase == ClaimPhase::RunningCommitted {
            assert!(matches!(
                cancel.await.expect("join cancel").expect("cancel task"),
                CancelOutcome::Accepted { .. }
            ));
            hooks.resume();
        } else {
            tokio::task::yield_now().await;
            hooks.resume();
            assert!(matches!(
                cancel.await.expect("join cancel").expect("cancel task"),
                CancelOutcome::Accepted { .. } | CancelOutcome::Cancelled { .. }
            ));
        }
        wait_for_status(&store, task.id, TaskStatus::Cancelled).await;
        wait_for_claim_resources_released(&hooks).await;
        assert_eq!(
            runner.starts.load(Ordering::SeqCst),
            0,
            "cancel before spawn must suppress the runner at phase {phase:?}"
        );
        assert_eq!(hooks.active_count(), 0);
        assert_eq!(hooks.available_permits(), 1);
        let terminal_count = store
            .task_events_after(task.id, EventCursor::ZERO, usize::MAX)
            .await
            .expect("load pre-spawn-cancel events")
            .events
            .into_iter()
            .filter(|event| event.payload.kind() == TaskEventKind::TaskCancelled)
            .count();
        assert_eq!(terminal_count, 1);
    }
}

#[tokio::test]
async fn quiesce_before_cancel_keeps_mailbox_fifo_at_the_provisional_claim_gate() {
    let temp_dir = tempfile::tempdir().expect("create FIFO claim-gate fixture");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open FIFO claim-gate store");
    store
        .migrate()
        .await
        .expect("migrate FIFO claim-gate store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 64)
        .await
        .expect("spawn FIFO claim-gate dispatcher");
    let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 8);
    let runner = Arc::new(CancellingRunner::default());
    let hooks = Arc::new(ClaimTestHooks::new(ClaimPhase::PermitAcquired));
    let manager = TaskManagerHandle::spawn_with_claim_hooks(
        (
            store.clone(),
            writer.clone(),
            dispatcher,
            ServiceStateController::new(ServiceState::Ready),
        ),
        runner.clone(),
        test_task_manager_launch_resources_for_repository(1, 1, &repository, temp_dir.path()),
        8,
        hooks.clone(),
    );
    let task = writer
        .create_task(
            NewTask::try_new(
                ClientRequestId::new(),
                repository.id,
                "quiesce precedes cancel at provisional gate",
            )
            .expect("construct FIFO claim-gate task"),
            background_deadline(),
        )
        .await
        .expect("create FIFO claim-gate task")
        .value
        .task()
        .clone();

    manager.notify_queued(task.id).await.expect("notify actor");
    hooks.wait_until_reached().await;
    assert_eq!(hooks.available_permits(), 0);

    let (quiesce_response, quiesce_receiver) = oneshot::channel();
    manager
        .sender
        .send(TaskManagerMessage::Quiesce {
            deadline: background_deadline(),
            response: quiesce_response,
        })
        .await
        .expect("enqueue quiesce first");
    let (cancel_response, cancel_receiver) = oneshot::channel();
    manager
        .sender
        .send(TaskManagerMessage::Cancel {
            task_id: task.id,
            response: cancel_response,
        })
        .await
        .expect("enqueue cancel second");
    hooks.resume();

    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), cancel_receiver)
            .await
            .expect("cancel receives an actor response")
            .expect("cancel response channel remains live"),
        Err(TaskManagerError::Frozen)
    ));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), quiesce_receiver)
            .await
            .expect("quiesce completes")
            .expect("quiesce response channel remains live")
            .expect("quiesce returns a typed result"),
        QuiesceResult::Durable { .. }
    ));
    assert_eq!(
        store
            .task_detail(task.id)
            .await
            .expect("load FIFO task")
            .expect("FIFO task remains stored")
            .task
            .status,
        TaskStatus::Interrupted
    );
    assert_eq!(runner.starts.load(Ordering::SeqCst), 0);
    assert_eq!(hooks.available_permits(), 1);
}
