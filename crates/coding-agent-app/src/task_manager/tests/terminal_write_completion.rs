use super::*;

#[cfg(feature = "test-support")]
#[tokio::test]
async fn terminal_won_rejects_every_drift_from_the_actor_owned_running_task() {
    type TerminalDrift = fn(&mut Task, &Task);
    let cases: [(&str, TerminalDrift); 10] = [
        ("prompt", |terminal, _| {
            terminal.prompt.push_str(" drift");
        }),
        ("client_request_id", |terminal, _| {
            terminal.client_request_id = ClientRequestId::new();
        }),
        ("retry_of", |terminal, _| {
            terminal.retry_of = Some(TaskId::new());
        }),
        ("created_at", |terminal, _| {
            terminal.created_at = UtcTimestamp::parse_rfc3339("2001-01-01T00:00:00Z")
                .expect("construct drifted creation timestamp");
        }),
        ("started_at_none", |terminal, _| {
            terminal.started_at = None;
        }),
        ("started_at", |terminal, _| {
            terminal.started_at = Some(
                UtcTimestamp::parse_rfc3339("2002-01-01T00:00:00Z")
                    .expect("construct drifted start timestamp"),
            );
        }),
        ("delivery_readiness", |terminal, _| {
            terminal.delivery_readiness = DeliveryReadiness::ReviewRejected;
        }),
        ("failure", |terminal, _| {
            terminal.failure = Some(TaskFailure {
                code: "DRIFTED_FAILURE".to_owned(),
                message: "a completed task cannot carry failure".to_owned(),
                retryable: false,
            });
        }),
        ("event_not_monotonic", |terminal, running| {
            terminal.last_event_id = running.last_event_id;
        }),
        ("finished_before_started", |terminal, _| {
            terminal.finished_at = Some(
                UtcTimestamp::parse_rfc3339("2000-01-01T00:00:00Z")
                    .expect("construct early finish timestamp"),
            );
        }),
    ];

    for (case, drift) in cases {
        let temp_dir = tempfile::tempdir().expect("create terminal drift fixture directory");
        let store = Store::open(temp_dir.path().join("store.sqlite3"))
            .await
            .expect("open terminal drift store");
        store.migrate().await.expect("migrate terminal drift store");
        let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
        let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
            .await
            .expect("spawn terminal drift dispatcher");
        let controller = Arc::new(
            StoreWriterTestController::try_new([StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseBeforeExecute,
                operation: Some(StoreWriterOperationKind::PersistStopIntentBatch),
                count: 1,
            }])
            .expect("construct terminal drift writer controller"),
        );
        let writer = StoreWriterHandle::spawn_with_test_controller(
            store.clone(),
            Arc::new(dispatcher.clone()),
            8,
            controller.clone(),
        );
        let runner = Arc::new(DelayedCancellationRunner::default());
        let manager = TaskManagerHandle::spawn(
            store.clone(),
            writer.clone(),
            dispatcher,
            ServiceStateController::new(ServiceState::Ready),
            runner.clone(),
            test_task_manager_launch_resources_for_repository(1, 1, &repository, temp_dir.path()),
            8,
        );
        let task = writer
            .create_task(
                NewTask::try_new(
                    ClientRequestId::new(),
                    repository.id,
                    format!("terminal drift {case}"),
                )
                .expect("construct terminal drift task"),
                background_deadline(),
            )
            .await
            .expect("create terminal drift task")
            .value
            .task()
            .clone();
        manager
            .notify_queued(task.id)
            .await
            .expect("notify terminal drift actor");
        tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
            .await
            .expect("terminal drift runner starts");
        let running = store
            .task_detail(task.id)
            .await
            .expect("load terminal drift running task")
            .expect("terminal drift task exists")
            .task;
        assert_eq!(running.status, TaskStatus::Running);

        let cancel = tokio::spawn({
            let manager = manager.clone();
            async move { manager.cancel(task.id).await }
        });
        controller
            .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
            .await;
        let pending_before = manager
            .active_pending_stop_write_for_test(task.id)
            .await
            .expect("inspect terminal drift stop write")
            .expect("terminal drift stop identity is actor-owned");
        let (identity, request) = match &pending_before {
            PendingDurableResult::PersistStopIntentBatch { identity, requests } => {
                assert_eq!(requests.len(), 1);
                (identity.clone(), requests[0])
            }
            _ => panic!("terminal drift fixture expects a stop-intent batch"),
        };
        let mut terminal = running.clone();
        terminal.status = TaskStatus::Completed;
        terminal.delivery_readiness = DeliveryReadiness::ReviewApproved;
        terminal.finished_at = terminal.started_at;
        terminal.last_event_id = EventId::new(
            running
                .last_event_id
                .get()
                .checked_add(1)
                .expect("terminal event ID remains in range"),
        )
        .expect("construct terminal event ID");
        assert!(
            Task::try_from_stored(terminal.clone()).is_ok(),
            "the shared baseline is a legal reviewed terminal"
        );
        drift(&mut terminal, &running);
        let completion = DurableCompletion {
            identity: identity.clone(),
            sequence_disposition: MutationSequenceDisposition::AdvanceNext,
            disposition: DurableDisposition::Confirmed(StopIntentBatchReceipt {
                items: vec![coding_agent_store::StopIntentBatchItem {
                    request,
                    outcome: PersistStopIntentOutcome::TerminalWon {
                        current: terminal.clone(),
                    },
                }],
            }),
        };
        manager
            .inject_stop_intent_completion_for_test(identity, completion)
            .await
            .expect("inject drifted TerminalWon receipt");
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }

        assert!(
            matches!(
                manager.safety_snapshot_for_test().await,
                Err(TaskManagerError::Frozen)
            ),
            "{case}: a drifted TerminalWon receipt must freeze the actor"
        );
        assert_eq!(
            manager
                .active_pending_stop_write_for_test(task.id)
                .await
                .expect("inspect retained terminal drift write"),
            Some(pending_before),
            "{case}: rejection preserves the exact identity, sequence, and request"
        );
        let snapshot = manager
            .active_stop_snapshot_for_test(task.id)
            .await
            .expect("inspect terminal drift ownership")
            .expect("terminal drift ownership remains active");
        assert_eq!(snapshot.stage, ActiveStopStageForTest::IntentWritePending);
        assert_eq!(snapshot.active_count, 1);
        assert_eq!(snapshot.available_permits, 0);
        assert!(!snapshot.terminal_task_set);
        assert!(
            !cancel.is_finished(),
            "{case}: a cancel waiter cannot receive the drifted terminal Task"
        );
        assert_eq!(
            store
                .task_detail(task.id)
                .await
                .expect("load retained terminal drift task")
                .expect("terminal drift task remains stored")
                .task
                .status,
            TaskStatus::Running,
            "{case}: no terminal projection or ownership release is permitted"
        );

        cancel.abort();
        assert_eq!(
            controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
            1
        );
        runner.release.notify_one();
    }
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn hard_freeze_still_projects_an_exact_recovered_terminal_won_and_releases_ownership() {
    let temp_dir = tempfile::tempdir().expect("create exact TerminalWon fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open exact TerminalWon store");
    store
        .migrate()
        .await
        .expect("migrate exact TerminalWon store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn exact TerminalWon dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseBeforeExecute,
            operation: Some(StoreWriterOperationKind::PersistStopIntentBatch),
            count: 1,
        }])
        .expect("construct exact TerminalWon writer controller"),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        store.clone(),
        Arc::new(dispatcher.clone()),
        8,
        controller.clone(),
    );
    let runner = Arc::new(DelayedCancellationRunner::default());
    let manager = TaskManagerHandle::spawn(
        store.clone(),
        writer.clone(),
        dispatcher,
        ServiceStateController::new(ServiceState::Ready),
        runner.clone(),
        test_task_manager_launch_resources_for_repository(1, 1, &repository, temp_dir.path()),
        8,
    );
    let task = writer
        .create_task(
            NewTask::try_new(
                ClientRequestId::new(),
                repository.id,
                "exact recovered TerminalWon",
            )
            .expect("construct exact TerminalWon task"),
            background_deadline(),
        )
        .await
        .expect("create exact TerminalWon task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify exact TerminalWon actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("exact TerminalWon runner starts");
    let running = store
        .task_detail(task.id)
        .await
        .expect("load exact TerminalWon running task")
        .expect("exact TerminalWon task exists")
        .task;

    let cancel = tokio::spawn({
        let manager = manager.clone();
        async move { manager.cancel(task.id).await }
    });
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;
    let terminal = persist_recovered_terminal(&store, &running).await;
    let pending = manager
        .active_pending_stop_write_for_test(task.id)
        .await
        .expect("inspect exact TerminalWon stop write")
        .expect("exact TerminalWon stop identity is actor-owned");
    let (identity, request) = match pending {
        PendingDurableResult::PersistStopIntentBatch { identity, requests } => {
            assert_eq!(requests.len(), 1);
            (identity, requests[0])
        }
        _ => panic!("exact TerminalWon fixture expects a stop-intent batch"),
    };
    manager
        .inject_stop_intent_completion_for_test(
            identity.clone(),
            DurableCompletion {
                identity,
                sequence_disposition: MutationSequenceDisposition::AdvanceNext,
                disposition: DurableDisposition::Confirmed(StopIntentBatchReceipt {
                    items: vec![coding_agent_store::StopIntentBatchItem {
                        request,
                        outcome: PersistStopIntentOutcome::TerminalWon {
                            current: terminal.clone(),
                        },
                    }],
                }),
            },
        )
        .await
        .expect("inject exact recovered TerminalWon receipt");

    assert!(
        manager.safety_snapshot_for_test().await.is_ok(),
        "a legal recovered Interrupted terminal is accepted"
    );
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), cancel)
            .await
            .expect("exact TerminalWon resolves cancel waiter")
            .expect("join exact TerminalWon cancel"),
        Ok(CancelOutcome::Finished { task }) if task == terminal
    ));
    let snapshot = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect exact TerminalWon ownership")
        .expect("runner still owns the task");
    assert_eq!(snapshot.stage, ActiveStopStageForTest::TerminalWon);
    assert_eq!(snapshot.active_count, 1);
    assert_eq!(snapshot.available_permits, 0);

    manager
        .freeze_degraded_for_test()
        .await
        .expect("hard-freeze the exact TerminalWon");
    runner.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if manager
                .active_stop_snapshot_for_test(task.id)
                .await
                .is_ok_and(|snapshot| snapshot.is_none())
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the exact TerminalWon projects and releases after hard freeze");
    assert_eq!(
        manager.safety_registry_snapshot_for_test(),
        SafetyRegistrySnapshotForTest {
            entry_count: 0,
            pending_critical_count: 0,
            safety_latched_count: 0,
        }
    );
    assert_eq!(
        store
            .task_detail(task.id)
            .await
            .expect("load the projected TerminalWon task")
            .expect("the projected TerminalWon task exists")
            .task,
        terminal
    );
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
}
