use super::*;

#[tokio::test(start_paused = true)]
async fn fake_runner_uses_exact_deadlines_and_cancels_before_the_next_boundary() {
    assert_eq!(
        FakeRunnerConfig::default().emission_interval(),
        Duration::from_millis(200)
    );
    let cancellation = CancellationToken::new();
    let (context, mut preparation_receiver) =
        fake_run_context_with_preparation(cancellation.clone());
    let task_id = context.task.id;
    let repository_id = context.task.repository_id;
    let attempt = context.task.attempt;
    let (sender, mut receiver) = mpsc::channel(8);
    let sink = RunnerEventSink {
        task_id,
        repository_id,
        attempt,
        sender,
    };
    let run = tokio::spawn(async move { FakeTaskRunner::default().run(context, sink).await });
    preparation_receiver
        .recv()
        .await
        .expect("fake runner reports synthetic preparation")
        .acknowledge();

    assert!(matches!(
        acknowledge_runner_event(&mut receiver, 1).await,
        RunnerEvent::PlanUpdated(_)
    ));
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(199)).await;
    tokio::task::yield_now().await;
    assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));

    tokio::time::advance(Duration::from_millis(1)).await;
    assert!(matches!(
        acknowledge_runner_event(&mut receiver, 2).await,
        RunnerEvent::ActivityAppended(entry) if entry.id() == "fake-plan-ready"
    ));
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(199)).await;
    cancellation.cancel();
    tokio::time::advance(Duration::from_millis(1)).await;

    assert_eq!(
        run.await.expect("join fake runner"),
        RunnerOutcome::Cancelled
    );
    assert!(matches!(
        receiver.try_recv(),
        Err(TryRecvError::Empty | TryRecvError::Disconnected)
    ));
}

#[tokio::test]
async fn preparation_ack_prevents_the_first_runner_event_from_overtaking_running_phase() {
    let cancellation = CancellationToken::new();
    let (mut context, mut preparation_receiver) = fake_run_context_with_preparation(cancellation);
    let first_event_sent = Arc::new(AtomicBool::new(false));
    let completion = tokio::spawn({
        let first_event_sent = Arc::clone(&first_event_sent);
        async move {
            context.complete_preparation_for_test().await;
            first_event_sent.store(true, Ordering::Release);
        }
    });
    let preparation = tokio::time::timeout(Duration::from_secs(2), preparation_receiver.recv())
        .await
        .expect("actor receives preparation completion")
        .expect("preparation completion channel remains open");

    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    assert!(
        !completion.is_finished(),
        "runner continuation must wait until the actor marks the task Running"
    );
    assert!(
        !first_event_sent.load(Ordering::Acquire),
        "reverse polling cannot expose the first event before the Running acknowledgement"
    );

    preparation.acknowledge();
    completion
        .await
        .expect("preparation acknowledgement unblocks the runner");
    assert!(first_event_sent.load(Ordering::Acquire));
}

async fn acknowledge_runner_event(
    receiver: &mut mpsc::Receiver<TaskManagerMessage>,
    event_id: i64,
) -> RunnerEvent {
    let message = receiver.recv().await.expect("fake runner sends an event");
    let TaskManagerMessage::RunnerEvent {
        event, response, ..
    } = message
    else {
        panic!("fake runner sink sends only runner events");
    };
    response
        .send(Ok(EventId::new(event_id).expect("positive event ID")))
        .expect("fake runner awaits event acknowledgement");
    event
}

fn fake_run_context_with_preparation(
    cancellation: CancellationToken,
) -> (
    RunContext,
    mpsc::Receiver<crate::run_context::PreparationCompleted>,
) {
    let repository_id = coding_agent_domain::RepositoryId::new();
    let timestamp = UtcTimestamp::parse_rfc3339("2026-07-15T00:00:00Z")
        .expect("construct fake runner timestamp");
    let repository_root = tempfile::tempdir().expect("create fake runner repository");
    let root = repository_root.path().canonicalize().unwrap();
    let task = Task::try_from_stored(Task {
        id: TaskId::new(),
        client_request_id: ClientRequestId::new(),
        repository_id,
        prompt: "direct fake runner test".to_owned(),
        status: TaskStatus::Running,
        delivery_readiness: coding_agent_domain::DeliveryReadiness::Unreviewed,
        attempt: 1,
        retry_of: None,
        created_at: timestamp,
        started_at: Some(timestamp),
        finished_at: None,
        last_event_id: EventId::new(1).expect("positive event ID"),
        failure: None,
    })
    .expect("construct valid running task");
    let repository = Repository {
        id: repository_id,
        selected_path: canonical(root.join("fake-selected")),
        display_name: "fake runner".to_owned(),
        git_root: canonical(root.join("fake-git")),
        cargo_workspace_root: canonical(root.join("fake-workspace")),
        created_at: timestamp,
        last_opened_at: timestamp,
    };
    let resources = test_task_manager_launch_resources(1, 1);
    let coordinator = resources.repository_control();
    let marker = RootCapability::open(&root)
        .expect("open fake runner repository capability")
        .identity_marker()
        .expect("read fake runner repository identity");
    let key = coordinator
        .register_alias(
            RepositoryIdentityLookup {
                repository_id,
                git_root: repository.git_root.clone(),
                git_identity_key: "fake-runner-repository".to_owned(),
            },
            &FixedMarkerResolver(marker),
        )
        .expect("register fake runner repository identity");
    let control_lease = coordinator
        .try_acquire(key)
        .expect("acquire fake runner control lease");
    let operation_nonce = 1;
    let process_scope = TaskProcessScopeOwnership::derive(
        &resources.instance_process_scope(),
        task.id,
        operation_nonce,
    )
    .expect("derive fake runner task process scope");
    let permit_ledger = PermitLedger::new(resources.limits());
    let permit = SharedPermitOwnership::new(
        permit_ledger.clone(),
        permit_ledger
            .reserve(task.id, key)
            .expect("reserve fake runner permit"),
        operation_nonce,
        process_scope.owner_id(),
    )
    .expect("construct fake runner permit authority");
    permit
        .mark_submitted()
        .expect("mark fake runner permit submitted");
    permit.adopt().expect("adopt fake runner permit");
    let (preparation_sender, preparation_receiver) = mpsc::channel(1);
    let context = RunContext::adopt_with_launch_ordinal(
        task,
        repository,
        cancellation,
        control_lease,
        process_scope,
        permit.witness(),
        preparation_sender,
        0,
    )
    .expect("adopt real fake runner launch resources");
    (context, preparation_receiver)
}
