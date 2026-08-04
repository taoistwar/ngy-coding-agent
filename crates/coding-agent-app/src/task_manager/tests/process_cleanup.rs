use super::*;

#[cfg(feature = "test-support")]
#[test]
fn terminal_release_confirmation_requires_runner_return_and_the_exact_task_scope() {
    let runtime = tempfile::tempdir().expect("create process-liveness runtime");
    let other_runtime = tempfile::tempdir().expect("create other process-liveness runtime");
    let directory =
        ProcessLivenessDirectory::open(runtime.path()).expect("open process-liveness runtime");
    let other_directory = ProcessLivenessDirectory::open(other_runtime.path())
        .expect("open other process-liveness runtime");
    let mut instance_id = [0x31; 16];
    instance_id[6] = 0x41;
    instance_id[8] = 0x81;
    let instance = directory
        .instance_scope(instance_id)
        .expect("create instance process scope");
    instance_id[0] ^= 0x7f;
    let other_instance = other_directory
        .instance_scope(instance_id)
        .expect("create other instance process scope");
    let task = TaskId::new();
    let other_task = TaskId::new();
    let task_scope = TaskProcessScopeOwnership::derive(&instance, task, 7)
        .expect("derive exact task process ownership");
    let other_scope = TaskProcessScopeOwnership::derive(&other_instance, task, 7)
        .expect("derive same-task ownership from another instance");
    let held_tree = task_scope
        .scope()
        .hold_tree_for_test()
        .expect("hold scope A process tree");
    assert!(matches!(
        TaskProcessCleanupConfirmation::try_new(
            RunnerReturnedState::new(&task_scope),
            &other_scope
        ),
        Err(crate::scheduler::TerminalReleaseProofError::ProcessTreeNotClean)
    ));
    assert!(matches!(
        TaskProcessCleanupConfirmation::try_new(RunnerReturnedState::new(&task_scope), &task_scope,),
        Err(crate::scheduler::TerminalReleaseProofError::ProcessTreeNotClean)
    ));
    let other_cleanup = TaskProcessCleanupConfirmation::try_new(
        RunnerReturnedState::new(&other_scope),
        &other_scope,
    )
    .expect("scope B has a self-consistent clean confirmation");
    let repository = tempfile::tempdir().expect("create repository identity");
    let marker = RootCapability::open(repository.path())
        .expect("open repository identity")
        .identity_marker()
        .expect("read repository identity");
    let key = RepositoryCoordinationKey::from_authenticated_marker(marker);
    let ledger = PermitLedger::new(
        SchedulerConcurrencyLimits::try_new(2, 2).expect("valid scheduler limits"),
    );
    let first = SharedPermitOwnership::new(
        ledger.clone(),
        ledger
            .reserve(task, key)
            .expect("reserve first exact permit"),
        7,
        task_scope.owner_id(),
    )
    .expect("share first permit");
    first.mark_submitted().expect("submit first permit");
    first.adopt().expect("adopt first permit");
    let repository_id = RepositoryId::new();
    let timestamp = UtcTimestamp::parse_rfc3339("2026-07-28T00:00:00Z")
        .expect("construct owner mismatch timestamp");
    let running_task = Task::try_from_stored(Task {
        id: task,
        client_request_id: ClientRequestId::new(),
        repository_id,
        prompt: "owner mismatch".to_owned(),
        status: TaskStatus::Running,
        delivery_readiness: coding_agent_domain::DeliveryReadiness::Unreviewed,
        attempt: 1,
        retry_of: None,
        created_at: timestamp,
        started_at: Some(timestamp),
        finished_at: None,
        last_event_id: EventId::new(1).expect("started event ID"),
        failure: None,
    })
    .expect("construct owner mismatch running task");
    let repository_record = Repository {
        id: repository_id,
        selected_path: canonical(repository.path().join("selected")),
        display_name: "owner mismatch".to_owned(),
        git_root: canonical(repository.path().join("git")),
        cargo_workspace_root: canonical(repository.path().join("workspace")),
        created_at: timestamp,
        last_opened_at: timestamp,
    };
    let coordinator = RepositoryControlCoordinator::new();
    coordinator
        .register_alias(
            RepositoryIdentityLookup {
                repository_id,
                git_root: repository_record.git_root.clone(),
                git_identity_key: "owner-mismatch".to_owned(),
            },
            &FixedMarkerResolver(marker),
        )
        .expect("register owner mismatch repository");
    let lease = coordinator
        .try_acquire(key)
        .expect("acquire owner mismatch control lease");
    let (preparation_sender, _preparation_receiver) = mpsc::channel(1);
    assert!(matches!(
        RunContext::adopt_with_launch_ordinal(
            running_task,
            repository_record,
            CancellationToken::new(),
            lease,
            other_scope.clone(),
            first.witness(),
            preparation_sender,
            0,
        ),
        Err(crate::run_context::RunContextOwnershipError::ProcessLivenessScopeMismatch)
    ));
    let terminal_event = EventId::new(10).expect("terminal event ID");
    let terminal_membership = EventCursor::new(10).expect("membership cursor");
    assert!(matches!(
        crate::scheduler::TerminalProcessCleanReleaseProof::try_new(
            task,
            TaskEventKind::TaskCompleted,
            terminal_event,
            EventCursor::new(10).expect("projection cursor"),
            terminal_membership,
            &first,
            &other_cleanup,
        ),
        Err(crate::scheduler::TerminalReleaseProofError::ProcessOwnerMismatch)
    ));
    drop(held_tree);
    let cleanup =
        TaskProcessCleanupConfirmation::try_new(RunnerReturnedState::new(&task_scope), &task_scope)
            .expect("runner-returned exact task scope is clean");
    assert!(matches!(
        crate::scheduler::TerminalProcessCleanReleaseProof::try_new(
            task,
            TaskEventKind::TaskStarted,
            terminal_event,
            EventCursor::new(10).expect("projection cursor"),
            terminal_membership,
            &first,
            &cleanup,
        ),
        Err(crate::scheduler::TerminalReleaseProofError::NotTerminalEvent)
    ));
    assert!(matches!(
        crate::scheduler::TerminalProcessCleanReleaseProof::try_new(
            other_task,
            TaskEventKind::TaskCompleted,
            terminal_event,
            EventCursor::new(10).expect("projection cursor"),
            terminal_membership,
            &first,
            &cleanup,
        ),
        Err(crate::scheduler::TerminalReleaseProofError::CleanupTaskMismatch)
    ));
    assert!(matches!(
        crate::scheduler::TerminalProcessCleanReleaseProof::try_new(
            task,
            TaskEventKind::TaskCompleted,
            terminal_event,
            EventCursor::new(9).expect("projection cursor"),
            terminal_membership,
            &first,
            &cleanup,
        ),
        Err(crate::scheduler::TerminalReleaseProofError::ProjectionBehindTerminal)
    ));
    assert!(matches!(
        crate::scheduler::TerminalProcessCleanReleaseProof::try_new(
            task,
            TaskEventKind::TaskCompleted,
            terminal_event,
            EventCursor::new(10).expect("projection cursor"),
            EventCursor::new(9).expect("membership cursor"),
            &first,
            &cleanup,
        ),
        Err(crate::scheduler::TerminalReleaseProofError::MembershipWatermarkBehindTerminal)
    ));
    let proof = crate::scheduler::TerminalProcessCleanReleaseProof::try_new(
        task,
        TaskEventKind::TaskCompleted,
        terminal_event,
        EventCursor::new(10).expect("projection cursor"),
        terminal_membership,
        &first,
        &cleanup,
    )
    .expect("exact returned/clean/terminal/projected proof");
    first
        .release_after_terminal_and_process_clean(&proof)
        .expect("release first exact permit");
    let second = SharedPermitOwnership::new(
        ledger.clone(),
        ledger
            .reserve(task, key)
            .expect("reserve a new permit for the same task"),
        7,
        task_scope.owner_id(),
    )
    .expect("share second permit");
    second.mark_submitted().expect("submit second permit");
    second.adopt().expect("adopt second permit");
    assert_eq!(
        second.release_after_terminal_and_process_clean(&proof),
        Err(PermitLedgerError::TokenIdentityMismatch)
    );
    assert!(matches!(
        crate::scheduler::TerminalProcessCleanReleaseProof::try_new(
            task,
            TaskEventKind::TaskCompleted,
            terminal_event,
            EventCursor::new(10).expect("projection cursor"),
            terminal_membership,
            &second,
            &cleanup,
        ),
        Err(crate::scheduler::TerminalReleaseProofError::CleanupAlreadyConsumed)
    ));
}

#[tokio::test]
async fn shutdown_process_cleanup_is_immediate_without_spawned_runners() {
    let tracker = ShutdownProcessCleanupTracker::default();

    tokio::time::timeout(
        PROCESS_CLEANUP_RETRY_INTERVAL,
        tracker.wait_for_all_registered(),
    )
    .await
    .expect("an empty frozen launch set is already process-clean");
}

#[tokio::test]
async fn shutdown_process_cleanup_retires_confirmed_runners_across_many_rounds() {
    let runtime = tempfile::tempdir().expect("create bounded process-cleanup runtime");
    let directory =
        ProcessLivenessDirectory::open(runtime.path()).expect("open bounded process runtime");
    let instance_scope = directory
        .instance_scope(*uuid::Uuid::new_v4().as_bytes())
        .expect("derive bounded process-cleanup instance scope");
    let tracker = Arc::new(ShutdownProcessCleanupTracker::default());

    for operation_nonce in 1..=32 {
        let process_scope =
            TaskProcessScopeOwnership::derive(&instance_scope, TaskId::new(), operation_nonce)
                .expect("derive bounded runner process ownership");
        assert!(tracker.register_spawned_runner(&process_scope));
        assert_eq!(
            tracker
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .outstanding
                .len(),
            1,
            "only the current unconfirmed runner belongs in the cleanup tracker"
        );

        tracker.runner_returned(process_scope);
        let proof = tokio::time::timeout(Duration::from_secs(2), tracker.wait_for_all_registered())
            .await
            .expect("a clean returned runner produces shutdown cleanup proof");
        assert_eq!(proof.tracker_id, tracker.id);
        assert!(
            tracker
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .outstanding
                .is_empty(),
            "confirmed runner history must not remain in the cleanup tracker"
        );
    }

    tokio::time::timeout(
        PROCESS_CLEANUP_RETRY_INTERVAL,
        tracker.wait_for_all_registered(),
    )
    .await
    .expect("retiring runner history preserves the empty-set shutdown proof");
}

#[test]
fn shutdown_process_cleanup_mismatch_does_not_retire_outstanding_ownership() {
    let runtime = tempfile::tempdir().expect("create mismatch process-cleanup runtime");
    let directory =
        ProcessLivenessDirectory::open(runtime.path()).expect("open mismatch process runtime");
    let instance_scope = directory
        .instance_scope(*uuid::Uuid::new_v4().as_bytes())
        .expect("derive mismatch process-cleanup instance scope");
    let process_scope = TaskProcessScopeOwnership::derive(&instance_scope, TaskId::new(), 7)
        .expect("derive registered process ownership");
    let unknown_scope = TaskProcessScopeOwnership::derive(&instance_scope, TaskId::new(), 8)
        .expect("derive unknown process ownership");
    let tracker = ShutdownProcessCleanupTracker::default();

    assert!(tracker.register_spawned_runner(&process_scope));
    assert!(!tracker.mark_confirmed(&unknown_scope));
    {
        let mut state = tracker
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .outstanding
            .get_mut(&process_scope.owner_id())
            .expect("registered process remains outstanding")
            .operation_nonce = 9;
    }

    assert!(!tracker.mark_confirmed(&process_scope));
    assert!(
        !tracker.all_registered_confirmed(),
        "unknown or mismatched cleanup must not produce shutdown proof"
    );
    assert_eq!(
        tracker
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .outstanding
            .len(),
        1,
        "mismatched ownership remains fail-closed and outstanding"
    );
}

#[tokio::test(start_paused = true)]
async fn shutdown_finalization_deadline_cancels_a_blocked_send_without_late_enqueue() {
    let (sender, mut mailbox) = mpsc::channel(1);
    sender
        .send(TaskManagerMessage::StorageChanged)
        .await
        .expect("fill the detached task-manager mailbox");
    let manager = detached_task_manager_handle(sender.clone());
    let proof = manager.freeze_and_wait_for_process_cleanup().await;
    let budget = Duration::from_secs(1);
    let deadline = Instant::now() + budget;
    let finalization = tokio::spawn({
        let manager = manager.clone();
        async move {
            manager
                .finalize_shutdown_after_process_cleanup(&proof, deadline)
                .await
        }
    });

    tokio::task::yield_now().await;
    assert!(
        !finalization.is_finished(),
        "the full mailbox must keep the finalization send pending"
    );
    tokio::time::advance(budget - Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert!(
        !finalization.is_finished(),
        "the blocked send must retain the original deadline"
    );
    tokio::time::advance(Duration::from_millis(1)).await;
    assert!(matches!(
        finalization.await.expect("join blocked finalization"),
        Err(TaskManagerError::DeadlineElapsed)
    ));

    assert!(matches!(
        mailbox.try_recv(),
        Ok(TaskManagerMessage::StorageChanged)
    ));
    tokio::task::yield_now().await;
    assert!(
        matches!(mailbox.try_recv(), Err(TryRecvError::Empty)),
        "dropping the timed-out send must prevent a late finalization enqueue"
    );
}

#[tokio::test(start_paused = true)]
async fn shutdown_finalization_deadline_covers_response_wait_after_send_succeeds() {
    let (sender, mut mailbox) = mpsc::channel(1);
    let manager = detached_task_manager_handle(sender);
    let proof = manager.freeze_and_wait_for_process_cleanup().await;
    let budget = Duration::from_secs(1);
    let deadline = Instant::now() + budget;
    let finalization = tokio::spawn({
        let manager = manager.clone();
        async move {
            manager
                .finalize_shutdown_after_process_cleanup(&proof, deadline)
                .await
        }
    });

    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(400)).await;
    let response = match mailbox
        .recv()
        .await
        .expect("receive the shutdown finalization message")
    {
        TaskManagerMessage::FinalizeShutdownAfterProcessCleanup {
            deadline: message_deadline,
            response,
            ..
        } => {
            assert_eq!(
                message_deadline, deadline,
                "the actor must receive the caller's absolute deadline"
            );
            response
        }
        _ => panic!("the detached mailbox received an unexpected message"),
    };

    tokio::time::advance(Duration::from_millis(599)).await;
    tokio::task::yield_now().await;
    assert!(
        !finalization.is_finished(),
        "the response wait must remain live immediately before the absolute deadline"
    );
    tokio::time::advance(Duration::from_millis(1)).await;
    assert!(matches!(
        finalization.await.expect("join response-wait finalization"),
        Err(TaskManagerError::DeadlineElapsed)
    ));
    assert!(
        response.is_closed(),
        "deadline expiry must drop the response receiver"
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn shutdown_process_cleanup_waits_for_the_exact_held_tree() {
    let runtime = tempfile::tempdir().expect("create shutdown process-cleanup runtime");
    let directory = ProcessLivenessDirectory::open(runtime.path())
        .expect("open shutdown process-cleanup runtime");
    let instance_scope = directory
        .instance_scope(*uuid::Uuid::new_v4().as_bytes())
        .expect("derive shutdown process-cleanup instance scope");
    let process_scope = TaskProcessScopeOwnership::derive(&instance_scope, TaskId::new(), 1)
        .expect("derive shutdown process-cleanup task scope");
    let held = process_scope
        .scope()
        .hold_tree_for_test()
        .expect("hold exact shutdown process tree");
    let tracker = Arc::new(ShutdownProcessCleanupTracker::default());
    assert!(tracker.register_spawned_runner(&process_scope));
    tracker.runner_returned(process_scope);

    let mut waiting = Box::pin(tracker.wait_for_all_registered());
    assert!(
        tokio::time::timeout(PROCESS_CLEANUP_RETRY_INTERVAL * 2, &mut waiting)
            .await
            .is_err(),
        "Held must never be converted into shutdown cleanup proof"
    );

    drop(held);
    tokio::time::timeout(Duration::from_secs(2), waiting)
        .await
        .expect("released exact tree eventually confirms shutdown cleanup");
}

#[test]
fn batch_terminal_preflight_rejects_a_later_corruption_without_consuming_the_first() {
    let runtime = tempfile::tempdir().expect("create batch-preflight runtime");
    let directory = ProcessLivenessDirectory::open(runtime.path())
        .expect("open batch-preflight process directory");
    let mut instance_id = [0x52; 16];
    instance_id[6] = 0x42;
    instance_id[8] = 0x82;
    let instance_scope = directory
        .instance_scope(instance_id)
        .expect("derive batch-preflight instance scope");
    let task_a = TaskId::new();
    let task_b = TaskId::new();
    let scope_a = TaskProcessScopeOwnership::derive(&instance_scope, task_a, 1)
        .expect("derive first batch-preflight scope");
    let scope_b = TaskProcessScopeOwnership::derive(&instance_scope, task_b, 2)
        .expect("derive second batch-preflight scope");
    let cleanup_a =
        TaskProcessCleanupConfirmation::try_new(RunnerReturnedState::new(&scope_a), &scope_a)
            .expect("first batch-preflight cleanup is clean");
    let cleanup_b =
        TaskProcessCleanupConfirmation::try_new(RunnerReturnedState::new(&scope_b), &scope_b)
            .expect("second batch-preflight cleanup is clean");
    let repository = tempfile::tempdir().expect("create batch-preflight repository");
    let marker = RootCapability::open(repository.path())
        .expect("open batch-preflight repository")
        .identity_marker()
        .expect("read batch-preflight repository identity");
    let key = RepositoryCoordinationKey::from_authenticated_marker(marker);
    let ledger = PermitLedger::new(
        SchedulerConcurrencyLimits::try_new(2, 2).expect("valid batch-preflight concurrency"),
    );
    let permit_a = SharedPermitOwnership::new(
        ledger.clone(),
        ledger
            .reserve(task_a, key)
            .expect("reserve first batch permit"),
        1,
        scope_a.owner_id(),
    )
    .expect("construct first batch permit");
    let permit_b = SharedPermitOwnership::new(
        ledger.clone(),
        ledger
            .reserve(task_b, key)
            .expect("reserve second batch permit"),
        2,
        scope_b.owner_id(),
    )
    .expect("construct second batch permit");
    for permit in [&permit_a, &permit_b] {
        permit.mark_submitted().expect("submit batch permit");
        permit.adopt().expect("adopt batch permit");
    }
    let terminal_a = EventId::new(10).expect("first terminal event");
    let terminal_b = EventId::new(11).expect("second terminal event");
    let projection = EventCursor::new(11).expect("batch projection");
    let all_preflight = [
        TerminalProcessCleanReleaseProof::preflight(
            task_a,
            TaskEventKind::TaskFailed,
            terminal_a,
            projection,
            projection,
            &permit_a,
            &cleanup_a,
        ),
        TerminalProcessCleanReleaseProof::preflight(
            task_b,
            TaskEventKind::ActivityAppended,
            terminal_b,
            projection,
            projection,
            &permit_b,
            &cleanup_b,
        ),
    ];
    assert!(all_preflight[0].is_ok());
    assert!(matches!(
        all_preflight[1],
        Err(crate::TerminalReleaseProofError::NotTerminalEvent)
    ));
    assert_eq!(
        permit_a.state(),
        Ok(crate::PermitOwnershipState::Active),
        "the first permit remains owned because the batch never entered its consume phase"
    );
    assert!(cleanup_a.is_available_for_terminal_release());
    assert_eq!(permit_b.state(), Ok(crate::PermitOwnershipState::Active));
    assert!(cleanup_b.is_available_for_terminal_release());
}
