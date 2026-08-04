use coding_agent_runtime::RootCapability;

use super::*;

#[test]
fn terminal_batch_preflights_every_item_before_releasing_any_capacity() {
    let repository = tempfile::tempdir().expect("create repository identity");
    let key = RepositoryCoordinationKey::from_authenticated_marker(
        RootCapability::open(repository.path())
            .expect("open repository identity")
            .identity_marker()
            .expect("observe repository identity"),
    );
    let ledger = PermitLedger::new(
        SchedulerConcurrencyLimits::try_new(2, 2).expect("valid scheduler limits"),
    );
    let first_task = TaskId::new();
    let second_task = TaskId::new();
    let first = active_permit(&ledger, first_task, key, 7, 101);
    let second = active_permit(&ledger, second_task, key, 11, 103);
    let first_cleanup =
        TaskProcessCleanupConfirmation::confirmed_for_atomic_release_test(first_task, 7, 101);
    let second_cleanup =
        TaskProcessCleanupConfirmation::confirmed_for_atomic_release_test(second_task, 11, 103);
    let event_id = EventId::new(17).expect("terminal event ID");
    let projection = EventCursor::new(17).expect("terminal projection");
    let membership = EventCursor::new(19).expect("membership watermark");
    let first_proof = TerminalProcessCleanReleaseProof::prepare_for_atomic_release(
        first_task,
        TaskEventKind::TaskCompleted,
        event_id,
        projection,
        membership,
        &first,
        &first_cleanup,
    )
    .expect("prepare first terminal proof");
    let second_proof = TerminalProcessCleanReleaseProof::prepare_for_atomic_release(
        second_task,
        TaskEventKind::TaskFailed,
        event_id,
        projection,
        membership,
        &second,
        &second_cleanup,
    )
    .expect("prepare second terminal proof");

    let corrupt_second = [
        PreparedTerminalPermitRelease::new(&first, &first_proof, &first_cleanup),
        PreparedTerminalPermitRelease::new(&second, &first_proof, &second_cleanup),
    ];
    assert_eq!(
        ledger.release_terminal_batch(&corrupt_second),
        Err(PermitLedgerError::TokenIdentityMismatch)
    );
    assert_eq!(
        first.state().expect("first permit state"),
        PermitOwnershipState::Active
    );
    assert_eq!(
        second.state().expect("second permit state"),
        PermitOwnershipState::Active
    );
    assert!(first_cleanup.is_available_for_terminal_release());
    assert!(second_cleanup.is_available_for_terminal_release());

    let exact = [
        PreparedTerminalPermitRelease::new(&first, &first_proof, &first_cleanup),
        PreparedTerminalPermitRelease::new(&second, &second_proof, &second_cleanup),
    ];
    ledger
        .release_terminal_batch(&exact)
        .expect("release exact terminal batch");
    assert_eq!(
        first.state().expect("first released state"),
        PermitOwnershipState::Released
    );
    assert_eq!(
        second.state().expect("second released state"),
        PermitOwnershipState::Released
    );
    assert!(first_cleanup.is_available_for_terminal_release());
    assert!(second_cleanup.is_available_for_terminal_release());
}

fn active_permit(
    ledger: &PermitLedger,
    task_id: TaskId,
    key: RepositoryCoordinationKey,
    admission_nonce: u64,
    process_owner_id: u64,
) -> SharedPermitOwnership {
    let permit = SharedPermitOwnership::new(
        ledger.clone(),
        ledger.reserve(task_id, key).expect("reserve permit"),
        admission_nonce,
        process_owner_id,
    )
    .expect("share permit ownership");
    permit.mark_submitted().expect("submit permit");
    permit.adopt().expect("adopt permit");
    permit
}
