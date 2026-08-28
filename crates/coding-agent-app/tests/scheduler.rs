use std::collections::HashSet;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::str::FromStr;

use coding_agent_app::{
    PermitLedger, PermitLedgerError, PermitOwnershipState, QueueReason, QueueReasonSignals,
    QueuedTaskCandidate, RepositoryCoordinationKey, SchedulerAdmissionGates,
    SchedulerConcurrencyLimits, SchedulerLimitError, SchedulerProjectionCandidate,
    SchedulerPublishOutcome, SchedulerPublisherError, SchedulerScanError, SchedulerStatePublisher,
    SharedPermitOwnership, advance_membership_watermark, is_membership_lifecycle_event,
    is_terminal_membership_event, project_queue_reason, scan_queued_candidates,
};
use coding_agent_domain::{
    EventCursor, EventId, RepositoryId, TaskEventKind, TaskId, UtcTimestamp,
};
use coding_agent_runtime::RootCapability;

fn task_id(suffix: u32) -> TaskId {
    TaskId::from_str(&format!("00000000-0000-4000-8000-{suffix:012x}")).expect("canonical task id")
}

fn repository_id(suffix: u32) -> RepositoryId {
    RepositoryId::from_str(&format!("10000000-0000-4000-8000-{suffix:012x}"))
        .expect("canonical repository id")
}

fn timestamp(second: u8) -> UtcTimestamp {
    UtcTimestamp::parse_rfc3339(&format!("2026-07-27T12:00:{second:02}.000000000Z"))
        .expect("canonical timestamp")
}

fn marker_key(directory: &tempfile::TempDir) -> RepositoryCoordinationKey {
    RepositoryCoordinationKey::from_authenticated_marker(
        RootCapability::open(directory.path().canonicalize().unwrap())
            .expect("open authenticated directory")
            .identity_marker()
            .expect("observe directory identity"),
    )
}

fn candidate(
    task: u32,
    repository: u32,
    key: RepositoryCoordinationKey,
    second: u8,
) -> QueuedTaskCandidate {
    QueuedTaskCandidate::new(
        task_id(task),
        repository_id(repository),
        key,
        timestamp(second),
    )
}

#[test]
fn concurrency_limits_cover_every_supported_boundary_and_reject_invalid_pairs() {
    for global in 1..=4 {
        for per_repository in 1..=global {
            let limits = SchedulerConcurrencyLimits::try_new(global, per_repository)
                .expect("supported limit pair");
            assert_eq!(limits.global().get(), global);
            assert_eq!(limits.per_repository().get(), per_repository);
        }
    }

    assert_eq!(
        SchedulerConcurrencyLimits::try_new(0, 1),
        Err(SchedulerLimitError::InvalidGlobal)
    );
    assert_eq!(
        SchedulerConcurrencyLimits::try_new(5, 1),
        Err(SchedulerLimitError::InvalidGlobal)
    );
    assert_eq!(
        SchedulerConcurrencyLimits::try_new(2, 0),
        Err(SchedulerLimitError::InvalidRepository)
    );
    assert_eq!(
        SchedulerConcurrencyLimits::try_new(4, 5),
        Err(SchedulerLimitError::InvalidRepository)
    );
    assert_eq!(
        SchedulerConcurrencyLimits::try_new(2, 3),
        Err(SchedulerLimitError::RepositoryExceedsGlobal)
    );
}

#[test]
fn queue_reason_has_exactly_five_values_and_exhaustive_fixed_priority() {
    let expected_values = [
        (QueueReason::ServicePaused, "service_paused"),
        (QueueReason::StoragePressure, "storage_pressure"),
        (QueueReason::GlobalCapacity, "global_capacity"),
        (QueueReason::RepositoryCapacity, "repository_capacity"),
        (
            QueueReason::RepositoryControlBusy,
            "repository_control_busy",
        ),
    ];
    assert_eq!(
        expected_values
            .iter()
            .map(|(reason, _)| *reason)
            .collect::<HashSet<_>>()
            .len(),
        5
    );
    for (reason, wire_value) in expected_values {
        assert_eq!(reason.as_str(), wire_value);
    }

    for mask in 0_u8..32 {
        let signals = QueueReasonSignals {
            service_paused: mask & 0b1 != 0,
            storage_pressure: mask & 0b10 != 0,
            global_capacity: mask & 0b100 != 0,
            repository_capacity: mask & 0b1000 != 0,
            repository_control_busy: mask & 0b1_0000 != 0,
        };
        let expected = if signals.service_paused {
            Some(QueueReason::ServicePaused)
        } else if signals.storage_pressure {
            Some(QueueReason::StoragePressure)
        } else if signals.global_capacity {
            Some(QueueReason::GlobalCapacity)
        } else if signals.repository_capacity {
            Some(QueueReason::RepositoryCapacity)
        } else if signals.repository_control_busy {
            Some(QueueReason::RepositoryControlBusy)
        } else {
            None
        };
        assert_eq!(project_queue_reason(signals), expected, "mask {mask:05b}");
    }
}

#[test]
fn scan_uses_created_at_then_task_id_even_when_input_is_reversed() {
    let repository = tempfile::tempdir().expect("repository directory");
    let key = marker_key(&repository);
    let ledger = PermitLedger::new(
        SchedulerConcurrencyLimits::try_new(2, 2).expect("valid scheduler limits"),
    );
    let same_time = timestamp(1);
    let later_id = QueuedTaskCandidate::new(task_id(2), repository_id(1), key, same_time);
    let earlier_id = QueuedTaskCandidate::new(task_id(1), repository_id(1), key, same_time);

    let scan = scan_queued_candidates(
        &[later_id, earlier_id],
        &ledger.snapshot(),
        &SchedulerAdmissionGates::default(),
    )
    .expect("scan");

    assert_eq!(scan.evaluations[0].candidate.task_id(), task_id(1));
    assert_eq!(scan.evaluations[1].candidate.task_id(), task_id(2));
    assert_eq!(
        scan.next_candidate.map(QueuedTaskCandidate::task_id),
        Some(task_id(1))
    );
}

#[test]
fn blocked_coordination_key_never_overtakes_but_newer_other_key_can_advance() {
    let repository_a = tempfile::tempdir().expect("repository A");
    let repository_b = tempfile::tempdir().expect("repository B");
    let key_a = marker_key(&repository_a);
    let key_b = marker_key(&repository_b);
    let limits = SchedulerConcurrencyLimits::try_new(2, 1).expect("valid scheduler limits");

    let a_first = candidate(1, 1, key_a, 1);
    let b_newer = candidate(2, 2, key_b, 2);
    let a_last = candidate(3, 3, key_a, 3);

    let storage_ledger = PermitLedger::new(limits);
    let mut storage_gates = SchedulerAdmissionGates::default();
    storage_gates.set_storage_pressure(a_first.task_id(), true);
    let storage_scan = scan_queued_candidates(
        &[a_last, b_newer, a_first],
        &storage_ledger.snapshot(),
        &storage_gates,
    )
    .expect("storage scan");
    assert_eq!(
        storage_scan
            .next_candidate
            .map(QueuedTaskCandidate::task_id),
        Some(b_newer.task_id())
    );
    let a_last_storage = storage_scan
        .evaluations
        .iter()
        .find(|evaluation| evaluation.candidate.task_id() == a_last.task_id())
        .expect("later A evaluation");
    assert_eq!(a_last_storage.reason, Some(QueueReason::StoragePressure));
    assert!(a_last_storage.blocked_by_earlier_same_key);

    let control_ledger = PermitLedger::new(limits);
    let mut control_gates = SchedulerAdmissionGates::default();
    control_gates.set_repository_control_busy(key_a, true);
    let control_scan = scan_queued_candidates(
        &[a_last, b_newer, a_first],
        &control_ledger.snapshot(),
        &control_gates,
    )
    .expect("control scan");
    assert_eq!(
        control_scan
            .next_candidate
            .map(QueuedTaskCandidate::task_id),
        Some(b_newer.task_id())
    );
    assert_eq!(
        control_scan.evaluations[0].reason,
        Some(QueueReason::RepositoryControlBusy)
    );

    let capacity_ledger = PermitLedger::new(limits);
    let held = capacity_ledger
        .reserve(task_id(99), key_a)
        .expect("reserve existing repository capacity");
    capacity_ledger
        .mark_submitted(&held)
        .expect("existing claim entered writer ingress");
    capacity_ledger.adopt(&held).expect("adopt existing task");
    let capacity_scan = scan_queued_candidates(
        &[a_last, b_newer, a_first],
        &capacity_ledger.snapshot(),
        &SchedulerAdmissionGates::default(),
    )
    .expect("capacity scan");
    assert_eq!(
        capacity_scan
            .next_candidate
            .map(QueuedTaskCandidate::task_id),
        Some(b_newer.task_id())
    );
    assert_eq!(
        capacity_scan.evaluations[0].reason,
        Some(QueueReason::RepositoryCapacity)
    );
}

#[test]
fn global_or_service_pause_blocks_every_key_using_priority_reason() {
    let repository_a = tempfile::tempdir().expect("repository A");
    let repository_b = tempfile::tempdir().expect("repository B");
    let key_a = marker_key(&repository_a);
    let key_b = marker_key(&repository_b);
    let limits = SchedulerConcurrencyLimits::try_new(1, 1).expect("valid limits");
    let ledger = PermitLedger::new(limits);
    let held = ledger
        .reserve(task_id(99), key_a)
        .expect("reserve global capacity");
    ledger
        .mark_submitted(&held)
        .expect("existing claim entered writer ingress");
    ledger.adopt(&held).expect("adopt held task");
    let queued = [candidate(1, 1, key_a, 1), candidate(2, 2, key_b, 2)];

    let capacity_scan = scan_queued_candidates(
        &queued,
        &ledger.snapshot(),
        &SchedulerAdmissionGates::default(),
    )
    .expect("capacity scan");
    assert!(capacity_scan.next_candidate.is_none());
    assert!(
        capacity_scan
            .evaluations
            .iter()
            .all(|evaluation| evaluation.reason == Some(QueueReason::GlobalCapacity))
    );

    let mut paused = SchedulerAdmissionGates::new(true);
    paused.set_storage_pressure(task_id(1), true);
    let paused_scan =
        scan_queued_candidates(&queued, &ledger.snapshot(), &paused).expect("paused scan");
    assert!(
        paused_scan
            .evaluations
            .iter()
            .all(|evaluation| evaluation.reason == Some(QueueReason::ServicePaused))
    );
}

#[test]
fn scan_rejects_duplicate_task_membership() {
    let repository = tempfile::tempdir().expect("repository");
    let key = marker_key(&repository);
    let repeated = candidate(1, 1, key, 1);
    let ledger = PermitLedger::new(
        SchedulerConcurrencyLimits::try_new(2, 2).expect("valid scheduler limits"),
    );
    assert_eq!(
        scan_queued_candidates(
            &[repeated, repeated],
            &ledger.snapshot(),
            &SchedulerAdmissionGates::default()
        ),
        Err(SchedulerScanError::DuplicateTask)
    );
}

#[test]
fn provisional_reserve_adopt_unknown_and_known_not_applied_are_exact() {
    let repository_a = tempfile::tempdir().expect("repository A");
    let repository_b = tempfile::tempdir().expect("repository B");
    let key_a = marker_key(&repository_a);
    let key_b = marker_key(&repository_b);
    let ledger = PermitLedger::new(
        SchedulerConcurrencyLimits::try_new(2, 1).expect("valid scheduler limits"),
    );

    let known_not_applied = ledger
        .reserve(task_id(1), key_a)
        .expect("reserve provisional permit");
    assert_eq!(
        ledger.state(&known_not_applied),
        Ok(PermitOwnershipState::Provisional)
    );
    assert_eq!(ledger.snapshot().global_owned(), 1);
    assert_eq!(ledger.snapshot().global_active(), 0);
    ledger
        .mark_submitted(&known_not_applied)
        .expect("claim entered writer ingress");
    ledger
        .release_known_not_applied(&known_not_applied)
        .expect("release a known-not-applied claim");
    assert_eq!(
        ledger.state(&known_not_applied),
        Ok(PermitOwnershipState::Released)
    );
    assert_eq!(ledger.snapshot().global_owned(), 0);
    assert_eq!(
        ledger.release_known_not_applied(&known_not_applied),
        Err(PermitLedgerError::AlreadyReleased)
    );

    let reconciled_not_applied = ledger
        .reserve(task_id(2), key_b)
        .expect("reserve unknown claim");
    ledger
        .mark_submitted(&reconciled_not_applied)
        .expect("claim entered writer ingress");
    ledger
        .retain_outcome_unknown(&reconciled_not_applied)
        .expect("retain unknown outcome");
    ledger
        .retain_outcome_unknown(&reconciled_not_applied)
        .expect("duplicate unknown notification is idempotent");
    assert_eq!(
        ledger.state(&reconciled_not_applied),
        Ok(PermitOwnershipState::OutcomeUnknown)
    );
    assert_eq!(ledger.snapshot().global_owned(), 1);
    assert_eq!(ledger.snapshot().global_active(), 0);
    ledger
        .release_known_not_applied(&reconciled_not_applied)
        .expect("exact reconciliation may release the retained permit");
    assert_eq!(
        ledger.state(&reconciled_not_applied),
        Ok(PermitOwnershipState::Released)
    );
    assert_eq!(ledger.snapshot().global_owned(), 0);

    let unknown = ledger
        .reserve(task_id(3), key_b)
        .expect("reserve a second unknown claim");
    ledger
        .mark_submitted(&unknown)
        .expect("claim entered writer ingress");
    ledger
        .retain_outcome_unknown(&unknown)
        .expect("retain the second unknown outcome");
    ledger
        .adopt(&unknown)
        .expect("exact reconciliation may adopt the retained permit");
    ledger
        .adopt(&unknown)
        .expect("duplicate applied callback is idempotent");
    assert_eq!(ledger.state(&unknown), Ok(PermitOwnershipState::Active));
    assert_eq!(ledger.snapshot().global_active(), 1);
}

#[test]
fn shared_permit_ownership_releases_only_the_exact_unsubmitted_state() {
    let repository = tempfile::tempdir().expect("repository");
    let key = marker_key(&repository);
    let ledger = PermitLedger::new(
        SchedulerConcurrencyLimits::try_new(2, 2).expect("valid scheduler limits"),
    );
    let shared = SharedPermitOwnership::new(
        ledger.clone(),
        ledger
            .reserve(task_id(1), key)
            .expect("reserve provisional permit"),
        1,
        11,
    )
    .expect("share exact permit ownership");
    let runner_witness = shared.witness();

    assert_eq!(shared.state(), Ok(PermitOwnershipState::Provisional));
    shared
        .release_unsubmitted()
        .expect("release a claim that was never submitted");
    assert_eq!(runner_witness.state(), Ok(PermitOwnershipState::Released));
    assert_eq!(ledger.snapshot().global_owned(), 0);
    assert_eq!(
        shared.release_unsubmitted(),
        Err(PermitLedgerError::AlreadyReleased)
    );
}

#[test]
fn submitted_is_a_distinct_linearization_state_and_duplicate_callbacks_do_not_widen_release() {
    let repository = tempfile::tempdir().expect("repository");
    let key = marker_key(&repository);
    let ledger = PermitLedger::new(
        SchedulerConcurrencyLimits::try_new(2, 2).expect("valid scheduler limits"),
    );
    let shared = SharedPermitOwnership::new(
        ledger.clone(),
        ledger
            .reserve(task_id(1), key)
            .expect("reserve provisional permit"),
        1,
        11,
    )
    .expect("share exact permit ownership");

    assert_eq!(
        shared.adopt(),
        Err(PermitLedgerError::InvalidTransition {
            from: PermitOwnershipState::Provisional,
            to: PermitOwnershipState::Active,
        })
    );
    assert_eq!(
        shared.retain_outcome_unknown(),
        Err(PermitLedgerError::InvalidTransition {
            from: PermitOwnershipState::Provisional,
            to: PermitOwnershipState::OutcomeUnknown,
        })
    );
    assert_eq!(
        shared.release_known_not_applied(),
        Err(PermitLedgerError::InvalidTransition {
            from: PermitOwnershipState::Provisional,
            to: PermitOwnershipState::Released,
        })
    );

    shared
        .mark_submitted()
        .expect("successful ingress handoff linearizes submission");
    shared
        .mark_submitted()
        .expect("duplicate submission callback is idempotent");
    assert_eq!(shared.state(), Ok(PermitOwnershipState::Submitted));
    assert_eq!(
        shared.release_unsubmitted(),
        Err(PermitLedgerError::InvalidTransition {
            from: PermitOwnershipState::Submitted,
            to: PermitOwnershipState::Released,
        })
    );
    shared
        .release_known_not_applied()
        .expect("exact known-not-applied completion releases submitted ownership");
}

#[test]
fn dropping_the_last_shared_permit_copy_without_release_fail_closes_capacity() {
    let repository = tempfile::tempdir().expect("repository");
    let key = marker_key(&repository);
    let ledger = PermitLedger::new(
        SchedulerConcurrencyLimits::try_new(2, 2).expect("valid scheduler limits"),
    );
    let task = task_id(1);
    let shared = SharedPermitOwnership::new(
        ledger.clone(),
        ledger
            .reserve(task, key)
            .expect("reserve provisional permit"),
        1,
        11,
    )
    .expect("share exact permit ownership");
    let runner_witness = shared.witness();

    drop(shared);
    assert!(!ledger.snapshot().has_abandoned());
    drop(runner_witness);

    let snapshot = ledger.snapshot();
    assert_eq!(snapshot.global_owned(), 1);
    assert_eq!(snapshot.abandoned_tasks(), &[task]);
}

#[test]
fn active_permits_cannot_use_a_known_not_applied_release_path() {
    let repository = tempfile::tempdir().expect("repository");
    let key = marker_key(&repository);
    let ledger = PermitLedger::new(
        SchedulerConcurrencyLimits::try_new(1, 1).expect("valid scheduler limits"),
    );
    let task = task_id(1);
    let permit = ledger.reserve(task, key).expect("reserve permit");
    ledger
        .mark_submitted(&permit)
        .expect("claim entered writer ingress");
    ledger.adopt(&permit).expect("adopt permit");
    assert_eq!(
        ledger.release_known_not_applied(&permit),
        Err(PermitLedgerError::InvalidTransition {
            from: PermitOwnershipState::Active,
            to: PermitOwnershipState::Released,
        })
    );
    assert_eq!(ledger.snapshot().global_owned(), 1);
}

#[test]
fn foreign_token_capacity_and_repository_capacity_fail_closed() {
    let repository_a = tempfile::tempdir().expect("repository A");
    let repository_b = tempfile::tempdir().expect("repository B");
    let key_a = marker_key(&repository_a);
    let key_b = marker_key(&repository_b);
    let limits = SchedulerConcurrencyLimits::try_new(2, 1).expect("valid scheduler limits");
    let ledger = PermitLedger::new(limits);
    let other_ledger = PermitLedger::new(limits);

    let permit_a = ledger.reserve(task_id(1), key_a).expect("reserve A");
    assert!(matches!(
        ledger.reserve(task_id(2), key_a),
        Err(PermitLedgerError::RepositoryCapacity)
    ));
    let permit_b = ledger.reserve(task_id(2), key_b).expect("reserve B");
    assert!(matches!(
        ledger.reserve(task_id(3), key_b),
        Err(PermitLedgerError::GlobalCapacity)
    ));
    assert_eq!(
        other_ledger.adopt(&permit_a),
        Err(PermitLedgerError::ForeignToken)
    );

    ledger.release_unsubmitted(&permit_a).expect("release A");
    ledger.release_unsubmitted(&permit_b).expect("release B");
}

#[test]
fn actor_panic_drop_retains_capacity_and_freezes_future_scans() {
    let repository_a = tempfile::tempdir().expect("repository A");
    let repository_b = tempfile::tempdir().expect("repository B");
    let key_a = marker_key(&repository_a);
    let key_b = marker_key(&repository_b);
    for (suffix, retain_unknown, adopt, expected_active) in [
        (1, false, false, 0),
        (2, true, false, 0),
        (3, false, true, 1),
    ] {
        let ledger = PermitLedger::new(
            SchedulerConcurrencyLimits::try_new(2, 1).expect("valid scheduler limits"),
        );
        let abandoned_task = task_id(suffix);
        let permit = ledger
            .reserve(abandoned_task, key_a)
            .expect("reserve permit");
        if retain_unknown || adopt {
            ledger
                .mark_submitted(&permit)
                .expect("claim entered writer ingress");
        }
        if retain_unknown {
            ledger
                .retain_outcome_unknown(&permit)
                .expect("retain unknown permit");
        }
        if adopt {
            ledger.adopt(&permit).expect("adopt permit");
        }

        let panic = catch_unwind(AssertUnwindSafe(move || {
            let _owned_by_actor = permit;
            panic!("simulated scheduler actor panic");
        }));
        assert!(panic.is_err());

        let snapshot = ledger.snapshot();
        assert_eq!(snapshot.global_owned(), 1);
        assert_eq!(snapshot.global_active(), expected_active);
        assert_eq!(snapshot.abandoned_tasks(), &[abandoned_task]);
        assert!(snapshot.has_abandoned());

        let queued = [candidate(99, 2, key_b, 2)];
        let scan = scan_queued_candidates(&queued, &snapshot, &SchedulerAdmissionGates::default())
            .expect("fail-closed scan");
        assert!(scan.next_candidate.is_none());
        assert_eq!(scan.evaluations[0].reason, Some(QueueReason::ServicePaused));
    }
}

#[test]
fn membership_watermark_advances_for_exactly_six_lifecycle_kinds() {
    let membership = [
        TaskEventKind::TaskQueued,
        TaskEventKind::TaskStarted,
        TaskEventKind::TaskCompleted,
        TaskEventKind::TaskFailed,
        TaskEventKind::TaskCancelled,
        TaskEventKind::TaskInterrupted,
    ];
    let non_membership = [
        TaskEventKind::PlanUpdated,
        TaskEventKind::ActivityAppended,
        TaskEventKind::DiffUpdated,
        TaskEventKind::TestUpdated,
        TaskEventKind::ReviewUpdated,
    ];

    assert!(membership.into_iter().all(is_membership_lifecycle_event));
    assert!(
        non_membership
            .into_iter()
            .all(|kind| !is_membership_lifecycle_event(kind))
    );
    assert!(
        [
            TaskEventKind::TaskCompleted,
            TaskEventKind::TaskFailed,
            TaskEventKind::TaskCancelled,
            TaskEventKind::TaskInterrupted,
        ]
        .into_iter()
        .all(is_terminal_membership_event)
    );
    assert!(!is_terminal_membership_event(TaskEventKind::TaskQueued));
    assert!(!is_terminal_membership_event(TaskEventKind::TaskStarted));

    for kind in membership {
        assert_eq!(
            advance_membership_watermark(
                EventCursor::ZERO,
                kind,
                EventId::new(10).expect("event ID")
            )
            .get(),
            10
        );
    }
    for kind in non_membership {
        assert_eq!(
            advance_membership_watermark(
                EventCursor::ZERO,
                kind,
                EventId::new(10).expect("event ID")
            ),
            EventCursor::ZERO
        );
    }
    assert_eq!(
        advance_membership_watermark(
            EventCursor::new(20).expect("cursor"),
            TaskEventKind::TaskCompleted,
            EventId::new(10).expect("event ID")
        )
        .get(),
        20,
        "out-of-order membership observation cannot move the watermark backwards"
    );
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PublicSemantics {
    admission_running: bool,
    queue_reasons: Vec<QueueReason>,
}

fn projection(
    public_state: PublicSemantics,
    membership: i64,
    service: u64,
) -> SchedulerProjectionCandidate<PublicSemantics> {
    SchedulerProjectionCandidate::new(
        public_state,
        EventCursor::new(membership).expect("membership cursor"),
        service,
    )
}

#[test]
fn publisher_only_advances_for_flushed_public_semantics_and_preserves_watermarks() {
    let initial = PublicSemantics {
        admission_running: true,
        queue_reasons: Vec::new(),
    };
    let mut publisher = SchedulerStatePublisher::new(projection(initial.clone(), 1, 0));
    let mut receiver = publisher.subscribe();
    assert_eq!(publisher.current().generation(), 0);
    assert!(!receiver.has_changed().expect("watch remains open"));

    let raw_available_bytes_before = 9_000_u64;
    let raw_available_bytes_after = 8_000_u64;
    assert_ne!(raw_available_bytes_before, raw_available_bytes_after);
    publisher
        .stage(projection(initial.clone(), 1, 0))
        .expect("stage semantically identical raw-sample recomputation");
    let unchanged = publisher.flush().expect("flush identical projection");
    assert!(matches!(unchanged, SchedulerPublishOutcome::Unchanged(_)));
    assert_eq!(unchanged.snapshot().generation(), 0);
    assert!(!receiver.has_changed().expect("watch remains open"));

    publisher
        .stage(projection(
            PublicSemantics {
                admission_running: false,
                queue_reasons: vec![QueueReason::ServicePaused],
            },
            1,
            0,
        ))
        .expect("stage intermediate state");
    publisher
        .stage(projection(
            PublicSemantics {
                admission_running: true,
                queue_reasons: vec![QueueReason::StoragePressure],
            },
            1,
            0,
        ))
        .expect("replace coalesced intermediate state");
    let published = publisher.flush().expect("flush latest projection");
    assert!(published.changed());
    assert_eq!(published.snapshot().generation(), 1);
    assert_eq!(
        published.snapshot().public_state().queue_reasons,
        vec![QueueReason::StoragePressure]
    );
    assert!(receiver.has_changed().expect("watch remains open"));
    assert_eq!(receiver.borrow_and_update().generation(), 1);

    publisher
        .stage(projection(
            PublicSemantics {
                admission_running: true,
                queue_reasons: vec![QueueReason::StoragePressure],
            },
            7,
            2,
        ))
        .expect("stage causal watermark update");
    let watermarked = publisher.flush().expect("flush watermarks");
    assert_eq!(watermarked.snapshot().generation(), 2);
    assert_eq!(watermarked.snapshot().as_of_event_id().get(), 7);
    assert_eq!(watermarked.snapshot().service_state_generation(), 2);

    publisher
        .stage(projection(
            PublicSemantics {
                admission_running: true,
                queue_reasons: vec![QueueReason::StoragePressure],
            },
            7,
            2,
        ))
        .expect("stage duplicate notification");
    assert!(!publisher.flush().expect("flush duplicate").changed());
    assert_eq!(publisher.current().generation(), 2);

    assert_eq!(
        publisher.stage(projection(initial.clone(), 6, 2)),
        Err(SchedulerPublisherError::MembershipWatermarkRegression)
    );
    assert_eq!(
        publisher.stage(projection(initial, 7, 1)),
        Err(SchedulerPublisherError::ServiceGenerationRegression)
    );
}

#[test]
fn watch_publisher_retains_only_the_latest_immutable_snapshot() {
    let initial = PublicSemantics {
        admission_running: true,
        queue_reasons: Vec::new(),
    };
    let mut publisher = SchedulerStatePublisher::new(projection(initial, 0, 0));
    let receiver = publisher.subscribe();

    for (membership, reason) in [
        (1, QueueReason::GlobalCapacity),
        (2, QueueReason::RepositoryCapacity),
        (3, QueueReason::RepositoryControlBusy),
    ] {
        publisher
            .stage(projection(
                PublicSemantics {
                    admission_running: true,
                    queue_reasons: vec![reason],
                },
                membership,
                0,
            ))
            .expect("stage latest state");
        publisher.flush().expect("publish latest state");
    }

    let latest = receiver.borrow().clone();
    assert_eq!(latest.generation(), 3);
    assert_eq!(latest.as_of_event_id().get(), 3);
    assert_eq!(
        latest.public_state().queue_reasons,
        vec![QueueReason::RepositoryControlBusy]
    );
}
