use std::num::NonZeroU64;

use coding_agent_domain::{
    ActivityLevel, ClientRequestId, DeliveryReadiness, DiffSnapshot, PlanSnapshot, Task,
    TestSnapshot, TestStatus, UtcTimestamp,
};

use super::*;

fn mutation_identity(task_id: TaskId, kind: DurableOperationKind) -> TaskMutationIdentity {
    TaskMutationIdentity {
        task_id,
        sequence: MutationSequence::new(NonZeroU64::new(1).expect("test sequence is non-zero")),
        kind,
    }
}

fn stop_request(task_id: TaskId, repository_id: RepositoryId) -> StopIntentRequest {
    StopIntentRequest {
        task_id,
        expected_repository_id: repository_id,
        expected_attempt: 1,
        kind: StopIntentKind::UserCancelled,
    }
}

fn stop_receipt(task_id: TaskId, repository_id: RepositoryId) -> StopIntentReceipt {
    StopIntentReceipt {
        task_id,
        repository_id,
        attempt: 1,
        kind: StopIntentKind::UserCancelled,
        requested_at: UtcTimestamp::parse_rfc3339("2026-08-03T00:00:00Z")
            .expect("test timestamp is valid"),
    }
}

fn cancelled_task(task_id: TaskId, repository_id: RepositoryId) -> Task {
    let timestamp =
        UtcTimestamp::parse_rfc3339("2026-08-03T00:00:00Z").expect("test timestamp is valid");
    Task::try_from_stored(Task {
        id: task_id,
        client_request_id: ClientRequestId::new(),
        repository_id,
        prompt: "terminal panel drain fixture".to_owned(),
        status: TaskStatus::Cancelled,
        delivery_readiness: DeliveryReadiness::Unreviewed,
        attempt: 1,
        retry_of: None,
        created_at: timestamp,
        started_at: Some(timestamp),
        finished_at: Some(timestamp),
        last_event_id: EventId::new(1).expect("test event id is valid"),
        failure: None,
    })
    .expect("test terminal task is structurally valid")
}

fn terminal_panel_events() -> Vec<(RunnerEvent, bool)> {
    vec![
        (
            RunnerEvent::PlanUpdated(PlanSnapshot::legacy(1, Vec::new())),
            false,
        ),
        (
            RunnerEvent::ActivityAppended(ActivityEntry::legacy(
                "terminal-activity",
                ActivityLevel::Info,
                "terminal activity",
                UtcTimestamp::parse_rfc3339("2026-08-03T00:00:00Z")
                    .expect("test timestamp is valid"),
            )),
            true,
        ),
        (
            RunnerEvent::DiffUpdated(DiffSnapshot {
                revision: 1,
                files: Vec::new(),
            }),
            true,
        ),
        (
            RunnerEvent::TestUpdated(TestSnapshot {
                revision: 1,
                status: TestStatus::Cancelled,
                cases: Vec::new(),
            }),
            true,
        ),
    ]
}

fn terminal_panel_drain_states() -> Vec<ActiveStopState> {
    let task_id = TaskId::new();
    let repository_id = RepositoryId::new();
    let identity = mutation_identity(task_id, DurableOperationKind::PersistStopIntent);
    let request = stop_request(task_id, repository_id);
    let deadline = Instant::now() + Duration::from_secs(5);

    vec![
        ActiveStopState::IntentSubmissionDeferred {
            kind: request.kind,
            identity,
            request,
            deadline,
            retries_remaining: STOP_WRITE_RETRY_LIMIT,
        },
        ActiveStopState::IntentWritePending {
            kind: request.kind,
            identity,
            request,
            deadline,
            retries_remaining: STOP_WRITE_RETRY_LIMIT,
        },
        ActiveStopState::IntentDurable {
            identity,
            receipt: stop_receipt(task_id, repository_id),
        },
    ]
}

#[test]
fn terminal_panel_drain_accepts_only_activity_diff_and_test_events() {
    for (state_index, stop_state) in terminal_panel_drain_states().into_iter().enumerate() {
        for (event, expected) in terminal_panel_events() {
            assert_eq!(
                runner_event_is_allowed_during_stop(&stop_state, &event),
                expected,
                "unexpected admission for stop stage {state_index} and {event:?}"
            );
        }
    }
}

#[test]
fn no_winner_accepts_every_runner_event_and_terminal_states_reject_every_runner_event() {
    let task_id = TaskId::new();
    let repository_id = RepositoryId::new();
    let receipt = stop_receipt(task_id, repository_id);
    let terminal = cancelled_task(task_id, repository_id);
    let closed_states = vec![
        ActiveStopState::FinalStopWritePending {
            kind: receipt.kind,
            receipt: Some(receipt),
            identity: mutation_identity(task_id, DurableOperationKind::FinalizeStoppedTask),
            request: FinalizeStoppedTaskRequest {
                task_id,
                expected_repository_id: repository_id,
                expected_attempt: 1,
                expected_intent: receipt.kind,
            },
            deadline: Instant::now() + Duration::from_secs(5),
            retries_remaining: STOP_WRITE_RETRY_LIMIT,
        },
        ActiveStopState::StopTerminal {
            receipt,
            task: terminal.clone(),
            terminal_event_id: terminal.last_event_id,
        },
        ActiveStopState::TerminalWon { task: terminal },
    ];

    for (event, _) in terminal_panel_events() {
        assert!(runner_event_is_allowed_during_stop(
            &ActiveStopState::NoWinner,
            &event
        ));
    }
    for closed_state in closed_states {
        for (event, _) in terminal_panel_events() {
            assert!(!runner_event_is_allowed_during_stop(&closed_state, &event));
        }
    }
}
