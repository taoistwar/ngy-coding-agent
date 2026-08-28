use std::ffi::OsStr;
use std::io::Read as _;
use std::time::Duration;

use coding_agent_runtime::{
    MAX_EXACT_CHILD_INPUT_BYTES_FOR_TEST, ProcessError, ProcessStdinTestOutcome,
    ProcessStdinTestScenario, exercise_process_stdin_for_test,
};

const FIXTURE_MODE: &str = "CODING_AGENT_PROCESS_STDIN_FIXTURE_MODE";
const FIXTURE_MARKER: &str = "CODING_AGENT_PROCESS_STDIN_FIXTURE_MARKER";
const FIXTURE_CAPTURE: &str = "CODING_AGENT_PROCESS_STDIN_FIXTURE_CAPTURE";

#[test]
#[ignore = "invoked only as the fixed process-stdin fixture child"]
fn process_stdin_fixture_child() {
    let mode = std::env::var(FIXTURE_MODE).expect("fixed fixture mode");
    let marker = std::env::var_os(FIXTURE_MARKER).expect("fixed fixture marker");
    std::fs::write(marker, b"spawned").expect("record fixture spawn");

    match mode.as_str() {
        "echo" | "nonzero" => {
            let mut bytes = Vec::new();
            std::io::stdin()
                .read_to_end(&mut bytes)
                .expect("read exact fixture stdin through EOF");
            let capture = std::env::var_os(FIXTURE_CAPTURE).expect("fixed fixture capture");
            std::fs::write(capture, bytes).expect("persist exact fixture input");
            if mode == "nonzero" {
                std::process::exit(7);
            }
        }
        "early-exit" => {
            close_fixture_stdin();
            std::thread::sleep(Duration::from_millis(50));
            std::process::exit(9);
        }
        "stall" => std::thread::sleep(Duration::from_secs(30)),
        other => panic!("unknown fixed process-stdin fixture mode: {other}"),
    }
}

#[tokio::test]
async fn default_command_still_receives_null_stdin_without_an_input_writer() {
    let observation =
        exercise_process_stdin_for_test(ProcessStdinTestScenario::NullStdin, Vec::new()).await;

    let ProcessStdinTestOutcome::Completed(result) = observation.outcome() else {
        panic!("null-stdin fixture failed: {:?}", observation.outcome());
    };
    assert_eq!(result.exit_code, Some(0));
    assert!(!result.timed_out);
    assert!(!result.cancelled);
    assert_eq!(observation.received_bytes(), Some([].as_slice()));
    assert_eq!(observation.tracked_tasks_after_shutdown(), 0);
    assert!(observation.spawned());
}

#[tokio::test]
async fn exact_input_bound_is_inclusive_and_oversize_is_rejected_before_spawn() {
    let at_limit = vec![0x5a; MAX_EXACT_CHILD_INPUT_BYTES_FOR_TEST];
    let accepted =
        exercise_process_stdin_for_test(ProcessStdinTestScenario::Echo, at_limit.clone()).await;
    assert!(matches!(
        accepted.outcome(),
        ProcessStdinTestOutcome::Completed(result) if result.exit_code == Some(0)
    ));
    assert_eq!(accepted.received_bytes(), Some(at_limit.as_slice()));
    assert_eq!(accepted.tracked_tasks_after_shutdown(), 0);

    let rejected = exercise_process_stdin_for_test(
        ProcessStdinTestScenario::Echo,
        vec![0x5a; MAX_EXACT_CHILD_INPUT_BYTES_FOR_TEST + 1],
    )
    .await;
    assert!(matches!(
        rejected.outcome(),
        ProcessStdinTestOutcome::RejectedBeforeSpawn
    ));
    assert!(!rejected.spawned());
    assert_eq!(rejected.tracked_tasks_after_shutdown(), 0);
}

#[tokio::test]
async fn binary_input_arrives_byte_exact_only_after_the_writer_closes_stdin() {
    let payload = vec![0x00, 0xff, b'\n', b'\r', 0x80, b'a', 0x00, b'z'];
    let observation =
        exercise_process_stdin_for_test(ProcessStdinTestScenario::Echo, payload.clone()).await;

    assert!(matches!(
        observation.outcome(),
        ProcessStdinTestOutcome::Completed(result) if result.exit_code == Some(0)
    ));
    assert_eq!(observation.received_bytes(), Some(payload.as_slice()));
    assert_eq!(observation.tracked_tasks_after_shutdown(), 0);
}

#[tokio::test]
async fn commit_message_and_ref_transaction_shapes_arrive_byte_exact() {
    let inputs = [
        b"coding-agent: merge task 018f attempt 2\n\nreviewed\n".as_slice(),
        b"start\nverify refs/heads/main 1111111111111111111111111111111111111111\ndelete refs/heads/codex/task 2222222222222222222222222222222222222222\nprepare\ncommit\n"
            .as_slice(),
    ];
    for payload in inputs {
        let observation =
            exercise_process_stdin_for_test(ProcessStdinTestScenario::Echo, payload.to_vec()).await;
        assert!(matches!(
            observation.outcome(),
            ProcessStdinTestOutcome::Completed(result) if result.exit_code == Some(0)
        ));
        assert_eq!(observation.received_bytes(), Some(payload));
    }
}

#[tokio::test]
async fn completed_input_followed_by_nonzero_exit_remains_a_normal_command_result() {
    let payload = b"fully-written-before-exit-seven\n".to_vec();
    let observation = exercise_process_stdin_for_test(
        ProcessStdinTestScenario::NonzeroAfterRead,
        payload.clone(),
    )
    .await;

    assert!(matches!(
        observation.outcome(),
        ProcessStdinTestOutcome::Completed(result)
            if result.exit_code == Some(7) && !result.timed_out && !result.cancelled
    ));
    assert_eq!(observation.received_bytes(), Some(payload.as_slice()));
    assert_eq!(observation.tracked_tasks_after_shutdown(), 0);
}

#[tokio::test]
async fn early_child_exit_is_a_typed_input_failure_and_never_a_plain_nonzero_result() {
    let payload = vec![0x6b; MAX_EXACT_CHILD_INPUT_BYTES_FOR_TEST];
    let observation =
        exercise_process_stdin_for_test(ProcessStdinTestScenario::ExitBeforeRead, payload).await;

    assert!(
        matches!(
            observation.outcome(),
            ProcessStdinTestOutcome::Failed(ProcessError::InputClosedEarly)
        ),
        "unexpected early-exit outcome: {observation:?}"
    );
    assert_eq!(observation.tracked_tasks_after_shutdown(), 0);
    assert!(observation.spawned());
}

#[tokio::test]
async fn timeout_and_cancel_win_over_their_induced_broken_pipe_and_join_the_writer() {
    let payload = vec![0x71; MAX_EXACT_CHILD_INPUT_BYTES_FOR_TEST];
    let timed_out = exercise_process_stdin_for_test(
        ProcessStdinTestScenario::TimeoutWhileBlocked,
        payload.clone(),
    )
    .await;
    assert!(matches!(
        timed_out.outcome(),
        ProcessStdinTestOutcome::Completed(result)
            if result.timed_out && !result.cancelled
    ));
    assert_eq!(timed_out.tracked_tasks_after_shutdown(), 0);

    let cancelled =
        exercise_process_stdin_for_test(ProcessStdinTestScenario::CancelWhileBlocked, payload)
            .await;
    assert!(matches!(
        cancelled.outcome(),
        ProcessStdinTestOutcome::Completed(result)
            if result.cancelled && !result.timed_out
    ));
    assert_eq!(cancelled.tracked_tasks_after_shutdown(), 0);
}

#[tokio::test]
async fn input_bytes_are_absent_from_debug_errors_and_child_arguments() {
    let sentinel = "task9-input-secret-9f0b52b4";
    let observation = exercise_process_stdin_for_test(
        ProcessStdinTestScenario::Echo,
        sentinel.as_bytes().to_vec(),
    )
    .await;

    assert!(!observation.input_debug().contains(sentinel));
    assert!(!format!("{observation:?}").contains(sentinel));
    assert!(
        observation
            .child_arguments()
            .iter()
            .all(|argument| !os_contains(argument, sentinel))
    );

    let early = exercise_process_stdin_for_test(
        ProcessStdinTestScenario::ExitBeforeRead,
        vec![0x73; MAX_EXACT_CHILD_INPUT_BYTES_FOR_TEST],
    )
    .await;
    assert!(!format!("{:?}", early.outcome()).contains(sentinel));
    assert_eq!(early.tracked_tasks_after_shutdown(), 0);
}

fn os_contains(value: &OsStr, needle: &str) -> bool {
    value.to_string_lossy().contains(needle)
}

#[cfg(unix)]
fn close_fixture_stdin() {
    assert_eq!(unsafe { libc::close(libc::STDIN_FILENO) }, 0);
}

#[cfg(windows)]
fn close_fixture_stdin() {
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE};

    const STD_INPUT_HANDLE: u32 = (-10_i32) as u32;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetStdHandle(n_std_handle: u32) -> HANDLE;
    }

    let handle = unsafe { GetStdHandle(STD_INPUT_HANDLE) };
    assert!(!handle.is_null());
    assert_ne!(handle, INVALID_HANDLE_VALUE);
    assert_ne!(unsafe { CloseHandle(handle) }, 0);
}
