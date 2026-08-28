use std::ffi::{OsStr, OsString};
use std::fmt;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::time;
use tokio_util::sync::CancellationToken;

use crate::command_policy::{ExecutionDirectory, PinnedExecutable, ValidatedCommand};
use crate::process_liveness::ProcessLivenessDirectory;

use super::super::{
    ChildEnvironment, CommandResult, ProcessError, ProcessLimits, ProcessSupervisor,
};
use super::model::{ExactChildInput, MAX_EXACT_CHILD_INPUT_BYTES};

pub const MAX_EXACT_CHILD_INPUT_BYTES_FOR_TEST: usize = MAX_EXACT_CHILD_INPUT_BYTES;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum ProcessStdinTestScenario {
    NullStdin,
    Echo,
    NonzeroAfterRead,
    ExitBeforeRead,
    TimeoutWhileBlocked,
    CancelWhileBlocked,
}

#[derive(Debug)]
#[doc(hidden)]
pub enum ProcessStdinTestOutcome {
    RejectedBeforeSpawn,
    Completed(CommandResult),
    Failed(ProcessError),
}

#[doc(hidden)]
pub struct ProcessStdinTestObservation {
    outcome: ProcessStdinTestOutcome,
    received_bytes: Option<Vec<u8>>,
    input_debug: String,
    child_arguments: Vec<OsString>,
    tracked_tasks_after_shutdown: usize,
    spawned: bool,
}

impl ProcessStdinTestObservation {
    pub fn outcome(&self) -> &ProcessStdinTestOutcome {
        &self.outcome
    }

    pub fn received_bytes(&self) -> Option<&[u8]> {
        self.received_bytes.as_deref()
    }

    pub fn input_debug(&self) -> &str {
        &self.input_debug
    }

    pub fn child_arguments(&self) -> &[OsString] {
        &self.child_arguments
    }

    pub const fn tracked_tasks_after_shutdown(&self) -> usize {
        self.tracked_tasks_after_shutdown
    }

    pub const fn spawned(&self) -> bool {
        self.spawned
    }
}

impl fmt::Debug for ProcessStdinTestObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProcessStdinTestObservation")
            .field("outcome", &self.outcome)
            .field(
                "received_bytes",
                &self.received_bytes.as_ref().map(Vec::len),
            )
            .field("input", &"<redacted>")
            .field("child_argument_count", &self.child_arguments.len())
            .field(
                "tracked_tasks_after_shutdown",
                &self.tracked_tasks_after_shutdown,
            )
            .field("spawned", &self.spawned)
            .finish()
    }
}

#[doc(hidden)]
pub async fn exercise_process_stdin_for_test(
    scenario: ProcessStdinTestScenario,
    payload: Vec<u8>,
) -> ProcessStdinTestObservation {
    let child_arguments = fixture_child_arguments();
    let exact_input = if scenario == ProcessStdinTestScenario::NullStdin {
        None
    } else {
        match ExactChildInput::try_new(payload) {
            Ok(input) => Some(input),
            Err(error) => {
                return ProcessStdinTestObservation {
                    outcome: ProcessStdinTestOutcome::RejectedBeforeSpawn,
                    received_bytes: None,
                    input_debug: format!("{error:?}"),
                    child_arguments,
                    tracked_tasks_after_shutdown: 0,
                    spawned: false,
                };
            }
        }
    };
    let input_debug = exact_input.as_ref().map_or_else(
        || "ExactChildInput(None)".to_owned(),
        |input| format!("{input:?}"),
    );
    let fixture = ProcessStdinFixture::new();
    let cancellation = CancellationToken::new();
    let cancel_task = cancellation_task(scenario, cancellation.clone());
    let timeout = command_timeout(scenario);
    let environment = fixture.environment(scenario);
    let command = ValidatedCommand::process_stdin_fixture_for_test(
        Arc::new(
            PinnedExecutable::open(std::env::current_exe().expect("resolve fixture executable"))
                .expect("pin fixture executable"),
        ),
        Arc::new(
            ExecutionDirectory::open(
                std::env::current_dir().expect("resolve fixture working directory"),
            )
            .expect("open fixture working directory"),
        ),
        environment,
        timeout,
        exact_input,
    )
    .expect("construct fixed process-stdin fixture command");
    let limits = ProcessLimits::try_new(
        64 * 1024,
        64 * 1024,
        Duration::from_secs(5),
        Duration::from_secs(3),
    )
    .expect("construct process-stdin fixture limits");
    let supervisor = ProcessSupervisor::new(limits, fixture.liveness_scope.clone());
    let result = supervisor.run(command, cancellation).await;
    if let Some(cancel_task) = cancel_task {
        let _ = cancel_task.await;
    }
    supervisor.shutdown().await;
    let tracked_tasks_after_shutdown = supervisor.tasks.len();
    let spawned = fixture.marker.is_file();
    let received_bytes = fixture.capture.is_file().then(|| {
        std::fs::read(&fixture.capture).expect("read fixed process-stdin fixture capture")
    });
    let outcome = match result {
        Ok(result) => ProcessStdinTestOutcome::Completed(result),
        Err(error) => ProcessStdinTestOutcome::Failed(error),
    };
    drop(supervisor);
    fixture.remove();
    ProcessStdinTestObservation {
        outcome,
        received_bytes,
        input_debug,
        child_arguments,
        tracked_tasks_after_shutdown,
        spawned,
    }
}

fn cancellation_task(
    scenario: ProcessStdinTestScenario,
    cancellation: CancellationToken,
) -> Option<tokio::task::JoinHandle<()>> {
    (scenario == ProcessStdinTestScenario::CancelWhileBlocked).then(|| {
        tokio::spawn(async move {
            time::sleep(Duration::from_millis(75)).await;
            cancellation.cancel();
        })
    })
}

fn command_timeout(scenario: ProcessStdinTestScenario) -> Duration {
    if scenario == ProcessStdinTestScenario::TimeoutWhileBlocked {
        Duration::from_millis(75)
    } else {
        Duration::from_secs(5)
    }
}

fn fixture_child_arguments() -> Vec<OsString> {
    [
        OsStr::new("--ignored"),
        OsStr::new("--exact"),
        OsStr::new("process_stdin_fixture_child"),
    ]
    .into_iter()
    .map(OsStr::to_os_string)
    .collect()
}

fn fixture_mode(scenario: ProcessStdinTestScenario) -> &'static str {
    match scenario {
        ProcessStdinTestScenario::NullStdin | ProcessStdinTestScenario::Echo => "echo",
        ProcessStdinTestScenario::NonzeroAfterRead => "nonzero",
        ProcessStdinTestScenario::ExitBeforeRead => "early-exit",
        ProcessStdinTestScenario::TimeoutWhileBlocked
        | ProcessStdinTestScenario::CancelWhileBlocked => "stall",
    }
}

struct ProcessStdinFixture {
    root: PathBuf,
    marker: PathBuf,
    capture: PathBuf,
    liveness_scope: crate::process_liveness::ProcessLivenessScope,
}

impl ProcessStdinFixture {
    fn new() -> Self {
        let (root, mut identity) = create_fixture_directory();
        let liveness = root.join("liveness");
        std::fs::create_dir(&liveness).expect("create process-stdin liveness directory");
        identity[6] = (identity[6] & 0x0f) | 0x40;
        identity[8] = (identity[8] & 0x3f) | 0x80;
        let liveness_scope = ProcessLivenessDirectory::open(&liveness)
            .and_then(|directory| directory.instance_scope(identity))
            .expect("create process-stdin fixture liveness scope");
        Self {
            marker: root.join("spawned.marker"),
            capture: root.join("stdin.capture"),
            root,
            liveness_scope,
        }
    }

    fn environment(&self, scenario: ProcessStdinTestScenario) -> ChildEnvironment {
        let mut environment = ChildEnvironment::from_current_process()
            .expect("construct fixed process-stdin fixture environment");
        environment.entries.insert(
            OsString::from("CODING_AGENT_PROCESS_STDIN_FIXTURE_MODE"),
            OsString::from(fixture_mode(scenario)),
        );
        environment.entries.insert(
            OsString::from("CODING_AGENT_PROCESS_STDIN_FIXTURE_MARKER"),
            self.marker.as_os_str().to_owned(),
        );
        environment.entries.insert(
            OsString::from("CODING_AGENT_PROCESS_STDIN_FIXTURE_CAPTURE"),
            self.capture.as_os_str().to_owned(),
        );
        environment
    }

    fn remove(self) {
        drop(self.liveness_scope);
        std::fs::remove_dir_all(self.root).expect("remove process-stdin fixture directory");
    }
}

fn create_fixture_directory() -> (PathBuf, [u8; 16]) {
    for _ in 0..32 {
        let mut identity = [0u8; 16];
        getrandom::fill(&mut identity).expect("generate process-stdin fixture identity");
        let root = std::env::temp_dir().join(format!(
            "coding-agent-process-stdin-{}",
            encode_hex(&identity)
        ));
        match std::fs::create_dir(&root) {
            Ok(()) => return (root, identity),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => panic!("create process-stdin fixture directory: {error}"),
        }
    }
    panic!("could not allocate a unique process-stdin fixture directory")
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}
