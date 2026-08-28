use std::io::Write;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use tempfile::TempDir;
use tokio::io::ReadBuf;

#[cfg(unix)]
use super::supervision::complete_liveness_eventually;
use super::*;
#[cfg(unix)]
use crate::command_policy::{
    DeliveryGitMutationCommandFactory, DeliveryGitSourceMutationBinding,
    DeliveryGitTemporaryIndexEnvironment, ExecutionDirectory, GitCommandBinding, PinnedExecutable,
};
#[cfg(unix)]
use crate::delivery::command::DeliveryGitReadOnlyBinding;
use crate::{ProcessCleanupProof, ProcessLivenessDirectory};

const HELPER_ENV: &str = "CODING_AGENT_PROCESS_HELPER";
const HELPER_PID_FILE: &str = "CODING_AGENT_PROCESS_HELPER_PID_FILE";
const HELPER_LEADER_PID_FILE: &str = "CODING_AGENT_PROCESS_HELPER_LEADER_PID_FILE";
const HELPER_RELEASE_FILE: &str = "CODING_AGENT_PROCESS_HELPER_RELEASE_FILE";
const HELPER_RUNTIME_DIRECTORY: &str = "CODING_AGENT_PROCESS_HELPER_RUNTIME_DIRECTORY";
const HELPER_TEST: &str = "process_supervisor::tests::process_helper_entrypoint";

#[test]
fn child_start_truth_table_distinguishes_only_proven_pre_spawn_failures() {
    let proven_not_started = [
        ProcessError::InvalidCommand,
        ProcessError::CommandPolicy(CommandPolicyError::InvalidTimeout),
        ProcessError::TimeoutOutsideLimit,
        ProcessError::SpawnFailed(io::Error::other("injected")),
        ProcessError::TreeSetupFailed(io::Error::other("injected")),
        ProcessError::LivenessSetupFailed(ProcessLivenessError::Unavailable),
    ];
    for error in proven_not_started {
        assert!(error.child_could_not_have_started(), "{error:?}");
    }

    let child_may_have_started = [
        ProcessError::MissingOutputPipe,
        ProcessError::MissingInputPipe,
        ProcessError::InputClosedEarly,
        ProcessError::InputWriteFailed(io::Error::other("injected")),
        ProcessError::InputCloseFailed(io::Error::other("injected")),
        ProcessError::InputCompletionUnknown,
        ProcessError::WaitFailed(io::Error::other("injected")),
        ProcessError::TreeControlLost(io::Error::other("injected")),
        ProcessError::TreeCleanupFailed(io::Error::other("injected")),
        ProcessError::CleanupTimedOut,
        ProcessError::LivenessCleanupUnproven,
        ProcessError::LivenessCleanupFailed(ProcessLivenessError::Unavailable),
        ProcessError::OutputDrainFailed(io::Error::other("injected")),
        ProcessError::WorkerFailed,
    ];
    for error in child_may_have_started {
        assert!(!error.child_could_not_have_started(), "{error:?}");
    }
}

#[test]
fn process_helper_entrypoint() {
    let Some(mode) = std::env::var_os(HELPER_ENV) else {
        return;
    };
    match mode.to_string_lossy().as_ref() {
        "split" => {
            print!("stdout-marker");
            eprint!("stderr-marker");
            flush_standard_streams();
            std::process::exit(0);
        }
        "exit-7" => std::process::exit(7),
        "flood" => {
            let stdout = std::thread::spawn(|| {
                let mut output = std::io::stdout().lock();
                output.write_all(b"stdout-head|").unwrap();
                output.write_all(&vec![b'o'; 256 * 1_024]).unwrap();
                output.write_all(b"|stdout-tail").unwrap();
                output.flush().unwrap();
            });
            let stderr = std::thread::spawn(|| {
                let mut output = std::io::stderr().lock();
                output.write_all(b"stderr-head|").unwrap();
                output.write_all(&vec![b'e'; 256 * 1_024]).unwrap();
                output.write_all(b"|stderr-tail").unwrap();
                output.flush().unwrap();
            });
            stdout.join().unwrap();
            stderr.join().unwrap();
            std::process::exit(0);
        }
        "binary" => {
            std::io::stdout()
                .write_all(&[0xff, 0x00, 0xfe, 0x7f])
                .unwrap();
            std::io::stderr().write_all(&[0x80, 0x81, 0x00]).unwrap();
            flush_standard_streams();
            std::process::exit(0);
        }
        "sleep" => {
            write_helper_pid();
            std::thread::sleep(Duration::from_secs(60));
            std::process::exit(0);
        }
        "leader" | "leader-closed-pipe" | "leader-sleep" | "leader-release" => {
            write_optional_helper_pid(HELPER_LEADER_PID_FILE);
            let mut child = std::process::Command::new(std::env::current_exe().unwrap());
            child.args(["--exact", HELPER_TEST, "--nocapture"]).env(
                HELPER_ENV,
                if mode == "leader-release" {
                    "grandchild-release"
                } else {
                    "grandchild"
                },
            );
            if mode == "leader-closed-pipe" {
                child.stdout(Stdio::null()).stderr(Stdio::null());
            }
            child.spawn().unwrap();
            wait_for_helper_pid_sync();
            if mode == "leader-sleep" {
                std::thread::sleep(Duration::from_secs(60));
            }
            std::process::exit(0);
        }
        "grandchild" | "grandchild-release" => {
            write_helper_pid();
            println!("grandchild-ready");
            std::io::stdout().flush().unwrap();
            if mode == "grandchild-release" {
                wait_for_release_file_sync();
                std::process::exit(0);
            }
            std::thread::sleep(Duration::from_secs(60));
            std::process::exit(0);
        }
        "primary-crash" => {
            let runtime_directory = PathBuf::from(
                std::env::var_os(HELPER_RUNTIME_DIRECTORY)
                    .expect("primary-crash runtime directory is configured"),
            );
            let pid_file = PathBuf::from(
                std::env::var_os(HELPER_PID_FILE)
                    .expect("primary-crash grandchild pid file is configured"),
            );
            let release_file = PathBuf::from(
                std::env::var_os(HELPER_RELEASE_FILE)
                    .expect("primary-crash release file is configured"),
            );
            let leader_pid_file = runtime_directory.join("primary-crash-leader-pid");
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                let directory = ProcessLivenessDirectory::open(&runtime_directory).unwrap();
                let task = directory
                    .instance_scope(test_uuid(41))
                    .unwrap()
                    .task_scope(test_uuid(42))
                    .unwrap();
                let limits = ProcessLimits::try_new(
                    1_024,
                    1_024,
                    Duration::from_secs(10),
                    Duration::from_secs(2),
                )
                .unwrap();
                let (supervisor, _gate) = ProcessSupervisor::new_paused_for_test(limits, task);
                let command = helper_command_for_directory(
                    "leader-release",
                    &runtime_directory,
                    &pid_file,
                    Some(&leader_pid_file),
                    Some(&release_file),
                    Duration::from_secs(10),
                );
                let _execution = supervisor
                    .start(command, None, CancellationToken::new())
                    .await
                    .unwrap();
                wait_for_helper_pid(&pid_file).await;
                let leader_id = wait_for_helper_pid(&leader_pid_file).await;
                wait_until_process_not_running(leader_id).await;
                std::process::exit(0);
            });
        }
        "environment" => {
            for key in [
                "CODING_AGENT_SENTINEL_SECRET",
                "OPENAI_API_KEY",
                "HTTP_PROXY",
                "HTTPS_PROXY",
                "ALL_PROXY",
                "NO_PROXY",
                "SSH_AUTH_SOCK",
                "GITHUB_TOKEN",
                "CI_JOB_TOKEN",
                "AWS_SECRET_ACCESS_KEY",
                "AZURE_CLIENT_SECRET",
                "GOOGLE_APPLICATION_CREDENTIALS",
                "GIT_ASKPASS",
                "SSH_ASKPASS",
                "GIT_EDITOR",
                "GIT_SEQUENCE_EDITOR",
                "EDITOR",
                "VISUAL",
                "CARGO_REGISTRY_TOKEN",
                "CARGO_BUILD_JOBS",
                "RUST_TEST_THREADS",
                "RUSTC_WRAPPER",
                "RUSTFLAGS",
                "LD_PRELOAD",
                "DYLD_INSERT_LIBRARIES",
            ] {
                println!("{key}={}", usize::from(std::env::var_os(key).is_some()));
            }
            println!(
                "CARGO_NET_OFFLINE={}",
                std::env::var("CARGO_NET_OFFLINE").unwrap_or_default()
            );
            flush_standard_streams();
            std::process::exit(0);
        }
        unexpected => panic!("unknown helper mode {unexpected}"),
    }
}

#[cfg(target_os = "macos")]
#[test]
fn macos_uses_the_validated_executable_path_instead_of_dev_fd() {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("pinned-tool");
    std::fs::write(&path, b"deterministic test image").unwrap();
    let path = std::fs::canonicalize(path).unwrap();
    let file = File::open(&path).unwrap();

    let executable = platform::Executable::new(&path, file).unwrap();

    assert_eq!(executable.program(), path);
    assert!(!executable.program().starts_with("/dev/fd"));
}

#[cfg(unix)]
#[test]
fn delivery_directory_and_config_slots_materialize_only_to_inherited_descriptors() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().canonicalize().unwrap();
    let git_path = root.join("git-directory");
    let work_tree_path = root.join("work-tree");
    let common_git_path = root.join("common-git");
    let sandbox_path = root.join("sandbox");
    for directory in [&git_path, &work_tree_path, &common_git_path, &sandbox_path] {
        std::fs::create_dir(directory).unwrap();
    }
    let config_path = sandbox_path.join(".coding-agent-empty-gitconfig");
    std::fs::write(&config_path, b"").unwrap();
    let config_file = File::open(&config_path).unwrap();
    std::fs::remove_file(&config_path).unwrap();
    let git_directory = Arc::new(ExecutionDirectory::open(&git_path).unwrap());
    let work_tree = Arc::new(ExecutionDirectory::open(&work_tree_path).unwrap());
    let repository = GitCommandBinding::try_new(git_directory, Arc::clone(&work_tree)).unwrap();
    let sandbox = Arc::new(ExecutionDirectory::open(&sandbox_path).unwrap());
    let config = Arc::new(
        crate::command_policy::DeliveryGitEmptyConfig::from_retained_sandbox_file(
            Arc::clone(&sandbox),
            config_file,
        )
        .unwrap(),
    );
    let mut environment = ChildEnvironment::default();
    environment.insert_test_value("GIT_CONFIG_GLOBAL", "<coding-agent-delivery-empty-config>");
    environment.insert_test_value("GIT_CONFIG_SYSTEM", "<coding-agent-delivery-empty-config>");
    let binding = DeliveryGitReadOnlyBinding {
        git: Arc::new(PinnedExecutable::open(std::env::current_exe().unwrap()).unwrap()),
        repository,
        common_git: Arc::new(ExecutionDirectory::open(&common_git_path).unwrap()),
        sandbox,
        config,
        environment,
        timeout: Duration::from_secs(10),
    };
    let command = ValidatedCommand::delivery_resolve_head(&binding).unwrap();
    let materialized = platform::DeliveryDescriptorArguments::try_new(&command).unwrap();
    let arguments = materialized
        .arguments()
        .iter()
        .map(|argument| argument.to_string_lossy().into_owned())
        .collect::<Vec<_>>();

    for forbidden in [&git_path, &work_tree_path, &common_git_path, &sandbox_path] {
        let forbidden = forbidden.to_string_lossy();
        assert!(
            arguments
                .iter()
                .all(|argument| !argument.contains(forbidden.as_ref()))
        );
        assert!(
            materialized
                .environment()
                .iter()
                .all(|(_, value)| !value.to_string_lossy().contains(forbidden.as_ref()))
        );
    }
    assert!(arguments[5].starts_with("--git-dir="));
    assert!(arguments[5].contains("/fd/"));
    assert_eq!(arguments[6], "--work-tree=.");
    assert!(
        arguments
            .iter()
            .any(|argument| argument == "core.hooksPath=/dev/null")
    );
    assert_eq!(materialized.environment().len(), 3);
    assert_eq!(materialized.environment()[0].0, "GIT_COMMON_DIR");
    assert!(
        materialized.environment()[0]
            .1
            .to_string_lossy()
            .contains("/fd/")
    );
    for key in ["GIT_CONFIG_GLOBAL", "GIT_CONFIG_SYSTEM"] {
        let value = materialized
            .environment()
            .iter()
            .find_map(|(actual, value)| (actual == key).then_some(value))
            .unwrap();
        assert!(value.to_string_lossy().contains("/fd/"));
        assert!(
            !value
                .to_string_lossy()
                .contains(&sandbox_path.to_string_lossy()[..])
        );
    }
    assert_eq!(materialized.inherited_resource_count(), 3);
    assert!(!config_path.exists());
    let descriptor_config = materialized
        .environment()
        .iter()
        .find_map(|(key, value)| (key == "GIT_CONFIG_GLOBAL").then_some(value))
        .unwrap();
    assert_eq!(std::fs::read(descriptor_config).unwrap(), b"");

    let temporary_index_path = root.join("temporary-index");
    std::fs::create_dir(&temporary_index_path).unwrap();
    let temporary_index = DeliveryGitTemporaryIndexEnvironment::try_new(Arc::new(
        ExecutionDirectory::open(&temporary_index_path).unwrap(),
    ))
    .unwrap();
    let source_mutations = DeliveryGitSourceMutationBinding::try_new(
        DeliveryGitMutationCommandFactory::from_authorized_for_test(Arc::clone(&binding.git)),
        &binding,
        40,
    )
    .unwrap();
    let temporary_index_command = source_mutations.write_tree(&temporary_index).unwrap();
    let temporary_index_materialized =
        platform::DeliveryDescriptorArguments::try_new(&temporary_index_command).unwrap();
    let descriptor_index = temporary_index_materialized
        .environment()
        .iter()
        .find_map(|(key, value)| (key == "GIT_INDEX_FILE").then_some(value))
        .unwrap();
    let descriptor_index = std::path::PathBuf::from(descriptor_index);
    assert!(descriptor_index.to_string_lossy().contains("/fd/"));
    assert!(descriptor_index.ends_with("index"));
    assert!(
        !descriptor_index
            .to_string_lossy()
            .contains(&temporary_index_path.to_string_lossy()[..])
    );
    std::fs::write(&descriptor_index, b"index").unwrap();
    std::fs::write(
        descriptor_index.parent().unwrap().join("index.lock"),
        b"lock",
    )
    .unwrap();
    assert_eq!(
        std::fs::read(temporary_index_path.join("index")).unwrap(),
        b"index"
    );
    assert_eq!(
        std::fs::read(temporary_index_path.join("index.lock")).unwrap(),
        b"lock"
    );
    assert_eq!(temporary_index_materialized.inherited_resource_count(), 4);
}

#[cfg(target_os = "macos")]
#[test]
fn macos_only_uses_the_exited_tree_kill_path_after_observed_leader_exit() {
    assert!(should_use_exited_tree_kill(&ObservedTermination::Exited(
        None
    )));
    assert!(!should_use_exited_tree_kill(
        &ObservedTermination::Cancelled
    ));
    assert!(!should_use_exited_tree_kill(&ObservedTermination::TimedOut));
    assert!(!should_use_exited_tree_kill(
        &ObservedTermination::WaitFailed(io::Error::other("wait failed"))
    ));
}

#[cfg(target_os = "macos")]
#[test]
fn macos_only_accepts_eof_after_an_exited_tree_kill_returns_eperm() {
    assert!(
        reconcile_exited_tree_kill(Err(io::Error::from_raw_os_error(libc::EPERM)), || Ok(true))
            .is_ok()
    );

    let writers_remain =
        reconcile_exited_tree_kill(Err(io::Error::from_raw_os_error(libc::EPERM)), || Ok(false))
            .unwrap_err();
    assert_eq!(writers_remain.raw_os_error(), Some(libc::EPERM));

    let probe_failed =
        reconcile_exited_tree_kill(Err(io::Error::from_raw_os_error(libc::EPERM)), || {
            Err(io::Error::from_raw_os_error(libc::EIO))
        })
        .unwrap_err();
    assert_eq!(probe_failed.raw_os_error(), Some(libc::EIO));

    let non_eperm =
        reconcile_exited_tree_kill(Err(io::Error::from_raw_os_error(libc::EINVAL)), || {
            panic!("non-EPERM failures must not probe liveness")
        })
        .unwrap_err();
    assert_eq!(non_eperm.raw_os_error(), Some(libc::EINVAL));

    reconcile_exited_tree_kill(Ok(()), || {
        panic!("successful kills must not probe liveness")
    })
    .unwrap();
}

#[cfg(target_os = "macos")]
#[test]
fn macos_liveness_probe_distinguishes_live_writers_from_eof() {
    let (read, write) = platform::create_liveness_pipe().unwrap();

    assert!(!platform::liveness_pipe_has_no_writers(&read).unwrap());
    drop(write);
    assert!(platform::liveness_pipe_has_no_writers(&read).unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attach_fault_retains_the_sentinel_until_tree_proof() {
    assert_fault_retains_sentinel_until_tree_proof(SupervisionFault::AttachAndResume, 51).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn anchor_loss_retains_the_sentinel_until_tree_proof() {
    assert_fault_retains_sentinel_until_tree_proof(SupervisionFault::AnchorLost, 52).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill_failure_retains_the_sentinel_until_tree_proof() {
    assert_fault_retains_sentinel_until_tree_proof(SupervisionFault::KillNow, 53).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attached_cleanup_wait_failure_retains_the_sentinel_until_tree_proof() {
    assert_fault_retains_sentinel_until_tree_proof(
        SupervisionFault::AttachedCleanupWaitAfterReap,
        54,
    )
    .await;
}

async fn assert_fault_retains_sentinel_until_tree_proof(fault: SupervisionFault, seed: u8) {
    let temp = tempfile::tempdir().unwrap();
    let directory = ProcessLivenessDirectory::open(temp.path().canonicalize().unwrap()).unwrap();
    let instance = directory.instance_scope(test_uuid(seed)).unwrap();
    let task = instance
        .task_scope(test_uuid(seed.wrapping_add(32)))
        .unwrap();
    let limits = ProcessLimits::try_new(
        1_024,
        1_024,
        Duration::from_secs(10),
        Duration::from_secs(2),
    )
    .unwrap();
    let (supervisor, fault_gate, proof_continuation_started) =
        ProcessSupervisor::new_faulted_paused_for_test(limits, task.clone(), fault);
    let pid_file = temp.path().join(format!("{fault:?}-grandchild-pid"));
    let leader_pid_file = temp.path().join(format!("{fault:?}-leader-pid"));
    let release_file = temp.path().join(format!("{fault:?}-release"));
    let running_supervisor = supervisor.clone();
    let running = tokio::spawn({
        let command = helper_command_with_tree_files(
            "leader-release",
            &temp,
            &pid_file,
            Some(&leader_pid_file),
            Some(&release_file),
            Duration::from_secs(10),
        );
        async move {
            running_supervisor
                .run(command, CancellationToken::new())
                .await
        }
    });
    let process_id = wait_for_helper_pid(&pid_file).await;
    let leader_id = wait_for_helper_pid(&leader_pid_file).await;
    wait_until_process_not_running(leader_id).await;

    assert!(
        process_is_running(process_id),
        "{fault:?} requires a live held grandchild before injection"
    );
    assert_eq!(task.active_tree_count(), 1);
    assert_eq!(task.cleanup_proof().unwrap(), ProcessCleanupProof::Held);

    fault_gate.notify_one();
    time::timeout(
        Duration::from_secs(3),
        proof_continuation_started.notified(),
    )
    .await
    .expect("fault path must enter its proof continuation before release");
    assert!(
        process_is_running(process_id),
        "{fault:?} must not kill the proof-gating grandchild"
    );
    assert_eq!(
        task.active_tree_count(),
        1,
        "{fault:?} must retain its in-memory sentinel registration before tree proof"
    );
    assert_eq!(
        task.cleanup_proof().unwrap(),
        ProcessCleanupProof::Held,
        "{fault:?} must retain the OS-held sentinel before tree proof"
    );
    assert_eq!(
        std::fs::read_dir(temp.path().join("process-liveness"))
            .unwrap()
            .count(),
        1,
        "{fault:?} must retain exactly one sentinel before tree proof"
    );

    std::fs::write(&release_file, b"release").unwrap();
    let error = time::timeout(Duration::from_secs(5), running)
        .await
        .expect("faulted supervision must return after tree proof")
        .unwrap()
        .unwrap_err();
    assert!(
        error.process_cleanup_is_unproven(),
        "{fault:?} must remain a cleanup-unproven error"
    );
    wait_until_process_gone(process_id).await;

    time::timeout(Duration::from_secs(3), async {
        while task.active_tree_count() != 0 {
            time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("the spawned sentinel owner must finish through tracked retry");
    time::timeout(Duration::from_secs(3), supervisor.shutdown())
        .await
        .expect("tracked sentinel retry must join during supervisor shutdown");
    assert_eq!(
        task.cleanup_proof().unwrap(),
        ProcessCleanupProof::Confirmed
    );
    assert_eq!(
        std::fs::read_dir(temp.path().join("process-liveness"))
            .unwrap()
            .count(),
        0,
        "the completed tracked retry must remove exactly its stale sentinel"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn separates_streams_reports_nonzero_and_drains_dual_pipe_floods() {
    let temp = tempfile::tempdir().unwrap();
    let supervisor = supervisor(512, Duration::from_secs(5));

    let split = supervisor
        .run(
            helper_command("split", &temp, Duration::from_secs(2)),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(split.exit_code, Some(0));
    assert!(contains(&split.stdout.head, b"stdout-marker"));
    assert!(!contains(&split.stdout.head, b"stderr-marker"));
    assert!(contains(&split.stderr.head, b"stderr-marker"));

    let nonzero = supervisor
        .run(
            helper_command("exit-7", &temp, Duration::from_secs(2)),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(nonzero.exit_code, Some(7));
    assert!(!nonzero.timed_out);
    assert!(!nonzero.cancelled);

    let flood = supervisor
        .run(
            helper_command("flood", &temp, Duration::from_secs(5)),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert!(flood.truncated);
    assert!(flood.stdout.observed_bytes > 256 * 1_024);
    assert!(flood.stderr.observed_bytes > 256 * 1_024);
    assert!(contains(&flood.stdout.head, b"stdout-head|"));
    assert!(flood.stdout.tail.ends_with(b"|stdout-tail"));
    assert!(contains(&flood.stderr.head, b"stderr-head|"));
    assert!(flood.stderr.tail.ends_with(b"|stderr-tail"));

    let binary = supervisor
        .run(
            helper_command("binary", &temp, Duration::from_secs(2)),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert!(contains(&binary.stdout.head, &[0xff, 0x00, 0xfe, 0x7f]));
    assert!(contains(&binary.stderr.head, &[0x80, 0x81, 0x00]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn task_scoped_process_faults_record_order_and_prove_zero_live() {
    use faults::{ProcessFault, ProcessFaultEventKind};

    let faults = [
        ProcessFault::BeforeSpawn,
        ProcessFault::AfterSpawnUnknown,
        ProcessFault::StdoutOverflow,
        ProcessFault::Deadline,
        ProcessFault::WaitUnknown,
        ProcessFault::ChannelUnknown,
        ProcessFault::KillFailure,
        ProcessFault::CleanupFailure,
    ];

    for (offset, fault) in faults.into_iter().enumerate() {
        let temp = tempfile::tempdir().unwrap();
        let supervisor = fault_supervisor(&temp, 80 + offset as u8);
        let controller = faults::ProcessFaultController::for_child(1, fault).unwrap();
        let outcome = controller
            .scope(supervisor.run(
                helper_command("split", &temp, Duration::from_secs(2)),
                CancellationToken::new(),
            ))
            .await;

        match (fault, outcome) {
            (ProcessFault::BeforeSpawn, Err(ProcessError::SpawnFailed(_)))
            | (ProcessFault::AfterSpawnUnknown, Err(ProcessError::InputCompletionUnknown))
            | (ProcessFault::WaitUnknown, Err(ProcessError::WaitFailed(_)))
            | (ProcessFault::ChannelUnknown, Err(ProcessError::WaitFailed(_)))
            | (
                ProcessFault::KillFailure | ProcessFault::CleanupFailure,
                Err(ProcessError::TreeCleanupFailed(_)),
            ) => {}
            (ProcessFault::StdoutOverflow, Ok(result)) => {
                assert!(result.truncated);
                assert!(result.stdout.truncated);
                assert!(!result.stdout.complete);
                assert!(result.stdout.observed_bytes > 512);
            }
            (ProcessFault::Deadline, Ok(result)) => {
                assert!(result.timed_out);
                assert!(!result.cancelled);
            }
            (fault, outcome) => panic!("unexpected {fault:?} outcome: {outcome:?}"),
        }

        time::timeout(Duration::from_secs(5), supervisor.shutdown())
            .await
            .expect("fault cleanup tasks must finish");
        let proof = controller
            .prove_zero_live(Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(proof.observed_children(), 1);
        assert_eq!(proof.checked_scopes(), 1);
        assert_eq!(
            controller
                .events()
                .into_iter()
                .map(|event| (event.child_ordinal(), event.kind()))
                .collect::<Vec<_>>(),
            vec![
                (1, ProcessFaultEventKind::Admitted),
                (1, ProcessFaultEventKind::Injected(fault)),
                (1, ProcessFaultEventKind::Returned),
            ],
            "the injected boundary must precede the returned event for {fault:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn process_fault_controllers_are_parallel_isolated_and_restore_scope() {
    async fn run_isolated(seed: u8) {
        use faults::{ProcessFault, ProcessFaultEventKind};

        let temp = tempfile::tempdir().unwrap();
        let supervisor = fault_supervisor(&temp, seed);
        let controller =
            faults::ProcessFaultController::for_child(1, ProcessFault::BeforeSpawn).unwrap();
        let outcome = controller
            .scope(supervisor.run(
                helper_command("split", &temp, Duration::from_secs(2)),
                CancellationToken::new(),
            ))
            .await;
        assert!(matches!(outcome, Err(ProcessError::SpawnFailed(_))));

        // The controller is inert after its scoped future returns. A
        // second child on the same Tokio task must therefore run normally
        // and must not consume another controlled ordinal.
        let retry = supervisor
            .run(
                helper_command("split", &temp, Duration::from_secs(2)),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(retry.exit_code, Some(0));
        time::timeout(Duration::from_secs(5), supervisor.shutdown())
            .await
            .expect("isolated supervisor must shut down");
        controller
            .prove_zero_live(Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(
            controller
                .events()
                .into_iter()
                .map(|event| (event.child_ordinal(), event.kind()))
                .collect::<Vec<_>>(),
            vec![
                (1, ProcessFaultEventKind::Admitted),
                (
                    1,
                    ProcessFaultEventKind::Injected(ProcessFault::BeforeSpawn),
                ),
                (1, ProcessFaultEventKind::Returned),
            ]
        );
    }

    let left = tokio::spawn(run_isolated(101));
    let right = tokio::spawn(run_isolated(102));
    let (left, right) = tokio::join!(left, right);
    left.unwrap();
    right.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pre_cancel_running_cancel_and_timeout_are_bounded_and_cancel_wins() {
    let temp = tempfile::tempdir().unwrap();
    let supervisor = supervisor(1_024, Duration::from_secs(5));
    let pid_file = temp.path().join("pid");

    let pre_cancelled = CancellationToken::new();
    pre_cancelled.cancel();
    let command = helper_command_with_pid("sleep", &temp, &pid_file, Duration::from_millis(1));
    let result = supervisor.run(command, pre_cancelled).await.unwrap();
    assert!(result.cancelled);
    assert!(!result.timed_out);
    assert!(!pid_file.exists());

    let cancellation = CancellationToken::new();
    let running_supervisor = supervisor.clone();
    let running = tokio::spawn({
        let cancellation = cancellation.clone();
        let command = helper_command_with_pid("sleep", &temp, &pid_file, Duration::from_secs(5));
        async move { running_supervisor.run(command, cancellation).await }
    });
    let process_id = wait_for_helper_pid(&pid_file).await;
    cancellation.cancel();
    let result = running.await.unwrap().unwrap();
    assert!(result.cancelled);
    assert!(!result.timed_out);
    wait_until_process_gone(process_id).await;

    let timeout_pid = temp.path().join("timeout-pid");
    let result = supervisor
        .run(
            helper_command_with_pid("sleep", &temp, &timeout_pid, Duration::from_millis(50)),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert!(result.timed_out);
    assert!(!result.cancelled);
    wait_until_process_gone(wait_for_helper_pid(&timeout_pid).await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_exit_and_aborted_supervisor_both_kill_the_entire_tree() {
    let temp = tempfile::tempdir().unwrap();
    let supervisor = supervisor(1_024, Duration::from_secs(5));

    for mode in ["leader", "leader-closed-pipe"] {
        let pid_file = temp.path().join(format!("{mode}-pid"));
        let execution = supervisor
            .start(
                helper_command_with_pid(mode, &temp, &pid_file, Duration::from_secs(5)),
                None,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        #[cfg(windows)]
        let tree = execution.tree.clone();
        let result = execution.wait().await.unwrap();
        assert_eq!(result.exit_code, Some(0));
        let process_id = wait_for_helper_pid(&pid_file).await;
        if mode == "leader-closed-pipe" {
            #[cfg(unix)]
            assert!(
                !process_is_running(process_id),
                "supervisor returned before the closed-pipe descendant terminated"
            );
            #[cfg(windows)]
            assert_eq!(tree.active_processes_for_test().unwrap(), 0);
        }
        wait_until_process_gone(process_id).await;
    }

    let abort_pid = temp.path().join("abort-pid");
    let running_supervisor = supervisor.clone();
    let execution = tokio::spawn({
        let command =
            helper_command_with_pid("leader-sleep", &temp, &abort_pid, Duration::from_secs(5));
        async move {
            running_supervisor
                .run(command, CancellationToken::new())
                .await
        }
    });
    let process_id = wait_for_helper_pid(&abort_pid).await;
    execution.abort();
    let _ = execution.await;
    wait_until_process_gone(process_id).await;
    supervisor.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scoped_supervisor_keeps_cleanup_unproven_until_the_whole_tree_exits() {
    let temp = tempfile::tempdir().unwrap();
    let directory = ProcessLivenessDirectory::open(temp.path().canonicalize().unwrap()).unwrap();
    let instance = directory.instance_scope(test_uuid(31)).unwrap();
    let task = instance.task_scope(test_uuid(32)).unwrap();
    let limits =
        ProcessLimits::try_new(1_024, 1_024, Duration::from_secs(5), Duration::from_secs(2))
            .unwrap();
    let (supervisor, supervision_gate) =
        ProcessSupervisor::new_paused_for_test(limits, task.clone());
    let pid_file = temp.path().join("scoped-grandchild-pid");
    let leader_pid_file = temp.path().join("scoped-leader-pid");
    let execution = supervisor
        .start(
            helper_command_with_tree_files(
                "leader",
                &temp,
                &pid_file,
                Some(&leader_pid_file),
                None,
                Duration::from_secs(5),
            ),
            None,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let process_id = wait_for_helper_pid(&pid_file).await;
    let leader_id = wait_for_helper_pid(&leader_pid_file).await;
    wait_until_process_not_running(leader_id).await;

    assert_eq!(task.active_tree_count(), 1);
    assert_eq!(
        task.cleanup_proof().unwrap(),
        ProcessCleanupProof::Held,
        "a live descendant must keep the cross-crash sentinel held"
    );
    #[cfg(windows)]
    {
        assert!(
            execution.tree.active_processes_for_test().unwrap() >= 1,
            "the Job must retain at least the live grandchild"
        );
        assert!(
            process_is_running(process_id),
            "the grandchild must remain live while supervision is paused"
        );
        assert_eq!(
            task.cleanup_proof().unwrap(),
            ProcessCleanupProof::Held,
            "the grandchild sentinel must remain held before Job cleanup"
        );
    }

    #[cfg(windows)]
    let tree_after_cleanup = execution.tree.clone();
    supervision_gate.notify_one();
    execution.wait().await.unwrap();
    #[cfg(windows)]
    assert_eq!(
        tree_after_cleanup.active_processes_for_test().unwrap(),
        0,
        "supervision may complete only after Job active count reaches zero"
    );
    supervisor.shutdown().await;
    wait_until_process_gone(process_id).await;

    assert_eq!(task.active_tree_count(), 0);
    assert_eq!(
        task.cleanup_proof().unwrap(),
        ProcessCleanupProof::Confirmed
    );
    assert_eq!(
        std::fs::read_dir(temp.path().join("process-liveness"))
            .unwrap()
            .count(),
        0,
        "successful cleanup must not leave a held or stale sentinel"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_primary_probes_the_crashed_primary_tree_before_cleanup_is_confirmed() {
    let temp = tempfile::tempdir().unwrap();
    let pid_file = temp.path().join("crash-grandchild-pid");
    let release_file = temp.path().join("release-crash-grandchild");
    let mut primary = tokio::process::Command::new(std::env::current_exe().unwrap());
    primary
        .args(["--exact", HELPER_TEST, "--nocapture"])
        .env(HELPER_ENV, "primary-crash")
        .env(HELPER_RUNTIME_DIRECTORY, temp.path())
        .env(HELPER_PID_FILE, &pid_file)
        .env(HELPER_RELEASE_FILE, &release_file)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let mut primary = primary.spawn().unwrap();
    let grandchild_id = wait_for_helper_pid(&pid_file).await;
    assert!(primary.wait().await.unwrap().success());

    let directory = ProcessLivenessDirectory::open(temp.path().canonicalize().unwrap()).unwrap();
    #[cfg(unix)]
    assert_eq!(
        directory.probe_stale().unwrap(),
        ProcessCleanupProof::Held,
        "an orphaned Unix grandchild must retain the inherited file-description lock"
    );
    #[cfg(windows)]
    assert!(
        matches!(
            directory.probe_stale().unwrap(),
            ProcessCleanupProof::Held | ProcessCleanupProof::Confirmed
        ),
        "the Windows probe may race Job close, but must never claim unknown state"
    );

    std::fs::write(&release_file, b"release").unwrap();
    wait_until_process_gone(grandchild_id).await;
    let deadline = TokioInstant::now() + Duration::from_secs(3);
    loop {
        match directory.probe_stale().unwrap() {
            ProcessCleanupProof::Confirmed => break,
            ProcessCleanupProof::Held if TokioInstant::now() < deadline => {
                time::sleep(Duration::from_millis(5)).await;
            }
            proof => panic!("crashed-primary sentinel did not become stale: {proof:?}"),
        }
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eventual_cleanup_keeps_retry_ownership_across_unknown_probe_state() {
    use std::os::fd::{FromRawFd as _, RawFd};

    let temp = tempfile::tempdir().unwrap();
    let directory = ProcessLivenessDirectory::open(temp.path().canonicalize().unwrap()).unwrap();
    let instance = directory.instance_scope(test_uuid(41)).unwrap();
    let task = instance.task_scope(test_uuid(42)).unwrap();
    let mut liveness = task.begin_tree().unwrap();
    let duplicate: RawFd = unsafe { libc::dup(liveness.raw_descriptor()) };
    assert!(
        duplicate >= 0,
        "duplicate the inherited sentinel descriptor"
    );
    let inherited = unsafe { std::fs::File::from_raw_fd(duplicate) };
    liveness.mark_spawned();

    let tracker = TaskTracker::new();
    tracker.spawn(complete_liveness_eventually(liveness));
    tracker.close();
    time::sleep(Duration::from_millis(20)).await;

    let sentinel_path = std::fs::read_dir(temp.path().join("process-liveness"))
        .unwrap()
        .next()
        .expect("one registered sentinel")
        .unwrap()
        .path();
    std::fs::write(&sentinel_path, b"temporarily-invalid").unwrap();
    drop(inherited);

    assert!(
        time::timeout(Duration::from_millis(100), tracker.wait())
            .await
            .is_err(),
        "Unknown must retain a background retry owner instead of detaching cleanup"
    );

    let name = sentinel_path.file_name().unwrap().to_string_lossy();
    std::fs::write(
        &sentinel_path,
        format!("coding-agent-process-liveness-v1\n{name}\n"),
    )
    .unwrap();
    time::timeout(Duration::from_secs(2), tracker.wait())
        .await
        .expect("cleanup owner completes after the probe becomes provable");
    assert_eq!(task.active_tree_count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_environment_is_allowlist_built_and_clears_sensitive_variables() {
    let temp = tempfile::tempdir().unwrap();
    let environment = ChildEnvironment::for_platform(&platform_environment(&temp));
    let sensitive_keys = [
        "CODING_AGENT_SENTINEL_SECRET",
        "OPENAI_API_KEY",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
        "SSH_AUTH_SOCK",
        "GITHUB_TOKEN",
        "CI_JOB_TOKEN",
        "AWS_SECRET_ACCESS_KEY",
        "AZURE_CLIENT_SECRET",
        "GOOGLE_APPLICATION_CREDENTIALS",
        "GIT_ASKPASS",
        "SSH_ASKPASS",
        "GIT_EDITOR",
        "GIT_SEQUENCE_EDITOR",
        "EDITOR",
        "VISUAL",
        "CARGO_REGISTRY_TOKEN",
        "CARGO_BUILD_JOBS",
        "RUST_TEST_THREADS",
        "RUSTC_WRAPPER",
        "RUSTFLAGS",
        "LD_PRELOAD",
        "DYLD_INSERT_LIBRARIES",
    ];
    for key in sensitive_keys {
        assert!(!environment.entries.contains_key(&OsString::from(key)));
    }
    let command = helper_command_with_environment(
        "environment",
        &temp,
        Duration::from_secs(2),
        environment,
        None,
    );

    let result = supervisor(4_096, Duration::from_secs(5))
        .run(command, CancellationToken::new())
        .await
        .unwrap();
    let stdout = String::from_utf8(result.stdout.head).unwrap();
    for key in sensitive_keys {
        assert!(stdout.contains(&format!("{key}=0")), "{stdout}");
    }
    assert!(stdout.contains("CARGO_NET_OFFLINE=true"));
}

#[test]
fn rust_toolchain_environment_has_an_exact_typed_allowlist() {
    let temp = tempfile::tempdir().unwrap();
    let compiler = std::env::current_exe().unwrap();
    let toolchain = RustToolchainEnvironment::try_new(
        vec![temp.path().to_path_buf()],
        temp.path().to_path_buf(),
        Some(temp.path().to_path_buf()),
        compiler.clone(),
        compiler.clone(),
    )
    .unwrap();
    let platform = platform_environment(&temp);
    let environment = ChildEnvironment::for_rust_toolchain(&platform, &toolchain);
    let mut actual = environment
        .entries
        .keys()
        .map(|key| key.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    let mut expected = vec![
        "CARGO_HOME",
        "CARGO_NET_OFFLINE",
        "CARGO_TERM_COLOR",
        "LANG",
        "LC_ALL",
        "PATH",
        "RUSTC",
        "RUSTDOC",
        "RUSTUP_HOME",
        "RUST_BACKTRACE",
    ];
    #[cfg(windows)]
    expected.extend(["SYSTEMROOT", "TEMP", "TMP", "WINDIR"]);
    #[cfg(unix)]
    expected.push("TMPDIR");
    actual.sort();
    expected.sort_unstable();
    assert_eq!(actual, expected);
    assert_eq!(environment.entries[&OsString::from("RUSTC")], compiler);
    assert_eq!(environment.entries[&OsString::from("RUSTDOC")], compiler);
    assert!(
        !environment
            .entries
            .contains_key(&OsString::from("CODING_AGENT_SENTINEL_SECRET"))
    );
    assert!(matches!(
        PlatformEnvironment::try_new(PathBuf::from("relative-temp"), None),
        Err(PlatformEnvironmentError::TempDirectory)
    ));
    #[cfg(windows)]
    assert!(matches!(
        PlatformEnvironment::try_new(temp.path().to_path_buf(), None),
        Err(PlatformEnvironmentError::SystemRoot)
    ));
    #[cfg(unix)]
    assert!(matches!(
        PlatformEnvironment::try_new(temp.path().to_path_buf(), Some(temp.path().to_path_buf())),
        Err(PlatformEnvironmentError::SystemRoot)
    ));

    assert!(matches!(
        RustToolchainEnvironment::try_new(
            vec![PathBuf::from("relative")],
            temp.path().to_path_buf(),
            None,
            std::env::current_exe().unwrap(),
            std::env::current_exe().unwrap(),
        ),
        Err(ToolchainEnvironmentError::Directory)
    ));
    assert!(matches!(
        RustToolchainEnvironment::try_new(
            vec![temp.path().to_path_buf()],
            temp.path().to_path_buf(),
            None,
            temp.path().join("missing-rustc"),
            std::env::current_exe().unwrap(),
        ),
        Err(ToolchainEnvironmentError::Compiler)
    ));

    let separator = if cfg!(windows) { ';' } else { ':' };
    let invalid_path_entry = temp.path().join(format!("invalid{separator}entry"));
    std::fs::create_dir(&invalid_path_entry).unwrap();
    assert!(matches!(
        RustToolchainEnvironment::try_new(
            vec![invalid_path_entry],
            temp.path().to_path_buf(),
            None,
            std::env::current_exe().unwrap(),
            std::env::current_exe().unwrap(),
        ),
        Err(ToolchainEnvironmentError::SearchPath)
    ));
}

#[cfg(all(windows, target_env = "msvc"))]
#[test]
fn windows_msvc_environment_adds_only_pinned_toolchain_entries() {
    let temp = tempfile::tempdir().unwrap();
    let linker_directory = temp.path().join("msvc-bin");
    let library_directory = temp.path().join("msvc-lib");
    let include_directory = temp.path().join("msvc-include");
    for directory in [&linker_directory, &library_directory, &include_directory] {
        std::fs::create_dir(directory).unwrap();
    }
    let linker = linker_directory.join("link.exe");
    std::fs::copy(std::env::current_exe().unwrap(), &linker).unwrap();
    let compiler = std::env::current_exe().unwrap();
    let toolchain = RustToolchainEnvironment::try_new(
        vec![linker_directory.clone(), temp.path().to_path_buf()],
        temp.path().to_path_buf(),
        None,
        compiler.clone(),
        compiler,
    )
    .unwrap()
    .with_windows_msvc(
        WindowsMsvcEnvironment::try_new(
            linker.clone(),
            vec![library_directory.clone()],
            vec![include_directory.clone()],
        )
        .unwrap(),
    );

    let environment =
        ChildEnvironment::for_rust_toolchain(&platform_environment(&temp), &toolchain);
    let linker_key = OsString::from(cargo_linker_environment_key().unwrap());
    assert_eq!(environment.entries[&linker_key], linker);
    assert_eq!(
        std::env::split_paths(&environment.entries[&OsString::from("LIB")]).collect::<Vec<_>>(),
        vec![library_directory]
    );
    assert_eq!(
        std::env::split_paths(&environment.entries[&OsString::from("INCLUDE")]).collect::<Vec<_>>(),
        vec![include_directory]
    );
    assert_eq!(
        std::env::split_paths(&environment.entries[&OsString::from("PATH")]).collect::<Vec<_>>(),
        vec![linker_directory, temp.path().to_path_buf()]
    );

    let mut actual = environment
        .entries
        .keys()
        .map(|key| key.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    let mut expected = vec![
        "CARGO_HOME",
        "CARGO_NET_OFFLINE",
        "CARGO_TERM_COLOR",
        cargo_linker_environment_key().unwrap(),
        "INCLUDE",
        "LANG",
        "LC_ALL",
        "LIB",
        "PATH",
        "RUSTC",
        "RUSTDOC",
        "RUST_BACKTRACE",
        "SYSTEMROOT",
        "TEMP",
        "TMP",
        "WINDIR",
    ];
    actual.sort();
    expected.sort_unstable();
    assert_eq!(actual, expected);
    assert!(
        !environment
            .entries
            .contains_key(&OsString::from("CODING_AGENT_SENTINEL_SECRET"))
    );
}

#[cfg(all(windows, target_env = "msvc"))]
#[test]
fn windows_msvc_environment_fails_closed_for_untrusted_paths() {
    let temp = tempfile::tempdir().unwrap();
    let library_directory = temp.path().join("lib");
    let include_directory = temp.path().join("include");
    std::fs::create_dir(&library_directory).unwrap();
    std::fs::create_dir(&include_directory).unwrap();
    let missing_linker = temp.path().join("missing-link.exe");

    assert!(matches!(
        WindowsMsvcEnvironment::try_new(
            PathBuf::from("relative-link.exe"),
            vec![library_directory.clone()],
            vec![include_directory.clone()],
        ),
        Err(ToolchainEnvironmentError::Linker)
    ));
    assert!(matches!(
        WindowsMsvcEnvironment::try_new(
            missing_linker,
            vec![library_directory.clone()],
            vec![include_directory.clone()],
        ),
        Err(ToolchainEnvironmentError::Linker)
    ));

    let linker = temp.path().join("link.exe");
    std::fs::copy(std::env::current_exe().unwrap(), &linker).unwrap();
    for library_directories in [
        Vec::new(),
        vec![PathBuf::from("relative-lib")],
        vec![temp.path().join("missing-lib")],
    ] {
        assert!(matches!(
            WindowsMsvcEnvironment::try_new(
                linker.clone(),
                library_directories,
                vec![include_directory.clone()],
            ),
            Err(ToolchainEnvironmentError::Directory)
        ));
    }
    assert!(matches!(
        WindowsMsvcEnvironment::try_new(
            linker,
            vec![library_directory],
            vec![PathBuf::from("relative-include")],
        ),
        Err(ToolchainEnvironmentError::Directory)
    ));
}

#[tokio::test]
async fn bounded_drain_cleanup_aborts_and_joins_every_pipe_task() {
    let active = Arc::new(AtomicUsize::new(2));
    let stdout = tokio::spawn(drain_stream(NeverReadyReader::new(active.clone()), 64));
    let stderr = tokio::spawn(drain_stream(NeverReadyReader::new(active.clone()), 64));

    let result = collect_drains_until(
        TokioInstant::now() + Duration::from_millis(20),
        stdout,
        stderr,
    )
    .await;

    assert!(matches!(result, Err(ProcessError::CleanupTimedOut)));
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[test]
fn head_tail_capture_has_exact_boundaries() {
    for (limit, input, expected_head, expected_tail, truncated) in [
        (1, b"a".as_slice(), b"a".as_slice(), b"".as_slice(), false),
        (1, b"ab".as_slice(), b"a".as_slice(), b"".as_slice(), true),
        (
            4,
            b"abcd".as_slice(),
            b"abcd".as_slice(),
            b"".as_slice(),
            false,
        ),
        (
            4,
            b"abcde".as_slice(),
            b"ab".as_slice(),
            b"de".as_slice(),
            true,
        ),
    ] {
        let mut capture = HeadTailCapture::new(limit);
        capture.push(input);
        let output = capture.finish();
        assert_eq!(output.head, expected_head);
        assert_eq!(output.tail, expected_tail);
        assert_eq!(output.truncated, truncated);
        assert_eq!(output.observed_bytes, input.len() as u64);
    }
}

#[test]
fn cancellation_wins_when_deadline_and_token_are_both_ready() {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert!(matches!(
        deadline_observation(&cancellation),
        ObservedTermination::Cancelled
    ));
}

struct NeverReadyReader {
    active: Arc<AtomicUsize>,
}

impl NeverReadyReader {
    fn new(active: Arc<AtomicUsize>) -> Self {
        Self { active }
    }
}

impl AsyncRead for NeverReadyReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        _buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

impl Drop for NeverReadyReader {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}

fn supervisor(output_bytes: usize, max_timeout: Duration) -> ProcessSupervisor {
    ProcessSupervisor::new(
        ProcessLimits::try_new(
            output_bytes,
            output_bytes,
            max_timeout,
            Duration::from_secs(2),
        )
        .unwrap(),
        crate::process_liveness::test_process_scope(),
    )
}

fn fault_supervisor(temp: &TempDir, seed: u8) -> ProcessSupervisor {
    let directory = ProcessLivenessDirectory::open(temp.path().canonicalize().unwrap()).unwrap();
    let instance = directory.instance_scope(test_uuid(seed)).unwrap();
    let task = instance
        .task_scope(test_uuid(seed.wrapping_add(64)))
        .unwrap();
    ProcessSupervisor::new(
        ProcessLimits::try_new(512, 512, Duration::from_secs(5), Duration::from_secs(2)).unwrap(),
        task,
    )
}

fn helper_command(mode: &str, temp: &TempDir, timeout: Duration) -> ValidatedCommand {
    helper_command_with_environment(
        mode,
        temp,
        timeout,
        ChildEnvironment::from_current_process().unwrap(),
        None,
    )
}

fn helper_command_with_pid(
    mode: &str,
    temp: &TempDir,
    pid_file: &Path,
    timeout: Duration,
) -> ValidatedCommand {
    helper_command_with_environment(
        mode,
        temp,
        timeout,
        ChildEnvironment::from_current_process().unwrap(),
        Some(pid_file),
    )
}

fn helper_command_with_environment(
    mode: &str,
    temp: &TempDir,
    timeout: Duration,
    mut environment: ChildEnvironment,
    pid_file: Option<&Path>,
) -> ValidatedCommand {
    environment.insert_test_value(HELPER_ENV, mode);
    if let Some(pid_file) = pid_file {
        environment.insert_test_value(HELPER_PID_FILE, pid_file.as_os_str());
    }
    ValidatedCommand::for_test(
        std::env::current_exe().unwrap(),
        ["--exact", HELPER_TEST, "--nocapture"]
            .into_iter()
            .map(OsString::from)
            .collect(),
        temp.path().canonicalize().unwrap(),
        environment,
        timeout,
    )
    .unwrap()
}

fn helper_command_with_tree_files(
    mode: &str,
    temp: &TempDir,
    pid_file: &Path,
    leader_pid_file: Option<&Path>,
    release_file: Option<&Path>,
    timeout: Duration,
) -> ValidatedCommand {
    helper_command_for_directory(
        mode,
        temp.path(),
        pid_file,
        leader_pid_file,
        release_file,
        timeout,
    )
}

fn helper_command_for_directory(
    mode: &str,
    directory: &Path,
    pid_file: &Path,
    leader_pid_file: Option<&Path>,
    release_file: Option<&Path>,
    timeout: Duration,
) -> ValidatedCommand {
    let mut environment = ChildEnvironment::from_current_process().unwrap();
    environment.insert_test_value(HELPER_ENV, mode);
    environment.insert_test_value(HELPER_PID_FILE, pid_file.as_os_str());
    if let Some(leader_pid_file) = leader_pid_file {
        environment.insert_test_value(HELPER_LEADER_PID_FILE, leader_pid_file.as_os_str());
    }
    if let Some(release_file) = release_file {
        environment.insert_test_value(HELPER_RELEASE_FILE, release_file.as_os_str());
    }
    ValidatedCommand::for_test(
        std::env::current_exe().unwrap(),
        ["--exact", HELPER_TEST, "--nocapture"]
            .into_iter()
            .map(OsString::from)
            .collect(),
        directory.canonicalize().unwrap(),
        environment,
        timeout,
    )
    .unwrap()
}

fn platform_environment(temp: &TempDir) -> PlatformEnvironment {
    #[cfg(windows)]
    let system_root = std::env::var_os("SYSTEMROOT")
        .or_else(|| std::env::var_os("WINDIR"))
        .map(PathBuf::from);
    #[cfg(unix)]
    let system_root = None;
    PlatformEnvironment::try_new(temp.path().to_path_buf(), system_root).unwrap()
}

fn flush_standard_streams() {
    std::io::stdout().flush().unwrap();
    std::io::stderr().flush().unwrap();
}

fn write_helper_pid() {
    let path = std::env::var_os(HELPER_PID_FILE).expect("helper pid file is configured");
    std::fs::write(path, std::process::id().to_string()).unwrap();
}

fn write_optional_helper_pid(environment_key: &str) {
    if let Some(path) = std::env::var_os(environment_key) {
        std::fs::write(path, std::process::id().to_string()).unwrap();
    }
}

fn wait_for_release_file_sync() {
    let path = PathBuf::from(
        std::env::var_os(HELPER_RELEASE_FILE).expect("helper release file is configured"),
    );
    while !path.exists() {
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn wait_for_helper_pid_sync() {
    let path =
        PathBuf::from(std::env::var_os(HELPER_PID_FILE).expect("helper pid file is configured"));
    let deadline = Instant::now() + Duration::from_secs(2);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "grandchild did not publish its pid"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

async fn wait_for_helper_pid(path: &Path) -> u32 {
    // Starting a helper may include re-hashing this comparatively large test
    // binary while revalidating its pinned executable. Slow and shared CI
    // workers must not turn that bounded authentication work into a false
    // process-start failure.
    let deadline = TokioInstant::now() + Duration::from_secs(15);
    loop {
        if let Ok(value) = std::fs::read_to_string(path) {
            return value.parse().unwrap();
        }
        assert!(
            TokioInstant::now() < deadline,
            "helper did not publish its pid"
        );
        time::sleep(Duration::from_millis(5)).await;
    }
}

async fn wait_until_process_gone(process_id: u32) {
    let deadline = TokioInstant::now() + Duration::from_secs(3);
    while process_exists(process_id) {
        assert!(
            TokioInstant::now() < deadline,
            "process {process_id} survived cleanup"
        );
        time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_until_process_not_running(process_id: u32) {
    let deadline = TokioInstant::now() + Duration::from_secs(3);
    while process_is_running(process_id) {
        assert!(
            TokioInstant::now() < deadline,
            "process {process_id} remained running"
        );
        time::sleep(Duration::from_millis(10)).await;
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn test_uuid(seed: u8) -> [u8; 16] {
    let mut identity = [seed; 16];
    identity[6] = (identity[6] & 0x0f) | 0x40;
    identity[8] = (identity[8] & 0x3f) | 0x80;
    identity
}

#[cfg(target_os = "linux")]
fn process_is_running(process_id: u32) -> bool {
    let Ok(stat) = std::fs::read_to_string(format!("/proc/{process_id}/stat")) else {
        return false;
    };
    stat.rsplit_once(") ")
        .and_then(|(_, fields)| fields.chars().next())
        .is_some_and(|state| state != 'Z')
}

#[cfg(all(unix, not(target_os = "linux")))]
fn process_is_running(process_id: u32) -> bool {
    process_exists(process_id)
}

#[cfg(windows)]
fn process_is_running(process_id: u32) -> bool {
    process_exists(process_id)
}

#[cfg(unix)]
fn process_exists(process_id: u32) -> bool {
    let Ok(process_id) = i32::try_from(process_id) else {
        return false;
    };
    if unsafe { libc::kill(process_id, 0) } == 0 {
        true
    } else {
        io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
    }
}

#[cfg(windows)]
fn process_exists(process_id: u32) -> bool {
    use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};

    use windows_sys::Win32::Foundation::{HANDLE, WAIT_TIMEOUT};
    use windows_sys::Win32::System::Threading::{
        OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE, WaitForSingleObject,
    };

    let process = unsafe {
        OpenProcess(
            PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
            0,
            process_id,
        )
    };
    if process.is_null() {
        return false;
    }
    let process = unsafe { OwnedHandle::from_raw_handle(process) };
    unsafe { WaitForSingleObject(process.as_raw_handle() as HANDLE, 0) == WAIT_TIMEOUT }
}
