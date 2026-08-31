use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn waits_for_bounded_eof_proof() {
    let temp = tempfile::tempdir().unwrap();
    let directory = ProcessLivenessDirectory::open(temp.path().canonicalize().unwrap()).unwrap();
    let task = directory
        .instance_scope(test_uuid(61))
        .unwrap()
        .task_scope(test_uuid(62))
        .unwrap();
    let limits =
        ProcessLimits::try_new(1_024, 1_024, Duration::from_secs(5), Duration::from_secs(2))
            .unwrap();
    let (supervisor, supervision_gate) =
        ProcessSupervisor::new_paused_for_test(limits, task.clone());
    let pid_file = temp.path().join("detached-eof-grandchild-pid");
    let leader_pid_file = temp.path().join("detached-eof-leader-pid");
    let release_file = temp.path().join("detached-eof-release");
    let mut release = HelperReleaseGuard::new(release_file.clone(), pid_file.clone());
    let running_supervisor = supervisor.clone();
    let running = tokio::spawn({
        let command = helper_command_with_tree_files(
            "leader-detached-release",
            &temp,
            &pid_file,
            Some(&leader_pid_file),
            Some(&release_file),
            Duration::from_secs(5),
        );
        async move {
            running_supervisor
                .run(command, CancellationToken::new())
                .await
        }
    });
    let process_id = wait_for_helper_pid(&pid_file).await;
    release.track_process(process_id);
    let leader_id = wait_for_helper_pid(&leader_pid_file).await;
    wait_until_process_not_running(leader_id).await;

    supervision_gate.notify_one();
    time::sleep(Duration::from_millis(100)).await;
    let waited_for_eof = !running.is_finished();
    let process_was_running = process_is_running(process_id);
    let proof_before_release = task.cleanup_proof();

    release.release().unwrap();
    let outcome = time::timeout(Duration::from_secs(5), running).await;
    let process_gone = process_gone_within(process_id, Duration::from_secs(3)).await;
    let shutdown = time::timeout(Duration::from_secs(3), supervisor.shutdown()).await;
    let active_tree_count = task.active_tree_count();
    let final_proof = task.cleanup_proof();

    assert!(
        waited_for_eof,
        "EPERM with a live inherited writer must wait for bounded EOF proof"
    );
    assert!(process_was_running);
    assert_eq!(proof_before_release.unwrap(), ProcessCleanupProof::Held);
    let result = outcome
        .expect("supervision must finish after the detached writer exits")
        .unwrap()
        .unwrap();
    assert_eq!(result.exit_code, Some(0));
    assert!(process_gone, "detached writer survived explicit release");
    assert!(shutdown.is_ok(), "supervisor did not join after EOF proof");
    assert_eq!(active_tree_count, 0);
    assert_eq!(final_proof.unwrap(), ProcessCleanupProof::Confirmed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_stays_fail_closed_until_background_eof_proof() {
    let temp = tempfile::tempdir().unwrap();
    let directory = ProcessLivenessDirectory::open(temp.path().canonicalize().unwrap()).unwrap();
    let task = directory
        .instance_scope(test_uuid(63))
        .unwrap()
        .task_scope(test_uuid(64))
        .unwrap();
    let limits = ProcessLimits::try_new(
        1_024,
        1_024,
        Duration::from_secs(5),
        Duration::from_millis(150),
    )
    .unwrap();
    let (supervisor, supervision_gate) =
        ProcessSupervisor::new_paused_for_test(limits, task.clone());
    let pid_file = temp.path().join("detached-timeout-grandchild-pid");
    let leader_pid_file = temp.path().join("detached-timeout-leader-pid");
    let release_file = temp.path().join("detached-timeout-release");
    let mut release = HelperReleaseGuard::new(release_file.clone(), pid_file.clone());
    let running_supervisor = supervisor.clone();
    let running = tokio::spawn({
        let command = helper_command_with_tree_files(
            "leader-detached-release",
            &temp,
            &pid_file,
            Some(&leader_pid_file),
            Some(&release_file),
            Duration::from_secs(5),
        );
        async move {
            running_supervisor
                .run(command, CancellationToken::new())
                .await
        }
    });
    let process_id = wait_for_helper_pid(&pid_file).await;
    release.track_process(process_id);
    let leader_id = wait_for_helper_pid(&leader_pid_file).await;
    wait_until_process_not_running(leader_id).await;

    supervision_gate.notify_one();
    let outcome = time::timeout(Duration::from_secs(3), running).await;
    let process_was_running = process_is_running(process_id);
    let active_before_release = task.active_tree_count();
    let proof_before_release = task.cleanup_proof();

    release.release().unwrap();
    let process_gone = process_gone_within(process_id, Duration::from_secs(3)).await;
    let background_proof = time::timeout(Duration::from_secs(3), async {
        while task.active_tree_count() != 0 {
            time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await;
    let shutdown = time::timeout(Duration::from_secs(3), supervisor.shutdown()).await;
    let final_proof = task.cleanup_proof();

    let error = outcome
        .expect("bounded EPERM reconciliation must honor the cleanup deadline")
        .unwrap()
        .unwrap_err();
    assert!(error.process_cleanup_is_unproven());
    let ProcessError::TreeCleanupFailed(kill_error) = error else {
        panic!("timed-out exited EPERM must preserve the original kill failure")
    };
    assert_eq!(kill_error.raw_os_error(), Some(libc::EPERM));
    assert!(process_was_running);
    assert_eq!(active_before_release, 1);
    assert_eq!(proof_before_release.unwrap(), ProcessCleanupProof::Held);
    assert!(process_gone, "detached writer survived explicit release");
    assert!(
        background_proof.is_ok(),
        "background tree proof did not release the liveness registration"
    );
    assert!(
        shutdown.is_ok(),
        "background tree proof did not join during shutdown"
    );
    assert_eq!(final_proof.unwrap(), ProcessCleanupProof::Confirmed);
}

struct HelperReleaseGuard {
    path: Option<PathBuf>,
    pid_path: PathBuf,
    process_id: Option<u32>,
}

impl HelperReleaseGuard {
    fn new(path: PathBuf, pid_path: PathBuf) -> Self {
        Self {
            path: Some(path),
            pid_path,
            process_id: None,
        }
    }

    fn track_process(&mut self, process_id: u32) {
        self.process_id = Some(process_id);
    }

    fn release(&mut self) -> io::Result<()> {
        if let Some(path) = self.path.as_ref() {
            std::fs::write(path, b"release")?;
            self.path = None;
        }
        Ok(())
    }

    fn process_id_for_drop(&self) -> Option<u32> {
        if let Some(process_id) = self.process_id {
            return Some(process_id);
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Ok(contents) = std::fs::read_to_string(&self.pid_path)
                && let Ok(process_id) = contents.trim().parse()
            {
                return Some(process_id);
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

impl Drop for HelperReleaseGuard {
    fn drop(&mut self) {
        if self.path.is_none() || self.release().is_err() {
            return;
        }
        let Some(process_id) = self.process_id_for_drop() else {
            return;
        };
        let deadline = Instant::now() + Duration::from_secs(3);
        while process_exists(process_id) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

async fn process_gone_within(process_id: u32, timeout: Duration) -> bool {
    time::timeout(timeout, async {
        while process_exists(process_id) {
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok()
}
