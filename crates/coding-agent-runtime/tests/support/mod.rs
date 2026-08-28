#![allow(dead_code)]

use std::io;
use std::path::Path;
use std::process::{Command, ExitStatus, Output, Stdio};
use std::sync::Mutex;

use coding_agent_runtime::{ProcessCleanupProof, ProcessLivenessDirectory, ProcessLivenessScope};

#[cfg(windows)]
mod windows_private;

/// Retains the exact liveness scopes handed to runtime components and proves
/// at fixture teardown that none of those scopes still owns a live child.
///
/// This is deliberately only an observer. It cannot construct a process,
/// bypass command policy, or force-clean a failed process tree.
#[derive(Default)]
pub struct ProcessScopeTracker {
    scopes: Mutex<Vec<ProcessLivenessScope>>,
}

impl ProcessScopeTracker {
    pub fn track(&self, scope: ProcessLivenessScope) -> ProcessLivenessScope {
        self.scopes
            .lock()
            .expect("lock process-scope tracker")
            .push(scope.clone());
        scope
    }

    pub fn assert_zero_live(&self) {
        let scopes = self.scopes.lock().expect("lock process-scope tracker");
        for scope in scopes.iter() {
            assert_eq!(
                scope.active_tree_count(),
                0,
                "delivery fixture retained a live process tree"
            );
            assert_eq!(
                scope
                    .cleanup_proof()
                    .expect("observe delivery fixture process cleanup"),
                ProcessCleanupProof::Confirmed,
                "delivery fixture could not prove process-tree cleanup"
            );
        }
    }
}

impl Drop for ProcessScopeTracker {
    fn drop(&mut self) {
        // Avoid turning an existing test assertion into a double-panic abort.
        // Passing tests always execute the proof below.
        if !std::thread::panicking() {
            self.assert_zero_live();
        }
    }
}

pub fn instance_process_scope(runtime_directory: &Path) -> ProcessLivenessScope {
    let liveness_runtime = private_liveness_runtime(runtime_directory);
    let mut instance_id = [0x15; 16];
    instance_id[6] = 0x45;
    instance_id[8] = 0x95;
    ProcessLivenessDirectory::open(&liveness_runtime)
        .expect("open process-liveness test directory")
        .instance_scope(instance_id)
        .expect("create process-liveness test instance scope")
}

pub fn private_liveness_runtime(runtime_directory: &Path) -> std::path::PathBuf {
    let liveness_runtime = runtime_directory.join(".process-liveness-test-runtime");
    prepare_private_directory(&liveness_runtime)
        .expect("prepare private process-liveness test runtime");
    liveness_runtime
        .canonicalize()
        .expect("canonicalize private process-liveness test runtime")
}

pub fn task_process_scope(runtime_directory: &Path) -> ProcessLivenessScope {
    let mut task_id = [0x25; 16];
    task_id[6] = 0x45;
    task_id[8] = 0xa5;
    instance_process_scope(runtime_directory)
        .task_scope(task_id)
        .expect("create process-liveness test task scope")
}

#[cfg(unix)]
fn prepare_private_directory(path: &Path) -> io::Result<()> {
    std::fs::create_dir_all(path)?;
    harden_private_directory(path)
}

#[cfg(windows)]
fn prepare_private_directory(path: &Path) -> io::Result<()> {
    windows_private::prepare(path)
}

#[cfg(unix)]
fn harden_private_directory(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
}

#[cfg(windows)]
fn harden_private_directory(path: &Path) -> io::Result<()> {
    windows_private::harden(path)
}

#[cfg(windows)]
pub fn add_non_owner_allow_ace(path: &Path) -> io::Result<()> {
    windows_private::add_non_owner_allow_ace(path)
}

pub fn command_output(command: &mut Command) -> io::Result<Output> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let child = {
        let _spawn_guard = coding_agent_runtime::acquire_process_spawn_lock();
        command.spawn()?
    };
    child.wait_with_output()
}

pub fn command_status(command: &mut Command) -> io::Result<ExitStatus> {
    let mut child = {
        let _spawn_guard = coding_agent_runtime::acquire_process_spawn_lock();
        command.spawn()?
    };
    child.wait()
}
