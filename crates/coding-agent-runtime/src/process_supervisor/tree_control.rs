use super::*;

#[derive(Clone)]
pub(super) struct TreeKillHandle {
    inner: Arc<TreeKillInner>,
}

struct TreeKillInner {
    platform: platform::ProcessTree,
    outcome: OnceLock<Result<(), KillFailure>>,
}

impl TreeKillHandle {
    pub(super) fn new(platform: platform::ProcessTree) -> Self {
        Self {
            inner: Arc::new(TreeKillInner {
                platform,
                outcome: OnceLock::new(),
            }),
        }
    }

    pub(super) fn kill_now(&self) -> io::Result<()> {
        self.inner
            .outcome
            .get_or_init(|| self.inner.platform.kill().map_err(KillFailure::from))
            .clone()
            .map_err(KillFailure::into_io_error)
    }

    #[cfg(target_os = "macos")]
    pub(super) fn kill_now_after_observed_exit(
        &self,
        leader_exit: &platform::LeaderExit,
    ) -> io::Result<()> {
        self.inner
            .outcome
            .get_or_init(|| {
                reconcile_exited_tree_kill(self.inner.platform.kill(), || {
                    leader_exit.liveness_pipe_has_no_writers_now()
                })
                .map_err(KillFailure::from)
            })
            .clone()
            .map_err(KillFailure::into_io_error)
    }

    pub(super) fn disarm_without_kill(&self) {
        let _ = self.inner.outcome.set(Ok(()));
    }

    pub(super) fn inject_kill_failure_for_test(&self, error: io::Error) -> io::Result<()> {
        let _ = self.inner.outcome.set(Err(KillFailure::from(error)));
        self.kill_now()
    }

    #[cfg(all(test, windows))]
    pub(super) fn active_processes_for_test(&self) -> io::Result<u32> {
        self.inner.platform.active_processes()
    }
}

#[derive(Debug, Clone)]
struct KillFailure {
    kind: io::ErrorKind,
    raw_os_error: Option<i32>,
}

impl From<io::Error> for KillFailure {
    fn from(error: io::Error) -> Self {
        Self {
            kind: error.kind(),
            raw_os_error: error.raw_os_error(),
        }
    }
}

impl KillFailure {
    fn into_io_error(self) -> io::Error {
        self.raw_os_error
            .map(io::Error::from_raw_os_error)
            .unwrap_or_else(|| io::Error::from(self.kind))
    }
}

pub(super) struct TreeWorkerGuard(pub(super) TreeKillHandle);

impl Drop for TreeWorkerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill_now();
    }
}

#[cfg(unix)]
pub(super) fn exit_signal(status: &ExitStatus) -> Option<i32> {
    use std::os::unix::process::ExitStatusExt;
    status.signal()
}

#[cfg(windows)]
pub(super) fn exit_signal(_: &ExitStatus) -> Option<i32> {
    None
}
