use super::*;

#[derive(Clone)]
pub(super) struct TreeKillHandle {
    inner: Arc<TreeKillInner>,
}

struct TreeKillInner {
    platform: platform::ProcessTree,
    state: Mutex<TreeKillState>,
}

enum TreeKillState {
    Armed,
    Settled(Result<(), KillFailure>),
    // XNU can reject a group signal after the waitable leader becomes a
    // zombie even though an out-of-group descendant still owns the inherited
    // liveness descriptor. This state is not success: only later EOF proof may
    // settle it, and ordinary/drop kill attempts continue to observe EPERM.
    #[cfg(any(test, target_os = "macos"))]
    AwaitingEofAfterExitedEperm(KillFailure),
}

impl TreeKillHandle {
    pub(super) fn new(platform: platform::ProcessTree) -> Self {
        Self {
            inner: Arc::new(TreeKillInner {
                platform,
                state: Mutex::new(TreeKillState::Armed),
            }),
        }
    }

    pub(super) fn kill_now(&self) -> io::Result<()> {
        let mut state = self.lock_state();
        if matches!(&*state, TreeKillState::Armed) {
            *state = TreeKillState::Settled(self.inner.platform.kill().map_err(KillFailure::from));
        }
        state_result(&state).map_err(KillFailure::into_io_error)
    }

    #[cfg(target_os = "macos")]
    pub(super) fn kill_now_after_observed_exit(
        &self,
        leader_exit: &platform::LeaderExit,
    ) -> io::Result<ExitedTreeKill> {
        let mut state = self.lock_state();
        let prior_failure = state_failure(&state);
        if matches!(&*state, TreeKillState::Armed) {
            *state = TreeKillState::Settled(self.inner.platform.kill().map_err(KillFailure::from));
        }
        reconcile_kill_state_after_observed_exit(&mut state, prior_failure, || {
            leader_exit.liveness_pipe_has_no_writers_now()
        })
    }

    #[cfg(target_os = "macos")]
    pub(super) fn confirm_exited_eperm_after_eof(&self) {
        let mut state = self.lock_state();
        confirm_exited_eperm_after_eof_state(&mut state);
    }

    pub(super) fn disarm_without_kill(&self) {
        let mut state = self.lock_state();
        if matches!(&*state, TreeKillState::Armed) {
            *state = TreeKillState::Settled(Ok(()));
        }
    }

    pub(super) fn inject_kill_failure_for_test(&self, error: io::Error) -> io::Result<()> {
        let mut state = self.lock_state();
        if matches!(&*state, TreeKillState::Armed) {
            *state = TreeKillState::Settled(Err(KillFailure::from(error)));
        }
        drop(state);
        self.kill_now()
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, TreeKillState> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[cfg(all(test, windows))]
    pub(super) fn active_processes_for_test(&self) -> io::Result<u32> {
        self.inner.platform.active_processes()
    }
}

fn state_result(state: &TreeKillState) -> Result<(), KillFailure> {
    match state {
        TreeKillState::Armed => unreachable!("tree kill state must be settled before inspection"),
        TreeKillState::Settled(result) => result.clone(),
        #[cfg(any(test, target_os = "macos"))]
        TreeKillState::AwaitingEofAfterExitedEperm(error) => Err(error.clone()),
    }
}

#[cfg(any(test, target_os = "macos"))]
fn reconcile_kill_state_after_observed_exit(
    state: &mut TreeKillState,
    prior_failure: Option<KillFailure>,
    liveness_probe: impl FnOnce() -> io::Result<bool>,
) -> io::Result<ExitedTreeKill> {
    let kill_result = state_result(state).map_err(KillFailure::into_io_error);
    match reconcile_exited_tree_kill(kill_result, liveness_probe) {
        Ok(ExitedTreeKill::Settled) => {
            *state = TreeKillState::Settled(Ok(()));
            Ok(ExitedTreeKill::Settled)
        }
        Ok(ExitedTreeKill::AwaitingEof(error)) => {
            let failure = KillFailure::from(error);
            *state = TreeKillState::AwaitingEofAfterExitedEperm(failure.clone());
            Ok(ExitedTreeKill::AwaitingEof(failure.into_io_error()))
        }
        Err(error) => {
            // A plain/drop kill may have won the first-writer race before the
            // authoritative exited observation. Reconciliation may improve a
            // cached EPERM with positive EOF, but a failed probe must not
            // replace that earlier failure with a later diagnostic.
            let failure = prior_failure.unwrap_or_else(|| KillFailure::from(error));
            *state = TreeKillState::Settled(Err(failure.clone()));
            Err(failure.into_io_error())
        }
    }
}

#[cfg(any(test, target_os = "macos"))]
fn state_failure(state: &TreeKillState) -> Option<KillFailure> {
    match state {
        TreeKillState::Settled(Err(error)) | TreeKillState::AwaitingEofAfterExitedEperm(error) => {
            Some(error.clone())
        }
        TreeKillState::Armed | TreeKillState::Settled(Ok(())) => None,
    }
}

#[cfg(any(test, target_os = "macos"))]
fn confirm_exited_eperm_after_eof_state(state: &mut TreeKillState) {
    if matches!(state, TreeKillState::AwaitingEofAfterExitedEperm(_)) {
        *state = TreeKillState::Settled(Ok(()));
    }
}

#[cfg(any(test, target_os = "macos"))]
#[derive(Debug)]
pub(super) enum ExitedTreeKill {
    Settled,
    AwaitingEof(io::Error),
}

#[cfg(target_os = "macos")]
pub(super) const EXITED_EPERM_RAW_OS_ERROR: i32 = libc::EPERM;
#[cfg(all(test, not(target_os = "macos")))]
pub(super) const EXITED_EPERM_RAW_OS_ERROR: i32 = 1;

#[cfg(any(test, target_os = "macos"))]
pub(super) fn reconcile_exited_tree_kill(
    kill_result: io::Result<()>,
    liveness_probe: impl FnOnce() -> io::Result<bool>,
) -> io::Result<ExitedTreeKill> {
    match kill_result {
        Err(kill_error) if kill_error.raw_os_error() == Some(EXITED_EPERM_RAW_OS_ERROR) => {
            match liveness_probe() {
                Ok(true) => Ok(ExitedTreeKill::Settled),
                Ok(false) => Ok(ExitedTreeKill::AwaitingEof(kill_error)),
                Err(probe_error) => Err(probe_error),
            }
        }
        Ok(()) => Ok(ExitedTreeKill::Settled),
        Err(error) => Err(error),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observed_exit_reconciles_a_cached_eperm_without_weakening_drop_cleanup() {
        let mut state = TreeKillState::Settled(Err(KillFailure::from(
            io::Error::from_raw_os_error(EXITED_EPERM_RAW_OS_ERROR),
        )));

        let prior_failure = state_failure(&state);
        let outcome =
            reconcile_kill_state_after_observed_exit(&mut state, prior_failure, || Ok(false))
                .unwrap();
        let ExitedTreeKill::AwaitingEof(error) = outcome else {
            panic!("a cached EPERM with live writers must await EOF")
        };
        assert_eq!(error.raw_os_error(), Some(EXITED_EPERM_RAW_OS_ERROR));
        assert_eq!(
            state_result(&state).unwrap_err().raw_os_error,
            Some(EXITED_EPERM_RAW_OS_ERROR),
            "ordinary and drop cleanup must keep observing EPERM before proof"
        );

        confirm_exited_eperm_after_eof_state(&mut state);
        assert!(state_result(&state).is_ok());
    }

    #[test]
    fn observed_exit_probe_failure_preserves_an_earlier_cached_kill_error() {
        let mut state = TreeKillState::Settled(Err(KillFailure::from(
            io::Error::from_raw_os_error(EXITED_EPERM_RAW_OS_ERROR),
        )));
        let prior_failure = state_failure(&state);

        let error = reconcile_kill_state_after_observed_exit(&mut state, prior_failure, || {
            Err(io::Error::other("probe failed"))
        })
        .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(EXITED_EPERM_RAW_OS_ERROR));
        assert_eq!(
            state_result(&state).unwrap_err().raw_os_error,
            Some(EXITED_EPERM_RAW_OS_ERROR)
        );
    }
}
