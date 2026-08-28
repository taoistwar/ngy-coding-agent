use super::*;

/// Single-use owner for a sentinel whose child spawn has committed.
///
/// Every exit path must either prove cleanup through this owner, transfer a
/// tree-proof continuation, or remain permanently fail-closed. Only a positive
/// OS tree-exit proof authorizes sentinel completion retries.
pub(super) struct SpawnedLivenessOwner {
    liveness: Option<ProcessLivenessSentinel>,
    tasks: TaskTracker,
    fallback: SpawnedLivenessFallback,
    #[cfg(test)]
    proof_continuation_started: Option<Arc<tokio::sync::Notify>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SpawnedLivenessFallback {
    PermanentFailClosed,
    RetryAfterTreeProof,
}

impl SpawnedLivenessOwner {
    pub(super) fn new(
        liveness: ProcessLivenessSentinel,
        tasks: TaskTracker,
        #[cfg(test)] proof_continuation_started: Option<Arc<tokio::sync::Notify>>,
    ) -> Self {
        Self {
            liveness: Some(liveness),
            tasks,
            fallback: SpawnedLivenessFallback::PermanentFailClosed,
            #[cfg(test)]
            proof_continuation_started,
        }
    }

    fn liveness_mut(&mut self) -> &mut ProcessLivenessSentinel {
        self.liveness
            .as_mut()
            .expect("spawned liveness ownership is present")
    }

    fn confirm_completed(&mut self) {
        self.liveness
            .take()
            .expect("confirmed liveness ownership is present");
    }

    fn authorize_retry_after_tree_proof(&mut self) {
        self.fallback = SpawnedLivenessFallback::RetryAfterTreeProof;
    }

    #[cfg(test)]
    fn acknowledge_proof_continuation_started(&mut self) {
        if let Some(started) = self.proof_continuation_started.take() {
            started.notify_one();
        }
    }

    fn handoff_once(&mut self) {
        if let Some(liveness) = self.liveness.take() {
            match self.fallback {
                SpawnedLivenessFallback::PermanentFailClosed => {
                    handoff_liveness_fail_closed(self.tasks.clone(), liveness);
                }
                SpawnedLivenessFallback::RetryAfterTreeProof => {
                    handoff_liveness_retry(self.tasks.clone(), liveness);
                }
            }
        }
    }
}

impl Drop for SpawnedLivenessOwner {
    fn drop(&mut self) {
        self.handoff_once();
    }
}

pub(super) struct ProcessExecution {
    pub(super) worker: Option<JoinHandle<Result<CommandResult, ProcessError>>>,
    pub(super) tree: TreeKillHandle,
    pub(super) abandonment: CancellationToken,
    #[cfg(any(test, feature = "test-support"))]
    pub(super) process_fault: Option<faults::ProcessFaultInvocation>,
    #[cfg(any(test, feature = "test-support"))]
    pub(super) stdout_limit: usize,
}

impl ProcessExecution {
    pub(super) async fn wait(mut self) -> Result<CommandResult, ProcessError> {
        let worker = self.worker.take().expect("process worker is present");
        let result = worker.await.map_err(|_| ProcessError::WorkerFailed)?;
        #[cfg(any(test, feature = "test-support"))]
        if let Some(invocation) = self.process_fault.as_ref() {
            match invocation.fault() {
                Some(faults::ProcessFault::StdoutOverflow) => {
                    invocation.injected();
                    return result.map(|mut result| {
                        let observed = result
                            .stdout
                            .observed_bytes
                            .max(self.stdout_limit.saturating_add(1) as u64);
                        result.stdout.observed_bytes = observed;
                        result.stdout.omitted_observed_bytes = observed
                            .saturating_sub(result.stdout.head.len() as u64)
                            .saturating_sub(result.stdout.tail.len() as u64);
                        result.stdout.truncated = true;
                        result.stdout.complete = false;
                        result.truncated = true;
                        result
                    });
                }
                Some(faults::ProcessFault::ChannelUnknown) => {
                    invocation.injected();
                    return Err(ProcessError::WaitFailed(faults::injected_error(
                        faults::ProcessFault::ChannelUnknown,
                    )));
                }
                Some(
                    faults::ProcessFault::BeforeSpawn
                    | faults::ProcessFault::AfterSpawnUnknown
                    | faults::ProcessFault::Deadline
                    | faults::ProcessFault::WaitUnknown
                    | faults::ProcessFault::KillFailure
                    | faults::ProcessFault::CleanupFailure,
                )
                | None => {}
            }
        }
        result
    }
}

impl Drop for ProcessExecution {
    fn drop(&mut self) {
        self.abandonment.cancel();
        let _ = self.tree.kill_now();
    }
}

pub(super) enum ObservedTermination {
    Exited(Option<ExitStatus>),
    Cancelled,
    TimedOut,
    InputFailed(ProcessError),
    AnchorLost(io::Error),
    WaitFailed(io::Error),
}

#[cfg(target_os = "macos")]
pub(super) fn should_use_exited_tree_kill(observed: &ObservedTermination) -> bool {
    matches!(observed, ObservedTermination::Exited(_))
}

#[cfg(target_os = "macos")]
pub(super) fn reconcile_exited_tree_kill(
    kill_result: io::Result<()>,
    liveness_probe: impl FnOnce() -> io::Result<bool>,
) -> io::Result<()> {
    match kill_result {
        Err(kill_error) if kill_error.raw_os_error() == Some(libc::EPERM) => {
            match liveness_probe() {
                Ok(true) => Ok(()),
                Ok(false) => Err(kill_error),
                Err(probe_error) => Err(probe_error),
            }
        }
        result => result,
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn supervise_child(
    mut child: Child,
    mut input_writer: Option<input::SupervisedExactInputWriter>,
    stdout_task: JoinHandle<io::Result<CapturedStream>>,
    stderr_task: JoinHandle<io::Result<CapturedStream>>,
    tree: TreeKillHandle,
    mut leader_exit: platform::LeaderExit,
    liveness: SpawnedLivenessOwner,
    limits: ProcessLimits,
    timeout: Duration,
    cancellation: CancellationToken,
    abandonment: CancellationToken,
    tasks: TaskTracker,
    started: Instant,
    #[cfg(test)] supervision_fault: Option<SupervisionFault>,
    #[cfg(any(test, feature = "test-support"))] process_fault: Option<
        faults::ProcessFaultInvocation,
    >,
) -> Result<CommandResult, ProcessError> {
    let _worker_guard = TreeWorkerGuard(tree.clone());
    let deadline = TokioInstant::now() + timeout;
    let observed = observe_termination(
        &mut child,
        &mut leader_exit,
        &mut input_writer,
        &cancellation,
        &abandonment,
        deadline,
        #[cfg(test)]
        supervision_fault,
        #[cfg(any(test, feature = "test-support"))]
        process_fault.as_ref(),
    )
    .await;

    let observed = match observed {
        ObservedTermination::AnchorLost(error) => {
            // A wildcard/external waiter consumed the Unix leader. The PGID is
            // no longer identity-bound, so fail without sending a group signal
            // that could target a reused identifier.
            tree.disarm_without_kill();
            let _ = child.start_kill();
            abort_and_join_child_io(input_writer, stdout_task, stderr_task).await;
            handoff_anchor_proof(tasks, tree, leader_exit, liveness);
            return Err(ProcessError::TreeControlLost(error));
        }
        observed => observed,
    };

    let ProvenTreeCleanup {
        status,
        input_writer,
        stdout_task,
        stderr_task,
        cleanup_deadline,
    } = prove_tree_cleanup(
        child,
        input_writer,
        stdout_task,
        stderr_task,
        tree,
        leader_exit,
        liveness,
        limits,
        tasks,
        &observed,
        #[cfg(test)]
        supervision_fault,
        #[cfg(any(test, feature = "test-support"))]
        process_fault.as_ref(),
    )
    .await?;
    let require_input_success = matches!(&observed, ObservedTermination::Exited(_));
    if let Err(error) = input::complete(input_writer, require_input_success, cleanup_deadline).await
    {
        abort_and_join_drains(stdout_task, stderr_task).await;
        return Err(error);
    }
    let (stdout, stderr) = collect_drains_until(cleanup_deadline, stdout_task, stderr_task).await?;

    let cancelled = matches!(&observed, ObservedTermination::Cancelled);
    let timed_out = matches!(&observed, ObservedTermination::TimedOut);
    match observed {
        ObservedTermination::WaitFailed(error) => return Err(ProcessError::WaitFailed(error)),
        ObservedTermination::InputFailed(error) => return Err(error),
        _ => {}
    }
    let truncated = stdout.truncated || stderr.truncated;
    Ok(CommandResult {
        exit_code: status.code(),
        signal: exit_signal(&status),
        timed_out,
        cancelled,
        stdout,
        stderr,
        truncated,
        duration_ms: started.elapsed().as_millis().min(u64::MAX as u128) as u64,
    })
}

struct ProvenTreeCleanup {
    status: ExitStatus,
    input_writer: Option<input::SupervisedExactInputWriter>,
    stdout_task: JoinHandle<io::Result<CapturedStream>>,
    stderr_task: JoinHandle<io::Result<CapturedStream>>,
    cleanup_deadline: TokioInstant,
}

#[allow(clippy::too_many_arguments)]
async fn prove_tree_cleanup(
    mut child: Child,
    input_writer: Option<input::SupervisedExactInputWriter>,
    stdout_task: JoinHandle<io::Result<CapturedStream>>,
    stderr_task: JoinHandle<io::Result<CapturedStream>>,
    tree: TreeKillHandle,
    mut leader_exit: platform::LeaderExit,
    mut liveness: SpawnedLivenessOwner,
    limits: ProcessLimits,
    tasks: TaskTracker,
    observed: &ObservedTermination,
    #[cfg(test)] supervision_fault: Option<SupervisionFault>,
    #[cfg(any(test, feature = "test-support"))] process_fault: Option<
        &faults::ProcessFaultInvocation,
    >,
) -> Result<ProvenTreeCleanup, ProcessError> {
    // Unix observes exit with waitid(WNOWAIT), so the group leader remains a
    // non-reusable PID anchor until this process-group kill has completed. XNU
    // filters SZOMB members while resolving a negative-PID kill, so a group with
    // only the waitable leader left can report EPERM. In that exact macOS case,
    // an EOF sentinel proves that every protocol-participating process exited.
    #[cfg(any(test, feature = "test-support"))]
    let injected_kill_error = process_fault.as_ref().and_then(|invocation| {
        if invocation.fault() == Some(faults::ProcessFault::KillFailure) {
            invocation.injected();
            Some(faults::injected_error(faults::ProcessFault::KillFailure))
        } else {
            None
        }
    });
    #[cfg(not(any(test, feature = "test-support")))]
    let injected_kill_error: Option<io::Error> = None;
    #[cfg(test)]
    let injected_kill_error = injected_kill_error.or_else(|| {
        (supervision_fault == Some(SupervisionFault::KillNow))
            .then(|| injected_supervision_error(SupervisionFault::KillNow))
    });
    let kill_error = if let Some(error) = injected_kill_error {
        tree.inject_kill_failure_for_test(error).err()
    } else {
        #[cfg(target_os = "macos")]
        {
            if should_use_exited_tree_kill(observed) {
                tree.kill_now_after_observed_exit(&leader_exit).err()
            } else {
                tree.kill_now().err()
            }
        }
        #[cfg(not(target_os = "macos"))]
        {
            tree.kill_now().err()
        }
    };
    if kill_error.is_some() {
        let _ = child.start_kill();
    }
    let cleanup_deadline = TokioInstant::now() + limits.cleanup_timeout;
    if let Some(error) = kill_error {
        abort_and_join_child_io(input_writer, stdout_task, stderr_task).await;
        handoff_tree_reap(tasks, child, tree.clone(), leader_exit, liveness);
        return Err(ProcessError::TreeCleanupFailed(error));
    }
    #[cfg(any(test, feature = "test-support"))]
    if process_fault
        .as_ref()
        .is_some_and(|invocation| invocation.fault() == Some(faults::ProcessFault::CleanupFailure))
    {
        let invocation = process_fault
            .as_ref()
            .expect("matched process fault invocation is present");
        invocation.injected();
        abort_and_join_child_io(input_writer, stdout_task, stderr_task).await;
        handoff_tree_reap(tasks, child, tree.clone(), leader_exit, liveness);
        return Err(ProcessError::TreeCleanupFailed(faults::injected_error(
            faults::ProcessFault::CleanupFailure,
        )));
    }
    match time::timeout_at(cleanup_deadline, leader_exit.wait_tree_before_reap()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            abort_and_join_child_io(input_writer, stdout_task, stderr_task).await;
            handoff_tree_reap(tasks, child, tree.clone(), leader_exit, liveness);
            return Err(ProcessError::TreeCleanupFailed(error));
        }
        Err(_) => {
            abort_and_join_child_io(input_writer, stdout_task, stderr_task).await;
            handoff_tree_reap(tasks, child, tree.clone(), leader_exit, liveness);
            return Err(ProcessError::CleanupTimedOut);
        }
    }
    let status = match observed {
        ObservedTermination::Exited(Some(status)) => status.to_owned(),
        _ => match time::timeout_at(cleanup_deadline, child.wait()).await {
            Ok(Ok(status)) => status,
            Ok(Err(error)) => {
                abort_and_join_child_io(input_writer, stdout_task, stderr_task).await;
                handoff_tree_reap(tasks, child, tree.clone(), leader_exit, liveness);
                return Err(ProcessError::TreeControlLost(error));
            }
            Err(_) => {
                abort_and_join_child_io(input_writer, stdout_task, stderr_task).await;
                handoff_tree_reap(tasks, child, tree.clone(), leader_exit, liveness);
                return Err(ProcessError::CleanupTimedOut);
            }
        },
    };

    match time::timeout_at(cleanup_deadline, leader_exit.wait_tree_after_reap()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            abort_and_join_child_io(input_writer, stdout_task, stderr_task).await;
            handoff_tree_proof(tasks, tree.clone(), leader_exit, liveness);
            return Err(ProcessError::TreeCleanupFailed(error));
        }
        Err(_) => {
            abort_and_join_child_io(input_writer, stdout_task, stderr_task).await;
            handoff_tree_proof(tasks, tree.clone(), leader_exit, liveness);
            return Err(ProcessError::CleanupTimedOut);
        }
    }

    liveness.authorize_retry_after_tree_proof();
    if let Err(error) = complete_liveness_with_deadline(&mut liveness, cleanup_deadline).await {
        abort_and_join_child_io(input_writer, stdout_task, stderr_task).await;
        return Err(error);
    }
    Ok(ProvenTreeCleanup {
        status,
        input_writer,
        stdout_task,
        stderr_task,
        cleanup_deadline,
    })
}

#[allow(clippy::too_many_arguments)]
async fn observe_termination(
    child: &mut Child,
    leader_exit: &mut platform::LeaderExit,
    input_writer: &mut Option<input::SupervisedExactInputWriter>,
    cancellation: &CancellationToken,
    abandonment: &CancellationToken,
    deadline: TokioInstant,
    #[cfg(test)] supervision_fault: Option<SupervisionFault>,
    #[cfg(any(test, feature = "test-support"))] process_fault: Option<
        &faults::ProcessFaultInvocation,
    >,
) -> ObservedTermination {
    #[cfg(test)]
    if supervision_fault == Some(SupervisionFault::AnchorLost) {
        return ObservedTermination::AnchorLost(injected_supervision_error(
            SupervisionFault::AnchorLost,
        ));
    }
    #[cfg(any(test, feature = "test-support"))]
    if let Some(invocation) = process_fault {
        match invocation.fault() {
            Some(faults::ProcessFault::Deadline) => {
                invocation.injected();
                return ObservedTermination::TimedOut;
            }
            Some(faults::ProcessFault::WaitUnknown) => {
                invocation.injected();
                return ObservedTermination::WaitFailed(faults::injected_error(
                    faults::ProcessFault::WaitUnknown,
                ));
            }
            Some(
                faults::ProcessFault::BeforeSpawn
                | faults::ProcessFault::AfterSpawnUnknown
                | faults::ProcessFault::StdoutOverflow
                | faults::ProcessFault::ChannelUnknown
                | faults::ProcessFault::KillFailure
                | faults::ProcessFault::CleanupFailure,
            )
            | None => {}
        }
    }
    loop {
        tokio::select! {
            biased;

            _ = cancellation.cancelled() => return ObservedTermination::Cancelled,
            _ = abandonment.cancelled() => return ObservedTermination::Cancelled,
            status = leader_exit.wait(child) => return match status {
                Ok(_status) if cancellation.is_cancelled() => ObservedTermination::Cancelled,
                Ok(status) => ObservedTermination::Exited(status),
                Err(error) if platform::leader_anchor_lost(&error) => {
                    ObservedTermination::AnchorLost(error)
                }
                Err(error) => ObservedTermination::WaitFailed(error),
            },
            input_result = input::wait_for_completion(input_writer) => match input_result {
                Ok(()) => continue,
                Err(error) => return ObservedTermination::InputFailed(error),
            },
            _ = time::sleep_until(deadline) => return deadline_observation(cancellation),
        }
    }
}

pub(super) fn deadline_observation(cancellation: &CancellationToken) -> ObservedTermination {
    if cancellation.is_cancelled() {
        ObservedTermination::Cancelled
    } else {
        ObservedTermination::TimedOut
    }
}

pub(super) async fn collect_drains_until(
    deadline: TokioInstant,
    mut stdout_task: JoinHandle<io::Result<CapturedStream>>,
    mut stderr_task: JoinHandle<io::Result<CapturedStream>>,
) -> Result<(CapturedStream, CapturedStream), ProcessError> {
    match time::timeout_at(deadline, async {
        let (stdout, stderr) = tokio::join!(&mut stdout_task, &mut stderr_task);
        Ok::<_, ProcessError>((join_captured(stdout)?, join_captured(stderr)?))
    })
    .await
    {
        Ok(result) => result,
        Err(_) => {
            abort_and_join_drains(stdout_task, stderr_task).await;
            Err(ProcessError::CleanupTimedOut)
        }
    }
}

async fn abort_and_join_drains(
    stdout_task: JoinHandle<io::Result<CapturedStream>>,
    stderr_task: JoinHandle<io::Result<CapturedStream>>,
) {
    stdout_task.abort();
    stderr_task.abort();
    let _ = tokio::join!(stdout_task, stderr_task);
}

async fn abort_and_join_child_io(
    input_writer: Option<input::SupervisedExactInputWriter>,
    stdout_task: JoinHandle<io::Result<CapturedStream>>,
    stderr_task: JoinHandle<io::Result<CapturedStream>>,
) {
    input::abort_and_join(input_writer).await;
    abort_and_join_drains(stdout_task, stderr_task).await;
}

pub(super) fn handoff_tree_reap(
    tasks: TaskTracker,
    mut child: Child,
    tree: TreeKillHandle,
    mut leader_exit: platform::LeaderExit,
    mut liveness: SpawnedLivenessOwner,
) {
    tasks.spawn(async move {
        #[cfg(test)]
        liveness.acknowledge_proof_continuation_started();
        let _guard = TreeWorkerGuard(tree);
        while leader_exit.wait_tree_before_reap().await.is_err() {
            time::sleep(Duration::from_millis(2)).await;
        }
        loop {
            let _ = child.start_kill();
            if child.wait().await.is_ok() {
                break;
            }
            time::sleep(Duration::from_millis(2)).await;
        }
        while leader_exit.wait_tree_after_reap().await.is_err() {
            time::sleep(Duration::from_millis(2)).await;
        }
        liveness.authorize_retry_after_tree_proof();
        complete_owned_liveness_eventually(&mut liveness).await;
    });
}

fn handoff_tree_proof(
    tasks: TaskTracker,
    tree: TreeKillHandle,
    mut leader_exit: platform::LeaderExit,
    mut liveness: SpawnedLivenessOwner,
) {
    tasks.spawn(async move {
        #[cfg(test)]
        liveness.acknowledge_proof_continuation_started();
        let _guard = TreeWorkerGuard(tree);
        while leader_exit.wait_tree_after_reap().await.is_err() {
            time::sleep(Duration::from_millis(2)).await;
        }
        liveness.authorize_retry_after_tree_proof();
        complete_owned_liveness_eventually(&mut liveness).await;
    });
}

fn handoff_anchor_proof(
    tasks: TaskTracker,
    tree: TreeKillHandle,
    mut leader_exit: platform::LeaderExit,
    mut liveness: SpawnedLivenessOwner,
) {
    tasks.spawn(async move {
        #[cfg(test)]
        liveness.acknowledge_proof_continuation_started();
        let _guard = TreeWorkerGuard(tree);
        while leader_exit.wait_tree_before_reap().await.is_err() {
            time::sleep(Duration::from_millis(2)).await;
        }
        while leader_exit.wait_tree_after_reap().await.is_err() {
            time::sleep(Duration::from_millis(2)).await;
        }
        liveness.authorize_retry_after_tree_proof();
        complete_owned_liveness_eventually(&mut liveness).await;
    });
}

async fn complete_liveness_with_deadline(
    liveness: &mut SpawnedLivenessOwner,
    deadline: TokioInstant,
) -> Result<(), ProcessError> {
    loop {
        match liveness.liveness_mut().try_complete_after_tree_exit() {
            Ok(ProcessCleanupProof::Confirmed) => {
                liveness.confirm_completed();
                return Ok(());
            }
            Ok(ProcessCleanupProof::Unknown) => {
                return Err(ProcessError::LivenessCleanupUnproven);
            }
            Err(error) => return Err(ProcessError::LivenessCleanupFailed(error)),
            Ok(ProcessCleanupProof::Held) => {
                if TokioInstant::now() >= deadline {
                    return Err(ProcessError::CleanupTimedOut);
                }
                time::sleep(Duration::from_millis(2)).await;
            }
        }
    }
}

pub(super) async fn complete_liveness_eventually(mut liveness: ProcessLivenessSentinel) {
    loop {
        match liveness.try_complete_after_tree_exit() {
            Ok(ProcessCleanupProof::Confirmed) => return,
            Ok(ProcessCleanupProof::Held | ProcessCleanupProof::Unknown) | Err(_) => {
                time::sleep(Duration::from_millis(2)).await;
            }
        }
    }
}

async fn complete_owned_liveness_eventually(liveness: &mut SpawnedLivenessOwner) {
    loop {
        match liveness.liveness_mut().try_complete_after_tree_exit() {
            Ok(ProcessCleanupProof::Confirmed) => {
                liveness.confirm_completed();
                return;
            }
            Ok(ProcessCleanupProof::Held | ProcessCleanupProof::Unknown) | Err(_) => {
                time::sleep(Duration::from_millis(2)).await;
            }
        }
    }
}

fn handoff_liveness_fail_closed(tasks: TaskTracker, liveness: ProcessLivenessSentinel) {
    tasks.spawn(async move {
        // No tree proof exists, so neither an exclusive sentinel probe nor a
        // shutdown request may convert this registration into "confirmed".
        std::future::pending::<()>().await;
        drop(liveness);
    });
}

fn handoff_liveness_retry(tasks: TaskTracker, liveness: ProcessLivenessSentinel) {
    tasks.spawn(async move {
        complete_liveness_eventually(liveness).await;
    });
}

pub(super) async fn cleanup_attached_spawn_failure(
    mut child: Child,
    tree: TreeKillHandle,
    mut leader_exit: platform::LeaderExit,
    mut liveness: SpawnedLivenessOwner,
    cleanup_timeout: Duration,
    tasks: TaskTracker,
    #[cfg(test)] supervision_fault: Option<SupervisionFault>,
) -> Result<(), ProcessError> {
    let _guard = TreeWorkerGuard(tree.clone());
    #[cfg(test)]
    let _kill_result = if supervision_fault == Some(SupervisionFault::AttachedCleanupWaitAfterReap)
    {
        tree.inject_kill_failure_for_test(injected_supervision_error(
            SupervisionFault::AttachedCleanupWaitAfterReap,
        ))
    } else {
        tree.kill_now()
    };
    #[cfg(not(test))]
    let _kill_result = tree.kill_now();
    #[cfg(all(test, unix))]
    if supervision_fault == Some(SupervisionFault::AttachedCleanupWaitAfterReap) {
        // Unix must await inherited-pipe EOF before it can reach the injected
        // after-reap failure. The spawned test task is already the proof
        // continuation here and owns every capability needed to finish it.
        liveness.acknowledge_proof_continuation_started();
    }
    let deadline = TokioInstant::now() + cleanup_timeout;
    match time::timeout_at(deadline, leader_exit.wait_tree_before_reap()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            handoff_tree_reap(tasks, child, tree, leader_exit, liveness);
            return Err(ProcessError::TreeCleanupFailed(error));
        }
        Err(_) => {
            handoff_tree_reap(tasks, child, tree, leader_exit, liveness);
            return Err(ProcessError::CleanupTimedOut);
        }
    }
    match time::timeout_at(deadline, child.wait()).await {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => {
            handoff_tree_reap(tasks, child, tree, leader_exit, liveness);
            return Err(ProcessError::TreeControlLost(error));
        }
        Err(_) => {
            handoff_tree_reap(tasks, child, tree, leader_exit, liveness);
            return Err(ProcessError::CleanupTimedOut);
        }
    }
    let wait_after_reap = async {
        #[cfg(test)]
        if supervision_fault == Some(SupervisionFault::AttachedCleanupWaitAfterReap) {
            return Err(injected_supervision_error(
                SupervisionFault::AttachedCleanupWaitAfterReap,
            ));
        }
        leader_exit.wait_tree_after_reap().await
    };
    match time::timeout_at(deadline, wait_after_reap).await {
        Ok(Ok(())) => {
            liveness.authorize_retry_after_tree_proof();
            complete_liveness_with_deadline(&mut liveness, deadline).await
        }
        Ok(Err(error)) => {
            handoff_tree_proof(tasks, tree, leader_exit, liveness);
            Err(ProcessError::TreeCleanupFailed(error))
        }
        Err(_) => {
            handoff_tree_proof(tasks, tree, leader_exit, liveness);
            Err(ProcessError::CleanupTimedOut)
        }
    }
}
