#[cfg(test)]
use super::supervision::handoff_tree_reap;
use super::*;

impl ProcessSupervisor {
    pub(super) async fn start(
        &self,
        command: ValidatedCommand,
        exact_input: Option<ExactChildInput>,
        cancellation: CancellationToken,
    ) -> Result<ProcessExecution, ProcessError> {
        let started = Instant::now();
        // This is process-global because Darwin's pipe()+fcntl fallback cannot
        // atomically create CLOEXEC descriptors. Runtime-owned spawns are
        // serialized across prepare -> spawn so they cannot inherit another
        // command's transient sentinel descriptors.
        let spawn_guard = acquire_process_spawn_lock();
        let PreparedSpawn {
            mut process,
            prepared,
            mut liveness,
        } = self.prepare_spawn(&command, &exact_input)?;
        // Test faults are admitted only after the validated command and every
        // retained dependency/config capability have been revalidated and the
        // platform spawn has been prepared. The controller sees only this
        // one-based admission ordinal, never command contents.
        #[cfg(any(test, feature = "test-support"))]
        let process_fault = faults::admit_current(&self.liveness_scope);
        #[cfg(any(test, feature = "test-support"))]
        if process_fault
            .as_ref()
            .is_some_and(|invocation| invocation.fault() == Some(faults::ProcessFault::BeforeSpawn))
        {
            let invocation = process_fault
                .as_ref()
                .expect("matched process fault invocation is present");
            invocation.injected();
            return Err(ProcessError::SpawnFailed(faults::injected_error(
                faults::ProcessFault::BeforeSpawn,
            )));
        }
        let child = process.spawn().map_err(ProcessError::SpawnFailed)?;
        liveness.mark_spawned();
        let liveness = SpawnedLivenessOwner::new(
            liveness,
            self.tasks.clone(),
            #[cfg(test)]
            self.proof_continuation_started.clone(),
        );
        drop(spawn_guard);

        let PreparedChildIo {
            child,
            input_writer,
            stdout,
            stderr,
            tree,
            liveness,
        } = self
            .prepare_child_io(
                self.attach_child(child, prepared, liveness).await?,
                exact_input,
                #[cfg(any(test, feature = "test-support"))]
                &process_fault,
            )
            .await?;
        self.launch_supervision(
            child,
            input_writer,
            stdout,
            stderr,
            tree,
            liveness,
            command.timeout(),
            cancellation,
            started,
            #[cfg(any(test, feature = "test-support"))]
            process_fault,
        )
    }

    async fn attach_child(
        &self,
        child: Child,
        prepared: platform::Prepared,
        liveness: SpawnedLivenessOwner,
    ) -> Result<AttachedChild, ProcessError> {
        let attached = prepared.attach_and_resume(&child);
        let tree = match attached {
            Ok(attached) => attached,
            Err(error) => {
                cleanup_failed_spawn(child, self.limits.cleanup_timeout, self.tasks.clone())
                    .await?;
                return Err(ProcessError::TreeControlLost(error));
            }
        };
        Ok(AttachedChild {
            child,
            tree,
            liveness,
        })
    }

    async fn prepare_child_io(
        &self,
        attached: AttachedChild,
        exact_input: Option<ExactChildInput>,
        #[cfg(any(test, feature = "test-support"))] process_fault: &Option<
            faults::ProcessFaultInvocation,
        >,
    ) -> Result<PreparedChildIo, ProcessError> {
        let AttachedChild {
            mut child,
            tree,
            liveness,
        } = attached;
        #[cfg(any(test, feature = "test-support"))]
        if process_fault.as_ref().is_some_and(|invocation| {
            invocation.fault() == Some(faults::ProcessFault::AfterSpawnUnknown)
        }) {
            let invocation = process_fault
                .as_ref()
                .expect("matched process fault invocation is present");
            invocation.injected();
            cleanup_attached_spawn_failure(
                child,
                tree.0,
                tree.1,
                liveness,
                self.limits.cleanup_timeout,
                self.tasks.clone(),
                #[cfg(test)]
                self.supervision_fault,
            )
            .await?;
            return Err(ProcessError::InputCompletionUnknown);
        }
        #[cfg(test)]
        if self.supervision_fault == Some(SupervisionFault::AttachAndResume) {
            if let Some(gate) = &self.supervision_gate {
                gate.notified().await;
            }
            handoff_tree_reap(self.tasks.clone(), child, tree.0, tree.1, liveness);
            return Err(ProcessError::TreeControlLost(injected_supervision_error(
                SupervisionFault::AttachAndResume,
            )));
        }
        #[cfg(test)]
        if self.supervision_fault == Some(SupervisionFault::AttachedCleanupWaitAfterReap) {
            if let Some(gate) = &self.supervision_gate {
                gate.notified().await;
            }
            drop(child.stdout.take());
        }
        let (Some(stdout), Some(stderr)) = (child.stdout.take(), child.stderr.take()) else {
            cleanup_attached_spawn_failure(
                child,
                tree.0,
                tree.1,
                liveness,
                self.limits.cleanup_timeout,
                self.tasks.clone(),
                #[cfg(test)]
                self.supervision_fault,
            )
            .await?;
            return Err(ProcessError::MissingOutputPipe);
        };
        let input_writer = match input::spawn_writer(&mut child, exact_input, &self.tasks) {
            Ok(writer) => writer,
            Err(error) => {
                drop((stdout, stderr));
                cleanup_attached_spawn_failure(
                    child,
                    tree.0,
                    tree.1,
                    liveness,
                    self.limits.cleanup_timeout,
                    self.tasks.clone(),
                    #[cfg(test)]
                    self.supervision_fault,
                )
                .await?;
                return Err(error);
            }
        };
        Ok(PreparedChildIo {
            child,
            input_writer,
            stdout,
            stderr,
            tree,
            liveness,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn launch_supervision(
        &self,
        child: Child,
        input_writer: Option<input::SupervisedExactInputWriter>,
        stdout: tokio::process::ChildStdout,
        stderr: tokio::process::ChildStderr,
        tree: (TreeKillHandle, platform::LeaderExit),
        liveness: SpawnedLivenessOwner,
        command_timeout: Duration,
        cancellation: CancellationToken,
        started: Instant,
        #[cfg(any(test, feature = "test-support"))] process_fault: Option<
            faults::ProcessFaultInvocation,
        >,
    ) -> Result<ProcessExecution, ProcessError> {
        let stdout_task = tokio::spawn(drain_stream(stdout, self.limits.stdout_bytes));
        let stderr_task = tokio::spawn(drain_stream(stderr, self.limits.stderr_bytes));
        let external_tree = tree.0.clone();
        let abandonment = CancellationToken::new();
        let limits = self.limits;
        let timeout = command_timeout;
        let worker_tasks = self.tasks.clone();
        let worker_abandonment = abandonment.clone();
        #[cfg(test)]
        let supervision_gate = self.supervision_gate.clone();
        #[cfg(test)]
        let supervision_fault = self.supervision_fault;
        #[cfg(any(test, feature = "test-support"))]
        let worker_process_fault = process_fault.clone();
        let worker = self.tasks.spawn(async move {
            #[cfg(test)]
            if let Some(gate) = supervision_gate {
                gate.notified().await;
            }
            supervise_child(
                child,
                input_writer,
                stdout_task,
                stderr_task,
                tree.0,
                tree.1,
                liveness,
                limits,
                timeout,
                cancellation,
                worker_abandonment,
                worker_tasks,
                started,
                #[cfg(test)]
                supervision_fault,
                #[cfg(any(test, feature = "test-support"))]
                worker_process_fault,
            )
            .await
        });

        Ok(ProcessExecution {
            worker: Some(worker),
            tree: external_tree,
            abandonment,
            #[cfg(any(test, feature = "test-support"))]
            process_fault,
            #[cfg(any(test, feature = "test-support"))]
            stdout_limit: self.limits.stdout_bytes,
        })
    }
}

struct AttachedChild {
    child: Child,
    tree: (TreeKillHandle, platform::LeaderExit),
    liveness: SpawnedLivenessOwner,
}

struct PreparedChildIo {
    child: Child,
    input_writer: Option<input::SupervisedExactInputWriter>,
    stdout: tokio::process::ChildStdout,
    stderr: tokio::process::ChildStderr,
    tree: (TreeKillHandle, platform::LeaderExit),
    liveness: SpawnedLivenessOwner,
}

struct PreparedSpawn {
    process: Command,
    prepared: platform::Prepared,
    liveness: ProcessLivenessSentinel,
}

impl ProcessSupervisor {
    fn prepare_spawn(
        &self,
        command: &ValidatedCommand,
        exact_input: &Option<ExactChildInput>,
    ) -> Result<PreparedSpawn, ProcessError> {
        command
            .executable()
            .revalidate()
            .map_err(ProcessError::CommandPolicy)?;
        command
            .working_directory()
            .revalidate()
            .map_err(ProcessError::CommandPolicy)?;
        for executable in command.dependent_executables() {
            executable
                .revalidate()
                .map_err(ProcessError::CommandPolicy)?;
        }
        for directory in command.dependent_directories() {
            directory
                .revalidate()
                .map_err(ProcessError::CommandPolicy)?;
        }
        if let Some(config) = command.delivery_git_empty_config() {
            config.revalidate().map_err(ProcessError::CommandPolicy)?;
        }
        let executable_file = command
            .executable()
            .cloned_file()
            .map_err(ProcessError::CommandPolicy)?;
        let working_directory = command
            .working_directory()
            .cloned_directory()
            .map_err(ProcessError::CommandPolicy)?;
        let dependent_directories = command
            .dependent_directories()
            .iter()
            .map(|directory| directory.cloned_directory())
            .collect::<Result<Vec<_>, _>>()
            .map_err(ProcessError::CommandPolicy)?;
        #[cfg(unix)]
        let delivery_descriptor_arguments =
            platform::DeliveryDescriptorArguments::try_new(command)?;
        let executable = platform::Executable::new(command.executable().path(), executable_file)
            .map_err(ProcessError::TreeSetupFailed)?;
        let mut process = Command::new(executable.program());
        #[cfg(unix)]
        let child_arguments = delivery_descriptor_arguments.arguments();
        #[cfg(windows)]
        let child_arguments = command.arguments();
        process
            .args(child_arguments)
            .env_clear()
            .envs(command.environment().entries())
            .stdin(input::child_stdin(exact_input))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        process.envs(
            delivery_descriptor_arguments
                .environment()
                .iter()
                .map(|(key, value)| (key, value)),
        );
        #[cfg(unix)]
        if let Some(argv0) = command.unix_argv0() {
            use std::os::unix::process::CommandExt as _;
            process.as_std_mut().arg0(argv0);
        }
        #[cfg(windows)]
        process.current_dir(command.working_directory().path());

        #[cfg(windows)]
        let working_directory_path_leases = command
            .working_directory()
            .acquire_spawn_path_leases()
            .map_err(ProcessError::CommandPolicy)?;
        #[cfg(windows)]
        let dependent_directory_path_leases = command
            .dependent_directories()
            .iter()
            .map(|directory| directory.acquire_spawn_path_leases())
            .collect::<Result<Vec<_>, _>>()
            .map_err(ProcessError::CommandPolicy)?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let liveness = self
            .liveness_scope
            .begin_tree()
            .map_err(ProcessError::LivenessSetupFailed)?;
        #[cfg(unix)]
        let delivery_descriptor_resources =
            delivery_descriptor_arguments.into_inherited_resources();
        #[cfg(unix)]
        let prepared = platform::prepare(
            &mut process,
            executable,
            working_directory,
            dependent_directories,
            delivery_descriptor_resources,
            liveness.raw_descriptor(),
        )
        .map_err(ProcessError::TreeSetupFailed)?;
        #[cfg(windows)]
        liveness
            .make_parent_handle_inheritable()
            .map_err(ProcessError::LivenessSetupFailed)?;
        #[cfg(windows)]
        let prepared = platform::prepare(
            &mut process,
            executable,
            working_directory,
            working_directory_path_leases,
            dependent_directories,
            dependent_directory_path_leases,
        )
        .map_err(ProcessError::TreeSetupFailed)?;
        Ok(PreparedSpawn {
            process,
            prepared,
            liveness,
        })
    }
}

async fn cleanup_failed_spawn(
    mut child: Child,
    cleanup_timeout: Duration,
    tasks: TaskTracker,
) -> Result<(), ProcessError> {
    let _ = child.start_kill();
    match time::timeout(cleanup_timeout, child.wait()).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(error)) => Err(ProcessError::TreeControlLost(error)),
        Err(_) => {
            tasks.spawn(async move {
                let _ = child.start_kill();
                let _ = child.wait().await;
            });
            Err(ProcessError::CleanupTimedOut)
        }
    }
}
