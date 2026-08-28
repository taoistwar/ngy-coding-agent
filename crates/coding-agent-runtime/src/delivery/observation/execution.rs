use tokio_util::sync::CancellationToken;

use crate::ProcessLivenessScope;
use crate::command_policy::ValidatedCommand;
use crate::process_supervisor::{CommandResult, ProcessError, ProcessLimits, ProcessSupervisor};

use super::DeliverySourceError;
use crate::delivery::output::{
    DeliveryCommandExit, classify_machine_result, classify_machine_result_zero_or_one,
    classify_machine_result_zero_or_one_with_output,
};

pub(crate) struct DeliveryCommandExecutor {
    supervisor: ProcessSupervisor,
}

/// Start-preserving failure for the first command in a delivery mutation
/// sequence. This stays private to delivery observation code so callers cannot
/// manufacture a zero-effect claim or use it as a generic command capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DeliveryMutationCommandError {
    /// The supervisor proved that no child could have started.
    NotStarted,
    /// A child may have started, or its completed result was not admissible.
    ChildOrResult(DeliverySourceError),
}

impl DeliveryCommandExecutor {
    pub(crate) fn new(limits: ProcessLimits, scope: ProcessLivenessScope) -> Self {
        Self {
            supervisor: ProcessSupervisor::new(limits, scope),
        }
    }

    pub(crate) const fn supervisor(&self) -> &ProcessSupervisor {
        &self.supervisor
    }

    pub(crate) async fn run(
        &self,
        command: ValidatedCommand,
        cancellation: CancellationToken,
        output_limit: usize,
    ) -> Result<Vec<u8>, DeliverySourceError> {
        let result = self.supervisor.run(command, cancellation).await?;
        classify_machine_result(&result, output_limit)
    }

    /// Runs the first real mutation while retaining the supervisor's narrow
    /// proof that no child could have started. Ordinary delivery execution
    /// deliberately collapses this distinction; only the first mutation may
    /// use it to preserve a known-not-applied outcome.
    pub(super) async fn run_start_preserving_mutation(
        &self,
        command: ValidatedCommand,
        cancellation: CancellationToken,
        output_limit: usize,
    ) -> Result<Vec<u8>, DeliveryMutationCommandError> {
        classify_start_preserving_mutation_result(
            self.supervisor.run(command, cancellation).await,
            output_limit,
        )
    }

    /// Runs one fixed quiet predicate whose only admitted outcomes are Git
    /// exit 0 (matched) and 1 (not matched). Predicate commands are expected
    /// to produce no output; any stdout is treated as an unexpected machine
    /// result rather than passed to a caller.
    pub(crate) async fn run_predicate(
        &self,
        command: ValidatedCommand,
        cancellation: CancellationToken,
        output_limit: usize,
    ) -> Result<DeliveryCommandExit, DeliverySourceError> {
        let result = self.supervisor.run(command, cancellation).await?;
        classify_machine_result_zero_or_one(&result, output_limit)
    }

    /// Runs the one fixed delivery command whose documented 0/1 result carries
    /// a bounded NUL-delimited machine protocol on stdout. The caller still
    /// cannot choose accepted exit statuses or bypass stream completeness.
    pub(crate) async fn run_machine_protocol(
        &self,
        command: ValidatedCommand,
        cancellation: CancellationToken,
        output_limit: usize,
    ) -> Result<(DeliveryCommandExit, Vec<u8>), DeliverySourceError> {
        let result = self.supervisor.run(command, cancellation).await?;
        classify_machine_result_zero_or_one_with_output(&result, output_limit)
    }
}

fn classify_start_preserving_mutation_result(
    result: Result<CommandResult, ProcessError>,
    output_limit: usize,
) -> Result<Vec<u8>, DeliveryMutationCommandError> {
    let result = match result {
        Ok(result) => result,
        Err(error) if error.child_could_not_have_started() => {
            return Err(DeliveryMutationCommandError::NotStarted);
        }
        Err(error) => {
            return Err(DeliveryMutationCommandError::ChildOrResult(error.into()));
        }
    };
    classify_machine_result(&result, output_limit)
        .map_err(DeliveryMutationCommandError::ChildOrResult)
}

#[cfg(test)]
mod tests {
    use std::io;

    use crate::process_supervisor::CapturedStream;

    use super::*;

    fn empty_stream() -> CapturedStream {
        CapturedStream {
            head: Vec::new(),
            tail: Vec::new(),
            observed_bytes: 0,
            omitted_observed_bytes: 0,
            truncated: false,
            complete: true,
        }
    }

    fn command_result(exit_code: i32) -> CommandResult {
        CommandResult {
            exit_code: Some(exit_code),
            signal: None,
            timed_out: false,
            cancelled: false,
            stdout: empty_stream(),
            stderr: empty_stream(),
            truncated: false,
            duration_ms: 0,
        }
    }

    #[test]
    fn mutation_execution_preserves_only_proven_not_started() {
        assert_eq!(
            classify_start_preserving_mutation_result(
                Err(ProcessError::SpawnFailed(io::Error::other("injected"))),
                32,
            ),
            Err(DeliveryMutationCommandError::NotStarted),
        );
        assert_eq!(
            classify_start_preserving_mutation_result(Err(ProcessError::WorkerFailed), 32),
            Err(DeliveryMutationCommandError::ChildOrResult(
                DeliverySourceError::ProcessCleanupUnproven,
            )),
        );
        assert_eq!(
            classify_start_preserving_mutation_result(Ok(command_result(1)), 32),
            Err(DeliveryMutationCommandError::ChildOrResult(
                DeliverySourceError::CommandFailed,
            )),
        );
    }
}
