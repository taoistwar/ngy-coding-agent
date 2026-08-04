use std::time::Instant;

use coding_agent_core::{
    CheckEvidenceStatus, MAX_VALIDATION_MODEL_RESULT_BYTES, RepositoryCheckCatalog, RequiredCheck,
    RequiredCheckKind, RequiredCheckSelector, RuntimeError, ToolResult, ValidationObservation,
    ValidationRuntime,
};
use tokio_util::sync::CancellationToken;

use crate::runtime_session::{KnownPathRedactor, RuntimeSession};
use crate::{CapturedStream, CargoRunResult, CargoRunStatus, CargoToolError, FingerprintError};

#[async_trait::async_trait]
impl RepositoryCheckCatalog for RuntimeSession {
    async fn required_check_selectors(
        &self,
        cancellation: CancellationToken,
    ) -> Result<Vec<RequiredCheckSelector>, RuntimeError> {
        let catalog = self
            .cargo
            .catalog_read_only(cancellation)
            .await
            .map_err(cargo_runtime_error)?;
        catalog
            .required_check_selectors()
            .map_err(cargo_runtime_error)
    }
}

#[async_trait::async_trait]
impl ValidationRuntime for RuntimeSession {
    async fn run_validation(
        &self,
        check: RequiredCheck,
        cancellation: CancellationToken,
    ) -> Result<ValidationObservation, RuntimeError> {
        let before = self
            .fingerprint
            .collect(cancellation.clone())
            .await
            .map_err(fingerprint_runtime_error)?;
        let started = Instant::now();
        let execution = match check.selector().kind() {
            RequiredCheckKind::CargoCheck => {
                self.cargo
                    .check(
                        self.cargo_jobs_per_task,
                        check.package(),
                        self.validation_timeout,
                        cancellation.clone(),
                    )
                    .await
            }
            RequiredCheckKind::CargoTest => {
                self.cargo
                    .test(
                        self.cargo_jobs_per_task,
                        check.package(),
                        check.integration_test(),
                        self.validation_timeout,
                        cancellation.clone(),
                    )
                    .await
            }
        };
        let execution_duration_ms = elapsed_millis(started);

        // The task cancellation domain also applies to the post-check
        // fingerprint. A cancellation therefore stops the task with no
        // terminal observation; it never mints cancelled evidence.
        let after = self
            .fingerprint
            .collect(cancellation)
            .await
            .map_err(fingerprint_runtime_error)?;
        if before != after {
            return Err(workspace_changed_error());
        }

        match execution {
            Ok(result) => observation_from_run(check, result, &self.output_redactor),
            Err(CargoToolError::TimedOut) => timeout_observation(check, execution_duration_ms),
            Err(error) => Err(cargo_runtime_error(error)),
        }
    }
}

fn observation_from_run(
    check: RequiredCheck,
    result: CargoRunResult,
    redactor: &KnownPathRedactor,
) -> Result<ValidationObservation, RuntimeError> {
    let status = match result.status {
        CargoRunStatus::Passed => CheckEvidenceStatus::Passed,
        CargoRunStatus::Failed | CargoRunStatus::TimedOut => CheckEvidenceStatus::Failed,
        CargoRunStatus::Cancelled => {
            return Err(RuntimeError::new(
                "COMMAND_CANCELLED",
                "Cargo validation was cancelled",
                false,
            ));
        }
    };
    let status_label = match result.status {
        CargoRunStatus::Passed => "passed",
        CargoRunStatus::Failed => "failed",
        CargoRunStatus::TimedOut => "timed_out",
        CargoRunStatus::Cancelled => "cancelled",
    };
    let stdout = retained_stream(&result.command.stdout, redactor);
    let stderr = retained_stream(&result.command.stderr, redactor);
    let content = format!(
        "status: {status_label}\nduration_ms: {}\nexit_code: {:?}\nsignal: {:?}\n\
         captured_output_truncated: {}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        result.command.duration_ms,
        result.command.exit_code,
        result.command.signal,
        result.command.truncated,
    );
    let (content, bounded_truncated) = truncate_utf8(content, MAX_VALIDATION_MODEL_RESULT_BYTES);
    let truncated = result.command.truncated || bounded_truncated;
    let model_result = model_result(content, status, truncated);
    ValidationObservation::try_new(
        model_result,
        check,
        status,
        result.command.duration_ms,
        truncated,
    )
}

fn model_result(content: String, status: CheckEvidenceStatus, truncated: bool) -> ToolResult {
    match (status, truncated) {
        (CheckEvidenceStatus::Passed, false) => ToolResult::text(content),
        (CheckEvidenceStatus::Passed, true) => ToolResult::truncated_text(content),
        (CheckEvidenceStatus::Failed | CheckEvidenceStatus::Cancelled, false) => {
            ToolResult::failed_text(content)
        }
        (CheckEvidenceStatus::Failed | CheckEvidenceStatus::Cancelled, true) => {
            ToolResult::truncated_failed_text(content)
        }
    }
}

fn timeout_observation(
    check: RequiredCheck,
    duration_ms: u64,
) -> Result<ValidationObservation, RuntimeError> {
    ValidationObservation::try_new(
        ToolResult::failed_text(format!(
            "status: timed_out\nduration_ms: {duration_ms}\nCargo validation timed out"
        )),
        check,
        CheckEvidenceStatus::Failed,
        duration_ms,
        false,
    )
}

fn retained_stream(stream: &CapturedStream, redactor: &KnownPathRedactor) -> String {
    let mut retained = Vec::with_capacity(stream.head.len().saturating_add(stream.tail.len()));
    retained.extend_from_slice(&stream.head);
    if !stream.head.is_empty() && !stream.tail.is_empty() {
        retained.extend_from_slice(b"\n...[captured bytes omitted]...\n");
    }
    retained.extend_from_slice(&stream.tail);
    redactor.redact(&String::from_utf8_lossy(&retained))
}

fn truncate_utf8(mut content: String, max_bytes: usize) -> (String, bool) {
    if content.len() <= max_bytes {
        return (content, false);
    }
    const MARKER: &str = "\n...[validation output truncated]";
    let mut end = max_bytes.saturating_sub(MARKER.len()).min(content.len());
    while !content.is_char_boundary(end) {
        end -= 1;
    }
    content.truncate(end);
    content.push_str(MARKER);
    (content, true)
}

fn elapsed_millis(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn cargo_runtime_error(error: CargoToolError) -> RuntimeError {
    let code = error.code();
    RuntimeError::new(
        code,
        "typed Cargo validation failed",
        matches!(code, "COMMAND_TIMED_OUT"),
    )
}

fn fingerprint_runtime_error(error: FingerprintError) -> RuntimeError {
    let code = error.code();
    RuntimeError::new(
        code,
        "workspace fingerprint failed during validation",
        matches!(code, "COMMAND_TIMED_OUT" | "WORKSPACE_CHANGED"),
    )
}

fn workspace_changed_error() -> RuntimeError {
    RuntimeError::new(
        "WORKSPACE_CHANGED",
        "workspace changed while the required check was running",
        true,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncation_preserves_utf8_and_the_exact_byte_bound() {
        let input = "界".repeat(MAX_VALIDATION_MODEL_RESULT_BYTES);
        let (output, truncated) = truncate_utf8(input, MAX_VALIDATION_MODEL_RESULT_BYTES);
        assert!(truncated);
        assert!(output.len() <= MAX_VALIDATION_MODEL_RESULT_BYTES);
        assert!(output.ends_with("...[validation output truncated]"));
    }
}
