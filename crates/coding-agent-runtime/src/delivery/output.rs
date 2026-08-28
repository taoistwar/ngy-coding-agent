use crate::process_supervisor::{CapturedStream, CommandResult};

use super::DeliverySourceError;

/// The only non-success exit contract admitted by fixed delivery predicates.
/// `Matched` maps Git exit status 0; `NotMatched` maps its documented status
/// 1. No caller can supply another expected status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeliveryCommandExit {
    Matched,
    NotMatched,
}

pub(crate) fn classify_machine_result(
    result: &CommandResult,
    output_limit: usize,
) -> Result<Vec<u8>, DeliverySourceError> {
    let (_, stdout) = classify_machine_result_for_exit_codes(result, output_limit, &[0])?;
    Ok(stdout)
}

/// Classifies a fixed Git predicate whose documented outcome is encoded as
/// exit 0 (equal) or 1 (different). Process interruption, signals, output
/// loss, stderr, and every other exit remain hard failures.
pub(crate) fn classify_machine_result_zero_or_one(
    result: &CommandResult,
    output_limit: usize,
) -> Result<DeliveryCommandExit, DeliverySourceError> {
    let (outcome, stdout) = classify_machine_result_zero_or_one_with_output(result, output_limit)?;
    if !stdout.is_empty() {
        return Err(DeliverySourceError::CommandFailed);
    }
    Ok(outcome)
}

/// Classifies the fixed 0/1 result contract while retaining bounded stdout for
/// the one typed command whose documented machine protocol is carried by both
/// outcomes: `git merge-tree --write-tree --messages --name-only -z`.
///
/// This is deliberately crate-private and does not provide a caller-selectable
/// exit-code list. Stderr, stream loss, truncation, cancellation, signals, and
/// every status other than 0/1 remain hard failures.
pub(crate) fn classify_machine_result_zero_or_one_with_output(
    result: &CommandResult,
    output_limit: usize,
) -> Result<(DeliveryCommandExit, Vec<u8>), DeliverySourceError> {
    let (exit_code, stdout) =
        classify_machine_result_for_exit_codes(result, output_limit, &[0, 1])?;
    let outcome = match exit_code {
        0 => DeliveryCommandExit::Matched,
        1 => DeliveryCommandExit::NotMatched,
        _ => return Err(DeliverySourceError::Internal),
    };
    Ok((outcome, stdout))
}

fn classify_machine_result_for_exit_codes(
    result: &CommandResult,
    output_limit: usize,
    allowed_exit_codes: &[i32],
) -> Result<(i32, Vec<u8>), DeliverySourceError> {
    if result.cancelled {
        return Err(DeliverySourceError::Cancelled);
    }
    if result.timed_out {
        return Err(DeliverySourceError::TimedOut);
    }
    let exit_code = result.exit_code.ok_or(DeliverySourceError::CommandFailed)?;
    if result.signal.is_some() || !allowed_exit_codes.contains(&exit_code) {
        return Err(DeliverySourceError::CommandFailed);
    }
    if result.truncated {
        return Err(DeliverySourceError::BoundsExceeded);
    }

    let stdout = complete_stream(&result.stdout, output_limit)?;
    let stderr = complete_stream(&result.stderr, output_limit)?;
    if stderr.is_empty() {
        Ok((exit_code, stdout))
    } else {
        Err(DeliverySourceError::CommandFailed)
    }
}

fn complete_stream(
    stream: &CapturedStream,
    output_limit: usize,
) -> Result<Vec<u8>, DeliverySourceError> {
    let retained = stream.head.len().saturating_add(stream.tail.len());
    if !stream.complete
        || stream.truncated
        || stream.omitted_observed_bytes != 0
        || stream.observed_bytes != retained as u64
        || retained > output_limit
    {
        return Err(DeliverySourceError::BoundsExceeded);
    }
    let mut output = Vec::with_capacity(retained);
    output.extend_from_slice(&stream.head);
    output.extend_from_slice(&stream.tail);
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream(bytes: &[u8]) -> CapturedStream {
        CapturedStream {
            head: bytes.to_vec(),
            tail: Vec::new(),
            observed_bytes: bytes.len() as u64,
            omitted_observed_bytes: 0,
            truncated: false,
            complete: true,
        }
    }

    fn result(exit_code: Option<i32>, stdout: &[u8], stderr: &[u8]) -> CommandResult {
        CommandResult {
            exit_code,
            signal: None,
            timed_out: false,
            cancelled: false,
            stdout: stream(stdout),
            stderr: stream(stderr),
            truncated: false,
            duration_ms: 0,
        }
    }

    #[test]
    fn fixed_predicates_map_only_clean_zero_or_one_to_typed_outcomes() {
        assert_eq!(
            classify_machine_result_zero_or_one(&result(Some(0), b"", b""), 32),
            Ok(DeliveryCommandExit::Matched)
        );
        assert_eq!(
            classify_machine_result_zero_or_one(&result(Some(1), b"", b""), 32),
            Ok(DeliveryCommandExit::NotMatched)
        );
        assert_eq!(
            classify_machine_result_zero_or_one(&result(Some(2), b"", b""), 32),
            Err(DeliverySourceError::CommandFailed)
        );
        assert_eq!(
            classify_machine_result_zero_or_one(&result(Some(1), b"", b"diagnostic"), 32),
            Err(DeliverySourceError::CommandFailed)
        );
        assert_eq!(
            classify_machine_result_zero_or_one(&result(Some(1), b"unexpected", b""), 32),
            Err(DeliverySourceError::CommandFailed)
        );
        assert_eq!(
            classify_machine_result(&result(Some(1), b"", b""), 32),
            Err(DeliverySourceError::CommandFailed)
        );
    }

    #[test]
    fn fixed_machine_protocol_may_retain_only_clean_bounded_zero_or_one_stdout() {
        assert_eq!(
            classify_machine_result_zero_or_one_with_output(&result(Some(0), b"tree\0\0", b""), 32),
            Ok((DeliveryCommandExit::Matched, b"tree\0\0".to_vec()))
        );
        assert_eq!(
            classify_machine_result_zero_or_one_with_output(&result(Some(1), b"tree\0", b""), 32),
            Ok((DeliveryCommandExit::NotMatched, b"tree\0".to_vec()))
        );
        assert_eq!(
            classify_machine_result_zero_or_one_with_output(&result(Some(2), b"tree\0", b""), 32),
            Err(DeliverySourceError::CommandFailed)
        );
    }
}
