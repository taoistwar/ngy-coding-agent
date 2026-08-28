use std::fmt;

use tokio_util::sync::CancellationToken;

use crate::WorktreeIdentity;
use crate::command_policy::DeliveryGitCommitEnvironment;
use crate::process_supervisor::ExactChildInput;

use super::command::DeliverySourceMutationCommands;
use super::observation::execution::DeliveryCommandExecutor;
use super::{
    DeliveryCommitOid, DeliveryGitObjectFormat, DeliverySourceError, DeliveryTreeOid,
    ProbedDeliveryGit,
};

pub(super) const DELIVERY_COMMIT_MESSAGE_TEMPLATE_VERSION: u32 = 1;
#[cfg(test)]
pub(super) const DELIVERY_COMMIT_AUTHOR_NAME: &str = "Coding Agent";
#[cfg(test)]
pub(super) const DELIVERY_COMMIT_AUTHOR_EMAIL: &str = "coding-agent@localhost";

const UTC_OFFSET: &str = "+0000";
/// A positive Unix timestamp accepted by the Git versions supported by the
/// delivery probe. It is deliberately fixed so the preflight object remains
/// deterministic and cannot be confused with the later durable commit.
const PREFLIGHT_EPOCH_SECONDS: i64 = 1_700_000_000;
const AUTHOR_HEADER_PREFIX: &str = "author Coding Agent <coding-agent@localhost> ";
const COMMITTER_HEADER_PREFIX: &str = "committer Coding Agent <coding-agent@localhost> ";
const PREFLIGHT_COMMIT_MESSAGE: &[u8] = b"coding-agent: preflight source candidate\n";

/// Immutable, application-owned input to one deterministic `git commit-tree`
/// invocation.
///
/// The constructor accepts only the canonical task identity and scalar values
/// persisted with `ObjectPending`; it deliberately does not accept a caller
/// supplied author, committer, message, or timezone.
#[derive(Clone, PartialEq, Eq)]
pub(super) struct DeliveryCommitInput {
    task_id: String,
    attempt: u32,
    message: Vec<u8>,
    author_header: Vec<u8>,
    committer_header: Vec<u8>,
    epoch_seconds: i64,
    author_date: String,
}

impl DeliveryCommitInput {
    pub(super) fn try_new(
        task_id: &str,
        attempt: u64,
        epoch_seconds: i64,
    ) -> Result<Self, DeliverySourceError> {
        let attempt = u32::try_from(attempt).map_err(|_| DeliverySourceError::Internal)?;
        if !is_canonical_task_id(task_id) || epoch_seconds <= 0 {
            return Err(DeliverySourceError::Internal);
        }

        let author_date = format!("{epoch_seconds} {UTC_OFFSET}");
        let mut message =
            format!("coding-agent: deliver task {task_id} attempt {attempt}").into_bytes();
        message.push(b'\n');

        let author_header = format!("{AUTHOR_HEADER_PREFIX}{author_date}").into_bytes();
        let committer_header = format!("{COMMITTER_HEADER_PREFIX}{author_date}").into_bytes();
        Ok(Self {
            task_id: task_id.to_owned(),
            attempt,
            message,
            author_header,
            committer_header,
            epoch_seconds,
            author_date,
        })
    }

    /// Fixed metadata for the temporary object used only by Task 13
    /// `merge-tree` preflight.  It is intentionally unrelated to the final
    /// ObjectPending source-commit metadata: user confirmation is required
    /// before that durable object intent may be created.
    fn preflight_only() -> Self {
        let author_date = format!("{PREFLIGHT_EPOCH_SECONDS} {UTC_OFFSET}");
        Self {
            task_id: String::new(),
            attempt: 0,
            message: PREFLIGHT_COMMIT_MESSAGE.to_vec(),
            author_header: format!("{AUTHOR_HEADER_PREFIX}{author_date}").into_bytes(),
            committer_header: format!("{COMMITTER_HEADER_PREFIX}{author_date}").into_bytes(),
            epoch_seconds: PREFLIGHT_EPOCH_SECONDS,
            author_date,
        }
    }

    pub(super) fn message_bytes(&self) -> &[u8] {
        &self.message
    }

    pub(super) const fn epoch_seconds(&self) -> i64 {
        self.epoch_seconds
    }

    fn matches_identity(&self, identity: &WorktreeIdentity) -> bool {
        self.task_id == identity.task_id() && self.attempt == identity.attempt()
    }

    /// Exact, clean-environment additions for the `commit-tree` child.
    ///
    /// The caller merges these values into an already typed empty Git
    /// environment; it must not take values from the ambient process.
    #[cfg(test)]
    pub(super) fn environment_entries(&self) -> [(&'static str, &str); 6] {
        [
            ("GIT_AUTHOR_NAME", DELIVERY_COMMIT_AUTHOR_NAME),
            ("GIT_AUTHOR_EMAIL", DELIVERY_COMMIT_AUTHOR_EMAIL),
            ("GIT_AUTHOR_DATE", &self.author_date),
            ("GIT_COMMITTER_NAME", DELIVERY_COMMIT_AUTHOR_NAME),
            ("GIT_COMMITTER_EMAIL", DELIVERY_COMMIT_AUTHOR_EMAIL),
            ("GIT_COMMITTER_DATE", &self.author_date),
        ]
    }

    pub(super) fn author_header_bytes(&self) -> &[u8] {
        &self.author_header
    }

    pub(super) fn committer_header_bytes(&self) -> &[u8] {
        &self.committer_header
    }
}

impl fmt::Debug for DeliveryCommitInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryCommitInput(<validated>)")
    }
}

/// Public, fixed-shape metadata for a deterministic P4-B source object.
///
/// It deliberately exposes only the persisted scalar inputs. The fixed
/// identity, UTC offset, template version, and exact message are owned by the
/// runtime and cannot be supplied by a caller.
#[derive(Clone, PartialEq, Eq)]
pub struct DeliverySourceCommitInput {
    inner: DeliveryCommitInput,
}

impl DeliverySourceCommitInput {
    pub fn try_new(
        task_id: &str,
        attempt: u64,
        epoch_seconds: i64,
    ) -> Result<Self, DeliverySourceError> {
        DeliveryCommitInput::try_new(task_id, attempt, epoch_seconds).map(|inner| Self { inner })
    }

    pub const fn message_template_version(&self) -> u32 {
        DELIVERY_COMMIT_MESSAGE_TEMPLATE_VERSION
    }

    pub(super) fn inner(&self) -> &DeliveryCommitInput {
        &self.inner
    }

    pub(super) const fn epoch_seconds(&self) -> i64 {
        self.inner.epoch_seconds()
    }

    pub(super) fn message_bytes(&self) -> &[u8] {
        self.inner.message_bytes()
    }

    pub(crate) fn matches_identity(&self, identity: &WorktreeIdentity) -> bool {
        self.inner.matches_identity(identity)
    }
}

impl fmt::Debug for DeliverySourceCommitInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliverySourceCommitInput(<validated>)")
    }
}

/// Verifies the exact raw response shape of `git cat-file --batch` for a
/// source commit that was addressed by `expected_commit`.
///
/// The normal batch protocol is `oid type size LF raw-object LF`. Rather than
/// accepting a loosely parsed commit, this compares the raw object payload to
/// the one and only header layout P4-B persists: one tree, one parent, fixed
/// author/committer headers and the exact LF-terminated message bytes.
pub(super) fn verify_batched_commit(
    response: &[u8],
    expected_commit: &DeliveryCommitOid,
    expected_tree: &DeliveryTreeOid,
    expected_parent: &DeliveryCommitOid,
    input: &DeliveryCommitInput,
) -> Result<(), DeliverySourceError> {
    let expected_payload = expected_commit_payload(expected_tree, expected_parent, input);
    let (header, remainder) = split_first_line(response)?;
    verify_batch_header(header, expected_commit, expected_payload.len())?;
    if remainder.len() != expected_payload.len().saturating_add(1)
        || !remainder.ends_with(b"\n")
        || remainder[..expected_payload.len()] != expected_payload
    {
        return Err(DeliverySourceError::CommandFailed);
    }
    Ok(())
}

/// Verifies the exact persisted source-commit shape without exposing the
/// private fixed metadata carried by [`DeliverySourceCommitInput`]. Later
/// cleanup phases use this wrapper when the linked worktree is already gone:
/// accepting only a generic `commit` object there would lose the deterministic
/// tree, parent, author, committer, and message proof established at creation.
pub(super) fn verify_batched_source_commit(
    response: &[u8],
    expected_commit: &DeliveryCommitOid,
    expected_tree: &DeliveryTreeOid,
    expected_parent: &DeliveryCommitOid,
    input: &DeliverySourceCommitInput,
) -> Result<(), DeliverySourceError> {
    verify_batched_commit(
        response,
        expected_commit,
        expected_tree,
        expected_parent,
        input.inner(),
    )
}

/// Creates the one deterministic, unreferenced source commit authorized by
/// the caller's already-persisted `ObjectPending` intent, then proves the raw
/// object has the expected fixed shape.
///
/// This performs no ref, real-index, or worktree mutation.  A caller must
/// treat any child outcome that cannot be classified by `DeliveryCommandExecutor`
/// as a separate reconciliation concern before attempting a subsequent side
/// effect.
pub(super) struct SourceCommitBuildRequest<'a> {
    pub(super) executor: &'a DeliveryCommandExecutor,
    pub(super) commands: &'a DeliverySourceMutationCommands,
    pub(super) probe: &'a ProbedDeliveryGit,
    pub(super) tree: &'a DeliveryTreeOid,
    pub(super) parent: &'a DeliveryCommitOid,
    pub(super) input: &'a DeliverySourceCommitInput,
    pub(super) cancellation: CancellationToken,
    pub(super) output_limit: usize,
}

pub(super) async fn build_and_verify_source_commit(
    request: SourceCommitBuildRequest<'_>,
) -> Result<DeliveryCommitOid, DeliverySourceError> {
    let SourceCommitBuildRequest {
        executor,
        commands,
        probe,
        tree,
        parent,
        input,
        cancellation,
        output_limit,
    } = request;
    if cancellation.is_cancelled() {
        return Err(DeliverySourceError::Cancelled);
    }
    probe
        .verify_current_executable()
        .map_err(|_| DeliverySourceError::AuthenticationChanged)?;
    build_and_verify_commit(
        executor,
        commands,
        probe,
        tree,
        parent,
        input.inner(),
        cancellation,
        output_limit,
    )
    .await
}

/// Creates only the fixed, unreachable commit used as the source side of a
/// Task 13 `merge-tree` calculation. This must never be repurposed as the
/// durable source commit: it deliberately has no task/attempt metadata.
pub(super) async fn build_and_verify_preflight_source_commit(
    executor: &DeliveryCommandExecutor,
    commands: &DeliverySourceMutationCommands,
    probe: &ProbedDeliveryGit,
    tree: &DeliveryTreeOid,
    parent: &DeliveryCommitOid,
    cancellation: CancellationToken,
    output_limit: usize,
) -> Result<DeliveryCommitOid, DeliverySourceError> {
    let input = DeliveryCommitInput::preflight_only();
    build_and_verify_commit(
        executor,
        commands,
        probe,
        tree,
        parent,
        &input,
        cancellation,
        output_limit,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn build_and_verify_commit(
    executor: &DeliveryCommandExecutor,
    commands: &DeliverySourceMutationCommands,
    probe: &ProbedDeliveryGit,
    tree: &DeliveryTreeOid,
    parent: &DeliveryCommitOid,
    input: &DeliveryCommitInput,
    cancellation: CancellationToken,
    output_limit: usize,
) -> Result<DeliveryCommitOid, DeliverySourceError> {
    let metadata = DeliveryGitCommitEnvironment::try_new(input.epoch_seconds())
        .map_err(|_| DeliverySourceError::Internal)?;
    let exact_input = ExactChildInput::try_new(input.message_bytes().to_vec())
        .map_err(|_| DeliverySourceError::BoundsExceeded)?;
    let commit_output = executor
        .run(
            commands.commit_tree(tree, parent, exact_input, &metadata)?,
            cancellation.clone(),
            output_limit,
        )
        .await?;
    let commit = parse_created_commit_oid(&commit_output, probe.object_format())?;
    let inspected = executor
        .run(
            commands.inspect_commit(&commit)?,
            cancellation,
            output_limit,
        )
        .await?;
    verify_batched_commit(&inspected, &commit, tree, parent, input)?;
    Ok(commit)
}

/// Inputs for re-proving a persisted expected source commit.  Grouping these
/// capability-bound values avoids a broad helper signature and keeps the
/// verification boundary visibly separate from object construction.
pub(super) struct SourceCommitVerificationRequest<'a> {
    pub(super) executor: &'a DeliveryCommandExecutor,
    pub(super) commands: &'a DeliverySourceMutationCommands,
    pub(super) probe: &'a ProbedDeliveryGit,
    pub(super) expected_commit: &'a DeliveryCommitOid,
    pub(super) expected_tree: &'a DeliveryTreeOid,
    pub(super) expected_parent: &'a DeliveryCommitOid,
    pub(super) input: &'a DeliverySourceCommitInput,
    pub(super) cancellation: CancellationToken,
    pub(super) output_limit: usize,
}

/// Re-proves a previously persisted expected source commit without creating a
/// new object or touching the real index/ref.  CommitPending apply and
/// recovery both use this exact verifier immediately before they rely on the
/// expected object identity.
pub(super) async fn verify_existing_source_commit(
    request: SourceCommitVerificationRequest<'_>,
) -> Result<(), DeliverySourceError> {
    let SourceCommitVerificationRequest {
        executor,
        commands,
        probe,
        expected_commit,
        expected_tree,
        expected_parent,
        input,
        cancellation,
        output_limit,
    } = request;
    if cancellation.is_cancelled() {
        return Err(DeliverySourceError::Cancelled);
    }
    probe
        .verify_current_executable()
        .map_err(|_| DeliverySourceError::AuthenticationChanged)?;
    let inspected = executor
        .run(
            commands.inspect_commit(expected_commit)?,
            cancellation,
            output_limit,
        )
        .await?;
    verify_batched_commit(
        &inspected,
        expected_commit,
        expected_tree,
        expected_parent,
        input.inner(),
    )
}

fn parse_created_commit_oid(
    output: &[u8],
    object_format: DeliveryGitObjectFormat,
) -> Result<DeliveryCommitOid, DeliverySourceError> {
    let length = object_format.hexadecimal_length();
    if output.len() != length.saturating_add(1) || output.get(length) != Some(&b'\n') {
        return Err(DeliverySourceError::CommandFailed);
    }
    let object_id =
        std::str::from_utf8(&output[..length]).map_err(|_| DeliverySourceError::CommandFailed)?;
    DeliveryCommitOid::try_new(object_id, object_format).ok_or(DeliverySourceError::CommandFailed)
}

fn expected_commit_payload(
    tree: &DeliveryTreeOid,
    parent: &DeliveryCommitOid,
    input: &DeliveryCommitInput,
) -> Vec<u8> {
    let mut payload = Vec::with_capacity(
        b"tree \nparent \n\n\n\n".len()
            + tree.as_str().len()
            + parent.as_str().len()
            + input.author_header.len()
            + input.committer_header.len()
            + input.message.len(),
    );
    payload.extend_from_slice(b"tree ");
    payload.extend_from_slice(tree.as_str().as_bytes());
    payload.extend_from_slice(b"\nparent ");
    payload.extend_from_slice(parent.as_str().as_bytes());
    payload.extend_from_slice(b"\n");
    payload.extend_from_slice(input.author_header_bytes());
    payload.extend_from_slice(b"\n");
    payload.extend_from_slice(input.committer_header_bytes());
    payload.extend_from_slice(b"\n\n");
    payload.extend_from_slice(input.message_bytes());
    payload
}

fn split_first_line(response: &[u8]) -> Result<(&[u8], &[u8]), DeliverySourceError> {
    let newline = response
        .iter()
        .position(|byte| *byte == b'\n')
        .ok_or(DeliverySourceError::CommandFailed)?;
    let (header, remainder) = response.split_at(newline);
    if header.contains(&b'\r') {
        return Err(DeliverySourceError::CommandFailed);
    }
    Ok((header, &remainder[1..]))
}

fn verify_batch_header(
    header: &[u8],
    expected_commit: &DeliveryCommitOid,
    expected_size: usize,
) -> Result<(), DeliverySourceError> {
    let mut fields = header.split(|byte| *byte == b' ');
    let oid = fields.next().ok_or(DeliverySourceError::CommandFailed)?;
    let object_type = fields.next().ok_or(DeliverySourceError::CommandFailed)?;
    let size = fields.next().ok_or(DeliverySourceError::CommandFailed)?;
    if fields.next().is_some()
        || oid != expected_commit.as_str().as_bytes()
        || object_type != b"commit"
        || parse_decimal_size(size)? != expected_size
    {
        return Err(DeliverySourceError::CommandFailed);
    }
    Ok(())
}

fn parse_decimal_size(value: &[u8]) -> Result<usize, DeliverySourceError> {
    if value.is_empty() || (value.len() > 1 && value[0] == b'0') {
        return Err(DeliverySourceError::CommandFailed);
    }
    value.iter().try_fold(0usize, |total, byte| {
        if !byte.is_ascii_digit() {
            return Err(DeliverySourceError::CommandFailed);
        }
        total
            .checked_mul(10)
            .and_then(|value| value.checked_add(usize::from(*byte - b'0')))
            .ok_or(DeliverySourceError::CommandFailed)
    })
}

fn is_canonical_task_id(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')
            }
        })
}

#[cfg(test)]
mod tests {
    use super::super::DeliveryGitObjectFormat;
    use super::*;

    const TASK_ID: &str = "123e4567-e89b-12d3-a456-426614174000";

    #[test]
    fn input_uses_fixed_identity_utc_timestamp_and_exact_message() {
        let input = DeliveryCommitInput::try_new(TASK_ID, 7, 1_700_000_000).unwrap();

        assert_eq!(
            input.message_bytes(),
            b"coding-agent: deliver task 123e4567-e89b-12d3-a456-426614174000 attempt 7\n"
        );
        assert_eq!(
            input.environment_entries(),
            [
                ("GIT_AUTHOR_NAME", "Coding Agent"),
                ("GIT_AUTHOR_EMAIL", "coding-agent@localhost"),
                ("GIT_AUTHOR_DATE", "1700000000 +0000"),
                ("GIT_COMMITTER_NAME", "Coding Agent"),
                ("GIT_COMMITTER_EMAIL", "coding-agent@localhost"),
                ("GIT_COMMITTER_DATE", "1700000000 +0000"),
            ]
        );
        assert_eq!(
            input.author_header_bytes(),
            b"author Coding Agent <coding-agent@localhost> 1700000000 +0000"
        );
        assert_eq!(
            input.committer_header_bytes(),
            b"committer Coding Agent <coding-agent@localhost> 1700000000 +0000"
        );
        assert_eq!(format!("{input:?}"), "DeliveryCommitInput(<validated>)");
    }

    #[test]
    fn input_rejects_noncanonical_task_id_and_nonpositive_epoch() {
        for task_id in [
            "123E4567-e89b-12d3-a456-426614174000",
            "123e4567e89b12d3a456426614174000",
            "123e4567-e89b-12d3-a456-42661417400z",
        ] {
            assert_eq!(
                DeliveryCommitInput::try_new(task_id, 0, 0),
                Err(DeliverySourceError::Internal)
            );
        }
        assert_eq!(
            DeliveryCommitInput::try_new(TASK_ID, 0, -1),
            Err(DeliverySourceError::Internal)
        );
        assert_eq!(
            DeliveryCommitInput::try_new(TASK_ID, 0, 0),
            Err(DeliverySourceError::Internal)
        );
        assert_eq!(
            DeliveryCommitInput::try_new(TASK_ID, u64::from(u32::MAX) + 1, 0),
            Err(DeliverySourceError::Internal)
        );
    }

    #[test]
    fn preflight_input_uses_fixed_positive_metadata_without_task_provenance() {
        let input = DeliveryCommitInput::preflight_only();

        assert_eq!(input.epoch_seconds(), PREFLIGHT_EPOCH_SECONDS);
        assert_eq!(
            input.message_bytes(),
            b"coding-agent: preflight source candidate\n"
        );
        assert_eq!(
            input.environment_entries(),
            [
                ("GIT_AUTHOR_NAME", "Coding Agent"),
                ("GIT_AUTHOR_EMAIL", "coding-agent@localhost"),
                ("GIT_AUTHOR_DATE", "1700000000 +0000"),
                ("GIT_COMMITTER_NAME", "Coding Agent"),
                ("GIT_COMMITTER_EMAIL", "coding-agent@localhost"),
                ("GIT_COMMITTER_DATE", "1700000000 +0000"),
            ]
        );
    }

    #[test]
    fn batch_verifier_accepts_only_the_exact_expected_object() {
        let (input, commit, tree, parent) = fixture();
        let payload = expected_commit_payload(&tree, &parent, &input);
        let response = batch_response(&commit, &payload);

        assert!(verify_batched_commit(&response, &commit, &tree, &parent, &input).is_ok());
    }

    #[test]
    fn persisted_source_wrapper_rejects_wrong_tree_parent_and_metadata() {
        let (input, commit, tree, parent) = fixture();
        let source_input = DeliverySourceCommitInput {
            inner: input.clone(),
        };
        let response = batch_response(&commit, &expected_commit_payload(&tree, &parent, &input));
        assert!(
            verify_batched_source_commit(&response, &commit, &tree, &parent, &source_input,)
                .is_ok()
        );

        let wrong_tree =
            DeliveryTreeOid::try_new(&"d".repeat(40), DeliveryGitObjectFormat::Sha1).unwrap();
        let wrong_parent =
            DeliveryCommitOid::try_new(&"e".repeat(40), DeliveryGitObjectFormat::Sha1).unwrap();
        let wrong_input = DeliverySourceCommitInput::try_new(TASK_ID, 7, 1_700_000_001).unwrap();
        for result in [
            verify_batched_source_commit(&response, &commit, &wrong_tree, &parent, &source_input),
            verify_batched_source_commit(&response, &commit, &tree, &wrong_parent, &source_input),
            verify_batched_source_commit(&response, &commit, &tree, &parent, &wrong_input),
        ] {
            assert_eq!(result, Err(DeliverySourceError::CommandFailed));
        }
    }

    #[test]
    fn batch_verifier_rejects_extra_commit_headers_and_wrong_message() {
        let (input, commit, tree, parent) = fixture();
        let mut extra_header = expected_commit_payload(&tree, &parent, &input);
        let insertion = extra_header
            .windows(2)
            .position(|window| window == b"\n\n")
            .unwrap();
        extra_header.splice(insertion..insertion, b"encoding UTF-8\n".iter().copied());
        let mut wrong_message = expected_commit_payload(&tree, &parent, &input);
        *wrong_message.last_mut().unwrap() = b'!';

        for response in [
            batch_response_with_declared_size(&commit, &extra_header),
            batch_response_with_declared_size(&commit, &wrong_message),
        ] {
            assert_eq!(
                verify_batched_commit(&response, &commit, &tree, &parent, &input),
                Err(DeliverySourceError::CommandFailed)
            );
        }
    }

    #[test]
    fn batch_verifier_rejects_protocol_ambiguity() {
        let (input, commit, tree, parent) = fixture();
        let payload = expected_commit_payload(&tree, &parent, &input);
        let valid = batch_response(&commit, &payload);
        let mut malformed_size = valid.clone();
        let size_start = commit.as_str().len() + b" commit ".len();
        malformed_size[size_start] = b'0';

        let mut missing_trailer = valid.clone();
        missing_trailer.pop();
        let mut extra_trailer = valid;
        extra_trailer.push(b'\n');

        for response in [malformed_size, missing_trailer, extra_trailer] {
            assert_eq!(
                verify_batched_commit(&response, &commit, &tree, &parent, &input),
                Err(DeliverySourceError::CommandFailed)
            );
        }
    }

    #[test]
    fn created_commit_parser_requires_one_lowercase_oid_and_one_lf() {
        let oid = "c".repeat(40);
        assert_eq!(
            parse_created_commit_oid(format!("{oid}\n").as_bytes(), DeliveryGitObjectFormat::Sha1)
                .unwrap()
                .as_str(),
            oid
        );
        for output in [
            format!("{oid}\r\n"),
            format!("{oid}\nextra"),
            format!("{}\n", "C".repeat(40)),
            format!("{}\n", "c".repeat(39)),
        ] {
            assert_eq!(
                parse_created_commit_oid(output.as_bytes(), DeliveryGitObjectFormat::Sha1),
                Err(DeliverySourceError::CommandFailed)
            );
        }
    }

    fn fixture() -> (
        DeliveryCommitInput,
        DeliveryCommitOid,
        DeliveryTreeOid,
        DeliveryCommitOid,
    ) {
        let input = DeliveryCommitInput::try_new(TASK_ID, 7, 1_700_000_000).unwrap();
        let commit =
            DeliveryCommitOid::try_new(&"c".repeat(40), DeliveryGitObjectFormat::Sha1).unwrap();
        let tree =
            DeliveryTreeOid::try_new(&"a".repeat(40), DeliveryGitObjectFormat::Sha1).unwrap();
        let parent =
            DeliveryCommitOid::try_new(&"b".repeat(40), DeliveryGitObjectFormat::Sha1).unwrap();
        (input, commit, tree, parent)
    }

    fn batch_response(commit: &DeliveryCommitOid, payload: &[u8]) -> Vec<u8> {
        batch_response_with_declared_size(commit, payload)
    }

    fn batch_response_with_declared_size(commit: &DeliveryCommitOid, payload: &[u8]) -> Vec<u8> {
        let mut response = format!("{} commit {}\n", commit.as_str(), payload.len()).into_bytes();
        response.extend_from_slice(payload);
        response.push(b'\n');
        response
    }
}
