use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::time::Duration;

use coding_agent_core::WorkspaceFingerprint;

use crate::command_policy::CommandPolicyError;
use crate::root_capability::DurableDirectoryIdentityV1;
use crate::{FingerprintError, ProcessError, WorktreeError, WorktreeIdentity};

use super::DeliveryGitObjectFormat;

const MAX_TARGET_BRANCH_BYTES: usize = 255;
pub(crate) const MAX_MERGE_CONFLICT_PATHS: usize = 128;
pub(crate) const MAX_MERGE_CONFLICT_PATH_BYTES: usize = 4_096;
pub(crate) const MAX_MERGE_CONFLICT_PAYLOAD_BYTES: usize = 65_536;

/// The user-confirmed target observation. These values are comparison inputs
/// only; neither becomes a caller-controlled Git argument or ref selector.
#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryTargetRequest {
    branch_name: String,
    expected_head: String,
}

impl DeliveryTargetRequest {
    pub fn try_new(
        branch_name: impl Into<String>,
        expected_head: impl Into<String>,
    ) -> Result<Self, DeliveryTargetError> {
        let branch_name = branch_name.into();
        let expected_head = expected_head.into();
        if !is_safe_local_branch_name(&branch_name) || !is_canonical_object_id(&expected_head) {
            return Err(DeliveryTargetError::InvalidRequest);
        }
        Ok(Self {
            branch_name,
            expected_head,
        })
    }

    pub fn branch_name(&self) -> &str {
        &self.branch_name
    }

    pub fn expected_head(&self) -> &str {
        &self.expected_head
    }
}

impl fmt::Debug for DeliveryTargetRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryTargetRequest(<validated>)")
    }
}

/// Stable, redacted rejection classes for a registered target checkout.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum DeliveryTargetError {
    InvalidLimits,
    InvalidRequest,
    AuthenticationChanged,
    TargetDetached,
    TargetBranchMismatch,
    TargetHeadChanged,
    TargetWorktreeDirty,
    TargetIgnoredPathCollision,
    TargetGitOperationInProgress,
    UnsafeGitConfiguration,
    UnsupportedGitAttributes,
    Cancelled,
    TimedOut,
    BoundsExceeded,
    CommandFailed,
    ChildOutcomeUnknown,
    ProcessCleanupUnproven,
    Internal,
}

impl DeliveryTargetError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::TargetDetached => "TARGET_BRANCH_DETACHED",
            Self::TargetBranchMismatch => "TARGET_BRANCH_MISMATCH",
            Self::TargetHeadChanged => "TARGET_HEAD_CHANGED",
            Self::TargetWorktreeDirty => "TARGET_WORKTREE_DIRTY",
            Self::TargetIgnoredPathCollision => "TARGET_IGNORED_PATH_COLLISION",
            Self::TargetGitOperationInProgress => "TARGET_GIT_OPERATION_IN_PROGRESS",
            Self::UnsafeGitConfiguration => "UNSAFE_GIT_CONFIGURATION",
            Self::UnsupportedGitAttributes => "UNSUPPORTED_GIT_ATTRIBUTES",
            Self::Cancelled => "COMMAND_CANCELLED",
            Self::TimedOut => "COMMAND_TIMED_OUT",
            Self::BoundsExceeded => "DELIVERY_SOURCE_BOUNDS_EXCEEDED",
            Self::ChildOutcomeUnknown => "DELIVERY_RECONCILIATION_REQUIRED",
            Self::ProcessCleanupUnproven => "PROCESS_TREE_CLEANUP_FAILED",
            Self::AuthenticationChanged => "WORKTREE_IDENTITY_MISMATCH",
            Self::InvalidLimits | Self::InvalidRequest | Self::CommandFailed | Self::Internal => {
                "DELIVERY_TARGET_INVALID"
            }
        }
    }
}

impl fmt::Debug for DeliveryTargetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryTargetError(<redacted>)")
    }
}

impl fmt::Display for DeliveryTargetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("delivery target authentication failed")
    }
}

impl Error for DeliveryTargetError {}

/// A bounded, canonical conflict path suitable for a later Store record or
/// client DTO. Raw byte paths never enter Debug or error formatting.
#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryConflictPath {
    encoding: DeliveryConflictPathEncoding,
    value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DeliveryConflictPathEncoding {
    Utf8,
    Base64Url,
}

impl DeliveryConflictPath {
    pub(crate) fn try_from_raw(raw: Vec<u8>) -> Result<Self, DeliveryPreflightError> {
        validate_conflict_path(&raw)?;
        let (encoding, value) = match String::from_utf8(raw.clone()) {
            Ok(value) => (DeliveryConflictPathEncoding::Utf8, value),
            Err(_) => (
                DeliveryConflictPathEncoding::Base64Url,
                encode_base64url_without_padding(&raw),
            ),
        };
        if value.is_empty() || value.len() > MAX_MERGE_CONFLICT_PATH_BYTES {
            return Err(DeliveryPreflightError::MalformedMergeTreeOutput);
        }
        Ok(Self { encoding, value })
    }

    pub const fn encoding(&self) -> DeliveryConflictPathEncoding {
        self.encoding
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

impl fmt::Debug for DeliveryConflictPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryConflictPath(<validated>)")
    }
}

/// Result of a target-side merge-tree preflight. The contained IDs are
/// format-validated object identities; neither source nor target checkout is
/// mutated by holding this value.
#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryPreflightResult {
    outcome: DeliveryPreflightOutcome,
}

#[derive(Clone, PartialEq, Eq)]
enum DeliveryPreflightOutcome {
    Ready {
        source_commit: DeliveryCommitOid,
        merge_base: DeliveryCommitOid,
        candidate_merge_tree: DeliveryTreeOid,
    },
    Conflict {
        source_commit: DeliveryCommitOid,
        merge_base: DeliveryCommitOid,
        candidate_merge_tree: DeliveryTreeOid,
        paths: Vec<DeliveryConflictPath>,
    },
}

impl DeliveryPreflightResult {
    pub(crate) fn ready(
        source_commit: DeliveryCommitOid,
        merge_base: DeliveryCommitOid,
        candidate_merge_tree: DeliveryTreeOid,
    ) -> Self {
        Self {
            outcome: DeliveryPreflightOutcome::Ready {
                source_commit,
                merge_base,
                candidate_merge_tree,
            },
        }
    }

    pub(crate) fn conflict(
        source_commit: DeliveryCommitOid,
        merge_base: DeliveryCommitOid,
        candidate_merge_tree: DeliveryTreeOid,
        paths: Vec<DeliveryConflictPath>,
    ) -> Result<Self, DeliveryPreflightError> {
        validate_conflict_path_set(&paths)?;
        Ok(Self {
            outcome: DeliveryPreflightOutcome::Conflict {
                source_commit,
                merge_base,
                candidate_merge_tree,
                paths,
            },
        })
    }

    pub fn is_ready(&self) -> bool {
        matches!(self.outcome, DeliveryPreflightOutcome::Ready { .. })
    }

    pub fn is_conflict(&self) -> bool {
        matches!(self.outcome, DeliveryPreflightOutcome::Conflict { .. })
    }

    pub(crate) fn candidate_merge_tree(&self) -> &DeliveryTreeOid {
        match &self.outcome {
            DeliveryPreflightOutcome::Ready {
                candidate_merge_tree,
                ..
            }
            | DeliveryPreflightOutcome::Conflict {
                candidate_merge_tree,
                ..
            } => candidate_merge_tree,
        }
    }

    pub fn source_commit_id(&self) -> &str {
        match &self.outcome {
            DeliveryPreflightOutcome::Ready { source_commit, .. }
            | DeliveryPreflightOutcome::Conflict { source_commit, .. } => source_commit.as_str(),
        }
    }

    pub fn merge_base_id(&self) -> &str {
        match &self.outcome {
            DeliveryPreflightOutcome::Ready { merge_base, .. }
            | DeliveryPreflightOutcome::Conflict { merge_base, .. } => merge_base.as_str(),
        }
    }

    pub(crate) fn merge_base(&self) -> &DeliveryCommitOid {
        match &self.outcome {
            DeliveryPreflightOutcome::Ready { merge_base, .. }
            | DeliveryPreflightOutcome::Conflict { merge_base, .. } => merge_base,
        }
    }

    pub fn candidate_merge_tree_id(&self) -> &str {
        match &self.outcome {
            DeliveryPreflightOutcome::Ready {
                candidate_merge_tree,
                ..
            }
            | DeliveryPreflightOutcome::Conflict {
                candidate_merge_tree,
                ..
            } => candidate_merge_tree.as_str(),
        }
    }

    pub fn conflict_paths(&self) -> Option<&[DeliveryConflictPath]> {
        match &self.outcome {
            DeliveryPreflightOutcome::Ready { .. } => None,
            DeliveryPreflightOutcome::Conflict { paths, .. } => Some(paths),
        }
    }
}

impl fmt::Debug for DeliveryPreflightResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.outcome {
            DeliveryPreflightOutcome::Ready { .. } => {
                formatter.write_str("DeliveryPreflightResult::Ready(<validated>)")
            }
            DeliveryPreflightOutcome::Conflict { paths, .. } => formatter
                .debug_struct("DeliveryPreflightResult::Conflict")
                .field("path_count", &paths.len())
                .field("paths", &"<redacted>")
                .finish(),
        }
    }
}

/// Preflight is observation-only. Errors distinguish known target/source
/// rejections from outcomes that must be reconciled, while all formatting
/// remains free of command output, filesystem paths, and argv values.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum DeliveryPreflightError {
    Target(DeliveryTargetError),
    Source(DeliverySourceError),
    SourceAlreadyInTarget,
    MalformedMergeTreeOutput,
    Internal,
}

impl DeliveryPreflightError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Target(error) => error.code(),
            Self::Source(
                DeliverySourceError::AuthenticationChanged | DeliverySourceError::UnsafeIndex,
            ) => "WORKTREE_IDENTITY_MISMATCH",
            Self::Source(error) => error.code(),
            Self::SourceAlreadyInTarget => "SOURCE_ALREADY_IN_TARGET",
            Self::MalformedMergeTreeOutput | Self::Internal => "DELIVERY_RECONCILIATION_REQUIRED",
        }
    }
}

impl From<DeliveryTargetError> for DeliveryPreflightError {
    fn from(error: DeliveryTargetError) -> Self {
        Self::Target(error)
    }
}

impl From<DeliverySourceError> for DeliveryPreflightError {
    fn from(error: DeliverySourceError) -> Self {
        Self::Source(error)
    }
}

impl fmt::Debug for DeliveryPreflightError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryPreflightError(<redacted>)")
    }
}

impl fmt::Display for DeliveryPreflightError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("delivery preflight failed")
    }
}

impl Error for DeliveryPreflightError {}

impl From<CommandPolicyError> for DeliveryTargetError {
    fn from(error: CommandPolicyError) -> Self {
        match error {
            CommandPolicyError::IdentityChanged | CommandPolicyError::InvalidGitBinding => {
                Self::AuthenticationChanged
            }
            _ => Self::Internal,
        }
    }
}

impl From<ProcessError> for DeliveryTargetError {
    fn from(error: ProcessError) -> Self {
        match error {
            ProcessError::CommandPolicy(error) => error.into(),
            ProcessError::TimeoutOutsideLimit => Self::InvalidLimits,
            ProcessError::TreeControlLost(_)
            | ProcessError::TreeCleanupFailed(_)
            | ProcessError::CleanupTimedOut
            | ProcessError::LivenessCleanupUnproven
            | ProcessError::LivenessCleanupFailed(_)
            | ProcessError::WorkerFailed => Self::ProcessCleanupUnproven,
            ProcessError::MissingOutputPipe
            | ProcessError::MissingInputPipe
            | ProcessError::InputClosedEarly
            | ProcessError::InputWriteFailed(_)
            | ProcessError::InputCloseFailed(_)
            | ProcessError::InputCompletionUnknown
            | ProcessError::WaitFailed(_)
            | ProcessError::OutputDrainFailed(_) => Self::ChildOutcomeUnknown,
            ProcessError::SpawnFailed(_)
            | ProcessError::TreeSetupFailed(_)
            | ProcessError::LivenessSetupFailed(_)
            | ProcessError::InvalidCommand => Self::CommandFailed,
        }
    }
}

fn is_canonical_object_id(value: &str) -> bool {
    matches!(value.len(), 40 | 64)
        && value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        && value.as_bytes().iter().any(|byte| *byte != b'0')
}

fn is_safe_local_branch_name(value: &str) -> bool {
    if value.is_empty()
        || value.len() > MAX_TARGET_BRANCH_BYTES
        || value.starts_with('/')
        || value.ends_with('/')
        || value.ends_with('.')
        || value.contains("..")
        || value.contains("//")
        || value.contains("@{")
        || value.bytes().any(|byte| {
            byte.is_ascii_control()
                || byte.is_ascii_whitespace()
                || matches!(byte, b'~' | b'^' | b':' | b'?' | b'*' | b'[' | b'\\')
        })
    {
        return false;
    }
    value
        .split('/')
        .all(|component| !component.is_empty() && !component.ends_with(".lock"))
}

fn validate_conflict_path(raw: &[u8]) -> Result<(), DeliveryPreflightError> {
    if raw.is_empty()
        || raw.len() > MAX_MERGE_CONFLICT_PATH_BYTES
        || raw.contains(&0)
        || matches!(raw.first(), Some(b'/' | b'\\'))
    {
        return Err(DeliveryPreflightError::MalformedMergeTreeOutput);
    }
    let components = raw.split(|byte| *byte == b'/').collect::<Vec<_>>();
    if components.iter().any(|component| {
        component.is_empty()
            || *component == b"."
            || *component == b".."
            || component.eq_ignore_ascii_case(b".git")
            || component.ends_with(b".")
            || component.ends_with(b" ")
            || component.contains(&b':')
    }) {
        return Err(DeliveryPreflightError::MalformedMergeTreeOutput);
    }
    Ok(())
}

/// Small local URL-safe base64 encoder for untrusted Git path bytes. Keeping
/// it here avoids widening the runtime dependency surface merely to turn a
/// non-UTF-8 conflict path into a display-safe opaque value.
fn encode_base64url_without_padding(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        encoded.push(char::from(ALPHABET[usize::from(first >> 2)]));
        if chunk.len() == 1 {
            encoded.push(char::from(ALPHABET[usize::from((first & 0x03) << 4)]));
            continue;
        }
        let second = chunk[1];
        encoded.push(char::from(
            ALPHABET[usize::from(((first & 0x03) << 4) | (second >> 4))],
        ));
        if chunk.len() == 2 {
            encoded.push(char::from(ALPHABET[usize::from((second & 0x0f) << 2)]));
            continue;
        }
        let third = chunk[2];
        encoded.push(char::from(
            ALPHABET[usize::from(((second & 0x0f) << 2) | (third >> 6))],
        ));
        encoded.push(char::from(ALPHABET[usize::from(third & 0x3f)]));
    }
    encoded
}

fn validate_conflict_path_set(
    paths: &[DeliveryConflictPath],
) -> Result<(), DeliveryPreflightError> {
    if paths.len() > MAX_MERGE_CONFLICT_PATHS {
        return Err(DeliveryPreflightError::MalformedMergeTreeOutput);
    }
    let mut unique = BTreeSet::new();
    let mut payload = 0usize;
    for path in paths {
        if path.value.is_empty()
            || path.value.len() > MAX_MERGE_CONFLICT_PATH_BYTES
            || !unique.insert((path.encoding, path.value.as_str()))
        {
            return Err(DeliveryPreflightError::MalformedMergeTreeOutput);
        }
        payload = payload
            .checked_add(path.value.len())
            .ok_or(DeliveryPreflightError::MalformedMergeTreeOutput)?;
        if payload > MAX_MERGE_CONFLICT_PAYLOAD_BYTES {
            return Err(DeliveryPreflightError::MalformedMergeTreeOutput);
        }
    }
    Ok(())
}

#[cfg(test)]
mod target_type_tests {
    use super::*;

    #[test]
    fn non_utf8_conflict_path_is_preserved_only_as_base64url() {
        let path = DeliveryConflictPath::try_from_raw(b"dir/\xffname".to_vec()).unwrap();
        assert_eq!(path.encoding(), DeliveryConflictPathEncoding::Base64Url);
        assert_eq!(path.value(), "ZGlyL_9uYW1l");
        assert_eq!(format!("{path:?}"), "DeliveryConflictPath(<validated>)");
    }

    #[test]
    fn base64url_encoder_uses_no_padding_for_all_remainder_lengths() {
        assert_eq!(encode_base64url_without_padding(b"a"), "YQ");
        assert_eq!(encode_base64url_without_padding(b"ab"), "YWI");
        assert_eq!(encode_base64url_without_padding(b"abc"), "YWJj");
    }
}

/// An object identity whose syntax has been checked against the format
/// negotiated by the retained delivery Git probe.
///
/// This stays internal because callers must not manufacture command inputs
/// from arbitrary strings. Public result values expose their already-verified
/// object IDs as read-only text instead.
#[derive(Clone, PartialEq, Eq)]
struct DeliveryGitObjectId(String);

impl DeliveryGitObjectId {
    fn try_new(value: &str, format: DeliveryGitObjectFormat) -> Option<Self> {
        let expected_length = format.hexadecimal_length();
        if value.len() != expected_length
            || !value
                .as_bytes()
                .iter()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
            || value.bytes().all(|byte| byte == b'0')
        {
            return None;
        }
        Some(Self(value.to_owned()))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

/// A validated Git tree object owned by a candidate-tree or object-verifier
/// workflow. It is intentionally not constructible outside the delivery
/// runtime.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct DeliveryTreeOid(DeliveryGitObjectId);

impl DeliveryTreeOid {
    pub(crate) fn try_new(value: &str, format: DeliveryGitObjectFormat) -> Option<Self> {
        DeliveryGitObjectId::try_new(value, format).map(Self)
    }

    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for DeliveryTreeOid {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryTreeOid(<validated>)")
    }
}

/// A validated Git commit object owned by the delivery runtime.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct DeliveryCommitOid(DeliveryGitObjectId);

impl DeliveryCommitOid {
    pub(crate) fn try_new(value: &str, format: DeliveryGitObjectFormat) -> Option<Self> {
        DeliveryGitObjectId::try_new(value, format).map(Self)
    }

    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for DeliveryCommitOid {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryCommitOid(<validated>)")
    }
}

/// An unreferenced candidate tree produced from an authenticated reviewed
/// source. It carries no filesystem, command, or temporary-index authority.
#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryCandidateTree {
    tree: DeliveryTreeOid,
    provenance: CandidateTreeProvenance,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct CandidateTreeProvenance {
    identity: WorktreeIdentity,
    base_commit: DeliveryCommitOid,
    branch_name: String,
    approved_fingerprint: WorkspaceFingerprint,
    config_attributes_digest: [u8; 32],
    common_identity: DurableDirectoryIdentityV1,
    admin_identity: DurableDirectoryIdentityV1,
}

impl CandidateTreeProvenance {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        identity: WorktreeIdentity,
        base_commit: DeliveryCommitOid,
        branch_name: String,
        approved_fingerprint: WorkspaceFingerprint,
        config_attributes_digest: [u8; 32],
        common_identity: DurableDirectoryIdentityV1,
        admin_identity: DurableDirectoryIdentityV1,
    ) -> Self {
        Self {
            identity,
            base_commit,
            branch_name,
            approved_fingerprint,
            config_attributes_digest,
            common_identity,
            admin_identity,
        }
    }

    pub(crate) const fn base_commit(&self) -> &DeliveryCommitOid {
        &self.base_commit
    }

    /// The approved fingerprint remains delivery-internal and is used only to
    /// prove that the no-follow candidate snapshot is the reviewed source
    /// before any temporary-index mutation begins.
    pub(crate) const fn approved_fingerprint(&self) -> WorkspaceFingerprint {
        self.approved_fingerprint
    }

    pub(crate) fn branch_name(&self) -> &str {
        &self.branch_name
    }

    pub(crate) const fn common_identity(&self) -> &DurableDirectoryIdentityV1 {
        &self.common_identity
    }

    pub(crate) const fn admin_identity(&self) -> &DurableDirectoryIdentityV1 {
        &self.admin_identity
    }

    pub(crate) const fn config_attributes_digest(&self) -> &[u8; 32] {
        &self.config_attributes_digest
    }
}

impl DeliveryCandidateTree {
    pub(crate) fn from_tree(tree: DeliveryTreeOid, provenance: CandidateTreeProvenance) -> Self {
        Self { tree, provenance }
    }

    /// The format-validated candidate tree object ID. This is the durable
    /// value the Store later binds into `ObjectPending`.
    pub fn object_id(&self) -> &str {
        self.tree.as_str()
    }

    pub(crate) const fn tree(&self) -> &DeliveryTreeOid {
        &self.tree
    }

    pub(crate) const fn provenance(&self) -> &CandidateTreeProvenance {
        &self.provenance
    }

    /// Ensures a durable candidate cannot be replayed through another
    /// task/attempt capability, even when both capabilities share one Git
    /// object database.
    pub(crate) fn is_bound_to(&self, provenance: &CandidateTreeProvenance) -> bool {
        self.provenance == *provenance
    }
}

impl fmt::Debug for DeliveryCandidateTree {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryCandidateTree(<validated>)")
    }
}

/// Opaque source objects prepared only after the durable preflight intent
/// exists.
///
/// The runtime owns construction so a caller cannot attach scalar object IDs
/// to another source capability.  The two public projections are deliberately
/// read-only persistence inputs; the capability-bound candidate and typed
/// object identities remain private to the delivery runtime.
pub struct PreparedDeliveryPreflightSource {
    candidate: DeliveryCandidateTree,
    source_commit: DeliveryCommitOid,
}

impl PreparedDeliveryPreflightSource {
    pub(crate) fn from_verified(
        candidate: DeliveryCandidateTree,
        source_commit: DeliveryCommitOid,
    ) -> Self {
        Self {
            candidate,
            source_commit,
        }
    }

    /// The format-validated candidate tree bound by the durable preflight
    /// intent before target-side merge observation begins.
    pub fn candidate_tree_id(&self) -> &str {
        self.candidate.object_id()
    }

    /// The deterministic, preflight-only source commit bound by the durable
    /// preflight intent before target-side merge observation begins.
    pub fn source_commit_id(&self) -> &str {
        self.source_commit.as_str()
    }

    pub(crate) const fn candidate(&self) -> &DeliveryCandidateTree {
        &self.candidate
    }

    pub(crate) const fn source_commit(&self) -> &DeliveryCommitOid {
        &self.source_commit
    }

    pub(crate) fn is_bound_to(&self, provenance: &CandidateTreeProvenance) -> bool {
        self.candidate.is_bound_to(provenance)
    }
}

impl fmt::Debug for PreparedDeliveryPreflightSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PreparedDeliveryPreflightSource(<opaque>)")
    }
}

/// An exact, unreferenced source commit whose raw object shape has been
/// verified against fixed, persisted source metadata. It holds no ref,
/// worktree, command, or temporary authority.
#[derive(Clone, PartialEq, Eq)]
pub struct DeliverySourceCommit {
    commit: DeliveryCommitOid,
    provenance: CandidateTreeProvenance,
}

impl DeliverySourceCommit {
    pub(crate) fn from_commit(
        commit: DeliveryCommitOid,
        provenance: CandidateTreeProvenance,
    ) -> Self {
        Self { commit, provenance }
    }

    /// The format-validated object ID of the verified unreferenced commit.
    pub fn object_id(&self) -> &str {
        self.commit.as_str()
    }

    pub(crate) const fn commit(&self) -> &DeliveryCommitOid {
        &self.commit
    }

    /// Prevents an exact object ID built for one candidate/evidence tuple
    /// from being attached to another source capability before Git is asked
    /// to perform a side effect.
    pub(crate) fn is_bound_to(&self, provenance: &CandidateTreeProvenance) -> bool {
        self.provenance == *provenance
    }
}

impl fmt::Debug for DeliverySourceCommit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliverySourceCommit(<validated>)")
    }
}

/// Durable source stage whose on-disk Git state must be classified before a
/// delivery worker retries a side effect.
///
/// This mirrors only the two pending states that have Git-side recovery
/// semantics.  The Store remains the owner of durable transitions; the
/// runtime uses this value solely to select a proof-oriented observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliverySourcePendingState {
    ObjectPending,
    CommitPending,
}

/// Proven outcome of observing one pending source intent.
///
/// `ReconciliationRequired` is deliberately a value rather than a guessed
/// success.  Callers must retain the authenticated repository and let their
/// durable orchestration layer poison/reconcile it instead of resetting or
/// cleaning the worktree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliverySourceRecoveryDisposition {
    /// The real source is still at its approved pre-object state, so the
    /// deterministic object command may be replayed.
    ReplayObject,
    /// A `CommitPending` real-index/ref side effect has not begun.
    Continue,
    /// The real index exactly matches the candidate and the CAS is still
    /// pending.
    StageComplete,
    /// The source ref, index and exact expected object prove application.
    Applied,
    /// No allowed durable state exactly matches the observed Git state.
    ReconciliationRequired,
}

/// Resource limits for one read-only delivery-source authentication pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliverySourceLimits {
    timeout: Duration,
    max_status_bytes: usize,
    max_config_bytes: usize,
    max_attributes_bytes: usize,
    max_paths: usize,
}

impl DeliverySourceLimits {
    pub fn try_new(
        timeout: Duration,
        max_status_bytes: usize,
        max_config_bytes: usize,
        max_attributes_bytes: usize,
        max_paths: usize,
    ) -> Result<Self, DeliverySourceError> {
        if timeout.is_zero()
            || max_status_bytes == 0
            || max_config_bytes == 0
            || max_attributes_bytes == 0
            || max_paths == 0
        {
            return Err(DeliverySourceError::InvalidLimits);
        }
        Ok(Self {
            timeout,
            max_status_bytes,
            max_config_bytes,
            max_attributes_bytes,
            max_paths,
        })
    }

    pub const fn timeout(self) -> Duration {
        self.timeout
    }

    pub const fn max_status_bytes(self) -> usize {
        self.max_status_bytes
    }

    pub const fn max_config_bytes(self) -> usize {
        self.max_config_bytes
    }

    pub const fn max_attributes_bytes(self) -> usize {
        self.max_attributes_bytes
    }

    pub const fn max_paths(self) -> usize {
        self.max_paths
    }
}

/// Stable, redacted failure classes for delivery-source authentication.
///
/// Variants deliberately carry no filesystem paths, Git values or child
/// output. Lower layers are collapsed into one of these fixed classes at the
/// boundary so both `Display` and `Debug` remain safe for durable diagnostics.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum DeliverySourceError {
    InvalidLimits,
    InvalidEnvironment,
    CommandPolicy,
    SourceChanged,
    Cancelled,
    TimedOut,
    BoundsExceeded,
    UnsafeGitConfiguration,
    AuthenticationChanged,
    UnsafeIndex,
    CommandFailed,
    ChildOutcomeUnknown,
    ProcessCleanupUnproven,
    SandboxUnavailable,
    SandboxCleanupUnproven,
    Internal,
}

impl DeliverySourceError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::SourceChanged | Self::AuthenticationChanged | Self::UnsafeIndex => {
                "DELIVERY_SOURCE_CHANGED"
            }
            Self::Cancelled => "COMMAND_CANCELLED",
            Self::TimedOut => "COMMAND_TIMED_OUT",
            Self::BoundsExceeded => "DELIVERY_SOURCE_BOUNDS_EXCEEDED",
            Self::UnsafeGitConfiguration => "UNSAFE_GIT_CONFIGURATION",
            Self::CommandFailed => "DELIVERY_SOURCE_COMMAND_FAILED",
            Self::ChildOutcomeUnknown => "DELIVERY_RECONCILIATION_REQUIRED",
            Self::ProcessCleanupUnproven | Self::SandboxCleanupUnproven => {
                "PROCESS_TREE_CLEANUP_FAILED"
            }
            Self::InvalidLimits
            | Self::InvalidEnvironment
            | Self::CommandPolicy
            | Self::SandboxUnavailable
            | Self::Internal => "DELIVERY_SOURCE_INVALID",
        }
    }
}

impl fmt::Debug for DeliverySourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliverySourceError(<redacted>)")
    }
}

impl fmt::Display for DeliverySourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("delivery source authentication failed")
    }
}

impl Error for DeliverySourceError {}

impl From<CommandPolicyError> for DeliverySourceError {
    fn from(error: CommandPolicyError) -> Self {
        match error {
            CommandPolicyError::IdentityChanged | CommandPolicyError::InvalidGitBinding => {
                Self::AuthenticationChanged
            }
            _ => Self::CommandPolicy,
        }
    }
}

impl From<ProcessError> for DeliverySourceError {
    fn from(error: ProcessError) -> Self {
        match error {
            ProcessError::CommandPolicy(error) => error.into(),
            ProcessError::InvalidCommand => Self::CommandPolicy,
            ProcessError::TimeoutOutsideLimit => Self::InvalidLimits,
            ProcessError::TreeControlLost(_)
            | ProcessError::TreeCleanupFailed(_)
            | ProcessError::CleanupTimedOut
            | ProcessError::LivenessCleanupUnproven
            | ProcessError::LivenessCleanupFailed(_)
            | ProcessError::WorkerFailed => Self::ProcessCleanupUnproven,
            ProcessError::SpawnFailed(_)
            | ProcessError::TreeSetupFailed(_)
            | ProcessError::LivenessSetupFailed(_) => Self::CommandFailed,
            ProcessError::MissingOutputPipe
            | ProcessError::MissingInputPipe
            | ProcessError::InputClosedEarly
            | ProcessError::InputWriteFailed(_)
            | ProcessError::InputCloseFailed(_)
            | ProcessError::InputCompletionUnknown
            | ProcessError::WaitFailed(_)
            | ProcessError::OutputDrainFailed(_) => Self::ChildOutcomeUnknown,
        }
    }
}

impl From<WorktreeError> for DeliverySourceError {
    fn from(error: WorktreeError) -> Self {
        match error {
            WorktreeError::CommandPolicy(error) => error.into(),
            WorktreeError::Process(error) => error.into(),
            other => match other.code() {
                "COMMAND_CANCELLED" => Self::Cancelled,
                "COMMAND_TIMED_OUT" => Self::TimedOut,
                "UNSAFE_GIT_CONFIGURATION" => Self::UnsafeGitConfiguration,
                "REPOSITORY_IDENTITY_MISMATCH" | "WORKTREE_STATE_INCONSISTENT" => {
                    Self::AuthenticationChanged
                }
                "PROCESS_TREE_CLEANUP_FAILED" => Self::ProcessCleanupUnproven,
                _ => Self::Internal,
            },
        }
    }
}

impl From<FingerprintError> for DeliverySourceError {
    fn from(error: FingerprintError) -> Self {
        match error {
            FingerprintError::InvalidLimits => Self::InvalidLimits,
            FingerprintError::InvalidEnvironment => Self::InvalidEnvironment,
            FingerprintError::CommandPolicy(error) => error.into(),
            FingerprintError::Process(error) => error.into(),
            FingerprintError::Cancelled => Self::Cancelled,
            FingerprintError::TimedOut => Self::TimedOut,
            FingerprintError::GitCommandFailed => Self::CommandFailed,
            FingerprintError::OutputIncomplete => Self::BoundsExceeded,
            FingerprintError::ListingInvalid => Self::UnsafeIndex,
            FingerprintError::TooManyFiles
            | FingerprintError::FileTooLarge
            | FingerprintError::TotalTooLarge => Self::BoundsExceeded,
            FingerprintError::WorkspaceChanged => Self::SourceChanged,
            FingerprintError::UnsupportedEntry
            | FingerprintError::PathInvalid
            | FingerprintError::UnsafeEntry(_) => Self::UnsafeIndex,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_reject_every_zero_dimension() {
        let valid = (Duration::from_secs(1), 1, 1, 1, 1);
        assert!(DeliverySourceLimits::try_new(valid.0, valid.1, valid.2, valid.3, valid.4).is_ok());
        assert!(DeliverySourceLimits::try_new(Duration::ZERO, 1, 1, 1, 1).is_err());
        assert!(DeliverySourceLimits::try_new(valid.0, 0, 1, 1, 1).is_err());
        assert!(DeliverySourceLimits::try_new(valid.0, 1, 0, 1, 1).is_err());
        assert!(DeliverySourceLimits::try_new(valid.0, 1, 1, 0, 1).is_err());
        assert!(DeliverySourceLimits::try_new(valid.0, 1, 1, 1, 0).is_err());
    }

    #[test]
    fn errors_are_stable_and_redacted() {
        let error = DeliverySourceError::UnsafeGitConfiguration;
        assert_eq!(error.code(), "UNSAFE_GIT_CONFIGURATION");
        assert_eq!(format!("{error:?}"), "DeliverySourceError(<redacted>)");
        assert_eq!(error.to_string(), "delivery source authentication failed");
    }

    #[test]
    fn preflight_distinguishes_source_drift_from_source_identity_failures() {
        assert_eq!(
            DeliveryPreflightError::Source(DeliverySourceError::SourceChanged).code(),
            "DELIVERY_SOURCE_CHANGED"
        );
        for error in [
            DeliverySourceError::AuthenticationChanged,
            DeliverySourceError::UnsafeIndex,
        ] {
            assert_eq!(
                DeliveryPreflightError::Source(error).code(),
                "WORKTREE_IDENTITY_MISMATCH"
            );
        }
    }

    #[test]
    fn nested_identity_failures_keep_the_source_changed_class() {
        let process = ProcessError::CommandPolicy(CommandPolicyError::IdentityChanged);
        assert_eq!(
            DeliverySourceError::from(process),
            DeliverySourceError::AuthenticationChanged
        );
        let fingerprint = FingerprintError::CommandPolicy(CommandPolicyError::InvalidGitBinding);
        assert_eq!(
            DeliverySourceError::from(fingerprint).code(),
            "DELIVERY_SOURCE_CHANGED"
        );
    }

    #[test]
    fn fingerprint_failures_map_to_stable_delivery_classes() {
        for (error, expected) in [
            (
                FingerprintError::GitCommandFailed,
                DeliverySourceError::CommandFailed,
            ),
            (
                FingerprintError::OutputIncomplete,
                DeliverySourceError::BoundsExceeded,
            ),
            (
                FingerprintError::ListingInvalid,
                DeliverySourceError::UnsafeIndex,
            ),
            (
                FingerprintError::UnsupportedEntry,
                DeliverySourceError::UnsafeIndex,
            ),
            (
                FingerprintError::UnsafeEntry(std::io::Error::other("secret worktree path")),
                DeliverySourceError::UnsafeIndex,
            ),
            (
                FingerprintError::TooManyFiles,
                DeliverySourceError::BoundsExceeded,
            ),
            (FingerprintError::Cancelled, DeliverySourceError::Cancelled),
            (FingerprintError::TimedOut, DeliverySourceError::TimedOut),
        ] {
            let mapped = DeliverySourceError::from(error);
            assert_eq!(mapped, expected);
            assert_eq!(format!("{mapped:?}"), "DeliverySourceError(<redacted>)");
            assert_eq!(mapped.to_string(), "delivery source authentication failed");
        }
    }

    #[test]
    fn process_failures_map_to_stable_delivery_classes() {
        for (error, expected) in [
            (
                ProcessError::InvalidCommand,
                DeliverySourceError::CommandPolicy,
            ),
            (
                ProcessError::TimeoutOutsideLimit,
                DeliverySourceError::InvalidLimits,
            ),
            (
                ProcessError::MissingOutputPipe,
                DeliverySourceError::ChildOutcomeUnknown,
            ),
            (
                ProcessError::SpawnFailed(std::io::Error::other("secret executable path")),
                DeliverySourceError::CommandFailed,
            ),
            (
                ProcessError::CleanupTimedOut,
                DeliverySourceError::ProcessCleanupUnproven,
            ),
        ] {
            let mapped = DeliverySourceError::from(error);
            assert_eq!(mapped, expected);
            assert_eq!(format!("{mapped:?}"), "DeliverySourceError(<redacted>)");
            assert_eq!(mapped.to_string(), "delivery source authentication failed");
        }
    }

    #[test]
    fn incomplete_input_or_output_channels_require_reconciliation() {
        for error in [
            ProcessError::MissingOutputPipe,
            ProcessError::MissingInputPipe,
            ProcessError::InputClosedEarly,
            ProcessError::InputWriteFailed(std::io::Error::other("input write failed")),
            ProcessError::InputCloseFailed(std::io::Error::other("input close failed")),
            ProcessError::InputCompletionUnknown,
            ProcessError::WaitFailed(std::io::Error::other("wait outcome unknown")),
            ProcessError::OutputDrainFailed(std::io::Error::other("output drain failed")),
        ] {
            let mapped = DeliverySourceError::from(error);
            assert_eq!(mapped, DeliverySourceError::ChildOutcomeUnknown);
            assert_eq!(mapped.code(), "DELIVERY_RECONCILIATION_REQUIRED");
            assert_eq!(format!("{mapped:?}"), "DeliverySourceError(<redacted>)");
        }

        assert_eq!(
            DeliverySourceError::from(ProcessError::CleanupTimedOut),
            DeliverySourceError::ProcessCleanupUnproven
        );
    }
}
