//! Narrow, redacted projection from authenticated runtime capabilities to
//! durable delivery facts.
//!
//! This module deliberately does not expose filesystem paths, retained
//! directory handles, Git command builders, or constructors from scalar
//! values.  The application can persist the projected facts, but only the
//! corresponding authenticated runtime capability or closed postcondition
//! proof can mint each projection.

use std::fmt;

use coding_agent_core::WorkspaceFingerprint;

use crate::WorktreeIdentity;

use super::{
    DeliveryCandidateTree, DeliveryCommitOid, DeliveryConflictPath, DeliveryGitObjectFormat,
    DeliveryMergeInput, DeliverySourceCapability, DeliverySourceCommit, DeliverySourceCommitInput,
    DeliverySourceError, DeliverySourcePendingState, DeliveryTargetCapability, DeliveryTreeOid,
};

const LOCAL_BRANCH_PREFIX: &str = "refs/heads/";
const DIRECTORY_IDENTITY_ALGORITHM: &str = "directory_identity_v1";
const FIXED_LOCK_REASON: &str = "codex-reserved";
const FIXED_COMMIT_IDENTITY_NAME: &str = "Coding Agent";
const FIXED_COMMIT_IDENTITY_EMAIL: &str = "coding-agent@localhost";

/// Stable rejection for malformed durable scalar input.  The value contains
/// no offending field so it is safe to route through durable diagnostics.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct DeliveryPersistenceInputError;

impl DeliveryPersistenceInputError {
    pub const fn code(self) -> &'static str {
        "DELIVERY_PERSISTED_INPUT_INVALID"
    }
}

impl fmt::Debug for DeliveryPersistenceInputError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryPersistenceInputError(<redacted>)")
    }
}

impl fmt::Display for DeliveryPersistenceInputError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("persisted delivery input is invalid")
    }
}

impl std::error::Error for DeliveryPersistenceInputError {}

/// Durable source state represented by a Store snapshot.  `Committed` is
/// distinct from `CommitPending`: its binder must prove the already-applied
/// postcondition and may never authorize the pending mutation path on drift.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryPersistedSourceState {
    ObjectPending,
    CommitPending,
    Committed,
}

/// Syntax-validated scalar Store view for source recovery.
///
/// Holding this value grants no filesystem, command, ref, or object mutation
/// authority.  In particular, there is deliberately no conversion from this
/// view to a recovery capability:
///
/// ```compile_fail
/// use coding_agent_runtime::{
///     DeliveryPersistedSourceRecovery, DeliverySourceRecoveryCapability,
/// };
/// fn cannot_promote(raw: DeliveryPersistedSourceRecovery) {
///     let _: DeliverySourceRecoveryCapability = raw.into();
/// }
/// ```
#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryPersistedSourceRecovery {
    object_format: DeliveryGitObjectFormat,
    state: DeliveryPersistedSourceState,
    identity: WorktreeIdentity,
    source_branch: String,
    base_commit: DeliveryCommitOid,
    approved_fingerprint: WorkspaceFingerprint,
    candidate_tree: DeliveryTreeOid,
    expected_source_commit: Option<DeliveryCommitOid>,
    source_input: DeliverySourceCommitInput,
    common_git_identity_digest: [u8; 32],
    worktree_admin_identity_digest: [u8; 32],
    source_config_attributes_digest: [u8; 32],
}

impl DeliveryPersistedSourceRecovery {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        object_format: DeliveryGitObjectFormat,
        state: DeliveryPersistedSourceState,
        identity: WorktreeIdentity,
        source_branch: impl Into<String>,
        base_commit: impl AsRef<str>,
        approved_fingerprint: WorkspaceFingerprint,
        candidate_tree: impl AsRef<str>,
        expected_source_commit: Option<impl AsRef<str>>,
        source_input: DeliverySourceCommitInput,
        common_git_identity_algorithm: &str,
        common_git_identity_digest: &str,
        worktree_admin_identity_algorithm: &str,
        worktree_admin_identity_digest: &str,
        source_config_attributes_digest: &str,
    ) -> Result<Self, DeliveryPersistenceInputError> {
        let source_branch = source_branch.into();
        let expected_source_commit = expected_source_commit
            .map(|value| parse_commit_oid(value.as_ref(), object_format))
            .transpose()?;
        if source_branch != local_branch_ref(&identity.branch_name())
            || !source_input.matches_identity(&identity)
            || !matches!(
                (state, expected_source_commit.as_ref()),
                (DeliveryPersistedSourceState::ObjectPending, None)
                    | (DeliveryPersistedSourceState::CommitPending, Some(_))
                    | (DeliveryPersistedSourceState::Committed, Some(_))
            )
            || common_git_identity_algorithm != DIRECTORY_IDENTITY_ALGORITHM
            || worktree_admin_identity_algorithm != DIRECTORY_IDENTITY_ALGORITHM
        {
            return Err(DeliveryPersistenceInputError);
        }
        Ok(Self {
            object_format,
            state,
            identity,
            source_branch,
            base_commit: parse_commit_oid(base_commit.as_ref(), object_format)?,
            approved_fingerprint,
            candidate_tree: parse_tree_oid(candidate_tree.as_ref(), object_format)?,
            expected_source_commit,
            source_input,
            common_git_identity_digest: parse_lower_hex_digest(common_git_identity_digest)?,
            worktree_admin_identity_digest: parse_lower_hex_digest(worktree_admin_identity_digest)?,
            source_config_attributes_digest: parse_lower_hex_digest(
                source_config_attributes_digest,
            )?,
        })
    }

    pub const fn state(&self) -> DeliveryPersistedSourceState {
        self.state
    }

    pub(super) const fn object_format(&self) -> DeliveryGitObjectFormat {
        self.object_format
    }

    pub(super) const fn identity(&self) -> &WorktreeIdentity {
        &self.identity
    }

    pub(super) fn source_branch(&self) -> &str {
        &self.source_branch
    }

    pub(super) const fn base_commit(&self) -> &DeliveryCommitOid {
        &self.base_commit
    }

    pub(super) const fn approved_fingerprint(&self) -> WorkspaceFingerprint {
        self.approved_fingerprint
    }

    pub(super) const fn candidate_tree(&self) -> &DeliveryTreeOid {
        &self.candidate_tree
    }

    pub(super) const fn expected_source_commit(&self) -> Option<&DeliveryCommitOid> {
        self.expected_source_commit.as_ref()
    }

    pub(super) const fn source_input(&self) -> &DeliverySourceCommitInput {
        &self.source_input
    }

    pub(super) const fn common_git_identity_digest(&self) -> &[u8; 32] {
        &self.common_git_identity_digest
    }

    pub(super) const fn worktree_admin_identity_digest(&self) -> &[u8; 32] {
        &self.worktree_admin_identity_digest
    }

    pub(super) const fn source_config_attributes_digest(&self) -> &[u8; 32] {
        &self.source_config_attributes_digest
    }

    pub(super) const fn pending_state(&self) -> DeliverySourcePendingState {
        match self.state {
            DeliveryPersistedSourceState::ObjectPending => {
                DeliverySourcePendingState::ObjectPending
            }
            DeliveryPersistedSourceState::CommitPending
            | DeliveryPersistedSourceState::Committed => DeliverySourcePendingState::CommitPending,
        }
    }
}

impl fmt::Debug for DeliveryPersistedSourceRecovery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliveryPersistedSourceRecovery")
            .field("state", &self.state)
            .field("scalars", &"<redacted>")
            .finish()
    }
}

/// Syntax-validated scalar Store view for target recovery.  The persisted
/// target config and security digests remain independent baselines; a binder
/// must never replace either with a freshly observed value.
///
/// ```compile_fail
/// use coding_agent_runtime::{
///     DeliveryPersistedTargetRecovery, DeliveryTargetRecoveryCapability,
/// };
/// fn cannot_promote(raw: DeliveryPersistedTargetRecovery) {
///     let _: DeliveryTargetRecoveryCapability = raw.into();
/// }
/// ```
#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryPersistedTargetRecovery {
    object_format: DeliveryGitObjectFormat,
    branch_name: String,
    old_head: DeliveryCommitOid,
    common_git_identity_digest: [u8; 32],
    target_config_attributes_digest: [u8; 32],
    target_security_digest: [u8; 32],
}

impl DeliveryPersistedTargetRecovery {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        object_format: DeliveryGitObjectFormat,
        target_branch: impl AsRef<str>,
        old_head: impl AsRef<str>,
        common_git_identity_algorithm: &str,
        common_git_identity_digest: &str,
        target_config_attributes_digest: &str,
        target_security_digest: &str,
    ) -> Result<Self, DeliveryPersistenceInputError> {
        let old_head = parse_commit_oid(old_head.as_ref(), object_format)?;
        let branch_name = parse_local_branch_ref(target_branch.as_ref())?;
        super::DeliveryTargetRequest::try_new(&branch_name, old_head.as_str())
            .map_err(|_| DeliveryPersistenceInputError)?;
        if common_git_identity_algorithm != DIRECTORY_IDENTITY_ALGORITHM {
            return Err(DeliveryPersistenceInputError);
        }
        Ok(Self {
            object_format,
            branch_name,
            old_head,
            common_git_identity_digest: parse_lower_hex_digest(common_git_identity_digest)?,
            target_config_attributes_digest: parse_lower_hex_digest(
                target_config_attributes_digest,
            )?,
            target_security_digest: parse_lower_hex_digest(target_security_digest)?,
        })
    }

    pub(super) const fn object_format(&self) -> DeliveryGitObjectFormat {
        self.object_format
    }

    pub(super) fn branch_name(&self) -> &str {
        &self.branch_name
    }

    pub(super) const fn old_head(&self) -> &DeliveryCommitOid {
        &self.old_head
    }

    pub(super) const fn common_git_identity_digest(&self) -> &[u8; 32] {
        &self.common_git_identity_digest
    }

    pub(super) const fn target_config_attributes_digest(&self) -> &[u8; 32] {
        &self.target_config_attributes_digest
    }

    pub(super) const fn target_security_digest(&self) -> &[u8; 32] {
        &self.target_security_digest
    }
}

impl fmt::Debug for DeliveryPersistedTargetRecovery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryPersistedTargetRecovery(<redacted>)")
    }
}

/// Syntax-validated expected-merge scalar view.  The object identities do not
/// become a merge capability until a fresh source/target pair re-proves the
/// exact commit shape.
///
/// ```compile_fail
/// use coding_agent_runtime::{
///     DeliveryMergeRecoveryCapability, DeliveryPersistedMergeRecovery,
/// };
/// fn cannot_promote(raw: DeliveryPersistedMergeRecovery) {
///     let _: DeliveryMergeRecoveryCapability = raw.into();
/// }
/// ```
#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryPersistedMergeRecovery {
    object_format: DeliveryGitObjectFormat,
    merge_base: DeliveryCommitOid,
    candidate_merge_tree: DeliveryTreeOid,
    expected_merge_commit: DeliveryCommitOid,
    input: DeliveryMergeInput,
}

impl DeliveryPersistedMergeRecovery {
    pub fn try_new(
        object_format: DeliveryGitObjectFormat,
        merge_base: impl AsRef<str>,
        candidate_merge_tree: impl AsRef<str>,
        expected_merge_commit: impl AsRef<str>,
        input: DeliveryMergeInput,
    ) -> Result<Self, DeliveryPersistenceInputError> {
        Ok(Self {
            object_format,
            merge_base: parse_commit_oid(merge_base.as_ref(), object_format)?,
            candidate_merge_tree: parse_tree_oid(candidate_merge_tree.as_ref(), object_format)?,
            expected_merge_commit: parse_commit_oid(expected_merge_commit.as_ref(), object_format)?,
            input,
        })
    }

    pub(super) const fn object_format(&self) -> DeliveryGitObjectFormat {
        self.object_format
    }

    pub(super) const fn merge_base(&self) -> &DeliveryCommitOid {
        &self.merge_base
    }

    pub(super) const fn candidate_merge_tree(&self) -> &DeliveryTreeOid {
        &self.candidate_merge_tree
    }

    pub(super) const fn expected_merge_commit(&self) -> &DeliveryCommitOid {
        &self.expected_merge_commit
    }

    pub(super) const fn input(&self) -> &DeliveryMergeInput {
        &self.input
    }
}

impl fmt::Debug for DeliveryPersistedMergeRecovery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryPersistedMergeRecovery(<redacted>)")
    }
}

/// Fixed deterministic Git commit metadata suitable for a Store proof.
#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryCommitPersistenceMetadata {
    date: String,
    message_template_version: u32,
    message: Vec<u8>,
}

impl DeliveryCommitPersistenceMetadata {
    pub(super) fn new(epoch_seconds: i64, message_template_version: u32, message: Vec<u8>) -> Self {
        Self {
            date: format!("{epoch_seconds} +0000"),
            message_template_version,
            message,
        }
    }

    pub const fn author_name(&self) -> &'static str {
        FIXED_COMMIT_IDENTITY_NAME
    }

    pub const fn author_email(&self) -> &'static str {
        FIXED_COMMIT_IDENTITY_EMAIL
    }

    pub const fn committer_name(&self) -> &'static str {
        FIXED_COMMIT_IDENTITY_NAME
    }

    pub const fn committer_email(&self) -> &'static str {
        FIXED_COMMIT_IDENTITY_EMAIL
    }

    pub fn author_date_bytes(&self) -> &[u8] {
        self.date.as_bytes()
    }

    pub fn committer_date_bytes(&self) -> &[u8] {
        self.date.as_bytes()
    }

    pub const fn message_template_version(&self) -> u32 {
        self.message_template_version
    }

    pub fn message_bytes(&self) -> &[u8] {
        &self.message
    }
}

impl fmt::Debug for DeliveryCommitPersistenceMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryCommitPersistenceMetadata(<redacted>)")
    }
}

/// Exact source object shape projected from an authenticated runtime object.
#[derive(Clone, PartialEq, Eq)]
pub struct DeliverySourceObjectPersistenceBinding {
    expected_source_commit: String,
    tree: String,
    parent: String,
    metadata: DeliveryCommitPersistenceMetadata,
}

impl DeliverySourceObjectPersistenceBinding {
    pub(super) fn new(
        expected_source_commit: String,
        tree: String,
        parent: String,
        metadata: DeliveryCommitPersistenceMetadata,
    ) -> Self {
        Self {
            expected_source_commit,
            tree,
            parent,
            metadata,
        }
    }

    pub fn expected_source_commit(&self) -> &str {
        &self.expected_source_commit
    }
    pub fn tree(&self) -> &str {
        &self.tree
    }
    pub fn parent(&self) -> &str {
        &self.parent
    }
    pub const fn metadata(&self) -> &DeliveryCommitPersistenceMetadata {
        &self.metadata
    }
}

impl fmt::Debug for DeliverySourceObjectPersistenceBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliverySourceObjectPersistenceBinding(<redacted>)")
    }
}

pub(super) fn source_object_persistence_binding(
    source: &DeliverySourceCapability,
    candidate: &DeliveryCandidateTree,
    expected: &DeliverySourceCommit,
    input: &DeliverySourceCommitInput,
) -> Result<DeliverySourceObjectPersistenceBinding, DeliverySourceError> {
    let provenance = source.candidate_tree_provenance()?;
    if !candidate.is_bound_to(&provenance)
        || !expected.is_bound_to(candidate.provenance())
        || !input.matches_identity(source.identity())
    {
        return Err(DeliverySourceError::AuthenticationChanged);
    }
    Ok(DeliverySourceObjectPersistenceBinding::new(
        expected.object_id().to_owned(),
        candidate.object_id().to_owned(),
        source.base_commit().to_owned(),
        DeliveryCommitPersistenceMetadata::new(
            input.epoch_seconds(),
            input.message_template_version(),
            input.message_bytes().to_vec(),
        ),
    ))
}

/// Exact expected merge object shape projected after runtime verification.
#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryExpectedMergePersistenceBinding {
    expected_merge_commit: String,
    tree: String,
    target_parent: String,
    source_parent: String,
    metadata: DeliveryCommitPersistenceMetadata,
}

impl DeliveryExpectedMergePersistenceBinding {
    pub(super) fn new(
        expected_merge_commit: String,
        tree: String,
        target_parent: String,
        source_parent: String,
        metadata: DeliveryCommitPersistenceMetadata,
    ) -> Self {
        Self {
            expected_merge_commit,
            tree,
            target_parent,
            source_parent,
            metadata,
        }
    }

    pub fn expected_merge_commit(&self) -> &str {
        &self.expected_merge_commit
    }
    pub fn tree(&self) -> &str {
        &self.tree
    }
    pub fn target_parent(&self) -> &str {
        &self.target_parent
    }
    pub fn source_parent(&self) -> &str {
        &self.source_parent
    }
    pub const fn metadata(&self) -> &DeliveryCommitPersistenceMetadata {
        &self.metadata
    }
}

impl fmt::Debug for DeliveryExpectedMergePersistenceBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryExpectedMergePersistenceBinding(<redacted>)")
    }
}

/// Store-facing proof of a committed source postcondition.  Counts and the
/// fixed lock reason are closed runtime facts rather than caller inputs.
#[derive(Clone, PartialEq, Eq)]
pub struct DeliverySourceAppliedPersistenceBinding {
    object: DeliverySourceObjectPersistenceBinding,
    source_branch: String,
    source_oid: String,
    common_git_identity_digest: String,
    worktree_admin_identity_digest: String,
    source_config_attributes_digest: String,
}

impl DeliverySourceAppliedPersistenceBinding {
    pub(super) fn new(
        object: DeliverySourceObjectPersistenceBinding,
        source_branch: String,
        source_oid: String,
        common_git_identity_digest: String,
        worktree_admin_identity_digest: String,
        source_config_attributes_digest: String,
    ) -> Self {
        Self {
            object,
            source_branch,
            source_oid,
            common_git_identity_digest,
            worktree_admin_identity_digest,
            source_config_attributes_digest,
        }
    }

    pub const fn object(&self) -> &DeliverySourceObjectPersistenceBinding {
        &self.object
    }
    pub fn source_branch(&self) -> &str {
        &self.source_branch
    }
    pub fn source_ref_oid(&self) -> &str {
        &self.source_oid
    }
    pub fn head_oid(&self) -> &str {
        &self.source_oid
    }
    pub fn index_tree(&self) -> &str {
        self.object.tree()
    }
    pub fn worktree_tree(&self) -> &str {
        self.object.tree()
    }
    pub const fn staged_entry_count(&self) -> u32 {
        0
    }
    pub const fn unstaged_entry_count(&self) -> u32 {
        0
    }
    pub const fn untracked_entry_count(&self) -> u32 {
        0
    }
    pub const fn unmerged_entry_count(&self) -> u32 {
        0
    }
    pub const fn common_git_identity_algorithm(&self) -> &'static str {
        DIRECTORY_IDENTITY_ALGORITHM
    }
    pub fn common_git_identity_digest(&self) -> &str {
        &self.common_git_identity_digest
    }
    pub const fn worktree_admin_identity_algorithm(&self) -> &'static str {
        DIRECTORY_IDENTITY_ALGORITHM
    }
    pub fn worktree_admin_identity_digest(&self) -> &str {
        &self.worktree_admin_identity_digest
    }
    pub const fn fixed_lock_reason(&self) -> &'static str {
        FIXED_LOCK_REASON
    }
    pub fn source_config_attributes_digest(&self) -> &str {
        &self.source_config_attributes_digest
    }
}

impl fmt::Debug for DeliverySourceAppliedPersistenceBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliverySourceAppliedPersistenceBinding(<redacted>)")
    }
}

/// Store-facing proof of the exact clean expected-merge postcondition.
#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryMergeAppliedPersistenceBinding {
    object: DeliveryExpectedMergePersistenceBinding,
    target_branch: String,
    target_head: String,
    source_branch: String,
    source_oid: String,
    common_git_identity_digest: String,
    worktree_admin_identity_digest: String,
    source_config_attributes_digest: String,
}

impl DeliveryMergeAppliedPersistenceBinding {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        object: DeliveryExpectedMergePersistenceBinding,
        target_branch: String,
        target_head: String,
        source_branch: String,
        source_oid: String,
        common_git_identity_digest: String,
        worktree_admin_identity_digest: String,
        source_config_attributes_digest: String,
    ) -> Self {
        Self {
            object,
            target_branch,
            target_head,
            source_branch,
            source_oid,
            common_git_identity_digest,
            worktree_admin_identity_digest,
            source_config_attributes_digest,
        }
    }

    pub const fn object(&self) -> &DeliveryExpectedMergePersistenceBinding {
        &self.object
    }
    pub fn target_branch(&self) -> &str {
        &self.target_branch
    }
    pub fn target_head(&self) -> &str {
        &self.target_head
    }
    pub fn source_branch(&self) -> &str {
        &self.source_branch
    }
    pub fn source_oid(&self) -> &str {
        &self.source_oid
    }
    pub const fn common_git_identity_algorithm(&self) -> &'static str {
        DIRECTORY_IDENTITY_ALGORITHM
    }
    pub fn common_git_identity_digest(&self) -> &str {
        &self.common_git_identity_digest
    }
    pub const fn worktree_admin_identity_algorithm(&self) -> &'static str {
        DIRECTORY_IDENTITY_ALGORITHM
    }
    pub fn worktree_admin_identity_digest(&self) -> &str {
        &self.worktree_admin_identity_digest
    }
    pub const fn fixed_lock_reason(&self) -> &'static str {
        FIXED_LOCK_REASON
    }
    pub fn source_config_attributes_digest(&self) -> &str {
        &self.source_config_attributes_digest
    }
    pub fn index_tree(&self) -> &str {
        self.object.tree()
    }
    pub fn worktree_tree(&self) -> &str {
        self.object.tree()
    }
    pub const fn staged_entry_count(&self) -> u32 {
        0
    }
    pub const fn unstaged_entry_count(&self) -> u32 {
        0
    }
    pub const fn untracked_entry_count(&self) -> u32 {
        0
    }
    pub const fn unmerged_entry_count(&self) -> u32 {
        0
    }
    pub const fn merge_head_is_absent(&self) -> bool {
        true
    }
    pub const fn merge_autostash_is_absent(&self) -> bool {
        true
    }
    pub const fn other_git_operation_is_clear(&self) -> bool {
        true
    }
}

impl fmt::Debug for DeliveryMergeAppliedPersistenceBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryMergeAppliedPersistenceBinding(<redacted>)")
    }
}

/// Store-facing proof captured from one real known-conflict child and its
/// repeated exact conflict observation.
#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryAbortPersistenceBinding {
    child_receipt_id: [u8; 16],
    target_branch: String,
    target_head: String,
    source_branch: String,
    source_oid: String,
    common_git_identity_digest: String,
    worktree_admin_identity_digest: String,
    source_config_attributes_digest: String,
    index_stages_digest: String,
    worktree_digest: String,
    conflict_paths: Vec<DeliveryConflictPath>,
}

impl DeliveryAbortPersistenceBinding {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        child_receipt_id: [u8; 16],
        target_branch: String,
        target_head: String,
        source_branch: String,
        source_oid: String,
        common_git_identity_digest: String,
        worktree_admin_identity_digest: String,
        source_config_attributes_digest: String,
        index_stages_digest: String,
        worktree_digest: String,
        conflict_paths: Vec<DeliveryConflictPath>,
    ) -> Option<Self> {
        if child_receipt_id == [0; 16] {
            return None;
        }
        Some(Self {
            child_receipt_id,
            target_branch,
            target_head,
            source_branch,
            source_oid,
            common_git_identity_digest,
            worktree_admin_identity_digest,
            source_config_attributes_digest,
            index_stages_digest,
            worktree_digest,
            conflict_paths,
        })
    }

    pub const fn child_receipt_id(&self) -> [u8; 16] {
        self.child_receipt_id
    }
    pub fn target_branch(&self) -> &str {
        &self.target_branch
    }
    pub fn target_head(&self) -> &str {
        &self.target_head
    }
    pub fn source_branch(&self) -> &str {
        &self.source_branch
    }
    pub fn source_oid(&self) -> &str {
        &self.source_oid
    }
    pub fn merge_head(&self) -> &str {
        &self.source_oid
    }
    pub const fn common_git_identity_algorithm(&self) -> &'static str {
        DIRECTORY_IDENTITY_ALGORITHM
    }
    pub fn common_git_identity_digest(&self) -> &str {
        &self.common_git_identity_digest
    }
    pub const fn worktree_admin_identity_algorithm(&self) -> &'static str {
        DIRECTORY_IDENTITY_ALGORITHM
    }
    pub fn worktree_admin_identity_digest(&self) -> &str {
        &self.worktree_admin_identity_digest
    }
    pub const fn fixed_lock_reason(&self) -> &'static str {
        FIXED_LOCK_REASON
    }
    pub fn source_config_attributes_digest(&self) -> &str {
        &self.source_config_attributes_digest
    }
    pub fn index_stages_digest(&self) -> &str {
        &self.index_stages_digest
    }
    pub fn worktree_digest(&self) -> &str {
        &self.worktree_digest
    }
    pub const fn merge_autostash_is_absent(&self) -> bool {
        true
    }
    pub const fn other_git_operation_is_clear(&self) -> bool {
        true
    }
    pub fn conflict_paths(&self) -> &[DeliveryConflictPath] {
        &self.conflict_paths
    }
}

impl fmt::Debug for DeliveryAbortPersistenceBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryAbortPersistenceBinding(<redacted>)")
    }
}

/// Store-facing proof of an exact clean post-abort scene.
#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryAbortAppliedPersistenceBinding {
    target_branch: String,
    target_head: String,
    source_branch: String,
    source_oid: String,
    common_git_identity_digest: String,
    worktree_admin_identity_digest: String,
    source_config_attributes_digest: String,
}

impl DeliveryAbortAppliedPersistenceBinding {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        target_branch: String,
        target_head: String,
        source_branch: String,
        source_oid: String,
        common_git_identity_digest: String,
        worktree_admin_identity_digest: String,
        source_config_attributes_digest: String,
    ) -> Self {
        Self {
            target_branch,
            target_head,
            source_branch,
            source_oid,
            common_git_identity_digest,
            worktree_admin_identity_digest,
            source_config_attributes_digest,
        }
    }
    pub fn target_branch(&self) -> &str {
        &self.target_branch
    }
    pub fn target_head(&self) -> &str {
        &self.target_head
    }
    pub fn source_branch(&self) -> &str {
        &self.source_branch
    }
    pub fn source_oid(&self) -> &str {
        &self.source_oid
    }
    pub const fn common_git_identity_algorithm(&self) -> &'static str {
        DIRECTORY_IDENTITY_ALGORITHM
    }
    pub fn common_git_identity_digest(&self) -> &str {
        &self.common_git_identity_digest
    }
    pub const fn worktree_admin_identity_algorithm(&self) -> &'static str {
        DIRECTORY_IDENTITY_ALGORITHM
    }
    pub fn worktree_admin_identity_digest(&self) -> &str {
        &self.worktree_admin_identity_digest
    }
    pub const fn fixed_lock_reason(&self) -> &'static str {
        FIXED_LOCK_REASON
    }
    pub fn source_config_attributes_digest(&self) -> &str {
        &self.source_config_attributes_digest
    }
    pub const fn staged_entry_count(&self) -> u32 {
        0
    }
    pub const fn unstaged_entry_count(&self) -> u32 {
        0
    }
    pub const fn untracked_entry_count(&self) -> u32 {
        0
    }
    pub const fn unmerged_entry_count(&self) -> u32 {
        0
    }
    pub const fn merge_head_is_absent(&self) -> bool {
        true
    }
    pub const fn merge_autostash_is_absent(&self) -> bool {
        true
    }
    pub const fn other_git_operation_is_clear(&self) -> bool {
        true
    }
}

impl fmt::Debug for DeliveryAbortAppliedPersistenceBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryAbortAppliedPersistenceBinding(<redacted>)")
    }
}

/// Persistence-only facts proven by one authenticated source/target pair.
///
/// The value is intentionally not serializable and has no public constructor.
/// It contains copies of durable scalar facts only; retaining it does not
/// retain or grant repository, filesystem, or process authority.
#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryPersistenceBinding {
    object_format: DeliveryGitObjectFormat,
    source_identity: WorktreeIdentity,
    source_branch: String,
    source_base_commit: String,
    approved_fingerprint: WorkspaceFingerprint,
    common_git_identity_digest: String,
    worktree_admin_identity_digest: String,
    source_config_attributes_digest: String,
    target_branch: String,
    expected_target_head: String,
    target_config_attributes_digest: String,
    target_security_digest: String,
}

impl DeliveryPersistenceBinding {
    fn from_authenticated_pair(
        source: &DeliverySourceCapability,
        target: &DeliveryTargetCapability,
    ) -> Result<Self, DeliverySourceError> {
        if source.common_directory_identity() != target.common_directory_identity()
            || !source
                .probe()
                .shares_repository_format_authority_with(target.probe())
        {
            return Err(DeliverySourceError::AuthenticationChanged);
        }

        Ok(Self {
            object_format: source.probe().object_format(),
            source_identity: source.identity().clone(),
            source_branch: local_branch_ref(source.branch_name()),
            source_base_commit: source.base_commit().to_owned(),
            approved_fingerprint: source.approved_fingerprint(),
            common_git_identity_digest: source.common_directory_identity().as_hex().to_owned(),
            worktree_admin_identity_digest: source.admin_directory_identity().as_hex().to_owned(),
            source_config_attributes_digest: encode_lower_hex(source.config_attributes_digest()),
            target_branch: local_branch_ref(target.branch_name()),
            expected_target_head: target.head_id().to_owned(),
            target_config_attributes_digest: encode_lower_hex(target.config_attributes_digest()),
            target_security_digest: encode_lower_hex(target.security_digest()),
        })
    }

    pub const fn object_format(&self) -> DeliveryGitObjectFormat {
        self.object_format
    }

    pub const fn source_identity(&self) -> &WorktreeIdentity {
        &self.source_identity
    }

    /// Fully qualified, validated local source ref (for example,
    /// `refs/heads/codex/task-...`).
    pub fn source_branch(&self) -> &str {
        &self.source_branch
    }

    pub fn source_base_commit(&self) -> &str {
        &self.source_base_commit
    }

    pub const fn approved_fingerprint(&self) -> WorkspaceFingerprint {
        self.approved_fingerprint
    }

    pub const fn common_git_identity_algorithm(&self) -> &'static str {
        "directory_identity_v1"
    }

    pub fn common_git_identity_digest(&self) -> &str {
        &self.common_git_identity_digest
    }

    pub const fn worktree_admin_identity_algorithm(&self) -> &'static str {
        "directory_identity_v1"
    }

    pub fn worktree_admin_identity_digest(&self) -> &str {
        &self.worktree_admin_identity_digest
    }

    /// Source config/attributes digest used by durable artifact provenance.
    pub fn source_config_attributes_digest(&self) -> &str {
        &self.source_config_attributes_digest
    }

    /// Fully qualified, validated local target ref.
    pub fn target_branch(&self) -> &str {
        &self.target_branch
    }

    pub fn expected_target_head(&self) -> &str {
        &self.expected_target_head
    }

    /// Target config/attributes digest used to bind later merge proofs.
    pub fn target_config_attributes_digest(&self) -> &str {
        &self.target_config_attributes_digest
    }

    /// Authenticated target security snapshot digest. The raw configuration,
    /// attributes, and checkout authority used to derive it never cross this
    /// persistence boundary.
    pub fn target_security_digest(&self) -> &str {
        &self.target_security_digest
    }
}

impl fmt::Debug for DeliveryPersistenceBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryPersistenceBinding(<redacted>)")
    }
}

impl DeliverySourceCapability {
    /// Projects persistence facts only when `target` was authenticated from
    /// the same repository, pinned Git authority, and object format.
    ///
    /// There is no scalar constructor for [`DeliveryPersistenceBinding`], so
    /// callers cannot substitute a path, identity digest, ref, or object ID.
    pub fn persistence_binding_for_target(
        &self,
        target: &DeliveryTargetCapability,
    ) -> Result<DeliveryPersistenceBinding, DeliverySourceError> {
        DeliveryPersistenceBinding::from_authenticated_pair(self, target)
    }
}

fn local_branch_ref(branch: &str) -> String {
    format!("{LOCAL_BRANCH_PREFIX}{branch}")
}

fn parse_local_branch_ref(value: &str) -> Result<String, DeliveryPersistenceInputError> {
    let branch = value
        .strip_prefix(LOCAL_BRANCH_PREFIX)
        .ok_or(DeliveryPersistenceInputError)?;
    if branch.is_empty() || local_branch_ref(branch) != value {
        return Err(DeliveryPersistenceInputError);
    }
    Ok(branch.to_owned())
}

fn parse_commit_oid(
    value: &str,
    object_format: DeliveryGitObjectFormat,
) -> Result<DeliveryCommitOid, DeliveryPersistenceInputError> {
    DeliveryCommitOid::try_new(value, object_format).ok_or(DeliveryPersistenceInputError)
}

fn parse_tree_oid(
    value: &str,
    object_format: DeliveryGitObjectFormat,
) -> Result<DeliveryTreeOid, DeliveryPersistenceInputError> {
    DeliveryTreeOid::try_new(value, object_format).ok_or(DeliveryPersistenceInputError)
}

pub(super) fn parse_lower_hex_digest(
    value: &str,
) -> Result<[u8; 32], DeliveryPersistenceInputError> {
    if value.len() != 64
        || !value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(DeliveryPersistenceInputError);
    }
    let mut decoded = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        decoded[index] = (decode_lower_hex_digit(pair[0])? << 4) | decode_lower_hex_digit(pair[1])?;
    }
    Ok(decoded)
}

fn decode_lower_hex_digit(value: u8) -> Result<u8, DeliveryPersistenceInputError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(DeliveryPersistenceInputError),
    }
}

pub(super) fn encode_lower_hex(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = [0_u8; 64];
    for (index, byte) in bytes.iter().copied().enumerate() {
        encoded[index * 2] = HEX[usize::from(byte >> 4)];
        encoded[index * 2 + 1] = HEX[usize::from(byte & 0x0f)];
    }
    String::from_utf8(encoded.to_vec()).expect("lower hexadecimal is valid UTF-8")
}

#[cfg(test)]
mod tests {
    use super::{encode_lower_hex, parse_lower_hex_digest};

    #[test]
    fn digest_encoding_is_fixed_width_lower_hex() {
        let mut digest = [0_u8; 32];
        digest[0] = 0xab;
        digest[31] = 0xcd;
        assert_eq!(
            encode_lower_hex(&digest),
            "ab000000000000000000000000000000000000000000000000000000000000cd"
        );
        assert_eq!(
            parse_lower_hex_digest(&encode_lower_hex(&digest)),
            Ok(digest)
        );
        assert!(parse_lower_hex_digest(&"AB".repeat(32)).is_err());
    }
}
