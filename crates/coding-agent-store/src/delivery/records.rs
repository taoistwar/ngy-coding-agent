use std::fmt;

use coding_agent_domain::{CanonicalPath, TaskId};
use uuid::Uuid;

use super::{
    BranchDisposition, CleanupKind, CleanupOperationState, DeliveryCommandId, DeliveryIdentity,
    DeliveryOperationId, DeliverySourceState, DeliveryTimestamp, DeliveryVersion,
    DirectoryIdentity, EvidenceIdentityV1, FailureCode, GitBranchRef, GitCommitOid, GitTreeOid,
    MergeOperationState, Sha256Digest,
};

#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryArtifactProvenance {
    pub identity: DeliveryIdentity,
    pub evidence: EvidenceIdentityV1,
    pub base_commit: GitCommitOid,
    pub source_branch: GitBranchRef,
    pub worktree_path: CanonicalPath,
    pub common_git_identity: DirectoryIdentity,
    pub worktree_admin_identity: DirectoryIdentity,
    pub fixed_lock_reason: String,
    pub config_attributes_digest: Sha256Digest,
}

impl fmt::Debug for DeliveryArtifactProvenance {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliveryArtifactProvenance")
            .field("identity", &self.identity)
            .field("evidence", &self.evidence)
            .field("git_and_path_values", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryCommitMetadata {
    pub author_name: String,
    pub author_email: String,
    pub committer_name: String,
    pub committer_email: String,
    pub author_date_bytes: String,
    pub committer_date_bytes: String,
    pub message_template_version: u32,
    pub message_bytes: Vec<u8>,
}

impl fmt::Debug for DeliveryCommitMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliveryCommitMetadata")
            .field("message_template_version", &self.message_template_version)
            .field("message_bytes", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct DeliverySourceRecord {
    pub provenance: DeliveryArtifactProvenance,
    pub origin_accepted_operation_id: DeliveryOperationId,
    pub origin_accept_receipt_id: DeliveryCommandId,
    pub origin_accepted_version: DeliveryVersion,
    pub candidate_tree: GitTreeOid,
    pub expected_parent: GitCommitOid,
    pub expected_source_commit: Option<GitCommitOid>,
    pub commit_metadata: DeliveryCommitMetadata,
    pub state: DeliverySourceState,
    pub failure_code: Option<FailureCode>,
    pub version: DeliveryVersion,
    pub created_at: DeliveryTimestamp,
    pub updated_at: DeliveryTimestamp,
    pub initial_transition_id: i64,
    pub current_transition_id: i64,
}

impl fmt::Debug for DeliverySourceRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliverySourceRecord")
            .field("identity", &self.provenance.identity)
            .field(
                "origin_accepted_operation_id",
                &self.origin_accepted_operation_id,
            )
            .field("state", &self.state)
            .field("version", &self.version)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeConflictPathEncoding {
    Utf8,
    Base64Url,
}

#[derive(Clone, PartialEq, Eq)]
pub struct MergeConflictRecord {
    pub ordinal: u8,
    pub path_encoding: MergeConflictPathEncoding,
    pub path_value: Vec<u8>,
}

/// Repository object inputs sealed after a durable preflight intent is created.
#[derive(Clone, PartialEq, Eq)]
pub struct PreparedMergePreflightInputs {
    pub candidate_tree: GitTreeOid,
    pub preflight_source_commit: GitCommitOid,
}

impl fmt::Debug for PreparedMergePreflightInputs {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedMergePreflightInputs")
            .field("repository_object_ids", &"<redacted>")
            .finish()
    }
}

impl fmt::Debug for MergeConflictRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MergeConflictRecord")
            .field("ordinal", &self.ordinal)
            .field("path_encoding", &self.path_encoding)
            .field("path_value", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct MergeOperationRecord {
    pub operation_id: DeliveryOperationId,
    pub provenance: DeliveryArtifactProvenance,
    /// `None` is the durable intent-only phase. Once present, the pair is immutable.
    pub preflight_inputs: Option<PreparedMergePreflightInputs>,
    pub delivery_source_task_id: Option<TaskId>,
    pub source_commit: Option<GitCommitOid>,
    pub preflight_receipt_id: DeliveryCommandId,
    pub accept_receipt_id: Option<DeliveryCommandId>,
    pub target_branch: GitBranchRef,
    pub expected_target_head: GitCommitOid,
    /// Target-side attributes baseline captured before any merge mutation.
    pub target_config_attributes_digest: Sha256Digest,
    /// Target-side Git security baseline captured before any merge mutation.
    pub target_security_digest: Sha256Digest,
    pub merge_base: Option<GitCommitOid>,
    pub candidate_merge_tree: Option<GitTreeOid>,
    pub merge_metadata: Option<DeliveryCommitMetadata>,
    pub expected_merge_commit: Option<GitCommitOid>,
    pub abort_child_receipt_id: Option<Uuid>,
    pub abort_merge_head: Option<GitCommitOid>,
    pub abort_index_stages_digest: Option<Sha256Digest>,
    pub abort_worktree_digest: Option<Sha256Digest>,
    pub abort_merge_autostash_proof: Option<String>,
    pub merged_disposition_task_id: Option<TaskId>,
    pub conflict_path_count: Option<u8>,
    pub conflicts: Vec<MergeConflictRecord>,
    pub state: MergeOperationState,
    pub failure_code: Option<FailureCode>,
    pub version: DeliveryVersion,
    pub created_at: DeliveryTimestamp,
    pub updated_at: DeliveryTimestamp,
    pub initial_transition_id: i64,
    pub current_transition_id: i64,
}

impl fmt::Debug for MergeOperationRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MergeOperationRecord")
            .field("operation_id", &self.operation_id)
            .field("identity", &self.provenance.identity)
            .field("state", &self.state)
            .field("version", &self.version)
            .field("sealed_conflict_count", &self.conflict_path_count)
            .field("conflict_count", &self.conflicts.len())
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ArtifactDispositionRecord {
    pub identity: DeliveryIdentity,
    pub merged_operation_id: DeliveryOperationId,
    pub delivery_source_task_id: TaskId,
    pub source_commit: GitCommitOid,
    pub worktree_cleanup_operation_id: Option<DeliveryOperationId>,
    pub worktree_cleanup_operation_version: Option<DeliveryVersion>,
    pub worktree_cleanup_operation_state: Option<CleanupOperationState>,
    pub branch_cleanup_operation_id: Option<DeliveryOperationId>,
    pub branch_cleanup_operation_version: Option<DeliveryVersion>,
    pub branch_cleanup_operation_state: Option<CleanupOperationState>,
    pub worktree_state: super::WorktreeDisposition,
    pub worktree_version: DeliveryVersion,
    pub worktree_failure_code: Option<FailureCode>,
    pub worktree_updated_at: DeliveryTimestamp,
    pub branch_state: BranchDisposition,
    pub branch_version: DeliveryVersion,
    pub branch_failure_code: Option<FailureCode>,
    pub branch_updated_at: DeliveryTimestamp,
    pub created_at: DeliveryTimestamp,
    pub worktree_initial_transition_id: i64,
    pub worktree_current_transition_id: i64,
    pub branch_initial_transition_id: i64,
    pub branch_current_transition_id: i64,
}

impl fmt::Debug for ArtifactDispositionRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactDispositionRecord")
            .field("identity", &self.identity)
            .field("worktree_state", &self.worktree_state)
            .field("worktree_version", &self.worktree_version)
            .field("branch_state", &self.branch_state)
            .field("branch_version", &self.branch_version)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct CleanupOperationRecord {
    pub operation_id: DeliveryOperationId,
    pub identity: DeliveryIdentity,
    pub kind: CleanupKind,
    pub origin_receipt_id: DeliveryCommandId,
    pub expected_merge_operation_id: DeliveryOperationId,
    pub disposition_task_id: TaskId,
    pub expected_worktree_path: CanonicalPath,
    pub expected_admin_identity: DirectoryIdentity,
    pub expected_common_git_identity: DirectoryIdentity,
    pub expected_source_ref: GitBranchRef,
    pub expected_source_oid: GitCommitOid,
    pub expected_disposition_version: DeliveryVersion,
    pub expected_target_ref: Option<GitBranchRef>,
    pub expected_target_head: Option<GitCommitOid>,
    pub origin_target_head: Option<GitCommitOid>,
    pub target_head_observations: Vec<CleanupTargetHeadObservationRecord>,
    pub state: CleanupOperationState,
    pub failure_code: Option<FailureCode>,
    pub version: DeliveryVersion,
    pub created_at: DeliveryTimestamp,
    pub updated_at: DeliveryTimestamp,
    pub initial_transition_id: i64,
    pub current_transition_id: i64,
}

#[derive(Clone, PartialEq, Eq)]
pub struct CleanupTargetHeadObservationRecord {
    pub operation_version: DeliveryVersion,
    pub target_head: GitCommitOid,
    pub observed_at: DeliveryTimestamp,
}

impl CleanupOperationRecord {
    pub fn target_head_at(&self, version: DeliveryVersion) -> Option<&GitCommitOid> {
        self.target_head_observations
            .iter()
            .find(|observation| observation.operation_version == version)
            .map(|observation| &observation.target_head)
    }
}

impl fmt::Debug for CleanupTargetHeadObservationRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CleanupTargetHeadObservationRecord")
            .field("operation_version", &self.operation_version)
            .field("target_head", &self.target_head)
            .field("observed_at", &self.observed_at)
            .finish()
    }
}

impl fmt::Debug for CleanupOperationRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CleanupOperationRecord")
            .field("operation_id", &self.operation_id)
            .field("identity", &self.identity)
            .field("kind", &self.kind)
            .field("state", &self.state)
            .field("version", &self.version)
            .finish()
    }
}
