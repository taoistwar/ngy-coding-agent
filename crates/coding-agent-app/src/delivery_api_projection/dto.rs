use coding_agent_domain::TaskId;
use coding_agent_store::{
    DeliveryOperationId, DeliveryVersion, GitBranchRef, GitCommitOid, GitTreeOid, Sha256Digest,
};

/// Stable eligibility state exposed to delivery API consumers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryEligibility {
    Eligible,
    Ineligible,
    Unavailable,
}

/// Typed, path-free reasons used by the delivery query projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryEligibilityReason {
    TaskNotFound,
    TaskNotCompleted,
    ReviewNotApproved,
    ApprovedEvidenceMissing,
    AttemptArtifactMissing,
    AttemptArtifactNotReady,
    TaskActive,
    ProcessCleanupUnproven,
    TargetBranchDetached,
    TargetBranchMismatch,
    TargetWorktreeDirty,
    TargetIgnoredPathCollision,
    TargetGitOperationInProgress,
    UnsafeGitConfiguration,
    UnsupportedGitAttributes,
    SourceAlreadyInTarget,
    RuntimeDrift,
    DeliveryOwned,
    AlreadyMerged,
    ReconciliationRequired,
    RepositoryBusy,
    RepositoryUnavailable,
    StoreUnavailable,
    RuntimeObservationUnavailable,
    ServiceNotReady,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryAllowedAction {
    RunPreflight,
    AcceptMerge,
    RemoveWorktree,
    DeleteBranch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryQueryUnavailableReason {
    StoreUnavailable,
    OrchestrationUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryTargetUnavailableReason {
    TargetBranchDetached,
    TargetBranchMismatch,
    TargetWorktreeDirty,
    TargetIgnoredPathCollision,
    TargetGitOperationInProgress,
    UnsafeGitConfiguration,
    UnsupportedGitAttributes,
    SourceAlreadyInTarget,
    TargetHeadChanged,
    RuntimeUnavailable,
    ProcessCleanupUnproven,
    ReconciliationRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "availability", rename_all = "snake_case")]
pub enum DeliveryTargetObservation {
    Available {
        branch: GitBranchRef,
        head: GitCommitOid,
    },
    Unavailable {
        reason: DeliveryTargetUnavailableReason,
    },
}

impl DeliveryTargetObservation {
    pub(crate) fn available(branch: GitBranchRef, head: GitCommitOid) -> Self {
        Self::Available { branch, head }
    }

    pub(crate) const fn unavailable(reason: DeliveryTargetUnavailableReason) -> Self {
        Self::Unavailable { reason }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryCleanupOperationState {
    UnlockPending,
    UnlockedPendingRemove,
    RemovePending,
    DeletePending,
    Completed,
    Failed,
    ReconciliationRequired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryCleanupOperationKind {
    RemoveWorktree,
    DeleteBranch,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeliveryEvidenceProjection {
    review_generation: u64,
    workspace_fingerprint: Sha256Digest,
}

impl DeliveryEvidenceProjection {
    pub(crate) const fn new(review_generation: u64, workspace_fingerprint: Sha256Digest) -> Self {
        Self {
            review_generation,
            workspace_fingerprint,
        }
    }

    pub const fn review_generation(&self) -> u64 {
        self.review_generation
    }

    pub const fn workspace_fingerprint(&self) -> &Sha256Digest {
        &self.workspace_fingerprint
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliverySourceProjectionState {
    ObjectPending,
    CommitPending,
    Committed,
    ReconciliationRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeliverySourceProjection {
    state: DeliverySourceProjectionState,
    version: DeliveryVersion,
    source_ref: GitBranchRef,
    source_oid: Option<GitCommitOid>,
}

impl DeliverySourceProjection {
    pub(crate) const fn new(
        state: DeliverySourceProjectionState,
        version: DeliveryVersion,
        source_ref: GitBranchRef,
        source_oid: Option<GitCommitOid>,
    ) -> Self {
        Self {
            state,
            version,
            source_ref,
            source_oid,
        }
    }

    pub const fn state(&self) -> DeliverySourceProjectionState {
        self.state
    }

    pub const fn version(&self) -> DeliveryVersion {
        self.version
    }

    pub const fn source_ref(&self) -> &GitBranchRef {
        &self.source_ref
    }

    pub const fn source_oid(&self) -> Option<&GitCommitOid> {
        self.source_oid.as_ref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryConflictPathEncoding {
    Utf8,
    Base64url,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeliveryConflictPathProjection {
    encoding: DeliveryConflictPathEncoding,
    path_bytes: Vec<u8>,
}

impl DeliveryConflictPathProjection {
    pub(crate) const fn new(encoding: DeliveryConflictPathEncoding, path_bytes: Vec<u8>) -> Self {
        Self {
            encoding,
            path_bytes,
        }
    }

    pub const fn encoding(&self) -> DeliveryConflictPathEncoding {
        self.encoding
    }

    pub fn path_bytes(&self) -> &[u8] {
        &self.path_bytes
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeliveryConflictSummaryProjection {
    path_count: u32,
    paths: Vec<DeliveryConflictPathProjection>,
    payload_bytes: u32,
    truncated: bool,
}

impl DeliveryConflictSummaryProjection {
    pub(crate) const fn new(
        path_count: u32,
        paths: Vec<DeliveryConflictPathProjection>,
        payload_bytes: u32,
        truncated: bool,
    ) -> Self {
        Self {
            path_count,
            paths,
            payload_bytes,
            truncated,
        }
    }

    pub const fn path_count(&self) -> u32 {
        self.path_count
    }

    pub fn paths(&self) -> &[DeliveryConflictPathProjection] {
        &self.paths
    }

    pub const fn payload_bytes(&self) -> u32 {
        self.payload_bytes
    }

    pub const fn truncated(&self) -> bool {
        self.truncated
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeliveryMergeOperationProjection {
    operation_id: DeliveryOperationId,
    task_id: TaskId,
    version: DeliveryVersion,
    state: DeliveryPreflightState,
    review_generation: u64,
    workspace_fingerprint: Sha256Digest,
    candidate_source_tree: Option<GitTreeOid>,
    preflight_source_commit: Option<GitCommitOid>,
    source_commit: Option<GitCommitOid>,
    target_branch: GitBranchRef,
    target_head: GitCommitOid,
    conflicts: Option<DeliveryConflictSummaryProjection>,
    failure: Option<String>,
}

impl DeliveryMergeOperationProjection {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        operation_id: DeliveryOperationId,
        task_id: TaskId,
        version: DeliveryVersion,
        state: DeliveryPreflightState,
        review_generation: u64,
        workspace_fingerprint: Sha256Digest,
        candidate_source_tree: Option<GitTreeOid>,
        preflight_source_commit: Option<GitCommitOid>,
        source_commit: Option<GitCommitOid>,
        target_branch: GitBranchRef,
        target_head: GitCommitOid,
        conflicts: Option<DeliveryConflictSummaryProjection>,
        failure: Option<String>,
    ) -> Self {
        Self {
            operation_id,
            task_id,
            version,
            state,
            review_generation,
            workspace_fingerprint,
            candidate_source_tree,
            preflight_source_commit,
            source_commit,
            target_branch,
            target_head,
            conflicts,
            failure,
        }
    }

    pub const fn operation_id(&self) -> DeliveryOperationId {
        self.operation_id
    }

    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub const fn version(&self) -> DeliveryVersion {
        self.version
    }

    pub const fn state(&self) -> DeliveryPreflightState {
        self.state
    }

    pub const fn review_generation(&self) -> u64 {
        self.review_generation
    }

    pub const fn workspace_fingerprint(&self) -> &Sha256Digest {
        &self.workspace_fingerprint
    }

    pub const fn candidate_source_tree(&self) -> Option<&GitTreeOid> {
        self.candidate_source_tree.as_ref()
    }

    pub const fn preflight_source_commit(&self) -> Option<&GitCommitOid> {
        self.preflight_source_commit.as_ref()
    }

    pub const fn source_commit(&self) -> Option<&GitCommitOid> {
        self.source_commit.as_ref()
    }

    pub const fn target_branch(&self) -> &GitBranchRef {
        &self.target_branch
    }

    pub const fn target_head(&self) -> &GitCommitOid {
        &self.target_head
    }

    pub const fn conflicts(&self) -> Option<&DeliveryConflictSummaryProjection> {
        self.conflicts.as_ref()
    }

    pub fn failure(&self) -> Option<&str> {
        self.failure.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeliveryCleanupOperationProjection {
    operation_id: DeliveryOperationId,
    task_id: TaskId,
    cleanup_kind: DeliveryCleanupOperationKind,
    version: DeliveryVersion,
    state: DeliveryCleanupOperationState,
    expected_disposition_version: DeliveryVersion,
    expected_merge_operation_id: DeliveryOperationId,
    expected_source_ref: GitBranchRef,
    expected_source_oid: GitCommitOid,
    target_branch: Option<GitBranchRef>,
    target_head: Option<GitCommitOid>,
    failure: Option<String>,
}

impl DeliveryCleanupOperationProjection {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        operation_id: DeliveryOperationId,
        task_id: TaskId,
        cleanup_kind: DeliveryCleanupOperationKind,
        version: DeliveryVersion,
        state: DeliveryCleanupOperationState,
        expected_disposition_version: DeliveryVersion,
        expected_merge_operation_id: DeliveryOperationId,
        expected_source_ref: GitBranchRef,
        expected_source_oid: GitCommitOid,
        target_branch: Option<GitBranchRef>,
        target_head: Option<GitCommitOid>,
        failure: Option<String>,
    ) -> Self {
        Self {
            operation_id,
            task_id,
            cleanup_kind,
            version,
            state,
            expected_disposition_version,
            expected_merge_operation_id,
            expected_source_ref,
            expected_source_oid,
            target_branch,
            target_head,
            failure,
        }
    }

    pub const fn operation_id(&self) -> DeliveryOperationId {
        self.operation_id
    }

    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub const fn cleanup_kind(&self) -> DeliveryCleanupOperationKind {
        self.cleanup_kind
    }

    pub const fn version(&self) -> DeliveryVersion {
        self.version
    }

    pub const fn state(&self) -> DeliveryCleanupOperationState {
        self.state
    }

    pub const fn expected_disposition_version(&self) -> DeliveryVersion {
        self.expected_disposition_version
    }

    pub const fn expected_merge_operation_id(&self) -> DeliveryOperationId {
        self.expected_merge_operation_id
    }

    pub const fn expected_source_ref(&self) -> &GitBranchRef {
        &self.expected_source_ref
    }

    pub const fn expected_source_oid(&self) -> &GitCommitOid {
        &self.expected_source_oid
    }

    pub const fn target_branch(&self) -> Option<&GitBranchRef> {
        self.target_branch.as_ref()
    }

    pub const fn target_head(&self) -> Option<&GitCommitOid> {
        self.target_head.as_ref()
    }

    pub fn failure(&self) -> Option<&str> {
        self.failure.as_deref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryWorktreeDispositionState {
    RetainedLocked,
    RetainedUnlocked,
    Removed,
    ReconciliationRequired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryBranchDispositionState {
    Retained,
    Deleted,
    ReconciliationRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeliveryArtifactDispositionProjection {
    merged_operation_id: DeliveryOperationId,
    source_ref: GitBranchRef,
    source_oid: GitCommitOid,
    worktree_state: DeliveryWorktreeDispositionState,
    worktree_version: DeliveryVersion,
    worktree_failure: Option<String>,
    branch_state: DeliveryBranchDispositionState,
    branch_version: DeliveryVersion,
    branch_failure: Option<String>,
}

impl DeliveryArtifactDispositionProjection {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        merged_operation_id: DeliveryOperationId,
        source_ref: GitBranchRef,
        source_oid: GitCommitOid,
        worktree_state: DeliveryWorktreeDispositionState,
        worktree_version: DeliveryVersion,
        worktree_failure: Option<String>,
        branch_state: DeliveryBranchDispositionState,
        branch_version: DeliveryVersion,
        branch_failure: Option<String>,
    ) -> Self {
        Self {
            merged_operation_id,
            source_ref,
            source_oid,
            worktree_state,
            worktree_version,
            worktree_failure,
            branch_state,
            branch_version,
            branch_failure,
        }
    }

    pub const fn merged_operation_id(&self) -> DeliveryOperationId {
        self.merged_operation_id
    }

    pub const fn source_ref(&self) -> &GitBranchRef {
        &self.source_ref
    }

    pub const fn source_oid(&self) -> &GitCommitOid {
        &self.source_oid
    }

    pub const fn worktree_state(&self) -> DeliveryWorktreeDispositionState {
        self.worktree_state
    }

    pub const fn worktree_version(&self) -> DeliveryVersion {
        self.worktree_version
    }

    pub fn worktree_failure(&self) -> Option<&str> {
        self.worktree_failure.as_deref()
    }

    pub const fn branch_state(&self) -> DeliveryBranchDispositionState {
        self.branch_state
    }

    pub const fn branch_version(&self) -> DeliveryVersion {
        self.branch_version
    }

    pub fn branch_failure(&self) -> Option<&str> {
        self.branch_failure.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DeliveryOperationProjection {
    Merge {
        operation_id: DeliveryOperationId,
        task_id: TaskId,
        version: DeliveryVersion,
        state: DeliveryPreflightState,
        details: Option<DeliveryMergeOperationProjection>,
    },
    Cleanup {
        operation_id: DeliveryOperationId,
        task_id: TaskId,
        version: DeliveryVersion,
        cleanup_kind: DeliveryCleanupOperationKind,
        state: DeliveryCleanupOperationState,
        details: Option<DeliveryCleanupOperationProjection>,
    },
}

impl DeliveryOperationProjection {
    pub const fn merge(
        operation_id: DeliveryOperationId,
        task_id: TaskId,
        version: DeliveryVersion,
        state: DeliveryPreflightState,
    ) -> Self {
        Self::Merge {
            operation_id,
            task_id,
            version,
            state,
            details: None,
        }
    }

    pub(crate) fn merge_detailed(details: DeliveryMergeOperationProjection) -> Self {
        Self::Merge {
            operation_id: details.operation_id(),
            task_id: details.task_id(),
            version: details.version(),
            state: details.state(),
            details: Some(details),
        }
    }

    pub const fn cleanup(
        operation_id: DeliveryOperationId,
        task_id: TaskId,
        version: DeliveryVersion,
        cleanup_kind: DeliveryCleanupOperationKind,
        state: DeliveryCleanupOperationState,
    ) -> Self {
        Self::Cleanup {
            operation_id,
            task_id,
            version,
            cleanup_kind,
            state,
            details: None,
        }
    }

    pub(crate) fn cleanup_detailed(details: DeliveryCleanupOperationProjection) -> Self {
        Self::Cleanup {
            operation_id: details.operation_id(),
            task_id: details.task_id(),
            version: details.version(),
            cleanup_kind: details.cleanup_kind(),
            state: details.state(),
            details: Some(details),
        }
    }

    pub const fn operation_id(&self) -> DeliveryOperationId {
        match self {
            Self::Merge { operation_id, .. } | Self::Cleanup { operation_id, .. } => *operation_id,
        }
    }

    pub const fn version(&self) -> DeliveryVersion {
        match self {
            Self::Merge { version, .. } | Self::Cleanup { version, .. } => *version,
        }
    }

    pub const fn task_id(&self) -> TaskId {
        match self {
            Self::Merge { task_id, .. } | Self::Cleanup { task_id, .. } => *task_id,
        }
    }

    pub const fn merge_details(&self) -> Option<&DeliveryMergeOperationProjection> {
        match self {
            Self::Merge { details, .. } => details.as_ref(),
            Self::Cleanup { .. } => None,
        }
    }

    pub const fn cleanup_details(&self) -> Option<&DeliveryCleanupOperationProjection> {
        match self {
            Self::Cleanup { details, .. } => details.as_ref(),
            Self::Merge { .. } => None,
        }
    }

    pub const fn allowed_actions(&self) -> &'static [DeliveryAllowedAction] {
        match self {
            Self::Merge {
                state: DeliveryPreflightState::PreflightReady,
                ..
            } => &[DeliveryAllowedAction::AcceptMerge],
            Self::Merge {
                state:
                    DeliveryPreflightState::Conflict
                    | DeliveryPreflightState::Rejected
                    | DeliveryPreflightState::Stale
                    | DeliveryPreflightState::Superseded
                    | DeliveryPreflightState::Failed,
                ..
            } => &[DeliveryAllowedAction::RunPreflight],
            Self::Cleanup {
                cleanup_kind: DeliveryCleanupOperationKind::RemoveWorktree,
                state: DeliveryCleanupOperationState::Failed,
                ..
            } => &[DeliveryAllowedAction::RemoveWorktree],
            Self::Cleanup {
                cleanup_kind: DeliveryCleanupOperationKind::DeleteBranch,
                state: DeliveryCleanupOperationState::Failed,
                ..
            } => &[DeliveryAllowedAction::DeleteBranch],
            Self::Merge { .. } | Self::Cleanup { .. } => &[],
        }
    }
}

pub(crate) struct DeliveryTaskProjectionContext {
    pub(crate) latest_operation: Option<DeliveryOperationProjection>,
    pub(crate) target: DeliveryTargetObservation,
    pub(crate) evidence: Option<DeliveryEvidenceProjection>,
    pub(crate) source: Option<DeliverySourceProjection>,
    pub(crate) latest_merge: Option<DeliveryMergeOperationProjection>,
    pub(crate) latest_cleanup: Option<DeliveryCleanupOperationProjection>,
    pub(crate) disposition: Option<DeliveryArtifactDispositionProjection>,
}

impl DeliveryTaskProjectionContext {
    #[cfg(test)]
    pub(crate) const fn minimal(
        latest_operation: Option<DeliveryOperationProjection>,
        target: DeliveryTargetObservation,
    ) -> Self {
        Self {
            latest_operation,
            target,
            evidence: None,
            source: None,
            latest_merge: None,
            latest_cleanup: None,
            disposition: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeliveryTaskProjection {
    task_id: TaskId,
    eligibility: DeliveryEligibility,
    reasons: Vec<DeliveryEligibilityReason>,
    latest_operation: Option<DeliveryOperationProjection>,
    target: DeliveryTargetObservation,
    evidence: Option<DeliveryEvidenceProjection>,
    source: Option<DeliverySourceProjection>,
    latest_merge: Option<DeliveryMergeOperationProjection>,
    latest_cleanup: Option<DeliveryCleanupOperationProjection>,
    disposition: Option<DeliveryArtifactDispositionProjection>,
    allowed_actions: Vec<DeliveryAllowedAction>,
}

impl DeliveryTaskProjection {
    pub(crate) fn new(
        task_id: TaskId,
        eligibility: DeliveryEligibility,
        reasons: Vec<DeliveryEligibilityReason>,
        context: DeliveryTaskProjectionContext,
        allowed_actions: Vec<DeliveryAllowedAction>,
    ) -> Self {
        Self {
            task_id,
            eligibility,
            reasons,
            latest_operation: context.latest_operation,
            target: context.target,
            evidence: context.evidence,
            source: context.source,
            latest_merge: context.latest_merge,
            latest_cleanup: context.latest_cleanup,
            disposition: context.disposition,
            allowed_actions,
        }
    }

    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub const fn eligibility(&self) -> DeliveryEligibility {
        self.eligibility
    }

    pub fn reasons(&self) -> &[DeliveryEligibilityReason] {
        &self.reasons
    }

    pub fn allowed_actions(&self) -> &[DeliveryAllowedAction] {
        &self.allowed_actions
    }

    pub fn latest_operation(&self) -> Option<DeliveryOperationProjection> {
        self.latest_operation.clone()
    }

    pub const fn target(&self) -> &DeliveryTargetObservation {
        &self.target
    }

    pub const fn evidence(&self) -> Option<&DeliveryEvidenceProjection> {
        self.evidence.as_ref()
    }

    pub const fn source(&self) -> Option<&DeliverySourceProjection> {
        self.source.as_ref()
    }

    pub const fn latest_merge(&self) -> Option<&DeliveryMergeOperationProjection> {
        self.latest_merge.as_ref()
    }

    pub const fn latest_cleanup(&self) -> Option<&DeliveryCleanupOperationProjection> {
        self.latest_cleanup.as_ref()
    }

    pub const fn disposition(&self) -> Option<&DeliveryArtifactDispositionProjection> {
        self.disposition.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
// Keep the rich projection inline: this is the authoritative public query
// envelope and boxing would unnecessarily leak transport layout into callers.
#[allow(clippy::large_enum_variant)]
pub enum DeliveryTaskQueryOutcome {
    Found {
        projection: DeliveryTaskProjection,
    },
    NotFound {
        task_id: TaskId,
    },
    Unavailable {
        task_id: TaskId,
        reason: DeliveryQueryUnavailableReason,
    },
}

impl DeliveryTaskQueryOutcome {
    pub(crate) const fn found(projection: DeliveryTaskProjection) -> Self {
        Self::Found { projection }
    }

    pub(crate) const fn not_found(task_id: TaskId) -> Self {
        Self::NotFound { task_id }
    }

    pub(crate) const fn unavailable(
        task_id: TaskId,
        reason: DeliveryQueryUnavailableReason,
    ) -> Self {
        Self::Unavailable { task_id, reason }
    }

    pub const fn task_id(&self) -> TaskId {
        match self {
            Self::Found { projection } => projection.task_id(),
            Self::NotFound { task_id } | Self::Unavailable { task_id, .. } => *task_id,
        }
    }

    pub const fn projection(&self) -> Option<&DeliveryTaskProjection> {
        match self {
            Self::Found { projection } => Some(projection),
            Self::NotFound { .. } | Self::Unavailable { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
// As above, preserve the by-value public projection contract.
#[allow(clippy::large_enum_variant)]
pub enum DeliveryOperationQueryOutcome {
    Found {
        operation: DeliveryOperationProjection,
    },
    NotFound {
        operation_id: DeliveryOperationId,
    },
    Unavailable {
        operation_id: DeliveryOperationId,
        reason: DeliveryQueryUnavailableReason,
    },
}

impl DeliveryOperationQueryOutcome {
    pub const fn found(operation: DeliveryOperationProjection) -> Self {
        Self::Found { operation }
    }

    pub const fn not_found(operation_id: DeliveryOperationId) -> Self {
        Self::NotFound { operation_id }
    }

    pub const fn unavailable(
        operation_id: DeliveryOperationId,
        reason: DeliveryQueryUnavailableReason,
    ) -> Self {
        Self::Unavailable {
            operation_id,
            reason,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryPreflightBusyReason {
    RepositoryBusy,
    WorkerQueueFull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryPreflightUnavailableReason {
    ManagerQuiescing,
    ServiceNotReady,
    RepositoryControlUnavailable,
    StoreUnavailable,
    RuntimeUnavailable,
    SourceInconsistent,
    ProcessProofUnavailable,
    CommandTimedOut,
    OutcomeUnknown,
    OrchestrationUnavailable,
}

/// A deterministic command rejection that is safe to expose through the HTTP
/// boundary. These values are deliberately typed at the manager boundary so
/// callers never select a stable API code by inspecting an error message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryCommandConflict {
    IdempotencyConflict,
    EvidenceStale,
    SourceChanged,
    PreflightStale,
    OperationInProgress,
    TargetBranchMismatch,
    TargetHeadChanged,
    MergeConflict,
    ArtifactCleanupNotAllowed,
    ArtifactProcessStillActive,
    WorktreeIdentityMismatch,
    SourceBranchNotMerged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryPreflightDurability {
    Created,
    Existing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryPreflightState {
    PreflightPending,
    PreflightReady,
    Conflict,
    Rejected,
    Stale,
    Superseded,
    Accepted,
    MergePending,
    Merged,
    AbortPending,
    Failed,
    ReconciliationRequired,
}

/// Path-free projection of one durably accepted preflight command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeliveryPreflightOperation {
    operation_id: DeliveryOperationId,
    durability: DeliveryPreflightDurability,
    state: DeliveryPreflightState,
}

/// Bounded retry advice for a command whose durable preflight intent exists,
/// while the next exact Store transition is proven not to have applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeliveryPreflightRetry {
    operation: DeliveryPreflightOperation,
    retry_after_millis: u64,
}

impl DeliveryPreflightRetry {
    pub(crate) const fn new(
        operation: DeliveryPreflightOperation,
        retry_after_millis: u64,
    ) -> Self {
        Self {
            operation,
            retry_after_millis,
        }
    }

    pub const fn operation(self) -> DeliveryPreflightOperation {
        self.operation
    }

    pub const fn retry_after_millis(self) -> u64 {
        self.retry_after_millis
    }
}

impl DeliveryPreflightOperation {
    pub(crate) const fn new(
        operation_id: DeliveryOperationId,
        durability: DeliveryPreflightDurability,
        state: DeliveryPreflightState,
    ) -> Self {
        Self {
            operation_id,
            durability,
            state,
        }
    }

    pub const fn operation_id(self) -> DeliveryOperationId {
        self.operation_id
    }

    pub const fn durability(self) -> DeliveryPreflightDurability {
        self.durability
    }

    pub const fn state(self) -> DeliveryPreflightState {
        self.state
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "outcome", content = "reason", rename_all = "snake_case")]
pub enum DeliveryPreflightOutcome {
    Durable(DeliveryPreflightOperation),
    KnownNotAppliedPersisted(DeliveryPreflightRetry),
    Ineligible(Vec<DeliveryEligibilityReason>),
    Conflict(DeliveryCommandConflict),
    Busy(DeliveryPreflightBusyReason),
    Unavailable(DeliveryPreflightUnavailableReason),
}

/// Whether an exact accept command created its durable receipt or replayed the
/// same canonical receipt. This is intentionally independent from later
/// source/merge progress: an HTTP response may be returned as soon as
/// `Accepted` is durable while the actor keeps repository ownership.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryMergeReceiptDisposition {
    Created,
    Existing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeliveryMergeAcceptance {
    operation_id: DeliveryOperationId,
    accepted_version: DeliveryVersion,
    receipt: DeliveryMergeReceiptDisposition,
}

impl DeliveryMergeAcceptance {
    pub(crate) const fn new(
        operation_id: DeliveryOperationId,
        accepted_version: DeliveryVersion,
        receipt: DeliveryMergeReceiptDisposition,
    ) -> Self {
        Self {
            operation_id,
            accepted_version,
            receipt,
        }
    }

    pub const fn operation_id(self) -> DeliveryOperationId {
        self.operation_id
    }

    pub const fn accepted_version(self) -> DeliveryVersion {
        self.accepted_version
    }

    pub const fn receipt(self) -> DeliveryMergeReceiptDisposition {
        self.receipt
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "outcome", content = "reason", rename_all = "snake_case")]
pub enum DeliveryMergeAcceptanceOutcome {
    Durable(DeliveryMergeAcceptance),
    Ineligible(Vec<DeliveryEligibilityReason>),
    Conflict(DeliveryCommandConflict),
    Busy(DeliveryPreflightBusyReason),
    Unavailable(DeliveryPreflightUnavailableReason),
}

/// Whether an independent cleanup command created its receipt or replayed the
/// exact canonical receipt after disconnect/restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryCleanupReceiptDisposition {
    Created,
    Existing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeliveryCleanupAcceptance {
    operation_id: DeliveryOperationId,
    accepted_version: DeliveryVersion,
    cleanup_kind: DeliveryCleanupOperationKind,
    accepted_state: DeliveryCleanupOperationState,
    receipt: DeliveryCleanupReceiptDisposition,
}

impl DeliveryCleanupAcceptance {
    pub(crate) const fn new(
        operation_id: DeliveryOperationId,
        accepted_version: DeliveryVersion,
        cleanup_kind: DeliveryCleanupOperationKind,
        accepted_state: DeliveryCleanupOperationState,
        receipt: DeliveryCleanupReceiptDisposition,
    ) -> Self {
        Self {
            operation_id,
            accepted_version,
            cleanup_kind,
            accepted_state,
            receipt,
        }
    }

    pub const fn operation_id(self) -> DeliveryOperationId {
        self.operation_id
    }

    pub const fn accepted_version(self) -> DeliveryVersion {
        self.accepted_version
    }

    pub const fn cleanup_kind(self) -> DeliveryCleanupOperationKind {
        self.cleanup_kind
    }

    pub const fn accepted_state(self) -> DeliveryCleanupOperationState {
        self.accepted_state
    }

    pub const fn receipt(self) -> DeliveryCleanupReceiptDisposition {
        self.receipt
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "outcome", content = "reason", rename_all = "snake_case")]
pub enum DeliveryCleanupAcceptanceOutcome {
    Durable(DeliveryCleanupAcceptance),
    Ineligible(Vec<DeliveryEligibilityReason>),
    Conflict(DeliveryCommandConflict),
    Busy(DeliveryPreflightBusyReason),
    Unavailable(DeliveryPreflightUnavailableReason),
}
