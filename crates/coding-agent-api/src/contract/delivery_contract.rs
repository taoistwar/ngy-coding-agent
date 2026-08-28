use serde::{Deserialize, Serialize};
use utoipa::openapi::Ref;
use utoipa::openapi::schema::{Discriminator, OneOfBuilder, Schema};
use utoipa::{PartialSchema, ToSchema};
use uuid::Uuid;

pub const DELIVERY_JS_SAFE_INTEGER_MAX: u64 = 9_007_199_254_740_991;
pub const DELIVERY_MAX_BRANCH_REF_BYTES: usize = 4_096;
pub const DELIVERY_MAX_CONFLICT_PATHS: usize = 128;
pub const DELIVERY_MAX_CONFLICT_PATH_BYTES: usize = 4_096;
pub const DELIVERY_MAX_CONFLICT_PAYLOAD_BYTES: usize = 65_536;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeliveryPreflightRequest {
    #[schema(
        format = Uuid,
        min_length = 36,
        max_length = 36,
        pattern = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    )]
    pub client_request_id: String,
    #[schema(
        min_length = 12,
        max_length = 4_096,
        pattern = "^refs/heads/.{1,4085}$"
    )]
    pub target_branch: String,
    #[schema(
        min_length = 40,
        max_length = 64,
        pattern = "^([0-9a-f]{40}|[0-9a-f]{64})$"
    )]
    pub expected_target_head: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeliveryMergeRequest {
    #[schema(
        format = Uuid,
        min_length = 36,
        max_length = 36,
        pattern = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    )]
    pub client_request_id: String,
    #[schema(
        format = Uuid,
        min_length = 36,
        max_length = 36,
        pattern = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    )]
    pub preflight_operation_id: String,
    #[schema(minimum = 1, maximum = 9_007_199_254_740_991_u64)]
    pub expected_operation_version: u64,
    #[schema(minimum = 0, maximum = 9_007_199_254_740_991_u64)]
    pub expected_review_generation: u64,
    #[schema(min_length = 64, max_length = 64, pattern = "^[0-9a-f]{64}$")]
    pub expected_workspace_fingerprint: String,
    #[schema(
        min_length = 12,
        max_length = 4_096,
        pattern = "^refs/heads/.{1,4085}$"
    )]
    pub target_branch: String,
    #[schema(
        min_length = 40,
        max_length = 64,
        pattern = "^([0-9a-f]{40}|[0-9a-f]{64})$"
    )]
    pub expected_target_head: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeliveryRemoveWorktreeRequest {
    #[schema(
        format = Uuid,
        min_length = 36,
        max_length = 36,
        pattern = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    )]
    pub client_request_id: String,
    #[schema(minimum = 1, maximum = 9_007_199_254_740_991_u64)]
    pub expected_disposition_version: u64,
    #[schema(
        format = Uuid,
        min_length = 36,
        max_length = 36,
        pattern = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    )]
    pub expected_merge_operation_id: String,
    #[schema(
        min_length = 12,
        max_length = 4_096,
        pattern = "^refs/heads/.{1,4085}$"
    )]
    pub expected_source_ref: String,
    #[schema(
        min_length = 40,
        max_length = 64,
        pattern = "^([0-9a-f]{40}|[0-9a-f]{64})$"
    )]
    pub expected_source_oid: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeliveryDeleteBranchRequest {
    #[schema(
        format = Uuid,
        min_length = 36,
        max_length = 36,
        pattern = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    )]
    pub client_request_id: String,
    #[schema(minimum = 1, maximum = 9_007_199_254_740_991_u64)]
    pub expected_disposition_version: u64,
    #[schema(
        format = Uuid,
        min_length = 36,
        max_length = 36,
        pattern = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    )]
    pub expected_merge_operation_id: String,
    #[schema(
        min_length = 12,
        max_length = 4_096,
        pattern = "^refs/heads/.{1,4085}$"
    )]
    pub expected_source_ref: String,
    #[schema(
        min_length = 40,
        max_length = 64,
        pattern = "^([0-9a-f]{40}|[0-9a-f]{64})$"
    )]
    pub expected_source_oid: String,
    #[schema(
        min_length = 12,
        max_length = 4_096,
        pattern = "^refs/heads/.{1,4085}$"
    )]
    pub target_branch: String,
    #[schema(
        min_length = 40,
        max_length = 64,
        pattern = "^([0-9a-f]{40}|[0-9a-f]{64})$"
    )]
    pub target_head: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryReceiptDispositionDto {
    Created,
    Existing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeliveryCommandResponse {
    pub receipt: DeliveryReceiptDispositionDto,
    pub operation: DeliveryOperationDto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryEligibilityDto {
    Eligible,
    Ineligible,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryEligibilityReasonDto {
    TaskNotCompleted,
    ReviewNotApproved,
    ApprovedEvidenceMissing,
    AttemptArtifactMissing,
    AttemptArtifactNotReady,
    TaskActive,
    ProcessCleanupUnproven,
    TargetBranchDetached,
    TargetBranchMismatch,
    TargetHeadChanged,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryAllowedActionDto {
    RunPreflight,
    AcceptMerge,
    RemoveWorktree,
    DeleteBranch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeliveryEvidenceSummaryDto {
    #[schema(minimum = 0, maximum = 9_007_199_254_740_991_u64)]
    pub review_generation: u64,
    #[schema(min_length = 64, max_length = 64, pattern = "^[0-9a-f]{64}$")]
    pub workspace_fingerprint: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryTargetUnavailableReasonDto {
    Detached,
    BranchMismatch,
    ObservationUnavailable,
    RepositoryBusy,
    RepositoryPoisoned,
    ServiceNotReady,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum DeliveryTargetObservationDto {
    Available(DeliveryAvailableTargetDto),
    Unavailable(DeliveryUnavailableTargetDto),
}

impl DeliveryTargetObservationDto {
    pub fn available(branch: String, head: String) -> Self {
        Self::Available(DeliveryAvailableTargetDto {
            available: true,
            branch,
            head,
        })
    }

    pub const fn unavailable(reason: DeliveryTargetUnavailableReasonDto) -> Self {
        Self::Unavailable(DeliveryUnavailableTargetDto {
            available: false,
            reason,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeliveryAvailableTargetDto {
    #[serde(deserialize_with = "deserialize_true")]
    available: bool,
    #[schema(
        min_length = 12,
        max_length = 4_096,
        pattern = "^refs/heads/.{1,4085}$"
    )]
    pub branch: String,
    #[schema(
        min_length = 40,
        max_length = 64,
        pattern = "^([0-9a-f]{40}|[0-9a-f]{64})$"
    )]
    pub head: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeliveryUnavailableTargetDto {
    #[serde(deserialize_with = "deserialize_false")]
    available: bool,
    pub reason: DeliveryTargetUnavailableReasonDto,
}

fn deserialize_true<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = bool::deserialize(deserializer)?;
    if value {
        Ok(true)
    } else {
        Err(serde::de::Error::custom("target availability must be true"))
    }
}

fn deserialize_false<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = bool::deserialize(deserializer)?;
    if value {
        Err(serde::de::Error::custom(
            "target availability must be false",
        ))
    } else {
        Ok(false)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeliverySourceStateDto {
    ObjectPending,
    CommitPending,
    Committed,
    ReconciliationRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeliverySourceDto {
    pub state: DeliverySourceStateDto,
    #[schema(minimum = 1, maximum = 9_007_199_254_740_991_u64)]
    pub version: u64,
    #[schema(
        min_length = 12,
        max_length = 4_096,
        pattern = "^refs/heads/.{1,4085}$"
    )]
    pub source_ref: String,
    #[schema(
        required = true,
        nullable = true,
        min_length = 40,
        max_length = 64,
        pattern = "^([0-9a-f]{40}|[0-9a-f]{64})$"
    )]
    pub source_oid: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryMergeStateDto {
    PreflightPending,
    PreflightReady,
    Accepted,
    MergePending,
    Merged,
    AbortPending,
    Conflict,
    Rejected,
    Stale,
    Superseded,
    Failed,
    ReconciliationRequired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryConflictPathEncodingDto {
    Utf8,
    Base64url,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeliveryConflictPathDto {
    pub encoding: DeliveryConflictPathEncodingDto,
    #[schema(min_length = 1, max_length = 4_096)]
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeliveryConflictSummaryDto {
    #[schema(minimum = 0, maximum = 128)]
    pub path_count: u32,
    #[schema(max_items = 128)]
    pub paths: Vec<DeliveryConflictPathDto>,
    #[schema(minimum = 0, maximum = 65_536)]
    pub payload_bytes: u32,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeliveryOperationFailureDto {
    #[schema(min_length = 1, max_length = 128, pattern = "^[A-Z][A-Z0-9_]*$")]
    pub code: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeliveryMergeOperationDto {
    #[schema(value_type = uuid::Uuid)]
    pub operation_id: Uuid,
    #[schema(minimum = 1, maximum = 9_007_199_254_740_991_u64)]
    pub version: u64,
    pub state: DeliveryMergeStateDto,
    #[schema(minimum = 0, maximum = 9_007_199_254_740_991_u64)]
    pub review_generation: u64,
    #[schema(min_length = 64, max_length = 64, pattern = "^[0-9a-f]{64}$")]
    pub workspace_fingerprint: String,
    #[schema(
        required = true,
        nullable = true,
        min_length = 40,
        max_length = 64,
        pattern = "^([0-9a-f]{40}|[0-9a-f]{64})$"
    )]
    pub candidate_source_tree: Option<String>,
    #[schema(
        required = true,
        nullable = true,
        min_length = 40,
        max_length = 64,
        pattern = "^([0-9a-f]{40}|[0-9a-f]{64})$"
    )]
    pub preflight_source_commit: Option<String>,
    #[schema(
        required = true,
        nullable = true,
        min_length = 40,
        max_length = 64,
        pattern = "^([0-9a-f]{40}|[0-9a-f]{64})$"
    )]
    pub source_commit: Option<String>,
    #[schema(
        min_length = 12,
        max_length = 4_096,
        pattern = "^refs/heads/.{1,4085}$"
    )]
    pub target_branch: String,
    #[schema(
        min_length = 40,
        max_length = 64,
        pattern = "^([0-9a-f]{40}|[0-9a-f]{64})$"
    )]
    pub target_head: String,
    #[schema(required = true, nullable = true)]
    pub conflicts: Option<DeliveryConflictSummaryDto>,
    #[schema(required = true, nullable = true)]
    pub failure: Option<DeliveryOperationFailureDto>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryCleanupKindDto {
    RemoveWorktree,
    DeleteBranch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryCleanupStateDto {
    UnlockPending,
    UnlockedPendingRemove,
    RemovePending,
    DeletePending,
    Completed,
    Failed,
    ReconciliationRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeliveryCleanupOperationDto {
    #[schema(value_type = uuid::Uuid)]
    pub operation_id: Uuid,
    pub cleanup_kind: DeliveryCleanupKindDto,
    #[schema(minimum = 1, maximum = 9_007_199_254_740_991_u64)]
    pub version: u64,
    pub state: DeliveryCleanupStateDto,
    #[schema(minimum = 1, maximum = 9_007_199_254_740_991_u64)]
    pub expected_disposition_version: u64,
    #[schema(value_type = uuid::Uuid)]
    pub expected_merge_operation_id: Uuid,
    #[schema(
        min_length = 12,
        max_length = 4_096,
        pattern = "^refs/heads/.{1,4085}$"
    )]
    pub expected_source_ref: String,
    #[schema(
        min_length = 40,
        max_length = 64,
        pattern = "^([0-9a-f]{40}|[0-9a-f]{64})$"
    )]
    pub expected_source_oid: String,
    #[schema(
        required = true,
        nullable = true,
        min_length = 12,
        max_length = 4_096,
        pattern = "^refs/heads/.{1,4085}$"
    )]
    pub target_branch: Option<String>,
    #[schema(
        required = true,
        nullable = true,
        min_length = 40,
        max_length = 64,
        pattern = "^([0-9a-f]{40}|[0-9a-f]{64})$"
    )]
    pub target_head: Option<String>,
    #[schema(required = true, nullable = true)]
    pub failure: Option<DeliveryOperationFailureDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DeliveryOperationDto {
    Merge(DeliveryMergeOperationDto),
    Cleanup(DeliveryCleanupOperationDto),
}

impl PartialSchema for DeliveryOperationDto {
    fn schema() -> utoipa::openapi::RefOr<Schema> {
        OneOfBuilder::new()
            .item(Ref::from_schema_name("DeliveryMergeOperationEnvelopeDto"))
            .item(Ref::from_schema_name("DeliveryCleanupOperationEnvelopeDto"))
            .discriminator(Some(Discriminator::with_mapping(
                "kind",
                [
                    (
                        "merge",
                        "#/components/schemas/DeliveryMergeOperationEnvelopeDto",
                    ),
                    (
                        "cleanup",
                        "#/components/schemas/DeliveryCleanupOperationEnvelopeDto",
                    ),
                ],
            )))
            .into()
    }
}

impl ToSchema for DeliveryOperationDto {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub enum DeliveryMergeOperationKindDto {
    #[serde(rename = "merge")]
    Merge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub enum DeliveryCleanupOperationKindDto {
    #[serde(rename = "cleanup")]
    Cleanup,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct DeliveryMergeOperationEnvelopeDto {
    pub kind: DeliveryMergeOperationKindDto,
    #[serde(flatten)]
    pub operation: DeliveryMergeOperationDto,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct DeliveryCleanupOperationEnvelopeDto {
    pub kind: DeliveryCleanupOperationKindDto,
    #[serde(flatten)]
    pub operation: DeliveryCleanupOperationDto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryWorktreeDispositionStateDto {
    RetainedLocked,
    RetainedUnlocked,
    Removed,
    ReconciliationRequired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryBranchDispositionStateDto {
    Retained,
    Deleted,
    ReconciliationRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeliveryWorktreeDispositionDto {
    pub state: DeliveryWorktreeDispositionStateDto,
    #[schema(minimum = 1, maximum = 9_007_199_254_740_991_u64)]
    pub version: u64,
    #[schema(required = true, nullable = true)]
    pub failure: Option<DeliveryOperationFailureDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeliveryBranchDispositionDto {
    pub state: DeliveryBranchDispositionStateDto,
    #[schema(minimum = 1, maximum = 9_007_199_254_740_991_u64)]
    pub version: u64,
    #[schema(required = true, nullable = true)]
    pub failure: Option<DeliveryOperationFailureDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeliveryArtifactDispositionDto {
    #[schema(value_type = uuid::Uuid)]
    pub merged_operation_id: Uuid,
    #[schema(
        min_length = 12,
        max_length = 4_096,
        pattern = "^refs/heads/.{1,4085}$"
    )]
    pub source_ref: String,
    #[schema(
        min_length = 40,
        max_length = 64,
        pattern = "^([0-9a-f]{40}|[0-9a-f]{64})$"
    )]
    pub source_oid: String,
    pub worktree: DeliveryWorktreeDispositionDto,
    pub branch: DeliveryBranchDispositionDto,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DeliveryTaskDto {
    #[schema(value_type = uuid::Uuid)]
    pub task_id: Uuid,
    pub eligibility: DeliveryEligibilityDto,
    #[schema(max_items = 32)]
    pub reasons: Vec<DeliveryEligibilityReasonDto>,
    #[schema(required = true, nullable = true)]
    pub evidence: Option<DeliveryEvidenceSummaryDto>,
    pub target: DeliveryTargetObservationDto,
    #[schema(required = true, nullable = true)]
    pub source: Option<DeliverySourceDto>,
    #[schema(required = true, nullable = true)]
    pub latest_merge: Option<DeliveryMergeOperationDto>,
    #[schema(required = true, nullable = true)]
    pub latest_cleanup: Option<DeliveryCleanupOperationDto>,
    #[schema(required = true, nullable = true)]
    pub disposition: Option<DeliveryArtifactDispositionDto>,
    #[schema(max_items = 4)]
    pub allowed_actions: Vec<DeliveryAllowedActionDto>,
}
