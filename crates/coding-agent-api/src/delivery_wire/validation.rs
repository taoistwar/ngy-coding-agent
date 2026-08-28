use coding_agent_domain::TaskId;
use uuid::Uuid;

use crate::{
    ApiError, DeliveryDeleteBranchRequest, DeliveryMergeRequest, DeliveryPreflightRequest,
    DeliveryRemoveWorktreeRequest,
};

const JS_SAFE_INTEGER_MAX: u64 = 9_007_199_254_740_991;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedDeliveryPreflightCommand {
    task_id: TaskId,
    client_request_id: Uuid,
    target_branch: String,
    expected_target_head: String,
}

impl ValidatedDeliveryPreflightCommand {
    pub fn try_new(task_id: TaskId, request: DeliveryPreflightRequest) -> Result<Self, ApiError> {
        Ok(Self {
            task_id,
            client_request_id: canonical_uuid(&request.client_request_id)?,
            target_branch: local_branch_ref(request.target_branch)?,
            expected_target_head: git_oid(request.expected_target_head)?,
        })
    }

    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub const fn client_request_id(&self) -> Uuid {
        self.client_request_id
    }

    pub fn target_branch(&self) -> &str {
        &self.target_branch
    }

    pub fn expected_target_head(&self) -> &str {
        &self.expected_target_head
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedDeliveryMergeCommand {
    task_id: TaskId,
    client_request_id: Uuid,
    preflight_operation_id: Uuid,
    expected_operation_version: u64,
    expected_review_generation: u64,
    expected_workspace_fingerprint: String,
    target_branch: String,
    expected_target_head: String,
}

impl ValidatedDeliveryMergeCommand {
    pub fn try_new(task_id: TaskId, request: DeliveryMergeRequest) -> Result<Self, ApiError> {
        Ok(Self {
            task_id,
            client_request_id: canonical_uuid(&request.client_request_id)?,
            preflight_operation_id: canonical_uuid(&request.preflight_operation_id)?,
            expected_operation_version: version(request.expected_operation_version)?,
            expected_review_generation: generation(request.expected_review_generation)?,
            expected_workspace_fingerprint: sha256(request.expected_workspace_fingerprint)?,
            target_branch: local_branch_ref(request.target_branch)?,
            expected_target_head: git_oid(request.expected_target_head)?,
        })
    }

    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub const fn client_request_id(&self) -> Uuid {
        self.client_request_id
    }

    pub const fn preflight_operation_id(&self) -> Uuid {
        self.preflight_operation_id
    }

    pub const fn expected_operation_version(&self) -> u64 {
        self.expected_operation_version
    }

    pub const fn expected_review_generation(&self) -> u64 {
        self.expected_review_generation
    }

    pub fn expected_workspace_fingerprint(&self) -> &str {
        &self.expected_workspace_fingerprint
    }

    pub fn target_branch(&self) -> &str {
        &self.target_branch
    }

    pub fn expected_target_head(&self) -> &str {
        &self.expected_target_head
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedDeliveryRemoveWorktreeCommand {
    task_id: TaskId,
    client_request_id: Uuid,
    expected_disposition_version: u64,
    expected_merge_operation_id: Uuid,
    expected_source_ref: String,
    expected_source_oid: String,
}

impl ValidatedDeliveryRemoveWorktreeCommand {
    pub fn try_new(
        task_id: TaskId,
        request: DeliveryRemoveWorktreeRequest,
    ) -> Result<Self, ApiError> {
        Ok(Self {
            task_id,
            client_request_id: canonical_uuid(&request.client_request_id)?,
            expected_disposition_version: version(request.expected_disposition_version)?,
            expected_merge_operation_id: canonical_uuid(&request.expected_merge_operation_id)?,
            expected_source_ref: local_branch_ref(request.expected_source_ref)?,
            expected_source_oid: git_oid(request.expected_source_oid)?,
        })
    }

    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub const fn client_request_id(&self) -> Uuid {
        self.client_request_id
    }

    pub const fn expected_disposition_version(&self) -> u64 {
        self.expected_disposition_version
    }

    pub const fn expected_merge_operation_id(&self) -> Uuid {
        self.expected_merge_operation_id
    }

    pub fn expected_source_ref(&self) -> &str {
        &self.expected_source_ref
    }

    pub fn expected_source_oid(&self) -> &str {
        &self.expected_source_oid
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedDeliveryDeleteBranchCommand {
    task_id: TaskId,
    client_request_id: Uuid,
    expected_disposition_version: u64,
    expected_merge_operation_id: Uuid,
    expected_source_ref: String,
    expected_source_oid: String,
    target_branch: String,
    target_head: String,
}

impl ValidatedDeliveryDeleteBranchCommand {
    pub fn try_new(
        task_id: TaskId,
        request: DeliveryDeleteBranchRequest,
    ) -> Result<Self, ApiError> {
        let expected_source_oid = git_oid(request.expected_source_oid)?;
        let target_head = git_oid(request.target_head)?;
        if expected_source_oid.len() != target_head.len() {
            return Err(ApiError::invalid_delivery_request());
        }
        Ok(Self {
            task_id,
            client_request_id: canonical_uuid(&request.client_request_id)?,
            expected_disposition_version: version(request.expected_disposition_version)?,
            expected_merge_operation_id: canonical_uuid(&request.expected_merge_operation_id)?,
            expected_source_ref: local_branch_ref(request.expected_source_ref)?,
            expected_source_oid,
            target_branch: local_branch_ref(request.target_branch)?,
            target_head,
        })
    }

    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub const fn client_request_id(&self) -> Uuid {
        self.client_request_id
    }

    pub const fn expected_disposition_version(&self) -> u64 {
        self.expected_disposition_version
    }

    pub const fn expected_merge_operation_id(&self) -> Uuid {
        self.expected_merge_operation_id
    }

    pub fn expected_source_ref(&self) -> &str {
        &self.expected_source_ref
    }

    pub fn expected_source_oid(&self) -> &str {
        &self.expected_source_oid
    }

    pub fn target_branch(&self) -> &str {
        &self.target_branch
    }

    pub fn target_head(&self) -> &str {
        &self.target_head
    }
}

fn canonical_uuid(value: &str) -> Result<Uuid, ApiError> {
    let parsed = Uuid::parse_str(value).map_err(|_| ApiError::invalid_delivery_request())?;
    if parsed.is_nil() || parsed.hyphenated().to_string() != value {
        return Err(ApiError::invalid_delivery_request());
    }
    Ok(parsed)
}

fn version(value: u64) -> Result<u64, ApiError> {
    if value == 0 || value > JS_SAFE_INTEGER_MAX {
        Err(ApiError::invalid_delivery_request())
    } else {
        Ok(value)
    }
}

fn generation(value: u64) -> Result<u64, ApiError> {
    if value > JS_SAFE_INTEGER_MAX {
        Err(ApiError::invalid_delivery_request())
    } else {
        Ok(value)
    }
}

fn sha256(value: String) -> Result<String, ApiError> {
    if value.len() == 64 && is_lower_hex(&value) {
        Ok(value)
    } else {
        Err(ApiError::invalid_delivery_request())
    }
}

fn git_oid(value: String) -> Result<String, ApiError> {
    if matches!(value.len(), 40 | 64)
        && is_lower_hex(&value)
        && value.as_bytes().iter().any(|byte| *byte != b'0')
    {
        Ok(value)
    } else {
        Err(ApiError::invalid_delivery_request())
    }
}

fn is_lower_hex(value: &str) -> bool {
    value
        .as_bytes()
        .iter()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
}

fn local_branch_ref(value: String) -> Result<String, ApiError> {
    let Some(short) = value.strip_prefix("refs/heads/") else {
        return Err(ApiError::invalid_delivery_request());
    };
    if short.is_empty()
        || value.len() > 4_096
        || short.starts_with('-')
        || value.ends_with('/')
        || value.ends_with('.')
        || value.contains("..")
        || value.contains("@{")
        || value.contains("//")
        || value.chars().any(|character| {
            character.is_control()
                || character == ' '
                || matches!(character, '~' | '^' | ':' | '?' | '*' | '[' | '\\')
        })
        || short.split('/').any(|component| {
            component.is_empty() || component.starts_with('.') || component.ends_with(".lock")
        })
    {
        Err(ApiError::invalid_delivery_request())
    } else {
        Ok(value)
    }
}
