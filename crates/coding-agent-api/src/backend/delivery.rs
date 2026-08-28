use coding_agent_domain::TaskId;
use uuid::Uuid;

use crate::{
    ApiError, ApiResult, AuthContext, DeliveryCommandResponse, DeliveryOperationDto,
    DeliveryTaskDto, ValidatedDeliveryDeleteBranchCommand, ValidatedDeliveryMergeCommand,
    ValidatedDeliveryPreflightCommand, ValidatedDeliveryRemoveWorktreeCommand,
};

/// Application-owned delivery port.
///
/// The API validates wire shape and bounds only. Implementations must pass these typed commands
/// to the Store-owned canonical receipt/hash implementation and return after durable acceptance;
/// routers never acquire repository leases, execute Git, or wait for background cleanup.
#[async_trait::async_trait]
pub trait DeliveryBackend: Send + Sync + 'static {
    async fn task_delivery(
        &self,
        auth: &AuthContext,
        task_id: TaskId,
    ) -> ApiResult<DeliveryTaskDto>;

    async fn delivery_operation(
        &self,
        auth: &AuthContext,
        operation_id: Uuid,
    ) -> ApiResult<DeliveryOperationDto>;

    async fn preflight(
        &self,
        auth: &AuthContext,
        command: ValidatedDeliveryPreflightCommand,
    ) -> ApiResult<DeliveryCommandResponse>;

    async fn accept_merge(
        &self,
        auth: &AuthContext,
        command: ValidatedDeliveryMergeCommand,
    ) -> ApiResult<DeliveryCommandResponse>;

    async fn remove_worktree(
        &self,
        auth: &AuthContext,
        command: ValidatedDeliveryRemoveWorktreeCommand,
    ) -> ApiResult<DeliveryCommandResponse>;

    async fn delete_branch(
        &self,
        auth: &AuthContext,
        command: ValidatedDeliveryDeleteBranchCommand,
    ) -> ApiResult<DeliveryCommandResponse>;
}

pub(crate) struct UnavailableDeliveryBackend;

#[async_trait::async_trait]
impl DeliveryBackend for UnavailableDeliveryBackend {
    async fn task_delivery(&self, _: &AuthContext, _: TaskId) -> ApiResult<DeliveryTaskDto> {
        Err(ApiError::delivery_backend_unavailable())
    }

    async fn delivery_operation(
        &self,
        _: &AuthContext,
        _: Uuid,
    ) -> ApiResult<DeliveryOperationDto> {
        Err(ApiError::delivery_backend_unavailable())
    }

    async fn preflight(
        &self,
        _: &AuthContext,
        _: ValidatedDeliveryPreflightCommand,
    ) -> ApiResult<DeliveryCommandResponse> {
        Err(ApiError::delivery_backend_unavailable())
    }

    async fn accept_merge(
        &self,
        _: &AuthContext,
        _: ValidatedDeliveryMergeCommand,
    ) -> ApiResult<DeliveryCommandResponse> {
        Err(ApiError::delivery_backend_unavailable())
    }

    async fn remove_worktree(
        &self,
        _: &AuthContext,
        _: ValidatedDeliveryRemoveWorktreeCommand,
    ) -> ApiResult<DeliveryCommandResponse> {
        Err(ApiError::delivery_backend_unavailable())
    }

    async fn delete_branch(
        &self,
        _: &AuthContext,
        _: ValidatedDeliveryDeleteBranchCommand,
    ) -> ApiResult<DeliveryCommandResponse> {
        Err(ApiError::delivery_backend_unavailable())
    }
}
