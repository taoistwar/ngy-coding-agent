use std::str::FromStr;
use std::sync::Arc;

use coding_agent_api::{
    ApiResult, AuthContext, DeliveryBackend, DeliveryCommandResponse, DeliveryOperationDto,
    DeliveryTaskDto, RequestSecurity, ValidatedDeliveryDeleteBranchCommand,
    ValidatedDeliveryMergeCommand, ValidatedDeliveryPreflightCommand,
    ValidatedDeliveryRemoveWorktreeCommand,
};
use coding_agent_domain::{ClientRequestId, TaskId};
use coding_agent_store::{
    AcceptMergeCommandRequest, DeleteBranchCommandRequest, DeliveryOperationId, DeliveryVersion,
    PreflightCommandRequest, RemoveWorktreeCommandRequest,
};
use uuid::Uuid;

use crate::{
    DeliveryAcceptRequest, DeliveryCleanupAcceptanceOutcome, DeliveryDeleteBranchRequest,
    DeliveryManagerHandle, DeliveryMergeAcceptanceOutcome, DeliveryOperationQueryOutcome,
    DeliveryPreflightOutcome, DeliveryRemoveWorktreeRequest,
};

use super::{ApplicationBackend, MutationGate};

mod error;
mod projection;

/// Narrow application adapter between the HTTP delivery port and the bounded
/// DeliveryManager actor. It deliberately owns no Store, repository-control,
/// runtime, path, or Git capability.
pub struct ApplicationDeliveryBackend {
    manager: DeliveryManagerHandle,
    mutation_gate: MutationGate,
}

impl ApplicationDeliveryBackend {
    pub fn new(manager: DeliveryManagerHandle, mutation_gate: MutationGate) -> Self {
        Self {
            manager,
            mutation_gate,
        }
    }

    async fn operation_after_durable_acceptance(
        &self,
        operation_id: DeliveryOperationId,
    ) -> ApiResult<DeliveryOperationDto> {
        let outcome = self
            .manager
            .query_operation(operation_id)
            .await
            .map_err(error::manager_error)?;
        match outcome {
            DeliveryOperationQueryOutcome::Found { operation } => {
                projection::operation_projection(&operation)
            }
            DeliveryOperationQueryOutcome::NotFound { .. } => Err(error::invalid_projection()),
            DeliveryOperationQueryOutcome::Unavailable { reason, .. } => {
                Err(error::query_unavailable(reason))
            }
        }
    }

    async fn preflight_response(
        &self,
        command: ValidatedDeliveryPreflightCommand,
    ) -> ApiResult<DeliveryCommandResponse> {
        let request = crate::DeliveryPreflightRequest::new(preflight_command(command)?);
        match self
            .manager
            .preflight(request)
            .await
            .map_err(error::manager_error)?
        {
            DeliveryPreflightOutcome::Durable(operation) => {
                let dto = self
                    .operation_after_durable_acceptance(operation.operation_id())
                    .await?;
                Ok(DeliveryCommandResponse {
                    receipt: projection::preflight_receipt(operation.durability()),
                    operation: dto,
                })
            }
            DeliveryPreflightOutcome::KnownNotAppliedPersisted(retry) => {
                let operation = retry.operation();
                let dto = self
                    .operation_after_durable_acceptance(operation.operation_id())
                    .await?;
                Ok(DeliveryCommandResponse {
                    receipt: projection::preflight_receipt(operation.durability()),
                    operation: dto,
                })
            }
            DeliveryPreflightOutcome::Ineligible(reasons) => Err(error::ineligible(&reasons)),
            DeliveryPreflightOutcome::Conflict(conflict) => Err(error::conflict(conflict)),
            DeliveryPreflightOutcome::Busy(reason) => Err(error::busy(reason)),
            DeliveryPreflightOutcome::Unavailable(reason) => Err(error::unavailable(reason)),
        }
    }

    async fn merge_response(
        &self,
        command: ValidatedDeliveryMergeCommand,
    ) -> ApiResult<DeliveryCommandResponse> {
        let request = DeliveryAcceptRequest::new(accept_command(command)?);
        match self
            .manager
            .accept_merge(request)
            .await
            .map_err(error::manager_error)?
        {
            DeliveryMergeAcceptanceOutcome::Durable(acceptance) => {
                let dto = self
                    .operation_after_durable_acceptance(acceptance.operation_id())
                    .await?;
                Ok(DeliveryCommandResponse {
                    receipt: projection::merge_receipt(acceptance.receipt()),
                    operation: dto,
                })
            }
            DeliveryMergeAcceptanceOutcome::Ineligible(reasons) => Err(error::ineligible(&reasons)),
            DeliveryMergeAcceptanceOutcome::Conflict(conflict) => Err(error::conflict(conflict)),
            DeliveryMergeAcceptanceOutcome::Busy(reason) => Err(error::busy(reason)),
            DeliveryMergeAcceptanceOutcome::Unavailable(reason) => Err(error::unavailable(reason)),
        }
    }

    async fn cleanup_response(
        &self,
        outcome: DeliveryCleanupAcceptanceOutcome,
    ) -> ApiResult<DeliveryCommandResponse> {
        match outcome {
            DeliveryCleanupAcceptanceOutcome::Durable(acceptance) => {
                let dto = self
                    .operation_after_durable_acceptance(acceptance.operation_id())
                    .await?;
                Ok(DeliveryCommandResponse {
                    receipt: projection::cleanup_receipt(acceptance.receipt()),
                    operation: dto,
                })
            }
            DeliveryCleanupAcceptanceOutcome::Ineligible(reasons) => {
                Err(error::cleanup_ineligible(&reasons))
            }
            DeliveryCleanupAcceptanceOutcome::Conflict(conflict) => Err(error::conflict(conflict)),
            DeliveryCleanupAcceptanceOutcome::Busy(reason) => Err(error::busy(reason)),
            DeliveryCleanupAcceptanceOutcome::Unavailable(reason) => {
                Err(error::unavailable(reason))
            }
        }
    }
}

#[async_trait::async_trait]
impl DeliveryBackend for ApplicationDeliveryBackend {
    async fn task_delivery(&self, _: &AuthContext, task_id: TaskId) -> ApiResult<DeliveryTaskDto> {
        projection::task_query(
            self.manager
                .query(task_id)
                .await
                .map_err(error::manager_error)?,
        )
    }

    async fn delivery_operation(
        &self,
        _: &AuthContext,
        operation_id: Uuid,
    ) -> ApiResult<DeliveryOperationDto> {
        let operation_id = DeliveryOperationId::from_str(&operation_id.hyphenated().to_string())
            .map_err(|_| error::invalid_validated_command())?;
        projection::operation_query(
            self.manager
                .query_operation(operation_id)
                .await
                .map_err(error::manager_error)?,
        )
    }

    async fn preflight(
        &self,
        _: &AuthContext,
        command: ValidatedDeliveryPreflightCommand,
    ) -> ApiResult<DeliveryCommandResponse> {
        self.mutation_gate
            .run_data_mutation(self.preflight_response(command))
            .await
    }

    async fn accept_merge(
        &self,
        _: &AuthContext,
        command: ValidatedDeliveryMergeCommand,
    ) -> ApiResult<DeliveryCommandResponse> {
        self.mutation_gate
            .run_data_mutation(self.merge_response(command))
            .await
    }

    async fn remove_worktree(
        &self,
        _: &AuthContext,
        command: ValidatedDeliveryRemoveWorktreeCommand,
    ) -> ApiResult<DeliveryCommandResponse> {
        self.mutation_gate
            .run_data_mutation(async {
                let request = DeliveryRemoveWorktreeRequest::new(remove_worktree_command(command)?);
                let outcome = self
                    .manager
                    .remove_worktree(request)
                    .await
                    .map_err(error::manager_error)?;
                self.cleanup_response(outcome).await
            })
            .await
    }

    async fn delete_branch(
        &self,
        _: &AuthContext,
        command: ValidatedDeliveryDeleteBranchCommand,
    ) -> ApiResult<DeliveryCommandResponse> {
        self.mutation_gate
            .run_data_mutation(async {
                let request = DeliveryDeleteBranchRequest::new(delete_branch_command(command)?);
                let outcome = self
                    .manager
                    .delete_branch(request)
                    .await
                    .map_err(error::manager_error)?;
                self.cleanup_response(outcome).await
            })
            .await
    }
}

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub fn map_delivery_command_conflict_for_test(
    conflict: crate::DeliveryCommandConflict,
) -> coding_agent_api::ApiError {
    error::conflict(conflict)
}

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub fn map_delivery_eligibility_for_test(
    reason: crate::DeliveryEligibilityReason,
) -> coding_agent_api::ApiError {
    error::ineligible(&[reason])
}

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub fn map_delivery_cleanup_eligibility_for_test(
    reason: crate::DeliveryEligibilityReason,
) -> coding_agent_api::ApiError {
    error::cleanup_ineligible(&[reason])
}

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub fn map_delivery_busy_for_test(
    reason: crate::DeliveryPreflightBusyReason,
) -> coding_agent_api::ApiError {
    error::busy(reason)
}

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub fn map_delivery_unavailable_for_test(
    reason: crate::DeliveryPreflightUnavailableReason,
) -> coding_agent_api::ApiError {
    error::unavailable(reason)
}

/// Production composition boundary used by primary startup. DeliveryManager is
/// spawned and recovered before this function is called; the HTTP layer only
/// receives its cloneable command handle.
pub fn build_application_api_router_with_delivery(
    backend: Arc<ApplicationBackend>,
    security: Arc<dyn RequestSecurity>,
    manager: DeliveryManagerHandle,
) -> axum::Router {
    let delivery = Arc::new(ApplicationDeliveryBackend::new(
        manager,
        backend.mutation_gate().clone(),
    ));
    coding_agent_api::build_api_router_with_delivery(backend.clone(), security, backend, delivery)
}

fn preflight_command(
    command: ValidatedDeliveryPreflightCommand,
) -> ApiResult<PreflightCommandRequest> {
    PreflightCommandRequest::try_new(
        client_request_id(command.client_request_id())?,
        command.task_id(),
        parse(command.target_branch())?,
        parse(command.expected_target_head())?,
    )
    .map_err(|_| error::invalid_validated_command())
}

fn accept_command(command: ValidatedDeliveryMergeCommand) -> ApiResult<AcceptMergeCommandRequest> {
    AcceptMergeCommandRequest::try_new(
        client_request_id(command.client_request_id())?,
        command.task_id(),
        operation_id(command.preflight_operation_id())?,
        DeliveryVersion::try_new(command.expected_operation_version())
            .map_err(|_| error::invalid_validated_command())?,
        command.expected_review_generation(),
        parse(command.expected_workspace_fingerprint())?,
        parse(command.target_branch())?,
        parse(command.expected_target_head())?,
    )
    .map_err(|_| error::invalid_validated_command())
}

fn remove_worktree_command(
    command: ValidatedDeliveryRemoveWorktreeCommand,
) -> ApiResult<RemoveWorktreeCommandRequest> {
    RemoveWorktreeCommandRequest::try_new(
        client_request_id(command.client_request_id())?,
        command.task_id(),
        DeliveryVersion::try_new(command.expected_disposition_version())
            .map_err(|_| error::invalid_validated_command())?,
        operation_id(command.expected_merge_operation_id())?,
        parse(command.expected_source_ref())?,
        parse(command.expected_source_oid())?,
    )
    .map_err(|_| error::invalid_validated_command())
}

fn delete_branch_command(
    command: ValidatedDeliveryDeleteBranchCommand,
) -> ApiResult<DeleteBranchCommandRequest> {
    DeleteBranchCommandRequest::try_new(
        client_request_id(command.client_request_id())?,
        command.task_id(),
        DeliveryVersion::try_new(command.expected_disposition_version())
            .map_err(|_| error::invalid_validated_command())?,
        operation_id(command.expected_merge_operation_id())?,
        parse(command.expected_source_ref())?,
        parse(command.expected_source_oid())?,
        parse(command.target_branch())?,
        parse(command.target_head())?,
    )
    .map_err(|_| error::invalid_validated_command())
}

fn client_request_id(value: Uuid) -> ApiResult<ClientRequestId> {
    value
        .hyphenated()
        .to_string()
        .parse()
        .map_err(|_| error::invalid_validated_command())
}

fn operation_id(value: Uuid) -> ApiResult<DeliveryOperationId> {
    value
        .hyphenated()
        .to_string()
        .parse()
        .map_err(|_| error::invalid_validated_command())
}

fn parse<T>(value: &str) -> ApiResult<T>
where
    T: FromStr,
{
    value
        .parse()
        .map_err(|_| error::invalid_validated_command())
}
