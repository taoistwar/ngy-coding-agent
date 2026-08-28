use axum::Json;
use axum::body::{Body, to_bytes};
use axum::extract::{Extension, Request, State};
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::{IntoResponse, Response};
use coding_agent_domain::TaskId;
use serde::de::DeserializeOwned;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;
use uuid::Uuid;

use crate::{
    ApiResult, DeliveryCommandResponse, DeliveryDeleteBranchRequest, DeliveryMergeRequest,
    DeliveryPreflightRequest, DeliveryReceiptDispositionDto, DeliveryRemoveWorktreeRequest,
    ValidatedDeliveryDeleteBranchCommand, ValidatedDeliveryMergeCommand,
    ValidatedDeliveryPreflightCommand, ValidatedDeliveryRemoveWorktreeCommand,
};

use super::{ApiState, RequestId, decode_json, finish, invalid_path_parameter, transport_error};

const MAX_DELIVERY_REQUEST_BODY_BYTES: usize = 65_536;

pub(super) fn add_routes(router: OpenApiRouter<ApiState>) -> OpenApiRouter<ApiState> {
    router
        .routes(routes!(task_delivery))
        .routes(routes!(delivery_operation))
        .routes(routes!(preflight))
        .routes(routes!(accept_merge))
        .routes(routes!(remove_worktree))
        .routes(routes!(delete_branch))
}

#[utoipa::path(
    get,
    path = "/api/tasks/{task_id}/delivery",
    params(("task_id" = uuid::Uuid, Path, description = "Task ID")),
    responses(
        (status = 200, body = crate::DeliveryTaskDto),
        (status = 400, body = crate::ApiErrorResponse),
        (status = 401, body = crate::ApiErrorResponse),
        (status = 403, body = crate::ApiErrorResponse),
        (status = 404, body = crate::ApiErrorResponse),
        (status = 500, body = crate::ApiErrorResponse),
        (status = 503, body = crate::ApiErrorResponse)
    )
)]
async fn task_delivery(
    State(state): State<ApiState>,
    Extension(request_id): Extension<RequestId>,
    request: Request,
) -> Response {
    let (parts, _) = request.into_parts();
    let result = async {
        let auth = state.security.authorize_read(&parts)?;
        let task_id = canonical_task_id_at(&parts, 3)?;
        Ok(Json(state.delivery.task_delivery(&auth, task_id).await?).into_response())
    }
    .await;
    finish(result, &request_id)
}

#[utoipa::path(
    get,
    path = "/api/delivery-operations/{operation_id}",
    params(("operation_id" = uuid::Uuid, Path, description = "Delivery operation ID")),
    responses(
        (status = 200, body = crate::DeliveryOperationDto),
        (status = 400, body = crate::ApiErrorResponse),
        (status = 401, body = crate::ApiErrorResponse),
        (status = 403, body = crate::ApiErrorResponse),
        (status = 404, body = crate::ApiErrorResponse),
        (status = 500, body = crate::ApiErrorResponse),
        (status = 503, body = crate::ApiErrorResponse)
    )
)]
async fn delivery_operation(
    State(state): State<ApiState>,
    Extension(request_id): Extension<RequestId>,
    request: Request,
) -> Response {
    let (parts, _) = request.into_parts();
    let result = async {
        let auth = state.security.authorize_read(&parts)?;
        let operation_id = canonical_uuid_at(&parts, 3)?;
        Ok(Json(
            state
                .delivery
                .delivery_operation(&auth, operation_id)
                .await?,
        )
        .into_response())
    }
    .await;
    finish(result, &request_id)
}

#[utoipa::path(
    post,
    path = "/api/tasks/{task_id}/merge/preflight",
    params(("task_id" = uuid::Uuid, Path, description = "Task ID")),
    request_body(content = crate::DeliveryPreflightRequest, content_type = "application/json"),
    responses(
        (status = 200, body = crate::DeliveryCommandResponse),
        (status = 201, body = crate::DeliveryCommandResponse),
        (status = 400, body = crate::ApiErrorResponse),
        (status = 401, body = crate::ApiErrorResponse),
        (status = 403, body = crate::ApiErrorResponse),
        (status = 404, body = crate::ApiErrorResponse),
        (status = 409, body = crate::ApiErrorResponse),
        (status = 415, body = crate::ApiErrorResponse),
        (status = 422, body = crate::ApiErrorResponse),
        (status = 500, body = crate::ApiErrorResponse),
        (status = 503, body = crate::ApiErrorResponse),
        (status = 504, body = crate::ApiErrorResponse)
    )
)]
async fn preflight(
    State(state): State<ApiState>,
    Extension(request_id): Extension<RequestId>,
    request: Request,
) -> Response {
    let (parts, body) = request.into_parts();
    let result = async {
        let auth = state.security.authorize_mutation(&parts)?;
        let task_id = canonical_task_id_at(&parts, 3)?;
        let input = decode_delivery_json::<DeliveryPreflightRequest>(parts, body).await?;
        let command = ValidatedDeliveryPreflightCommand::try_new(task_id, input)?;
        Ok(command_response(
            state.delivery.preflight(&auth, command).await?,
            StatusCode::CREATED,
        ))
    }
    .await;
    finish(result, &request_id)
}

#[utoipa::path(
    post,
    path = "/api/tasks/{task_id}/merge",
    params(("task_id" = uuid::Uuid, Path, description = "Task ID")),
    request_body(content = crate::DeliveryMergeRequest, content_type = "application/json"),
    responses(
        (status = 200, body = crate::DeliveryCommandResponse),
        (status = 202, body = crate::DeliveryCommandResponse),
        (status = 400, body = crate::ApiErrorResponse),
        (status = 401, body = crate::ApiErrorResponse),
        (status = 403, body = crate::ApiErrorResponse),
        (status = 404, body = crate::ApiErrorResponse),
        (status = 409, body = crate::ApiErrorResponse),
        (status = 415, body = crate::ApiErrorResponse),
        (status = 422, body = crate::ApiErrorResponse),
        (status = 500, body = crate::ApiErrorResponse),
        (status = 503, body = crate::ApiErrorResponse),
        (status = 504, body = crate::ApiErrorResponse)
    )
)]
async fn accept_merge(
    State(state): State<ApiState>,
    Extension(request_id): Extension<RequestId>,
    request: Request,
) -> Response {
    let (parts, body) = request.into_parts();
    let result = async {
        let auth = state.security.authorize_mutation(&parts)?;
        let task_id = canonical_task_id_at(&parts, 3)?;
        let input = decode_delivery_json::<DeliveryMergeRequest>(parts, body).await?;
        let command = ValidatedDeliveryMergeCommand::try_new(task_id, input)?;
        Ok(command_response(
            state.delivery.accept_merge(&auth, command).await?,
            StatusCode::ACCEPTED,
        ))
    }
    .await;
    finish(result, &request_id)
}

#[utoipa::path(
    post,
    path = "/api/tasks/{task_id}/cleanup/worktree",
    params(("task_id" = uuid::Uuid, Path, description = "Task ID")),
    request_body(content = crate::DeliveryRemoveWorktreeRequest, content_type = "application/json"),
    responses(
        (status = 200, body = crate::DeliveryCommandResponse),
        (status = 202, body = crate::DeliveryCommandResponse),
        (status = 400, body = crate::ApiErrorResponse),
        (status = 401, body = crate::ApiErrorResponse),
        (status = 403, body = crate::ApiErrorResponse),
        (status = 404, body = crate::ApiErrorResponse),
        (status = 409, body = crate::ApiErrorResponse),
        (status = 415, body = crate::ApiErrorResponse),
        (status = 422, body = crate::ApiErrorResponse),
        (status = 500, body = crate::ApiErrorResponse),
        (status = 503, body = crate::ApiErrorResponse),
        (status = 504, body = crate::ApiErrorResponse)
    )
)]
async fn remove_worktree(
    State(state): State<ApiState>,
    Extension(request_id): Extension<RequestId>,
    request: Request,
) -> Response {
    let (parts, body) = request.into_parts();
    let result = async {
        let auth = state.security.authorize_mutation(&parts)?;
        let task_id = canonical_task_id_at(&parts, 3)?;
        let input = decode_delivery_json::<DeliveryRemoveWorktreeRequest>(parts, body).await?;
        let command = ValidatedDeliveryRemoveWorktreeCommand::try_new(task_id, input)?;
        Ok(command_response(
            state.delivery.remove_worktree(&auth, command).await?,
            StatusCode::ACCEPTED,
        ))
    }
    .await;
    finish(result, &request_id)
}

#[utoipa::path(
    post,
    path = "/api/tasks/{task_id}/cleanup/branch",
    params(("task_id" = uuid::Uuid, Path, description = "Task ID")),
    request_body(content = crate::DeliveryDeleteBranchRequest, content_type = "application/json"),
    responses(
        (status = 200, body = crate::DeliveryCommandResponse),
        (status = 202, body = crate::DeliveryCommandResponse),
        (status = 400, body = crate::ApiErrorResponse),
        (status = 401, body = crate::ApiErrorResponse),
        (status = 403, body = crate::ApiErrorResponse),
        (status = 404, body = crate::ApiErrorResponse),
        (status = 409, body = crate::ApiErrorResponse),
        (status = 415, body = crate::ApiErrorResponse),
        (status = 422, body = crate::ApiErrorResponse),
        (status = 500, body = crate::ApiErrorResponse),
        (status = 503, body = crate::ApiErrorResponse),
        (status = 504, body = crate::ApiErrorResponse)
    )
)]
async fn delete_branch(
    State(state): State<ApiState>,
    Extension(request_id): Extension<RequestId>,
    request: Request,
) -> Response {
    let (parts, body) = request.into_parts();
    let result = async {
        let auth = state.security.authorize_mutation(&parts)?;
        let task_id = canonical_task_id_at(&parts, 3)?;
        let input = decode_delivery_json::<DeliveryDeleteBranchRequest>(parts, body).await?;
        let command = ValidatedDeliveryDeleteBranchCommand::try_new(task_id, input)?;
        Ok(command_response(
            state.delivery.delete_branch(&auth, command).await?,
            StatusCode::ACCEPTED,
        ))
    }
    .await;
    finish(result, &request_id)
}

fn command_response(response: DeliveryCommandResponse, created: StatusCode) -> Response {
    let status = match response.receipt {
        DeliveryReceiptDispositionDto::Created => created,
        DeliveryReceiptDispositionDto::Existing => StatusCode::OK,
    };
    (status, Json(response)).into_response()
}

async fn decode_delivery_json<T: DeserializeOwned>(
    parts: http::request::Parts,
    body: Body,
) -> ApiResult<T> {
    ensure_json_content_type(&parts)?;
    let bytes = to_bytes(body, MAX_DELIVERY_REQUEST_BODY_BYTES)
        .await
        .map_err(|_| crate::ApiError::invalid_delivery_request())?;
    decode_json(Request::from_parts(parts, Body::from(bytes))).await
}

fn ensure_json_content_type(parts: &http::request::Parts) -> ApiResult<()> {
    let valid = parts
        .headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .map(|value| {
            value.split_once('/').is_some_and(|(top, subtype)| {
                top == "application" && (subtype == "json" || subtype.ends_with("+json"))
            })
        })
        .unwrap_or(false);
    if valid {
        Ok(())
    } else {
        Err(transport_error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "UNSUPPORTED_MEDIA_TYPE",
            "the request Content-Type must be application/json",
        ))
    }
}

fn canonical_task_id_at(parts: &http::request::Parts, segment: usize) -> ApiResult<TaskId> {
    let value = path_segment(parts, segment)?;
    canonical_uuid(value)?;
    value.parse().map_err(|_| invalid_path_parameter())
}

fn canonical_uuid_at(parts: &http::request::Parts, segment: usize) -> ApiResult<Uuid> {
    canonical_uuid(path_segment(parts, segment)?)
}

fn path_segment(parts: &http::request::Parts, segment: usize) -> ApiResult<&str> {
    parts
        .uri
        .path()
        .split('/')
        .nth(segment)
        .ok_or_else(invalid_path_parameter)
}

fn canonical_uuid(value: &str) -> ApiResult<Uuid> {
    let uuid = Uuid::parse_str(value).map_err(|_| invalid_path_parameter())?;
    if uuid.is_nil() || uuid.hyphenated().to_string() != value {
        Err(invalid_path_parameter())
    } else {
        Ok(uuid)
    }
}
