use std::collections::BTreeMap;
use std::convert::Infallible;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use axum::Json;
use axum::body::{Body, Bytes};
use axum::extract::{Extension, FromRequest, Request, State};
use axum::http::header::{CONTENT_TYPE, SET_COOKIE};
use axum::http::{HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::sse::Event;
use axum::response::{IntoResponse, Response, Sse};
use coding_agent_domain::{RepositoryId, TaskId};
use futures_util::FutureExt as _;
use futures_util::stream;
use serde::de::DeserializeOwned;
use utoipa::OpenApi as _;
use utoipa::openapi::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;
use uuid::Uuid;

use crate::{
    AddRepositoryRequest, ApiBackend, ApiDoc, ApiError, ApiErrorResponse, ApiResult,
    BootstrapResponse, CancelResult, CancellationAcceptedResponse, CreateResult, CreateTaskRequest,
    QuitResponse, RepositoryDto, RequestSecurity, SessionExchangeRequest, SseBackend,
    TaskDetailDto, TaskDto, TaskEventDto,
};

const REQUEST_ID_HEADER: &str = "x-request-id";

#[derive(Clone)]
struct ApiState {
    backend: Arc<dyn ApiBackend>,
    security: Arc<dyn RequestSecurity>,
    sse: Arc<dyn SseBackend>,
}

#[derive(Clone)]
struct RequestId(String);

pub fn build_api_router(
    backend: Arc<dyn ApiBackend>,
    security: Arc<dyn RequestSecurity>,
    sse: Arc<dyn SseBackend>,
) -> axum::Router {
    let state = ApiState {
        backend,
        security,
        sse,
    };
    let router: axum::Router<ApiState> = unbound_api_router().into();
    router
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .with_state(state.clone())
        .layer(middleware::from_fn_with_state(state, host_guard))
        .layer(middleware::from_fn(request_envelope))
}

pub fn api_openapi() -> OpenApi {
    unbound_api_router().into_openapi()
}

fn unbound_api_router() -> OpenApiRouter<ApiState> {
    OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(exchange_session))
        .routes(routes!(bootstrap))
        .routes(routes!(repositories))
        .routes(routes!(add_repository))
        .routes(routes!(pick_repository))
        .routes(routes!(tasks))
        .routes(routes!(create_task))
        .routes(routes!(task_detail))
        .routes(routes!(cancel_task))
        .routes(routes!(retry_task))
        .routes(routes!(task_events))
        .routes(routes!(events))
        .routes(routes!(quit))
}

#[utoipa::path(
    post,
    path = "/api/session/exchange",
    request_body(content = SessionExchangeRequest, content_type = "application/json"),
    responses(
        (status = 204, description = "Session established"),
        (status = 400, body = crate::ApiErrorResponse),
        (status = 401, body = crate::ApiErrorResponse),
        (status = 403, body = crate::ApiErrorResponse),
        (status = 415, body = crate::ApiErrorResponse),
        (status = 500, body = crate::ApiErrorResponse)
    )
)]
async fn exchange_session(
    State(state): State<ApiState>,
    Extension(request_id): Extension<RequestId>,
    request: Request,
) -> Response {
    let (parts, body) = request.into_parts();
    let body_request = Request::from_parts(clone_parts(&parts), body);
    let result = async {
        let body: SessionExchangeRequest = decode_json(body_request).await?;
        let exchange = state.security.exchange(&parts, &body.token).await?;
        let mut response = StatusCode::NO_CONTENT.into_response();
        response
            .headers_mut()
            .insert(SET_COOKIE, exchange.set_cookie);
        Ok(response)
    }
    .await;
    finish(result, &request_id)
}

#[utoipa::path(
    get,
    path = "/api/bootstrap",
    responses(
        (status = 200, body = BootstrapResponse),
        (status = 400, body = crate::ApiErrorResponse),
        (status = 401, body = crate::ApiErrorResponse),
        (status = 403, body = crate::ApiErrorResponse),
        (status = 500, body = crate::ApiErrorResponse)
    )
)]
async fn bootstrap(
    State(state): State<ApiState>,
    Extension(request_id): Extension<RequestId>,
    request: Request,
) -> Response {
    let (parts, _) = request.into_parts();
    let result = async {
        let auth = state.security.authorize_read(&parts)?;
        Ok(Json(state.backend.bootstrap(&auth).await?).into_response())
    }
    .await;
    finish(result, &request_id)
}

#[utoipa::path(
    get,
    path = "/api/repositories",
    responses(
        (status = 200, body = [RepositoryDto]),
        (status = 400, body = crate::ApiErrorResponse),
        (status = 401, body = crate::ApiErrorResponse),
        (status = 403, body = crate::ApiErrorResponse),
        (status = 500, body = crate::ApiErrorResponse)
    )
)]
async fn repositories(
    State(state): State<ApiState>,
    Extension(request_id): Extension<RequestId>,
    request: Request,
) -> Response {
    let (parts, _) = request.into_parts();
    let result = async {
        let auth = state.security.authorize_read(&parts)?;
        Ok(Json(state.backend.list_repositories(&auth).await?).into_response())
    }
    .await;
    finish(result, &request_id)
}

#[utoipa::path(
    post,
    path = "/api/repositories",
    request_body(content = AddRepositoryRequest, content_type = "application/json"),
    responses(
        (status = 200, body = RepositoryDto),
        (status = 201, body = RepositoryDto),
        (status = 400, body = crate::ApiErrorResponse),
        (status = 401, body = crate::ApiErrorResponse),
        (status = 403, body = crate::ApiErrorResponse),
        (status = 415, body = crate::ApiErrorResponse),
        (status = 422, body = crate::ApiErrorResponse),
        (status = 500, body = crate::ApiErrorResponse),
        (status = 503, body = crate::ApiErrorResponse)
    )
)]
async fn add_repository(
    State(state): State<ApiState>,
    Extension(request_id): Extension<RequestId>,
    request: Request,
) -> Response {
    let (parts, body) = request.into_parts();
    let result = async {
        let auth = state.security.authorize_mutation(&parts)?;
        let request = Request::from_parts(parts, body);
        let input = decode_json::<AddRepositoryRequest>(request).await?;
        Ok(created_response(
            state.backend.add_repository(&auth, input).await?,
        ))
    }
    .await;
    finish(result, &request_id)
}

#[utoipa::path(
    post,
    path = "/api/repositories/pick",
    responses(
        (status = 200, body = RepositoryDto),
        (status = 201, body = RepositoryDto),
        (status = 204, description = "Picker cancelled"),
        (status = 400, body = crate::ApiErrorResponse),
        (status = 401, body = crate::ApiErrorResponse),
        (status = 403, body = crate::ApiErrorResponse),
        (status = 409, body = crate::ApiErrorResponse),
        (status = 422, body = crate::ApiErrorResponse),
        (status = 500, body = crate::ApiErrorResponse),
        (status = 503, body = crate::ApiErrorResponse)
    )
)]
async fn pick_repository(
    State(state): State<ApiState>,
    Extension(request_id): Extension<RequestId>,
    request: Request,
) -> Response {
    let (parts, _) = request.into_parts();
    let result = async {
        let auth = state.security.authorize_mutation(&parts)?;
        Ok(match state.backend.pick_repository(&auth).await? {
            Some(result) => created_response(result),
            None => StatusCode::NO_CONTENT.into_response(),
        })
    }
    .await;
    finish(result, &request_id)
}

#[utoipa::path(
    get,
    path = "/api/tasks",
    params(("repository_id" = Option<uuid::Uuid>, Query, description = "Repository filter")),
    responses(
        (status = 200, body = [TaskDto]),
        (status = 400, body = crate::ApiErrorResponse),
        (status = 401, body = crate::ApiErrorResponse),
        (status = 403, body = crate::ApiErrorResponse),
        (status = 500, body = crate::ApiErrorResponse)
    )
)]
async fn tasks(
    State(state): State<ApiState>,
    Extension(request_id): Extension<RequestId>,
    request: Request,
) -> Response {
    let (parts, _) = request.into_parts();
    let result = async {
        let auth = state.security.authorize_read(&parts)?;
        let repository_id = query_value(parts.uri.query(), "repository_id")
            .map(parse_repository_id)
            .transpose()?;
        Ok(Json(state.backend.list_tasks(&auth, repository_id).await?).into_response())
    }
    .await;
    finish(result, &request_id)
}

#[utoipa::path(
    post,
    path = "/api/tasks",
    request_body(content = CreateTaskRequest, content_type = "application/json"),
    responses(
        (status = 200, body = TaskDto),
        (status = 201, body = TaskDto),
        (status = 400, body = crate::ApiErrorResponse),
        (status = 401, body = crate::ApiErrorResponse),
        (status = 403, body = crate::ApiErrorResponse),
        (status = 409, body = crate::ApiErrorResponse),
        (status = 415, body = crate::ApiErrorResponse),
        (status = 422, body = crate::ApiErrorResponse),
        (status = 500, body = crate::ApiErrorResponse),
        (status = 503, body = crate::ApiErrorResponse)
    )
)]
async fn create_task(
    State(state): State<ApiState>,
    Extension(request_id): Extension<RequestId>,
    request: Request,
) -> Response {
    let (parts, body) = request.into_parts();
    let result = async {
        let auth = state.security.authorize_mutation(&parts)?;
        let request = Request::from_parts(parts, body);
        let input = decode_json::<CreateTaskRequest>(request).await?;
        Ok(created_response(
            state.backend.create_task(&auth, input).await?,
        ))
    }
    .await;
    finish(result, &request_id)
}

#[utoipa::path(
    get,
    path = "/api/tasks/{id}",
    params(("id" = uuid::Uuid, Path, description = "Task ID")),
    responses(
        (status = 200, body = TaskDetailDto),
        (status = 400, body = crate::ApiErrorResponse),
        (status = 401, body = crate::ApiErrorResponse),
        (status = 403, body = crate::ApiErrorResponse),
        (status = 404, body = crate::ApiErrorResponse),
        (status = 500, body = crate::ApiErrorResponse)
    )
)]
async fn task_detail(
    State(state): State<ApiState>,
    Extension(request_id): Extension<RequestId>,
    request: Request,
) -> Response {
    let (parts, _) = request.into_parts();
    let result = async {
        let auth = state.security.authorize_read(&parts)?;
        let id = task_id_at(&parts, 3)?;
        Ok(Json(state.backend.task_detail(&auth, id).await?).into_response())
    }
    .await;
    finish(result, &request_id)
}

#[utoipa::path(
    post,
    path = "/api/tasks/{id}/cancel",
    params(("id" = uuid::Uuid, Path, description = "Task ID")),
    responses(
        (status = 200, body = TaskDto),
        (status = 202, body = CancellationAcceptedResponse),
        (status = 400, body = crate::ApiErrorResponse),
        (status = 401, body = crate::ApiErrorResponse),
        (status = 403, body = crate::ApiErrorResponse),
        (status = 404, body = crate::ApiErrorResponse),
        (status = 409, body = crate::ApiErrorResponse),
        (status = 500, body = crate::ApiErrorResponse),
        (status = 503, body = crate::ApiErrorResponse)
    )
)]
async fn cancel_task(
    State(state): State<ApiState>,
    Extension(request_id): Extension<RequestId>,
    request: Request,
) -> Response {
    let (parts, _) = request.into_parts();
    let result = async {
        let auth = state.security.authorize_mutation(&parts)?;
        let id = task_id_at(&parts, 3)?;
        Ok(match state.backend.cancel_task(&auth, id).await? {
            CancelResult::Finished(task) => Json(task).into_response(),
            CancelResult::Accepted { task } => (
                StatusCode::ACCEPTED,
                Json(CancellationAcceptedResponse::new(task)),
            )
                .into_response(),
        })
    }
    .await;
    finish(result, &request_id)
}

#[utoipa::path(
    post,
    path = "/api/tasks/{id}/retry",
    params(("id" = uuid::Uuid, Path, description = "Task ID")),
    responses(
        (status = 200, body = TaskDto),
        (status = 201, body = TaskDto),
        (status = 400, body = crate::ApiErrorResponse),
        (status = 401, body = crate::ApiErrorResponse),
        (status = 403, body = crate::ApiErrorResponse),
        (status = 404, body = crate::ApiErrorResponse),
        (status = 409, body = crate::ApiErrorResponse),
        (status = 500, body = crate::ApiErrorResponse),
        (status = 503, body = crate::ApiErrorResponse)
    )
)]
async fn retry_task(
    State(state): State<ApiState>,
    Extension(request_id): Extension<RequestId>,
    request: Request,
) -> Response {
    let (parts, _) = request.into_parts();
    let result = async {
        let auth = state.security.authorize_mutation(&parts)?;
        let id = task_id_at(&parts, 3)?;
        Ok(created_response(state.backend.retry_task(&auth, id).await?))
    }
    .await;
    finish(result, &request_id)
}

#[utoipa::path(
    get,
    path = "/api/tasks/{id}/events",
    params(
        ("id" = uuid::Uuid, Path, description = "Task ID"),
        ("after" = Option<i64>, Query, description = "Exclusive event cursor")
    ),
    responses(
        (status = 200, body = [TaskEventDto]),
        (status = 400, body = crate::ApiErrorResponse),
        (status = 401, body = crate::ApiErrorResponse),
        (status = 403, body = crate::ApiErrorResponse),
        (status = 404, body = crate::ApiErrorResponse),
        (status = 500, body = crate::ApiErrorResponse)
    )
)]
async fn task_events(
    State(state): State<ApiState>,
    Extension(request_id): Extension<RequestId>,
    request: Request,
) -> Response {
    let (parts, _) = request.into_parts();
    let result = async {
        let auth = state.security.authorize_read(&parts)?;
        let id = task_id_at(&parts, 3)?;
        let after = after_query(&parts)?;
        Ok(Json(state.backend.task_events(&auth, id, after).await?).into_response())
    }
    .await;
    finish(result, &request_id)
}

#[utoipa::path(
    get,
    path = "/api/events",
    params(("after" = Option<i64>, Query, description = "Exclusive event cursor")),
    responses(
        (status = 200, description = "Server-sent event stream", content_type = "text/event-stream", body = String),
        (status = 400, body = crate::ApiErrorResponse),
        (status = 401, body = crate::ApiErrorResponse),
        (status = 403, body = crate::ApiErrorResponse),
        (status = 500, body = crate::ApiErrorResponse)
    )
)]
async fn events(
    State(state): State<ApiState>,
    Extension(request_id): Extension<RequestId>,
    request: Request,
) -> Response {
    let (parts, _) = request.into_parts();
    let result = async {
        let _auth = state.security.authorize_read(&parts)?;
        let _after = after_query(&parts)?;
        let control = state.sse.current_service_state().await?;
        let data = serde_json::to_string(&control).map_err(internal_serialization_error)?;
        let output = stream::once(async move {
            Ok::<Event, Infallible>(Event::default().event("service.state").data(data))
        });
        Ok(Sse::new(output).into_response())
    }
    .await;
    finish(result, &request_id)
}

#[utoipa::path(
    post,
    path = "/api/app/quit",
    responses(
        (status = 202, body = QuitResponse),
        (status = 400, body = crate::ApiErrorResponse),
        (status = 401, body = crate::ApiErrorResponse),
        (status = 403, body = crate::ApiErrorResponse),
        (status = 500, body = crate::ApiErrorResponse),
        (status = 503, body = crate::ApiErrorResponse)
    )
)]
async fn quit(
    State(state): State<ApiState>,
    Extension(request_id): Extension<RequestId>,
    request: Request,
) -> Response {
    let (parts, _) = request.into_parts();
    let result = async {
        let auth = state.security.authorize_mutation(&parts)?;
        let mut acceptance = state.backend.request_quit(&auth).await?;
        let trigger = acceptance.take_trigger().ok_or_else(internal_quit_error)?;
        quit_response(trigger)
    }
    .await;
    finish(result, &request_id)
}

fn created_response<T: serde::Serialize>(result: CreateResult<T>) -> Response {
    match result {
        CreateResult::Created(value) => (StatusCode::CREATED, Json(value)).into_response(),
        CreateResult::Existing(value) => Json(value).into_response(),
    }
}

fn quit_response(trigger: Box<dyn FnOnce() + Send + 'static>) -> ApiResult<Response> {
    let bytes = serde_json::to_vec(&QuitResponse::shutting_down())
        .map(Bytes::from)
        .map_err(internal_serialization_error)?;
    let output = stream::unfold(
        (Some(bytes), Some(trigger)),
        |(mut bytes, mut trigger)| async move {
            if let Some(bytes) = bytes.take() {
                Some((Ok::<Bytes, Infallible>(bytes), (None, trigger)))
            } else {
                if let Some(trigger) = trigger.take() {
                    trigger();
                }
                None
            }
        },
    );
    let mut response = (StatusCode::ACCEPTED, Body::from_stream(output)).into_response();
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    Ok(response)
}

async fn request_envelope(mut request: Request, next: Next) -> Response {
    let request_id = choose_request_id(request.headers());
    request.extensions_mut().insert(request_id.clone());
    let response = AssertUnwindSafe(next.run(request)).catch_unwind().await;
    let mut response = match response {
        Ok(response) => response,
        Err(_) => error_response(internal_panic_error(), &request_id),
    };
    response.headers_mut().insert(
        REQUEST_ID_HEADER,
        HeaderValue::from_str(&request_id.0).expect("UUID request ID is a valid header"),
    );
    let error_code = response
        .extensions()
        .get::<ResponseErrorCode>()
        .map(|value| value.0.as_str())
        .unwrap_or("NONE");
    tracing::info!(
        request_id = %request_id.0,
        status = response.status().as_u16(),
        error_code,
        "API request completed"
    );
    response
}

async fn host_guard(State(state): State<ApiState>, request: Request, next: Next) -> Response {
    let request_id = request
        .extensions()
        .get::<RequestId>()
        .cloned()
        .unwrap_or_else(|| RequestId(Uuid::new_v4().to_string()));
    let (parts, body) = request.into_parts();
    if let Err(error) = state.security.validate_host(&parts) {
        return error_response(error, &request_id);
    }
    next.run(Request::from_parts(parts, body)).await
}

async fn not_found(Extension(request_id): Extension<RequestId>) -> Response {
    error_response(
        transport_error(
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            "the route was not found",
        ),
        &request_id,
    )
}

async fn method_not_allowed(Extension(request_id): Extension<RequestId>) -> Response {
    error_response(
        transport_error(
            StatusCode::METHOD_NOT_ALLOWED,
            "METHOD_NOT_ALLOWED",
            "the method is not allowed for this route",
        ),
        &request_id,
    )
}

fn finish(result: ApiResult<Response>, request_id: &RequestId) -> Response {
    result.unwrap_or_else(|error| error_response(error, request_id))
}

fn error_response(error: ApiError, request_id: &RequestId) -> Response {
    let code = error.code.clone();
    let mut response = (
        error.status,
        Json(ApiErrorResponse {
            code: error.code,
            message: error.message,
            retryable: error.retryable,
            request_id: request_id.0.clone(),
            details: error.details,
        }),
    )
        .into_response();
    response.extensions_mut().insert(ResponseErrorCode(code));
    response
}

#[derive(Clone)]
struct ResponseErrorCode(String);

fn choose_request_id(headers: &http::HeaderMap) -> RequestId {
    let mut values = headers.get_all(REQUEST_ID_HEADER).iter();
    let first = values.next();
    let only_one = values.next().is_none();
    let canonical = first
        .filter(|_| only_one)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| Uuid::parse_str(value).ok().map(|id| (value, id)))
        .filter(|(value, id)| *value == id.to_string())
        .map(|(_, id)| id.to_string());
    RequestId(canonical.unwrap_or_else(|| Uuid::new_v4().to_string()))
}

async fn decode_json<T: DeserializeOwned>(request: Request) -> ApiResult<T> {
    Json::<T>::from_request(request, &())
        .await
        .map(|Json(value)| value)
        .map_err(|rejection| {
            let status = rejection.status();
            if status == StatusCode::UNSUPPORTED_MEDIA_TYPE {
                transport_error(
                    status,
                    "UNSUPPORTED_MEDIA_TYPE",
                    "the request Content-Type must be application/json",
                )
            } else {
                transport_error(
                    StatusCode::BAD_REQUEST,
                    "INVALID_JSON",
                    "the JSON request body is invalid",
                )
            }
        })
}

fn clone_parts(parts: &http::request::Parts) -> http::request::Parts {
    let mut request = http::Request::builder()
        .method(parts.method.clone())
        .uri(parts.uri.clone())
        .version(parts.version)
        .body(())
        .expect("copy valid request parts");
    *request.headers_mut() = parts.headers.clone();
    let (copied, ()) = request.into_parts();
    copied
}

fn task_id_at(parts: &http::request::Parts, segment: usize) -> ApiResult<TaskId> {
    parts
        .uri
        .path()
        .split('/')
        .nth(segment)
        .and_then(|value| value.parse().ok())
        .ok_or_else(invalid_path_parameter)
}

fn after_query(parts: &http::request::Parts) -> ApiResult<i64> {
    let after = query_value(parts.uri.query(), "after")
        .unwrap_or("0")
        .parse::<i64>()
        .map_err(|_| invalid_query())?;
    if after < 0 {
        Err(invalid_query())
    } else {
        Ok(after)
    }
}

fn query_value<'a>(query: Option<&'a str>, key: &str) -> Option<&'a str> {
    query?.split('&').find_map(|pair| {
        let (name, value) = pair.split_once('=')?;
        (name == key).then_some(value)
    })
}

fn parse_repository_id(value: &str) -> ApiResult<RepositoryId> {
    value.parse().map_err(|_| invalid_query())
}

fn invalid_path_parameter() -> ApiError {
    transport_error(
        StatusCode::BAD_REQUEST,
        "INVALID_PATH_PARAMETER",
        "a path parameter is invalid",
    )
}

fn invalid_query() -> ApiError {
    transport_error(
        StatusCode::BAD_REQUEST,
        "INVALID_QUERY",
        "a query parameter is invalid",
    )
}

fn internal_serialization_error(_: serde_json::Error) -> ApiError {
    transport_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "INTERNAL_ERROR",
        "the server could not encode the response",
    )
}

fn internal_quit_error() -> ApiError {
    transport_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "INTERNAL_ERROR",
        "the server could not prepare shutdown",
    )
}

fn internal_panic_error() -> ApiError {
    transport_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "INTERNAL_ERROR",
        "the request could not be completed",
    )
}

fn transport_error(status: StatusCode, code: &str, message: &str) -> ApiError {
    ApiError {
        status,
        code: code.to_owned(),
        message: message.to_owned(),
        retryable: false,
        details: BTreeMap::new(),
    }
}
