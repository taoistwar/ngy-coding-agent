use std::sync::Arc;

use axum::extract::{Request, State};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use coding_agent_api::{ApiError, ApiErrorResponse};
use coding_agent_domain::UtcTimestamp;
use http::{HeaderValue, StatusCode};
use serde::Serialize;
use uuid::Uuid;

use crate::security::LAUNCH_TOKEN_LIFETIME;
use crate::{SecurityManager, StartupPhase, StartupPhaseController, WallClock};

use super::{api_error, app_starting, internal_error};

pub(super) const REQUEST_ID_HEADER: &str = "x-request-id";
pub(super) const LOCAL_READY_PATH: &str = "/_local/ready";
pub(super) const LOCAL_REOPEN_PATH: &str = "/_local/reopen";
const CONTENT_SECURITY_POLICY: &str = "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'";

#[derive(Clone)]
struct RuntimeRouterState {
    instance_id: Uuid,
    phase: StartupPhaseController,
    security: SecurityManager,
    wall_clock: Arc<dyn WallClock>,
}

#[derive(Clone)]
struct RuntimeRequestId(String);

#[derive(Serialize)]
struct ReadyResponse {
    instance_id: Uuid,
    state: StartupPhase,
}

#[derive(Serialize)]
struct ReopenResponse {
    url: String,
    expires_at: String,
}

/// Wraps the business API with the process-local startup and launcher endpoints.
///
/// The guard is deliberately outside the supplied API router: every request, including
/// requests rejected while the process is starting, must pass exact Host validation.
pub fn build_runtime_router(
    api_router: Router,
    instance_id: Uuid,
    phase: StartupPhaseController,
    security: SecurityManager,
    wall_clock: Arc<dyn WallClock>,
) -> Router {
    let state = RuntimeRouterState {
        instance_id,
        phase,
        security,
        wall_clock,
    };
    let local_router = Router::new()
        .route(LOCAL_READY_PATH, get(local_ready))
        .route(LOCAL_REOPEN_PATH, post(local_reopen))
        .with_state(state.clone());

    local_router
        .merge(api_router)
        .fallback_service(crate::StaticAssetService::new())
        .layer(middleware::from_fn_with_state(state, runtime_guard))
}

async fn runtime_guard(
    State(state): State<RuntimeRouterState>,
    mut request: Request,
    next: Next,
) -> Response {
    let request_id = choose_runtime_request_id(request.headers());
    request
        .extensions_mut()
        .insert(RuntimeRequestId(request_id.clone()));
    let (parts, body) = request.into_parts();
    let no_store_request =
        path_is_within(parts.uri.path(), "/api") || path_is_within(parts.uri.path(), "/_local");
    let host_result = state.security.validate_host(&parts);
    let local_startup_path = matches!(parts.uri.path(), LOCAL_READY_PATH | LOCAL_REOPEN_PATH);
    let request = Request::from_parts(parts, body);

    let response = if let Err(error) = host_result {
        runtime_error_response(error, &request_id)
    } else if state.phase.current() == StartupPhase::Starting && !local_startup_path {
        runtime_error_response(app_starting(), &request_id)
    } else {
        next.run(request).await
    };

    apply_response_policy(with_request_id(response, &request_id), no_store_request)
}

fn apply_response_policy(mut response: Response, no_store_request: bool) -> Response {
    let html_response = response
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("text/html"));
    let headers = response.headers_mut();

    headers.insert(
        "content-security-policy",
        HeaderValue::from_static(CONTENT_SECURITY_POLICY),
    );
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    headers.insert("referrer-policy", HeaderValue::from_static("no-referrer"));
    if no_store_request || html_response {
        headers.insert(
            http::header::CACHE_CONTROL,
            HeaderValue::from_static("no-store"),
        );
    }

    for header in [
        "access-control-allow-credentials",
        "access-control-allow-headers",
        "access-control-allow-methods",
        "access-control-allow-origin",
        "access-control-allow-private-network",
        "access-control-expose-headers",
        "access-control-max-age",
    ] {
        headers.remove(header);
    }

    response
}

fn path_is_within(path: &str, prefix: &str) -> bool {
    path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

async fn local_ready(State(state): State<RuntimeRouterState>, request: Request) -> Response {
    let request_id = runtime_request_id(&request);
    let (parts, _) = request.into_parts();
    if let Err(error) = state.security.authorize_launcher(&parts) {
        return runtime_error_response(error, &request_id);
    }
    Json(ReadyResponse {
        instance_id: state.instance_id,
        state: state.phase.current(),
    })
    .into_response()
}

async fn local_reopen(State(state): State<RuntimeRouterState>, request: Request) -> Response {
    let request_id = runtime_request_id(&request);
    let (parts, _) = request.into_parts();
    if let Err(error) = state.security.authorize_launcher(&parts) {
        return runtime_error_response(error, &request_id);
    }
    if state.phase.current() != StartupPhase::Ready {
        return runtime_error_response(app_starting(), &request_id);
    }

    // Build the externally reported deadline before issuing the token. This makes the
    // advertised lifetime no longer than the monotonic two-minute security lifetime.
    let expires_at = match UtcTimestamp::new(
        state
            .wall_clock
            .now_utc()
            .saturating_add(time::Duration::seconds(
                i64::try_from(LAUNCH_TOKEN_LIFETIME.as_secs())
                    .expect("launch-token lifetime fits an i64"),
            )),
    ) {
        Ok(value) => value.to_string(),
        Err(_) => return runtime_error_response(internal_error(), &request_id),
    };
    let token = match state.security.issue_launch_token() {
        Ok(token) => token,
        Err(_) => {
            return runtime_error_response(
                api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "SECURITY_RANDOM_UNAVAILABLE",
                    "secure random generation is temporarily unavailable",
                    false,
                ),
                &request_id,
            );
        }
    };
    Json(ReopenResponse {
        url: format!(
            "{}/#token={}",
            state.security.public_origin(),
            token.as_str()
        ),
        expires_at,
    })
    .into_response()
}

fn runtime_request_id(request: &Request) -> String {
    request
        .extensions()
        .get::<RuntimeRequestId>()
        .map(|value| value.0.clone())
        .unwrap_or_else(|| Uuid::new_v4().to_string())
}

fn choose_runtime_request_id(headers: &http::HeaderMap) -> String {
    let mut values = headers.get_all(REQUEST_ID_HEADER).iter();
    let first = values.next();
    let only_one = values.next().is_none();
    first
        .filter(|_| only_one)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| Uuid::parse_str(value).ok().map(|id| (value, id)))
        .filter(|(value, id)| *value == id.to_string())
        .map(|(_, id)| id.to_string())
        .unwrap_or_else(|| Uuid::new_v4().to_string())
}

fn runtime_error_response(error: ApiError, request_id: &str) -> Response {
    (
        error.status,
        Json(ApiErrorResponse {
            code: error.code,
            message: error.message,
            retryable: error.retryable,
            request_id: request_id.to_owned(),
            details: error.details,
        }),
    )
        .into_response()
}

fn with_request_id(mut response: Response, request_id: &str) -> Response {
    if !response.headers().contains_key(REQUEST_ID_HEADER) {
        response.headers_mut().insert(
            REQUEST_ID_HEADER,
            HeaderValue::from_str(request_id).expect("UUID request ID is a valid header"),
        );
    }
    response
}
