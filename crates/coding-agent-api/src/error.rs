use std::collections::BTreeMap;

use http::StatusCode;
use serde_json::Value;

pub type ApiResult<T> = Result<T, ApiError>;

#[derive(Debug, thiserror::Error)]
#[error("{code}: {message}")]
pub struct ApiError {
    pub status: StatusCode,
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub details: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, utoipa::ToSchema)]
pub struct ApiErrorResponse {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub request_id: String,
    pub details: BTreeMap<String, Value>,
}
