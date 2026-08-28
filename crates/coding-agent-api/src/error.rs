use std::collections::BTreeMap;

use http::StatusCode;
use serde_json::Value;

mod delivery;

pub use delivery::DeliveryApiErrorKind;

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

impl ApiError {
    pub fn task_queue_full(queued_tasks: u32, max_queued_tasks: u32) -> Self {
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            code: "TASK_QUEUE_FULL".to_owned(),
            message: "the task queue is full; retry after capacity becomes available".to_owned(),
            retryable: true,
            details: BTreeMap::from([
                ("queued_tasks".to_owned(), Value::from(queued_tasks)),
                ("max_queued_tasks".to_owned(), Value::from(max_queued_tasks)),
            ]),
        }
    }

    pub fn task_stop_already_requested() -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code: "TASK_STOP_ALREADY_REQUESTED".to_owned(),
            message: "another stop request already won for this task".to_owned(),
            retryable: false,
            details: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, utoipa::ToSchema)]
pub struct ApiErrorResponse {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub request_id: String,
    pub details: BTreeMap<String, Value>,
}
