use coding_agent_core::ProviderError;

pub const PROVIDER_CONFIG_INVALID: &str = "PROVIDER_CONFIG_INVALID";
pub const PROVIDER_UNAUTHORIZED: &str = "PROVIDER_UNAUTHORIZED";
pub const PROVIDER_RATE_LIMITED: &str = "PROVIDER_RATE_LIMITED";
pub const PROVIDER_RESPONSE_INVALID: &str = "PROVIDER_RESPONSE_INVALID";
pub const PROVIDER_RESPONSE_SCHEMA_UNSUPPORTED: &str = "PROVIDER_RESPONSE_SCHEMA_UNSUPPORTED";
pub const PROVIDER_RESPONSE_REASONING_REJECTED: &str = "PROVIDER_RESPONSE_REASONING_REJECTED";
pub const PROVIDER_RESPONSE_FINISH_UNSUPPORTED: &str = "PROVIDER_RESPONSE_FINISH_UNSUPPORTED";
pub const PROVIDER_RESPONSE_TOOL_CHOICE_VIOLATED: &str = "PROVIDER_RESPONSE_TOOL_CHOICE_VIOLATED";
/// Legacy code retained so persisted failures from older builds remain recognizable.
pub const PROVIDER_UNSUPPORTED_MULTIPLE_TOOL_CALLS: &str =
    "PROVIDER_UNSUPPORTED_MULTIPLE_TOOL_CALLS";
pub const PROVIDER_REDIRECT_REJECTED: &str = "PROVIDER_REDIRECT_REJECTED";
pub const PROVIDER_REQUEST_REJECTED: &str = "PROVIDER_REQUEST_REJECTED";
pub const PROVIDER_UNAVAILABLE: &str = "PROVIDER_UNAVAILABLE";
pub const PROVIDER_TRANSPORT_FAILED: &str = "PROVIDER_TRANSPORT_FAILED";
pub const PROVIDER_CLIENT_INIT_FAILED: &str = "PROVIDER_CLIENT_INIT_FAILED";
pub const PROVIDER_TASK_BYTE_LIMIT_REACHED: &str = "PROVIDER_TASK_BYTE_LIMIT_REACHED";
pub const PROVIDER_REQUEST_BYTE_LIMIT_REACHED: &str = "PROVIDER_REQUEST_BYTE_LIMIT_REACHED";
pub const PROVIDER_CANCELLED: &str = "PROVIDER_CANCELLED";

pub fn map_http_status(status: u16) -> ProviderError {
    match status {
        401 | 403 => safe_error(
            PROVIDER_UNAUTHORIZED,
            "The provider rejected its credentials.",
            false,
        ),
        429 => safe_error(
            PROVIDER_RATE_LIMITED,
            "The provider rate limit was reached.",
            true,
        ),
        408 | 425 | 500..=599 => safe_error(
            PROVIDER_UNAVAILABLE,
            "The provider is temporarily unavailable.",
            true,
        ),
        300..=399 => safe_error(
            PROVIDER_REDIRECT_REJECTED,
            "The provider returned a redirect, which is not permitted.",
            false,
        ),
        _ => safe_error(
            PROVIDER_REQUEST_REJECTED,
            "The provider rejected the request.",
            false,
        ),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportFailure {
    Connect,
    Timeout,
    Disconnected,
}

pub fn map_transport_failure(failure: TransportFailure) -> ProviderError {
    let message = match failure {
        TransportFailure::Connect => "The provider connection could not be established.",
        TransportFailure::Timeout => "The provider request timed out.",
        TransportFailure::Disconnected => "The provider connection ended unexpectedly.",
    };
    safe_error(PROVIDER_TRANSPORT_FAILED, message, true)
}

pub(crate) fn invalid_response(message: &'static str) -> ProviderError {
    safe_error(PROVIDER_RESPONSE_INVALID, message, false)
}

pub(crate) fn unsupported_response_schema() -> ProviderError {
    safe_error(
        PROVIDER_RESPONSE_SCHEMA_UNSUPPORTED,
        "The provider response schema is unsupported.",
        false,
    )
}

pub(crate) fn rejected_response_reasoning() -> ProviderError {
    safe_error(
        PROVIDER_RESPONSE_REASONING_REJECTED,
        "The provider response contains unsupported reasoning output.",
        false,
    )
}

pub(crate) fn unsupported_response_finish() -> ProviderError {
    safe_error(
        PROVIDER_RESPONSE_FINISH_UNSUPPORTED,
        "The provider response completion state is unsupported.",
        false,
    )
}

pub(crate) fn tool_choice_violated() -> ProviderError {
    safe_error(
        PROVIDER_RESPONSE_TOOL_CHOICE_VIOLATED,
        "The provider response did not honor the requested tool choice.",
        false,
    )
}

pub(crate) fn invalid_request(message: &'static str) -> ProviderError {
    safe_error(PROVIDER_REQUEST_REJECTED, message, false)
}

pub(crate) fn client_init_failed() -> ProviderError {
    safe_error(
        PROVIDER_CLIENT_INIT_FAILED,
        "The provider client could not be initialized.",
        false,
    )
}

pub(crate) fn task_byte_limit_reached() -> ProviderError {
    safe_error(
        PROVIDER_TASK_BYTE_LIMIT_REACHED,
        "The provider byte budget for this task was reached.",
        false,
    )
}

pub(crate) fn request_limit_reached() -> ProviderError {
    safe_error(
        PROVIDER_REQUEST_BYTE_LIMIT_REACHED,
        "The provider request exceeded the configured byte limit.",
        false,
    )
}

pub(crate) fn cancelled() -> ProviderError {
    safe_error(
        PROVIDER_CANCELLED,
        "The provider request was cancelled.",
        false,
    )
}

fn safe_error(code: &'static str, message: &'static str, retryable: bool) -> ProviderError {
    ProviderError::new(code, message, retryable)
}
