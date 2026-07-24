/// Stable provider decode classification for ordinary assistant output where
/// a typed role action is required.
pub const PROVIDER_RESPONSE_ROLE_OUTPUT_INVALID: &str = "PROVIDER_RESPONSE_ROLE_OUTPUT_INVALID";

/// Stable provider decode classification for a tool action that violates the
/// current role capability or required-action contract.
pub const PROVIDER_RESPONSE_ROLE_ACTION_NOT_ALLOWED: &str =
    "PROVIDER_RESPONSE_ROLE_ACTION_NOT_ALLOWED";

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("provider request failed ({code}): {message}")]
pub struct ProviderError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

impl ProviderError {
    pub fn new(code: impl Into<String>, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("runtime operation failed ({code}): {message}")]
pub struct RuntimeError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

impl RuntimeError {
    pub fn new(code: impl Into<String>, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable,
        }
    }
}
