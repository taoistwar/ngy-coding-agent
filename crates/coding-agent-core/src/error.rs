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
