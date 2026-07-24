//! Strict, secret-safe seams for the supported provider protocol.
//!
//! The HTTP transport is intentionally implemented separately. This crate exposes the
//! persisted configuration parser, bounded Chat Completions JSON codec, stable error mapping,
//! and redaction primitives that the transport must use at logging and user boundaries.

mod client;
mod config;
mod error;
mod protocol;
mod redaction;

pub use client::{
    ChatCompletionsClient, ChatCompletionsProvider, ClientLimits, ClientLimitsError,
    ProviderResponseMetadata,
};
pub use config::{
    ApiKey, MAX_PROVIDER_CONFIG_BYTES, MIN_PROVIDER_API_KEY_BYTES, ProviderConfig,
    ProviderConfigError, ProviderConfigErrorReason, ProviderThinkingMode,
    ProviderToolChoiceCompatibility,
};
pub use error::{
    PROVIDER_CANCELLED, PROVIDER_CLIENT_INIT_FAILED, PROVIDER_CONFIG_INVALID,
    PROVIDER_RATE_LIMITED, PROVIDER_REDIRECT_REJECTED, PROVIDER_REQUEST_BYTE_LIMIT_REACHED,
    PROVIDER_REQUEST_REJECTED, PROVIDER_RESPONSE_FINISH_UNSUPPORTED, PROVIDER_RESPONSE_INVALID,
    PROVIDER_RESPONSE_REASONING_REJECTED, PROVIDER_RESPONSE_ROLE_ACTION_NOT_ALLOWED,
    PROVIDER_RESPONSE_ROLE_OUTPUT_INVALID, PROVIDER_RESPONSE_SCHEMA_UNSUPPORTED,
    PROVIDER_RESPONSE_TOOL_CHOICE_VIOLATED, PROVIDER_TASK_BYTE_LIMIT_REACHED,
    PROVIDER_TRANSPORT_FAILED, PROVIDER_UNAUTHORIZED, PROVIDER_UNAVAILABLE,
    PROVIDER_UNSUPPORTED_MULTIPLE_TOOL_CALLS, TransportFailure, map_http_status,
    map_transport_failure,
};
pub use protocol::{
    decode_chat_completions_response, decode_chat_completions_response_for_request,
    encode_chat_completions_request, encode_chat_completions_request_with_compatibility,
};
pub use redaction::{RedactedText, SecretRedactor};
