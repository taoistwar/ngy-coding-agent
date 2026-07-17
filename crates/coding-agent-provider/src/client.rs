use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use coding_agent_core::{
    ModelMessage, ModelProvider, ModelRequest, ModelResponse, ProviderError, ToolRequest,
};
use futures_util::StreamExt as _;
use reqwest::header::{
    ACCEPT, ACCEPT_ENCODING, AUTHORIZATION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE,
    HeaderMap, HeaderValue,
};
use tokio_util::sync::CancellationToken;

use crate::error::{
    TransportFailure, cancelled, client_init_failed, invalid_response, map_http_status,
    map_transport_failure, request_limit_reached, task_byte_limit_reached,
};
use crate::{
    ProviderConfig, SecretRedactor, decode_chat_completions_response,
    encode_chat_completions_request,
};

const JSON_MEDIA_TYPE: &str = "application/json";
const REQUEST_ID_HEADER: &str = "x-request-id";
const MAX_METADATA_BYTES: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientLimits {
    connect_timeout: Duration,
    request_timeout: Duration,
    max_request_bytes: usize,
    max_response_bytes: usize,
    max_task_provider_bytes: usize,
}

impl ClientLimits {
    pub const fn try_new(
        connect_timeout: Duration,
        request_timeout: Duration,
        max_request_bytes: usize,
        max_response_bytes: usize,
        max_task_provider_bytes: usize,
    ) -> Result<Self, ClientLimitsError> {
        if connect_timeout.is_zero() {
            return Err(ClientLimitsError::ZeroConnectTimeout);
        }
        if request_timeout.is_zero() {
            return Err(ClientLimitsError::ZeroRequestTimeout);
        }
        if max_request_bytes == 0 {
            return Err(ClientLimitsError::ZeroRequestBytes);
        }
        if max_response_bytes == 0 {
            return Err(ClientLimitsError::ZeroResponseBytes);
        }
        if max_task_provider_bytes == 0 {
            return Err(ClientLimitsError::ZeroTaskProviderBytes);
        }
        Ok(Self {
            connect_timeout,
            request_timeout,
            max_request_bytes,
            max_response_bytes,
            max_task_provider_bytes,
        })
    }

    pub const fn connect_timeout(self) -> Duration {
        self.connect_timeout
    }

    pub const fn request_timeout(self) -> Duration {
        self.request_timeout
    }

    pub const fn max_request_bytes(self) -> usize {
        self.max_request_bytes
    }

    pub const fn max_response_bytes(self) -> usize {
        self.max_response_bytes
    }

    pub const fn max_task_provider_bytes(self) -> usize {
        self.max_task_provider_bytes
    }
}

impl Default for ClientLimits {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(120),
            max_request_bytes: 1024 * 1024,
            max_response_bytes: 1024 * 1024,
            max_task_provider_bytes: 8 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ClientLimitsError {
    #[error("provider connect timeout must be nonzero")]
    ZeroConnectTimeout,
    #[error("provider request timeout must be nonzero")]
    ZeroRequestTimeout,
    #[error("provider request byte limit must be nonzero")]
    ZeroRequestBytes,
    #[error("provider response byte limit must be nonzero")]
    ZeroResponseBytes,
    #[error("provider task byte limit must be nonzero")]
    ZeroTaskProviderBytes,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ProviderResponseMetadata {
    endpoint_origin: String,
    status: u16,
    request_id: Option<String>,
}

impl ProviderResponseMetadata {
    pub fn endpoint_origin(&self) -> &str {
        &self.endpoint_origin
    }

    pub const fn status(&self) -> u16 {
        self.status
    }

    pub fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }
}

impl fmt::Debug for ProviderResponseMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderResponseMetadata")
            .field("endpoint_origin", &self.endpoint_origin)
            .field("status", &self.status)
            .field("request_id", &self.request_id)
            .finish()
    }
}

impl fmt::Display for ProviderResponseMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "provider response status {} from {} (request_id={})",
            self.status,
            self.endpoint_origin,
            self.request_id.as_deref().unwrap_or("unavailable")
        )
    }
}

/// Reusable rustls-backed transport and immutable provider configuration.
///
/// Call [`Self::start_task`] for every agent run. Task byte accounting and response metadata are
/// intentionally absent from this reusable client.
#[derive(Clone)]
pub struct ChatCompletionsClient {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    config: ProviderConfig,
    authorization: HeaderValue,
    client: reqwest::Client,
    limits: ClientLimits,
    endpoint_origin: String,
}

impl ChatCompletionsClient {
    pub fn new(config: ProviderConfig, limits: ClientLimits) -> Result<Self, ProviderError> {
        let mut authorization =
            HeaderValue::from_str(&format!("Bearer {}", config.api_key().expose_secret()))
                .map_err(|_| client_init_failed())?;
        authorization.set_sensitive(true);
        let endpoint_origin = config
            .redactor()
            .for_log_bounded(
                &config.base_url().origin().ascii_serialization(),
                MAX_METADATA_BYTES,
            )
            .into_string();

        let mut builder = reqwest::Client::builder()
            .tls_backend_rustls()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .no_proxy()
            .referer(false)
            .connection_verbose(false)
            .connect_timeout(limits.connect_timeout())
            .timeout(limits.request_timeout())
            .pool_max_idle_per_host(1);
        if config.base_url().scheme() == "https" {
            builder = builder.https_only(true);
        }
        let client = builder.build().map_err(|_| client_init_failed())?;

        Ok(Self {
            inner: Arc::new(ClientInner {
                config,
                authorization,
                client,
                limits,
                endpoint_origin,
            }),
        })
    }

    pub fn start_task(&self) -> ChatCompletionsProvider {
        ChatCompletionsProvider {
            client: self.clone(),
            task_budget: Arc::new(TaskByteBudget::new(
                self.inner.limits.max_task_provider_bytes(),
            )),
            last_metadata: Arc::new(Mutex::new(None)),
        }
    }

    /// Returns the configured secret boundary for model-visible runtime context.
    pub fn context_redactor(&self) -> Arc<dyn coding_agent_core::ContextRedactor> {
        Arc::new(self.inner.config.redactor())
    }

    pub fn limits(&self) -> ClientLimits {
        self.inner.limits
    }
}

impl fmt::Debug for ChatCompletionsClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChatCompletionsClient")
            .field("endpoint_origin", &self.inner.endpoint_origin)
            .field("limits", &self.inner.limits)
            .field("authorization", &"<redacted>")
            .finish_non_exhaustive()
    }
}

/// A task-scoped [`ModelProvider`] session with an independent cumulative byte budget.
#[derive(Clone)]
pub struct ChatCompletionsProvider {
    client: ChatCompletionsClient,
    task_budget: Arc<TaskByteBudget>,
    last_metadata: Arc<Mutex<Option<ProviderResponseMetadata>>>,
}

impl ChatCompletionsProvider {
    pub fn task_provider_bytes(&self) -> usize {
        self.task_budget.used()
    }

    pub fn last_response_metadata(&self) -> Option<ProviderResponseMetadata> {
        self.last_metadata
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    async fn complete_inner(
        &self,
        request: ModelRequest,
        cancellation: CancellationToken,
    ) -> Result<ModelResponse, ProviderError> {
        self.clear_metadata();
        if cancellation.is_cancelled() {
            return Err(cancelled());
        }
        let inner = &self.client.inner;
        let encoded = encode_chat_completions_request(inner.config.model(), &request)?;
        if encoded.len() > inner.limits.max_request_bytes() {
            return Err(request_limit_reached());
        }
        let metadata_redactor = request_metadata_redactor(&inner.config, &request);
        self.task_budget.charge(encoded.len())?;
        if cancellation.is_cancelled() {
            return Err(cancelled());
        }

        let pending = inner
            .client
            .post(inner.config.chat_completions_url().clone())
            .header(AUTHORIZATION, inner.authorization.clone())
            .header(ACCEPT, JSON_MEDIA_TYPE)
            .header(ACCEPT_ENCODING, "identity")
            .header(CONTENT_TYPE, JSON_MEDIA_TYPE)
            .body(encoded)
            .send();
        let response = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(cancelled()),
            result = pending => result.map_err(map_reqwest_error)?,
        };

        let status = response.status();
        self.capture_metadata(status.as_u16(), response.headers(), &metadata_redactor);
        if !status.is_success() {
            return Err(map_http_status(status.as_u16()));
        }
        if !content_encoding_is_identity(response.headers()) {
            return Err(invalid_response(
                "The provider response content encoding is unsupported.",
            ));
        }
        if !content_type_is_json(response.headers()) {
            return Err(invalid_response(
                "The provider response content type is unsupported.",
            ));
        }
        if declared_length_exceeds(response.headers(), inner.limits.max_response_bytes()) {
            return Err(response_limit_error());
        }

        let mut received = 0usize;
        let mut aggregate = Vec::with_capacity(
            response
                .content_length()
                .and_then(|length| usize::try_from(length).ok())
                .unwrap_or(0)
                .min(inner.limits.max_response_bytes()),
        );
        let mut stream = response.bytes_stream();
        loop {
            let next = tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(cancelled()),
                next = stream.next() => next,
            };
            let Some(chunk) = next else {
                break;
            };
            let chunk = chunk.map_err(map_reqwest_error)?;
            received = received
                .checked_add(chunk.len())
                .ok_or_else(response_limit_error)?;
            if received > inner.limits.max_response_bytes() {
                self.task_budget.exhaust();
                return Err(response_limit_error());
            }
            self.task_budget.charge(chunk.len())?;
            aggregate.extend_from_slice(&chunk);
        }
        let decoded =
            decode_chat_completions_response(&aggregate, inner.limits.max_response_bytes())?;
        if response_contains_secret(&decoded, inner.config.api_key().expose_secret()) {
            return Err(invalid_response(
                "The provider response contained protected configuration data.",
            ));
        }
        Ok(decoded)
    }

    fn clear_metadata(&self) {
        *self
            .last_metadata
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }

    fn capture_metadata(&self, status: u16, headers: &HeaderMap, redactor: &SecretRedactor) {
        let request_id = headers
            .get(REQUEST_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty())
            .map(|value| {
                redactor
                    .for_log_bounded(value, MAX_METADATA_BYTES)
                    .into_string()
            });
        let metadata = ProviderResponseMetadata {
            endpoint_origin: self.client.inner.endpoint_origin.clone(),
            status,
            request_id,
        };
        *self
            .last_metadata
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(metadata);
    }
}

impl fmt::Debug for ChatCompletionsProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChatCompletionsProvider")
            .field("endpoint_origin", &self.client.inner.endpoint_origin)
            .field("limits", &self.client.inner.limits)
            .field("task_provider_bytes", &self.task_provider_bytes())
            .field("authorization", &"<redacted>")
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl ModelProvider for ChatCompletionsProvider {
    async fn complete(
        &self,
        request: ModelRequest,
        cancellation: CancellationToken,
    ) -> Result<ModelResponse, ProviderError> {
        self.complete_inner(request, cancellation).await
    }
}

struct TaskByteBudget {
    used: AtomicUsize,
    max: usize,
}

impl TaskByteBudget {
    const fn new(max: usize) -> Self {
        Self {
            used: AtomicUsize::new(0),
            max,
        }
    }

    fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }

    fn charge(&self, amount: usize) -> Result<(), ProviderError> {
        let result = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(amount).filter(|next| *next <= self.max)
            });
        if result.is_err() {
            self.exhaust();
            return Err(task_byte_limit_reached());
        }
        Ok(())
    }

    fn exhaust(&self) {
        self.used.store(self.max, Ordering::Release);
    }
}

fn map_reqwest_error(error: reqwest::Error) -> ProviderError {
    if error.is_timeout() {
        map_transport_failure(TransportFailure::Timeout)
    } else if error.is_connect() {
        map_transport_failure(TransportFailure::Connect)
    } else {
        map_transport_failure(TransportFailure::Disconnected)
    }
}

fn request_metadata_redactor(config: &ProviderConfig, request: &ModelRequest) -> SecretRedactor {
    let mut redactor = config.redactor().with_secret(config.model());
    for message in &request.messages {
        redactor = match message {
            ModelMessage::System(content)
            | ModelMessage::User(content)
            | ModelMessage::Assistant(content) => redactor.with_secret(content.as_str()),
            ModelMessage::AssistantToolCall(call) => {
                with_tool_request_secrets(redactor.with_secret(call.id.as_str()), &call.request)
            }
            ModelMessage::ToolResult {
                tool_call_id,
                content,
            } => redactor
                .with_secret(tool_call_id.as_str())
                .with_secret(content.as_str()),
        };
    }
    redactor
}

fn with_tool_request_secrets(
    mut redactor: SecretRedactor,
    request: &ToolRequest,
) -> SecretRedactor {
    match request {
        ToolRequest::ListFiles { path, .. } | ToolRequest::ReadFile { path, .. } => {
            redactor = redactor.with_secret(path.as_str());
        }
        ToolRequest::SearchText {
            query, path, glob, ..
        } => {
            redactor = redactor
                .with_secret(query.as_str())
                .with_secret(path.as_str());
            if let Some(glob) = glob {
                redactor = redactor.with_secret(glob.as_str());
            }
        }
        ToolRequest::ReplaceFile {
            path,
            expected_sha256,
            content,
        } => {
            redactor = redactor
                .with_secret(path.as_str())
                .with_secret(content.as_str());
            if let Some(expected_sha256) = expected_sha256 {
                redactor = redactor.with_secret(expected_sha256.as_str());
            }
        }
        ToolRequest::CargoCheck { package, .. } => {
            if let Some(package) = package {
                redactor = redactor.with_secret(package.as_str());
            }
        }
        ToolRequest::CargoTest { package, test, .. } => {
            if let Some(package) = package {
                redactor = redactor.with_secret(package.as_str());
            }
            if let Some(test) = test {
                redactor = redactor.with_secret(test.as_str());
            }
        }
        ToolRequest::GitStatus | ToolRequest::GitDiff => {}
    }
    redactor
}

fn response_contains_secret(response: &ModelResponse, secret: &str) -> bool {
    match response {
        ModelResponse::Final { content } => content.contains(secret),
        ModelResponse::ToolCall(call) => {
            call.id.contains(secret) || tool_request_contains_secret(&call.request, secret)
        }
    }
}

fn tool_request_contains_secret(request: &ToolRequest, secret: &str) -> bool {
    match request {
        ToolRequest::ListFiles { path, .. } | ToolRequest::ReadFile { path, .. } => {
            path.contains(secret)
        }
        ToolRequest::SearchText {
            query, path, glob, ..
        } => {
            query.contains(secret)
                || path.contains(secret)
                || glob.as_deref().is_some_and(|glob| glob.contains(secret))
        }
        ToolRequest::ReplaceFile {
            path,
            expected_sha256,
            content,
        } => {
            path.contains(secret)
                || expected_sha256
                    .as_deref()
                    .is_some_and(|expected_sha256| expected_sha256.contains(secret))
                || content.contains(secret)
        }
        ToolRequest::CargoCheck { package, .. } => package
            .as_deref()
            .is_some_and(|package| package.contains(secret)),
        ToolRequest::CargoTest { package, test, .. } => {
            package
                .as_deref()
                .is_some_and(|package| package.contains(secret))
                || test.as_deref().is_some_and(|test| test.contains(secret))
        }
        ToolRequest::GitStatus | ToolRequest::GitDiff => false,
    }
}

fn content_encoding_is_identity(headers: &HeaderMap) -> bool {
    let mut values = headers.get_all(CONTENT_ENCODING).iter();
    let Some(first) = values.next() else {
        return true;
    };
    first
        .to_str()
        .is_ok_and(|value| value.eq_ignore_ascii_case("identity"))
        && values.next().is_none()
}

fn content_type_is_json(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(';')
                .next()
                .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case(JSON_MEDIA_TYPE))
        })
}

fn declared_length_exceeds(headers: &HeaderMap, max_response_bytes: usize) -> bool {
    headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|length| length > u64::try_from(max_response_bytes).unwrap_or(u64::MAX))
}

fn response_limit_error() -> ProviderError {
    invalid_response("The provider response exceeded the configured byte limit.")
}
