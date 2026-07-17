use std::fmt;

use serde::Deserialize;
use url::{Host, Url};

use crate::PROVIDER_CONFIG_INVALID;
use crate::redaction::SecretRedactor;

pub const MAX_PROVIDER_CONFIG_BYTES: usize = 16 * 1024;
const MAX_BASE_URL_BYTES: usize = 2_048;
const MAX_MODEL_BYTES: usize = 256;
const MAX_API_KEY_BYTES: usize = 4_096;
/// Minimum length accepted for an API key so exact-match redaction cannot become pathological.
pub const MIN_PROVIDER_API_KEY_BYTES: usize = 8;

#[derive(Clone, PartialEq, Eq)]
pub struct ApiKey(String);

impl ApiKey {
    pub(crate) fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ApiKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ApiKey(<redacted>)")
    }
}

impl fmt::Display for ApiKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ProviderConfig {
    base_url: Url,
    chat_completions_url: Url,
    model: String,
    api_key: ApiKey,
}

impl ProviderConfig {
    pub fn from_json(encoded: &[u8]) -> Result<Self, ProviderConfigError> {
        Self::parse(encoded, BaseUrlPolicy::HttpsOnly)
    }

    /// Explicit escape hatch for local mock servers in tests.
    ///
    /// Production configuration loading must use [`ProviderConfig::from_json`]. The exception is
    /// deliberately limited to IP-literal loopback hosts so DNS resolution cannot widen it.
    #[doc(hidden)]
    pub fn from_json_allow_loopback_http_for_test(
        encoded: &[u8],
    ) -> Result<Self, ProviderConfigError> {
        Self::parse(encoded, BaseUrlPolicy::HttpsOrLoopbackHttpForTests)
    }

    pub fn base_url(&self) -> &Url {
        &self.base_url
    }

    pub fn chat_completions_url(&self) -> &Url {
        &self.chat_completions_url
    }

    pub fn model(&self) -> &str {
        &self.model
    }

    pub fn api_key(&self) -> &ApiKey {
        &self.api_key
    }

    pub fn redactor(&self) -> SecretRedactor {
        SecretRedactor::new().with_secret(self.api_key.expose_secret())
    }

    fn parse(encoded: &[u8], policy: BaseUrlPolicy) -> Result<Self, ProviderConfigError> {
        if encoded.len() > MAX_PROVIDER_CONFIG_BYTES {
            return Err(ProviderConfigError::new(
                ProviderConfigErrorReason::DocumentTooLarge,
            ));
        }
        let wire: ProviderConfigWire = serde_json::from_slice(encoded)
            .map_err(|_| ProviderConfigError::new(ProviderConfigErrorReason::InvalidDocument))?;

        let base_url = validate_base_url(&wire.base_url, policy)?;
        if wire.model.is_empty()
            || wire.model.len() > MAX_MODEL_BYTES
            || wire.model.trim() != wire.model
            || wire.model.chars().any(char::is_control)
        {
            return Err(ProviderConfigError::new(
                ProviderConfigErrorReason::InvalidModel,
            ));
        }
        if wire.api_key.len() < MIN_PROVIDER_API_KEY_BYTES
            || wire.api_key.len() > MAX_API_KEY_BYTES
            || !wire
                .api_key
                .bytes()
                .all(|byte| (0x21..=0x7e).contains(&byte))
        {
            return Err(ProviderConfigError::new(
                ProviderConfigErrorReason::InvalidApiKey,
            ));
        }

        let chat_completions_url = base_url
            .join("v1/chat/completions")
            .map_err(|_| ProviderConfigError::new(ProviderConfigErrorReason::InvalidBaseUrl))?;
        Ok(Self {
            base_url,
            chat_completions_url,
            model: wire.model,
            api_key: ApiKey(wire.api_key),
        })
    }
}

impl fmt::Debug for ProviderConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderConfig")
            .field("base_url", &"<redacted>")
            .field("model", &"<redacted>")
            .field("api_key", &"<redacted>")
            .finish()
    }
}

impl fmt::Display for ProviderConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("provider configuration (<redacted>)")
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderConfigWire {
    base_url: String,
    model: String,
    api_key: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BaseUrlPolicy {
    HttpsOnly,
    HttpsOrLoopbackHttpForTests,
}

fn validate_base_url(raw: &str, policy: BaseUrlPolicy) -> Result<Url, ProviderConfigError> {
    if raw.is_empty() || raw.len() > MAX_BASE_URL_BYTES || raw.chars().any(char::is_control) {
        return Err(ProviderConfigError::new(
            ProviderConfigErrorReason::InvalidBaseUrl,
        ));
    }
    let mut url = Url::parse(raw)
        .map_err(|_| ProviderConfigError::new(ProviderConfigErrorReason::InvalidBaseUrl))?;
    if url.cannot_be_a_base() || url.host().is_none() {
        return Err(ProviderConfigError::new(
            ProviderConfigErrorReason::InvalidBaseUrl,
        ));
    }
    if authority_contains_userinfo(raw) || !url.username().is_empty() || url.password().is_some() {
        return Err(ProviderConfigError::new(
            ProviderConfigErrorReason::BaseUrlUserInfo,
        ));
    }
    if url.query().is_some() {
        return Err(ProviderConfigError::new(
            ProviderConfigErrorReason::BaseUrlQuery,
        ));
    }
    if url.fragment().is_some() {
        return Err(ProviderConfigError::new(
            ProviderConfigErrorReason::BaseUrlFragment,
        ));
    }

    match url.scheme() {
        "https" => {}
        "http"
            if policy == BaseUrlPolicy::HttpsOrLoopbackHttpForTests
                && is_ip_literal_loopback(&url) => {}
        _ => {
            return Err(ProviderConfigError::new(
                ProviderConfigErrorReason::InsecureBaseUrl,
            ));
        }
    }

    if !url.path().ends_with('/') {
        let mut normalized_path = url.path().to_owned();
        normalized_path.push('/');
        url.set_path(&normalized_path);
    }
    Ok(url)
}

fn authority_contains_userinfo(raw: &str) -> bool {
    let Some((_, after_scheme)) = raw.split_once("://") else {
        return false;
    };
    after_scheme
        .split(['/', '?', '#'])
        .next()
        .is_some_and(|authority| authority.contains('@'))
}

fn is_ip_literal_loopback(url: &Url) -> bool {
    match url.host() {
        Some(Host::Ipv4(address)) => address.is_loopback(),
        Some(Host::Ipv6(address)) => address.is_loopback(),
        _ => false,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderConfigErrorReason {
    DocumentTooLarge,
    InvalidDocument,
    InvalidBaseUrl,
    InsecureBaseUrl,
    BaseUrlUserInfo,
    BaseUrlQuery,
    BaseUrlFragment,
    InvalidModel,
    InvalidApiKey,
}

impl ProviderConfigErrorReason {
    const fn message(self) -> &'static str {
        match self {
            Self::DocumentTooLarge => "the configuration document is too large",
            Self::InvalidDocument => "the configuration document does not match the schema",
            Self::InvalidBaseUrl => "the provider base URL is invalid",
            Self::InsecureBaseUrl => "the provider base URL must use HTTPS",
            Self::BaseUrlUserInfo => "the provider base URL must not contain user information",
            Self::BaseUrlQuery => "the provider base URL must not contain a query",
            Self::BaseUrlFragment => "the provider base URL must not contain a fragment",
            Self::InvalidModel => "the provider model is invalid",
            Self::InvalidApiKey => "the provider API key is invalid",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("provider configuration is invalid: {}", .reason.message())]
pub struct ProviderConfigError {
    reason: ProviderConfigErrorReason,
}

impl ProviderConfigError {
    const fn new(reason: ProviderConfigErrorReason) -> Self {
        Self { reason }
    }

    pub const fn code(&self) -> &'static str {
        PROVIDER_CONFIG_INVALID
    }

    pub const fn reason(&self) -> ProviderConfigErrorReason {
        self.reason
    }

    pub const fn retryable(&self) -> bool {
        false
    }
}
