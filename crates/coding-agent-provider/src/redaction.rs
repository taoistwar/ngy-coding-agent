use std::fmt;
use std::sync::LazyLock;

use regex::Regex;

const MAX_REDACTED_BOUNDARY_BYTES: usize = 4 * 1024;

static SECRET_HEADER: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)((?:authorization|x-api-key|api-key|x-auth-token)\s*[:=]\s*)(?:bearer\s+)?[^\s,;]+",
    )
    .expect("secret header redaction regex")
});
static JSON_SECRET: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?i)("(?:api[_-]?key|access[_-]?token|token|secret|password|authorization)"\s*:\s*")[^"]*(")"#,
    )
    .expect("JSON secret redaction regex")
});
static QUERY_SECRET: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)([?&](?:api[_-]?key|access[_-]?token|token|secret|password)=)[^&#\s]+")
        .expect("query secret redaction regex")
});

#[derive(Clone, Default, PartialEq, Eq)]
pub struct SecretRedactor {
    secrets: Vec<String>,
}

impl SecretRedactor {
    pub const fn new() -> Self {
        Self {
            secrets: Vec::new(),
        }
    }

    pub fn with_secret(mut self, secret: impl Into<String>) -> Self {
        let secret = secret.into();
        if !secret.is_empty() && !self.secrets.iter().any(|existing| existing == &secret) {
            self.secrets.push(secret);
            self.secrets
                .sort_unstable_by_key(|value| std::cmp::Reverse(value.len()));
        }
        self
    }

    pub fn for_log(&self, raw: &str) -> RedactedText {
        self.for_log_bounded(raw, MAX_REDACTED_BOUNDARY_BYTES)
    }

    pub fn for_log_bounded(&self, raw: &str, max_bytes: usize) -> RedactedText {
        RedactedText(truncate_utf8(self.redact(raw), max_bytes))
    }

    pub fn for_user(&self, raw: &str) -> RedactedText {
        self.for_user_bounded(raw, MAX_REDACTED_BOUNDARY_BYTES)
    }

    pub fn for_user_bounded(&self, raw: &str, max_bytes: usize) -> RedactedText {
        RedactedText(truncate_utf8(self.redact(raw), max_bytes))
    }

    fn redact(&self, raw: &str) -> String {
        let mut value = raw.to_owned();
        for secret in &self.secrets {
            value = value.replace(secret, "<redacted>");
        }
        value = SECRET_HEADER
            .replace_all(&value, "${1}<redacted>")
            .into_owned();
        value = JSON_SECRET
            .replace_all(&value, "${1}<redacted>${2}")
            .into_owned();
        QUERY_SECRET
            .replace_all(&value, "${1}<redacted>")
            .into_owned()
    }
}

impl fmt::Debug for SecretRedactor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecretRedactor")
            .field("secrets", &"<redacted>")
            .finish()
    }
}

impl fmt::Display for SecretRedactor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("secret redactor (<redacted>)")
    }
}

impl coding_agent_core::ContextRedactor for SecretRedactor {
    fn redact(&self, content: &str) -> String {
        SecretRedactor::redact(self, content)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct RedactedText(String);

impl RedactedText {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Debug for RedactedText {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("RedactedText")
            .field(&self.0)
            .finish()
    }
}

impl fmt::Display for RedactedText {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

fn truncate_utf8(mut value: String, max_bytes: usize) -> String {
    const SUFFIX: &str = "<truncated>";
    if value.len() <= max_bytes {
        return value;
    }
    if max_bytes <= SUFFIX.len() {
        let mut end = max_bytes;
        while !SUFFIX.is_char_boundary(end) {
            end -= 1;
        }
        return SUFFIX[..end].to_owned();
    }

    let mut end = max_bytes - SUFFIX.len();
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
    value.push_str(SUFFIX);
    value
}
