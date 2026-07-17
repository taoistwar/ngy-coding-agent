use std::fmt;

use coding_agent_provider::{MAX_PROVIDER_CONFIG_BYTES, PROVIDER_CONFIG_INVALID, ProviderConfig};

use crate::platform::{PlatformPaths, PrivateFileReadError, read_private_file_bounded};

const PROVIDER_CONFIG_FILE: &str = "provider.json";

pub fn load_provider_config(
    paths: &PlatformPaths,
) -> Result<ProviderConfig, ProviderConfigLoadError> {
    let encoded = read_private_file_bounded(
        &paths.data_dir.join(PROVIDER_CONFIG_FILE),
        MAX_PROVIDER_CONFIG_BYTES,
    )
    .map_err(ProviderConfigLoadError::from_private_file)?;
    ProviderConfig::from_json(&encoded)
        .map_err(|_| ProviderConfigLoadError::new(ProviderConfigLoadErrorKind::Invalid))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderConfigLoadErrorKind {
    Missing,
    NotPrivate,
    TooLarge,
    Io,
    Invalid,
}

impl ProviderConfigLoadErrorKind {
    const fn message(self) -> &'static str {
        match self {
            Self::Missing => "The private provider configuration file is missing.",
            Self::NotPrivate => "The provider configuration file is not private.",
            Self::TooLarge => "The provider configuration file is too large.",
            Self::Io => "The provider configuration file could not be read.",
            Self::Invalid => "The provider configuration file is invalid.",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ProviderConfigLoadError {
    kind: ProviderConfigLoadErrorKind,
}

impl ProviderConfigLoadError {
    const fn new(kind: ProviderConfigLoadErrorKind) -> Self {
        Self { kind }
    }

    fn from_private_file(error: PrivateFileReadError) -> Self {
        let kind = match error {
            PrivateFileReadError::Open(error) if error.kind() == std::io::ErrorKind::NotFound => {
                ProviderConfigLoadErrorKind::Missing
            }
            PrivateFileReadError::Open(error)
                if error.kind() == std::io::ErrorKind::PermissionDenied =>
            {
                ProviderConfigLoadErrorKind::NotPrivate
            }
            PrivateFileReadError::NotPrivate(_) => ProviderConfigLoadErrorKind::NotPrivate,
            PrivateFileReadError::TooLarge => ProviderConfigLoadErrorKind::TooLarge,
            PrivateFileReadError::Open(_)
            | PrivateFileReadError::Metadata(_)
            | PrivateFileReadError::Read(_) => ProviderConfigLoadErrorKind::Io,
        };
        Self::new(kind)
    }

    pub const fn kind(&self) -> ProviderConfigLoadErrorKind {
        self.kind
    }

    pub const fn code(&self) -> &'static str {
        PROVIDER_CONFIG_INVALID
    }

    pub const fn retryable(&self) -> bool {
        false
    }
}

impl fmt::Debug for ProviderConfigLoadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderConfigLoadError")
            .field("code", &PROVIDER_CONFIG_INVALID)
            .field("kind", &self.kind)
            .finish()
    }
}

impl fmt::Display for ProviderConfigLoadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.kind.message())
    }
}

impl std::error::Error for ProviderConfigLoadError {}
