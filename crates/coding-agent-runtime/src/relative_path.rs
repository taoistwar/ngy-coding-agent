use std::path::Path;

const MAX_PATH_BYTES: usize = 4_096;
const MAX_COMPONENT_BYTES: usize = 255;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RelativePath(String);

impl RelativePath {
    pub fn parse(value: impl Into<String>) -> Result<Self, RelativePathError> {
        let value = value.into();
        validate(&value)?;
        Ok(Self(value))
    }

    pub fn try_from_os_path(path: &Path) -> Result<Self, RelativePathError> {
        let value = path.to_str().ok_or(RelativePathError::NonUtf8)?;
        Self::parse(value)
    }

    pub fn is_root(&self) -> bool {
        self.0.is_empty()
    }

    pub fn as_slash_str(&self) -> &str {
        &self.0
    }

    pub fn components(&self) -> impl Iterator<Item = &str> {
        self.0.split('/').filter(|component| !component.is_empty())
    }

    pub(crate) fn join_component(&self, component: &str) -> Result<Self, RelativePathError> {
        if self.is_root() {
            Self::parse(component)
        } else {
            Self::parse(format!("{}/{component}", self.as_slash_str()))
        }
    }
}

fn validate(value: &str) -> Result<(), RelativePathError> {
    if value.len() > MAX_PATH_BYTES {
        return Err(RelativePathError::PathTooLong);
    }
    if value.starts_with('/') || has_drive_prefix(value) {
        return Err(RelativePathError::Absolute);
    }
    if value.contains('\\') {
        return Err(RelativePathError::Backslash);
    }
    if value.contains('\0') {
        return Err(RelativePathError::Nul);
    }
    if value.is_empty() {
        return Ok(());
    }

    for component in value.split('/') {
        if component.is_empty() {
            return Err(RelativePathError::EmptyComponent);
        }
        if component == "." {
            return Err(RelativePathError::CurrentDirectory);
        }
        if component == ".." {
            return Err(RelativePathError::ParentDirectory);
        }
        if component.len() > MAX_COMPONENT_BYTES {
            return Err(RelativePathError::ComponentTooLong);
        }
        if component.contains(':') {
            return Err(RelativePathError::AlternateDataStream);
        }

        let windows_equivalent = component.trim_end_matches(['.', ' ']);
        if windows_equivalent.eq_ignore_ascii_case(".git") {
            return Err(RelativePathError::ProtectedMetadata);
        }
        if windows_equivalent.len() != component.len() {
            return Err(RelativePathError::TrailingDotOrSpace);
        }
        if is_reserved_device_name(windows_equivalent) {
            return Err(RelativePathError::ReservedDeviceName);
        }
    }
    Ok(())
}

fn has_drive_prefix(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

fn is_reserved_device_name(component: &str) -> bool {
    let stem = component.split('.').next().unwrap_or(component);
    if ["CON", "PRN", "AUX", "NUL", "CLOCK$"]
        .iter()
        .any(|reserved| stem.eq_ignore_ascii_case(reserved))
    {
        return true;
    }
    let bytes = stem.as_bytes();
    bytes.len() == 4
        && (bytes[..3].eq_ignore_ascii_case(b"COM") || bytes[..3].eq_ignore_ascii_case(b"LPT"))
        && matches!(bytes[3], b'1'..=b'9')
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RelativePathError {
    #[error("path must be relative to the worktree root")]
    Absolute,
    #[error("path must use slash separators")]
    Backslash,
    #[error("path contains an empty component")]
    EmptyComponent,
    #[error("path contains a current-directory component")]
    CurrentDirectory,
    #[error("path contains a parent-directory component")]
    ParentDirectory,
    #[error("path contains a NUL byte")]
    Nul,
    #[error("path contains Windows alternate-data-stream syntax")]
    AlternateDataStream,
    #[error("Git metadata is protected")]
    ProtectedMetadata,
    #[error("path component has a trailing dot or space")]
    TrailingDotOrSpace,
    #[error("path component is a reserved Windows device name")]
    ReservedDeviceName,
    #[error("path is not valid UTF-8")]
    NonUtf8,
    #[error("path exceeds the maximum byte length")]
    PathTooLong,
    #[error("path component exceeds the maximum byte length")]
    ComponentTooLong,
}
