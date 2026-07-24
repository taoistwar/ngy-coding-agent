use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use time::format_description::FormatItem;
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;
use time::{OffsetDateTime, UtcOffset};

const UTC_TIMESTAMP_FORMAT: &[FormatItem<'static>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:9]Z");

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DomainError {
    #[error("prompt must contain between 1 and 50,000 Unicode scalar values")]
    InvalidPrompt,
    #[error("canonical path must be absolute and normalized")]
    InvalidCanonicalPath,
    #[error("timestamp must be valid RFC 3339")]
    InvalidTimestamp,
    #[error("event ID must be positive")]
    InvalidEventId,
    #[error("event cursor must be nonnegative")]
    InvalidEventCursor,
    #[error("task attempt must be at least one")]
    InvalidTaskAttempt,
    #[error("task fields do not match its status")]
    InvalidTaskState,
    #[error("quality evidence violates the domain contract")]
    InvalidQualityEvidence,
    #[error("plan violates the domain contract")]
    InvalidPlan,
    #[error("activity violates the domain contract")]
    InvalidActivity,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct CanonicalPath(PathBuf);

impl CanonicalPath {
    pub fn try_from_canonical(path: impl Into<PathBuf>) -> Result<Self, DomainError> {
        let path = path.into();
        let normalized = path.components().collect::<PathBuf>();
        let has_forbidden_component = path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir));

        if !path.is_absolute()
            || has_forbidden_component
            || normalized.as_os_str() != path.as_os_str()
        {
            return Err(DomainError::InvalidCanonicalPath);
        }

        Ok(Self(path))
    }

    pub fn as_path(&self) -> &Path {
        &self.0
    }
}

impl<'de> Deserialize<'de> for CanonicalPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let path = PathBuf::deserialize(deserializer)?;
        Self::try_from_canonical(path).map_err(D::Error::custom)
    }
}

impl fmt::Display for CanonicalPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.display().fmt(formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UtcTimestamp(OffsetDateTime);

impl UtcTimestamp {
    pub fn new(value: OffsetDateTime) -> Result<Self, DomainError> {
        let value = value.to_offset(UtcOffset::UTC);
        if (0..=9999).contains(&value.year()) {
            Ok(Self(value))
        } else {
            Err(DomainError::InvalidTimestamp)
        }
    }

    pub fn parse_rfc3339(value: &str) -> Result<Self, DomainError> {
        OffsetDateTime::parse(value, &Rfc3339)
            .map_err(|_| DomainError::InvalidTimestamp)
            .and_then(Self::new)
    }

    pub const fn as_offset_date_time(self) -> OffsetDateTime {
        self.0
    }
}

impl FromStr for UtcTimestamp {
    type Err = DomainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse_rfc3339(value)
    }
}

impl fmt::Display for UtcTimestamp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = self
            .0
            .format(UTC_TIMESTAMP_FORMAT)
            .map_err(|_| fmt::Error)?;
        formatter.write_str(&value)
    }
}

impl Serialize for UtcTimestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for UtcTimestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse_rfc3339(&value).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct EventId(i64);

impl EventId {
    pub const fn new(value: i64) -> Result<Self, DomainError> {
        if value > 0 {
            Ok(Self(value))
        } else {
            Err(DomainError::InvalidEventId)
        }
    }

    pub const fn get(self) -> i64 {
        self.0
    }
}

impl<'de> Deserialize<'de> for EventId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(i64::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct EventCursor(i64);

impl EventCursor {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: i64) -> Result<Self, DomainError> {
        if value >= 0 {
            Ok(Self(value))
        } else {
            Err(DomainError::InvalidEventCursor)
        }
    }

    pub const fn get(self) -> i64 {
        self.0
    }
}

impl<'de> Deserialize<'de> for EventCursor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(i64::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

impl fmt::Display for EventCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}
