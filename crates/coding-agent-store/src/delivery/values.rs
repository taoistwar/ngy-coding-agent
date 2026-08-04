use std::fmt;
use std::str::FromStr;

use coding_agent_domain::{ClientRequestId, UtcTimestamp};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::DeliveryError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct DeliveryOperationId(Uuid);

impl DeliveryOperationId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for DeliveryOperationId {
    fn default() -> Self {
        Self::new()
    }
}

impl FromStr for DeliveryOperationId {
    type Err = DeliveryError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let uuid = parse_canonical_uuid(value)?;
        Ok(Self(uuid))
    }
}

impl<'de> Deserialize<'de> for DeliveryOperationId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

impl fmt::Display for DeliveryOperationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.hyphenated().fmt(formatter)
    }
}

/// Canonical, non-nil identifier for an immutable delivery command receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct DeliveryCommandId(Uuid);

impl DeliveryCommandId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for DeliveryCommandId {
    fn default() -> Self {
        Self::new()
    }
}

impl TryFrom<ClientRequestId> for DeliveryCommandId {
    type Error = DeliveryError;

    fn try_from(value: ClientRequestId) -> Result<Self, Self::Error> {
        if value.as_uuid().is_nil() {
            Err(DeliveryError::InvalidUuid)
        } else {
            Ok(Self(value.as_uuid()))
        }
    }
}

impl FromStr for DeliveryCommandId {
    type Err = DeliveryError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_canonical_uuid(value).map(Self)
    }
}

impl<'de> Deserialize<'de> for DeliveryCommandId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

impl fmt::Display for DeliveryCommandId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.hyphenated().fmt(formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GitObjectAlgorithm {
    Sha1,
    Sha256,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct GitOid(String);

impl GitOid {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub const fn algorithm(&self) -> GitObjectAlgorithm {
        match self.0.len() {
            40 => GitObjectAlgorithm::Sha1,
            64 => GitObjectAlgorithm::Sha256,
            _ => unreachable!(),
        }
    }
}

impl FromStr for GitOid {
    type Err = DeliveryError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if !matches!(value.len(), 40 | 64)
            || !is_lower_hex(value)
            || value.as_bytes().iter().all(|byte| *byte == b'0')
        {
            return Err(DeliveryError::InvalidGitOid);
        }
        Ok(Self(value.to_owned()))
    }
}

impl<'de> Deserialize<'de> for GitOid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

impl fmt::Display for GitOid {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

macro_rules! typed_oid {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(GitOid);

        impl $name {
            pub fn as_str(&self) -> &str {
                self.0.as_str()
            }

            pub const fn algorithm(&self) -> GitObjectAlgorithm {
                self.0.algorithm()
            }

            pub const fn as_oid(&self) -> &GitOid {
                &self.0
            }
        }

        impl FromStr for $name {
            type Err = DeliveryError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                value.parse().map(Self)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

typed_oid!(GitCommitOid);
typed_oid!(GitTreeOid);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct Sha256Digest(String);

impl Sha256Digest {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for Sha256Digest {
    type Err = DeliveryError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != 64 || !is_lower_hex(value) {
            return Err(DeliveryError::InvalidSha256Digest);
        }
        Ok(Self(value.to_owned()))
    }
}

impl<'de> Deserialize<'de> for Sha256Digest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

impl fmt::Display for Sha256Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct GitBranchRef(String);

impl GitBranchRef {
    pub const MAX_BYTES: usize = 4_096;

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for GitBranchRef {
    type Err = DeliveryError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if !is_valid_local_branch_ref(value) {
            return Err(DeliveryError::InvalidGitBranchRef);
        }
        Ok(Self(value.to_owned()))
    }
}

impl<'de> Deserialize<'de> for GitBranchRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

impl fmt::Display for GitBranchRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct DeliveryTimestamp(UtcTimestamp);

impl DeliveryTimestamp {
    pub const fn as_utc(self) -> UtcTimestamp {
        self.0
    }
}

impl FromStr for DeliveryTimestamp {
    type Err = DeliveryError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let timestamp =
            UtcTimestamp::parse_rfc3339(value).map_err(|_| DeliveryError::InvalidTimestamp)?;
        if timestamp.to_string() != value {
            return Err(DeliveryError::InvalidTimestamp);
        }
        Ok(Self(timestamp))
    }
}

impl<'de> Deserialize<'de> for DeliveryTimestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

impl fmt::Display for DeliveryTimestamp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct DeliveryVersion(u64);

impl DeliveryVersion {
    /// Largest integer that round-trips exactly through JSON/TypeScript numbers.
    pub const MAX: u64 = 9_007_199_254_740_991;

    pub const fn initial() -> Self {
        Self(1)
    }

    pub const fn try_new(value: u64) -> Result<Self, DeliveryError> {
        if value == 0 || value > Self::MAX {
            Err(DeliveryError::InvalidVersion)
        } else {
            Ok(Self(value))
        }
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn next(self) -> Result<Self, DeliveryError> {
        match self.0.checked_add(1) {
            Some(value) => Self::try_new(value),
            None => Err(DeliveryError::InvalidVersion),
        }
    }
}

impl<'de> Deserialize<'de> for DeliveryVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::try_new(u64::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

impl fmt::Display for DeliveryVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct FailureCode(String);

impl FailureCode {
    pub const MAX_BYTES: usize = 128;

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for FailureCode {
    type Err = DeliveryError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let bytes = value.as_bytes();
        let valid = !bytes.is_empty()
            && bytes.len() <= Self::MAX_BYTES
            && bytes.first().is_some_and(u8::is_ascii_uppercase)
            && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
            && bytes
                .iter()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || *byte == b'_');
        if !valid {
            return Err(DeliveryError::InvalidFailureCode);
        }
        Ok(Self(value.to_owned()))
    }
}

impl<'de> Deserialize<'de> for FailureCode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

impl fmt::Display for FailureCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

pub(crate) fn parse_canonical_uuid(value: &str) -> Result<Uuid, DeliveryError> {
    let uuid = Uuid::parse_str(value).map_err(|_| DeliveryError::InvalidUuid)?;
    if uuid.is_nil() || uuid.hyphenated().to_string() != value {
        return Err(DeliveryError::InvalidUuid);
    }
    Ok(uuid)
}

fn is_lower_hex(value: &str) -> bool {
    value
        .as_bytes()
        .iter()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
}

fn is_valid_local_branch_ref(value: &str) -> bool {
    let Some(short) = value.strip_prefix("refs/heads/") else {
        return false;
    };
    if short.is_empty()
        || value.len() > GitBranchRef::MAX_BYTES
        || short.starts_with('-')
        || value.ends_with('/')
        || value.ends_with('.')
        || value.contains("..")
        || value.contains("@{")
        || value.contains("//")
        || value.chars().any(|character| {
            character.is_control()
                || character == ' '
                || matches!(character, '~' | '^' | ':' | '?' | '*' | '[' | '\\')
        })
    {
        return false;
    }

    short.split('/').all(|component| {
        !component.is_empty() && !component.starts_with('.') && !component.ends_with(".lock")
    })
}
