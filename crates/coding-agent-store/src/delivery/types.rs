use std::fmt;
use std::str::FromStr;

use coding_agent_domain::{EventId, MAX_WORKSPACE_GENERATION, RepositoryId, TaskId};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::values::parse_canonical_uuid;
use super::{DeliveryError, Sha256Digest};

pub const EVIDENCE_IDENTITY_ALGORITHM_V1: &str = "evidence_identity_v1";
pub const DIRECTORY_IDENTITY_ALGORITHM_V1: &str = "directory_identity_v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DeliveryIdentity {
    task_id: TaskId,
    repository_id: RepositoryId,
    attempt: u32,
}

impl DeliveryIdentity {
    pub fn try_new(
        task_id: TaskId,
        repository_id: RepositoryId,
        attempt: u32,
    ) -> Result<Self, DeliveryError> {
        if task_id.as_uuid().is_nil() || repository_id.as_uuid().is_nil() || attempt == 0 {
            return Err(DeliveryError::InvalidIdentity);
        }
        Ok(Self {
            task_id,
            repository_id,
            attempt,
        })
    }

    pub(crate) fn try_from_text(
        task_id: &str,
        repository_id: &str,
        attempt: u32,
    ) -> Result<Self, DeliveryError> {
        parse_canonical_uuid(task_id)?;
        parse_canonical_uuid(repository_id)?;
        let task_id = TaskId::from_str(task_id).map_err(|_| DeliveryError::InvalidIdentity)?;
        let repository_id =
            RepositoryId::from_str(repository_id).map_err(|_| DeliveryError::InvalidIdentity)?;
        Self::try_new(task_id, repository_id, attempt)
    }

    pub const fn task_id(self) -> TaskId {
        self.task_id
    }

    pub const fn repository_id(self) -> RepositoryId {
        self.repository_id
    }

    pub const fn attempt(self) -> u32 {
        self.attempt
    }
}

#[derive(Serialize)]
struct SerializableDeliveryIdentity {
    task_id: String,
    repository_id: String,
    attempt: u32,
}

impl Serialize for DeliveryIdentity {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        SerializableDeliveryIdentity {
            task_id: self.task_id.to_string(),
            repository_id: self.repository_id.to_string(),
            attempt: self.attempt,
        }
        .serialize(serializer)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDeliveryIdentity {
    task_id: String,
    repository_id: String,
    attempt: u32,
}

impl<'de> Deserialize<'de> for DeliveryIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawDeliveryIdentity::deserialize(deserializer)?;
        Self::try_from_text(&raw.task_id, &raw.repository_id, raw.attempt).map_err(D::Error::custom)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct EvidenceIdentityV1 {
    identity: DeliveryIdentity,
    final_review_round: u8,
    final_review_event_id: EventId,
    workspace_generation: u64,
    workspace_fingerprint: Sha256Digest,
    checks_digest: Sha256Digest,
    coverage_digest: Sha256Digest,
}

impl fmt::Debug for EvidenceIdentityV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EvidenceIdentityV1")
            .field("identity", &self.identity)
            .field("final_review_round", &self.final_review_round)
            .field("final_review_event_id", &self.final_review_event_id)
            .field("workspace_generation", &self.workspace_generation)
            .field("workspace_fingerprint", &"<redacted>")
            .field("checks_digest", &"<redacted>")
            .field("coverage_digest", &"<redacted>")
            .finish()
    }
}

impl EvidenceIdentityV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        identity: DeliveryIdentity,
        final_review_round: u8,
        final_review_event_id: EventId,
        workspace_generation: u64,
        workspace_fingerprint: Sha256Digest,
        checks_digest: Sha256Digest,
        coverage_digest: Sha256Digest,
    ) -> Result<Self, DeliveryError> {
        if !(1..=3).contains(&final_review_round) || workspace_generation > MAX_WORKSPACE_GENERATION
        {
            return Err(DeliveryError::InvalidEvidenceIdentity);
        }
        Ok(Self {
            identity,
            final_review_round,
            final_review_event_id,
            workspace_generation,
            workspace_fingerprint,
            checks_digest,
            coverage_digest,
        })
    }

    pub const fn algorithm(&self) -> &'static str {
        EVIDENCE_IDENTITY_ALGORITHM_V1
    }

    pub const fn identity(&self) -> DeliveryIdentity {
        self.identity
    }

    pub const fn final_review_round(&self) -> u8 {
        self.final_review_round
    }

    pub const fn final_review_event_id(&self) -> EventId {
        self.final_review_event_id
    }

    pub const fn workspace_generation(&self) -> u64 {
        self.workspace_generation
    }

    pub const fn workspace_fingerprint(&self) -> &Sha256Digest {
        &self.workspace_fingerprint
    }

    pub const fn checks_digest(&self) -> &Sha256Digest {
        &self.checks_digest
    }

    pub const fn coverage_digest(&self) -> &Sha256Digest {
        &self.coverage_digest
    }
}

#[derive(Serialize)]
struct EvidenceIdentityRef<'a> {
    algorithm: &'static str,
    task_id: String,
    repository_id: String,
    attempt: u32,
    final_review_round: u8,
    final_review_event_id: i64,
    workspace_generation: u64,
    workspace_fingerprint: &'a Sha256Digest,
    checks_digest: &'a Sha256Digest,
    coverage_digest: &'a Sha256Digest,
}

impl Serialize for EvidenceIdentityV1 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        EvidenceIdentityRef {
            algorithm: EVIDENCE_IDENTITY_ALGORITHM_V1,
            task_id: self.identity.task_id.to_string(),
            repository_id: self.identity.repository_id.to_string(),
            attempt: self.identity.attempt,
            final_review_round: self.final_review_round,
            final_review_event_id: self.final_review_event_id.get(),
            workspace_generation: self.workspace_generation,
            workspace_fingerprint: &self.workspace_fingerprint,
            checks_digest: &self.checks_digest,
            coverage_digest: &self.coverage_digest,
        }
        .serialize(serializer)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawEvidenceIdentity {
    algorithm: String,
    task_id: String,
    repository_id: String,
    attempt: u32,
    final_review_round: u8,
    final_review_event_id: i64,
    workspace_generation: u64,
    workspace_fingerprint: Sha256Digest,
    checks_digest: Sha256Digest,
    coverage_digest: Sha256Digest,
}

impl<'de> Deserialize<'de> for EvidenceIdentityV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawEvidenceIdentity::deserialize(deserializer)?;
        if raw.algorithm != EVIDENCE_IDENTITY_ALGORITHM_V1 {
            return Err(D::Error::custom(DeliveryError::InvalidEvidenceIdentity));
        }
        let identity =
            DeliveryIdentity::try_from_text(&raw.task_id, &raw.repository_id, raw.attempt)
                .map_err(D::Error::custom)?;
        let event_id = EventId::new(raw.final_review_event_id)
            .map_err(|_| D::Error::custom(DeliveryError::InvalidEvidenceIdentity))?;
        Self::try_new(
            identity,
            raw.final_review_round,
            event_id,
            raw.workspace_generation,
            raw.workspace_fingerprint,
            raw.checks_digest,
            raw.coverage_digest,
        )
        .map_err(D::Error::custom)
    }
}

/// Durable, domain-separated directory identity used only at persistence/runtime seams.
///
/// It intentionally has no general-purpose Serde implementation. Persistence code must
/// bind the two validated storage parts directly, and API projections must define a
/// separate DTO that never exposes either value.
///
/// The validated storage parts are exposed together so runtime recovery adapters
/// cannot accidentally persist or compare a digest without its domain separator.
///
/// ```
/// use coding_agent_store::DirectoryIdentity;
///
/// let identity = DirectoryIdentity::try_new(
///     "directory_identity_v1",
///     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
/// )?;
/// assert_eq!(identity.storage_parts().0, "directory_identity_v1");
/// # Ok::<(), coding_agent_store::DeliveryError>(())
/// ```
#[derive(Clone, PartialEq, Eq)]
pub struct DirectoryIdentity {
    pub(crate) digest: Sha256Digest,
}

impl std::fmt::Debug for DirectoryIdentity {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DirectoryIdentity")
            .field("algorithm", &DIRECTORY_IDENTITY_ALGORITHM_V1)
            .field("digest", &"<redacted>")
            .finish()
    }
}

impl DirectoryIdentity {
    pub fn try_new(algorithm: &str, digest: &str) -> Result<Self, DeliveryError> {
        if algorithm != DIRECTORY_IDENTITY_ALGORITHM_V1 {
            return Err(DeliveryError::InvalidDirectoryIdentity);
        }
        Ok(Self {
            digest: digest.parse()?,
        })
    }

    pub const fn algorithm(&self) -> &'static str {
        DIRECTORY_IDENTITY_ALGORITHM_V1
    }

    /// Returns the validated, domain-separated persistence representation.
    pub fn storage_parts(&self) -> (&'static str, &str) {
        (DIRECTORY_IDENTITY_ALGORITHM_V1, self.digest.as_str())
    }
}
