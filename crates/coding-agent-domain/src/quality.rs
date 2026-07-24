use std::collections::HashSet;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};

use crate::{DomainError, UtcTimestamp};

const MAX_JSON_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
pub const MAX_WORKSPACE_GENERATION: u64 = 9_007_199_254_740_991;
pub const MAX_CARGO_SELECTOR_BYTES: usize = 128;
const MAX_REVIEW_EVIDENCE_BYTES: usize = 128 * 1024;
const MAX_REQUIRED_CHECKS: usize = 16;
const MAX_FINDINGS: usize = 32;
const MAX_REVIEW_SUMMARY_SCALARS: usize = 4_096;
const MAX_FINDING_MESSAGE_SCALARS: usize = 2_048;
const MAX_CHECK_SUMMARY_BYTES: usize = 2_048;
const MAX_REVIEW_CHUNKS: u8 = 8;
const SYSTEM_WORKSPACE_CHANGED_MESSAGE: &str =
    "Workspace changed during review; review evidence was invalidated";

#[derive(Default)]
enum RequiredNullable<T> {
    #[default]
    Missing,
    Present(Option<T>),
}

impl<T> RequiredNullable<T> {
    fn into_option(self) -> Result<Option<T>, DomainError> {
        match self {
            Self::Missing => Err(DomainError::InvalidQualityEvidence),
            Self::Present(value) => Ok(value),
        }
    }
}

impl<'de, T> Deserialize<'de> for RequiredNullable<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<T>::deserialize(deserializer).map(Self::Present)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkspaceDigestAlgorithm {
    #[serde(rename = "workspace_fingerprint_v1")]
    WorkspaceFingerprintV1,
}

impl WorkspaceDigestAlgorithm {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WorkspaceFingerprintV1 => "workspace_fingerprint_v1",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WorkspaceDigest {
    algorithm: WorkspaceDigestAlgorithm,
    value: String,
}

impl WorkspaceDigest {
    pub fn try_new(value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into();
        if !is_lower_hex_64(&value) {
            return Err(DomainError::InvalidQualityEvidence);
        }
        Ok(Self {
            algorithm: WorkspaceDigestAlgorithm::WorkspaceFingerprintV1,
            value,
        })
    }

    pub const fn algorithm(&self) -> &'static str {
        self.algorithm.as_str()
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawWorkspaceDigest {
    algorithm: WorkspaceDigestAlgorithm,
    value: String,
}

impl<'de> Deserialize<'de> for WorkspaceDigest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawWorkspaceDigest::deserialize(deserializer)?;
        if raw.algorithm != WorkspaceDigestAlgorithm::WorkspaceFingerprintV1 {
            return Err(D::Error::custom(DomainError::InvalidQualityEvidence));
        }
        Self::try_new(raw.value).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequiredCheck {
    id: String,
    selector: RequiredCheckSelector,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RequiredCheckKind {
    CargoCheck,
    CargoTest,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RequiredCheckSelector {
    value: RequiredCheckSelectorValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum RequiredCheckSelectorValue {
    CargoCheck {
        package: Option<String>,
    },
    CargoTest {
        package: Option<String>,
        integration_test: Option<String>,
    },
}

impl RequiredCheckSelector {
    pub fn try_cargo_check(package: Option<String>) -> Result<Self, DomainError> {
        validate_package(package.as_deref())?;
        Ok(Self {
            value: RequiredCheckSelectorValue::CargoCheck { package },
        })
    }

    pub fn try_cargo_test(
        package: Option<String>,
        integration_test: Option<String>,
    ) -> Result<Self, DomainError> {
        validate_package(package.as_deref())?;
        if integration_test
            .as_deref()
            .is_some_and(|value| !is_valid_cargo_selector(value))
            || (integration_test.is_some() && package.is_none())
        {
            return Err(DomainError::InvalidQualityEvidence);
        }
        Ok(Self {
            value: RequiredCheckSelectorValue::CargoTest {
                package,
                integration_test,
            },
        })
    }

    pub const fn kind(&self) -> RequiredCheckKind {
        match self.value {
            RequiredCheckSelectorValue::CargoCheck { .. } => RequiredCheckKind::CargoCheck,
            RequiredCheckSelectorValue::CargoTest { .. } => RequiredCheckKind::CargoTest,
        }
    }

    pub fn package(&self) -> Option<&str> {
        match &self.value {
            RequiredCheckSelectorValue::CargoCheck { package }
            | RequiredCheckSelectorValue::CargoTest { package, .. } => package.as_deref(),
        }
    }

    pub fn integration_test(&self) -> Option<&str> {
        match &self.value {
            RequiredCheckSelectorValue::CargoCheck { .. } => None,
            RequiredCheckSelectorValue::CargoTest {
                integration_test, ..
            } => integration_test.as_deref(),
        }
    }
}

impl RequiredCheck {
    pub fn try_cargo_check(
        id: impl Into<String>,
        package: Option<String>,
    ) -> Result<Self, DomainError> {
        Self::try_from_selector(id, RequiredCheckSelector::try_cargo_check(package)?)
    }

    pub fn try_cargo_test(
        id: impl Into<String>,
        package: Option<String>,
        integration_test: Option<String>,
    ) -> Result<Self, DomainError> {
        Self::try_from_selector(
            id,
            RequiredCheckSelector::try_cargo_test(package, integration_test)?,
        )
    }

    pub fn try_from_selector(
        id: impl Into<String>,
        selector: RequiredCheckSelector,
    ) -> Result<Self, DomainError> {
        let id = id.into();
        if id.is_empty() {
            return Err(DomainError::InvalidQualityEvidence);
        }
        Ok(Self { id, selector })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub const fn is_cargo_test(&self) -> bool {
        matches!(self.selector.kind(), RequiredCheckKind::CargoTest)
    }

    pub const fn selector(&self) -> &RequiredCheckSelector {
        &self.selector
    }

    pub fn package(&self) -> Option<&str> {
        self.selector.package()
    }

    pub fn integration_test(&self) -> Option<&str> {
        self.selector.integration_test()
    }

    fn same_selector(&self, other: &Self) -> bool {
        self.selector == other.selector
    }
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum RequiredCheckRef<'a> {
    CargoCheck {
        id: &'a str,
        package: Option<&'a str>,
    },
    CargoTest {
        id: &'a str,
        package: Option<&'a str>,
        integration_test: Option<&'a str>,
    },
}

impl Serialize for RequiredCheck {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match &self.selector.value {
            RequiredCheckSelectorValue::CargoCheck { package } => RequiredCheckRef::CargoCheck {
                id: &self.id,
                package: package.as_deref(),
            },
            RequiredCheckSelectorValue::CargoTest {
                package,
                integration_test,
            } => RequiredCheckRef::CargoTest {
                id: &self.id,
                package: package.as_deref(),
                integration_test: integration_test.as_deref(),
            },
        }
        .serialize(serializer)
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum RawRequiredCheck {
    CargoCheck {
        id: String,
        #[serde(default)]
        package: RequiredNullable<String>,
    },
    CargoTest {
        id: String,
        #[serde(default)]
        package: RequiredNullable<String>,
        #[serde(default)]
        integration_test: RequiredNullable<String>,
    },
}

impl<'de> Deserialize<'de> for RequiredCheck {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match RawRequiredCheck::deserialize(deserializer)? {
            RawRequiredCheck::CargoCheck { id, package } => {
                Self::try_cargo_check(id, package.into_option().map_err(D::Error::custom)?)
                    .map_err(D::Error::custom)
            }
            RawRequiredCheck::CargoTest {
                id,
                package,
                integration_test,
            } => Self::try_cargo_test(
                id,
                package.into_option().map_err(D::Error::custom)?,
                integration_test.into_option().map_err(D::Error::custom)?,
            )
            .map_err(D::Error::custom),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckActor {
    Executor,
    Reviewer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckEvidenceStatus {
    Passed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CheckEvidence {
    check_id: String,
    actor: CheckActor,
    role_run: u32,
    workspace_generation: u64,
    workspace_digest: WorkspaceDigest,
    status: CheckEvidenceStatus,
    duration_ms: u64,
    summary: String,
    truncated: bool,
}

impl CheckEvidence {
    #[allow(clippy::too_many_arguments)]
    pub fn try_for_check(
        check: &RequiredCheck,
        actor: CheckActor,
        role_run: u32,
        workspace_generation: u64,
        workspace_digest: WorkspaceDigest,
        status: CheckEvidenceStatus,
        duration_ms: u64,
        summary: impl Into<String>,
        truncated: bool,
    ) -> Result<Self, DomainError> {
        Self::try_from_check_id(
            check.id.clone(),
            actor,
            role_run,
            workspace_generation,
            workspace_digest,
            status,
            duration_ms,
            summary,
            truncated,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn try_from_check_id(
        check_id: String,
        actor: CheckActor,
        role_run: u32,
        workspace_generation: u64,
        workspace_digest: WorkspaceDigest,
        status: CheckEvidenceStatus,
        duration_ms: u64,
        summary: impl Into<String>,
        truncated: bool,
    ) -> Result<Self, DomainError> {
        let summary = summary.into();
        if check_id.is_empty()
            || role_run == 0
            || workspace_generation > MAX_WORKSPACE_GENERATION
            || duration_ms > MAX_JSON_SAFE_INTEGER
            || summary.is_empty()
            || summary.len() > MAX_CHECK_SUMMARY_BYTES
        {
            return Err(DomainError::InvalidQualityEvidence);
        }
        Ok(Self {
            check_id,
            actor,
            role_run,
            workspace_generation,
            workspace_digest,
            status,
            duration_ms,
            summary,
            truncated,
        })
    }

    pub fn check_id(&self) -> &str {
        &self.check_id
    }

    pub const fn actor(&self) -> CheckActor {
        self.actor
    }

    pub const fn role_run(&self) -> u32 {
        self.role_run
    }

    pub const fn workspace_generation(&self) -> u64 {
        self.workspace_generation
    }

    pub const fn workspace_digest(&self) -> &WorkspaceDigest {
        &self.workspace_digest
    }

    pub const fn status(&self) -> CheckEvidenceStatus {
        self.status
    }

    pub const fn duration_ms(&self) -> u64 {
        self.duration_ms
    }

    pub fn summary(&self) -> &str {
        &self.summary
    }

    pub const fn truncated(&self) -> bool {
        self.truncated
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCheckEvidence {
    check_id: String,
    actor: CheckActor,
    role_run: u32,
    workspace_generation: u64,
    workspace_digest: WorkspaceDigest,
    status: CheckEvidenceStatus,
    duration_ms: u64,
    summary: String,
    truncated: bool,
}

impl<'de> Deserialize<'de> for CheckEvidence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawCheckEvidence::deserialize(deserializer)?;
        Self::try_from_check_id(
            raw.check_id,
            raw.actor,
            raw.role_run,
            raw.workspace_generation,
            raw.workspace_digest,
            raw.status,
            raw.duration_ms,
            raw.summary,
            raw.truncated,
        )
        .map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingSeverity {
    Blocking,
    Advisory,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReviewFinding {
    id: String,
    severity: FindingSeverity,
    message: String,
    path: Option<String>,
    line: Option<u64>,
}

impl ReviewFinding {
    pub fn try_for_review(
        round: u8,
        ordinal: usize,
        severity: FindingSeverity,
        message: impl Into<String>,
        path: Option<String>,
        line: Option<u64>,
    ) -> Result<Self, DomainError> {
        if !(1..=3).contains(&round) || !(1..=MAX_FINDINGS).contains(&ordinal) {
            return Err(DomainError::InvalidQualityEvidence);
        }
        Self::try_from_parts(
            expected_finding_id(round, ordinal),
            severity,
            message,
            path,
            line,
        )
    }

    fn try_from_parts(
        id: String,
        severity: FindingSeverity,
        message: impl Into<String>,
        path: Option<String>,
        line: Option<u64>,
    ) -> Result<Self, DomainError> {
        let message = message.into();
        if !is_valid_finding_id(&id)
            || message.is_empty()
            || message.chars().count() > MAX_FINDING_MESSAGE_SCALARS
            || line == Some(0)
            || line.is_some_and(|value| value > MAX_JSON_SAFE_INTEGER)
            || (line.is_some() && path.is_none())
            || path
                .as_deref()
                .is_some_and(|value| !is_valid_review_path(value))
        {
            return Err(DomainError::InvalidQualityEvidence);
        }
        Ok(Self {
            id,
            severity,
            message,
            path,
            line,
        })
    }

    pub fn system_workspace_changed(round: u8) -> Result<Self, DomainError> {
        if !(1..=3).contains(&round) {
            return Err(DomainError::InvalidQualityEvidence);
        }
        Self::try_for_review(
            round,
            1,
            FindingSeverity::Blocking,
            SYSTEM_WORKSPACE_CHANGED_MESSAGE,
            None,
            None,
        )
    }

    fn is_system_workspace_changed(&self, round: u8) -> bool {
        self.id == expected_finding_id(round, 1)
            && self.severity == FindingSeverity::Blocking
            && self.message == SYSTEM_WORKSPACE_CHANGED_MESSAGE
            && self.path.is_none()
            && self.line.is_none()
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn matches_review_position(&self, round: u8, ordinal: usize) -> bool {
        self.id == expected_finding_id(round, ordinal)
    }

    pub const fn severity(&self) -> FindingSeverity {
        self.severity
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }

    pub const fn line(&self) -> Option<u64> {
        self.line
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawReviewFinding {
    id: String,
    severity: FindingSeverity,
    message: String,
    #[serde(default)]
    path: RequiredNullable<String>,
    #[serde(default)]
    line: RequiredNullable<u64>,
}

impl<'de> Deserialize<'de> for ReviewFinding {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawReviewFinding::deserialize(deserializer)?;
        Self::try_from_parts(
            raw.id,
            raw.severity,
            raw.message,
            raw.path.into_option().map_err(D::Error::custom)?,
            raw.line.into_option().map_err(D::Error::custom)?,
        )
        .map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReviewCoverageEvidence {
    generation: u64,
    workspace_digest: WorkspaceDigest,
    manifest_sha256: String,
    covered_chunks: Vec<u8>,
    total_chunks: u8,
}

impl ReviewCoverageEvidence {
    pub fn try_new(
        generation: u64,
        workspace_digest: WorkspaceDigest,
        manifest_sha256: impl Into<String>,
        covered_chunks: Vec<u8>,
        total_chunks: u8,
    ) -> Result<Self, DomainError> {
        let manifest_sha256 = manifest_sha256.into();
        if generation > MAX_WORKSPACE_GENERATION
            || !is_lower_hex_64(&manifest_sha256)
            || total_chunks > MAX_REVIEW_CHUNKS
            || covered_chunks
                .windows(2)
                .any(|window| window[0] >= window[1])
            || covered_chunks.iter().any(|chunk| *chunk >= total_chunks)
        {
            return Err(DomainError::InvalidQualityEvidence);
        }
        Ok(Self {
            generation,
            workspace_digest,
            manifest_sha256,
            covered_chunks,
            total_chunks,
        })
    }

    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub const fn workspace_digest(&self) -> &WorkspaceDigest {
        &self.workspace_digest
    }

    pub fn manifest_sha256(&self) -> &str {
        &self.manifest_sha256
    }

    pub fn covered_chunks(&self) -> &[u8] {
        &self.covered_chunks
    }

    pub const fn total_chunks(&self) -> u8 {
        self.total_chunks
    }

    pub fn is_complete(&self) -> bool {
        self.covered_chunks.len() == usize::from(self.total_chunks)
            && self.covered_chunks.iter().copied().eq(0..self.total_chunks)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawReviewCoverageEvidence {
    generation: u64,
    workspace_digest: WorkspaceDigest,
    manifest_sha256: String,
    covered_chunks: Vec<u8>,
    total_chunks: u8,
}

impl<'de> Deserialize<'de> for ReviewCoverageEvidence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawReviewCoverageEvidence::deserialize(deserializer)?;
        Self::try_new(
            raw.generation,
            raw.workspace_digest,
            raw.manifest_sha256,
            raw.covered_chunks,
            raw.total_chunks,
        )
        .map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewDecisionSource {
    Reviewer,
    System,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewVerdict {
    Approved,
    ChangesRequested,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NewReviewEvidence {
    round: u8,
    decision_source: ReviewDecisionSource,
    workspace_generation: u64,
    workspace_digest: WorkspaceDigest,
    verdict: ReviewVerdict,
    summary: String,
    findings: Vec<ReviewFinding>,
    added_required_checks: Vec<RequiredCheck>,
    required_checks: Vec<RequiredCheck>,
    check_evidence: Vec<CheckEvidence>,
    coverage: Option<ReviewCoverageEvidence>,
}

impl NewReviewEvidence {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        round: u8,
        decision_source: ReviewDecisionSource,
        workspace_generation: u64,
        workspace_digest: WorkspaceDigest,
        verdict: ReviewVerdict,
        summary: impl Into<String>,
        findings: Vec<ReviewFinding>,
        added_required_checks: Vec<RequiredCheck>,
        required_checks: Vec<RequiredCheck>,
        check_evidence: Vec<CheckEvidence>,
        coverage: Option<ReviewCoverageEvidence>,
    ) -> Result<Self, DomainError> {
        let summary = summary.into();
        let candidate = Self {
            round,
            decision_source,
            workspace_generation,
            workspace_digest,
            verdict,
            summary,
            findings,
            added_required_checks,
            required_checks,
            check_evidence,
            coverage,
        };
        candidate.validate()?;
        candidate.validate_encoded_size()?;
        Ok(candidate)
    }

    fn validate(&self) -> Result<(), DomainError> {
        if !(1..=3).contains(&self.round)
            || self.workspace_generation > MAX_WORKSPACE_GENERATION
            || self.summary.is_empty()
            || self.summary.chars().count() > MAX_REVIEW_SUMMARY_SCALARS
            || self.findings.len() > MAX_FINDINGS
            || !(1..=MAX_REQUIRED_CHECKS).contains(&self.required_checks.len())
            || self.added_required_checks.len() > MAX_REQUIRED_CHECKS
            || self.check_evidence.len() > MAX_REQUIRED_CHECKS
        {
            return Err(DomainError::InvalidQualityEvidence);
        }

        validate_findings(self.round, &self.findings)?;
        validate_required_checks(&self.required_checks, true)?;
        validate_required_checks(&self.added_required_checks, false)?;

        if !is_ordered_subset(&self.added_required_checks, &self.required_checks) {
            return Err(DomainError::InvalidQualityEvidence);
        }

        let required_ids = self
            .required_checks
            .iter()
            .map(RequiredCheck::id)
            .collect::<HashSet<_>>();
        let mut evidence_ids = HashSet::new();
        let mut previous_required_index = None;
        for evidence in &self.check_evidence {
            let required_index = self
                .required_checks
                .iter()
                .position(|required| required.id() == evidence.check_id())
                .ok_or(DomainError::InvalidQualityEvidence)?;
            if !evidence_ids.insert(evidence.check_id())
                || !required_ids.contains(evidence.check_id())
                || previous_required_index.is_some_and(|previous| required_index <= previous)
                || evidence.workspace_generation() != self.workspace_generation
                || evidence.workspace_digest() != &self.workspace_digest
            {
                return Err(DomainError::InvalidQualityEvidence);
            }
            previous_required_index = Some(required_index);
        }

        if self.coverage.as_ref().is_some_and(|coverage| {
            coverage.generation() != self.workspace_generation
                || coverage.workspace_digest() != &self.workspace_digest
        }) {
            return Err(DomainError::InvalidQualityEvidence);
        }

        let has_blocking = self
            .findings
            .iter()
            .any(|finding| finding.severity() == FindingSeverity::Blocking);
        match self.verdict {
            ReviewVerdict::Approved => {
                if self.decision_source != ReviewDecisionSource::Reviewer
                    || has_blocking
                    || !self
                        .coverage
                        .as_ref()
                        .is_some_and(ReviewCoverageEvidence::is_complete)
                    || self.check_evidence.len() != self.required_checks.len()
                    || self
                        .check_evidence
                        .iter()
                        .any(|evidence| evidence.status() != CheckEvidenceStatus::Passed)
                {
                    return Err(DomainError::InvalidQualityEvidence);
                }
            }
            ReviewVerdict::ChangesRequested => {
                if !has_blocking {
                    return Err(DomainError::InvalidQualityEvidence);
                }
            }
        }

        if self.decision_source == ReviewDecisionSource::System
            && (self.verdict != ReviewVerdict::ChangesRequested
                || self.findings.len() != 1
                || !self.findings[0].is_system_workspace_changed(self.round)
                || self.coverage.is_some()
                || !self.check_evidence.is_empty())
        {
            return Err(DomainError::InvalidQualityEvidence);
        }

        Ok(())
    }

    fn validate_encoded_size(&self) -> Result<(), DomainError> {
        if serde_json::to_vec(self)
            .map_err(|_| DomainError::InvalidQualityEvidence)?
            .len()
            > MAX_REVIEW_EVIDENCE_BYTES
        {
            return Err(DomainError::InvalidQualityEvidence);
        }
        Ok(())
    }

    pub const fn round(&self) -> u8 {
        self.round
    }

    pub const fn decision_source(&self) -> ReviewDecisionSource {
        self.decision_source
    }

    pub const fn workspace_generation(&self) -> u64 {
        self.workspace_generation
    }

    pub const fn workspace_digest(&self) -> &WorkspaceDigest {
        &self.workspace_digest
    }

    pub const fn verdict(&self) -> ReviewVerdict {
        self.verdict
    }

    pub fn summary(&self) -> &str {
        &self.summary
    }

    pub fn findings(&self) -> &[ReviewFinding] {
        &self.findings
    }

    pub fn added_required_checks(&self) -> &[RequiredCheck] {
        &self.added_required_checks
    }

    pub fn required_checks(&self) -> &[RequiredCheck] {
        &self.required_checks
    }

    pub fn check_evidence(&self) -> &[CheckEvidence] {
        &self.check_evidence
    }

    pub const fn coverage(&self) -> Option<&ReviewCoverageEvidence> {
        self.coverage.as_ref()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawNewReviewEvidence {
    round: u8,
    decision_source: ReviewDecisionSource,
    workspace_generation: u64,
    workspace_digest: WorkspaceDigest,
    verdict: ReviewVerdict,
    summary: String,
    findings: Vec<ReviewFinding>,
    added_required_checks: Vec<RequiredCheck>,
    required_checks: Vec<RequiredCheck>,
    check_evidence: Vec<CheckEvidence>,
    #[serde(default)]
    coverage: RequiredNullable<ReviewCoverageEvidence>,
}

impl TryFrom<RawNewReviewEvidence> for NewReviewEvidence {
    type Error = DomainError;

    fn try_from(raw: RawNewReviewEvidence) -> Result<Self, Self::Error> {
        Self::try_new(
            raw.round,
            raw.decision_source,
            raw.workspace_generation,
            raw.workspace_digest,
            raw.verdict,
            raw.summary,
            raw.findings,
            raw.added_required_checks,
            raw.required_checks,
            raw.check_evidence,
            raw.coverage.into_option()?,
        )
    }
}

impl<'de> Deserialize<'de> for NewReviewEvidence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        RawNewReviewEvidence::deserialize(deserializer)?
            .try_into()
            .map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReviewEvidence {
    round: u8,
    decision_source: ReviewDecisionSource,
    workspace_generation: u64,
    workspace_digest: WorkspaceDigest,
    verdict: ReviewVerdict,
    summary: String,
    findings: Vec<ReviewFinding>,
    added_required_checks: Vec<RequiredCheck>,
    required_checks: Vec<RequiredCheck>,
    check_evidence: Vec<CheckEvidence>,
    coverage: Option<ReviewCoverageEvidence>,
    created_at: UtcTimestamp,
}

impl ReviewEvidence {
    pub fn try_from_new(
        new: NewReviewEvidence,
        created_at: UtcTimestamp,
    ) -> Result<Self, DomainError> {
        let evidence = Self {
            round: new.round,
            decision_source: new.decision_source,
            workspace_generation: new.workspace_generation,
            workspace_digest: new.workspace_digest,
            verdict: new.verdict,
            summary: new.summary,
            findings: new.findings,
            added_required_checks: new.added_required_checks,
            required_checks: new.required_checks,
            check_evidence: new.check_evidence,
            coverage: new.coverage,
            created_at,
        };
        if serde_json::to_vec(&evidence)
            .map_err(|_| DomainError::InvalidQualityEvidence)?
            .len()
            > MAX_REVIEW_EVIDENCE_BYTES
        {
            return Err(DomainError::InvalidQualityEvidence);
        }
        Ok(evidence)
    }

    pub const fn round(&self) -> u8 {
        self.round
    }

    pub const fn decision_source(&self) -> ReviewDecisionSource {
        self.decision_source
    }

    pub const fn workspace_generation(&self) -> u64 {
        self.workspace_generation
    }

    pub const fn workspace_digest(&self) -> &WorkspaceDigest {
        &self.workspace_digest
    }

    pub const fn verdict(&self) -> ReviewVerdict {
        self.verdict
    }

    pub fn summary(&self) -> &str {
        &self.summary
    }

    pub fn findings(&self) -> &[ReviewFinding] {
        &self.findings
    }

    pub fn added_required_checks(&self) -> &[RequiredCheck] {
        &self.added_required_checks
    }

    pub fn required_checks(&self) -> &[RequiredCheck] {
        &self.required_checks
    }

    pub fn check_evidence(&self) -> &[CheckEvidence] {
        &self.check_evidence
    }

    pub const fn coverage(&self) -> Option<&ReviewCoverageEvidence> {
        self.coverage.as_ref()
    }

    pub const fn created_at(&self) -> UtcTimestamp {
        self.created_at
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawReviewEvidence {
    round: u8,
    decision_source: ReviewDecisionSource,
    workspace_generation: u64,
    workspace_digest: WorkspaceDigest,
    verdict: ReviewVerdict,
    summary: String,
    findings: Vec<ReviewFinding>,
    added_required_checks: Vec<RequiredCheck>,
    required_checks: Vec<RequiredCheck>,
    check_evidence: Vec<CheckEvidence>,
    #[serde(default)]
    coverage: RequiredNullable<ReviewCoverageEvidence>,
    created_at: UtcTimestamp,
}

impl<'de> Deserialize<'de> for ReviewEvidence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawReviewEvidence::deserialize(deserializer)?;
        let new = NewReviewEvidence::try_new(
            raw.round,
            raw.decision_source,
            raw.workspace_generation,
            raw.workspace_digest,
            raw.verdict,
            raw.summary,
            raw.findings,
            raw.added_required_checks,
            raw.required_checks,
            raw.check_evidence,
            raw.coverage.into_option().map_err(D::Error::custom)?,
        )
        .map_err(D::Error::custom)?;
        Self::try_from_new(new, raw.created_at).map_err(D::Error::custom)
    }
}

fn validate_package(package: Option<&str>) -> Result<(), DomainError> {
    if package.is_some_and(|value| !is_valid_cargo_selector(value)) {
        return Err(DomainError::InvalidQualityEvidence);
    }
    Ok(())
}

fn validate_findings(round: u8, findings: &[ReviewFinding]) -> Result<(), DomainError> {
    let mut ids = HashSet::new();
    if findings.iter().enumerate().any(|(index, finding)| {
        finding.id() != expected_finding_id(round, index + 1) || !ids.insert(finding.id())
    }) {
        return Err(DomainError::InvalidQualityEvidence);
    }
    Ok(())
}

fn expected_finding_id(round: u8, ordinal: usize) -> String {
    format!("review-{round}-finding-{ordinal}")
}

fn is_valid_finding_id(value: &str) -> bool {
    let Some(suffix) = value.strip_prefix("review-") else {
        return false;
    };
    let Some((round, ordinal)) = suffix.split_once("-finding-") else {
        return false;
    };
    let (Ok(round), Ok(ordinal)) = (round.parse::<u8>(), ordinal.parse::<usize>()) else {
        return false;
    };
    (1..=3).contains(&round)
        && (1..=MAX_FINDINGS).contains(&ordinal)
        && value == expected_finding_id(round, ordinal)
}

pub fn is_valid_cargo_selector(value: &str) -> bool {
    let bytes = value.as_bytes();
    (1..=MAX_CARGO_SELECTOR_BYTES).contains(&bytes.len())
        && matches!(bytes[0], b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_')
        && bytes
            .iter()
            .all(|byte| matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-'))
}

fn validate_required_checks(
    checks: &[RequiredCheck],
    require_cargo_test: bool,
) -> Result<(), DomainError> {
    let mut ids = HashSet::new();
    for (index, check) in checks.iter().enumerate() {
        if !ids.insert(check.id())
            || checks[..index]
                .iter()
                .any(|existing| existing.same_selector(check))
        {
            return Err(DomainError::InvalidQualityEvidence);
        }
    }
    if require_cargo_test && !checks.iter().any(RequiredCheck::is_cargo_test) {
        return Err(DomainError::InvalidQualityEvidence);
    }
    Ok(())
}

fn is_ordered_subset(subset: &[RequiredCheck], full: &[RequiredCheck]) -> bool {
    let mut full_index = 0;
    for expected in subset {
        let Some(relative_index) = full[full_index..]
            .iter()
            .position(|candidate| candidate == expected)
        else {
            return false;
        };
        full_index += relative_index + 1;
    }
    true
}

fn is_lower_hex_64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_valid_review_path(value: &str) -> bool {
    const MAX_PATH_BYTES: usize = 4_096;
    const MAX_COMPONENT_BYTES: usize = 255;

    if value.is_empty()
        || value.len() > MAX_PATH_BYTES
        || value.starts_with('/')
        || has_drive_prefix(value)
        || value.contains(['\\', '\0'])
    {
        return false;
    }
    value.split('/').all(|component| {
        if component.is_empty()
            || component == "."
            || component == ".."
            || component.len() > MAX_COMPONENT_BYTES
            || component.contains(':')
        {
            return false;
        }
        let windows_equivalent = component.trim_end_matches(['.', ' ']);
        windows_equivalent.len() == component.len()
            && !windows_equivalent.eq_ignore_ascii_case(".git")
            && !is_reserved_device_name(windows_equivalent)
    })
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
