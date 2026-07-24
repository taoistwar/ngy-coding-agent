use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};

use coding_agent_domain::{
    CheckActor, CheckEvidence, CheckEvidenceStatus, MAX_WORKSPACE_GENERATION, NewReviewEvidence,
    RequiredCheck, RequiredCheckKind, ReviewVerdict, TestCase, TestSnapshot, TestStatus,
    WorkspaceDigest,
};
use thiserror::Error;

use crate::WorkspaceFingerprint;

const MAX_REQUIRED_CHECKS: usize = 16;
const QUEUED_SUMMARY: &str = "Awaiting current-generation evidence";
const RUNNING_SUMMARY: &str = "Check is running";

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum QualityStateError {
    #[error("workspace generation exceeds the supported maximum")]
    InvalidWorkspaceGeneration,
    #[error("workspace generation cannot be incremented safely")]
    WorkspaceGenerationOverflow,
    #[error("current check evidence does not match the workspace checkpoint")]
    ObservationCheckpointMismatch,
    #[error("the required-check set must contain at least one cargo_test")]
    CargoTestRequired,
    #[error("the required-check set contains a duplicate check id")]
    DuplicateCheckId,
    #[error("the required-check set contains a duplicate selector")]
    DuplicateCheckSelector,
    #[error("the required-check set exceeds sixteen unique checks")]
    TooManyRequiredChecks,
    #[error("the requested check is not required")]
    UnknownCheck,
    #[error("the requested check already has a current queued or running attempt")]
    CheckAlreadyActive,
    #[error("the requested check is not queued for the current checkpoint")]
    CheckNotQueued,
    #[error("the requested check is not running for the current checkpoint")]
    CheckNotRunning,
    #[error("the check role run must be greater than zero")]
    InvalidCheckRoleRun,
    #[error("a unique check attempt id could not be allocated")]
    CheckAttemptOverflow,
    #[error("the check run token does not identify the current running attempt")]
    StaleCheckRunToken,
    #[error("the terminal check fields are invalid")]
    InvalidTerminalObservation,
    #[error("not every required check has latest current passed evidence")]
    ChecksNotPassed,
    #[error("review evidence does not match the current workspace checkpoint")]
    ReviewCheckpointMismatch,
    #[error("review required checks do not exactly match ledger order")]
    ReviewRequiredChecksMismatch,
    #[error("review check evidence is not the exact current terminal projection")]
    ReviewCheckEvidenceMismatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointChange {
    Unchanged,
    Advanced { generation: u64 },
}

#[derive(Debug, PartialEq, Eq)]
pub struct WorkspaceCheckpoint {
    generation: u64,
    fingerprint: WorkspaceFingerprint,
    current_check_observations: HashMap<String, CheckEvidence>,
}

impl WorkspaceCheckpoint {
    pub fn new(fingerprint: WorkspaceFingerprint) -> Self {
        Self {
            generation: 0,
            fingerprint,
            current_check_observations: HashMap::new(),
        }
    }

    pub fn try_at_generation(
        generation: u64,
        fingerprint: WorkspaceFingerprint,
    ) -> Result<Self, QualityStateError> {
        if generation > MAX_WORKSPACE_GENERATION {
            return Err(QualityStateError::InvalidWorkspaceGeneration);
        }

        Ok(Self {
            generation,
            fingerprint,
            current_check_observations: HashMap::new(),
        })
    }

    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub const fn fingerprint(&self) -> WorkspaceFingerprint {
        self.fingerprint
    }

    pub fn workspace_digest(&self) -> WorkspaceDigest {
        digest_for_fingerprint(self.fingerprint)
    }

    pub fn current_observation(&self, check_id: &str) -> Option<&CheckEvidence> {
        self.current_check_observations.get(check_id)
    }

    pub fn current_observations(&self) -> impl Iterator<Item = &CheckEvidence> {
        self.current_check_observations.values()
    }

    pub fn observe_stable(
        &mut self,
        fingerprint: WorkspaceFingerprint,
    ) -> Result<CheckpointChange, QualityStateError> {
        if fingerprint == self.fingerprint {
            return Ok(CheckpointChange::Unchanged);
        }

        let generation = self
            .generation
            .checked_add(1)
            .filter(|generation| *generation <= MAX_WORKSPACE_GENERATION)
            .ok_or(QualityStateError::WorkspaceGenerationOverflow)?;
        self.generation = generation;
        self.fingerprint = fingerprint;
        self.current_check_observations.clear();
        Ok(CheckpointChange::Advanced { generation })
    }

    fn remove_observation(&mut self, check_id: &str) {
        self.current_check_observations.remove(check_id);
    }

    fn record_observation(&mut self, observation: CheckEvidence) -> Result<(), QualityStateError> {
        if !self.matches_observation(&observation) {
            return Err(QualityStateError::ObservationCheckpointMismatch);
        }
        self.current_check_observations
            .insert(observation.check_id().to_owned(), observation);
        Ok(())
    }

    fn matches_observation(&self, observation: &CheckEvidence) -> bool {
        observation.workspace_generation() == self.generation
            && observation.workspace_digest() == &self.workspace_digest()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActiveCheckStatus {
    Queued,
    Running,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveCheck {
    attempt_id: u64,
    generation: u64,
    workspace_digest: WorkspaceDigest,
    status: ActiveCheckStatus,
    actor: Option<CheckActor>,
    role_run: Option<u32>,
}

impl ActiveCheck {
    fn queued(attempt_id: u64, checkpoint: &WorkspaceCheckpoint) -> Self {
        Self {
            attempt_id,
            generation: checkpoint.generation(),
            workspace_digest: checkpoint.workspace_digest(),
            status: ActiveCheckStatus::Queued,
            actor: None,
            role_run: None,
        }
    }

    fn matches(&self, checkpoint: &WorkspaceCheckpoint) -> bool {
        self.generation == checkpoint.generation()
            && self.workspace_digest == checkpoint.workspace_digest()
    }

    fn matches_token(&self, token: &CheckRunToken) -> bool {
        self.attempt_id == token.attempt_id
            && self.generation == token.generation
            && self.workspace_digest == token.workspace_digest
            && self.status == ActiveCheckStatus::Running
            && self.actor == Some(token.actor)
            && self.role_run == Some(token.role_run)
    }
}

/// Linear authority to complete one exact check attempt.
///
/// Fields are private and the token is intentionally neither `Clone` nor
/// `Copy`: only `RequiredCheckLedger::mark_check_running` can mint it, and
/// `finish_check` consumes it.
#[derive(Debug, PartialEq, Eq)]
pub struct CheckRunToken {
    check_id: String,
    attempt_id: u64,
    generation: u64,
    workspace_digest: WorkspaceDigest,
    actor: CheckActor,
    role_run: u32,
}

#[derive(Debug, PartialEq, Eq)]
pub struct RequiredCheckLedger {
    checks: Vec<RequiredCheck>,
    active_checks: HashMap<String, ActiveCheck>,
}

impl RequiredCheckLedger {
    pub fn try_new(checks: Vec<RequiredCheck>) -> Result<Self, QualityStateError> {
        validate_initial_checks(&checks)?;
        Ok(Self {
            checks,
            active_checks: HashMap::new(),
        })
    }

    pub fn checks(&self) -> &[RequiredCheck] {
        &self.checks
    }

    pub fn check_by_selector(
        &self,
        selector: &coding_agent_domain::RequiredCheckSelector,
    ) -> Option<&RequiredCheck> {
        self.checks
            .iter()
            .find(|check| check.selector() == selector)
    }

    pub fn check_by_id(&self, check_id: &str) -> Option<&RequiredCheck> {
        self.checks.iter().find(|check| check.id() == check_id)
    }

    /// Atomically appends checks in their supplied creation order.
    ///
    /// Selectors already present in the ledger or repeated in the same batch
    /// are folded into the existing CheckId and do not count as additions.
    pub fn append_checks(
        &mut self,
        additions: Vec<RequiredCheck>,
    ) -> Result<usize, QualityStateError> {
        let mut ids = self
            .checks
            .iter()
            .map(|check| (check.id().to_owned(), check.selector().clone()))
            .collect::<HashMap<_, _>>();
        let mut selectors = self
            .checks
            .iter()
            .map(|check| check.selector().clone())
            .collect::<HashSet<_>>();
        let mut accepted = Vec::new();

        for addition in additions {
            if let Some(existing_selector) = ids.get(addition.id()) {
                if existing_selector == addition.selector() {
                    continue;
                }
                return Err(QualityStateError::DuplicateCheckId);
            }
            if selectors.contains(addition.selector()) {
                continue;
            }

            ids.insert(addition.id().to_owned(), addition.selector().clone());
            selectors.insert(addition.selector().clone());
            accepted.push(addition);
        }

        if self
            .checks
            .len()
            .checked_add(accepted.len())
            .is_none_or(|count| count > MAX_REQUIRED_CHECKS)
        {
            return Err(QualityStateError::TooManyRequiredChecks);
        }

        let added = accepted.len();
        self.checks.extend(accepted);
        Ok(added)
    }

    /// Rolls back only the exact unaccepted tail of a Reviewer terminal
    /// candidate. These checks must never have been queued or observed.
    pub(crate) fn rollback_unaccepted_review_checks(
        &mut self,
        checkpoint: &WorkspaceCheckpoint,
        additions: &[RequiredCheck],
    ) -> Result<(), QualityStateError> {
        if additions.is_empty() {
            return Ok(());
        }
        let start = self
            .checks
            .len()
            .checked_sub(additions.len())
            .ok_or(QualityStateError::UnknownCheck)?;
        if self.checks[start..] != *additions
            || additions.iter().any(|check| {
                self.active_checks.contains_key(check.id())
                    || checkpoint.current_observation(check.id()).is_some()
            })
        {
            return Err(QualityStateError::ReviewRequiredChecksMismatch);
        }
        self.checks.truncate(start);
        Ok(())
    }

    /// Revokes any terminal evidence before a check is scheduled to rerun.
    pub fn queue_check(
        &mut self,
        checkpoint: &mut WorkspaceCheckpoint,
        check_id: &str,
    ) -> Result<(), QualityStateError> {
        self.require_check(check_id)?;
        if self
            .active_checks
            .get(check_id)
            .is_some_and(|active| active.matches(checkpoint))
        {
            return Err(QualityStateError::CheckAlreadyActive);
        }

        let attempt_id = allocate_check_attempt_id()?;
        checkpoint.remove_observation(check_id);
        self.active_checks.insert(
            check_id.to_owned(),
            ActiveCheck::queued(attempt_id, checkpoint),
        );
        Ok(())
    }

    pub fn mark_check_running(
        &mut self,
        checkpoint: &WorkspaceCheckpoint,
        check_id: &str,
        actor: CheckActor,
        role_run: u32,
    ) -> Result<CheckRunToken, QualityStateError> {
        self.require_check(check_id)?;
        if role_run == 0 {
            return Err(QualityStateError::InvalidCheckRoleRun);
        }
        let active = self
            .active_checks
            .get_mut(check_id)
            .ok_or(QualityStateError::CheckNotQueued)?;
        if !active.matches(checkpoint) || active.status != ActiveCheckStatus::Queued {
            return Err(QualityStateError::CheckNotQueued);
        }
        active.status = ActiveCheckStatus::Running;
        active.actor = Some(actor);
        active.role_run = Some(role_run);
        Ok(CheckRunToken {
            check_id: check_id.to_owned(),
            attempt_id: active.attempt_id,
            generation: active.generation,
            workspace_digest: active.workspace_digest.clone(),
            actor,
            role_run,
        })
    }

    /// Releases one exact queued attempt when its queued projection could not
    /// be emitted.
    pub fn abandon_queued_check(
        &mut self,
        checkpoint: &WorkspaceCheckpoint,
        check_id: &str,
    ) -> Result<(), QualityStateError> {
        self.require_check(check_id)?;
        let active = self
            .active_checks
            .get(check_id)
            .ok_or(QualityStateError::CheckNotQueued)?;
        if !active.matches(checkpoint) || active.status != ActiveCheckStatus::Queued {
            return Err(QualityStateError::CheckNotQueued);
        }
        self.active_checks.remove(check_id);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn finish_check(
        &mut self,
        checkpoint: &mut WorkspaceCheckpoint,
        token: CheckRunToken,
        status: CheckEvidenceStatus,
        duration_ms: u64,
        summary: impl Into<String>,
        truncated: bool,
    ) -> Result<(), QualityStateError> {
        let check = self.require_check(&token.check_id)?;
        if token.generation != checkpoint.generation()
            || token.workspace_digest != checkpoint.workspace_digest()
        {
            return Err(QualityStateError::ObservationCheckpointMismatch);
        }

        let active = self
            .active_checks
            .get(&token.check_id)
            .ok_or(QualityStateError::StaleCheckRunToken)?;
        if !active.matches(checkpoint) || !active.matches_token(&token) {
            return Err(QualityStateError::StaleCheckRunToken);
        }

        let observation = match CheckEvidence::try_for_check(
            check,
            token.actor,
            token.role_run,
            token.generation,
            token.workspace_digest.clone(),
            status,
            duration_ms,
            summary,
            truncated,
        ) {
            Ok(observation) => observation,
            Err(_) => {
                self.active_checks.remove(&token.check_id);
                return Err(QualityStateError::InvalidTerminalObservation);
            }
        };
        let check_id = token.check_id;
        checkpoint.record_observation(observation)?;
        self.active_checks.remove(&check_id);
        Ok(())
    }

    /// Releases one exact queued/running attempt when its trusted runtime
    /// failed before producing terminal evidence.
    pub fn abandon_check_run(&mut self, token: CheckRunToken) -> Result<(), QualityStateError> {
        self.require_check(&token.check_id)?;
        let active = self
            .active_checks
            .get(&token.check_id)
            .ok_or(QualityStateError::StaleCheckRunToken)?;
        if !active.matches_token(&token) {
            return Err(QualityStateError::StaleCheckRunToken);
        }
        self.active_checks.remove(&token.check_id);
        Ok(())
    }

    pub fn all_current_checks_passed(&self, checkpoint: &WorkspaceCheckpoint) -> bool {
        self.checks.iter().all(|check| {
            !self.is_active_for_current_checkpoint(check.id(), checkpoint)
                && checkpoint
                    .current_observation(check.id())
                    .is_some_and(|observation| {
                        checkpoint.matches_observation(observation)
                            && observation.status() == CheckEvidenceStatus::Passed
                    })
        })
    }

    /// Returns every current terminal observation in required-check creation order.
    /// Missing or actively rerunning checks are omitted; failed and cancelled
    /// observations remain visible so review evidence cannot selectively hide them.
    pub fn current_evidence(&self, checkpoint: &WorkspaceCheckpoint) -> Vec<CheckEvidence> {
        self.checks
            .iter()
            .filter(|check| !self.is_active_for_current_checkpoint(check.id(), checkpoint))
            .filter_map(|check| checkpoint.current_observation(check.id()))
            .filter(|observation| checkpoint.matches_observation(observation))
            .cloned()
            .collect()
    }

    pub fn approval_evidence(
        &self,
        checkpoint: &WorkspaceCheckpoint,
    ) -> Result<Vec<CheckEvidence>, QualityStateError> {
        if !self.all_current_checks_passed(checkpoint) {
            return Err(QualityStateError::ChecksNotPassed);
        }
        Ok(self.current_evidence(checkpoint))
    }

    /// Binds a validated domain review to this exact checkpoint and ledger.
    ///
    /// This prevents a `changes_requested` review from omitting current
    /// failures/cancellations and prevents any verdict from replaying evidence
    /// that was superseded by a later attempt on the same workspace digest.
    pub fn validate_review_evidence(
        &self,
        checkpoint: &WorkspaceCheckpoint,
        review: &NewReviewEvidence,
    ) -> Result<(), QualityStateError> {
        if review.workspace_generation() != checkpoint.generation()
            || review.workspace_digest() != &checkpoint.workspace_digest()
        {
            return Err(QualityStateError::ReviewCheckpointMismatch);
        }
        if review.required_checks() != self.checks() {
            return Err(QualityStateError::ReviewRequiredChecksMismatch);
        }

        let current_evidence = self.current_evidence(checkpoint);
        if review.check_evidence() != current_evidence {
            return Err(QualityStateError::ReviewCheckEvidenceMismatch);
        }
        if review.verdict() == ReviewVerdict::Approved
            && !self.all_current_checks_passed(checkpoint)
        {
            return Err(QualityStateError::ChecksNotPassed);
        }
        Ok(())
    }

    fn require_check(&self, check_id: &str) -> Result<&RequiredCheck, QualityStateError> {
        self.checks
            .iter()
            .find(|check| check.id() == check_id)
            .ok_or(QualityStateError::UnknownCheck)
    }

    fn current_active_status(
        &self,
        check_id: &str,
        checkpoint: &WorkspaceCheckpoint,
    ) -> Option<ActiveCheckStatus> {
        self.active_checks
            .get(check_id)
            .filter(|active| active.matches(checkpoint))
            .map(|active| active.status)
    }

    fn is_active_for_current_checkpoint(
        &self,
        check_id: &str,
        checkpoint: &WorkspaceCheckpoint,
    ) -> bool {
        self.current_active_status(check_id, checkpoint).is_some()
    }
}

/// The single canonical projection of current required-check state.
pub fn project_test_snapshot(
    ledger: &RequiredCheckLedger,
    checkpoint: &WorkspaceCheckpoint,
) -> TestSnapshot {
    let cases = ledger
        .checks()
        .iter()
        .map(|check| project_test_case(ledger, checkpoint, check))
        .collect::<Vec<_>>();
    let status = aggregate_status(&cases);
    TestSnapshot {
        revision: checkpoint.generation(),
        status,
        cases,
    }
}

/// Conservatively projects required checks when a fresh stable terminal
/// snapshot could not be obtained. No prior-generation success, failure, or
/// active state is treated as current evidence.
pub fn project_unverified_test_snapshot(
    ledger: &RequiredCheckLedger,
    best_known_generation: u64,
) -> TestSnapshot {
    let cases = ledger
        .checks()
        .iter()
        .map(|check| TestCase {
            id: check.id().to_owned(),
            name: display_name(check),
            status: TestStatus::Queued,
            duration_ms: 0,
            summary: QUEUED_SUMMARY.to_owned(),
        })
        .collect::<Vec<_>>();
    TestSnapshot {
        revision: best_known_generation,
        status: TestStatus::Queued,
        cases,
    }
}

fn project_test_case(
    ledger: &RequiredCheckLedger,
    checkpoint: &WorkspaceCheckpoint,
    check: &RequiredCheck,
) -> TestCase {
    let (status, duration_ms, summary) = match ledger.current_active_status(check.id(), checkpoint)
    {
        Some(ActiveCheckStatus::Running) => (TestStatus::Running, 0, RUNNING_SUMMARY.to_owned()),
        Some(ActiveCheckStatus::Queued) => (TestStatus::Queued, 0, QUEUED_SUMMARY.to_owned()),
        None => match checkpoint.current_observation(check.id()) {
            Some(observation) if checkpoint.matches_observation(observation) => (
                project_evidence_status(observation.status()),
                observation.duration_ms(),
                observation.summary().to_owned(),
            ),
            _ => (TestStatus::Queued, 0, QUEUED_SUMMARY.to_owned()),
        },
    };

    TestCase {
        id: check.id().to_owned(),
        name: display_name(check),
        status,
        duration_ms,
        summary,
    }
}

fn aggregate_status(cases: &[TestCase]) -> TestStatus {
    [
        TestStatus::Running,
        TestStatus::Failed,
        TestStatus::Cancelled,
        TestStatus::Queued,
        TestStatus::Passed,
    ]
    .into_iter()
    .find(|status| cases.iter().any(|case| case.status == *status))
    .unwrap_or(TestStatus::Queued)
}

fn project_evidence_status(status: CheckEvidenceStatus) -> TestStatus {
    match status {
        CheckEvidenceStatus::Passed => TestStatus::Passed,
        CheckEvidenceStatus::Failed => TestStatus::Failed,
        CheckEvidenceStatus::Cancelled => TestStatus::Cancelled,
    }
}

fn display_name(check: &RequiredCheck) -> String {
    let package = check.package().unwrap_or("workspace");
    match check.selector().kind() {
        RequiredCheckKind::CargoCheck => format!("cargo_check[package={package}]"),
        RequiredCheckKind::CargoTest => format!(
            "cargo_test[package={package};integration_test={}]",
            check.integration_test().unwrap_or("all")
        ),
    }
}

fn validate_initial_checks(checks: &[RequiredCheck]) -> Result<(), QualityStateError> {
    if checks.len() > MAX_REQUIRED_CHECKS {
        return Err(QualityStateError::TooManyRequiredChecks);
    }
    if !checks.iter().any(RequiredCheck::is_cargo_test) {
        return Err(QualityStateError::CargoTestRequired);
    }

    let mut ids = HashSet::new();
    let mut selectors = HashSet::new();
    for check in checks {
        if !ids.insert(check.id()) {
            return Err(QualityStateError::DuplicateCheckId);
        }
        if !selectors.insert(check.selector()) {
            return Err(QualityStateError::DuplicateCheckSelector);
        }
    }
    Ok(())
}

fn allocate_check_attempt_id() -> Result<u64, QualityStateError> {
    static NEXT_ATTEMPT_ID: AtomicU64 = AtomicU64::new(1);

    NEXT_ATTEMPT_ID
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(1)
        })
        .map_err(|_| QualityStateError::CheckAttemptOverflow)
}

fn digest_for_fingerprint(fingerprint: WorkspaceFingerprint) -> WorkspaceDigest {
    let mut value = String::with_capacity(64);
    for byte in fingerprint.as_bytes() {
        write!(&mut value, "{byte:02x}").expect("writing hexadecimal to a String cannot fail");
    }
    WorkspaceDigest::try_new(value).expect("a 32-byte fingerprint always has a valid v1 digest")
}
