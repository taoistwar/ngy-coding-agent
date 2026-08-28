//! Task 15 unexpected-conflict proof and exact abort orchestration.
//!
//! A non-zero `git merge` result is never enough to authorize cleanup.  The
//! actual merge classifier first produces a non-forgeable conflict token,
//! this module consumes that token while re-proving the complete source and
//! target scene, and only the resulting opaque proof may reach the one fixed
//! `git merge --abort` command.  No path, digest, object ID, command argument,
//! or filesystem authority is accepted from a caller.

use std::error::Error;
use std::fmt;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::process_supervisor::{CapturedStream, CommandResult};
use crate::root_capability::DurableDirectoryIdentityV1;

use super::target::StableMergeConflictObservation;
use super::types::{MAX_MERGE_CONFLICT_PATHS, MAX_MERGE_CONFLICT_PAYLOAD_BYTES};
use super::{
    CandidateTreeProvenance, DeliveryAbortAppliedPersistenceBinding,
    DeliveryAbortPersistenceBinding, DeliveryCandidateTree, DeliveryCommitOid,
    DeliveryConflictPath, DeliveryExpectedMerge, DeliveryMergeAppliedPersistenceBinding,
    DeliverySourceCapability, DeliverySourceCommit, DeliverySourceCommitInput, DeliverySourceError,
    DeliverySourceProvisioner, DeliveryTargetCapability, DeliveryTargetError,
    DeliveryTargetProvisioner, DeliveryTreeOid,
};

/// Non-forgeable evidence that the one actual merge child had a known exit-1
/// outcome and immediately left the exact expected conflict scene.
///
/// The token deliberately has no public constructor and is consumed while an
/// abort proof is captured.  It is not durable by itself: the outer delivery
/// actor must first persist the resulting [`DeliveryAbortProof`] as
/// `AbortPending` before requesting any abort side effect.
#[derive(PartialEq, Eq)]
pub struct DeliveryKnownMergeConflict {
    inner: Box<DeliveryKnownMergeConflictInner>,
}

#[derive(PartialEq, Eq)]
struct DeliveryKnownMergeConflictInner {
    candidate: DeliveryCandidateTree,
    source_commit: DeliverySourceCommit,
    source_input: DeliverySourceCommitInput,
    expected_commit: DeliveryCommitOid,
    expected_tree: DeliveryTreeOid,
    expected_merge_base: DeliveryCommitOid,
    expected_target_parent: DeliveryCommitOid,
    expected_source_parent: DeliveryCommitOid,
    source_provenance: CandidateTreeProvenance,
    target_branch: String,
    target_common_identity: DurableDirectoryIdentityV1,
    target_config_attributes_digest: [u8; 32],
    target_security_digest: [u8; 32],
    observation: StableMergeConflictObservation,
}

impl DeliveryKnownMergeConflict {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn from_observed_actual_merge(
        source: &DeliverySourceCapability,
        target: &DeliveryTargetCapability,
        candidate: &DeliveryCandidateTree,
        source_commit: &DeliverySourceCommit,
        source_input: &DeliverySourceCommitInput,
        expected: &DeliveryExpectedMerge,
        observation: StableMergeConflictObservation,
    ) -> Result<Self, DeliveryAbortError> {
        let source_provenance = source.candidate_tree_provenance()?;
        if !candidate.is_bound_to(&source_provenance)
            || !source_commit.is_bound_to(candidate.provenance())
            || !source_input.matches_identity(source.identity())
            || !expected.is_bound_to(&source_provenance, target, candidate, source_commit)
        {
            return Err(DeliveryAbortError::InvalidProof);
        }
        Ok(Self {
            inner: Box::new(DeliveryKnownMergeConflictInner {
                candidate: candidate.clone(),
                source_commit: source_commit.clone(),
                source_input: source_input.clone(),
                expected_commit: expected.commit().clone(),
                expected_tree: expected.tree().clone(),
                expected_merge_base: expected.merge_base().clone(),
                expected_target_parent: expected.target_parent().clone(),
                expected_source_parent: expected.source_parent().clone(),
                source_provenance,
                target_branch: target.branch_name().to_owned(),
                target_common_identity: target.common_directory_identity().clone(),
                target_config_attributes_digest: *target.config_attributes_digest(),
                target_security_digest: *target.security_digest(),
                observation,
            }),
        })
    }

    pub(super) fn matches_context(
        &self,
        source: &DeliverySourceCapability,
        target: &DeliveryTargetCapability,
        source_commit: &DeliverySourceCommit,
        expected: &DeliveryExpectedMerge,
    ) -> bool {
        source
            .candidate_tree_provenance()
            .is_ok_and(|current| current == self.inner.source_provenance)
            && self
                .inner
                .candidate
                .is_bound_to(&self.inner.source_provenance)
            && *source_commit == self.inner.source_commit
            && self
                .inner
                .source_commit
                .is_bound_to(self.inner.candidate.provenance())
            && self.inner.source_input.matches_identity(source.identity())
            && self.inner.target_branch == target.branch_name()
            && self.inner.expected_target_parent == *target.head()
            && self.inner.expected_source_parent == *source_commit.commit()
            && self.inner.expected_commit == *expected.commit()
            && self.inner.expected_tree == *expected.tree()
            && self.inner.expected_merge_base == *expected.merge_base()
            && self.inner.expected_target_parent == *expected.target_parent()
            && self.inner.expected_source_parent == *expected.source_parent()
            && self.inner.target_common_identity == *target.common_directory_identity()
            && self.inner.target_config_attributes_digest == *target.config_attributes_digest()
            && self.inner.target_security_digest == *target.security_digest()
    }
}

impl fmt::Debug for DeliveryKnownMergeConflict {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryKnownMergeConflict(<opaque>)")
    }
}

/// Durable-runtime facts that may be adapted to the Store's `AbortPending`
/// proof.  Directory provenance, raw paths, and digests remain private; the
/// only public projection is the bounded, display-safe conflict path list.
#[derive(PartialEq, Eq)]
pub struct DeliveryAbortProof {
    conflict: DeliveryKnownMergeConflict,
    conflict_paths: Vec<DeliveryConflictPath>,
}

impl DeliveryAbortProof {
    pub fn conflict_paths(&self) -> &[DeliveryConflictPath] {
        &self.conflict_paths
    }

    /// Rehydrates a durable `AbortPending` proof only from a freshly rebound
    /// source/target/expected-merge context and a fresh exact conflict scene.
    /// The returned value is still inert until the application re-authorizes
    /// the exact durable operation through [`authorize_persisted_delivery_abort`].
    #[allow(clippy::too_many_arguments)]
    pub(super) fn from_persisted_recovery_observation(
        source: &DeliverySourceCapability,
        target: &DeliveryTargetCapability,
        candidate: &DeliveryCandidateTree,
        source_commit: &DeliverySourceCommit,
        source_input: &DeliverySourceCommitInput,
        expected: &DeliveryExpectedMerge,
        observation: StableMergeConflictObservation,
    ) -> Result<Option<Self>, DeliveryAbortError> {
        let conflict = DeliveryKnownMergeConflict::from_observed_actual_merge(
            source,
            target,
            candidate,
            source_commit,
            source_input,
            expected,
            observation,
        )?;
        let Some(conflict_paths) = bounded_conflict_paths(conflict.inner.observation.raw_paths())
        else {
            return Ok(None);
        };
        Ok(Some(Self {
            conflict,
            conflict_paths,
        }))
    }

    /// Projects the exact proof needed by the Store's `BeginAbort`
    /// transaction. The application-owned child receipt is accepted only as a
    /// non-nil correlation identity; all Git facts come from this real,
    /// repeatedly observed conflict proof.
    pub fn persistence_binding(
        &self,
        child_receipt_id: [u8; 16],
    ) -> Option<DeliveryAbortPersistenceBinding> {
        let provenance = &self.conflict.inner.source_provenance;
        DeliveryAbortPersistenceBinding::new(
            child_receipt_id,
            format!("refs/heads/{}", self.conflict.inner.target_branch),
            self.conflict
                .inner
                .expected_target_parent
                .as_str()
                .to_owned(),
            format!("refs/heads/{}", provenance.branch_name()),
            self.conflict
                .inner
                .expected_source_parent
                .as_str()
                .to_owned(),
            provenance.common_identity().as_hex().to_owned(),
            provenance.admin_identity().as_hex().to_owned(),
            super::persistence::encode_lower_hex(provenance.config_attributes_digest()),
            super::persistence::encode_lower_hex(
                &self.conflict.inner.observation.index_stages_digest(),
            ),
            super::persistence::encode_lower_hex(
                &self.conflict.inner.observation.worktree_digest(),
            ),
            self.conflict_paths.clone(),
        )
    }

    pub(super) fn matches_context(
        &self,
        source: &DeliverySourceCapability,
        target: &DeliveryTargetCapability,
        source_commit: &DeliverySourceCommit,
    ) -> bool {
        source
            .candidate_tree_provenance()
            .is_ok_and(|current| current == self.conflict.inner.source_provenance)
            && self
                .conflict
                .inner
                .candidate
                .is_bound_to(&self.conflict.inner.source_provenance)
            && *source_commit == self.conflict.inner.source_commit
            && self
                .conflict
                .inner
                .source_input
                .matches_identity(source.identity())
            && self.conflict.inner.target_branch == target.branch_name()
            && self.conflict.inner.expected_target_parent == *target.head()
            && self.conflict.inner.expected_source_parent == *source_commit.commit()
            && self.conflict.inner.target_common_identity == *target.common_directory_identity()
            && self.conflict.inner.target_config_attributes_digest
                == *target.config_attributes_digest()
            && self.conflict.inner.target_security_digest == *target.security_digest()
    }

    #[allow(dead_code)]
    pub(super) fn index_stages_digest(&self) -> [u8; 32] {
        self.conflict.inner.observation.index_stages_digest()
    }

    #[allow(dead_code)]
    pub(super) fn worktree_digest(&self) -> [u8; 32] {
        self.conflict.inner.observation.worktree_digest()
    }

    pub(super) const fn candidate(&self) -> &DeliveryCandidateTree {
        &self.conflict.inner.candidate
    }

    pub(super) const fn source_input(&self) -> &DeliverySourceCommitInput {
        &self.conflict.inner.source_input
    }

    pub(super) const fn expected_source_parent(&self) -> &DeliveryCommitOid {
        &self.conflict.inner.expected_source_parent
    }

    pub(super) const fn expected_merge_base(&self) -> &DeliveryCommitOid {
        &self.conflict.inner.expected_merge_base
    }

    pub(super) const fn expected_tree(&self) -> &DeliveryTreeOid {
        &self.conflict.inner.expected_tree
    }

    pub(super) fn matches_observation(&self, observation: &StableMergeConflictObservation) -> bool {
        self.conflict.inner.observation == *observation
    }

    pub(super) fn applied_proof(&self) -> DeliveryAbortAppliedProof {
        let provenance = &self.conflict.inner.source_provenance;
        DeliveryAbortAppliedProof {
            persistence: DeliveryAbortAppliedPersistenceBinding::new(
                format!("refs/heads/{}", self.conflict.inner.target_branch),
                self.conflict
                    .inner
                    .expected_target_parent
                    .as_str()
                    .to_owned(),
                format!("refs/heads/{}", provenance.branch_name()),
                self.conflict
                    .inner
                    .expected_source_parent
                    .as_str()
                    .to_owned(),
                provenance.common_identity().as_hex().to_owned(),
                provenance.admin_identity().as_hex().to_owned(),
                super::persistence::encode_lower_hex(provenance.config_attributes_digest()),
            ),
            conflict_paths: self.conflict_paths.clone(),
        }
    }
}

impl fmt::Debug for DeliveryAbortProof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliveryAbortProof")
            .field("conflict_path_count", &self.conflict_paths.len())
            .field("conflict_paths", &"<redacted>")
            .finish()
    }
}

/// Result of consuming a known-child token through a fresh durable scene
/// proof.  Reconciliation is an ordinary result because drift is not an
/// execution error and must never invite a blind retry.
#[derive(Debug, PartialEq, Eq)]
pub enum DeliveryAbortProofCapture {
    Proven(DeliveryAbortProof),
    ReconciliationRequired,
}

/// Application boundary that must confirm the exact proof is durably bound
/// to an `AbortPending` transition before runtime mutation authority exists.
///
/// The runtime deliberately does not depend on the Store crate. Task 21 wires
/// this trait to the StoreWriter transaction; tests use a recording
/// implementation. Returning `Ok(())` is therefore an authority-bearing
/// assertion by the trusted application layer, not a user-controlled value.
#[async_trait]
pub trait DeliveryAbortPendingAuthorizer: Send + Sync {
    type Error: Send;

    async fn authorize_persisted_abort_pending(
        &self,
        proof: &DeliveryAbortProof,
    ) -> Result<(), Self::Error>;
}

/// One-shot mutation authority created only after the application confirms a
/// durable `AbortPending` record for the exact captured proof.
pub struct DeliveryAbortCapability {
    proof: DeliveryAbortProof,
}

impl DeliveryAbortCapability {
    pub(super) const fn proof(&self) -> &DeliveryAbortProof {
        &self.proof
    }
}

impl fmt::Debug for DeliveryAbortCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryAbortCapability(<opaque>)")
    }
}

/// Crosses the Store durability barrier without introducing a runtime->Store
/// dependency. A failed confirmation returns no abort capability.
pub async fn authorize_persisted_delivery_abort<A>(
    proof: DeliveryAbortProof,
    authorizer: &A,
) -> Result<DeliveryAbortCapability, A::Error>
where
    A: DeliveryAbortPendingAuthorizer,
{
    authorizer.authorize_persisted_abort_pending(&proof).await?;
    Ok(DeliveryAbortCapability { proof })
}

/// Opaque postcondition proof returned only after the target is back at the
/// authenticated old HEAD, the checkout is clean with no merge state, and the
/// committed source proof still matches. The application may use this value
/// to complete the durable `AbortPending -> Conflict` transition; callers
/// cannot construct it or extract mutable authority from it.
#[derive(PartialEq, Eq)]
pub struct DeliveryAbortAppliedProof {
    persistence: DeliveryAbortAppliedPersistenceBinding,
    conflict_paths: Vec<DeliveryConflictPath>,
}

impl DeliveryAbortAppliedProof {
    pub fn target_branch(&self) -> &str {
        self.persistence
            .target_branch()
            .strip_prefix("refs/heads/")
            .expect("runtime persistence always uses a local branch ref")
    }

    pub fn target_head_id(&self) -> &str {
        self.persistence.target_head()
    }

    pub fn source_commit_id(&self) -> &str {
        self.persistence.source_oid()
    }

    pub fn conflict_paths(&self) -> &[DeliveryConflictPath] {
        &self.conflict_paths
    }

    pub const fn persistence_binding(&self) -> &DeliveryAbortAppliedPersistenceBinding {
        &self.persistence
    }
}

impl fmt::Debug for DeliveryAbortAppliedProof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliveryAbortAppliedProof")
            .field("postcondition", &"<validated>")
            .field("conflict_path_count", &self.conflict_paths.len())
            .finish()
    }
}

/// Proven result of one exact abort attempt.
#[derive(Debug, PartialEq, Eq)]
pub enum DeliveryAbortOutcome {
    Applied(DeliveryAbortAppliedProof),
    KnownNotApplied,
    ReconciliationRequired,
}

/// Opaque proof that recovery observed the exact expected merge commit as the
/// clean target HEAD, with the committed source and the same target scene
/// revalidated around that observation. Only the recovery classifier may
/// mint this value; public getters expose safe durable facts, not mutation
/// authority.
#[derive(PartialEq, Eq)]
pub struct DeliveryMergeAppliedProof {
    persistence: DeliveryMergeAppliedPersistenceBinding,
}

impl DeliveryMergeAppliedProof {
    pub(super) fn from_recovery_postcondition(
        expected: &DeliveryExpectedMerge,
        source: &DeliverySourceCapability,
        target: &DeliveryTargetCapability,
        source_commit: &DeliverySourceCommit,
    ) -> Option<Self> {
        if expected.target_parent() != target.head()
            || expected.source_parent() != source_commit.commit()
        {
            return None;
        }
        Some(Self {
            persistence: DeliveryMergeAppliedPersistenceBinding::new(
                expected.persistence_binding().ok()?,
                format!("refs/heads/{}", target.branch_name()),
                expected.commit().as_str().to_owned(),
                format!("refs/heads/{}", source.branch_name()),
                source_commit.commit().as_str().to_owned(),
                source.common_directory_identity().as_hex().to_owned(),
                source.admin_directory_identity().as_hex().to_owned(),
                super::persistence::encode_lower_hex(source.config_attributes_digest()),
            ),
        })
    }

    pub fn expected_commit_id(&self) -> &str {
        self.persistence.object().expected_merge_commit()
    }

    pub fn expected_tree_id(&self) -> &str {
        self.persistence.object().tree()
    }

    pub fn target_branch(&self) -> &str {
        self.persistence
            .target_branch()
            .strip_prefix("refs/heads/")
            .expect("runtime persistence always uses a local branch ref")
    }

    pub fn source_commit_id(&self) -> &str {
        self.persistence.source_oid()
    }

    pub const fn persistence_binding(&self) -> &DeliveryMergeAppliedPersistenceBinding {
        &self.persistence
    }
}

impl fmt::Debug for DeliveryMergeAppliedProof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliveryMergeAppliedProof")
            .field("postcondition", &"<validated>")
            .finish()
    }
}

/// Recovery decision for a durable `MergePending` operation.
#[derive(Debug, PartialEq, Eq)]
pub enum DeliveryMergePendingDisposition {
    RetryExactMerge,
    MergeApplied(Box<DeliveryMergeAppliedProof>),
    ReconciliationRequired,
}

/// Recovery decision for a durable `AbortPending` operation.
#[derive(Debug, PartialEq, Eq)]
pub enum DeliveryAbortPendingDisposition {
    RetryExactAbort,
    AbortApplied(DeliveryAbortAppliedProof),
    ReconciliationRequired,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum DeliveryAbortError {
    InvalidProof,
    Source(DeliverySourceError),
    Target(DeliveryTargetError),
}

impl DeliveryAbortError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Source(error) => error.code(),
            Self::Target(error) => error.code(),
            Self::InvalidProof => "DELIVERY_RECONCILIATION_REQUIRED",
        }
    }
}

impl From<DeliverySourceError> for DeliveryAbortError {
    fn from(error: DeliverySourceError) -> Self {
        Self::Source(error)
    }
}

impl From<DeliveryTargetError> for DeliveryAbortError {
    fn from(error: DeliveryTargetError) -> Self {
        Self::Target(error)
    }
}

impl fmt::Debug for DeliveryAbortError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryAbortError(<redacted>)")
    }
}

impl fmt::Display for DeliveryAbortError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("delivery abort failed")
    }
}

impl Error for DeliveryAbortError {}

/// Consumes a known-child conflict token and proves the exact scene that an
/// outer Store transition may bind to `AbortPending`.
#[allow(clippy::too_many_arguments)]
pub async fn capture_delivery_abort_proof(
    source_provisioner: &DeliverySourceProvisioner,
    target_provisioner: &DeliveryTargetProvisioner,
    source: &DeliverySourceCapability,
    target: &DeliveryTargetCapability,
    source_commit: &DeliverySourceCommit,
    expected: &DeliveryExpectedMerge,
    known_conflict: DeliveryKnownMergeConflict,
    cancellation: CancellationToken,
) -> Result<DeliveryAbortProofCapture, DeliveryAbortError> {
    if !known_conflict.matches_context(source, target, source_commit, expected) {
        return Ok(DeliveryAbortProofCapture::ReconciliationRequired);
    }
    if source_provisioner
        .revalidate_preflight_committed_source(
            source,
            &known_conflict.inner.candidate,
            source_commit,
            &known_conflict.inner.source_input,
            cancellation.clone(),
        )
        .await
        .is_err()
    {
        return Ok(DeliveryAbortProofCapture::ReconciliationRequired);
    }
    let Some(observed) = target_provisioner
        .observe_expected_merge_conflict(
            target,
            &known_conflict.inner.expected_merge_base,
            &known_conflict.inner.expected_source_parent,
            &known_conflict.inner.expected_tree,
            cancellation.clone(),
        )
        .await
        .ok()
        .flatten()
    else {
        return Ok(DeliveryAbortProofCapture::ReconciliationRequired);
    };
    if observed != known_conflict.inner.observation {
        return Ok(DeliveryAbortProofCapture::ReconciliationRequired);
    }
    if source_provisioner
        .revalidate_preflight_committed_source(
            source,
            &known_conflict.inner.candidate,
            source_commit,
            &known_conflict.inner.source_input,
            cancellation.clone(),
        )
        .await
        .is_err()
    {
        return Ok(DeliveryAbortProofCapture::ReconciliationRequired);
    }
    let Some(closing_observation) = target_provisioner
        .observe_expected_merge_conflict(
            target,
            &known_conflict.inner.expected_merge_base,
            &known_conflict.inner.expected_source_parent,
            &known_conflict.inner.expected_tree,
            cancellation,
        )
        .await
        .ok()
        .flatten()
    else {
        return Ok(DeliveryAbortProofCapture::ReconciliationRequired);
    };
    if closing_observation != known_conflict.inner.observation {
        return Ok(DeliveryAbortProofCapture::ReconciliationRequired);
    }
    let Some(conflict_paths) = bounded_conflict_paths(closing_observation.raw_paths()) else {
        return Ok(DeliveryAbortProofCapture::ReconciliationRequired);
    };
    Ok(DeliveryAbortProofCapture::Proven(DeliveryAbortProof {
        conflict: known_conflict,
        conflict_paths,
    }))
}

/// Executes at most one fixed abort after re-proving an already-persisted
/// abort scene.  If a prior successful abort lost its Store reply, the exact
/// old-HEAD/clean postcondition is returned as `Applied` without spawning a
/// second child.
#[allow(clippy::too_many_arguments)]
pub async fn abort_expected_delivery_merge(
    source_provisioner: &DeliverySourceProvisioner,
    target_provisioner: &DeliveryTargetProvisioner,
    source: &DeliverySourceCapability,
    target: &DeliveryTargetCapability,
    source_commit: &DeliverySourceCommit,
    capability: &DeliveryAbortCapability,
    cancellation: CancellationToken,
) -> Result<DeliveryAbortOutcome, DeliveryAbortError> {
    let proof = &capability.proof;
    if !proof.matches_context(source, target, source_commit) {
        return Ok(DeliveryAbortOutcome::ReconciliationRequired);
    }
    if !revalidate_committed_source(
        source_provisioner,
        source,
        source_commit,
        proof,
        cancellation.clone(),
    )
    .await
    {
        return Ok(DeliveryAbortOutcome::ReconciliationRequired);
    }

    // Query-first retry: a lost Store reply after a successful abort must not
    // execute the side effect again.
    if abort_applied_postcondition_is_exact(
        source_provisioner,
        target_provisioner,
        source,
        target,
        source_commit,
        proof,
        cancellation.clone(),
    )
    .await
    {
        return Ok(DeliveryAbortOutcome::Applied(proof.applied_proof()));
    }

    let Some(observed) = target_provisioner
        .observe_expected_merge_conflict(
            target,
            &proof.conflict.inner.expected_merge_base,
            &proof.conflict.inner.expected_source_parent,
            &proof.conflict.inner.expected_tree,
            cancellation.clone(),
        )
        .await
        .ok()
        .flatten()
    else {
        return Ok(DeliveryAbortOutcome::ReconciliationRequired);
    };
    if observed != proof.conflict.inner.observation {
        return Ok(DeliveryAbortOutcome::ReconciliationRequired);
    }
    if !revalidate_committed_source(
        source_provisioner,
        source,
        source_commit,
        proof,
        cancellation.clone(),
    )
    .await
    {
        return Ok(DeliveryAbortOutcome::ReconciliationRequired);
    }

    // The source proof above may be expensive. Re-sample the exact abort
    // scene once more immediately before command construction so an
    // autostash or conflicting operation introduced during that interval is
    // never knowingly handed to Git.
    let final_scene_matches = target_provisioner
        .observe_expected_merge_conflict(
            target,
            &proof.conflict.inner.expected_merge_base,
            &proof.conflict.inner.expected_source_parent,
            &proof.conflict.inner.expected_tree,
            cancellation.clone(),
        )
        .await
        .ok()
        .flatten()
        .is_some_and(|observed| observed == proof.conflict.inner.observation);
    if !final_scene_matches {
        return Ok(DeliveryAbortOutcome::ReconciliationRequired);
    }

    let commands = match target.mutation_commands() {
        Ok(commands) => commands,
        Err(_) => return Ok(DeliveryAbortOutcome::ReconciliationRequired),
    };
    let command = match commands.merge_abort() {
        Ok(command) => command,
        Err(_) => return Ok(DeliveryAbortOutcome::ReconciliationRequired),
    };
    target_provisioner.run_actual_merge_boundary_hook("before-actual-abort-spawn");
    let result = match target_provisioner
        .executor()
        .supervisor()
        .run(command, cancellation)
        .await
    {
        Ok(result) => result,
        Err(error) if error.child_could_not_have_started() => {
            return Ok(DeliveryAbortOutcome::KnownNotApplied);
        }
        // A failed cleanup proof forbids starting any fresh observation child.
        Err(error) if error.process_cleanup_is_unproven() => {
            return Ok(DeliveryAbortOutcome::ReconciliationRequired);
        }
        // The child may have started, but cleanup is confirmed. Ignore the
        // process envelope and classify only a freshly closed Git fact.
        Err(_) => {
            return Ok(classify_unknown_abort_result(
                source_provisioner,
                target_provisioner,
                source,
                target,
                source_commit,
                proof,
            )
            .await);
        }
    };
    classify_abort_result(
        source_provisioner,
        target_provisioner,
        source,
        target,
        source_commit,
        proof,
        result,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn classify_abort_result(
    source_provisioner: &DeliverySourceProvisioner,
    target_provisioner: &DeliveryTargetProvisioner,
    source: &DeliverySourceCapability,
    target: &DeliveryTargetCapability,
    source_commit: &DeliverySourceCommit,
    proof: &DeliveryAbortProof,
    result: CommandResult,
) -> Result<DeliveryAbortOutcome, DeliveryAbortError> {
    match classify_abort_child_envelope(&result, target_provisioner.limits().max_status_bytes()) {
        AbortChildEnvelope::ExitZero => {
            if abort_applied_postcondition_is_exact(
                source_provisioner,
                target_provisioner,
                source,
                target,
                source_commit,
                proof,
                CancellationToken::new(),
            )
            .await
            {
                Ok(DeliveryAbortOutcome::Applied(proof.applied_proof()))
            } else {
                Ok(DeliveryAbortOutcome::ReconciliationRequired)
            }
        }
        AbortChildEnvelope::ExitNonZero | AbortChildEnvelope::Unknown => {
            Ok(classify_unknown_abort_result(
                source_provisioner,
                target_provisioner,
                source,
                target,
                source_commit,
                proof,
            )
            .await)
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn classify_unknown_abort_result(
    source_provisioner: &DeliverySourceProvisioner,
    target_provisioner: &DeliveryTargetProvisioner,
    source: &DeliverySourceCapability,
    target: &DeliveryTargetCapability,
    source_commit: &DeliverySourceCommit,
    proof: &DeliveryAbortProof,
) -> DeliveryAbortOutcome {
    if abort_applied_postcondition_is_exact(
        source_provisioner,
        target_provisioner,
        source,
        target,
        source_commit,
        proof,
        CancellationToken::new(),
    )
    .await
    {
        return DeliveryAbortOutcome::Applied(proof.applied_proof());
    }
    if abort_not_applied_postcondition_is_exact(
        source_provisioner,
        target_provisioner,
        source,
        target,
        source_commit,
        proof,
        CancellationToken::new(),
    )
    .await
    {
        DeliveryAbortOutcome::KnownNotApplied
    } else {
        DeliveryAbortOutcome::ReconciliationRequired
    }
}

#[allow(clippy::too_many_arguments)]
async fn abort_not_applied_postcondition_is_exact(
    source_provisioner: &DeliverySourceProvisioner,
    target_provisioner: &DeliveryTargetProvisioner,
    source: &DeliverySourceCapability,
    target: &DeliveryTargetCapability,
    source_commit: &DeliverySourceCommit,
    proof: &DeliveryAbortProof,
    cancellation: CancellationToken,
) -> bool {
    // Close the exact still-conflicted scene as S -> T -> S -> T. A process
    // error or timeout is never itself evidence that abort was not applied.
    revalidate_committed_source(
        source_provisioner,
        source,
        source_commit,
        proof,
        cancellation.clone(),
    )
    .await
        && conflict_scene_matches(target_provisioner, target, proof, cancellation.clone()).await
        && revalidate_committed_source(
            source_provisioner,
            source,
            source_commit,
            proof,
            cancellation.clone(),
        )
        .await
        && conflict_scene_matches(target_provisioner, target, proof, cancellation).await
}

#[allow(clippy::too_many_arguments)]
async fn abort_applied_postcondition_is_exact(
    source_provisioner: &DeliverySourceProvisioner,
    target_provisioner: &DeliveryTargetProvisioner,
    source: &DeliverySourceCapability,
    target: &DeliveryTargetCapability,
    source_commit: &DeliverySourceCommit,
    proof: &DeliveryAbortProof,
    cancellation: CancellationToken,
) -> bool {
    // The repository lease held by the application serializes cooperating
    // delivery mutations. Repeating both independent facts closes the runtime
    // observation around external drift without ever repairing it.
    revalidate_committed_source(
        source_provisioner,
        source,
        source_commit,
        proof,
        cancellation.clone(),
    )
    .await
        && target_provisioner
            .revalidate_delivery_target(target, cancellation.clone())
            .await
            .is_ok()
        && revalidate_committed_source(
            source_provisioner,
            source,
            source_commit,
            proof,
            cancellation.clone(),
        )
        .await
        && target_provisioner
            .revalidate_delivery_target(target, cancellation)
            .await
            .is_ok()
}

async fn conflict_scene_matches(
    target_provisioner: &DeliveryTargetProvisioner,
    target: &DeliveryTargetCapability,
    proof: &DeliveryAbortProof,
    cancellation: CancellationToken,
) -> bool {
    target_provisioner
        .observe_expected_merge_conflict(
            target,
            &proof.conflict.inner.expected_merge_base,
            &proof.conflict.inner.expected_source_parent,
            &proof.conflict.inner.expected_tree,
            cancellation,
        )
        .await
        .ok()
        .flatten()
        .is_some_and(|observed| observed == proof.conflict.inner.observation)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AbortChildEnvelope {
    ExitZero,
    ExitNonZero,
    Unknown,
}

fn classify_abort_child_envelope(
    result: &CommandResult,
    output_limit: usize,
) -> AbortChildEnvelope {
    if !result_streams_are_complete(result, output_limit) {
        return AbortChildEnvelope::Unknown;
    }
    match result.exit_code {
        Some(0) => AbortChildEnvelope::ExitZero,
        Some(_) => AbortChildEnvelope::ExitNonZero,
        None => AbortChildEnvelope::Unknown,
    }
}

async fn revalidate_committed_source(
    provisioner: &DeliverySourceProvisioner,
    source: &DeliverySourceCapability,
    source_commit: &DeliverySourceCommit,
    proof: &DeliveryAbortProof,
    cancellation: CancellationToken,
) -> bool {
    provisioner
        .revalidate_preflight_committed_source(
            source,
            &proof.conflict.inner.candidate,
            source_commit,
            &proof.conflict.inner.source_input,
            cancellation,
        )
        .await
        .is_ok()
}

fn bounded_conflict_paths(raw_paths: &[Vec<u8>]) -> Option<Vec<DeliveryConflictPath>> {
    if raw_paths.is_empty() || raw_paths.len() > MAX_MERGE_CONFLICT_PATHS {
        return None;
    }
    let mut payload_bytes = 0usize;
    let mut paths = Vec::with_capacity(raw_paths.len());
    for raw in raw_paths {
        let path = DeliveryConflictPath::try_from_raw(raw.clone()).ok()?;
        payload_bytes = payload_bytes.checked_add(path.value().len())?;
        if payload_bytes > MAX_MERGE_CONFLICT_PAYLOAD_BYTES {
            return None;
        }
        paths.push(path);
    }
    Some(paths)
}

fn result_streams_are_complete(result: &CommandResult, output_limit: usize) -> bool {
    !result.cancelled
        && !result.timed_out
        && result.signal.is_none()
        && !result.truncated
        && stream_is_complete(&result.stdout, output_limit)
        && stream_is_complete(&result.stderr, output_limit)
}

fn stream_is_complete(stream: &CapturedStream, output_limit: usize) -> bool {
    let retained = stream.head.len().saturating_add(stream.tail.len());
    stream.complete
        && !stream.truncated
        && stream.omitted_observed_bytes == 0
        && stream.observed_bytes == retained as u64
        && retained <= output_limit
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abort_child_envelope_never_treats_unknown_or_incomplete_output_as_known() {
        let mut result = complete_result(Some(0));
        assert_eq!(
            classify_abort_child_envelope(&result, 1024),
            AbortChildEnvelope::ExitZero
        );
        result.exit_code = Some(1);
        assert_eq!(
            classify_abort_child_envelope(&result, 1024),
            AbortChildEnvelope::ExitNonZero
        );

        for make_unknown in [
            |result: &mut CommandResult| result.exit_code = None,
            |result: &mut CommandResult| result.cancelled = true,
            |result: &mut CommandResult| result.timed_out = true,
            |result: &mut CommandResult| result.signal = Some(9),
            |result: &mut CommandResult| result.truncated = true,
            |result: &mut CommandResult| result.stdout.complete = false,
            |result: &mut CommandResult| result.stderr.truncated = true,
            |result: &mut CommandResult| result.stdout.omitted_observed_bytes = 1,
        ] {
            let mut result = complete_result(Some(1));
            make_unknown(&mut result);
            assert_eq!(
                classify_abort_child_envelope(&result, 1024),
                AbortChildEnvelope::Unknown
            );
        }

        let oversized = complete_result(Some(1));
        assert_eq!(
            classify_abort_child_envelope(&oversized, 0),
            AbortChildEnvelope::Unknown
        );
    }

    fn complete_result(exit_code: Option<i32>) -> CommandResult {
        CommandResult {
            exit_code,
            signal: None,
            timed_out: false,
            cancelled: false,
            stdout: complete_stream(b"x"),
            stderr: complete_stream(&[]),
            truncated: false,
            duration_ms: 0,
        }
    }

    fn complete_stream(bytes: &[u8]) -> CapturedStream {
        CapturedStream {
            head: bytes.to_vec(),
            tail: Vec::new(),
            observed_bytes: bytes.len() as u64,
            omitted_observed_bytes: 0,
            truncated: false,
            complete: true,
        }
    }
}
