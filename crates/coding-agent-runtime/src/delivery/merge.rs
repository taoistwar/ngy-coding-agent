//! Task 14 expected-merge proof and fixed actual-merge orchestration.
//!
//! This module owns the narrow transition from an already committed source
//! and a clean preflight to an exact expected merge object and, later, one
//! authenticated `git merge`.  It never resets, cleans, checks out, aborts,
//! or guesses that a non-zero child result changed no repository state.

use std::error::Error;
use std::fmt;

use tokio_util::sync::CancellationToken;

use crate::command_policy::DeliveryGitCommitEnvironment;
use crate::process_supervisor::{CapturedStream, CommandResult};

use super::command::DeliveryMergeMessage;
use super::target::StableMergeConflictObservation;
use super::{
    CandidateTreeProvenance, DeliveryCandidateTree, DeliveryCommitOid, DeliveryGitObjectFormat,
    DeliveryKnownMergeConflict, DeliveryPreflightError, DeliveryPreflightResult,
    DeliveryPreflightSource, DeliverySourceCapability, DeliverySourceCommit,
    DeliverySourceCommitInput, DeliverySourceError, DeliverySourceProvisioner,
    DeliveryTargetCapability, DeliveryTargetError, DeliveryTargetProvisioner, DeliveryTreeOid,
    preflight_delivery_merge,
};

const DELIVERY_MERGE_MESSAGE_TEMPLATE_VERSION: u32 = 1;

/// Fixed metadata accepted with one user-confirmed merge operation.
///
/// Callers cannot choose an author, committer, message template, timezone, or
/// arbitrary command input.  The public value contains only the durable
/// scalar values the outer Store will persist with `Accepted`/`MergePending`.
#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryMergeInput {
    task_id: String,
    attempt: u64,
    epoch_seconds: i64,
}

impl DeliveryMergeInput {
    pub fn try_new(
        task_id: &str,
        attempt: u64,
        epoch_seconds: i64,
    ) -> Result<Self, DeliveryMergeError> {
        if epoch_seconds <= 0 {
            return Err(DeliveryMergeError::InvalidInput);
        }
        // The private message constructor owns canonical task/attempt syntax
        // and the exact ASCII template. Construct it once here solely as a
        // validation boundary; later commands reconstruct the same fixed
        // message from these immutable scalars.
        DeliveryMergeMessage::try_new(task_id, attempt)
            .map_err(|_| DeliveryMergeError::InvalidInput)?;
        Ok(Self {
            task_id: task_id.to_owned(),
            attempt,
            epoch_seconds,
        })
    }

    fn message(&self) -> Result<DeliveryMergeMessage, DeliveryMergeError> {
        DeliveryMergeMessage::try_new(&self.task_id, self.attempt)
            .map_err(|_| DeliveryMergeError::InvalidInput)
    }

    fn metadata(&self) -> Result<DeliveryGitCommitEnvironment, DeliveryMergeError> {
        DeliveryGitCommitEnvironment::try_new(self.epoch_seconds)
            .map_err(|_| DeliveryMergeError::InvalidInput)
    }

    pub(super) fn matches_identity(&self, identity: &crate::WorktreeIdentity) -> bool {
        self.task_id == identity.task_id() && self.attempt == u64::from(identity.attempt())
    }

    pub(super) const fn epoch_seconds(&self) -> i64 {
        self.epoch_seconds
    }

    pub(super) fn message_bytes(&self) -> Result<Vec<u8>, DeliveryMergeError> {
        Ok(self.message()?.object_message_bytes())
    }
}

impl fmt::Debug for DeliveryMergeInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryMergeInput(<validated>)")
    }
}

/// An exact, unreferenced merge object created before the target checkout is
/// mutated.  The outer Store persists only its public object ID plus its
/// accepted scalar input; retained capability and provenance values stay
/// runtime-private.
pub struct DeliveryExpectedMerge {
    commit: DeliveryCommitOid,
    tree: DeliveryTreeOid,
    target_parent: DeliveryCommitOid,
    source_parent: DeliveryCommitOid,
    input: DeliveryMergeInput,
    preflight: DeliveryPreflightResult,
    provenance: MergeProvenance,
}

#[derive(Clone, PartialEq, Eq)]
struct MergeProvenance {
    source: CandidateTreeProvenance,
    target_common_identity: crate::root_capability::DurableDirectoryIdentityV1,
    target_branch: String,
    target_head: DeliveryCommitOid,
    target_config_attributes_digest: [u8; 32],
    target_security_digest: [u8; 32],
}

impl MergeProvenance {
    fn capture(source: CandidateTreeProvenance, target: &DeliveryTargetCapability) -> Self {
        Self {
            source,
            target_common_identity: target.common_directory_identity().clone(),
            target_branch: target.branch_name().to_owned(),
            target_head: target.head().clone(),
            target_config_attributes_digest: *target.config_attributes_digest(),
            target_security_digest: *target.security_digest(),
        }
    }

    pub(super) fn is_bound_to(
        &self,
        source: &CandidateTreeProvenance,
        target: &DeliveryTargetCapability,
    ) -> bool {
        self.source == *source
            && self.target_common_identity == *target.common_directory_identity()
            && self.target_branch == target.branch_name()
            && self.target_head == *target.head()
            && self.target_config_attributes_digest == *target.config_attributes_digest()
            && self.target_security_digest == *target.security_digest()
    }
}

impl DeliveryExpectedMerge {
    /// The durable exact object ID. It is safe to expose because it was parsed
    /// from the fixed `commit-tree` output and then verified byte-for-byte.
    pub fn object_id(&self) -> &str {
        self.commit.as_str()
    }

    pub(super) const fn commit(&self) -> &DeliveryCommitOid {
        &self.commit
    }

    pub(super) const fn tree(&self) -> &DeliveryTreeOid {
        &self.tree
    }

    pub(super) const fn target_parent(&self) -> &DeliveryCommitOid {
        &self.target_parent
    }

    pub(super) const fn source_parent(&self) -> &DeliveryCommitOid {
        &self.source_parent
    }

    pub(super) fn merge_base(&self) -> &DeliveryCommitOid {
        self.preflight.merge_base()
    }

    pub(super) fn is_bound_to(
        &self,
        source: &CandidateTreeProvenance,
        target: &DeliveryTargetCapability,
        candidate: &DeliveryCandidateTree,
        source_commit: &DeliverySourceCommit,
    ) -> bool {
        self.provenance.is_bound_to(source, target)
            && self.tree == *self.preflight.candidate_merge_tree()
            && self.target_parent == *target.head()
            && self.source_parent == *source_commit.commit()
            && candidate.is_bound_to(source)
            && source_commit.is_bound_to(candidate.provenance())
    }

    pub fn persistence_binding(
        &self,
    ) -> Result<super::DeliveryExpectedMergePersistenceBinding, DeliveryMergeError> {
        Ok(super::DeliveryExpectedMergePersistenceBinding::new(
            self.commit.as_str().to_owned(),
            self.tree.as_str().to_owned(),
            self.target_parent.as_str().to_owned(),
            self.source_parent.as_str().to_owned(),
            super::DeliveryCommitPersistenceMetadata::new(
                self.input.epoch_seconds(),
                DELIVERY_MERGE_MESSAGE_TEMPLATE_VERSION,
                self.input.message_bytes()?,
            ),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn from_persisted_recovery(
        commit: DeliveryCommitOid,
        tree: DeliveryTreeOid,
        target_parent: DeliveryCommitOid,
        source_parent: DeliveryCommitOid,
        input: DeliveryMergeInput,
        merge_base: DeliveryCommitOid,
        source_provenance: CandidateTreeProvenance,
        target: &DeliveryTargetCapability,
    ) -> Self {
        Self {
            commit,
            tree: tree.clone(),
            target_parent,
            source_parent: source_parent.clone(),
            input,
            preflight: DeliveryPreflightResult::ready(source_parent, merge_base, tree),
            provenance: MergeProvenance::capture(source_provenance, target),
        }
    }
}

impl fmt::Debug for DeliveryExpectedMerge {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryExpectedMerge(<validated>)")
    }
}

/// Proven outcome of one actual fixed merge child.  Only `Applied` is a
/// success fact. The other values never imply an automatic retry, cleanup, or
/// abort; Task 15 is responsible for durable conflict/abort handling.
#[derive(Debug, PartialEq, Eq)]
pub enum DeliveryMergeOutcome {
    Applied,
    KnownNotApplied,
    ConflictObserved(DeliveryKnownMergeConflict),
    ReconciliationRequired,
}

/// Stable, redacted failures before an actual merge child has been launched.
/// Once process execution begins, ambiguous cases are represented by
/// [`DeliveryMergeOutcome::ReconciliationRequired`] instead of an error that
/// could be mistaken for proven zero effect.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum DeliveryMergeError {
    InvalidInput,
    Preflight(DeliveryPreflightError),
    Source(DeliverySourceError),
    Target(DeliveryTargetError),
    PreflightStale,
    ExpectedObjectInvalid,
}

impl DeliveryMergeError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidInput => "DELIVERY_MERGE_INVALID",
            Self::Preflight(error) => error.code(),
            Self::Source(error) => error.code(),
            Self::Target(error) => error.code(),
            Self::PreflightStale => "DELIVERY_PREFLIGHT_STALE",
            Self::ExpectedObjectInvalid => "DELIVERY_RECONCILIATION_REQUIRED",
        }
    }
}

impl From<DeliveryPreflightError> for DeliveryMergeError {
    fn from(error: DeliveryPreflightError) -> Self {
        Self::Preflight(error)
    }
}

impl From<DeliverySourceError> for DeliveryMergeError {
    fn from(error: DeliverySourceError) -> Self {
        Self::Source(error)
    }
}

impl From<DeliveryTargetError> for DeliveryMergeError {
    fn from(error: DeliveryTargetError) -> Self {
        Self::Target(error)
    }
}

impl fmt::Debug for DeliveryMergeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryMergeError(<redacted>)")
    }
}

impl fmt::Display for DeliveryMergeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("delivery merge failed")
    }
}

impl Error for DeliveryMergeError {}

/// Builds and verifies the expected deterministic two-parent merge commit.
///
/// The method repeats the full committed-source/target preflight immediately
/// before and after the only permitted dangling-object write.  A caller must
/// persist the returned exact object ID before asking
/// [`apply_expected_delivery_merge`] to launch a target mutation.
#[allow(clippy::too_many_arguments)]
pub async fn build_expected_delivery_merge(
    source_provisioner: &DeliverySourceProvisioner,
    target_provisioner: &DeliveryTargetProvisioner,
    source: &DeliverySourceCapability,
    target: &DeliveryTargetCapability,
    candidate: &DeliveryCandidateTree,
    source_commit: &DeliverySourceCommit,
    source_input: &DeliverySourceCommitInput,
    preflight: &DeliveryPreflightResult,
    input: &DeliveryMergeInput,
    cancellation: CancellationToken,
) -> Result<DeliveryExpectedMerge, DeliveryMergeError> {
    let source_provenance =
        require_merge_bindings(source, target, candidate, source_commit, input)?;
    require_ready_preflight(preflight, source_commit)?;
    reprove_ready_preflight(
        source_provisioner,
        target_provisioner,
        source,
        target,
        candidate,
        source_commit,
        source_input,
        preflight,
        cancellation.clone(),
    )
    .await?;

    let message = input.message()?;
    let metadata = input.metadata()?;
    let commands = target.mutation_commands()?;
    let output = target_provisioner
        .executor()
        .run(
            commands.commit_expected_merge(
                preflight.candidate_merge_tree(),
                target.head(),
                source_commit.commit(),
                &message,
                &metadata,
            )?,
            cancellation.clone(),
            target_provisioner.limits().max_status_bytes(),
        )
        .await?;
    let commit = parse_created_merge_oid(&output, target.probe().object_format())?;
    verify_expected_merge_object(
        target_provisioner,
        target,
        &commit,
        preflight.candidate_merge_tree(),
        target.head(),
        source_commit.commit(),
        &message,
        input.epoch_seconds,
        cancellation.clone(),
    )
    .await?;

    // The dangling object is permitted to remain if this last authentication
    // detects drift.  It must never be reinterpreted as authority for a later
    // target mutation without a fresh matching proof.
    reprove_ready_preflight(
        source_provisioner,
        target_provisioner,
        source,
        target,
        candidate,
        source_commit,
        source_input,
        preflight,
        cancellation,
    )
    .await?;

    Ok(DeliveryExpectedMerge {
        commit,
        tree: preflight.candidate_merge_tree().clone(),
        target_parent: target.head().clone(),
        source_parent: source_commit.commit().clone(),
        input: input.clone(),
        preflight: preflight.clone(),
        provenance: MergeProvenance::capture(source_provenance, target),
    })
}

/// Performs the single fixed no-ff actual merge only after fresh source,
/// target, merge-tree, write-set, ignored-collision, and expected-object
/// proofs. It never runs reset, clean, checkout, or abort.
#[allow(clippy::too_many_arguments)]
pub async fn apply_expected_delivery_merge(
    source_provisioner: &DeliverySourceProvisioner,
    target_provisioner: &DeliveryTargetProvisioner,
    source: &DeliverySourceCapability,
    target: &DeliveryTargetCapability,
    candidate: &DeliveryCandidateTree,
    source_commit: &DeliverySourceCommit,
    source_input: &DeliverySourceCommitInput,
    preflight: &DeliveryPreflightResult,
    expected: &DeliveryExpectedMerge,
    cancellation: CancellationToken,
) -> Result<DeliveryMergeOutcome, DeliveryMergeError> {
    let source_provenance =
        require_merge_bindings(source, target, candidate, source_commit, &expected.input)?;
    if preflight != &expected.preflight
        || !expected.is_bound_to(&source_provenance, target, candidate, source_commit)
    {
        return Err(DeliveryMergeError::PreflightStale);
    }
    reprove_ready_preflight(
        source_provisioner,
        target_provisioner,
        source,
        target,
        candidate,
        source_commit,
        source_input,
        preflight,
        cancellation.clone(),
    )
    .await?;

    let message = expected.input.message()?;
    verify_expected_merge_object(
        target_provisioner,
        target,
        &expected.commit,
        &expected.tree,
        &expected.target_parent,
        &expected.source_parent,
        &message,
        expected.input.epoch_seconds,
        cancellation.clone(),
    )
    .await?;
    target_provisioner
        .run_actual_merge_boundary_hook("after-expected-merge-object-before-final-preflight");
    // `cat-file` proves only the durable object.  Re-run the complete
    // source/target/merge-tree/collision proof after that object read so a
    // target or source change in the narrow interval cannot authorize an
    // actual merge child on a new live state.
    reprove_ready_preflight(
        source_provisioner,
        target_provisioner,
        source,
        target,
        candidate,
        source_commit,
        source_input,
        preflight,
        cancellation.clone(),
    )
    .await?;
    target_provisioner
        .run_actual_merge_boundary_hook("after-final-preflight-before-last-collision-recheck");
    // Git's ignored-path handling is a final best-effort guard, not a proof
    // boundary. Repeat the complete preflight after the last observable test
    // boundary so a newly introduced ignored node is rejected before the
    // actual merge child can be constructed.
    reprove_ready_preflight(
        source_provisioner,
        target_provisioner,
        source,
        target,
        candidate,
        source_commit,
        source_input,
        preflight,
        cancellation.clone(),
    )
    .await?;
    target_provisioner
        .run_actual_merge_boundary_hook("after-last-collision-recheck-before-actual-merge-spawn");

    let metadata = expected.input.metadata()?;
    let command = target
        .mutation_commands()?
        .merge(source_commit.commit(), &message, &metadata)?;
    let run_result = target_provisioner
        .executor()
        .supervisor()
        .run(command, cancellation)
        .await;
    target_provisioner
        .run_actual_merge_boundary_hook("after-actual-merge-child-before-outcome-proof");
    let result = match run_result {
        Ok(result) => result,
        Err(error) if error.child_could_not_have_started() => {
            return Ok(DeliveryMergeOutcome::KnownNotApplied);
        }
        // Never start observation children while descendant cleanup is still
        // unproven. A durable recovery pass may classify the scene later.
        Err(error) if error.process_cleanup_is_unproven() => {
            return Ok(DeliveryMergeOutcome::ReconciliationRequired);
        }
        // The mutation child may have started, but its tree cleanup is proven.
        // Classify only a freshly closed exact repository fact, never the
        // process error itself.
        Err(_) => {
            return Ok(
                classify_unknown_actual_merge_result(&ActualMergeClassificationContext {
                    source_provisioner,
                    target_provisioner,
                    source,
                    target,
                    candidate,
                    source_commit,
                    source_input,
                    expected,
                    message: &message,
                })
                .await,
            );
        }
    };
    classify_actual_merge_result(
        ActualMergeClassificationContext {
            source_provisioner,
            target_provisioner,
            source,
            target,
            candidate,
            source_commit,
            source_input,
            expected,
            message: &message,
        },
        result,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn reprove_ready_preflight(
    source_provisioner: &DeliverySourceProvisioner,
    target_provisioner: &DeliveryTargetProvisioner,
    source: &DeliverySourceCapability,
    target: &DeliveryTargetCapability,
    candidate: &DeliveryCandidateTree,
    source_commit: &DeliverySourceCommit,
    source_input: &DeliverySourceCommitInput,
    expected: &DeliveryPreflightResult,
    cancellation: CancellationToken,
) -> Result<(), DeliveryMergeError> {
    let observed = preflight_delivery_merge(
        source_provisioner,
        target_provisioner,
        target,
        DeliveryPreflightSource::committed(source, candidate, source_commit, source_input),
        cancellation,
    )
    .await?;
    if !observed.is_ready() || observed != *expected {
        return Err(DeliveryMergeError::PreflightStale);
    }
    Ok(())
}

fn require_merge_bindings(
    source: &DeliverySourceCapability,
    target: &DeliveryTargetCapability,
    candidate: &DeliveryCandidateTree,
    source_commit: &DeliverySourceCommit,
    input: &DeliveryMergeInput,
) -> Result<CandidateTreeProvenance, DeliveryMergeError> {
    if !source
        .probe()
        .shares_repository_format_authority_with(target.probe())
        || source.common_directory_identity() != target.common_directory_identity()
        || source.branch_name() == target.branch_name()
    {
        return Err(DeliveryTargetError::AuthenticationChanged.into());
    }
    let provenance = source.candidate_tree_provenance()?;
    let message = input.message()?;
    if !candidate.is_bound_to(&provenance)
        || !source_commit.is_bound_to(candidate.provenance())
        || !message.matches_identity(source.identity())
    {
        return Err(DeliverySourceError::AuthenticationChanged.into());
    }
    Ok(provenance)
}

fn require_ready_preflight(
    preflight: &DeliveryPreflightResult,
    source_commit: &DeliverySourceCommit,
) -> Result<(), DeliveryMergeError> {
    if !preflight.is_ready() || preflight.source_commit_id() != source_commit.object_id() {
        return Err(DeliveryMergeError::PreflightStale);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn verify_expected_merge_object(
    target_provisioner: &DeliveryTargetProvisioner,
    target: &DeliveryTargetCapability,
    expected_commit: &DeliveryCommitOid,
    expected_tree: &DeliveryTreeOid,
    expected_target_parent: &DeliveryCommitOid,
    expected_source_parent: &DeliveryCommitOid,
    message: &DeliveryMergeMessage,
    epoch_seconds: i64,
    cancellation: CancellationToken,
) -> Result<(), DeliveryMergeError> {
    let output = target_provisioner
        .executor()
        .run(
            target
                .mutation_commands()?
                .inspect_commit(expected_commit)?,
            cancellation,
            target_provisioner.limits().max_status_bytes(),
        )
        .await?;
    verify_batched_expected_merge(
        &output,
        expected_commit,
        expected_tree,
        expected_target_parent,
        expected_source_parent,
        message,
        epoch_seconds,
    )
}

/// Re-proves only the exact persisted merge object shape for recovery.  It
/// does not inspect or mutate the live target checkout; callers must surround
/// it with their phase-specific authenticated target observation.
pub(super) async fn revalidate_expected_delivery_merge_object(
    target_provisioner: &DeliveryTargetProvisioner,
    target: &DeliveryTargetCapability,
    expected: &DeliveryExpectedMerge,
    cancellation: CancellationToken,
) -> Result<(), DeliveryMergeError> {
    let message = expected.input.message()?;
    verify_expected_merge_object(
        target_provisioner,
        target,
        &expected.commit,
        &expected.tree,
        &expected.target_parent,
        &expected.source_parent,
        &message,
        expected.input.epoch_seconds,
        cancellation,
    )
    .await
}

struct ActualMergeClassificationContext<'a> {
    source_provisioner: &'a DeliverySourceProvisioner,
    target_provisioner: &'a DeliveryTargetProvisioner,
    source: &'a DeliverySourceCapability,
    target: &'a DeliveryTargetCapability,
    candidate: &'a DeliveryCandidateTree,
    source_commit: &'a DeliverySourceCommit,
    source_input: &'a DeliverySourceCommitInput,
    expected: &'a DeliveryExpectedMerge,
    message: &'a DeliveryMergeMessage,
}

async fn classify_actual_merge_result(
    context: ActualMergeClassificationContext<'_>,
    result: CommandResult,
) -> Result<DeliveryMergeOutcome, DeliveryMergeError> {
    if !merge_result_streams_are_complete(
        &result,
        context.target_provisioner.limits().max_status_bytes(),
    ) {
        return Ok(classify_unknown_actual_merge_result(&context).await);
    }
    let ActualMergeClassificationContext {
        source_provisioner,
        target_provisioner,
        source,
        target,
        candidate,
        source_commit,
        source_input,
        expected,
        message,
    } = context;
    match result.exit_code {
        Some(0) => {
            match verify_expected_merge_object(
                target_provisioner,
                target,
                &expected.commit,
                &expected.tree,
                &expected.target_parent,
                &expected.source_parent,
                message,
                expected.input.epoch_seconds,
                CancellationToken::new(),
            )
            .await
            {
                Ok(())
                    if terminal_actual_merge_state_is_closed(
                        source_provisioner,
                        target_provisioner,
                        source,
                        target,
                        candidate,
                        source_commit,
                        source_input,
                        ActualMergeTargetState::Applied(&expected.commit),
                    )
                    .await =>
                {
                    Ok(DeliveryMergeOutcome::Applied)
                }
                Err(_) => Ok(DeliveryMergeOutcome::ReconciliationRequired),
                Ok(()) => Ok(DeliveryMergeOutcome::ReconciliationRequired),
            }
        }
        Some(1) => {
            if terminal_actual_merge_state_is_closed(
                source_provisioner,
                target_provisioner,
                source,
                target,
                candidate,
                source_commit,
                source_input,
                ActualMergeTargetState::Original,
            )
            .await
            {
                return Ok(DeliveryMergeOutcome::KnownNotApplied);
            }
            match closed_actual_merge_conflict_observation(
                source_provisioner,
                target_provisioner,
                source,
                target,
                candidate,
                source_commit,
                source_input,
                expected,
            )
            .await
            {
                Some(observation) => {
                    match DeliveryKnownMergeConflict::from_observed_actual_merge(
                        source,
                        target,
                        candidate,
                        source_commit,
                        source_input,
                        expected,
                        observation,
                    ) {
                        Ok(conflict) => Ok(DeliveryMergeOutcome::ConflictObserved(conflict)),
                        Err(_) => Ok(DeliveryMergeOutcome::ReconciliationRequired),
                    }
                }
                None => Ok(DeliveryMergeOutcome::ReconciliationRequired),
            }
        }
        _ => Ok(
            classify_unknown_actual_merge_result(&ActualMergeClassificationContext {
                source_provisioner,
                target_provisioner,
                source,
                target,
                candidate,
                source_commit,
                source_input,
                expected,
                message,
            })
            .await,
        ),
    }
}

/// A process envelope which cannot prove the mutation result is classified
/// only after process cleanup has already succeeded. The applied branch also
/// re-proves the immutable expected object, then both branches close the live
/// source/target scene as S -> T -> S -> T. Any partial, conflicting, or
/// unobservable scene stays reconciliation-required.
async fn classify_unknown_actual_merge_result(
    context: &ActualMergeClassificationContext<'_>,
) -> DeliveryMergeOutcome {
    if verify_expected_merge_object(
        context.target_provisioner,
        context.target,
        &context.expected.commit,
        &context.expected.tree,
        &context.expected.target_parent,
        &context.expected.source_parent,
        context.message,
        context.expected.input.epoch_seconds,
        CancellationToken::new(),
    )
    .await
    .is_ok()
        && terminal_actual_merge_state_is_closed(
            context.source_provisioner,
            context.target_provisioner,
            context.source,
            context.target,
            context.candidate,
            context.source_commit,
            context.source_input,
            ActualMergeTargetState::Applied(&context.expected.commit),
        )
        .await
    {
        return DeliveryMergeOutcome::Applied;
    }
    if terminal_actual_merge_state_is_closed(
        context.source_provisioner,
        context.target_provisioner,
        context.source,
        context.target,
        context.candidate,
        context.source_commit,
        context.source_input,
        ActualMergeTargetState::Original,
    )
    .await
    {
        DeliveryMergeOutcome::KnownNotApplied
    } else {
        DeliveryMergeOutcome::ReconciliationRequired
    }
}

#[derive(Clone, Copy)]
enum ActualMergeTargetState<'a> {
    Original,
    Applied(&'a DeliveryCommitOid),
}

#[allow(clippy::too_many_arguments)]
async fn terminal_actual_merge_state_is_closed(
    source_provisioner: &DeliverySourceProvisioner,
    target_provisioner: &DeliveryTargetProvisioner,
    source: &DeliverySourceCapability,
    target: &DeliveryTargetCapability,
    candidate: &DeliveryCandidateTree,
    source_commit: &DeliverySourceCommit,
    source_input: &DeliverySourceCommitInput,
    target_state: ActualMergeTargetState<'_>,
) -> bool {
    // The repository lease held by the application serializes cooperating
    // delivery mutations. Repeat both independent facts so an actual child
    // result cannot become terminal after observing only one side of the
    // source/target pair.
    for _ in 0..2 {
        if source_provisioner
            .revalidate_preflight_committed_source(
                source,
                candidate,
                source_commit,
                source_input,
                CancellationToken::new(),
            )
            .await
            .is_err()
        {
            return false;
        }
        let target_result = match target_state {
            ActualMergeTargetState::Original => {
                target_provisioner
                    .revalidate_delivery_target(target, CancellationToken::new())
                    .await
            }
            ActualMergeTargetState::Applied(expected_head) => {
                target_provisioner
                    .revalidate_applied_delivery_target(
                        target,
                        expected_head,
                        CancellationToken::new(),
                    )
                    .await
            }
        };
        if target_result.is_err() {
            return false;
        }
    }
    true
}

#[allow(clippy::too_many_arguments)]
async fn closed_actual_merge_conflict_observation(
    source_provisioner: &DeliverySourceProvisioner,
    target_provisioner: &DeliveryTargetProvisioner,
    source: &DeliverySourceCapability,
    target: &DeliveryTargetCapability,
    candidate: &DeliveryCandidateTree,
    source_commit: &DeliverySourceCommit,
    source_input: &DeliverySourceCommitInput,
    expected: &DeliveryExpectedMerge,
) -> Option<StableMergeConflictObservation> {
    let mut first = None;
    for round in 0..2 {
        source_provisioner
            .revalidate_preflight_committed_source(
                source,
                candidate,
                source_commit,
                source_input,
                CancellationToken::new(),
            )
            .await
            .ok()?;
        let observed = target_provisioner
            .observe_expected_merge_conflict(
                target,
                expected.merge_base(),
                source_commit.commit(),
                expected.tree(),
                CancellationToken::new(),
            )
            .await
            .ok()??;
        if round == 0 {
            first = Some(observed);
        } else if first.as_ref() != Some(&observed) {
            return None;
        } else {
            return Some(observed);
        }
    }
    None
}

fn merge_result_streams_are_complete(result: &CommandResult, output_limit: usize) -> bool {
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

fn parse_created_merge_oid(
    output: &[u8],
    object_format: DeliveryGitObjectFormat,
) -> Result<DeliveryCommitOid, DeliveryMergeError> {
    let length = object_format.hexadecimal_length();
    if output.len() != length.saturating_add(1) || output.get(length) != Some(&b'\n') {
        return Err(DeliveryMergeError::ExpectedObjectInvalid);
    }
    let object_id = std::str::from_utf8(&output[..length])
        .map_err(|_| DeliveryMergeError::ExpectedObjectInvalid)?;
    DeliveryCommitOid::try_new(object_id, object_format)
        .ok_or(DeliveryMergeError::ExpectedObjectInvalid)
}

fn verify_batched_expected_merge(
    response: &[u8],
    expected_commit: &DeliveryCommitOid,
    expected_tree: &DeliveryTreeOid,
    expected_target_parent: &DeliveryCommitOid,
    expected_source_parent: &DeliveryCommitOid,
    message: &DeliveryMergeMessage,
    epoch_seconds: i64,
) -> Result<(), DeliveryMergeError> {
    let payload = expected_merge_payload(
        expected_tree,
        expected_target_parent,
        expected_source_parent,
        message,
        epoch_seconds,
    );
    let newline = response
        .iter()
        .position(|byte| *byte == b'\n')
        .ok_or(DeliveryMergeError::ExpectedObjectInvalid)?;
    let header = &response[..newline];
    let remainder = &response[newline + 1..];
    if header.contains(&b'\r') || !verify_batch_header(header, expected_commit, payload.len()) {
        return Err(DeliveryMergeError::ExpectedObjectInvalid);
    }
    if remainder.len() != payload.len().saturating_add(1)
        || !remainder.ends_with(b"\n")
        || remainder[..payload.len()] != payload
    {
        return Err(DeliveryMergeError::ExpectedObjectInvalid);
    }
    Ok(())
}

/// Accepts exactly the ordinary `cat-file --batch` commit header.  Keeping
/// this parser local prevents the two-parent Task 14 proof from accidentally
/// inheriting a one-parent source-object assumption.
fn verify_batch_header(
    header: &[u8],
    expected_commit: &DeliveryCommitOid,
    expected_size: usize,
) -> bool {
    let mut fields = header.split(|byte| *byte == b' ');
    let Some(object_id) = fields.next() else {
        return false;
    };
    let Some(object_type) = fields.next() else {
        return false;
    };
    let Some(size) = fields.next() else {
        return false;
    };
    fields.next().is_none()
        && object_id == expected_commit.as_str().as_bytes()
        && object_type == b"commit"
        && parse_decimal_size(size) == Some(expected_size)
}

fn parse_decimal_size(value: &[u8]) -> Option<usize> {
    if value.is_empty() || (value.len() > 1 && value[0] == b'0') {
        return None;
    }
    value.iter().try_fold(0usize, |total, byte| {
        byte.is_ascii_digit()
            .then_some(())
            .and_then(|()| total.checked_mul(10))
            .and_then(|total| total.checked_add(usize::from(*byte - b'0')))
    })
}

fn expected_merge_payload(
    tree: &DeliveryTreeOid,
    target_parent: &DeliveryCommitOid,
    source_parent: &DeliveryCommitOid,
    message: &DeliveryMergeMessage,
    epoch_seconds: i64,
) -> Vec<u8> {
    let message = message.object_message_bytes();
    let epoch = epoch_seconds.to_string();
    let mut payload = Vec::with_capacity(
        b"tree \nparent \nparent \nauthor Coding Agent <coding-agent@localhost>  +0000\ncommitter Coding Agent <coding-agent@localhost>  +0000\n\n"
            .len()
            + tree.as_str().len()
            + target_parent.as_str().len()
            + source_parent.as_str().len()
            + epoch.len().saturating_mul(2)
            + message.len()
            + 64,
    );
    payload.extend_from_slice(b"tree ");
    payload.extend_from_slice(tree.as_str().as_bytes());
    payload.extend_from_slice(b"\nparent ");
    payload.extend_from_slice(target_parent.as_str().as_bytes());
    payload.extend_from_slice(b"\nparent ");
    payload.extend_from_slice(source_parent.as_str().as_bytes());
    payload.extend_from_slice(b"\nauthor Coding Agent <coding-agent@localhost> ");
    payload.extend_from_slice(epoch.as_bytes());
    payload.extend_from_slice(b" +0000\ncommitter Coding Agent <coding-agent@localhost> ");
    payload.extend_from_slice(epoch.as_bytes());
    payload.extend_from_slice(b" +0000\n\n");
    payload.extend_from_slice(&message);
    payload
}

#[cfg(test)]
mod tests {
    use super::super::DeliveryGitObjectFormat;
    use super::*;

    const TASK_ID: &str = "123e4567-e89b-12d3-a456-426614174000";
    const EXPECTED_COMMIT: &str = "cccccccccccccccccccccccccccccccccccccccc";
    const TREE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const TARGET_PARENT: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const SOURCE_PARENT: &str = "dddddddddddddddddddddddddddddddddddddddd";
    const EPOCH_SECONDS: i64 = 1_700_000_000;

    #[test]
    fn expected_merge_payload_is_the_exact_ordered_two_parent_commit() {
        let (commit, tree, target_parent, source_parent, message) = fixture();
        let payload = expected_merge_payload(
            &tree,
            &target_parent,
            &source_parent,
            &message,
            EPOCH_SECONDS,
        );

        assert_eq!(
            payload.as_slice(),
            concat!(
                "tree aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n",
                "parent bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\n",
                "parent dddddddddddddddddddddddddddddddddddddddd\n",
                "author Coding Agent <coding-agent@localhost> 1700000000 +0000\n",
                "committer Coding Agent <coding-agent@localhost> 1700000000 +0000\n",
                "\n",
                "coding-agent: merge task 123e4567-e89b-12d3-a456-426614174000 attempt 7\n",
            )
            .as_bytes()
        );

        let response = batch_response(&commit, &payload);
        assert!(
            verify_batched_expected_merge(
                &response,
                &commit,
                &tree,
                &target_parent,
                &source_parent,
                &message,
                EPOCH_SECONDS,
            )
            .is_ok()
        );
    }

    #[test]
    fn batch_verifier_rejects_parent_order_and_parent_count_changes() {
        let (commit, tree, target_parent, source_parent, message) = fixture();
        let reversed = expected_merge_payload(
            &tree,
            &source_parent,
            &target_parent,
            &message,
            EPOCH_SECONDS,
        );
        let mut missing_second_parent = expected_merge_payload(
            &tree,
            &target_parent,
            &source_parent,
            &message,
            EPOCH_SECONDS,
        );
        let second_parent = format!("\nparent {}", source_parent.as_str());
        let offset = missing_second_parent
            .windows(second_parent.len())
            .position(|window| window == second_parent.as_bytes())
            .expect("fixture contains source parent exactly once");
        missing_second_parent.drain(offset..offset + second_parent.len());

        for response in [
            batch_response(&commit, &reversed),
            batch_response(&commit, &missing_second_parent),
        ] {
            assert_eq!(
                verify_batched_expected_merge(
                    &response,
                    &commit,
                    &tree,
                    &target_parent,
                    &source_parent,
                    &message,
                    EPOCH_SECONDS,
                ),
                Err(DeliveryMergeError::ExpectedObjectInvalid)
            );
        }
    }

    #[test]
    fn batch_verifier_rejects_malformed_headers_and_declared_sizes() {
        let (commit, tree, target_parent, source_parent, message) = fixture();
        let payload = expected_merge_payload(
            &tree,
            &target_parent,
            &source_parent,
            &message,
            EPOCH_SECONDS,
        );
        let malformed_headers = [
            format!("{} blob {}", commit.as_str(), payload.len()),
            format!("{} commit", commit.as_str()),
            format!("{} commit 0{}", commit.as_str(), payload.len()),
            format!("{} commit {}", commit.as_str(), payload.len() + 1),
            format!("{} commit {}x", commit.as_str(), payload.len()),
            format!("{} commit {} extra", commit.as_str(), payload.len()),
            format!("{} commit {}\r", commit.as_str(), payload.len()),
        ];

        for header in malformed_headers {
            let response = batch_response_with_header(header.as_bytes(), &payload);
            assert_eq!(
                verify_batched_expected_merge(
                    &response,
                    &commit,
                    &tree,
                    &target_parent,
                    &source_parent,
                    &message,
                    EPOCH_SECONDS,
                ),
                Err(DeliveryMergeError::ExpectedObjectInvalid),
                "header {header:?} must not be accepted"
            );
        }
    }

    #[test]
    fn incomplete_or_abnormal_process_state_cannot_prove_merge_application() {
        let output_limit = 16;
        let clean = command_result();
        assert!(merge_result_streams_are_complete(&clean, output_limit));

        let mut cases = Vec::new();

        let mut cancelled = clean.clone();
        cancelled.cancelled = true;
        cases.push(("cancelled", cancelled));

        let mut timed_out = clean.clone();
        timed_out.timed_out = true;
        cases.push(("timed out", timed_out));

        let mut signalled = clean.clone();
        signalled.signal = Some(9);
        cases.push(("signal", signalled));

        let mut result_truncated = clean.clone();
        result_truncated.truncated = true;
        cases.push(("result truncation", result_truncated));

        let mut stdout_incomplete = clean.clone();
        stdout_incomplete.stdout.complete = false;
        cases.push(("incomplete stdout", stdout_incomplete));

        let mut stderr_truncated = clean.clone();
        stderr_truncated.stderr.truncated = true;
        cases.push(("truncated stderr", stderr_truncated));

        let mut stdout_omitted = clean.clone();
        stdout_omitted.stdout.omitted_observed_bytes = 1;
        stdout_omitted.stdout.observed_bytes = 1;
        cases.push(("omitted stdout", stdout_omitted));

        let mut inconsistent_count = clean.clone();
        inconsistent_count.stderr.observed_bytes = 1;
        cases.push(("inconsistent observed byte count", inconsistent_count));

        let mut oversized = clean.clone();
        oversized.stdout.head = vec![b'x'; output_limit + 1];
        oversized.stdout.observed_bytes = (output_limit + 1) as u64;
        cases.push(("output above proof bound", oversized));

        // `classify_actual_merge_result` checks this gate before interpreting
        // the exit code. Every rejected envelope therefore requires fresh
        // exact repository closure; the envelope alone proves no outcome.
        for (case, result) in cases {
            assert!(
                !merge_result_streams_are_complete(&result, output_limit),
                "{case} must require fresh repository closure"
            );
        }
    }

    fn fixture() -> (
        DeliveryCommitOid,
        DeliveryTreeOid,
        DeliveryCommitOid,
        DeliveryCommitOid,
        DeliveryMergeMessage,
    ) {
        (
            DeliveryCommitOid::try_new(EXPECTED_COMMIT, DeliveryGitObjectFormat::Sha1).unwrap(),
            DeliveryTreeOid::try_new(TREE, DeliveryGitObjectFormat::Sha1).unwrap(),
            DeliveryCommitOid::try_new(TARGET_PARENT, DeliveryGitObjectFormat::Sha1).unwrap(),
            DeliveryCommitOid::try_new(SOURCE_PARENT, DeliveryGitObjectFormat::Sha1).unwrap(),
            DeliveryMergeMessage::try_new(TASK_ID, 7).unwrap(),
        )
    }

    fn batch_response(commit: &DeliveryCommitOid, payload: &[u8]) -> Vec<u8> {
        let header = format!("{} commit {}", commit.as_str(), payload.len());
        batch_response_with_header(header.as_bytes(), payload)
    }

    fn batch_response_with_header(header: &[u8], payload: &[u8]) -> Vec<u8> {
        let mut response = Vec::with_capacity(header.len() + payload.len() + 2);
        response.extend_from_slice(header);
        response.push(b'\n');
        response.extend_from_slice(payload);
        response.push(b'\n');
        response
    }

    fn command_result() -> CommandResult {
        CommandResult {
            exit_code: Some(0),
            signal: None,
            timed_out: false,
            cancelled: false,
            stdout: complete_stream(&[]),
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
