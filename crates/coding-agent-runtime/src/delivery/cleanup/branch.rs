//! Target-verified source-branch cleanup.
//!
//! Branch deletion is deliberately a second receipt after worktree cleanup.
//! The runtime first proves that the application-owned linked worktree is
//! absent, then binds one authenticated registered checkout, the exact source
//! ref/OID, and the persisted target ref/OID into an opaque intent. Every
//! retry starts with fresh, bounded Git observations. The sole mutation is one
//! atomic `verify target + delete source` ref transaction.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::{
    CleanupObservation, DeliveryWorktreeCleanupError, DeliveryWorktreeCleanupIntent,
    DeliveryWorktreeCleanupProvisioner,
};
use crate::SealedProcessLivenessScope;
use crate::delivery::command::DeliveryBranchCleanupCommands;
use crate::delivery::observation::parse_object_id;
use crate::delivery::output::DeliveryCommandExit;
use crate::delivery::source_commit::verify_batched_source_commit;
use crate::delivery::{
    DeliveryCommitOid, DeliverySourceCommitInput, DeliverySourceError, DeliverySourceProvisioner,
    DeliveryTargetCapability, DeliveryTreeOid,
};
use crate::worktree::CleanupTopologyObservation;

/// In-process evidence for exactly one persisted branch-cleanup target.
///
/// There is intentionally no constructor from refs, object IDs, paths, or
/// Store strings. Task 21 owns any future trusted persisted rehydration
/// adapter. Runtime capture consumes a worktree-cleanup intent whose exact
/// topology has just been re-proved absent and a fresh target capability.
#[derive(Clone)]
pub struct DeliveryBranchCleanupIntent {
    inner: Arc<DeliveryBranchCleanupIntentInner>,
}

struct DeliveryBranchCleanupIntentInner {
    context: Arc<DeliveryBranchCleanupContext>,
    expected_target: DeliveryCommitOid,
    generation: u64,
}

struct DeliveryBranchCleanupContext {
    worktree: DeliveryWorktreeCleanupIntent,
    target: DeliveryTargetCapability,
    source_branch: String,
    target_branch: String,
    expected_source: DeliveryCommitOid,
    expected_source_tree: DeliveryTreeOid,
    expected_source_parent: DeliveryCommitOid,
    source_input: DeliverySourceCommitInput,
    generation_gate: Arc<DeliveryBranchCleanupGenerationGate>,
}

const INITIAL_BRANCH_CLEANUP_GENERATION: u64 = 1;
const BRANCH_CLEANUP_MUTATION_CLAIMED: u64 = 1 << 63;
const MAX_BRANCH_CLEANUP_GENERATION: u64 = BRANCH_CLEANUP_MUTATION_CLAIMED - 1;

/// Process-local revocation for one opaque branch-cleanup intent chain.
///
/// The low bits are the active generation. The high bit is a short-lived
/// mutation claim. A refresh can advance the generation only while no delete
/// is in flight, and a delete can be claimed only by the active generation.
/// Task 22 still owns binding that local generation to the persisted operation
/// version; this gate only prevents already-minted runtime capabilities from
/// regaining authority after their refresh has been adopted.
struct DeliveryBranchCleanupGenerationGate {
    state: AtomicU64,
}

impl DeliveryBranchCleanupGenerationGate {
    fn new() -> Self {
        Self {
            state: AtomicU64::new(INITIAL_BRANCH_CLEANUP_GENERATION),
        }
    }

    fn is_current(&self, generation: u64) -> bool {
        self.state.load(Ordering::Acquire) & MAX_BRANCH_CLEANUP_GENERATION == generation
    }

    fn try_adopt(&self, previous: u64, next: u64) -> bool {
        previous < MAX_BRANCH_CLEANUP_GENERATION
            && next == previous + 1
            && self
                .state
                .compare_exchange(previous, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
    }

    fn try_claim_mutation(
        self: &Arc<Self>,
        generation: u64,
    ) -> Option<DeliveryBranchCleanupMutationClaim> {
        let claimed = generation | BRANCH_CLEANUP_MUTATION_CLAIMED;
        self.state
            .compare_exchange(generation, claimed, Ordering::AcqRel, Ordering::Acquire)
            .ok()?;
        Some(DeliveryBranchCleanupMutationClaim {
            gate: Arc::clone(self),
            generation,
        })
    }
}

struct DeliveryBranchCleanupMutationClaim {
    gate: Arc<DeliveryBranchCleanupGenerationGate>,
    generation: u64,
}

impl Drop for DeliveryBranchCleanupMutationClaim {
    fn drop(&mut self) {
        let claimed = self.generation | BRANCH_CLEANUP_MUTATION_CLAIMED;
        let released = self.gate.state.compare_exchange(
            claimed,
            self.generation,
            Ordering::Release,
            Ordering::Relaxed,
        );
        debug_assert!(released.is_ok(), "branch cleanup mutation claim changed");
    }
}

impl DeliveryBranchCleanupIntent {
    /// Compares exact process-local provenance without exposing any durable
    /// value. Trusted authorizers use this to bind a capability to the intent
    /// whose `DeletePending` version they accepted.
    pub fn is_same_runtime_intent(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    fn refreshed(&self, fresh_target: DeliveryCommitOid) -> Option<Self> {
        let generation = self.inner.generation.checked_add(1)?;
        if generation > MAX_BRANCH_CLEANUP_GENERATION {
            return None;
        }
        Some(Self {
            inner: Arc::new(DeliveryBranchCleanupIntentInner {
                context: Arc::clone(&self.inner.context),
                expected_target: fresh_target,
                generation,
            }),
        })
    }

    fn is_current_generation(&self) -> bool {
        self.inner
            .context
            .generation_gate
            .is_current(self.inner.generation)
    }

    fn try_claim_mutation(&self) -> Option<DeliveryBranchCleanupMutationClaim> {
        self.inner
            .context
            .generation_gate
            .try_claim_mutation(self.inner.generation)
    }
}

impl fmt::Debug for DeliveryBranchCleanupIntent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryBranchCleanupIntent(<opaque>)")
    }
}

/// Durable `DeletePending` authorization. Implementations normally bind this
/// call to the exact Store operation/version and persisted expected target
/// HEAD that accepted the cleanup request.
#[async_trait]
pub trait DeliveryDeletePendingAuthorizer: Send + Sync {
    type Error: Send;

    async fn authorize_persisted_delete_pending(
        &self,
        intent: &DeliveryBranchCleanupIntent,
    ) -> Result<(), Self::Error>;
}

/// One-shot authority for a single persisted `DeletePending` intent.
pub struct DeliveryDeletePendingCapability {
    intent: DeliveryBranchCleanupIntent,
}

impl fmt::Debug for DeliveryDeletePendingCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryDeletePendingCapability(<opaque>)")
    }
}

/// Authorizes only the exact opaque branch-cleanup intent accepted by the
/// caller's durable-state boundary.
pub async fn authorize_persisted_delivery_branch_delete<A>(
    authorizer: &A,
    intent: DeliveryBranchCleanupIntent,
) -> Result<DeliveryDeletePendingCapability, A::Error>
where
    A: DeliveryDeletePendingAuthorizer,
{
    authorizer
        .authorize_persisted_delete_pending(&intent)
        .await?;
    Ok(DeliveryDeletePendingCapability { intent })
}

/// Opaque proof that the persisted target legally advanced while continuing
/// to contain the exact source commit. The fresh HEAD may be persisted, then
/// the refreshed intent must cross a new authorization boundary. An older
/// capability remains bound to its older target CAS and can never delete.
pub struct DeliveryBranchCleanupRefreshProof {
    refreshed_intent: DeliveryBranchCleanupIntent,
}

impl DeliveryBranchCleanupRefreshProof {
    /// The newly observed target HEAD that must become the next persisted
    /// `DeletePending` expected target before another capability is minted.
    pub fn fresh_target_head(&self) -> &str {
        self.refreshed_intent.inner.expected_target.as_str()
    }

    /// Adopts this refresh and consumes the proof into a new opaque intent.
    ///
    /// The caller must first persist the fresh target as the next
    /// `DeletePending` version. Adoption atomically revokes every capability
    /// minted from the preceding runtime generation. It fails closed if a
    /// delete is already in flight or another refresh won the generation CAS;
    /// on failure the proof is returned so the caller can finish the in-flight
    /// operation or re-observe durable state. The returned intent still has to
    /// cross the durable authorizer before it can mutate a ref.
    pub fn into_refreshed_intent(
        self,
    ) -> Result<DeliveryBranchCleanupIntent, DeliveryBranchCleanupRefreshProof> {
        let previous = self.refreshed_intent.inner.generation - 1;
        if self
            .refreshed_intent
            .inner
            .context
            .generation_gate
            .try_adopt(previous, self.refreshed_intent.inner.generation)
        {
            Ok(self.refreshed_intent)
        } else {
            Err(self)
        }
    }
}

impl fmt::Debug for DeliveryBranchCleanupRefreshProof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryBranchCleanupRefreshProof(<opaque>)")
    }
}

/// Query-first result for a persisted source-branch `DeletePending` receipt.
pub enum DeliveryDeletePendingDisposition {
    RetryExactDelete,
    Deleted,
    RefreshExpectedTarget(DeliveryBranchCleanupRefreshProof),
    KnownNotAppliedSourceNotMerged,
    KnownNotAppliedCommandTimedOut,
    ReconciliationRequired,
}

impl fmt::Debug for DeliveryDeletePendingDisposition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::RetryExactDelete => "DeliveryDeletePendingDisposition::RetryExactDelete",
            Self::Deleted => "DeliveryDeletePendingDisposition::Deleted",
            Self::RefreshExpectedTarget(_) => {
                "DeliveryDeletePendingDisposition::RefreshExpectedTarget(<opaque>)"
            }
            Self::KnownNotAppliedSourceNotMerged => {
                "DeliveryDeletePendingDisposition::KnownNotAppliedSourceNotMerged"
            }
            Self::KnownNotAppliedCommandTimedOut => {
                "DeliveryDeletePendingDisposition::KnownNotAppliedCommandTimedOut"
            }
            Self::ReconciliationRequired => {
                "DeliveryDeletePendingDisposition::ReconciliationRequired"
            }
        })
    }
}

impl DeliveryWorktreeCleanupProvisioner {
    /// Captures the second cleanup intent only after re-proving the first
    /// cleanup's exact `Removed` fact and the target/source deletion scene.
    #[allow(clippy::too_many_arguments)]
    pub async fn capture_branch_cleanup_intent(
        &self,
        source_provisioner: &DeliverySourceProvisioner,
        cleanup_intent: DeliveryWorktreeCleanupIntent,
        target: DeliveryTargetCapability,
        processes: &SealedProcessLivenessScope,
        cancellation: CancellationToken,
    ) -> Result<DeliveryBranchCleanupIntent, DeliveryWorktreeCleanupError> {
        self.require_process_cleanup(processes)?;
        if !target
            .probe()
            .shares_repository_format_authority_with(&cleanup_intent.inner.repository_probe)
            || target.branch_name() == cleanup_intent.inner.source_branch
            || !cleanup_intent
                .inner
                .source
                .matches_cleanup_common_identity(target.common_directory_identity())
        {
            return Err(DeliveryWorktreeCleanupError::AuthenticationChanged);
        }

        let removed = self
            .observe_cleanup_state(
                source_provisioner,
                &cleanup_intent,
                processes,
                cancellation.clone(),
            )
            .await?;
        if removed.fact() != CleanupObservation::AbsentExact {
            return Err(DeliveryWorktreeCleanupError::SourceChanged);
        }

        let expected_source = cleanup_intent.inner.expected_source_commit.clone();
        let expected_source_tree = DeliveryTreeOid::try_new(
            cleanup_intent.inner.source.candidate_tree_object_id(),
            cleanup_intent.inner.repository_probe.object_format(),
        )
        .ok_or(DeliveryWorktreeCleanupError::AuthenticationChanged)?;
        let expected_source_parent = DeliveryCommitOid::try_new(
            cleanup_intent.inner.source.base_commit_object_id(),
            cleanup_intent.inner.repository_probe.object_format(),
        )
        .ok_or(DeliveryWorktreeCleanupError::AuthenticationChanged)?;
        let source_input = cleanup_intent.inner.source.input().clone();
        let source_branch = cleanup_intent.inner.source_branch.clone();
        let target_branch = target.branch_name().to_owned();
        let expected_target = target.head().clone();
        let generation_gate = Arc::new(DeliveryBranchCleanupGenerationGate::new());
        let intent = DeliveryBranchCleanupIntent {
            inner: Arc::new(DeliveryBranchCleanupIntentInner {
                context: Arc::new(DeliveryBranchCleanupContext {
                    worktree: cleanup_intent,
                    target,
                    source_branch,
                    target_branch,
                    expected_source,
                    expected_source_tree,
                    expected_source_parent,
                    source_input,
                    generation_gate,
                }),
                expected_target,
                generation: INITIAL_BRANCH_CLEANUP_GENERATION,
            }),
        };

        let observation = self
            .observe_branch_cleanup_state(&intent, processes, cancellation)
            .await?;
        match observation {
            BranchCleanupObservation::ExactPresent => Ok(intent),
            BranchCleanupObservation::Absent
            | BranchCleanupObservation::Refresh(_)
            | BranchCleanupObservation::SourceNotMerged
            | BranchCleanupObservation::Inconsistent => {
                Err(DeliveryWorktreeCleanupError::SourceChanged)
            }
        }
    }

    /// Re-observes a persisted delete intent without mutating any ref.
    pub async fn classify_delivery_delete_pending(
        &self,
        capability: &DeliveryDeletePendingCapability,
        processes: &SealedProcessLivenessScope,
        cancellation: CancellationToken,
    ) -> Result<DeliveryDeletePendingDisposition, DeliveryWorktreeCleanupError> {
        let observation = self
            .observe_branch_cleanup_state(&capability.intent, processes, cancellation)
            .await?;
        Ok(branch_disposition(&capability.intent, observation))
    }

    /// Performs the sole branch-cleanup mutation after two fresh exact query
    /// boundaries, then classifies the durable fact again under an independent
    /// cancellation token. The capability is consumed at the mutation edge.
    pub async fn retry_delivery_delete_pending(
        &self,
        capability: DeliveryDeletePendingCapability,
        processes: &SealedProcessLivenessScope,
        cancellation: CancellationToken,
    ) -> Result<DeliveryDeletePendingDisposition, DeliveryWorktreeCleanupError> {
        let first = self
            .observe_branch_cleanup_state(&capability.intent, processes, cancellation.clone())
            .await?;
        if first != BranchCleanupObservation::ExactPresent {
            return Ok(branch_disposition(&capability.intent, first));
        }

        let rechecked = self
            .observe_branch_cleanup_state(&capability.intent, processes, cancellation.clone())
            .await?;
        if rechecked != BranchCleanupObservation::ExactPresent {
            return Ok(branch_disposition(&capability.intent, rechecked));
        }

        self.run_cleanup_boundary_hook("after-branch-query-before-delete-spawn");
        let Some(generation_claim) = capability.intent.try_claim_mutation() else {
            return Ok(DeliveryDeletePendingDisposition::ReconciliationRequired);
        };
        let commands = self.branch_cleanup_commands(&capability.intent)?;
        self.run_cleanup_boundary_hook("before-atomic-branch-delete-spawn");
        let child = self
            .executor
            .run(
                commands.delete_source_transaction()?,
                cancellation,
                self.limits.max_status_bytes(),
            )
            .await;
        drop(commands);

        if branch_child_cleanup_is_unproven(&child) {
            return Err(DeliveryWorktreeCleanupError::ProcessCleanupUnproven);
        }

        let closing = match self
            .observe_branch_cleanup_state(&capability.intent, processes, CancellationToken::new())
            .await
        {
            Ok(observation) => observation,
            Err(DeliveryWorktreeCleanupError::ProcessCleanupUnproven) => {
                return Err(DeliveryWorktreeCleanupError::ProcessCleanupUnproven);
            }
            Err(_) => return Ok(DeliveryDeletePendingDisposition::ReconciliationRequired),
        };

        if !successful_branch_child_matches_closing_observation(&child, &closing) {
            drop(generation_claim);
            return Ok(DeliveryDeletePendingDisposition::ReconciliationRequired);
        }

        let disposition = match (&child, closing) {
            (_, BranchCleanupObservation::Absent) => DeliveryDeletePendingDisposition::Deleted,
            (
                Err(DeliverySourceError::TimedOut),
                BranchCleanupObservation::ExactPresent
                | BranchCleanupObservation::Refresh(_)
                | BranchCleanupObservation::SourceNotMerged,
            ) => DeliveryDeletePendingDisposition::KnownNotAppliedCommandTimedOut,
            (Err(DeliverySourceError::ChildOutcomeUnknown), _) => {
                DeliveryDeletePendingDisposition::ReconciliationRequired
            }
            (Err(_), BranchCleanupObservation::ExactPresent) => {
                DeliveryDeletePendingDisposition::RetryExactDelete
            }
            (_, observation) => branch_disposition(&capability.intent, observation),
        };
        drop(generation_claim);
        Ok(disposition)
    }

    async fn observe_branch_cleanup_state(
        &self,
        intent: &DeliveryBranchCleanupIntent,
        processes: &SealedProcessLivenessScope,
        cancellation: CancellationToken,
    ) -> Result<BranchCleanupObservation, DeliveryWorktreeCleanupError> {
        if cancellation.is_cancelled() {
            return Err(DeliveryWorktreeCleanupError::Cancelled);
        }
        if !self.branch_intent_context_is_current(intent)
            || self.require_process_cleanup(processes).is_err()
            || !self.branch_worktree_is_absent(intent)
        {
            return Ok(BranchCleanupObservation::Inconsistent);
        }

        let commands = self.branch_cleanup_commands(intent)?;
        let first = match self
            .observe_branch_cleanup_scene(&commands, intent, cancellation.clone())
            .await?
        {
            Some(scene) => scene,
            None => return Ok(BranchCleanupObservation::Inconsistent),
        };
        if self.require_process_cleanup(processes).is_err()
            || !self.branch_worktree_is_absent(intent)
            || !self.branch_target_authentication_is_current(intent)
        {
            return Ok(BranchCleanupObservation::Inconsistent);
        }
        let second = match self
            .observe_branch_cleanup_scene(&commands, intent, cancellation)
            .await?
        {
            Some(scene) => scene,
            None => return Ok(BranchCleanupObservation::Inconsistent),
        };
        if first != second
            || self.require_process_cleanup(processes).is_err()
            || !self.branch_worktree_is_absent(intent)
            || !self.branch_target_authentication_is_current(intent)
        {
            return Ok(BranchCleanupObservation::Inconsistent);
        }
        Ok(branch_observation_for_observed_scene(
            &first,
            &intent.inner.expected_target,
        ))
    }

    async fn observe_branch_cleanup_scene(
        &self,
        commands: &DeliveryBranchCleanupCommands,
        intent: &DeliveryBranchCleanupIntent,
        cancellation: CancellationToken,
    ) -> Result<Option<ObservedBranchCleanupScene>, DeliveryWorktreeCleanupError> {
        let source = match self
            .observe_branch_source_ref(commands, intent, cancellation.clone())
            .await?
        {
            Some(source) => source,
            None => return Ok(None),
        };
        let fresh_target = match self
            .observe_branch_target_ref(commands, intent, cancellation.clone())
            .await?
        {
            Some(TargetRefObservation::Commit(target)) => target,
            Some(TargetRefObservation::Absent) => {
                return Ok(Some(ObservedBranchCleanupScene::TargetAbsent(source)));
            }
            None => return Ok(None),
        };
        let expected_source_is_commit = match self
            .observe_expected_source_commit(
                commands.inspect_expected_source_commit()?,
                &intent.inner.context,
                cancellation.clone(),
            )
            .await?
        {
            Some(observation) => observation,
            None => return Ok(None),
        };
        let expected_target_is_commit = match self
            .observe_branch_commit_object(
                commands.inspect_expected_target_commit()?,
                &intent.inner.expected_target,
                cancellation.clone(),
            )
            .await?
        {
            Some(observation) => observation,
            None => return Ok(None),
        };
        let fresh_target_is_commit = match self
            .observe_branch_commit_object(
                commands.inspect_fresh_target_commit(&fresh_target)?,
                &fresh_target,
                cancellation.clone(),
            )
            .await?
        {
            Some(observation) => observation,
            None => return Ok(None),
        };
        if !expected_source_is_commit || !expected_target_is_commit || !fresh_target_is_commit {
            return Ok(Some(ObservedBranchCleanupScene::TargetPresent(
                BranchCleanupScene {
                    source,
                    fresh_target,
                    expected_source_is_commit,
                    expected_target_is_commit,
                    fresh_target_is_commit,
                    source_is_ancestor: false,
                    expected_target_is_ancestor: false,
                    source_is_checked_out: false,
                },
            )));
        }
        let source_is_ancestor = self
            .observe_branch_predicate(
                commands.source_is_ancestor_of_fresh_target(&fresh_target)?,
                cancellation.clone(),
            )
            .await?;
        let expected_target_is_ancestor = self
            .observe_branch_predicate(
                commands.expected_target_is_ancestor_of_fresh_target(&fresh_target)?,
                cancellation.clone(),
            )
            .await?;
        let worktrees = match branch_bounded_output(
            self.executor
                .run(
                    commands.worktree_list_porcelain()?,
                    cancellation,
                    self.limits.max_status_bytes(),
                )
                .await,
        )? {
            Some(output) => output,
            None => return Ok(None),
        };
        let Some(source_is_checked_out) = source_branch_is_checked_out(
            &worktrees,
            &intent.inner.context.source_branch,
            intent
                .inner
                .context
                .worktree
                .inner
                .repository_probe
                .object_format()
                .hexadecimal_length(),
            self.limits.max_paths(),
        ) else {
            return Ok(None);
        };

        Ok(Some(ObservedBranchCleanupScene::TargetPresent(
            BranchCleanupScene {
                source,
                fresh_target,
                expected_source_is_commit,
                expected_target_is_commit,
                fresh_target_is_commit,
                source_is_ancestor,
                expected_target_is_ancestor,
                source_is_checked_out,
            },
        )))
    }

    async fn observe_branch_source_ref(
        &self,
        commands: &DeliveryBranchCleanupCommands,
        intent: &DeliveryBranchCleanupIntent,
        cancellation: CancellationToken,
    ) -> Result<Option<SourceRefObservation>, DeliveryWorktreeCleanupError> {
        if !self
            .observe_direct_branch_ref(commands.source_ref_symbolic()?, cancellation.clone())
            .await?
        {
            return Ok(None);
        }
        let Some((exit, output)) = branch_bounded_protocol(
            self.executor
                .run_machine_protocol(
                    commands.resolve_source_ref_raw()?,
                    cancellation,
                    self.limits.max_status_bytes(),
                )
                .await,
        )?
        else {
            return Ok(None);
        };
        match exit {
            DeliveryCommandExit::NotMatched if output.is_empty() => {
                Ok(Some(SourceRefObservation::Absent))
            }
            DeliveryCommandExit::Matched => {
                let Some(observed) = parse_branch_oid(
                    &output,
                    intent
                        .inner
                        .context
                        .worktree
                        .inner
                        .repository_probe
                        .object_format(),
                    intent
                        .inner
                        .context
                        .worktree
                        .inner
                        .repository_probe
                        .object_format()
                        .hexadecimal_length(),
                ) else {
                    return Ok(None);
                };
                if observed == intent.inner.context.expected_source {
                    Ok(Some(SourceRefObservation::Exact))
                } else {
                    Ok(Some(SourceRefObservation::Drift))
                }
            }
            DeliveryCommandExit::NotMatched => Ok(None),
        }
    }

    async fn observe_branch_target_ref(
        &self,
        commands: &DeliveryBranchCleanupCommands,
        intent: &DeliveryBranchCleanupIntent,
        cancellation: CancellationToken,
    ) -> Result<Option<TargetRefObservation>, DeliveryWorktreeCleanupError> {
        if !self
            .observe_direct_branch_ref(commands.target_ref_symbolic()?, cancellation.clone())
            .await?
        {
            return Ok(None);
        }
        let Some((exit, output)) = branch_bounded_protocol(
            self.executor
                .run_machine_protocol(
                    commands.resolve_target_ref_raw()?,
                    cancellation,
                    self.limits.max_status_bytes(),
                )
                .await,
        )?
        else {
            return Ok(None);
        };
        match exit {
            DeliveryCommandExit::Matched => Ok(parse_branch_oid(
                &output,
                intent
                    .inner
                    .context
                    .worktree
                    .inner
                    .repository_probe
                    .object_format(),
                intent
                    .inner
                    .context
                    .worktree
                    .inner
                    .repository_probe
                    .object_format()
                    .hexadecimal_length(),
            )
            .map(TargetRefObservation::Commit)),
            DeliveryCommandExit::NotMatched if output.is_empty() => {
                Ok(Some(TargetRefObservation::Absent))
            }
            DeliveryCommandExit::NotMatched => Ok(None),
        }
    }

    async fn observe_direct_branch_ref(
        &self,
        command: crate::command_policy::ValidatedCommand,
        cancellation: CancellationToken,
    ) -> Result<bool, DeliveryWorktreeCleanupError> {
        let Some((exit, output)) = branch_bounded_protocol(
            self.executor
                .run_machine_protocol(command, cancellation, self.limits.max_status_bytes())
                .await,
        )?
        else {
            return Ok(false);
        };
        Ok(matches!(exit, DeliveryCommandExit::NotMatched) && output.is_empty())
    }

    async fn observe_branch_commit_object(
        &self,
        command: crate::command_policy::ValidatedCommand,
        expected: &DeliveryCommitOid,
        cancellation: CancellationToken,
    ) -> Result<Option<bool>, DeliveryWorktreeCleanupError> {
        let Some(output) = branch_bounded_output(
            self.executor
                .run(command, cancellation, self.limits.max_status_bytes())
                .await,
        )?
        else {
            return Ok(None);
        };
        Ok(Some(batch_output_is_exact_commit(
            &output,
            expected.as_str(),
        )))
    }

    async fn observe_expected_source_commit(
        &self,
        command: crate::command_policy::ValidatedCommand,
        context: &DeliveryBranchCleanupContext,
        cancellation: CancellationToken,
    ) -> Result<Option<bool>, DeliveryWorktreeCleanupError> {
        let Some(output) = branch_bounded_output(
            self.executor
                .run(command, cancellation, self.limits.max_status_bytes())
                .await,
        )?
        else {
            return Ok(None);
        };
        Ok(Some(
            verify_batched_source_commit(
                &output,
                &context.expected_source,
                &context.expected_source_tree,
                &context.expected_source_parent,
                &context.source_input,
            )
            .is_ok(),
        ))
    }

    async fn observe_branch_predicate(
        &self,
        command: crate::command_policy::ValidatedCommand,
        cancellation: CancellationToken,
    ) -> Result<bool, DeliveryWorktreeCleanupError> {
        self.executor
            .run_predicate(command, cancellation, self.limits.max_status_bytes())
            .await
            .map(|exit| exit == DeliveryCommandExit::Matched)
            .map_err(Into::into)
    }

    fn branch_cleanup_commands(
        &self,
        intent: &DeliveryBranchCleanupIntent,
    ) -> Result<DeliveryBranchCleanupCommands, DeliveryWorktreeCleanupError> {
        let context = &intent.inner.context;
        if !self.branch_target_authentication_is_current(intent) {
            return Err(DeliveryWorktreeCleanupError::AuthenticationChanged);
        }
        context
            .target
            .commands()
            .branch_cleanup_commands(
                context.target.probe(),
                context.target.authentication().command_context(),
                &context.source_branch,
                &context.target_branch,
                &context.expected_source,
                &intent.inner.expected_target,
            )
            .map_err(Into::into)
    }

    fn branch_intent_context_is_current(&self, intent: &DeliveryBranchCleanupIntent) -> bool {
        let context = &intent.inner.context;
        intent.is_current_generation()
            && self
                .probe
                .shares_probed_authority_with(context.target.probe())
            && context
                .target
                .probe()
                .shares_repository_format_authority_with(&context.worktree.inner.repository_probe)
            && context.source_branch == context.worktree.inner.source_branch
            && context.expected_source == context.worktree.inner.expected_source_commit
            && context.target_branch == context.target.branch_name()
            && context.source_branch != context.target_branch
            && context
                .worktree
                .inner
                .source
                .matches_cleanup_common_identity(context.target.common_directory_identity())
            && self.branch_target_authentication_is_current(intent)
    }

    fn branch_target_authentication_is_current(
        &self,
        intent: &DeliveryBranchCleanupIntent,
    ) -> bool {
        let target = &intent.inner.context.target;
        self.probe.verify_current_executable().is_ok()
            && self.probe.shares_probed_authority_with(target.probe())
            && target
                .revalidate_branch_cleanup_security(self.limits)
                .is_ok()
    }

    fn branch_worktree_is_absent(&self, intent: &DeliveryBranchCleanupIntent) -> bool {
        matches!(
            self.topology_authenticator
                .observe_topology(&intent.inner.context.worktree.inner.topology),
            CleanupTopologyObservation::Absent(_)
        )
    }

    /// Installs the deterministic Task 17 mutation boundary used only by
    /// integration tests. It shares the existing cleanup hook storage so one
    /// provisioner cannot accidentally run two independent test hooks.
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn set_branch_cleanup_boundary_hook_for_tests(
        &mut self,
        hook: impl Fn(&'static str) + Send + Sync + 'static,
    ) {
        self.set_cleanup_boundary_hook_for_tests(hook);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SourceRefObservation {
    Absent,
    Exact,
    Drift,
}

#[derive(PartialEq, Eq)]
enum TargetRefObservation {
    Absent,
    Commit(DeliveryCommitOid),
}

#[derive(PartialEq, Eq)]
enum ObservedBranchCleanupScene {
    TargetAbsent(SourceRefObservation),
    TargetPresent(BranchCleanupScene),
}

#[derive(PartialEq, Eq)]
struct BranchCleanupScene {
    source: SourceRefObservation,
    fresh_target: DeliveryCommitOid,
    expected_source_is_commit: bool,
    expected_target_is_commit: bool,
    fresh_target_is_commit: bool,
    source_is_ancestor: bool,
    expected_target_is_ancestor: bool,
    source_is_checked_out: bool,
}

#[derive(PartialEq, Eq)]
enum BranchCleanupObservation {
    ExactPresent,
    Absent,
    Refresh(DeliveryCommitOid),
    SourceNotMerged,
    Inconsistent,
}

fn branch_observation_for_observed_scene(
    scene: &ObservedBranchCleanupScene,
    expected_target: &DeliveryCommitOid,
) -> BranchCleanupObservation {
    match scene {
        ObservedBranchCleanupScene::TargetAbsent(SourceRefObservation::Exact) => {
            BranchCleanupObservation::SourceNotMerged
        }
        ObservedBranchCleanupScene::TargetAbsent(
            SourceRefObservation::Absent | SourceRefObservation::Drift,
        ) => BranchCleanupObservation::Inconsistent,
        ObservedBranchCleanupScene::TargetPresent(scene) => {
            branch_observation_for(scene, expected_target)
        }
    }
}

fn branch_observation_for(
    scene: &BranchCleanupScene,
    expected_target: &DeliveryCommitOid,
) -> BranchCleanupObservation {
    if !scene.expected_source_is_commit
        || !scene.expected_target_is_commit
        || !scene.fresh_target_is_commit
        || scene.source_is_checked_out
        || scene.source == SourceRefObservation::Drift
    {
        return BranchCleanupObservation::Inconsistent;
    }

    // A still-exact source proves the delete has not happened. Failure of the
    // fresh source->target ancestry predicate is therefore a typed
    // known-not-applied fact even if the target was concurrently reset. An
    // absent source cannot use that inference and remains reconciliation.
    if !scene.source_is_ancestor {
        return if scene.source == SourceRefObservation::Exact {
            BranchCleanupObservation::SourceNotMerged
        } else {
            BranchCleanupObservation::Inconsistent
        };
    }
    if !scene.expected_target_is_ancestor {
        return BranchCleanupObservation::Inconsistent;
    }

    match scene.source {
        SourceRefObservation::Exact if scene.fresh_target == *expected_target => {
            BranchCleanupObservation::ExactPresent
        }
        SourceRefObservation::Exact => {
            BranchCleanupObservation::Refresh(scene.fresh_target.clone())
        }
        SourceRefObservation::Absent => BranchCleanupObservation::Absent,
        SourceRefObservation::Drift => BranchCleanupObservation::Inconsistent,
    }
}

fn branch_disposition(
    intent: &DeliveryBranchCleanupIntent,
    observation: BranchCleanupObservation,
) -> DeliveryDeletePendingDisposition {
    match observation {
        BranchCleanupObservation::ExactPresent => {
            DeliveryDeletePendingDisposition::RetryExactDelete
        }
        BranchCleanupObservation::Absent => DeliveryDeletePendingDisposition::Deleted,
        BranchCleanupObservation::Refresh(fresh_target) => {
            let Some(refreshed_intent) = intent.refreshed(fresh_target) else {
                return DeliveryDeletePendingDisposition::ReconciliationRequired;
            };
            DeliveryDeletePendingDisposition::RefreshExpectedTarget(
                DeliveryBranchCleanupRefreshProof { refreshed_intent },
            )
        }
        BranchCleanupObservation::SourceNotMerged => {
            DeliveryDeletePendingDisposition::KnownNotAppliedSourceNotMerged
        }
        BranchCleanupObservation::Inconsistent => {
            DeliveryDeletePendingDisposition::ReconciliationRequired
        }
    }
}

fn parse_branch_oid(
    output: &[u8],
    format: crate::delivery::DeliveryGitObjectFormat,
    hexadecimal_length: usize,
) -> Option<DeliveryCommitOid> {
    let value = parse_object_id(output, hexadecimal_length).ok()?;
    DeliveryCommitOid::try_new(value, format)
}

fn branch_bounded_protocol(
    result: Result<(DeliveryCommandExit, Vec<u8>), DeliverySourceError>,
) -> Result<Option<(DeliveryCommandExit, Vec<u8>)>, DeliveryWorktreeCleanupError> {
    match result {
        Ok(observation) => Ok(Some(observation)),
        Err(DeliverySourceError::BoundsExceeded) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn branch_bounded_output(
    result: Result<Vec<u8>, DeliverySourceError>,
) -> Result<Option<Vec<u8>>, DeliveryWorktreeCleanupError> {
    match result {
        Ok(output) => Ok(Some(output)),
        Err(DeliverySourceError::BoundsExceeded) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn branch_child_cleanup_is_unproven(result: &Result<Vec<u8>, DeliverySourceError>) -> bool {
    matches!(
        result,
        Err(DeliverySourceError::ProcessCleanupUnproven
            | DeliverySourceError::SandboxCleanupUnproven)
    )
}

fn successful_branch_child_matches_closing_observation(
    result: &Result<Vec<u8>, DeliverySourceError>,
    closing: &BranchCleanupObservation,
) -> bool {
    result.is_err() || matches!(closing, BranchCleanupObservation::Absent)
}

/// Verifies the complete, ordinary `cat-file --batch` response for one commit
/// object. The payload itself is immutable under the expected OID; this parser
/// proves the exact object name, type, declared size, framing, and completeness
/// without logging or returning any commit content.
fn batch_output_is_exact_commit(output: &[u8], expected: &str) -> bool {
    let Some(header_end) = output.iter().position(|byte| *byte == b'\n') else {
        return false;
    };
    let header = &output[..header_end];
    if header.contains(&b'\r') {
        return false;
    }
    let prefix = format!("{expected} commit ");
    let Some(raw_size) = header.strip_prefix(prefix.as_bytes()) else {
        return false;
    };
    if raw_size.is_empty()
        || !raw_size.iter().all(u8::is_ascii_digit)
        || (raw_size.len() > 1 && raw_size[0] == b'0')
    {
        return false;
    }
    let Ok(raw_size) = std::str::from_utf8(raw_size) else {
        return false;
    };
    let Ok(size) = raw_size.parse::<usize>() else {
        return false;
    };
    let payload = &output[header_end + 1..];
    size != 0 && payload.len() == size.saturating_add(1) && payload.last() == Some(&b'\n')
}

/// Parses the complete NUL-delimited `git worktree list --porcelain -z`
/// protocol and reports whether the exact source local branch is checked out.
/// Unknown fields, incomplete records, duplicate fields, excessive record
/// counts, and malformed OIDs all fail closed.
fn source_branch_is_checked_out(
    output: &[u8],
    source_branch: &str,
    object_id_length: usize,
    max_records: usize,
) -> Option<bool> {
    if output.len() < 2 || !output.ends_with(&[0, 0]) || max_records == 0 {
        return None;
    }
    let expected_ref = format!("refs/heads/{source_branch}");
    let payload = &output[..output.len() - 1];
    let mut record: Option<WorktreeRecord> = None;
    let mut records = 0usize;
    let mut source_checked_out = false;

    for field in payload.split(|byte| *byte == 0) {
        if field.is_empty() {
            let completed = record.take()?;
            if !completed.is_complete() {
                return None;
            }
            records = records.checked_add(1)?;
            if records > max_records {
                return None;
            }
            source_checked_out |= completed.source_checked_out;
            continue;
        }

        let Some(current) = record.as_mut() else {
            let path = field.strip_prefix(b"worktree ")?;
            if path.is_empty() {
                return None;
            }
            record = Some(WorktreeRecord::new());
            continue;
        };
        if let Some(head) = field.strip_prefix(b"HEAD ") {
            if current.head || !is_canonical_hex_oid(head, object_id_length) {
                return None;
            }
            current.head = true;
        } else if let Some(branch) = field.strip_prefix(b"branch ") {
            if current.branch_or_detached || !branch.starts_with(b"refs/heads/") {
                return None;
            }
            current.branch_or_detached = true;
            current.source_checked_out = branch == expected_ref.as_bytes();
        } else if field == b"detached" {
            if current.branch_or_detached {
                return None;
            }
            current.branch_or_detached = true;
        } else if field == b"bare" {
            if current.bare {
                return None;
            }
            current.bare = true;
        } else if field == b"locked" || field.starts_with(b"locked ") {
            if current.locked {
                return None;
            }
            current.locked = true;
        } else if field == b"prunable" || field.starts_with(b"prunable ") {
            if current.prunable {
                return None;
            }
            current.prunable = true;
        } else {
            return None;
        }
    }

    if record.is_some() || records == 0 {
        None
    } else {
        Some(source_checked_out)
    }
}

fn is_canonical_hex_oid(value: &[u8], expected_length: usize) -> bool {
    value.len() == expected_length
        && value
            .iter()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        && value.iter().any(|byte| *byte != b'0')
}

struct WorktreeRecord {
    head: bool,
    branch_or_detached: bool,
    bare: bool,
    locked: bool,
    prunable: bool,
    source_checked_out: bool,
}

impl WorktreeRecord {
    const fn new() -> Self {
        Self {
            head: false,
            branch_or_detached: false,
            bare: false,
            locked: false,
            prunable: false,
            source_checked_out: false,
        }
    }

    const fn is_complete(&self) -> bool {
        if self.bare {
            !self.head && !self.branch_or_detached
        } else {
            self.head && self.branch_or_detached
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oid(byte: char) -> DeliveryCommitOid {
        DeliveryCommitOid::try_new(
            &byte.to_string().repeat(40),
            crate::delivery::DeliveryGitObjectFormat::Sha1,
        )
        .unwrap()
    }

    fn scene(source: SourceRefObservation) -> BranchCleanupScene {
        BranchCleanupScene {
            source,
            fresh_target: oid('b'),
            expected_source_is_commit: true,
            expected_target_is_commit: true,
            fresh_target_is_commit: true,
            source_is_ancestor: true,
            expected_target_is_ancestor: true,
            source_is_checked_out: false,
        }
    }

    #[test]
    fn adopted_refresh_revokes_all_old_generation_capabilities() {
        let gate = Arc::new(DeliveryBranchCleanupGenerationGate::new());
        assert!(gate.is_current(INITIAL_BRANCH_CLEANUP_GENERATION));

        let refreshed = INITIAL_BRANCH_CLEANUP_GENERATION + 1;
        assert!(gate.try_adopt(INITIAL_BRANCH_CLEANUP_GENERATION, refreshed));

        // These two failed claims represent independently minted capabilities
        // for the same old persisted intent. Neither can regain mutation
        // authority even if Git later returns to its old expected target HEAD.
        assert!(
            gate.try_claim_mutation(INITIAL_BRANCH_CLEANUP_GENERATION)
                .is_none()
        );
        assert!(
            gate.try_claim_mutation(INITIAL_BRANCH_CLEANUP_GENERATION)
                .is_none()
        );
        assert!(!gate.is_current(INITIAL_BRANCH_CLEANUP_GENERATION));
        assert!(gate.is_current(refreshed));
        assert!(gate.try_claim_mutation(refreshed).is_some());
    }

    #[test]
    fn refresh_adoption_and_delete_claim_are_mutually_exclusive() {
        let gate = Arc::new(DeliveryBranchCleanupGenerationGate::new());
        let mutation = gate
            .try_claim_mutation(INITIAL_BRANCH_CLEANUP_GENERATION)
            .expect("active generation may claim the mutation edge");
        let refreshed = INITIAL_BRANCH_CLEANUP_GENERATION + 1;

        assert!(!gate.try_adopt(INITIAL_BRANCH_CLEANUP_GENERATION, refreshed));
        drop(mutation);
        assert!(gate.try_adopt(INITIAL_BRANCH_CLEANUP_GENERATION, refreshed));

        // A competing proof for the same preceding generation is stale once
        // the first proof wins the generation CAS.
        assert!(!gate.try_adopt(INITIAL_BRANCH_CLEANUP_GENERATION, refreshed));
    }

    #[test]
    fn successful_delete_child_requires_an_exact_absent_closing_scene() {
        let success = Ok(Vec::new());
        assert!(successful_branch_child_matches_closing_observation(
            &success,
            &BranchCleanupObservation::Absent,
        ));
        for contradictory in [
            BranchCleanupObservation::ExactPresent,
            BranchCleanupObservation::Refresh(oid('c')),
            BranchCleanupObservation::SourceNotMerged,
            BranchCleanupObservation::Inconsistent,
        ] {
            assert!(!successful_branch_child_matches_closing_observation(
                &success,
                &contradictory,
            ));
        }
        assert!(successful_branch_child_matches_closing_observation(
            &Err(DeliverySourceError::TimedOut),
            &BranchCleanupObservation::ExactPresent,
        ));
    }

    #[test]
    fn delete_pending_truth_table_separates_exact_absent_refresh_and_not_merged() {
        let expected = oid('b');
        assert!(matches!(
            branch_observation_for(&scene(SourceRefObservation::Exact), &expected),
            BranchCleanupObservation::ExactPresent
        ));
        assert!(matches!(
            branch_observation_for(&scene(SourceRefObservation::Absent), &expected),
            BranchCleanupObservation::Absent
        ));

        let mut advanced = scene(SourceRefObservation::Exact);
        advanced.fresh_target = oid('c');
        assert!(matches!(
            branch_observation_for(&advanced, &expected),
            BranchCleanupObservation::Refresh(head) if head == oid('c')
        ));

        let mut not_merged = scene(SourceRefObservation::Exact);
        not_merged.source_is_ancestor = false;
        not_merged.expected_target_is_ancestor = false;
        assert!(matches!(
            branch_observation_for(&not_merged, &expected),
            BranchCleanupObservation::SourceNotMerged
        ));
        not_merged.source = SourceRefObservation::Absent;
        assert!(matches!(
            branch_observation_for(&not_merged, &expected),
            BranchCleanupObservation::Inconsistent
        ));

        assert!(matches!(
            branch_observation_for_observed_scene(
                &ObservedBranchCleanupScene::TargetAbsent(SourceRefObservation::Exact),
                &expected,
            ),
            BranchCleanupObservation::SourceNotMerged
        ));
        for source in [SourceRefObservation::Absent, SourceRefObservation::Drift] {
            assert!(matches!(
                branch_observation_for_observed_scene(
                    &ObservedBranchCleanupScene::TargetAbsent(source),
                    &expected,
                ),
                BranchCleanupObservation::Inconsistent
            ));
        }
    }

    #[test]
    fn unsafe_branch_facts_always_require_reconciliation() {
        let expected = oid('b');
        let drift = scene(SourceRefObservation::Drift);
        let mut source_tag = scene(SourceRefObservation::Exact);
        source_tag.expected_source_is_commit = false;
        let mut target_tag = scene(SourceRefObservation::Exact);
        target_tag.fresh_target_is_commit = false;
        let mut persisted_target_missing = scene(SourceRefObservation::Exact);
        persisted_target_missing.expected_target_is_commit = false;
        for unsafe_scene in [drift, source_tag, target_tag, persisted_target_missing] {
            assert!(matches!(
                branch_observation_for(&unsafe_scene, &expected),
                BranchCleanupObservation::Inconsistent
            ));
        }

        let mut checked_out = scene(SourceRefObservation::Exact);
        checked_out.source_is_checked_out = true;
        assert!(matches!(
            branch_observation_for(&checked_out, &expected),
            BranchCleanupObservation::Inconsistent
        ));

        let mut reset = scene(SourceRefObservation::Exact);
        reset.fresh_target = oid('c');
        reset.expected_target_is_ancestor = false;
        assert!(matches!(
            branch_observation_for(&reset, &expected),
            BranchCleanupObservation::Inconsistent
        ));
    }

    #[test]
    fn worktree_porcelain_parser_is_complete_bounded_and_branch_exact() {
        let head = "a".repeat(40);
        let output = format!(
            "worktree C:/repo\0HEAD {head}\0branch refs/heads/main\0\0worktree C:/task\0HEAD {head}\0branch refs/heads/codex/task\0locked reason\0\0"
        );
        assert_eq!(
            source_branch_is_checked_out(output.as_bytes(), "codex/task", 40, 2),
            Some(true)
        );
        assert_eq!(
            source_branch_is_checked_out(output.as_bytes(), "codex/other", 40, 2),
            Some(false)
        );
        assert_eq!(
            source_branch_is_checked_out(output.as_bytes(), "codex/task", 40, 1),
            None
        );

        let malformed =
            format!("worktree C:/repo\0HEAD {head}\0branch refs/heads/main\0unknown value\0\0");
        assert_eq!(
            source_branch_is_checked_out(malformed.as_bytes(), "codex/task", 40, 2),
            None
        );
        assert_eq!(
            source_branch_is_checked_out(
                &output.as_bytes()[..output.len() - 1],
                "codex/task",
                40,
                2
            ),
            None
        );
    }

    #[test]
    fn batch_commit_parser_rejects_wrong_type_size_and_trailing_bytes() {
        let object = "a".repeat(40);
        let payload = b"tree bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\n\nmessage\n";
        let mut valid = format!("{object} commit {}\n", payload.len()).into_bytes();
        valid.extend_from_slice(payload);
        valid.push(b'\n');
        assert!(batch_output_is_exact_commit(&valid, &object));

        let mut wrong_type = format!("{object} tag {}\n", payload.len()).into_bytes();
        wrong_type.extend_from_slice(payload);
        wrong_type.push(b'\n');
        assert!(!batch_output_is_exact_commit(&wrong_type, &object));

        let mut wrong_size = format!("{object} commit {}\n", payload.len() + 1).into_bytes();
        wrong_size.extend_from_slice(payload);
        wrong_size.push(b'\n');
        assert!(!batch_output_is_exact_commit(&wrong_size, &object));

        valid.push(b'x');
        assert!(!batch_output_is_exact_commit(&valid, &object));
    }
}
