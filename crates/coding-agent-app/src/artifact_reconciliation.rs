use std::collections::HashMap;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use coding_agent_domain::RepositoryId;
use coding_agent_runtime::{WorktreeIdentity, WorktreeObservation, WorktreeProvisioner};
use coding_agent_store::{
    AttemptArtifactIdentity, AttemptArtifactState, ReserveAttemptArtifact, Store, StoreError,
    TaskAttemptArtifact,
};
use futures_util::{StreamExt as _, stream};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::delivery_reconciliation::DeliveryArtifactOwnershipRouter;
use crate::repository_control::{
    RepositoryControlCoordinator, RepositoryControlError, RepositoryControlLease,
    RepositoryControlPoisonReason, RepositoryIdentityResolver,
};
use crate::scheduler::RepositoryCoordinationKey;
use crate::{StoreWriterError, StoreWriterHandle};

const WORKTREE_RESERVATION_ABANDONED: &str = "WORKTREE_RESERVATION_ABANDONED";
const WORKTREE_STATE_INCONSISTENT: &str = "WORKTREE_STATE_INCONSISTENT";
const DEFAULT_ARTIFACT_OBSERVATION_TIMEOUT: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartArtifactObservation {
    Absent,
    Ready,
    Partial,
    Inconsistent,
    Unavailable,
    ProcessCleanupUnproven,
    RepositoryMismatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactReconciliationDecision {
    Ready,
    Inconsistent(&'static str),
    RetainReserved,
}

pub const fn decide_restart_artifact(
    observation: RestartArtifactObservation,
) -> ArtifactReconciliationDecision {
    match observation {
        RestartArtifactObservation::Absent => {
            ArtifactReconciliationDecision::Inconsistent(WORKTREE_RESERVATION_ABANDONED)
        }
        RestartArtifactObservation::Ready => ArtifactReconciliationDecision::Ready,
        RestartArtifactObservation::Partial | RestartArtifactObservation::Inconsistent => {
            ArtifactReconciliationDecision::Inconsistent(WORKTREE_STATE_INCONSISTENT)
        }
        RestartArtifactObservation::RepositoryMismatch => {
            ArtifactReconciliationDecision::Inconsistent(WORKTREE_STATE_INCONSISTENT)
        }
        RestartArtifactObservation::Unavailable
        | RestartArtifactObservation::ProcessCleanupUnproven => {
            ArtifactReconciliationDecision::RetainReserved
        }
    }
}

#[async_trait::async_trait]
pub trait AttemptArtifactObserver: Send + Sync {
    async fn observe(&self, artifact: &TaskAttemptArtifact) -> RestartArtifactObservation;
}

/// Runtime-backed observer for restart reconciliation. Each provisioner is
/// already bound to one registered repository and its retained Git/artifact
/// capabilities; persisted rows provide identity values, never authority.
pub struct WorktreeArtifactObserver {
    provisioners: HashMap<RepositoryId, Arc<WorktreeProvisioner>>,
    observation_timeout: Duration,
}

impl WorktreeArtifactObserver {
    pub fn new(
        provisioners: impl IntoIterator<Item = (RepositoryId, Arc<WorktreeProvisioner>)>,
    ) -> Self {
        Self {
            provisioners: provisioners.into_iter().collect(),
            observation_timeout: DEFAULT_ARTIFACT_OBSERVATION_TIMEOUT,
        }
    }

    async fn observe_with_bounded_token(
        &self,
        provisioner: &WorktreeProvisioner,
        reservation: &coding_agent_runtime::WorktreeReservation,
    ) -> coding_agent_runtime::WorktreeObservationOutcome {
        let cancellation = CancellationToken::new();
        let observation = provisioner.observe_with_safety(reservation, cancellation.clone());
        tokio::pin!(observation);
        tokio::select! {
            outcome = &mut observation => outcome,
            () = tokio::time::sleep(self.observation_timeout) => {
                cancellation.cancel();
                observation.await
            }
        }
    }
}

#[async_trait::async_trait]
impl AttemptArtifactObserver for WorktreeArtifactObserver {
    async fn observe(&self, artifact: &TaskAttemptArtifact) -> RestartArtifactObservation {
        let Some(provisioner) = self.provisioners.get(&artifact.identity.repository_id) else {
            return RestartArtifactObservation::Unavailable;
        };
        let identity = match WorktreeIdentity::try_new(
            artifact.identity.repository_id.to_string(),
            artifact.identity.task_id.to_string(),
            artifact.identity.attempt,
        ) {
            Ok(identity) => identity,
            Err(_) => return RestartArtifactObservation::Inconsistent,
        };
        let reservation = match provisioner.restore_reservation(
            identity,
            artifact.base_commit.clone(),
            artifact.branch_name.clone(),
            artifact.worktree_path.as_path().to_owned(),
        ) {
            Ok(reservation) => reservation,
            Err(_) => return RestartArtifactObservation::Inconsistent,
        };
        let outcome = self
            .observe_with_bounded_token(provisioner, &reservation)
            .await;
        if outcome.process_cleanup_is_unproven() {
            return RestartArtifactObservation::ProcessCleanupUnproven;
        }
        if outcome.repository_poison_required() {
            return RestartArtifactObservation::RepositoryMismatch;
        }
        match outcome.observation() {
            WorktreeObservation::Absent => RestartArtifactObservation::Absent,
            WorktreeObservation::Ready => RestartArtifactObservation::Ready,
            WorktreeObservation::BranchOnly
            | WorktreeObservation::AdministrativeCreated
            | WorktreeObservation::CheckoutPartial => RestartArtifactObservation::Partial,
            WorktreeObservation::Inconsistent => RestartArtifactObservation::Inconsistent,
            WorktreeObservation::Unavailable => RestartArtifactObservation::Unavailable,
        }
    }
}

/// Exact durable artifact evidence produced only from an authoritative Store
/// mutation result or after an ambiguous mutation was reconciled by an exact
/// Store query.
///
/// Fields and construction remain private so later coordinator integration can
/// accept this value without callers being able to forge it.
pub struct VerifiedArtifactReconciliationEvidence {
    artifact: TaskAttemptArtifact,
}

impl VerifiedArtifactReconciliationEvidence {
    fn new(artifact: TaskAttemptArtifact) -> Self {
        Self { artifact }
    }

    pub const fn artifact(&self) -> &TaskAttemptArtifact {
        &self.artifact
    }

    pub const fn identity(&self) -> AttemptArtifactIdentity {
        self.artifact.identity
    }

    pub const fn state(&self) -> AttemptArtifactState {
        self.artifact.state
    }

    pub fn failure_code(&self) -> Option<&str> {
        self.artifact.failure_code.as_deref()
    }
}

impl fmt::Debug for VerifiedArtifactReconciliationEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerifiedArtifactReconciliationEvidence")
            .field("identity", &self.artifact.identity)
            .field("state", &self.artifact.state)
            .field("failure_code", &self.artifact.failure_code)
            .finish_non_exhaustive()
    }
}

/// Typed result of one artifact mutation.
///
/// `Reconciled` means the write returned an error, but an exact authoritative
/// query proved that the requested durable state exists. `Unresolved` never
/// means "known not applied"; callers must retain ownership and reconcile.
pub enum ArtifactMutationDisposition {
    Confirmed(TaskAttemptArtifact),
    Reconciled(VerifiedArtifactReconciliationEvidence),
    Unresolved,
    Conflict,
}

impl ArtifactMutationDisposition {
    pub const fn artifact(&self) -> Option<&TaskAttemptArtifact> {
        match self {
            Self::Confirmed(artifact) => Some(artifact),
            Self::Reconciled(evidence) => Some(evidence.artifact()),
            Self::Unresolved | Self::Conflict => None,
        }
    }

    pub const fn reconciliation_evidence(&self) -> Option<&VerifiedArtifactReconciliationEvidence> {
        match self {
            Self::Reconciled(evidence) => Some(evidence),
            Self::Confirmed(_) | Self::Unresolved | Self::Conflict => None,
        }
    }
}

impl fmt::Debug for ArtifactMutationDisposition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Confirmed(artifact) => formatter
                .debug_struct("Confirmed")
                .field("identity", &artifact.identity)
                .field("state", &artifact.state)
                .finish(),
            Self::Reconciled(evidence) => {
                formatter.debug_tuple("Reconciled").field(evidence).finish()
            }
            Self::Unresolved => formatter.write_str("Unresolved"),
            Self::Conflict => formatter.write_str("Conflict"),
        }
    }
}

/// Startup-only adapter. Every mutation goes directly through Store and this
/// type contains no StoreWriter handle.
#[derive(Clone)]
pub struct StartupDirectStoreArtifactAdapter {
    store: Store,
}

impl StartupDirectStoreArtifactAdapter {
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    pub async fn list_reserved_attempt_artifacts(
        &self,
    ) -> Result<Vec<TaskAttemptArtifact>, StoreError> {
        self.store.list_reserved_attempt_artifacts().await
    }

    pub async fn reserve_attempt_artifact(
        &self,
        input: ReserveAttemptArtifact,
    ) -> ArtifactMutationDisposition {
        let expected = ExpectedArtifactMutation::Reservation(input.clone());
        match self.store.reserve_attempt_artifact(input).await {
            Ok(outcome) => confirmed(expected, outcome.artifact().clone()),
            Err(_) => reconcile_after_error(&self.store, expected).await,
        }
    }

    pub async fn mark_attempt_artifact_ready(
        &self,
        identity: AttemptArtifactIdentity,
    ) -> ArtifactMutationDisposition {
        let expected = ExpectedArtifactMutation::Ready(identity);
        match self.store.mark_attempt_artifact_ready(identity).await {
            Ok(outcome) => confirmed(expected, outcome.artifact().clone()),
            Err(_) => reconcile_after_error(&self.store, expected).await,
        }
    }

    pub async fn mark_attempt_artifact_inconsistent(
        &self,
        identity: AttemptArtifactIdentity,
        failure_code: impl Into<String>,
    ) -> ArtifactMutationDisposition {
        let failure_code = failure_code.into();
        let expected = ExpectedArtifactMutation::Inconsistent {
            identity,
            failure_code: failure_code.clone(),
        };
        match self
            .store
            .mark_attempt_artifact_inconsistent(identity, failure_code)
            .await
        {
            Ok(outcome) => confirmed(expected, outcome.artifact().clone()),
            Err(_) => reconcile_after_error(&self.store, expected).await,
        }
    }
}

/// Production adapter. Store is retained only for authoritative reads; every
/// mutation is submitted through StoreWriter.
#[derive(Clone)]
pub struct LiveStoreWriterArtifactAdapter {
    store: Store,
    writer: StoreWriterHandle,
}

impl LiveStoreWriterArtifactAdapter {
    pub fn new(store: Store, writer: StoreWriterHandle) -> Self {
        Self { store, writer }
    }

    pub async fn list_reserved_attempt_artifacts(
        &self,
    ) -> Result<Vec<TaskAttemptArtifact>, StoreError> {
        self.store.list_reserved_attempt_artifacts().await
    }

    pub async fn reserve_attempt_artifact(
        &self,
        input: ReserveAttemptArtifact,
        deadline: Instant,
    ) -> ArtifactMutationDisposition {
        let expected = ExpectedArtifactMutation::Reservation(input.clone());
        match self.writer.reserve_attempt_artifact(input, deadline).await {
            Ok(receipt) => confirmed(expected, receipt.value.artifact().clone()),
            Err(_) => reconcile_after_error(&self.store, expected).await,
        }
    }

    pub async fn mark_attempt_artifact_ready(
        &self,
        identity: AttemptArtifactIdentity,
        deadline: Instant,
    ) -> ArtifactMutationDisposition {
        let expected = ExpectedArtifactMutation::Ready(identity);
        match self
            .writer
            .mark_attempt_artifact_ready(identity, deadline)
            .await
        {
            Ok(receipt) => confirmed(expected, receipt.value.artifact().clone()),
            Err(_) => reconcile_after_error(&self.store, expected).await,
        }
    }

    pub async fn mark_attempt_artifact_inconsistent(
        &self,
        identity: AttemptArtifactIdentity,
        failure_code: impl Into<String>,
        deadline: Instant,
    ) -> ArtifactMutationDisposition {
        let failure_code = failure_code.into();
        let expected = ExpectedArtifactMutation::Inconsistent {
            identity,
            failure_code: failure_code.clone(),
        };
        match self
            .writer
            .mark_attempt_artifact_inconsistent(identity, failure_code, deadline)
            .await
        {
            Ok(receipt) => confirmed(expected, receipt.value.artifact().clone()),
            Err(_) => reconcile_after_error(&self.store, expected).await,
        }
    }
}

#[derive(Debug, Clone)]
enum ExpectedArtifactMutation {
    Reservation(ReserveAttemptArtifact),
    Ready(AttemptArtifactIdentity),
    Inconsistent {
        identity: AttemptArtifactIdentity,
        failure_code: String,
    },
}

impl ExpectedArtifactMutation {
    const fn identity(&self) -> AttemptArtifactIdentity {
        match self {
            Self::Reservation(input) => input.identity,
            Self::Ready(identity) | Self::Inconsistent { identity, .. } => *identity,
        }
    }

    fn classify(&self, artifact: &TaskAttemptArtifact) -> ExactArtifactQuery {
        match self {
            Self::Reservation(input) => {
                if artifact.identity == input.identity
                    && artifact.base_commit == input.base_commit
                    && artifact.branch_name == input.branch_name
                    && artifact.worktree_path == input.worktree_path
                {
                    ExactArtifactQuery::Exact
                } else {
                    ExactArtifactQuery::Conflict
                }
            }
            Self::Ready(identity) => {
                classify_terminal(artifact, *identity, AttemptArtifactState::Ready, None)
            }
            Self::Inconsistent {
                identity,
                failure_code,
            } => classify_terminal(
                artifact,
                *identity,
                AttemptArtifactState::Inconsistent,
                Some(failure_code),
            ),
        }
    }

    const fn missing(&self) -> ExactArtifactQuery {
        match self {
            Self::Reservation(_) => ExactArtifactQuery::Pending,
            Self::Ready(_) | Self::Inconsistent { .. } => ExactArtifactQuery::Conflict,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExactArtifactQuery {
    Exact,
    Pending,
    Conflict,
}

fn classify_terminal(
    artifact: &TaskAttemptArtifact,
    identity: AttemptArtifactIdentity,
    state: AttemptArtifactState,
    failure_code: Option<&String>,
) -> ExactArtifactQuery {
    if artifact.identity != identity {
        return ExactArtifactQuery::Conflict;
    }
    if artifact.state == state && artifact.failure_code.as_ref() == failure_code {
        return ExactArtifactQuery::Exact;
    }
    if artifact.state == AttemptArtifactState::Reserved {
        return ExactArtifactQuery::Pending;
    }
    ExactArtifactQuery::Conflict
}

fn confirmed(
    expected: ExpectedArtifactMutation,
    artifact: TaskAttemptArtifact,
) -> ArtifactMutationDisposition {
    if expected.classify(&artifact) == ExactArtifactQuery::Exact {
        ArtifactMutationDisposition::Confirmed(artifact)
    } else {
        ArtifactMutationDisposition::Conflict
    }
}

async fn reconcile_after_error(
    store: &Store,
    expected: ExpectedArtifactMutation,
) -> ArtifactMutationDisposition {
    match store
        .load_attempt_artifact(expected.identity().task_id)
        .await
    {
        Ok(Some(artifact)) => match expected.classify(&artifact) {
            ExactArtifactQuery::Exact => ArtifactMutationDisposition::Reconciled(
                VerifiedArtifactReconciliationEvidence::new(artifact),
            ),
            ExactArtifactQuery::Pending => ArtifactMutationDisposition::Unresolved,
            ExactArtifactQuery::Conflict => ArtifactMutationDisposition::Conflict,
        },
        Ok(None) => match expected.missing() {
            ExactArtifactQuery::Pending => ArtifactMutationDisposition::Unresolved,
            ExactArtifactQuery::Conflict | ExactArtifactQuery::Exact => {
                ArtifactMutationDisposition::Conflict
            }
        },
        Err(_) => ArtifactMutationDisposition::Unresolved,
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ArtifactReconciliationSummary {
    pub examined: usize,
    pub marked_ready: usize,
    pub marked_inconsistent: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum ArtifactReconciliationError {
    #[error("artifact reconciliation write timeout must be non-zero")]
    InvalidTimeout,
    #[error("artifact observation is temporarily unavailable")]
    ObservationUnavailable { identity: AttemptArtifactIdentity },
    #[error("artifact observation process cleanup could not be proven")]
    ObservationProcessCleanupUnproven { identity: AttemptArtifactIdentity },
    #[error("artifact observation found a repository control identity mismatch")]
    ObservationRepositoryMismatch { identity: AttemptArtifactIdentity },
    #[error("artifact mutation outcome remains unresolved")]
    MutationUnresolved { identity: AttemptArtifactIdentity },
    #[error("artifact mutation conflicts with durable state")]
    MutationConflict { identity: AttemptArtifactIdentity },
    #[error("delivery-owned artifact routing is inconsistent")]
    DeliveryOwnershipInconsistent,
    #[error(transparent)]
    RepositoryControl(#[from] RepositoryControlError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Writer(#[from] StoreWriterError),
}

enum ReconciliationAdapter<'a> {
    Startup(&'a StartupDirectStoreArtifactAdapter),
    Live {
        adapter: &'a LiveStoreWriterArtifactAdapter,
        write_timeout: Duration,
    },
}

impl ReconciliationAdapter<'_> {
    async fn list_reserved_attempt_artifacts(
        &self,
    ) -> Result<Vec<TaskAttemptArtifact>, StoreError> {
        match self {
            Self::Startup(adapter) => adapter.list_reserved_attempt_artifacts().await,
            Self::Live { adapter, .. } => adapter.list_reserved_attempt_artifacts().await,
        }
    }

    async fn apply(
        &self,
        identity: AttemptArtifactIdentity,
        decision: ArtifactReconciliationDecision,
    ) -> ArtifactMutationDisposition {
        match (self, decision) {
            (Self::Startup(adapter), ArtifactReconciliationDecision::Ready) => {
                adapter.mark_attempt_artifact_ready(identity).await
            }
            (
                Self::Startup(adapter),
                ArtifactReconciliationDecision::Inconsistent(failure_code),
            ) => {
                adapter
                    .mark_attempt_artifact_inconsistent(identity, failure_code)
                    .await
            }
            (
                Self::Live {
                    adapter,
                    write_timeout,
                },
                ArtifactReconciliationDecision::Ready,
            ) => {
                adapter
                    .mark_attempt_artifact_ready(identity, Instant::now() + *write_timeout)
                    .await
            }
            (
                Self::Live {
                    adapter,
                    write_timeout,
                },
                ArtifactReconciliationDecision::Inconsistent(failure_code),
            ) => {
                adapter
                    .mark_attempt_artifact_inconsistent(
                        identity,
                        failure_code,
                        Instant::now() + *write_timeout,
                    )
                    .await
            }
            (_, ArtifactReconciliationDecision::RetainReserved) => {
                ArtifactMutationDisposition::Unresolved
            }
        }
    }
}

/// Compatibility entry for production/live reconciliation. Store is read-only
/// through the live adapter and every mutation remains serialized by
/// StoreWriterHandle.
pub async fn reconcile_restart_artifacts(
    store: &Store,
    writer: &StoreWriterHandle,
    observer: &dyn AttemptArtifactObserver,
    write_timeout: Duration,
) -> Result<ArtifactReconciliationSummary, ArtifactReconciliationError> {
    if write_timeout.is_zero() {
        return Err(ArtifactReconciliationError::InvalidTimeout);
    }
    let adapter = LiveStoreWriterArtifactAdapter::new(store.clone(), writer.clone());
    reconcile_with_adapter(
        ReconciliationAdapter::Live {
            adapter: &adapter,
            write_timeout,
        },
        observer,
    )
    .await
}

/// Startup-only direct-Store reconciliation. The adapter type makes the
/// startup mutation exception explicit and cannot contain a StoreWriter.
pub async fn reconcile_startup_artifacts_direct(
    adapter: &StartupDirectStoreArtifactAdapter,
    observer: &dyn AttemptArtifactObserver,
) -> Result<ArtifactReconciliationSummary, ArtifactReconciliationError> {
    reconcile_with_adapter(ReconciliationAdapter::Startup(adapter), observer).await
}

/// Startup-only direct-Store reconciliation grouped by authenticated common-Git
/// identity.
///
/// The coordinator must already contain every repository alias. Artifacts in
/// one coordination group are observed and reconciled strictly in
/// `(created_at, task_id)` order. Independent groups overlap up to
/// `max_parallel_groups`. Each artifact revalidates its registered common-Git
/// identity immediately before and after its fresh observation while the
/// reconciliation lease remains owned. Every started group is drained before
/// an error is returned so cancellation cannot abandon an owned lease.
pub async fn reconcile_startup_artifacts_grouped(
    adapter: &StartupDirectStoreArtifactAdapter,
    coordinator: &RepositoryControlCoordinator,
    resolver: &dyn RepositoryIdentityResolver,
    observer: &dyn AttemptArtifactObserver,
    max_parallel_groups: NonZeroUsize,
) -> Result<ArtifactReconciliationSummary, ArtifactReconciliationError> {
    // Audit the complete delivery ownership graph before the P4-A observer is
    // allowed to see any Reserved artifact. A valid delivery-owned artifact is
    // Ready and therefore absent from this list; overlap or identity drift is
    // a fail-closed startup error, never a fallback to the base observer.
    let ownership = DeliveryArtifactOwnershipRouter::load(&adapter.store)
        .await
        .map_err(|_| ArtifactReconciliationError::DeliveryOwnershipInconsistent)?;
    reconcile_startup_artifacts_grouped_with_ownership(
        adapter,
        coordinator,
        resolver,
        observer,
        max_parallel_groups,
        &ownership,
    )
    .await
}

pub(crate) async fn reconcile_startup_artifacts_grouped_with_ownership(
    adapter: &StartupDirectStoreArtifactAdapter,
    coordinator: &RepositoryControlCoordinator,
    resolver: &dyn RepositoryIdentityResolver,
    observer: &dyn AttemptArtifactObserver,
    max_parallel_groups: NonZeroUsize,
    ownership: &DeliveryArtifactOwnershipRouter,
) -> Result<ArtifactReconciliationSummary, ArtifactReconciliationError> {
    let mut artifacts = adapter.list_reserved_attempt_artifacts().await?;
    for artifact in &artifacts {
        ownership
            .require_base_lifecycle(artifact)
            .map_err(|_| ArtifactReconciliationError::DeliveryOwnershipInconsistent)?;
    }
    artifacts.sort_unstable_by(|left, right| {
        left.created_at.cmp(&right.created_at).then_with(|| {
            left.identity
                .task_id
                .as_uuid()
                .cmp(&right.identity.task_id.as_uuid())
        })
    });

    let mut group_indices = HashMap::<RepositoryCoordinationKey, usize>::new();
    let mut groups = Vec::<StartupArtifactGroup>::new();
    for artifact in artifacts {
        let key = coordinator.coordination_key(artifact.identity.repository_id)?;
        if let Some(index) = group_indices.get(&key).copied() {
            groups[index].artifacts.push(artifact);
        } else {
            let index = groups.len();
            group_indices.insert(key, index);
            groups.push(StartupArtifactGroup {
                key,
                artifacts: vec![artifact],
            });
        }
    }

    let mut results = stream::iter(groups.into_iter().enumerate())
        .map(|(index, group)| async move {
            (
                index,
                group.key,
                reconcile_startup_group(adapter, coordinator, resolver, observer, group).await,
            )
        })
        .buffer_unordered(max_parallel_groups.get())
        .collect::<Vec<_>>()
        .await;
    results.sort_unstable_by_key(|(index, _, _)| *index);

    let mut summary = ArtifactReconciliationSummary::default();
    let mut first_error = None;
    for (_, key, result) in results {
        match result {
            Ok(group_summary) => add_summary(&mut summary, group_summary),
            Err(error) => {
                coordinator.require_reconciliation(
                    key,
                    RepositoryControlPoisonReason::AbnormalLeaseDrop,
                )?;
                first_error.get_or_insert(error);
            }
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(summary),
    }
}

struct StartupArtifactGroup {
    key: RepositoryCoordinationKey,
    artifacts: Vec<TaskAttemptArtifact>,
}

async fn reconcile_startup_group(
    adapter: &StartupDirectStoreArtifactAdapter,
    coordinator: &RepositoryControlCoordinator,
    resolver: &dyn RepositoryIdentityResolver,
    observer: &dyn AttemptArtifactObserver,
    group: StartupArtifactGroup,
) -> Result<ArtifactReconciliationSummary, ArtifactReconciliationError> {
    let StartupArtifactGroup { key, artifacts } = group;
    let mut summary = ArtifactReconciliationSummary::default();
    let mut first_error = None;
    for artifact in artifacts {
        match reconcile_startup_artifact(adapter, coordinator, resolver, observer, key, artifact)
            .await
        {
            Ok(item_summary) => add_summary(&mut summary, item_summary),
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }
    }
    match first_error {
        Some(error) => {
            coordinator
                .require_reconciliation(key, RepositoryControlPoisonReason::AbnormalLeaseDrop)?;
            Err(error)
        }
        None => Ok(summary),
    }
}

async fn reconcile_startup_artifact(
    adapter: &StartupDirectStoreArtifactAdapter,
    coordinator: &RepositoryControlCoordinator,
    resolver: &dyn RepositoryIdentityResolver,
    observer: &dyn AttemptArtifactObserver,
    key: RepositoryCoordinationKey,
    artifact: TaskAttemptArtifact,
) -> Result<ArtifactReconciliationSummary, ArtifactReconciliationError> {
    coordinator.require_reconciliation(key, RepositoryControlPoisonReason::AbnormalLeaseDrop)?;
    let lease = coordinator.try_acquire_reconciliation(key)?;
    if let Err(error) =
        revalidate_startup_identity(coordinator, resolver, artifact.identity.repository_id, key)
    {
        return Err(finish_startup_identity_failure(adapter, lease, &artifact, error).await);
    }
    let observation = observer.observe(&artifact).await;
    if observation == RestartArtifactObservation::ProcessCleanupUnproven {
        lease.retain_fail_closed(RepositoryControlPoisonReason::GitChildOutcomeUnknown)?;
        return Err(
            ArtifactReconciliationError::ObservationProcessCleanupUnproven {
                identity: artifact.identity,
            },
        );
    }
    let repository_mismatch = observation == RestartArtifactObservation::RepositoryMismatch;
    let decision = decide_restart_artifact(observation);
    if let Err(error) =
        revalidate_startup_identity(coordinator, resolver, artifact.identity.repository_id, key)
    {
        return Err(finish_startup_identity_failure(adapter, lease, &artifact, error).await);
    }
    if decision == ArtifactReconciliationDecision::RetainReserved {
        lease.poison(RepositoryControlPoisonReason::AbnormalLeaseDrop)?;
        return Err(ArtifactReconciliationError::ObservationUnavailable {
            identity: artifact.identity,
        });
    }

    let expected_state = match decision {
        ArtifactReconciliationDecision::Ready => AttemptArtifactState::Ready,
        ArtifactReconciliationDecision::Inconsistent(_) => AttemptArtifactState::Inconsistent,
        ArtifactReconciliationDecision::RetainReserved => {
            unreachable!("retain-reserved returns before mutation")
        }
    };
    let disposition = match decision {
        ArtifactReconciliationDecision::Ready => {
            adapter.mark_attempt_artifact_ready(artifact.identity).await
        }
        ArtifactReconciliationDecision::Inconsistent(failure_code) => {
            adapter
                .mark_attempt_artifact_inconsistent(artifact.identity, failure_code)
                .await
        }
        ArtifactReconciliationDecision::RetainReserved => {
            unreachable!("retain-reserved returns before mutation")
        }
    };
    let evidence = match disposition {
        ArtifactMutationDisposition::Confirmed(artifact) => {
            VerifiedArtifactReconciliationEvidence::new(artifact)
        }
        ArtifactMutationDisposition::Reconciled(evidence) => evidence,
        ArtifactMutationDisposition::Unresolved => {
            lease.poison(startup_mutation_poison_reason(decision))?;
            return Err(ArtifactReconciliationError::MutationUnresolved {
                identity: artifact.identity,
            });
        }
        ArtifactMutationDisposition::Conflict => {
            lease.poison(startup_mutation_poison_reason(decision))?;
            return Err(ArtifactReconciliationError::MutationConflict {
                identity: artifact.identity,
            });
        }
    };
    if repository_mismatch {
        lease.poison(RepositoryControlPoisonReason::SideEffectIdentityMismatch)?;
        return Err(ArtifactReconciliationError::ObservationRepositoryMismatch {
            identity: artifact.identity,
        });
    }
    let proof =
        match lease.verify_artifact_reconciliation(artifact.identity, expected_state, &evidence) {
            Ok(proof) => proof,
            Err(error) => {
                lease.poison(startup_mutation_poison_reason(decision))?;
                return Err(error.into());
            }
        };
    lease.clean_release_after_reconciliation(proof)?;

    Ok(match decision {
        ArtifactReconciliationDecision::Ready => ArtifactReconciliationSummary {
            examined: 1,
            marked_ready: 1,
            marked_inconsistent: 0,
        },
        ArtifactReconciliationDecision::Inconsistent(_) => ArtifactReconciliationSummary {
            examined: 1,
            marked_ready: 0,
            marked_inconsistent: 1,
        },
        ArtifactReconciliationDecision::RetainReserved => {
            unreachable!("retain-reserved returns before mutation")
        }
    })
}

async fn finish_startup_identity_failure(
    adapter: &StartupDirectStoreArtifactAdapter,
    lease: RepositoryControlLease,
    artifact: &TaskAttemptArtifact,
    error: RepositoryControlError,
) -> ArtifactReconciliationError {
    let completion = if matches!(
        error,
        RepositoryControlError::IdentityDrift | RepositoryControlError::AliasConflict
    ) {
        match adapter
            .mark_attempt_artifact_inconsistent(artifact.identity, WORKTREE_STATE_INCONSISTENT)
            .await
        {
            ArtifactMutationDisposition::Confirmed(_)
            | ArtifactMutationDisposition::Reconciled(_) => {
                lease.poison(RepositoryControlPoisonReason::SideEffectIdentityMismatch)
            }
            ArtifactMutationDisposition::Unresolved | ArtifactMutationDisposition::Conflict => {
                lease.retain_fail_closed(RepositoryControlPoisonReason::InconsistentWriteFailed)
            }
        }
    } else if error == RepositoryControlError::IdentityUnavailable {
        lease.poison(RepositoryControlPoisonReason::IdentityUnavailable)
    } else {
        lease.retain_fail_closed(identity_poison_reason(error))
    };
    match completion {
        Ok(()) => error.into(),
        Err(completion_error) => completion_error.into(),
    }
}

fn revalidate_startup_identity(
    coordinator: &RepositoryControlCoordinator,
    resolver: &dyn RepositoryIdentityResolver,
    repository_id: RepositoryId,
    expected_key: RepositoryCoordinationKey,
) -> Result<(), RepositoryControlError> {
    let observed_key = coordinator.revalidate_repository(repository_id, resolver)?;
    if observed_key == expected_key {
        Ok(())
    } else {
        Err(RepositoryControlError::AliasConflict)
    }
}

const fn identity_poison_reason(error: RepositoryControlError) -> RepositoryControlPoisonReason {
    match error {
        RepositoryControlError::IdentityUnavailable => {
            RepositoryControlPoisonReason::IdentityUnavailable
        }
        RepositoryControlError::IdentityDrift => RepositoryControlPoisonReason::IdentityDrift,
        RepositoryControlError::AliasConflict => RepositoryControlPoisonReason::AliasConflict,
        _ => RepositoryControlPoisonReason::AbnormalLeaseDrop,
    }
}

const fn startup_mutation_poison_reason(
    decision: ArtifactReconciliationDecision,
) -> RepositoryControlPoisonReason {
    match decision {
        ArtifactReconciliationDecision::Ready => RepositoryControlPoisonReason::ReadyWriteFailed,
        ArtifactReconciliationDecision::Inconsistent(_) => {
            RepositoryControlPoisonReason::InconsistentWriteFailed
        }
        ArtifactReconciliationDecision::RetainReserved => {
            RepositoryControlPoisonReason::AbnormalLeaseDrop
        }
    }
}

fn add_summary(
    aggregate: &mut ArtifactReconciliationSummary,
    increment: ArtifactReconciliationSummary,
) {
    aggregate.examined += increment.examined;
    aggregate.marked_ready += increment.marked_ready;
    aggregate.marked_inconsistent += increment.marked_inconsistent;
}

async fn reconcile_with_adapter(
    adapter: ReconciliationAdapter<'_>,
    observer: &dyn AttemptArtifactObserver,
) -> Result<ArtifactReconciliationSummary, ArtifactReconciliationError> {
    let artifacts = adapter.list_reserved_attempt_artifacts().await?;
    let mut summary = ArtifactReconciliationSummary::default();
    for artifact in artifacts {
        debug_assert_eq!(artifact.state, AttemptArtifactState::Reserved);
        summary.examined += 1;
        let observation = observer.observe(&artifact).await;
        if observation == RestartArtifactObservation::ProcessCleanupUnproven {
            return Err(
                ArtifactReconciliationError::ObservationProcessCleanupUnproven {
                    identity: artifact.identity,
                },
            );
        }
        let decision = decide_restart_artifact(observation);
        if decision == ArtifactReconciliationDecision::RetainReserved {
            return Err(ArtifactReconciliationError::ObservationUnavailable {
                identity: artifact.identity,
            });
        }
        let disposition = adapter.apply(artifact.identity, decision).await;
        match disposition {
            ArtifactMutationDisposition::Confirmed(_)
            | ArtifactMutationDisposition::Reconciled(_) => match decision {
                ArtifactReconciliationDecision::Ready => summary.marked_ready += 1,
                ArtifactReconciliationDecision::Inconsistent(_) => {
                    summary.marked_inconsistent += 1;
                }
                ArtifactReconciliationDecision::RetainReserved => {
                    unreachable!("retain-reserved returns before mutation")
                }
            },
            ArtifactMutationDisposition::Unresolved => {
                return Err(ArtifactReconciliationError::MutationUnresolved {
                    identity: artifact.identity,
                });
            }
            ArtifactMutationDisposition::Conflict => {
                return Err(ArtifactReconciliationError::MutationConflict {
                    identity: artifact.identity,
                });
            }
        }
        if observation == RestartArtifactObservation::RepositoryMismatch {
            return Err(ArtifactReconciliationError::ObservationRepositoryMismatch {
                identity: artifact.identity,
            });
        }
    }
    Ok(summary)
}
