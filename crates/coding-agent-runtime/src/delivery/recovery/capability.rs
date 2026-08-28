use super::*;
/// Runtime-owned values needed to observe or resume one source-side pending
/// operation.
///
/// `from_source` captures this only from an authenticated capability and a
/// candidate already bound to it.  It carries no path, command, or directory
/// authority, but it does retain opaque common/admin directory provenance so a
/// later authentication cannot silently bind the intent to a replacement Git
/// control plane. The cleanup Store adapter preserves this binding through a
/// crate-private authenticate-then-bind constructor; callers cannot
/// manufacture it from raw strings.
#[derive(Clone, PartialEq, Eq)]
pub struct DeliverySourceRecoveryIntent {
    pending: DeliverySourcePendingState,
    identity: WorktreeIdentity,
    base_commit_object_id: String,
    candidate_tree_object_id: String,
    expected_source_commit_object_id: Option<String>,
    input: DeliverySourceCommitInput,
    approved_fingerprint: WorkspaceFingerprint,
    config_attributes_digest: [u8; 32],
    provenance: DeliveryRecoveryProvenanceV1,
}

impl DeliverySourceRecoveryIntent {
    /// Captures an inert recovery intent from one fully authenticated source
    /// and its already-verified candidate/object metadata.
    ///
    /// This is intentionally the only public constructor: the runtime must
    /// not accept a caller-selected task, object, or directory provenance for
    /// a recovery operation.
    pub fn from_source(
        pending: DeliverySourcePendingState,
        source: &DeliverySourceCapability,
        candidate: &DeliveryCandidateTree,
        expected: Option<&DeliverySourceCommit>,
        input: DeliverySourceCommitInput,
    ) -> Result<Self, DeliverySourceError> {
        let candidate_provenance = source.candidate_tree_provenance()?;
        if !input.matches_identity(source.identity())
            || !candidate.is_bound_to(&candidate_provenance)
            || expected.is_some_and(|commit| !commit.is_bound_to(candidate.provenance()))
            || !matches!(
                (pending, expected),
                (DeliverySourcePendingState::ObjectPending, None)
                    | (DeliverySourcePendingState::CommitPending, Some(_))
            )
        {
            return Err(DeliverySourceError::AuthenticationChanged);
        }

        Ok(Self {
            pending,
            identity: source.identity().clone(),
            base_commit_object_id: source.base_commit().to_owned(),
            candidate_tree_object_id: candidate.object_id().to_owned(),
            expected_source_commit_object_id: expected.map(|commit| commit.object_id().to_owned()),
            input,
            approved_fingerprint: source.approved_fingerprint(),
            config_attributes_digest: *source.config_attributes_digest(),
            provenance: DeliveryRecoveryProvenanceV1::from_source(source),
        })
    }

    /// Reconstructs the inert source observation values only after a trusted
    /// cleanup-topology binder has authenticated the persisted common/admin
    /// identities. This remains crate-private so raw Store values can never be
    /// promoted directly into recovery or mutation authority.
    pub(in crate::delivery) fn from_persisted_cleanup(
        reservation: &WorktreeReservation,
        persisted: &DeliveryPersistedSourceRecovery,
        topology: &CleanupTopologyIntentV1,
    ) -> Result<Self, DeliverySourceError> {
        let expected = persisted
            .expected_source_commit()
            .ok_or(DeliverySourceError::AuthenticationChanged)?;
        if persisted.state() != DeliveryPersistedSourceState::Committed
            || persisted.identity() != reservation.identity()
            || persisted.source_branch() != format!("refs/heads/{}", reservation.branch_name())
            || persisted.base_commit().as_str() != reservation.base_commit()
            || !persisted
                .source_input()
                .matches_identity(reservation.identity())
            || topology.common_directory_identity().digest()
                != persisted.common_git_identity_digest()
            || topology.admin_directory_identity_digest()
                != persisted.worktree_admin_identity_digest()
        {
            return Err(DeliverySourceError::AuthenticationChanged);
        }
        Ok(Self {
            pending: DeliverySourcePendingState::CommitPending,
            identity: persisted.identity().clone(),
            base_commit_object_id: persisted.base_commit().as_str().to_owned(),
            candidate_tree_object_id: persisted.candidate_tree().as_str().to_owned(),
            expected_source_commit_object_id: Some(expected.as_str().to_owned()),
            input: persisted.source_input().clone(),
            approved_fingerprint: persisted.approved_fingerprint(),
            config_attributes_digest: *persisted.source_config_attributes_digest(),
            provenance: DeliveryRecoveryProvenanceV1::from_persisted_cleanup(topology),
        })
    }

    pub const fn pending_state(&self) -> DeliverySourcePendingState {
        self.pending
    }

    pub(in crate::delivery) fn candidate_tree_object_id(&self) -> &str {
        &self.candidate_tree_object_id
    }

    pub(in crate::delivery) const fn identity(&self) -> &WorktreeIdentity {
        &self.identity
    }

    pub(in crate::delivery) fn base_commit_object_id(&self) -> &str {
        &self.base_commit_object_id
    }

    pub(in crate::delivery) fn expected_source_commit_object_id(&self) -> Option<&str> {
        self.expected_source_commit_object_id.as_deref()
    }

    pub(in crate::delivery) const fn input(&self) -> &DeliverySourceCommitInput {
        &self.input
    }

    pub(in crate::delivery) const fn approved_fingerprint(&self) -> WorkspaceFingerprint {
        self.approved_fingerprint
    }

    pub(in crate::delivery) const fn config_attributes_digest(&self) -> &[u8; 32] {
        &self.config_attributes_digest
    }

    pub(in crate::delivery) fn is_bound_to_source(
        &self,
        source: &DeliverySourceCapability,
    ) -> bool {
        self.provenance.matches_source(source)
    }

    pub(in crate::delivery) fn is_bound_to_cleanup_source(
        &self,
        reservation: &WorktreeReservation,
        common_identity: &DurableDirectoryIdentityV1,
        admin_identity: &DurableDirectoryIdentityV1,
    ) -> bool {
        self.pending == DeliverySourcePendingState::CommitPending
            && self.identity == *reservation.identity()
            && self.base_commit_object_id == reservation.base_commit()
            && self.expected_source_commit_object_id.is_some()
            && self.input.matches_identity(reservation.identity())
            && self
                .provenance
                .matches_cleanup(common_identity, admin_identity)
    }

    /// Compares only the authenticated common-Git provenance needed to prove
    /// that a later registered-checkout capability addresses the same
    /// repository as this committed source. Branch cleanup deliberately does
    /// not expose or serialize the identity itself.
    pub(in crate::delivery) fn matches_cleanup_common_identity(
        &self,
        common_identity: &DurableDirectoryIdentityV1,
    ) -> bool {
        self.provenance.common_identity_digest == *common_identity.digest()
    }
}

impl fmt::Debug for DeliverySourceRecoveryIntent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliverySourceRecoveryIntent(<validated>)")
    }
}

/// Opaque v1 control-plane provenance captured only from a source capability.
///
/// It deliberately has neither textual accessors nor a `Debug` implementation:
/// durable directory identities are authorization evidence, not API, log, or
/// error payload.
#[derive(Clone, PartialEq, Eq)]
struct DeliveryRecoveryProvenanceV1 {
    common_identity_digest: [u8; 32],
    admin_identity_digest: [u8; 32],
    _reserved_lock: ReservedLockV1,
}

impl DeliveryRecoveryProvenanceV1 {
    fn from_source(source: &DeliverySourceCapability) -> Self {
        Self {
            common_identity_digest: *source.common_directory_identity().digest(),
            admin_identity_digest: *source.admin_directory_identity().digest(),
            _reserved_lock: ReservedLockV1,
        }
    }

    fn from_persisted_cleanup(topology: &CleanupTopologyIntentV1) -> Self {
        Self {
            common_identity_digest: *topology.common_directory_identity().digest(),
            admin_identity_digest: *topology.admin_directory_identity_digest(),
            _reserved_lock: ReservedLockV1,
        }
    }

    fn matches_source(&self, source: &DeliverySourceCapability) -> bool {
        self.common_identity_digest == *source.common_directory_identity().digest()
            && self.admin_identity_digest == *source.admin_directory_identity().digest()
    }

    fn matches_cleanup(
        &self,
        common_identity: &DurableDirectoryIdentityV1,
        admin_identity: &DurableDirectoryIdentityV1,
    ) -> bool {
        self.common_identity_digest == *common_identity.digest()
            && self.admin_identity_digest == *admin_identity.digest()
    }
}

/// The linked-worktree authenticator proves the fixed `codex-reserved` lock
/// before a capability can exist.  Keeping an explicit version marker here
/// binds that invariant into the intent without exposing a lock string.
#[derive(Clone, Copy, PartialEq, Eq)]
struct ReservedLockV1;

/// A recovery intent after its durable values have been syntax-checked against
/// one fresh authenticated source. It owns the capability so callers cannot
/// mix candidate, expected object, or metadata from another source.
pub struct DeliverySourceRecoveryCapability {
    source: DeliverySourceCapability,
    pending: DeliverySourcePendingState,
    candidate: DeliveryCandidateTree,
    expected: Option<DeliverySourceCommit>,
    input: DeliverySourceCommitInput,
}

/// Result of binding inert source scalars through fresh provisioner authority.
/// Known identity/config/object drift is a value so callers cannot confuse it
/// with a retryable infrastructure error.
pub enum DeliverySourceRecoveryBindingOutcome {
    Bound(Box<DeliverySourceRecoveryCapability>),
    ReconciliationRequired,
}

impl fmt::Debug for DeliverySourceRecoveryBindingOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bound(_) => {
                formatter.write_str("DeliverySourceRecoveryBindingOutcome::Bound(<opaque>)")
            }
            Self::ReconciliationRequired => {
                formatter.write_str("DeliverySourceRecoveryBindingOutcome::ReconciliationRequired")
            }
        }
    }
}

impl DeliverySourceRecoveryCapability {
    pub(in crate::delivery) fn from_bound(
        source: DeliverySourceCapability,
        pending: DeliverySourcePendingState,
        candidate: DeliveryCandidateTree,
        expected: Option<DeliverySourceCommit>,
        input: DeliverySourceCommitInput,
    ) -> Self {
        Self {
            source,
            pending,
            candidate,
            expected,
            input,
        }
    }

    pub const fn pending_state(&self) -> DeliverySourcePendingState {
        self.pending
    }

    pub(in crate::delivery) const fn source(&self) -> &DeliverySourceCapability {
        &self.source
    }

    pub(in crate::delivery) const fn candidate(&self) -> &DeliveryCandidateTree {
        &self.candidate
    }

    pub(in crate::delivery) const fn expected(&self) -> Option<&DeliverySourceCommit> {
        self.expected.as_ref()
    }

    pub(in crate::delivery) const fn input(&self) -> &DeliverySourceCommitInput {
        &self.input
    }
}

impl fmt::Debug for DeliverySourceRecoveryCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliverySourceRecoveryCapability(<opaque>)")
    }
}

/// Runtime-owned recovery binding captured from one authenticated live target.
///
/// The intent deliberately has no public constructor from a branch, object
/// ID, digest, or filesystem identity. `from_live` is the in-process capture
/// boundary; persisted target recovery binds through the provisioner's fresh
/// registered-checkout authentication instead of exposing a raw constructor.
#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryTargetRecoveryIntent {
    branch_name: String,
    old_head: DeliveryCommitOid,
    common_identity: DurableDirectoryIdentityV1,
    config_attributes_digest: [u8; 32],
    security_digest: [u8; 32],
}

impl DeliveryTargetRecoveryIntent {
    /// Captures the exact pre-mutation target provenance. This performs no Git
    /// observation because the input capability already owns that proof.
    pub fn from_live(target: &DeliveryTargetCapability) -> Self {
        Self {
            branch_name: target.branch_name().to_owned(),
            old_head: target.head().clone(),
            common_identity: target.common_directory_identity().clone(),
            config_attributes_digest: *target.config_attributes_digest(),
            security_digest: *target.security_digest(),
        }
    }

    pub(in crate::delivery) fn branch_name(&self) -> &str {
        &self.branch_name
    }

    pub(in crate::delivery) const fn old_head(&self) -> &DeliveryCommitOid {
        &self.old_head
    }

    pub(in crate::delivery) const fn common_identity(&self) -> &DurableDirectoryIdentityV1 {
        &self.common_identity
    }

    pub(in crate::delivery) const fn config_attributes_digest(&self) -> &[u8; 32] {
        &self.config_attributes_digest
    }

    pub(in crate::delivery) const fn security_digest(&self) -> &[u8; 32] {
        &self.security_digest
    }
}

impl fmt::Debug for DeliveryTargetRecoveryIntent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryTargetRecoveryIntent(<opaque>)")
    }
}

/// A persisted target intent rebound to a fresh registered-checkout
/// authentication. It exposes no path, command, object, or digest authority;
/// only delivery recovery classifiers may inspect its inner target binding.
pub struct DeliveryTargetRecoveryCapability {
    target: DeliveryTargetCapability,
}

/// Result of binding inert target scalars through fresh registered-checkout
/// authority.
pub enum DeliveryTargetRecoveryBindingOutcome {
    Bound(Box<DeliveryTargetRecoveryCapability>),
    ReconciliationRequired,
}

impl fmt::Debug for DeliveryTargetRecoveryBindingOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bound(_) => {
                formatter.write_str("DeliveryTargetRecoveryBindingOutcome::Bound(<opaque>)")
            }
            Self::ReconciliationRequired => {
                formatter.write_str("DeliveryTargetRecoveryBindingOutcome::ReconciliationRequired")
            }
        }
    }
}

/// Fully rebound source/target/expected-object recovery authority.  Raw Store
/// records cannot construct this value, and no component capability or OID is
/// exposed to callers.
pub struct DeliveryMergeRecoveryCapability {
    pub(super) source: DeliverySourceRecoveryCapability,
    pub(super) target: DeliveryTargetRecoveryCapability,
    pub(super) preflight: DeliveryPreflightResult,
    pub(super) expected: DeliveryExpectedMerge,
}

impl fmt::Debug for DeliveryMergeRecoveryCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryMergeRecoveryCapability(<opaque>)")
    }
}

pub enum DeliveryMergeRecoveryBindingOutcome {
    Bound(Box<DeliveryMergeRecoveryCapability>),
    ReconciliationRequired,
}

impl fmt::Debug for DeliveryMergeRecoveryBindingOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bound(_) => {
                formatter.write_str("DeliveryMergeRecoveryBindingOutcome::Bound(<opaque>)")
            }
            Self::ReconciliationRequired => {
                formatter.write_str("DeliveryMergeRecoveryBindingOutcome::ReconciliationRequired")
            }
        }
    }
}

/// Re-proves a persisted committed source and exact merge object before
/// composing either into recovery authority.  Every child in this function is
/// read-only; known drift produces `ReconciliationRequired`.
pub async fn bind_persisted_delivery_merge_recovery(
    source_provisioner: &DeliverySourceProvisioner,
    target_provisioner: &DeliveryTargetProvisioner,
    source: DeliverySourceRecoveryCapability,
    target: DeliveryTargetRecoveryCapability,
    persisted: &DeliveryPersistedMergeRecovery,
    cancellation: CancellationToken,
) -> Result<DeliveryMergeRecoveryBindingOutcome, DeliveryAbortError> {
    if source.expected().is_none()
        || persisted.object_format() != source.source().probe().object_format()
        || persisted.object_format() != target.target().probe().object_format()
        || !persisted
            .input()
            .matches_identity(source.source().identity())
        || !source
            .source()
            .probe()
            .shares_repository_format_authority_with(target.target().probe())
        || source.source().common_directory_identity()
            != target.target().common_directory_identity()
    {
        return Ok(DeliveryMergeRecoveryBindingOutcome::ReconciliationRequired);
    }
    let source_commit = source
        .expected()
        .expect("persisted merge recovery requires a source commit");
    if !committed_source_is_current(
        source_provisioner,
        source.source(),
        source.candidate(),
        source_commit,
        source.input(),
        cancellation.clone(),
    )
    .await?
    {
        return Ok(DeliveryMergeRecoveryBindingOutcome::ReconciliationRequired);
    }
    let source_provenance = match source.source().candidate_tree_provenance() {
        Ok(value) => value,
        Err(error) if is_source_recovery_mismatch(error) => {
            return Ok(DeliveryMergeRecoveryBindingOutcome::ReconciliationRequired);
        }
        Err(error) => return Err(error.into()),
    };
    let expected = DeliveryExpectedMerge::from_persisted_recovery(
        persisted.expected_merge_commit().clone(),
        persisted.candidate_merge_tree().clone(),
        target.target().head().clone(),
        source_commit.commit().clone(),
        persisted.input().clone(),
        persisted.merge_base().clone(),
        source_provenance,
        target.target(),
    );
    if let Err(error) = revalidate_expected_delivery_merge_object(
        target_provisioner,
        target.target(),
        &expected,
        cancellation,
    )
    .await
    {
        return merge_object_observation_failure(
            error,
            DeliveryMergeRecoveryBindingOutcome::ReconciliationRequired,
        );
    }
    let preflight = DeliveryPreflightResult::ready(
        source_commit.commit().clone(),
        persisted.merge_base().clone(),
        persisted.candidate_merge_tree().clone(),
    );
    Ok(DeliveryMergeRecoveryBindingOutcome::Bound(Box::new(
        DeliveryMergeRecoveryCapability {
            source,
            target,
            preflight,
            expected,
        },
    )))
}

impl DeliveryTargetRecoveryCapability {
    pub(in crate::delivery) const fn from_bound(target: DeliveryTargetCapability) -> Self {
        Self { target }
    }

    pub(in crate::delivery) const fn target(&self) -> &DeliveryTargetCapability {
        &self.target
    }

    pub(in crate::delivery) fn into_target(self) -> DeliveryTargetCapability {
        self.target
    }
}

impl fmt::Debug for DeliveryTargetRecoveryCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryTargetRecoveryCapability(<opaque>)")
    }
}

/// Chooses the only recovery disposition that is valid for an already-proven
/// source state.  The Git observation itself remains private to the delivery
/// provisioner; this small function makes the durable-state truth table easy
/// to test without giving callers command or filesystem authority.
pub(in crate::delivery) const fn disposition_for(
    pending: DeliverySourcePendingState,
    observation: RecoveryObservation,
) -> DeliverySourceRecoveryDisposition {
    match (pending, observation) {
        (DeliverySourcePendingState::ObjectPending, RecoveryObservation::ApprovedPreStage) => {
            DeliverySourceRecoveryDisposition::ReplayObject
        }
        (DeliverySourcePendingState::CommitPending, RecoveryObservation::ApprovedPreStage) => {
            DeliverySourceRecoveryDisposition::Continue
        }
        (DeliverySourcePendingState::CommitPending, RecoveryObservation::CandidateStaged) => {
            DeliverySourceRecoveryDisposition::StageComplete
        }
        (DeliverySourcePendingState::CommitPending, RecoveryObservation::ExpectedApplied) => {
            DeliverySourceRecoveryDisposition::Applied
        }
        _ => DeliverySourceRecoveryDisposition::ReconciliationRequired,
    }
}

/// Private normalized facts derived from bounded, typed Git observations.
/// Keeping this enum private prevents callers from manufacturing a recovery
/// success without first authenticating and observing the repository.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::delivery) enum RecoveryObservation {
    ApprovedPreStage,
    CandidateStaged,
    ExpectedApplied,
    Inconsistent,
}
