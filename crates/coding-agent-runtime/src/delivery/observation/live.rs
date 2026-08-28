use super::*;

impl DeliverySourceProvisioner {
    pub async fn open_delivery_source(
        &self,
        reservation: &WorktreeReservation,
        approved_fingerprint: WorkspaceFingerprint,
        cancellation: CancellationToken,
    ) -> Result<DeliverySourceCapability, DeliverySourceError> {
        require_not_cancelled(&cancellation)?;
        let (authentication, security) = self.authenticate_source(reservation)?;
        let (repository_probe, commands) = self
            .repository_bound_source_commands(
                authentication.command_context(),
                cancellation.clone(),
            )
            .await?;
        let attributes_digest = self
            .observe_reviewed_source(ReviewedSourceObservation {
                commands: &commands,
                authentication: &authentication,
                expected_base: reservation.base_commit(),
                expected_branch: reservation.branch_name(),
                approved_fingerprint,
                security: &security,
                cancellation: cancellation.clone(),
            })
            .await?;
        self.require_repository_object_format(&commands, &repository_probe, cancellation.clone())
            .await?;
        self.finalize_authentication(&authentication, &security)?;
        Ok(DeliverySourceCapability::new(
            reservation,
            approved_fingerprint,
            attributes_digest,
            repository_probe,
            authentication,
            commands,
            Arc::clone(&self.sandbox),
        ))
    }

    /// Constructs an unreferenced candidate tree only when the already-opened
    /// source remains the exact reviewed source both before and after the
    /// private-index mutation sequence.  No Store or ref mutation is involved
    /// at this runtime boundary.
    pub async fn build_candidate_tree(
        &self,
        source: &DeliverySourceCapability,
        cancellation: CancellationToken,
    ) -> Result<DeliveryCandidateTree, DeliverySourceError> {
        require_not_cancelled(&cancellation)?;
        self.revalidate_open_source(source, cancellation.clone())
            .await?;
        self.after_candidate_revalidation();
        let commands = source.commands().mutation_commands(source.probe())?;
        let candidate = source_tree::build_candidate_tree(
            &self.executor,
            &commands,
            source.probe(),
            source.candidate_tree_provenance()?,
            self.fingerprint_limits,
            cancellation.clone(),
            self.limits.max_status_bytes(),
        )
        .await?;
        self.after_candidate_tree_write();
        self.revalidate_open_source(source, cancellation).await?;
        Ok(candidate)
    }

    /// Creates and verifies an unreferenced deterministic source commit from
    /// the caller's already-persisted ObjectPending metadata. This boundary
    /// cannot update a ref, real index, or worktree.
    pub async fn build_source_commit(
        &self,
        source: &DeliverySourceCapability,
        candidate: &DeliveryCandidateTree,
        input: &DeliverySourceCommitInput,
        cancellation: CancellationToken,
    ) -> Result<DeliverySourceCommit, DeliverySourceError> {
        require_not_cancelled(&cancellation)?;
        self.require_capability_binding(source)?;
        source.sandbox().revalidate()?;
        source
            .authentication()
            .reauthenticate()
            .map_err(DeliverySourceError::from)?;
        let parent =
            DeliveryCommitOid::try_new(source.base_commit(), source.probe().object_format())
                .ok_or(DeliverySourceError::AuthenticationChanged)?;
        if !candidate.is_bound_to(&source.candidate_tree_provenance()?)
            || !input.matches_identity(source.identity())
        {
            return Err(DeliverySourceError::AuthenticationChanged);
        }
        let commands = source.commands().mutation_commands(source.probe())?;
        let commit = source_commit::build_and_verify_source_commit(
            source_commit::SourceCommitBuildRequest {
                executor: &self.executor,
                commands: &commands,
                probe: source.probe(),
                tree: candidate.tree(),
                parent: &parent,
                input,
                cancellation,
                output_limit: self.limits.max_status_bytes(),
            },
        )
        .await?;
        Ok(DeliverySourceCommit::from_commit(
            commit,
            candidate.provenance().clone(),
        ))
    }

    /// Re-proves a freshly built deterministic source object and projects the
    /// exact scalar shape the Store may bind to `CommitPending`.
    ///
    /// The returned value carries no repository or command authority.  A
    /// caller cannot mint it from object IDs: this provisioner first
    /// reauthenticates the source, proves the candidate is a tree, and checks
    /// the raw commit tree/parent/metadata shape.
    pub async fn project_delivery_source_object(
        &self,
        source: &DeliverySourceCapability,
        candidate: &DeliveryCandidateTree,
        expected: &DeliverySourceCommit,
        input: &DeliverySourceCommitInput,
        cancellation: CancellationToken,
    ) -> Result<super::super::DeliverySourceObjectPersistenceBinding, DeliverySourceError> {
        require_not_cancelled(&cancellation)?;
        let parent = self.require_commit_pending_binding(source, candidate, input)?;
        if !expected.is_bound_to(candidate.provenance()) {
            return Err(DeliverySourceError::AuthenticationChanged);
        }
        self.revalidate_pre_stage_source(source, cancellation.clone())
            .await?;
        let commands = source
            .commands()
            .real_index_commands(source.probe(), source.branch_name())?;
        self.require_candidate_tree_type(&commands, candidate, cancellation.clone())
            .await?;
        self.verify_expected_source_commit(
            source,
            expected.commit(),
            candidate.tree(),
            &parent,
            input,
            cancellation,
        )
        .await?;
        super::super::persistence::source_object_persistence_binding(
            source, candidate, expected, input,
        )
    }

    /// Creates the fixed, preflight-only source object for `merge-tree`.
    /// Unlike [`Self::build_source_commit`], this has no ObjectPending input
    /// and cannot be mistaken for the later durable source commit.
    pub(in crate::delivery) async fn build_preflight_source_commit(
        &self,
        source: &DeliverySourceCapability,
        candidate: &DeliveryCandidateTree,
        cancellation: CancellationToken,
    ) -> Result<DeliveryCommitOid, DeliverySourceError> {
        require_not_cancelled(&cancellation)?;
        self.require_capability_binding(source)?;
        source.sandbox().revalidate()?;
        source
            .authentication()
            .reauthenticate()
            .map_err(DeliverySourceError::from)?;
        let parent =
            DeliveryCommitOid::try_new(source.base_commit(), source.probe().object_format())
                .ok_or(DeliverySourceError::AuthenticationChanged)?;
        if !candidate.is_bound_to(&source.candidate_tree_provenance()?) {
            return Err(DeliverySourceError::AuthenticationChanged);
        }
        let commands = source.commands().mutation_commands(source.probe())?;
        source_commit::build_and_verify_preflight_source_commit(
            &self.executor,
            &commands,
            source.probe(),
            candidate.tree(),
            &parent,
            cancellation,
            self.limits.max_status_bytes(),
        )
        .await
    }

    /// Prepares the two exact object identities persisted by the durable
    /// preflight intent.  Applications call this only after `PreflightPending`
    /// exists; the method never updates a source ref, the real index, or either
    /// worktree.
    ///
    /// Construction is capability-only: neither object ID can be supplied by
    /// the caller.  The reviewed source is freshly proven around candidate
    /// creation and again after the fixed preflight commit is verified.
    pub async fn prepare_delivery_preflight_source(
        &self,
        source: &DeliverySourceCapability,
        cancellation: CancellationToken,
    ) -> Result<PreparedDeliveryPreflightSource, DeliverySourceError> {
        require_not_cancelled(&cancellation)?;
        self.revalidate_preflight_candidate_source(source, cancellation.clone())
            .await?;
        let candidate = self
            .build_candidate_tree(source, cancellation.clone())
            .await?;
        self.revalidate_preflight_candidate_source(source, cancellation.clone())
            .await?;
        let source_commit = self
            .build_preflight_source_commit(source, &candidate, cancellation.clone())
            .await?;
        self.revalidate_preflight_candidate_source(source, cancellation)
            .await?;
        Ok(PreparedDeliveryPreflightSource::from_verified(
            candidate,
            source_commit,
        ))
    }

    /// Re-authenticates a prepared source and re-proves its exact fixed commit
    /// before any target-side `merge-tree` command may be constructed.
    ///
    /// Re-running deterministic `commit-tree` is an idempotent object-database
    /// replay.  Its raw commit is inspected by the existing fixed verifier and
    /// must reproduce the bound object ID; refs, real indexes, and worktrees
    /// remain outside this command vocabulary.
    pub(in crate::delivery) async fn revalidate_prepared_delivery_preflight_source(
        &self,
        source: &DeliverySourceCapability,
        prepared: &PreparedDeliveryPreflightSource,
        cancellation: CancellationToken,
    ) -> Result<(), DeliverySourceError> {
        require_not_cancelled(&cancellation)?;
        self.revalidate_preflight_candidate_source(source, cancellation.clone())
            .await?;
        if !prepared.is_bound_to(&source.candidate_tree_provenance()?) {
            return Err(DeliverySourceError::AuthenticationChanged);
        }
        let observed = self
            .build_preflight_source_commit(source, prepared.candidate(), cancellation.clone())
            .await?;
        if &observed != prepared.source_commit() {
            return Err(DeliverySourceError::AuthenticationChanged);
        }
        self.revalidate_preflight_candidate_source(source, cancellation)
            .await
    }

    /// Re-proves the reviewed, pre-commit source state for the target-side
    /// preflight path.  The method stays delivery-private because it exposes
    /// no additional source capability to callers; it merely keeps the
    /// candidate-object branch from relying on a stale source observation.
    pub(in crate::delivery) async fn revalidate_preflight_candidate_source(
        &self,
        source: &DeliverySourceCapability,
        cancellation: CancellationToken,
    ) -> Result<(), DeliverySourceError> {
        self.revalidate_open_source(source, cancellation).await
    }

    /// Re-proves a source that has already advanced to its exact deterministic
    /// source commit.  This differs from the normal open-state proof: the
    /// source HEAD and real index are now expected to equal the persisted
    /// candidate/commit tuple rather than the original base/fingerprint scene.
    pub(in crate::delivery) async fn revalidate_preflight_committed_source(
        &self,
        source: &DeliverySourceCapability,
        candidate: &DeliveryCandidateTree,
        expected: &DeliverySourceCommit,
        input: &DeliverySourceCommitInput,
        cancellation: CancellationToken,
    ) -> Result<(), DeliverySourceError> {
        self.require_capability_binding(source)?;
        if !candidate.is_bound_to(&source.candidate_tree_provenance()?)
            || !expected.is_bound_to(candidate.provenance())
            || !input.matches_identity(source.identity())
        {
            return Err(DeliverySourceError::AuthenticationChanged);
        }
        let commands = source
            .commands()
            .real_index_commands(source.probe(), source.branch_name())?;
        self.require_applied_source_state(
            source,
            candidate,
            expected,
            input,
            &commands,
            cancellation,
        )
        .await
    }
}
