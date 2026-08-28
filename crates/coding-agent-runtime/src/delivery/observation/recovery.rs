use super::*;

impl DeliverySourceProvisioner {
    /// Binds a runtime-owned recovery intent through fresh linked-worktree
    /// authentication without requiring the source to still be pre-staged.
    ///
    /// This is deliberately distinct from `open_delivery_source`: normal
    /// opening proves the approved pre-stage state, whereas recovery must also
    /// observe the documented staged and already-applied CommitPending states.
    pub async fn open_delivery_source_for_recovery(
        &self,
        reservation: &WorktreeReservation,
        intent: &DeliverySourceRecoveryIntent,
        cancellation: CancellationToken,
    ) -> Result<DeliverySourceRecoveryCapability, DeliverySourceError> {
        require_not_cancelled(&cancellation)?;
        let (authentication, security) = self.authenticate_source(reservation)?;
        let (repository_probe, commands) = self
            .repository_bound_source_commands(
                authentication.command_context(),
                cancellation.clone(),
            )
            .await?;
        self.require_repository_object_format(&commands, &repository_probe, cancellation.clone())
            .await?;
        self.finalize_authentication(&authentication, &security)?;

        let source = DeliverySourceCapability::new(
            reservation,
            intent.approved_fingerprint(),
            *intent.config_attributes_digest(),
            repository_probe,
            authentication,
            commands,
            Arc::clone(&self.sandbox),
        );
        self.bind_recovery_intent(source, intent)
    }

    /// Authenticates inert Store scalars against a fresh linked-worktree and
    /// strictly re-proves every referenced Git object before returning opaque
    /// recovery authority. Known drift is a typed reconciliation outcome;
    /// this boundary never runs a mutation command.
    pub async fn bind_persisted_delivery_source_recovery(
        &self,
        reservation: &WorktreeReservation,
        persisted: &DeliveryPersistedSourceRecovery,
        cancellation: CancellationToken,
    ) -> Result<DeliverySourceRecoveryBindingOutcome, DeliverySourceError> {
        require_not_cancelled(&cancellation)?;
        if persisted.identity() != reservation.identity()
            || persisted.base_commit().as_str() != reservation.base_commit()
            || persisted.source_branch() != format!("refs/heads/{}", reservation.branch_name())
            || !persisted
                .source_input()
                .matches_identity(reservation.identity())
        {
            return Ok(DeliverySourceRecoveryBindingOutcome::ReconciliationRequired);
        }
        let (authentication, security) = match self.authenticate_source(reservation) {
            Ok(value) => value,
            Err(error) if is_recovery_mismatch(error) => {
                return Ok(DeliverySourceRecoveryBindingOutcome::ReconciliationRequired);
            }
            Err(error) => return Err(error),
        };
        let context = authentication.command_context();
        if context.common_identity.digest() != persisted.common_git_identity_digest()
            || context.admin_identity.digest() != persisted.worktree_admin_identity_digest()
        {
            return Ok(DeliverySourceRecoveryBindingOutcome::ReconciliationRequired);
        }
        let (repository_probe, commands) = match self
            .repository_bound_source_commands(context, cancellation.clone())
            .await
        {
            Ok(value) => value,
            Err(error) if is_recovery_mismatch(error) => {
                return Ok(DeliverySourceRecoveryBindingOutcome::ReconciliationRequired);
            }
            Err(error) => return Err(error),
        };
        if persisted.object_format() != repository_probe.object_format() {
            return Ok(DeliverySourceRecoveryBindingOutcome::ReconciliationRequired);
        }
        if let Err(error) = self
            .require_repository_object_format(&commands, &repository_probe, cancellation.clone())
            .await
        {
            return if is_recovery_mismatch(error) {
                Ok(DeliverySourceRecoveryBindingOutcome::ReconciliationRequired)
            } else {
                Err(error)
            };
        }
        if let Err(error) = self.finalize_authentication(&authentication, &security) {
            return if is_recovery_mismatch(error) {
                Ok(DeliverySourceRecoveryBindingOutcome::ReconciliationRequired)
            } else {
                Err(error)
            };
        }
        let source = DeliverySourceCapability::new(
            reservation,
            persisted.approved_fingerprint(),
            *persisted.source_config_attributes_digest(),
            repository_probe,
            authentication,
            commands,
            Arc::clone(&self.sandbox),
        );
        let provenance = match source.candidate_tree_provenance() {
            Ok(value) => value,
            Err(error) if is_recovery_mismatch(error) => {
                return Ok(DeliverySourceRecoveryBindingOutcome::ReconciliationRequired);
            }
            Err(error) => return Err(error),
        };
        let candidate =
            DeliveryCandidateTree::from_tree(persisted.candidate_tree().clone(), provenance);
        let expected = persisted.expected_source_commit().map(|commit| {
            DeliverySourceCommit::from_commit(commit.clone(), candidate.provenance().clone())
        });
        let real_index = match source
            .commands()
            .real_index_commands(source.probe(), source.branch_name())
        {
            Ok(value) => value,
            Err(error) if is_recovery_mismatch(error) => {
                return Ok(DeliverySourceRecoveryBindingOutcome::ReconciliationRequired);
            }
            Err(error) => return Err(error),
        };
        if let Err(error) = self
            .require_candidate_tree_type(&real_index, &candidate, cancellation.clone())
            .await
        {
            return if is_recovery_mismatch(error) {
                Ok(DeliverySourceRecoveryBindingOutcome::ReconciliationRequired)
            } else {
                Err(error)
            };
        }
        if let Some(expected) = expected.as_ref()
            && let Err(error) = self
                .verify_expected_source_commit(
                    &source,
                    expected.commit(),
                    candidate.tree(),
                    persisted.base_commit(),
                    persisted.source_input(),
                    cancellation.clone(),
                )
                .await
        {
            return if is_recovery_mismatch(error) {
                Ok(DeliverySourceRecoveryBindingOutcome::ReconciliationRequired)
            } else {
                Err(error)
            };
        }
        let current_security_matches = self
            .current_security_digest_matches(&source, &security, cancellation.clone())
            .await;
        match current_security_matches {
            Ok(true) => {}
            Ok(false) => return Ok(DeliverySourceRecoveryBindingOutcome::ReconciliationRequired),
            Err(error) if is_recovery_mismatch(error) => {
                return Ok(DeliverySourceRecoveryBindingOutcome::ReconciliationRequired);
            }
            Err(error) => return Err(error),
        }
        let recovery = DeliverySourceRecoveryCapability::from_bound(
            source,
            persisted.pending_state(),
            candidate,
            expected,
            persisted.source_input().clone(),
        );
        if persisted.state() == DeliveryPersistedSourceState::Committed {
            let Some(expected) = recovery.expected() else {
                return Ok(DeliverySourceRecoveryBindingOutcome::ReconciliationRequired);
            };
            match self
                .revalidate_preflight_committed_source(
                    recovery.source(),
                    recovery.candidate(),
                    expected,
                    recovery.input(),
                    cancellation,
                )
                .await
            {
                Ok(()) => {}
                Err(error) if is_recovery_mismatch(error) => {
                    return Ok(DeliverySourceRecoveryBindingOutcome::ReconciliationRequired);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(DeliverySourceRecoveryBindingOutcome::Bound(Box::new(
            recovery,
        )))
    }

    /// Replays the deterministic ObjectPending object creation only after a
    /// fresh, side-effect-free observation proves the original approved
    /// pre-stage state. It never updates the real index or source ref.
    pub async fn replay_source_commit(
        &self,
        recovery: &DeliverySourceRecoveryCapability,
        cancellation: CancellationToken,
    ) -> Result<DeliverySourceCommit, DeliverySourceError> {
        if recovery.pending_state() != DeliverySourcePendingState::ObjectPending
            || self
                .classify_source_recovery(recovery, cancellation.clone())
                .await?
                != DeliverySourceRecoveryDisposition::ReplayObject
        {
            return Err(DeliverySourceError::SourceChanged);
        }
        self.revalidate_open_source(recovery.source(), cancellation.clone())
            .await?;
        self.build_source_commit(
            recovery.source(),
            recovery.candidate(),
            recovery.input(),
            cancellation,
        )
        .await
    }

    /// Applies an already-persisted `CommitPending` intent to the real source
    /// index and its authenticated branch. There is deliberately no Store
    /// transition here: the caller may mark `Committed` only after this method
    /// returns the fully proven `Applied` disposition.
    ///
    /// The recovery disposition is observed before every write. A fully proven
    /// staged scene resumes only the CAS; a crash between `read-tree` and the
    /// required stat-cache refresh remains conservatively unreconciled. A
    /// prior crash after CAS returns its already-proven fact. No branch
    /// performs reset, clean, or checkout.
    pub async fn apply_source_commit(
        &self,
        recovery: &DeliverySourceRecoveryCapability,
        cancellation: CancellationToken,
    ) -> Result<DeliverySourceRecoveryDisposition, DeliverySourceError> {
        if recovery.pending_state() != DeliverySourcePendingState::CommitPending {
            return Ok(DeliverySourceRecoveryDisposition::ReconciliationRequired);
        }
        match self
            .classify_source_recovery(recovery, cancellation.clone())
            .await?
        {
            DeliverySourceRecoveryDisposition::Continue => {
                self.apply_pre_stage_source_commit(
                    recovery.source(),
                    recovery.candidate(),
                    recovery
                        .expected()
                        .ok_or(DeliverySourceError::AuthenticationChanged)?,
                    recovery.input(),
                    cancellation,
                )
                .await
            }
            DeliverySourceRecoveryDisposition::StageComplete => {
                let result = self
                    .complete_staged_source_commit(
                        recovery.source(),
                        recovery.candidate(),
                        recovery
                            .expected()
                            .ok_or(DeliverySourceError::AuthenticationChanged)?,
                        recovery.input(),
                        cancellation,
                    )
                    .await;
                match result {
                    Err(error) if is_recovery_mismatch(error) => {
                        Ok(DeliverySourceRecoveryDisposition::ReconciliationRequired)
                    }
                    other => other,
                }
            }
            DeliverySourceRecoveryDisposition::Applied => {
                Ok(DeliverySourceRecoveryDisposition::Applied)
            }
            DeliverySourceRecoveryDisposition::ReplayObject
            | DeliverySourceRecoveryDisposition::ReconciliationRequired => {
                Ok(DeliverySourceRecoveryDisposition::ReconciliationRequired)
            }
        }
    }

    /// Observes a freshly bound persisted source intent without mutating the
    /// real index, ref, or worktree. All mismatch combinations deliberately
    /// remain reconciliation work.
    pub async fn classify_source_recovery(
        &self,
        recovery: &DeliverySourceRecoveryCapability,
        cancellation: CancellationToken,
    ) -> Result<DeliverySourceRecoveryDisposition, DeliverySourceError> {
        require_not_cancelled(&cancellation)?;
        let source = recovery.source();
        let candidate = recovery.candidate();
        let input = recovery.input();
        let pending = recovery.pending_state();
        let expected = recovery.expected();
        let base = self.require_commit_pending_binding(source, candidate, input)?;
        if matches!(
            (pending, expected),
            (DeliverySourcePendingState::ObjectPending, Some(_))
                | (DeliverySourcePendingState::CommitPending, None)
        ) {
            return Ok(DeliverySourceRecoveryDisposition::ReconciliationRequired);
        }
        let commands = source
            .commands()
            .real_index_commands(source.probe(), source.branch_name())?;
        let observation = self
            .observe_pending_source_state(
                PendingSourceStateObservation {
                    source,
                    candidate,
                    expected,
                    input,
                    base: &base,
                    commands: &commands,
                },
                cancellation,
            )
            .await?;
        Ok(delivery_recovery::disposition_for(pending, observation))
    }

    fn bind_recovery_intent(
        &self,
        source: DeliverySourceCapability,
        intent: &DeliverySourceRecoveryIntent,
    ) -> Result<DeliverySourceRecoveryCapability, DeliverySourceError> {
        self.require_capability_binding(&source)?;
        if intent.identity() != source.identity()
            || intent.base_commit_object_id() != source.base_commit()
            || !intent.input().matches_identity(source.identity())
            || !intent.is_bound_to_source(&source)
        {
            return Err(DeliverySourceError::AuthenticationChanged);
        }
        let candidate_tree = super::DeliveryTreeOid::try_new(
            intent.candidate_tree_object_id(),
            source.probe().object_format(),
        )
        .ok_or(DeliverySourceError::AuthenticationChanged)?;
        let candidate =
            DeliveryCandidateTree::from_tree(candidate_tree, source.candidate_tree_provenance()?);
        let expected = match intent.expected_source_commit_object_id() {
            Some(object_id) => Some(DeliverySourceCommit::from_commit(
                DeliveryCommitOid::try_new(object_id, source.probe().object_format())
                    .ok_or(DeliverySourceError::AuthenticationChanged)?,
                candidate.provenance().clone(),
            )),
            None => None,
        };
        if !matches!(
            (intent.pending_state(), expected.as_ref()),
            (DeliverySourcePendingState::ObjectPending, None)
                | (DeliverySourcePendingState::CommitPending, Some(_))
        ) {
            return Err(DeliverySourceError::AuthenticationChanged);
        }
        Ok(DeliverySourceRecoveryCapability::from_bound(
            source,
            intent.pending_state(),
            candidate,
            expected,
            intent.input().clone(),
        ))
    }

    async fn apply_pre_stage_source_commit(
        &self,
        source: &DeliverySourceCapability,
        candidate: &DeliveryCandidateTree,
        expected: &DeliverySourceCommit,
        input: &DeliverySourceCommitInput,
        cancellation: CancellationToken,
    ) -> Result<DeliverySourceRecoveryDisposition, DeliverySourceError> {
        require_not_cancelled(&cancellation)?;
        let expected_tree = self.require_commit_pending_binding(source, candidate, input)?;
        self.revalidate_pre_stage_source(source, cancellation.clone())
            .await?;
        // The expected object is durable input to CommitPending, not a value
        // Git may validate only after it has already changed the real index.
        // Prove its exact tree/parent/metadata shape before the first real
        // index mutation.
        self.verify_expected_source_commit(
            source,
            expected.commit(),
            candidate.tree(),
            &expected_tree,
            input,
            cancellation.clone(),
        )
        .await?;
        let commands = source
            .commands()
            .real_index_commands(source.probe(), source.branch_name())?;
        self.require_candidate_tree_type(&commands, candidate, cancellation.clone())
            .await?;

        self.executor
            .run_start_preserving_mutation(
                commands.stage_candidate_in_real_index(candidate.tree())?,
                cancellation.clone(),
                self.limits.max_status_bytes(),
            )
            .await
            .map_err(first_real_index_mutation_error)?;
        // `read-tree --reset` installs tree entries with no current stat-cache
        // data. Refresh that cache before the fixed `diff-files` predicate;
        // otherwise an unchanged work tree can look dirty solely because the
        // freshly staged index has not yet observed its files.
        self.executor
            .run(
                commands.refresh_real_index_stat()?,
                cancellation.clone(),
                self.limits.max_status_bytes(),
            )
            .await
            .map_err(post_real_index_mutation_error)?;
        self.after_real_index_add();

        self.require_predicate_matched(
            commands.index_matches_tree(candidate.tree())?,
            cancellation.clone(),
        )
        .await
        .map_err(post_real_index_mutation_error)?;

        self.verify_expected_source_commit(
            source,
            expected.commit(),
            candidate.tree(),
            &expected_tree,
            input,
            cancellation.clone(),
        )
        .await
        .map_err(post_real_index_mutation_error)?;
        self.after_source_object_reverify();

        self.executor
            .run(
                commands.update_source_ref_cas(expected.commit(), &expected_tree)?,
                cancellation.clone(),
                self.limits.max_status_bytes(),
            )
            .await
            .map_err(post_real_index_mutation_error)?;
        self.after_source_ref_cas();

        self.require_applied_source_state(
            source,
            candidate,
            expected,
            input,
            &commands,
            cancellation,
        )
        .await
        .map_err(post_real_index_mutation_error)?;
        Ok(DeliverySourceRecoveryDisposition::Applied)
    }

    /// Resumes only the compare-and-swap portion of a CommitPending intent
    /// whose real index is already proven to equal the candidate tree.
    async fn complete_staged_source_commit(
        &self,
        source: &DeliverySourceCapability,
        candidate: &DeliveryCandidateTree,
        expected: &DeliverySourceCommit,
        input: &DeliverySourceCommitInput,
        cancellation: CancellationToken,
    ) -> Result<DeliverySourceRecoveryDisposition, DeliverySourceError> {
        require_not_cancelled(&cancellation)?;
        let base = self.require_commit_pending_binding(source, candidate, input)?;
        let commands = source
            .commands()
            .real_index_commands(source.probe(), source.branch_name())?;
        let security = self.prepare_recovery_observation(source)?;
        self.require_no_real_index_lock(source)?;
        self.require_control_state(
            source.commands(),
            base.as_str(),
            source.branch_name(),
            cancellation.clone(),
        )
        .await?;
        self.require_predicate_matched(
            commands.index_matches_tree(candidate.tree())?,
            cancellation.clone(),
        )
        .await?;
        self.require_predicate_matched(commands.worktree_matches_index()?, cancellation.clone())
            .await?;
        self.require_no_untracked_paths(source, cancellation.clone())
            .await?;
        self.require_candidate_tree_type(&commands, candidate, cancellation.clone())
            .await?;
        self.verify_expected_source_commit(
            source,
            expected.commit(),
            candidate.tree(),
            &base,
            input,
            cancellation.clone(),
        )
        .await
        .map_err(post_real_index_mutation_error)?;
        self.require_current_security_digest(source, &security, cancellation.clone())
            .await
            .map_err(post_real_index_mutation_error)?;
        self.finalize_authentication(source.authentication(), &security)
            .map_err(post_real_index_mutation_error)?;
        self.executor
            .run(
                commands.update_source_ref_cas(expected.commit(), &base)?,
                cancellation.clone(),
                self.limits.max_status_bytes(),
            )
            .await
            .map_err(post_real_index_mutation_error)?;
        self.after_source_ref_cas();
        self.require_applied_source_state(
            source,
            candidate,
            expected,
            input,
            &commands,
            cancellation,
        )
        .await
        .map_err(post_real_index_mutation_error)?;
        Ok(DeliverySourceRecoveryDisposition::Applied)
    }

    pub(super) fn require_commit_pending_binding(
        &self,
        source: &DeliverySourceCapability,
        candidate: &DeliveryCandidateTree,
        input: &DeliverySourceCommitInput,
    ) -> Result<DeliveryCommitOid, DeliverySourceError> {
        self.require_capability_binding(source)?;
        if !candidate.is_bound_to(&source.candidate_tree_provenance()?)
            || !input.matches_identity(source.identity())
        {
            return Err(DeliverySourceError::AuthenticationChanged);
        }
        DeliveryCommitOid::try_new(source.base_commit(), source.probe().object_format())
            .ok_or(DeliverySourceError::AuthenticationChanged)
    }

    pub(super) async fn revalidate_pre_stage_source(
        &self,
        source: &DeliverySourceCapability,
        cancellation: CancellationToken,
    ) -> Result<(), DeliverySourceError> {
        self.revalidate_open_source(source, cancellation).await
    }

    pub(super) async fn verify_expected_source_commit(
        &self,
        source: &DeliverySourceCapability,
        expected_commit: &DeliveryCommitOid,
        expected_tree: &super::DeliveryTreeOid,
        expected_parent: &DeliveryCommitOid,
        input: &DeliverySourceCommitInput,
        cancellation: CancellationToken,
    ) -> Result<(), DeliverySourceError> {
        let object_commands = source.commands().mutation_commands(source.probe())?;
        source_commit::verify_existing_source_commit(
            source_commit::SourceCommitVerificationRequest {
                executor: &self.executor,
                commands: &object_commands,
                probe: source.probe(),
                expected_commit,
                expected_tree,
                expected_parent,
                input,
                cancellation,
                output_limit: self.limits.max_status_bytes(),
            },
        )
        .await?;
        Ok(())
    }

    /// Proves a persisted candidate still denotes a tree before a recovery
    /// path lets Git replace the real index or publish a source ref.  A
    /// non-tree/missing object is a known source mismatch; an indeterminate
    /// child outcome remains distinct so callers do not retry a side effect
    /// on a guess.
    pub(super) async fn require_candidate_tree_type(
        &self,
        commands: &DeliverySourceRealIndexCommands,
        candidate: &DeliveryCandidateTree,
        cancellation: CancellationToken,
    ) -> Result<(), DeliverySourceError> {
        match self
            .executor
            .run(
                commands.inspect_candidate_object_type(candidate.tree())?,
                cancellation,
                EXACT_GIT_OBJECT_TYPE_OUTPUT_LIMIT,
            )
            .await
        {
            Ok(output) => require_exact_tree_object_type(&output),
            Err(DeliverySourceError::CommandFailed) => Err(DeliverySourceError::SourceChanged),
            Err(error) => Err(error),
        }
    }

    pub(super) async fn require_applied_source_state(
        &self,
        source: &DeliverySourceCapability,
        candidate: &DeliveryCandidateTree,
        expected: &DeliverySourceCommit,
        input: &DeliverySourceCommitInput,
        commands: &DeliverySourceRealIndexCommands,
        cancellation: CancellationToken,
    ) -> Result<(), DeliverySourceError> {
        let base = self.require_commit_pending_binding(source, candidate, input)?;
        let security = self.prepare_recovery_observation(source)?;
        self.require_no_real_index_lock(source)?;
        self.require_candidate_tree_type(commands, candidate, cancellation.clone())
            .await?;
        self.require_control_state(
            source.commands(),
            expected.commit().as_str(),
            source.branch_name(),
            cancellation.clone(),
        )
        .await?;
        self.require_predicate_matched(
            commands.index_matches_tree(candidate.tree())?,
            cancellation.clone(),
        )
        .await?;
        self.require_predicate_matched(commands.worktree_matches_index()?, cancellation.clone())
            .await?;
        self.require_no_untracked_paths(source, cancellation.clone())
            .await?;
        self.verify_expected_source_commit(
            source,
            expected.commit(),
            candidate.tree(),
            &base,
            input,
            cancellation.clone(),
        )
        .await?;
        self.require_current_security_digest(source, &security, cancellation.clone())
            .await?;
        self.finalize_authentication(source.authentication(), &security)
    }

    async fn observe_pending_source_state(
        &self,
        observation: PendingSourceStateObservation<'_>,
        cancellation: CancellationToken,
    ) -> Result<RecoveryObservation, DeliverySourceError> {
        let first = self
            .observe_pending_source_state_once(&observation, cancellation.clone())
            .await?;
        let second = self
            .observe_pending_source_state_once(&observation, cancellation)
            .await?;
        Ok(if first == second {
            first
        } else {
            RecoveryObservation::Inconsistent
        })
    }

    async fn observe_pending_source_state_once(
        &self,
        observation: &PendingSourceStateObservation<'_>,
        cancellation: CancellationToken,
    ) -> Result<RecoveryObservation, DeliverySourceError> {
        let observation = async {
            let source = observation.source;
            let candidate = observation.candidate;
            let expected = observation.expected;
            let input = observation.input;
            let base = observation.base;
            let commands = observation.commands;
            let security = self.prepare_recovery_observation(source)?;
            if self.require_no_real_index_lock(source).is_err() {
                return Ok(RecoveryObservation::Inconsistent);
            }
            self.require_candidate_tree_type(commands, candidate, cancellation.clone())
                .await?;
            let head = self
                .observe_source_head(source, cancellation.clone())
                .await?;
            let attributes_match = self
                .current_security_digest_matches(source, &security, cancellation.clone())
                .await?;
            if !attributes_match {
                return Ok(RecoveryObservation::Inconsistent);
            }
            let observation = if head.as_ref() == Some(base) {
                if self
                    .is_approved_pre_stage(source, cancellation.clone())
                    .await?
                {
                    if let Some(expected) = expected {
                        self.verify_expected_source_commit(
                            source,
                            expected.commit(),
                            candidate.tree(),
                            base,
                            input,
                            cancellation.clone(),
                        )
                        .await?;
                    }
                    RecoveryObservation::ApprovedPreStage
                } else if self
                    .is_candidate_staged(source, candidate, commands, cancellation.clone())
                    .await?
                {
                    if let Some(expected) = expected {
                        self.verify_expected_source_commit(
                            source,
                            expected.commit(),
                            candidate.tree(),
                            base,
                            input,
                            cancellation.clone(),
                        )
                        .await?;
                    }
                    RecoveryObservation::CandidateStaged
                } else {
                    RecoveryObservation::Inconsistent
                }
            } else if let Some(expected) = expected {
                if head.as_ref() == Some(expected.commit())
                    && self
                        .is_candidate_staged(source, candidate, commands, cancellation.clone())
                        .await?
                {
                    self.verify_expected_source_commit(
                        source,
                        expected.commit(),
                        candidate.tree(),
                        base,
                        input,
                        cancellation.clone(),
                    )
                    .await?;
                    RecoveryObservation::ExpectedApplied
                } else {
                    RecoveryObservation::Inconsistent
                }
            } else {
                RecoveryObservation::Inconsistent
            };
            self.finalize_authentication(source.authentication(), &security)?;
            Ok(observation)
        }
        .await;
        match observation {
            Ok(observation) => Ok(observation),
            Err(error) if is_recovery_mismatch(error) => Ok(RecoveryObservation::Inconsistent),
            Err(error) => Err(error),
        }
    }

    fn prepare_recovery_observation(
        &self,
        source: &DeliverySourceCapability,
    ) -> Result<GitSecuritySnapshot, DeliverySourceError> {
        self.require_capability_binding(source)?;
        source.sandbox().revalidate()?;
        source
            .authentication()
            .reauthenticate()
            .map_err(DeliverySourceError::from)?;
        let security = GitSecuritySnapshot::capture_authenticated(
            source.authentication().command_context(),
            self.limits,
        )?;
        let repeated = GitSecuritySnapshot::capture_authenticated(
            source.authentication().command_context(),
            self.limits,
        )?;
        require_same_security_snapshot(&security, &repeated)?;
        Ok(security)
    }

    fn require_no_real_index_lock(
        &self,
        source: &DeliverySourceCapability,
    ) -> Result<(), DeliverySourceError> {
        let root = source
            .authentication()
            .command_context()
            .worktree_admin
            .capability
            .try_clone_root()
            .map_err(|_| DeliverySourceError::AuthenticationChanged)?;
        match child_entry_exists(&root, OsStr::new("index.lock")) {
            Ok(false) => Ok(()),
            Ok(true) => Err(DeliverySourceError::SourceChanged),
            Err(_) => Err(DeliverySourceError::AuthenticationChanged),
        }
    }

    async fn observe_source_head(
        &self,
        source: &DeliverySourceCapability,
        cancellation: CancellationToken,
    ) -> Result<Option<DeliveryCommitOid>, DeliverySourceError> {
        let head = self
            .executor
            .run(
                source.commands().resolve_head()?,
                cancellation.clone(),
                self.limits.max_status_bytes(),
            )
            .await?;
        let symbolic = match self
            .executor
            .run(
                source.commands().symbolic_head()?,
                cancellation,
                self.limits.max_status_bytes(),
            )
            .await
        {
            Ok(value) => value,
            Err(DeliverySourceError::CommandFailed) => return Ok(None),
            Err(error) => return Err(error),
        };
        let expected_symbolic = format!("refs/heads/{}", source.branch_name());
        if parse_one_line(&symbolic)? != expected_symbolic {
            return Ok(None);
        }
        let object = parse_object_id(&head, source.probe().object_format().hexadecimal_length())?;
        Ok(DeliveryCommitOid::try_new(
            object,
            source.probe().object_format(),
        ))
    }

    async fn is_approved_pre_stage(
        &self,
        source: &DeliverySourceCapability,
        cancellation: CancellationToken,
    ) -> Result<bool, DeliverySourceError> {
        let observed = self
            .collect_fingerprint(source.commands(), source.authentication(), cancellation)
            .await?;
        Ok(observed.fingerprint == source.approved_fingerprint())
    }

    async fn is_candidate_staged(
        &self,
        source: &DeliverySourceCapability,
        candidate: &DeliveryCandidateTree,
        commands: &DeliverySourceRealIndexCommands,
        cancellation: CancellationToken,
    ) -> Result<bool, DeliverySourceError> {
        if !self
            .predicate_matches(
                commands.index_matches_tree(candidate.tree())?,
                cancellation.clone(),
            )
            .await?
        {
            return Ok(false);
        }
        if !self
            .predicate_matches(commands.worktree_matches_index()?, cancellation.clone())
            .await?
        {
            return Ok(false);
        }
        self.require_no_untracked_paths(source, cancellation)
            .await?;
        Ok(true)
    }

    async fn require_no_untracked_paths(
        &self,
        source: &DeliverySourceCapability,
        cancellation: CancellationToken,
    ) -> Result<(), DeliverySourceError> {
        let output = self
            .executor
            .run(
                source.commands().untracked_paths()?,
                cancellation,
                self.limits.max_status_bytes(),
            )
            .await?;
        if output.is_empty() {
            Ok(())
        } else {
            Err(DeliverySourceError::SourceChanged)
        }
    }

    async fn require_predicate_matched(
        &self,
        command: crate::command_policy::ValidatedCommand,
        cancellation: CancellationToken,
    ) -> Result<(), DeliverySourceError> {
        if self.predicate_matches(command, cancellation).await? {
            Ok(())
        } else {
            Err(DeliverySourceError::SourceChanged)
        }
    }

    async fn predicate_matches(
        &self,
        command: crate::command_policy::ValidatedCommand,
        cancellation: CancellationToken,
    ) -> Result<bool, DeliverySourceError> {
        Ok(matches!(
            self.executor
                .run_predicate(command, cancellation, self.limits.max_status_bytes())
                .await?,
            DeliveryCommandExit::Matched
        ))
    }

    async fn require_current_security_digest(
        &self,
        source: &DeliverySourceCapability,
        security: &GitSecuritySnapshot,
        cancellation: CancellationToken,
    ) -> Result<(), DeliverySourceError> {
        if self
            .current_security_digest_matches(source, security, cancellation)
            .await?
        {
            Ok(())
        } else {
            Err(DeliverySourceError::SourceChanged)
        }
    }

    async fn current_security_digest_matches(
        &self,
        source: &DeliverySourceCapability,
        security: &GitSecuritySnapshot,
        cancellation: CancellationToken,
    ) -> Result<bool, DeliverySourceError> {
        let observed = self
            .collect_fingerprint(
                source.commands(),
                source.authentication(),
                cancellation.clone(),
            )
            .await?;
        let attributes = self
            .require_safe_attributes(source.commands(), &observed, security, cancellation)
            .await?;
        Ok(attributes == *source.config_attributes_digest())
    }
}
