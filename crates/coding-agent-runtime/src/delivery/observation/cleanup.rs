use super::*;

impl DeliverySourceProvisioner {
    /// Re-proves the exact committed source through a cleanup-specific,
    /// unlocked-aware transient context. Unlike normal source recovery this
    /// deliberately permits a missing fixed lock, but it does not weaken the
    /// ordinary `DeliverySourceCapability` authentication path.
    ///
    /// Clean observations verify the persisted commit shape, source ref/HEAD,
    /// candidate index, committed-phase config/attributes baseline, absence of
    /// Git operation state, and a stable no-follow worktree fingerprint. Dirty
    /// observations retain the exact control/security/topology proof without
    /// requiring the intentionally changed index/worktree to match the clean
    /// baseline. All mismatch classes collapse to `Inconsistent`; unknown
    /// child/process outcomes remain typed errors for the cleanup state machine
    /// to reconcile.
    pub(in crate::delivery) async fn capture_committed_source_cleanup_proof(
        &self,
        present: &CleanupPresentAuthentication,
        reservation: &WorktreeReservation,
        source_intent: &DeliverySourceRecoveryIntent,
        cancellation: CancellationToken,
    ) -> Result<DeliveryCommittedSourceCleanupProof, DeliveryCommittedSourceCleanupCaptureError>
    {
        self.require_cleanup_source_binding(present, reservation, source_intent)?;
        let scene = self
            .observe_stable_committed_source_cleanup_scene(
                present,
                reservation,
                source_intent,
                cancellation,
            )
            .await?;
        if !scene.status.is_empty() {
            committed_cleanup_diagnostic!("source_status_nonempty");
            return Err(DeliveryCommittedSourceCleanupCaptureError::Dirty);
        }
        if !scene.ignored_untracked.is_empty() {
            committed_cleanup_diagnostic!("source_ignored_untracked_nonempty");
            return Err(DeliveryCommittedSourceCleanupCaptureError::Dirty);
        }
        if scene.clean.is_none() {
            committed_cleanup_diagnostic!("source_clean_scene_absent");
            return Err(DeliverySourceError::SourceChanged.into());
        }
        Ok(DeliveryCommittedSourceCleanupProof { scene })
    }

    /// Rebinds a persisted cleanup phase to a stable authenticated source even
    /// when late user/runtime files make it dirty. The returned proof remains
    /// dirty-scene-specific: phase classifiers may authorize an exact unlock,
    /// but they reject or reconcile before constructing a remove command.
    pub(in crate::delivery) async fn capture_committed_source_cleanup_recovery_proof(
        &self,
        present: &CleanupPresentAuthentication,
        reservation: &WorktreeReservation,
        source_intent: &DeliverySourceRecoveryIntent,
        cancellation: CancellationToken,
    ) -> Result<DeliveryCommittedSourceCleanupProof, DeliveryCommittedSourceCleanupCaptureError>
    {
        self.require_cleanup_source_binding(present, reservation, source_intent)?;
        let scene = self
            .observe_stable_committed_source_cleanup_scene(
                present,
                reservation,
                source_intent,
                cancellation,
            )
            .await?;
        if scene.status.is_empty() && scene.ignored_untracked.is_empty() && scene.clean.is_none() {
            committed_cleanup_diagnostic!("source_recovery_clean_scene_absent");
            return Err(DeliverySourceError::SourceChanged.into());
        }
        Ok(DeliveryCommittedSourceCleanupProof { scene })
    }

    pub(in crate::delivery) async fn observe_committed_source_for_cleanup(
        &self,
        present: &CleanupPresentAuthentication,
        reservation: &WorktreeReservation,
        source_intent: &DeliverySourceRecoveryIntent,
        expected: &DeliveryCommittedSourceCleanupProof,
        cancellation: CancellationToken,
    ) -> Result<DeliveryCommittedSourceCleanupObservation, DeliverySourceError> {
        if self
            .require_cleanup_source_binding(present, reservation, source_intent)
            .is_err()
        {
            return Ok(DeliveryCommittedSourceCleanupObservation::Inconsistent);
        }

        let observation = async {
            let scene = self
                .observe_committed_source_for_cleanup_once(
                    present,
                    reservation,
                    source_intent,
                    cancellation,
                )
                .await?;
            let result = if scene.status.is_empty()
                && scene.ignored_untracked.is_empty()
                && scene.clean.is_some()
            {
                if scene == expected.scene {
                    DeliveryCommittedSourceCleanupObservation::ExactClean
                } else {
                    DeliveryCommittedSourceCleanupObservation::Inconsistent
                }
            } else {
                DeliveryCommittedSourceCleanupObservation::ExactDirty
            };
            Ok(result)
        }
        .await;

        match observation {
            Ok(observation) => Ok(observation),
            Err(error) if is_cleanup_observation_mismatch(error) => {
                Ok(DeliveryCommittedSourceCleanupObservation::Inconsistent)
            }
            Err(error) => Err(error),
        }
    }

    fn require_cleanup_source_binding(
        &self,
        present: &CleanupPresentAuthentication,
        reservation: &WorktreeReservation,
        source_intent: &DeliverySourceRecoveryIntent,
    ) -> Result<(), DeliverySourceError> {
        if source_intent.is_bound_to_cleanup_source(
            reservation,
            present.common_directory_identity(),
            present.admin_directory_identity(),
        ) {
            Ok(())
        } else {
            Err(DeliverySourceError::AuthenticationChanged)
        }
    }

    async fn observe_stable_committed_source_cleanup_scene(
        &self,
        present: &CleanupPresentAuthentication,
        reservation: &WorktreeReservation,
        source_intent: &DeliverySourceRecoveryIntent,
        cancellation: CancellationToken,
    ) -> Result<CommittedSourceCleanupScene, DeliverySourceError> {
        let first = self
            .observe_committed_source_for_cleanup_once(
                present,
                reservation,
                source_intent,
                cancellation.clone(),
            )
            .await?;
        let second = self
            .observe_committed_source_for_cleanup_once(
                present,
                reservation,
                source_intent,
                cancellation,
            )
            .await?;
        if first == second {
            Ok(second)
        } else {
            committed_cleanup_diagnostic!("source_scene_unstable");
            Err(DeliverySourceError::SourceChanged)
        }
    }

    async fn observe_committed_source_for_cleanup_once(
        &self,
        present: &CleanupPresentAuthentication,
        reservation: &WorktreeReservation,
        source_intent: &DeliverySourceRecoveryIntent,
        cancellation: CancellationToken,
    ) -> Result<CommittedSourceCleanupScene, DeliverySourceError> {
        require_not_cancelled(&cancellation)?;
        self.sandbox.revalidate()?;
        let context = present.source_command_context();
        let (repository_probe, commands) = self
            .repository_bound_source_commands(context, cancellation.clone())
            .await?;
        let real_index =
            commands.real_index_commands(&repository_probe, reservation.branch_name())?;
        let object_commands = commands.mutation_commands(&repository_probe)?;
        let candidate = DeliveryTreeOid::try_new(
            source_intent.candidate_tree_object_id(),
            repository_probe.object_format(),
        )
        .ok_or(DeliverySourceError::AuthenticationChanged)?;
        let expected = DeliveryCommitOid::try_new(
            source_intent
                .expected_source_commit_object_id()
                .ok_or(DeliverySourceError::AuthenticationChanged)?,
            repository_probe.object_format(),
        )
        .ok_or(DeliverySourceError::AuthenticationChanged)?;
        let base = DeliveryCommitOid::try_new(
            source_intent.base_commit_object_id(),
            repository_probe.object_format(),
        )
        .ok_or(DeliverySourceError::AuthenticationChanged)?;

        let security = GitSecuritySnapshot::capture_authenticated(context, self.limits)?;
        let admin_root = context
            .worktree_admin
            .capability
            .try_clone_root()
            .map_err(|_| DeliverySourceError::AuthenticationChanged)?;
        if has_in_progress_git_state(&admin_root)
            .map_err(|_| DeliverySourceError::AuthenticationChanged)?
        {
            committed_cleanup_diagnostic!("source_git_state_before");
            return Err(DeliverySourceError::SourceChanged);
        }
        self.require_control_state(
            &commands,
            expected.as_str(),
            reservation.branch_name(),
            cancellation.clone(),
        )
        .await?;
        source_commit::verify_existing_source_commit(
            source_commit::SourceCommitVerificationRequest {
                executor: &self.executor,
                commands: &object_commands,
                probe: &repository_probe,
                expected_commit: &expected,
                expected_tree: &candidate,
                expected_parent: &base,
                input: source_intent.input(),
                cancellation: cancellation.clone(),
                output_limit: self.limits.max_status_bytes(),
            },
        )
        .await?;

        let initial_status = self
            .executor
            .run(
                commands.status_porcelain_v2()?,
                cancellation.clone(),
                self.limits.max_status_bytes(),
            )
            .await?;
        let initial_ignored_untracked = self
            .executor
            .run(
                commands.ignored_untracked_paths()?,
                cancellation.clone(),
                self.limits.max_status_bytes(),
            )
            .await?;

        // A dirty source is a proven non-remove scene, not an authentication
        // failure. Only a status-clean source must reproduce the persisted
        // candidate path/attribute/fingerprint proof before another remove
        // attempt can be authorized.
        let clean = if initial_status.is_empty() && initial_ignored_untracked.is_empty() {
            let object_type = self
                .executor
                .run(
                    real_index.inspect_candidate_object_type(&candidate)?,
                    cancellation.clone(),
                    EXACT_GIT_OBJECT_TYPE_OUTPUT_LIMIT,
                )
                .await?;
            require_exact_tree_object_type(&object_type)?;
            let tracked_output = self
                .executor
                .run(
                    commands.index_entries()?,
                    cancellation.clone(),
                    self.limits.max_status_bytes(),
                )
                .await?;
            let tracked_paths = parse_delivery_tracked_paths(
                &tracked_output,
                self.limits.max_paths(),
                repository_probe.object_format().hexadecimal_length(),
            )?;
            let attributes = self
                .require_safe_attributes_for_paths(
                    &commands,
                    &tracked_paths,
                    &security,
                    cancellation.clone(),
                )
                .await?;
            let fingerprint = WorkspaceFingerprinter::collect_delivery(
                self.executor.supervisor(),
                &commands,
                Arc::clone(&context.worktree.execution),
                self.fingerprint_limits,
                self.limits.max_status_bytes(),
                repository_probe.object_format().hexadecimal_length(),
                cancellation.clone(),
            )
            .await?;
            // The approved fingerprint and attribute digest describe the
            // reviewed pre-commit path/index domain. A committed source can
            // legitimately change an entry from untracked to tracked or
            // remove a tracked path. Keep the current values in the repeated
            // cleanup scene instead of comparing across those phases.
            Some(CommittedSourceCleanupCleanScene {
                tracked_paths,
                attributes,
                fingerprint: fingerprint.fingerprint,
                fingerprint_paths: fingerprint.paths,
            })
        } else {
            None
        };
        let final_security = GitSecuritySnapshot::capture_authenticated(context, self.limits)?;
        require_same_security_snapshot(&security, &final_security)?;
        let final_status = self
            .executor
            .run(
                commands.status_porcelain_v2()?,
                cancellation.clone(),
                self.limits.max_status_bytes(),
            )
            .await?;
        let final_ignored_untracked = self
            .executor
            .run(
                commands.ignored_untracked_paths()?,
                cancellation.clone(),
                self.limits.max_status_bytes(),
            )
            .await?;
        if initial_status != final_status || initial_ignored_untracked != final_ignored_untracked {
            committed_cleanup_diagnostic!("source_status_unstable");
            return Err(DeliverySourceError::SourceChanged);
        }
        if has_in_progress_git_state(&admin_root)
            .map_err(|_| DeliverySourceError::AuthenticationChanged)?
        {
            committed_cleanup_diagnostic!("source_git_state_after");
            return Err(DeliverySourceError::SourceChanged);
        }
        self.require_repository_object_format(&commands, &repository_probe, cancellation)
            .await?;
        self.sandbox.revalidate()?;

        Ok(CommittedSourceCleanupScene {
            security,
            status: final_status,
            ignored_untracked: final_ignored_untracked,
            clean,
        })
    }
}
