use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::process_supervisor::{PlatformEnvironment, ProcessLimits};
use crate::root_capability::DurableDirectoryIdentityV1;
use crate::{
    ProcessLivenessScope, RegisteredCheckoutAuthentication, RegisteredCheckoutAuthenticator,
    RootCapability, WorktreeError, WorktreeProvisioner,
};

use super::command::{DeliveryTargetMutationCommands, DeliveryTargetReadCommands};
use super::config::GitSecuritySnapshot;
use super::git_state::has_in_progress_git_state;
use super::observation::DeliveryCommandExecutor;
use super::recovery::{
    DeliveryTargetRecoveryBindingOutcome, DeliveryTargetRecoveryCapability,
    DeliveryTargetRecoveryIntent,
};
use super::sandbox::DeliveryCommandSandbox;
use super::{
    DeliveryCommitOid, DeliveryGitObjectFormat, DeliveryPersistedTargetRecovery,
    DeliverySourceError, DeliverySourceLimits, DeliveryTargetError, DeliveryTargetRequest,
    ProbedDeliveryGit,
};

mod conflict;

pub(super) use conflict::StableMergeConflictObservation;

const MAX_ATTRIBUTE_INPUT_BYTES: usize = 64 * 1024;

macro_rules! target_recovery_diagnostic {
    ($predicate:literal) => {
        #[cfg(feature = "test-support")]
        eprintln!(
            "test-support delivery target recovery binding rejected: predicate={}",
            $predicate
        );
    };
}

/// Read-only entry point for one registered primary checkout.
///
/// This provisioner never accepts a checkout path, `git-dir`, ref expression,
/// or command argument from its caller. Its target authority comes only from
/// the registered `WorktreeProvisioner` binding, then passes the primary
/// checkout's independent A/B authentication boundary.
pub struct DeliveryTargetProvisioner {
    probe: Arc<ProbedDeliveryGit>,
    authenticator: RegisteredCheckoutAuthenticator,
    sandbox: Arc<DeliveryCommandSandbox>,
    platform: PlatformEnvironment,
    executor: DeliveryCommandExecutor,
    limits: DeliverySourceLimits,
    #[cfg(feature = "test-support")]
    actual_merge_boundary_hook: Option<Arc<dyn Fn(&'static str) + Send + Sync + 'static>>,
    #[cfg(feature = "test-support")]
    registered_observation_boundary_hook: Option<Arc<dyn Fn(&'static str) + Send + Sync + 'static>>,
}

impl DeliveryTargetProvisioner {
    /// Builds the target observer from the exact registered repository
    /// binding. `temporary_directory` must designate the same retained private
    /// runtime directory that was used for the delivery Git probe; this keeps
    /// platform process setup outside both the target checkout and its Git
    /// administration directory.
    pub fn from_worktree_provisioner(
        worktrees: &WorktreeProvisioner,
        probe: Arc<ProbedDeliveryGit>,
        temporary_directory: impl AsRef<Path>,
        process_liveness_scope: ProcessLivenessScope,
        process_limits: ProcessLimits,
        limits: DeliverySourceLimits,
    ) -> Result<Self, DeliveryTargetError> {
        probe
            .verify_current_executable()
            .map_err(|_| DeliveryTargetError::AuthenticationChanged)?;
        let authenticator = worktrees
            .registered_checkout_authenticator(probe.pinned_executable())
            .map_err(map_worktree_error)?;
        let (temporary_path, temporary) =
            authenticated_temporary_directory(temporary_directory.as_ref())?;
        if !temporary.has_same_identity(probe.private_runtime()) {
            return Err(DeliveryTargetError::AuthenticationChanged);
        }
        let platform = delivery_platform_environment(temporary_path)?;
        let sandbox = Arc::new(
            DeliveryCommandSandbox::create(Arc::clone(probe.private_runtime()))
                .map_err(map_source_error)?,
        );
        sandbox.revalidate().map_err(map_source_error)?;
        Ok(Self {
            probe,
            authenticator,
            sandbox,
            platform,
            executor: DeliveryCommandExecutor::new(process_limits, process_liveness_scope),
            limits,
            #[cfg(feature = "test-support")]
            actual_merge_boundary_hook: None,
            #[cfg(feature = "test-support")]
            registered_observation_boundary_hook: None,
        })
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn set_actual_merge_boundary_hook_for_tests(
        &mut self,
        hook: impl Fn(&'static str) + Send + Sync + 'static,
    ) {
        self.actual_merge_boundary_hook = Some(Arc::new(hook));
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn set_registered_observation_boundary_hook_for_tests(
        &mut self,
        hook: impl Fn(&'static str) + Send + Sync + 'static,
    ) {
        self.registered_observation_boundary_hook = Some(Arc::new(hook));
    }

    /// Discovers and authenticates the registered primary checkout without
    /// accepting a caller-selected branch or object ID.
    ///
    /// The symbolic branch and raw `HEAD` are read through fixed commands
    /// after the retained checkout A boundary. The complete clean/config/
    /// attributes/object-format proof is then repeated around those exact
    /// discovered values before the closing B boundary. The returned wrapper
    /// contains an opaque target capability and redacted scalar accessors; it
    /// exposes no checkout path, Git command, or filesystem authority.
    pub async fn observe_registered_delivery_target(
        &self,
        cancellation: CancellationToken,
    ) -> Result<RegisteredDeliveryTargetObservation, DeliveryTargetError> {
        require_not_cancelled(&cancellation)?;
        let (authentication, security) = self.authenticate_target()?;
        let (repository_probe, commands) = self
            .repository_bound_target_commands(&authentication, cancellation.clone())
            .await?;
        let (branch_name, head) = self
            .discover_registered_target_identity(&commands, cancellation.clone())
            .await?;
        self.run_registered_observation_boundary_hook("after-identity-discovery");
        let attributes_digest = self
            .observe_target(
                &commands,
                &authentication,
                &branch_name,
                &head,
                &security,
                cancellation.clone(),
            )
            .await?;
        self.require_repository_object_format(&commands, &repository_probe, cancellation)
            .await?;
        self.finalize_authentication(&authentication, &security)?;
        Ok(RegisteredDeliveryTargetObservation::new(
            DeliveryTargetCapability::new(
                DeliveryTargetIdentityContext {
                    branch_name,
                    head,
                    config_attributes_digest: attributes_digest,
                    security_digest: *security.digest(),
                },
                DeliveryTargetRuntimeContext {
                    probe: repository_probe,
                    authentication,
                    commands,
                    sandbox: Arc::clone(&self.sandbox),
                },
            ),
        ))
    }

    /// Authenticates the registered primary checkout and proves its exact
    /// target branch, expected HEAD, clean index/worktree, inactive Git
    /// operation state, and safe config/attributes view. This is observation
    /// only: it does not update a ref, index, worktree file, or checkout.
    pub async fn open_delivery_target(
        &self,
        request: &DeliveryTargetRequest,
        cancellation: CancellationToken,
    ) -> Result<DeliveryTargetCapability, DeliveryTargetError> {
        require_not_cancelled(&cancellation)?;
        let (authentication, security) = self.authenticate_target()?;
        let (repository_probe, commands) = self
            .repository_bound_target_commands(&authentication, cancellation.clone())
            .await?;
        let expected_head =
            DeliveryCommitOid::try_new(request.expected_head(), repository_probe.object_format())
                .ok_or(DeliveryTargetError::InvalidRequest)?;
        let attributes_digest = self
            .observe_target(
                &commands,
                &authentication,
                request.branch_name(),
                &expected_head,
                &security,
                cancellation.clone(),
            )
            .await?;
        self.require_repository_object_format(&commands, &repository_probe, cancellation.clone())
            .await?;
        self.finalize_authentication(&authentication, &security)?;
        Ok(DeliveryTargetCapability::new(
            DeliveryTargetIdentityContext {
                branch_name: request.branch_name().to_owned(),
                head: expected_head,
                config_attributes_digest: attributes_digest,
                security_digest: *security.digest(),
            },
            DeliveryTargetRuntimeContext {
                probe: repository_probe,
                authentication,
                commands,
                sandbox: Arc::clone(&self.sandbox),
            },
        ))
    }

    /// Rebinds a persisted pre-mutation target identity to a fresh registered
    /// checkout authentication without assuming which pending scene is live.
    ///
    /// Unlike [`Self::open_delivery_target`], this boundary intentionally does
    /// not require old-HEAD/clean state. The phase-specific recovery
    /// classifier immediately proves exactly one of old-clean,
    /// expected-applied, or exact-conflict. Common Git identity and raw
    /// security provenance must already match here, before any Git command is
    /// made available to that classifier.
    pub async fn open_delivery_target_for_recovery(
        &self,
        intent: &DeliveryTargetRecoveryIntent,
        cancellation: CancellationToken,
    ) -> Result<DeliveryTargetRecoveryCapability, DeliveryTargetError> {
        require_not_cancelled(&cancellation)?;
        let (authentication, security) = self.authenticate_target()?;
        if authentication.command_context().common_directory_identity() != intent.common_identity()
        {
            return Err(DeliveryTargetError::AuthenticationChanged);
        }
        if security.digest() != intent.security_digest() {
            return Err(DeliveryTargetError::UnsafeGitConfiguration);
        }
        let (repository_probe, commands) = self
            .repository_bound_target_commands(&authentication, cancellation.clone())
            .await?;
        let old_head = DeliveryCommitOid::try_new(
            intent.old_head().as_str(),
            repository_probe.object_format(),
        )
        .ok_or(DeliveryTargetError::AuthenticationChanged)?;
        self.require_repository_object_format(&commands, &repository_probe, cancellation.clone())
            .await?;
        self.finalize_authentication(&authentication, &security)?;
        Ok(DeliveryTargetRecoveryCapability::from_bound(
            DeliveryTargetCapability::new(
                DeliveryTargetIdentityContext {
                    branch_name: intent.branch_name().to_owned(),
                    head: old_head,
                    config_attributes_digest: *intent.config_attributes_digest(),
                    security_digest: *intent.security_digest(),
                },
                DeliveryTargetRuntimeContext {
                    probe: repository_probe,
                    authentication,
                    commands,
                    sandbox: Arc::clone(&self.sandbox),
                },
            ),
        ))
    }

    /// Binds inert persisted target facts through a fresh registered-checkout
    /// authentication. The persisted target config/security values remain the
    /// old baseline used by phase classifiers; they are never replaced by a
    /// new observation during reconstruction.
    ///
    /// Config/attributes is intentionally not recomputed at this boundary:
    /// an already-applied merge or exact conflict can change the tracked path
    /// set that participates in that digest. The old-clean classifier compares
    /// the persisted digest exactly, while applied/conflict classifiers close
    /// their own object, path, attribute, and checkout scenes. Raw security is
    /// path-set independent and therefore must match during this bind.
    pub async fn bind_persisted_delivery_target_recovery(
        &self,
        persisted: &DeliveryPersistedTargetRecovery,
        cancellation: CancellationToken,
    ) -> Result<DeliveryTargetRecoveryBindingOutcome, DeliveryTargetError> {
        require_not_cancelled(&cancellation)?;
        let (authentication, security) = match self.authenticate_target() {
            Ok(value) => value,
            Err(error) if is_target_recovery_mismatch(error) => {
                target_recovery_diagnostic!("target_authentication");
                return Ok(DeliveryTargetRecoveryBindingOutcome::ReconciliationRequired);
            }
            Err(error) => return Err(error),
        };
        if authentication
            .command_context()
            .common_directory_identity()
            .digest()
            != persisted.common_git_identity_digest()
        {
            target_recovery_diagnostic!("target_common_identity");
            return Ok(DeliveryTargetRecoveryBindingOutcome::ReconciliationRequired);
        }
        if security.digest() != persisted.target_security_digest() {
            target_recovery_diagnostic!("target_security_digest");
            return Ok(DeliveryTargetRecoveryBindingOutcome::ReconciliationRequired);
        }
        let (repository_probe, commands) = match self
            .repository_bound_target_commands(&authentication, cancellation.clone())
            .await
        {
            Ok(value) => value,
            Err(error) if is_target_recovery_mismatch(error) => {
                target_recovery_diagnostic!("target_repository_commands");
                return Ok(DeliveryTargetRecoveryBindingOutcome::ReconciliationRequired);
            }
            Err(error) => return Err(error),
        };
        if persisted.object_format() != repository_probe.object_format() {
            target_recovery_diagnostic!("target_object_format");
            return Ok(DeliveryTargetRecoveryBindingOutcome::ReconciliationRequired);
        }
        if let Err(error) = self
            .require_repository_object_format(&commands, &repository_probe, cancellation.clone())
            .await
        {
            return if is_target_recovery_mismatch(error) {
                target_recovery_diagnostic!("target_repository_object_format");
                Ok(DeliveryTargetRecoveryBindingOutcome::ReconciliationRequired)
            } else {
                Err(error)
            };
        }
        if let Err(error) = self.finalize_authentication(&authentication, &security) {
            return if is_target_recovery_mismatch(error) {
                target_recovery_diagnostic!("target_finalize_authentication");
                Ok(DeliveryTargetRecoveryBindingOutcome::ReconciliationRequired)
            } else {
                Err(error)
            };
        }
        Ok(DeliveryTargetRecoveryBindingOutcome::Bound(Box::new(
            DeliveryTargetRecoveryCapability::from_bound(DeliveryTargetCapability::new(
                DeliveryTargetIdentityContext {
                    branch_name: persisted.branch_name().to_owned(),
                    head: persisted.old_head().clone(),
                    config_attributes_digest: *persisted.target_config_attributes_digest(),
                    security_digest: *persisted.target_security_digest(),
                },
                DeliveryTargetRuntimeContext {
                    probe: repository_probe,
                    authentication,
                    commands,
                    sandbox: Arc::clone(&self.sandbox),
                },
            )),
        )))
    }

    /// Repeats the read-only target proof for a capability created by this
    /// provisioner. Later preflight and merge stages use this boundary rather
    /// than extending the lifetime of an earlier observation.
    pub async fn revalidate_delivery_target(
        &self,
        target: &DeliveryTargetCapability,
        cancellation: CancellationToken,
    ) -> Result<(), DeliveryTargetError> {
        require_not_cancelled(&cancellation)?;
        self.require_capability_binding(target)?;
        target.sandbox().revalidate().map_err(map_source_error)?;
        target
            .authentication()
            .reauthenticate()
            .map_err(map_worktree_error)?;
        let security = self.capture_security(target.authentication())?;
        let repeated = self.capture_security(target.authentication())?;
        require_same_security_snapshot(&security, &repeated)?;
        let attributes_digest = self
            .observe_target(
                target.commands(),
                target.authentication(),
                target.branch_name(),
                target.head(),
                &security,
                cancellation.clone(),
            )
            .await?;
        if attributes_digest != *target.config_attributes_digest() {
            return Err(DeliveryTargetError::TargetWorktreeDirty);
        }
        self.require_repository_object_format(target.commands(), target.probe(), cancellation)
            .await?;
        self.finalize_authentication(target.authentication(), &security)
    }

    /// Re-proves the registered checkout after an actual merge has advanced
    /// its authenticated branch from the capability's old HEAD to one exact,
    /// already-verified expected commit.  This is intentionally distinct from
    /// [`Self::revalidate_delivery_target`]: the latter proves the pre-merge
    /// state and must never be weakened to accept a changed HEAD.
    pub(super) async fn revalidate_applied_delivery_target(
        &self,
        target: &DeliveryTargetCapability,
        expected_head: &DeliveryCommitOid,
        cancellation: CancellationToken,
    ) -> Result<(), DeliveryTargetError> {
        require_not_cancelled(&cancellation)?;
        self.require_capability_binding(target)?;
        target.sandbox().revalidate().map_err(map_source_error)?;
        target
            .authentication()
            .reauthenticate()
            .map_err(map_worktree_error)?;
        let security = self.capture_security(target.authentication())?;
        let repeated = self.capture_security(target.authentication())?;
        require_same_security_snapshot(&security, &repeated)?;
        if security.digest() != target.security_digest() {
            return Err(DeliveryTargetError::UnsafeGitConfiguration);
        }
        self.observe_target(
            target.commands(),
            target.authentication(),
            target.branch_name(),
            expected_head,
            &security,
            cancellation.clone(),
        )
        .await?;
        self.require_repository_object_format(target.commands(), target.probe(), cancellation)
            .await?;
        self.finalize_authentication(target.authentication(), &security)
    }

    /// Authenticates the exact pre-merge target and checks every path that a
    /// fresh object-only merge would write.  This deliberately does not reuse
    /// a preflight's old path listing: callers must supply the just-observed
    /// fixed write set and this method repeats authentication around the
    /// attribute lookup before an actual merge child can be spawned.
    pub(super) async fn require_safe_merge_write_set_attributes(
        &self,
        target: &DeliveryTargetCapability,
        paths: &[Vec<u8>],
        cancellation: CancellationToken,
    ) -> Result<(), DeliveryTargetError> {
        require_not_cancelled(&cancellation)?;
        self.require_capability_binding(target)?;
        target.sandbox().revalidate().map_err(map_source_error)?;
        target
            .authentication()
            .reauthenticate()
            .map_err(map_worktree_error)?;
        let security = self.capture_security(target.authentication())?;
        let repeated = self.capture_security(target.authentication())?;
        require_same_security_snapshot(&security, &repeated)?;
        if security.digest() != target.security_digest() {
            return Err(DeliveryTargetError::UnsafeGitConfiguration);
        }
        self.require_safe_attribute_paths(target.commands(), &security, paths, cancellation)
            .await?;
        self.finalize_authentication(target.authentication(), &security)
    }

    #[allow(dead_code)]
    pub(super) const fn executor(&self) -> &DeliveryCommandExecutor {
        &self.executor
    }

    #[allow(dead_code)]
    pub(super) const fn limits(&self) -> DeliverySourceLimits {
        self.limits
    }

    fn authenticate_target(
        &self,
    ) -> Result<(RegisteredCheckoutAuthentication, GitSecuritySnapshot), DeliveryTargetError> {
        self.sandbox.revalidate().map_err(map_source_error)?;
        let authentication = self
            .authenticator
            .authenticate()
            .map_err(map_worktree_error)?;
        let security = self.capture_security(&authentication)?;
        let repeated = self.capture_security(&authentication)?;
        require_same_security_snapshot(&security, &repeated)?;
        Ok((authentication, security))
    }

    fn capture_security(
        &self,
        authentication: &RegisteredCheckoutAuthentication,
    ) -> Result<GitSecuritySnapshot, DeliveryTargetError> {
        let context = authentication.command_context();
        GitSecuritySnapshot::capture(
            &context.common_git.capability,
            &context.checkout_git.capability,
            self.limits,
        )
        .map_err(map_security_error)
    }

    async fn repository_bound_target_commands(
        &self,
        authentication: &RegisteredCheckoutAuthentication,
        cancellation: CancellationToken,
    ) -> Result<(Arc<ProbedDeliveryGit>, DeliveryTargetReadCommands), DeliveryTargetError> {
        let discovery_commands = DeliveryTargetReadCommands::try_new(
            &self.probe,
            authentication.command_context(),
            Arc::clone(&self.sandbox),
            &self.platform,
            self.limits.timeout(),
        )
        .map_err(map_source_error)?;
        let output = self
            .executor
            .run(
                discovery_commands
                    .repository_object_format()
                    .map_err(map_source_error)?,
                cancellation.clone(),
                self.limits.max_status_bytes(),
            )
            .await
            .map_err(map_source_error)?;
        let object_format = DeliveryGitObjectFormat::parse_exact_git_output(&output)
            .ok_or(DeliveryTargetError::CommandFailed)?;
        let repository_probe = Arc::new(
            self.probe
                .bind_repository_object_format(object_format)
                .map_err(|_| DeliveryTargetError::AuthenticationChanged)?,
        );
        let commands = DeliveryTargetReadCommands::try_new(
            &repository_probe,
            authentication.command_context(),
            Arc::clone(&self.sandbox),
            &self.platform,
            self.limits.timeout(),
        )
        .map_err(map_source_error)?;
        self.require_repository_object_format(&commands, &repository_probe, cancellation)
            .await?;
        Ok((repository_probe, commands))
    }

    async fn require_repository_object_format(
        &self,
        commands: &DeliveryTargetReadCommands,
        repository_probe: &ProbedDeliveryGit,
        cancellation: CancellationToken,
    ) -> Result<(), DeliveryTargetError> {
        let output = self
            .executor
            .run(
                commands
                    .repository_object_format()
                    .map_err(map_source_error)?,
                cancellation,
                self.limits.max_status_bytes(),
            )
            .await
            .map_err(map_source_error)?;
        if DeliveryGitObjectFormat::parse_exact_git_output(&output)
            == Some(repository_probe.object_format())
        {
            Ok(())
        } else {
            Err(DeliveryTargetError::AuthenticationChanged)
        }
    }

    async fn discover_registered_target_identity(
        &self,
        commands: &DeliveryTargetReadCommands,
        cancellation: CancellationToken,
    ) -> Result<(String, DeliveryCommitOid), DeliveryTargetError> {
        let symbolic = match self
            .executor
            .run(
                commands.symbolic_head().map_err(map_source_error)?,
                cancellation.clone(),
                self.limits.max_status_bytes(),
            )
            .await
        {
            Ok(output) => output,
            Err(DeliverySourceError::CommandFailed) => {
                return Err(DeliveryTargetError::TargetDetached);
            }
            Err(error) => return Err(map_source_error(error)),
        };
        let symbolic = parse_exact_line(&symbolic)?;
        let branch_name = symbolic
            .strip_prefix("refs/heads/")
            .ok_or(DeliveryTargetError::AuthenticationChanged)?;

        let raw_head = self
            .executor
            .run(
                commands.resolve_head().map_err(map_source_error)?,
                cancellation,
                self.limits.max_status_bytes(),
            )
            .await
            .map_err(map_source_error)?;
        let head = parse_target_commit(&raw_head, commands.object_format())?;

        // Reuse the public request validator only as a scalar grammar check.
        // No request value is accepted by this discovery boundary.
        DeliveryTargetRequest::try_new(branch_name, head.as_str())?;
        Ok((branch_name.to_owned(), head))
    }

    #[allow(clippy::too_many_arguments)]
    async fn observe_target(
        &self,
        commands: &DeliveryTargetReadCommands,
        authentication: &RegisteredCheckoutAuthentication,
        expected_branch: &str,
        expected_head: &DeliveryCommitOid,
        security: &GitSecuritySnapshot,
        cancellation: CancellationToken,
    ) -> Result<[u8; 32], DeliveryTargetError> {
        self.require_no_git_operation(authentication)?;
        self.require_control_state(
            commands,
            expected_branch,
            expected_head,
            cancellation.clone(),
        )
        .await?;
        self.require_clean_state(commands, cancellation.clone())
            .await?;
        let attributes_digest = self
            .require_safe_attributes(commands, security, cancellation.clone())
            .await?;

        // Repeating every observable condition makes the capability bind a
        // stable checkout, not merely a sequence whose beginning happened to
        // be clean. Any external change is rejected without a compensating
        // write to the user's checkout.
        self.require_no_git_operation(authentication)?;
        self.require_control_state(
            commands,
            expected_branch,
            expected_head,
            cancellation.clone(),
        )
        .await?;
        self.require_clean_state(commands, cancellation.clone())
            .await?;
        let final_attributes_digest = self
            .require_safe_attributes(commands, security, cancellation.clone())
            .await?;
        if attributes_digest != final_attributes_digest {
            return Err(DeliveryTargetError::TargetWorktreeDirty);
        }
        self.require_control_state(
            commands,
            expected_branch,
            expected_head,
            cancellation.clone(),
        )
        .await?;
        self.require_clean_state(commands, cancellation).await?;
        self.require_no_git_operation(authentication)?;
        Ok(attributes_digest)
    }

    async fn require_control_state(
        &self,
        commands: &DeliveryTargetReadCommands,
        expected_branch: &str,
        expected_head: &DeliveryCommitOid,
        cancellation: CancellationToken,
    ) -> Result<(), DeliveryTargetError> {
        let symbolic = match self
            .executor
            .run(
                commands.symbolic_head().map_err(map_source_error)?,
                cancellation.clone(),
                self.limits.max_status_bytes(),
            )
            .await
        {
            Ok(output) => output,
            // `symbolic-ref --quiet HEAD` uses its documented status 1 for a
            // detached HEAD. The typed command has no caller-selectable argv,
            // so treating an unproven symbolic reference as detached is the
            // conservative, zero-side-effect rejection.
            Err(DeliverySourceError::CommandFailed) => {
                return Err(DeliveryTargetError::TargetDetached);
            }
            Err(error) => return Err(map_source_error(error)),
        };
        let expected_symbolic = format!("refs/heads/{expected_branch}");
        if parse_exact_line(&symbolic).ok() != Some(expected_symbolic.as_str()) {
            return Err(DeliveryTargetError::TargetBranchMismatch);
        }

        let head = self
            .executor
            .run(
                commands.resolve_head().map_err(map_source_error)?,
                cancellation,
                self.limits.max_status_bytes(),
            )
            .await
            .map_err(map_source_error)?;
        let observed = parse_target_commit(&head, commands.object_format())?;
        if observed != *expected_head {
            return Err(DeliveryTargetError::TargetHeadChanged);
        }
        Ok(())
    }

    async fn require_clean_state(
        &self,
        commands: &DeliveryTargetReadCommands,
        cancellation: CancellationToken,
    ) -> Result<(), DeliveryTargetError> {
        let status = self
            .executor
            .run(
                commands.status_porcelain_v2().map_err(map_source_error)?,
                cancellation.clone(),
                self.limits.max_status_bytes(),
            )
            .await
            .map_err(map_source_error)?;
        if !status.is_empty() {
            return Err(DeliveryTargetError::TargetWorktreeDirty);
        }
        let unmerged = self
            .executor
            .run(
                commands.unmerged_entries().map_err(map_source_error)?,
                cancellation,
                self.limits.max_status_bytes(),
            )
            .await
            .map_err(map_source_error)?;
        if unmerged.is_empty() {
            Ok(())
        } else {
            Err(DeliveryTargetError::TargetWorktreeDirty)
        }
    }

    async fn require_safe_attributes(
        &self,
        commands: &DeliveryTargetReadCommands,
        security: &GitSecuritySnapshot,
        cancellation: CancellationToken,
    ) -> Result<[u8; 32], DeliveryTargetError> {
        let output = self
            .executor
            .run(
                commands.tracked_paths().map_err(map_source_error)?,
                cancellation.clone(),
                self.limits.max_status_bytes(),
            )
            .await
            .map_err(map_source_error)?;
        let paths = parse_target_paths(&output, self.limits.max_paths())?;
        self.require_safe_attribute_paths(commands, security, &paths, cancellation)
            .await
    }

    async fn require_safe_attribute_paths(
        &self,
        commands: &DeliveryTargetReadCommands,
        security: &GitSecuritySnapshot,
        paths: &[Vec<u8>],
        cancellation: CancellationToken,
    ) -> Result<[u8; 32], DeliveryTargetError> {
        if paths.len() > self.limits.max_paths() {
            return Err(DeliveryTargetError::BoundsExceeded);
        }
        let mut digest = security.config_attributes_digest_builder();
        for paths in attribute_path_chunks(paths)? {
            let output = self
                .executor
                .run(
                    commands
                        .check_attributes(paths)
                        .map_err(map_attribute_error)?,
                    cancellation.clone(),
                    self.limits.max_attributes_bytes(),
                )
                .await
                .map_err(map_attribute_error)?;
            digest
                .append_checked_attributes(&output, paths)
                .map_err(map_attribute_error)?;
        }
        Ok(digest.finish())
    }

    fn require_no_git_operation(
        &self,
        authentication: &RegisteredCheckoutAuthentication,
    ) -> Result<(), DeliveryTargetError> {
        require_no_active_git_operation(authentication)
    }

    fn require_capability_binding(
        &self,
        target: &DeliveryTargetCapability,
    ) -> Result<(), DeliveryTargetError> {
        if self.probe.shares_probed_authority_with(target.probe())
            && Arc::ptr_eq(&self.sandbox, target.sandbox())
        {
            Ok(())
        } else {
            Err(DeliveryTargetError::AuthenticationChanged)
        }
    }

    fn finalize_authentication(
        &self,
        authentication: &RegisteredCheckoutAuthentication,
        expected_security: &GitSecuritySnapshot,
    ) -> Result<(), DeliveryTargetError> {
        let final_security = self.capture_security(authentication)?;
        require_same_security_snapshot(expected_security, &final_security)?;
        authentication
            .reauthenticate()
            .map_err(map_worktree_error)?;
        self.sandbox.revalidate().map_err(map_source_error)
    }

    pub(super) fn run_actual_merge_boundary_hook(&self, phase: &'static str) {
        #[cfg(feature = "test-support")]
        if let Some(hook) = &self.actual_merge_boundary_hook {
            hook(phase);
        }
        #[cfg(not(feature = "test-support"))]
        let _ = phase;
    }

    fn run_registered_observation_boundary_hook(&self, phase: &'static str) {
        #[cfg(feature = "test-support")]
        if let Some(hook) = &self.registered_observation_boundary_hook {
            hook(phase);
        }
        #[cfg(not(feature = "test-support"))]
        let _ = phase;
    }
}

impl fmt::Debug for DeliveryTargetProvisioner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryTargetProvisioner(<opaque>)")
    }
}

/// Opaque proof that an exact registered primary checkout passed the target
/// observation boundary. It intentionally retains no mutable Git operation
/// and exposes only typed, read-only facts to the public API.
pub struct DeliveryTargetCapability {
    branch_name: String,
    head: DeliveryCommitOid,
    config_attributes_digest: [u8; 32],
    security_digest: [u8; 32],
    probe: Arc<ProbedDeliveryGit>,
    authentication: RegisteredCheckoutAuthentication,
    commands: DeliveryTargetReadCommands,
    sandbox: Arc<DeliveryCommandSandbox>,
}

/// Opaque result of registered-target discovery.
///
/// It is impossible to construct from a caller-provided branch or object ID.
/// The contained capability may be borrowed by the typed delivery runtime or
/// consumed after the application has copied the redacted scalar facts it
/// needs for persistence.
pub struct RegisteredDeliveryTargetObservation {
    capability: DeliveryTargetCapability,
}

impl RegisteredDeliveryTargetObservation {
    fn new(capability: DeliveryTargetCapability) -> Self {
        Self { capability }
    }

    pub fn branch_name(&self) -> &str {
        self.capability.branch_name()
    }

    pub fn head_id(&self) -> &str {
        self.capability.head_id()
    }

    pub fn object_format(&self) -> DeliveryGitObjectFormat {
        self.capability.probe.object_format()
    }

    pub const fn capability(&self) -> &DeliveryTargetCapability {
        &self.capability
    }

    pub fn into_capability(self) -> DeliveryTargetCapability {
        self.capability
    }
}

impl fmt::Debug for RegisteredDeliveryTargetObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RegisteredDeliveryTargetObservation(<opaque>)")
    }
}

struct DeliveryTargetIdentityContext {
    branch_name: String,
    head: DeliveryCommitOid,
    config_attributes_digest: [u8; 32],
    security_digest: [u8; 32],
}

struct DeliveryTargetRuntimeContext {
    probe: Arc<ProbedDeliveryGit>,
    authentication: RegisteredCheckoutAuthentication,
    commands: DeliveryTargetReadCommands,
    sandbox: Arc<DeliveryCommandSandbox>,
}

impl DeliveryTargetCapability {
    fn new(identity: DeliveryTargetIdentityContext, runtime: DeliveryTargetRuntimeContext) -> Self {
        Self {
            branch_name: identity.branch_name,
            head: identity.head,
            config_attributes_digest: identity.config_attributes_digest,
            security_digest: identity.security_digest,
            probe: runtime.probe,
            authentication: runtime.authentication,
            commands: runtime.commands,
            sandbox: runtime.sandbox,
        }
    }

    pub fn branch_name(&self) -> &str {
        &self.branch_name
    }

    /// The exact target commit authenticated at the closing B boundary.
    pub fn head_id(&self) -> &str {
        self.head.as_str()
    }

    pub(super) const fn config_attributes_digest(&self) -> &[u8; 32] {
        &self.config_attributes_digest
    }

    /// Raw local config and common `info/attributes` proof captured at the
    /// closing target-authentication boundary.  It remains delivery-private
    /// because callers must not serialize, display, or compare filesystem
    /// authority tokens themselves.
    pub(super) const fn security_digest(&self) -> &[u8; 32] {
        &self.security_digest
    }

    /// Opaque common-Git provenance used only by the delivery preflight pair
    /// check. It never enters a public result, diagnostic, or command.
    pub(super) const fn common_directory_identity(&self) -> &DurableDirectoryIdentityV1 {
        self.authentication
            .command_context()
            .common_directory_identity()
    }

    /// Returns a duplicated no-follow checkout-root capability only to other
    /// delivery runtime modules. It is used by the later collision scanner;
    /// no path or generic filesystem authority enters the public API.
    #[allow(dead_code)]
    pub(super) fn checkout_root(&self) -> Result<RootCapability, DeliveryTargetError> {
        self.authentication
            .reauthenticate()
            .map_err(map_worktree_error)?;
        self.authentication
            .command_context()
            .checkout
            .capability
            .try_clone_capability()
            .map_err(|_| DeliveryTargetError::AuthenticationChanged)
    }

    pub(super) const fn head(&self) -> &DeliveryCommitOid {
        &self.head
    }

    pub(super) const fn probe(&self) -> &Arc<ProbedDeliveryGit> {
        &self.probe
    }

    pub(super) const fn authentication(&self) -> &RegisteredCheckoutAuthentication {
        &self.authentication
    }

    pub(super) const fn commands(&self) -> &DeliveryTargetReadCommands {
        &self.commands
    }

    /// Re-proves the fixed no-active-operation condition used by branch
    /// cleanup recovery. A target capability was clean when captured, but a
    /// later `DeletePending` retry must not infer that MERGE_HEAD, rebase,
    /// sequencer, index-lock, or another Git control state is still absent.
    pub(super) fn require_no_git_operation_for_branch_cleanup(
        &self,
    ) -> Result<(), DeliveryTargetError> {
        self.authentication
            .reauthenticate()
            .map_err(map_worktree_error)?;
        require_no_active_git_operation(&self.authentication)
    }

    /// Re-proves the complete registered-checkout security boundary used by
    /// branch cleanup. A retained target capability alone is insufficient:
    /// repository config or common `info/attributes` may have changed since
    /// the target was opened. The repeated snapshots and closing
    /// authentication make this suitable for every A/B query boundary and
    /// immediately before constructing the ref-transaction command.
    pub(super) fn revalidate_branch_cleanup_security(
        &self,
        limits: DeliverySourceLimits,
    ) -> Result<(), DeliveryTargetError> {
        self.sandbox.revalidate().map_err(map_source_error)?;
        self.authentication
            .reauthenticate()
            .map_err(map_worktree_error)?;
        let context = self.authentication.command_context();
        let first = GitSecuritySnapshot::capture(
            &context.common_git.capability,
            &context.checkout_git.capability,
            limits,
        )
        .map_err(map_security_error)?;
        let repeated = GitSecuritySnapshot::capture(
            &context.common_git.capability,
            &context.checkout_git.capability,
            limits,
        )
        .map_err(map_security_error)?;
        require_same_security_snapshot(&first, &repeated)?;
        if first.digest() != self.security_digest() {
            return Err(DeliveryTargetError::UnsafeGitConfiguration);
        }
        self.require_no_git_operation_for_branch_cleanup()?;
        self.authentication
            .reauthenticate()
            .map_err(map_worktree_error)?;
        let closing = GitSecuritySnapshot::capture(
            &context.common_git.capability,
            &context.checkout_git.capability,
            limits,
        )
        .map_err(map_security_error)?;
        require_same_security_snapshot(&first, &closing)
    }

    /// Narrows the retained registered-checkout authority to the Task 14
    /// mutation vocabulary.  It has no path, ref, argv, or environment input
    /// and performs a fresh no-follow authentication before the command
    /// policy layer compares the retained `Arc` identities.
    pub(super) fn mutation_commands(
        &self,
    ) -> Result<DeliveryTargetMutationCommands, DeliveryTargetError> {
        self.authentication
            .reauthenticate()
            .map_err(map_worktree_error)?;
        self.commands
            .mutation_commands(&self.probe, self.authentication.command_context())
            .map_err(map_source_error)
    }

    pub(super) const fn sandbox(&self) -> &Arc<DeliveryCommandSandbox> {
        &self.sandbox
    }
}

impl fmt::Debug for DeliveryTargetCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryTargetCapability(<opaque>)")
    }
}

fn require_no_active_git_operation(
    authentication: &RegisteredCheckoutAuthentication,
) -> Result<(), DeliveryTargetError> {
    let root = authentication
        .command_context()
        .checkout_git
        .capability
        .try_clone_root()
        .map_err(|_| DeliveryTargetError::AuthenticationChanged)?;
    match has_in_progress_git_state(&root) {
        Ok(false) => Ok(()),
        Ok(true) => Err(DeliveryTargetError::TargetGitOperationInProgress),
        Err(_) => Err(DeliveryTargetError::AuthenticationChanged),
    }
}

fn authenticated_temporary_directory(
    path: &Path,
) -> Result<(PathBuf, Arc<crate::ExecutionDirectory>), DeliveryTargetError> {
    let original =
        crate::ExecutionDirectory::open(path).map_err(|_| DeliveryTargetError::InvalidLimits)?;
    let canonical = std::fs::canonicalize(path).map_err(|_| DeliveryTargetError::InvalidLimits)?;
    let canonical_directory = crate::ExecutionDirectory::open(&canonical)
        .map_err(|_| DeliveryTargetError::InvalidLimits)?;
    if !original.has_same_identity(&canonical_directory) {
        return Err(DeliveryTargetError::AuthenticationChanged);
    }
    Ok((canonical, Arc::new(canonical_directory)))
}

fn delivery_platform_environment(
    path: PathBuf,
) -> Result<PlatformEnvironment, DeliveryTargetError> {
    #[cfg(windows)]
    let system_root = std::env::var_os("SYSTEMROOT")
        .or_else(|| std::env::var_os("WINDIR"))
        .map(PathBuf::from);
    #[cfg(unix)]
    let system_root = None;
    PlatformEnvironment::try_new(path, system_root).map_err(|_| DeliveryTargetError::InvalidLimits)
}

fn require_not_cancelled(cancellation: &CancellationToken) -> Result<(), DeliveryTargetError> {
    if cancellation.is_cancelled() {
        Err(DeliveryTargetError::Cancelled)
    } else {
        Ok(())
    }
}

fn is_target_recovery_mismatch(error: DeliveryTargetError) -> bool {
    matches!(
        error,
        DeliveryTargetError::AuthenticationChanged
            | DeliveryTargetError::TargetDetached
            | DeliveryTargetError::TargetBranchMismatch
            | DeliveryTargetError::TargetHeadChanged
            | DeliveryTargetError::TargetWorktreeDirty
            | DeliveryTargetError::TargetIgnoredPathCollision
            | DeliveryTargetError::TargetGitOperationInProgress
            | DeliveryTargetError::UnsafeGitConfiguration
            | DeliveryTargetError::UnsupportedGitAttributes
            | DeliveryTargetError::CommandFailed
    )
}

fn require_same_security_snapshot(
    expected: &GitSecuritySnapshot,
    observed: &GitSecuritySnapshot,
) -> Result<(), DeliveryTargetError> {
    if expected == observed {
        Ok(())
    } else {
        Err(DeliveryTargetError::UnsafeGitConfiguration)
    }
}

fn parse_exact_line(output: &[u8]) -> Result<&str, DeliveryTargetError> {
    let mut value = output;
    if value.last() == Some(&b'\n') {
        value = &value[..value.len() - 1];
        if value.last() == Some(&b'\r') {
            value = &value[..value.len() - 1];
        }
    }
    if value.is_empty() || value.contains(&0) || value.contains(&b'\n') || value.contains(&b'\r') {
        return Err(DeliveryTargetError::AuthenticationChanged);
    }
    std::str::from_utf8(value).map_err(|_| DeliveryTargetError::AuthenticationChanged)
}

fn parse_target_commit(
    output: &[u8],
    object_format: super::DeliveryGitObjectFormat,
) -> Result<DeliveryCommitOid, DeliveryTargetError> {
    DeliveryCommitOid::try_new(parse_exact_line(output)?, object_format)
        .ok_or(DeliveryTargetError::AuthenticationChanged)
}

fn parse_target_paths(
    output: &[u8],
    max_paths: usize,
) -> Result<Vec<Vec<u8>>, DeliveryTargetError> {
    if output.is_empty() {
        return Ok(Vec::new());
    }
    if output.last() != Some(&0) {
        return Err(DeliveryTargetError::AuthenticationChanged);
    }
    let mut paths = Vec::new();
    let mut unique = BTreeSet::new();
    for raw_path in output[..output.len() - 1].split(|byte| *byte == 0) {
        validate_target_path(raw_path)?;
        if !unique.insert(raw_path) {
            return Err(DeliveryTargetError::AuthenticationChanged);
        }
        if paths.len() == max_paths {
            return Err(DeliveryTargetError::BoundsExceeded);
        }
        paths.push(raw_path.to_vec());
    }
    Ok(paths)
}

fn validate_target_path(path: &[u8]) -> Result<(), DeliveryTargetError> {
    if path.is_empty()
        || path.contains(&0)
        || matches!(path.first(), Some(b'/' | b'\\'))
        || path
            .split(|byte| matches!(byte, b'/' | b'\\'))
            .any(|component| {
                component.is_empty()
                    || component == b"."
                    || component == b".."
                    || component.eq_ignore_ascii_case(b".git")
            })
    {
        return Err(DeliveryTargetError::AuthenticationChanged);
    }
    #[cfg(windows)]
    {
        let path =
            std::str::from_utf8(path).map_err(|_| DeliveryTargetError::AuthenticationChanged)?;
        crate::RelativePath::parse(path.to_owned())
            .map_err(|_| DeliveryTargetError::AuthenticationChanged)?;
    }
    Ok(())
}

fn attribute_path_chunks(paths: &[Vec<u8>]) -> Result<Vec<&[Vec<u8>]>, DeliveryTargetError> {
    let mut chunks = Vec::new();
    let mut start = 0usize;
    let mut bytes = 0usize;
    for (index, path) in paths.iter().enumerate() {
        let framed = path
            .len()
            .checked_add(1)
            .ok_or(DeliveryTargetError::BoundsExceeded)?;
        if framed > MAX_ATTRIBUTE_INPUT_BYTES {
            return Err(DeliveryTargetError::BoundsExceeded);
        }
        if bytes != 0 && bytes.saturating_add(framed) > MAX_ATTRIBUTE_INPUT_BYTES {
            chunks.push(&paths[start..index]);
            start = index;
            bytes = 0;
        }
        bytes = bytes
            .checked_add(framed)
            .ok_or(DeliveryTargetError::BoundsExceeded)?;
    }
    if start < paths.len() {
        chunks.push(&paths[start..]);
    }
    Ok(chunks)
}

fn map_security_error(error: DeliverySourceError) -> DeliveryTargetError {
    match error {
        DeliverySourceError::UnsafeGitConfiguration => DeliveryTargetError::UnsafeGitConfiguration,
        other => map_source_error(other),
    }
}

fn map_attribute_error(error: DeliverySourceError) -> DeliveryTargetError {
    match error {
        DeliverySourceError::UnsafeGitConfiguration | DeliverySourceError::UnsafeIndex => {
            DeliveryTargetError::UnsupportedGitAttributes
        }
        other => map_source_error(other),
    }
}

fn map_source_error(error: DeliverySourceError) -> DeliveryTargetError {
    match error {
        DeliverySourceError::InvalidLimits | DeliverySourceError::InvalidEnvironment => {
            DeliveryTargetError::InvalidLimits
        }
        DeliverySourceError::Cancelled => DeliveryTargetError::Cancelled,
        DeliverySourceError::TimedOut => DeliveryTargetError::TimedOut,
        DeliverySourceError::BoundsExceeded => DeliveryTargetError::BoundsExceeded,
        DeliverySourceError::UnsafeGitConfiguration => DeliveryTargetError::UnsafeGitConfiguration,
        DeliverySourceError::CommandFailed => DeliveryTargetError::CommandFailed,
        DeliverySourceError::ChildOutcomeUnknown => DeliveryTargetError::ChildOutcomeUnknown,
        DeliverySourceError::ProcessCleanupUnproven
        | DeliverySourceError::SandboxCleanupUnproven => {
            DeliveryTargetError::ProcessCleanupUnproven
        }
        DeliverySourceError::AuthenticationChanged
        | DeliverySourceError::SourceChanged
        | DeliverySourceError::UnsafeIndex
        | DeliverySourceError::CommandPolicy
        | DeliverySourceError::SandboxUnavailable => DeliveryTargetError::AuthenticationChanged,
        DeliverySourceError::Internal => DeliveryTargetError::Internal,
    }
}

fn map_worktree_error(error: WorktreeError) -> DeliveryTargetError {
    match error {
        WorktreeError::CommandPolicy(error) => error.into(),
        WorktreeError::Process(error) => error.into(),
        WorktreeError::Cancelled => DeliveryTargetError::Cancelled,
        WorktreeError::TimedOut => DeliveryTargetError::TimedOut,
        WorktreeError::UnsafeGitConfiguration => DeliveryTargetError::UnsafeGitConfiguration,
        WorktreeError::CommonGitIdentityUnavailable
        | WorktreeError::CommonGitIdentityMismatch
        | WorktreeError::InvalidRepository
        | WorktreeError::LinkedMetadataInvalid
        | WorktreeError::PostconditionFailed
        | WorktreeError::InconsistentArtifact => DeliveryTargetError::AuthenticationChanged,
        WorktreeError::InvalidLimits | WorktreeError::InvalidEnvironment => {
            DeliveryTargetError::InvalidLimits
        }
        WorktreeError::Io(_) | WorktreeError::GitCommandFailed | WorktreeError::OutputInvalid => {
            DeliveryTargetError::CommandFailed
        }
        WorktreeError::InvalidIdentity
        | WorktreeError::InvalidReservation
        | WorktreeError::CargoWorkspaceOutsideRepository
        | WorktreeError::DestinationConflict
        | WorktreeError::ArtifactPathInvalid
        | WorktreeError::BranchConflict
        | WorktreeError::UnbornHead
        | WorktreeError::WorktreeContentChanged
        | WorktreeError::PartialCreation
        | WorktreeError::NestedWorkspaceMissing
        | WorktreeError::Cargo(_) => DeliveryTargetError::Internal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracked_path_parser_accepts_only_bounded_safe_nul_records() {
        assert_eq!(
            parse_target_paths(b"one.rs\0dir/two.rs\0", 2).unwrap(),
            vec![b"one.rs".to_vec(), b"dir/two.rs".to_vec()]
        );
        for output in [b"unterminated".as_slice(), b"../escape\0", b".git/config\0"] {
            assert!(parse_target_paths(output, 2).is_err());
        }
        assert_eq!(
            parse_target_paths(b"one.rs\0two.rs\0", 1),
            Err(DeliveryTargetError::BoundsExceeded)
        );
    }

    #[test]
    fn target_line_parser_rejects_multiline_and_non_utf8_output() {
        assert_eq!(
            parse_exact_line(b"refs/heads/main\n").unwrap(),
            "refs/heads/main"
        );
        assert!(parse_exact_line(b"first\nsecond\n").is_err());
        assert!(parse_exact_line(b"bad\xff\n").is_err());
    }

    #[test]
    fn attribute_chunks_never_exceed_the_exact_input_budget() {
        let paths = vec![vec![b'a'; 40_000], vec![b'b'; 40_000]];
        let chunks = attribute_path_chunks(&paths).unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0], &paths[0..1]);
        assert_eq!(chunks[1], &paths[1..2]);
    }
}
