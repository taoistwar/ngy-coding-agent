use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::command_policy::{
    CommandPolicyError, ExecutionDirectory, GitCommandBinding, PinnedExecutable, child_visible_path,
};
use crate::native_fs::{open_child_directory, read_directory_names};
use crate::root_capability::{DirectoryIdentityDomain, DurableDirectoryIdentityV1};
use crate::{DirectoryIdentityError, DirectoryIdentityMarker, RelativePath, RootCapability};

use super::super::{
    WorktreeError, WorktreeProvisioner, WorktreeReservation, parse_commit_output,
    relative_path_from_path,
};
use super::identity::{
    RetainedDirectory, map_common_identity_error, require_exact_git, require_execution_identity,
};
use super::linked::{LinkedWorktreeAuthentication, LinkedWorktreeCommandContext};
use super::metadata::{
    CleanupAdminLockState, admin_relative_path, cleanup_admin_identity_and_backlink_are_absent,
    cleanup_admin_identity_and_backlink_are_unique, read_admin_gitdir,
    validate_cleanup_present_metadata,
};

const ADMIN_NAMESPACE: &str = "worktrees";
const MAX_COMMON_GIT_SIBLINGS: usize = 4_096;

/// Authentication authority for the phase-changing worktree cleanup flow.
///
/// Unlike normal linked-worktree authentication, this type can observe the
/// exact same owned topology after the fixed lock marker is removed and after
/// the worktree/admin pair is removed. It never relaxes the normal
/// `LinkedWorktreeAuthentication` path.
pub(crate) struct CleanupWorktreeAuthenticator {
    git: Arc<PinnedExecutable>,
    repository_id: String,
    original_binding: GitCommandBinding,
    common_git_directory: PathBuf,
    common_git_identity: DirectoryIdentityMarker,
    common_git_capability: RootCapability,
    common_git_execution: Arc<ExecutionDirectory>,
    artifact_root: PathBuf,
    artifact_root_directory: Arc<ExecutionDirectory>,
    artifact_root_capability: RootCapability,
    artifact_root_identity: DirectoryIdentityMarker,
    cargo_workspace_offset: PathBuf,
}

impl fmt::Debug for CleanupWorktreeAuthenticator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CleanupWorktreeAuthenticator(<opaque>)")
    }
}

impl CleanupWorktreeAuthenticator {
    pub(crate) fn from_provisioner(
        provisioner: &WorktreeProvisioner,
        expected_git: &Arc<PinnedExecutable>,
    ) -> Result<Self, WorktreeError> {
        require_exact_git(&provisioner.git, expected_git)?;
        provisioner.validate_common_git_identity()?;
        provisioner
            .original_binding
            .revalidate()
            .map_err(WorktreeError::CommandPolicy)?;
        provisioner
            .artifact_root_directory
            .revalidate()
            .map_err(WorktreeError::CommandPolicy)?;

        let common_git_capability = provisioner
            .common_git_capability
            .try_clone_capability()
            .map_err(|_| WorktreeError::CommonGitIdentityUnavailable)?;
        let common_git_execution = Arc::clone(provisioner.original_binding.git_directory());
        require_execution_identity(
            &common_git_execution,
            provisioner.common_git_identity,
            WorktreeError::CommonGitIdentityMismatch,
        )?;

        let artifact_root_capability = provisioner
            .artifact_root_capability
            .try_clone_capability()
            .map_err(WorktreeError::Io)?;
        let artifact_root_identity = artifact_root_capability
            .identity_marker()
            .map_err(|_| WorktreeError::ArtifactPathInvalid)?;
        require_execution_identity(
            &provisioner.artifact_root_directory,
            artifact_root_identity,
            WorktreeError::ArtifactPathInvalid,
        )?;

        Ok(Self {
            git: Arc::clone(expected_git),
            repository_id: provisioner.repository_id.clone(),
            original_binding: provisioner.original_binding.clone(),
            common_git_directory: provisioner.common_git_directory.clone(),
            common_git_identity: provisioner.common_git_identity,
            common_git_capability,
            common_git_execution,
            artifact_root: provisioner.artifact_root.clone(),
            artifact_root_directory: Arc::clone(&provisioner.artifact_root_directory),
            artifact_root_capability,
            artifact_root_identity,
            cargo_workspace_offset: provisioner.cargo_workspace_offset.clone(),
        })
    }

    /// Captures the only cleanup origin admitted by Task 16: an already
    /// authenticated source with the fixed `codex-reserved` lock intact.
    pub(crate) fn capture_locked_identity(
        &self,
        reservation: &WorktreeReservation,
        source: LinkedWorktreeAuthentication,
    ) -> Result<CleanupTopologyIntentV1, WorktreeError> {
        self.validate_reservation(reservation)?;
        self.revalidate_origins()?;
        source.reauthenticate()?;

        let worktree_relative = relative_path_from_path(&reservation.identity.relative_path())?;
        if !source.cleanup_origin_matches(
            reservation,
            &self.git,
            &self.original_binding,
            self.artifact_root_identity,
            &worktree_relative,
        ) {
            return Err(WorktreeError::InvalidReservation);
        }

        let common_identity = DurableDirectoryIdentityV1::derive(
            &self.common_git_capability,
            DirectoryIdentityDomain::CommonGit,
        )
        .map_err(map_common_identity_error)?;
        if &common_identity != source.cleanup_common_identity() {
            return Err(WorktreeError::LinkedMetadataInvalid);
        }

        let admin_name = source.cleanup_admin_name().to_owned();
        let admin_relative = admin_relative_path(&admin_name)?;
        if &admin_relative != source.cleanup_admin_relative() {
            return Err(WorktreeError::LinkedMetadataInvalid);
        }
        let (admin_marker, admin_identity) = self.capture_admin_identity(&admin_relative)?;
        if admin_marker != source.cleanup_admin_marker()
            || &admin_identity != source.cleanup_admin_identity()
        {
            return Err(WorktreeError::LinkedMetadataInvalid);
        }
        let admin_namespace_identity =
            self.capture_unique_admin_namespace(reservation, admin_marker)?;
        let source_context = source.command_context();
        let lock_state = validate_cleanup_present_metadata(
            &source_context.worktree_admin.capability,
            &source_context.worktree.capability,
            &self.common_git_directory,
            reservation,
            &admin_name,
        )?;
        let (repeated_admin_marker, repeated_admin_identity) =
            self.capture_admin_identity(&admin_relative)?;
        if lock_state != CleanupAdminLockState::Locked
            || repeated_admin_marker != admin_marker
            || repeated_admin_identity != admin_identity
        {
            return Err(WorktreeError::LinkedMetadataInvalid);
        }

        let (parent_chain, worktree_identity) = self.capture_worktree_chain(&worktree_relative)?;
        if worktree_identity != source.cleanup_worktree_marker() {
            return Err(WorktreeError::LinkedMetadataInvalid);
        }

        let intent = CleanupTopologyIntentV1 {
            reservation: reservation.clone(),
            common_identity,
            artifact_root_identity: self.artifact_root_identity,
            parent_chain,
            worktree_relative,
            worktree_path: reservation.worktree_path().to_owned(),
            binding: CleanupTopologyBindingV1::Present(CleanupPresentTopologyBindingV1 {
                admin_namespace_identity,
                admin_name,
                admin_relative,
                admin_marker,
                admin_identity,
                worktree_identity,
            }),
        };
        // Consuming the linked authentication is intentional: its retained
        // worktree/admin ExecutionDirectories must be closed before an unlock
        // or non-force remove command is ever constructed.
        drop(source);
        Ok(intent)
    }

    /// Rebinds syntax-checked durable topology facts through the retained
    /// common-Git and artifact-root authorities. This adapter is deliberately
    /// inert: it authenticates only and cannot construct a cleanup command.
    ///
    /// A present source must reproduce the exact durable admin identity and
    /// backlink. An absent source must prove both the reserved worktree path
    /// and every possible one-level admin alias absent before an intent is
    /// returned. Fresh process-local markers describe only that authenticated
    /// restart observation; they are never accepted as persisted evidence.
    pub(crate) fn bind_persisted_identity(
        &self,
        reservation: &WorktreeReservation,
        expected_common_identity_digest: &[u8; 32],
        expected_admin_identity_digest: &[u8; 32],
    ) -> Result<CleanupTopologyIntentV1, WorktreeError> {
        self.validate_reservation(reservation)?;
        self.revalidate_origins()?;
        let common_identity = DurableDirectoryIdentityV1::derive(
            &self.common_git_capability,
            DirectoryIdentityDomain::CommonGit,
        )
        .map_err(map_common_identity_error)?;
        if common_identity.digest() != expected_common_identity_digest {
            return Err(WorktreeError::CommonGitIdentityMismatch);
        }

        let worktree_relative = relative_path_from_path(&reservation.identity.relative_path())?;
        let binding = match self.capture_worktree_chain(&worktree_relative) {
            Ok((parent_chain, worktree_identity)) => {
                let present = self.capture_persisted_present_binding(
                    reservation,
                    &worktree_relative,
                    worktree_identity,
                    expected_admin_identity_digest,
                )?;
                (parent_chain, CleanupTopologyBindingV1::Present(present))
            }
            Err(WorktreeError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                let parent_chain = self.capture_worktree_parent_chain(&worktree_relative)?;
                match self
                    .artifact_root_capability
                    .open_directory(&worktree_relative)
                {
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Ok(_) => return Err(WorktreeError::LinkedMetadataInvalid),
                    Err(error) => return Err(WorktreeError::Io(error)),
                }
                let admin_namespace_identity = self
                    .capture_persisted_absent_admin(reservation, expected_admin_identity_digest)?;
                (
                    parent_chain,
                    CleanupTopologyBindingV1::PersistedAbsent {
                        admin_namespace_identity,
                        expected_admin_identity_digest: *expected_admin_identity_digest,
                    },
                )
            }
            Err(error) => return Err(error),
        };
        let intent = CleanupTopologyIntentV1 {
            reservation: reservation.clone(),
            common_identity,
            artifact_root_identity: self.artifact_root_identity,
            parent_chain: binding.0,
            worktree_relative,
            worktree_path: reservation.worktree_path().to_owned(),
            binding: binding.1,
        };

        let exact = matches!(
            (&intent.binding, self.observe_topology(&intent)),
            (
                CleanupTopologyBindingV1::Present(_),
                CleanupTopologyObservation::Locked(_)
            ) | (
                CleanupTopologyBindingV1::Present(_),
                CleanupTopologyObservation::Unlocked(_)
            ) | (
                CleanupTopologyBindingV1::PersistedAbsent { .. },
                CleanupTopologyObservation::Absent(_),
            )
        );
        if exact {
            Ok(intent)
        } else {
            Err(WorktreeError::LinkedMetadataInvalid)
        }
    }

    /// Performs a fresh no-follow topology observation. A present result owns a
    /// short-lived source command context for closing read checks. Callers may
    /// retain it through fixed command construction, but must drop it or
    /// consume it with `into_target` before spawning an unlock or non-force
    /// remove child. An absent result retains only primary-checkout command
    /// authority.
    pub(crate) fn observe_topology(
        &self,
        intent: &CleanupTopologyIntentV1,
    ) -> CleanupTopologyObservation {
        match self.observe_topology_inner(intent) {
            Ok(kind @ TopologyKind::Locked) => self
                .capture_present_observation(intent, kind)
                .map(CleanupTopologyObservation::Locked)
                .unwrap_or_else(CleanupTopologyObservation::from_failure),
            Ok(kind @ TopologyKind::Unlocked) => self
                .capture_present_observation(intent, kind)
                .map(CleanupTopologyObservation::Unlocked)
                .unwrap_or_else(CleanupTopologyObservation::from_failure),
            Ok(TopologyKind::Absent) => CleanupTopologyObservation::Absent(
                CleanupAbsentAuthentication::from_observation(self, intent),
            ),
            Err(ObservationFailure::Inconsistent) => CleanupTopologyObservation::Inconsistent,
            Err(ObservationFailure::Unavailable) => CleanupTopologyObservation::Unavailable,
        }
    }

    fn capture_present_observation(
        &self,
        intent: &CleanupTopologyIntentV1,
        expected_kind: TopologyKind,
    ) -> Result<CleanupPresentAuthentication, ObservationFailure> {
        let CleanupTopologyBindingV1::Present(binding) = &intent.binding else {
            return Err(ObservationFailure::Inconsistent);
        };
        let context = self
            .open_source_command_context(intent, binding)
            .map_err(observation_worktree_error)?;
        if context.common_identity != intent.common_identity
            || context.admin_identity != binding.admin_identity
            || context.worktree_admin.marker != binding.admin_marker
            || context.worktree.marker != binding.worktree_identity
        {
            return Err(ObservationFailure::Inconsistent);
        }
        if self.observe_topology_inner(intent)? != expected_kind {
            return Err(ObservationFailure::Inconsistent);
        }
        Ok(CleanupPresentAuthentication::from_observation(
            self, intent, binding, context,
        ))
    }

    fn open_source_command_context(
        &self,
        intent: &CleanupTopologyIntentV1,
        binding: &CleanupPresentTopologyBindingV1,
    ) -> Result<LinkedWorktreeCommandContext, WorktreeError> {
        let common_git = RetainedDirectory::from_existing(
            Arc::clone(&self.common_git_execution),
            self.common_git_capability
                .try_clone_capability()
                .map_err(|_| WorktreeError::CommonGitIdentityUnavailable)?,
            self.common_git_identity,
            WorktreeError::CommonGitIdentityMismatch,
        )?;
        let admin_path = self
            .common_git_directory
            .join(ADMIN_NAMESPACE)
            .join(&binding.admin_name);
        let worktree_admin = RetainedDirectory::open_child(
            &self.common_git_capability,
            &binding.admin_relative,
            &admin_path,
        )?;
        let worktree = RetainedDirectory::open_child(
            &self.artifact_root_capability,
            &intent.worktree_relative,
            &intent.worktree_path,
        )?;
        LinkedWorktreeCommandContext::new(
            Arc::clone(&self.git),
            common_git,
            worktree_admin,
            worktree,
        )
    }

    fn observe_topology_inner(
        &self,
        intent: &CleanupTopologyIntentV1,
    ) -> Result<TopologyKind, ObservationFailure> {
        self.validate_intent(intent)?;
        self.revalidate_origins_for_observation()?;
        require_observed_durable_identity(
            &self.common_git_capability,
            DirectoryIdentityDomain::CommonGit,
            &intent.common_identity,
        )?;

        let first_worktree = self.observe_worktree(intent)?;
        let first_admin = self.observe_admin(intent)?;
        let second_worktree = self.observe_worktree(intent)?;
        let second_admin = self.observe_admin(intent)?;
        if first_worktree != second_worktree || first_admin != second_admin {
            return Err(ObservationFailure::Inconsistent);
        }
        classify_topology(second_worktree, second_admin).ok_or(ObservationFailure::Inconsistent)
    }

    fn observe_worktree(
        &self,
        intent: &CleanupTopologyIntentV1,
    ) -> Result<bool, ObservationFailure> {
        self.artifact_root_capability
            .require_identity(intent.artifact_root_identity)
            .map_err(observation_identity_error)?;

        for parent in &intent.parent_chain {
            let directory = self
                .artifact_root_capability
                .open_directory(&parent.relative)
                .map_err(observation_required_directory_error)?;
            let capability = RootCapability::from_authenticated_directory(directory)
                .map_err(observation_required_directory_error)?;
            capability
                .require_identity(parent.identity)
                .map_err(observation_identity_error)?;
        }

        let directory = match self
            .artifact_root_capability
            .open_directory(&intent.worktree_relative)
        {
            Ok(directory) => directory,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(observation_io_error(&error)),
        };
        let CleanupTopologyBindingV1::Present(binding) = &intent.binding else {
            return Err(ObservationFailure::Inconsistent);
        };
        let capability = RootCapability::from_authenticated_directory(directory)
            .map_err(|error| observation_io_error(&error))?;
        capability
            .require_identity(binding.worktree_identity)
            .map_err(observation_identity_error)?;
        Ok(true)
    }

    fn observe_admin(
        &self,
        intent: &CleanupTopologyIntentV1,
    ) -> Result<ObservedAdminState, ObservationFailure> {
        let namespace_relative = RelativePath::parse(ADMIN_NAMESPACE.to_owned())
            .map_err(|_| ObservationFailure::Inconsistent)?;
        let namespace = match self
            .common_git_capability
            .open_directory(&namespace_relative)
        {
            Ok(namespace) => namespace,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return self.observe_absent_admin_namespace(&namespace_relative, intent);
            }
            Err(error) => return Err(observation_io_error(&error)),
        };
        let namespace = RootCapability::from_authenticated_directory(namespace)
            .map_err(observation_required_directory_error)?;
        match &intent.binding {
            CleanupTopologyBindingV1::Present(binding) => namespace
                .require_identity(binding.admin_namespace_identity)
                .map_err(observation_identity_error)?,
            CleanupTopologyBindingV1::PersistedAbsent {
                admin_namespace_identity: Some(expected),
                ..
            } => namespace
                .require_identity(*expected)
                .map_err(observation_identity_error)?,
            CleanupTopologyBindingV1::PersistedAbsent {
                admin_namespace_identity: None,
                ..
            } => return Err(ObservationFailure::Inconsistent),
        }

        let CleanupTopologyBindingV1::Present(binding) = &intent.binding else {
            return self.observe_persisted_absent_admin(&namespace, intent);
        };

        let admin = match self
            .common_git_capability
            .open_directory(&binding.admin_relative)
        {
            Ok(admin) => admin,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return cleanup_admin_identity_and_backlink_are_absent(
                    &namespace,
                    &intent.reservation,
                    binding.admin_marker,
                )
                .map_err(observation_worktree_error)
                .and_then(|absent| {
                    if absent {
                        Ok(ObservedAdminState::AdminAbsent)
                    } else {
                        Err(ObservationFailure::Inconsistent)
                    }
                });
            }
            Err(error) => return Err(observation_io_error(&error)),
        };
        let admin = RootCapability::from_authenticated_directory(admin)
            .map_err(|error| observation_io_error(&error))?;
        admin
            .require_identity(binding.admin_marker)
            .map_err(observation_identity_error)?;
        require_observed_durable_identity(
            &admin,
            DirectoryIdentityDomain::WorktreeAdmin,
            &binding.admin_identity,
        )?;

        let worktree = self
            .artifact_root_capability
            .open_directory(&intent.worktree_relative)
            .map_err(observation_required_directory_error)?;
        let worktree = RootCapability::from_authenticated_directory(worktree)
            .map_err(observation_required_directory_error)?;
        worktree
            .require_identity(binding.worktree_identity)
            .map_err(observation_identity_error)?;
        if !cleanup_admin_identity_and_backlink_are_unique(
            &namespace,
            &intent.reservation,
            binding.admin_marker,
        )
        .map_err(observation_worktree_error)?
        {
            return Err(ObservationFailure::Inconsistent);
        }

        match validate_cleanup_present_metadata(
            &admin,
            &worktree,
            &self.common_git_directory,
            &intent.reservation,
            &binding.admin_name,
        )
        .map_err(observation_worktree_error)?
        {
            CleanupAdminLockState::Locked => Ok(ObservedAdminState::Locked),
            CleanupAdminLockState::Unlocked => Ok(ObservedAdminState::Unlocked),
        }
    }

    fn observe_absent_admin_namespace(
        &self,
        namespace_relative: &RelativePath,
        intent: &CleanupTopologyIntentV1,
    ) -> Result<ObservedAdminState, ObservationFailure> {
        if let CleanupTopologyBindingV1::PersistedAbsent {
            admin_namespace_identity: None,
            expected_admin_identity_digest,
        } = &intent.binding
        {
            if !self
                .persisted_admin_alias_is_absent(expected_admin_identity_digest)
                .map_err(observation_worktree_error)?
            {
                return Err(ObservationFailure::Inconsistent);
            }
            return match self
                .common_git_capability
                .open_directory(namespace_relative)
            {
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    Ok(ObservedAdminState::NamespaceAbsent)
                }
                Ok(_) => Err(ObservationFailure::Inconsistent),
                Err(error) => Err(observation_io_error(&error)),
            };
        }
        let CleanupTopologyBindingV1::Present(binding) = &intent.binding else {
            return Err(ObservationFailure::Inconsistent);
        };
        reject_renamed_admin_aliases(
            &self.common_git_capability,
            binding.admin_namespace_identity,
            binding.admin_marker,
        )?;

        // Close the scan by requiring the exact namespace path to remain
        // absent. A replacement or a renamed captured namespace moving back
        // into place is inconsistent, even if its directory identity differs.
        match self
            .common_git_capability
            .open_directory(namespace_relative)
        {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                Ok(ObservedAdminState::NamespaceAbsent)
            }
            Ok(_) => Err(ObservationFailure::Inconsistent),
            Err(error) => Err(observation_io_error(&error)),
        }
    }

    fn observe_persisted_absent_admin(
        &self,
        namespace: &RootCapability,
        intent: &CleanupTopologyIntentV1,
    ) -> Result<ObservedAdminState, ObservationFailure> {
        let CleanupTopologyBindingV1::PersistedAbsent {
            admin_namespace_identity: Some(namespace_identity),
            expected_admin_identity_digest,
        } = &intent.binding
        else {
            return Err(ObservationFailure::Inconsistent);
        };
        if !self
            .persisted_admin_namespace_is_absent(
                namespace,
                &intent.reservation,
                expected_admin_identity_digest,
            )
            .map_err(observation_worktree_error)?
            || !self
                .persisted_admin_alias_is_absent(expected_admin_identity_digest)
                .map_err(observation_worktree_error)?
        {
            return Err(ObservationFailure::Inconsistent);
        }
        let namespace_relative = RelativePath::parse(ADMIN_NAMESPACE.to_owned())
            .map_err(|_| ObservationFailure::Inconsistent)?;
        let repeated = self
            .common_git_capability
            .open_directory(&namespace_relative)
            .map_err(observation_required_directory_error)?;
        RootCapability::from_authenticated_directory(repeated)
            .map_err(observation_required_directory_error)?
            .require_identity(*namespace_identity)
            .map_err(observation_identity_error)?;
        Ok(ObservedAdminState::AdminAbsent)
    }

    fn capture_persisted_present_binding(
        &self,
        reservation: &WorktreeReservation,
        worktree_relative: &RelativePath,
        worktree_identity: DirectoryIdentityMarker,
        expected_admin_identity_digest: &[u8; 32],
    ) -> Result<CleanupPresentTopologyBindingV1, WorktreeError> {
        let namespace_relative = RelativePath::parse(ADMIN_NAMESPACE.to_owned())
            .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
        let namespace = self
            .common_git_capability
            .open_directory(&namespace_relative)
            .map_err(WorktreeError::Io)?;
        let namespace =
            RootCapability::from_authenticated_directory(namespace).map_err(WorktreeError::Io)?;
        let admin_namespace_identity = namespace
            .identity_marker()
            .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
        let mut root = namespace.try_clone_root().map_err(WorktreeError::Io)?;
        let entries =
            read_directory_names(&mut root, MAX_COMMON_GIT_SIBLINGS).map_err(WorktreeError::Io)?;
        let mut matched = None;
        for entry in entries {
            if entry == "." || entry == ".." {
                continue;
            }
            let directory = match open_child_directory(&root, &entry) {
                Ok(directory) => directory,
                Err(error) if entry_is_not_a_directory(&error) => {
                    return Err(WorktreeError::LinkedMetadataInvalid);
                }
                Err(error) => return Err(WorktreeError::Io(error)),
            };
            let admin = RootCapability::from_authenticated_directory(directory)
                .map_err(WorktreeError::Io)?;
            let durable =
                DurableDirectoryIdentityV1::derive(&admin, DirectoryIdentityDomain::WorktreeAdmin)
                    .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
            if durable.digest() != expected_admin_identity_digest {
                continue;
            }
            let name = entry
                .to_str()
                .filter(|name| !name.contains('/'))
                .ok_or(WorktreeError::LinkedMetadataInvalid)?;
            RelativePath::parse(name.to_owned())
                .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
            let marker = admin
                .identity_marker()
                .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
            if matched
                .replace((name.to_owned(), marker, durable))
                .is_some()
            {
                return Err(WorktreeError::LinkedMetadataInvalid);
            }
        }
        let (admin_name, admin_marker, admin_identity) =
            matched.ok_or(WorktreeError::LinkedMetadataInvalid)?;
        let admin_relative = admin_relative_path(&admin_name)?;
        let captured_namespace = self.capture_unique_admin_namespace(reservation, admin_marker)?;
        if captured_namespace != admin_namespace_identity {
            return Err(WorktreeError::LinkedMetadataInvalid);
        }
        let (repeated_admin_marker, repeated_admin_identity) =
            self.capture_admin_identity(&admin_relative)?;
        if repeated_admin_marker != admin_marker
            || repeated_admin_identity != admin_identity
            || repeated_admin_identity.digest() != expected_admin_identity_digest
        {
            return Err(WorktreeError::LinkedMetadataInvalid);
        }
        let admin = self
            .common_git_capability
            .open_directory(&admin_relative)
            .map_err(WorktreeError::Io)?;
        let admin =
            RootCapability::from_authenticated_directory(admin).map_err(WorktreeError::Io)?;
        admin
            .require_identity(admin_marker)
            .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
        let worktree = self
            .artifact_root_capability
            .open_directory(worktree_relative)
            .map_err(WorktreeError::Io)?;
        let worktree =
            RootCapability::from_authenticated_directory(worktree).map_err(WorktreeError::Io)?;
        worktree
            .require_identity(worktree_identity)
            .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
        validate_cleanup_present_metadata(
            &admin,
            &worktree,
            &self.common_git_directory,
            reservation,
            &admin_name,
        )?;
        Ok(CleanupPresentTopologyBindingV1 {
            admin_namespace_identity,
            admin_name,
            admin_relative,
            admin_marker,
            admin_identity,
            worktree_identity,
        })
    }

    fn capture_persisted_absent_admin(
        &self,
        reservation: &WorktreeReservation,
        expected_admin_identity_digest: &[u8; 32],
    ) -> Result<Option<DirectoryIdentityMarker>, WorktreeError> {
        let namespace_relative = RelativePath::parse(ADMIN_NAMESPACE.to_owned())
            .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
        let namespace = match self
            .common_git_capability
            .open_directory(&namespace_relative)
        {
            Ok(namespace) => namespace,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if !self.persisted_admin_alias_is_absent(expected_admin_identity_digest)? {
                    return Err(WorktreeError::LinkedMetadataInvalid);
                }
                return match self
                    .common_git_capability
                    .open_directory(&namespace_relative)
                {
                    Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
                    Ok(_) => Err(WorktreeError::LinkedMetadataInvalid),
                    Err(error) => Err(WorktreeError::Io(error)),
                };
            }
            Err(error) => return Err(WorktreeError::Io(error)),
        };
        let namespace =
            RootCapability::from_authenticated_directory(namespace).map_err(WorktreeError::Io)?;
        let namespace_identity = namespace
            .identity_marker()
            .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
        if !self.persisted_admin_namespace_is_absent(
            &namespace,
            reservation,
            expected_admin_identity_digest,
        )? || !self.persisted_admin_alias_is_absent(expected_admin_identity_digest)?
        {
            return Err(WorktreeError::LinkedMetadataInvalid);
        }
        let repeated = self
            .common_git_capability
            .open_directory(&namespace_relative)
            .map_err(WorktreeError::Io)?;
        RootCapability::from_authenticated_directory(repeated)
            .map_err(WorktreeError::Io)?
            .require_identity(namespace_identity)
            .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
        Ok(Some(namespace_identity))
    }

    fn persisted_admin_namespace_is_absent(
        &self,
        namespace: &RootCapability,
        reservation: &WorktreeReservation,
        expected_admin_identity_digest: &[u8; 32],
    ) -> Result<bool, WorktreeError> {
        let mut root = namespace.try_clone_root().map_err(WorktreeError::Io)?;
        let entries =
            read_directory_names(&mut root, MAX_COMMON_GIT_SIBLINGS).map_err(WorktreeError::Io)?;
        let expected_gitdir = child_visible_path(reservation.worktree_path()).join(".git");
        for entry in entries {
            if entry == "." || entry == ".." {
                continue;
            }
            let directory = match open_child_directory(&root, &entry) {
                Ok(directory) => directory,
                Err(error) if entry_is_not_a_directory(&error) => {
                    return Err(WorktreeError::LinkedMetadataInvalid);
                }
                Err(error) => return Err(WorktreeError::Io(error)),
            };
            let admin = RootCapability::from_authenticated_directory(directory)
                .map_err(WorktreeError::Io)?;
            let durable =
                DurableDirectoryIdentityV1::derive(&admin, DirectoryIdentityDomain::WorktreeAdmin)
                    .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
            if durable.digest() == expected_admin_identity_digest {
                return Ok(false);
            }
            let name = entry
                .to_str()
                .filter(|name| !name.contains('/'))
                .ok_or(WorktreeError::LinkedMetadataInvalid)?;
            RelativePath::parse(name.to_owned())
                .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
            if read_admin_gitdir(&self.common_git_capability, name)? == expected_gitdir {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn persisted_admin_alias_is_absent(
        &self,
        expected_admin_identity_digest: &[u8; 32],
    ) -> Result<bool, WorktreeError> {
        let scan = self
            .common_git_capability
            .try_clone_capability()
            .map_err(WorktreeError::Io)?;
        let mut root = scan.try_clone_root().map_err(WorktreeError::Io)?;
        let entries =
            read_directory_names(&mut root, MAX_COMMON_GIT_SIBLINGS).map_err(WorktreeError::Io)?;
        for entry in entries {
            if entry == "." || entry == ".." {
                continue;
            }
            let directory = match open_child_directory(&root, &entry) {
                Ok(directory) => directory,
                Err(error) if entry_is_not_a_directory(&error) => continue,
                Err(error) => return Err(WorktreeError::Io(error)),
            };
            let directory = RootCapability::from_authenticated_directory(directory)
                .map_err(WorktreeError::Io)?;
            if durable_admin_digest_matches(&directory, expected_admin_identity_digest)? {
                return Ok(false);
            }
            let mut child_root = directory.try_clone_root().map_err(WorktreeError::Io)?;
            let children = read_directory_names(&mut child_root, MAX_COMMON_GIT_SIBLINGS)
                .map_err(WorktreeError::Io)?;
            for child in children {
                if child == "." || child == ".." {
                    continue;
                }
                let nested = match open_child_directory(&child_root, &child) {
                    Ok(directory) => directory,
                    Err(error) if entry_is_not_a_directory(&error) => continue,
                    Err(error) => return Err(WorktreeError::Io(error)),
                };
                let nested = RootCapability::from_authenticated_directory(nested)
                    .map_err(WorktreeError::Io)?;
                if durable_admin_digest_matches(&nested, expected_admin_identity_digest)? {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    fn validate_reservation(&self, reservation: &WorktreeReservation) -> Result<(), WorktreeError> {
        if reservation.branch_name != reservation.identity.branch_name()
            || reservation.identity.repository_id() != self.repository_id
            || reservation.worktree_path
                != self
                    .artifact_root
                    .join(reservation.identity.relative_path())
            || reservation.source_common_git_identity != self.common_git_identity
            || reservation.cargo_workspace_offset != self.cargo_workspace_offset
            || parse_commit_output(reservation.base_commit.as_bytes()).is_err()
        {
            return Err(WorktreeError::InvalidReservation);
        }
        Ok(())
    }

    fn validate_intent(&self, intent: &CleanupTopologyIntentV1) -> Result<(), ObservationFailure> {
        self.validate_reservation(&intent.reservation)
            .map_err(|_| ObservationFailure::Inconsistent)?;
        let expected_relative =
            relative_path_from_path(&intent.reservation.identity.relative_path())
                .map_err(|_| ObservationFailure::Inconsistent)?;
        if intent.artifact_root_identity != self.artifact_root_identity
            || intent.worktree_relative != expected_relative
            || intent.worktree_path.as_path() != intent.reservation.worktree_path()
        {
            return Err(ObservationFailure::Inconsistent);
        }
        if let CleanupTopologyBindingV1::Present(binding) = &intent.binding {
            let expected_admin = admin_relative_path(&binding.admin_name)
                .map_err(|_| ObservationFailure::Inconsistent)?;
            if binding.admin_relative != expected_admin {
                return Err(ObservationFailure::Inconsistent);
            }
        }
        Ok(())
    }

    fn revalidate_origins(&self) -> Result<(), WorktreeError> {
        self.git
            .revalidate()
            .map_err(WorktreeError::CommandPolicy)?;
        self.common_git_capability
            .require_identity(self.common_git_identity)
            .map_err(map_common_identity_error)?;
        require_execution_identity(
            &self.common_git_execution,
            self.common_git_identity,
            WorktreeError::CommonGitIdentityMismatch,
        )?;
        self.original_binding
            .revalidate()
            .map_err(WorktreeError::CommandPolicy)?;
        self.artifact_root_capability
            .require_identity(self.artifact_root_identity)
            .map_err(|_| WorktreeError::ArtifactPathInvalid)?;
        require_execution_identity(
            &self.artifact_root_directory,
            self.artifact_root_identity,
            WorktreeError::ArtifactPathInvalid,
        )
    }

    fn revalidate_origins_for_observation(&self) -> Result<(), ObservationFailure> {
        self.git.revalidate().map_err(observation_command_error)?;
        self.common_git_capability
            .require_identity(self.common_git_identity)
            .map_err(observation_identity_error)?;
        require_execution_identity(
            &self.common_git_execution,
            self.common_git_identity,
            WorktreeError::CommonGitIdentityMismatch,
        )
        .map_err(observation_worktree_error)?;
        self.original_binding
            .revalidate()
            .map_err(observation_command_error)?;
        self.artifact_root_capability
            .require_identity(self.artifact_root_identity)
            .map_err(observation_identity_error)?;
        require_execution_identity(
            &self.artifact_root_directory,
            self.artifact_root_identity,
            WorktreeError::ArtifactPathInvalid,
        )
        .map_err(observation_worktree_error)
    }

    fn capture_unique_admin_namespace(
        &self,
        reservation: &WorktreeReservation,
        admin_marker: DirectoryIdentityMarker,
    ) -> Result<DirectoryIdentityMarker, WorktreeError> {
        let namespace = RelativePath::parse(ADMIN_NAMESPACE.to_owned())
            .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
        let directory = self
            .common_git_capability
            .open_directory(&namespace)
            .map_err(WorktreeError::Io)?;
        let capability =
            RootCapability::from_authenticated_directory(directory).map_err(WorktreeError::Io)?;
        let identity = capability
            .identity_marker()
            .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
        if !cleanup_admin_identity_and_backlink_are_unique(&capability, reservation, admin_marker)?
        {
            return Err(WorktreeError::LinkedMetadataInvalid);
        }
        let repeated = self
            .common_git_capability
            .open_directory(&namespace)
            .map_err(WorktreeError::Io)?;
        RootCapability::from_authenticated_directory(repeated)
            .map_err(WorktreeError::Io)?
            .require_identity(identity)
            .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
        Ok(identity)
    }

    fn capture_admin_identity(
        &self,
        relative: &RelativePath,
    ) -> Result<(DirectoryIdentityMarker, DurableDirectoryIdentityV1), WorktreeError> {
        let admin = self
            .common_git_capability
            .open_directory(relative)
            .map_err(WorktreeError::Io)?;
        let admin =
            RootCapability::from_authenticated_directory(admin).map_err(WorktreeError::Io)?;
        let marker = admin
            .identity_marker()
            .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
        let durable =
            DurableDirectoryIdentityV1::derive(&admin, DirectoryIdentityDomain::WorktreeAdmin)
                .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
        Ok((marker, durable))
    }

    fn capture_worktree_chain(
        &self,
        worktree_relative: &RelativePath,
    ) -> Result<(Vec<CleanupParentIdentity>, DirectoryIdentityMarker), WorktreeError> {
        let chain = self.capture_worktree_parent_chain(worktree_relative)?;
        let components = worktree_relative.components().collect::<Vec<_>>();
        let (leaf, _) = components
            .split_last()
            .ok_or(WorktreeError::InvalidIdentity)?;
        let mut relative =
            RelativePath::parse(String::new()).map_err(|_| WorktreeError::InvalidIdentity)?;
        for component in components.iter().take(components.len() - 1) {
            relative = relative
                .join_component(component)
                .map_err(|_| WorktreeError::InvalidIdentity)?;
        }
        relative = relative
            .join_component(leaf)
            .map_err(|_| WorktreeError::InvalidIdentity)?;
        if &relative != worktree_relative {
            return Err(WorktreeError::InvalidIdentity);
        }
        let worktree = self
            .artifact_root_capability
            .open_directory(worktree_relative)
            .map_err(WorktreeError::Io)?;
        let worktree =
            RootCapability::from_authenticated_directory(worktree).map_err(WorktreeError::Io)?;
        let identity = worktree
            .identity_marker()
            .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
        Ok((chain, identity))
    }

    fn capture_worktree_parent_chain(
        &self,
        worktree_relative: &RelativePath,
    ) -> Result<Vec<CleanupParentIdentity>, WorktreeError> {
        let components = worktree_relative.components().collect::<Vec<_>>();
        let (_, parents) = components
            .split_last()
            .ok_or(WorktreeError::InvalidIdentity)?;
        let mut relative =
            RelativePath::parse(String::new()).map_err(|_| WorktreeError::InvalidIdentity)?;
        let mut chain = Vec::with_capacity(parents.len());
        for component in parents {
            relative = relative
                .join_component(component)
                .map_err(|_| WorktreeError::InvalidIdentity)?;
            let directory = self
                .artifact_root_capability
                .open_directory(&relative)
                .map_err(WorktreeError::Io)?;
            let capability = RootCapability::from_authenticated_directory(directory)
                .map_err(WorktreeError::Io)?;
            let identity = capability
                .identity_marker()
                .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
            chain.push(CleanupParentIdentity {
                relative: relative.clone(),
                identity,
            });
        }
        Ok(chain)
    }
}

/// Opaque topology intent captured before the first cleanup side effect or
/// rebound by the trusted persisted adapter. It contains no Git binding,
/// target execution directory, or command authority; a target is minted only
/// by a fresh topology observation.
///
/// `common_identity` and `admin_identity` are durable V1 identities. The
/// artifact root, parent chain, admin namespace, and worktree are currently
/// bound by no-follow process-local markers because no dedicated durable
/// identity domains exist for them. The restart adapter first authenticates
/// the durable common/admin evidence and exact absent-or-present topology, then
/// captures fresh markers solely as the baseline for subsequent A/B checks.
pub(crate) struct CleanupTopologyIntentV1 {
    reservation: WorktreeReservation,
    common_identity: DurableDirectoryIdentityV1,
    artifact_root_identity: DirectoryIdentityMarker,
    parent_chain: Vec<CleanupParentIdentity>,
    worktree_relative: RelativePath,
    worktree_path: PathBuf,
    binding: CleanupTopologyBindingV1,
}

impl CleanupTopologyIntentV1 {
    pub(crate) const fn common_directory_identity(&self) -> &DurableDirectoryIdentityV1 {
        &self.common_identity
    }

    pub(crate) const fn admin_directory_identity_digest(&self) -> &[u8; 32] {
        match &self.binding {
            CleanupTopologyBindingV1::Present(binding) => binding.admin_identity.digest(),
            CleanupTopologyBindingV1::PersistedAbsent {
                expected_admin_identity_digest,
                ..
            } => expected_admin_identity_digest,
        }
    }
}

impl fmt::Debug for CleanupTopologyIntentV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CleanupTopologyIntentV1(<opaque>)")
    }
}

struct CleanupParentIdentity {
    relative: RelativePath,
    identity: DirectoryIdentityMarker,
}

enum CleanupTopologyBindingV1 {
    Present(CleanupPresentTopologyBindingV1),
    PersistedAbsent {
        admin_namespace_identity: Option<DirectoryIdentityMarker>,
        expected_admin_identity_digest: [u8; 32],
    },
}

struct CleanupPresentTopologyBindingV1 {
    admin_namespace_identity: DirectoryIdentityMarker,
    admin_name: String,
    admin_relative: RelativePath,
    admin_marker: DirectoryIdentityMarker,
    admin_identity: DurableDirectoryIdentityV1,
    worktree_identity: DirectoryIdentityMarker,
}

/// Opaque cleanup command authority. It retains only the registered primary
/// checkout binding and the authenticated target namespace path; it does not
/// retain an execution directory or filesystem handle for the removable
/// worktree/admin objects.
pub(crate) struct CleanupWorktreeTarget {
    git: Arc<PinnedExecutable>,
    binding: GitCommandBinding,
    path: PathBuf,
}

impl CleanupWorktreeTarget {
    pub(crate) const fn git(&self) -> &Arc<PinnedExecutable> {
        &self.git
    }

    pub(crate) const fn command_binding(&self) -> &GitCommandBinding {
        &self.binding
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn revalidate(&self) -> Result<(), CommandPolicyError> {
        self.git.revalidate()?;
        self.binding.revalidate()
    }
}

impl fmt::Debug for CleanupWorktreeTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CleanupWorktreeTarget(<opaque>)")
    }
}

pub(crate) enum CleanupTopologyObservation {
    Locked(CleanupPresentAuthentication),
    Unlocked(CleanupPresentAuthentication),
    Absent(CleanupAbsentAuthentication),
    Inconsistent,
    Unavailable,
}

impl CleanupTopologyObservation {
    fn from_failure(failure: ObservationFailure) -> Self {
        match failure {
            ObservationFailure::Inconsistent => Self::Inconsistent,
            ObservationFailure::Unavailable => Self::Unavailable,
        }
    }
}

impl fmt::Debug for CleanupTopologyObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Locked(_) => formatter.write_str("CleanupTopologyObservation::Locked(<opaque>)"),
            Self::Unlocked(_) => {
                formatter.write_str("CleanupTopologyObservation::Unlocked(<opaque>)")
            }
            Self::Absent(_) => formatter.write_str("CleanupTopologyObservation::Absent(<opaque>)"),
            Self::Inconsistent => formatter.write_str("CleanupTopologyObservation::Inconsistent"),
            Self::Unavailable => formatter.write_str("CleanupTopologyObservation::Unavailable"),
        }
    }
}

pub(crate) struct CleanupPresentAuthentication {
    source_context: LinkedWorktreeCommandContext,
    target: CleanupWorktreeTarget,
    common_identity: DurableDirectoryIdentityV1,
    admin_identity: DurableDirectoryIdentityV1,
}

impl CleanupPresentAuthentication {
    fn from_observation(
        authenticator: &CleanupWorktreeAuthenticator,
        intent: &CleanupTopologyIntentV1,
        binding: &CleanupPresentTopologyBindingV1,
        source_context: LinkedWorktreeCommandContext,
    ) -> Self {
        Self {
            source_context,
            target: CleanupWorktreeTarget {
                git: Arc::clone(&authenticator.git),
                binding: authenticator.original_binding.clone(),
                path: intent.worktree_path.clone(),
            },
            common_identity: intent.common_identity.clone(),
            admin_identity: binding.admin_identity.clone(),
        }
    }

    pub(crate) const fn source_command_context(&self) -> &LinkedWorktreeCommandContext {
        &self.source_context
    }

    /// Borrows the primary-checkout-only read authority while the transient
    /// source context is still open. Read commands built from this target do
    /// not retain the removable worktree or admin directories.
    pub(crate) const fn target(&self) -> &CleanupWorktreeTarget {
        &self.target
    }

    pub(crate) fn into_target(self) -> CleanupWorktreeTarget {
        let Self {
            source_context,
            target,
            common_identity: _,
            admin_identity: _,
        } = self;
        drop(source_context);
        target
    }

    pub(crate) const fn common_directory_identity(&self) -> &DurableDirectoryIdentityV1 {
        &self.common_identity
    }

    pub(crate) const fn admin_directory_identity(&self) -> &DurableDirectoryIdentityV1 {
        &self.admin_identity
    }
}

impl fmt::Debug for CleanupPresentAuthentication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CleanupPresentAuthentication(<opaque>)")
    }
}

pub(crate) struct CleanupAbsentAuthentication {
    target: CleanupWorktreeTarget,
}

impl CleanupAbsentAuthentication {
    fn from_observation(
        authenticator: &CleanupWorktreeAuthenticator,
        intent: &CleanupTopologyIntentV1,
    ) -> Self {
        Self {
            target: CleanupWorktreeTarget {
                git: Arc::clone(&authenticator.git),
                binding: authenticator.original_binding.clone(),
                path: intent.worktree_path.clone(),
            },
        }
    }

    pub(crate) const fn target(&self) -> &CleanupWorktreeTarget {
        &self.target
    }
}

impl fmt::Debug for CleanupAbsentAuthentication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CleanupAbsentAuthentication(<opaque>)")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObservedAdminState {
    Locked,
    Unlocked,
    AdminAbsent,
    NamespaceAbsent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TopologyKind {
    Locked,
    Unlocked,
    Absent,
}

fn classify_topology(
    worktree_present: bool,
    admin_state: ObservedAdminState,
) -> Option<TopologyKind> {
    match (worktree_present, admin_state) {
        (true, ObservedAdminState::Locked) => Some(TopologyKind::Locked),
        (true, ObservedAdminState::Unlocked) => Some(TopologyKind::Unlocked),
        (false, ObservedAdminState::AdminAbsent | ObservedAdminState::NamespaceAbsent) => {
            Some(TopologyKind::Absent)
        }
        (true, ObservedAdminState::AdminAbsent | ObservedAdminState::NamespaceAbsent)
        | (false, ObservedAdminState::Locked | ObservedAdminState::Unlocked) => None,
    }
}

fn reject_renamed_admin_aliases(
    common_git: &RootCapability,
    captured_namespace: DirectoryIdentityMarker,
    captured_admin: DirectoryIdentityMarker,
) -> Result<(), ObservationFailure> {
    let scan = common_git
        .try_clone_capability()
        .map_err(|error| observation_io_error(&error))?;
    let mut root = scan
        .try_clone_root()
        .map_err(|error| observation_io_error(&error))?;
    let entries = read_directory_names(&mut root, MAX_COMMON_GIT_SIBLINGS)
        .map_err(|error| observation_io_error(&error))?;

    for name in entries {
        if name == "." || name == ".." {
            continue;
        }
        let directory = match open_child_directory(&root, &name) {
            Ok(directory) => directory,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) if entry_is_not_a_directory(&error) => continue,
            Err(error) => return Err(observation_io_error(&error)),
        };
        let directory = RootCapability::from_authenticated_directory(directory)
            .map_err(|error| observation_io_error(&error))?;
        let marker = directory
            .identity_marker()
            .map_err(observation_identity_error)?;
        if marker == captured_namespace || marker == captured_admin {
            return Err(ObservationFailure::Inconsistent);
        }
    }
    Ok(())
}

fn durable_admin_digest_matches(
    directory: &RootCapability,
    expected: &[u8; 32],
) -> Result<bool, WorktreeError> {
    let durable =
        DurableDirectoryIdentityV1::derive(directory, DirectoryIdentityDomain::WorktreeAdmin)
            .map_err(|_| WorktreeError::LinkedMetadataInvalid)?;
    Ok(durable.digest() == expected)
}

fn entry_is_not_a_directory(error: &io::Error) -> bool {
    if error.kind() == io::ErrorKind::NotADirectory {
        return true;
    }
    #[cfg(windows)]
    {
        error.raw_os_error() == Some(267)
    }
    #[cfg(unix)]
    {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObservationFailure {
    Inconsistent,
    Unavailable,
}

fn observation_required_directory_error(error: io::Error) -> ObservationFailure {
    if error.kind() == io::ErrorKind::NotFound {
        ObservationFailure::Inconsistent
    } else {
        observation_io_error(&error)
    }
}

fn observation_io_error(error: &io::Error) -> ObservationFailure {
    #[cfg(unix)]
    if error.raw_os_error() == Some(libc::ELOOP) {
        return ObservationFailure::Inconsistent;
    }
    #[cfg(windows)]
    if error.raw_os_error() == Some(267) {
        return ObservationFailure::Inconsistent;
    }
    match error.kind() {
        io::ErrorKind::NotFound
        | io::ErrorKind::NotADirectory
        | io::ErrorKind::IsADirectory
        | io::ErrorKind::InvalidData
        | io::ErrorKind::InvalidInput => ObservationFailure::Inconsistent,
        _ => ObservationFailure::Unavailable,
    }
}

fn observation_identity_error(error: DirectoryIdentityError) -> ObservationFailure {
    match error {
        DirectoryIdentityError::Mismatch => ObservationFailure::Inconsistent,
        DirectoryIdentityError::Unavailable => ObservationFailure::Unavailable,
    }
}

fn require_observed_durable_identity(
    capability: &RootCapability,
    domain: DirectoryIdentityDomain,
    expected: &DurableDirectoryIdentityV1,
) -> Result<(), ObservationFailure> {
    let current = DurableDirectoryIdentityV1::derive(capability, domain)
        .map_err(observation_identity_error)?;
    if current == *expected {
        Ok(())
    } else {
        Err(ObservationFailure::Inconsistent)
    }
}

fn observation_command_error(error: CommandPolicyError) -> ObservationFailure {
    match error {
        CommandPolicyError::OpenFailed(error) => observation_io_error(&error),
        CommandPolicyError::IdentityChanged
        | CommandPolicyError::InvalidGitBinding
        | CommandPolicyError::NotExecutable
        | CommandPolicyError::RelativePath
        | CommandPolicyError::MissingFileName
        | CommandPolicyError::InvalidTimeout
        | CommandPolicyError::InvalidCargoSelection
        | CommandPolicyError::InvalidCargoEnvironment
        | CommandPolicyError::InvalidGitPath => ObservationFailure::Inconsistent,
    }
}

fn observation_worktree_error(error: WorktreeError) -> ObservationFailure {
    match error {
        WorktreeError::Io(error) => observation_io_error(&error),
        WorktreeError::CommandPolicy(error) => observation_command_error(error),
        WorktreeError::CommonGitIdentityUnavailable => ObservationFailure::Unavailable,
        WorktreeError::InvalidIdentity
        | WorktreeError::InvalidReservation
        | WorktreeError::CommonGitIdentityMismatch
        | WorktreeError::InvalidRepository
        | WorktreeError::CargoWorkspaceOutsideRepository
        | WorktreeError::DestinationConflict
        | WorktreeError::ArtifactPathInvalid
        | WorktreeError::BranchConflict
        | WorktreeError::LinkedMetadataInvalid
        | WorktreeError::PostconditionFailed
        | WorktreeError::WorktreeContentChanged
        | WorktreeError::PartialCreation
        | WorktreeError::InconsistentArtifact
        | WorktreeError::NestedWorkspaceMissing => ObservationFailure::Inconsistent,
        WorktreeError::InvalidLimits
        | WorktreeError::UnbornHead
        | WorktreeError::UnsafeGitConfiguration
        | WorktreeError::InvalidEnvironment
        | WorktreeError::Cancelled
        | WorktreeError::TimedOut
        | WorktreeError::GitCommandFailed
        | WorktreeError::OutputInvalid
        | WorktreeError::Process(_)
        | WorktreeError::Cargo(_) => ObservationFailure::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use crate::{DirectoryIdentityMarker, RootCapability};

    use super::{
        ObservationFailure, ObservedAdminState, TopologyKind, classify_topology,
        reject_renamed_admin_aliases,
    };

    fn directory_marker(path: &Path) -> DirectoryIdentityMarker {
        RootCapability::open(path.canonicalize().unwrap())
            .unwrap()
            .identity_marker()
            .unwrap()
    }

    #[test]
    fn topology_requires_the_admin_and_worktree_to_move_together() {
        assert_eq!(
            classify_topology(true, ObservedAdminState::Locked),
            Some(TopologyKind::Locked)
        );
        assert_eq!(
            classify_topology(true, ObservedAdminState::Unlocked),
            Some(TopologyKind::Unlocked)
        );
        assert_eq!(
            classify_topology(false, ObservedAdminState::AdminAbsent),
            Some(TopologyKind::Absent)
        );
        assert_eq!(
            classify_topology(false, ObservedAdminState::NamespaceAbsent),
            Some(TopologyKind::Absent)
        );
        assert_eq!(
            classify_topology(true, ObservedAdminState::AdminAbsent),
            None
        );
        assert_eq!(
            classify_topology(true, ObservedAdminState::NamespaceAbsent),
            None
        );
        assert_eq!(classify_topology(false, ObservedAdminState::Locked), None);
        assert_eq!(classify_topology(false, ObservedAdminState::Unlocked), None);
        assert_ne!(
            ObservedAdminState::AdminAbsent,
            ObservedAdminState::NamespaceAbsent
        );
    }

    #[test]
    fn namespace_absence_rejects_a_renamed_namespace_alias() {
        let fixture = tempfile::tempdir().unwrap();
        let namespace = fixture.path().join("worktrees");
        let admin = namespace.join("reserved");
        fs::create_dir_all(&admin).unwrap();
        let namespace_marker = directory_marker(&namespace);
        let admin_marker = directory_marker(&admin);
        fs::rename(&namespace, fixture.path().join("worktrees-renamed")).unwrap();
        let common_git = RootCapability::open(fixture.path().canonicalize().unwrap()).unwrap();

        assert_eq!(
            reject_renamed_admin_aliases(&common_git, namespace_marker, admin_marker),
            Err(ObservationFailure::Inconsistent)
        );
    }

    #[test]
    fn namespace_absence_rejects_a_renamed_admin_alias() {
        let fixture = tempfile::tempdir().unwrap();
        let namespace = fixture.path().join("worktrees");
        let admin = namespace.join("reserved");
        fs::create_dir_all(&admin).unwrap();
        let namespace_marker = directory_marker(&namespace);
        let admin_marker = directory_marker(&admin);
        fs::rename(&admin, fixture.path().join("admin-renamed")).unwrap();
        fs::remove_dir(&namespace).unwrap();
        let common_git = RootCapability::open(fixture.path().canonicalize().unwrap()).unwrap();

        assert_eq!(
            reject_renamed_admin_aliases(&common_git, namespace_marker, admin_marker),
            Err(ObservationFailure::Inconsistent)
        );
    }

    #[test]
    fn namespace_absence_ignores_unrelated_common_git_siblings() {
        let fixture = tempfile::tempdir().unwrap();
        let namespace = fixture.path().join("worktrees");
        let admin = namespace.join("reserved");
        fs::create_dir_all(&admin).unwrap();
        let namespace_marker = directory_marker(&namespace);
        let admin_marker = directory_marker(&admin);
        // Unix may recycle the inode of an unlinked directory immediately.
        // Retain the captured objects while creating unrelated siblings so
        // this fixture tests alias detection rather than inode reuse.
        #[cfg(unix)]
        let _namespace_lease = fs::File::open(&namespace).unwrap();
        #[cfg(unix)]
        let _admin_lease = fs::File::open(&admin).unwrap();
        fs::remove_dir(&admin).unwrap();
        fs::remove_dir(&namespace).unwrap();
        fs::create_dir(fixture.path().join("objects")).unwrap();
        fs::write(fixture.path().join("HEAD"), b"ref: refs/heads/main\n").unwrap();
        let common_git = RootCapability::open(fixture.path().canonicalize().unwrap()).unwrap();

        assert_eq!(
            reject_renamed_admin_aliases(&common_git, namespace_marker, admin_marker),
            Ok(())
        );
    }
}
