use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::command_policy::ExecutionDirectory;
use crate::fingerprint::{DeliverySnapshotEntry, DeliverySourceSnapshot, WorkspaceFingerprinter};
#[cfg(unix)]
use crate::native_fs::open_child_directory;
use crate::native_fs::{
    child_directory_matches, child_entry_exists, child_file_matches, open_child_file,
    quarantine_child_entry_no_replace, read_directory_names, remove_child_directory,
    remove_child_file, reopen_directory_for_child_directory, reopen_directory_for_delete,
};
#[cfg(windows)]
use crate::native_fs::{create_child_directory_with_created, reopen_file_for_delete};
use crate::root_capability::{DirectoryPathGuard, directory_identity_marker, ensure_plain_file};
use crate::{FingerprintLimits, RelativePath};
use tokio_util::sync::CancellationToken;

use super::command::{
    DeliveryIndexInfoInput, DeliverySnapshotHashInput, DeliverySourceMutationCommands,
};
use super::observation::{DeliveryCommandExecutor, parse_object_id};
use super::{
    CandidateTreeProvenance, DeliveryCandidateTree, DeliveryCommitOid, DeliverySourceError,
    DeliveryTreeOid, ProbedDeliveryGit,
};

const MAX_ALLOCATION_ATTEMPTS: usize = 32;
const INDEX_FILE_NAME: &str = "index";
const INDEX_LOCK_FILE_NAME: &str = "index.lock";

/// A retained, application-private Git index namespace for one candidate-tree
/// construction.
///
/// This is deliberately separate from [`super::sandbox::DeliveryCommandSandbox`].
/// The long-lived command sandbox must remain empty, whereas this authority
/// admits only Git's fixed `index` child after a successful typed index writer.
/// Its private directory name is also a recognizable crash residue, allowing a
/// later maintenance process to distinguish an uncleaned authority from a
/// repository index.
pub(super) struct DeliveryTemporaryIndex {
    parent: Arc<ExecutionDirectory>,
    name: String,
    path: PathBuf,
    directory: Option<Arc<ExecutionDirectory>>,
    directory_guard: Option<DirectoryPathGuard>,
    index_file: Option<File>,
    stage: TemporaryIndexStage,
    cleanup_permitted: bool,
    cleaned: bool,
}

impl DeliveryTemporaryIndex {
    /// Creates a new empty index namespace below the authenticated private
    /// runtime directory. The fixed `index` name is intentionally not created
    /// until Git has successfully written it.
    pub(super) fn create(parent: Arc<ExecutionDirectory>) -> Result<Self, DeliverySourceError> {
        parent
            .revalidate()
            .map_err(|_| temporary_index_unavailable())?;
        let parent_root = parent
            .cloned_root_capability()
            .map_err(|_| temporary_index_unavailable())?;

        for _ in 0..MAX_ALLOCATION_ATTEMPTS {
            let name = random_index_directory_name()?;
            let parent_handle = parent_root
                .try_clone_root()
                .and_then(|root| reopen_directory_for_child_directory(&root))
                .map_err(|_| temporary_index_unavailable())?;
            let created = create_direct_child_exclusive(&parent_handle, OsStr::new(&name))
                .map_err(|_| temporary_index_unavailable())?;
            let Some(created) = created else {
                continue;
            };

            let mut temporary_index =
                Self::from_created_directory(Arc::clone(&parent), &parent_root, name, created)?;
            if let Err(error) = temporary_index.verify_empty() {
                return match temporary_index.cleanup_known_stable() {
                    Ok(()) => Err(error),
                    Err(_) => Err(temporary_index_cleanup_unproven()),
                };
            }
            return Ok(temporary_index);
        }

        Err(temporary_index_unavailable())
    }

    fn from_created_directory(
        parent: Arc<ExecutionDirectory>,
        parent_root: &crate::RootCapability,
        name: String,
        created: File,
    ) -> Result<Self, DeliverySourceError> {
        let relative =
            RelativePath::parse(name.clone()).map_err(|_| temporary_index_cleanup_unproven())?;
        let directory_guard = parent_root
            .ensure_directory_path(&relative)
            .map_err(|_| temporary_index_cleanup_unproven())?;
        let path = parent.path().join(&name);
        if !is_direct_child(&path, parent.path(), &name) {
            return Err(temporary_index_cleanup_unproven());
        }
        // The retained path guard holds every component lease while this
        // execution capability opens the visible directory. The exact-created
        // handle, guard and resulting authority are compared immediately
        // below, so this does not convert a namespace lookup into authority.
        let directory = Arc::new(
            ExecutionDirectory::open(&path).map_err(|_| temporary_index_cleanup_unproven())?,
        );
        require_same_directory(&created, &directory_guard, &directory)?;
        parent
            .revalidate()
            .map_err(|_| temporary_index_cleanup_unproven())?;

        Ok(Self {
            parent,
            name,
            path,
            directory: Some(directory),
            directory_guard: Some(directory_guard),
            index_file: None,
            stage: TemporaryIndexStage::Empty,
            cleanup_permitted: true,
            cleaned: false,
        })
    }

    /// Returns only the retained directory capability needed to construct the
    /// command-policy's opaque `GIT_INDEX_FILE` authority. No raw path is
    /// exposed from this module.
    pub(super) fn directory_authority(&self) -> Arc<ExecutionDirectory> {
        Arc::clone(
            self.directory
                .as_ref()
                .expect("a live temporary index retains its directory authority"),
        )
    }

    /// Verifies the namespace in its current lifecycle stage.
    pub(super) fn revalidate(&self) -> Result<(), DeliverySourceError> {
        if self.cleaned {
            return Err(temporary_index_unavailable());
        }
        self.validate_directory_identity()?;
        match self.stage {
            TemporaryIndexStage::Empty => self.validate_empty_layout(),
            TemporaryIndexStage::Index => self.validate_index_state(),
        }
    }

    /// Requires the creation-state invariant before the first `read-tree`
    /// writer. In particular, a stale `index.lock` residue is not silently
    /// reused.
    pub(super) fn verify_empty(&self) -> Result<(), DeliverySourceError> {
        if self.cleaned || self.stage != TemporaryIndexStage::Empty {
            return Err(temporary_index_unavailable());
        }
        self.validate_directory_identity()?;
        self.validate_empty_layout()
    }

    /// Accepts the exact `index` object produced by a successfully completed
    /// typed Git writer. The previous retained index may legitimately have
    /// been atomically replaced by Git, so this method deliberately validates
    /// the stable directory authority rather than the previous index identity.
    pub(super) fn refresh_after_successful_writer(&mut self) -> Result<(), DeliverySourceError> {
        if self.cleaned {
            return Err(temporary_index_unavailable());
        }
        self.validate_directory_identity()?;
        let index_file = self.open_verified_index_file()?;
        self.index_file = Some(index_file);
        self.stage = TemporaryIndexStage::Index;
        self.validate_index_state()
    }

    /// `write-tree` is not allowed to alter the accepted temporary index. This
    /// postcondition closes the candidate-tree command sequence before its
    /// object ID can be returned to higher layers.
    pub(super) fn verify_after_write_tree(&self) -> Result<(), DeliverySourceError> {
        if self.stage != TemporaryIndexStage::Index {
            return Err(temporary_index_unavailable());
        }
        self.revalidate()
    }

    /// Removes this authority only after every owned name and retained handle
    /// still agree. On ambiguity it leaves the namespace in place rather than
    /// deleting a replacement supplied by another process.
    pub(super) fn cleanup_known_stable(&mut self) -> Result<(), DeliverySourceError> {
        if self.cleaned {
            return Ok(());
        }
        if !self.cleanup_permitted {
            return Err(temporary_index_cleanup_unproven());
        }
        self.revalidate()
            .map_err(|_| temporary_index_cleanup_unproven())?;
        if self.stage == TemporaryIndexStage::Index {
            self.remove_accepted_index()?;
            self.index_file = None;
            self.stage = TemporaryIndexStage::Empty;
            self.validate_empty_layout()
                .map_err(|_| temporary_index_cleanup_unproven())?;
        }
        self.remove_empty_directory()
    }

    /// Marks the namespace as possibly modified by a child whose final state
    /// was not proven.  Dropping the authority will then retain the residue
    /// rather than attempting a speculative cleanup through names Git may
    /// have changed.
    pub(super) fn abandon(&mut self) {
        self.cleanup_permitted = false;
    }

    fn cleanup_is_permitted(&self) -> bool {
        self.cleanup_permitted
    }

    fn validate_directory_identity(&self) -> Result<(), DeliverySourceError> {
        if !is_direct_child(&self.path, self.parent.path(), &self.name) {
            return Err(temporary_index_unavailable());
        }
        self.parent
            .revalidate()
            .map_err(|_| temporary_index_unavailable())?;
        let directory = self.directory()?;
        let directory_guard = self
            .directory_guard
            .as_ref()
            .ok_or_else(temporary_index_unavailable)?;
        directory
            .revalidate()
            .map_err(|_| temporary_index_unavailable())?;
        require_guard_identity(directory_guard, directory)?;

        let root = self
            .parent
            .cloned_root_capability()
            .map_err(|_| temporary_index_unavailable())?;
        let relative =
            RelativePath::parse(self.name.clone()).map_err(|_| temporary_index_unavailable())?;
        let named = root
            .open_directory(&relative)
            .map_err(|_| temporary_index_unavailable())?;
        if directory_identity_marker(&named).map_err(|_| temporary_index_unavailable())?
            != directory_identity(directory)?
        {
            return Err(temporary_index_unavailable());
        }
        Ok(())
    }

    fn validate_empty_layout(&self) -> Result<(), DeliverySourceError> {
        let mut directory = self.directory_root()?;
        let entries =
            read_directory_names(&mut directory, 1).map_err(|_| temporary_index_unavailable())?;
        if entries.is_empty() {
            Ok(())
        } else {
            Err(temporary_index_unavailable())
        }
    }

    fn validate_index_state(&self) -> Result<(), DeliverySourceError> {
        let index_file = self
            .index_file
            .as_ref()
            .ok_or_else(temporary_index_unavailable)?;
        let directory = self.directory_root()?;
        self.require_index_layout(&directory)?;
        ensure_plain_file(index_file).map_err(|_| temporary_index_unavailable())?;
        if child_file_matches(&directory, OsStr::new(INDEX_FILE_NAME), index_file)
            .map_err(|_| temporary_index_unavailable())?
        {
            Ok(())
        } else {
            Err(temporary_index_unavailable())
        }
    }

    fn open_verified_index_file(&self) -> Result<File, DeliverySourceError> {
        let directory = self.directory_root()?;
        self.require_index_layout(&directory)?;
        let index = open_child_file(&directory, OsStr::new(INDEX_FILE_NAME))
            .map_err(|_| temporary_index_unavailable())?;
        ensure_plain_file(&index).map_err(|_| temporary_index_unavailable())?;
        self.require_index_layout(&directory)?;
        if child_file_matches(&directory, OsStr::new(INDEX_FILE_NAME), &index)
            .map_err(|_| temporary_index_unavailable())?
        {
            Ok(index)
        } else {
            Err(temporary_index_unavailable())
        }
    }

    fn require_index_layout(&self, directory: &File) -> Result<(), DeliverySourceError> {
        let mut enumeration = directory
            .try_clone()
            .map_err(|_| temporary_index_unavailable())?;
        let entries = read_directory_names(&mut enumeration, 2)
            .map_err(|_| temporary_index_unavailable())?
            .into_iter()
            .collect::<BTreeSet<_>>();
        let expected = BTreeSet::from([OsString::from(INDEX_FILE_NAME)]);
        if entries != expected
            || child_entry_exists(directory, OsStr::new(INDEX_LOCK_FILE_NAME))
                .map_err(|_| temporary_index_unavailable())?
        {
            return Err(temporary_index_unavailable());
        }
        Ok(())
    }

    fn directory(&self) -> Result<&Arc<ExecutionDirectory>, DeliverySourceError> {
        self.directory
            .as_ref()
            .ok_or_else(temporary_index_unavailable)
    }

    fn directory_root(&self) -> Result<File, DeliverySourceError> {
        let root = self
            .directory()?
            .cloned_root_capability()
            .map_err(|_| temporary_index_unavailable())?;
        root.try_clone_root()
            .map_err(|_| temporary_index_unavailable())
    }

    fn remove_accepted_index(&mut self) -> Result<(), DeliverySourceError> {
        let directory = self
            .directory_root()
            .map_err(|_| temporary_index_cleanup_unproven())?;
        let index = self
            .index_file
            .as_ref()
            .ok_or_else(temporary_index_cleanup_unproven)?;
        if !child_file_matches(&directory, OsStr::new(INDEX_FILE_NAME), index)
            .map_err(|_| temporary_index_cleanup_unproven())?
        {
            return Err(temporary_index_cleanup_unproven());
        }
        let deletion = index_deletion_handle(index)?;
        if !child_file_matches(&directory, OsStr::new(INDEX_FILE_NAME), index)
            .map_err(|_| temporary_index_cleanup_unproven())?
        {
            return Err(temporary_index_cleanup_unproven());
        }
        let quarantine =
            quarantine_file(&directory, OsStr::new(INDEX_FILE_NAME), index, &deletion)?;
        remove_child_file(&directory, &quarantine, &deletion)
            .map_err(|_| temporary_index_cleanup_unproven())?;
        drop(deletion);
        if child_entry_exists(&directory, &quarantine)
            .map_err(|_| temporary_index_cleanup_unproven())?
        {
            return Err(temporary_index_cleanup_unproven());
        }
        Ok(())
    }

    fn remove_empty_directory(&mut self) -> Result<(), DeliverySourceError> {
        self.validate_directory_identity()
            .map_err(|_| temporary_index_cleanup_unproven())?;
        self.validate_empty_layout()
            .map_err(|_| temporary_index_cleanup_unproven())?;

        let parent = self.cleanup_parent()?;
        let retained_directory = self
            .directory_root()
            .map_err(|_| temporary_index_cleanup_unproven())?;

        // The guard intentionally prevents namespace replacement on Windows.
        // Release it before reopening DELETE access from the already-retained
        // directory handle.  Reopening by this handle cannot resolve a
        // replacement through the parent namespace.
        drop(self.directory_guard.take());
        if !child_directory_matches(&parent, OsStr::new(&self.name), &retained_directory)
            .map_err(|_| temporary_index_cleanup_unproven())?
        {
            return Err(temporary_index_cleanup_unproven());
        }
        let deletion = reopen_directory_for_delete(&retained_directory)
            .map_err(|_| temporary_index_cleanup_unproven())?;
        if !child_directory_matches(&parent, OsStr::new(&self.name), &retained_directory)
            .map_err(|_| temporary_index_cleanup_unproven())?
        {
            return Err(temporary_index_cleanup_unproven());
        }
        require_directory_empty(&retained_directory)?;
        let quarantine = quarantine_directory(
            &parent,
            OsStr::new(&self.name),
            &retained_directory,
            &deletion,
        )?;

        drop(self.directory.take());
        remove_child_directory(&parent, &quarantine, &deletion)
            .map_err(|_| temporary_index_cleanup_unproven())?;
        drop(deletion);
        drop(retained_directory);
        if child_entry_exists(&parent, &quarantine)
            .map_err(|_| temporary_index_cleanup_unproven())?
        {
            return Err(temporary_index_cleanup_unproven());
        }
        self.cleaned = true;
        self.parent
            .revalidate()
            .map_err(|_| temporary_index_cleanup_unproven())?;
        if child_entry_exists(&parent, OsStr::new(&self.name))
            .map_err(|_| temporary_index_cleanup_unproven())?
        {
            return Err(temporary_index_cleanup_unproven());
        }
        Ok(())
    }

    fn cleanup_parent(&self) -> Result<File, DeliverySourceError> {
        let root = self
            .parent
            .cloned_root_capability()
            .map_err(|_| temporary_index_cleanup_unproven())?;
        let handle = root
            .try_clone_root()
            .map_err(|_| temporary_index_cleanup_unproven())?;
        reopen_directory_for_child_directory(&handle)
            .map_err(|_| temporary_index_cleanup_unproven())
    }
}

impl fmt::Debug for DeliveryTemporaryIndex {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryTemporaryIndex(<opaque>)")
    }
}

impl Drop for DeliveryTemporaryIndex {
    fn drop(&mut self) {
        if self.cleanup_permitted {
            let _ = self.cleanup_known_stable();
        }
    }
}

/// Builds an unreferenced candidate tree through an authority private index.
/// The caller owns source A/B authentication around this sequence; this
/// module owns only the index namespace and fixed Git mutation lifecycle.
pub(super) async fn build_candidate_tree(
    executor: &DeliveryCommandExecutor,
    commands: &DeliverySourceMutationCommands,
    probe: &ProbedDeliveryGit,
    provenance: CandidateTreeProvenance,
    fingerprint_limits: FingerprintLimits,
    cancellation: CancellationToken,
    output_limit: usize,
) -> Result<DeliveryCandidateTree, DeliverySourceError> {
    let context = CandidateTreeCommandContext {
        executor,
        commands,
        probe,
        cancellation,
        output_limit,
    };
    if context.cancellation.is_cancelled() {
        return Err(DeliverySourceError::Cancelled);
    }
    let base = provenance.base_commit().clone();
    let mut snapshot = capture_stable_approved_snapshot(
        &context,
        StableApprovedSnapshotInput {
            base: &base,
            fingerprint_limits,
            approved_fingerprint: provenance.approved_fingerprint(),
        },
    )
    .await?;
    let mut temporary_index =
        DeliveryTemporaryIndex::create(Arc::clone(context.probe.private_runtime()))?;
    let result = build_tree_with_temporary_index(
        &context,
        TemporaryIndexTreeBuildInput {
            base: &base,
            snapshot: &mut snapshot,
            temporary_index: &mut temporary_index,
        },
    )
    .await;
    finish_temporary_index(&mut temporary_index, provenance, result)
}

struct CandidateTreeCommandContext<'a> {
    executor: &'a DeliveryCommandExecutor,
    commands: &'a DeliverySourceMutationCommands,
    probe: &'a ProbedDeliveryGit,
    cancellation: CancellationToken,
    output_limit: usize,
}

impl<'a> CandidateTreeCommandContext<'a> {
    fn index_record_flush_context<'index>(
        &self,
        temporary_index: &'index mut DeliveryTemporaryIndex,
    ) -> IndexRecordFlushContext<'a, 'index> {
        IndexRecordFlushContext {
            executor: self.executor,
            commands: self.commands,
            temporary_index,
            object_id_hexadecimal_length: self.probe.object_format().hexadecimal_length(),
            cancellation: self.cancellation.clone(),
            output_limit: self.output_limit,
        }
    }
}

struct TemporaryIndexTreeBuildInput<'a> {
    base: &'a DeliveryCommitOid,
    snapshot: &'a mut DeliverySourceSnapshot,
    temporary_index: &'a mut DeliveryTemporaryIndex,
}

struct StableApprovedSnapshotInput<'a> {
    base: &'a DeliveryCommitOid,
    fingerprint_limits: FingerprintLimits,
    approved_fingerprint: coding_agent_core::WorkspaceFingerprint,
}

struct IndexRecordFlushContext<'a, 'index> {
    executor: &'a DeliveryCommandExecutor,
    commands: &'a DeliverySourceMutationCommands,
    temporary_index: &'index mut DeliveryTemporaryIndex,
    object_id_hexadecimal_length: usize,
    cancellation: CancellationToken,
    output_limit: usize,
}

async fn build_tree_with_temporary_index(
    context: &CandidateTreeCommandContext<'_>,
    input: TemporaryIndexTreeBuildInput<'_>,
) -> Result<DeliveryTreeOid, DeliverySourceError> {
    let TemporaryIndexTreeBuildInput {
        base,
        snapshot,
        temporary_index,
    } = input;
    temporary_index.verify_empty()?;
    run_index_child(
        context.executor,
        context.commands.read_tree(temporary_index, base),
        temporary_index,
        context.cancellation.clone(),
        context.output_limit,
    )
    .await?;
    accept_index_writer(temporary_index)?;

    write_snapshot_to_temporary_index(context, snapshot, temporary_index).await?;

    let output = run_index_child(
        context.executor,
        context.commands.write_tree(temporary_index),
        temporary_index,
        context.cancellation.clone(),
        context.output_limit,
    )
    .await?;
    // `write-tree` may atomically refresh cache-tree data in its private
    // index. Accept the new identity only after the successful child outcome,
    // then prove the resulting layout before returning its object ID.
    accept_index_writer(temporary_index)?;
    if let Err(error) = temporary_index.verify_after_write_tree() {
        temporary_index.abandon();
        return Err(error);
    }
    let object_id = parse_object_id(&output, context.probe.object_format().hexadecimal_length())
        .map_err(|_| DeliverySourceError::CommandFailed)?;
    DeliveryTreeOid::try_new(object_id, context.probe.object_format())
        .ok_or(DeliverySourceError::CommandFailed)
}

/// Takes two no-follow, identity-bound snapshots under the same configured
/// fingerprint limits. Both the Git listings and the fingerprint domain must
/// agree, and the retained second snapshot must equal the approved review
/// fingerprint before Git is allowed to mutate even the private index.
async fn capture_stable_approved_snapshot(
    context: &CandidateTreeCommandContext<'_>,
    input: StableApprovedSnapshotInput<'_>,
) -> Result<DeliverySourceSnapshot, DeliverySourceError> {
    let StableApprovedSnapshotInput {
        base,
        fingerprint_limits,
        approved_fingerprint,
    } = input;
    let work_tree = context.commands.snapshot_work_tree()?;
    let tracked_before = run_snapshot_listing(
        context.executor,
        context.commands.snapshot_index_entries(),
        context.cancellation.clone(),
        context.output_limit,
    )
    .await?;
    let untracked_before = run_snapshot_listing(
        context.executor,
        context.commands.snapshot_untracked_paths(),
        context.cancellation.clone(),
        context.output_limit,
    )
    .await?;
    let deleted_before = run_snapshot_listing(
        context.executor,
        context.commands.snapshot_deleted_base_paths(base),
        context.cancellation.clone(),
        context.output_limit,
    )
    .await?;
    let first = WorkspaceFingerprinter::capture_delivery_snapshot(
        &work_tree,
        fingerprint_limits,
        &tracked_before,
        &untracked_before,
        &deleted_before,
        context.probe.object_format().hexadecimal_length(),
        &context.cancellation,
    )
    .map_err(DeliverySourceError::from)?;
    let first_fingerprint = first.fingerprint();
    // The first capture exists only to detect drift. Release its raw bytes
    // before taking the retained second snapshot so the approved total-byte
    // bound is not silently doubled in memory.
    drop(first);

    let tracked_after = run_snapshot_listing(
        context.executor,
        context.commands.snapshot_index_entries(),
        context.cancellation.clone(),
        context.output_limit,
    )
    .await?;
    let untracked_after = run_snapshot_listing(
        context.executor,
        context.commands.snapshot_untracked_paths(),
        context.cancellation.clone(),
        context.output_limit,
    )
    .await?;
    let deleted_after = run_snapshot_listing(
        context.executor,
        context.commands.snapshot_deleted_base_paths(base),
        context.cancellation.clone(),
        context.output_limit,
    )
    .await?;
    let second = WorkspaceFingerprinter::capture_delivery_snapshot(
        &work_tree,
        fingerprint_limits,
        &tracked_after,
        &untracked_after,
        &deleted_after,
        context.probe.object_format().hexadecimal_length(),
        &context.cancellation,
    )
    .map_err(DeliverySourceError::from)?;

    if context.cancellation.is_cancelled() {
        return Err(DeliverySourceError::Cancelled);
    }
    if tracked_before != tracked_after
        || untracked_before != untracked_after
        || deleted_before != deleted_after
        || first_fingerprint != second.fingerprint()
        || second.fingerprint() != approved_fingerprint
    {
        return Err(DeliverySourceError::SourceChanged);
    }
    Ok(second)
}

async fn run_snapshot_listing(
    executor: &DeliveryCommandExecutor,
    command: Result<crate::command_policy::ValidatedCommand, DeliverySourceError>,
    cancellation: CancellationToken,
    output_limit: usize,
) -> Result<Vec<u8>, DeliverySourceError> {
    executor.run(command?, cancellation, output_limit).await
}

/// Reconstructs the private index from a Rust-held snapshot. Git receives
/// only exact blob bytes through `hash-object --stdin` and typed NUL records
/// through `update-index --index-info`; it is never given a workspace path to
/// reopen.
async fn write_snapshot_to_temporary_index(
    context: &CandidateTreeCommandContext<'_>,
    snapshot: &mut DeliverySourceSnapshot,
    temporary_index: &mut DeliveryTemporaryIndex,
) -> Result<(), DeliverySourceError> {
    let mut records = Vec::new();
    for entry in snapshot.take_entries() {
        let record = match entry {
            DeliverySnapshotEntry::Present {
                raw_path,
                mode,
                bytes,
            } => {
                let output = run_index_child(
                    context.executor,
                    context
                        .commands
                        .hash_snapshot_file(DeliverySnapshotHashInput::try_new(bytes)?),
                    temporary_index,
                    context.cancellation.clone(),
                    context.output_limit,
                )
                .await?;
                let object_id =
                    parse_object_id(&output, context.probe.object_format().hexadecimal_length())
                        .map_err(|_| DeliverySourceError::CommandFailed)?;
                index_info_record(mode.as_bytes(), object_id.as_bytes(), &raw_path)?
            }
            DeliverySnapshotEntry::Deleted { raw_path } => index_info_record(
                b"0",
                &zero_object_id(context.probe.object_format().hexadecimal_length()),
                &raw_path,
            )?,
        };
        append_or_flush_index_record(
            context.index_record_flush_context(temporary_index),
            &mut records,
            record,
        )
        .await?;
    }
    let mut flush_context = context.index_record_flush_context(temporary_index);
    flush_index_records(&mut flush_context, &mut records).await
}

async fn append_or_flush_index_record(
    mut context: IndexRecordFlushContext<'_, '_>,
    records: &mut Vec<u8>,
    record: Vec<u8>,
) -> Result<(), DeliverySourceError> {
    if record.len() > DeliveryIndexInfoInput::maximum_bytes() {
        return Err(DeliverySourceError::BoundsExceeded);
    }
    if !records.is_empty()
        && records
            .len()
            .checked_add(record.len())
            .ok_or(DeliverySourceError::BoundsExceeded)?
            > DeliveryIndexInfoInput::maximum_bytes()
    {
        flush_index_records(&mut context, records).await?;
    }
    records.extend_from_slice(&record);
    Ok(())
}

async fn flush_index_records(
    context: &mut IndexRecordFlushContext<'_, '_>,
    records: &mut Vec<u8>,
) -> Result<(), DeliverySourceError> {
    if records.is_empty() {
        return Ok(());
    }
    let input = DeliveryIndexInfoInput::try_new(
        std::mem::take(records),
        context.object_id_hexadecimal_length,
    )?;
    run_index_child(
        context.executor,
        context
            .commands
            .update_index_info(context.temporary_index, input),
        context.temporary_index,
        context.cancellation.clone(),
        context.output_limit,
    )
    .await?;
    accept_index_writer(context.temporary_index)
}

fn index_info_record(
    mode: &[u8],
    object_id: &[u8],
    raw_path: &[u8],
) -> Result<Vec<u8>, DeliverySourceError> {
    let capacity = mode
        .len()
        .checked_add(1)
        .and_then(|value| value.checked_add(object_id.len()))
        .and_then(|value| value.checked_add(1))
        .and_then(|value| value.checked_add(raw_path.len()))
        .and_then(|value| value.checked_add(1))
        .ok_or(DeliverySourceError::BoundsExceeded)?;
    let mut record = Vec::new();
    record
        .try_reserve_exact(capacity)
        .map_err(|_| DeliverySourceError::BoundsExceeded)?;
    record.extend_from_slice(mode);
    record.push(b' ');
    record.extend_from_slice(object_id);
    record.push(b'\t');
    record.extend_from_slice(raw_path);
    record.push(0);
    Ok(record)
}

fn zero_object_id(object_id_hexadecimal_length: usize) -> Vec<u8> {
    vec![b'0'; object_id_hexadecimal_length]
}

async fn run_index_child(
    executor: &DeliveryCommandExecutor,
    command: Result<crate::command_policy::ValidatedCommand, DeliverySourceError>,
    temporary_index: &mut DeliveryTemporaryIndex,
    cancellation: CancellationToken,
    output_limit: usize,
) -> Result<Vec<u8>, DeliverySourceError> {
    let command = command?;
    match executor.run(command, cancellation, output_limit).await {
        Ok(output) => Ok(output),
        Err(error) => {
            temporary_index.abandon();
            Err(error)
        }
    }
}

fn accept_index_writer(
    temporary_index: &mut DeliveryTemporaryIndex,
) -> Result<(), DeliverySourceError> {
    if let Err(error) = temporary_index.refresh_after_successful_writer() {
        temporary_index.abandon();
        return Err(error);
    }
    Ok(())
}

fn finish_temporary_index(
    temporary_index: &mut DeliveryTemporaryIndex,
    provenance: CandidateTreeProvenance,
    result: Result<DeliveryTreeOid, DeliverySourceError>,
) -> Result<DeliveryCandidateTree, DeliverySourceError> {
    match result {
        Ok(tree) => {
            temporary_index.cleanup_known_stable()?;
            Ok(DeliveryCandidateTree::from_tree(tree, provenance))
        }
        Err(error) if !temporary_index.cleanup_is_permitted() => Err(error),
        Err(error) => {
            temporary_index.cleanup_known_stable()?;
            Err(error)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TemporaryIndexStage {
    Empty,
    Index,
}

fn require_same_directory(
    created: &File,
    directory_guard: &DirectoryPathGuard,
    directory: &ExecutionDirectory,
) -> Result<(), DeliverySourceError> {
    let created_identity =
        directory_identity_marker(created).map_err(|_| temporary_index_cleanup_unproven())?;
    let guarded_identity = directory_guard
        .try_clone_final()
        .and_then(|file| directory_identity_marker(&file).map_err(io::Error::other))
        .map_err(|_| temporary_index_cleanup_unproven())?;
    if created_identity == guarded_identity && guarded_identity == directory_identity(directory)? {
        Ok(())
    } else {
        Err(temporary_index_cleanup_unproven())
    }
}

fn require_guard_identity(
    directory_guard: &DirectoryPathGuard,
    directory: &ExecutionDirectory,
) -> Result<(), DeliverySourceError> {
    let guarded = directory_guard
        .try_clone_final()
        .and_then(|file| directory_identity_marker(&file).map_err(io::Error::other))
        .map_err(|_| temporary_index_unavailable())?;
    if guarded == directory_identity(directory)? {
        Ok(())
    } else {
        Err(temporary_index_unavailable())
    }
}

fn directory_identity(
    directory: &ExecutionDirectory,
) -> Result<crate::DirectoryIdentityMarker, DeliverySourceError> {
    directory
        .cloned_root_capability()
        .and_then(|root| {
            root.identity_marker()
                .map_err(|error| crate::CommandPolicyError::OpenFailed(io::Error::other(error)))
        })
        .map_err(|_| temporary_index_unavailable())
}

fn quarantine_file(
    parent: &File,
    source: &OsStr,
    retained: &File,
    deletion: &File,
) -> Result<OsString, DeliverySourceError> {
    if !child_file_matches(parent, source, retained)
        .map_err(|_| temporary_index_cleanup_unproven())?
    {
        return Err(temporary_index_cleanup_unproven());
    }
    for _ in 0..MAX_ALLOCATION_ATTEMPTS {
        let quarantine = random_quarantine_name()?;
        match quarantine_child_entry_no_replace(parent, source, &quarantine, deletion) {
            Ok(()) => {
                if child_file_matches(parent, &quarantine, retained)
                    .map_err(|_| temporary_index_cleanup_unproven())?
                {
                    return Ok(quarantine);
                }
                return Err(temporary_index_cleanup_unproven());
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(_) => return Err(temporary_index_cleanup_unproven()),
        }
    }
    Err(temporary_index_cleanup_unproven())
}

fn quarantine_directory(
    parent: &File,
    source: &OsStr,
    retained: &File,
    deletion: &File,
) -> Result<OsString, DeliverySourceError> {
    if !child_directory_matches(parent, source, retained)
        .map_err(|_| temporary_index_cleanup_unproven())?
    {
        return Err(temporary_index_cleanup_unproven());
    }
    for _ in 0..MAX_ALLOCATION_ATTEMPTS {
        let quarantine = random_quarantine_name()?;
        match quarantine_child_entry_no_replace(parent, source, &quarantine, deletion) {
            Ok(()) => {
                if child_directory_matches(parent, &quarantine, retained)
                    .map_err(|_| temporary_index_cleanup_unproven())?
                {
                    return Ok(quarantine);
                }
                return Err(temporary_index_cleanup_unproven());
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(_) => return Err(temporary_index_cleanup_unproven()),
        }
    }
    Err(temporary_index_cleanup_unproven())
}

fn require_directory_empty(directory: &File) -> Result<(), DeliverySourceError> {
    let mut enumeration = directory
        .try_clone()
        .map_err(|_| temporary_index_cleanup_unproven())?;
    if read_directory_names(&mut enumeration, 1)
        .map_err(|_| temporary_index_cleanup_unproven())?
        .is_empty()
    {
        Ok(())
    } else {
        Err(temporary_index_cleanup_unproven())
    }
}

#[cfg(unix)]
fn index_deletion_handle(index: &File) -> Result<File, DeliverySourceError> {
    index
        .try_clone()
        .map_err(|_| temporary_index_cleanup_unproven())
}

#[cfg(windows)]
fn index_deletion_handle(index: &File) -> Result<File, DeliverySourceError> {
    reopen_file_for_delete(index).map_err(|_| temporary_index_cleanup_unproven())
}

#[cfg(unix)]
fn create_direct_child_exclusive(parent: &File, name: &OsStr) -> io::Result<Option<File>> {
    use std::ffi::CString;
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStrExt;

    let name = CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "directory name contains NUL"))?;
    let result = unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) };
    if result == 0 {
        open_child_directory(parent, OsStr::from_bytes(name.as_bytes())).map(Some)
    } else {
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::AlreadyExists {
            Ok(None)
        } else {
            Err(error)
        }
    }
}

#[cfg(windows)]
fn create_direct_child_exclusive(parent: &File, name: &OsStr) -> io::Result<Option<File>> {
    create_child_directory_with_created(parent, name)
        .map(|(directory, created)| created.then_some(directory))
}

fn random_index_directory_name() -> Result<String, DeliverySourceError> {
    let mut random = [0u8; 16];
    getrandom::fill(&mut random).map_err(|_| temporary_index_unavailable())?;
    let mut name = String::from(".coding-agent-delivery-index-v1-");
    for byte in random {
        use std::fmt::Write as _;

        write!(&mut name, "{byte:02x}").expect("writing hexadecimal bytes to String cannot fail");
    }
    Ok(name)
}

fn random_quarantine_name() -> Result<OsString, DeliverySourceError> {
    let mut random = [0u8; 16];
    getrandom::fill(&mut random).map_err(|_| temporary_index_cleanup_unproven())?;
    let mut name = String::from(".coding-agent-delivery-index-cleanup-v1-");
    for byte in random {
        use std::fmt::Write as _;

        write!(&mut name, "{byte:02x}").expect("writing hexadecimal bytes to String cannot fail");
    }
    Ok(OsString::from(name))
}

fn is_direct_child(path: &Path, parent: &Path, name: &str) -> bool {
    path.is_absolute()
        && parent.is_absolute()
        && path.parent() == Some(parent)
        && path.file_name() == Some(OsStr::new(name))
        && !name.contains(['/', '\\'])
}

const fn temporary_index_unavailable() -> DeliverySourceError {
    DeliverySourceError::SandboxUnavailable
}

const fn temporary_index_cleanup_unproven() -> DeliverySourceError {
    DeliverySourceError::SandboxCleanupUnproven
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temporary_index_is_opaque_and_accepts_only_the_fixed_index_file() {
        let fixture = TemporaryIndexFixture::new();
        let mut temporary_index =
            DeliveryTemporaryIndex::create(Arc::clone(&fixture.parent)).unwrap();

        assert_eq!(
            format!("{temporary_index:?}"),
            "DeliveryTemporaryIndex(<opaque>)"
        );
        assert!(!format!("{temporary_index:?}").contains(fixture.root.to_string_lossy().as_ref()));
        temporary_index.verify_empty().unwrap();

        std::fs::write(
            temporary_index.path.join(INDEX_FILE_NAME),
            b"candidate index",
        )
        .unwrap();
        temporary_index.refresh_after_successful_writer().unwrap();
        temporary_index.verify_after_write_tree().unwrap();
        temporary_index.cleanup_known_stable().unwrap();
        fixture.assert_empty();
    }

    #[test]
    fn temporary_index_rejects_a_lock_residue() {
        let fixture = TemporaryIndexFixture::new();
        let mut temporary_index =
            DeliveryTemporaryIndex::create(Arc::clone(&fixture.parent)).unwrap();

        std::fs::write(
            temporary_index.path.join(INDEX_LOCK_FILE_NAME),
            b"stale lock",
        )
        .unwrap();
        assert_eq!(
            temporary_index
                .refresh_after_successful_writer()
                .unwrap_err(),
            DeliverySourceError::SandboxUnavailable
        );

        std::fs::remove_file(temporary_index.path.join(INDEX_LOCK_FILE_NAME)).unwrap();
        temporary_index.cleanup_known_stable().unwrap();
        fixture.assert_empty();
    }

    #[test]
    fn cleanup_never_deletes_a_foreign_directory_replacement() {
        let fixture = TemporaryIndexFixture::new();
        let mut temporary_index =
            DeliveryTemporaryIndex::create(Arc::clone(&fixture.parent)).unwrap();
        let original = temporary_index.path.clone();
        let moved = fixture.root.join("moved-temporary-index");

        if std::fs::rename(&original, &moved).is_err() {
            temporary_index.cleanup_known_stable().unwrap();
            fixture.assert_empty();
            return;
        }
        std::fs::create_dir(&original).unwrap();
        let foreign = original.join("foreign");
        std::fs::write(&foreign, b"keep").unwrap();

        assert_eq!(
            temporary_index.cleanup_known_stable().unwrap_err(),
            DeliverySourceError::SandboxCleanupUnproven
        );
        assert_eq!(std::fs::read(&foreign).unwrap(), b"keep");

        drop(temporary_index);
        std::fs::remove_dir_all(original).unwrap();
        std::fs::remove_dir_all(moved).unwrap();
        fixture.assert_empty();
    }

    #[test]
    fn cleanup_never_deletes_a_foreign_index_file_replacement() {
        let fixture = TemporaryIndexFixture::new();
        let mut temporary_index =
            DeliveryTemporaryIndex::create(Arc::clone(&fixture.parent)).unwrap();
        let index = temporary_index.path.join(INDEX_FILE_NAME);
        let moved = temporary_index.path.join("moved-index");
        std::fs::write(&index, b"owned index").unwrap();
        temporary_index.refresh_after_successful_writer().unwrap();

        std::fs::rename(&index, &moved).unwrap();
        std::fs::write(&index, b"foreign index").unwrap();

        assert_eq!(
            temporary_index.cleanup_known_stable().unwrap_err(),
            DeliverySourceError::SandboxCleanupUnproven
        );
        assert_eq!(std::fs::read(&index).unwrap(), b"foreign index");

        drop(temporary_index);
        std::fs::remove_dir_all(index.parent().unwrap()).unwrap();
        fixture.assert_empty();
    }

    struct TemporaryIndexFixture {
        _temporary: tempfile::TempDir,
        root: PathBuf,
        parent: Arc<ExecutionDirectory>,
    }

    impl TemporaryIndexFixture {
        fn new() -> Self {
            let target_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target");
            std::fs::create_dir_all(&target_root).unwrap();
            let temporary = tempfile::Builder::new()
                .prefix("delivery-temp-index-")
                .tempdir_in(target_root)
                .unwrap();
            let root = temporary.path().canonicalize().unwrap();
            let parent = Arc::new(ExecutionDirectory::open(&root).unwrap());
            Self {
                _temporary: temporary,
                root,
                parent,
            }
        }

        fn assert_empty(&self) {
            assert_eq!(std::fs::read_dir(&self.root).unwrap().count(), 0);
        }
    }
}
