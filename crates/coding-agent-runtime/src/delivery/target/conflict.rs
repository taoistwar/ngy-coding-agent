use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::io::Read as _;

use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::native_fs::child_entry_exists;
use crate::{RelativePath, RootCapability};

use super::super::command::DeliveryTargetReadCommands;
use super::super::git_state::has_disallowed_git_state;
use super::super::{DeliveryCommitOid, DeliveryTargetError, DeliveryTreeOid};
use super::{
    DeliveryTargetCapability, DeliveryTargetProvisioner, map_source_error, map_worktree_error,
    require_not_cancelled, require_same_security_snapshot, validate_target_path,
};

const MERGE_CONFLICT_ALLOWED_GIT_STATE_ENTRIES: [&str; 5] = [
    "AUTO_MERGE",
    "MERGE_HEAD",
    "MERGE_MODE",
    "MERGE_MSG",
    "MERGE_RR",
];
const MERGE_CONFLICT_INDEX_DIGEST_DOMAIN: &[u8] =
    b"coding-agent.delivery.merge-conflict.full-index.v2\0";
const MERGE_CONFLICT_WORKTREE_DIGEST_DOMAIN: &[u8] =
    b"coding-agent.delivery.merge-conflict.full-worktree.v2\0";

impl DeliveryTargetProvisioner {
    /// Recognizes only the narrow, observable conflict scene that may enter
    /// the durable abort workflow. The result contains canonical digests and
    /// validated raw conflict paths, never raw command output or filesystem
    /// authority. Ordinary stage-0 records are admitted only when every field
    /// matches the object-only old-target -> expected-tree diff; any other
    /// staged/untracked record, unsupported node kind, competing Git
    /// operation, or A/B drift rejects the candidate.
    pub(in super::super) async fn observe_expected_merge_conflict(
        &self,
        target: &DeliveryTargetCapability,
        merge_base: &DeliveryCommitOid,
        source: &DeliveryCommitOid,
        expected_tree: &DeliveryTreeOid,
        cancellation: CancellationToken,
    ) -> Result<Option<StableMergeConflictObservation>, DeliveryTargetError> {
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
        let git_root = target
            .authentication()
            .command_context()
            .checkout_git
            .capability
            .try_clone_capability()
            .map_err(|_| DeliveryTargetError::AuthenticationChanged)?;
        let checkout_root = target.checkout_root()?;
        let scene_context = MergeConflictSceneContext {
            commands: target.commands(),
            git_root: &git_root,
            checkout_root: &checkout_root,
            old_target: target.head(),
            merge_base,
            source,
            expected_tree,
        };
        self.require_control_state(
            target.commands(),
            target.branch_name(),
            target.head(),
            cancellation.clone(),
        )
        .await?;
        let before = self
            .observe_merge_conflict_scene(&scene_context, cancellation.clone())
            .await?;
        self.require_control_state(
            target.commands(),
            target.branch_name(),
            target.head(),
            cancellation.clone(),
        )
        .await?;
        let after = self
            .observe_merge_conflict_scene(&scene_context, cancellation.clone())
            .await?;
        self.require_control_state(
            target.commands(),
            target.branch_name(),
            target.head(),
            cancellation,
        )
        .await?;
        if before != after {
            return Err(DeliveryTargetError::AuthenticationChanged);
        }
        self.finalize_authentication(target.authentication(), &security)?;
        if after.state.autostash
            || after.state.disallowed_operation_state
            || after.state.merge_head.as_deref() != Some(source.as_str())
            || after.state.original_head.as_deref() != Some(target.head().as_str())
        {
            return Ok(None);
        }
        Ok(after.observation)
    }

    /// Captures the complete observable conflict scene through fixed,
    /// read-only Git commands.  A merge exit code and `MERGE_HEAD` alone are
    /// not enough evidence: a later durable abort workflow may only receive a
    /// candidate conflict after the complete expected write-set index and
    /// worktree state have remained stable across the surrounding
    /// control-state proof.
    async fn observe_merge_conflict_scene(
        &self,
        context: &MergeConflictSceneContext<'_>,
        cancellation: CancellationToken,
    ) -> Result<MergeConflictScene, DeliveryTargetError> {
        let state = observe_merge_conflict_state(context.git_root, self.limits.max_status_bytes())?;
        let expected_diff = self
            .executor
            .run(
                context
                    .commands
                    .expected_merge_raw_diff(context.old_target, context.expected_tree)
                    .map_err(map_source_error)?,
                cancellation.clone(),
                self.limits.max_status_bytes(),
            )
            .await
            .map_err(map_source_error)?;
        let unmerged = self
            .executor
            .run(
                context
                    .commands
                    .unmerged_entries()
                    .map_err(map_source_error)?,
                cancellation.clone(),
                self.limits.max_status_bytes(),
            )
            .await
            .map_err(map_source_error)?;
        let status = self
            .executor
            .run(
                context
                    .commands
                    .status_porcelain_v2()
                    .map_err(map_source_error)?,
                cancellation.clone(),
                self.limits.max_status_bytes(),
            )
            .await
            .map_err(map_source_error)?;
        let index = parse_unmerged_index(
            &unmerged,
            context.commands.object_format().hexadecimal_length(),
            self.limits.max_status_bytes(),
            self.limits.max_paths(),
        )?;
        let expected_write_set = parse_expected_merge_raw_diff(
            &expected_diff,
            context.commands.object_format().hexadecimal_length(),
            self.limits.max_status_bytes(),
            self.limits.max_paths(),
        )?;
        let observation = if index.is_empty() && status.is_empty() {
            None
        } else {
            let (requested_paths, typed_paths) =
                expected_conflict_query_paths(&index, &expected_write_set)?;
            let merge_base_entries = self
                .executor
                .run(
                    context
                        .commands
                        .expected_conflict_tree_entries(context.merge_base, &typed_paths)
                        .map_err(map_source_error)?,
                    cancellation.clone(),
                    self.limits.max_status_bytes(),
                )
                .await
                .map_err(map_source_error)?;
            let source_entries = self
                .executor
                .run(
                    context
                        .commands
                        .expected_conflict_tree_entries(context.source, &typed_paths)
                        .map_err(map_source_error)?,
                    cancellation,
                    self.limits.max_status_bytes(),
                )
                .await
                .map_err(map_source_error)?;
            let merge_base_entries = parse_expected_conflict_tree_entries(
                &merge_base_entries,
                &requested_paths,
                context.commands.object_format().hexadecimal_length(),
                self.limits.max_status_bytes(),
                self.limits.max_paths(),
            )?;
            let source_entries = parse_expected_conflict_tree_entries(
                &source_entries,
                &requested_paths,
                context.commands.object_format().hexadecimal_length(),
                self.limits.max_status_bytes(),
                self.limits.max_paths(),
            )?;
            let porcelain = parse_expected_merge_porcelain_v2(
                &status,
                &expected_write_set,
                &merge_base_entries,
                &source_entries,
                context.commands.object_format().hexadecimal_length(),
                self.limits.max_status_bytes(),
                self.limits.max_paths(),
            )?;
            if index.is_empty() || !conflict_index_matches_porcelain(&index, &porcelain) {
                return Err(DeliveryTargetError::AuthenticationChanged);
            }
            let raw_paths = porcelain
                .iter()
                .filter(|(_, entry)| matches!(entry, PorcelainMergeEntry::Conflict(_)))
                .map(|(path, _)| path.clone())
                .collect::<Vec<_>>();
            let index_stages_digest = digest_conflict_index(&porcelain)?;
            let worktree_digest = digest_conflict_worktree(
                context.checkout_root,
                &porcelain,
                self.limits.max_status_bytes(),
            )?;
            Some(StableMergeConflictObservation {
                index_stages_digest,
                worktree_digest,
                raw_paths,
            })
        };
        Ok(MergeConflictScene { state, observation })
    }
}

/// Stable delivery-private evidence for one exact conflicted index/worktree
/// scene. Raw paths remain private so the abort layer can convert them into
/// its own bounded durable path type; neither Git output nor filesystem
/// authority is retained.
#[derive(Clone, PartialEq, Eq)]
pub(in super::super) struct StableMergeConflictObservation {
    index_stages_digest: [u8; 32],
    worktree_digest: [u8; 32],
    raw_paths: Vec<Vec<u8>>,
}

impl StableMergeConflictObservation {
    pub(in super::super) const fn index_stages_digest(&self) -> [u8; 32] {
        self.index_stages_digest
    }

    pub(in super::super) const fn worktree_digest(&self) -> [u8; 32] {
        self.worktree_digest
    }

    pub(in super::super) fn raw_paths(&self) -> &[Vec<u8>] {
        &self.raw_paths
    }
}

#[derive(Clone, PartialEq, Eq)]
struct MergeConflictState {
    merge_head: Option<String>,
    original_head: Option<String>,
    autostash: bool,
    disallowed_operation_state: bool,
}

struct MergeConflictSceneContext<'a> {
    commands: &'a DeliveryTargetReadCommands,
    git_root: &'a RootCapability,
    checkout_root: &'a RootCapability,
    old_target: &'a DeliveryCommitOid,
    merge_base: &'a DeliveryCommitOid,
    source: &'a DeliveryCommitOid,
    expected_tree: &'a DeliveryTreeOid,
}

#[derive(Clone, PartialEq, Eq)]
struct MergeConflictScene {
    state: MergeConflictState,
    observation: Option<StableMergeConflictObservation>,
}

#[derive(Clone, PartialEq, Eq)]
struct ConflictIndexEntry {
    mode: Vec<u8>,
    object_id: Vec<u8>,
}

type ConflictIndex = BTreeMap<Vec<u8>, BTreeMap<u8, ConflictIndexEntry>>;

/// Exact blob entries selected from one immutable commit tree for the
/// already-observed conflict paths. A missing key means that path is absent
/// from the tree; no synthetic zero object ID enters the proof.
type ExpectedConflictTree = BTreeMap<Vec<u8>, ConflictIndexEntry>;

/// One path from the fixed raw old-target -> expected-tree diff. Keeping the
/// raw canonical fields lets the porcelain parser bind every ordinary stage-0
/// entry byte-for-byte without exposing a generic revision or path surface.
#[derive(Clone, PartialEq, Eq)]
struct ExpectedMergeWriteEntry {
    status: u8,
    old_mode: Vec<u8>,
    new_mode: Vec<u8>,
    old_object_id: Vec<u8>,
    new_object_id: Vec<u8>,
}

type ExpectedMergeWriteSet = BTreeMap<Vec<u8>, ExpectedMergeWriteEntry>;

#[derive(Clone, Copy, PartialEq, Eq)]
enum ConflictWorktreeMode {
    Absent,
    Regular,
    Executable,
}

impl ConflictWorktreeMode {
    fn parse(mode: &[u8]) -> Result<Self, DeliveryTargetError> {
        match mode {
            b"000000" => Ok(Self::Absent),
            b"100644" => Ok(Self::Regular),
            b"100755" => Ok(Self::Executable),
            _ => Err(DeliveryTargetError::AuthenticationChanged),
        }
    }

    const fn as_git_mode(self) -> &'static [u8] {
        match self {
            Self::Absent => b"000000",
            Self::Regular => b"100644",
            Self::Executable => b"100755",
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
struct PorcelainConflictEntry {
    stages: BTreeMap<u8, ConflictIndexEntry>,
    worktree_mode: ConflictWorktreeMode,
}

#[derive(Clone, PartialEq, Eq)]
struct PorcelainOrdinaryEntry {
    index_mode: Vec<u8>,
    index_object_id: Vec<u8>,
    worktree_mode: ConflictWorktreeMode,
}

#[derive(Clone, PartialEq, Eq)]
enum PorcelainMergeEntry {
    Ordinary(PorcelainOrdinaryEntry),
    Conflict(PorcelainConflictEntry),
}

impl PorcelainMergeEntry {
    const fn worktree_mode(&self) -> ConflictWorktreeMode {
        match self {
            Self::Ordinary(entry) => entry.worktree_mode,
            Self::Conflict(entry) => entry.worktree_mode,
        }
    }
}

type PorcelainMerge = BTreeMap<Vec<u8>, PorcelainMergeEntry>;

fn observe_merge_conflict_state(
    root: &RootCapability,
    limit: usize,
) -> Result<MergeConflictState, DeliveryTargetError> {
    let merge_head = read_exact_git_state_line(root, "MERGE_HEAD", limit)?;
    let original_head = read_exact_git_state_line(root, "ORIG_HEAD", limit)?;
    let autostash = child_entry_exists(
        &root
            .try_clone_root()
            .map_err(|_| DeliveryTargetError::AuthenticationChanged)?,
        OsStr::new("MERGE_AUTOSTASH"),
    )
    .map_err(|_| DeliveryTargetError::AuthenticationChanged)?;
    let operation_root = root
        .try_clone_root()
        .map_err(|_| DeliveryTargetError::AuthenticationChanged)?;
    let disallowed_operation_state =
        has_disallowed_git_state(&operation_root, &MERGE_CONFLICT_ALLOWED_GIT_STATE_ENTRIES)
            .map_err(|_| DeliveryTargetError::AuthenticationChanged)?;
    Ok(MergeConflictState {
        merge_head,
        original_head,
        autostash,
        disallowed_operation_state,
    })
}

fn parse_unmerged_index(
    output: &[u8],
    object_id_length: usize,
    output_limit: usize,
    max_paths: usize,
) -> Result<ConflictIndex, DeliveryTargetError> {
    require_bounded_nul_protocol(output, output_limit)?;
    if output.is_empty() {
        return Ok(BTreeMap::new());
    }

    let mut index = ConflictIndex::new();
    for record in output[..output.len() - 1].split(|byte| *byte == 0) {
        let tab = record
            .iter()
            .position(|byte| *byte == b'\t')
            .ok_or(DeliveryTargetError::AuthenticationChanged)?;
        let (metadata, raw_path) = record.split_at(tab);
        let raw_path = &raw_path[1..];
        validate_conflict_path(raw_path)?;

        let mut fields = metadata.split(|byte| *byte == b' ');
        let mode = fields
            .next()
            .ok_or(DeliveryTargetError::AuthenticationChanged)?;
        let object_id = fields
            .next()
            .ok_or(DeliveryTargetError::AuthenticationChanged)?;
        let stage = fields
            .next()
            .ok_or(DeliveryTargetError::AuthenticationChanged)?;
        if fields.next().is_some()
            || !is_regular_index_mode(mode)
            || !is_canonical_nonzero_object_id(object_id, object_id_length)
            || !matches!(stage, b"1" | b"2" | b"3")
        {
            return Err(DeliveryTargetError::AuthenticationChanged);
        }

        if !index.contains_key(raw_path) && index.len() == max_paths {
            return Err(DeliveryTargetError::BoundsExceeded);
        }
        let stages = index.entry(raw_path.to_vec()).or_default();
        if stages
            .insert(
                stage[0] - b'0',
                ConflictIndexEntry {
                    mode: mode.to_vec(),
                    object_id: object_id.to_vec(),
                },
            )
            .is_some()
        {
            return Err(DeliveryTargetError::AuthenticationChanged);
        }
    }
    Ok(index)
}

fn expected_conflict_query_paths(
    index: &ConflictIndex,
    expected_write_set: &ExpectedMergeWriteSet,
) -> Result<(BTreeSet<Vec<u8>>, Vec<RelativePath>), DeliveryTargetError> {
    if index.is_empty() {
        return Err(DeliveryTargetError::AuthenticationChanged);
    }
    let mut requested = BTreeSet::new();
    let mut typed = Vec::with_capacity(index.len());
    for raw_path in index.keys() {
        if !expected_write_set.contains_key(raw_path) {
            return Err(DeliveryTargetError::AuthenticationChanged);
        }
        let path = std::str::from_utf8(raw_path)
            .map_err(|_| DeliveryTargetError::AuthenticationChanged)?;
        let path = RelativePath::parse(path.to_owned())
            .map_err(|_| DeliveryTargetError::AuthenticationChanged)?;
        if path.is_root() || !requested.insert(raw_path.clone()) {
            return Err(DeliveryTargetError::AuthenticationChanged);
        }
        typed.push(path);
    }
    Ok((requested, typed))
}

fn parse_expected_conflict_tree_entries(
    output: &[u8],
    requested_paths: &BTreeSet<Vec<u8>>,
    object_id_length: usize,
    output_limit: usize,
    max_paths: usize,
) -> Result<ExpectedConflictTree, DeliveryTargetError> {
    require_bounded_nul_protocol(output, output_limit)?;
    if requested_paths.is_empty() || requested_paths.len() > max_paths {
        return Err(DeliveryTargetError::BoundsExceeded);
    }
    if output.is_empty() {
        return Ok(BTreeMap::new());
    }

    let mut entries = ExpectedConflictTree::new();
    for record in output[..output.len() - 1].split(|byte| *byte == 0) {
        let tab = record
            .iter()
            .position(|byte| *byte == b'\t')
            .ok_or(DeliveryTargetError::AuthenticationChanged)?;
        let (metadata, raw_path) = record.split_at(tab);
        let raw_path = &raw_path[1..];
        validate_conflict_path(raw_path)?;
        if !requested_paths.contains(raw_path) {
            return Err(DeliveryTargetError::AuthenticationChanged);
        }

        let mut fields = metadata.split(|byte| *byte == b' ');
        let mode = fields
            .next()
            .ok_or(DeliveryTargetError::AuthenticationChanged)?;
        let kind = fields
            .next()
            .ok_or(DeliveryTargetError::AuthenticationChanged)?;
        let object_id = fields
            .next()
            .ok_or(DeliveryTargetError::AuthenticationChanged)?;
        if fields.next().is_some()
            || kind != b"blob"
            || !is_regular_index_mode(mode)
            || !is_canonical_nonzero_object_id(object_id, object_id_length)
        {
            return Err(DeliveryTargetError::AuthenticationChanged);
        }
        if entries.len() == max_paths
            || entries
                .insert(
                    raw_path.to_vec(),
                    ConflictIndexEntry {
                        mode: mode.to_vec(),
                        object_id: object_id.to_vec(),
                    },
                )
                .is_some()
        {
            return Err(DeliveryTargetError::AuthenticationChanged);
        }
    }
    Ok(entries)
}

fn parse_expected_merge_raw_diff(
    output: &[u8],
    object_id_length: usize,
    output_limit: usize,
    max_paths: usize,
) -> Result<ExpectedMergeWriteSet, DeliveryTargetError> {
    require_bounded_nul_protocol(output, output_limit)?;
    if output.is_empty() {
        return Ok(BTreeMap::new());
    }

    let mut records = output[..output.len() - 1].split(|byte| *byte == 0);
    let mut write_set = ExpectedMergeWriteSet::new();
    while let Some(metadata) = records.next() {
        let raw_path = records
            .next()
            .ok_or(DeliveryTargetError::AuthenticationChanged)?;
        if metadata.first() != Some(&b':') {
            return Err(DeliveryTargetError::AuthenticationChanged);
        }
        let mut fields = metadata[1..].split(|byte| *byte == b' ');
        let old_mode = fields
            .next()
            .ok_or(DeliveryTargetError::AuthenticationChanged)?;
        let new_mode = fields
            .next()
            .ok_or(DeliveryTargetError::AuthenticationChanged)?;
        let old_object_id = fields
            .next()
            .ok_or(DeliveryTargetError::AuthenticationChanged)?;
        let new_object_id = fields
            .next()
            .ok_or(DeliveryTargetError::AuthenticationChanged)?;
        let status = fields
            .next()
            .ok_or(DeliveryTargetError::AuthenticationChanged)?;
        if fields.next().is_some() || status.len() != 1 {
            return Err(DeliveryTargetError::AuthenticationChanged);
        }

        let old_present = validate_raw_tree_entry(old_mode, old_object_id, object_id_length)?;
        let new_present = validate_raw_tree_entry(new_mode, new_object_id, object_id_length)?;
        let valid_status = match status[0] {
            b'A' => !old_present && new_present,
            b'D' => old_present && !new_present,
            b'M' => {
                old_present
                    && new_present
                    && (old_mode != new_mode || old_object_id != new_object_id)
            }
            _ => false,
        };
        if !valid_status {
            return Err(DeliveryTargetError::AuthenticationChanged);
        }
        validate_conflict_path(raw_path)?;
        if write_set.contains_key(raw_path) {
            return Err(DeliveryTargetError::AuthenticationChanged);
        }
        if write_set.len() == max_paths {
            return Err(DeliveryTargetError::BoundsExceeded);
        }
        write_set.insert(
            raw_path.to_vec(),
            ExpectedMergeWriteEntry {
                status: status[0],
                old_mode: old_mode.to_vec(),
                new_mode: new_mode.to_vec(),
                old_object_id: old_object_id.to_vec(),
                new_object_id: new_object_id.to_vec(),
            },
        );
    }
    Ok(write_set)
}

fn validate_raw_tree_entry(
    mode: &[u8],
    object_id: &[u8],
    object_id_length: usize,
) -> Result<bool, DeliveryTargetError> {
    if mode == b"000000" && is_zero_object_id(object_id, object_id_length) {
        return Ok(false);
    }
    if is_regular_index_mode(mode) && is_canonical_nonzero_object_id(object_id, object_id_length) {
        return Ok(true);
    }
    Err(DeliveryTargetError::AuthenticationChanged)
}

fn parse_expected_merge_porcelain_v2(
    output: &[u8],
    expected: &ExpectedMergeWriteSet,
    merge_base_entries: &ExpectedConflictTree,
    source_entries: &ExpectedConflictTree,
    object_id_length: usize,
    output_limit: usize,
    max_paths: usize,
) -> Result<PorcelainMerge, DeliveryTargetError> {
    require_bounded_nul_protocol(output, output_limit)?;
    if output.is_empty() {
        return Err(DeliveryTargetError::AuthenticationChanged);
    }

    let mut index = PorcelainMerge::new();
    for record in output[..output.len() - 1].split(|byte| *byte == 0) {
        let (raw_path, entry) = match record.first() {
            Some(b'1') => {
                parse_expected_ordinary_porcelain_record(record, expected, object_id_length)?
            }
            Some(b'u') => parse_expected_unmerged_porcelain_record(
                record,
                expected,
                merge_base_entries,
                source_entries,
                object_id_length,
            )?,
            _ => return Err(DeliveryTargetError::AuthenticationChanged),
        };
        if index.contains_key(raw_path) {
            return Err(DeliveryTargetError::AuthenticationChanged);
        }
        if index.len() == max_paths {
            return Err(DeliveryTargetError::BoundsExceeded);
        }
        index.insert(raw_path.to_vec(), entry);
    }

    let has_conflict = index
        .values()
        .any(|entry| matches!(entry, PorcelainMergeEntry::Conflict(_)));
    if !has_conflict || index.len() != expected.len() || !expected.keys().eq(index.keys()) {
        return Err(DeliveryTargetError::AuthenticationChanged);
    }
    Ok(index)
}

fn parse_expected_ordinary_porcelain_record<'a>(
    record: &'a [u8],
    expected: &ExpectedMergeWriteSet,
    object_id_length: usize,
) -> Result<(&'a [u8], PorcelainMergeEntry), DeliveryTargetError> {
    let (fields, raw_path) = split_porcelain_v2_record::<8>(record)?;
    validate_conflict_path(raw_path)?;
    let expected = expected
        .get(raw_path)
        .ok_or(DeliveryTargetError::AuthenticationChanged)?;
    if fields[0] != b"1"
        || fields[1].len() != 2
        || fields[1][0] != expected.status
        || fields[1][1] != b'.'
        || fields[2] != b"N..."
        || fields[3] != expected.old_mode
        || fields[4] != expected.new_mode
        || fields[5] != expected.new_mode
        || fields[6] != expected.old_object_id
        || fields[7] != expected.new_object_id
    {
        return Err(DeliveryTargetError::AuthenticationChanged);
    }
    let worktree_mode = ConflictWorktreeMode::parse(fields[5])?;
    let _ = validate_raw_tree_entry(fields[4], fields[7], object_id_length)?;
    Ok((
        raw_path,
        PorcelainMergeEntry::Ordinary(PorcelainOrdinaryEntry {
            index_mode: fields[4].to_vec(),
            index_object_id: fields[7].to_vec(),
            worktree_mode,
        }),
    ))
}

fn parse_expected_unmerged_porcelain_record<'a>(
    record: &'a [u8],
    expected: &ExpectedMergeWriteSet,
    merge_base_entries: &ExpectedConflictTree,
    source_entries: &ExpectedConflictTree,
    object_id_length: usize,
) -> Result<(&'a [u8], PorcelainMergeEntry), DeliveryTargetError> {
    let (fields, raw_path) = split_porcelain_v2_record::<10>(record)?;
    validate_conflict_path(raw_path)?;
    let expected = expected
        .get(raw_path)
        .ok_or(DeliveryTargetError::AuthenticationChanged)?;
    if fields[0] != b"u" || fields[2] != b"N..." {
        return Err(DeliveryTargetError::AuthenticationChanged);
    }
    let worktree_mode = ConflictWorktreeMode::parse(fields[6])?;

    let mut stages = BTreeMap::new();
    for (stage, mode_index, object_index) in [(1u8, 3usize, 7usize), (2, 4, 8), (3, 5, 9)] {
        if let Some(entry) = parse_porcelain_conflict_stage(
            fields[mode_index],
            fields[object_index],
            object_id_length,
        )? {
            stages.insert(stage, entry);
        }
    }
    let expected_stages = exact_expected_conflict_stages(
        raw_path,
        expected,
        merge_base_entries,
        source_entries,
        object_id_length,
    )?;
    if stages != expected_stages || !conflict_xy_matches_stages(fields[1], &expected_stages) {
        return Err(DeliveryTargetError::AuthenticationChanged);
    }
    Ok((
        raw_path,
        PorcelainMergeEntry::Conflict(PorcelainConflictEntry {
            stages,
            worktree_mode,
        }),
    ))
}

fn exact_expected_conflict_stages(
    raw_path: &[u8],
    expected: &ExpectedMergeWriteEntry,
    merge_base_entries: &ExpectedConflictTree,
    source_entries: &ExpectedConflictTree,
    object_id_length: usize,
) -> Result<BTreeMap<u8, ConflictIndexEntry>, DeliveryTargetError> {
    let mut stages = BTreeMap::new();
    if let Some(entry) = merge_base_entries.get(raw_path) {
        stages.insert(1, entry.clone());
    }
    if expected.old_mode == b"000000" && expected.old_object_id.iter().all(|byte| *byte == b'0') {
        // Absent old-target entries have no stage 2.
    } else if is_regular_index_mode(&expected.old_mode)
        && is_canonical_nonzero_object_id(&expected.old_object_id, object_id_length)
    {
        stages.insert(
            2,
            ConflictIndexEntry {
                mode: expected.old_mode.clone(),
                object_id: expected.old_object_id.clone(),
            },
        );
    } else {
        return Err(DeliveryTargetError::AuthenticationChanged);
    }
    if let Some(entry) = source_entries.get(raw_path) {
        stages.insert(3, entry.clone());
    }
    if stages.is_empty() {
        return Err(DeliveryTargetError::AuthenticationChanged);
    }
    Ok(stages)
}

fn conflict_xy_matches_stages(xy: &[u8], stages: &BTreeMap<u8, ConflictIndexEntry>) -> bool {
    let present = stages.keys().copied().collect::<Vec<_>>();
    match xy {
        b"DD" => present.as_slice() == [1],
        b"AU" => present.as_slice() == [2],
        b"UD" => present.as_slice() == [1, 2],
        b"UA" => present.as_slice() == [3],
        b"DU" => present.as_slice() == [1, 3],
        b"AA" => present.as_slice() == [2, 3],
        b"UU" => present.as_slice() == [1, 2, 3],
        _ => false,
    }
}

fn conflict_index_matches_porcelain(index: &ConflictIndex, porcelain: &PorcelainMerge) -> bool {
    let conflict_count = porcelain
        .values()
        .filter(|entry| matches!(entry, PorcelainMergeEntry::Conflict(_)))
        .count();
    index.len() == conflict_count
        && index.iter().all(|(path, stages)| {
            matches!(
                porcelain.get(path),
                Some(PorcelainMergeEntry::Conflict(entry)) if entry.stages == *stages
            )
        })
}

fn require_bounded_nul_protocol(
    output: &[u8],
    output_limit: usize,
) -> Result<(), DeliveryTargetError> {
    if output.len() > output_limit {
        return Err(DeliveryTargetError::BoundsExceeded);
    }
    if !output.is_empty() && output.last() != Some(&0) {
        return Err(DeliveryTargetError::AuthenticationChanged);
    }
    Ok(())
}

fn split_porcelain_v2_record<const FIELD_COUNT: usize>(
    record: &[u8],
) -> Result<([&[u8]; FIELD_COUNT], &[u8]), DeliveryTargetError> {
    let mut fields: [&[u8]; FIELD_COUNT] = [&[]; FIELD_COUNT];
    let mut remaining = record;
    for field in &mut fields {
        let separator = remaining
            .iter()
            .position(|byte| *byte == b' ')
            .ok_or(DeliveryTargetError::AuthenticationChanged)?;
        *field = &remaining[..separator];
        remaining = &remaining[separator + 1..];
    }
    if remaining.is_empty() {
        return Err(DeliveryTargetError::AuthenticationChanged);
    }
    Ok((fields, remaining))
}

fn parse_porcelain_conflict_stage(
    mode: &[u8],
    object_id: &[u8],
    object_id_length: usize,
) -> Result<Option<ConflictIndexEntry>, DeliveryTargetError> {
    let absent = mode == b"000000" && is_zero_object_id(object_id, object_id_length);
    if absent {
        return Ok(None);
    }
    if is_regular_index_mode(mode) && is_canonical_nonzero_object_id(object_id, object_id_length) {
        return Ok(Some(ConflictIndexEntry {
            mode: mode.to_vec(),
            object_id: object_id.to_vec(),
        }));
    }
    Err(DeliveryTargetError::AuthenticationChanged)
}

fn is_regular_index_mode(mode: &[u8]) -> bool {
    matches!(mode, b"100644" | b"100755")
}

fn is_canonical_nonzero_object_id(value: &[u8], hexadecimal_length: usize) -> bool {
    value.len() == hexadecimal_length
        && value
            .iter()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        && !value.iter().all(|byte| *byte == b'0')
}

fn is_zero_object_id(value: &[u8], hexadecimal_length: usize) -> bool {
    value.len() == hexadecimal_length && value.iter().all(|byte| *byte == b'0')
}

fn validate_conflict_path(path: &[u8]) -> Result<(), DeliveryTargetError> {
    validate_target_path(path)?;
    let path = std::str::from_utf8(path).map_err(|_| DeliveryTargetError::AuthenticationChanged)?;
    RelativePath::parse(path.to_owned()).map_err(|_| DeliveryTargetError::AuthenticationChanged)?;
    Ok(())
}

fn digest_conflict_index(index: &PorcelainMerge) -> Result<[u8; 32], DeliveryTargetError> {
    let mut digest = Sha256::new();
    digest.update(MERGE_CONFLICT_INDEX_DIGEST_DOMAIN);
    append_digest_usize(&mut digest, index.len())?;
    for (path, entry) in index {
        append_digest_frame(&mut digest, path)?;
        match entry {
            PorcelainMergeEntry::Ordinary(entry) => {
                digest.update([0]);
                append_digest_frame(&mut digest, &entry.index_mode)?;
                append_digest_frame(&mut digest, &entry.index_object_id)?;
            }
            PorcelainMergeEntry::Conflict(entry) => {
                digest.update([1]);
                append_digest_usize(&mut digest, entry.stages.len())?;
                for (stage, entry) in &entry.stages {
                    digest.update([*stage]);
                    append_digest_frame(&mut digest, &entry.mode)?;
                    append_digest_frame(&mut digest, &entry.object_id)?;
                }
            }
        }
    }
    Ok(digest.finalize().into())
}

fn digest_conflict_worktree(
    root: &RootCapability,
    porcelain: &PorcelainMerge,
    byte_limit: usize,
) -> Result<[u8; 32], DeliveryTargetError> {
    let mut digest = Sha256::new();
    digest.update(MERGE_CONFLICT_WORKTREE_DIGEST_DOMAIN);
    append_digest_usize(&mut digest, porcelain.len())?;
    let mut total_bytes = 0usize;
    for (raw_path, entry) in porcelain {
        let worktree_mode = entry.worktree_mode();
        append_digest_frame(&mut digest, raw_path)?;
        append_digest_frame(&mut digest, worktree_mode.as_git_mode())?;
        let text = std::str::from_utf8(raw_path)
            .map_err(|_| DeliveryTargetError::AuthenticationChanged)?;
        let relative = RelativePath::parse(text.to_owned())
            .map_err(|_| DeliveryTargetError::AuthenticationChanged)?;
        let mut file = match root.open_file_for_read(&relative) {
            Ok(file) => file,
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound
                    && worktree_mode == ConflictWorktreeMode::Absent =>
            {
                digest.update([0]);
                continue;
            }
            Err(_) => return Err(DeliveryTargetError::AuthenticationChanged),
        };
        let metadata = file
            .metadata()
            .map_err(|_| DeliveryTargetError::AuthenticationChanged)?;
        if !worktree_metadata_matches_mode(&metadata, worktree_mode) {
            return Err(DeliveryTargetError::AuthenticationChanged);
        }
        let expected_length =
            usize::try_from(metadata.len()).map_err(|_| DeliveryTargetError::BoundsExceeded)?;
        total_bytes = total_bytes
            .checked_add(expected_length)
            .ok_or(DeliveryTargetError::BoundsExceeded)?;
        if total_bytes > byte_limit {
            return Err(DeliveryTargetError::BoundsExceeded);
        }

        digest.update([1]);
        append_digest_usize(&mut digest, expected_length)?;
        let read_limit = u64::try_from(expected_length)
            .ok()
            .and_then(|length| length.checked_add(1))
            .ok_or(DeliveryTargetError::BoundsExceeded)?;
        let mut reader = file.by_ref().take(read_limit);
        let mut observed_length = 0usize;
        let mut buffer = [0u8; 8 * 1024];
        loop {
            let read = reader
                .read(&mut buffer)
                .map_err(|_| DeliveryTargetError::AuthenticationChanged)?;
            if read == 0 {
                break;
            }
            observed_length = observed_length
                .checked_add(read)
                .ok_or(DeliveryTargetError::BoundsExceeded)?;
            digest.update(&buffer[..read]);
        }
        if observed_length != expected_length {
            return Err(DeliveryTargetError::AuthenticationChanged);
        }
        let repeated = file
            .metadata()
            .map_err(|_| DeliveryTargetError::AuthenticationChanged)?;
        if !worktree_metadata_matches_mode(&repeated, worktree_mode)
            || repeated.len() != metadata.len()
        {
            return Err(DeliveryTargetError::AuthenticationChanged);
        }
    }
    Ok(digest.finalize().into())
}

fn worktree_metadata_matches_mode(
    metadata: &std::fs::Metadata,
    expected: ConflictWorktreeMode,
) -> bool {
    if !metadata.is_file() || expected == ConflictWorktreeMode::Absent {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;

        let executable = metadata.mode() & 0o111 != 0;
        executable == (expected == ConflictWorktreeMode::Executable)
    }
    #[cfg(not(unix))]
    {
        // Git's porcelain `mW` is the authoritative executable-mode
        // observation on platforms whose native metadata has no Unix mode
        // bits. The no-follow handle still proves the node is a regular file.
        true
    }
}

fn append_digest_usize(digest: &mut Sha256, value: usize) -> Result<(), DeliveryTargetError> {
    let value = u64::try_from(value).map_err(|_| DeliveryTargetError::BoundsExceeded)?;
    digest.update(value.to_be_bytes());
    Ok(())
}

fn append_digest_frame(digest: &mut Sha256, value: &[u8]) -> Result<(), DeliveryTargetError> {
    append_digest_usize(digest, value.len())?;
    digest.update(value);
    Ok(())
}

fn read_exact_git_state_line(
    root: &RootCapability,
    name: &str,
    limit: usize,
) -> Result<Option<String>, DeliveryTargetError> {
    let path = RelativePath::parse(name.to_owned())
        .map_err(|_| DeliveryTargetError::AuthenticationChanged)?;
    let mut file = match root.open_file_for_read(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(DeliveryTargetError::AuthenticationChanged),
    };
    let read_limit = u64::try_from(limit)
        .ok()
        .and_then(|value| value.checked_add(1))
        .ok_or(DeliveryTargetError::BoundsExceeded)?;
    let mut bytes = Vec::with_capacity(limit.min(256));
    file.by_ref()
        .take(read_limit)
        .read_to_end(&mut bytes)
        .map_err(|_| DeliveryTargetError::AuthenticationChanged)?;
    if bytes.len() > limit {
        return Err(DeliveryTargetError::BoundsExceeded);
    }
    let value = parse_exact_git_state_line(&bytes)?;
    Ok(Some(value.to_owned()))
}

/// Git control-state files are LF-delimited protocol records, not platform
/// text files.  CRLF would make the exact stored commit value ambiguous, so
/// unlike general command output this parser rejects it rather than trimming.
fn parse_exact_git_state_line(output: &[u8]) -> Result<&str, DeliveryTargetError> {
    let value = output.strip_suffix(b"\n").unwrap_or(output);
    if value.is_empty() || value.contains(&0) || value.contains(&b'\n') || value.contains(&b'\r') {
        return Err(DeliveryTargetError::AuthenticationChanged);
    }
    std::str::from_utf8(value).map_err(|_| DeliveryTargetError::AuthenticationChanged)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expected_conflict_tree_entry(
        path: &str,
        mode: &str,
        object_id: &str,
    ) -> ExpectedConflictTree {
        BTreeMap::from([(
            path.as_bytes().to_vec(),
            ConflictIndexEntry {
                mode: mode.as_bytes().to_vec(),
                object_id: object_id.as_bytes().to_vec(),
            },
        )])
    }

    fn expected_conflict_trees_from_index(
        index: &ConflictIndex,
    ) -> (ExpectedConflictTree, ExpectedConflictTree) {
        let select_stage = |stage| {
            index
                .iter()
                .filter_map(|(path, stages)| {
                    stages
                        .get(&stage)
                        .map(|entry| (path.clone(), entry.clone()))
                })
                .collect()
        };
        (select_stage(1), select_stage(3))
    }

    #[test]
    fn mixed_conflict_parser_binds_stage_zero_and_projects_only_unmerged_paths() {
        let base = "a".repeat(40);
        let ours = "b".repeat(40);
        let theirs = "c".repeat(40);
        let clean_old = "d".repeat(40);
        let clean_new = "e".repeat(40);
        let expected_conflict = "f".repeat(40);
        let raw = format!(
            ":100644 100644 {clean_old} {clean_new} M\0clean.txt\0\
             :100755 100644 {ours} {expected_conflict} M\0dir/conflict file\0"
        );
        let unmerged = format!(
            "100644 {base} 1\tdir/conflict file\0\
             100755 {ours} 2\tdir/conflict file\0\
             100644 {theirs} 3\tdir/conflict file\0"
        );
        let status = format!(
            "1 M. N... 100644 100644 100644 {clean_old} {clean_new} clean.txt\0\
             u UU N... 100644 100755 100644 100755 {base} {ours} {theirs} \
             dir/conflict file\0"
        );
        let expected = parse_expected_merge_raw_diff(raw.as_bytes(), 40, 16_384, 8).unwrap();
        let index = parse_unmerged_index(unmerged.as_bytes(), 40, 16_384, 8).unwrap();
        let (merge_base_entries, source_entries) = expected_conflict_trees_from_index(&index);
        let porcelain = parse_expected_merge_porcelain_v2(
            status.as_bytes(),
            &expected,
            &merge_base_entries,
            &source_entries,
            40,
            16_384,
            8,
        )
        .unwrap();
        assert!(conflict_index_matches_porcelain(&index, &porcelain));
        assert_eq!(index.len(), 1);
        assert_eq!(index.values().next().unwrap().len(), 3);
        assert_eq!(porcelain.len(), 2);
        let conflict_paths = porcelain
            .iter()
            .filter(|(_, entry)| matches!(entry, PorcelainMergeEntry::Conflict(_)))
            .map(|(path, _)| path.as_slice())
            .collect::<Vec<_>>();
        assert_eq!(conflict_paths, vec![b"dir/conflict file".as_slice()]);
        assert!(matches!(
            porcelain.get(b"dir/conflict file".as_slice()),
            Some(PorcelainMergeEntry::Conflict(entry))
                if entry.worktree_mode == ConflictWorktreeMode::Executable
        ));
        assert_ne!(
            digest_conflict_index(&porcelain).unwrap(),
            digest_conflict_index(
                &parse_expected_merge_porcelain_v2(
                    status.replace(&clean_new, &"9".repeat(40)).as_bytes(),
                    &parse_expected_merge_raw_diff(
                        raw.replace(&clean_new, &"9".repeat(40)).as_bytes(),
                        40,
                        16_384,
                        8,
                    )
                    .unwrap(),
                    &merge_base_entries,
                    &source_entries,
                    40,
                    16_384,
                    8,
                )
                .unwrap(),
            )
            .unwrap()
        );
    }

    #[test]
    fn raw_expected_write_set_parser_rejects_noncanonical_or_renamed_entries() {
        let zero = "0".repeat(40);
        let old = "a".repeat(40);
        let new = "b".repeat(40);
        let valid = format!(":100644 100755 {old} {new} M\0file.txt\0");
        assert_eq!(
            parse_expected_merge_raw_diff(valid.as_bytes(), 40, 8_192, 1)
                .unwrap()
                .len(),
            1
        );
        for invalid in [
            format!(":100644 100644 {old} {new} R100\0file.txt\0old.txt\0"),
            format!(":100644 100644 {} {new} M\0file.txt\0", "A".repeat(40)),
            format!(":000000 100644 {old} {new} A\0file.txt\0"),
            format!(":100644 000000 {old} {zero} M\0file.txt\0"),
            format!(":120000 100644 {old} {new} T\0file.txt\0"),
            format!(":100644 100644 {old} {new} M\0../escape\0"),
        ] {
            assert!(parse_expected_merge_raw_diff(invalid.as_bytes(), 40, 8_192, 8).is_err());
        }
    }

    #[test]
    fn expected_conflict_tree_parser_is_exact_bounded_and_treats_omission_as_absence() {
        let regular = "a".repeat(40);
        let executable = "b".repeat(40);
        let requested = [
            b"file.txt".to_vec(),
            b"exec.sh".to_vec(),
            b"missing.txt".to_vec(),
        ]
        .into_iter()
        .collect::<BTreeSet<_>>();
        let output = format!(
            "100644 blob {regular}\tfile.txt\0\
             100755 blob {executable}\texec.sh\0"
        );
        let entries =
            parse_expected_conflict_tree_entries(output.as_bytes(), &requested, 40, 8_192, 3)
                .unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[b"file.txt".as_slice()].mode, b"100644");
        assert_eq!(entries[b"exec.sh".as_slice()].mode, b"100755");
        assert!(!entries.contains_key(b"missing.txt".as_slice()));
        assert!(
            parse_expected_conflict_tree_entries(b"", &requested, 40, 8_192, 3)
                .unwrap()
                .is_empty()
        );
        let sha256 = "c".repeat(64);
        let sha256_requested = [b"sha256.txt".to_vec()]
            .into_iter()
            .collect::<BTreeSet<_>>();
        assert_eq!(
            parse_expected_conflict_tree_entries(
                format!("100644 blob {sha256}\tsha256.txt\0").as_bytes(),
                &sha256_requested,
                64,
                8_192,
                1,
            )
            .unwrap()
            .len(),
            1
        );

        for invalid in [
            format!("040000 tree {regular}\tfile.txt\0"),
            format!("120000 blob {regular}\tfile.txt\0"),
            format!("160000 commit {regular}\tfile.txt\0"),
            format!("100644 blob {}\tfile.txt\0", "a".repeat(39)),
            format!("100644 blob {}\tfile.txt\0", "A".repeat(40)),
            format!("100644 blob {}\tfile.txt\0", "0".repeat(40)),
            format!("100644 blob {regular}\tunrequested.txt\0"),
            format!(
                "100644 blob {regular}\tfile.txt\0\
                 100644 blob {regular}\tfile.txt\0"
            ),
            format!("100644  blob {regular}\tfile.txt\0"),
            format!("100644 blob {regular}\t../escape\0"),
            format!("100644 blob {regular}\tfile.txt"),
        ] {
            assert!(
                parse_expected_conflict_tree_entries(invalid.as_bytes(), &requested, 40, 8_192, 3,)
                    .is_err()
            );
        }
        assert!(matches!(
            parse_expected_conflict_tree_entries(output.as_bytes(), &requested, 40, 16, 3),
            Err(DeliveryTargetError::BoundsExceeded)
        ));
        assert!(matches!(
            parse_expected_conflict_tree_entries(output.as_bytes(), &requested, 40, 8_192, 2),
            Err(DeliveryTargetError::BoundsExceeded)
        ));
    }

    #[test]
    fn conflict_tree_queries_are_limited_to_expected_merge_write_paths() {
        let base = "a".repeat(40);
        let target = "b".repeat(40);
        let merged = "c".repeat(40);
        let index = parse_unmerged_index(
            format!("100644 {base} 1\tconflict.txt\0").as_bytes(),
            40,
            8_192,
            1,
        )
        .unwrap();
        let expected = parse_expected_merge_raw_diff(
            format!(":100644 100644 {target} {merged} M\0conflict.txt\0").as_bytes(),
            40,
            8_192,
            1,
        )
        .unwrap();
        let (requested, typed) = expected_conflict_query_paths(&index, &expected).unwrap();
        let exact_requested = [b"conflict.txt".to_vec()]
            .into_iter()
            .collect::<BTreeSet<_>>();
        assert_eq!(requested, exact_requested);
        assert_eq!(typed[0].as_slash_str(), "conflict.txt");

        let unrelated = parse_expected_merge_raw_diff(
            format!(":100644 100644 {target} {merged} M\0other.txt\0").as_bytes(),
            40,
            8_192,
            1,
        )
        .unwrap();
        assert!(expected_conflict_query_paths(&index, &unrelated).is_err());
    }

    #[test]
    fn conflict_stage_attribution_binds_base_target_source_and_xy_presence() {
        let zero = "0".repeat(40);
        let base = "a".repeat(40);
        let target = "b".repeat(40);
        let source = "c".repeat(40);
        let merged = "d".repeat(40);

        for (xy, presence) in [
            ("DD", &[1u8][..]),
            ("AU", &[2][..]),
            ("UD", &[1, 2][..]),
            ("UA", &[3][..]),
            ("DU", &[1, 3][..]),
            ("AA", &[2, 3][..]),
            ("UU", &[1, 2, 3][..]),
        ] {
            let present = |stage| presence.contains(&stage);
            let mode = |stage| if present(stage) { "100644" } else { "000000" };
            let object = |stage| match stage {
                1 if present(1) => base.as_str(),
                2 if present(2) => target.as_str(),
                3 if present(3) => source.as_str(),
                _ => zero.as_str(),
            };
            let raw = if present(2) {
                format!(":100644 100644 {target} {merged} M\0conflict.txt\0")
            } else {
                format!(":000000 100644 {zero} {merged} A\0conflict.txt\0")
            };
            let expected = parse_expected_merge_raw_diff(raw.as_bytes(), 40, 8_192, 1).unwrap();
            let merge_base_entries = if present(1) {
                expected_conflict_tree_entry("conflict.txt", "100644", &base)
            } else {
                BTreeMap::new()
            };
            let source_entries = if present(3) {
                expected_conflict_tree_entry("conflict.txt", "100644", &source)
            } else {
                BTreeMap::new()
            };
            let status = format!(
                "u {xy} N... {} {} {} 100644 {} {} {} conflict.txt\0",
                mode(1),
                mode(2),
                mode(3),
                object(1),
                object(2),
                object(3),
            );
            assert!(
                parse_expected_merge_porcelain_v2(
                    status.as_bytes(),
                    &expected,
                    &merge_base_entries,
                    &source_entries,
                    40,
                    8_192,
                    1,
                )
                .is_ok(),
                "canonical {xy} presence must be accepted"
            );
        }

        let expected = parse_expected_merge_raw_diff(
            format!(":100644 100644 {target} {merged} M\0conflict.txt\0").as_bytes(),
            40,
            8_192,
            1,
        )
        .unwrap();
        let exact_status = format!(
            "u UU N... 100644 100644 100644 100644 {base} {target} {source} conflict.txt\0"
        );
        let exact_base = expected_conflict_tree_entry("conflict.txt", "100644", &base);
        let exact_source = expected_conflict_tree_entry("conflict.txt", "100644", &source);
        assert!(
            parse_expected_merge_porcelain_v2(
                exact_status.as_bytes(),
                &expected,
                &exact_base,
                &exact_source,
                40,
                8_192,
                1,
            )
            .is_ok()
        );

        for (base_mode, base_oid, source_mode, source_oid) in [
            ("100755", base.as_str(), "100644", source.as_str()),
            ("100644", merged.as_str(), "100644", source.as_str()),
            ("100644", base.as_str(), "100755", source.as_str()),
            ("100644", base.as_str(), "100644", merged.as_str()),
        ] {
            let changed_base = expected_conflict_tree_entry("conflict.txt", base_mode, base_oid);
            let changed_source =
                expected_conflict_tree_entry("conflict.txt", source_mode, source_oid);
            assert!(
                parse_expected_merge_porcelain_v2(
                    exact_status.as_bytes(),
                    &expected,
                    &changed_base,
                    &changed_source,
                    40,
                    8_192,
                    1,
                )
                .is_err()
            );
        }
        assert!(
            parse_expected_merge_porcelain_v2(
                exact_status.replace("u UU", "u AA").as_bytes(),
                &expected,
                &exact_base,
                &exact_source,
                40,
                8_192,
                1,
            )
            .is_err()
        );
        for missing_stage in [
            format!(
                "u UU N... 000000 100644 100644 100644 {zero} {target} {source} conflict.txt\0"
            ),
            format!("u UU N... 100644 000000 100644 100644 {base} {zero} {source} conflict.txt\0"),
            format!("u UU N... 100644 100644 000000 100644 {base} {target} {zero} conflict.txt\0"),
        ] {
            assert!(
                parse_expected_merge_porcelain_v2(
                    missing_stage.as_bytes(),
                    &expected,
                    &exact_base,
                    &exact_source,
                    40,
                    8_192,
                    1,
                )
                .is_err()
            );
        }
    }

    #[test]
    fn conflict_parsers_accept_an_absent_stage_but_reject_noncanonical_objects() {
        let base = "a".repeat(40);
        let theirs = "b".repeat(40);
        let expected_new = "c".repeat(40);
        let zero = "0".repeat(40);
        let unmerged = format!(
            "100644 {base} 1\tdeleted.txt\0\
             100644 {theirs} 3\tdeleted.txt\0"
        );
        let raw = format!(":000000 100644 {zero} {expected_new} A\0deleted.txt\0");
        let status =
            format!("u DU N... 100644 000000 100644 100644 {base} {zero} {theirs} deleted.txt\0");
        let expected = parse_expected_merge_raw_diff(raw.as_bytes(), 40, 8_192, 1).unwrap();
        let index = parse_unmerged_index(unmerged.as_bytes(), 40, 8_192, 1).unwrap();
        let (merge_base_entries, source_entries) = expected_conflict_trees_from_index(&index);
        let porcelain = parse_expected_merge_porcelain_v2(
            status.as_bytes(),
            &expected,
            &merge_base_entries,
            &source_entries,
            40,
            8_192,
            1,
        )
        .unwrap();
        assert!(conflict_index_matches_porcelain(&index, &porcelain));

        let uppercase = format!("100644 {} 1\tdeleted.txt\0", "A".repeat(40));
        assert!(parse_unmerged_index(uppercase.as_bytes(), 40, 8_192, 1).is_err());
        let zero_index = format!("100644 {zero} 1\tdeleted.txt\0");
        assert!(parse_unmerged_index(zero_index.as_bytes(), 40, 8_192, 1).is_err());
    }

    #[test]
    fn conflict_porcelain_requires_exact_expected_path_set_and_stage_zero() {
        let zero = "0".repeat(40);
        let old = "a".repeat(40);
        let new = "b".repeat(40);
        let base = "c".repeat(40);
        let theirs = "d".repeat(40);
        let expected_output = format!(
            ":100644 100644 {old} {new} M\0ordinary.txt\0\
             :000000 100644 {zero} {new} A\0conflict.txt\0"
        );
        let expected =
            parse_expected_merge_raw_diff(expected_output.as_bytes(), 40, 8_192, 8).unwrap();
        let conflict =
            format!("u DU N... 100644 000000 100644 100644 {base} {zero} {theirs} conflict.txt\0");
        let exact = format!("1 M. N... 100644 100644 100644 {old} {new} ordinary.txt\0{conflict}");
        let merge_base_entries = expected_conflict_tree_entry("conflict.txt", "100644", &base);
        let source_entries = expected_conflict_tree_entry("conflict.txt", "100644", &theirs);
        assert!(
            parse_expected_merge_porcelain_v2(
                exact.as_bytes(),
                &expected,
                &merge_base_entries,
                &source_entries,
                40,
                8_192,
                8,
            )
            .is_ok()
        );
        for output in [
            conflict.clone(),
            exact.replace("1 M.", "1 .M"),
            exact.replace(&new, &"e".repeat(40)),
            format!("{exact}? untracked.txt\0"),
            format!("{exact}! ignored.txt\0"),
            format!("{exact}2 R. N... 100644 100644 100644 {old} {new} R100 moved.txt\0old.txt\0"),
        ] {
            assert!(
                parse_expected_merge_porcelain_v2(
                    output.as_bytes(),
                    &expected,
                    &merge_base_entries,
                    &source_entries,
                    40,
                    8_192,
                    8,
                )
                .is_err()
            );
        }
    }

    #[test]
    fn conflict_index_rejects_duplicate_stages_and_path_bounds() {
        let object = "a".repeat(40);
        let duplicate = format!(
            "100644 {object} 2\tone.txt\0\
             100644 {object} 2\tone.txt\0"
        );
        assert!(parse_unmerged_index(duplicate.as_bytes(), 40, 8_192, 8).is_err());
        let two_paths = format!(
            "100644 {object} 2\tone.txt\0\
             100644 {object} 3\ttwo.txt\0"
        );
        assert!(matches!(
            parse_unmerged_index(two_paths.as_bytes(), 40, 8_192, 1),
            Err(DeliveryTargetError::BoundsExceeded)
        ));
    }

    #[test]
    fn mixed_conflict_worktree_digest_covers_ordinary_and_unmerged_paths() {
        let temporary = tempfile::tempdir().unwrap();
        std::fs::write(temporary.path().join("ordinary.txt"), b"ordinary one\n").unwrap();
        std::fs::write(temporary.path().join("conflict.txt"), b"conflict bytes\n").unwrap();
        let root = RootCapability::open(temporary.path().canonicalize().unwrap()).unwrap();
        let zero = "0".repeat(40);
        let old = "a".repeat(40);
        let new = "b".repeat(40);
        let base = "c".repeat(40);
        let theirs = "d".repeat(40);
        let expected_output = format!(
            ":100644 100644 {old} {new} M\0ordinary.txt\0\
             :000000 100644 {zero} {new} A\0conflict.txt\0"
        );
        let expected =
            parse_expected_merge_raw_diff(expected_output.as_bytes(), 40, 8_192, 8).unwrap();
        let status = format!(
            "1 M. N... 100644 100644 100644 {old} {new} ordinary.txt\0\
             u DU N... 100644 000000 100644 100644 {base} {zero} {theirs} conflict.txt\0"
        );
        let merge_base_entries = expected_conflict_tree_entry("conflict.txt", "100644", &base);
        let source_entries = expected_conflict_tree_entry("conflict.txt", "100644", &theirs);
        let porcelain = parse_expected_merge_porcelain_v2(
            status.as_bytes(),
            &expected,
            &merge_base_entries,
            &source_entries,
            40,
            8_192,
            8,
        )
        .unwrap();
        let first = digest_conflict_worktree(&root, &porcelain, 8_192).unwrap();
        std::fs::write(temporary.path().join("ordinary.txt"), b"ordinary two\n").unwrap();
        let second = digest_conflict_worktree(&root, &porcelain, 8_192).unwrap();
        assert_ne!(first, second);
    }

    #[cfg(unix)]
    #[test]
    fn conflict_worktree_digest_rejects_chmod_drift_and_binds_the_mode() {
        use std::os::unix::fs::PermissionsExt as _;

        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("conflicted.txt");
        std::fs::write(&path, b"conflict bytes\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let root = RootCapability::open(temporary.path().canonicalize().unwrap()).unwrap();
        let base = "a".repeat(40);
        let ours = "b".repeat(40);
        let theirs = "c".repeat(40);
        let expected_new = "d".repeat(40);
        let raw = format!(":100644 100644 {ours} {expected_new} M\0conflicted.txt\0");
        let expected = parse_expected_merge_raw_diff(raw.as_bytes(), 40, 16_384, 1).unwrap();
        let merge_base_entries = expected_conflict_tree_entry("conflicted.txt", "100644", &base);
        let source_entries = expected_conflict_tree_entry("conflicted.txt", "100644", &theirs);
        let status = |worktree_mode: &str| {
            format!(
                "u UU N... 100644 100644 100644 {worktree_mode} {base} {ours} {theirs} \
                 conflicted.txt\0"
            )
        };
        let regular = parse_expected_merge_porcelain_v2(
            status("100644").as_bytes(),
            &expected,
            &merge_base_entries,
            &source_entries,
            40,
            16_384,
            1,
        )
        .unwrap();
        let regular_digest = digest_conflict_worktree(&root, &regular, 16_384).unwrap();

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(matches!(
            digest_conflict_worktree(&root, &regular, 16_384),
            Err(DeliveryTargetError::AuthenticationChanged)
        ));

        let executable = parse_expected_merge_porcelain_v2(
            status("100755").as_bytes(),
            &expected,
            &merge_base_entries,
            &source_entries,
            40,
            16_384,
            1,
        )
        .unwrap();
        let executable_digest = digest_conflict_worktree(&root, &executable, 16_384).unwrap();
        assert_ne!(regular_digest, executable_digest);
    }
}
