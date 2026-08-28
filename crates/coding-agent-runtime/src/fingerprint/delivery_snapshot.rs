use super::*;
use crate::delivery::command::DeliverySourceReadCommands;
use crate::delivery::output::classify_machine_result;

impl WorkspaceFingerprinter {
    /// Captures a source-tree snapshot with exactly the approved fingerprint
    /// algorithm. Every present entry is opened relative to the retained
    /// no-follow worktree capability, bounded again at capture time, read into
    /// redacted memory, and checked against both its handle and namespace
    /// identity after the read. Missing tracked entries and separately
    /// observed base-index removals become typed deletions; a missing
    /// untracked entry fails closed. The deletion list is intentionally not
    /// part of the fingerprint domain: it is a base-relative index replay
    /// operation, while the fingerprint keeps its established reviewed-byte
    /// semantics.
    pub(crate) fn capture_delivery_snapshot(
        work_tree: &ExecutionDirectory,
        limits: FingerprintLimits,
        tracked: &[u8],
        untracked: &[u8],
        deleted_base_paths: &[u8],
        object_id_hexadecimal_length: usize,
        cancellation: &CancellationToken,
    ) -> Result<DeliverySourceSnapshot, FingerprintError> {
        let entries = parse_entries(
            tracked,
            untracked,
            limits.max_files,
            Some(object_id_hexadecimal_length),
        )?;
        let deleted_base_paths = parse_deleted_base_paths(deleted_base_paths, limits.max_files)?;
        if entries.len().saturating_add(deleted_base_paths.len()) > limits.max_files {
            return Err(FingerprintError::TooManyFiles);
        }
        let current_paths = entries
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let mut hasher = Sha256::new();
        hasher.update(FINGERPRINT_DOMAIN);
        let mut total_bytes = 0u64;
        #[cfg(windows)]
        let mut leases = Vec::new();
        #[cfg(not(windows))]
        let leases = Vec::new();
        let mut deleted_entries = Vec::new();
        let mut present_entries = Vec::with_capacity(entries.len());

        for (raw_path, entry) in entries {
            check_cancelled(cancellation)?;
            hash_entry_prefix(&mut hasher, &raw_path, &entry.origin)?;
            match open_worktree_file(work_tree, &entry.path) {
                Ok(mut opened) => {
                    #[cfg(windows)]
                    let lease = reopen_file_read_lease(&opened.file)
                        .map_err(FingerprintError::UnsafeEntry)?;
                    let before = opened
                        .file
                        .metadata()
                        .map_err(FingerprintError::UnsafeEntry)?;
                    ensure_plain_file(&opened.file).map_err(FingerprintError::UnsafeEntry)?;
                    let length = check_entry_length(
                        length_within_limits(&before, limits)?,
                        &mut total_bytes,
                        limits,
                    )?;
                    hash_file_type(&mut hasher, &before)?;
                    hasher.update(length.to_be_bytes());
                    let bytes = capture_file_bytes(
                        &mut opened.file,
                        length,
                        limits.max_file_bytes,
                        cancellation,
                        &mut hasher,
                    )?;
                    validate_opened_entry(&opened, &before)?;
                    present_entries.push(DeliverySnapshotEntry::Present {
                        raw_path,
                        mode: snapshot_git_mode(&before, &entry.origin)?,
                        bytes,
                    });
                    #[cfg(windows)]
                    leases.push(lease);
                }
                Err(OpenWorktreeError::Missing)
                    if matches!(entry.origin, EntryOrigin::Tracked { .. }) =>
                {
                    hash_frame(&mut hasher, 4, &[])?;
                    deleted_entries.push(DeliverySnapshotEntry::Deleted { raw_path });
                }
                Err(OpenWorktreeError::Missing) => {
                    return Err(FingerprintError::WorkspaceChanged);
                }
                Err(OpenWorktreeError::Unsafe(error)) => {
                    return Err(FingerprintError::UnsafeEntry(error));
                }
            }
        }
        for deleted_base_path in deleted_base_paths {
            if !current_paths.contains(&deleted_base_path.raw) {
                deleted_entries.push(DeliverySnapshotEntry::Deleted {
                    raw_path: deleted_base_path.raw,
                });
            }
        }
        deleted_entries.extend(present_entries);
        hasher.update(total_bytes.to_be_bytes());
        Ok(DeliverySourceSnapshot {
            fingerprint: WorkspaceFingerprint::from_bytes(hasher.finalize().into()),
            entries: deleted_entries,
            leases,
        })
    }

    /// Collects the same workspace fingerprint through the stricter P4-B
    /// command binding. Every Git child therefore retains the probed
    /// executable plus common/admin/worktree and private-sandbox authorities.
    pub(crate) async fn collect_delivery(
        supervisor: &ProcessSupervisor,
        commands: &DeliverySourceReadCommands,
        work_tree: Arc<ExecutionDirectory>,
        limits: FingerprintLimits,
        max_command_output_bytes: usize,
        object_id_hexadecimal_length: usize,
        cancellation: CancellationToken,
    ) -> Result<DeliveryFingerprintObservation, crate::DeliverySourceError> {
        check_cancelled(&cancellation).map_err(crate::DeliverySourceError::from)?;
        let tracked_before = run_delivery_fingerprint_command(
            supervisor,
            commands.index_entries(),
            cancellation.clone(),
            max_command_output_bytes,
        )
        .await?;
        let untracked_before = run_delivery_fingerprint_command(
            supervisor,
            commands.untracked_paths(),
            cancellation.clone(),
            max_command_output_bytes,
        )
        .await?;
        let first_entries = parse_entries(
            &tracked_before,
            &untracked_before,
            limits.max_files,
            Some(object_id_hexadecimal_length),
        )
        .map_err(crate::DeliverySourceError::from)?;
        let first = Self::hash_entries(&work_tree, limits, first_entries, &cancellation)
            .map_err(crate::DeliverySourceError::from)?;

        let tracked_after = run_delivery_fingerprint_command(
            supervisor,
            commands.index_entries(),
            cancellation.clone(),
            max_command_output_bytes,
        )
        .await?;
        let untracked_after = run_delivery_fingerprint_command(
            supervisor,
            commands.untracked_paths(),
            cancellation.clone(),
            max_command_output_bytes,
        )
        .await?;
        let second_entries = parse_entries(
            &tracked_after,
            &untracked_after,
            limits.max_files,
            Some(object_id_hexadecimal_length),
        )
        .map_err(crate::DeliverySourceError::from)?;
        let paths = second_entries.keys().cloned().collect::<Vec<_>>();
        let second = Self::hash_entries(&work_tree, limits, second_entries, &cancellation)
            .map_err(crate::DeliverySourceError::from)?;
        if cancellation.is_cancelled() {
            return Err(crate::DeliverySourceError::Cancelled);
        }

        if tracked_before != tracked_after
            || untracked_before != untracked_after
            || first.fingerprint != second.fingerprint
        {
            return Err(crate::DeliverySourceError::SourceChanged);
        }
        drop(first.leases);
        drop(second.leases);
        Ok(DeliveryFingerprintObservation {
            fingerprint: second.fingerprint,
            paths,
        })
    }
}

pub(crate) struct DeliveryFingerprintObservation {
    pub(crate) fingerprint: WorkspaceFingerprint,
    pub(crate) paths: Vec<Vec<u8>>,
}

async fn run_delivery_fingerprint_command(
    supervisor: &ProcessSupervisor,
    command: Result<ValidatedCommand, crate::DeliverySourceError>,
    cancellation: CancellationToken,
    max_output_bytes: usize,
) -> Result<Vec<u8>, crate::DeliverySourceError> {
    let command = command?;
    let result = supervisor
        .run(command, cancellation)
        .await
        .map_err(crate::DeliverySourceError::from)?;
    classify_machine_result(&result, max_output_bytes)
}

/// A redacted, no-follow byte snapshot used only to reconstruct Task 11's
/// private temporary index. It carries no pathname authority: consumers can
/// pass its bytes only to the fixed `hash-object --no-filters --stdin` and
/// `update-index --index-info` commands.
pub(crate) struct DeliverySourceSnapshot {
    fingerprint: WorkspaceFingerprint,
    entries: Vec<DeliverySnapshotEntry>,
    // Windows read leases keep every captured identity stable until the
    // snapshot is consumed. Unix relies on the documented private-runtime
    // boundary plus the no-follow/identity proof taken around every read.
    leases: Vec<File>,
}

impl DeliverySourceSnapshot {
    pub(crate) const fn fingerprint(&self) -> WorkspaceFingerprint {
        self.fingerprint
    }

    pub(crate) fn take_entries(&mut self) -> Vec<DeliverySnapshotEntry> {
        // Keep the retained Windows leases alive for every subsequent Git
        // child, even after the redacted byte buffers have moved into one
        // exact stdin payload at a time.
        let _ = self.leases.len();
        std::mem::take(&mut self.entries)
    }
}

/// One safe source-tree operation reconstructed from the captured bytes.
/// Neither variant implements `Debug`, so raw worktree names and content
/// cannot enter diagnostics accidentally.
pub(crate) enum DeliverySnapshotEntry {
    Present {
        raw_path: Vec<u8>,
        mode: DeliverySnapshotGitMode,
        bytes: Vec<u8>,
    },
    Deleted {
        raw_path: Vec<u8>,
    },
}

/// The only regular-file modes admitted by the source safety contract.
pub(crate) enum DeliverySnapshotGitMode {
    Regular,
    Executable,
}

impl DeliverySnapshotGitMode {
    pub(crate) const fn as_bytes(&self) -> &'static [u8] {
        match self {
            Self::Regular => b"100644",
            Self::Executable => b"100755",
        }
    }
}

/// Strictly extracts the tracked path set from the fixed delivery
/// `ls-files --cached --stage -v -z` protocol. Cleanup uses this exact index
/// path set when rechecking the persisted config/attributes digest, so a late
/// untracked file can be classified as dirty without being mistaken for a
/// change to the authenticated candidate path set.
pub(crate) fn parse_delivery_tracked_paths(
    tracked: &[u8],
    max_files: usize,
    object_id_hexadecimal_length: usize,
) -> Result<Vec<Vec<u8>>, FingerprintError> {
    parse_entries(tracked, &[], max_files, Some(object_id_hexadecimal_length))
        .map(|entries| entries.into_keys().collect())
}

/// Parses the fixed `diff-index --diff-filter=D --name-only -z` output used
/// only while replaying an already-approved snapshot into a private index.
/// A duplicate path is ambiguous input, not an instruction to delete twice.
fn parse_deleted_base_paths(
    output: &[u8],
    max_files: usize,
) -> Result<Vec<RawGitPath>, FingerprintError> {
    let mut paths = BTreeMap::new();
    for record in nul_records(output)? {
        if paths.len() >= max_files {
            return Err(FingerprintError::TooManyFiles);
        }
        let path = RawGitPath::parse(record)?;
        if paths.insert(path.raw.clone(), path).is_some() {
            return Err(FingerprintError::ListingInvalid);
        }
    }
    Ok(paths.into_values().collect())
}

/// Reads one already no-follow-opened worktree file into redacted memory while
/// contributing the exact same bytes to the fingerprint domain. Allocation is
/// bounded by the same per-file limit used by the normal fingerprinter.
fn capture_file_bytes(
    file: &mut File,
    expected_length: u64,
    maximum: u64,
    cancellation: &CancellationToken,
    hasher: &mut Sha256,
) -> Result<Vec<u8>, FingerprintError> {
    let capacity = usize::try_from(expected_length).map_err(|_| FingerprintError::FileTooLarge)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| FingerprintError::FileTooLarge)?;
    let mut buffer = [0u8; STREAM_BUFFER_BYTES];
    let mut observed = 0u64;
    loop {
        check_cancelled(cancellation)?;
        let read = file
            .read(&mut buffer)
            .map_err(FingerprintError::UnsafeEntry)?;
        if read == 0 {
            break;
        }
        observed = observed
            .checked_add(read as u64)
            .ok_or(FingerprintError::FileTooLarge)?;
        if observed > maximum || observed > expected_length {
            return Err(FingerprintError::WorkspaceChanged);
        }
        hasher.update(&buffer[..read]);
        bytes.extend_from_slice(&buffer[..read]);
    }
    if observed != expected_length {
        return Err(FingerprintError::WorkspaceChanged);
    }
    Ok(bytes)
}

#[cfg(unix)]
fn snapshot_git_mode(
    metadata: &Metadata,
    _origin: &EntryOrigin,
) -> Result<DeliverySnapshotGitMode, FingerprintError> {
    use std::os::unix::fs::MetadataExt;

    if !metadata.file_type().is_file() {
        return Err(FingerprintError::UnsupportedEntry);
    }
    Ok(if metadata.mode() & 0o100 == 0 {
        DeliverySnapshotGitMode::Regular
    } else {
        DeliverySnapshotGitMode::Executable
    })
}

#[cfg(windows)]
fn snapshot_git_mode(
    metadata: &Metadata,
    origin: &EntryOrigin,
) -> Result<DeliverySnapshotGitMode, FingerprintError> {
    if !metadata.file_type().is_file() {
        return Err(FingerprintError::UnsupportedEntry);
    }
    // Git on Windows normally has core.filemode disabled. Preserve the tracked
    // mode already authenticated from the index rather than letting Windows
    // filesystem attributes silently turn an existing executable into 100644.
    match origin {
        EntryOrigin::Tracked { metadata } => tracked_git_mode(metadata),
        EntryOrigin::Untracked => Ok(DeliverySnapshotGitMode::Regular),
    }
}

#[cfg(windows)]
fn tracked_git_mode(metadata: &[u8]) -> Result<DeliverySnapshotGitMode, FingerprintError> {
    let mode = metadata
        .split(|byte| *byte == b' ')
        .nth(1)
        .ok_or(FingerprintError::UnsupportedEntry)?;
    match mode {
        b"100644" => Ok(DeliverySnapshotGitMode::Regular),
        b"100755" => Ok(DeliverySnapshotGitMode::Executable),
        _ => Err(FingerprintError::UnsupportedEntry),
    }
}

#[cfg(all(test, unix))]
mod unix_snapshot_mode_tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    use super::{DeliverySnapshotGitMode, snapshot_git_mode};

    #[test]
    fn snapshot_mode_uses_only_owner_execute_bit() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("mode-check");
        fs::write(&path, b"bytes").unwrap();

        for (mode, expected) in [
            (0o644, DeliverySnapshotGitMode::Regular),
            (0o744, DeliverySnapshotGitMode::Executable),
            (0o645, DeliverySnapshotGitMode::Regular),
        ] {
            fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
            match expected {
                DeliverySnapshotGitMode::Regular => assert!(matches!(
                    snapshot_git_mode(
                        &fs::metadata(&path).unwrap(),
                        &super::EntryOrigin::Untracked,
                    )
                    .unwrap(),
                    DeliverySnapshotGitMode::Regular
                )),
                DeliverySnapshotGitMode::Executable => assert!(matches!(
                    snapshot_git_mode(
                        &fs::metadata(&path).unwrap(),
                        &super::EntryOrigin::Untracked,
                    )
                    .unwrap(),
                    DeliverySnapshotGitMode::Executable
                )),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio_util::sync::CancellationToken;

    use super::*;

    const TRACKED_FILE: &[u8] =
        b"H 100644 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 0\tpresent.txt\0";

    #[test]
    fn cleanup_tracked_paths_require_exact_stage_zero_regular_entries() {
        assert_eq!(
            parse_delivery_tracked_paths(TRACKED_FILE, 2, 40).unwrap(),
            vec![b"present.txt".to_vec()]
        );
        for output in [
            b"H 100644 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 1\tpresent.txt\0".as_slice(),
            b"H 120000 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 0\tpresent.txt\0".as_slice(),
            b"H 100644 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 0\tpresent.txt".as_slice(),
        ] {
            assert!(parse_delivery_tracked_paths(output, 2, 40).is_err());
        }
    }

    #[test]
    fn deleted_base_paths_reject_malformed_records_and_missing_nul_terminator() {
        for output in [
            b"missing-terminator".as_slice(),
            b"first.txt\0\0".as_slice(),
        ] {
            assert!(matches!(
                parse_deleted_base_paths(output, 4),
                Err(FingerprintError::ListingInvalid)
            ));
        }
    }

    #[test]
    fn deleted_base_paths_reject_duplicate_paths() {
        assert!(matches!(
            parse_deleted_base_paths(b"removed.txt\0removed.txt\0", 4),
            Err(FingerprintError::ListingInvalid)
        ));
    }

    #[test]
    fn delivery_snapshot_rejects_combined_present_and_deleted_paths_over_limit() {
        let directory = tempfile::tempdir().unwrap();
        let work_tree = ExecutionDirectory::open(directory.path().canonicalize().unwrap()).unwrap();
        let limits = FingerprintLimits::try_new(Duration::from_secs(1), 1, 1024, 1024).unwrap();

        assert!(matches!(
            WorkspaceFingerprinter::capture_delivery_snapshot(
                &work_tree,
                limits,
                TRACKED_FILE,
                b"",
                b"removed.txt\0",
                40,
                &CancellationToken::new(),
            ),
            Err(FingerprintError::TooManyFiles)
        ));
    }

    #[test]
    fn delivery_snapshot_keeps_visible_readd_instead_of_replaying_base_deletion() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("present.txt"), b"captured bytes").unwrap();
        let work_tree = ExecutionDirectory::open(directory.path().canonicalize().unwrap()).unwrap();
        let limits = FingerprintLimits::try_new(Duration::from_secs(1), 2, 1024, 1024).unwrap();

        let mut snapshot = WorkspaceFingerprinter::capture_delivery_snapshot(
            &work_tree,
            limits,
            TRACKED_FILE,
            b"",
            b"present.txt\0",
            40,
            &CancellationToken::new(),
        )
        .unwrap();

        let entries = snapshot.take_entries();
        assert_eq!(entries.len(), 1);
        assert!(matches!(
            &entries[0],
            DeliverySnapshotEntry::Present { raw_path, .. } if raw_path.as_slice() == b"present.txt"
        ));
    }
}
