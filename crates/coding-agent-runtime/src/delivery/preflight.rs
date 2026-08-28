//! Strict, side-effect-free parsing for Task 13 `git merge-tree` preflight.
//!
//! This module intentionally contains no checkout authority, process runner,
//! or generic Git command construction. It turns one already bounded output
//! from the fixed typed command vocabulary into opaque runtime values.

use std::collections::BTreeSet;
use std::fmt;

use tokio_util::sync::CancellationToken;

use super::collision::{parse_target_path_listing, require_no_ignored_target_collision};
use super::output::DeliveryCommandExit;
use super::types::{MAX_MERGE_CONFLICT_PATHS, MAX_MERGE_CONFLICT_PAYLOAD_BYTES};
use super::{
    DeliveryCandidateTree, DeliveryCommitOid, DeliveryConflictPath, DeliveryGitObjectFormat,
    DeliveryPreflightError, DeliveryPreflightResult, DeliverySourceCapability,
    DeliverySourceCommit, DeliverySourceCommitInput, DeliverySourceProvisioner,
    DeliveryTargetCapability, DeliveryTargetError, DeliveryTargetProvisioner, DeliveryTreeOid,
    PreparedDeliveryPreflightSource,
};

/// A source object choice for an observation-only target preflight.
///
/// `Candidate` permits the runtime to create the deterministic source commit
/// as an unreachable object after a fresh reviewed-source proof. `Committed`
/// requires the exact persisted source commit to still prove its candidate
/// tree, source ref, index, worktree, metadata, and security evidence. Neither
/// variant exposes a generic Git ref, command, path, or metadata input.
pub enum DeliveryPreflightSource<'a> {
    Candidate {
        source: &'a DeliverySourceCapability,
        candidate: &'a DeliveryCandidateTree,
    },
    Committed {
        source: &'a DeliverySourceCapability,
        candidate: &'a DeliveryCandidateTree,
        commit: &'a DeliverySourceCommit,
        input: &'a DeliverySourceCommitInput,
    },
}

impl<'a> DeliveryPreflightSource<'a> {
    /// Selects an uncommitted, approved candidate. The exact deterministic
    /// source object is materialized only as a dangling Git object for the
    /// preflight; no source ref, index, or worktree mutation is performed.
    pub const fn candidate(
        source: &'a DeliverySourceCapability,
        candidate: &'a DeliveryCandidateTree,
    ) -> Self {
        Self::Candidate { source, candidate }
    }

    /// Selects an exact source commit that was already persisted and applied
    /// through the source-side CommitPending proof.
    pub const fn committed(
        source: &'a DeliverySourceCapability,
        candidate: &'a DeliveryCandidateTree,
        commit: &'a DeliverySourceCommit,
        input: &'a DeliverySourceCommitInput,
    ) -> Self {
        Self::Committed {
            source,
            candidate,
            commit,
            input,
        }
    }
}

impl fmt::Debug for DeliveryPreflightSource<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryPreflightSource(<validated>)")
    }
}

enum PreflightSourceProof<'proof, 'source> {
    Legacy(&'proof DeliveryPreflightSource<'source>),
    Prepared {
        source: &'proof DeliverySourceCapability,
        prepared: &'proof PreparedDeliveryPreflightSource,
    },
}

impl<'proof, 'source> PreflightSourceProof<'proof, 'source> {
    fn source(&self) -> &DeliverySourceCapability {
        match self {
            Self::Legacy(source) => legacy_source_capability(source),
            Self::Prepared { source, .. } => source,
        }
    }

    const fn is_prepared(&self) -> bool {
        matches!(self, Self::Prepared { .. })
    }

    async fn revalidate(
        &self,
        provisioner: &DeliverySourceProvisioner,
        cancellation: CancellationToken,
    ) -> Result<(), DeliveryPreflightError> {
        match self {
            Self::Legacy(source) => {
                revalidate_source_for_preflight(provisioner, source, cancellation).await
            }
            Self::Prepared { source, prepared } => provisioner
                .revalidate_prepared_delivery_preflight_source(source, prepared, cancellation)
                .await
                .map_err(DeliveryPreflightError::from),
        }
    }
}

/// Runs the complete, target-side preflight without modifying either checkout.
///
/// The only permitted Git write is an unreachable deterministic object for an
/// uncommitted candidate source. Target `merge-tree` and its write-set scan
/// are object-only. The target checkout itself remains read-only throughout.
pub async fn preflight_delivery_merge(
    source_provisioner: &DeliverySourceProvisioner,
    target_provisioner: &DeliveryTargetProvisioner,
    target: &DeliveryTargetCapability,
    source: DeliveryPreflightSource<'_>,
    cancellation: CancellationToken,
) -> Result<DeliveryPreflightResult, DeliveryPreflightError> {
    target_provisioner
        .revalidate_delivery_target(target, cancellation.clone())
        .await?;
    revalidate_source_for_preflight(source_provisioner, &source, cancellation.clone()).await?;
    let source_capability = legacy_source_capability(&source);
    require_same_preflight_repository_and_distinct_branches(source_capability, target)?;

    // This is deliberately after the source/target pairing proof: candidate
    // preflight may create one dangling object, so invalid cross-repository or
    // same-branch pairings must fail before that sole permitted side effect.
    let source_commit =
        resolve_source_commit(source_provisioner, &source, cancellation.clone()).await?;

    target_provisioner
        .revalidate_delivery_target(target, cancellation.clone())
        .await?;
    revalidate_source_for_preflight(source_provisioner, &source, cancellation.clone()).await?;
    require_same_preflight_repository_and_distinct_branches(source_capability, target)?;

    run_verified_delivery_preflight(
        source_provisioner,
        target_provisioner,
        target,
        PreflightSourceProof::Legacy(&source),
        source_commit,
        cancellation,
    )
    .await
}

/// Runs target-side preflight from the exact object IDs already persisted by
/// the two-stage `PreflightPending -> BindInputs` protocol.
///
/// The prepared value and source capability are re-bound and freshly proven
/// before any target merge observation.  A second fresh proof immediately
/// precedes `merge-tree`, closing the durable-input boundary without admitting
/// caller-selected object IDs or mutable checkout authority.
pub async fn preflight_prepared_delivery_merge(
    source_provisioner: &DeliverySourceProvisioner,
    target_provisioner: &DeliveryTargetProvisioner,
    target: &DeliveryTargetCapability,
    source: &DeliverySourceCapability,
    prepared: &PreparedDeliveryPreflightSource,
    cancellation: CancellationToken,
) -> Result<DeliveryPreflightResult, DeliveryPreflightError> {
    source_provisioner
        .revalidate_prepared_delivery_preflight_source(source, prepared, cancellation.clone())
        .await?;
    target_provisioner
        .revalidate_delivery_target(target, cancellation.clone())
        .await?;
    require_same_preflight_repository_and_distinct_branches(source, target)?;

    run_verified_delivery_preflight(
        source_provisioner,
        target_provisioner,
        target,
        PreflightSourceProof::Prepared { source, prepared },
        prepared.source_commit().clone(),
        cancellation,
    )
    .await
}

async fn run_verified_delivery_preflight(
    source_provisioner: &DeliverySourceProvisioner,
    target_provisioner: &DeliveryTargetProvisioner,
    target: &DeliveryTargetCapability,
    source: PreflightSourceProof<'_, '_>,
    source_commit: DeliveryCommitOid,
    cancellation: CancellationToken,
) -> Result<DeliveryPreflightResult, DeliveryPreflightError> {
    let commands = target.commands();
    if matches!(
        target_provisioner
            .executor()
            .run_predicate(
                commands.source_is_ancestor_of_target(&source_commit, target.head())?,
                cancellation.clone(),
                target_provisioner.limits().max_status_bytes(),
            )
            .await?,
        DeliveryCommandExit::Matched
    ) {
        return Err(DeliveryPreflightError::SourceAlreadyInTarget);
    }

    let merge_base_output = target_provisioner
        .executor()
        .run(
            commands.merge_base(target.head(), &source_commit)?,
            cancellation.clone(),
            target_provisioner.limits().max_status_bytes(),
        )
        .await?;
    let merge_base = parse_single_merge_base(&merge_base_output, target.probe().object_format())?;

    // Prepared inputs crossed a durable Store boundary after object creation.
    // Re-prove both authorities and the exact fixed source commit immediately
    // before the first merge-tree child. The legacy wrapper retains its
    // established command ordering and performs its existing closing proof.
    if source.is_prepared() {
        source
            .revalidate(source_provisioner, cancellation.clone())
            .await?;
        target_provisioner
            .revalidate_delivery_target(target, cancellation.clone())
            .await?;
        require_same_preflight_repository_and_distinct_branches(source.source(), target)?;
    }

    let (merge_exit, merge_output) = target_provisioner
        .executor()
        .run_machine_protocol(
            commands.merge_tree(target.head(), &source_commit)?,
            cancellation.clone(),
            target_provisioner.limits().max_status_bytes(),
        )
        .await?;
    let result = parse_merge_tree_result(
        merge_exit,
        &merge_output,
        target.probe().object_format(),
        source_commit,
        merge_base,
    )?;

    if result.is_ready() {
        // The target proof must still cover the exact HEAD that was passed to
        // merge-tree before an object-only write-set is turned into a collision
        // decision about the live checkout namespace.
        target_provisioner
            .revalidate_delivery_target(target, cancellation.clone())
            .await?;
        let write_set_output = target_provisioner
            .executor()
            .run(
                commands.merge_write_set(target.head(), result.candidate_merge_tree())?,
                cancellation.clone(),
                target_provisioner.limits().max_status_bytes(),
            )
            .await?;
        let ignored_output = target_provisioner
            .executor()
            .run(
                commands.ignored_untracked_paths()?,
                cancellation.clone(),
                target_provisioner.limits().max_status_bytes(),
            )
            .await?;
        let write_set = parse_target_path_listing(
            &write_set_output,
            target_provisioner.limits().max_status_bytes(),
            false,
        )?;
        let ignored = parse_target_path_listing(
            &ignored_output,
            target_provisioner.limits().max_status_bytes(),
            true,
        )?;
        // A clean object-only merge can add paths which were not tracked by
        // the old target. Validate their effective attributes too before the
        // result is allowed to authorize a later actual merge child.
        let write_set_attribute_paths = write_set
            .iter()
            .map(|path| path.raw_path().to_vec())
            .collect::<Vec<_>>();
        target_provisioner
            .require_safe_merge_write_set_attributes(
                target,
                &write_set_attribute_paths,
                cancellation.clone(),
            )
            .await?;
        target_provisioner
            .revalidate_delivery_target(target, cancellation.clone())
            .await?;
        let target_root = target.checkout_root()?;
        require_no_ignored_target_collision(&target_root, &ignored, &write_set)?;
    }

    source
        .revalidate(source_provisioner, cancellation.clone())
        .await?;
    target_provisioner
        .revalidate_delivery_target(target, cancellation)
        .await?;
    Ok(result)
}

/// Proves the two independently-minted capabilities can safely participate in
/// one repository-local preflight. This is an authority relationship, not a
/// caller-provided path or ref comparison.
fn require_same_preflight_repository_and_distinct_branches(
    source: &DeliverySourceCapability,
    target: &DeliveryTargetCapability,
) -> Result<(), DeliveryPreflightError> {
    if !source
        .probe()
        .shares_repository_format_authority_with(target.probe())
        || source.common_directory_identity() != target.common_directory_identity()
    {
        return Err(DeliveryTargetError::AuthenticationChanged.into());
    }
    if source.branch_name() == target.branch_name() {
        return Err(DeliveryTargetError::TargetBranchMismatch.into());
    }
    Ok(())
}

fn legacy_source_capability<'source>(
    source: &DeliveryPreflightSource<'source>,
) -> &'source DeliverySourceCapability {
    match source {
        DeliveryPreflightSource::Candidate { source, .. }
        | DeliveryPreflightSource::Committed { source, .. } => source,
    }
}

async fn resolve_source_commit(
    provisioner: &DeliverySourceProvisioner,
    source: &DeliveryPreflightSource<'_>,
    cancellation: CancellationToken,
) -> Result<DeliveryCommitOid, DeliveryPreflightError> {
    match source {
        DeliveryPreflightSource::Candidate { source, candidate } => {
            provisioner
                .revalidate_preflight_candidate_source(source, cancellation.clone())
                .await?;
            let commit = provisioner
                .build_preflight_source_commit(source, candidate, cancellation.clone())
                .await?;
            provisioner
                .revalidate_preflight_candidate_source(source, cancellation)
                .await?;
            Ok(commit)
        }
        DeliveryPreflightSource::Committed {
            source,
            candidate,
            commit,
            input,
        } => {
            provisioner
                .revalidate_preflight_committed_source(
                    source,
                    candidate,
                    commit,
                    input,
                    cancellation,
                )
                .await?;
            Ok(commit.commit().clone())
        }
    }
}

async fn revalidate_source_for_preflight(
    provisioner: &DeliverySourceProvisioner,
    source: &DeliveryPreflightSource<'_>,
    cancellation: CancellationToken,
) -> Result<(), DeliveryPreflightError> {
    match source {
        DeliveryPreflightSource::Candidate { source, .. } => {
            provisioner
                .revalidate_preflight_candidate_source(source, cancellation)
                .await?;
        }
        DeliveryPreflightSource::Committed {
            source,
            candidate,
            commit,
            input,
        } => {
            provisioner
                .revalidate_preflight_committed_source(
                    source,
                    candidate,
                    commit,
                    input,
                    cancellation,
                )
                .await?;
        }
    }
    Ok(())
}

fn parse_single_merge_base(
    output: &[u8],
    object_format: DeliveryGitObjectFormat,
) -> Result<DeliveryCommitOid, DeliveryPreflightError> {
    let object_id = output
        .strip_suffix(b"\n")
        .filter(|value| !value.is_empty() && !value.contains(&b'\n'))
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(|value| DeliveryCommitOid::try_new(value, object_format))
        .ok_or(DeliveryPreflightError::MalformedMergeTreeOutput)?;
    Ok(object_id)
}

/// Parses the exact modern `merge-tree --write-tree --messages --name-only -z`
/// protocol.
///
/// Without `--stdin`, Git emits one top-level tree object ID followed by a NUL.
/// A clean result has an empty conflicted-path section but may still carry
/// informational records such as `Auto-merging`. A conflicted result has one
/// or more name-only conflicted paths, a NUL beginning the informational section, then zero or more
/// records of `<path-count> NUL <paths...> <type> NUL <message> NUL`.
/// Informational records are parsed for exact framing and discarded: their
/// text is not a stable runtime or UI contract.
pub(super) fn parse_merge_tree_result(
    exit: DeliveryCommandExit,
    output: &[u8],
    object_format: DeliveryGitObjectFormat,
    source_commit: DeliveryCommitOid,
    merge_base: DeliveryCommitOid,
) -> Result<DeliveryPreflightResult, DeliveryPreflightError> {
    let (tree, remainder) = take_record(output)?;
    let tree = std::str::from_utf8(tree)
        .ok()
        .and_then(|value| DeliveryTreeOid::try_new(value, object_format))
        .ok_or(DeliveryPreflightError::MalformedMergeTreeOutput)?;

    match exit {
        DeliveryCommandExit::Matched => {
            let (paths, remainder) = parse_conflict_paths(remainder)?;
            if !paths.is_empty() {
                return Err(DeliveryPreflightError::MalformedMergeTreeOutput);
            }
            parse_conflict_messages(remainder)?;
            Ok(DeliveryPreflightResult::ready(
                source_commit,
                merge_base,
                tree,
            ))
        }
        DeliveryCommandExit::NotMatched => {
            let (paths, remainder) = parse_conflict_paths(remainder)?;
            parse_conflict_messages(remainder)?;
            DeliveryPreflightResult::conflict(source_commit, merge_base, tree, paths)
        }
    }
}

/// Reads a non-empty NUL-terminated protocol field.
fn take_record(output: &[u8]) -> Result<(&[u8], &[u8]), DeliveryPreflightError> {
    let (record, remainder) = take_framed_record(output)?;
    if record.is_empty() {
        return Err(DeliveryPreflightError::MalformedMergeTreeOutput);
    }
    Ok((record, remainder))
}

/// Reads one NUL-terminated protocol field, retaining an empty field for the
/// sole structural separator between conflicted-file info and messages.
fn take_framed_record(output: &[u8]) -> Result<(&[u8], &[u8]), DeliveryPreflightError> {
    let index = output
        .iter()
        .position(|byte| *byte == 0)
        .ok_or(DeliveryPreflightError::MalformedMergeTreeOutput)?;
    Ok((&output[..index], &output[index + 1..]))
}

fn parse_conflict_paths(
    mut output: &[u8],
) -> Result<(Vec<DeliveryConflictPath>, &[u8]), DeliveryPreflightError> {
    let mut paths = Vec::new();
    let mut raw_paths = BTreeSet::new();
    let mut payload_bytes = 0usize;
    loop {
        let (record, remainder) = take_framed_record(output)?;
        output = remainder;
        if record.is_empty() {
            break;
        }
        if paths.len() >= MAX_MERGE_CONFLICT_PATHS || !raw_paths.insert(record.to_vec()) {
            return Err(DeliveryPreflightError::MalformedMergeTreeOutput);
        }

        let path = DeliveryConflictPath::try_from_raw(record.to_vec())?;
        payload_bytes = payload_bytes
            .checked_add(path.value().len())
            .ok_or(DeliveryPreflightError::MalformedMergeTreeOutput)?;
        if payload_bytes > MAX_MERGE_CONFLICT_PAYLOAD_BYTES {
            return Err(DeliveryPreflightError::MalformedMergeTreeOutput);
        }
        paths.push(path);
    }
    Ok((paths, output))
}

/// Validates and discards Git's structured informational conflict section.
///
/// Git documents each `-z` record as a canonical decimal count, exactly that
/// many non-empty path-or-branch fields, a non-empty stable type, and a
/// non-empty free-form message. We deliberately never interpret or retain the
/// latter three fields; their framing is still an integrity boundary.
fn parse_conflict_messages(mut output: &[u8]) -> Result<(), DeliveryPreflightError> {
    while !output.is_empty() {
        let (encoded_path_count, remainder) = take_record(output)?;
        let path_count = parse_canonical_path_count(encoded_path_count)?;
        output = remainder;

        for _ in 0..path_count {
            let (_, remainder) = take_record(output)?;
            output = remainder;
        }

        let (_, remainder) = take_record(output)?;
        output = remainder;
        let (_, remainder) = take_record(output)?;
        output = remainder;
    }
    Ok(())
}

fn parse_canonical_path_count(encoded: &[u8]) -> Result<usize, DeliveryPreflightError> {
    if encoded == b"0" {
        return Ok(0);
    }
    if !matches!(encoded.first(), Some(b'1'..=b'9'))
        || !encoded.iter().all(|byte| byte.is_ascii_digit())
    {
        return Err(DeliveryPreflightError::MalformedMergeTreeOutput);
    }

    encoded.iter().try_fold(0usize, |count, byte| {
        count
            .checked_mul(10)
            .and_then(|count| count.checked_add(usize::from(*byte - b'0')))
            .ok_or(DeliveryPreflightError::MalformedMergeTreeOutput)
    })
}

#[cfg(test)]
mod tests {
    use super::super::types::{
        DeliveryConflictPathEncoding, MAX_MERGE_CONFLICT_PATH_BYTES, MAX_MERGE_CONFLICT_PATHS,
    };
    use super::*;

    const OID: &str = "0123456789abcdef0123456789abcdef01234567";
    const OTHER_OID: &str = "fedcba9876543210fedcba9876543210fedcba98";

    fn commit(value: &str) -> DeliveryCommitOid {
        DeliveryCommitOid::try_new(value, DeliveryGitObjectFormat::Sha1).unwrap()
    }

    fn append_record(output: &mut Vec<u8>, record: &[u8]) {
        output.extend_from_slice(record);
        output.push(0);
    }

    fn conflict_output(paths: &[&[u8]]) -> Vec<u8> {
        let mut output = Vec::new();
        append_record(&mut output, OID.as_bytes());
        for path in paths {
            append_record(&mut output, path);
        }
        output.push(0);
        output
    }

    fn append_message_record(
        output: &mut Vec<u8>,
        paths: &[&[u8]],
        conflict_type: &[u8],
        message: &[u8],
    ) {
        append_record(output, paths.len().to_string().as_bytes());
        for path in paths {
            append_record(output, path);
        }
        append_record(output, conflict_type);
        append_record(output, message);
    }

    fn parse_clean(output: &[u8]) -> Result<DeliveryPreflightResult, DeliveryPreflightError> {
        parse_merge_tree_result(
            DeliveryCommandExit::Matched,
            output,
            DeliveryGitObjectFormat::Sha1,
            commit(OID),
            commit(OTHER_OID),
        )
    }

    #[test]
    fn clean_protocol_accepts_framed_informational_messages_but_no_conflict_paths() {
        let mut clean = conflict_output(&[]);
        append_message_record(
            &mut clean,
            &[b"tracked.txt"],
            b"Auto-merging",
            b"Auto-merging tracked.txt",
        );
        assert!(parse_clean(&clean).unwrap().is_ready());

        let with_conflict_path = conflict_output(&[b"tracked.txt"]);
        assert!(matches!(
            parse_clean(&with_conflict_path),
            Err(DeliveryPreflightError::MalformedMergeTreeOutput)
        ));
    }

    fn parse_conflict(output: &[u8]) -> Result<DeliveryPreflightResult, DeliveryPreflightError> {
        parse_merge_tree_result(
            DeliveryCommandExit::NotMatched,
            output,
            DeliveryGitObjectFormat::Sha1,
            commit(OID),
            commit(OTHER_OID),
        )
    }

    #[test]
    fn merge_base_parser_requires_one_canonical_complete_oid_line() {
        let oid = format!("{OID}\n");
        assert_eq!(
            parse_single_merge_base(oid.as_bytes(), DeliveryGitObjectFormat::Sha1)
                .unwrap()
                .as_str(),
            OID
        );
        let malformed = vec![
            Vec::new(),
            b"not-an-object\n".to_vec(),
            format!("{OID}\n{OTHER_OID}\n").into_bytes(),
            format!("{}\n", "0".repeat(40)).into_bytes(),
        ];
        for malformed in &malformed {
            assert_eq!(
                parse_single_merge_base(malformed, DeliveryGitObjectFormat::Sha1),
                Err(DeliveryPreflightError::MalformedMergeTreeOutput)
            );
        }
    }

    #[test]
    fn clean_protocol_requires_tree_and_double_nul_only() {
        let output = format!("{OID}\0\0").into_bytes();
        let result = parse_merge_tree_result(
            DeliveryCommandExit::Matched,
            &output,
            DeliveryGitObjectFormat::Sha1,
            commit(OID),
            commit(OTHER_OID),
        )
        .unwrap();
        assert!(result.is_ready());

        let malformed = format!("{OID}\0unexpected\0").into_bytes();
        assert!(matches!(
            parse_merge_tree_result(
                DeliveryCommandExit::Matched,
                &malformed,
                DeliveryGitObjectFormat::Sha1,
                commit(OID),
                commit(OTHER_OID),
            ),
            Err(DeliveryPreflightError::MalformedMergeTreeOutput)
        ));
    }

    #[test]
    fn conflict_protocol_keeps_only_bounded_redacted_name_only_paths() {
        let mut output = conflict_output(&[b"src/lib.rs", b"src/main.rs"]);
        append_message_record(
            &mut output,
            &[b"src/lib.rs"],
            b"Auto-merging",
            b"Auto-merging src/lib.rs\n",
        );
        append_message_record(
            &mut output,
            &[b"src/lib.rs", b"src/main.rs"],
            b"CONFLICT (contents)",
            b"conflict detail\n",
        );

        let result = parse_conflict(&output).unwrap();
        let paths = result.conflict_paths().unwrap();
        assert_eq!(paths.len(), 2);
        assert_eq!(paths[0].value(), "src/lib.rs");
        assert_eq!(paths[1].value(), "src/main.rs");
        assert!(!format!("{result:?}").contains("src/lib.rs"));
    }

    #[test]
    fn conflict_protocol_allows_empty_name_only_list_and_zero_path_messages() {
        let mut output = conflict_output(&[]);
        append_message_record(
            &mut output,
            &[],
            b"CONFLICT (directory rename)",
            b"directory rename conflict\n",
        );

        let result = parse_conflict(&output).unwrap();
        assert_eq!(result.conflict_paths(), Some(&[][..]));

        let no_messages = conflict_output(&[]);
        let result = parse_conflict(&no_messages).unwrap();
        assert_eq!(result.conflict_paths(), Some(&[][..]));
    }

    #[test]
    fn malformed_message_count_sections_fail_closed() {
        for suffix in [
            b"01\0path\0type\0message\0".as_slice(),
            b"not-a-number\0path\0type\0message\0".as_slice(),
            b"1\0path\0type\0".as_slice(),
            b"1\0path\0\0message\0".as_slice(),
            b"1\0path\0type\0message".as_slice(),
        ] {
            let mut output = conflict_output(&[]);
            output.extend_from_slice(suffix);
            assert_eq!(
                parse_conflict(&output),
                Err(DeliveryPreflightError::MalformedMergeTreeOutput)
            );
        }
    }

    #[test]
    fn unsafe_duplicate_and_oversized_conflict_paths_fail_closed() {
        for paths in [
            vec![b"../outside".as_slice()],
            vec![b"duplicate".as_slice(), b"duplicate".as_slice()],
        ] {
            assert_eq!(
                parse_conflict(&conflict_output(&paths)),
                Err(DeliveryPreflightError::MalformedMergeTreeOutput)
            );
        }

        let oversized = vec![b'a'; MAX_MERGE_CONFLICT_PATH_BYTES + 1];
        assert_eq!(
            parse_conflict(&conflict_output(&[&oversized])),
            Err(DeliveryPreflightError::MalformedMergeTreeOutput)
        );
    }

    #[test]
    fn conflict_protocol_rejects_more_than_the_bounded_path_count() {
        let paths = (0..=MAX_MERGE_CONFLICT_PATHS)
            .map(|index| format!("path-{index:03}").into_bytes())
            .collect::<Vec<_>>();
        let path_refs = paths.iter().map(Vec::as_slice).collect::<Vec<_>>();

        assert_eq!(
            parse_conflict(&conflict_output(&path_refs)),
            Err(DeliveryPreflightError::MalformedMergeTreeOutput)
        );
    }

    #[test]
    fn conflict_protocol_converts_non_utf8_name_only_path_to_base64url() {
        let result = parse_conflict(&conflict_output(&[b"dir/\xffname"])).unwrap();
        let path = &result.conflict_paths().unwrap()[0];

        assert_eq!(path.encoding(), DeliveryConflictPathEncoding::Base64Url);
        assert_eq!(path.value(), "ZGlyL_9uYW1l");
        assert!(!format!("{result:?}").contains("ZGlyL_9uYW1l"));
    }

    #[test]
    fn total_wire_payload_limit_is_enforced_before_results_escape() {
        let paths = (0..17)
            .map(|index| {
                let mut path = format!("{index:02x}").into_bytes();
                for _ in 0..15 {
                    path.push(b'/');
                    path.extend(std::iter::repeat_n(b'a', 255));
                }
                path.push(b'/');
                path.extend(std::iter::repeat_n(b'b', 253));
                assert_eq!(path.len(), MAX_MERGE_CONFLICT_PATH_BYTES);
                path
            })
            .collect::<Vec<_>>();
        let path_refs = paths.iter().map(Vec::as_slice).collect::<Vec<_>>();

        assert_eq!(
            parse_conflict(&conflict_output(&path_refs)),
            Err(DeliveryPreflightError::MalformedMergeTreeOutput)
        );
    }
}
