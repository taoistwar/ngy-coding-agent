//! Fail-closed ignored-path collision detection for a registered target.
//!
//! Git supplies both inputs as NUL-delimited byte paths.  This module never
//! turns them into filesystem paths for Git, and it uses the target's retained
//! root capability only to prove that a colliding ignored node is either a
//! plain no-follow file/directory or an unsafe link/reparse node before
//! reporting the collision.

use std::{collections::BTreeSet, io};

use crate::{RelativePath, RootCapability};

use super::DeliveryTargetError;

const MAX_TARGET_PATH_ENTRIES: usize = 16_384;
const MAX_TARGET_PATH_BYTES: usize = 4_096;

#[derive(Clone)]
pub(super) struct TargetIgnoredPath {
    path: Vec<u8>,
    directory_hint: bool,
}

impl TargetIgnoredPath {
    fn parse(record: &[u8]) -> Result<Self, DeliveryTargetError> {
        let directory_hint = record.last() == Some(&b'/');
        let path = if directory_hint {
            &record[..record.len().saturating_sub(1)]
        } else {
            record
        };
        validate_raw_target_path(path)?;
        Ok(Self {
            path: path.to_vec(),
            directory_hint,
        })
    }

    /// Retains the already validated Git byte path inside the delivery module.
    /// It is used only to feed a fixed attribute-check command before actual
    /// merge spawn; no path crosses the public capability boundary.
    pub(super) fn raw_path(&self) -> &[u8] {
        &self.path
    }
}

impl std::fmt::Debug for TargetIgnoredPath {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("TargetIgnoredPath(<validated>)")
    }
}

/// Strictly decodes a NUL-delimited listing emitted by one fixed target
/// command. An empty listing is represented by an empty output. Duplicate,
/// malformed, or unterminated records are not normalized away because they
/// would make the collision proof ambiguous.
pub(super) fn parse_target_path_listing(
    output: &[u8],
    output_limit: usize,
    ignored_listing: bool,
) -> Result<Vec<TargetIgnoredPath>, DeliveryTargetError> {
    if output.len() > output_limit {
        return Err(DeliveryTargetError::BoundsExceeded);
    }
    if output.is_empty() {
        return Ok(Vec::new());
    }
    if output.last() != Some(&0) {
        return Err(DeliveryTargetError::CommandFailed);
    }

    let mut paths = Vec::new();
    let mut unique = BTreeSet::new();
    let mut payload_bytes = 0usize;
    for record in output[..output.len() - 1].split(|byte| *byte == 0) {
        if record.is_empty() {
            return Err(DeliveryTargetError::CommandFailed);
        }
        if paths.len() == MAX_TARGET_PATH_ENTRIES {
            return Err(DeliveryTargetError::BoundsExceeded);
        }
        let path = if ignored_listing {
            TargetIgnoredPath::parse(record)?
        } else {
            TargetIgnoredPath {
                path: validate_raw_target_path(record)?.to_vec(),
                directory_hint: false,
            }
        };
        if !unique.insert(path.path.clone()) {
            return Err(DeliveryTargetError::CommandFailed);
        }
        payload_bytes = payload_bytes
            .checked_add(path.path.len())
            .ok_or(DeliveryTargetError::BoundsExceeded)?;
        if payload_bytes > output_limit {
            return Err(DeliveryTargetError::BoundsExceeded);
        }
        paths.push(path);
    }
    Ok(paths)
}

/// Rejects every equal, ancestor, or descendant intersection between an
/// ignored untracked target node and a merge-result write set. The caller gets
/// no path disclosure. Before returning the collision, the relevant ignored
/// entry is opened through the retained target root without following links.
/// A no-follow link or reparse rejection is itself an unsafe collision; a
/// missing, replaced, wrong-kind, or otherwise unproven node fails
/// authentication instead of being treated as harmless.
pub(super) fn require_no_ignored_target_collision(
    target_root: &RootCapability,
    ignored: &[TargetIgnoredPath],
    write_set: &[TargetIgnoredPath],
) -> Result<(), DeliveryTargetError> {
    for ignored_path in ignored {
        if write_set
            .iter()
            .any(|write_path| paths_intersect(&ignored_path.path, &write_path.path))
        {
            validate_ignored_node(target_root, ignored_path)?;
            return Err(DeliveryTargetError::TargetIgnoredPathCollision);
        }
    }
    Ok(())
}

fn validate_ignored_node(
    target_root: &RootCapability,
    ignored: &TargetIgnoredPath,
) -> Result<(), DeliveryTargetError> {
    let text = std::str::from_utf8(&ignored.path)
        .map_err(|_| DeliveryTargetError::AuthenticationChanged)?;
    let relative = RelativePath::parse(text.to_owned())
        .map_err(|_| DeliveryTargetError::AuthenticationChanged)?;
    let result = if ignored.directory_hint {
        target_root.open_directory(&relative).map(|_| ())
    } else {
        target_root.open_file_for_read(&relative).map(|_| ())
    };
    result.map_err(map_ignored_node_open_error)
}

/// `RootCapability` keeps link/reparse handling fail-closed at the native
/// boundary. Preserve that proof while exposing the stable collision result
/// required when Git has already identified the node as relevant to the merge
/// write set.
fn map_ignored_node_open_error(error: io::Error) -> DeliveryTargetError {
    if ignored_node_is_link_or_reparse_error(&error) {
        DeliveryTargetError::TargetIgnoredPathCollision
    } else {
        DeliveryTargetError::AuthenticationChanged
    }
}

#[cfg(unix)]
fn ignored_node_is_link_or_reparse_error(error: &io::Error) -> bool {
    // `RootCapability` opens every Unix component with O_NOFOLLOW. ELOOP is
    // therefore the native proof that a relevant component is a link; it is
    // never followed before this classification.
    error.raw_os_error() == Some(libc::ELOOP)
}

#[cfg(windows)]
fn ignored_node_is_link_or_reparse_error(error: &io::Error) -> bool {
    // Windows opens with FILE_OPEN_REPARSE_POINT, then RootCapability's
    // ensure_plain_file/directory rejects the retained reparse handle as
    // InvalidData. Wrong-kind opens fail before that check as the platform
    // type error and remain authentication failures below.
    error.kind() == io::ErrorKind::InvalidData
}

fn validate_raw_target_path(path: &[u8]) -> Result<&[u8], DeliveryTargetError> {
    if path.is_empty()
        || path.len() > MAX_TARGET_PATH_BYTES
        || path.contains(&0)
        || matches!(path.first(), Some(b'/' | b'\\'))
    {
        return Err(DeliveryTargetError::CommandFailed);
    }
    let text = std::str::from_utf8(path).map_err(|_| DeliveryTargetError::AuthenticationChanged)?;
    RelativePath::parse(text.to_owned()).map_err(|_| DeliveryTargetError::AuthenticationChanged)?;
    Ok(path)
}

#[cfg(not(windows))]
fn paths_intersect(left: &[u8], right: &[u8]) -> bool {
    left == right
        || left
            .strip_prefix(right)
            .is_some_and(|suffix| suffix.first() == Some(&b'/'))
        || right
            .strip_prefix(left)
            .is_some_and(|suffix| suffix.first() == Some(&b'/'))
}

/// `RootCapability` opens Windows child names with `OBJ_CASE_INSENSITIVE`.
/// The collision proof must therefore use the same conservative namespace
/// relation instead of treating raw Git bytes as case-sensitive names. ASCII
/// components use the exact Windows-insensitive relation we can prove. For
/// non-ASCII component pairs whose Windows upcasing relation is not exposed
/// by Rust, treating them as potentially equal is deliberately conservative:
/// it rejects an ambiguous delivery rather than miss a possible overwrite.
#[cfg(windows)]
fn paths_intersect(left: &[u8], right: &[u8]) -> bool {
    let mut left_components = left.split(|byte| *byte == b'/');
    let mut right_components = right.split(|byte| *byte == b'/');
    loop {
        match (left_components.next(), right_components.next()) {
            (Some(left), Some(right)) if windows_components_may_alias(left, right) => {}
            (Some(_), Some(_)) => return false,
            (None, None) | (None, Some(_)) | (Some(_), None) => return true,
        }
    }
}

#[cfg(windows)]
fn windows_components_may_alias(left: &[u8], right: &[u8]) -> bool {
    left == right
        || (left.is_ascii() && right.is_ascii() && left.eq_ignore_ascii_case(right))
        || !left.is_ascii()
        || !right.is_ascii()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_listing_is_nul_framed_bounded_and_deduplicated() {
        let ignored = parse_target_path_listing(b"ignored/\0other.bin\0", 128, true).unwrap();
        assert_eq!(ignored.len(), 2);
        assert!(ignored[0].directory_hint);
        assert!(!ignored[1].directory_hint);

        for malformed in [
            b"unterminated".as_slice(),
            b"same\0same\0".as_slice(),
            b"one\0\0".as_slice(),
        ] {
            assert!(parse_target_path_listing(malformed, 128, true).is_err());
        }
        assert!(matches!(
            parse_target_path_listing(b"too-long\0", 8, true),
            Err(DeliveryTargetError::BoundsExceeded)
        ));
        assert!(parse_target_path_listing(b"exact\0", 6, true).is_ok());
        assert_eq!(parse_target_path_listing(b"", 0, true).unwrap().len(), 0);
    }

    #[test]
    fn equal_and_parent_child_paths_intersect_without_string_normalization() {
        for (left, right) in [
            (b"same".as_slice(), b"same".as_slice()),
            (b"ignored".as_slice(), b"ignored/child".as_slice()),
            (b"ignored/child".as_slice(), b"ignored".as_slice()),
        ] {
            assert!(paths_intersect(left, right));
        }
        assert!(!paths_intersect(b"ignored-a", b"ignored-b"));
        assert!(!paths_intersect(b"ignored", b"ignored-a"));
    }

    #[cfg(windows)]
    #[test]
    fn windows_collision_relation_matches_no_follow_case_insensitive_opens() {
        assert!(paths_intersect(b"IGNORED", b"ignored/child"));
        assert!(paths_intersect(
            "dir/\u{00e9}".as_bytes(),
            "DIR/\u{03b2}".as_bytes()
        ));
        assert!(!paths_intersect(b"ignored-a", b"IGNORED-b"));
    }

    #[test]
    fn collision_requires_a_plain_existing_file_or_directory() {
        let temporary = tempfile::tempdir().unwrap();
        std::fs::write(temporary.path().join("ignored-file"), b"keep").unwrap();
        std::fs::create_dir(temporary.path().join("ignored-directory")).unwrap();
        let root = RootCapability::open(temporary.path()).unwrap();

        let ignored_file = parse_target_path_listing(b"ignored-file\0", 128, true).unwrap();
        let file_write = parse_target_path_listing(b"ignored-file\0", 128, false).unwrap();
        assert_eq!(
            require_no_ignored_target_collision(&root, &ignored_file, &file_write),
            Err(DeliveryTargetError::TargetIgnoredPathCollision)
        );

        let ignored_directory =
            parse_target_path_listing(b"ignored-directory/\0", 128, true).unwrap();
        let child_write =
            parse_target_path_listing(b"ignored-directory/child\0", 128, false).unwrap();
        assert_eq!(
            require_no_ignored_target_collision(&root, &ignored_directory, &child_write),
            Err(DeliveryTargetError::TargetIgnoredPathCollision)
        );

        let wrong_kind = parse_target_path_listing(b"ignored-file/\0", 128, true).unwrap();
        assert_eq!(
            require_no_ignored_target_collision(&root, &wrong_kind, &file_write),
            Err(DeliveryTargetError::AuthenticationChanged)
        );
    }

    #[cfg(unix)]
    #[test]
    fn ignored_symlink_is_never_followed_when_reporting_a_collision() {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().unwrap();
        std::fs::write(temporary.path().join("target"), b"outside").unwrap();
        symlink("target", temporary.path().join("ignored-link")).unwrap();
        let root = RootCapability::open(temporary.path()).unwrap();
        let ignored = parse_target_path_listing(b"ignored-link\0", 128, true).unwrap();
        let write_set = parse_target_path_listing(b"ignored-link\0", 128, false).unwrap();

        assert_eq!(
            require_no_ignored_target_collision(&root, &ignored, &write_set),
            Err(DeliveryTargetError::TargetIgnoredPathCollision)
        );

        let ignored_as_directory =
            parse_target_path_listing(b"ignored-link/\0", 128, true).unwrap();
        assert_eq!(
            require_no_ignored_target_collision(&root, &ignored_as_directory, &write_set),
            Err(DeliveryTargetError::TargetIgnoredPathCollision)
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_reparse_rejection_reports_the_relevant_collision() {
        assert_eq!(
            map_ignored_node_open_error(io::Error::from(io::ErrorKind::InvalidData)),
            DeliveryTargetError::TargetIgnoredPathCollision
        );
        assert_eq!(
            map_ignored_node_open_error(io::Error::from(io::ErrorKind::NotFound)),
            DeliveryTargetError::AuthenticationChanged
        );
        assert_eq!(
            map_ignored_node_open_error(io::Error::from(io::ErrorKind::IsADirectory)),
            DeliveryTargetError::AuthenticationChanged
        );
    }
}
