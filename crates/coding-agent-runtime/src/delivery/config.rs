use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::io::{self, Read};

use sha2::{Digest, Sha256};

use crate::native_fs::child_entry_exists;
use crate::worktree::LinkedWorktreeCommandContext;
use crate::{RelativePath, RootCapability};

use super::{DeliverySourceError, DeliverySourceLimits};

const SNAPSHOT_DOMAIN: &[u8] = b"coding-agent:delivery-git-security-snapshot:v1\0";
const CONFIG_ATTRIBUTES_DIGEST_DOMAIN: &[u8] =
    b"coding-agent:delivery-config-attributes-digest:v1\0";
const CHECKED_ATTRIBUTE_NAMES: [&[u8]; 4] =
    [b"filter", b"diff", b"merge", b"working-tree-encoding"];
const CHECKED_ATTRIBUTE_FIELDS_PER_PATH: usize = CHECKED_ATTRIBUTE_NAMES.len() * 3;

/// Exact raw security inputs authenticated without invoking Git's config
/// machinery. Equality is used across observation boundaries to detect drift.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct GitSecuritySnapshot {
    common_config: RawFileSnapshot,
    common_attributes: RawFileSnapshot,
    digest: [u8; 32],
}

impl GitSecuritySnapshot {
    pub(crate) fn capture_authenticated(
        authenticated: &LinkedWorktreeCommandContext,
        limits: DeliverySourceLimits,
    ) -> Result<Self, DeliverySourceError> {
        Self::capture(
            &authenticated.common_git.capability,
            &authenticated.worktree_admin.capability,
            limits,
        )
    }

    pub(crate) fn capture(
        common_git: &RootCapability,
        worktree_admin: &RootCapability,
        limits: DeliverySourceLimits,
    ) -> Result<Self, DeliverySourceError> {
        require_admin_config_absent(worktree_admin)?;
        let common_config = read_raw_file(common_git, "config", limits.max_config_bytes(), true)?;
        validate_common_config(common_config.contents())?;
        let common_attributes = read_raw_file(
            common_git,
            "info/attributes",
            limits.max_attributes_bytes(),
            false,
        )?;
        validate_attributes(common_attributes.contents())?;
        let digest = snapshot_digest(&common_config, &common_attributes);
        Ok(Self {
            common_config,
            common_attributes,
            digest,
        })
    }

    pub(crate) const fn digest(&self) -> &[u8; 32] {
        &self.digest
    }

    pub(crate) fn config_attributes_digest_builder(&self) -> ConfigAttributesDigestBuilder {
        ConfigAttributesDigestBuilder::new(self.digest)
    }
}

impl std::fmt::Debug for GitSecuritySnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("GitSecuritySnapshot(<redacted>)")
    }
}

/// Incremental, chunk-boundary-independent digest of the raw security snapshot
/// and the exact resolved attributes returned for every requested path.
pub(crate) struct ConfigAttributesDigestBuilder {
    digest: Sha256,
    path_count: u64,
}

impl ConfigAttributesDigestBuilder {
    fn new(raw_snapshot_digest: [u8; 32]) -> Self {
        let mut digest = Sha256::new();
        digest.update(CONFIG_ATTRIBUTES_DIGEST_DOMAIN);
        append_digest_frame(&mut digest, b"raw-security-snapshot", &raw_snapshot_digest);
        Self {
            digest,
            path_count: 0,
        }
    }

    pub(crate) fn append_checked_attributes(
        &mut self,
        output: &[u8],
        requested_paths: &[Vec<u8>],
    ) -> Result<(), DeliverySourceError> {
        let fields = checked_attribute_fields(output, requested_paths)?;
        let added = u64::try_from(requested_paths.len())
            .map_err(|_| DeliverySourceError::BoundsExceeded)?;
        let path_count = self
            .path_count
            .checked_add(added)
            .ok_or(DeliverySourceError::BoundsExceeded)?;

        for (path, path_fields) in requested_paths
            .iter()
            .zip(fields.chunks_exact(CHECKED_ATTRIBUTE_FIELDS_PER_PATH))
        {
            append_digest_frame(&mut self.digest, b"requested-path", path);
            for field in path_fields {
                append_digest_frame(&mut self.digest, b"resolved-attribute-field", field);
            }
        }
        self.path_count = path_count;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> [u8; 32] {
        append_digest_frame(
            &mut self.digest,
            b"requested-path-count",
            &self.path_count.to_be_bytes(),
        );
        self.digest.finalize().into()
    }
}

impl std::fmt::Debug for ConfigAttributesDigestBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ConfigAttributesDigestBuilder(<redacted>)")
    }
}

#[derive(Clone, PartialEq, Eq)]
struct RawFileSnapshot {
    present: bool,
    contents: Vec<u8>,
}

impl RawFileSnapshot {
    fn absent() -> Self {
        Self {
            present: false,
            contents: Vec::new(),
        }
    }

    fn present(contents: Vec<u8>) -> Self {
        Self {
            present: true,
            contents,
        }
    }

    fn contents(&self) -> &[u8] {
        &self.contents
    }
}

fn require_admin_config_absent(admin: &RootCapability) -> Result<(), DeliverySourceError> {
    let root = admin
        .try_clone_root()
        .map_err(|_| DeliverySourceError::AuthenticationChanged)?;
    match child_entry_exists(&root, OsStr::new("config.worktree")) {
        Ok(false) => Ok(()),
        Ok(true) => Err(DeliverySourceError::UnsafeGitConfiguration),
        Err(_) => Err(DeliverySourceError::AuthenticationChanged),
    }
}

fn read_raw_file(
    root: &RootCapability,
    relative: &str,
    limit: usize,
    required: bool,
) -> Result<RawFileSnapshot, DeliverySourceError> {
    let path =
        RelativePath::parse(relative.to_owned()).map_err(|_| DeliverySourceError::Internal)?;
    let mut file = match root.open_file_for_read(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound && !required => {
            return Ok(RawFileSnapshot::absent());
        }
        Err(_) => return Err(DeliverySourceError::AuthenticationChanged),
    };
    let read_limit = u64::try_from(limit)
        .ok()
        .and_then(|value| value.checked_add(1))
        .ok_or(DeliverySourceError::InvalidLimits)?;
    let mut contents = Vec::with_capacity(limit.min(64 * 1024));
    file.by_ref()
        .take(read_limit)
        .read_to_end(&mut contents)
        .map_err(|_| DeliverySourceError::AuthenticationChanged)?;
    if contents.len() > limit {
        return Err(DeliverySourceError::BoundsExceeded);
    }
    Ok(RawFileSnapshot::present(contents))
}

fn snapshot_digest(config: &RawFileSnapshot, attributes: &RawFileSnapshot) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(SNAPSHOT_DOMAIN);
    append_snapshot_file(&mut digest, b"common-config", config);
    append_snapshot_file(&mut digest, b"common-info-attributes", attributes);
    digest.finalize().into()
}

fn append_snapshot_file(digest: &mut Sha256, tag: &[u8], file: &RawFileSnapshot) {
    digest.update((tag.len() as u64).to_be_bytes());
    digest.update(tag);
    digest.update([u8::from(file.present)]);
    digest.update((file.contents.len() as u64).to_be_bytes());
    digest.update(&file.contents);
}

fn append_digest_frame(digest: &mut Sha256, tag: &[u8], value: &[u8]) {
    digest.update((tag.len() as u64).to_be_bytes());
    digest.update(tag);
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
}

fn validate_common_config(contents: &[u8]) -> Result<(), DeliverySourceError> {
    let text =
        std::str::from_utf8(contents).map_err(|_| DeliverySourceError::UnsafeGitConfiguration)?;
    let mut section = ConfigSection::default();
    for raw_line in text.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if line.starts_with('[') {
            section = parse_section(line)?;
            if matches!(section.name.as_str(), "include" | "includeif") {
                return Err(DeliverySourceError::UnsafeGitConfiguration);
            }
            continue;
        }
        let key = parse_key(line)?;
        if config_key_is_unsafe(&section, &key) {
            return Err(DeliverySourceError::UnsafeGitConfiguration);
        }
    }
    Ok(())
}

#[derive(Default)]
struct ConfigSection {
    name: String,
    subsection: Option<String>,
}

fn parse_section(line: &str) -> Result<ConfigSection, DeliverySourceError> {
    let close = line
        .find(']')
        .ok_or(DeliverySourceError::UnsafeGitConfiguration)?;
    let trailing = line[close + 1..].trim();
    if !trailing.is_empty() && !trailing.starts_with('#') && !trailing.starts_with(';') {
        return Err(DeliverySourceError::UnsafeGitConfiguration);
    }
    let body = line[1..close].trim();
    if body.is_empty() || !body.is_ascii() {
        return Err(DeliverySourceError::UnsafeGitConfiguration);
    }
    let (name, subsection) = if let Some(split) = body.find(char::is_whitespace) {
        let name = &body[..split];
        let subsection = parse_quoted_subsection(body[split..].trim())?;
        (name, Some(subsection))
    } else if let Some((name, subsection)) = body.split_once('.') {
        (name, Some(subsection.to_ascii_lowercase()))
    } else {
        (body, None)
    };
    if !valid_config_name(name) {
        return Err(DeliverySourceError::UnsafeGitConfiguration);
    }
    Ok(ConfigSection {
        name: name.to_ascii_lowercase(),
        subsection,
    })
}

fn parse_quoted_subsection(value: &str) -> Result<String, DeliverySourceError> {
    if value.len() < 2 || !value.starts_with('"') || !value.ends_with('"') {
        return Err(DeliverySourceError::UnsafeGitConfiguration);
    }
    let body = &value[1..value.len() - 1];
    if !body.is_ascii()
        || body
            .chars()
            .any(|character| matches!(character, '\n' | '\r' | '\0'))
    {
        return Err(DeliverySourceError::UnsafeGitConfiguration);
    }
    Ok(body.to_ascii_lowercase())
}

fn parse_key(line: &str) -> Result<String, DeliverySourceError> {
    let end = line
        .find(|character: char| character == '=' || character.is_whitespace())
        .unwrap_or(line.len());
    let key = &line[..end];
    if !valid_config_name(key) {
        return Err(DeliverySourceError::UnsafeGitConfiguration);
    }
    Ok(key.to_ascii_lowercase())
}

fn valid_config_name(value: &str) -> bool {
    !value.is_empty()
        && value.is_ascii()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
}

fn config_key_is_unsafe(section: &ConfigSection, key: &str) -> bool {
    match section.name.as_str() {
        "include" | "includeif" => true,
        "extensions" => key == "worktreeconfig",
        "filter" => matches!(key, "clean" | "smudge" | "process" | "required"),
        "diff" => matches!(key, "command" | "textconv"),
        "merge" if section.subsection.is_some() => key == "driver",
        // `merge.verifySignatures` is safe here because every delivery command
        // supplies the fixed `-c merge.verifySignatures=false` override. Keep
        // rejecting the remaining merge knobs, which can alter mutation
        // behavior or select external tooling.
        "merge" => matches!(key, "autostash" | "gpgsign" | "tool"),
        "rerere" => true,
        "branch" => key == "mergeoptions",
        "core" => matches!(key, "hookspath" | "askpass"),
        "commit" | "tag" => key == "gpgsign",
        "user" => key == "signingkey",
        "credential" => key == "helper",
        "gpg" => key == "program" || section.subsection.is_some(),
        "difftool" | "mergetool" => key == "cmd" || key == "path",
        _ => false,
    }
}

fn validate_attributes(contents: &[u8]) -> Result<(), DeliverySourceError> {
    for raw_line in contents.split(|byte| *byte == b'\n') {
        let line = trim_ascii(raw_line);
        if line.is_empty() || line[0] == b'#' {
            continue;
        }
        let mut fields = line.split(|byte| byte.is_ascii_whitespace());
        let _pattern = fields.next();
        for attribute in fields.filter(|field| !field.is_empty()) {
            if dangerous_attribute(attribute) {
                return Err(DeliverySourceError::UnsafeGitConfiguration);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
fn validate_checked_attributes(
    output: &[u8],
    requested_paths: &[Vec<u8>],
) -> Result<(), DeliverySourceError> {
    checked_attribute_fields(output, requested_paths).map(|_| ())
}

fn checked_attribute_fields<'a>(
    output: &'a [u8],
    requested_paths: &[Vec<u8>],
) -> Result<Vec<&'a [u8]>, DeliverySourceError> {
    if output.is_empty() {
        return if requested_paths.is_empty() {
            Ok(Vec::new())
        } else {
            Err(DeliverySourceError::UnsafeGitConfiguration)
        };
    }
    if requested_paths.is_empty() || output.last() != Some(&0) {
        return Err(DeliverySourceError::UnsafeGitConfiguration);
    }
    let expected_fields = requested_paths
        .len()
        .checked_mul(CHECKED_ATTRIBUTE_FIELDS_PER_PATH)
        .ok_or(DeliverySourceError::BoundsExceeded)?;
    let fields = output[..output.len() - 1]
        .split(|byte| *byte == 0)
        .collect::<Vec<_>>();
    if fields.len() != expected_fields {
        return Err(DeliverySourceError::UnsafeGitConfiguration);
    }

    let mut unique_paths = BTreeSet::new();
    for (requested_path, path_fields) in requested_paths
        .iter()
        .zip(fields.chunks_exact(CHECKED_ATTRIBUTE_FIELDS_PER_PATH))
    {
        if requested_path.is_empty()
            || requested_path.contains(&0)
            || !unique_paths.insert(requested_path.as_slice())
        {
            return Err(DeliverySourceError::UnsafeGitConfiguration);
        }
        for (triple, expected_attribute) in path_fields.chunks_exact(3).zip(CHECKED_ATTRIBUTE_NAMES)
        {
            let value_is_disabled = triple[2] == b"unspecified" || triple[2] == b"unset";
            if triple[0] != requested_path || triple[1] != expected_attribute || !value_is_disabled
            {
                return Err(DeliverySourceError::UnsafeGitConfiguration);
            }
        }
    }
    Ok(fields)
}

fn dangerous_attribute(attribute: &[u8]) -> bool {
    let attribute = attribute
        .strip_prefix(b"-")
        .or_else(|| attribute.strip_prefix(b"!"))
        .unwrap_or(attribute);
    let name = attribute
        .split(|byte| *byte == b'=')
        .next()
        .unwrap_or_default();
    name.eq_ignore_ascii_case(b"filter")
        || name.eq_ignore_ascii_case(b"diff")
        || name.eq_ignore_ascii_case(b"merge")
        || name.eq_ignore_ascii_case(b"working-tree-encoding")
}

fn trim_ascii(mut value: &[u8]) -> &[u8] {
    while value.first().is_some_and(u8::is_ascii_whitespace) {
        value = &value[1..];
    }
    while value.last().is_some_and(u8::is_ascii_whitespace) {
        value = &value[..value.len() - 1];
    }
    value
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::Duration;

    use super::*;

    #[test]
    fn dangerous_config_mechanisms_are_rejected_without_parsing_values() {
        for config in [
            "[include]\npath = elsewhere\n",
            "[includeIf \"gitdir:**\"]\npath = elsewhere\n",
            "[filter \"driver\"]\nprocess = helper --arg\n",
            "[diff \"driver\"]\ntextconv = helper\n",
            "[merge \"driver\"]\ndriver = helper %O %A %B\n",
            "[branch \"main\"]\nmergeOptions = --squash\n",
            "[extensions]\nworktreeConfig = true\n",
            "[core]\nhooksPath = hooks\n",
            "[rerere]\nenabled\n",
            "[credential]\nhelper = executable\n",
            "[user]\nsigningKey = secret\n",
        ] {
            assert_eq!(
                validate_common_config(config.as_bytes()),
                Err(DeliverySourceError::UnsafeGitConfiguration),
                "{config}"
            );
        }
    }

    #[test]
    fn ordinary_repository_config_is_accepted() {
        let config = b"[core]\n\trepositoryformatversion = 0\n\tfilemode = false\n[remote \"origin\"]\n\turl = local\n[user]\n\tname = Example\n";
        assert!(validate_common_config(config).is_ok());
    }

    #[test]
    fn merge_verify_signatures_is_accepted_only_for_the_fixed_command_override() {
        assert!(validate_common_config(b"[merge]\nverifySignatures = true\n").is_ok());

        for config in [
            b"[merge]\nautoStash = true\n".as_slice(),
            b"[merge]\ngpgSign = true\n".as_slice(),
            b"[merge]\ntool = external\n".as_slice(),
            b"[branch \"main\"]\nmergeOptions = --squash\n".as_slice(),
        ] {
            assert_eq!(
                validate_common_config(config),
                Err(DeliverySourceError::UnsafeGitConfiguration),
                "{config:?}"
            );
        }
    }

    #[test]
    fn dangerous_attributes_are_rejected() {
        assert!(validate_attributes(b"*.rs text eol=lf\n").is_ok());
        for attributes in [
            &b"*.bin filter=lfs\n"[..],
            &b"*.txt diff=external\n"[..],
            &b"*.dat merge=ours\n"[..],
            &b"*.txt working-tree-encoding=UTF-16\n"[..],
        ] {
            assert_eq!(
                validate_attributes(attributes),
                Err(DeliverySourceError::UnsafeGitConfiguration)
            );
        }
    }

    #[test]
    fn raw_snapshot_is_stable_and_presence_sensitive() {
        let fixture = SnapshotFixture::new();
        let first = fixture.capture();
        let reopened = fixture.capture();
        assert_eq!(first, reopened);
        assert_eq!(first.digest(), reopened.digest());
        assert_eq!(format!("{first:?}"), "GitSecuritySnapshot(<redacted>)");

        fs::create_dir_all(fixture.common_path.join("info")).unwrap();
        fs::write(fixture.common_path.join("info/attributes"), b"").unwrap();
        let present_empty = fixture.capture();
        assert_ne!(first, present_empty);
        assert_ne!(first.digest(), present_empty.digest());
    }

    #[test]
    fn admin_config_worktree_is_rejected_for_every_entry_kind() {
        for kind in ["file", "directory"] {
            let fixture = SnapshotFixture::new();
            let path = fixture.admin_path.join("config.worktree");
            if kind == "file" {
                fs::write(path, b"").unwrap();
            } else {
                fs::create_dir(path).unwrap();
            }
            assert_eq!(
                GitSecuritySnapshot::capture(&fixture.common, &fixture.admin, fixture.limits),
                Err(DeliverySourceError::UnsafeGitConfiguration)
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn admin_config_worktree_symlink_is_rejected_without_following() {
        use std::os::unix::fs::symlink;

        let fixture = SnapshotFixture::new();
        let outside = fixture._temporary.path().join("outside");
        fs::write(&outside, b"[include]\npath = foreign\n").unwrap();
        symlink(outside, fixture.admin_path.join("config.worktree")).unwrap();
        assert_eq!(
            GitSecuritySnapshot::capture(&fixture.common, &fixture.admin, fixture.limits),
            Err(DeliverySourceError::UnsafeGitConfiguration)
        );
    }

    #[test]
    fn raw_snapshot_reads_are_bounded() {
        let fixture = SnapshotFixture::new();
        fs::write(fixture.common_path.join("config"), vec![b'x'; 65]).unwrap();
        assert_eq!(
            GitSecuritySnapshot::capture(&fixture.common, &fixture.admin, fixture.limits),
            Err(DeliverySourceError::BoundsExceeded)
        );
    }

    #[test]
    fn checked_attribute_protocol_matches_every_requested_path_exactly() {
        let one = b"one".to_vec();
        let two = b"two".to_vec();
        let one_safe = checked_attributes(
            &one,
            [
                b"unspecified".as_slice(),
                b"unset".as_slice(),
                b"unspecified".as_slice(),
                b"unset".as_slice(),
            ],
        );
        let two_safe = checked_attributes(
            &two,
            [
                b"unset".as_slice(),
                b"unspecified".as_slice(),
                b"unset".as_slice(),
                b"unspecified".as_slice(),
            ],
        );

        assert!(validate_checked_attributes(b"", &[]).is_ok());
        assert!(validate_checked_attributes(&one_safe, std::slice::from_ref(&one)).is_ok());

        let mut both = one_safe.clone();
        both.extend_from_slice(&two_safe);
        assert!(validate_checked_attributes(&both, &[one.clone(), two.clone()]).is_ok());
        let mut duplicate_output = one_safe.clone();
        duplicate_output.extend_from_slice(&one_safe);

        for (output, requested) in [
            (Vec::new(), vec![one.clone()]),
            (one_safe.clone(), Vec::new()),
            (one_safe.clone(), vec![two.clone()]),
            (one_safe.clone(), vec![one.clone(), two.clone()]),
            (both.clone(), vec![one.clone()]),
            (duplicate_output, vec![one.clone(), one.clone()]),
        ] {
            assert_eq!(
                validate_checked_attributes(&output, &requested),
                Err(DeliverySourceError::UnsafeGitConfiguration),
                "output={output:?}, requested={requested:?}"
            );
        }
    }

    #[test]
    fn checked_attribute_protocol_rejects_reordering_and_effective_values() {
        let path = b"one".to_vec();
        let mut reordered = checked_attributes(
            &path,
            [
                b"unspecified".as_slice(),
                b"unset".as_slice(),
                b"unspecified".as_slice(),
                b"unset".as_slice(),
            ],
        );
        let diff = find_subslice(&reordered, b"diff").unwrap();
        reordered[diff..diff + 4].copy_from_slice(b"xxxx");
        assert_eq!(
            validate_checked_attributes(&reordered, std::slice::from_ref(&path)),
            Err(DeliverySourceError::UnsafeGitConfiguration)
        );

        for values in [
            [b"driver".as_slice(), b"unset", b"unspecified", b"unset"],
            [
                b"unspecified".as_slice(),
                b"driver",
                b"unset",
                b"unspecified",
            ],
            [b"unset".as_slice(), b"unspecified", b"ours", b"unset"],
            [
                b"unspecified".as_slice(),
                b"unset",
                b"unspecified",
                b"UTF-16",
            ],
        ] {
            let output = checked_attributes(&path, values);
            assert_eq!(
                validate_checked_attributes(&output, std::slice::from_ref(&path)),
                Err(DeliverySourceError::UnsafeGitConfiguration)
            );
        }

        assert_eq!(
            validate_checked_attributes(b"unterminated", std::slice::from_ref(&path)),
            Err(DeliverySourceError::UnsafeGitConfiguration)
        );
    }

    #[test]
    fn config_attributes_digest_binds_snapshot_paths_and_resolved_output() {
        let fixture = SnapshotFixture::new();
        let snapshot = fixture.capture();
        let one = b"one".to_vec();
        let two = b"two".to_vec();
        let one_output = checked_attributes(
            &one,
            [
                b"unspecified".as_slice(),
                b"unset".as_slice(),
                b"unspecified".as_slice(),
                b"unset".as_slice(),
            ],
        );
        let two_output = checked_attributes(
            &two,
            [
                b"unset".as_slice(),
                b"unspecified".as_slice(),
                b"unset".as_slice(),
                b"unspecified".as_slice(),
            ],
        );

        let mut combined = snapshot.config_attributes_digest_builder();
        let mut all_output = one_output.clone();
        all_output.extend_from_slice(&two_output);
        combined
            .append_checked_attributes(&all_output, &[one.clone(), two.clone()])
            .unwrap();

        let mut chunked = snapshot.config_attributes_digest_builder();
        chunked
            .append_checked_attributes(&one_output, std::slice::from_ref(&one))
            .unwrap();
        chunked
            .append_checked_attributes(&two_output, std::slice::from_ref(&two))
            .unwrap();
        assert_eq!(combined.finish(), chunked.finish());

        let mut changed_path = snapshot.config_attributes_digest_builder();
        let other = b"other".to_vec();
        let other_output = checked_attributes(
            &other,
            [
                b"unspecified".as_slice(),
                b"unset".as_slice(),
                b"unspecified".as_slice(),
                b"unset".as_slice(),
            ],
        );
        changed_path
            .append_checked_attributes(&other_output, std::slice::from_ref(&other))
            .unwrap();

        let mut changed_value = snapshot.config_attributes_digest_builder();
        let changed_output = checked_attributes(
            &one,
            [
                b"unset".as_slice(),
                b"unset".as_slice(),
                b"unspecified".as_slice(),
                b"unset".as_slice(),
            ],
        );
        changed_value
            .append_checked_attributes(&changed_output, std::slice::from_ref(&one))
            .unwrap();

        let mut original = snapshot.config_attributes_digest_builder();
        original
            .append_checked_attributes(&one_output, std::slice::from_ref(&one))
            .unwrap();
        let original = original.finish();
        assert_ne!(original, changed_path.finish());
        assert_ne!(original, changed_value.finish());

        fs::write(
            fixture.common_path.join("config"),
            b"[core]\n\tbare = false\n\tfilemode = false\n",
        )
        .unwrap();
        let changed_snapshot = fixture.capture();
        let mut changed_raw = changed_snapshot.config_attributes_digest_builder();
        changed_raw
            .append_checked_attributes(&one_output, std::slice::from_ref(&one))
            .unwrap();
        assert_ne!(original, changed_raw.finish());
    }

    fn checked_attributes(path: &[u8], values: [&[u8]; 4]) -> Vec<u8> {
        let mut output = Vec::new();
        for (attribute, value) in [
            b"filter".as_slice(),
            b"diff".as_slice(),
            b"merge".as_slice(),
            b"working-tree-encoding".as_slice(),
        ]
        .into_iter()
        .zip(values)
        {
            output.extend_from_slice(path);
            output.push(0);
            output.extend_from_slice(attribute);
            output.push(0);
            output.extend_from_slice(value);
            output.push(0);
        }
        output
    }

    fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack
            .windows(needle.len())
            .position(|candidate| candidate == needle)
    }

    struct SnapshotFixture {
        _temporary: tempfile::TempDir,
        common_path: std::path::PathBuf,
        admin_path: std::path::PathBuf,
        common: RootCapability,
        admin: RootCapability,
        limits: DeliverySourceLimits,
    }

    impl SnapshotFixture {
        fn new() -> Self {
            let temporary = tempfile::tempdir().unwrap();
            let common_path = temporary.path().join("common");
            let admin_path = temporary.path().join("admin");
            fs::create_dir(&common_path).unwrap();
            fs::create_dir(&admin_path).unwrap();
            fs::write(common_path.join("config"), b"[core]\n\tbare = false\n").unwrap();
            let common = RootCapability::open(common_path.canonicalize().unwrap()).unwrap();
            let admin = RootCapability::open(admin_path.canonicalize().unwrap()).unwrap();
            let limits =
                DeliverySourceLimits::try_new(Duration::from_secs(1), 64, 64, 64, 4).unwrap();
            Self {
                _temporary: temporary,
                common_path,
                admin_path,
                common,
                admin,
                limits,
            }
        }

        fn capture(&self) -> GitSecuritySnapshot {
            GitSecuritySnapshot::capture(&self.common, &self.admin, self.limits).unwrap()
        }
    }
}
