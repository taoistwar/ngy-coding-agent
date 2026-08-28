use super::*;

impl ValidatedCommand {
    pub(crate) fn delivery_repository_object_format(
        binding: &DeliveryGitReadOnlyBinding,
    ) -> Result<Self, CommandPolicyError> {
        delivery_read_only_command(binding, ["rev-parse", "--show-object-format"])
    }

    pub(crate) fn delivery_resolve_head(
        binding: &DeliveryGitReadOnlyBinding,
    ) -> Result<Self, CommandPolicyError> {
        delivery_read_only_command(
            binding,
            ["rev-parse", "--verify", "--end-of-options", "HEAD^{commit}"],
        )
    }

    pub(crate) fn delivery_symbolic_head(
        binding: &DeliveryGitReadOnlyBinding,
    ) -> Result<Self, CommandPolicyError> {
        delivery_read_only_command(binding, ["symbolic-ref", "--quiet", "HEAD"])
    }

    pub(crate) fn delivery_index_entries(
        binding: &DeliveryGitReadOnlyBinding,
    ) -> Result<Self, CommandPolicyError> {
        delivery_read_only_command(
            binding,
            ["ls-files", "--cached", "--stage", "-v", "-z", "--"],
        )
    }

    pub(crate) fn delivery_untracked_paths(
        binding: &DeliveryGitReadOnlyBinding,
    ) -> Result<Self, CommandPolicyError> {
        delivery_read_only_command(
            binding,
            ["ls-files", "--others", "--exclude-standard", "-z", "--"],
        )
    }

    /// Captures the complete target cleanliness state in a machine-readable
    /// form.  The fixed vocabulary deliberately includes untracked files and
    /// submodules: a target checkout is eligible only when this output is
    /// empty, rather than merely when its tracked diff is empty.
    pub(crate) fn delivery_target_status(
        binding: &DeliveryGitReadOnlyBinding,
    ) -> Result<Self, CommandPolicyError> {
        delivery_read_only_command(
            binding,
            [
                "status",
                "--porcelain=v2",
                "-z",
                "--untracked-files=all",
                "--ignore-submodules=none",
                "--no-renames",
                "--",
            ],
        )
    }

    /// Lists every unmerged index entry in the target checkout.  The target
    /// observer treats any output as an in-progress/conflicted index rather
    /// than attempting a repair.
    pub(crate) fn delivery_target_unmerged_entries(
        binding: &DeliveryGitReadOnlyBinding,
    ) -> Result<Self, CommandPolicyError> {
        delivery_read_only_command(binding, ["ls-files", "--unmerged", "-z", "--"])
    }

    /// Lists all tracked target paths for the pre-merge attribute safety
    /// check.  This intentionally has no caller-selected pathspec.
    pub(crate) fn delivery_target_tracked_paths(
        binding: &DeliveryGitReadOnlyBinding,
    ) -> Result<Self, CommandPolicyError> {
        delivery_read_only_command(binding, ["ls-files", "--cached", "-z", "--"])
    }

    /// Lists only ignored, untracked target paths.  `--directory` preserves
    /// ignored directory collision information, including empty directories,
    /// without incorrectly classifying tracked paths by their ignore rules.
    pub(crate) fn delivery_target_ignored_untracked_paths(
        binding: &DeliveryGitReadOnlyBinding,
    ) -> Result<Self, CommandPolicyError> {
        delivery_read_only_command(
            binding,
            [
                "ls-files",
                "--others",
                "--ignored",
                "--exclude-standard",
                "--directory",
                "-z",
                "--",
            ],
        )
    }

    /// Returns the fixed `merge-base --is-ancestor` predicate used to reject
    /// a source already contained by the target.  Both revisions are complete
    /// object IDs validated against the probed object format; no revision
    /// expression or ref name enters the command surface.
    pub(crate) fn delivery_source_is_ancestor_of_target(
        binding: &DeliveryGitReadOnlyBinding,
        source: &str,
        target: &str,
        object_id_length: usize,
    ) -> Result<Self, CommandPolicyError> {
        require_delivery_object_id(source, object_id_length)?;
        require_delivery_object_id(target, object_id_length)?;
        delivery_read_only_command(binding, ["merge-base", "--is-ancestor", source, target])
    }

    /// Lists the complete merge-base set for two authenticated commit IDs.
    ///
    /// `--all` deliberately prevents Git from selecting one unspecified base
    /// when a criss-cross history has multiple best common ancestors. The
    /// preflight parser must therefore accept exactly the deterministic shape
    /// it supports, rather than silently relying on Git's selection order.
    pub(crate) fn delivery_merge_base(
        binding: &DeliveryGitReadOnlyBinding,
        target: &str,
        source: &str,
        object_id_length: usize,
    ) -> Result<Self, CommandPolicyError> {
        require_delivery_object_id(target, object_id_length)?;
        require_delivery_object_id(source, object_id_length)?;
        delivery_read_only_command(binding, ["merge-base", "--all", target, source])
    }

    /// Runs the Git >=2.45 object-only merge preflight.  This command never
    /// accepts a caller-selected ref, pathspec, configuration, or argument;
    /// target and source are both validated complete commit IDs.
    pub(crate) fn delivery_merge_tree(
        binding: &DeliveryGitReadOnlyBinding,
        target: &str,
        source: &str,
        object_id_length: usize,
    ) -> Result<Self, CommandPolicyError> {
        require_delivery_object_id(target, object_id_length)?;
        require_delivery_object_id(source, object_id_length)?;
        delivery_read_only_command(
            binding,
            [
                "merge-tree",
                "--write-tree",
                "--messages",
                "--name-only",
                "-z",
                target,
                source,
            ],
        )
    }

    /// Computes the exact changed path set between the authenticated target
    /// commit and a preflight merge tree.  This remains object-only: no
    /// worktree pathname supplied by a caller reaches Git.
    pub(crate) fn delivery_merge_write_set(
        binding: &DeliveryGitReadOnlyBinding,
        target: &str,
        merged_tree: &str,
        object_id_length: usize,
    ) -> Result<Self, CommandPolicyError> {
        require_delivery_object_id(target, object_id_length)?;
        require_delivery_object_id(merged_tree, object_id_length)?;
        delivery_read_only_command(
            binding,
            [
                "diff-tree",
                "--no-commit-id",
                "--name-only",
                "-r",
                "-z",
                "--no-renames",
                "--no-ext-diff",
                target,
                merged_tree,
                "--",
            ],
        )
    }

    /// Describes the exact tree entries a preflight merge would change from
    /// the authenticated old target. Unlike the name-only preflight listing,
    /// this fixed raw protocol carries the old/new modes and object IDs needed
    /// to bind ordinary stage-0 entries observed after an actual merge exits
    /// with conflicts. Both object arguments are validated complete IDs and
    /// no caller-selected path is admitted to argv.
    pub(crate) fn delivery_expected_merge_raw_diff(
        binding: &DeliveryGitReadOnlyBinding,
        target: &str,
        merged_tree: &str,
        object_id_length: usize,
    ) -> Result<Self, CommandPolicyError> {
        require_delivery_object_id(target, object_id_length)?;
        require_delivery_object_id(merged_tree, object_id_length)?;
        let abbreviation = match object_id_length {
            40 => "--abbrev=40",
            64 => "--abbrev=64",
            _ => return Err(CommandPolicyError::InvalidGitBinding),
        };
        delivery_read_only_command(
            binding,
            [
                "diff-tree",
                "--no-commit-id",
                "--raw",
                abbreviation,
                "-r",
                "-z",
                "--no-renames",
                "--no-ext-diff",
                target,
                merged_tree,
                "--",
            ],
        )
    }

    /// Reads the base/source blob entry for each already-observed conflict
    /// path. The object is a complete authenticated commit ID and every path
    /// is a bounded literal pathspec. The command deliberately omits `-r`, so
    /// a directory entry cannot expand into an unbounded recursive listing;
    /// the strict consumer rejects a returned tree entry instead.
    pub(crate) fn delivery_expected_conflict_tree_entries(
        binding: &DeliveryGitReadOnlyBinding,
        commit: &str,
        paths: &[&str],
        object_id_length: usize,
    ) -> Result<Self, CommandPolicyError> {
        if !matches!(object_id_length, 40 | 64) {
            return Err(CommandPolicyError::InvalidGitBinding);
        }
        require_delivery_object_id(commit, object_id_length)?;
        if paths.is_empty() || paths.len() > MAX_MERGE_CONFLICT_PATHS {
            return Err(CommandPolicyError::InvalidGitPath);
        }

        let mut unique = BTreeSet::new();
        let mut payload_bytes = 0usize;
        for path in paths {
            if path.is_empty() || path.len() > MAX_MERGE_CONFLICT_PATH_BYTES {
                return Err(CommandPolicyError::InvalidGitPath);
            }
            validate_git_diff_path(OsStr::new(path))?;
            if !unique.insert(*path) {
                return Err(CommandPolicyError::InvalidGitPath);
            }
            payload_bytes = payload_bytes
                .checked_add(path.len())
                .ok_or(CommandPolicyError::InvalidGitPath)?;
            if payload_bytes > MAX_MERGE_CONFLICT_PAYLOAD_BYTES {
                return Err(CommandPolicyError::InvalidGitPath);
            }
        }

        let abbreviation = match object_id_length {
            40 => "--abbrev=40",
            64 => "--abbrev=64",
            _ => unreachable!("object format length was checked above"),
        };
        let mut arguments = ["ls-tree", "-z", "--full-tree", abbreviation, commit, "--"]
            .into_iter()
            .map(OsString::from)
            .collect::<Vec<_>>();
        arguments.extend(paths.iter().map(|path| OsString::from(*path)));
        delivery_read_only_owned_command(binding, arguments)
    }

    /// Lists the current-index paths deleted from one already authenticated
    /// base commit.  `--no-renames` makes the output an unambiguous sequence
    /// of old-path tombstones even when repository configuration enables
    /// rename detection.
    pub(crate) fn delivery_deleted_base_paths(
        binding: &DeliveryGitReadOnlyBinding,
        base: &str,
        object_id_length: usize,
    ) -> Result<Self, CommandPolicyError> {
        require_delivery_object_id(base, object_id_length)?;
        delivery_read_only_command(
            binding,
            [
                "diff-index",
                "--cached",
                "--no-renames",
                "--no-ext-diff",
                "--diff-filter=D",
                "--name-only",
                "-z",
                base,
                "--",
            ],
        )
    }

    pub(crate) fn delivery_check_attributes(
        binding: &DeliveryGitReadOnlyBinding,
        input: Vec<u8>,
    ) -> Result<Self, CommandPolicyError> {
        let mut command = delivery_read_only_command(
            binding,
            [
                "check-attr",
                "-z",
                "--stdin",
                "filter",
                "diff",
                "merge",
                "working-tree-encoding",
            ],
        )?;
        command.exact_input = Some(
            ExactChildInput::try_new(input).map_err(|_| CommandPolicyError::InvalidGitBinding)?,
        );
        Ok(command)
    }
}
