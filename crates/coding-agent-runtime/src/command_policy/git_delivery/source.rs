use super::*;

/// A retained temporary-index directory that can only contribute the fixed
/// `index` child as `GIT_INDEX_FILE`.
///
/// The path is deliberately not exposed to delivery callers. The source-tree
/// lifecycle owns creation, post-write validation and cleanup of the directory;
/// this command-policy value only binds it to the child process which performs
/// one of the admitted index mutations.
#[derive(Clone)]
pub(crate) struct DeliveryGitTemporaryIndexEnvironment {
    directory: Arc<ExecutionDirectory>,
}

impl DeliveryGitTemporaryIndexEnvironment {
    pub(crate) fn try_new(directory: Arc<ExecutionDirectory>) -> Result<Self, CommandPolicyError> {
        directory.revalidate()?;
        Ok(Self { directory })
    }

    pub(super) fn child_environment(
        &self,
        environment: &ChildEnvironment,
    ) -> Result<ChildEnvironment, CommandPolicyError> {
        self.directory.revalidate()?;
        let mut entries = environment.entries().clone();
        if entries.contains_key(&OsString::from("GIT_INDEX_FILE")) {
            return Err(CommandPolicyError::InvalidGitBinding);
        }
        #[cfg(unix)]
        let index_file = OsString::from(DELIVERY_GIT_TEMPORARY_INDEX_SENTINEL);
        #[cfg(windows)]
        let index_file = self.index_file_path();
        entries.insert(OsString::from("GIT_INDEX_FILE"), index_file);
        Ok(ChildEnvironment::from_entries(entries))
    }

    pub(super) fn require_distinct_from(
        &self,
        binding: &DeliveryGitReadOnlyBinding,
    ) -> Result<(), CommandPolicyError> {
        self.directory.revalidate()?;
        for directory in [
            &binding.repository.git_directory,
            &binding.repository.work_tree,
            &binding.common_git,
            &binding.sandbox,
        ] {
            if self.directory.has_same_identity(directory) {
                return Err(CommandPolicyError::InvalidGitBinding);
            }
        }
        Ok(())
    }

    pub(super) fn directory(&self) -> &Arc<ExecutionDirectory> {
        &self.directory
    }

    #[cfg(windows)]
    pub(super) fn index_file_path(&self) -> OsString {
        child_visible_path(&self.directory.path().join("index")).into_os_string()
    }
}

impl fmt::Debug for DeliveryGitTemporaryIndexEnvironment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryGitTemporaryIndexEnvironment(<opaque>)")
    }
}

/// Fixed author and committer metadata for one deterministic source commit.
///
/// No caller-supplied environment entries are admitted: the application owns
/// the identities and only persists a positive UTC epoch second. Git rejects
/// the otherwise syntactically tempting zero epoch date form, so the command
/// policy fails closed before any object or target mutation command is built.
pub(crate) struct DeliveryGitCommitEnvironment {
    date: OsString,
}

impl DeliveryGitCommitEnvironment {
    pub(crate) fn try_new(epoch_seconds: i64) -> Result<Self, CommandPolicyError> {
        if epoch_seconds <= 0 {
            return Err(CommandPolicyError::InvalidGitBinding);
        }
        Ok(Self {
            date: OsString::from(format!("{epoch_seconds} +0000")),
        })
    }

    pub(super) fn child_environment(
        &self,
        environment: &ChildEnvironment,
    ) -> Result<ChildEnvironment, CommandPolicyError> {
        let mut entries = environment.entries().clone();
        for (key, value) in [
            ("GIT_AUTHOR_NAME", OsString::from("Coding Agent")),
            ("GIT_AUTHOR_EMAIL", OsString::from("coding-agent@localhost")),
            ("GIT_AUTHOR_DATE", self.date.clone()),
            ("GIT_COMMITTER_NAME", OsString::from("Coding Agent")),
            (
                "GIT_COMMITTER_EMAIL",
                OsString::from("coding-agent@localhost"),
            ),
            ("GIT_COMMITTER_DATE", self.date.clone()),
        ] {
            if entries.insert(OsString::from(key), value).is_some() {
                return Err(CommandPolicyError::InvalidGitBinding);
            }
        }
        Ok(ChildEnvironment::from_entries(entries))
    }
}

impl fmt::Debug for DeliveryGitCommitEnvironment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryGitCommitEnvironment(<opaque>)")
    }
}

/// Capability-bound, fixed-shape source mutation commands.
///
/// This is intentionally not a generic Git command builder. It copies the
/// already-authenticated read-only delivery binding and accepts only the
/// source-object and real-index actions admitted by Tasks 11 and 12.
pub(crate) struct DeliveryGitSourceMutationBinding {
    factory: DeliveryGitMutationCommandFactory,
    binding: DeliveryGitReadOnlyBinding,
    object_id_length: usize,
    source_ref: Option<DeliveryGitSourceRef>,
}

/// The one source branch which a real-index capability may update.
///
/// The branch name is validated once while the capability is narrowed, then
/// retained as the complete `refs/heads/...` name.  Later command builders do
/// not accept a ref argument, so no caller can redirect the CAS to another
/// namespace.
#[derive(Clone)]
pub(super) struct DeliveryGitSourceRef(String);

impl DeliveryGitSourceRef {
    pub(super) fn try_new(source_branch: &str) -> Result<Self, CommandPolicyError> {
        super::super::validate_worktree_branch_name(source_branch)?;
        let (_, attempt_text) = source_branch
            .rsplit_once("-attempt-")
            .ok_or(CommandPolicyError::InvalidGitBinding)?;
        let attempt = attempt_text
            .parse::<u32>()
            .map_err(|_| CommandPolicyError::InvalidGitBinding)?;
        if attempt == 0 || attempt_text.starts_with('0') {
            return Err(CommandPolicyError::InvalidGitBinding);
        }
        Ok(Self(format!("refs/heads/{source_branch}")))
    }

    pub(super) fn as_str(&self) -> &str {
        &self.0
    }
}

impl DeliveryGitSourceMutationBinding {
    pub(crate) fn try_new(
        factory: DeliveryGitMutationCommandFactory,
        read_only: &DeliveryGitReadOnlyBinding,
        object_id_length: usize,
    ) -> Result<Self, CommandPolicyError> {
        if !matches!(object_id_length, 40 | 64) {
            return Err(CommandPolicyError::InvalidGitBinding);
        }
        factory.revalidate_for(&read_only.git)?;
        revalidate_delivery_read_only_binding(read_only)?;
        Ok(Self {
            factory,
            binding: clone_delivery_read_only_binding(read_only),
            object_id_length,
            source_ref: None,
        })
    }

    /// Narrows this source-object binding to the one authenticated source
    /// branch whose real index and ref may be changed during CommitPending.
    ///
    /// `source_branch` is accepted only at this capability-construction
    /// boundary.  The resulting binding retains the validated full ref name
    /// internally and exposes no ref-taking command method.
    pub(crate) fn real_index_binding(
        &self,
        source_branch: &str,
    ) -> Result<Self, CommandPolicyError> {
        self.revalidate()?;
        Ok(Self {
            factory: self.factory.clone(),
            binding: clone_delivery_read_only_binding(&self.binding),
            object_id_length: self.object_id_length,
            source_ref: Some(DeliveryGitSourceRef::try_new(source_branch)?),
        })
    }

    /// Returns the already authenticated worktree authority used exclusively
    /// by Task 11 to take a no-follow, identity-bound byte snapshot. No path
    /// is returned and this does not create a generic command input slot.
    pub(crate) fn snapshot_work_tree(&self) -> Result<Arc<ExecutionDirectory>, CommandPolicyError> {
        self.revalidate()?;
        Ok(Arc::clone(&self.binding.repository.work_tree))
    }

    pub(crate) fn read_tree(
        &self,
        temporary_index: &DeliveryGitTemporaryIndexEnvironment,
        base: &str,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        self.require_object_id(base)?;
        self.command(
            Some(temporary_index),
            ["read-tree", "--reset", base],
            None,
            None,
        )
    }

    /// Hashes one already captured snapshot file exactly as supplied. The
    /// fixed `--no-filters --stdin` shape is the security boundary: Git is
    /// never given a worktree path to reopen, and no worktree attributes or
    /// `filter.*` configuration can select a clean/process helper.
    pub(crate) fn hash_snapshot_file(
        &self,
        input: ExactChildInput,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        self.command(
            None,
            ["hash-object", "-w", "--no-filters", "--stdin"],
            Some(input),
            None,
        )
    }

    /// Reconstructs a temporary index from fully validated, NUL-delimited
    /// cache-info records.  The caller cannot supply an argv/path/environment
    /// slot; only an exact internal stdin payload is admitted.
    pub(crate) fn update_index_info(
        &self,
        temporary_index: &DeliveryGitTemporaryIndexEnvironment,
        input: ExactChildInput,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        self.command(
            Some(temporary_index),
            ["update-index", "--add", "--replace", "-z", "--index-info"],
            Some(input),
            None,
        )
    }

    /// Lists the exact index entries used to construct the Task 11 snapshot.
    /// This delegates to the same fixed read-only vocabulary used during
    /// authentication; no caller-selected path or listing arguments exist.
    pub(crate) fn index_entries(&self) -> Result<ValidatedCommand, CommandPolicyError> {
        ValidatedCommand::delivery_index_entries(&self.binding)
    }

    /// Lists the exact non-ignored untracked paths used to construct the Task
    /// 11 snapshot.  Its ignore behavior is fixed by the read-only binding.
    pub(crate) fn untracked_paths(&self) -> Result<ValidatedCommand, CommandPolicyError> {
        ValidatedCommand::delivery_untracked_paths(&self.binding)
    }

    /// Lists only the base paths removed from the current index. This feeds
    /// the private-index replay operation, not a generic diff surface.
    pub(crate) fn deleted_base_paths(
        &self,
        base: &str,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        ValidatedCommand::delivery_deleted_base_paths(&self.binding, base, self.object_id_length)
    }

    pub(crate) fn write_tree(
        &self,
        temporary_index: &DeliveryGitTemporaryIndexEnvironment,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        self.command(Some(temporary_index), ["write-tree"], None, None)
    }

    pub(crate) fn commit_tree(
        &self,
        tree: &str,
        parent: &str,
        input: ExactChildInput,
        metadata: &DeliveryGitCommitEnvironment,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        self.require_object_id(tree)?;
        self.require_object_id(parent)?;
        self.command(
            None,
            ["commit-tree", "--no-gpg-sign", tree, "-p", parent],
            Some(input),
            Some(metadata),
        )
    }

    pub(crate) fn cat_file_commit(
        &self,
        object: &str,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        self.require_object_id(object)?;
        let input = ExactChildInput::try_new(cat_file_batch_input(object))
            .map_err(|_| CommandPolicyError::InvalidGitBinding)?;
        self.command(None, ["cat-file", "--batch"], Some(input), None)
    }

    /// Reports the exact object type for one previously syntax-validated
    /// candidate object. The fixed `-t` form deliberately does not peel a
    /// commit or tag to a tree; the delivery adapter accepts only `tree\n`.
    pub(crate) fn cat_file_candidate_type(
        &self,
        candidate: &str,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        self.require_object_id(candidate)?;
        self.command(None, ["cat-file", "-t", candidate], None, None)
    }

    /// Replaces the real source index with the already authenticated candidate
    /// tree without asking Git to inspect worktree content.  This closes the
    /// interval in which a changed attributes/filter configuration could run
    /// repository-controlled code after source revalidation.
    ///
    /// This command is reachable only from a source-ref-bound real-index
    /// capability, never from the unreferenced-object builder.
    pub(crate) fn stage_candidate_in_real_index(
        &self,
        candidate: &str,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        self.require_object_id(candidate)?;
        self.real_index_command(["read-tree", "--reset", candidate])
    }

    /// Refreshes only stat-cache data for the already staged real index. The
    /// command has no caller-controlled path or argument surface; `-q` avoids
    /// treating normal cache refresh observations as user-facing diagnostics.
    pub(crate) fn refresh_real_index_stat(&self) -> Result<ValidatedCommand, CommandPolicyError> {
        self.real_index_command(["update-index", "--refresh", "-q"])
    }

    /// Atomically changes only the retained source branch from `base` to the
    /// already verified expected source commit. `--no-deref` prevents a
    /// symbolic ref from redirecting the CAS to another namespace.
    pub(crate) fn update_source_ref_cas(
        &self,
        expected: &str,
        base: &str,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        self.require_object_id(expected)?;
        self.require_object_id(base)?;
        let source_ref = self.require_source_ref()?.as_str();
        self.real_index_command(["update-ref", "--no-deref", source_ref, expected, base])
    }

    /// Purely compares the real source index with the approved candidate tree.
    /// Exit status zero means exact equality; one means a different index.
    pub(crate) fn index_matches_tree(
        &self,
        tree: &str,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        self.require_object_id(tree)?;
        self.real_index_command(["diff-index", "--cached", "--quiet", tree, "--"])
    }

    /// Purely compares the real source work tree with its real index. Exit
    /// status zero means exact equality; one means a different work tree.
    pub(crate) fn worktree_matches_index(&self) -> Result<ValidatedCommand, CommandPolicyError> {
        self.real_index_command(["diff-files", "--quiet", "--"])
    }

    fn command<const N: usize>(
        &self,
        temporary_index: Option<&DeliveryGitTemporaryIndexEnvironment>,
        command_arguments: [&str; N],
        exact_input: Option<ExactChildInput>,
        commit_metadata: Option<&DeliveryGitCommitEnvironment>,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        self.command_owned(
            temporary_index,
            command_arguments.into_iter().map(OsString::from).collect(),
            exact_input,
            commit_metadata,
        )
    }

    fn command_owned(
        &self,
        temporary_index: Option<&DeliveryGitTemporaryIndexEnvironment>,
        command_arguments: Vec<OsString>,
        exact_input: Option<ExactChildInput>,
        commit_metadata: Option<&DeliveryGitCommitEnvironment>,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        self.revalidate()?;
        let environment = match temporary_index {
            Some(temporary_index) => {
                temporary_index.require_distinct_from(&self.binding)?;
                temporary_index.child_environment(&self.binding.environment)?
            }
            None => self.binding.environment.clone(),
        };
        let environment = match commit_metadata {
            Some(metadata) => metadata.child_environment(&environment)?,
            None => environment,
        };
        let mut arguments = self.binding.repository.delivery_fixed_arguments();
        append_delivery_read_only_configuration(&mut arguments);
        arguments.extend(command_arguments);
        let mut dependent_directories = vec![
            Arc::clone(&self.binding.repository.git_directory),
            Arc::clone(&self.binding.repository.work_tree),
            Arc::clone(&self.binding.common_git),
            Arc::clone(&self.binding.sandbox),
        ];
        if let Some(temporary_index) = temporary_index {
            dependent_directories.push(Arc::clone(temporary_index.directory()));
        }
        let mut command = ValidatedCommand::build_git(
            Arc::clone(&self.binding.git),
            &self.binding.repository,
            arguments,
            environment,
            self.binding.timeout,
        )?
        .with_dependent_directories(dependent_directories)?;
        #[cfg(unix)]
        {
            let directory_bindings = match temporary_index {
                Some(temporary_index) => {
                    super::super::UnixDeliveryDirectoryBindings::repository_with_temporary_index(
                        Arc::clone(&self.binding.repository.git_directory),
                        Arc::clone(&self.binding.repository.work_tree),
                        Arc::clone(&self.binding.common_git),
                        Arc::clone(temporary_index.directory()),
                    )
                }
                None => super::super::UnixDeliveryDirectoryBindings::repository(
                    Arc::clone(&self.binding.repository.git_directory),
                    Arc::clone(&self.binding.repository.work_tree),
                    Arc::clone(&self.binding.common_git),
                ),
            };
            command = command.with_delivery_unix_directory_bindings(directory_bindings)?;
        }
        command = command.with_delivery_git_empty_config(Arc::clone(&self.binding.config))?;
        command.exact_input = exact_input;
        Ok(command)
    }

    fn real_index_command<const N: usize>(
        &self,
        command_arguments: [&str; N],
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        self.require_source_ref()?;
        self.command(None, command_arguments, None, None)
    }

    fn require_source_ref(&self) -> Result<&DeliveryGitSourceRef, CommandPolicyError> {
        self.source_ref
            .as_ref()
            .ok_or(CommandPolicyError::InvalidGitBinding)
    }

    fn require_object_id(&self, object: &str) -> Result<(), CommandPolicyError> {
        if object.len() == self.object_id_length
            && object
                .as_bytes()
                .iter()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
            && object.as_bytes().iter().any(|byte| *byte != b'0')
        {
            Ok(())
        } else {
            Err(CommandPolicyError::InvalidGitBinding)
        }
    }

    fn revalidate(&self) -> Result<(), CommandPolicyError> {
        self.factory.revalidate_for(&self.binding.git)?;
        revalidate_delivery_read_only_binding(&self.binding)
    }
}

impl fmt::Debug for DeliveryGitSourceMutationBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryGitSourceMutationBinding(<opaque>)")
    }
}
