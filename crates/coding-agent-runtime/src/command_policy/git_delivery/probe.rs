use super::*;

const TARGET_REF: &str = "refs/heads/main";
const SOURCE_REF: &str = "refs/heads/coding-agent-probe-source";
const MERGE_MESSAGE: &str = "coding-agent: delivery capability probe";

/// A Git object identity admitted only after object-format-aware validation.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ProbeGitObjectId(String);

impl ProbeGitObjectId {
    pub(crate) fn try_new(value: &str, hexadecimal_length: usize) -> Option<Self> {
        if !matches!(hexadecimal_length, 40 | 64)
            || value.len() != hexadecimal_length
            || !value
                .as_bytes()
                .iter()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return None;
        }
        Some(Self(value.to_owned()))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ProbeGitObjectId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProbeGitObjectId(<validated>)")
    }
}

/// Typed commands which may run before the private probe repository exists.
#[derive(Clone)]
pub(crate) struct DeliveryGitProbeCommands {
    git: Arc<PinnedExecutable>,
    repository: Arc<ExecutionDirectory>,
    config: Arc<DeliveryGitEmptyConfig>,
    environment: ChildEnvironment,
    timeout: Duration,
}

impl DeliveryGitProbeCommands {
    pub(crate) fn try_new(
        git: Arc<PinnedExecutable>,
        repository: Arc<ExecutionDirectory>,
        config: Arc<DeliveryGitEmptyConfig>,
        environment: ChildEnvironment,
        timeout: Duration,
    ) -> Result<Self, CommandPolicyError> {
        git.revalidate()?;
        repository.revalidate()?;
        config.validates_delivery_git_environment(&environment)?;
        if timeout.is_zero() {
            return Err(CommandPolicyError::InvalidTimeout);
        }
        Ok(Self {
            git,
            repository,
            config,
            environment,
            timeout,
        })
    }

    pub(crate) fn version(&self) -> Result<ValidatedCommand, CommandPolicyError> {
        ValidatedCommand::build(
            Arc::clone(&self.git),
            Arc::clone(&self.repository),
            vec![OsString::from("--version")],
            self.environment.clone(),
            self.timeout,
        )
        .and_then(|command| self.with_probe_config(command))
    }

    pub(crate) fn initialize_repository(&self) -> Result<ValidatedCommand, CommandPolicyError> {
        let mut arguments = unbound_probe_arguments();
        arguments.extend([
            OsString::from("init"),
            OsString::from("--quiet"),
            OsString::from("--initial-branch=main"),
        ]);
        let command = ValidatedCommand::build(
            Arc::clone(&self.git),
            Arc::clone(&self.repository),
            arguments,
            self.environment.clone(),
            self.timeout,
        )?;
        self.with_probe_config(command)
    }

    pub(crate) fn bind_repository(
        &self,
        git_directory: Arc<ExecutionDirectory>,
    ) -> Result<DeliveryGitRepositoryProbeCommands, CommandPolicyError> {
        let binding = GitCommandBinding::try_new(git_directory, Arc::clone(&self.repository))?;
        Ok(DeliveryGitRepositoryProbeCommands {
            git: Arc::clone(&self.git),
            binding,
            config: Arc::clone(&self.config),
            environment: self.environment.clone(),
            commit_environment: commit_environment(&self.environment),
            timeout: self.timeout,
        })
    }

    fn with_probe_config(
        &self,
        command: ValidatedCommand,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        command.with_delivery_git_empty_config(Arc::clone(&self.config))
    }
}

impl fmt::Debug for DeliveryGitProbeCommands {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryGitProbeCommands(<opaque>)")
    }
}

/// Typed commands used only inside the application-private probe repository.
pub(crate) struct DeliveryGitRepositoryProbeCommands {
    git: Arc<PinnedExecutable>,
    binding: GitCommandBinding,
    config: Arc<DeliveryGitEmptyConfig>,
    environment: ChildEnvironment,
    commit_environment: ChildEnvironment,
    timeout: Duration,
}

impl DeliveryGitRepositoryProbeCommands {
    pub(crate) fn object_format(&self) -> Result<ValidatedCommand, CommandPolicyError> {
        self.command(["rev-parse", "--show-object-format"])
    }

    pub(crate) fn empty_tree(&self) -> Result<ValidatedCommand, CommandPolicyError> {
        self.command_with_input(["mktree"], Vec::new())
    }

    pub(crate) fn probe_blob(&self) -> Result<ValidatedCommand, CommandPolicyError> {
        self.command_with_input(
            ["hash-object", "-w", "--stdin"],
            b"P4-B delivery probe\n".to_vec(),
        )
    }

    pub(crate) fn source_tree(
        &self,
        blob: &ProbeGitObjectId,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        let input = format!("100644 blob {}\tprobe.txt\n", blob.as_str()).into_bytes();
        self.command_with_input(["mktree"], input)
    }

    pub(crate) fn base_commit(
        &self,
        tree: &ProbeGitObjectId,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        self.commit_tree(tree, None, b"base\n".to_vec())
    }

    pub(crate) fn target_commit(
        &self,
        tree: &ProbeGitObjectId,
        parent: &ProbeGitObjectId,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        self.commit_tree(tree, Some(parent), b"target\n".to_vec())
    }

    pub(crate) fn source_commit(
        &self,
        tree: &ProbeGitObjectId,
        parent: &ProbeGitObjectId,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        self.commit_tree(tree, Some(parent), b"source\n".to_vec())
    }

    pub(crate) fn set_target_ref(
        &self,
        target: &ProbeGitObjectId,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        self.command(["update-ref", TARGET_REF, target.as_str()])
    }

    pub(crate) fn set_source_ref(
        &self,
        source: &ProbeGitObjectId,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        self.command(["update-ref", SOURCE_REF, source.as_str()])
    }

    pub(crate) fn merge_tree(
        &self,
        target: &ProbeGitObjectId,
        source: &ProbeGitObjectId,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        self.command([
            "merge-tree",
            "--write-tree",
            "--messages",
            "--name-only",
            "-z",
            target.as_str(),
            source.as_str(),
        ])
    }

    pub(crate) fn merge(
        &self,
        source: &ProbeGitObjectId,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        let mut arguments = self.fixed_arguments();
        arguments.extend(merge_arguments(source));
        ValidatedCommand::build_git(
            Arc::clone(&self.git),
            &self.binding,
            arguments,
            self.commit_environment.clone(),
            self.timeout,
        )
        .and_then(|command| self.with_probe_dependencies(command))
    }

    pub(crate) fn resolve_head(&self) -> Result<ValidatedCommand, CommandPolicyError> {
        self.command(["rev-parse", "--verify", "--end-of-options", "HEAD^{commit}"])
    }

    pub(crate) fn delete_source_transaction(
        &self,
        target: &ProbeGitObjectId,
        source: &ProbeGitObjectId,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        let input = delete_source_transaction_input(target, source);
        self.command_with_input(["update-ref", "--stdin"], input)
    }

    pub(crate) fn source_ref_exists(&self) -> Result<ValidatedCommand, CommandPolicyError> {
        self.command(["show-ref", "--verify", "--quiet", SOURCE_REF])
    }

    fn commit_tree(
        &self,
        tree: &ProbeGitObjectId,
        parent: Option<&ProbeGitObjectId>,
        message: Vec<u8>,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        let mut arguments = self.fixed_arguments();
        arguments.extend([OsString::from("commit-tree"), OsString::from(tree.as_str())]);
        if let Some(parent) = parent {
            arguments.extend([OsString::from("-p"), OsString::from(parent.as_str())]);
        }
        self.build_with_input(arguments, self.commit_environment.clone(), message)
    }

    fn command<const N: usize>(
        &self,
        arguments: [&str; N],
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        let mut fixed = self.fixed_arguments();
        fixed.extend(arguments.into_iter().map(OsString::from));
        ValidatedCommand::build_git(
            Arc::clone(&self.git),
            &self.binding,
            fixed,
            self.environment.clone(),
            self.timeout,
        )
        .and_then(|command| self.with_probe_dependencies(command))
    }

    fn command_with_input<const N: usize>(
        &self,
        arguments: [&str; N],
        input: Vec<u8>,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        let mut fixed = self.fixed_arguments();
        fixed.extend(arguments.into_iter().map(OsString::from));
        self.build_with_input(fixed, self.environment.clone(), input)
    }

    fn build_with_input(
        &self,
        arguments: Vec<OsString>,
        environment: ChildEnvironment,
        input: Vec<u8>,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        let exact_input =
            ExactChildInput::try_new(input).map_err(|_| CommandPolicyError::InvalidGitBinding)?;
        let mut command = ValidatedCommand::build_git(
            Arc::clone(&self.git),
            &self.binding,
            arguments,
            environment,
            self.timeout,
        )?;
        command.exact_input = Some(exact_input);
        self.with_probe_dependencies(command)
    }

    fn fixed_arguments(&self) -> Vec<OsString> {
        let mut arguments = self.binding.delivery_fixed_arguments();
        append_probe_configuration(&mut arguments);
        arguments
    }

    fn with_probe_dependencies(
        &self,
        command: ValidatedCommand,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        let command = command.with_dependent_directories(vec![
            Arc::clone(&self.binding.git_directory),
            Arc::clone(&self.binding.work_tree),
        ])?;
        #[cfg(unix)]
        let command = command.with_delivery_unix_directory_bindings(
            super::super::UnixDeliveryDirectoryBindings::repository(
                Arc::clone(&self.binding.git_directory),
                Arc::clone(&self.binding.work_tree),
                Arc::clone(&self.binding.git_directory),
            ),
        )?;
        self.with_probe_config(command)
    }

    fn with_probe_config(
        &self,
        command: ValidatedCommand,
    ) -> Result<ValidatedCommand, CommandPolicyError> {
        command.with_delivery_git_empty_config(Arc::clone(&self.config))
    }
}

impl fmt::Debug for DeliveryGitRepositoryProbeCommands {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryGitRepositoryProbeCommands(<opaque>)")
    }
}

pub(super) fn unbound_probe_arguments() -> Vec<OsString> {
    let mut arguments = vec![
        OsString::from("--no-pager"),
        OsString::from("--literal-pathspecs"),
        OsString::from("--no-optional-locks"),
        OsString::from("--no-replace-objects"),
        OsString::from("--no-lazy-fetch"),
    ];
    append_probe_configuration(&mut arguments);
    arguments.extend([OsString::from("-c"), OsString::from("init.templateDir=")]);
    arguments
}

pub(super) fn append_probe_configuration(arguments: &mut Vec<OsString>) {
    for configuration in [
        "commit.gpgSign=false",
        "merge.gpgSign=false",
        "merge.verifySignatures=false",
        "merge.autoStash=false",
        "merge.stat=false",
        "merge.log=false",
        "rerere.enabled=false",
        "credential.helper=",
        "core.askPass=",
        "core.attributesFile=",
        "core.excludesFile=",
        "user.name=Coding Agent Probe",
        "user.email=probe@invalid.local",
    ] {
        arguments.extend([OsString::from("-c"), OsString::from(configuration)]);
    }
    arguments.extend([
        OsString::from("-c"),
        OsString::from(super::super::git_hooks_path_configuration()),
    ]);
}

pub(super) fn merge_arguments(source: &ProbeGitObjectId) -> Vec<OsString> {
    [
        "merge",
        "--no-ff",
        "--strategy=ort",
        "--no-edit",
        "--no-verify",
        "--no-verify-signatures",
        "--no-gpg-sign",
        "--no-autostash",
        "--no-rerere-autoupdate",
        "--no-overwrite-ignore",
        "--no-log",
        "--no-stat",
        "--cleanup=verbatim",
        "-m",
        MERGE_MESSAGE,
        "--",
        source.as_str(),
    ]
    .into_iter()
    .map(OsString::from)
    .collect()
}

pub(super) fn delete_source_transaction_input(
    target: &ProbeGitObjectId,
    source: &ProbeGitObjectId,
) -> Vec<u8> {
    format!(
        "start\nverify {TARGET_REF} {}\ndelete {SOURCE_REF} {}\nprepare\ncommit\n",
        target.as_str(),
        source.as_str()
    )
    .into_bytes()
}

pub(super) fn commit_environment(environment: &ChildEnvironment) -> ChildEnvironment {
    let mut entries = environment.entries().clone();
    for (key, value) in [
        ("GIT_AUTHOR_NAME", "Coding Agent Probe"),
        ("GIT_AUTHOR_EMAIL", "probe@invalid.local"),
        ("GIT_AUTHOR_DATE", "2000-01-01T00:00:00+0000"),
        ("GIT_COMMITTER_NAME", "Coding Agent Probe"),
        ("GIT_COMMITTER_EMAIL", "probe@invalid.local"),
        ("GIT_COMMITTER_DATE", "2000-01-01T00:00:00+0000"),
        ("GIT_MERGE_AUTOEDIT", "no"),
    ] {
        entries.insert(OsString::from(key), OsString::from(value));
    }
    ChildEnvironment::from_entries(entries)
}
